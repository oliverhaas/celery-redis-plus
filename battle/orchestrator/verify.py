from __future__ import annotations

import json
import math
from array import array
from dataclasses import dataclass, field
from itertools import batched, islice
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable

    import redis

# Event types with a known expected set, so a missing one is measurable event loss.
EVENT_TYPES = ("task-sent", "task-received", "task-started", "task-succeeded")
# Deliberately NOT an EVENT_TYPES entry: no task is expected to fail, so there is nothing to
# measure loss against, and the row would report an empty 0/0 instead of the count.
FAILURE_EVENT_TYPE = "task-failed"
# Host and container stamp their times against the same kernel clock, so this only absorbs
# jitter; countdowns are 5-30s, far too large for it to mask a real early delivery.
EARLY_DELIVERY_TOLERANCE = 0.5
# One SCAN batch, one pipeline: neither the key list nor the reply list is ever ledger-sized.
SCAN_BATCH = 5000
# Ids kept per bucket. The count beside them is the truth; these are for the human to grep.
ID_SAMPLE_LIMIT = 50
# Scratch destination for the executed-but-never-submitted difference. Matches none of the
# scan patterns below, and redis drops it when the difference is empty.
_UNEXPECTED_KEY = "verify:unexpected"
_EXPECTED_SET = {
    "task-sent": "submitted_ids",
    "task-received": "executed_ids",
    "task-started": "executed_ids",
    "task-succeeded": "executed_ids",
}
_MB = 1024 * 1024


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return math.nan
    k = max(0, math.ceil(p / 100.0 * len(sorted_values)) - 1)
    return sorted_values[k]


@dataclass
class IdSample:
    """A full count plus at most `ID_SAMPLE_LIMIT` ids, so a bucket cannot grow with the run."""

    count: int = 0
    sample: list[str] = field(default_factory=list)

    def add(self, task_id: str) -> None:
        self.count += 1
        if len(self.sample) < ID_SAMPLE_LIMIT:
            self.sample.append(task_id)

    def ordered(self) -> list[str]:
        """Scan order is arbitrary; sorting the sample keeps a small run's output stable."""
        return sorted(self.sample)


@dataclass
class TaskStats:
    submitted: int = 0
    exactly_once: int = 0
    failed: IdSample = field(default_factory=IdSample)
    missing: IdSample = field(default_factory=IdSample)
    duplicates_hard_kill: IdSample = field(default_factory=IdSample)
    duplicates_soft_kill: IdSample = field(default_factory=IdSample)
    duplicates_unattributed: IdSample = field(default_factory=IdSample)
    unexpected: IdSample = field(default_factory=IdSample)


def attributable(
    first: float,
    last: float,
    kill_times: Iterable[float],
    visibility_timeout: float,
    margin: float,
) -> bool:
    """Whether some kill could have abandoned this task between its first and last execution."""
    # The trailing bound stops a kill long before the second execution from excusing it.
    return any(first - margin <= t_kill <= last <= t_kill + visibility_timeout + margin for t_kill in kill_times)


class TaskFolder:
    """Folds one submitted task at a time into counts, capped id samples, and diagnostics."""

    def __init__(
        self,
        hard_kill_times: list[float],
        soft_kill_times: list[float],
        visibility_timeout: float,
        margin: float = 5.0,
    ) -> None:
        self.hard_kill_times = hard_kill_times
        self.soft_kill_times = soft_kill_times
        self.visibility_timeout = visibility_timeout
        self.margin = margin
        self.stats = TaskStats()
        self.diagnostics: dict[str, list[list[str | float]]] = {}

    def add(self, task_id: str, executions: list[tuple[str, float]], *, failed: bool = False) -> None:
        """`executions` holds (hostname, started_at) pairs, ascending by timestamp."""
        self.stats.submitted += 1
        if not executions:
            # A task whose body raised leaves the same hole in the ledger as a lost message; the
            # worker's own failure event is the only thing that tells the two apart.
            bucket = self.stats.failed if failed else self.stats.missing
            bucket.add(task_id)
        elif len(executions) == 1:
            self.stats.exactly_once += 1
        else:
            self._add_duplicate(task_id, executions)

    def _add_duplicate(self, task_id: str, executions: list[tuple[str, float]]) -> None:
        first, last = executions[0][1], executions[-1][1]
        # Hard is tested first so a duplicate bracketing both is never masked by the soft one.
        if attributable(first, last, self.hard_kill_times, self.visibility_timeout, self.margin):
            self.stats.duplicates_hard_kill.add(task_id)
        elif attributable(first, last, self.soft_kill_times, self.visibility_timeout, self.margin):
            self.stats.duplicates_soft_kill.add(task_id)
        else:
            self.stats.duplicates_unattributed.add(task_id)
            # The ledger redis is gone by the time anyone reads the scorecard, so which container
            # ran an unexplained re-execution, and when, has to be captured here or not at all.
            if len(self.diagnostics) < ID_SAMPLE_LIMIT:
                self.diagnostics[task_id] = [list(execution) for execution in executions]


def _dist(values: array[float]) -> dict:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        # Carried because a percentile cannot express an early delivery: a negative latency only
        # pulls p50 down, which reads as an improvement.
        "min": ordered[0] if ordered else math.nan,
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": ordered[-1] if ordered else math.nan,
    }


class LatencyFolder:
    """Accumulates latencies into array('d'), 8 bytes a value against a Python list's ~32."""

    def __init__(self, visibility_timeout: float) -> None:
        self.visibility_timeout = visibility_timeout
        self.per_type: dict[str, array[float]] = {}
        self.first_exec_past_vt = 0
        self.early = IdSample()

    def add(self, task_id: str, task_type: str, eligible_at: float, first_execution: float) -> None:
        latency = first_execution - eligible_at
        self.per_type.setdefault(task_type, array("d")).append(latency)
        # A proxy for visibility-timeout recovery, not a count of restore operations: a first
        # execution this far past eligibility almost certainly waited out at least one VT window.
        if latency > self.visibility_timeout:
            self.first_exec_past_vt += 1
        # A countdown message delivered before its due time is a delayed-delivery defect that
        # every other measurement here reports as an improvement.
        if latency < -EARLY_DELIVERY_TOLERANCE:
            self.early.add(task_id)

    def result(self) -> dict:
        overall: array[float] = array("d")
        for values in self.per_type.values():
            overall.extend(values)
        return {
            "overall": _dist(overall),
            "per_type": {task_type: _dist(values) for task_type, values in sorted(self.per_type.items())},
            "first_exec_past_vt": self.first_exec_past_vt,
            "early_deliveries": self.early.count,
            "early_deliveries_sample": self.early.ordered(),
        }


class EventFolder:
    """Folds one ledgered event at a time into per-type seen counts and delay distributions."""

    def __init__(self) -> None:
        self.seen = dict.fromkeys(EVENT_TYPES, 0)
        self.delays: dict[str, array[float]] = {event_type: array("d") for event_type in EVENT_TYPES}

    def add(self, event_type: str, event_ts: float, received_at: float, *, expected: bool) -> None:
        if expected:
            self.seen[event_type] += 1
        if event_ts and received_at:
            self.delays[event_type].append(received_at - event_ts)

    def result(self, expected_counts: dict[str, int]) -> dict:
        result: dict[str, dict] = {}
        for event_type in EVENT_TYPES:
            expected = expected_counts[event_type]
            seen = self.seen[event_type]
            delays = sorted(self.delays[event_type])
            result[event_type] = {
                "expected": expected,
                "seen": seen,
                "loss_pct": (100.0 * (expected - seen) / expected) if expected else 0.0,
                "delay_p50": percentile(delays, 50),
                "delay_p95": percentile(delays, 95),
                "delay_max": delays[-1] if delays else math.nan,
            }
        return result


@dataclass
class LedgerData:
    """Aggregates only: nothing here grows with task count beyond the capped id samples."""

    tasks: TaskStats = field(default_factory=TaskStats)
    latency: dict = field(default_factory=lambda: LatencyFolder(0).result())
    events: dict = field(default_factory=lambda: EventFolder().result(dict.fromkeys(EVENT_TYPES, 0)))
    duplicate_diagnostics: dict = field(default_factory=dict)
    kills: list[dict] = field(default_factory=list)


@dataclass
class RunSignals:
    """Producer/chaos counters, owned by the caller so a `KeyboardInterrupt` still reports them."""

    unexpected_deaths: list[str] = field(default_factory=list)
    restart_failures: list[str] = field(default_factory=list)
    producer_errors: int = 0
    chaos_errors: int = 0
    chaos_join_timed_out: bool = False


def _as_float(raw: bytes | None) -> float:
    return float(raw or 0.0)


def _execution_row(entry: bytes) -> tuple[str, float]:
    """One `hostname,started_at` ledger row. The hostname names the container that ran it."""
    hostname, _, started_at = entry.decode().rpartition(",")
    return hostname, float(started_at)


def _add_unexecuted(client: redis.Redis, tasks: TaskFolder, task_ids: list[str]) -> None:
    """Splits a batch's unexecuted tasks into worker-side failures and holes in the ledger."""
    if not task_ids:
        return
    pipe = client.pipeline(transaction=False)
    for task_id in task_ids:
        pipe.exists(f"event:{FAILURE_EVENT_TYPE}:{task_id}")
    for task_id, reported_failed in zip(task_ids, pipe.execute(), strict=True):
        tasks.add(task_id, [], failed=bool(reported_failed))


def _walk_submitted(client: redis.Redis, tasks: TaskFolder, latency: LatencyFolder) -> None:
    """Folds every submitted task into the two folders, one SCAN batch and one pipeline at a time."""
    keys = client.scan_iter(match="submitted:*", count=SCAN_BATCH)
    for batch in batched(keys, SCAN_BATCH, strict=False):
        task_ids = [key.decode().removeprefix("submitted:") for key in batch]
        pipe = client.pipeline(transaction=False)
        for task_id in task_ids:
            # Only the two fields something reads; `priority` and `sent_at` stay unread in redis.
            pipe.hmget(f"submitted:{task_id}", ["type", "eligible_at"])
            pipe.lrange(f"executions:{task_id}", 0, -1)
        replies = pipe.execute()
        unexecuted: list[str] = []
        for task_id, meta, entries in zip(task_ids, replies[::2], replies[1::2], strict=True):
            executions = sorted((_execution_row(entry) for entry in entries), key=lambda row: row[1])
            if not executions:
                unexecuted.append(task_id)
                continue
            tasks.add(task_id, executions)
            latency.add(task_id, meta[0].decode(), _as_float(meta[1]), executions[0][1])
        _add_unexecuted(client, tasks, unexecuted)


def _walk_events(client: redis.Redis, expected_counts: dict[str, int]) -> dict:
    folder = EventFolder()
    keys = client.scan_iter(match="event:*", count=SCAN_BATCH)
    for batch in batched(keys, SCAN_BATCH, strict=False):
        wanted: list[str] = []
        pipe = client.pipeline(transaction=False)
        for raw_key in batch:
            key = raw_key.decode()
            _, event_type, task_uuid = key.split(":", 2)
            if event_type not in _EXPECTED_SET:
                continue
            wanted.append(event_type)
            pipe.hmget(key, ["event_ts", "received_at"])
            # Membership resolved redis-side, so the host never holds an id set of its own.
            pipe.sismember(_EXPECTED_SET[event_type], task_uuid)
        if not wanted:
            continue
        replies = pipe.execute()
        for event_type, stamps, expected in zip(wanted, replies[::2], replies[1::2], strict=True):
            folder.add(event_type, _as_float(stamps[0]), _as_float(stamps[1]), expected=bool(expected))
    return folder.result(expected_counts)


def _unexpected_executions(client: redis.Redis) -> IdSample:
    """Executed ids with no submission, differenced redis-side rather than on the host."""
    count = cast("int", client.sdiffstore(_UNEXPECTED_KEY, ["executed_ids", "submitted_ids"]))
    if not count:
        return IdSample()
    sample = [task_id.decode() for task_id in islice(client.sscan_iter(_UNEXPECTED_KEY), ID_SAMPLE_LIMIT)]
    return IdSample(count=count, sample=sample)


def read_ledger(client: redis.Redis, visibility_timeout: float) -> LedgerData:
    """Streams the ledger into aggregates. Host memory is flat in task count, by construction."""
    kills = [json.loads(entry) for entry in client.lrange("kills", 0, -1)]  # ty: ignore[not-iterable]
    tasks = TaskFolder(
        [kill["t_kill"] for kill in kills if kill.get("sigkilled")],
        [kill["t_kill"] for kill in kills if not kill.get("sigkilled")],
        visibility_timeout,
    )
    latency = LatencyFolder(visibility_timeout)
    _walk_submitted(client, tasks, latency)
    tasks.stats.unexpected = _unexpected_executions(client)
    cardinality = {key: cast("int", client.scard(key)) for key in ("submitted_ids", "executed_ids")}
    expected_counts = {event_type: cardinality[key] for event_type, key in _EXPECTED_SET.items()}
    return LedgerData(
        tasks=tasks.stats,
        latency=latency.result(),
        events=_walk_events(client, expected_counts),
        duplicate_diagnostics=tasks.diagnostics,
        kills=kills,
    )


# kombu tracks in-flight messages fleet-wide rather than per queue: one hash holding the bodies
# and one sorted set holding their visibility deadlines.
STOCK_UNACKED_KEY = "unacked"
STOCK_UNACKED_INDEX_KEY = "unacked_index"


def check_broker_clean(client: redis.Redis, transport: str) -> dict:
    """Counts what the transport left behind: queued messages, index entries, message bodies."""
    result: dict = {"queues": {}, "indices": {}, "message_hashes": 0}
    if transport == "stock":
        # Every list on the broker db is a kombu queue; matching on type rather than on a name
        # pattern covers the separator-suffixed priority sub-queues too.
        for key in client.scan_iter(count=1000, _type="list"):
            depth = client.llen(key)
            if depth:
                result["queues"][key.decode()] = depth
        held = cast("int", client.zcard(STOCK_UNACKED_INDEX_KEY))
        if held:
            result["indices"][STOCK_UNACKED_INDEX_KEY] = held
        # The unacked hash holds the message body itself, so an entry is stock's leaked message.
        result["message_hashes"] = cast("int", client.hlen(STOCK_UNACKED_KEY))
        return result
    for key in client.scan_iter(match="queue:*", count=1000):
        depth = client.zcard(key)
        if depth:
            result["queues"][key.decode()] = depth
    for key in client.scan_iter(match="messages_index:*", count=1000):
        depth = client.zcard(key)
        if depth:
            result["indices"][key.decode()] = depth
    result["message_hashes"] = sum(1 for _ in client.scan_iter(match="message:*", count=1000))
    return result


def broker_is_empty(broker_clean: dict) -> bool:
    """True when a `check_broker_clean` result holds no queue, index, or message keys."""
    return not (broker_clean["queues"] or broker_clean["indices"] or broker_clean["message_hashes"])


def build_scorecard(  # noqa: PLR0913
    config_info: dict,
    data: LedgerData,
    drain_ok: bool,
    signals: RunSignals,
    broker_clean: dict | None,
    transport: str,
) -> dict:
    enforced = transport == "plus" and config_info.get("acks_late", True)
    stats = data.tasks
    settled = _nothing_left_to_redeliver(config_info, drain_ok, broker_clean)
    buckets = {
        "failed": stats.failed,
        "lost": stats.missing if settled else IdSample(),
        "pending": IdSample() if settled else stats.missing,
        "duplicates_hard_kill": stats.duplicates_hard_kill,
        "duplicates_soft_kill": stats.duplicates_soft_kill,
        "duplicates_unattributed": stats.duplicates_unattributed,
        "unexpected": stats.unexpected,
    }
    tasks: dict = {"submitted": stats.submitted, "exactly_once": stats.exactly_once}
    for name, bucket in buckets.items():
        # Count and sample side by side: every reader wants the count, the human wants some ids.
        tasks[name] = bucket.count
        tasks[f"{name}_sample"] = bucket.ordered()
    scorecard = {
        "config": config_info,
        "kills": data.kills,
        "unexpected_deaths": signals.unexpected_deaths,
        "restart_failures": signals.restart_failures,
        "producer_errors": signals.producer_errors,
        "chaos_errors": signals.chaos_errors,
        "chaos_join_timed_out": signals.chaos_join_timed_out,
        "drain_ok": drain_ok,
        "tasks": tasks,
        "duplicate_diagnostics": data.duplicate_diagnostics,
        "latency": data.latency,
        "events": data.events,
        "broker_clean": broker_clean,
        "verdict": {"mode": "enforced" if enforced else "report-only"},
    }
    if enforced:
        passed, failures = evaluate_verdict(scorecard)
        scorecard["verdict"].update({"passed": passed, "failures": failures})  # ty: ignore[no-matching-overload]
    else:
        scorecard["verdict"].update({"passed": None, "failures": []})  # ty: ignore[no-matching-overload]
    return scorecard


def _task_verdict_failures(scorecard: dict) -> list[str]:
    """The verdict failures that come out of the task classification itself."""
    failures: list[str] = []
    tasks = scorecard["tasks"]
    # Every other check is "this count is zero", which an empty run satisfies trivially, so an
    # absent signal has to fail on its own terms rather than read as a good one.
    if not tasks["submitted"]:
        failures.append("no tasks were submitted")
    if tasks["lost"]:
        failures.append(f"{tasks['lost']} tasks lost (first: {tasks['lost_sample'][:5]})")
    if tasks["failed"]:
        failures.append(
            f"{tasks['failed']} tasks failed in the worker, not lost by the transport "
            f"(first: {tasks['failed_sample'][:5]})",
        )
    if tasks["pending"]:
        failures.append(f"{tasks['pending']} tasks still pending after drain timeout")
    if tasks["duplicates_unattributed"]:
        failures.append(
            f"{tasks['duplicates_unattributed']} duplicates not attributable to any kill "
            f"(first: {tasks['duplicates_unattributed_sample'][:5]})",
        )
    if tasks["unexpected"]:
        # The producer ledgers a submission only after a successful send, so each producer_error
        # accounts for exactly one of these. Without that count it reads as an invented task.
        failures.append(
            f"{tasks['unexpected']} executions never submitted "
            f"(producer_errors={scorecard['producer_errors']}, each of which loses one submission record)",
        )
    return failures


def _nothing_left_to_redeliver(config_info: dict, drain_ok: bool, broker_clean: dict | None) -> bool:
    """Whether an unexecuted task can still be called late rather than lost."""
    if drain_ok:
        return True
    if broker_clean is None:
        return False
    # Early ack drops a message before the task body runs, so an empty broker there says nothing
    # about work still in flight. Only under acks_late does empty prove nothing can come back.
    return bool(config_info.get("acks_late", True)) and broker_is_empty(broker_clean)


def _duplicate_rate_failure(scorecard: dict) -> str | None:
    """Fails a run that re-executes too much work per kill, whatever the attribution says.

    The excusal window is ~35s wide and kills land ~19s apart, so attribution alone would absorb
    a systematic duplication regression. `Profile.max_duplicates_per_kill` derives the threshold.
    """
    kills = len(scorecard["kills"])
    limit = scorecard["config"].get("max_duplicates_per_kill")
    # The soak profile kills nothing, and a scorecard predating the ceiling has no limit to apply.
    if not kills or limit is None:
        return None
    tasks = scorecard["tasks"]
    attributed = tasks["duplicates_hard_kill"] + tasks["duplicates_soft_kill"]
    rate = attributed / kills
    if rate <= limit:
        return None
    return (
        f"{attributed} kill-attributed duplicates over {kills} kills is {rate:.2f} per kill, "
        f"above the {limit:.2f} ceiling"
    )


def evaluate_verdict(scorecard: dict) -> tuple[bool, list[str]]:
    config = scorecard["config"]
    failures = _task_verdict_failures(scorecard)
    rate_failure = _duplicate_rate_failure(scorecard)
    if rate_failure:
        failures.append(rate_failure)
    if not scorecard["kills"] and (config.get("kill_interval") or config.get("kill_schedule")):
        failures.append("chaos injected no kills")
    if scorecard["chaos_errors"]:
        failures.append(f"{scorecard['chaos_errors']} chaos iterations failed")
    if scorecard["restart_failures"]:
        failures.append(f"workers never came back after a kill: {scorecard['restart_failures']}")
    if scorecard.get("chaos_join_timed_out"):
        # An in-flight kill never reached `kills`, so its duplicates read as unattributed and the
        # per-kill ceiling divides by a denominator that is short.
        failures.append("chaos thread outlived its join timeout; the kill timeline may be incomplete")
    latency = scorecard["latency"]
    if latency["early_deliveries"]:
        failures.append(
            f"{latency['early_deliveries']} tasks executed before their eligibility time "
            f"(first: {latency['early_deliveries_sample'][:5]})",
        )
    if not scorecard["drain_ok"]:
        failures.append("drain did not complete before timeout")
    if scorecard["unexpected_deaths"]:
        failures.append(f"unexpected worker deaths: {scorecard['unexpected_deaths']}")
    broker_clean = scorecard.get("broker_clean")
    if broker_clean and not broker_is_empty(broker_clean):
        failures.append(f"broker not clean / orphaned keys: {broker_clean}")
    return (not failures, failures)


def print_scorecard(scorecard: dict) -> None:
    tasks = scorecard["tasks"]
    print("=" * 64)
    print("  BATTLE SCORECARD")
    print("=" * 64)
    print(f"  config: {scorecard['config']}")
    kills = scorecard["kills"]
    sigkills = sum(1 for kill in kills if kill.get("sigkilled"))
    print(f"  kills: {len(kills)} total ({sigkills} with SIGKILL)")
    print(
        f"  thread health: producer_errors={scorecard['producer_errors']} "
        f"chaos_errors={scorecard['chaos_errors']} "
        f"restart_failures={scorecard['restart_failures']}",
    )
    if scorecard.get("chaos_join_timed_out"):
        print("  CHAOS THREAD DID NOT STOP: a kill in flight during the drain is missing from the timeline")
    print(f"  tasks: {tasks['submitted']} submitted, {tasks['exactly_once']} exactly-once")
    print(
        f"         lost={tasks['lost']} pending={tasks['pending']} "
        f"failed(worker-side)={tasks['failed']} "
        f"dupes(hard-kill)={tasks['duplicates_hard_kill']} "
        f"dupes(soft-kill)={tasks['duplicates_soft_kill']} "
        f"dupes(UNATTRIBUTED)={tasks['duplicates_unattributed']} unexpected={tasks['unexpected']}",
    )
    latency = scorecard["latency"]
    overall = latency["overall"]
    print(
        f"  latency: min={overall['min']:.2f}s p50={overall['p50']:.2f}s p95={overall['p95']:.2f}s "
        f"p99={overall['p99']:.2f}s max={overall['max']:.2f}s first_exec_past_vt={latency['first_exec_past_vt']}",
    )
    if latency["early_deliveries"]:
        print(f"  DELIVERED EARLY: {latency['early_deliveries']} (first: {latency['early_deliveries_sample'][:5]})")
    for event_type, info in scorecard["events"].items():
        print(
            f"  events[{event_type}]: {info['seen']}/{info['expected']} "
            f"(loss {info['loss_pct']:.2f}%), delay p95={info['delay_p95']:.2f}s",
        )
    if scorecard.get("broker_clean") is not None:
        print(f"  broker clean: {scorecard['broker_clean']}")
    soak = scorecard.get("soak")
    if soak and soak.get("error"):
        print(f"  soak: unavailable ({soak['error']})")
    elif soak and not soak.get("samples"):
        # summarize_samples returns a short dict with none of the memory/throughput keys when no
        # row parsed: a run under one sample interval, or one where every sample raised.
        print(
            f"  soak: no samples collected "
            f"({soak.get('skipped', 0)} unparseable, {soak.get('errors', 0)} sampling errors)",
        )
    elif soak:
        print(
            f"  soak: {soak['samples']} samples "
            f"({soak.get('skipped', 0)} unparseable, {soak.get('errors', 0)} sampling errors)",
        )
        print(f"    redis mem: {soak['redis_mem_start'] / _MB:.1f}MB -> {soak['redis_mem_end'] / _MB:.1f}MB")
        for name, start in sorted(soak["mem_start"].items()):
            end = soak["mem_end"].get(name, 0)
            peak = soak["mem_max"].get(name, 0)
            print(f"    {name}: {start / _MB:.1f} -> {end / _MB:.1f}MB (max {peak / _MB:.1f}MB)")
        rate = soak["throughput_per_interval"]
        print(f"    throughput/interval: min={rate['min']} mean={rate['mean']:.1f} max={rate['max']}")
    verdict = scorecard["verdict"]
    if verdict["mode"] == "report-only":
        reason = (
            "stock transport baseline"
            if scorecard["config"].get("transport") != "plus"
            else "early ack: tasks killed mid-execution are lost by design"
        )
        print(f"  verdict: REPORT-ONLY ({reason})")
    elif verdict["passed"]:
        print("  verdict: PASS")
    else:
        print("  verdict: FAIL")
        for failure in verdict["failures"]:
            print(f"    - {failure}")
    print("=" * 64)
