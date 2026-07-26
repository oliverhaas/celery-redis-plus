from __future__ import annotations

import argparse
import dataclasses
import json
import os
import random
import threading
import time
from pathlib import Path
from typing import cast

import redis

from . import compose
from .chaos import ChaosMonkey
from .producer import Producer
from .profiles import PROFILES, RunConfig, make_config, parse_duration
from .sampler import Sampler, summarize_samples
from .verify import (
    EVENT_TYPES,
    RunSignals,
    broker_is_empty,
    build_scorecard,
    check_broker_clean,
    print_scorecard,
    read_ledger,
)

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
# Covers a worst-case warm iteration: 120s `docker wait`, the kill downtime, and the restart poll.
CHAOS_JOIN_TIMEOUT = 180.0
# Scratch key for the drain gate's set difference; matches none of read_ledger's scan patterns.
_DRAIN_OUTSTANDING_KEY = "drain:outstanding"
# kombu gates restore_visible to every 10th call of a timer that fires every 10s.
KOMBU_RESTORE_SWEEP_GAP = 100.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="battle", description="celery-redis-plus battle-testing harness")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="full lifecycle: up, load+chaos, drain, verify, report, down")
    run.add_argument("--profile", choices=["smoke", "chaos", "soak"], default="smoke")
    run.add_argument("--transport", choices=["plus", "stock"], default="plus")
    run.add_argument("--broker", choices=["redis", "valkey"], default="redis")
    run.add_argument("--workers", type=int, default=None)
    run.add_argument("--concurrency", type=int, default=None, help="prefork children per worker container")
    run.add_argument("--pool", choices=["prefork", "threads"], default=None)
    run.add_argument("--rate", type=float, default=None)
    run.add_argument("--duration", type=str, default=None, help="e.g. 90s, 15m, 2h")
    run.add_argument("--seed", type=int, default=42)
    run.add_argument(
        "--visibility-timeout",
        type=int,
        default=None,
        help="seconds before an unacked message is restored; raise it above the delayed countdown ceiling "
        "to separate genuine redelivery from ETA tasks parked unacked",
    )
    acks = run.add_mutually_exclusive_group()
    acks.add_argument(
        "--acks-late",
        dest="acks_late",
        action="store_true",
        default=None,
        help="ack after task completion (default; enables visibility-timeout recovery)",
    )
    acks.add_argument(
        "--no-acks-late",
        dest="acks_late",
        action="store_false",
        default=None,
        help="ack on delivery (Celery default); tasks killed mid-execution are lost by design",
    )
    run.add_argument(
        "--event-patch",
        dest="event_patch",
        action="store_true",
        default=None,
        help="patch Celery's EventDispatcher so pool threads cannot lose buffered events",
    )
    run.add_argument(
        "--no-delayed",
        dest="no_delayed",
        action="store_true",
        help="drop countdown tasks from the mix; isolates task loss from ETA/visibility-timeout effects",
    )
    run.add_argument(
        "--drain-timeout",
        type=float,
        default=None,
        help="seconds to wait for outstanding tasks after load stops (default: derived from the profile)",
    )
    run.add_argument("--keep-up", action="store_true", help="leave containers running after the run")
    run.add_argument("--dry-run", action="store_true", help="print resolved config and exit")

    verify = sub.add_parser(
        "verify",
        help="recompute scorecard from the live ledger (no drain; work the broker still holds counts as pending)",
    )
    verify.add_argument("--transport", choices=["plus", "stock"], default=None, help="override stored run_config")

    compare = sub.add_parser("compare", help="side-by-side table of two scorecard JSONs")
    compare.add_argument("scorecard_a")
    compare.add_argument("scorecard_b")

    sub.add_parser("down", help="tear down the compose stack")
    return parser


def resolve_config(args: argparse.Namespace) -> RunConfig:
    overrides: dict[str, object] = {}
    if args.workers is not None:
        overrides["workers"] = args.workers
    if args.concurrency is not None:
        overrides["concurrency"] = args.concurrency
    if args.pool is not None:
        overrides["pool"] = args.pool
    if args.rate is not None:
        overrides["rate"] = args.rate
    if args.duration is not None:
        overrides["duration"] = parse_duration(args.duration)
    if args.acks_late is not None:
        overrides["acks_late"] = args.acks_late
    if args.event_patch is not None:
        overrides["event_patch"] = args.event_patch
    if args.visibility_timeout is not None:
        overrides["visibility_timeout"] = args.visibility_timeout
    if args.no_delayed:
        # pick_type normalises by the weight sum, so dropping the key reweights the rest.
        overrides["mix"] = {k: v for k, v in PROFILES[args.profile].mix.items() if k != "delayed"}
    cfg = make_config(args.profile, transport=args.transport, broker=args.broker, seed=args.seed, **overrides)
    if args.keep_up:
        cfg = dataclasses.replace(cfg, keep_up=True)
    if args.drain_timeout is not None:
        cfg = dataclasses.replace(cfg, drain_timeout=args.drain_timeout)
    return cfg


def _config_info(config: RunConfig) -> dict:
    return {
        "profile": config.profile.name,
        "transport": config.transport,
        "broker": config.broker,
        "seed": config.seed,
        "workers": config.profile.workers,
        "concurrency": config.profile.concurrency,
        "prefetch": config.profile.prefetch,
        "pool": config.profile.pool,
        "event_patch": config.profile.event_patch,
        "rate": config.profile.rate,
        "duration": config.profile.duration,
        "visibility_timeout": config.profile.visibility_timeout,
        "requeue_interval": config.profile.requeue_interval,
        "acks_late": config.profile.acks_late,
        "mix": config.profile.mix,
        "drain_timeout": drain_timeout(config),
        # Lets evaluate_verdict tell "chaos injected no kills" from "none were asked for".
        "kill_interval": config.profile.kill_interval,
        "kill_schedule": config.profile.kill_schedule,
        "max_duplicates_per_kill": config.profile.max_duplicates_per_kill,
    }


def _wait_for_workers(config: RunConfig, timeout: float = 120.0) -> None:
    from battle.battle_app.app import create_app

    app = create_app("producer")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pongs = app.control.ping(timeout=2.0) or []
        if len(pongs) >= config.profile.workers:
            print(f"workers ready: {len(pongs)}/{config.profile.workers}")
            return
        time.sleep(2)
    raise RuntimeError(f"workers did not become ready within {timeout}s")


def _outstanding_submissions(ledger_client: redis.Redis) -> int:
    """How many submitted task ids have no execution recorded yet.

    A difference, not a cardinality comparison: `executed_ids` is not a subset of
    `submitted_ids` (the producer ledgers only after a successful send), so a ghost id there
    would otherwise mask a task still queued. SDIFFSTORE returns the count without pulling ids.
    """
    return cast("int", ledger_client.sdiffstore(_DRAIN_OUTSTANDING_KEY, ["submitted_ids", "executed_ids"]))


def drain_timeout(config: RunConfig) -> float:
    """How long to wait for outstanding tasks before calling them lost."""
    # Covers restore latency, not just the visibility timeout: billing a slow sweep as a lost task
    # would be a measurement artefact.
    profile = config.profile
    if config.drain_timeout is not None:
        return config.drain_timeout
    sweep_gap = KOMBU_RESTORE_SWEEP_GAP if config.transport == "stock" else float(profile.requeue_interval)
    return 2 * profile.visibility_timeout + 2 * sweep_gap + profile.delayed_countdown[1] + 30


def _drain(config: RunConfig, ledger_client: redis.Redis, broker_client: redis.Redis) -> bool:
    timeout = drain_timeout(config)
    print(f"draining (timeout {timeout:.0f}s)...")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        outstanding = _outstanding_submissions(ledger_client)
        # The ledger is written before the acks_late ack, so it reads "done" while the last
        # acks are still in flight and the leak check that follows would trip on them.
        if not outstanding and broker_is_empty(check_broker_clean(broker_client, config.transport)):
            print(f"drained: {ledger_client.scard('submitted_ids')} submitted, none outstanding")
            return True
        time.sleep(2)
    print(
        f"drain TIMED OUT: {_outstanding_submissions(ledger_client)} of "
        f"{ledger_client.scard('submitted_ids')} submitted tasks never executed",
    )
    return False


def _run_verification(
    config: RunConfig,
    ledger_client: redis.Redis,
    broker_client: redis.Redis,
    drain_ok: bool,
    signals: RunSignals,
) -> dict:
    data = read_ledger(ledger_client, config.profile.visibility_timeout)
    broker_clean = check_broker_clean(broker_client, config.transport)
    return build_scorecard(_config_info(config), data, drain_ok, signals, broker_clean, config.transport)


def _save_scorecard(scorecard: dict, config: RunConfig) -> Path:
    RESULTS_DIR.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    path = RESULTS_DIR / f"{stamp}-{config.transport}-{config.profile.name}.json"
    path.write_text(json.dumps(scorecard, indent=2, default=str))
    print(f"scorecard saved: {path}")
    return path


def _start_sampler(
    config: RunConfig,
    stop: threading.Event,
    *,
    broker_client: redis.Redis,
    ledger_client: redis.Redis,
) -> tuple[Sampler | None, Path | None]:
    """Starts the soak sampler when the profile enables it; otherwise a no-op.

    The sampler is optional observability: a start failure here (mkdir, thread creation) must
    never sink an otherwise-valid run, so any exception is swallowed and reported as (None, None).
    """
    if not config.profile.sample_interval:
        return None, None
    try:
        RESULTS_DIR.mkdir(exist_ok=True)
        soak_path = RESULTS_DIR / f"{time.strftime('%Y%m%d-%H%M%S')}-{config.transport}-soak.jsonl"
        sampler = Sampler(config, broker_client, ledger_client, soak_path, stop)
        sampler.start()
    except Exception as exc:
        print(f"sampler: could not start (continuing without soak sampling): {exc}", flush=True)
        return None, None
    return sampler, soak_path


def _finish_sampler(sampler: Sampler | None, soak_path: Path | None, scorecard: dict) -> None:
    """Joins the sampler and attaches its summary; a sampler failure must not sink verification."""
    if sampler is None or soak_path is None:
        return
    sampler.join(timeout=10.0)
    try:
        scorecard["soak"] = summarize_samples(soak_path)
    except Exception as exc:
        print(f"sampler: could not summarize samples (continuing): {exc}", flush=True)
        scorecard["soak"] = {"samples": 0, "skipped": 0, "error": str(exc)}
    scorecard["soak"]["errors"] = sampler.errors


def _collect_signals(signals: RunSignals, producer: Producer, chaos: ChaosMonkey) -> None:
    """Snapshots thread-owned counters into the caller's container; safe to call repeatedly."""
    signals.producer_errors = producer.errors
    signals.chaos_errors = chaos.errors
    signals.unexpected_deaths[:] = chaos.unexpected_deaths
    signals.restart_failures[:] = chaos.restart_failures


def _join_chaos(chaos: ChaosMonkey) -> None:
    """Waits out an in-flight kill so its `docker start` cannot land after the teardown."""
    if not chaos.is_alive():
        return
    print(f"waiting up to {CHAOS_JOIN_TIMEOUT:.0f}s for the chaos thread before teardown")
    chaos.join(CHAOS_JOIN_TIMEOUT)
    if chaos.is_alive():
        print("chaos thread still running; teardown may race an in-flight container restart")


def _run_lifecycle(
    config: RunConfig,
    stop: threading.Event,
    signals: RunSignals,
    *,
    ledger_client: redis.Redis,
    broker_client: redis.Redis,
) -> dict:
    """Runs producer + chaos (+ sampler) to completion; refreshes the caller-owned `signals`
    in place (before the long `_drain` wait) and returns the scorecard.
    """
    # Before anything builds a Celery app: the flush wipes `_kombu.binding.celery`, and an app
    # that already declared it will not redeclare, so kombu silently drops every publish.
    ledger_client.flushall()
    broker_client.flushall()
    ledger_client.set("run_config", json.dumps(_config_info(config)))
    _wait_for_workers(config)

    from battle.battle_app.app import create_app

    producer = Producer(config, create_app("producer"), ledger_client, random.Random(config.seed), stop)
    chaos = ChaosMonkey(config, ledger_client, random.Random(config.seed + 1), stop)
    producer.start()
    chaos.start()
    try:
        sampler, soak_path = _start_sampler(config, stop, broker_client=broker_client, ledger_client=ledger_client)
        while producer.is_alive():
            producer.join(timeout=10.0)
            _collect_signals(signals, producer, chaos)
            print(
                f"  progress: submitted={producer.submitted} "
                f"executed={ledger_client.scard('executed_ids')} kills={len(chaos.timeline)}",
            )
        stop.set()
        chaos.join(timeout=CHAOS_JOIN_TIMEOUT)
        signals.chaos_join_timed_out = chaos.is_alive()
        if signals.chaos_join_timed_out:
            print(f"chaos: thread still running after {CHAOS_JOIN_TIMEOUT:.0f}s; the kill timeline is incomplete")
        _collect_signals(signals, producer, chaos)
        drain_ok = _drain(config, ledger_client, broker_client)
        scorecard = _run_verification(config, ledger_client, broker_client, drain_ok, signals)
        _finish_sampler(sampler, soak_path, scorecard)
    except BaseException:
        # A Ctrl-C leaves the chaos thread mid-kill, and its `docker start` would race the
        # caller's `docker compose down` and strand a container outside the compose project.
        stop.set()
        _join_chaos(chaos)
        raise
    return scorecard


def cmd_run(args: argparse.Namespace) -> int:
    config = resolve_config(args)
    if args.dry_run:
        print(json.dumps(dataclasses.asdict(config), indent=2, default=str))
        return 0
    os.environ.update(config.host_env())
    # celery.utils.time.adjust_timestamp shifts each event by (its offset - the consumer's), so
    # a non-UTC host injects that difference into every task-sent delay the UTC monitor reads.
    os.environ["TZ"] = "UTC"
    time.tzset()
    print(f"battle run: {_config_info(config)}")
    stop = threading.Event()
    signals = RunSignals()
    scorecard: dict | None = None
    try:
        # Inside the try: a bring-up that fails partway still leaves containers behind, and the
        # finally below is the only thing that takes them down.
        compose.up(config)
        ledger_client = redis.Redis.from_url(config.host_ledger_url)
        broker_client = redis.Redis.from_url(config.host_broker_url)
        scorecard = _run_lifecycle(
            config,
            stop,
            signals,
            ledger_client=ledger_client,
            broker_client=broker_client,
        )
    except KeyboardInterrupt:
        print("\ninterrupted: attempting best-effort verification")
        stop.set()
        try:
            ledger_client = redis.Redis.from_url(config.host_ledger_url)
            broker_client = redis.Redis.from_url(config.host_broker_url)
            scorecard = _run_verification(
                config,
                ledger_client,
                broker_client,
                drain_ok=False,
                signals=signals,
            )
        except Exception as exc:
            print(f"verification impossible: {exc!r}")
    finally:
        stop.set()
        if not config.keep_up:
            compose.down(config)
    if scorecard is None:
        return 1
    # Save first: the `finally` above has already taken the ledger redis down with `-v`, so this
    # JSON is the only surviving record of the run and must not depend on the print succeeding.
    _save_scorecard(scorecard, config)
    print_scorecard(scorecard)
    verdict = scorecard["verdict"]
    return 0 if verdict["mode"] == "report-only" or verdict["passed"] else 1


def cmd_verify(args: argparse.Namespace) -> int:
    # Rebuild a scorecard from the live ledger of a --keep-up run.
    default_cfg = make_config()
    ledger_client = redis.Redis.from_url(default_cfg.host_ledger_url)
    stored = ledger_client.get("run_config")
    if stored is None:
        print("no run_config in ledger; is the stack up (battle run --keep-up)?")
        return 1
    info = json.loads(stored)  # ty: ignore[invalid-argument-type]
    transport = args.transport or info["transport"]
    # Anything missing here echoes a profile default, so a threads run reads back as prefork.
    restored = ("concurrency", "prefetch", "pool", "event_patch", "mix")
    sizing = {key: info[key] for key in restored if key in info}
    config = make_config(
        info["profile"],
        transport=transport,
        broker=info["broker"],
        seed=info["seed"],
        workers=info["workers"],
        rate=info["rate"],
        duration=info["duration"],
        visibility_timeout=info["visibility_timeout"],
        requeue_interval=info["requeue_interval"],
        acks_late=info.get("acks_late", True),
        **sizing,
    )
    if info.get("drain_timeout") is not None:
        # A RunConfig field rather than a Profile one, so make_config cannot carry it.
        config = dataclasses.replace(config, drain_timeout=info["drain_timeout"])
    broker_client = redis.Redis.from_url(config.host_broker_url)
    # A live stack has not drained, so the empty-broker check is what separates lost from pending.
    scorecard = _run_verification(config, ledger_client, broker_client, drain_ok=False, signals=RunSignals())
    _save_scorecard(scorecard, config)
    print_scorecard(scorecard)
    return 0


# The widest cell is the event-loss `seen/expected (loss%)`, 23 characters at the chaos profile's
# ~3M tasks; a 16-wide column overflowed and shunted every event row out of alignment.
_LABEL_WIDTH = 34
_VALUE_WIDTH = 25


def _compare_row(label: str, a: object, b: object) -> str:
    return f"  {label:<{_LABEL_WIDTH}} {a!s:>{_VALUE_WIDTH}} {b!s:>{_VALUE_WIDTH}}"


# (label, path into the scorecard dict, may-be-a-list). The flag keeps pre-cap scorecards, which
# stored id lists where these now store counts, rendering a number instead of a dash.
_COMPARE_ROWS: tuple[tuple[str, tuple[str, ...], bool], ...] = (
    # Sizing first: a pair that differs here is not a comparison, and the table should say so.
    ("workers", ("config", "workers"), False),
    ("concurrency", ("config", "concurrency"), False),
    ("pool", ("config", "pool"), False),
    ("event patch", ("config", "event_patch"), False),
    ("prefetch", ("config", "prefetch"), False),
    ("visibility timeout (s)", ("config", "visibility_timeout"), False),
    ("drain timeout (s)", ("config", "drain_timeout"), False),
    ("rate (tasks/s)", ("config", "rate"), False),
    ("submitted", ("tasks", "submitted"), False),
    ("exactly once", ("tasks", "exactly_once"), False),
    ("lost", ("tasks", "lost"), True),
    ("pending", ("tasks", "pending"), True),
    ("failed (worker-side)", ("tasks", "failed"), True),
    ("duplicates (hard-kill)", ("tasks", "duplicates_hard_kill"), True),
    ("duplicates (soft-kill)", ("tasks", "duplicates_soft_kill"), True),
    ("duplicates (unattributed)", ("tasks", "duplicates_unattributed"), True),
    # min, not just percentiles: a countdown delivered early is a negative latency, which every
    # percentile renders as an improvement.
    ("latency min (s)", ("latency", "overall", "min"), False),
    ("latency p50 (s)", ("latency", "overall", "p50"), False),
    ("latency p95 (s)", ("latency", "overall", "p95"), False),
    ("latency p99 (s)", ("latency", "overall", "p99"), False),
    ("latency max (s)", ("latency", "overall", "max"), False),
    ("first exec past VT", ("latency", "first_exec_past_vt"), False),
    ("delivered early", ("latency", "early_deliveries"), True),
    ("drain ok", ("drain_ok",), False),
    ("verdict mode", ("verdict", "mode"), False),
    ("verdict", ("verdict", "passed"), False),
)

_MISSING = object()


def _compare_value(card: dict, path: tuple[str, ...]) -> object:
    value: object = card
    for part in path:
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _format_cell(value: object, *, as_length: bool) -> str:
    if value is _MISSING:
        return "-"
    if as_length and isinstance(value, list):
        value = len(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _event_loss_cell(card: dict, event_type: str) -> str:
    """Renders `seen/expected (loss%)`, or `-` when nothing was measured (or the key is absent)."""
    info = card.get("events", {}).get(event_type)
    if not isinstance(info, dict) or not info.get("expected"):
        return "-"
    return f"{info.get('seen', 0)}/{info['expected']} ({info.get('loss_pct', 0.0):.2f}%)"


def _card_label(card: dict) -> str:
    config = card.get("config", {})
    return f"{config.get('transport', '?')}/{config.get('profile', '?')}"


def _load_scorecard(path: str) -> dict | None:
    try:
        card = json.loads(Path(path).read_text())
    except OSError as exc:
        print(f"cannot read scorecard {path}: {exc}")
        return None
    except json.JSONDecodeError as exc:
        print(f"cannot parse scorecard {path} as JSON: {exc}")
        return None
    # A soak *.jsonl sample line is valid JSON, so shape is what separates a scorecard from
    # any other file in results/. Without this the table renders an all-dashes column and exits 0.
    if not isinstance(card, dict) or not {"config", "tasks", "verdict"} <= card.keys():
        print(f"{path} is not a scorecard (missing config/tasks/verdict)")
        return None
    return card


def cmd_compare(args: argparse.Namespace) -> int:
    card_a = _load_scorecard(args.scorecard_a)
    card_b = _load_scorecard(args.scorecard_b)
    if card_a is None or card_b is None:
        return 1
    print(_compare_row("metric", _card_label(card_a), _card_label(card_b)))
    print("  " + "-" * (_LABEL_WIDTH + 2 * _VALUE_WIDTH + 2))
    for label, path, as_length in _COMPARE_ROWS:
        value_a = _format_cell(_compare_value(card_a, path), as_length=as_length)
        value_b = _format_cell(_compare_value(card_b, path), as_length=as_length)
        print(_compare_row(label, value_a, value_b))
    for event_type in EVENT_TYPES:
        print(
            _compare_row(
                f"event loss {event_type} (%)",
                _event_loss_cell(card_a, event_type),
                _event_loss_cell(card_b, event_type),
            ),
        )
    return 0


def cmd_down(args: argparse.Namespace) -> int:
    compose.down(make_config())
    print("battle stack removed")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {"run": cmd_run, "verify": cmd_verify, "compare": cmd_compare, "down": cmd_down}
    return handlers[args.command](args)
