from __future__ import annotations

import itertools
import json
import re
import threading
import time
from typing import TYPE_CHECKING, cast

from battle.orchestrator.compose import docker

if TYPE_CHECKING:
    from pathlib import Path
    from typing import TextIO

    import redis

    from battle.orchestrator.profiles import RunConfig

_MEM_RE = re.compile(r"^([\d.]+)\s*(B|KiB|MiB|GiB)$")
_MEM_FACTORS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3}


def parse_mem(text: str) -> int:
    """Parses a `docker stats` memory value (e.g. "12.3MiB") into bytes."""
    match = _MEM_RE.match(text.strip())
    if match is None:
        raise ValueError(f"unparseable memory value: {text!r}")
    return int(float(match.group(1)) * _MEM_FACTORS[match.group(2)])


def _container_memory() -> dict[str, int]:
    """Current RSS-ish memory usage of every `battle-worker-*` container, keyed by name.

    Raises RuntimeError on a failed `docker stats` call or an empty result, so the caller counts
    it as a sampling error instead of silently recording an empty memory timeline.
    """
    result = docker(
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}\t{{.MemUsage}}",
        check=False,
        capture=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker stats failed (rc={result.returncode}): {result.stderr.strip()}")
    memory: dict[str, int] = {}
    for line in result.stdout.splitlines():
        name, _, usage = line.partition("\t")
        if not name.startswith("battle-worker-"):
            continue
        used = usage.split("/")[0].strip()
        try:
            memory[name] = parse_mem(used)
        except ValueError:
            continue
    if not memory:
        raise RuntimeError("docker stats returned no battle-worker-* rows")
    return memory


class Sampler(threading.Thread):
    """Periodic soak sampler: worker RSS, broker memory, and key depths -> JSONL timeline."""

    def __init__(
        self,
        config: RunConfig,
        broker_client: redis.Redis,
        ledger_client: redis.Redis,
        out_path: Path,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="battle-sampler", daemon=True)
        self.config = config
        self.broker = broker_client
        self.ledger = ledger_client
        self.out_path = out_path
        self.stop_event = stop_event
        self.errors = 0

    def run(self) -> None:
        interval = self.config.profile.sample_interval
        if not interval:  # _start_sampler never builds us with sampling disabled
            return
        try:
            out = self.out_path.open("a")
        except OSError as exc:
            print(f"sampler: could not open {self.out_path} (disabled for this run): {exc}", flush=True)
            self.errors += 1
            return
        try:
            while not self.stop_event.wait(interval):
                self._sample_and_write(out)
        finally:
            out.close()

    def _sample_and_write(self, out: TextIO) -> None:
        """One sample/write cycle. Any error here is counted, not fatal to the thread."""
        try:
            row = self._sample()
            out.write(json.dumps(row) + "\n")
            out.flush()
        except Exception as exc:
            print(f"sampler: sample failed (continuing): {exc}", flush=True)
            self.errors += 1

    def _sample(self) -> dict:
        queue_depth = sum(
            cast("int", self.broker.zcard(key)) for key in self.broker.scan_iter(match="queue:*", count=1000)
        )
        index_depth = sum(
            cast("int", self.broker.zcard(key)) for key in self.broker.scan_iter(match="messages_index:*", count=1000)
        )
        message_keys = sum(1 for _ in self.broker.scan_iter(match="message:*", count=1000))
        memory_info = cast("dict[str, int]", self.broker.info("memory"))
        # A docker outage must not discard the Redis-side depths, which are the primary leak
        # signal; it also skews throughput deltas by merging two intervals into one.
        try:
            memory = _container_memory()
        except Exception as exc:
            print(f"sampler: container memory unavailable (continuing): {exc}", flush=True)
            self.errors += 1
            memory = {}
        return {
            "t": time.time(),
            "mem": memory,
            "redis_mem": memory_info["used_memory"],
            "dbsize": self.broker.dbsize(),
            "queue_depth": queue_depth,
            "index_depth": index_depth,
            "message_keys": message_keys,
            "executed": self.ledger.scard("executed_ids"),
        }


def summarize_samples(path: Path) -> dict:
    """Rolls a Sampler JSONL timeline into start/end/max memory and throughput stats.

    Lines that fail to parse (e.g. a trailing line truncated by a `sampler.join()` timeout
    racing an in-flight write) are skipped and counted rather than aborting the whole summary.
    """
    rows = []
    skipped = 0
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            skipped += 1
    if not rows:
        return {"samples": 0, "skipped": skipped}
    containers = sorted({name for row in rows for name in row.get("mem", {})})
    deltas = [b["executed"] - a["executed"] for a, b in itertools.pairwise(rows)]
    return {
        "samples": len(rows),
        "skipped": skipped,
        "mem_start": {c: next(r["mem"][c] for r in rows if c in r["mem"]) for c in containers},
        "mem_end": {c: next(r["mem"][c] for r in reversed(rows) if c in r["mem"]) for c in containers},
        "mem_max": {c: max(r["mem"][c] for r in rows if c in r["mem"]) for c in containers},
        "redis_mem_start": rows[0]["redis_mem"],
        "redis_mem_end": rows[-1]["redis_mem"],
        "throughput_per_interval": {
            "min": min(deltas) if deltas else 0,
            "mean": (sum(deltas) / len(deltas)) if deltas else 0.0,
            "max": max(deltas) if deltas else 0,
        },
    }
