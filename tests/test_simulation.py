"""Production simulation test for celery-redis-plus.

Submits batches of tasks across multiple concurrent worker processes with
periodic restarts (graceful and hard crash) and verifies every task executes
exactly once via an external Redis ledger.

Each worker runs in its own subprocess with an isolated Celery app and
transport, matching real production deployments.

Run with:
    uv run pytest -m manual tests/test_simulation.py -v -s -k redis

Configure via environment variables:
    SIM_NUM_WORKERS=4           Number of concurrent workers (default 4)
    SIM_TASKS_PER_BATCH=10000   Tasks per batch (default 10000)
    SIM_NUM_BATCHES=1           Number of batches (default 1)
    SIM_RESTART_INTERVAL=10     Seconds between worker restarts (default 10)
    SIM_HARD_CRASH_RATIO=0.3    Fraction of restarts that simulate hard crash
    SIM_VISIBILITY_TIMEOUT=30   Visibility timeout in seconds (production: 300)
    SIM_REQUEUE_CHECK_INTERVAL=6 Requeue check interval in seconds (production: 60)

Quick smoke test:
    SIM_TASKS_PER_BATCH=100 uv run pytest -m manual tests/test_simulation.py -v -s -k redis
"""

from __future__ import annotations

import multiprocessing
import os
import random
import threading
import time
import uuid
from typing import Any

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker

import celery_redis_plus.constants
import celery_redis_plus.transport
from celery_redis_plus.transport import Channel, client_lib

# --- Configuration via environment variables ---
# Defaults are ~10x compressed from production (VT=300, RCI=60)
SIM_NUM_WORKERS = int(os.environ.get("SIM_NUM_WORKERS", "4"))
SIM_TASKS_PER_BATCH = int(os.environ.get("SIM_TASKS_PER_BATCH", "10000"))
SIM_NUM_BATCHES = int(os.environ.get("SIM_NUM_BATCHES", "1"))
SIM_RESTART_INTERVAL = int(os.environ.get("SIM_RESTART_INTERVAL", "10"))
SIM_HARD_CRASH_RATIO = float(os.environ.get("SIM_HARD_CRASH_RATIO", "0.3"))
SIM_VISIBILITY_TIMEOUT = int(os.environ.get("SIM_VISIBILITY_TIMEOUT", "30"))
SIM_REQUEUE_CHECK_INTERVAL = int(os.environ.get("SIM_REQUEUE_CHECK_INTERVAL", "6"))

LEDGER_DB = 2


def _make_sim_app(host: str, port: int) -> Celery:
    """Create a Celery app configured for simulation testing."""
    app = Celery("simulation")
    app.conf.update(
        broker_url=f"redis://{host}:{port}/0",
        broker_transport="celery_redis_plus.transport:Transport",
        result_backend=f"redis://{host}:{port}/1",
        task_serializer="json",
        accept_content=["json"],
        worker_prefetch_multiplier=4,
        result_backend_always_retry=False,
        broker_connection_max_retries=0,
        broker_transport_options={
            "visibility_timeout": SIM_VISIBILITY_TIMEOUT,
        },
    )
    return app


def _record_execution(host: str, port: int, task_uuid: str) -> None:
    """Record task execution in external ledger (Redis DB 2)."""
    r = client_lib.Redis(host=host, port=port, db=LEDGER_DB)
    try:
        r.hincrby(f"ledger:{task_uuid}", "count", 1)
    finally:
        r.close()


def _register_tasks(app: Celery, host: str, port: int) -> dict[str, Any]:
    """Register simulation tasks on the app."""

    @app.task(name="sim.normal")
    def normal_task(task_uuid: str) -> str:
        time.sleep(random.uniform(0.01, 0.2))
        _record_execution(host, port, task_uuid)
        return task_uuid

    @app.task(name="sim.delayed")
    def delayed_task(task_uuid: str) -> str:
        time.sleep(random.uniform(0.01, 0.2))
        _record_execution(host, port, task_uuid)
        return task_uuid

    @app.task(name="sim.slow")
    def slow_task(task_uuid: str) -> str:
        time.sleep(random.uniform(1.0, 5.0))
        _record_execution(host, port, task_uuid)
        return task_uuid

    return {
        "normal": normal_task,
        "delayed": delayed_task,
        "slow": slow_task,
    }


# ---------------------------------------------------------------------------
# Worker subprocess
# ---------------------------------------------------------------------------


def _worker_process_main(
    host: str,
    port: int,
    worker_num: int,
    started_event: multiprocessing.synchronize.Event,
    stop_event: multiprocessing.synchronize.Event,
    hard_crash_flag: multiprocessing.synchronize.Event,
) -> None:
    """Entry point for a worker subprocess.

    Creates its own Celery app + transport (fully isolated from other workers).
    Blocks on *stop_event* until told to shut down.  If *hard_crash_flag* is
    set, ``Channel.do_restore`` is disabled so unacked messages remain in
    ``messages_index`` for visibility-timeout recovery.
    """
    # Patch RCI for this worker process (overrides conftest's patch of 2s)
    celery_redis_plus.constants.DEFAULT_REQUEUE_CHECK_INTERVAL = SIM_REQUEUE_CHECK_INTERVAL  # type: ignore[misc]
    celery_redis_plus.transport.DEFAULT_REQUEUE_CHECK_INTERVAL = SIM_REQUEUE_CHECK_INTERVAL  # type: ignore[misc]

    app = _make_sim_app(host, port)
    _register_tasks(app, host, port)

    with start_worker(
        app,
        pool="threads",
        concurrency=4,
        shutdown_timeout=10.0,
        perform_ping_check=False,
        hostname=f"sim-{worker_num}@localhost",
    ):
        started_event.set()
        stop_event.wait()
        # Set do_restore=False BEFORE leaving the context manager so
        # QoS.restore_unacked_once() sees it during shutdown.
        if hard_crash_flag.is_set():
            Channel.do_restore = False

    app.close()


# ---------------------------------------------------------------------------
# WorkerPool — manages concurrent worker subprocesses
# ---------------------------------------------------------------------------


class WorkerPool:
    """Manages multiple concurrent Celery worker subprocesses.

    Each worker runs in its own process with an isolated Celery app,
    transport, and Redis connections — matching real deployments.
    Workers can be individually restarted (graceful or hard crash)
    without affecting other running workers.
    """

    def __init__(self, host: str, port: int, num_workers: int) -> None:
        self.host = host
        self.port = port
        self.num_workers = num_workers
        self._slots: list[
            tuple[
                multiprocessing.Process,
                multiprocessing.synchronize.Event,
                multiprocessing.synchronize.Event,
                int,
            ]
        ] = []
        self._counter = 0
        self._lock = threading.Lock()
        self.events: list[dict[str, Any]] = []

    def start_all(self) -> None:
        """Start all workers."""
        for _ in range(self.num_workers):
            self._start_new()

    def _start_new(self) -> int:
        """Start a new worker subprocess and return its number."""
        self._counter += 1
        num = self._counter
        started = multiprocessing.Event()
        stop = multiprocessing.Event()
        hard_crash = multiprocessing.Event()
        p = multiprocessing.Process(
            target=_worker_process_main,
            args=(self.host, self.port, num, started, stop, hard_crash),
            daemon=True,
        )
        p.start()
        if not started.wait(timeout=60):
            raise RuntimeError(f"Worker {num} failed to start within 60s")
        with self._lock:
            self._slots.append((p, stop, hard_crash, num))
        return num

    def restart_random(self, hard: bool = False) -> dict[str, Any] | None:
        """Stop a random worker and start a replacement.

        For hard crash: sets the ``hard_crash_flag`` so the subprocess
        disables ``Channel.do_restore`` before shutting down — unacked
        messages stay in ``messages_index`` for visibility-timeout recovery.
        """
        with self._lock:
            if not self._slots:
                return None
            idx = random.randrange(len(self._slots))
            proc, stop_event, hard_flag, old_num = self._slots.pop(idx)

        if hard:
            hard_flag.set()
        stop_event.set()
        proc.join(timeout=120)

        new_num = self._start_new()

        event = {
            "old_worker": old_num,
            "new_worker": new_num,
            "crash_type": "hard" if hard else "graceful",
        }
        self.events.append(event)
        print(f"  Worker {old_num} -> {new_num} ({event['crash_type']} restart)")
        return event

    def stop_all(self) -> None:
        """Gracefully stop all workers."""
        with self._lock:
            slots = list(self._slots)
            self._slots.clear()
        for _, stop_event, _, _ in slots:
            stop_event.set()
        for proc, _, _, _ in slots:
            proc.join(timeout=60)


# ---------------------------------------------------------------------------
# Batch submission & progress tracking
# ---------------------------------------------------------------------------


def _submit_batch(
    tasks: dict[str, Any],
    count: int,
) -> list[tuple[str, str]]:
    """Submit a batch of tasks. Returns list of (uuid, type) tuples.

    Distribution: 70% normal, 25% delayed (countdown 7-15s), 5% slow.
    """
    submitted: list[tuple[str, str]] = []
    for _ in range(count):
        task_uuid = str(uuid.uuid4())
        roll = random.random()
        if roll < 0.70:
            tasks["normal"].delay(task_uuid)
            submitted.append((task_uuid, "normal"))
        elif roll < 0.95:
            countdown = random.uniform(7.0, 15.0)
            tasks["delayed"].apply_async(args=(task_uuid,), countdown=countdown)
            submitted.append((task_uuid, "delayed"))
        else:
            tasks["slow"].delay(task_uuid)
            submitted.append((task_uuid, "slow"))
    return submitted


def _count_completed(ledger: Any, uuids: set[str]) -> int:
    """Count how many task UUIDs have been recorded in the ledger."""
    pipe = ledger.pipeline()
    for u in uuids:
        pipe.hget(f"ledger:{u}", "count")
    results = pipe.execute()
    return sum(1 for r in results if r is not None)


def _wait_for_batch(
    ledger: Any,
    uuids: set[str],
    pool: WorkerPool,
    timeout: float,
    restart_interval: float,
    hard_crash_ratio: float,
) -> int:
    """Wait for batch completion, restarting workers periodically.

    Returns the number of completed tasks.
    """
    start = time.monotonic()
    last_restart = start
    total = len(uuids)

    while time.monotonic() - start < timeout:
        completed = _count_completed(ledger, uuids)
        elapsed = time.monotonic() - start
        print(f"  Progress: {completed}/{total} ({elapsed:.0f}s)")

        if completed >= total:
            return completed

        # Periodically restart a random worker
        if time.monotonic() - last_restart >= restart_interval:
            hard = random.random() < hard_crash_ratio
            pool.restart_random(hard=hard)
            last_restart = time.monotonic()

        time.sleep(5)

    return _count_completed(ledger, uuids)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _verify_exactly_once(
    ledger: Any,
    all_submitted: list[tuple[str, str]],
    events: list[dict[str, Any]],
) -> None:
    """Verify every submitted task was executed exactly once and print report."""
    submitted_uuids = {u for u, _ in all_submitted}
    total_submitted = len(submitted_uuids)

    executed_exactly_once = 0
    missing: list[str] = []
    duplicated: list[tuple[str, int]] = []

    for task_uuid in submitted_uuids:
        count_raw = ledger.hget(f"ledger:{task_uuid}", "count")
        if count_raw is None:
            missing.append(task_uuid)
        else:
            count = int(count_raw)
            if count == 1:
                executed_exactly_once += 1
            elif count > 1:
                duplicated.append((task_uuid, count))

    # Check for unexpected UUIDs in ledger
    all_ledger_keys = [k.decode() for k in ledger.keys("ledger:*")]
    ledger_uuids = {k.removeprefix("ledger:") for k in all_ledger_keys}
    unexpected = list(ledger_uuids - submitted_uuids)

    # Type breakdown
    type_counts: dict[str, int] = {}
    for _, task_type in all_submitted:
        type_counts[task_type] = type_counts.get(task_type, 0) + 1

    hard_crashes = sum(1 for e in events if e["crash_type"] == "hard")
    graceful = sum(1 for e in events if e["crash_type"] == "graceful")

    print(f"\n{'=' * 60}")
    print("  SIMULATION RESULTS")
    print(f"{'=' * 60}")
    print(f"  Workers: {SIM_NUM_WORKERS} concurrent")
    print(f"  Batches: {SIM_NUM_BATCHES} x {SIM_TASKS_PER_BATCH} tasks")
    print(f"  Restarts: {len(events)} ({graceful} graceful, {hard_crashes} hard)")
    print(f"  Tasks submitted: {total_submitted}")
    for ttype, tcount in sorted(type_counts.items()):
        print(f"    {ttype}: {tcount}")
    print(f"  Executed exactly once: {executed_exactly_once}")
    print(f"  Missing (lost): {len(missing)}")
    print(f"  Duplicated: {len(duplicated)}")
    print(f"  Unexpected: {len(unexpected)}")
    if duplicated:
        print(f"  Duplicated details (first 10): {duplicated[:10]}")
    if missing:
        print(f"  Missing details (first 10): {missing[:10]}")
    print(f"{'=' * 60}")

    assert len(unexpected) == 0, f"Unexpected task UUIDs in ledger: {unexpected[:10]}"
    assert len(duplicated) == 0, f"{len(duplicated)} tasks executed more than once: {duplicated[:10]}"
    assert len(missing) == 0, f"{len(missing)} tasks lost (never executed): {missing[:10]}"
    assert executed_exactly_once == total_submitted


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


@pytest.mark.manual
class TestProductionSimulation:
    """Production workload simulation with multiple concurrent workers.

    Submits batches of tasks across multiple worker subprocesses with
    periodic restarts (graceful and hard crash), then verifies exactly-once
    task execution via an external Redis ledger.
    """

    def test_exactly_once_with_worker_restarts(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
    ) -> None:
        host, port, _image = redis_container
        redis_client.flushall()

        # Patch RCI in main process (overrides conftest's patch of 2s)
        celery_redis_plus.constants.DEFAULT_REQUEUE_CHECK_INTERVAL = SIM_REQUEUE_CHECK_INTERVAL  # type: ignore[misc]
        celery_redis_plus.transport.DEFAULT_REQUEUE_CHECK_INTERVAL = SIM_REQUEUE_CHECK_INTERVAL  # type: ignore[misc]

        # Main-process app used only for submitting tasks
        app = _make_sim_app(host, port)
        tasks = _register_tasks(app, host, port)

        ledger = client_lib.Redis(host=host, port=port, db=LEDGER_DB)
        ledger.flushdb()

        # Estimate batch timeout:
        # weighted avg task time * count / workers * 2x safety + VT + delayed countdown
        avg_task_time = 0.70 * 0.1 + 0.25 * 0.1 + 0.05 * 3  # ~0.25s
        batch_timeout = avg_task_time * SIM_TASKS_PER_BATCH / SIM_NUM_WORKERS * 2 + SIM_VISIBILITY_TIMEOUT * 3 + 15

        print(f"\n{'=' * 60}")
        print("  PRODUCTION SIMULATION")
        print(f"  Workers: {SIM_NUM_WORKERS}")
        print(f"  Batches: {SIM_NUM_BATCHES} x {SIM_TASKS_PER_BATCH} tasks")
        print(f"  Restart interval: ~{SIM_RESTART_INTERVAL}s")
        print(f"  Hard crash ratio: {SIM_HARD_CRASH_RATIO}")
        print(f"  Visibility timeout: {SIM_VISIBILITY_TIMEOUT}s")
        print(f"  Requeue check interval: {SIM_REQUEUE_CHECK_INTERVAL}s")
        print(f"  Batch timeout: {batch_timeout:.0f}s")
        print(f"{'=' * 60}")

        pool = WorkerPool(host, port, SIM_NUM_WORKERS)
        pool.start_all()

        all_submitted: list[tuple[str, str]] = []

        try:
            for batch_num in range(SIM_NUM_BATCHES):
                print(f"\n  --- Batch {batch_num + 1}/{SIM_NUM_BATCHES} ---")
                print(f"  Submitting {SIM_TASKS_PER_BATCH} tasks...")

                batch = _submit_batch(tasks, SIM_TASKS_PER_BATCH)
                all_submitted.extend(batch)
                batch_uuids = {u for u, _ in batch}

                print(
                    f"  Submitted. Waiting for completion (timeout: {batch_timeout:.0f}s)...",
                )

                completed = _wait_for_batch(
                    ledger,
                    batch_uuids,
                    pool,
                    timeout=batch_timeout,
                    restart_interval=SIM_RESTART_INTERVAL,
                    hard_crash_ratio=SIM_HARD_CRASH_RATIO,
                )
                print(
                    f"  Batch {batch_num + 1} done: {completed}/{len(batch_uuids)}",
                )
        finally:
            pool.stop_all()

        _verify_exactly_once(ledger, all_submitted, pool.events)

        ledger.flushdb()
        ledger.close()
        app.close()
