"""Production simulation test for celery-redis-plus.

Runs a sustained workload with periodic worker restarts (graceful and hard crash)
and verifies every task executes exactly once via an external Redis ledger.

Run with:
    uv run pytest -m manual tests/test_simulation.py -v -s -k redis

Configure via environment variables:
    SIM_DURATION_SECONDS=1800   Total simulation time (default 30 min)
    SIM_TASKS_PER_SECOND=2      Submission rate
    SIM_RESTART_INTERVAL=45     Avg seconds between worker restarts
    SIM_HARD_CRASH_RATIO=0.3    Fraction of restarts that simulate hard crash
    SIM_VISIBILITY_TIMEOUT=10   Visibility timeout in seconds
"""

from __future__ import annotations

import os
import random
import threading
import time
import uuid
from typing import Any

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop

from celery_redis_plus.transport import Channel, client_lib

# --- Configuration via environment variables ---
SIM_DURATION_SECONDS = int(os.environ.get("SIM_DURATION_SECONDS", "1800"))
SIM_TASKS_PER_SECOND = int(os.environ.get("SIM_TASKS_PER_SECOND", "2"))
SIM_RESTART_INTERVAL = int(os.environ.get("SIM_RESTART_INTERVAL", "45"))
SIM_HARD_CRASH_RATIO = float(os.environ.get("SIM_HARD_CRASH_RATIO", "0.3"))
SIM_VISIBILITY_TIMEOUT = int(os.environ.get("SIM_VISIBILITY_TIMEOUT", "10"))

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
        time.sleep(random.uniform(0.5, 2.0))
        _record_execution(host, port, task_uuid)
        return task_uuid

    @app.task(name="sim.delayed")
    def delayed_task(task_uuid: str) -> str:
        time.sleep(random.uniform(0.5, 2.0))
        _record_execution(host, port, task_uuid)
        return task_uuid

    @app.task(name="sim.slow")
    def slow_task(task_uuid: str) -> str:
        time.sleep(random.uniform(10.0, 30.0))
        _record_execution(host, port, task_uuid)
        return task_uuid

    return {
        "normal": normal_task,
        "delayed": delayed_task,
        "slow": slow_task,
    }


class TaskSubmitter:
    """Background thread that continuously submits tasks."""

    def __init__(self, tasks: dict[str, Any], tasks_per_second: int) -> None:
        self.tasks = tasks
        self.tasks_per_second = tasks_per_second
        self.submitted: list[tuple[str, str]] = []  # (uuid, type)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def _run(self) -> None:
        interval = 1.0 / self.tasks_per_second
        while not self._stop_event.is_set():
            task_uuid = str(uuid.uuid4())
            roll = random.random()
            if roll < 0.5:
                task_type = "normal"
                self.tasks["normal"].delay(task_uuid)
            elif roll < 0.7:
                task_type = "delayed"
                countdown = random.uniform(3.0, 10.0)
                self.tasks["delayed"].apply_async(args=(task_uuid,), countdown=countdown)
            else:
                task_type = "slow"
                self.tasks["slow"].delay(task_uuid)

            with self._lock:
                self.submitted.append((task_uuid, task_type))

            self._stop_event.wait(interval)

    @property
    def submission_count(self) -> int:
        with self._lock:
            return len(self.submitted)


def _run_worker_lifecycle(
    app: Celery,
    duration: float,
    restart_interval: float,
    hard_crash_ratio: float,
) -> list[dict[str, Any]]:
    """Cycle through worker instances with periodic restarts.

    Returns a log of worker lifecycle events.
    """
    events: list[dict[str, Any]] = []
    sim_start = time.monotonic()
    worker_num = 0

    while time.monotonic() - sim_start < duration:
        worker_num += 1
        remaining = duration - (time.monotonic() - sim_start)
        if remaining <= 0:
            break

        run_time = min(
            restart_interval + random.uniform(-10, 10),
            remaining,
        )
        run_time = max(run_time, 10)  # At least 10 seconds

        is_hard_crash = random.random() < hard_crash_ratio

        event: dict[str, Any] = {
            "worker_num": worker_num,
            "start_time": time.monotonic() - sim_start,
            "planned_run_time": run_time,
            "crash_type": "hard" if is_hard_crash else "graceful",
        }

        app.loader.import_task_module("celery_redis_plus")

        if is_hard_crash:
            # Hard crash: disable restore before exiting context manager
            Channel.do_restore = False

        try:
            with start_worker(
                app,
                pool="solo",
                shutdown_timeout=60.0,
                perform_ping_check=False,
            ):
                time.sleep(run_time)
        finally:
            if is_hard_crash:
                Channel.do_restore = True

        event["end_time"] = time.monotonic() - sim_start
        events.append(event)

        print(
            f"  Worker {worker_num}: {event['crash_type']} restart at {event['end_time']:.0f}s",
        )

        set_event_loop(None)
        time.sleep(1)  # Brief gap between workers

    return events


def _verify_exactly_once(
    ledger: Any,
    submitter: TaskSubmitter,
    lifecycle_events: list[dict[str, Any]],
) -> None:
    """Verify every submitted task was executed exactly once and print report."""
    total_submitted = len(submitter.submitted)
    submitted_uuids = {s[0] for s in submitter.submitted}

    executed_exactly_once = 0
    missing = []
    duplicated = []

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
    for _, task_type in submitter.submitted:
        type_counts[task_type] = type_counts.get(task_type, 0) + 1

    hard_crashes = sum(1 for e in lifecycle_events if e["crash_type"] == "hard")
    graceful = sum(1 for e in lifecycle_events if e["crash_type"] == "graceful")

    print(f"\n{'=' * 60}")
    print("  SIMULATION RESULTS")
    print(f"{'=' * 60}")
    print(f"  Duration: {SIM_DURATION_SECONDS}s")
    print(f"  Workers started: {len(lifecycle_events)}")
    print(f"    Graceful restarts: {graceful}")
    print(f"    Hard crashes: {hard_crashes}")
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


@pytest.mark.manual
class TestProductionSimulation:
    """Production workload simulation with worker restarts.

    Verifies exactly-once task execution across graceful restarts
    and simulated hard crashes (visibility timeout recovery).
    """

    def test_exactly_once_with_worker_restarts(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
    ) -> None:
        host, port, _image = redis_container
        redis_client.flushall()

        app = _make_sim_app(host, port)
        tasks = _register_tasks(app, host, port)

        # Ledger on DB 2
        ledger = client_lib.Redis(host=host, port=port, db=LEDGER_DB)
        ledger.flushdb()

        print(f"\n{'=' * 60}")
        print("  PRODUCTION SIMULATION")
        print(f"  Duration: {SIM_DURATION_SECONDS}s")
        print(f"  Tasks/sec: {SIM_TASKS_PER_SECOND}")
        print(f"  Restart interval: ~{SIM_RESTART_INTERVAL}s")
        print(f"  Hard crash ratio: {SIM_HARD_CRASH_RATIO}")
        print(f"  Visibility timeout: {SIM_VISIBILITY_TIMEOUT}s")
        print(f"{'=' * 60}")

        submitter = TaskSubmitter(tasks, SIM_TASKS_PER_SECOND)
        submitter.start()

        try:
            lifecycle_events = _run_worker_lifecycle(
                app,
                duration=SIM_DURATION_SECONDS,
                restart_interval=SIM_RESTART_INTERVAL,
                hard_crash_ratio=SIM_HARD_CRASH_RATIO,
            )
        finally:
            submitter.stop()

        # --- Drain phase ---
        print(f"\n  Drain phase: {submitter.submission_count} tasks submitted, waiting for completion...")

        app.loader.import_task_module("celery_redis_plus")
        drain_timeout = SIM_VISIBILITY_TIMEOUT * 3 + 120

        with submitter._lock:
            submitted_uuids = {s[0] for s in submitter.submitted}

        with start_worker(
            app,
            pool="solo",
            shutdown_timeout=60.0,
            perform_ping_check=False,
        ):
            drain_start = time.monotonic()
            while time.monotonic() - drain_start < drain_timeout:
                recorded = 0
                for task_uuid in submitted_uuids:
                    count = ledger.hget(f"ledger:{task_uuid}", "count")
                    if count and int(count) >= 1:
                        recorded += 1
                if recorded >= len(submitted_uuids):
                    print(f"  All {recorded} tasks completed after {time.monotonic() - drain_start:.0f}s drain")
                    break
                print(f"  Drain: {recorded}/{len(submitted_uuids)} tasks recorded...")
                time.sleep(5)

        set_event_loop(None)

        # --- Verification ---
        _verify_exactly_once(ledger, submitter, lifecycle_events)

        # Cleanup
        ledger.flushdb()
        ledger.close()
        app.close()
