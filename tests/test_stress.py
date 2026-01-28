"""Stress tests for celery-redis-plus.

These tests are excluded from normal test runs and must be run manually:
    uv run pytest -m manual tests/test_stress.py -v
"""

from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from celery import Celery


@pytest.mark.manual
class TestStressExecution:
    """Stress tests that execute thousands of tasks through a real Celery worker.

    These tests verify:
    - High-volume task execution (~10k tasks)
    - Mixed workloads (CPU-bound, IO-bound)
    - Retry behavior for transient failures
    - Priority handling under load
    """

    def test_high_volume_mixed_workload(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Execute many tasks with mixed CPU/IO workloads and retries.

        Task distribution:
        - 40% CPU-bound tasks (hash calculations)
        - 40% IO-bound tasks (sleep)
        - 20% retry tasks (fail 1-2 times before succeeding)

        All tasks should complete successfully.
        """

        @celery_app.task
        def cpu_task(task_id: int, iterations: int) -> dict[str, Any]:
            """Simulate CPU-bound work with hash calculations."""
            result = 0
            for i in range(iterations):
                result = hash((result, i, task_id)) % 1000000
            return {"task_id": task_id, "type": "cpu", "result": result}

        @celery_app.task
        def io_task(task_id: int, sleep_ms: int) -> dict[str, Any]:
            """Simulate IO-bound work with sleep."""
            time.sleep(sleep_ms / 1000.0)
            return {"task_id": task_id, "type": "io", "slept_ms": sleep_ms}

        @celery_app.task(
            bind=True,
            max_retries=3,
            retry_backoff=False,
            retry_jitter=False,
        )
        def retry_task(self, task_id: int, fail_count: int) -> dict[str, Any]:
            """Task that fails a specified number of times before succeeding.

            Uses self.request.retries to track attempt count (Celery built-in).
            """
            current_attempt = self.request.retries + 1  # retries starts at 0

            if current_attempt <= fail_count:
                # Manually retry with countdown=0 for immediate retry
                raise self.retry(
                    exc=ValueError(f"Simulated failure {current_attempt}/{fail_count}"),
                    countdown=0,
                )

            return {
                "task_id": task_id,
                "type": "retry",
                "attempts": current_attempt,
                "expected_failures": fail_count,
            }

        celery_worker.reload()

        # Configuration: 40% CPU, 40% IO, 20% retry tasks
        total_tasks = 10000
        cpu_ratio = 0.4
        io_ratio = 0.4

        num_cpu = int(total_tasks * cpu_ratio)
        num_io = int(total_tasks * io_ratio)
        num_retry = total_tasks - num_cpu - num_io

        results: list[Any] = []
        task_id = 0

        # Submit CPU tasks with varying workloads
        for _ in range(num_cpu):
            iterations = random.randint(100, 1000)
            results.append(cpu_task.delay(task_id, iterations))
            task_id += 1

        # Submit IO tasks with varying sleep times
        for _ in range(num_io):
            sleep_ms = random.randint(1, 10)
            results.append(io_task.delay(task_id, sleep_ms))
            task_id += 1

        # Submit retry tasks that fail 1-2 times before succeeding
        for _ in range(num_retry):
            fail_count = random.randint(1, 2)
            results.append(retry_task.delay(task_id, fail_count))
            task_id += 1

        # Collect results with timeout
        # Allow generous timeout for tasks with retries
        timeout_per_task = 5  # seconds (generous for retries)
        total_timeout = max(60, int(total_tasks * timeout_per_task))

        start_time = time.time()
        completed = []
        failed = []

        for i, result in enumerate(results):
            try:
                remaining_timeout = total_timeout - (time.time() - start_time)
                if remaining_timeout <= 0:
                    failed.append((i, "timeout"))
                    continue
                value = result.get(timeout=min(30, remaining_timeout))
                completed.append(value)
            except Exception as e:
                failed.append((i, str(e)))

        elapsed = time.time() - start_time

        # Verify results
        assert len(failed) == 0, f"Failed tasks: {failed[:10]}{'...' if len(failed) > 10 else ''}"
        assert len(completed) == total_tasks

        # Verify task types
        cpu_results = [r for r in completed if r["type"] == "cpu"]
        io_results = [r for r in completed if r["type"] == "io"]
        retry_results = [r for r in completed if r["type"] == "retry"]

        assert len(cpu_results) == num_cpu
        assert len(io_results) == num_io
        assert len(retry_results) == num_retry

        # Verify retry tasks actually retried
        for r in retry_results:
            assert r["attempts"] > r["expected_failures"], f"Task {r['task_id']} didn't retry enough"

        # Report stats
        print("\n--- Stress Test Results ---")
        print(f"Total tasks: {total_tasks}")
        print(f"  CPU tasks: {num_cpu}")
        print(f"  IO tasks: {num_io}")
        print(f"  Retry tasks: {num_retry}")
        print(f"Elapsed time: {elapsed:.2f}s")
        print(f"Throughput: {total_tasks / elapsed:.1f} tasks/sec")

    def test_priority_ordering_under_load(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that priority ordering is maintained under high load.

        Submit tasks at different priorities and verify higher priority
        tasks tend to complete before lower priority ones.

        Strategy: Submit low priority tasks first, then high priority.
        Each task does a small sleep to create observable ordering.
        If priorities work, high priority tasks should "cut in line".
        """

        @celery_app.task
        def priority_task(task_id: int, priority: int) -> dict[str, Any]:
            """Do small work and return completion time for ordering analysis."""
            # Small sleep to make ordering observable
            time.sleep(0.01)
            return {
                "task_id": task_id,
                "priority": priority,
                "completed_at": time.time(),
            }

        celery_worker.reload()

        # Use only two priority levels for clearer signal
        num_tasks = 1000  # 500 low + 500 high
        low_priority = 0
        high_priority = 255

        results = []
        task_id = 0

        # First, submit ALL low priority tasks
        for _ in range(num_tasks // 2):
            results.append(
                (
                    task_id,
                    low_priority,
                    priority_task.apply_async(
                        args=(task_id, low_priority),
                        priority=low_priority,
                    ),
                ),
            )
            task_id += 1
            time.sleep(0.001)  # Small delay to ensure different timestamps

        # Then submit ALL high priority tasks
        # These should "jump ahead" of remaining low priority tasks
        for _ in range(num_tasks // 2):
            results.append(
                (
                    task_id,
                    high_priority,
                    priority_task.apply_async(
                        args=(task_id, high_priority),
                        priority=high_priority,
                    ),
                ),
            )
            task_id += 1
            time.sleep(0.001)

        # Collect all results and their completion times
        completed_results = []
        for _tid, _priority, result in results:
            value = result.get(timeout=60)
            completed_results.append(value)

        # Sort by completion time to get actual execution order
        completed_results.sort(key=lambda x: x["completed_at"])

        # Assign positions based on completion order
        for position, result in enumerate(completed_results):
            result["position"] = position

        # Calculate average positions for each priority
        low_positions = [r["position"] for r in completed_results if r["priority"] == low_priority]
        high_positions = [r["position"] for r in completed_results if r["priority"] == high_priority]

        avg_low = sum(low_positions) / len(low_positions)
        avg_high = sum(high_positions) / len(high_positions)

        print("\n--- Priority Analysis ---")
        print(f"Low priority ({low_priority}): avg position {avg_low:.1f}")
        print(f"High priority ({high_priority}): avg position {avg_high:.1f}")
        print(f"Completion order (by task_id): {[r['task_id'] for r in completed_results]}")

        # High priority tasks (submitted second) should complete earlier on average
        # than low priority tasks (submitted first)
        # Allow some tolerance since some low priority tasks may have started before
        # high priority tasks were submitted
        assert avg_high < avg_low, (
            f"High priority ({high_priority}) avg position {avg_high:.1f} should be "
            f"less than low priority ({low_priority}) avg position {avg_low:.1f}"
        )

    def test_sustained_throughput(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test sustained throughput over multiple batches.

        Submit tasks in waves and verify consistent processing rate.
        """

        @celery_app.task
        def fast_task(batch: int, task_id: int) -> dict[str, int]:
            """Minimal task for throughput testing."""
            return {"batch": batch, "task_id": task_id}

        celery_worker.reload()

        num_batches = 10
        tasks_per_batch = 1000
        batch_results: list[dict[str, Any]] = []

        for batch in range(num_batches):
            batch_start = time.time()
            results = [fast_task.delay(batch, i) for i in range(tasks_per_batch)]

            # Wait for batch completion
            for result in results:
                result.get(timeout=60)

            batch_elapsed = time.time() - batch_start
            throughput = tasks_per_batch / batch_elapsed

            batch_results.append(
                {
                    "batch": batch,
                    "elapsed": batch_elapsed,
                    "throughput": throughput,
                },
            )

        # Report throughput stats
        throughputs = [b["throughput"] for b in batch_results]
        avg_throughput = sum(throughputs) / len(throughputs)
        min_throughput = min(throughputs)
        max_throughput = max(throughputs)

        print("\n--- Sustained Throughput Results ---")
        print(f"Batches: {num_batches} x {tasks_per_batch} tasks")
        print(f"Avg throughput: {avg_throughput:.1f} tasks/sec")
        print(f"Min throughput: {min_throughput:.1f} tasks/sec")
        print(f"Max throughput: {max_throughput:.1f} tasks/sec")

        # Verify throughput doesn't degrade significantly
        # Last batch should be at least 50% of first batch throughput
        assert batch_results[-1]["throughput"] >= batch_results[0]["throughput"] * 0.5, (
            "Throughput degraded significantly over time"
        )
