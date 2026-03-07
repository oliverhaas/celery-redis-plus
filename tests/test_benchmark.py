"""Benchmark tests comparing celery-redis-plus vs standard Redis transport.

Run with:
    uv run pytest -m manual tests/test_benchmark.py -v -s
"""

from __future__ import annotations

import statistics
import time
from typing import Any

import pytest
from celery import Celery
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop


def _make_app(
    host: str,
    port: int,
    transport: str | None = None,
) -> Celery:
    """Create a Celery app for benchmarking.

    Args:
        host: Redis host.
        port: Redis port.
        transport: Transport class path, or None for standard Redis.
    """
    name = "bench-plus" if transport else "bench-standard"
    app = Celery(name)
    config: dict[str, Any] = {
        "broker_url": f"redis://{host}:{port}/0",
        "result_backend": f"redis://{host}:{port}/1",
        "task_serializer": "json",
        "accept_content": ["json"],
        "worker_prefetch_multiplier": 1,
        "result_backend_always_retry": False,
        "broker_connection_max_retries": 0,
    }
    if transport:
        config["broker_transport"] = transport
    app.conf.update(config)
    return app


def _run_throughput(
    app: Celery,
    num_tasks: int,
    num_batches: int,
) -> list[dict[str, float]]:
    """Run throughput benchmark on a Celery app with an embedded worker.

    Returns per-batch timing results.
    """

    @app.task(name="bench.noop")
    def noop(i: int) -> int:
        return i

    includes = ["celery_redis_plus"] if app.conf.get("broker_transport") else []
    for module in includes:
        app.loader.import_task_module(module)

    with start_worker(
        app,
        pool="solo",
        shutdown_timeout=30.0,
        perform_ping_check=False,
    ):
        batch_results = []
        for batch in range(num_batches):
            start = time.monotonic()
            results = [noop.delay(i) for i in range(num_tasks)]
            for r in results:
                r.get(timeout=120)
            elapsed = time.monotonic() - start
            batch_results.append(
                {
                    "batch": batch,
                    "elapsed": elapsed,
                    "throughput": num_tasks / elapsed,
                },
            )
    return batch_results


@pytest.mark.manual
class TestBenchmark:
    """Benchmark comparison: celery-redis-plus vs standard Redis transport.

    Runs identical workloads through both transports and prints comparison.
    """

    def test_throughput_comparison(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
    ) -> None:
        """Compare end-to-end throughput of both transports."""
        host, port, _image = redis_container
        num_tasks = 1000
        num_batches = 5

        # --- Standard Redis transport ---
        redis_client.flushdb()
        std_app = _make_app(host, port, transport=None)
        std_results = _run_throughput(std_app, num_tasks, num_batches)
        std_app.close()
        set_event_loop(None)

        # --- celery-redis-plus transport ---
        redis_client.flushdb()
        plus_app = _make_app(host, port, transport="celery_redis_plus.transport:Transport")
        plus_results = _run_throughput(plus_app, num_tasks, num_batches)
        plus_app.close()
        set_event_loop(None)

        # --- Print comparison ---
        std_throughputs = [b["throughput"] for b in std_results]
        plus_throughputs = [b["throughput"] for b in plus_results]

        std_mean = statistics.mean(std_throughputs)
        plus_mean = statistics.mean(plus_throughputs)
        ratio = plus_mean / std_mean if std_mean > 0 else float("inf")

        print("\n")
        print("=" * 60)
        print(f"  THROUGHPUT BENCHMARK ({num_batches} x {num_tasks} tasks)")
        print("=" * 60)
        print(f"  {'Transport':<25} {'Mean':>8} {'Min':>8} {'Max':>8}  (tasks/s)")
        print(f"  {'-' * 55}")
        print(
            f"  {'Standard Redis':<25} {std_mean:>8.1f} {min(std_throughputs):>8.1f} {max(std_throughputs):>8.1f}",
        )
        print(
            f"  {'celery-redis-plus':<25} {plus_mean:>8.1f} {min(plus_throughputs):>8.1f} {max(plus_throughputs):>8.1f}",
        )
        print(f"  {'-' * 55}")
        print(f"  Ratio (plus / standard): {ratio:.2f}x")
        print("=" * 60)

        # Per-batch detail
        print(f"\n  {'Batch':<8} {'Standard':>12} {'Plus':>12}  (tasks/s)")
        print(f"  {'-' * 36}")
        for i in range(num_batches):
            print(f"  {i:<8} {std_throughputs[i]:>12.1f} {plus_throughputs[i]:>12.1f}")

    def test_latency_comparison(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
    ) -> None:
        """Compare single-task round-trip latency of both transports."""
        host, port, _image = redis_container
        num_samples = 200

        def run_latency(app: Celery) -> list[float]:
            @app.task(name="bench.ping")
            def ping() -> bool:
                return True

            includes = ["celery_redis_plus"] if app.conf.get("broker_transport") else []
            for module in includes:
                app.loader.import_task_module(module)

            latencies: list[float] = []
            with start_worker(
                app,
                pool="solo",
                shutdown_timeout=30.0,
                perform_ping_check=False,
            ):
                # Warm up
                for _ in range(10):
                    ping.delay().get(timeout=30)

                for _ in range(num_samples):
                    start = time.monotonic()
                    ping.delay().get(timeout=30)
                    latencies.append(time.monotonic() - start)
            return latencies

        # --- Standard Redis transport ---
        redis_client.flushdb()
        std_app = _make_app(host, port, transport=None)
        std_latencies = run_latency(std_app)
        std_app.close()
        set_event_loop(None)

        # --- celery-redis-plus transport ---
        redis_client.flushdb()
        plus_app = _make_app(host, port, transport="celery_redis_plus.transport:Transport")
        plus_latencies = run_latency(plus_app)
        plus_app.close()
        set_event_loop(None)

        # --- Print comparison ---
        std_p50 = statistics.median(std_latencies) * 1000
        std_p95 = sorted(std_latencies)[int(0.95 * len(std_latencies))] * 1000
        std_mean = statistics.mean(std_latencies) * 1000

        plus_p50 = statistics.median(plus_latencies) * 1000
        plus_p95 = sorted(plus_latencies)[int(0.95 * len(plus_latencies))] * 1000
        plus_mean = statistics.mean(plus_latencies) * 1000

        print("\n")
        print("=" * 60)
        print(f"  LATENCY BENCHMARK ({num_samples} round trips)")
        print("=" * 60)
        print(f"  {'Transport':<25} {'Mean':>8} {'P50':>8} {'P95':>8}  (ms)")
        print(f"  {'-' * 55}")
        print(f"  {'Standard Redis':<25} {std_mean:>8.1f} {std_p50:>8.1f} {std_p95:>8.1f}")
        print(f"  {'celery-redis-plus':<25} {plus_mean:>8.1f} {plus_p50:>8.1f} {plus_p95:>8.1f}")
        print(f"  {'-' * 55}")
        print(f"  Ratio (plus / standard): {plus_mean / std_mean:.2f}x")
        print("=" * 60)
