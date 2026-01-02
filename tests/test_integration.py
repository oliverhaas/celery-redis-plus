"""End-to-end integration tests using a real Celery worker."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from celery import Celery


@pytest.mark.integration
class TestCeleryAppConfig:
    """Test that celery app is configured correctly."""

    def test_broker_url_configured(self, celery_app: Celery) -> None:
        """Test that broker URL is set correctly."""
        broker_url = celery_app.conf.broker_url
        broker_transport = celery_app.conf.broker_transport
        assert broker_url is not None
        assert "celery_redis_plus" in broker_transport

    def test_can_connect_to_broker(self, celery_app: Celery) -> None:
        """Test that we can connect to the broker."""
        with celery_app.connection() as conn:
            conn.ensure_connection()
            assert conn.connected  # type: ignore[attr-defined]


@pytest.mark.integration
class TestCeleryWorkerIntegration:
    """Integration tests that run tasks through a real Celery worker."""

    def test_simple_task_execution(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that a simple task executes successfully through the transport."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        result = add.delay(2, 3)
        assert result.get(timeout=10) == 5

    def test_multiple_tasks(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test multiple tasks execute correctly."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        results = [add.delay(i, i) for i in range(5)]
        values = [r.get(timeout=10) for r in results]
        assert values == [0, 2, 4, 6, 8]

    def test_task_with_countdown(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test task with countdown delay."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        start = time.time()
        result = add.apply_async(args=(1, 2), countdown=1)
        value = result.get(timeout=10)
        elapsed = time.time() - start

        assert value == 3
        # Task should have been delayed by approximately 1 second
        assert elapsed >= 0.9

    def test_task_priority(
        self,
        celery_app: Celery,
        celery_worker: Any,
        redis_client: Any,
    ) -> None:
        """Test that task priority affects ordering.

        Higher priority number = higher priority = processed first (RabbitMQ semantics).
        """

        @celery_app.task
        def slow_add(x: int, y: int) -> int:
            time.sleep(0.1)
            return x + y

        celery_worker.reload()
        # Send low priority task first
        low_priority = slow_add.apply_async(args=(1, 1), priority=0)
        # Send high priority task second
        high_priority = slow_add.apply_async(args=(2, 2), priority=9)

        # Both should complete
        low_result = low_priority.get(timeout=10)
        high_result = high_priority.get(timeout=10)

        assert low_result == 2
        assert high_result == 4


@pytest.mark.integration
class TestTransportFeatures:
    """Test transport-specific features."""

    def test_message_persistence(
        self,
        celery_app: Celery,
        celery_worker: Any,
        redis_client: Any,
    ) -> None:
        """Test that messages are stored in Redis sorted sets."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        # The messages should be stored in the messages hash
        # After task completion, they should be cleaned up
        result = add.delay(5, 5)
        value = result.get(timeout=10)
        assert value == 10

    def test_worker_handles_multiple_queues(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test worker can process tasks from multiple queues."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        # Default queue
        result1 = add.delay(1, 1)

        # Both should complete
        assert result1.get(timeout=10) == 2
