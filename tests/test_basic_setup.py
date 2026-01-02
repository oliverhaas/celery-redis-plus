"""Basic setup tests to verify test infrastructure works correctly."""

from __future__ import annotations

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
