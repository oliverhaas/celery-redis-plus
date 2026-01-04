"""Pytest configuration for celery-redis-plus tests."""

from __future__ import annotations


def pytest_configure() -> None:
    """Patch transport settings before test collection.

    This runs before any test modules are imported, ensuring patches
    are in place when the transport module loads.

    - polling_interval: 1s (default 10s) for faster worker shutdown
    - DEFAULT_REQUEUE_CHECK_INTERVAL: 2s (default 60s) to test native delayed delivery
    """
    import celery_redis_plus.constants

    celery_redis_plus.constants.DEFAULT_REQUEUE_CHECK_INTERVAL = 2  # type: ignore[misc]

    import celery_redis_plus.transport

    celery_redis_plus.transport.Transport.polling_interval = 1


# Re-export fixtures from fixtures package
# ruff: noqa: E402  # Module level import not at top of file (intentional - pytest_configure must run first)
from tests.fixtures import (
    celery_app,
    celery_config,
    celery_includes,
    celery_worker,
    cleanup_async_results,
    clear_kombu_global_event_loop,
    clear_redis,
    redis_client,
    redis_container,
)

__all__ = [
    "celery_app",
    "celery_config",
    "celery_includes",
    "celery_worker",
    "cleanup_async_results",
    "clear_kombu_global_event_loop",
    "clear_redis",
    "redis_client",
    "redis_container",
]

# Enable celery.contrib.pytest plugin for celery_app and celery_worker fixtures
pytest_plugins = ("celery.contrib.pytest",)
