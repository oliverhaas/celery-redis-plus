"""Pytest configuration for celery-redis-plus tests."""

from __future__ import annotations


def pytest_configure() -> None:
    """Patch transport settings before test collection.

    This runs before any test modules are imported, ensuring patches
    are in place when the transport module loads.

    - polling_interval: 1s (default 10s) for faster worker shutdown
    - DEFAULT_REQUEUE_CHECK_INTERVAL: 2s (default 60s) to test native delayed delivery
    """
    import celery_redis_plus.transport

    # Patch both the constants module and the transport module's local binding
    # (transport.py uses `from .constants import DEFAULT_REQUEUE_CHECK_INTERVAL`)
    celery_redis_plus.constants.DEFAULT_REQUEUE_CHECK_INTERVAL = 2  # type: ignore[misc]
    celery_redis_plus.transport.DEFAULT_REQUEUE_CHECK_INTERVAL = 2  # type: ignore[misc]

    celery_redis_plus.transport.Transport.polling_interval = 1


# Re-export fixtures from fixtures package
# ruff: noqa: E402  # Module level import not at top of file (intentional - pytest_configure must run first)
import pytest

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
    "global_keyprefix",
    "redis_client",
    "redis_container",
]


@pytest.fixture(params=["", "testprefix:"], ids=["no-prefix", "with-prefix"])
def global_keyprefix(request: pytest.FixtureRequest) -> str:
    """Parametrized global key prefix for testing with and without key prefixing."""
    return request.param


# Enable celery.contrib.pytest plugin for celery_app and celery_worker fixtures
pytest_plugins = ("celery.contrib.pytest",)
