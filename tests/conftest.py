"""Pytest configuration for celery-redis-plus tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

# Re-export fixtures from fixtures.py
from tests.fixtures import redis_client, redis_container

if TYPE_CHECKING:
    pass

__all__ = [
    "redis_client",
    "redis_container",
]

# Enable celery.contrib.pytest plugin for celery_app and celery_worker fixtures
pytest_plugins = ("celery.contrib.pytest",)


@pytest.fixture(scope="session")
def celery_config(redis_container: tuple[str, int, str]) -> dict[str, Any]:
    """Configure Celery to use our custom Redis transport.

    This fixture is used by celery.contrib.pytest's celery_app fixture.

    Args:
        redis_container: Tuple of (host, port, image) from redis_container fixture.

    Returns:
        Celery configuration dictionary.
    """
    host, port, _image = redis_container
    return {
        "broker_url": f"celery_redis_plus.transport:Transport://{host}:{port}/0",
        "result_backend": f"redis://{host}:{port}/1",
    }


@pytest.fixture(scope="session")
def celery_includes() -> list[str]:
    """Modules to import when the worker starts.

    This ensures our signal handlers are registered.
    """
    return ["celery_redis_plus"]
