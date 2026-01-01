"""Pytest fixtures for celery-redis-plus tests."""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from celery import Celery

if TYPE_CHECKING:
    from redis import Redis


@pytest.fixture
def celery_app() -> Celery:
    """Create a Celery app for testing."""
    app = Celery("test_app")
    app.config_from_object(
        {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        }
    )
    return app


@pytest.fixture
def mock_redis_client() -> MagicMock:
    """Create a mock Redis client."""
    client = MagicMock()
    client.zadd = MagicMock(return_value=1)
    client.zrangebyscore = MagicMock(return_value=[])
    client.zrem = MagicMock(return_value=1)
    client.lpush = MagicMock(return_value=1)
    client.eval = MagicMock(return_value=0)
    return client


# Fixtures for integration tests with testcontainers
@pytest.fixture(scope="session")
def redis_container() -> Generator[tuple[str, int]]:
    """Start a Redis container for integration tests.

    Yields:
        Tuple of (host, port) for the Redis container.
    """
    try:
        from testcontainers.redis import RedisContainer
    except ImportError:
        pytest.skip("testcontainers not installed")

    with RedisContainer() as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(6379)
        yield host, int(port)


@pytest.fixture
def redis_client(redis_container: tuple[str, int]) -> Generator[Redis]:
    """Create a Redis client connected to the test container.

    Args:
        redis_container: Tuple of (host, port) from redis_container fixture.

    Yields:
        Connected Redis client.
    """
    import redis

    host, port = redis_container
    client = redis.Redis(host=host, port=port, decode_responses=False)
    yield client
    client.flushall()
    client.close()


@pytest.fixture
def celery_app_with_redis(redis_container: tuple[str, int]) -> Celery:
    """Create a Celery app configured for the Redis test container.

    Args:
        redis_container: Tuple of (host, port) from redis_container fixture.

    Returns:
        Configured Celery app.
    """
    host, port = redis_container
    app = Celery("test_app")
    app.config_from_object(
        {
            "broker_url": f"redis://{host}:{port}/0",
            "result_backend": f"redis://{host}:{port}/1",
        }
    )
    return app
