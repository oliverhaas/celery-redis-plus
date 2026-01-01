"""Pytest fixtures for celery-redis-plus tests."""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from celery import Celery

if TYPE_CHECKING:
    from redis import Redis


# Container images for testing
REDIS_IMAGE = "redis:latest"
VALKEY_IMAGE = "valkey/valkey:latest"


@pytest.fixture
def celery_app() -> Celery:
    """Create a Celery app for testing."""
    app = Celery("test_app")
    app.config_from_object(
        {
            "broker_url": "memory://",
            "result_backend": "cache+memory://",
            "task_always_eager": True,
        },
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


def _get_ci_redis_configs() -> list[tuple[str, int, str]]:
    """Get Redis configurations from CI environment variables."""
    configs = []

    # Check for Redis service (GitHub Actions sets up services on localhost)
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")
    if os.environ.get("CI"):
        configs.append((redis_host, int(redis_port), "redis:latest"))

    # Check for Valkey service
    valkey_host = os.environ.get("VALKEY_HOST", "localhost")
    valkey_port = os.environ.get("VALKEY_PORT", "6380")
    if os.environ.get("CI"):
        configs.append((valkey_host, int(valkey_port), "valkey/valkey:latest"))

    return configs


# Fixtures for integration tests with testcontainers or CI services
@pytest.fixture(scope="session", params=[REDIS_IMAGE, VALKEY_IMAGE], ids=["redis", "valkey"])
def redis_container(request: pytest.FixtureRequest) -> Generator[tuple[str, int, str]]:
    """Start a Redis/Valkey container for integration tests.

    This fixture is parametrized to run tests against both Redis and Valkey.
    In CI environments, it uses pre-configured services instead of testcontainers.

    Yields:
        Tuple of (host, port, image_name) for the container.
    """
    image = request.param

    # Check if running in CI with pre-configured services
    if os.environ.get("CI"):
        if image == REDIS_IMAGE:
            host = os.environ.get("REDIS_HOST", "localhost")
            port = int(os.environ.get("REDIS_PORT", "6379"))
        else:
            host = os.environ.get("VALKEY_HOST", "localhost")
            port = int(os.environ.get("VALKEY_PORT", "6380"))
        yield host, port, image
        return

    # Use testcontainers for local development
    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.waiting_utils import wait_for_logs
    except ImportError:
        pytest.skip("testcontainers not installed")

    with DockerContainer(image).with_exposed_ports(6379) as container:
        wait_for_logs(container, "Ready to accept connections")
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield host, int(port), image


@pytest.fixture
def redis_client(redis_container: tuple[str, int, str]) -> Generator[Redis]:
    """Create a Redis client connected to the test container.

    Args:
        redis_container: Tuple of (host, port, image) from redis_container fixture.

    Yields:
        Connected Redis client.
    """
    import redis

    host, port, _image = redis_container
    client = redis.Redis(host=host, port=port, decode_responses=False)
    yield client
    client.flushall()
    client.close()


@pytest.fixture
def celery_app_with_redis(redis_container: tuple[str, int, str]) -> Celery:
    """Create a Celery app configured for the Redis test container.

    Args:
        redis_container: Tuple of (host, port, image) from redis_container fixture.

    Returns:
        Configured Celery app.
    """
    host, port, _image = redis_container
    app = Celery("test_app")
    app.config_from_object(
        {
            "broker_url": f"redis://{host}:{port}/0",
            "result_backend": f"redis://{host}:{port}/1",
        },
    )
    return app
