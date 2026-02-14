"""Redis/Valkey test fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

# Import client library from transport (handles redis-py vs valkey-py)
from celery_redis_plus.transport import client_lib

if TYPE_CHECKING:
    from collections.abc import Generator


# Container images for testing
REDIS_IMAGE = "redis:latest"
VALKEY_IMAGE = "valkey/valkey:latest"


@pytest.fixture(scope="session", params=[REDIS_IMAGE, VALKEY_IMAGE], ids=["redis", "valkey"])
def redis_container(request: pytest.FixtureRequest) -> Generator[tuple[str, int, str]]:
    """Start a Redis/Valkey container for integration tests.

    This fixture is parametrized to run tests against both Redis and Valkey.

    Yields:
        Tuple of (host, port, image_name) for the container.
    """
    image = request.param

    with DockerContainer(image).with_exposed_ports(6379) as container:
        wait_for_logs(container, "Ready to accept connections")
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield host, int(port), image


@pytest.fixture
def redis_client(redis_container: tuple[str, int, str]) -> Generator[Any]:
    """Create a Redis client connected to the test container.

    Args:
        redis_container: Tuple of (host, port, image) from redis_container fixture.

    Yields:
        Connected Redis client.
    """
    host, port, _image = redis_container
    client = client_lib.Redis(host=host, port=port, decode_responses=False)
    yield client
    client.flushall()
    client.close()


@pytest.fixture
def clear_redis(redis_client: Any) -> None:
    """Clear Redis database before each test.

    This fixture ensures tests start with a clean state.
    """
    redis_client.flushdb()
