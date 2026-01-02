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


@pytest.fixture
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
        "broker_url": f"redis://{host}:{port}/0",
        "broker_transport": "celery_redis_plus.transport:Transport",
        "result_backend": f"redis://{host}:{port}/1",
    }


@pytest.fixture
def celery_includes() -> list[str]:
    """Modules to import when the worker starts.

    This ensures our signal handlers are registered.
    """
    return ["celery_redis_plus"]


@pytest.fixture
def cleanup_async_results() -> Any:
    """Cleanup async results to prevent error messages during teardown."""
    import gc

    from celery.result import AsyncResult

    yield

    gc.collect()
    async_results = [obj for obj in gc.get_objects() if isinstance(obj, AsyncResult)]

    for async_result in async_results:
        if async_result.backend is not None:
            async_result.forget()
            async_result.backend = None


@pytest.fixture
def clear_kombu_global_event_loop() -> Any:
    """Clear kombu's global event loop to prevent reuse of closed hubs between test runs."""
    from kombu.asynchronous import set_event_loop

    yield

    set_event_loop(None)


@pytest.fixture
def celery_app(
    celery_app: Any,
    redis_container: tuple[str, int, str],
    cleanup_async_results: Any,
    clear_kombu_global_event_loop: Any,
) -> Any:
    """Override celery_app to ensure proper cleanup.

    The extra fixture dependencies ensure proper teardown order.
    """
    return celery_app


@pytest.fixture
def celery_worker(
    celery_app: Any,
    celery_includes: list[str],
    celery_worker_pool: str,
    celery_worker_parameters: dict[str, Any],
) -> Any:
    """Override celery_worker to use our custom celery_app."""
    from celery.contrib.pytest import NO_WORKER
    from celery.contrib.testing import worker

    if not NO_WORKER:
        for module in celery_includes:
            celery_app.loader.import_task_module(module)
        with worker.start_worker(
            celery_app,
            pool=celery_worker_pool,
            **celery_worker_parameters,
        ) as w:
            yield w
