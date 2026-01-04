"""Celery test fixtures."""

from __future__ import annotations

from typing import Any

import pytest


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
    clear_redis: None,
    cleanup_async_results: Any,
    clear_kombu_global_event_loop: Any,
) -> Any:
    """Override celery_app to ensure proper cleanup.

    The clear_redis fixture ensures each test starts with a clean Redis state.
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
            shutdown_timeout=30.0,
            **celery_worker_parameters,
        ) as w:
            yield w
