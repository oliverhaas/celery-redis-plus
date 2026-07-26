from __future__ import annotations

import os
from typing import Literal

from celery import Celery

Role = Literal["worker", "producer", "monitor"]


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def create_app(role: Role = "worker") -> Celery:
    transport = os.environ.get("BATTLE_TRANSPORT", "plus")
    if os.environ.get("BATTLE_EVENT_PATCH") == "1":
        # Relative, so this resolves both as battle_app.app in the container and as
        # battle.battle_app.app from the host test suite.
        from . import event_patch

        event_patch.install()
    app = Celery(f"battle-{role}")
    app.conf.update(
        broker_url=os.environ.get("BATTLE_BROKER_URL", "redis://127.0.0.1:6390/0"),
        task_serializer="json",
        accept_content=["json"],
        result_backend=None,
        task_ignore_result=True,
        task_send_sent_event=True,
        worker_send_task_events=True,
        worker_pool=os.environ.get("BATTLE_POOL", "prefork"),
        worker_concurrency=_env_int("BATTLE_CONCURRENCY", 4),
        worker_prefetch_multiplier=_env_int("BATTLE_PREFETCH", 4),
        task_acks_late=os.environ.get("BATTLE_ACKS_LATE", "0") == "1",
        worker_soft_shutdown_timeout=_env_int("BATTLE_SOFT_SHUTDOWN_TIMEOUT", 8),
        broker_connection_retry_on_startup=True,
        # Publishes draw from this pool; Celery's default of 10 is tight for a sustained producer.
        broker_pool_limit=_env_int("BATTLE_BROKER_POOL_LIMIT", 32),
        broker_transport_options={"visibility_timeout": _env_int("BATTLE_VISIBILITY_TIMEOUT", 30)},
    )
    if transport == "plus":
        app.conf.broker_transport = "celery_redis_plus.transport:Transport"
        if role != "producer":
            # Only consuming roles need the requeue timer; never patch in the host/pytest process.
            # DEFAULT_REQUEUE_CHECK_INTERVAL is not a transport option, so patch it like tests/conftest.py does.
            import celery_redis_plus.constants
            import celery_redis_plus.transport

            seconds = _env_int("BATTLE_REQUEUE_INTERVAL", 10)
            celery_redis_plus.constants.DEFAULT_REQUEUE_CHECK_INTERVAL = seconds  # ty: ignore[invalid-assignment]
            celery_redis_plus.transport.DEFAULT_REQUEUE_CHECK_INTERVAL = seconds  # ty: ignore[invalid-assignment]
    return app
