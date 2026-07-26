from __future__ import annotations

import os
import time
from typing import Any

import redis

_client: redis.Redis | None = None
_client_pid: int | None = None


def get_client() -> redis.Redis:
    """Lazy, fork-safe client: prefork children each get their own connection."""
    global _client, _client_pid  # noqa: PLW0603
    pid = os.getpid()
    if _client is None or _client_pid != pid:
        _client = redis.Redis.from_url(os.environ.get("BATTLE_LEDGER_URL", "redis://127.0.0.1:6391/0"))
        _client_pid = pid
    return _client


def record_execution(task_id: str, hostname: str, started_at: float) -> None:
    """Record one completed execution, idempotently. Retries, then raises.

    Raising fails the task, which Celery acks and discards anyway, so the hole this leaves in the
    ledger is shaped exactly like a lost message. The `task-failed` event is what separates the
    two; verification reads it and files the task under `failed` rather than `lost`.
    """
    global _client  # noqa: PLW0603
    entry = f"{hostname},{started_at:.3f}"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            client = get_client()
            # MULTI/EXEC lands both writes together, so finding this entry proves the earlier
            # attempt committed in full and a second push would read as a real duplicate.
            if attempt and client.lpos(f"executions:{task_id}", entry) is not None:
                return
            pipe = client.pipeline(transaction=True)
            pipe.rpush(f"executions:{task_id}", entry)
            pipe.sadd("executed_ids", task_id)
            pipe.execute()
        except redis.RedisError as exc:
            last_exc = exc
            _client = None
            time.sleep(0.2)
        else:
            return
    raise RuntimeError(f"ledger unreachable after retries: {last_exc}")


def record_submission(  # noqa: PLR0913
    client: redis.Redis,
    task_id: str,
    task_type: str,
    priority: int,
    sent_at: float,
    eligible_at: float,
) -> None:
    pipe = client.pipeline(transaction=False)
    pipe.hset(
        f"submitted:{task_id}",
        mapping={
            "type": task_type,
            "priority": priority,
            "sent_at": f"{sent_at:.3f}",
            "eligible_at": f"{eligible_at:.3f}",
        },
    )
    pipe.sadd("submitted_ids", task_id)
    pipe.execute()


def record_event(client: redis.Redis, event: dict[str, Any]) -> None:
    """Ledger a Celery event. Non-task events (no uuid) are ignored."""
    task_uuid = event.get("uuid")
    if not task_uuid:
        return
    key = f"event:{event['type']}:{task_uuid}"
    pipe = client.pipeline(transaction=False)
    pipe.hincrby(key, "count", 1)
    pipe.hsetnx(key, "event_ts", f"{event.get('timestamp', 0.0):.3f}")
    pipe.hsetnx(key, "received_at", f"{event.get('local_received', 0.0):.3f}")
    pipe.execute()
