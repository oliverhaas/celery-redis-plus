"""Tests for migrating tasks from standard Celery Redis transport to celery-redis-plus.

This module tests that tasks queued using Celery's standard Redis transport
(LIST-based) can be migrated to celery-redis-plus (sorted set-based) using
Celery's built-in migration tools.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from celery import Celery
from celery.contrib.migrate import migrate_tasks

from celery_redis_plus.constants import (
    MESSAGE_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
)


def _decode_celery_body(body_str: str) -> Any:
    """Decode a Celery message body (base64 encoded JSON)."""
    return json.loads(base64.b64decode(body_str).decode("utf-8"))


@pytest.mark.integration
class TestMigrationFromStandardRedisTransport:
    """Tests for migrating tasks from standard Celery Redis transport.

    This test class verifies that tasks queued using the standard Redis transport
    (LIST-based) can be migrated to the celery-redis-plus transport (sorted set-based)
    using Celery's built-in migration tools.
    """

    def test_migrate_tasks_from_list_to_sorted_set(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        clear_redis: None,
    ) -> None:
        """Test migrating tasks from standard Redis transport to celery-redis-plus.

        This test:
        1. Creates tasks using the standard Redis transport (LIST-based)
        2. Verifies messages are in Redis LISTs
        3. Uses celery.contrib.migrate to move tasks to celery-redis-plus
        4. Verifies messages are now in sorted sets with per-message hashes
        """
        host, port, _image = redis_container

        # Step 1: Create source app with STANDARD Redis transport
        source_app = Celery("source_app")
        source_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
            task_serializer="json",
            accept_content=["json"],
        )

        @source_app.task(name="tasks.add")
        def add(x: int, y: int) -> int:
            return x + y

        # Step 2: Publish tasks using standard transport (no worker)
        for i in range(3):
            add.apply_async(args=(i, i * 2), task_id=f"migrate-task-{i}")

        # Verify messages are in Redis LIST (standard format)
        # Standard transport uses the queue name directly as the key
        list_length = redis_client.llen("celery")
        assert list_length == 3, f"Expected 3 messages in LIST, got {list_length}"

        # Peek at message format in LIST
        list_messages = redis_client.lrange("celery", 0, -1)
        assert len(list_messages) == 3
        for msg_bytes in list_messages:
            msg = json.loads(msg_bytes)
            assert "body" in msg
            assert "properties" in msg

        # Step 3: Create destination app with celery-redis-plus transport
        dest_app = Celery("dest_app")
        dest_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        # Step 4: Migrate tasks using Celery's migration tools
        with source_app.connection() as source_conn, dest_app.connection() as dest_conn:
            # Note: celery-stubs has incorrect type signature (expects strings, but runtime accepts Connection)
            state = migrate_tasks(
                source_conn,  # type: ignore[arg-type]
                dest_conn,  # type: ignore[arg-type]
                app=dest_app,
                timeout=5.0,
                ack_messages=True,  # Acknowledge messages from source
            )

        # Step 5: Verify messages are now in sorted set format
        # Standard LIST should be empty (messages consumed)
        list_length_after = redis_client.llen("celery")
        assert list_length_after == 0, f"Expected LIST to be empty, got {list_length_after}"

        # celery-redis-plus sorted set should have messages
        sorted_set_length = redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery")
        assert sorted_set_length == 3, f"Expected 3 messages in sorted set, got {sorted_set_length}"

        # Verify per-message hashes exist
        members = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(members) == 3

        for delivery_tag_raw in members:
            delivery_tag = delivery_tag_raw.decode() if isinstance(delivery_tag_raw, bytes) else delivery_tag_raw
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            assert redis_client.exists(message_key), f"Message hash {message_key} not found"

            # Verify message hash has expected fields
            payload = redis_client.hget(message_key, "payload")
            assert payload is not None
            routing_key = redis_client.hget(message_key, "routing_key")
            assert routing_key is not None

        # Step 6: Verify migration state
        assert state.count == 3, f"Expected 3 messages migrated, got {state.count}"

        # Cleanup
        source_app.close()
        dest_app.close()

    def test_migrate_preserves_task_data(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        clear_redis: None,
    ) -> None:
        """Test that migration preserves task arguments and metadata."""
        host, port, _image = redis_container

        # Create source app with standard transport
        source_app = Celery("source_app")
        source_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        @source_app.task(name="tasks.complex")
        def complex_task(data: dict, count: int) -> dict:
            return {"data": data, "count": count}

        # Publish task with complex arguments
        original_args = ({"key": "value", "nested": [1, 2, 3]}, 42)
        original_task_id = "preserve-test-task"
        complex_task.apply_async(
            args=original_args,
            task_id=original_task_id,
        )

        # Migrate to celery-redis-plus
        dest_app = Celery("dest_app")
        dest_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        with source_app.connection() as source_conn, dest_app.connection() as dest_conn:
            migrate_tasks(
                source_conn,  # type: ignore[arg-type]
                dest_conn,  # type: ignore[arg-type]
                app=dest_app,
                timeout=5.0,
                ack_messages=True,
            )

        # Verify task data is preserved in new format
        members = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(members) == 1

        delivery_tag = members[0]
        if isinstance(delivery_tag, bytes):
            delivery_tag = delivery_tag.decode()

        message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
        payload_json = redis_client.hget(message_key, "payload")
        payload = json.loads(payload_json)

        # Verify task body contains original arguments
        # Body is base64 encoded in Celery v2 protocol
        body = _decode_celery_body(payload["body"])
        # Celery v2 protocol: body is [args, kwargs, embed]
        if isinstance(body, list) and len(body) >= 2:
            migrated_args = body[0]
            assert migrated_args == list(original_args)

        # Cleanup
        source_app.close()
        dest_app.close()

    def test_migrate_priority_tasks(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        clear_redis: None,
    ) -> None:
        """Test migrating tasks with different priorities.

        Standard transport uses multiple lists with priority suffixes.
        celery-redis-plus encodes priority in sorted set scores.
        """
        host, port, _image = redis_container

        # Create source app with standard transport
        source_app = Celery("source_app")
        source_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
            broker_transport_options={"priority_steps": list(range(10))},
        )

        @source_app.task(name="tasks.prioritized")
        def prioritized_task(name: str) -> str:
            return name

        # Publish tasks with different priorities
        prioritized_task.apply_async(args=("low",), priority=1, task_id="low-pri")
        prioritized_task.apply_async(args=("high",), priority=9, task_id="high-pri")
        prioritized_task.apply_async(args=("normal",), priority=5, task_id="normal-pri")

        # Standard transport may create different lists per priority step
        # Verify at least some messages exist (across possibly multiple keys)
        total_in_lists = 0
        for i in range(10):
            # Standard transport uses separator chars for priority keys
            key = f"celery\x06\x16{i}"
            total_in_lists += redis_client.llen(key)
        # Also check the base key
        total_in_lists += redis_client.llen("celery")
        assert total_in_lists == 3, f"Expected 3 messages in LISTs, got {total_in_lists}"

        # Migrate to celery-redis-plus
        dest_app = Celery("dest_app")
        dest_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        with source_app.connection() as source_conn, dest_app.connection() as dest_conn:
            state = migrate_tasks(
                source_conn,  # type: ignore[arg-type]
                dest_conn,  # type: ignore[arg-type]
                app=dest_app,
                timeout=5.0,
                ack_messages=True,
            )

        assert state.count == 3, f"Expected 3 messages migrated, got {state.count}"

        # All tasks should now be in single sorted set with priority scores
        sorted_set_length = redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery")
        assert sorted_set_length == 3, f"Expected 3 in sorted set, got {sorted_set_length}"

        # Verify messages are ordered correctly (high priority = lower score)
        members_with_scores = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)

        # Extract task names from payloads to verify ordering
        task_order = []
        for delivery_tag_raw, _score in members_with_scores:
            delivery_tag = delivery_tag_raw.decode() if isinstance(delivery_tag_raw, bytes) else delivery_tag_raw
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload_json = redis_client.hget(message_key, "payload")
            payload = json.loads(payload_json)
            # Body is base64 encoded in Celery v2 protocol
            body = _decode_celery_body(payload["body"])
            if isinstance(body, list):
                task_name = body[0][0]  # First arg
                task_order.append(task_name)

        # Higher priority tasks should come first (lower score)
        # Expected order: high, normal, low
        assert task_order[0] == "high", f"Expected 'high' first, got {task_order}"
        assert task_order[-1] == "low", f"Expected 'low' last, got {task_order}"

        # Cleanup
        source_app.close()
        dest_app.close()

    def test_migrated_tasks_execute_with_new_worker(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        clear_redis: None,
    ) -> None:
        """Test that migrated tasks can be executed by a celery-redis-plus worker.

        This is an end-to-end test that:
        1. Publishes tasks with standard transport
        2. Migrates to celery-redis-plus
        3. Starts a worker and executes the migrated tasks
        """
        host, port, _image = redis_container

        # Create source app with standard transport
        source_app = Celery("source_app")
        source_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        @source_app.task(name="tasks.add_numbers")
        def add_numbers(x: int, y: int) -> int:
            return x + y

        # Publish tasks (store task IDs for later verification)
        task_ids = []
        for i in range(3):
            result = add_numbers.apply_async(args=(i, i + 1), task_id=f"exec-test-{i}")
            task_ids.append(result.id)

        # Create destination app with celery-redis-plus transport
        dest_app = Celery("dest_app")
        dest_app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            result_backend=f"redis://{host}:{port}/1",
            task_serializer="json",
            accept_content=["json"],
        )

        # Register same task on destination app
        @dest_app.task(name="tasks.add_numbers")
        def add_numbers_dest(x: int, y: int) -> int:
            return x + y

        # Migrate tasks
        with source_app.connection() as source_conn, dest_app.connection() as dest_conn:
            migrate_tasks(
                source_conn,  # type: ignore[arg-type]
                dest_conn,  # type: ignore[arg-type]
                app=dest_app,
                timeout=5.0,
                ack_messages=True,
            )

        # Verify messages were migrated
        sorted_set_length = redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery")
        assert sorted_set_length == 3

        # Start worker and execute tasks
        from celery.contrib.testing.worker import start_worker

        with start_worker(
            dest_app,
            pool="solo",
            shutdown_timeout=30.0,
            perform_ping_check=False,  # Skip ping check since we're using standalone app
        ):
            # Get results for migrated tasks
            from celery.result import AsyncResult

            results = []
            for task_id in task_ids:
                async_result = AsyncResult(task_id, app=dest_app)
                result = async_result.get(timeout=10)
                results.append(result)

            # Verify results: (0+1), (1+2), (2+3) = 1, 3, 5
            assert results == [1, 3, 5]

        # Cleanup
        source_app.close()
        dest_app.close()
