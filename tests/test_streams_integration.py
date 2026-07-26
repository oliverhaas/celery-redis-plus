"""Integration tests for the Redis Streams transport (testcontainers: Redis and Valkey)."""

from __future__ import annotations

import time
from queue import Empty
from typing import TYPE_CHECKING, Any, cast

import pytest

from celery_redis_plus.constants import (
    DEFAULT_CONSUMER_GROUP,
    DELAYED_KEY_PREFIX,
    STREAM_KEY_PREFIX,
)
from celery_redis_plus.streams import Channel, QoS, Transport

if TYPE_CHECKING:
    from celery import Celery


@pytest.fixture
def celery_config(redis_container: tuple[str, int, str], global_keyprefix: str) -> dict[str, Any]:
    """Configure Celery to use the streams transport via the valkey-streams:// scheme.

    Same-name override of the sorted-set config in tests/fixtures/celery.py:
    pytest resolves the module-level fixture first, so celery.contrib.pytest's
    celery_app fixture picks up the streams broker for every test in this file.

    Args:
        redis_container: Tuple of (host, port, image) from redis_container fixture.
        global_keyprefix: Key prefix (empty or "testprefix:").

    Returns:
        Celery configuration dictionary.
    """
    host, port, _image = redis_container
    config: dict[str, Any] = {
        "broker_url": f"valkey-streams://{host}:{port}/0",
        "result_backend": f"redis://{host}:{port}/1",
    }
    if global_keyprefix:
        config["broker_transport_options"] = {"global_keyprefix": global_keyprefix}
    return config


def _make_streams_app(host: str, port: int, global_keyprefix: str, **transport_options: Any) -> Celery:
    """Create a Celery app on the streams transport with explicit transport options.

    For tests that need per-test options (visibility_timeout, consumer_name,
    max_restore_count, ...) which the shared celery_config fixture does not set.

    Args:
        host: Redis container host.
        port: Redis container port.
        global_keyprefix: Key prefix (empty or "testprefix:"), added to the options when set.
        **transport_options: Extra broker_transport_options entries.

    Returns:
        Configured Celery app; callers must close() it.
    """
    from celery import Celery as CeleryApp

    options: dict[str, Any] = dict(transport_options)
    if global_keyprefix:
        options["global_keyprefix"] = global_keyprefix

    app = CeleryApp("test_streams_options")
    app.conf.update(
        broker_url=f"valkey-streams://{host}:{port}/0",
        result_backend=f"redis://{host}:{port}/1",
        task_always_eager=False,
    )
    if options:
        app.conf.update(broker_transport_options=options)
    return app


@pytest.mark.integration
class TestStreamsBasicFlow:
    """Basic publish/consume/ack flow through the streams transport."""

    def test_scheme_selects_streams_transport(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that the valkey-streams:// broker URL resolves to the streams Transport."""
        with celery_app.connection() as conn:
            assert isinstance(conn.transport, Transport)

    def test_put_then_get_roundtrip(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that _put appends to the level stream and _get returns the message."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages (defensive; the session container is shared)
            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            delivery_tag = f"roundtrip-{time.time()}"
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }

            channel._put("celery", message)

            # Default priority 0 buckets to level 0
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1

            consumed = channel._get("celery")

            assert consumed["body"] == '{"task": "test.add", "args": [1, 2]}'
            assert consumed["properties"]["delivery_tag"] == delivery_tag
            # XREADGROUP registers the entry as pending but does not remove it:
            # it stays in the stream until acked
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1

    def test_ack_removes_entry_from_stream(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that ack removes the entry from both the stream and the PEL (XACK + XDEL)."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            delivery_tag = f"ack-test-{time.time()}"
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }
            channel._put("celery", message)
            channel._get("celery")

            qos = cast("QoS", channel.qos)
            assert delivery_tag in qos._in_flight

            qos.ack(delivery_tag)

            assert delivery_tag not in qos._in_flight
            # Entry deleted from the stream (streams shrink on every ack)
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0
            # And no longer pending in the consumer group
            pending = channel.client.xpending_range(
                f"{STREAM_KEY_PREFIX}celery:0",
                DEFAULT_CONSUMER_GROUP,
                min="-",
                max="+",
                count=10,
            )
            assert pending == []

    def test_get_raises_empty_when_queue_empty(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _get raises Empty on a queue with no messages."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            with pytest.raises(Empty):
                channel._get("empty_streams_queue")

    def test_purge_removes_streams_and_delayed_zset(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that _purge counts and deletes stream entries plus staged delayed messages."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            immediate_msg = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": "purge-immediate",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }
            channel._put("celery", immediate_msg)

            delayed_msg = {
                "body": '{"task": "test.add", "args": [3, 4]}',
                "properties": {
                    "delivery_tag": "purge-delayed",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                    "eta": time.time() + 120,
                },
            }
            channel._put("celery", delayed_msg)

            assert channel._size("celery") == 2

            purged = channel._purge("celery")

            assert purged == 2
            assert not redis_client.exists(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0")
            assert not redis_client.exists(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery")

    def test_task_execution_end_to_end(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test full task execution through a real worker over the streams transport."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        result = add.apply_async(args=(1, 2))
        value = result.get(timeout=10)

        assert value == 3


@pytest.mark.integration
class TestStreamsPriority:
    """Priority-step bucketing and consume ordering across level streams."""

    def test_high_priority_consumed_before_low_priority(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that _get drains higher level streams first regardless of publish order."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:9",
            )

            # Publish low priority first
            low_msg = {
                "body": '{"marker": "low"}',
                "properties": {
                    "delivery_tag": f"low-pri-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "priority": 0,
                    "headers": {},
                },
            }
            channel._put("celery", low_msg)

            # Publish high priority second
            high_msg = {
                "body": '{"marker": "high"}',
                "properties": {
                    "delivery_tag": f"high-pri-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "priority": 9,
                    "headers": {},
                },
            }
            channel._put("celery", high_msg)

            # One entry per level stream (steps default [0, 3, 6, 9])
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:9") == 1

            first = channel._get("celery")
            second = channel._get("celery")

            assert first["body"] == '{"marker": "high"}'
            assert second["body"] == '{"marker": "low"}'
            with pytest.raises(Empty):
                channel._get("celery")

    def test_priority_bucketed_to_highest_step_at_or_below(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that message priority maps onto the highest configured step <= priority."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:3",
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:9",
            )

            for delivery_tag, priority in [("bucket-p4", 4), ("bucket-p255", 255), ("bucket-default", None)]:
                properties: dict[str, Any] = {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                }
                if priority is not None:
                    properties["priority"] = priority
                channel._put("celery", {"body": "test", "properties": properties})

            # 4 -> step 3 (highest step <= 4); 255 -> step 9; missing -> step 0
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:3") == 1
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:9") == 1
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1


@pytest.mark.integration
class TestStreamsDelayed:
    """Native delayed delivery via the delayed:{queue} staging zset and the Lua pump."""

    def test_delayed_message_stored_in_delayed_zset(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a far-future eta stages the message in delayed:{queue}, not a stream."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            eta_timestamp = time.time() + 120  # far above the patched 2s threshold
            delivery_tag = f"delayed-storage-{time.time()}"
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                    "eta": eta_timestamp,
                },
            }

            channel._put("celery", message)

            entries = redis_client.zrange(
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
            assert len(entries) == 1
            member, score = entries[0]
            # Member is the full serialized message; score is the absolute eta in ms
            assert delivery_tag.encode() in member
            assert score == pytest.approx(eta_timestamp * 1000, abs=5)

            # No stream entry yet
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0

    def test_short_delay_goes_directly_to_stream(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that delays <= DEFAULT_REQUEUE_CHECK_INTERVAL are treated as immediate."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            # 1s <= the patched 2s interval, so this is NOT native delayed
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": f"short-delay-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                    "eta": time.time() + 1,
                },
            }

            channel._put("celery", message)

            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1
            assert redis_client.zcard(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery") == 0

    def test_delayed_message_delivered_after_eta(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test delayed delivery accuracy: the pump moves the message only once its eta has passed."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            start = time.time()
            eta_timestamp = start + 3  # > the patched 2s threshold -> native delayed
            delivery_tag = f"delayed-accuracy-{start}"
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                    "eta": eta_timestamp,
                },
            }
            channel._put("celery", message)

            assert redis_client.zcard(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery") == 1

            # Not due yet: the pump moves nothing and the queue reads empty
            assert channel._move_delayed("celery") == 0
            with pytest.raises(Empty):
                channel._get("celery")

            time.sleep(3.2)

            # Due now: the pump moves it into the level stream and _get delivers it
            assert channel._move_delayed("celery") == 1
            consumed = channel._get("celery")
            elapsed = time.time() - start

            assert consumed["properties"]["delivery_tag"] == delivery_tag
            assert elapsed >= 3  # never delivered before its eta (lower bound only)
            assert redis_client.zcard(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery") == 0
