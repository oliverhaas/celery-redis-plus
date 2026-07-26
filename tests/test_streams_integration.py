"""Integration tests for the Redis Streams transport (testcontainers: Redis and Valkey)."""

from __future__ import annotations

import time
from queue import Empty
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import patch

import pytest
from kombu import Connection

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


@pytest.mark.integration
class TestStreamsReclaimIntegration:
    """Discover-then-claim recovery of messages from consumers that stopped heartbeating.

    Read-only XPENDING IDLE discovery followed by a counting XCLAIM
    (Channel._reclaim_and_deliver). XAUTOCLAIM is not part of this design at
    all (see carry-forward.md section 4).
    """

    def test_peer_reclaims_unacked_message_after_visibility_timeout(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a peer claims an unacked message once its idle time exceeds visibility_timeout."""
        host, port, _image = redis_container
        redis_client.flushdb()

        app_a = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-a")
        app_b = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-b")
        try:
            with app_a.connection() as conn_a, app_b.connection() as conn_b:
                channel_a = cast("Channel", conn_a.default_channel)
                channel_b = cast("Channel", conn_b.default_channel)

                delivery_tag = "reclaim-vt-test"
                message = {
                    "body": '{"task": "test.add", "args": [1, 2]}',
                    "properties": {
                        "delivery_tag": delivery_tag,
                        "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                        "headers": {},
                    },
                }
                channel_a._put("celery", message)

                consumed = channel_a._get("celery")
                assert consumed["properties"]["delivery_tag"] == delivery_tag
                # worker-a never acks: simulates a worker that died mid-task

                with patch.object(channel_b.connection, "_deliver") as mock_deliver:
                    # Before the visibility timeout elapses the peer finds nothing to claim
                    assert channel_b._reclaim_and_deliver("celery", 10) == 0
                    mock_deliver.assert_not_called()

                    time.sleep(2.5)  # idle now exceeds visibility_timeout=2

                    claimed = channel_b._reclaim_and_deliver("celery", 10)

                    assert claimed == 1
                    payload, queue = mock_deliver.call_args[0]
                    assert queue == "celery"
                    assert payload["properties"]["delivery_tag"] == delivery_tag
                    # Second delivery -> x-restore-count injected, same location as the
                    # sorted-set transport (properties.headers) for parity
                    assert payload["properties"]["headers"]["x-restore-count"] == 1

                # PEL ownership moved to worker-b
                pending = channel_b.client.xpending_range(
                    f"{STREAM_KEY_PREFIX}celery:0",
                    DEFAULT_CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=10,
                )
                assert len(pending) == 1
                assert pending[0]["consumer"] == b"worker-b"
        finally:
            app_a.close()
            app_b.close()

    def test_reclaim_redelivers_idle_message_with_restore_count(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A message idle in the PEL past visibility_timeout is reclaimed by a peer and redelivered.

        Unlike the test above (which mocks Transport._deliver to inspect the
        raw payload), this drives the real basic_consume dispatch chain, so
        the delivered message is a genuine kombu Message wired to the
        reclaiming channel rather than a mocked call argument.

        Sequence: publish via one channel ("producer-worker"), consume it into
        the PEL without acking (simulating a worker that picked up the message
        then died before it could ack), wait past a short visibility_timeout,
        then reclaim via a second channel with a different consumer identity
        ("reclaimer-worker") standing in for a live peer.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "reclaim-integration-queue"

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": 0.3,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        reclaimer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": 0.3,
                "consumer_name": "reclaimer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-reclaim",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker" without acking, then
            # abandon it (simulates a worker dying before it could ack).
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-reclaim"

            # Let visibility_timeout (0.3s) elapse with a generous margin so the
            # entry is reliably reclaimable even under a slow container start or
            # a GC pause on a loaded CI host.
            time.sleep(1.0)

            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 1
        assert len(delivered) == 1
        # basic_consume wraps the raw dict _reclaim_and_deliver hands to
        # connection._deliver in a kombu Message before invoking the callback.
        delivered_message = delivered[0]
        assert delivered_message.delivery_tag == "tag-integration-reclaim"
        assert delivered_message.properties["headers"]["x-restore-count"] == 1


@pytest.mark.integration
class TestStreamsHeartbeatIntegration:
    """XCLAIM JUSTID heartbeats keeping in-flight messages alive past the visibility timeout."""

    def test_heartbeat_prevents_peer_reclaim(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that heartbeats keep an in-flight message unclaimed longer than visibility_timeout."""
        host, port, _image = redis_container
        redis_client.flushdb()

        app_a = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-a")
        app_b = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-b")
        try:
            with app_a.connection() as conn_a, app_b.connection() as conn_b:
                channel_a = cast("Channel", conn_a.default_channel)
                channel_b = cast("Channel", conn_b.default_channel)

                message = {
                    "body": '{"task": "test.add", "args": [1, 2]}',
                    "properties": {
                        "delivery_tag": "heartbeat-survival-test",
                        "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                        "headers": {},
                    },
                }
                channel_a._put("celery", message)
                channel_a._get("celery")

                # Task "runs" for 3s, longer than visibility_timeout=2, with the worker
                # main loop heartbeating every 0.5s
                for _ in range(6):
                    time.sleep(0.5)
                    channel_a._heartbeat()

                with patch.object(channel_b.connection, "_deliver") as mock_deliver:
                    # Idle was reset by the heartbeats: the peer finds nothing
                    assert channel_b._reclaim_and_deliver("celery", 10) == 0
                    mock_deliver.assert_not_called()

                    # Once the heartbeat stops the message becomes reclaimable again
                    time.sleep(2.5)
                    assert channel_b._reclaim_and_deliver("celery", 10) == 1

                    payload, _queue = mock_deliver.call_args[0]
                    # JUSTID heartbeats do not bump the delivery count: this is still
                    # only the first restore
                    assert payload["properties"]["headers"]["x-restore-count"] == 1
        finally:
            app_a.close()
            app_b.close()

    def test_heartbeat_keeps_in_flight_message_alive_past_visibility_timeout(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A message held by a live worker survives repeated reclaim attempts while heartbeated.

        Unlike the test above, this drives the real basic_consume dispatch
        chain (a delivered list populated via a callback) instead of mocking
        Transport._deliver, and builds the reclaimer's channel and consumer
        group up front so that setup cost is paid before the heartbeat loop
        rather than in the critical window between the last heartbeat and the
        reclaim check below. The reclaim must find nothing: the heartbeat
        resets the idle clock before it ever qualifies as abandoned.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "heartbeat-integration-queue"
        visibility_timeout = 0.3

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        reclaimer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "reclaimer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-heartbeat",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker" and hold it, standing
            # in for a task that is still running (never acked).
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-heartbeat"

            # Build the reclaimer's channel and consumer group up front, so
            # that setup cost is paid before the heartbeat loop rather than
            # in the critical window between the last heartbeat and the
            # reclaim check below.
            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            # Heartbeat well past visibility_timeout, resetting idle every
            # half-period so it never crosses the timeout.
            for _ in range(6):
                time.sleep(visibility_timeout / 2)
                producer_channel._heartbeat()

            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 0
        assert delivered == []


@pytest.mark.integration
class TestStreamsPoisonIntegration:
    """max_restore_count enforcement and dead-letter copies for poison messages."""

    def test_message_dropped_after_max_restore_count(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a message exceeding max_restore_count is dropped instead of redelivered.

        Each reclaim is driven by a distinct peer (worker-a delivers and never
        acks; worker-b reclaims once; worker-c reclaims again), never by the
        channel that currently holds the entry. _own_in_flight_message_ids
        deliberately never lets a channel reclaim an id it already holds
        in-flight itself (it could be racing a still-running callback on that
        same process), so a single, self-reclaiming channel would stay
        permanently exempt from max_restore_count instead of exercising the
        poison cap. Three distinct workers is the realistic shape of this
        scenario: worker-a dies mid-task, worker-b picks it up and also fails
        to ack, worker-c is the one that finally exceeds the cap and drops it.
        """
        host, port, _image = redis_container
        redis_client.flushdb()

        app_a = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-a")
        app_b = _make_streams_app(
            host,
            port,
            global_keyprefix,
            visibility_timeout=2,
            max_restore_count=1,
            consumer_name="worker-b",
        )
        app_c = _make_streams_app(
            host,
            port,
            global_keyprefix,
            visibility_timeout=2,
            max_restore_count=1,
            consumer_name="worker-c",
        )
        try:
            with app_a.connection() as conn_a, app_b.connection() as conn_b, app_c.connection() as conn_c:
                channel_a = cast("Channel", conn_a.default_channel)
                channel_b = cast("Channel", conn_b.default_channel)
                channel_c = cast("Channel", conn_c.default_channel)

                message = {
                    "body": '{"task": "test.add", "args": [1, 2]}',
                    "properties": {
                        "delivery_tag": "poison-drop-test",
                        "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                        "headers": {},
                    },
                }
                channel_a._put("celery", message)
                channel_a._get("celery")  # delivery 1, never acked (crash-looping task)

                with patch.object(channel_b.connection, "_deliver") as mock_deliver_b:
                    time.sleep(2.5)
                    # worker-b reclaims: restore count 1 == max_restore_count -> still delivered
                    assert channel_b._reclaim_and_deliver("celery", 10) == 1
                    assert mock_deliver_b.call_count == 1
                    payload, _queue = mock_deliver_b.call_args[0]
                    assert payload["properties"]["headers"]["x-restore-count"] == 1
                # worker-b never acks either (also crash-looping)

                with patch.object(channel_c.connection, "_deliver") as mock_deliver_c:
                    time.sleep(2.5)
                    # worker-c reclaims: restore count 2 > max_restore_count -> dropped
                    channel_c._reclaim_and_deliver("celery", 10)
                    mock_deliver_c.assert_not_called()

                # Dropped message is fully gone: no stream entry, nothing pending
                assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0
                pending = channel_c.client.xpending_range(
                    f"{STREAM_KEY_PREFIX}celery:0",
                    DEFAULT_CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=10,
                )
                assert pending == []
        finally:
            app_a.close()
            app_b.close()
            app_c.close()

    def test_dead_letter_copy_written_when_configured(
        self,
        redis_container: tuple[str, int, str],
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a poisoned message is copied to the dead letter stream before being dropped.

        As above, the reclaim is driven by a peer (worker-b), never by the
        delivering channel itself: _own_in_flight_message_ids would otherwise
        keep worker-a's own never-acked delivery permanently exempt from the
        poison cap.
        """
        host, port, _image = redis_container
        redis_client.flushdb()

        app_a = _make_streams_app(host, port, global_keyprefix, visibility_timeout=2, consumer_name="worker-a")
        app_b = _make_streams_app(
            host,
            port,
            global_keyprefix,
            visibility_timeout=2,
            max_restore_count=0,
            dead_letter_stream="dead-letters",
            consumer_name="worker-b",
        )
        try:
            with app_a.connection() as conn_a, app_b.connection() as conn_b:
                channel_a = cast("Channel", conn_a.default_channel)
                channel_b = cast("Channel", conn_b.default_channel)

                message = {
                    "body": '{"task": "test.add", "args": [1, 2]}',
                    "properties": {
                        "delivery_tag": "poison-dlq-test",
                        "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                        "headers": {},
                    },
                }
                channel_a._put("celery", message)
                channel_a._get("celery")  # delivery 1, never acked

                with patch.object(channel_b.connection, "_deliver") as mock_deliver:
                    time.sleep(2.5)
                    # restore count 1 > max_restore_count=0 -> dead-letter + drop, no delivery
                    channel_b._reclaim_and_deliver("celery", 10)
                    mock_deliver.assert_not_called()

                # Original entry gone from the queue stream
                assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0
                # Copy landed in the dead letter stream (prefixed like all transport keys)
                entries = redis_client.xrange(f"{global_keyprefix}dead-letters")
                assert len(entries) == 1
                _entry_id, fields = entries[0]
                assert b"poison-dlq-test" in fields[b"payload"]
        finally:
            app_a.close()
            app_b.close()
