"""Integration tests for the Redis Streams transport (testcontainers: Redis and Valkey)."""

from __future__ import annotations

import time
from queue import Empty
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch

import pytest
from kombu import Connection
from kombu.asynchronous import Hub
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import dumps as json_dumps

from celery_redis_plus.constants import (
    DEFAULT_CONSUMER_GROUP,
    DELAYED_KEY_PREFIX,
    SHUTDOWN_IDLE_MS,
    STREAM_KEY_PREFIX,
)
from celery_redis_plus.streams import (
    _STREAMS_CLEANUP_CONSUMERS_LUA,
    Channel,
    QoS,
    Transport,
    priority_to_level,
)

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


@pytest.mark.integration
class TestStreamsShutdownIntegration:
    """Graceful shutdown: XCLAIM IDLE makes in-flight entries instantly reclaimable.

    Exercises the real client returned by Channel._get_client() (plain
    redis-py/valkey-py when global_keyprefix is falsy, PrefixedStrictRedis
    when truthy, via the parametrized global_keyprefix fixture), so both
    XCLAIM prefixing paths actually run against a live server.
    """

    def test_restore_unacked_once_marks_in_flight_reclaimable(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that restore_unacked_once applies XCLAIM IDLE so pending entries look long-idle."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            delivery_tag = f"shutdown-idle-{time.time()}"
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

            qos.restore_unacked_once()

            # Entry still exists (no payload movement) but its idle time was set far
            # above any sane visibility timeout, so any peer can claim it instantly
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1
            pending = channel.client.xpending_range(
                f"{STREAM_KEY_PREFIX}celery:0",
                DEFAULT_CONSUMER_GROUP,
                min="-",
                max="+",
                count=10,
            )
            assert len(pending) == 1
            # Allow 1s slack for server-side rounding between XCLAIM and XPENDING
            assert pending[0]["time_since_delivered"] >= SHUTDOWN_IDLE_MS - 1000

    def test_restore_unacked_once_releases_message_for_instant_peer_reclaim(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """The graceful shutdown release lets a peer reclaim the message immediately.

        Sequence: publish and consume via one channel ("producer-worker") so
        the entry sits in its PEL, then call the real producer_channel.close()
        (the production path: a Consumer bootstep closing its channel while
        the message is still in flight), not restore_unacked_once() directly.
        close() runs virtual.Channel.close() (which reaches
        QoS.restore_unacked_once()) before _disconnect_pools()/_close_clients()
        tear the channel's pool down, so whatever pool conn_or_acquire() uses
        during the release is disconnected afterward rather than leaked. Then
        read XPENDING directly to confirm the entry's idle time is now far
        above visibility_timeout while
        times_delivered is untouched by the release itself, then confirm a
        second channel's reclaim pass picks the entry up right away, well
        inside a visibility_timeout deliberately set long enough that a
        natural timeout expiry could not explain the pickup.

        This subsumes a simpler two-app "restore then peer reclaims" check
        (dropped, no assertion lost): everything that variant would show
        (peer claims == 1, matching delivery_tag) is asserted here too, via
        the heavier, more production-realistic path (real close(), plus the
        pre-claim times_delivered/idle inspection, plus the elapsed-time
        proof that the pickup is not just a natural visibility_timeout
        expiry).
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "shutdown-integration-queue"
        visibility_timeout = 20.0

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
                    "delivery_tag": "tag-integration-shutdown",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker", standing in for a
            # worker whose Consumer bootstep is closing while the message is
            # still in flight.
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-shutdown"

            producer_qos = cast("QoS", producer_channel.qos)
            stream_key, _message_id = producer_qos._in_flight["tag-integration-shutdown"]

            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            # No real Celery worker pool is registered for this connection;
            # patching just skips the executor-wait branch (covered by unit
            # tests) so this test isolates the XCLAIM IDLE release itself.
            # close(), not restore_unacked_once() directly: exercises the
            # real production shutdown path (see docstring).
            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
                producer_channel.close()

            assert producer_channel.closed is True
            assert producer_qos._in_flight == {}

            with reclaimer_channel.conn_or_acquire() as client:
                pending_after = client.xpending_range(
                    stream_key,
                    reclaimer_channel.consumer_group,
                    min="-",
                    max="+",
                    count=10,
                )
            assert len(pending_after) == 1
            entry = pending_after[0]
            # Idle time is now enormous: far above visibility_timeout (20s = 20000ms).
            assert entry["time_since_delivered"] >= SHUTDOWN_IDLE_MS
            # The release itself never bumps times_delivered: still 1, the
            # count from the original XREADGROUP delivery.
            assert int(entry["times_delivered"]) == 1

            start = time.monotonic()
            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
            elapsed = time.monotonic() - start
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 1
        assert len(delivered) == 1
        assert delivered[0].delivery_tag == "tag-integration-shutdown"
        # Reclaimed well inside the visibility_timeout window: this proves
        # the pickup came from the artificial idle release, not from
        # visibility_timeout naturally elapsing.
        assert elapsed < visibility_timeout / 2

    def test_deferred_ack_during_close_restore_window_still_fully_acks(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A hub.call_soon()-deferred ack drained during close() leaves no PEL or stream entry.

        F1 regression (Fix round 3): QoS.ack/reject's collected-channel no-op
        guard was keyed off channel.closed, but virtual.Channel.close() sets
        `closed = True` for a genuine shutdown too, well before it reaches
        restore_unacked_once(). Graceful shutdown's _drain_hub_callbacks()
        runs inside that same restore_unacked_once() call, specifically to
        flush acks from tasks that finished just before shutdown (scheduled
        via hub.call_soon()). Keying the no-op off `closed` silently dropped
        every one of those acks, leaving the PEL entry and stream entry
        behind (and, via the sibling restore path, given an artificial
        SHUTDOWN_IDLE_MS idle time), so a peer's reclaim pass would
        immediately redeliver an already-completed task. Reproduces the
        reviewer's repro shape directly: a real kombu Hub, a message acked
        via hub.call_soon(message.ack), then a real Channel.close().
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "graceful-shutdown-ack-queue"
        visibility_timeout = 20.0

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        hub = Hub()
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-graceful-shutdown-ack",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)
            # basic_get (not the raw _get) returns a real Message wired to
            # this channel, so message.ack() below exercises the exact same
            # call chain a completed task's deferred ack goes through in
            # production.
            consumed = producer_channel.basic_get(queue, no_ack=False)
            assert consumed is not None
            assert consumed.delivery_tag == "tag-graceful-shutdown-ack"

            producer_qos = cast("QoS", producer_channel.qos)
            stream_key, message_id = producer_qos._in_flight["tag-graceful-shutdown-ack"]

            # Wire a real Hub onto the channel's connection cycle, exactly as
            # Transport.register_with_event_loop() does in production, and
            # defer the ack the way a just-finished task's completion
            # callback does: call_soon(), not a direct call.
            producer_channel.connection.cycle._loop = hub
            hub.call_soon(consumed.ack)

            # No worker pool registered for this connection: isolates the
            # deferred-ack drain from the executor-wait branch (covered by
            # the test above and unit tests), so this test exercises only
            # the ack-during-restore-window guard.
            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
                producer_channel.close()

            assert producer_channel.closed is True
            assert producer_qos._in_flight == {}

            inspector_conn = Connection(
                broker_url,
                transport="celery_redis_plus.streams:Transport",
                transport_options={
                    "visibility_timeout": visibility_timeout,
                    "consumer_name": "inspector-worker",
                    "global_keyprefix": global_keyprefix,
                },
            )
            try:
                inspector_channel = cast("Channel", inspector_conn.channel())
                with inspector_channel.conn_or_acquire() as client:
                    pending_after = client.xpending_range(
                        stream_key,
                        inspector_channel.consumer_group,
                        min="-",
                        max="+",
                        count=10,
                    )
                    stream_entries = client.xrange(stream_key, message_id, message_id)
                # XACK removed the PEL entry...
                assert pending_after == []
                # ...and XDEL removed the stream entry itself.
                assert stream_entries == []
            finally:
                inspector_conn.close()
        finally:
            hub.close()
            producer_conn.close()

    def test_connection_collect_does_not_release_pel_or_shutdown_executor(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """Connection.collect() must not touch in-flight PEL entries or the executor.

        collect() is kombu's reconnect-cleanup escape hatch (celery calls it from
        on_connection_error_after_connected after a lost broker connection, not
        on a genuine shutdown), so this drives the real Connection.collect(),
        not Transport._collect directly, to prove kombu's dispatch actually
        finds and calls our _collect hook. A worker pool is registered for
        this connection so a stray executor.shutdown() would be observable;
        the PEL entry must be left exactly as XREADGROUP delivered it, for a
        peer to reclaim only after the visibility timeout naturally elapses,
        same as any other unreleased in-flight message.

        kombu's Connection.collect() severs the transport from its owning
        Connection unconditionally (Connection._do_close_transport sets
        transport.client = None even when a _collect hook handled the
        channels), so producer_conn/producer_channel are unusable for
        anything afterward, by kombu's own design, not because of a defect
        here. The post-collect state is inspected through a separate,
        independent connection instead, exactly as a peer reclaiming after a
        real lost connection would.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "collect-integration-queue"
        visibility_timeout = 20.0

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        inspector_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "inspector-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())
            inspector_channel = cast("Channel", inspector_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-collect",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-collect"

            producer_qos = cast("QoS", producer_channel.qos)
            in_flight_before = dict(producer_qos._in_flight)
            stream_key, _message_id = in_flight_before["tag-integration-collect"]

            mock_executor = MagicMock()
            mock_pool = MagicMock()
            mock_pool.executor = mock_executor

            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=mock_pool):
                producer_conn.collect()

            mock_executor.shutdown.assert_not_called()
            # The message is still owned by this worker: metadata untouched.
            assert producer_qos._in_flight == in_flight_before

            with inspector_channel.conn_or_acquire() as client:
                pending_after = client.xpending_range(
                    stream_key,
                    inspector_channel.consumer_group,
                    min="-",
                    max="+",
                    count=10,
                )
            assert len(pending_after) == 1
            entry = pending_after[0]
            # Idle time is small (a fraction of a second since XREADGROUP),
            # nowhere near the artificial SHUTDOWN_IDLE_MS a release would set.
            assert entry["time_since_delivered"] < SHUTDOWN_IDLE_MS
            assert int(entry["times_delivered"]) == 1
        finally:
            producer_conn.close()
            inspector_conn.close()


@pytest.mark.integration
class TestStreamsTTL:
    """Message TTL (x-message-ttl, lazy drop) and queue TTL (x-expires, PEXPIRE refresh)."""

    def test_expired_message_dropped_on_consume(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that entries older than the queue's x-message-ttl are dropped at delivery time."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            # Declare queue with a short message TTL (1 second)
            channel._new_queue("celery", arguments={"x-message-ttl": 1000})

            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": "ttl-expired-test",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }
            channel._put("celery", message)
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 1

            time.sleep(1.5)  # entry id timestamp is now older than the 1s TTL

            with pytest.raises(Empty):
                channel._get("celery")

            # The expired entry was acked and deleted, not left behind
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0

    def test_expired_delayed_message_dropped_by_pump(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that the delayed pump drops staged messages whose message TTL already expired."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            channel._new_queue("celery", arguments={"x-message-ttl": 1000})

            payload = {
                "body": "test",
                "properties": {
                    "delivery_tag": "ttl-delayed-expired",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }
            # Stage a member that became due 5s ago: older than the 1s TTL
            due_ms = (time.time() - 5) * 1000
            channel.client.zadd(f"{DELAYED_KEY_PREFIX}celery", {json_dumps(payload): due_ms})

            moved = channel._move_delayed("celery")

            assert moved == 0
            # Dropped from the zset without ever reaching a stream
            assert redis_client.zcard(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery") == 0
            assert redis_client.xlen(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0") == 0

    def test_queue_expires_sets_pexpire_on_stream_and_delayed_keys(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that the x-expires refresh applies PEXPIRE to level streams and the delayed zset."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            redis_client.delete(
                f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0",
                f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery",
            )

            # Set _expires directly to use a short TTL for a fast test
            # (validation in _new_queue enforces >= 10s, but PEXPIRE itself takes any value)
            channel._expires["celery"] = 2000

            immediate_msg = {
                "body": "test",
                "properties": {
                    "delivery_tag": "expires-stream-key",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }
            channel._put("celery", immediate_msg)  # creates stream:celery:0

            delayed_msg = {
                "body": "test",
                "properties": {
                    "delivery_tag": "expires-delayed-key",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                    "eta": time.time() + 120,
                },
            }
            channel._put("celery", delayed_msg)  # creates delayed:celery

            channel._refresh_queue_expires()

            stream_ttl = redis_client.pttl(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0")
            assert 0 < stream_ttl <= 2000
            delayed_ttl = redis_client.pttl(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery")
            assert 0 < delayed_ttl <= 2000

            # Wait for the TTL to expire (no refresh)
            time.sleep(2.5)

            assert not redis_client.exists(f"{global_keyprefix}{STREAM_KEY_PREFIX}celery:0")
            assert not redis_client.exists(f"{global_keyprefix}{DELAYED_KEY_PREFIX}celery")


@pytest.mark.integration
class TestStreamsHygiene:
    """Consumer-group hygiene: XGROUP DELCONSUMER for long-idle consumers.

    _cleanup_consumers depends on the SHAPE of the real xinfo_consumers reply: a
    list of dicts with 'name' (bytes), 'pending', and 'idle' keys. A prior round
    on this branch shipped a reply-shape bug (xautoclaim unpacked as a 3-tuple)
    that all 362 mocked unit tests passed over; mocking xinfo_consumers here
    would be equally blind to a similar mismatch. This exercises the real
    client returned by Channel._get_client() (plain redis-py/valkey-py when
    global_keyprefix is falsy, PrefixedStrictRedis when truthy, via the
    parametrized global_keyprefix fixture), so both prefixing paths run too.
    """

    def test_cleanup_deletes_idle_zero_pending_consumer_but_keeps_busy_one(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """Two consumers read one message each from the same group; one acks
        (pending drops to zero) and the other does not (pending stays one).
        After both go idle past CONSUMER_IDLE_CLEANUP_FACTOR * visibility_timeout,
        a cleanup pass run under the idle consumer's own identity leaves it in
        place (the script never deletes its caller, even though idle-worker
        otherwise meets every deletion criterion), and a later pass run under a
        third, uninvolved consumer identity then deletes only the idle,
        zero-pending consumer and leaves the busy one untouched.

        Subsumes the brief's two simpler single-assertion tests (idle+acked
        gets deleted; busy+never-acked survives), no assertion lost: both
        outcomes are asserted here too, plus the self-exclusion guard and the
        immediate-before-threshold guard that the simpler versions did not
        cover at all.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "cleanup-integration-queue"
        # idle threshold = CONSUMER_IDLE_CLEANUP_FACTOR * visibility_timeout * 1000
        # = 12 * 0.05 * 1000 = 600ms, comfortably crossed by the sleep below.
        visibility_timeout = 0.05

        def make_conn(consumer_name: str) -> Connection:
            return Connection(
                broker_url,
                transport="celery_redis_plus.streams:Transport",
                transport_options={
                    "visibility_timeout": visibility_timeout,
                    "consumer_name": consumer_name,
                    "global_keyprefix": global_keyprefix,
                },
            )

        idle_conn = make_conn("idle-worker")
        busy_conn = make_conn("busy-worker")
        cleaner_conn = make_conn("cleaner-worker")
        try:
            idle_channel = cast("Channel", idle_conn.channel())
            busy_channel = cast("Channel", busy_conn.channel())

            def make_message(tag: str) -> dict[str, Any]:
                return {
                    "body": '{"task": "test"}',
                    "properties": {
                        "delivery_tag": tag,
                        "delivery_info": {"exchange": "", "routing_key": queue},
                        "headers": {},
                    },
                }

            idle_channel._put(queue, make_message("tag-idle"))
            idle_channel._put(queue, make_message("tag-busy"))

            # Two distinct consumer names each read one entry (XREADGROUP '>'
            # never redelivers an entry already claimed by the group).
            consumed_idle = idle_channel._get(queue)
            assert consumed_idle["properties"]["delivery_tag"] == "tag-idle"
            consumed_busy = busy_channel._get(queue)
            assert consumed_busy["properties"]["delivery_tag"] == "tag-busy"

            # idle-worker acks (pending -> 0); busy-worker never acks (pending stays 1)
            cast("QoS", idle_channel.qos).ack("tag-idle")

            priority = idle_channel._get_message_priority({"properties": {}}, reverse=False)
            level = priority_to_level(priority, idle_channel.priority_steps)
            stream_key = idle_channel._stream_key(queue, level)

            def remaining_consumer_names(channel: Channel) -> set[str]:
                with channel.conn_or_acquire() as client:
                    remaining = client.xinfo_consumers(stream_key, channel.consumer_group)
                return {bytes_to_str(consumer["name"]) for consumer in remaining}

            cleaner_channel = cast("Channel", cleaner_conn.channel())
            cleaner_channel._queue_cycle = [queue]

            # idle-worker already has zero pending here (N2: the idle-threshold
            # guard, not the pending guard, must be what spares it). Running
            # cleanup under a third identity immediately, before either
            # consumer has gone idle past the threshold, must still leave
            # idle-worker in place. Deleting the `idle > idle_threshold_ms`
            # clause from the Lua script makes this assertion fail.
            cleaner_channel._cleanup_consumers()
            names_before_idle_threshold = remaining_consumer_names(cleaner_channel)
            assert "idle-worker" in names_before_idle_threshold
            assert "busy-worker" in names_before_idle_threshold

            # Let both consumers cross the idle cleanup threshold.
            time.sleep(1.0)

            # A consumer never deletes itself, even though idle-worker otherwise
            # meets every deletion criterion (idle past threshold, zero pending):
            # the script's own_consumer exclusion is evaluated for "idle-worker"
            # here, not for "cleaner-worker" below.
            idle_channel._queue_cycle = [queue]
            idle_channel._cleanup_consumers()
            names_after_self_cleanup = remaining_consumer_names(idle_channel)
            assert "idle-worker" in names_after_self_cleanup
            assert "busy-worker" in names_after_self_cleanup

            # A separate identity that is not itself a consumer of this stream
            # deletes the idle, zero-pending peer and spares the busy one.
            cleaner_channel._cleanup_consumers()
            remaining_names = remaining_consumer_names(cleaner_channel)
        finally:
            idle_conn.close()
            busy_conn.close()
            cleaner_conn.close()

        assert "idle-worker" not in remaining_names
        assert "busy-worker" in remaining_names

    def test_cleanup_script_classifies_missing_and_unexpected_errors(
        self,
        redis_client: Any,
        clear_redis: None,
    ) -> None:
        """The script's pcall around XINFO CONSUMERS treats a missing stream
        or a missing consumer group as a silent no-op ([]), but reports any
        other XINFO failure back distinctly ([-1, message]) instead of also
        treating it as a no-op.

        Audit note (N2/N6): deleting the no-such-key/NOGROUP match clause
        would make every XINFO failure look like an ordinary no-op, and
        nothing else in the suite would notice; this calls the real script
        against a real server for all three outcomes so a regression there
        goes red here.
        """
        script = redis_client.register_script(_STREAMS_CLEANUP_CONSUMERS_LUA)

        missing_key_result = script(keys=["cleanup-script-missing-stream"], args=["celery", "worker-1", 1000])
        assert missing_key_result == []

        # Stream exists, but its consumer group does not: constructed via a
        # raw XADD, bypassing _ensure_group's MKSTREAM+XGROUP CREATE pairing
        # so the group is genuinely absent (not reachable through the public
        # Channel API, which always creates both together).
        no_group_key = "cleanup-script-no-group-stream"
        redis_client.xadd(no_group_key, {"body": "x"})
        no_group_result = script(keys=[no_group_key], args=["celery", "worker-1", 1000])
        assert no_group_result == []

        # A key of the wrong type is a real, unexpected error and must not
        # be classified as a no-op.
        wrong_type_key = "cleanup-script-wrong-type"
        redis_client.set(wrong_type_key, "not-a-stream")
        wrong_type_result = script(keys=[wrong_type_key], args=["celery", "worker-1", 1000])
        assert wrong_type_result[0] == -1
        message = wrong_type_result[1]
        if isinstance(message, bytes):
            message = message.decode()
        assert "WRONGTYPE" in message


@pytest.mark.integration
class TestStreamsSizePurgeIntegration:
    """Integration tests for Channel._size, Channel._purge, and Channel._has_queue against real Redis/Valkey.

    Exercises the real XLEN/ZCARD/DEL/EXISTS replies through the real client
    (plain redis-py/valkey-py when global_keyprefix is falsy, PrefixedStrictRedis
    when truthy, via the parametrized global_keyprefix fixture), so the
    prefixing path noted in the carry-forward notes for DEL/XLEN/ZCARD/EXISTS
    actually runs. The default-priority message below lands in
    stream:{queue}:0, the LAST key _has_queue passes to EXISTS, which is
    exactly the position a first-key-only prefix bug would miss (Fix round 2,
    N1).
    """

    def test_size_and_purge_count_priorities_and_delayed_messages(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """_size sums entries across priority-level streams plus the delayed
        zset; _purge returns that same count and leaves the queue empty;
        _has_queue reports True while a level stream or the delayed zset
        exists and False once none of them do.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "size-purge-integration-queue"
        missing_queue = "size-purge-integration-queue-never-declared"

        conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={"global_keyprefix": global_keyprefix},
        )
        try:
            channel = cast("Channel", conn.channel())

            def make_message(tag: str, priority: int, eta: float | None = None) -> dict[str, Any]:
                properties: dict[str, Any] = {
                    "delivery_tag": tag,
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                    "priority": priority,
                }
                if eta is not None:
                    properties["eta"] = eta
                return {"body": '{"task": "test"}', "properties": properties}

            assert channel._has_queue(missing_queue) is False

            # A lone default-priority message lands in stream:{queue}:0, the
            # LAST key _has_queue passes to EXISTS (_stream_keys_for_queue
            # returns highest level first). Checking _has_queue here, before
            # any higher-priority stream exists, is what actually exercises
            # the first-key-only prefix bug (Fix round 2, N1): the
            # higher-priority streams added below would otherwise make the
            # first EXISTS key match regardless of whether the rest prefixed
            # correctly, masking the bug.
            channel._put(queue, make_message("tag-p0", priority=0))
            assert channel._has_queue(queue) is True

            # Two more immediate messages across two other priority streams...
            channel._put(queue, make_message("tag-p5", priority=5))
            channel._put(queue, make_message("tag-p9", priority=9))
            # ...plus one native-delayed message staged in the delayed zset
            # (DEFAULT_REQUEUE_CHECK_INTERVAL is patched to 2s in conftest, so
            # a 10s eta is comfortably past the native-delayed threshold).
            channel._put(queue, make_message("tag-delayed", priority=0, eta=time.time() + 10))

            assert channel._size(queue) == 4
            assert channel._has_queue(queue) is True

            purged = channel._purge(queue)

            assert purged == 4
            assert channel._size(queue) == 0
            assert channel._has_queue(queue) is False
        finally:
            conn.close()
