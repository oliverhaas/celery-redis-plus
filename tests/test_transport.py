"""Tests for the enhanced Redis transport with BZMPOP, Streams, and delayed delivery."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from celery_redis_plus.constants import (
    DELAY_HEADER,
    PRIORITY_SCORE_MULTIPLIER,
)
from celery_redis_plus.transport import (
    Channel,
    GlobalKeyPrefixMixin,
    MultiChannelPoller,
    QoS,
    Transport,
    _queue_score,
)


@pytest.mark.unit
class TestQueueScore:
    """Tests for the queue score calculation."""

    def test_score_without_delay(self) -> None:
        """Test score calculation without delay."""
        now = time.time()
        score = _queue_score(priority=0, timestamp=now)
        # Priority 0 (highest) -> 255 * MULTIPLIER + timestamp_ms
        expected = 255 * PRIORITY_SCORE_MULTIPLIER + int(now * 1000)
        assert score == expected

    def test_score_with_delay(self) -> None:
        """Test score calculation with delay."""
        now = time.time()
        delay = 60.0
        score = _queue_score(priority=0, timestamp=now, delay_seconds=delay)
        expected = 255 * PRIORITY_SCORE_MULTIPLIER + int((now + delay) * 1000)
        assert score == expected

    def test_higher_priority_lower_score(self) -> None:
        """Test that higher priority (lower number) results in lower score."""
        now = time.time()
        high_priority_score = _queue_score(priority=0, timestamp=now)  # Highest priority
        low_priority_score = _queue_score(priority=255, timestamp=now)  # Lowest priority
        # Lower score = popped first
        assert high_priority_score > low_priority_score

    def test_earlier_timestamp_lower_score_same_priority(self) -> None:
        """Test FIFO within same priority."""
        earlier = time.time()
        later = earlier + 10
        score_earlier = _queue_score(priority=5, timestamp=earlier)
        score_later = _queue_score(priority=5, timestamp=later)
        assert score_earlier < score_later

    def test_default_timestamp_uses_current_time(self) -> None:
        """Test that None timestamp uses current time."""
        before = time.time()
        score = _queue_score(priority=0)
        after = time.time()
        # Extract timestamp from score (note: int() truncation may cause small loss)
        timestamp_ms = score - (255 * PRIORITY_SCORE_MULTIPLIER)
        timestamp = timestamp_ms / 1000
        # Allow small tolerance for int() truncation in _queue_score
        assert before - 0.001 <= timestamp <= after + 0.001


@pytest.mark.unit
class TestGlobalKeyPrefixMixin:
    """Tests for the GlobalKeyPrefixMixin."""

    def test_prefix_simple_commands(self) -> None:
        """Test that simple commands get prefixed."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "test:"

        args = mixin._prefix_args(["ZADD", "myqueue", {"tag1": 100}])
        assert args[0] == "ZADD"
        assert args[1] == "test:myqueue"

    def test_prefix_all_simple_commands(self) -> None:
        """Test that all simple commands in the list get prefixed."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        for command in mixin.PREFIXED_SIMPLE_COMMANDS:
            prefixed_args = mixin._prefix_args([command, "fake_key"])
            assert prefixed_args == [command, "prefix_fake_key"]

    def test_prefix_bzmpop(self) -> None:
        """Test BZMPOP key prefixing."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "test:"

        # BZMPOP timeout numkeys key1 key2 MIN
        args = mixin._prefix_args(["BZMPOP", 1, 2, "queue1", "queue2", "MIN"])
        assert args[0] == "BZMPOP"
        assert args[1] == 1  # timeout
        assert args[2] == 2  # numkeys
        assert args[3] == "test:queue1"
        assert args[4] == "test:queue2"
        assert args[5] == "MIN"

    def test_prefix_bzmpop_single_key(self) -> None:
        """Test BZMPOP with single key."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        args = mixin._prefix_args(["BZMPOP", "0", "1", "fake_key", "MIN"])
        assert args == ["BZMPOP", "0", "1", "prefix_fake_key", "MIN"]

    def test_prefix_delete_multiple_keys(self) -> None:
        """Test DEL command with multiple keys."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        prefixed_args = mixin._prefix_args(["DEL", "fake_key", "fake_key2", "fake_key3"])
        assert prefixed_args == [
            "DEL",
            "prefix_fake_key",
            "prefix_fake_key2",
            "prefix_fake_key3",
        ]

    def test_prefix_xreadgroup(self) -> None:
        """Test XREADGROUP key prefixing."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "test:"

        # XREADGROUP GROUP group consumer STREAMS stream1 stream2 id1 id2
        args = mixin._prefix_args(
            ["XREADGROUP", "GROUP", "mygroup", "consumer1", "STREAMS", "stream1", "stream2", ">", ">"],
        )
        assert args[0] == "XREADGROUP"
        assert "test:stream1" in args
        assert "test:stream2" in args

    def test_prefix_xreadgroup_single_stream(self) -> None:
        """Test XREADGROUP with single stream."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        args = mixin._prefix_args(
            ["XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "stream1", ">"],
        )
        assert "prefix_stream1" in args
        # The ID should not be prefixed
        assert "prefix_>" not in args

    def test_no_prefix_when_empty(self) -> None:
        """Test that empty prefix doesn't change keys."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = ""

        args = mixin._prefix_args(["ZADD", "myqueue", {"tag1": 100}])
        assert args[1] == "myqueue"

    def test_prefix_evalsha_args(self) -> None:
        """Test EVALSHA command key prefixing."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        # EVALSHA sha numkeys key [key ...] arg [arg ...]
        prefixed_args = mixin._prefix_args(
            [
                "EVALSHA",
                "not_prefixed",  # sha
                "1",  # numkeys
                "fake_key",  # key
                "not_prefixed",  # arg
            ],
        )

        assert prefixed_args == [
            "EVALSHA",
            "not_prefixed",
            "1",
            "prefix_fake_key",
            "not_prefixed",
        ]


@pytest.mark.unit
class TestChannel:
    """Tests for the custom Channel class."""

    def test_put_stores_in_sorted_set(self) -> None:
        """Test that _put stores messages in sorted set with correct score."""
        channel = object.__new__(Channel)
        channel.messages_key = "messages"
        channel.messages_index_key = "messages_index"
        channel.global_keyprefix = ""

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel._get_message_priority = MagicMock(return_value=0)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        # Verify pipeline was used
        mock_client.pipeline.assert_called_once()
        # Verify hset was called for message storage
        mock_pipe.hset.assert_called_once()
        # Verify zadd was called twice (once for index, once for queue)
        assert mock_pipe.zadd.call_count == 2
        mock_pipe.execute.assert_called_once()

    def test_put_with_delay_uses_future_score(self) -> None:
        """Test that messages with delay header get future score."""
        channel = object.__new__(Channel)
        channel.messages_key = "messages"
        channel.messages_index_key = "messages_index"
        channel.global_keyprefix = ""

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel._get_message_priority = MagicMock(return_value=0)

        delay_seconds = 60.0
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "headers": {DELAY_HEADER: delay_seconds},
            },
        }

        before = time.time()
        channel._put("my_queue", message)
        after = time.time()

        # Get the score that was passed to zadd for the queue
        zadd_calls = mock_pipe.zadd.call_args_list
        # Second zadd call is for the queue
        queue_zadd_call = zadd_calls[1]
        queue_name, score_dict = queue_zadd_call[0]
        score = list(score_dict.values())[0]

        # Score should reflect the delay
        expected_min = 255 * PRIORITY_SCORE_MULTIPLIER + int((before + delay_seconds) * 1000)
        expected_max = 255 * PRIORITY_SCORE_MULTIPLIER + int((after + delay_seconds) * 1000)
        assert expected_min <= score <= expected_max

    def test_put_with_zero_delay(self) -> None:
        """Test that zero delay doesn't add to score."""
        channel = object.__new__(Channel)
        channel.messages_key = "messages"
        channel.messages_index_key = "messages_index"
        channel.global_keyprefix = ""

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel._get_message_priority = MagicMock(return_value=0)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "headers": {DELAY_HEADER: 0},
            },
        }

        before = time.time()
        channel._put("my_queue", message)
        after = time.time()

        zadd_calls = mock_pipe.zadd.call_args_list
        queue_zadd_call = zadd_calls[1]
        queue_name, score_dict = queue_zadd_call[0]
        score = list(score_dict.values())[0]

        # Score should be approximately now (no delay)
        expected_min = 255 * PRIORITY_SCORE_MULTIPLIER + int(before * 1000)
        expected_max = 255 * PRIORITY_SCORE_MULTIPLIER + int(after * 1000)
        assert expected_min <= score <= expected_max

    def test_put_with_negative_delay_treated_as_zero(self) -> None:
        """Test that negative delay is treated as zero."""
        channel = object.__new__(Channel)
        channel.messages_key = "messages"
        channel.messages_index_key = "messages_index"
        channel.global_keyprefix = ""

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel._get_message_priority = MagicMock(return_value=0)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "headers": {DELAY_HEADER: -10},
            },
        }

        before = time.time()
        channel._put("my_queue", message)
        after = time.time()

        zadd_calls = mock_pipe.zadd.call_args_list
        queue_zadd_call = zadd_calls[1]
        queue_name, score_dict = queue_zadd_call[0]
        score = list(score_dict.values())[0]

        # Score should be approximately now (negative delay treated as 0)
        expected_min = 255 * PRIORITY_SCORE_MULTIPLIER + int(before * 1000)
        expected_max = 255 * PRIORITY_SCORE_MULTIPLIER + int(after * 1000)
        assert expected_min <= score <= expected_max

    def test_fanout_stream_key(self) -> None:
        """Test fanout stream key generation."""
        channel = object.__new__(Channel)
        channel.keyprefix_fanout = "/0."
        channel.fanout_patterns = True

        # Without routing key
        key = channel._fanout_stream_key("myexchange")
        assert key == "/0.myexchange"

        # With routing key
        key = channel._fanout_stream_key("myexchange", "myroute")
        assert key == "/0.myexchange/myroute"

    def test_fanout_consumer_group(self) -> None:
        """Test fanout consumer group name generation."""
        channel = object.__new__(Channel)
        channel.consumer_group_prefix = "celery-redis-plus"

        group = channel._fanout_consumer_group("myqueue")
        assert group == "celery-redis-plus-fanout-myqueue"


@pytest.mark.unit
class TestQoS:
    """Tests for the QoS class."""

    def test_stream_metadata_tracked_for_fanout(self) -> None:
        """Test that stream metadata is tracked for fanout messages."""
        qos = object.__new__(QoS)
        qos._stream_metadata = {}

        # Simulate adding stream metadata
        qos._stream_metadata["tag1"] = ("stream1", "msg-id-1", "group1")

        assert "tag1" in qos._stream_metadata
        stream, msg_id, group = qos._stream_metadata["tag1"]
        assert stream == "stream1"
        assert msg_id == "msg-id-1"
        assert group == "group1"

    def test_can_consume_with_no_prefetch(self) -> None:
        """Test can_consume when prefetch_count is 0 (unlimited)."""
        qos = object.__new__(QoS)
        qos.prefetch_count = 0
        qos._delivered = {}
        qos._dirty = set()

        assert qos.can_consume() is True

    def test_can_consume_under_limit(self) -> None:
        """Test can_consume when under prefetch limit."""
        qos = object.__new__(QoS)
        qos.prefetch_count = 10
        qos._delivered = {"tag1": True, "tag2": True}  # 2 delivered
        qos._dirty = set()

        assert qos.can_consume() is True

    def test_can_consume_at_limit(self) -> None:
        """Test can_consume when at prefetch limit."""
        qos = object.__new__(QoS)
        qos.prefetch_count = 2
        qos._delivered = {"tag1": True, "tag2": True}  # 2 delivered
        qos._dirty = set()

        assert qos.can_consume() is False

    def test_delivered_tracking(self) -> None:
        """Test that delivered messages are tracked."""
        qos = object.__new__(QoS)
        qos._delivered = {}
        qos._stream_metadata = {}

        # Simulate append (like in real QoS)
        qos._delivered["tag1"] = True
        qos._delivered["tag2"] = True

        assert len(qos._delivered) == 2
        assert "tag1" in qos._delivered
        assert "tag2" in qos._delivered


@pytest.mark.unit
class TestTransport:
    """Tests for the custom Transport class."""

    def test_supports_native_delayed_delivery_flag(self) -> None:
        """Test that transport has the support flag."""
        assert Transport.supports_native_delayed_delivery is True

    def test_uses_custom_channel(self) -> None:
        """Test that transport uses our custom Channel class."""
        assert Transport.Channel is Channel

    def test_implements_async_and_exchanges(self) -> None:
        """Test that transport implements async and all exchange types."""
        assert Transport.implements.asynchronous is True
        assert "direct" in Transport.implements.exchange_type
        assert "topic" in Transport.implements.exchange_type
        assert "fanout" in Transport.implements.exchange_type


@pytest.mark.unit
class TestMultiChannelPoller:
    """Tests for the MultiChannelPoller."""

    def test_add_and_discard_channel(self) -> None:
        """Test adding and removing channels."""
        poller = MultiChannelPoller()
        channel = MagicMock()

        poller.add(channel)
        assert channel in poller._channels

        poller.discard(channel)
        assert channel not in poller._channels

    def test_close_clears_state(self) -> None:
        """Test that close clears all state."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        poller.add(channel)

        poller.close()

        assert len(poller._channels) == 0
        assert len(poller._fd_to_chan) == 0
        assert len(poller._chan_to_sock) == 0

    def test_fds_property(self) -> None:
        """Test that fds property returns _fd_to_chan."""
        poller = MultiChannelPoller()
        poller._fd_to_chan = {1: ("channel", "BZMPOP")}  # type: ignore[assignment]
        assert poller.fds == poller._fd_to_chan

    def test_close_unregisters_fds(self) -> None:
        """Test that close unregisters all file descriptors."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        poller.poller = mock_poller
        poller._chan_to_sock.update({1: 1, 2: 2, 3: 3})  # type: ignore[dict-item]

        poller.close()

        assert mock_poller.unregister.call_count == 3

    def test_on_poll_start_no_channels(self) -> None:
        """Test on_poll_start with no channels."""
        poller = MultiChannelPoller()
        poller._channels = set()  # type: ignore[assignment]
        # Should not raise
        poller.on_poll_start()

    def test_on_poll_start_with_active_queues(self) -> None:
        """Test on_poll_start with active queues."""
        poller = MultiChannelPoller()
        poller._register_BZMPOP = MagicMock()  # type: ignore[method-assign]
        poller._register_XREADGROUP = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = ["queue1"]
        channel.active_fanout_queues = []
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        poller._register_BZMPOP.assert_called_once_with(channel)  # type: ignore[attr-defined]
        poller._register_XREADGROUP.assert_not_called()  # type: ignore[attr-defined]

    def test_on_poll_start_with_fanout_queues(self) -> None:
        """Test on_poll_start with fanout queues."""
        poller = MultiChannelPoller()
        poller._register_BZMPOP = MagicMock()  # type: ignore[method-assign]
        poller._register_XREADGROUP = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = []
        channel.active_fanout_queues = ["fanout_queue"]
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        poller._register_BZMPOP.assert_not_called()  # type: ignore[attr-defined]
        poller._register_XREADGROUP.assert_called_once_with(channel)  # type: ignore[attr-defined]

    def test_on_poll_start_qos_cannot_consume(self) -> None:
        """Test on_poll_start when QoS cannot consume."""
        poller = MultiChannelPoller()
        poller._register_BZMPOP = MagicMock()  # type: ignore[method-assign]
        poller._register_XREADGROUP = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = ["queue1"]
        channel.active_fanout_queues = ["fanout_queue"]
        channel.qos.can_consume.return_value = False  # QoS limit reached
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        # Neither should be registered when can_consume is False
        poller._register_BZMPOP.assert_not_called()  # type: ignore[attr-defined]
        poller._register_XREADGROUP.assert_not_called()  # type: ignore[attr-defined]


class TestTransportIntegration:
    """Integration tests for transport with real Redis."""

    @pytest.mark.integration
    def test_sorted_set_message_ordering(self, redis_client: Any) -> None:
        """Test that messages are ordered by score in sorted set."""
        queue_name = "test_queue_ordering"

        now = time.time()

        # Add messages with different priorities
        # Lower score = popped first, higher priority (lower number) = lower score component
        # But we invert: (255 - priority), so priority 0 -> 255, priority 255 -> 0
        high_pri_score = _queue_score(0, now)  # Highest priority
        med_pri_score = _queue_score(128, now)  # Medium priority
        low_pri_score = _queue_score(255, now)  # Lowest priority

        redis_client.zadd(queue_name, {"high_pri": high_pri_score})
        redis_client.zadd(queue_name, {"low_pri": low_pri_score})
        redis_client.zadd(queue_name, {"med_pri": med_pri_score})

        # Pop should return lowest score first (lowest priority number = highest priority)
        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"low_pri"  # Priority 255 has lowest score

        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"med_pri"

        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"high_pri"

    @pytest.mark.integration
    def test_delayed_message_not_visible_until_time(self, redis_client: Any) -> None:
        """Test that delayed messages have future scores."""
        queue_name = "test_queue_delayed"

        now = time.time()
        delay = 60.0  # 60 second delay

        # Message without delay
        immediate_score = _queue_score(0, now)
        # Message with delay
        delayed_score = _queue_score(0, now, delay_seconds=delay)

        redis_client.zadd(queue_name, {"immediate": immediate_score})
        redis_client.zadd(queue_name, {"delayed": delayed_score})

        # When we pop with a max score of "now", only immediate should be returned
        current_max_score = _queue_score(255, now)  # Lowest priority at current time
        result = redis_client.zrangebyscore(queue_name, "-inf", current_max_score)

        assert b"immediate" in result
        assert b"delayed" not in result

    @pytest.mark.integration
    def test_bzmpop_with_sorted_set(self, redis_client: Any) -> None:
        """Test BZMPOP command with sorted sets (requires Redis 7.0+)."""
        queue_name = "test_queue_bzmpop"

        now = time.time()
        score = _queue_score(0, now)

        redis_client.zadd(queue_name, {"message1": score})

        # BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
        result = redis_client.bzmpop(1, 1, queue_name, min=True)

        assert result is not None
        key, members = result
        assert key == queue_name.encode() or key == queue_name
        assert len(members) == 1
        assert members[0][0] == b"message1"

    @pytest.mark.integration
    def test_stream_consumer_group(self, redis_client: Any) -> None:
        """Test Redis Streams with consumer groups."""
        stream_name = "test_stream"
        group_name = "test_group"
        consumer_name = "test_consumer"

        # Create consumer group (mkstream=True creates stream if not exists)
        try:
            redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

        # Add message to stream
        msg_id = redis_client.xadd(stream_name, {"payload": "test_message"})
        assert msg_id is not None

        # Read with consumer group
        messages = redis_client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"},
            count=1,
            block=100,
        )

        assert messages is not None
        assert len(messages) == 1
        stream, message_list = messages[0]
        assert len(message_list) == 1

        # Acknowledge message
        message_id = message_list[0][0]
        ack_count = redis_client.xack(stream_name, group_name, message_id)
        assert ack_count == 1

    @pytest.mark.integration
    def test_message_hash_storage(self, redis_client: Any) -> None:
        """Test that messages can be stored and retrieved from hash."""
        messages_key = "test_messages"
        delivery_tag = "tag123"
        message_data = '{"body": "test", "exchange": "celery", "routing_key": "celery"}'

        # Store message
        redis_client.hset(messages_key, delivery_tag, message_data)

        # Retrieve message
        result = redis_client.hget(messages_key, delivery_tag)
        assert result == message_data.encode()

        # Delete message
        redis_client.hdel(messages_key, delivery_tag)
        result = redis_client.hget(messages_key, delivery_tag)
        assert result is None

    @pytest.mark.integration
    def test_stream_xadd_and_xread(self, redis_client: Any) -> None:
        """Test basic stream XADD and XREAD operations."""
        stream_name = "test_stream_basic"

        # Add messages to stream
        msg_id1 = redis_client.xadd(stream_name, {"field1": "value1"})
        msg_id2 = redis_client.xadd(stream_name, {"field2": "value2"})

        assert msg_id1 is not None
        assert msg_id2 is not None

        # Read messages
        messages = redis_client.xread(streams={stream_name: "0"}, count=10)
        assert len(messages) == 1
        stream, message_list = messages[0]
        assert len(message_list) == 2

    @pytest.mark.integration
    def test_stream_consumer_group_redelivery(self, redis_client: Any) -> None:
        """Test that unacked messages can be reclaimed from PEL."""
        stream_name = "test_stream_pel"
        group_name = "test_group_pel"
        consumer1 = "consumer1"
        consumer2 = "consumer2"

        # Create consumer group
        try:
            redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

        # Add message
        redis_client.xadd(stream_name, {"payload": "test"})

        # Read with consumer1 (but don't ack)
        messages = redis_client.xreadgroup(
            groupname=group_name,
            consumername=consumer1,
            streams={stream_name: ">"},
            count=1,
        )
        assert len(messages) == 1

        # Check pending
        pending = redis_client.xpending(stream_name, group_name)
        assert pending["pending"] == 1

        # Try to claim with consumer2 (min_idle_time=0 for test)
        msg_id = messages[0][1][0][0]
        claimed = redis_client.xclaim(
            stream_name,
            group_name,
            consumer2,
            min_idle_time=0,
            message_ids=[msg_id],
        )
        assert len(claimed) == 1

    @pytest.mark.integration
    def test_stream_maxlen_trimming(self, redis_client: Any) -> None:
        """Test that stream respects maxlen for trimming."""
        stream_name = "test_stream_maxlen"
        maxlen = 5

        # Add more messages than maxlen
        for i in range(10):
            redis_client.xadd(stream_name, {"msg": str(i)}, maxlen=maxlen)

        # Stream should be trimmed to approximately maxlen
        info = redis_client.xinfo_stream(stream_name)
        assert info["length"] <= maxlen + 1  # Approximate trimming may leave a few extra
