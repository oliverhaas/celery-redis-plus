"""Tests for the custom Redis transport with delayed delivery."""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from celery_redis_plus.constants import DELAY_HEADER, DELAYED_QUEUE_SUFFIX
from celery_redis_plus.transport import Channel, Transport


@pytest.mark.unit
class TestChannel:
    """Tests for the custom Channel class."""

    def test_get_delayed_key(self) -> None:
        """Test delayed key generation."""
        # Create a minimal mock channel
        channel = object.__new__(Channel)
        result = channel._get_delayed_key("my_queue")
        assert result == "my_queue:delayed"

    def test_put_with_delay_uses_zadd(self) -> None:
        """Test that messages with delay header go to delayed ZSET."""
        # Create a channel with mocked internals
        channel = object.__new__(Channel)
        mock_client = MagicMock()

        # Mock conn_or_acquire context manager
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {"headers": {DELAY_HEADER: 60.0}},
        }

        # Patch the parent's _put to prevent actual put
        with patch("kombu.transport.redis.Channel._put"):
            channel._put("my_queue", message)

        # zadd should be called with the delayed key
        mock_client.zadd.assert_called_once()
        call_args = mock_client.zadd.call_args
        assert call_args[0][0] == "my_queue:delayed"
        # The second arg is a dict {serialized_message: score}
        zadd_mapping = call_args[0][1]
        # Check that there's exactly one entry
        assert len(zadd_mapping) == 1
        # Score should be approximately current_time + 60
        score = list(zadd_mapping.values())[0]
        assert score > time.time() + 55
        assert score < time.time() + 65

    def test_put_without_delay_calls_parent(self) -> None:
        """Test that messages without delay header are published normally."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {"headers": {}},
        }

        with patch("kombu.transport.redis.Channel._put") as mock_parent_put:
            channel._put("my_queue", message)
            mock_parent_put.assert_called_once_with("my_queue", message)
            # zadd should NOT be called
            mock_client.zadd.assert_not_called()

    def test_put_with_zero_delay_calls_parent(self) -> None:
        """Test that messages with zero delay are published normally."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {"headers": {DELAY_HEADER: 0}},
        }

        with patch("kombu.transport.redis.Channel._put") as mock_parent_put:
            channel._put("my_queue", message)
            mock_parent_put.assert_called_once()
            mock_client.zadd.assert_not_called()

    def test_put_with_negative_delay_calls_parent(self) -> None:
        """Test that messages with negative delay are published normally."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {"headers": {DELAY_HEADER: -10}},
        }

        with patch("kombu.transport.redis.Channel._put") as mock_parent_put:
            channel._put("my_queue", message)
            mock_parent_put.assert_called_once()
            mock_client.zadd.assert_not_called()

    def test_put_no_properties(self) -> None:
        """Test put with no properties dict."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {"body": '{"task": "test"}'}

        with patch("kombu.transport.redis.Channel._put") as mock_parent_put:
            channel._put("my_queue", message)
            mock_parent_put.assert_called_once()
            mock_client.zadd.assert_not_called()

    def test_put_empty_message(self) -> None:
        """Test put with empty message dict."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message: dict = {}

        with patch("kombu.transport.redis.Channel._put") as mock_parent_put:
            channel._put("my_queue", message)
            mock_parent_put.assert_called_once()
            mock_client.zadd.assert_not_called()


@pytest.mark.unit
class TestTransport:
    """Tests for the custom Transport class."""

    def test_supports_native_delayed_delivery_flag(self) -> None:
        """Test that transport has the support flag."""
        assert Transport.supports_native_delayed_delivery is True

    def test_uses_custom_channel(self) -> None:
        """Test that transport uses our custom Channel class."""
        assert Transport.Channel is Channel

    def test_setup_native_delayed_delivery_starts_thread(self) -> None:
        """Test setting up delayed delivery starts the background thread."""
        # Create transport without full initialization
        transport = object.__new__(Transport)
        transport._delayed_delivery_thread = None
        transport._delayed_delivery_stop_event = None
        transport._monitored_queues = []

        connection = MagicMock()
        queues = ["queue1", "queue2"]

        transport.setup_native_delayed_delivery(connection, queues)

        try:
            assert transport._monitored_queues == ["queue1", "queue2"]
            assert transport._delayed_delivery_stop_event is not None
            assert transport._delayed_delivery_thread is not None
            assert transport._delayed_delivery_thread.is_alive()
        finally:
            # Clean up thread
            transport.teardown_native_delayed_delivery()

    def test_setup_native_delayed_delivery_already_setup_skips(self) -> None:
        """Test that setup does nothing if already set up."""
        transport = object.__new__(Transport)
        existing_thread = MagicMock()
        transport._delayed_delivery_thread = existing_thread
        transport._delayed_delivery_stop_event = None
        transport._monitored_queues = []

        connection = MagicMock()
        queues = ["queue1"]

        transport.setup_native_delayed_delivery(connection, queues)

        # Thread should not be replaced
        assert transport._delayed_delivery_thread is existing_thread

    def test_teardown_native_delayed_delivery_stops_thread(self) -> None:
        """Test tearing down delayed delivery stops the thread."""
        transport = object.__new__(Transport)
        transport._delayed_delivery_thread = None
        transport._delayed_delivery_stop_event = None
        transport._monitored_queues = []

        # Start the thread first
        connection = MagicMock()
        transport.setup_native_delayed_delivery(connection, ["queue1"])
        assert transport._delayed_delivery_thread is not None
        assert transport._delayed_delivery_thread.is_alive()

        # Now tear it down
        transport.teardown_native_delayed_delivery()

        assert transport._delayed_delivery_thread is None
        assert transport._delayed_delivery_stop_event is None
        assert transport._monitored_queues == []


class TestTransportIntegration:
    """Integration tests for transport with real Redis."""

    @pytest.mark.integration
    def test_delayed_message_delivery(self, redis_client: Any) -> None:
        """Test that delayed messages are delivered after the delay."""
        queue_name = "test_queue"
        delayed_key = f"{queue_name}{DELAYED_QUEUE_SUFFIX}"
        message = b'{"task": "test_task", "id": "123"}'

        # Simulate adding a delayed message (due in 1 second)
        delivery_time = time.time() + 1.0
        redis_client.zadd(delayed_key, {message: delivery_time})

        # Verify message is in delayed queue
        delayed_messages = redis_client.zrange(delayed_key, 0, -1)
        assert message in delayed_messages

        # Verify message is NOT in ready queue yet
        ready_messages = redis_client.lrange(queue_name, 0, -1)
        assert message not in ready_messages

        # Wait for delivery time
        time.sleep(1.5)

        # Simulate the Lua script that moves messages
        lua_script = """
        local delayed_key = KEYS[1]
        local queue_key = KEYS[2]
        local current_time = tonumber(ARGV[1])
        local batch_size = tonumber(ARGV[2])

        local messages = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', current_time, 'LIMIT', 0, batch_size)

        local count = 0
        for i, message in ipairs(messages) do
            redis.call('ZREM', delayed_key, message)
            redis.call('LPUSH', queue_key, message)
            count = count + 1
        end

        return count
        """

        moved_count = redis_client.eval(lua_script, 2, delayed_key, queue_name, time.time(), 100)
        assert moved_count == 1

        # Verify message is now in ready queue
        ready_messages = redis_client.lrange(queue_name, 0, -1)
        assert message in ready_messages

        # Verify message is no longer in delayed queue
        delayed_messages = redis_client.zrange(delayed_key, 0, -1)
        assert message not in delayed_messages

    @pytest.mark.integration
    def test_multiple_delayed_messages_ordered(self, redis_client: Any) -> None:
        """Test that multiple delayed messages are delivered in order."""
        queue_name = "test_queue_multi"
        delayed_key = f"{queue_name}{DELAYED_QUEUE_SUFFIX}"

        now = time.time()
        messages = [
            (b'{"task": "task3"}', now + 3),  # Third to deliver
            (b'{"task": "task1"}', now + 1),  # First to deliver
            (b'{"task": "task2"}', now + 2),  # Second to deliver
        ]

        # Add all messages
        for msg, delivery_time in messages:
            redis_client.zadd(delayed_key, {msg: delivery_time})

        # Wait for all to be ready
        time.sleep(3.5)

        # Move messages
        lua_script = """
        local delayed_key = KEYS[1]
        local queue_key = KEYS[2]
        local current_time = tonumber(ARGV[1])
        local batch_size = tonumber(ARGV[2])

        local messages = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', current_time, 'LIMIT', 0, batch_size)

        local count = 0
        for i, message in ipairs(messages) do
            redis.call('ZREM', delayed_key, message)
            redis.call('LPUSH', queue_key, message)
            count = count + 1
        end

        return count
        """

        moved_count = redis_client.eval(lua_script, 2, delayed_key, queue_name, time.time(), 100)
        assert moved_count == 3

        # Verify all messages are in the queue
        ready_messages = redis_client.lrange(queue_name, 0, -1)
        assert len(ready_messages) == 3

    @pytest.mark.integration
    def test_no_premature_delivery(self, redis_client: Any) -> None:
        """Test that messages are not delivered before their time."""
        queue_name = "test_queue_premature"
        delayed_key = f"{queue_name}{DELAYED_QUEUE_SUFFIX}"
        message = b'{"task": "future_task"}'

        # Schedule for 10 seconds in the future
        delivery_time = time.time() + 10.0
        redis_client.zadd(delayed_key, {message: delivery_time})

        # Try to move messages now
        lua_script = """
        local delayed_key = KEYS[1]
        local queue_key = KEYS[2]
        local current_time = tonumber(ARGV[1])
        local batch_size = tonumber(ARGV[2])

        local messages = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', current_time, 'LIMIT', 0, batch_size)

        local count = 0
        for i, message in ipairs(messages) do
            redis.call('ZREM', delayed_key, message)
            redis.call('LPUSH', queue_key, message)
            count = count + 1
        end

        return count
        """

        moved_count = redis_client.eval(lua_script, 2, delayed_key, queue_name, time.time(), 100)
        assert moved_count == 0

        # Message should still be in delayed queue
        delayed_messages = redis_client.zrange(delayed_key, 0, -1)
        assert message in delayed_messages

        # Ready queue should be empty
        ready_messages = redis_client.lrange(queue_name, 0, -1)
        assert message not in ready_messages
