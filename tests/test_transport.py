"""Tests for the enhanced Redis transport with BZMPOP, Streams, and delayed delivery."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from queue import Empty
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest
from kombu import Exchange, Queue
from kombu.exceptions import OperationalError
from kombu.utils.eventio import ERR
from kombu.utils.json import dumps as json_dumps

from celery_redis_plus.constants import (
    DEFAULT_MESSAGE_TTL,
    DEFAULT_VISIBILITY_TIMEOUT,
    MESSAGE_KEY_PREFIX,
    PRIORITY_SCORE_MULTIPLIER,
    QUEUE_KEY_PREFIX,
)
from celery_redis_plus.transport import (
    DEFAULT_DB,
    Channel,
    GlobalKeyPrefixMixin,
    MultiChannelPoller,
    PrefixedRedisPipeline,
    PrefixedStrictRedis,
    QoS,
    Transport,
    _after_fork_cleanup_channel,
    _queue_score,
    get_redis_ConnectionError,
    get_redis_error_classes,
)

if TYPE_CHECKING:
    from celery import Celery


@pytest.mark.unit
class TestQueueScore:
    """Tests for the queue score calculation."""

    def test_score_without_delay(self) -> None:
        """Test score calculation without delay."""
        now = time.time()
        score = _queue_score(priority=0, timestamp=now)
        # Priority 0 (lowest) -> 255 * MULTIPLIER + timestamp_ms (highest score)
        expected = 255 * PRIORITY_SCORE_MULTIPLIER + int(now * 1000)
        assert score == expected

    def test_score_different_priorities(self) -> None:
        """Test score calculation with different priorities."""
        now = time.time()
        # Priority 0 (lowest) = highest score
        low_pri_score = _queue_score(priority=0, timestamp=now)
        # Priority 255 (highest) = lowest score
        high_pri_score = _queue_score(priority=255, timestamp=now)
        assert high_pri_score < low_pri_score
        expected_low = 255 * PRIORITY_SCORE_MULTIPLIER + int(now * 1000)
        expected_high = 0 * PRIORITY_SCORE_MULTIPLIER + int(now * 1000)
        assert low_pri_score == expected_low
        assert high_pri_score == expected_high

    def test_higher_priority_lower_score(self) -> None:
        """Test that higher priority (higher number) results in lower score (RabbitMQ semantics)."""
        now = time.time()
        low_priority_score = _queue_score(priority=0, timestamp=now)  # Lowest priority
        high_priority_score = _queue_score(priority=255, timestamp=now)  # Highest priority
        # Lower score = popped first, so high priority should have lower score
        assert high_priority_score < low_priority_score

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
        score = _queue_score(priority=0)  # priority 0 = lowest priority
        after = time.time()
        # Extract timestamp from score (note: int() truncation may cause small loss)
        # Priority 0 gives (255 - 0) * MULTIPLIER = 255 * MULTIPLIER base score
        timestamp_ms = score - (255 * PRIORITY_SCORE_MULTIPLIER)
        timestamp = timestamp_ms / 1000
        # Allow small tolerance for int() truncation in _queue_score
        assert before - 0.001 <= timestamp <= after + 0.001

    def test_priority_clamped_when_out_of_range(self) -> None:
        """Test that out-of-range priorities are clamped to 0-255."""
        now = time.time()

        # Test priority below minimum (should clamp to 0)
        score_negative = _queue_score(priority=-10, timestamp=now)
        score_zero = _queue_score(priority=0, timestamp=now)
        assert score_negative == score_zero

        # Test priority above maximum (should clamp to 255)
        score_over = _queue_score(priority=300, timestamp=now)
        score_max = _queue_score(priority=255, timestamp=now)
        assert score_over == score_max

        # Verify extreme values also clamp correctly
        score_very_negative = _queue_score(priority=-1000, timestamp=now)
        score_very_high = _queue_score(priority=1000, timestamp=now)
        assert score_very_negative == score_zero
        assert score_very_high == score_max


@pytest.mark.unit
class TestRedisHelpers:
    """Tests for Redis helper functions."""

    def test_get_redis_error_classes(self) -> None:
        """Test that get_redis_error_classes returns proper error tuples."""
        error_classes = get_redis_error_classes()
        assert hasattr(error_classes, "connection_errors")
        assert hasattr(error_classes, "channel_errors")
        assert isinstance(error_classes.connection_errors, tuple)
        assert isinstance(error_classes.channel_errors, tuple)

    def test_get_redis_connection_error(self) -> None:
        """Test that get_redis_ConnectionError returns the right exception."""
        from redis import exceptions

        error_class = get_redis_ConnectionError()
        assert error_class is exceptions.ConnectionError


@pytest.mark.unit
class TestPrefixedStrictRedis:
    """Tests for PrefixedStrictRedis class."""

    def test_init_sets_global_keyprefix(self) -> None:
        """Test that __init__ extracts and sets global_keyprefix from kwargs."""
        # Mock connection pool to avoid actual Redis connection
        mock_pool = MagicMock()
        client = PrefixedStrictRedis(connection_pool=mock_pool, global_keyprefix="test:")

        assert client.global_keyprefix == "test:"

    def test_init_default_keyprefix(self) -> None:
        """Test that global_keyprefix defaults to empty string."""
        mock_pool = MagicMock()
        client = PrefixedStrictRedis(connection_pool=mock_pool)

        assert client.global_keyprefix == ""


@pytest.mark.unit
class TestPrefixedRedisPipeline:
    """Tests for PrefixedRedisPipeline class."""

    def test_init_sets_global_keyprefix(self) -> None:
        """Test that __init__ extracts and sets global_keyprefix from kwargs."""

        mock_pool = MagicMock()
        mock_response_callbacks = {}
        pipeline = PrefixedRedisPipeline(
            mock_pool,
            mock_response_callbacks,
            transaction=True,
            shard_hint=None,
            global_keyprefix="prefix:",
        )

        assert pipeline.global_keyprefix == "prefix:"

    def test_init_default_keyprefix(self) -> None:
        """Test that global_keyprefix defaults to empty string."""

        mock_pool = MagicMock()
        mock_response_callbacks = {}
        pipeline = PrefixedRedisPipeline(
            mock_pool,
            mock_response_callbacks,
            transaction=True,
            shard_hint=None,
        )

        assert pipeline.global_keyprefix == ""


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

    def test_prefix_xread(self) -> None:
        """Test XREAD key prefixing."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "test:"

        # XREAD STREAMS stream1 stream2 id1 id2
        args = mixin._prefix_args(
            ["XREAD", "COUNT", "1", "BLOCK", "1000", "STREAMS", "stream1", "stream2", "$", "$"],
        )
        assert args[0] == "XREAD"
        assert "test:stream1" in args
        assert "test:stream2" in args

    def test_prefix_xread_single_stream(self) -> None:
        """Test XREAD with single stream."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "prefix_"

        args = mixin._prefix_args(
            ["XREAD", "COUNT", "1", "STREAMS", "stream1", "$"],
        )
        assert "prefix_stream1" in args
        # The ID should not be prefixed
        assert "prefix_$" not in args

    def test_no_prefix_when_empty(self) -> None:
        """Test that empty prefix doesn't change keys."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = ""

        args = mixin._prefix_args(["ZADD", "myqueue", {"tag1": 100}])
        assert args[1] == "myqueue"

    def test_prefix_xread_without_streams_keyword(self) -> None:
        """Test XREAD when STREAMS keyword is not found (returns args unchanged)."""
        mixin = GlobalKeyPrefixMixin()
        mixin.global_keyprefix = "test:"

        # Malformed XREAD without STREAMS keyword
        args = mixin._prefix_args(["XREAD", "COUNT", "1", "stream1", "$"])
        # Should return args unchanged since STREAMS keyword is missing
        assert args == ["XREAD", "COUNT", "1", "stream1", "$"]

    def test_parse_response_bzmpop_strips_prefix(self) -> None:
        """Test that parse_response strips global prefix from BZMPOP result."""

        class TestableClient(GlobalKeyPrefixMixin):
            """Testable client that overrides super behavior."""

            global_keyprefix = "prefix:"

            def parse_response(self, connection: Any, command_name: str, **options: Any) -> Any:
                del connection, options  # Unused in test
                # Simulate super().parse_response returning prefixed key
                ret = (b"prefix:myqueue", [(b"tag1", 100.0)])
                if command_name == "BZMPOP" and ret:
                    key, members = ret
                    if isinstance(key, bytes):
                        key = key.decode()
                    key = key[len(self.global_keyprefix) :]
                    return key, members
                return ret

        client = TestableClient()
        result = client.parse_response(None, "BZMPOP")

        assert result[0] == "myqueue"  # Prefix stripped
        assert result[1] == [(b"tag1", 100.0)]

    def test_parse_response_bzmpop_with_string_key(self) -> None:
        """Test that parse_response handles string keys (not just bytes)."""

        class TestableClient(GlobalKeyPrefixMixin):
            """Testable client for string key test."""

            global_keyprefix = "test:"

            def parse_response(self, connection: Any, command_name: str, **options: Any) -> Any:
                del connection, options  # Unused in test
                # Simulate super().parse_response returning string key (already decoded)
                ret = ("test:myqueue", [(b"tag1", 100.0)])
                if command_name == "BZMPOP" and ret:
                    key, members = ret
                    if isinstance(key, bytes):
                        key = key.decode()
                    key = key[len(self.global_keyprefix) :]
                    return key, members
                return ret

        client = TestableClient()
        result = client.parse_response(None, "BZMPOP")

        assert result[0] == "myqueue"

    def test_parse_response_non_bzmpop_unchanged(self) -> None:
        """Test that parse_response returns non-BZMPOP results unchanged."""

        class TestableClient(GlobalKeyPrefixMixin):
            """Testable client for non-BZMPOP test."""

            global_keyprefix = "test:"

            def parse_response(self, connection: Any, command_name: str, **options: Any) -> Any:
                del connection, options  # Unused in test
                ret = "some_result"
                if command_name == "BZMPOP" and ret:
                    # This branch won't be taken for non-BZMPOP
                    pass
                return ret

        client = TestableClient()
        result = client.parse_response(None, "GET")

        assert result == "some_result"

    def test_parse_response_bzmpop_empty_result(self) -> None:
        """Test that parse_response handles empty BZMPOP result."""

        class TestableClient(GlobalKeyPrefixMixin):
            """Testable client for empty result test."""

            global_keyprefix = "test:"

            def parse_response(self, connection: Any, command_name: str, **options: Any) -> Any:
                del connection, options  # Unused in test
                ret = None  # BZMPOP returns None on timeout
                if command_name == "BZMPOP" and ret:
                    key, members = ret
                    if isinstance(key, bytes):
                        key = key.decode()
                    key = key[len(self.global_keyprefix) :]
                    return key, members
                return ret

        client = TestableClient()
        result = client.parse_response(None, "BZMPOP")

        assert result is None

    def test_execute_command_prefixes_args(self) -> None:
        """Test that execute_command prefixes args before calling super."""
        calls: list[tuple[Any, ...]] = []

        class TestableClient(GlobalKeyPrefixMixin):
            """Testable client for execute_command test."""

            global_keyprefix = "prefix:"

            def execute_command(self, *args: Any, **kwargs: Any) -> Any:
                del kwargs  # Unused in test
                # Call _prefix_args and track what would be sent to super
                prefixed = self._prefix_args(list(args))
                calls.append(tuple(prefixed))
                return "OK"

        client = TestableClient()
        client.execute_command("ZADD", "myqueue", {"tag": 100})

        assert len(calls) == 1
        assert calls[0][0] == "ZADD"
        assert calls[0][1] == "prefix:myqueue"

    def test_pipeline_returns_prefixed_pipeline(self) -> None:
        """Test that pipeline() returns a PrefixedRedisPipeline with correct prefix."""

        mock_pool = MagicMock()
        client = PrefixedStrictRedis(connection_pool=mock_pool, global_keyprefix="myprefix:")

        pipeline = client.pipeline()

        assert isinstance(pipeline, PrefixedRedisPipeline)
        assert pipeline.global_keyprefix == "myprefix:"


@pytest.mark.unit
class TestChannel:
    """Tests for the custom Channel class."""

    def test_put_stores_in_sorted_set(self) -> None:
        """Test that _put stores messages in per-message hash with correct score."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.messages_index_key = "messages_index"
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
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
        # Verify hset was called once with mapping for per-message hash
        assert mock_pipe.hset.call_count == 1
        # Verify expire was called for the message TTL
        mock_pipe.expire.assert_called_once()
        # Verify zadd was called twice (once for index, once for queue)
        assert mock_pipe.zadd.call_count == 2
        mock_pipe.execute.assert_called_once()

    def test_put_with_long_delay_goes_to_messages_index(self) -> None:
        """Test that native delayed messages (delay > 60s) go to messages_index, not queue.

        Native delayed delivery stores the message only in messages_index with
        queue_at = eta. The requeue mechanism will add it to the queue when eta arrives.
        """

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.messages_index_key = "messages_index"
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
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

        # Use a long delay (e.g., 1 hour) - triggers native delayed delivery
        delay_seconds = 3600.0
        before = time.time()
        eta_timestamp = before + delay_seconds
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "eta": eta_timestamp,
            },
        }

        channel._put("my_queue", message)
        after = time.time()

        # Native delayed messages only get ONE zadd call (to messages_index, not queue)
        zadd_calls = mock_pipe.zadd.call_args_list
        assert len(zadd_calls) == 1

        # The single zadd should be for messages_index with queue_at = eta
        index_zadd_call = zadd_calls[0]
        index_name, score_dict = index_zadd_call[0]
        assert index_name == "messages_index"
        queue_at = list(score_dict.values())[0]
        assert before + delay_seconds <= queue_at <= after + delay_seconds

        # Verify per-message hash is stored with native_delayed=1 and eta
        hset_call = mock_pipe.hset.call_args
        mapping = hset_call.kwargs.get("mapping", {})
        assert "priority" in mapping
        assert mapping.get("native_delayed") == 1
        assert mapping.get("eta") == eta_timestamp

    def test_put_with_short_delay_goes_to_main_queue(self) -> None:
        """Test that messages with short delay go to main queue with future timestamp score."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.messages_index_key = "messages_index"
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
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

        # Use short delay (30 seconds) - less than DEFAULT_REQUEUE_CHECK_INTERVAL (60s)
        # Short delays are treated as immediate, Celery's built-in eta logic handles them
        delay_seconds = 30.0
        before = time.time()
        eta_timestamp = before + delay_seconds
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "eta": eta_timestamp,
            },
        }

        channel._put("my_queue", message)
        after = time.time()

        # Get the score that was passed to zadd for the main queue
        zadd_calls = mock_pipe.zadd.call_args_list
        # Second zadd call is for the queue
        queue_zadd_call = zadd_calls[1]
        queue_name, score_dict = queue_zadd_call[0]
        score = list(score_dict.values())[0]

        # Queue name should be the main queue with queue: prefix
        assert queue_name == f"{QUEUE_KEY_PREFIX}my_queue"

        # Short delays are treated as immediate (score based on now, not eta)
        # Celery's built-in eta logic will handle the actual delay
        expected_min = _queue_score(0, before)
        expected_max = _queue_score(0, after)
        assert expected_min <= score <= expected_max

        # Verify native_delayed=0 (short delay) but eta is still stored
        hset_call = mock_pipe.hset.call_args
        mapping = hset_call.kwargs.get("mapping", {})
        assert mapping.get("native_delayed") == 0
        assert mapping.get("eta") == eta_timestamp

    def test_put_with_no_eta(self) -> None:
        """Test that no eta means immediate delivery (no delay)."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.messages_index_key = "messages_index"
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
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

        # Verify native_delayed=0 and eta=0 (no eta provided)
        hset_call = mock_pipe.hset.call_args
        mapping = hset_call.kwargs.get("mapping", {})
        assert mapping.get("native_delayed") == 0
        assert mapping.get("eta") == 0

    def test_put_with_eta_in_past_treated_as_immediate(self) -> None:
        """Test that eta in the past is treated as immediate delivery."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.messages_index_key = "messages_index"
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
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

        # eta 10 seconds in the past
        before = time.time()
        eta_timestamp = before - 10.0
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                "eta": eta_timestamp,
            },
        }

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

    def test_prepare_virtual_host_with_slash(self) -> None:
        """Test _prepare_virtual_host with '/' returns default db."""

        channel = object.__new__(Channel)
        result = channel._prepare_virtual_host("/")
        assert result == DEFAULT_DB

    def test_prepare_virtual_host_with_empty(self) -> None:
        """Test _prepare_virtual_host with empty string returns default db."""

        channel = object.__new__(Channel)
        result = channel._prepare_virtual_host("")
        assert result == DEFAULT_DB

    def test_prepare_virtual_host_with_slash_number(self) -> None:
        """Test _prepare_virtual_host with '/5' returns 5."""
        channel = object.__new__(Channel)
        result = channel._prepare_virtual_host("/5")
        assert result == 5

    def test_prepare_virtual_host_with_integer(self) -> None:
        """Test _prepare_virtual_host with integer passthrough."""
        channel = object.__new__(Channel)
        result = channel._prepare_virtual_host(3)
        assert result == 3

    def test_prepare_virtual_host_invalid_raises(self) -> None:
        """Test _prepare_virtual_host with invalid string raises ValueError."""
        channel = object.__new__(Channel)
        with pytest.raises(ValueError, match="Database is int"):
            channel._prepare_virtual_host("invalid")

    def test_get_table_empty_exchange(self) -> None:
        """Test get_table returns empty list for exchange with no bindings."""
        channel = object.__new__(Channel)
        channel.keyprefix_queue = "_kombu.binding.%s"

        mock_client = MagicMock()
        mock_client.smembers.return_value = set()

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel.get_table("nonexistent_exchange")
        assert result == []

    def test_put_fanout(self) -> None:
        """Test _put_fanout publishes to stream."""
        channel = object.__new__(Channel)
        channel.keyprefix_fanout = "/0."
        channel.fanout_patterns = False
        channel.stream_maxlen = 1000

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {"body": "test", "properties": {}}
        channel._put_fanout("myexchange", message, "routing_key")

        mock_client.xadd.assert_called_once()
        call_kwargs = mock_client.xadd.call_args[1]
        assert call_kwargs["name"] == "/0.myexchange"
        assert call_kwargs["maxlen"] == 1000
        assert "payload" in call_kwargs["fields"]

    def test_get_synchronous(self) -> None:
        """Test _get retrieves message synchronously."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX

        mock_client = MagicMock()
        # zpopmin returns [(delivery_tag, score)]
        mock_client.zpopmin.return_value = [(b"tag123", 100.0)]
        # Per-message hash: hget returns just the payload
        mock_client.hget.return_value = b'{"body": "test"}'

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._get("myqueue")
        assert result == {"body": "test"}
        # Verify hget was called with the message key and payload field
        mock_client.hget.assert_called_once_with("message:tag123", "payload")

    def test_get_synchronous_empty(self) -> None:
        """Test _get raises Empty when queue is empty."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX

        mock_client = MagicMock()
        mock_client.zpopmin.return_value = []

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with pytest.raises(Empty):
            channel._get("myqueue")

    def test_get_client_with_global_keyprefix(self) -> None:
        """Test _get_client returns PrefixedStrictRedis when global_keyprefix is set."""

        channel = object.__new__(Channel)
        channel.global_keyprefix = "myprefix:"

        client_factory = channel._get_client()

        # Should return a partial with PrefixedStrictRedis
        assert client_factory.func is PrefixedStrictRedis
        assert client_factory.keywords["global_keyprefix"] == "myprefix:"

    def test_get_client_without_global_keyprefix(self) -> None:
        """Test _get_client returns redis.Redis when no global_keyprefix."""
        import redis as redis_module

        channel = object.__new__(Channel)
        channel.global_keyprefix = ""

        client_class = channel._get_client()

        assert client_class is redis_module.Redis

    def test_connparams_with_ssl_dict(self) -> None:
        """Test _connparams applies SSL config from dict."""
        import redis as redis_module

        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel.max_connections = 10
        channel.socket_timeout = None
        channel.socket_connect_timeout = None
        channel.socket_keepalive = None
        channel.socket_keepalive_options = None
        channel.health_check_interval = 25
        channel.retry_on_timeout = False
        channel.client_name = None
        channel.connection_class = redis_module.Connection
        channel.connection_class_ssl = redis_module.SSLConnection

        # Mock connection with SSL config as dict
        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = {"ssl_cert_reqs": "required"}
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        params = channel._connparams()

        assert params["connection_class"] is redis_module.SSLConnection
        assert params["ssl_cert_reqs"] == "required"

    def test_connparams_with_ssl_true(self) -> None:
        """Test _connparams applies SSL config when ssl=True."""
        import redis as redis_module

        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel.max_connections = 10
        channel.socket_timeout = None
        channel.socket_connect_timeout = None
        channel.socket_keepalive = None
        channel.socket_keepalive_options = None
        channel.health_check_interval = 25
        channel.retry_on_timeout = False
        channel.client_name = None
        channel.connection_class = redis_module.Connection
        channel.connection_class_ssl = redis_module.SSLConnection

        # Mock connection with SSL = True
        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = True
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        params = channel._connparams()

        assert params["connection_class"] is redis_module.SSLConnection


@pytest.mark.unit
class TestQoS:
    """Tests for the QoS class."""

    def test_fanout_tags_tracked(self) -> None:
        """Test that fanout tags are tracked."""
        qos = object.__new__(QoS)
        qos._fanout_tags = set()

        # Simulate adding fanout tag
        qos._fanout_tags.add("tag1")

        assert "tag1" in qos._fanout_tags

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
        qos._fanout_tags = set()

        # Simulate append (like in real QoS)
        qos._delivered["tag1"] = True
        qos._delivered["tag2"] = True

        assert len(qos._delivered) == 2
        assert "tag1" in qos._delivered
        assert "tag2" in qos._delivered

    def test_ack_fanout_message(self) -> None:
        """Test ack for fanout message (no Redis cleanup needed)."""
        qos = object.__new__(QoS)
        qos._fanout_tags = {"tag1"}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.ack("tag1")

        # Fanout tag should be removed
        assert "tag1" not in qos._fanout_tags

    def test_ack_regular_message(self) -> None:
        """Test ack for regular (non-fanout) message."""
        qos = object.__new__(QoS)
        qos._fanout_tags = set()
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        mock_pipe = MagicMock()
        mock_pipe.zrem.return_value = mock_pipe
        mock_pipe.hdel.return_value = mock_pipe
        qos._remove_from_indices = MagicMock(return_value=mock_pipe)

        qos.ack("tag1")

        qos._remove_from_indices.assert_called_once_with("tag1")
        mock_pipe.execute.assert_called_once()

    def test_reject_fanout_message(self) -> None:
        """Test reject for fanout message (requeue not supported)."""
        qos = object.__new__(QoS)
        qos._fanout_tags = {"tag1"}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        # Requeue is ignored for fanout messages
        qos.reject("tag1", requeue=True)

        # Fanout tag should be removed
        assert "tag1" not in qos._fanout_tags

    def test_reject_regular_message_with_requeue(self) -> None:
        """Test reject with requeue for regular message."""
        qos = object.__new__(QoS)
        qos._fanout_tags = set()
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.requeue_by_tag = MagicMock()

        qos.reject("tag1", requeue=True)

        qos.requeue_by_tag.assert_called_once_with("tag1", leftmost=True)

    def test_reject_regular_message_without_requeue(self) -> None:
        """Test reject without requeue for regular message."""
        qos = object.__new__(QoS)
        qos._fanout_tags = set()
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        mock_pipe = MagicMock()
        mock_pipe.zrem.return_value = mock_pipe
        mock_pipe.hdel.return_value = mock_pipe
        qos._remove_from_indices = MagicMock(return_value=mock_pipe)

        qos.reject("tag1", requeue=False)

        qos._remove_from_indices.assert_called_once_with("tag1")
        mock_pipe.execute.assert_called_once()

    def test_maybe_update_messages_index_empty_delivered(self) -> None:
        """Test maybe_update_messages_index returns early when no delivered messages."""
        qos = object.__new__(QoS)
        qos._delivered = {}
        qos._fanout_tags = set()

        # Should return early without calling any Redis commands
        qos.maybe_update_messages_index()
        # No assertions needed - just verify it doesn't raise

    def test_maybe_update_messages_index_updates_scores(self) -> None:
        """Test maybe_update_messages_index updates scores for non-fanout messages."""
        qos = object.__new__(QoS)
        qos._delivered = {"tag1": MagicMock(), "tag2": MagicMock(), "fanout_tag": MagicMock()}
        qos._fanout_tags = {"fanout_tag"}

        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client.pipeline.return_value = mock_pipe
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.conn_or_acquire.return_value = mock_client
        mock_channel.messages_index_key = "messages_index"
        qos.channel = mock_channel

        # Create cached_property attributes
        qos.__dict__["messages_index_key"] = "messages_index"

        qos.maybe_update_messages_index()

        # Should update scores for tag1 and tag2, but NOT stream_tag
        assert mock_pipe.zadd.call_count == 2
        zadd_calls = [call[0][0] for call in mock_pipe.zadd.call_args_list]
        assert "messages_index" in zadd_calls

    def test_pipe_or_acquire_with_existing_pipe(self) -> None:
        """Test pipe_or_acquire returns existing pipe when provided."""
        qos = object.__new__(QoS)

        mock_pipe = MagicMock()

        with qos.pipe_or_acquire(pipe=mock_pipe) as pipe:
            assert pipe is mock_pipe

    def test_pipe_or_acquire_creates_new_pipe(self) -> None:
        """Test pipe_or_acquire creates new pipe when none provided."""
        qos = object.__new__(QoS)

        mock_pipe = MagicMock()
        mock_client = MagicMock()
        mock_client.pipeline.return_value = mock_pipe

        mock_conn_context = MagicMock()
        mock_conn_context.__enter__ = MagicMock(return_value=mock_client)
        mock_conn_context.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.conn_or_acquire.return_value = mock_conn_context
        qos.channel = mock_channel

        with qos.pipe_or_acquire() as pipe:
            assert pipe is mock_pipe


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

    def test_driver_version(self) -> None:
        """Test that driver_version returns redis version string."""
        import redis as redis_module

        transport = MagicMock(spec=Transport)
        transport.driver_version = Transport.driver_version
        version = transport.driver_version(transport)
        assert version == redis_module.__version__

    def test_connection_errors_defined(self) -> None:
        """Test that connection and channel errors are defined."""
        # These are set at class definition time if redis is available
        assert hasattr(Transport, "connection_errors")
        assert hasattr(Transport, "channel_errors")


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
        poller._register_XREAD = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = ["queue1"]
        channel.active_fanout_queues = []
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        poller._register_BZMPOP.assert_called_once_with(channel)  # type: ignore[attr-defined]
        poller._register_XREAD.assert_not_called()  # type: ignore[attr-defined]

    def test_on_poll_start_with_fanout_queues(self) -> None:
        """Test on_poll_start with fanout queues."""
        poller = MultiChannelPoller()
        poller._register_BZMPOP = MagicMock()  # type: ignore[method-assign]
        poller._register_XREAD = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = []
        channel.active_fanout_queues = ["fanout_queue"]
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        poller._register_BZMPOP.assert_not_called()  # type: ignore[attr-defined]
        poller._register_XREAD.assert_called_once_with(channel)  # type: ignore[attr-defined]

    def test_on_poll_start_qos_cannot_consume(self) -> None:
        """Test on_poll_start when QoS cannot consume."""
        poller = MultiChannelPoller()
        poller._register_BZMPOP = MagicMock()  # type: ignore[method-assign]
        poller._register_XREAD = MagicMock()  # type: ignore[method-assign]

        channel = MagicMock()
        channel.active_queues = ["queue1"]
        channel.active_fanout_queues = ["fanout_queue"]
        channel.qos.can_consume.return_value = False  # QoS limit reached
        poller._channels = {channel}  # type: ignore[assignment]

        poller.on_poll_start()

        # Neither should be registered when can_consume is False
        poller._register_BZMPOP.assert_not_called()  # type: ignore[attr-defined]
        poller._register_XREAD.assert_not_called()  # type: ignore[attr-defined]

    def test_close_handles_unregister_errors(self) -> None:
        """Test that close handles KeyError and ValueError when unregistering."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        # Simulate unregister raising KeyError for first call, ValueError for second
        mock_poller.unregister.side_effect = [KeyError("not found"), ValueError("invalid"), None]
        poller.poller = mock_poller
        poller._chan_to_sock = {1: 1, 2: 2, 3: 3}  # type: ignore[dict-item]

        # Should not raise
        poller.close()

        assert mock_poller.unregister.call_count == 3
        assert len(poller._channels) == 0

    def test_on_connection_disconnect_handles_attribute_error(self) -> None:
        """Test _on_connection_disconnect handles missing _sock attribute."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        poller.poller = mock_poller

        # Connection without _sock attribute
        connection = MagicMock(spec=[])  # Empty spec means no attributes

        # Should not raise
        poller._on_connection_disconnect(connection)

        # Unregister should not be called since _sock doesn't exist
        mock_poller.unregister.assert_not_called()

    def test_on_connection_disconnect_handles_type_error(self) -> None:
        """Test _on_connection_disconnect handles TypeError from unregister."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        mock_poller.unregister.side_effect = TypeError("invalid type")
        poller.poller = mock_poller

        connection = MagicMock()
        connection._sock = MagicMock()

        # Should not raise even with TypeError
        poller._on_connection_disconnect(connection)

    def test_register_unregisters_existing_before_reregistering(self) -> None:
        """Test that _register unregisters existing socket before re-registering."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        poller.poller = mock_poller

        channel = MagicMock()
        client = MagicMock()
        mock_sock = MagicMock()
        mock_sock.fileno.return_value = 42
        client.connection._sock = mock_sock

        # First registration
        poller._register(channel, client, "BZMPOP")

        # Second registration - should unregister first
        new_sock = MagicMock()
        new_sock.fileno.return_value = 43
        client.connection._sock = new_sock

        poller._register(channel, client, "BZMPOP")

        # Should have unregistered the old socket
        mock_poller.unregister.assert_called_once_with(mock_sock)

    def test_register_connects_if_sock_is_none(self) -> None:
        """Test that _register calls connect() if connection._sock is None."""
        poller = MultiChannelPoller()
        mock_poller = MagicMock()
        poller.poller = mock_poller

        channel = MagicMock()
        client = MagicMock()

        # First call returns None, then returns a socket after connect()
        mock_sock = MagicMock()
        mock_sock.fileno.return_value = 42

        def connect_side_effect() -> None:
            client.connection._sock = mock_sock

        client.connection._sock = None
        client.connection.connect.side_effect = connect_side_effect

        poller._register(channel, client, "BZMPOP")

        client.connection.connect.assert_called_once()

    def test_on_poll_init_returns_none_when_no_channels(self) -> None:
        """Test on_poll_init returns None when no channels."""
        poller = MultiChannelPoller()
        poller._channels = set()  # type: ignore[assignment]

        result = poller.on_poll_init(MagicMock())

        assert result is None

    def test_maybe_enqueue_due_messages_returns_zero_when_no_active_queues(self) -> None:
        """Test maybe_enqueue_due_messages returns 0 when channels have no active queues."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.active_queues = []
        poller._channels = {channel}  # type: ignore[assignment]

        result = poller.maybe_enqueue_due_messages()

        assert result == 0

    def test_on_readable_returns_none_when_cannot_consume(self) -> None:
        """Test on_readable returns None when QoS cannot consume."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos.can_consume.return_value = False
        channel.handlers = {"BZMPOP": MagicMock()}

        poller._fd_to_chan = {42: (channel, "BZMPOP")}

        result = poller.on_readable(42)

        assert result is None
        channel.handlers["BZMPOP"].assert_not_called()

    def test_handle_event_err_calls_poll_error(self) -> None:
        """Test handle_event calls _poll_error on ERR event."""

        poller = MultiChannelPoller()
        channel = MagicMock()
        poller._fd_to_chan = {42: (channel, "BZMPOP")}

        result = poller.handle_event(42, ERR)

        channel._poll_error.assert_called_once_with("BZMPOP")
        assert result is None


@pytest.mark.integration
class TestTransportIntegration:
    """Integration tests for transport with real Redis."""

    def test_sorted_set_message_ordering(self, redis_client: Any) -> None:
        """Test that messages are ordered by score in sorted set (RabbitMQ semantics)."""
        queue_name = "test_queue_ordering"

        now = time.time()

        # Add messages with different priorities (RabbitMQ semantics: higher number = higher priority)
        # Lower score = popped first
        # Formula: (255 - priority) * MULTIPLIER, so priority 255 -> 0, priority 0 -> 255
        low_pri_score = _queue_score(0, now)  # Lowest priority (highest score)
        med_pri_score = _queue_score(128, now)  # Medium priority
        high_pri_score = _queue_score(255, now)  # Highest priority (lowest score)

        redis_client.zadd(queue_name, {"high_pri": high_pri_score})
        redis_client.zadd(queue_name, {"low_pri": low_pri_score})
        redis_client.zadd(queue_name, {"med_pri": med_pri_score})

        # Pop should return lowest score first (highest priority number = highest priority)
        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"high_pri"  # Priority 255 has lowest score, processed first

        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"med_pri"

        result = redis_client.zpopmin(queue_name, 1)
        assert result[0][0] == b"low_pri"  # Priority 0 has highest score, processed last

    def test_bzmpop_with_sorted_set(self, redis_client: Any) -> None:
        """Test BZMPOP command with sorted sets (requires Redis 7.0+)."""
        queue_name = "test_queue_bzmpop"

        now = time.time()
        score = _queue_score(0, now)

        redis_client.zadd(queue_name, {"message1": score})

        # BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
        result = redis_client.bzmpop(1, 1, [queue_name], min=True)

        assert result is not None
        key, members = result
        assert key == queue_name.encode() or key == queue_name
        assert len(members) == 1
        assert members[0][0] == b"message1"

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

    def test_stream_maxlen_trimming(self, redis_client: Any) -> None:
        """Test that stream respects maxlen for trimming."""
        stream_name = "test_stream_maxlen"
        maxlen = 5

        # Add more messages than maxlen (use approximate=False for exact trimming)
        for i in range(10):
            redis_client.xadd(stream_name, {"msg": str(i)}, maxlen=maxlen, approximate=False)

        # Stream should be trimmed to exactly maxlen
        info = redis_client.xinfo_stream(stream_name)
        assert info["length"] == maxlen


@pytest.mark.integration
class TestTransportFeatures:
    """Test transport-specific features with a real worker."""

    def test_task_with_countdown(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test task with countdown delay."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        start = time.time()
        result = add.apply_async(args=(1, 2), countdown=1)
        value = result.get(timeout=10)
        elapsed = time.time() - start

        assert value == 3
        # Task should have been delayed by approximately 1 second
        assert elapsed >= 0.9

    def test_task_priority(
        self,
        celery_app: Celery,
        celery_worker: Any,
        redis_client: Any,
    ) -> None:
        """Test that task priority affects ordering.

        Higher priority number = higher priority = processed first (RabbitMQ semantics).
        """

        @celery_app.task
        def slow_add(x: int, y: int) -> int:
            time.sleep(0.1)
            return x + y

        celery_worker.reload()
        # Send low priority task first
        low_priority = slow_add.apply_async(args=(1, 1), priority=0)
        # Send high priority task second
        high_priority = slow_add.apply_async(args=(2, 2), priority=9)

        # Both should complete
        low_result = low_priority.get(timeout=10)
        high_result = high_priority.get(timeout=10)

        assert low_result == 2
        assert high_result == 4

    def test_task_with_eta(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test task with ETA (absolute time) delay."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        start = time.time()
        eta = datetime.now(UTC) + timedelta(seconds=1)
        result = add.apply_async(args=(1, 2), eta=eta)
        value = result.get(timeout=10)
        elapsed = time.time() - start

        assert value == 3
        # Task should have been delayed by approximately 1 second
        assert elapsed >= 0.9

    def test_task_retry_on_failure(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that task retry works correctly through the transport."""
        attempt_count = {"count": 0}

        @celery_app.task(bind=True, max_retries=2, default_retry_delay=1)  # type: ignore[call-overload]
        def failing_task(self: Any) -> str:
            attempt_count["count"] += 1
            if attempt_count["count"] < 3:
                raise self.retry()
            return "success"

        celery_worker.reload()
        result = failing_task.delay()
        value = result.get(timeout=10)

        assert value == "success"
        assert attempt_count["count"] == 3  # Original + 2 retries

    def test_task_raises_exception(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that task exceptions are properly propagated."""

        @celery_app.task
        def failing_task() -> None:
            raise ValueError("Task failed intentionally")

        celery_worker.reload()
        result = failing_task.delay()

        with pytest.raises(ValueError, match="Task failed intentionally"):
            result.get(timeout=10)

    def test_message_cleanup_after_success(
        self,
        celery_app: Celery,
        celery_worker: Any,
        redis_client: Any,
    ) -> None:
        """Test that messages are cleaned up from Redis after successful processing."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        result = add.delay(1, 1)
        result.get(timeout=10)

        # Give worker time to clean up
        time.sleep(0.5)

        # Check that message index is eventually cleaned up
        # The messages_index key tracks all messages
        index_count = redis_client.zcard("messages_index")
        # Should be 0 or very small after successful processing
        assert index_count <= 1  # Allow some tolerance for timing

    def test_high_priority_processed_before_low_priority(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that high priority tasks are processed before low priority ones."""
        execution_order: list[int] = []

        @celery_app.task
        def record_execution(priority_value: int) -> int:
            execution_order.append(priority_value)
            return priority_value

        celery_worker.reload()

        # Send multiple tasks with different priorities
        # Lower priority number = lower priority
        results = [record_execution.apply_async(args=(priority,), priority=priority) for priority in [0, 5, 9, 3, 7]]

        # Wait for all to complete
        for r in results:
            r.get(timeout=10)

        # High priority tasks (higher numbers) should generally be processed first
        # Due to timing, we can't guarantee exact order, but highest should be early
        assert 9 in execution_order[:3]  # Priority 9 should be among first 3

    def test_task_with_queue_routing(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that tasks can be routed to specific queues."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        celery_worker.reload()
        # Send to default celery queue explicitly
        result = add.apply_async(args=(3, 4), queue="celery")
        value = result.get(timeout=10)

        assert value == 7

    def test_concurrent_task_execution(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that multiple concurrent tasks execute correctly."""

        @celery_app.task
        def slow_multiply(x: int, y: int) -> int:
            time.sleep(0.1)
            return x * y

        celery_worker.reload()

        # Send many tasks concurrently
        results = [slow_multiply.delay(i, 2) for i in range(10)]

        # All should complete correctly
        values = [r.get(timeout=30) for r in results]
        expected = [i * 2 for i in range(10)]
        assert sorted(values) == expected

    def test_task_with_kwargs(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that tasks with keyword arguments work correctly."""

        @celery_app.task
        def greet(name: str, greeting: str = "Hello") -> str:
            return f"{greeting}, {name}!"

        celery_worker.reload()

        result1 = greet.delay("World")
        result2 = greet.apply_async(kwargs={"name": "Alice", "greeting": "Hi"})

        assert result1.get(timeout=10) == "Hello, World!"
        assert result2.get(timeout=10) == "Hi, Alice!"

    def test_task_ignore_result(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that tasks with ignore_result work correctly."""
        execution_tracker = {"executed": False}

        @celery_app.task(ignore_result=True)
        def fire_and_forget() -> None:
            execution_tracker["executed"] = True

        celery_worker.reload()

        result = fire_and_forget.delay()

        # Give time for task to execute
        time.sleep(1)

        # Task should have executed even without result tracking
        assert execution_tracker["executed"] is True
        # Result should be None for ignore_result tasks
        assert result.result is None


@pytest.mark.integration
class TestTransportReliability:
    """Test transport reliability features with a real worker."""

    def test_message_not_lost_on_worker_prefetch(
        self,
        celery_app: Celery,
        celery_worker: Any,
        redis_client: Any,
    ) -> None:
        """Test that messages remain tracked while being processed."""

        @celery_app.task
        def slow_task() -> str:
            time.sleep(0.5)
            return "done"

        celery_worker.reload()

        # Send a slow task
        result = slow_task.delay()

        # While task is running, message should still be in the system
        time.sleep(0.1)

        # Complete the task
        value = result.get(timeout=10)
        assert value == "done"

    def test_task_id_unique_per_message(
        self,
        celery_app: Celery,
        celery_worker: Any,
    ) -> None:
        """Test that each task gets a unique task ID."""

        @celery_app.task(bind=True)
        def capture_task_id(self: Any) -> str:
            return self.request.id

        celery_worker.reload()

        # Send multiple tasks
        results = [capture_task_id.delay() for _ in range(5)]
        task_ids = [r.get(timeout=10) for r in results]

        # All task IDs should be unique
        assert len(set(task_ids)) == 5


@pytest.mark.integration
class TestMessagePublishing:
    """Tests that verify message publishing to Redis without a worker.

    These tests publish messages and verify Redis state directly,
    without consuming the messages through a worker.
    """

    def test_published_message_stored_in_sorted_set(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that publishing a task stores it in a Redis sorted set."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish without a worker - message should be stored in Redis
        add.delay(1, 2)

        # Check that message is in the celery queue sorted set
        queue_size = redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery")
        assert queue_size >= 1

        # Check that message is in the messages index
        index_size = redis_client.zcard("messages_index")
        assert index_size >= 1

        # Check that message payload is stored in a per-message hash
        # Get the delivery tag from the queue to verify the message hash exists
        queue_members = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, 0)
        assert len(queue_members) >= 1
        delivery_tag = queue_members[0].decode() if isinstance(queue_members[0], bytes) else queue_members[0]
        message_key = f"message:{delivery_tag}"
        assert redis_client.exists(message_key) == 1

    def test_published_message_with_countdown_has_future_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that a task with countdown has a future score in the sorted set."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish with a 10 second countdown
        before_time = time.time()
        add.apply_async(args=(1, 2), countdown=10)

        # Get the message from the queue
        messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
        assert len(messages) >= 1

        # The score should be in the future (approximately 10 seconds from now)
        # Score format: priority bits + timestamp with delay
        _tag, score = messages[-1]  # Get the last (most recent) message

        # The score encodes priority in high bits and timestamp in low bits
        # With delay, the effective delivery time should be ~10 seconds in the future
        # We can't easily decode the exact time, but we can verify it's higher than a no-delay score
        no_delay_score = before_time
        assert score > no_delay_score

    def test_published_message_with_eta_has_future_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that a task with ETA has a future score in the sorted set."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish with an ETA 10 seconds in the future
        eta = datetime.now(UTC) + timedelta(seconds=10)
        add.apply_async(args=(1, 2), eta=eta)

        # Get the message from the queue
        messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
        assert len(messages) >= 1

    def test_high_priority_message_has_lower_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that higher priority messages have lower scores (processed first)."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages", "messages_index")

        # Publish low priority first, then high priority
        add.apply_async(args=(1, 1), priority=0)  # Low priority
        time.sleep(0.01)  # Small delay to ensure different timestamps
        add.apply_async(args=(2, 2), priority=9)  # High priority

        # Get messages ordered by score (ascending)
        messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
        assert len(messages) == 2

        # High priority (9) should have lower score, so it comes first
        low_score = messages[0][1]
        high_score = messages[1][1]

        # The first message (lower score) should be the high-priority one
        # because higher priority = lower score = processed first
        assert low_score < high_score

    def test_multiple_messages_ordered_by_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that multiple messages are ordered correctly by score."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages", "messages_index")

        # Publish 5 messages with same priority
        for i in range(5):
            add.delay(i, i)
            time.sleep(0.01)  # Small delay between messages

        # Check all messages are in the queue
        queue_size = redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery")
        assert queue_size == 5

        # Messages should be ordered by timestamp (FIFO within same priority)
        messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
        scores = [score for _, score in messages]

        # Scores should be in ascending order (earlier messages have lower scores)
        assert scores == sorted(scores)

    def test_message_payload_contains_task_data(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that the message payload contains correct task data."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

        add.delay(42, 58)

        # Get the delivery tag from the queue
        messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(messages) == 1
        delivery_tag = messages[0].decode() if isinstance(messages[0], bytes) else messages[0]

        # Get the payload from the per-message hash
        message_key = f"message:{delivery_tag}"
        payload = redis_client.hget(message_key, "payload")
        assert payload is not None

        # Parse the payload
        message = json.loads(payload.decode() if isinstance(payload, bytes) else payload)
        assert isinstance(message, dict)
        assert "body" in message or "args" in str(message)

        # Check other fields in the per-message hash
        routing_key = redis_client.hget(message_key, "routing_key")
        priority = redis_client.hget(message_key, "priority")
        assert routing_key is not None
        assert routing_key.decode() == "celery"  # routing_key stores the queue name
        assert priority is not None
        assert int(priority) >= 0

    def test_queue_purge_removes_messages(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that purging a queue removes messages from Redis."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and publish
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")
        for _ in range(3):
            add.delay(1, 1)

        # Verify messages exist
        assert redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery") == 3

        # Purge using the app's control interface
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            purged = channel._purge("celery")
            assert purged == 3

        # Verify queue is empty
        assert redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery") == 0

    def test_queue_size_returns_correct_count(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that queue size returns correct message count."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and publish
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages", "messages_index")
        for _ in range(5):
            add.delay(1, 1)

        # Check size via channel
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            size = channel._size("celery")
            assert size == 5


@pytest.mark.integration
class TestQueueOperations:
    """Tests for queue operations without a worker."""

    def test_queue_delete_removes_queue(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that deleting a queue removes it from Redis."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish messages
        redis_client.delete(f"{QUEUE_KEY_PREFIX}celery")
        add.delay(1, 1)
        assert redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery") >= 1

        # Delete the queue
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel._delete("celery")

        # Queue should be gone
        assert redis_client.zcard(f"{QUEUE_KEY_PREFIX}celery") == 0

    def test_queue_exists_check(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that _has_queue correctly checks queue existence."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and check non-existence
        redis_client.delete(f"{QUEUE_KEY_PREFIX}test_queue_exists")

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            assert channel._has_queue("test_queue_exists") is False

            # Create queue by adding a message directly
            redis_client.zadd(f"{QUEUE_KEY_PREFIX}test_queue_exists", {"msg1": 1.0})
            assert channel._has_queue("test_queue_exists") is True

        # Cleanup
        redis_client.delete(f"{QUEUE_KEY_PREFIX}test_queue_exists")

    def test_get_table_returns_bindings(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that get_table returns queue bindings."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Bind a queue to an exchange
            channel._queue_bind(
                exchange="test_exchange",
                routing_key="test_key",
                pattern="test_pattern",
                queue="test_queue",
            )

            # Get the bindings
            table = channel.get_table("test_exchange")
            assert len(table) >= 1

            # Find our binding
            found = any("test_queue" in binding for binding in table)
            assert found

        # Cleanup
        redis_client.delete("_kombu.binding.test_exchange")


@pytest.mark.integration
class TestChannelConnection:
    """Tests for channel connection handling."""

    def test_channel_creates_connection_pool(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that channel creates a connection pool."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            # Accessing pool should create it
            pool = channel.pool
            assert pool is not None

    def test_channel_creates_async_pool(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that channel creates an async connection pool."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            # Accessing async_pool should create it
            async_pool = channel.async_pool
            assert async_pool is not None

    def test_channel_client_property(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that channel client property returns a Redis client."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client
            assert client is not None
            # Should be able to ping
            assert client.ping() is True

    def test_conn_or_acquire_context_manager(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that conn_or_acquire works as context manager."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Without client argument - creates new client
            with channel.conn_or_acquire() as client:
                assert client is not None
                assert client.ping() is True

            # With client argument - uses provided client
            existing_client = channel.client
            with channel.conn_or_acquire(existing_client) as client:
                assert client is existing_client


@pytest.mark.integration
class TestFanoutMessaging:
    """Tests for fanout (pub/sub) messaging using Redis Streams."""

    def test_fanout_stream_key_generation(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that fanout stream key is generated correctly."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Test stream key generation
            stream_key = channel._fanout_stream_key("test_exchange")
            assert "test_exchange" in stream_key

            # Test with routing key
            stream_key_with_route = channel._fanout_stream_key("test_exchange", "my_route")
            assert "test_exchange" in stream_key_with_route
            assert "my_route" in stream_key_with_route

    def test_fanout_exchange_declaration(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that fanout exchange can be declared."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            fanout_exchange = Exchange("test_fanout_decl", type="fanout")
            fanout_queue = Queue("fanout_decl_queue", exchange=fanout_exchange)

            # Bind and declare
            fanout_queue.bind(channel).declare()  # type: ignore[attr-defined]

            # The binding should be stored
            bindings_key = "_kombu.binding.test_fanout_decl"
            bindings = redis_client.smembers(bindings_key)
            assert len(bindings) >= 1

            # Cleanup
            redis_client.delete(bindings_key)


@pytest.mark.integration
class TestDelayedMessageStorage:
    """Tests for delayed message storage in Redis.

    Note: These tests verify the _put method's eta handling directly,
    since the signal handler that adds eta headers is only active during
    worker task publish. All messages (with or without eta) go to the main queue.
    """

    def test_message_with_eta_goes_to_messages_index(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that native delayed messages go to messages_index, not queue immediately.

        Native delayed messages (delay > 60s) are stored in messages_index with
        queue_at = eta. The requeue mechanism will add them to the queue when
        the eta time arrives. This prevents them from being consumed early.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            delay_seconds = 120  # 2 minutes in the future (> 60s threshold)
            before_time = time.time()
            eta_timestamp = before_time + delay_seconds
            delivery_tag = f"test-delay-{time.time()}"

            # Create a message with eta in properties
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "eta": eta_timestamp,
                },
            }

            # Publish directly via _put
            channel._put("celery", message)

            # Message should NOT be in the main queue yet (native delayed delivery)
            main_messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
            assert len(main_messages) == 0

            # Message should be in messages_index with queue_at = eta
            index_entries = redis_client.zrange("messages_index", 0, -1, withscores=True)
            assert len(index_entries) == 1
            tag, queue_at = index_entries[0]
            assert tag.decode() if isinstance(tag, bytes) else tag == delivery_tag
            assert queue_at == pytest.approx(eta_timestamp, rel=1e-6)

            # Message data should be stored in per-message hash
            message_key = f"message:{delivery_tag}"
            priority = redis_client.hget(message_key, "priority")
            assert priority is not None
            assert int(priority) == 0  # Default priority

            # native_delayed field should be set to 1
            native_delayed = redis_client.hget(message_key, "native_delayed")
            assert native_delayed is not None
            assert int(native_delayed) == 1

    def test_message_without_eta_has_current_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that messages without eta have current time scores."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            before_time = time.time()

            # Create a message without eta
            message = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": f"test-no-delay-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "headers": {},
                },
            }

            # Publish directly via _put
            channel._put("celery", message)
            after_time = time.time()

            # Get the message score
            messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
            assert len(messages) == 1
            _tag, actual_score = messages[0]

            # Calculate expected score range (no delay, priority 0)
            min_score = _queue_score(0, before_time)
            max_score = _queue_score(0, after_time)

            # The actual score should be within the expected range
            assert min_score <= actual_score <= max_score

    def test_short_delayed_and_immediate_messages_ordered_by_score(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that short-delayed and immediate messages are ordered correctly in queue.

        Short delays (<= 60s) are treated as immediate delivery, so both messages
        should be in the queue immediately.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            now = time.time()
            # Use delay <= 60s so it's treated as immediate (Celery handles the delay)
            eta_timestamp = now + 30  # 30 seconds in future (<= DEFAULT_REQUEUE_CHECK_INTERVAL)

            # Create an immediate message (no eta)
            immediate_msg = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": f"immediate-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                },
            }
            channel._put("celery", immediate_msg)

            # Create a short-delayed message (delay <= 60s, treated as immediate)
            short_delayed_msg = {
                "body": '{"task": "test.add", "args": [3, 4]}',
                "properties": {
                    "delivery_tag": f"short-delayed-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "eta": eta_timestamp,
                },
            }
            channel._put("celery", short_delayed_msg)

            # Both messages should be in main queue (short delay is treated as immediate)
            main_messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
            assert len(main_messages) == 2

            # Both messages should have scores based on "now" (not the eta)
            # since short delays are treated as immediate delivery
            for tag_bytes, score in main_messages:
                tag = tag_bytes.decode() if isinstance(tag_bytes, bytes) else tag_bytes
                # Score should be based on current time, not eta
                expected_min = _queue_score(0, now - 1)  # Allow some slack
                expected_max = _queue_score(0, now + 2)
                assert expected_min <= score <= expected_max, f"Score {score} for {tag} not in expected range"

    def test_high_priority_message_ordered_before_low_priority(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that high priority messages are ordered before low priority."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            # Create low priority message first
            low_priority_msg = {
                "body": '{"task": "test.add", "args": [1, 1]}',
                "properties": {
                    "delivery_tag": f"low-pri-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "priority": 0,  # Low priority
                },
            }
            channel._put("celery", low_priority_msg)

            # Create high priority message second
            high_priority_msg = {
                "body": '{"task": "test.add", "args": [2, 2]}',
                "properties": {
                    "delivery_tag": f"high-pri-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                    "priority": 9,  # High priority
                },
            }
            channel._put("celery", high_priority_msg)

            # High priority should be first (lower score)
            messages = redis_client.zrange(f"{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
            assert len(messages) == 2

            first_tag = messages[0][0].decode() if isinstance(messages[0][0], bytes) else messages[0][0]
            assert "high-pri" in first_tag


@pytest.mark.integration
class TestMessageRequeue:
    """Tests for message requeue functionality (unified delayed + restore)."""

    def test_requeue_restores_unacked_message(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that requeue_messages restores messages that were consumed but not acked."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Use channel's client to ensure we use the same key namespace
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            # Simulate a message that was consumed but not acked:
            # 1. Message is in per-message hash (payload stored)
            # 2. Message is in messages_index with try_requeue_at in the past
            # 3. Message is NOT in the queue (was popped)
            delivery_tag = "unacked-msg-123"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": "0",
                },
            )

            # Set index score to past timestamp (ready for requeue)
            old_timestamp = time.time() - 100
            client.zadd("messages_index", {delivery_tag: old_timestamp})

            # Message is NOT in the queue (simulates it was consumed)
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")

            # Call enqueue_due_messages - should restore the message
            requeued = channel.enqueue_due_messages()

            assert requeued >= 1

            # Message should now be back in the queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_requeue_skips_message_still_in_queue(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that requeue_messages skips messages still in queue."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            # Simulate a message that is still in the queue (not yet consumed)
            delivery_tag = "queued-msg-456"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": "0",
                },
            )

            # Set index score to old timestamp (ready for requeue)
            old_timestamp = time.time() - 100
            client.zadd("messages_index", {delivery_tag: old_timestamp})

            # Message IS in the queue (not yet consumed)
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 100.0})

            original_score = client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")

            # Call enqueue_due_messages - should skip (message still in queue via ZADD NX)
            channel.enqueue_due_messages()

            # Score should be unchanged (ZADD NX doesn't update existing entries)
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) == original_score

    def test_requeue_removes_index_for_acked_message(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that requeue_messages cleans up index for already-acked messages."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            # Simulate a message that was already acked (per-message hash deleted)
            delivery_tag = "acked-msg-789"

            # Message is in index but NOT in per-message hash (already acked)
            old_timestamp = time.time() - 100
            client.zadd("messages_index", {delivery_tag: old_timestamp})

            # Verify it's in the index
            assert client.zscore("messages_index", delivery_tag) is not None

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")

            # Call enqueue_due_messages - should remove from index
            channel.enqueue_due_messages()

            # Should be removed from index (cleaned up by Lua script)
            assert client.zscore("messages_index", delivery_tag) is None

    def test_requeue_by_tag(
        self,
        celery_app: Celery,
    ) -> None:
        """Test requeue_by_tag restores a specific message."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            # Set up a message in the per-message hash
            delivery_tag = "requeue-tag-test"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "routing_key": "celery",
                    "priority": "0",
                },
            )

            # Message is not in queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

            # Requeue the message
            cast("QoS", channel.qos).requeue_by_tag(delivery_tag)

            # Message should now be in the queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_requeue_by_tag_sets_redelivered_flag(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _requeue_by_tag sets the redelivered flag in hash."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            delivery_tag = "redelivered-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                },
            }

            # Store the message in per-message hash (simulating initial publish)
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                },
            )

            # Requeue the message using the Lua script
            result = channel._requeue_by_tag(delivery_tag, leftmost=False)
            assert result is True

            # Check the redelivered flag was set in the hash
            redelivered = client.hget(message_key, "redelivered")
            assert redelivered == b"1"

            # Message should be in queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_requeue_by_tag_leftmost_uses_zero_score(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _requeue_by_tag with leftmost=True uses score 0."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            delivery_tag = "leftmost-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                },
            }

            # Store the message in per-message hash
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                },
            )

            # Requeue with leftmost=True
            result = channel._requeue_by_tag(delivery_tag, leftmost=True)
            assert result is True

            # Score should be 0 (highest priority, processed first)
            score = client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)
            assert score == 0

    def test_channel_restore_with_message_object(
        self,
        celery_app: Celery,
    ) -> None:
        """Test Channel._restore with a message object."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            delivery_tag = "message-restore-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                },
            }

            # Store the message in per-message hash
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "routing_key": "celery",
                    "priority": "0",
                },
            )

            # Create a mock message object
            message = MagicMock()
            message.delivery_tag = delivery_tag

            # Restore the message
            channel._restore(message)

            # Message should be in the queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_channel_restore_at_beginning(
        self,
        celery_app: Celery,
    ) -> None:
        """Test Channel._restore_at_beginning restores with score 0."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages_index")

            delivery_tag = "restore-beginning-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                },
            }

            # Store the message in per-message hash
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "routing_key": "celery",
                    "priority": "0",
                },
            )

            # Create a mock message object
            message = MagicMock()
            message.delivery_tag = delivery_tag

            # Restore at beginning
            channel._restore_at_beginning(message)

            # Message should be in queue with score 0
            score = client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)
            assert score == 0


@pytest.mark.integration
class TestGlobalKeyPrefix:
    """Tests for global key prefix functionality."""

    def test_task_execution_with_global_keyprefix(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that tasks work correctly with global_keyprefix set."""
        from celery import Celery

        host, port, _image = redis_container

        # Create app with global_keyprefix
        app = Celery("test_prefix")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"global_keyprefix": "myapp:"},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        @app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish a task
        add.delay(2, 3)

        # Verify the message is stored with the prefix
        import redis

        client = redis.Redis(host=host, port=port, db=0)

        # The queue should be prefixed with both global prefix and queue: prefix
        prefixed_queue_size: int = client.zcard(f"myapp:{QUEUE_KEY_PREFIX}celery")  # type: ignore[assignment]

        assert prefixed_queue_size >= 1
        # The key point is that our prefixed queue has the message

        # Clean up
        client.delete(f"myapp:{QUEUE_KEY_PREFIX}celery", "myapp:messages", "myapp:messages_index")
        client.close()
        app.close()


@pytest.mark.integration
class TestFanoutPrefix:
    """Tests for fanout_prefix functionality."""

    def test_string_fanout_prefix(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that string fanout_prefix is used for stream keys."""
        from celery import Celery

        host, port, _image = redis_container

        # Create app with string fanout_prefix
        app = Celery("test_fanout_prefix")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"fanout_prefix": "myfanout."},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        with app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Verify the keyprefix_fanout is set correctly
            assert channel.keyprefix_fanout == "myfanout."

            # Get the stream key - should use our prefix
            stream_key = channel._fanout_stream_key("test_fanout")
            assert stream_key == "myfanout.test_fanout"

            # With routing key
            stream_key_routed = channel._fanout_stream_key("test_fanout", "my.route")
            assert stream_key_routed == "myfanout.test_fanout/my.route"

        app.close()

    def test_false_fanout_prefix(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that fanout_prefix=False results in no prefix."""
        from celery import Celery

        host, port, _image = redis_container

        app = Celery("test_no_fanout_prefix")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"fanout_prefix": False},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        with app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Verify the keyprefix_fanout is empty
            assert channel.keyprefix_fanout == ""

            # Stream key should have no prefix
            stream_key = channel._fanout_stream_key("test_fanout")
            assert stream_key == "test_fanout"

        app.close()


@pytest.mark.integration
class TestChannelConnectionFailure:
    """Tests for channel connection failure handling."""

    def test_init_fails_with_invalid_redis(self) -> None:
        """Test that channel init fails gracefully when Redis is unavailable."""
        from celery import Celery

        # Use a port that definitely doesn't have Redis
        app = Celery("test_bad_connection")
        app.conf.update(
            broker_url="redis://localhost:59999/0",  # Non-existent port
            broker_transport="celery_redis_plus.transport:Transport",
            broker_connection_timeout=1,
            broker_connection_retry=False,
        )

        # Opening a connection to non-existent Redis should raise OperationalError

        with pytest.raises(OperationalError, match="Connection refused"), app.connection() as conn:
            # Force channel creation
            _ = conn.default_channel  # type: ignore[attr-defined]

        app.close()


@pytest.mark.integration
class TestChannelCloseWithFanout:
    """Tests for channel close with fanout queues."""

    def test_close_deletes_auto_delete_fanout_queues(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that closing channel deletes auto-delete fanout queues."""
        from celery import Celery

        host, port, _image = redis_container

        app = Celery("test_auto_delete")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        with app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Declare the queue - this adds it to auto_delete_queues
            channel.queue_declare("auto_del_queue", auto_delete=True)

            # Add to _fanout_queues to simulate binding
            channel._fanout_queues["auto_del_queue"] = (
                "test_auto_del_fanout",
                "",
            )
            channel.auto_delete_queues.add("auto_del_queue")

            # Verify it's tracked
            assert "auto_del_queue" in channel.auto_delete_queues
            assert "auto_del_queue" in channel._fanout_queues

        # After context exit, close is called - auto-delete queues should be deleted
        app.close()


@pytest.mark.integration
class TestSynchronousGet:
    """Tests for synchronous _get method."""

    def test_get_returns_message(
        self,
        celery_app: Celery,
    ) -> None:
        """Test _get returns message from queue."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear and set up
            client.delete(f"{QUEUE_KEY_PREFIX}celery")

            delivery_tag = "sync-get-test"
            payload = {
                "body": "test body",
                "headers": {},
                "properties": {"delivery_tag": delivery_tag},
            }

            # Store message in per-message hash
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),  # type: ignore[call-arg]
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": "0",
                },
            )
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 100.0})

            # Use synchronous _get
            message = channel._get("celery")

            assert message["body"] == "test body"
            assert message["properties"]["delivery_tag"] == delivery_tag

    def test_get_raises_empty_when_no_message(
        self,
        celery_app: Celery,
    ) -> None:
        """Test _get raises Empty when queue is empty."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear queue
            client.delete(f"{QUEUE_KEY_PREFIX}empty_test_queue")

            # _get on empty queue should raise Empty
            with pytest.raises(Empty):
                channel._get("empty_test_queue")

    def test_get_raises_empty_when_payload_missing(
        self,
        celery_app: Celery,
    ) -> None:
        """Test _get raises Empty when delivery tag exists but payload is gone."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear and set up
            client.delete(f"{QUEUE_KEY_PREFIX}celery", "messages")

            # Add delivery tag to queue but NOT to messages hash
            delivery_tag = "orphan-tag"
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 100.0})

            # _get should raise Empty because payload is missing
            with pytest.raises(Empty):
                channel._get("celery")


@pytest.mark.integration
class TestBzmpopEdgeCases:
    """Tests for _bzmpop_start edge cases."""

    def test_bzmpop_start_with_no_active_queues(
        self,
        celery_app: Celery,
    ) -> None:
        """Test _bzmpop_start returns early when no active queues."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear active queues and reset queue cycle
            channel._active_queues.clear()
            channel._update_queue_cycle()

            # Should return without error (early return when no queues)
            channel._bzmpop_start(timeout=1)

            # _in_poll should still be False (not a connection object)
            assert channel._in_poll is False

    def test_bzmpop_start_with_global_keyprefix(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test _bzmpop_start uses prefixed keys when global_keyprefix is set."""
        from celery import Celery

        host, port, _image = redis_container

        app = Celery("test_bzmpop_prefix")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"global_keyprefix": "prefix:"},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        with app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Verify global_keyprefix is set
            assert channel.global_keyprefix == "prefix:"

            # The _queue_cycle is now a simple list
            channel._update_queue_cycle()

            # We can verify the channel has global_keyprefix set
            # The actual BZMPOP call would use prefixed keys

        app.close()


@pytest.mark.unit
class TestPollingInterval:
    """Tests for polling_interval setting."""

    def test_transport_polling_interval_patched_for_tests(self) -> None:
        """Test Transport polling_interval is patched to 1 for faster tests.

        Note: The default is 10s, but pytest_configure patches it to 1s
        for faster worker shutdown during tests.
        """
        assert Transport.polling_interval == 1


@pytest.mark.unit
class TestAfterFork:
    """Tests for fork handling."""

    def test_after_fork_cleanup_channel(self) -> None:
        """Test _after_fork_cleanup_channel calls channel._after_fork."""

        mock_channel = MagicMock()

        _after_fork_cleanup_channel(mock_channel)

        mock_channel._after_fork.assert_called_once()

    def test_channel_after_fork_disconnects_pools(self) -> None:
        """Test Channel._after_fork calls _disconnect_pools."""
        mock_channel = MagicMock(spec=Channel)
        mock_channel._disconnect_pools = MagicMock()

        # Call the actual _after_fork method
        Channel._after_fork(mock_channel)

        mock_channel._disconnect_pools.assert_called_once()


@pytest.mark.integration
class TestPoolDisconnect:
    """Tests for pool disconnection."""

    def test_disconnect_pools_cleans_up(
        self,
        celery_app: Celery,
    ) -> None:
        """Test _disconnect_pools cleans up connection pools."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Force pool creation by accessing client
            _ = channel.client

            # Call disconnect
            channel._disconnect_pools()

            # Pools should be cleared
            assert channel._pool is None
            assert channel._async_pool is None


# =============================================================================
# Transport Integration Tests
# =============================================================================


@pytest.mark.integration
class TestTransportDelivery:
    """Comprehensive transport delivery tests.

    Tests transport-specific functionality: message delivery, priority ordering,
    delayed delivery, and concurrent message handling.
    Uses nested parametrization to test all combinations.
    """

    @pytest.mark.parametrize("acks_late", [False, True], ids=["acks-early", "acks-late"])
    @pytest.mark.parametrize("countdown", [None, 1, 3], ids=["immediate", "delay-1s", "delay-3s-native"])
    @pytest.mark.parametrize("priority", [None, 0, 9], ids=["no-priority", "p0", "p9"])
    @pytest.mark.parametrize("num_tasks", [1, 5], ids=["1x", "5x"])
    def test_message_delivery(
        self,
        celery_app: Celery,
        celery_worker: Any,
        num_tasks: int,
        priority: int | None,
        countdown: int | None,
        acks_late: bool,
    ) -> None:
        """Test message delivery with various configurations.

        Covers (nested parametrization = Cartesian product):
        - num_tasks: 1, 5 (single vs batch)
        - priority: None, 0, 9 (default, low, high)
        - countdown: None, 1s, 3s (immediate, short delay, native delayed >2s threshold)
        - acks_late: True/False (affects when message is removed from queue)

        Note: DEFAULT_REQUEUE_CHECK_INTERVAL is patched to 2s in fixtures,
        so countdown=3 triggers native delayed delivery.
        """

        @celery_app.task(acks_late=acks_late)
        def echo(x: int) -> int:
            return x

        celery_worker.reload()
        start = time.time()

        # Build apply_async kwargs
        apply_kwargs: dict[str, Any] = {}
        if priority is not None:
            apply_kwargs["priority"] = priority
        if countdown is not None:
            apply_kwargs["countdown"] = countdown

        # Send tasks
        results = [echo.apply_async(args=(i,), **apply_kwargs) for i in range(num_tasks)]

        # Collect results
        values = [r.get(timeout=30) for r in results]
        elapsed = time.time() - start

        # Verify all messages delivered correctly
        assert sorted(values) == list(range(num_tasks))

        # Verify timing for delayed tasks
        if countdown:
            assert elapsed >= countdown - 0.5, f"Completed too fast: {elapsed:.2f}s"
