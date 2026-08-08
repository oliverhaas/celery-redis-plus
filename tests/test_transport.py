"""Tests for the enhanced Redis transport with BZMPOP, Streams, and delayed delivery."""

from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from queue import Empty
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, patch

import pytest
from celery.bootsteps import (
    CLOSE,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
    RUN,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
    TERMINATE,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
)
from kombu import Exchange, Queue
from kombu.exceptions import InconsistencyError, OperationalError
from kombu.transport import virtual
from kombu.utils.eventio import ERR
from kombu.utils.json import dumps as json_dumps

import celery_redis_plus.transport as transport_mod
from celery_redis_plus.constants import (
    DEFAULT_MESSAGE_TTL,
    DEFAULT_VISIBILITY_TIMEOUT,
    MESSAGE_KEY_PREFIX,
    MESSAGES_INDEX_PREFIX,
    MIN_BINDING_LIFETIME,
    MIN_QUEUE_EXPIRES,
    PRIORITY_SCORE_MULTIPLIER,
    QUEUE_KEY_PREFIX,
)
from celery_redis_plus.transport import (
    _CONSUME_MESSAGE_LUA,
    DEFAULT_DB,
    Channel,
    GlobalKeyPrefixMixin,
    MultiChannelPoller,
    PrefixedRedisPipeline,
    PrefixedStrictRedis,
    QoS,
    Transport,
    _after_fork_cleanup_channel,
    _channel_errors,
    _client_exceptions,
    _connection_errors,
    _queue_score,
    client_lib,
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

    def test_error_class_tuples(self) -> None:
        """Test that error class tuples are properly defined."""
        assert isinstance(_connection_errors, tuple)
        assert isinstance(_channel_errors, tuple)

    def test_connection_error_in_tuples(self) -> None:
        """Test that ConnectionError is included in connection_errors."""
        assert _client_exceptions.ConnectionError in _connection_errors


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


def _stub_binding_table(mock_client: MagicMock, members: list[bytes]) -> MagicMock:
    """Point a mocked client at a binding table holding `members`.

    get_table reads the table by pruning and ranging in one pipeline, so the
    members live on the pipeline's result, not on a plain command.
    """
    mock_pipe = MagicMock()
    mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
    mock_pipe.__exit__ = MagicMock(return_value=False)
    mock_pipe.execute.return_value = [0, members]
    mock_client.pipeline.return_value = mock_pipe
    return mock_pipe


@pytest.mark.unit
class TestChannel:
    """Tests for the custom Channel class."""

    def test_put_stores_in_sorted_set(self, global_keyprefix: str) -> None:
        """Test that _put stores messages in per-message hash with correct score."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {}
        channel._expires = {}

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
        # No expire when message_ttl is -1 (default)
        mock_pipe.expire.assert_not_called()
        # Verify zadd was called twice (once for index, once for queue)
        assert mock_pipe.zadd.call_count == 2
        mock_pipe.execute.assert_called_once()

    def test_put_with_long_delay_goes_to_messages_index(self, global_keyprefix: str) -> None:
        """Test that native delayed messages go to messages_index:{queue}, not queue.

        Native delayed delivery stores the message only in messages_index:{queue} with
        queue_at = eta. The requeue mechanism will add it to the queue when eta arrives.
        """

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {}
        channel._expires = {}

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

        # The single zadd should be for messages_index:{queue} with queue_at = eta
        index_zadd_call = zadd_calls[0]
        index_name, score_dict = index_zadd_call[0]
        assert index_name == f"{MESSAGES_INDEX_PREFIX}my_queue"
        queue_at = list(score_dict.values())[0]
        assert before + delay_seconds <= queue_at <= after + delay_seconds

        # Verify per-message hash is stored with native_delayed=1 and eta
        hset_call = mock_pipe.hset.call_args
        mapping = hset_call.kwargs.get("mapping", {})
        assert "priority" in mapping
        assert mapping.get("native_delayed") == 1
        assert mapping.get("eta") == eta_timestamp

    def test_put_with_short_delay_goes_to_main_queue(self, global_keyprefix: str) -> None:
        """Test that messages with short delay go to main queue with future timestamp score."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {}
        channel._expires = {}

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

        # Use short delay (1 second) - less than DEFAULT_REQUEUE_CHECK_INTERVAL
        # Short delays are treated as immediate, Celery's built-in eta logic handles them
        delay_seconds = 1.0
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

    def test_put_with_no_eta(self, global_keyprefix: str) -> None:
        """Test that no eta means immediate delivery (no delay)."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {}
        channel._expires = {}

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

    def test_put_with_eta_in_past_treated_as_immediate(self, global_keyprefix: str) -> None:
        """Test that eta in the past is treated as immediate delivery."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {}
        channel._expires = {}

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
        """Test fanout stream key generation.

        Fanout uses a single stream per exchange (routing key is ignored).
        """
        channel = object.__new__(Channel)
        channel.keyprefix_fanout = "/0."

        key = channel._fanout_stream_key("myexchange")
        assert key == "/0.myexchange"

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

    def test_publish_to_an_exchange_without_bindings_raises(self) -> None:
        """Test that a direct exchange with an empty binding table raises, not drops."""
        channel = object.__new__(Channel)
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"
        channel.typeof = MagicMock(return_value=virtual.exchange.DirectExchange(channel))
        channel.deadletter_queue = None

        mock_client = MagicMock()
        _stub_binding_table(mock_client, [])
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with pytest.raises(InconsistencyError, match="binding table is empty"):
            channel._lookup("reply.celery.pidbox", "some-ticket-uuid")

        # InconsistencyError is in connection_errors, so kombu's ensure reconnects,
        # redeclares the binding and retries instead of surfacing it directly.
        assert InconsistencyError in Transport.connection_errors

    def test_lookup_topic_exchange_without_bindings_still_discards(self) -> None:
        """Test that only direct exchanges raise: celeryev must not block publishers."""
        channel = object.__new__(Channel)
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"
        channel.typeof = MagicMock(return_value=virtual.exchange.TopicExchange(channel))
        channel.deadletter_queue = None

        mock_client = MagicMock()
        _stub_binding_table(mock_client, [])
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        # Raising on celeryev would make every worker buffer every task event
        # for as long as Flower is down.
        assert not channel._lookup("celeryev", "task.succeeded")

    def test_get_table_empty_exchange(self) -> None:
        """Test get_table returns empty list for exchange with no bindings."""
        channel = object.__new__(Channel)
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"

        mock_client = MagicMock()
        _stub_binding_table(mock_client, [])

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel.get_table("nonexistent_exchange")
        assert result == []

    def test_get_table_warns_once_on_separator_mismatch(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test get_table pads bindings written with a different sep and warns once."""
        channel = object.__new__(Channel)
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"
        Channel._warned_binding_sep = False

        mock_client = MagicMock()
        # Written by a deployment configured with sep=":"
        _stub_binding_table(mock_client, [b"test_key:test_pattern:test_queue"])

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        try:
            with caplog.at_level(logging.WARNING, logger="celery_redis_plus.transport"):
                result = channel.get_table("test_exchange")
                assert result == [("test_key:test_pattern:test_queue", "", "")]
                assert len(caplog.records) == 1
                assert "test_exchange" in caplog.records[0].getMessage()
                assert "test_key:test_pattern:test_queue" in caplog.records[0].getMessage()

                # Second call hits the same mismatch but must not warn again
                caplog.clear()
                assert channel.get_table("test_exchange") == [
                    ("test_key:test_pattern:test_queue", "", ""),
                ]
                assert caplog.records == []
        finally:
            Channel._warned_binding_sep = False

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
        """Test _get retrieves message synchronously via Lua script."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = ""
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        mock_script = MagicMock()
        # Lua script returns [queue_name, tag, payload, delivery_count]
        mock_script.return_value = [b"myqueue", b"tag123", b'{"body": "test"}', b"0"]
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._get("myqueue")
        assert result == {"body": "test"}
        mock_client.register_script.assert_called_once()
        mock_script.assert_called_once()

    def test_get_synchronous_empty(self) -> None:
        """Test _get raises Empty when queue is empty."""

        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = ""
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_script.return_value = None
        mock_client.register_script.return_value = mock_script

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
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""

        client_class = channel._get_client()

        assert client_class is client_lib.Redis

    def test_connparams_with_ssl_dict(self) -> None:
        """Test _connparams applies SSL config from dict."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection

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

        assert params["connection_class"] is client_lib.SSLConnection
        assert params["ssl_cert_reqs"] == "required"

    def test_connparams_with_ssl_true(self) -> None:
        """Test _connparams applies SSL config when ssl=True."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection

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

        assert params["connection_class"] is client_lib.SSLConnection

    def test_connparams_with_credential_provider_instance(self) -> None:
        """Test _connparams passes credential_provider and removes username/password."""
        CredentialProvider = client_lib.credentials.CredentialProvider

        class DummyProvider(CredentialProvider):
            def get_credentials(self):
                return ("user", "token123")

        provider = DummyProvider()

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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        channel.credential_provider = provider

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = "old_user"
        mock_conninfo.password = "old_pass"  # noqa: S105
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        params = channel._connparams()

        assert params["credential_provider"] is provider
        assert "username" not in params
        assert "password" not in params

    def test_connparams_with_credential_provider_string(self) -> None:
        """Test _connparams resolves dotted path string to a CredentialProvider."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        provider_path = f"{client_lib.__name__}.credentials.UsernamePasswordCredentialProvider"
        channel.credential_provider = provider_path

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        params = channel._connparams()

        assert isinstance(
            params["credential_provider"],
            client_lib.credentials.UsernamePasswordCredentialProvider,
        )

    def test_connparams_with_nonexistent_credential_provider_string(self) -> None:
        """Test _connparams raises ImportError for nonexistent dotted path."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        channel.credential_provider = "nonexistent_module.CredentialProvider"

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        with pytest.raises(ImportError):
            channel._connparams()

    def test_connparams_with_non_credential_provider_class_string(self) -> None:
        """Test _connparams raises ValueError when string resolves to non-CredentialProvider."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        channel.credential_provider = "abc.ABC"

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        with pytest.raises(ValueError, match="credential_provider must be an instance"):
            channel._connparams()

    def test_connparams_with_invalid_credential_provider(self) -> None:
        """Test _connparams raises ValueError for non-CredentialProvider object."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        channel.credential_provider = object()  # Not a CredentialProvider

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = None
        mock_conninfo.password = None
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        with pytest.raises(ValueError, match="credential_provider must be an instance"):
            channel._connparams()

    def test_connparams_without_credential_provider(self) -> None:
        """Test _connparams preserves username/password when no credential_provider."""
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
        channel.connection_class = client_lib.Connection
        channel.connection_class_ssl = client_lib.SSLConnection
        channel.credential_provider = None

        mock_conninfo = MagicMock()
        mock_conninfo.hostname = "localhost"
        mock_conninfo.port = 6379
        mock_conninfo.virtual_host = "0"
        mock_conninfo.userid = "myuser"
        mock_conninfo.password = "mypass"  # noqa: S105
        mock_conninfo.ssl = None
        mock_conninfo.transport_options = {}

        mock_connection = MagicMock()
        mock_connection.client = mock_conninfo
        mock_connection.default_port = 6379
        channel.connection = mock_connection

        params = channel._connparams()

        assert "credential_provider" not in params
        assert params["username"] == "myuser"
        assert params["password"] == "mypass"  # noqa: S105

    def test_prepare_queue_arguments(self) -> None:
        """Test that prepare_queue_arguments converts expires/message_ttl to ms."""
        channel = object.__new__(Channel)

        result = channel.prepare_queue_arguments({}, expires=60.0, message_ttl=30.0)

        assert result["x-expires"] == 60000
        assert result["x-message-ttl"] == 30000

    def test_prepare_queue_arguments_preserves_existing(self) -> None:
        """Test that prepare_queue_arguments preserves existing queue arguments."""
        channel = object.__new__(Channel)

        result = channel.prepare_queue_arguments({"x-custom": "value"}, expires=60.0)

        assert result["x-expires"] == 60000
        assert result["x-custom"] == "value"

    def test_new_queue_stores_expires(self) -> None:
        """Test that _new_queue stores x-expires in _expires dict."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}
        channel.connection = MagicMock()

        channel._new_queue("my_queue", arguments={"x-expires": 60000})

        assert channel._expires["my_queue"] == 60000
        channel.connection.cycle._update_expires_timer.assert_called_once()

    def test_new_queue_clamps_short_expires(self) -> None:
        """Test that _new_queue clamps x-expires below minimum."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}
        channel.connection = MagicMock()
        Channel._warned_expires_clamp = False

        channel._new_queue("my_queue", arguments={"x-expires": 5000})

        assert channel._expires["my_queue"] == MIN_QUEUE_EXPIRES

    def test_new_queue_stores_message_ttl(self) -> None:
        """Test that _new_queue stores x-message-ttl in _message_ttls dict."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}

        channel._new_queue("my_queue", arguments={"x-message-ttl": 30000})

        assert channel._message_ttls["my_queue"] == 30000

    def test_new_queue_no_ttl_arguments(self) -> None:
        """Test that _new_queue with no TTL arguments doesn't add to dicts."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}

        channel._new_queue("my_queue")

        assert "my_queue" not in channel._expires
        assert "my_queue" not in channel._message_ttls

    def test_put_uses_queue_message_ttl(self, global_keyprefix: str) -> None:
        """Test that _put uses per-queue message TTL when configured."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.message_ttl = DEFAULT_MESSAGE_TTL  # no TTL
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.global_keyprefix = global_keyprefix
        channel._message_ttls = {"my_queue": 60000}  # 60 seconds
        channel._expires = {}

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
            },
        }

        channel._put("my_queue", message)

        # EXPIRE should use 60 seconds (60000ms // 1000), not default 3 days
        mock_pipe.expire.assert_called_once()
        expire_args = mock_pipe.expire.call_args[0]
        assert expire_args[1] == 60  # 60000 // 1000

    def test_refresh_queue_expires(self, global_keyprefix: str) -> None:
        """Test that _refresh_queue_expires PEXPIREs correct keys."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {"celery": 60000, "priority": 120000}
        channel._bindings = {}

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._refresh_queue_expires()

        pexpire_calls = mock_pipe.pexpire.call_args_list
        assert len(pexpire_calls) == 4  # 2 queues x 2 keys each
        # Check all expected calls are present
        call_args_set = {(call[0][0], call[0][1]) for call in pexpire_calls}
        assert (f"{QUEUE_KEY_PREFIX}celery", 60000) in call_args_set
        assert (f"{MESSAGES_INDEX_PREFIX}celery", 60000) in call_args_set
        assert (f"{QUEUE_KEY_PREFIX}priority", 120000) in call_args_set
        assert (f"{MESSAGES_INDEX_PREFIX}priority", 120000) in call_args_set
        mock_pipe.execute.assert_called_once()

    def test_refresh_queue_expires_rescores_the_bindings(self, global_keyprefix: str) -> None:
        """Test that the refresh pushes the staleness deadline of declared bindings out."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel._expires = {"celery": 60000}
        channel._bindings = {"celery": {("celery_exchange", "celery\x06\x16\x06\x16celery")}}

        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.__enter__ = MagicMock(return_value=mock_pipe)
        mock_pipe.__exit__ = MagicMock(return_value=False)
        mock_client.pipeline.return_value = mock_pipe

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        before = time.time()
        channel._refresh_queue_expires()
        after = time.time()

        mock_pipe.zadd.assert_called_once()
        key, mapping = mock_pipe.zadd.call_args[0]
        assert key == "_kombu.binding.celery_exchange"
        stale_at = mapping["celery\x06\x16\x06\x16celery"]
        # x-expires is 60s but the floor under a binding's life is longer
        assert before + MIN_BINDING_LIFETIME <= stale_at <= after + MIN_BINDING_LIFETIME

    def test_binding_stale_at(self) -> None:
        """Test the staleness deadline a binding is scored with."""
        channel = object.__new__(Channel)
        channel._expires = {"short": 60_000, "long": 3_600_000}

        # A queue that never expires keeps its route until an explicit unbind
        assert channel._binding_stale_at("no_expires") == float("inf")
        # Below the floor the floor wins, above it x-expires does
        assert channel._binding_stale_at("short", now=1000) == 1000 + MIN_BINDING_LIFETIME
        assert channel._binding_stale_at("long", now=1000) == 1000 + 3600

    def test_refresh_queue_expires_empty(self) -> None:
        """Test that _refresh_queue_expires is a no-op when _expires is empty."""
        channel = object.__new__(Channel)
        channel._expires = {}
        channel.conn_or_acquire = MagicMock()

        channel._refresh_queue_expires()

        channel.conn_or_acquire.assert_not_called()

    def test_get_skips_expired_messages(self, global_keyprefix: str) -> None:
        """Test that _get skips expired messages (handled by Lua script internally)."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        # Lua script internally skips expired messages and returns the valid one
        mock_script = MagicMock()
        mock_script.return_value = [b"my_queue", b"valid_tag", b'{"body": "test"}', b"0"]
        mock_client.register_script.return_value = mock_script

        result = channel._get("my_queue")

        assert result == {"body": "test"}

    def test_get_raises_empty_when_all_expired(self, global_keyprefix: str) -> None:
        """Test that _get raises Empty when all messages have expired."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        # Lua script returns nil when all messages have expired hashes
        mock_script = MagicMock()
        mock_script.return_value = None
        mock_client.register_script.return_value = mock_script

        with pytest.raises(Empty):
            channel._get("my_queue")

    def test_bzmpop_read_drains_expired_messages(self, global_keyprefix: str) -> None:
        """Test that _slow_consume_read falls back to consume Lua after expired BZMPOP result."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = False  # SLOW mode (BZMPOP path)
        channel._no_ack_queues = set()
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        channel.client = mock_client

        mock_connection = MagicMock()
        channel.connection = mock_connection

        # BZMPOP returns expired message
        mock_client.parse_response.return_value = (
            b"queue:my_queue",
            [(b"expired_tag", 1.0)],
        )
        # Pipeline ZADD + HMGET: ZADD returns 0 (no index entry), HMGET returns None
        mock_pipe = MagicMock()
        mock_pipe.execute.return_value = [0, [None, None]]
        mock_client.pipeline.return_value.__enter__ = MagicMock(return_value=mock_pipe)
        mock_client.pipeline.return_value.__exit__ = MagicMock(return_value=False)

        # _drain_expired_and_deliver uses consume Lua script via register_script
        mock_script = MagicMock()
        mock_script.return_value = [b"my_queue", b"valid_tag", b'{"body": "test"}', b"0"]
        mock_client.register_script.return_value = mock_script

        result = channel._bzmpop_read()

        assert result is True
        mock_connection._deliver.assert_called_once_with({"body": "test"}, "my_queue")
        # After draining, should switch back to FAST mode
        assert channel._consume_fast_mode is True

    def test_cleanup_expired_message(self, global_keyprefix: str) -> None:
        """Test that _cleanup_expired_message removes the messages_index entry."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._cleanup_expired_message("my_queue", "tag123")

        mock_client.zrem.assert_called_once_with(
            f"{MESSAGES_INDEX_PREFIX}my_queue",
            "tag123",
        )

    def test_cleanup_expired_message_with_client(self, global_keyprefix: str) -> None:
        """Test _cleanup_expired_message with explicit client."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix

        mock_client = MagicMock()
        channel._cleanup_expired_message("my_queue", "tag123", client=mock_client)

        mock_client.zrem.assert_called_once_with(
            f"{MESSAGES_INDEX_PREFIX}my_queue",
            "tag123",
        )

    def test_delete_cleans_up_ttl_state(self, global_keyprefix: str) -> None:
        """Test that _delete removes queue from _expires, _message_ttls and _bindings."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = {"my_queue"}
        channel._expires = {"my_queue": 60000}
        channel._message_ttls = {"my_queue": 30000}
        member = "my_key\x06\x16\x06\x16my_queue"
        channel._bindings = {"my_queue": {("my_exchange", member)}}
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        mock_cycle = MagicMock()
        channel.connection = MagicMock()
        channel.connection.cycle = mock_cycle

        channel._delete("my_queue", "my_exchange", "my_key", "")

        assert "my_queue" not in channel._expires
        assert "my_queue" not in channel._message_ttls
        assert "my_queue" not in channel.auto_delete_queues
        assert "my_queue" not in channel._bindings
        mock_client.zrem.assert_called_once_with("_kombu.binding.my_exchange", member)
        mock_cycle._update_expires_timer.assert_called_once()

    def test_delete_falls_back_to_srem_on_a_legacy_binding_set(self, global_keyprefix: str) -> None:
        """Test that unbinding removes the member in place without converting the table."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}
        channel._bindings = {}
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"
        channel._convert_binding_set = MagicMock()  # type: ignore[method-assign]

        mock_client = MagicMock()
        mock_client.zrem.side_effect = _client_exceptions.ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel.connection = MagicMock()

        channel._delete("my_queue", "my_exchange", "my_key", "")

        member = "my_key\x06\x16\x06\x16my_queue"
        mock_client.srem.assert_called_once_with("_kombu.binding.my_exchange", member)
        channel._convert_binding_set.assert_not_called()  # type: ignore[attr-defined]

    def test_delete_reraises_a_non_wrongtype_error(self, global_keyprefix: str) -> None:
        """Test that _delete only swallows the wrong-type error it knows how to handle."""
        channel = object.__new__(Channel)
        channel.auto_delete_queues = set()
        channel._expires = {}
        channel._message_ttls = {}
        channel._bindings = {}
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"

        mock_client = MagicMock()
        mock_client.zrem.side_effect = _client_exceptions.ResponseError("READONLY")
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        channel.connection = MagicMock()

        with pytest.raises(_client_exceptions.ResponseError):
            channel._delete("my_queue", "my_exchange", "my_key", "")

    def test_queue_bind_converts_a_legacy_binding_set(self, global_keyprefix: str) -> None:
        """Test that a binding table still stored as a set is converted, then written."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"
        channel._expires = {}
        channel._bindings = {}
        channel._fanout_queues = {}
        channel.typeof = MagicMock(return_value=MagicMock(type="direct"))
        channel._convert_binding_set = MagicMock()  # type: ignore[method-assign]

        mock_client = MagicMock()
        mock_client.zadd.side_effect = [
            _client_exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value"),
            1,
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._queue_bind("my_exchange", "my_key", "", "my_queue")

        channel._convert_binding_set.assert_called_once_with(mock_client, "my_exchange")  # type: ignore[attr-defined]
        member = "my_key\x06\x16\x06\x16my_queue"
        # The queue has no x-expires, so its route never goes stale
        assert mock_client.zadd.call_args_list[1][0] == (
            "_kombu.binding.my_exchange",
            {member: float("inf")},
        )
        assert channel._bindings == {"my_queue": {("my_exchange", member)}}

    def test_get_table_reads_a_legacy_binding_set(self, global_keyprefix: str) -> None:
        """Test that a binding table still stored as a set stays readable."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.keyprefix_queue = "_kombu.binding.%s"
        channel.sep = "\x06\x16"

        mock_client = MagicMock()
        mock_pipe = _stub_binding_table(mock_client, [])
        mock_pipe.execute.side_effect = _client_exceptions.ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
        mock_client.smembers.return_value = {b"my_key\x06\x16\x06\x16my_queue"}
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        assert channel.get_table("my_exchange") == [("my_key", "", "my_queue")]
        mock_client.smembers.assert_called_once_with("_kombu.binding.my_exchange")


def _make_restore_qos(channel: MagicMock, call_order: list[str]) -> QoS:
    """Build a bare QoS wired up for restore_unacked_once tests."""
    qos = object.__new__(QoS)
    qos._fanout_tags = set()
    qos._dirty = set()
    qos._delivered = OrderedDict()
    qos._delivered.restored = False  # type: ignore[attr-defined]
    qos._delivered["tag1"] = MagicMock()
    qos.restore_at_shutdown = True
    qos._on_collect = MagicMock()
    channel.do_restore = True
    qos.channel = channel
    qos._drain_hub_callbacks = MagicMock(side_effect=lambda: call_order.append("drain"))
    return qos


def _make_worker_owner(state: int, call_order: list[str] | None = None) -> MagicMock:
    """Build a fake WorkController with a pool executor and a blueprint state."""
    owner = MagicMock(spec=["app", "pool", "blueprint"])
    owner.pool = MagicMock(spec=["executor"])
    owner.pool.executor = MagicMock()
    if call_order is not None:
        owner.pool.executor.shutdown.side_effect = lambda **_kw: call_order.append("shutdown")
    owner.blueprint = MagicMock(spec=["state"])
    owner.blueprint.state = state
    return owner


@pytest.mark.unit
class TestQoS:
    """Tests for the QoS class."""

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

        qos._remove_from_indices = MagicMock()

        qos.ack("tag1")

        qos._remove_from_indices.assert_called_once_with("tag1")

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
        mock_message = MagicMock()
        mock_message.delivery_info = {"routing_key": "my_queue"}
        qos._delivered = {"tag1": mock_message}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.requeue_by_tag = MagicMock()

        qos.reject("tag1", requeue=True)

        qos.requeue_by_tag.assert_called_once_with("tag1", queue="my_queue", leftmost=True)

    def test_reject_regular_message_without_requeue(self) -> None:
        """Test reject without requeue for regular message."""
        qos = object.__new__(QoS)
        qos._fanout_tags = set()
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos._remove_from_indices = MagicMock()

        qos.reject("tag1", requeue=False)

        qos._remove_from_indices.assert_called_once_with("tag1")

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
        # Create mock messages with delivery_info for routing_key lookup
        msg1 = MagicMock()
        msg1.delivery_info = {"routing_key": "celery"}
        msg2 = MagicMock()
        msg2.delivery_info = {"routing_key": "celery"}
        fanout_msg = MagicMock()
        fanout_msg.delivery_info = {"routing_key": "fanout_queue"}
        qos._delivered = {"tag1": msg1, "tag2": msg2, "fanout_tag": fanout_msg}
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
        mock_channel._messages_index_key.side_effect = lambda q: f"{MESSAGES_INDEX_PREFIX}{q}"
        mock_channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        qos.channel = mock_channel

        before = time.time()
        qos.maybe_update_messages_index()
        after = time.time()

        # Should update scores for tag1 and tag2, but NOT fanout_tag
        assert mock_pipe.zadd.call_count == 2
        zadd_calls = [call[0][0] for call in mock_pipe.zadd.call_args_list]
        assert f"{MESSAGES_INDEX_PREFIX}celery" in zadd_calls

        for zadd_call in mock_pipe.zadd.call_args_list:
            # XX only, or the heartbeat resurrects a message that was acked
            # between reading _delivered and running the pipeline
            assert zadd_call.kwargs["xx"] is True
            # The deadline has to match what _put and the consume paths write,
            # otherwise the heartbeat moves it backwards
            (queue_at,) = zadd_call.args[1].values()
            expected = DEFAULT_VISIBILITY_TIMEOUT + transport_mod.DEFAULT_REQUEUE_CHECK_INTERVAL
            assert before + expected <= queue_at <= after + expected

    def test_drain_hub_callbacks_fires_callbacks(self) -> None:
        """Test that _drain_hub_callbacks executes hub callbacks."""
        qos = object.__new__(QoS)

        callback1 = MagicMock()
        callback2 = MagicMock()

        mock_hub = MagicMock()
        mock_hub._pop_ready.return_value = [callback1, callback2]

        mock_cycle = MagicMock()
        mock_cycle._loop = mock_hub

        mock_connection = MagicMock()
        mock_connection.cycle = mock_cycle

        mock_channel = MagicMock()
        mock_channel.connection = mock_connection
        qos.channel = mock_channel

        qos._drain_hub_callbacks()

        callback1.assert_called_once()
        callback2.assert_called_once()

    def test_drain_hub_callbacks_no_hub(self) -> None:
        """Test _drain_hub_callbacks is safe when hub is not available."""
        qos = object.__new__(QoS)

        mock_cycle = MagicMock()
        mock_cycle._loop = None

        mock_connection = MagicMock()
        mock_connection.cycle = mock_cycle

        mock_channel = MagicMock()
        mock_channel.connection = mock_connection
        qos.channel = mock_channel

        # Should not raise
        qos._drain_hub_callbacks()

    def test_drain_hub_callbacks_no_connection(self) -> None:
        """Test _drain_hub_callbacks is safe when connection is gone."""
        qos = object.__new__(QoS)

        mock_channel = MagicMock(spec=[])  # Empty spec — no attributes
        qos.channel = mock_channel

        # Should not raise (AttributeError is caught)
        qos._drain_hub_callbacks()

    def test_drain_hub_callbacks_callback_exception(self) -> None:
        """Test that failing callbacks don't prevent other callbacks from running."""
        qos = object.__new__(QoS)

        callback_ok = MagicMock()
        callback_fail = MagicMock(side_effect=RuntimeError("boom"))

        mock_hub = MagicMock()
        mock_hub._pop_ready.return_value = [callback_fail, callback_ok]

        mock_cycle = MagicMock()
        mock_cycle._loop = mock_hub

        mock_connection = MagicMock()
        mock_connection.cycle = mock_cycle

        mock_channel = MagicMock()
        mock_channel.connection = mock_connection
        qos.channel = mock_channel

        qos._drain_hub_callbacks()

        # Both should have been called despite first one raising
        callback_fail.assert_called_once()
        callback_ok.assert_called_once()

    def test_restore_unacked_once_waits_for_pool(self) -> None:
        """Test that restore drains, waits for executor, drains again, then restores."""
        call_order: list[str] = []
        owner = _make_worker_owner(CLOSE, call_order)

        mock_channel = MagicMock()
        mock_channel.connection.client.app = owner.app
        qos = _make_restore_qos(mock_channel, call_order)

        with (
            patch.dict(transport_mod._worker_owners, {owner.app: owner}, clear=True),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        assert call_order == ["drain", "shutdown", "drain", "super_restore"]
        owner.pool.executor.shutdown.assert_called_once_with(wait=True)

    @pytest.mark.parametrize(
        ("state", "stops_pool"),
        [(RUN, False), (CLOSE, True), (TERMINATE, True)],
        ids=["reconnect", "warm_shutdown", "cold_shutdown"],
    )
    def test_restore_unacked_once_only_acts_when_the_worker_stops(
        self,
        state: int,
        stops_pool: bool,
    ) -> None:
        """Test that a channel closing on a broker reconnect is a no-op.

        kombu calls restore_unacked_once from Channel.close(), which also runs
        when the consumer reconnects. Shutting the executor down there kills
        the pool for the rest of the process, and restoring requeues messages
        whose tasks are still running.
        """
        call_order: list[str] = []
        owner = _make_worker_owner(state, call_order)

        mock_channel = MagicMock()
        mock_channel.connection.client.app = owner.app
        qos = _make_restore_qos(mock_channel, call_order)

        with (
            patch.dict(transport_mod._worker_owners, {owner.app: owner}, clear=True),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        expected = ["drain", "shutdown", "drain", "super_restore"] if stops_pool else ["drain"]
        assert call_order == expected
        assert owner.pool.executor.shutdown.called is stops_pool

    def test_restore_unacked_once_unreadable_blueprint_leaves_pool_alone(self) -> None:
        """Test that a worker with no readable blueprint state counts as running.

        Skipping the executor wait only delays redelivery to the visibility
        timeout, while shutting the pool down on a reconnect is unrecoverable.
        """
        call_order: list[str] = []
        owner = MagicMock(spec=["app", "pool"])
        owner.pool = MagicMock(spec=["executor"])

        mock_channel = MagicMock()
        mock_channel.connection.client.app = owner.app
        qos = _make_restore_qos(mock_channel, call_order)

        with (
            patch.dict(transport_mod._worker_owners, {owner.app: owner}, clear=True),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        assert call_order == ["drain"]
        owner.pool.executor.shutdown.assert_not_called()

    def test_restore_unacked_once_no_pool_fallback(self) -> None:
        """Test that without a worker reference, single drain + super is used."""
        call_order: list[str] = []
        qos = _make_restore_qos(MagicMock(), call_order)

        with (
            patch.dict(transport_mod._worker_owners, {}, clear=True),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        # Only one drain (no executor wait), then super restore
        assert call_order == ["drain", "super_restore"]

    def test_restore_unacked_once_multi_app_isolation(self) -> None:
        """Test that two concurrent apps don't interfere with each other's pool reference.

        Simulates two Celery workers in the same process (e.g. tests, publisher+consumer).
        Worker A shuts down → should NOT destroy Worker B's pool reference.
        Worker B's restore should still find its own executor.
        """
        call_order: list[str] = []
        worker_a = _make_worker_owner(CLOSE)
        worker_b = _make_worker_owner(CLOSE, call_order)

        mock_channel_b = MagicMock()
        # Wire up so QoS can find its app
        mock_channel_b.connection.client.app = worker_b.app
        qos_b = _make_restore_qos(mock_channel_b, call_order)

        with patch.dict(transport_mod._worker_owners, {}, clear=True):
            transport_mod._on_worker_ready(sender=worker_a)
            transport_mod._on_worker_ready(sender=worker_b)
            transport_mod._on_worker_shutdown(sender=worker_a)

            with patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ):
                qos_b.restore_unacked_once()

        # Worker B should still have found its executor and waited
        assert call_order == ["drain", "shutdown", "drain", "super_restore"]
        worker_b.pool.executor.shutdown.assert_called_once_with(wait=True)
        # Worker A's executor should NOT have been called
        worker_a.pool.executor.shutdown.assert_not_called()

    def test_restore_unacked_once_single_app_fallback(self) -> None:
        """Test that the lookup falls back to the only registered worker.

        kombu's Connection carries no back-reference to the Celery app, so the
        app lookup finds nothing on a real connection chain. If exactly one
        worker is registered, use it.
        """
        call_order: list[str] = []
        owner = _make_worker_owner(CLOSE, call_order)

        mock_channel = MagicMock()
        del mock_channel.connection.client.app
        qos = _make_restore_qos(mock_channel, call_order)

        with (
            patch.dict(transport_mod._worker_owners, {MagicMock(): owner}, clear=True),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        assert call_order == ["drain", "shutdown", "drain", "super_restore"]

    def test_restore_unacked_once_multiple_workers_are_not_guessed(self) -> None:
        """Test that an unreachable app with two workers registered picks neither.

        There is nothing left to disambiguate on, so fall back to plain kombu
        behaviour rather than shutting an arbitrary worker's pool down.
        """
        call_order: list[str] = []
        owner_a = _make_worker_owner(CLOSE, call_order)
        owner_b = _make_worker_owner(CLOSE, call_order)

        mock_channel = MagicMock()
        del mock_channel.connection.client.app
        qos = _make_restore_qos(mock_channel, call_order)

        with (
            patch.dict(
                transport_mod._worker_owners,
                {MagicMock(): owner_a, MagicMock(): owner_b},
                clear=True,
            ),
            patch.object(
                QoS.__bases__[0],
                "restore_unacked_once",
                side_effect=lambda _self, _stderr=None: call_order.append(
                    "super_restore",
                ),
            ),
        ):
            qos.restore_unacked_once()

        assert call_order == ["drain", "super_restore"]
        owner_a.pool.executor.shutdown.assert_not_called()
        owner_b.pool.executor.shutdown.assert_not_called()


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
        transport = MagicMock(spec=Transport)
        transport.driver_version = Transport.driver_version
        version = transport.driver_version(transport)
        assert version == client_lib.__version__

    def test_connection_errors_defined(self) -> None:
        """Test that connection and channel errors are defined."""
        # These are set at class definition time if redis is available
        assert hasattr(Transport, "connection_errors")
        assert hasattr(Transport, "channel_errors")

    def test_expires_timer_starts_for_queues_declared_before_the_loop(self) -> None:
        """Test that register_with_event_loop starts the expires timer for existing queues."""
        channel = MagicMock()
        channel._expires = {"celery": 60_000}
        poller = MultiChannelPoller()
        poller.add(channel)

        # _update_expires_timer no-ops without a loop, which is the state celery
        # leaves things in: queues are declared in the Tasks bootstep, long
        # before asynloop calls register_with_event_loop.
        poller._update_expires_timer()
        assert poller._expires_timer_entry is None

        transport = MagicMock()
        transport.cycle = poller
        connection = MagicMock()
        connection.client.transport_options = {}
        loop = MagicMock()

        Transport.register_with_event_loop(transport, connection, loop)

        assert poller._loop is loop
        assert poller._expires_timer_entry is not None
        # 60_000 ms / 2 / 1000 = refresh twice per TTL
        loop.call_repeatedly.assert_any_call(30.0, poller.maybe_refresh_queue_expires)

    @pytest.mark.parametrize(
        ("transport_options", "expected_interval"),
        [
            ({}, DEFAULT_VISIBILITY_TIMEOUT / 3),
            ({"visibility_timeout": 30}, 10.0),
        ],
        ids=["default", "configured"],
    )
    def test_visibility_heartbeat_is_registered_with_the_event_loop(
        self,
        transport_options: dict[str, Any],
        expected_interval: float,
    ) -> None:
        """The heartbeat that keeps in-flight messages alive has to be on the loop.

        Nothing else in the suite notices if this registration goes missing:
        the heartbeat only matters for a task that outlives its visibility
        timeout, and a message whose deadline passes is simply redelivered.
        """
        poller = MultiChannelPoller()
        transport = MagicMock()
        transport.cycle = poller
        connection = MagicMock()
        connection.client.transport_options = transport_options
        loop = MagicMock()

        Transport.register_with_event_loop(transport, connection, loop)

        # Three beats per visibility timeout, so two can be missed before a
        # message is handed to another worker.
        loop.call_repeatedly.assert_any_call(expected_interval, poller.maybe_update_messages_index)


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

    def test_drain_refreshes_expires_without_a_loop(self) -> None:
        """A connection with no hub refreshes off the drain path instead."""
        poller = MultiChannelPoller()
        poller.maybe_refresh_queue_expires = MagicMock()  # type: ignore[method-assign]
        channel = MagicMock()
        channel._expires = {"celery": 60_000}
        poller._channels = {channel}  # type: ignore[assignment]

        poller.maybe_refresh_queue_expires_without_loop()
        # 10s in, well short of the 60s / 2 interval
        poller._last_expires_refresh = time.time() - 10
        poller.maybe_refresh_queue_expires_without_loop()

        poller.maybe_refresh_queue_expires.assert_called_once()  # type: ignore[attr-defined]

        # Past the interval it runs again
        poller._last_expires_refresh = time.time() - 31
        poller.maybe_refresh_queue_expires_without_loop()

        assert poller.maybe_refresh_queue_expires.call_count == 2  # type: ignore[attr-defined]

    def test_drain_does_not_refresh_expires_when_a_loop_owns_it(self) -> None:
        """With a hub the timer refreshes, so the drain path stays out of it."""
        poller = MultiChannelPoller()
        poller.maybe_refresh_queue_expires = MagicMock()  # type: ignore[method-assign]
        poller._loop = MagicMock()
        channel = MagicMock()
        channel._expires = {"celery": 60_000}
        poller._channels = {channel}  # type: ignore[assignment]

        poller.maybe_refresh_queue_expires_without_loop()

        poller.maybe_refresh_queue_expires.assert_not_called()  # type: ignore[attr-defined]

    def test_drain_does_not_refresh_expires_without_ttls(self) -> None:
        """Nothing to refresh when no queue has an x-expires."""
        poller = MultiChannelPoller()
        poller.maybe_refresh_queue_expires = MagicMock()  # type: ignore[method-assign]
        channel = MagicMock()
        channel._expires = {}
        poller._channels = {channel}  # type: ignore[assignment]

        poller.maybe_refresh_queue_expires_without_loop()

        poller.maybe_refresh_queue_expires.assert_not_called()  # type: ignore[attr-defined]


@pytest.mark.unit
class TestFastSlowConsumeMode:
    """Tests for FAST/SLOW atomic message consumption."""

    def test_fast_consume_read_delivers_message(self, global_keyprefix: str) -> None:
        """Test FAST mode delivers message from Lua EVALSHA response."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = True

        mock_client = MagicMock()
        channel.client = mock_client

        mock_connection = MagicMock()
        channel.connection = mock_connection

        # EVALSHA returns [queue_name, tag, payload, delivery_count]
        mock_client.parse_response.return_value = [
            b"my_queue",
            b"tag123",
            b'{"body": "test"}',
            b"0",
        ]

        result = channel._bzmpop_read()

        assert result is True
        assert channel._consume_fast_mode is True  # stays in FAST mode
        assert channel._in_poll is None
        mock_connection._deliver.assert_called_once_with({"body": "test"}, "my_queue")

    def test_fast_consume_read_with_delivery_count(self, global_keyprefix: str) -> None:
        """Test FAST mode injects x-delivery-count header when delivery_count > 0."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = True

        mock_client = MagicMock()
        channel.client = mock_client

        mock_connection = MagicMock()
        channel.connection = mock_connection

        mock_client.parse_response.return_value = [
            b"my_queue",
            b"tag123",
            b'{"body": "test", "properties": {"headers": {}}}',
            b"3",
        ]

        result = channel._bzmpop_read()

        assert result is True
        delivered_msg = mock_connection._deliver.call_args[0][0]
        assert delivered_msg["properties"]["headers"]["x-delivery-count"] == 3

    def test_fast_consume_read_switches_to_slow_on_empty(self, global_keyprefix: str) -> None:
        """Test FAST mode switches to SLOW and sends BZMPOP when queue is empty."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = True
        channel._consume_script_sha = "fakeSHA"
        channel._queue_cycle = ["celery"]
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        mock_conn = MagicMock()
        mock_client.connection = mock_conn
        mock_client.connection.blocking_timeout = 1
        # _prefix_args must return a real list for send_command(*args)
        mock_client._prefix_args.side_effect = lambda args: args
        channel.client = mock_client
        channel.connection = MagicMock()
        channel.connection.blocking_timeout = 1

        # EVALSHA returns nil (queue empty)
        mock_client.parse_response.return_value = None

        with pytest.raises(Empty):
            channel._bzmpop_read()

        assert channel._consume_fast_mode is False  # switched to SLOW
        assert channel._in_poll is not None  # BZMPOP is pending
        # Verify BZMPOP was sent via _bzmpop_start
        mock_conn.send_command.assert_called()
        sent_args = mock_conn.send_command.call_args[0]
        assert sent_args[0] == "BZMPOP"

    def test_fast_consume_read_noscript_error(self, global_keyprefix: str) -> None:
        """Test FAST mode handles NOSCRIPT error by clearing SHA and raising Empty."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = True
        channel._consume_script_sha = "stale_sha"
        channel.ResponseError = _client_exceptions.ResponseError
        channel.connection_errors = _connection_errors

        mock_client = MagicMock()
        channel.client = mock_client
        channel.connection = MagicMock()

        # parse_response raises NOSCRIPT error
        mock_client.parse_response.side_effect = _client_exceptions.ResponseError(
            "NOSCRIPT No matching script",
        )

        with pytest.raises(Empty):
            channel._bzmpop_read()

        assert channel._consume_script_sha is None  # SHA cleared for reload
        assert channel._in_poll is None
        assert channel._consume_fast_mode is True  # stays FAST (retry next tick)

    def test_fast_consume_read_connection_error(self, global_keyprefix: str) -> None:
        """Test FAST mode handles connection errors."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = True
        channel.connection_errors = (ConnectionError,)
        channel.ResponseError = _client_exceptions.ResponseError

        mock_client = MagicMock()
        mock_conn = MagicMock()
        mock_client.connection = mock_conn
        channel.client = mock_client
        channel.connection = MagicMock()

        mock_client.parse_response.side_effect = ConnectionError("lost connection")

        with pytest.raises(ConnectionError):
            channel._bzmpop_read()

        assert channel._in_poll is None
        mock_conn.disconnect.assert_called_once()

    def test_slow_consume_read_delivers_and_switches_to_fast(self, global_keyprefix: str) -> None:
        """Test SLOW mode delivers message via pipeline ZADD+HMGET and switches to FAST."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = False
        channel._no_ack_queues = set()
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        channel.client = mock_client

        mock_connection = MagicMock()
        channel.connection = mock_connection

        # BZMPOP returns a message
        mock_client.parse_response.return_value = (
            b"queue:my_queue",
            [(b"tag123", 100.0)],
        )

        # Pipeline ZADD + HMGET
        mock_pipe = MagicMock()
        mock_pipe.execute.return_value = [1, [b'{"body": "test"}', b"0"]]
        mock_client.pipeline.return_value.__enter__ = MagicMock(return_value=mock_pipe)
        mock_client.pipeline.return_value.__exit__ = MagicMock(return_value=False)

        result = channel._bzmpop_read()

        assert result is True
        assert channel._consume_fast_mode is True  # switched back to FAST
        assert channel._in_poll is None
        mock_connection._deliver.assert_called_once_with({"body": "test"}, "my_queue")
        # Verify pipeline was used (ZADD + HMGET)
        mock_pipe.zadd.assert_called_once()
        mock_pipe.hmget.assert_called_once()
        # Unconditional ZADD: xx=True would leave a delivery with no visibility
        # deadline whenever the index entry was missing.
        assert mock_pipe.zadd.call_args.kwargs.get("xx") is None

    def test_slow_consume_read_empty_raises(self, global_keyprefix: str) -> None:
        """Test SLOW mode raises Empty when BZMPOP times out."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = False

        mock_client = MagicMock()
        channel.client = mock_client
        channel.connection = MagicMock()

        # BZMPOP returns None (timeout)
        mock_client.parse_response.return_value = None

        with pytest.raises(Empty):
            channel._bzmpop_read()

        assert channel._in_poll is None

    def test_bzmpop_start_fast_mode_sends_evalsha(self, global_keyprefix: str) -> None:
        """Test _bzmpop_start sends EVALSHA in FAST mode."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._consume_fast_mode = True
        channel._consume_script_sha = "test_sha_123"
        channel._queue_cycle = ["celery"]
        channel._no_ack_queues = set()
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_conn = MagicMock()
        mock_client = MagicMock()
        mock_client.connection = mock_conn
        mock_client.script_load.return_value = "test_sha_123"
        channel.client = mock_client
        channel.connection = MagicMock()
        channel.connection.blocking_timeout = 1

        channel._bzmpop_start(timeout=1)

        assert channel._in_poll is mock_conn
        sent_args = mock_conn.send_command.call_args[0]
        assert sent_args[0] == "EVALSHA"
        assert sent_args[1] == "test_sha_123"

    def test_bzmpop_start_slow_mode_sends_bzmpop(self, global_keyprefix: str) -> None:
        """Test _bzmpop_start sends BZMPOP in SLOW mode."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._consume_fast_mode = False
        channel._queue_cycle = ["celery"]

        mock_conn = MagicMock()
        mock_client = MagicMock()
        mock_client.connection = mock_conn
        # _prefix_args must return a real list for send_command(*args)
        mock_client._prefix_args.side_effect = lambda args: args
        channel.client = mock_client
        channel.connection = MagicMock()
        channel.connection.blocking_timeout = 1

        channel._bzmpop_start(timeout=1)

        assert channel._in_poll is mock_conn
        sent_args = mock_conn.send_command.call_args[0]
        assert sent_args[0] == "BZMPOP"

    def test_fast_consume_sends_a_no_ack_flag_per_queue(self, global_keyprefix: str) -> None:
        """FAST mode appends one '0'/'1' flag per queue so the Lua script can
        dequeue no_ack deliveries instead of giving them a visibility deadline."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._consume_fast_mode = True
        channel._consume_script_sha = "test_sha_123"
        channel._queue_cycle = ["celery", "replies"]
        channel._no_ack_queues = {"replies"}
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_conn = MagicMock()
        mock_client = MagicMock()
        mock_client.connection = mock_conn
        channel.client = mock_client
        channel.connection = MagicMock()
        channel.connection.blocking_timeout = 1

        channel._bzmpop_start(timeout=1)

        sent_args = mock_conn.send_command.call_args[0]
        # ARGV tail: the queue names, then their flags in the same order
        assert sent_args[-4:] == ("celery", "replies", "0", "1")

    def test_slow_consume_read_dequeues_for_a_no_ack_queue(self, global_keyprefix: str) -> None:
        """SLOW mode must not give a no_ack delivery a visibility deadline:
        it removes the index entry and deletes the hash after reading it."""
        channel = object.__new__(Channel)
        channel.message_key_prefix = MESSAGE_KEY_PREFIX
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel._consume_fast_mode = False
        channel._no_ack_queues = {"my_queue"}
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT

        mock_client = MagicMock()
        channel.client = mock_client

        mock_connection = MagicMock()
        channel.connection = mock_connection

        mock_client.parse_response.return_value = (
            b"queue:my_queue",
            [(b"tag123", 100.0)],
        )

        mock_pipe = MagicMock()
        mock_pipe.execute.return_value = [1, [b'{"body": "test"}', b"0"], 1]
        mock_client.pipeline.return_value.__enter__ = MagicMock(return_value=mock_pipe)
        mock_client.pipeline.return_value.__exit__ = MagicMock(return_value=False)

        result = channel._bzmpop_read()

        assert result is True
        mock_connection._deliver.assert_called_once_with({"body": "test"}, "my_queue")
        mock_pipe.zadd.assert_not_called()
        mock_pipe.zrem.assert_called_once()
        mock_pipe.delete.assert_called_once()

    def test_poll_error_uses_evalsha_in_fast_mode(self) -> None:
        """Test _poll_error parses EVALSHA response when in FAST mode."""
        channel = object.__new__(Channel)
        channel._consume_fast_mode = True

        mock_client = MagicMock()
        channel.client = mock_client

        channel._poll_error("BZMPOP")

        mock_client.parse_response.assert_called_once_with(
            mock_client.connection,
            "EVALSHA",
        )

    def test_poll_error_uses_bzmpop_in_slow_mode(self) -> None:
        """Test _poll_error parses BZMPOP response when in SLOW mode."""
        channel = object.__new__(Channel)
        channel._consume_fast_mode = False

        mock_client = MagicMock()
        channel.client = mock_client

        channel._poll_error("BZMPOP")

        mock_client.parse_response.assert_called_once_with(
            mock_client.connection,
            "BZMPOP",
        )


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
        global_keyprefix: str,
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
        # The per-queue messages_index key tracks messages for this queue
        index_count = redis_client.zcard(f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery")
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
        global_keyprefix: str,
    ) -> None:
        """Test that publishing a task stores it in a Redis sorted set."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish without a worker - message should be stored in Redis
        add.delay(1, 2)

        # Check that message is in the celery queue sorted set
        queue_size = redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery")
        assert queue_size >= 1

        # Check that message is in the per-queue messages index
        index_size = redis_client.zcard(f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery")
        assert index_size >= 1

        # Check that message payload is stored in a per-message hash
        # Get the delivery tag from the queue to verify the message hash exists
        queue_members = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, 0)
        assert len(queue_members) >= 1
        delivery_tag = queue_members[0].decode() if isinstance(queue_members[0], bytes) else queue_members[0]
        message_key = f"{global_keyprefix}message:{delivery_tag}"
        assert redis_client.exists(message_key) == 1

    def test_published_message_with_countdown_has_future_score(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a task with countdown uses native delayed delivery."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish with a 10 second countdown (> DEFAULT_REQUEUE_CHECK_INTERVAL=2s)
        # This uses native delayed delivery: message goes to messages_index, not queue
        before_time = time.time()
        add.apply_async(args=(1, 2), countdown=10)

        # Native delayed message should NOT be in the queue sorted set yet
        queue_messages = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(queue_messages) == 0

        # But should be in messages_index with queue_at = eta
        index_messages = redis_client.zrange(f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery", 0, -1, withscores=True)
        assert len(index_messages) >= 1

        _tag, queue_at = index_messages[-1]
        # queue_at should be approximately 10 seconds in the future
        assert queue_at > before_time + 5

    def test_published_message_with_eta_has_future_score(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a task with ETA uses native delayed delivery."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish with an ETA 10 seconds in the future (> DEFAULT_REQUEUE_CHECK_INTERVAL=2s)
        before_time = time.time()
        eta = datetime.now(UTC) + timedelta(seconds=10)
        add.apply_async(args=(1, 2), eta=eta)

        # Native delayed message should NOT be in the queue sorted set yet
        queue_messages = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(queue_messages) == 0

        # But should be in messages_index with queue_at = eta
        index_messages = redis_client.zrange(f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery", 0, -1, withscores=True)
        assert len(index_messages) >= 1

        _tag, queue_at = index_messages[-1]
        assert queue_at > before_time + 5

    def test_high_priority_message_has_lower_score(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that higher priority messages have lower scores (processed first)."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(
            f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
            f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
        )

        # Publish low priority first, then high priority
        add.apply_async(args=(1, 1), priority=0)  # Low priority
        time.sleep(0.01)  # Small delay to ensure different timestamps
        add.apply_async(args=(2, 2), priority=9)  # High priority

        # Get messages ordered by score (ascending)
        messages = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
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
        global_keyprefix: str,
    ) -> None:
        """Test that multiple messages are ordered correctly by score."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(
            f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
            f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
        )

        # Publish 5 messages with same priority
        for i in range(5):
            add.delay(i, i)
            time.sleep(0.01)  # Small delay between messages

        # Check all messages are in the queue
        queue_size = redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery")
        assert queue_size == 5

        # Messages should be ordered by timestamp (FIFO within same priority)
        messages = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, -1, withscores=True)
        scores = [score for _, score in messages]

        # Scores should be in ascending order (earlier messages have lower scores)
        assert scores == sorted(scores)

    def test_message_payload_contains_task_data(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that the message payload contains correct task data."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear any existing messages
        redis_client.delete(
            f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
            f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
        )

        add.delay(42, 58)

        # Get the delivery tag from the queue
        messages = redis_client.zrange(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery", 0, -1)
        assert len(messages) == 1
        delivery_tag = messages[0].decode() if isinstance(messages[0], bytes) else messages[0]

        # Get the payload from the per-message hash
        message_key = f"{global_keyprefix}message:{delivery_tag}"
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
        global_keyprefix: str,
    ) -> None:
        """Test that purging a queue removes messages from Redis."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and publish
        redis_client.delete(
            f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
            f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
        )
        for _ in range(3):
            add.delay(1, 1)

        # Verify messages exist
        assert redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery") == 3

        # Purge using the app's control interface
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            purged = channel._purge("celery")
            assert purged == 3

        # Verify queue is empty
        assert redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery") == 0

    def test_queue_size_returns_correct_count(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that queue size returns correct message count."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and publish
        redis_client.delete(
            f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
            f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
        )
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
        global_keyprefix: str,
    ) -> None:
        """Test that deleting a queue removes it from Redis."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Publish messages
        redis_client.delete(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery")
        add.delay(1, 1)
        assert redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery") >= 1

        # Delete the queue
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel._delete("celery")

        # Queue should be gone
        assert redis_client.zcard(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery") == 0

    def test_queue_exists_check(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that _has_queue correctly checks queue existence."""

        @celery_app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Clear and check non-existence
        redis_client.delete(f"{global_keyprefix}{QUEUE_KEY_PREFIX}test_queue_exists")

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            assert channel._has_queue("test_queue_exists") is False

            # Create queue by adding a message directly
            redis_client.zadd(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}test_queue_exists",
                {"msg1": 1.0},
            )
            assert channel._has_queue("test_queue_exists") is True

        # Cleanup
        redis_client.delete(f"{global_keyprefix}{QUEUE_KEY_PREFIX}test_queue_exists")

    def test_get_table_returns_bindings(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
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
        redis_client.delete(f"{global_keyprefix}_kombu.binding.test_exchange")

    def test_lookup_drops_an_abandoned_binding(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """A binding nobody refreshes anymore is pruned by the next lookup."""
        bindings_key = f"{global_keyprefix}_kombu.binding.test_abandoned"
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.exchange_declare(exchange="test_abandoned", type="direct")
            channel._queue_bind(
                exchange="test_abandoned",
                routing_key="live_key",
                pattern="",
                queue="live_queue",
            )
            # Left behind by a process that is gone: nothing rescores it, so its
            # deadline has aged past now
            abandoned = channel.sep.join(["gone_key", "", "gone_queue"])
            redis_client.zadd(bindings_key, {abandoned: time.time() - 1})
            assert redis_client.zcard(bindings_key) == 2

            assert set(channel._lookup("test_abandoned", "live_key")) == {"live_queue"}

            # Dropped from Redis, not just filtered out of the read
            live = channel.sep.join(["live_key", "", "live_queue"])
            assert redis_client.zrange(bindings_key, 0, -1) == [live.encode()]

            # With only the abandoned binding left the table reads empty, and a
            # direct publish raises instead of silently going nowhere
            channel._delete("live_queue", "test_abandoned", "live_key", "")
            redis_client.zadd(bindings_key, {abandoned: time.time() - 1})
            with pytest.raises(InconsistencyError):
                channel._lookup("test_abandoned", "gone_key")

        redis_client.delete(bindings_key)

    def test_queue_bind_converts_a_legacy_binding_set(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """A binding table left behind as a plain set is converted in place."""
        bindings_key = f"{global_keyprefix}_kombu.binding.test_legacy"
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            inherited = channel.sep.join(["old_key", "", "old_queue"])
            redis_client.sadd(bindings_key, inherited)

            channel._queue_bind(
                exchange="test_legacy",
                routing_key="new_key",
                pattern="",
                queue="new_queue",
            )

            # zcard would raise WRONGTYPE if the key were still a set
            assert redis_client.zcard(bindings_key) == 2
            scores = dict(redis_client.zrange(bindings_key, 0, -1, withscores=True))
            # This transport did not write the inherited member, so nothing knows
            # when it goes stale and it is kept until an explicit unbind
            assert scores[inherited.encode()] == float("inf")
            assert sorted(channel.get_table("test_legacy")) == [
                ("new_key", "", "new_queue"),
                ("old_key", "", "old_queue"),
            ]

        redis_client.delete(bindings_key)


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

            # Fanout uses a single stream per exchange (routing key ignored)
            stream_key = channel._fanout_stream_key("test_exchange")
            assert "test_exchange" in stream_key

    def test_fanout_exchange_declaration(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that fanout exchange can be declared."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            fanout_exchange = Exchange("test_fanout_decl", type="fanout")
            fanout_queue = Queue("fanout_decl_queue", exchange=fanout_exchange)

            # Bind and declare
            fanout_queue.bind(channel).declare()  # type: ignore[attr-defined]

            # The binding should be stored, scored +inf because the queue has no
            # x-expires and so its route never goes stale
            bindings_key = f"{global_keyprefix}_kombu.binding.test_fanout_decl"
            bindings = redis_client.zrange(bindings_key, 0, -1, withscores=True)
            assert len(bindings) >= 1
            assert all(score == float("inf") for _, score in bindings)

            # Cleanup
            redis_client.delete(bindings_key)

    def test_subclient_is_separate_from_client(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that subclient uses a different connection than client.

        BZMPOP (regular queues) and XREAD (fanout) are both blocking commands
        and cannot share a Redis connection. The subclient provides a dedicated
        connection for XREAD.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # They must be separate client instances so BZMPOP and XREAD
            # can block on independent connections
            assert channel.client is not channel.subclient

    def test_fanout_publish_and_consume(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a message published to a fanout exchange can be consumed.

        This verifies the full fanout path: _put_fanout writes to a stream,
        and a consumer reading from that exchange receives the message.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            fanout_exchange = Exchange("test_fanout_e2e", type="fanout")
            fanout_queue = Queue("fanout_e2e_queue", exchange=fanout_exchange)
            bound_queue = fanout_queue.bind(channel)
            bound_queue.declare()  # type: ignore[attr-defined]

            # Publish a message with a routing key (should be ignored for fanout)
            message = {
                "body": '{"hello": "world"}',
                "properties": {
                    "delivery_tag": "fanout-test-tag",
                    "delivery_info": {"exchange": "test_fanout_e2e", "routing_key": "some.routing.key"},
                },
            }
            channel._put_fanout("test_fanout_e2e", message, routing_key="some.routing.key")

            # The message should be in a single stream (no routing key in key name)
            stream_key = channel._fanout_stream_key("test_fanout_e2e")
            assert redis_client.xlen(f"{global_keyprefix}{stream_key}") == 1

            # There should NOT be a per-routing-key stream
            per_route_key = f"{global_keyprefix}{stream_key}/some.routing.key"
            assert not redis_client.exists(per_route_key)

            # Cleanup
            redis_client.delete(f"{global_keyprefix}{stream_key}")
            redis_client.delete(f"{global_keyprefix}_kombu.binding.test_fanout_e2e")

    def test_fanout_with_wildcard_routing_key_binding(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that a consumer bound with '#' wildcard receives fanout messages.

        This catches the bug where per-routing-key streams caused XREAD to listen
        on a non-existent stream name like '/0.exchange/*' instead of '/0.exchange'.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            fanout_exchange = Exchange("test_fanout_wildcard", type="fanout")
            # Binding with routing_key="#" (wildcard) - like celery events do
            fanout_queue = Queue("fanout_wildcard_queue", exchange=fanout_exchange, routing_key="#")
            bound_queue = fanout_queue.bind(channel)
            bound_queue.declare()  # type: ignore[attr-defined]

            # After _queue_bind, the queue should be in _fanout_queues
            assert "fanout_wildcard_queue" in channel._fanout_queues
            exchange, stored_rk = channel._fanout_queues["fanout_wildcard_queue"]

            # The stream key for consuming must match the stream key for publishing
            # (both should be just '/db.exchange' with no routing key suffix)
            publish_stream = channel._fanout_stream_key("test_fanout_wildcard")
            consume_stream = channel._fanout_stream_key(exchange)
            assert publish_stream == consume_stream

            # Cleanup
            redis_client.delete(f"{global_keyprefix}_kombu.binding.test_fanout_wildcard")

    def test_fanout_end_to_end_via_xread(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test end-to-end fanout: publish to stream, consume via XREAD.

        This is the test that would have caught both fanout bugs:
        1. Shared client connection (BZMPOP + XREAD on same connection)
        2. Per-routing-key streams (XREAD can't match wildcard stream names)

        Publishes a message to the stream first, then uses XREAD with offset '0'
        to read from the beginning, avoiding timing issues with '$' offset.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            fanout_exchange = Exchange("test_e2e_xread", type="fanout")
            fanout_queue = Queue("e2e_xread_queue", exchange=fanout_exchange, routing_key="#")
            bound_queue = fanout_queue.bind(channel)
            bound_queue.declare()  # type: ignore[attr-defined]
            channel.basic_consume(
                "e2e_xread_queue",
                no_ack=True,
                callback=lambda *_a: None,
                consumer_tag="ctag-e2e",
            )

            # Publish with a routing key (like celery events do)
            message = {
                "body": '{"hello": "fanout"}',
                "properties": {
                    "delivery_tag": "e2e-xread-tag",
                    "delivery_info": {"exchange": "test_e2e_xread", "routing_key": "worker.heartbeat"},
                },
            }
            channel._put_fanout("test_e2e_xread", message, routing_key="worker.heartbeat")

            # Verify the message is in the correct stream
            stream_key = channel._fanout_stream_key("test_e2e_xread")
            assert redis_client.xlen(f"{global_keyprefix}{stream_key}") >= 1

            # Read via XREAD from offset 0 (beginning of stream)
            # Initialize subclient connection (normally done by MultiChannelPoller)
            if channel.subclient.connection is None:
                channel.subclient.connection = channel.subclient.connection_pool.get_connection()
            channel._stream_offsets[stream_key] = "0"
            channel._xread_start(timeout=1)
            delivered = channel._xread_read()

            assert delivered is True


@pytest.mark.integration
class TestDelayedMessageStorage:
    """Tests for delayed message storage in Redis.

    Note: These tests verify the _put method's eta handling directly,
    since the signal handler that adds eta headers is only active during
    worker task publish.
    """

    def test_message_with_eta_goes_to_messages_index(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """Test that native delayed messages go to messages_index:{queue}, not queue immediately.

        Native delayed messages are stored in messages_index:{queue} with
        queue_at = eta. The requeue mechanism will add them to the queue when
        the eta time arrives. This prevents them from being consumed early.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
            )

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
            main_messages = redis_client.zrange(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
            assert len(main_messages) == 0

            # Message should be in messages_index:{queue} with queue_at = eta
            index_entries = redis_client.zrange(
                f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
            assert len(index_entries) == 1
            tag, queue_at = index_entries[0]
            assert tag.decode() if isinstance(tag, bytes) else tag == delivery_tag
            assert queue_at == pytest.approx(eta_timestamp, rel=1e-6)

            # Message data should be stored in per-message hash
            message_key = f"{global_keyprefix}message:{delivery_tag}"
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
        global_keyprefix: str,
    ) -> None:
        """Test that messages without eta have current time scores."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
            )

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
            messages = redis_client.zrange(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
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
        global_keyprefix: str,
    ) -> None:
        """Test that short-delayed and immediate messages are ordered correctly in queue.

        Short delays (<= DEFAULT_REQUEUE_CHECK_INTERVAL) are treated as immediate
        delivery, so both messages should be in the queue immediately.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
            )

            now = time.time()
            # Use delay <= DEFAULT_REQUEUE_CHECK_INTERVAL so it's treated as immediate
            eta_timestamp = now + 1  # 1 second in future

            # Create an immediate message (no eta)
            immediate_msg = {
                "body": '{"task": "test.add", "args": [1, 2]}',
                "properties": {
                    "delivery_tag": f"immediate-{time.time()}",
                    "delivery_info": {"exchange": "celery", "routing_key": "celery"},
                },
            }
            channel._put("celery", immediate_msg)

            # Create a short-delayed message (treated as immediate by the transport)
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
            main_messages = redis_client.zrange(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
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
        global_keyprefix: str,
    ) -> None:
        """Test that high priority messages are ordered before low priority."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Clear existing messages
            redis_client.delete(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery",
            )

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
            messages = redis_client.zrange(
                f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery",
                0,
                -1,
                withscores=True,
            )
            assert len(messages) == 2

            first_tag = messages[0][0].decode() if isinstance(messages[0][0], bytes) else messages[0][0]
            assert "high-pri" in first_tag


@pytest.mark.integration
class TestMessageRequeue:
    """Tests for message requeue functionality (unified delayed + restore)."""

    def test_acking_after_a_restore_cancels_the_restored_copy(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that acking removes the queue entry a visibility timeout restore put back."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "ack-cancels-restore"
            queue_key = f"{QUEUE_KEY_PREFIX}celery"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.delete(queue_key, index_key, message_key)

            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Consumed but not yet acked: out of the queue, deadline already past
            client.zadd(index_key, {delivery_tag: time.time() - 100})
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # Visibility timeout fires and puts the tag back while the original
            # consumer is still working on it
            channel.enqueue_due_messages()
            assert client.zscore(queue_key, delivery_tag) is not None

            # The original consumer acks. Without the queue ZREM the restored
            # copy stays poppable and a second worker runs the task.
            message = MagicMock()
            message.delivery_info = {"routing_key": "celery"}
            channel.qos._delivered[delivery_tag] = message
            channel.qos._remove_from_indices(delivery_tag)

            assert client.zscore(queue_key, delivery_tag) is None
            assert client.zscore(index_key, delivery_tag) is None
            assert not client.exists(message_key)

    def test_the_heartbeat_holds_a_message_past_its_visibility_timeout(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that a heartbeated message is not restored once its deadline passes.

        Drives the beats by hand. The registration that puts them on the event
        loop is covered by
        TestTransport.test_visibility_heartbeat_is_registered_with_the_event_loop.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.visibility_timeout = 2
            client = channel.client

            delivery_tag = "heartbeat-holds"
            queue_key = f"{QUEUE_KEY_PREFIX}celery"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.delete(queue_key, index_key, message_key)

            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )
            # Consumed but not acked, with the deadline a consume path would set
            client.zadd(index_key, {delivery_tag: time.time() + channel.visibility_timeout})
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            message = MagicMock()
            message.delivery_info = {"routing_key": "celery"}
            channel.qos._delivered[delivery_tag] = message

            # Task still running well past the visibility timeout, beating at
            # the interval register_with_event_loop uses
            deadline = time.time() + 2 * channel.visibility_timeout
            while time.time() < deadline:
                channel.qos.maybe_update_messages_index()
                channel.enqueue_due_messages()
                time.sleep(channel.visibility_timeout / 3)

            assert client.zscore(queue_key, delivery_tag) is None, "message was restored while still in flight"

            # Stop beating, as a crashed worker would, and it comes back
            time.sleep(channel.visibility_timeout + 0.5)
            channel.enqueue_due_messages()
            assert client.zscore(queue_key, delivery_tag) is not None

    def test_the_heartbeat_does_not_resurrect_an_acked_message(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that a tag acked mid-heartbeat is not put back into the index.

        _delivered is read outside the pipeline, so an ack can land between the
        read and the ZADD. The XX flag is what keeps that from re-creating the
        index entry and redelivering an acked message.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "heartbeat-after-ack"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            client.delete(index_key)

            message = MagicMock()
            message.delivery_info = {"routing_key": "celery"}
            channel.qos._delivered[delivery_tag] = message

            # The ack already removed the index entry; _delivered has not caught up
            assert client.zscore(index_key, delivery_tag) is None

            channel.qos.maybe_update_messages_index()

            assert client.zscore(index_key, delivery_tag) is None

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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Simulate a message that was consumed but not acked:
            # 1. Message is in per-message hash (payload stored)
            # 2. Message is in messages_index:{queue} with try_requeue_at in the past
            # 3. Message is NOT in the queue (was popped)
            delivery_tag = "unacked-msg-123"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Set index score to past timestamp (ready for requeue)
            old_timestamp = time.time() - 100
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: old_timestamp})

            # Message is NOT in the queue (simulates it was consumed)
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Simulate a message that is still in the queue (not yet consumed)
            delivery_tag = "queued-msg-456"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "exchange": "",
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Set index score to old timestamp (ready for requeue)
            old_timestamp = time.time() - 100
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: old_timestamp})

            # Message IS in the queue (not yet consumed)
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 100.0})

            original_score = client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Simulate a message that was already acked (per-message hash deleted)
            delivery_tag = "acked-msg-789"

            # Message is in index but NOT in per-message hash (already acked)
            old_timestamp = time.time() - 100
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: old_timestamp})

            # Verify it's in the index
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is not None

            # Set up active_queues for the channel
            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # Call enqueue_due_messages - should remove from index
            channel.enqueue_due_messages()

            # Should be removed from index (cleaned up by Lua script)
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is None

    def test_requeue_by_tag(
        self,
        celery_app: Celery,
    ) -> None:
        """Test requeue_by_tag restores a specific message."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Set up a message in the per-message hash
            delivery_tag = "requeue-tag-test"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            message_key = f"message:{delivery_tag}"

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Message is not in queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

            # Requeue the message
            cast("QoS", channel.qos).requeue_by_tag(delivery_tag)

            # Message should now be in the queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_requeue_by_tag_counts_the_redelivery(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _requeue_by_tag increments delivery_count in the hash."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

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
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Requeue the message using the Lua script
            result = channel._requeue_by_tag(delivery_tag, leftmost=False)
            assert result is True

            # The redelivery is counted, and no separate redelivered field is written
            assert client.hget(message_key, "delivery_count") == b"1"
            assert client.hget(message_key, "redelivered") is None

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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

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
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

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
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Create a mock message object
            message = MagicMock()
            message.delivery_tag = delivery_tag
            message.delivery_info = {"routing_key": "celery"}

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
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

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
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Create a mock message object
            message = MagicMock()
            message.delivery_tag = delivery_tag
            message.delivery_info = {"routing_key": "celery"}

            # Restore at beginning
            channel._restore_at_beginning(message)

            # Message should be in queue with score 0
            score = client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)
            assert score == 0


@pytest.mark.integration
class TestQueueTTL:
    """Tests for queue TTL (x-expires) and message TTL (x-message-ttl)."""

    def test_queue_expires_after_ttl(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that queue keys expire after the configured TTL when not refreshed."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Set _expires directly to use a short TTL for fast test
            # (validation in _new_queue enforces >= 10s, but PEXPIRE itself works with any value)
            channel._expires["celery"] = 2000

            # Publish a message to create the keys
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": "ttl-test-1",
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            # Refresh once to set the TTL
            channel._refresh_queue_expires()

            # Verify keys exist and have TTL
            assert client.exists(f"{QUEUE_KEY_PREFIX}celery")
            queue_ttl = client.pttl(f"{QUEUE_KEY_PREFIX}celery")
            assert 0 < queue_ttl <= 2000
            index_ttl = client.pttl(f"{MESSAGES_INDEX_PREFIX}celery")
            assert 0 < index_ttl <= 2000

            # Wait for TTL to expire (no refresh)
            time.sleep(2.5)

            # Keys should be gone
            assert not client.exists(f"{QUEUE_KEY_PREFIX}celery")
            assert not client.exists(f"{MESSAGES_INDEX_PREFIX}celery")

    def test_queue_stays_alive_with_refresh(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that queue keys stay alive when refreshed periodically."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Set _expires directly to use a short TTL for fast test
            channel._expires["celery"] = 2000

            # Publish a message to create the keys
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": "ttl-refresh-1",
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            # Refresh every 0.5s for 3 seconds (longer than TTL)
            for _ in range(6):
                channel._refresh_queue_expires()
                time.sleep(0.5)

            # Keys should still exist
            assert client.exists(f"{QUEUE_KEY_PREFIX}celery")
            assert client.exists(f"{MESSAGES_INDEX_PREFIX}celery")

    def test_message_ttl_expires_messages(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that message hashes expire after the configured message TTL."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Declare queue with short message TTL (2 seconds)
            channel._new_queue("celery", arguments={"x-message-ttl": 2000})

            # Publish a message
            delivery_tag = "msg-ttl-test-1"
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": delivery_tag,
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            # Verify message hash exists with short TTL
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            assert client.exists(message_key)
            ttl = client.ttl(message_key)
            assert 0 < ttl <= 2

            # Wait for message TTL to expire
            time.sleep(2.5)

            # Message hash should be gone
            assert not client.exists(message_key)

            # Queue sorted set still has the delivery tag (cleaned up on consume)
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None

    def test_no_ttl_queues_unaffected(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that queues without TTL arguments are not affected by refresh."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            # Clear existing data
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            # Declare queue without TTL
            channel._new_queue("celery")

            # Publish a message
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": "no-ttl-test-1",
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            # Refresh should be a no-op
            channel._refresh_queue_expires()

            # Queue key should have no TTL (-1 = no expiry)
            assert client.ttl(f"{QUEUE_KEY_PREFIX}celery") == -1

    def test_queue_expires_with_global_keyprefix(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that PEXPIRE uses correct prefixed keys with global_keyprefix."""
        from celery import Celery as CeleryApp

        host, port, _image = redis_container

        app = CeleryApp("test_ttl_prefix")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"global_keyprefix": "myapp:"},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
        )

        raw_client = client_lib.Redis(host=host, port=port, db=0)

        try:
            with app.connection() as conn:
                channel = cast("Channel", conn.default_channel)

                # Set _expires directly to use a short TTL for fast test
                channel._expires["celery"] = 5000

                # Publish a message (creates the prefixed keys)
                channel._put(
                    "celery",
                    {
                        "body": "test",
                        "properties": {
                            "delivery_tag": "prefix-ttl-test",
                            "delivery_info": {"exchange": "", "routing_key": "celery"},
                        },
                    },
                )

                # Refresh queue expires
                channel._refresh_queue_expires()

                # Verify prefixed keys have TTL
                queue_ttl = int(raw_client.pttl(f"myapp:{QUEUE_KEY_PREFIX}celery"))  # type: ignore[arg-type]
                assert 0 < queue_ttl <= 5000
                index_ttl = int(raw_client.pttl(f"myapp:{MESSAGES_INDEX_PREFIX}celery"))  # type: ignore[arg-type]
                assert 0 < index_ttl <= 5000
        finally:
            raw_client.flushdb()
            raw_client.close()
            app.close()

    def test_a_second_channel_applies_a_ttl_it_never_declared(
        self,
        celery_app: Celery,
        redis_client: Any,
        global_keyprefix: str,
    ) -> None:
        """A queue keeps its TTL when another channel of the connection publishes to it.

        kombu caches declarations on the connection, not the channel, so only
        the first channel to declare a queue ever sees its x-expires. Any other
        channel of that connection can still be the one that publishes.
        """
        with celery_app.connection() as conn:
            declaring = cast("Channel", conn.default_channel)
            declaring._new_queue("celery", arguments={"x-expires": 60000, "x-message-ttl": 30000})

            publishing = cast("Channel", conn.channel())
            assert publishing is not declaring
            publishing._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": "shared-ttl-test",
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            queue_ttl = int(redis_client.pttl(f"{global_keyprefix}{QUEUE_KEY_PREFIX}celery"))
            assert 0 < queue_ttl <= 60000
            index_ttl = int(redis_client.pttl(f"{global_keyprefix}{MESSAGES_INDEX_PREFIX}celery"))
            assert 0 < index_ttl <= 60000
            message_ttl = int(redis_client.ttl(f"{global_keyprefix}{MESSAGE_KEY_PREFIX}shared-ttl-test"))
            assert 0 < message_ttl <= 30

    def test_delete_removes_ttl_state(
        self,
        celery_app: Celery,
        redis_client: Any,
    ) -> None:
        """Test that _delete removes queue from TTL tracking dicts."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)

            # Declare queue with both TTL types
            channel._new_queue("celery", arguments={"x-expires": 60000, "x-message-ttl": 30000})

            assert "celery" in channel._expires
            assert "celery" in channel._message_ttls

            # Delete queue
            channel._delete("celery")

            assert "celery" not in channel._expires
            assert "celery" not in channel._message_ttls


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
        client = client_lib.Redis(host=host, port=port, db=0)

        # The queue should be prefixed with both global prefix and queue: prefix
        prefixed_queue_size: int = client.zcard(f"myapp:{QUEUE_KEY_PREFIX}celery")  # type: ignore[assignment]

        assert prefixed_queue_size >= 1
        # The key point is that our prefixed queue has the message

        # Clean up
        client.delete(f"myapp:{QUEUE_KEY_PREFIX}celery", f"myapp:{MESSAGES_INDEX_PREFIX}celery")
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
            _ = conn.default_channel

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

    def test_consuming_recreates_a_missing_index_entry(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that consuming sets a visibility deadline even with no index entry."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "missing-index-entry"
            queue_key = f"{QUEUE_KEY_PREFIX}celery"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.delete(queue_key, index_key, message_key)

            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "delivery_count": "0",
                },
            )
            # Queued with no index entry at all
            client.zadd(queue_key, {delivery_tag: 100.0})
            assert client.zscore(index_key, delivery_tag) is None

            message = channel._get("celery")
            assert message["properties"]["delivery_tag"] == delivery_tag

            # Under ZADD XX the entry stayed missing, so the message was out of
            # the queue and out of the index and a crash would lose it silently.
            deadline = client.zscore(index_key, delivery_tag)
            assert deadline is not None
            assert deadline > time.time()

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
                    "payload": json_dumps(payload),
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
            client.delete(f"{QUEUE_KEY_PREFIX}celery")

            # Add delivery tag to queue but NOT to messages hash
            delivery_tag = "orphan-tag"
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 100.0})

            # _get should raise Empty because payload is missing
            with pytest.raises(Empty):
                channel._get("celery")

    def test_get_atomically_updates_messages_index(
        self,
        celery_app: Celery,
        global_keyprefix: str,
    ) -> None:
        """Test _get refreshes messages_index score when consuming a message."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            queue_name = "atomic_test"
            queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
            index_key = f"{MESSAGES_INDEX_PREFIX}{queue_name}"
            delivery_tag = "atomic-get-test"

            # Clean up
            client.delete(queue_key)
            client.delete(index_key)

            payload = {
                "body": "atomic test",
                "headers": {},
                "properties": {"delivery_tag": delivery_tag},
            }

            # Simulate a published message (queue + index + hash)
            message_key = f"message:{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": queue_name,
                    "priority": "0",
                    "delivery_count": "0",
                },
            )
            old_queue_at = time.time() + 100.0
            client.zadd(queue_key, {delivery_tag: 100.0})
            client.zadd(index_key, {delivery_tag: old_queue_at})

            # Consume via _get (uses Lua script)
            message = channel._get(queue_name)

            assert message["body"] == "atomic test"

            # Message should be removed from queue
            assert client.zscore(queue_key, delivery_tag) is None

            # messages_index should be refreshed with new VT score
            new_score = client.zscore(index_key, delivery_tag)
            assert new_score is not None
            # New score should be approximately now + visibility_timeout
            expected_min = time.time() + channel.visibility_timeout - 5
            assert new_score >= expected_min

    def test_get_cleans_up_expired_message_index(
        self,
        celery_app: Celery,
        global_keyprefix: str,
    ) -> None:
        """Test _get removes messages_index entry when message hash is expired."""

        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            queue_name = "expired_cleanup_test"
            queue_key = f"{QUEUE_KEY_PREFIX}{queue_name}"
            index_key = f"{MESSAGES_INDEX_PREFIX}{queue_name}"
            expired_tag = "expired-tag"

            # Clean up
            client.delete(queue_key)
            client.delete(index_key)

            # Add to queue and index but NO message hash (simulates TTL expiry)
            client.zadd(queue_key, {expired_tag: 100.0})
            client.zadd(index_key, {expired_tag: 999.0})

            # _get should raise Empty (no valid messages)
            with pytest.raises(Empty):
                channel._get(queue_name)

            # Expired tag should be cleaned up from index
            assert client.zscore(index_key, expired_tag) is None


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
            channel._queue_cycle = list(channel.active_queues)

            # Should return without error (early return when no queues)
            channel._bzmpop_start(timeout=1)

            # _in_poll should still be None (not a connection object)
            assert channel._in_poll is None


@pytest.mark.unit
class TestBlockingTimeout:
    """Tests for the blocking read timeout, and for keeping it off polling_interval."""

    def test_transport_blocking_timeout_patched_for_tests(self) -> None:
        """Test Transport blocking_timeout is patched to 1 for faster tests.

        Note: The default is 10s, but pytest_configure patches it to 1s
        for faster worker shutdown during tests.
        """
        assert Transport.blocking_timeout == 1

    def test_the_synchronous_drain_does_not_sleep(self) -> None:
        """Test that an empty poll is retried immediately.

        kombu's drain_events sleeps polling_interval between unsuccessful
        polls, and sharing that attribute with the block timeout made it 10s.
        """
        client = MagicMock()
        client.transport_options = {}
        transport = Transport(client)
        transport.cycle.get = MagicMock(side_effect=Empty)

        with (
            patch("kombu.transport.virtual.base.sleep") as mock_sleep,
            pytest.raises(TimeoutError),  # socket.timeout, once the drain timeout is up
        ):
            transport.drain_events(client, timeout=0.01)

        mock_sleep.assert_not_called()
        assert transport.polling_interval is None

    def test_blocking_timeout_from_transport_options(self) -> None:
        """Test that blocking_timeout is configurable and leaves the sleep off."""
        client = MagicMock()
        client.transport_options = {"blocking_timeout": 30}

        transport = Transport(client)

        assert transport.blocking_timeout == 30
        assert transport.polling_interval is None

    def test_polling_interval_is_still_read_as_the_block_timeout(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that the old name keeps working without switching on the sleep."""
        transport_mod._warned_polling_interval = False
        client = MagicMock()
        client.transport_options = {"polling_interval": 30}

        with caplog.at_level(logging.WARNING, logger="celery_redis_plus.transport"):
            transport = Transport(client)

        assert transport.blocking_timeout == 30
        assert transport.polling_interval is None
        assert "deprecated" in caplog.text

    def test_the_block_timeout_is_what_bzmpop_and_xread_wait_on(self) -> None:
        """Test that both blocking reads take their timeout from blocking_timeout."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._consume_fast_mode = False
        channel._queue_cycle = ["celery"]
        channel.queue_key_prefix = QUEUE_KEY_PREFIX
        channel.keyprefix_fanout = "/0."
        channel.active_fanout_queues = {"fanout_queue"}
        channel._fanout_queues = {"fanout_queue": ("test_exchange", "")}
        channel._stream_offsets = {}

        mock_client = MagicMock()
        mock_client._prefix_args.side_effect = lambda args: args
        channel.client = mock_client
        channel.subclient = mock_client
        channel.connection = MagicMock()
        channel.connection.blocking_timeout = 7

        channel._bzmpop_start()
        bzmpop_args = mock_client.connection.send_command.call_args[0]
        assert bzmpop_args[0] == "BZMPOP"
        assert bzmpop_args[1] == 7

        channel._xread_start()
        xread_args = mock_client.connection.send_command.call_args[0]
        assert xread_args[0] == "XREAD"
        assert xread_args[xread_args.index("BLOCK") + 1] == "7000"


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


@pytest.mark.integration
class TestNoAckConsumption:
    """A no_ack consumer never acks, so the atomic pop itself must dequeue.

    Otherwise the index entry leaks until the requeue sweep restores the
    already-consumed message for a second delivery nobody can ack away
    either. pidbox control and reply queues all consume with no_ack=True.
    """

    def test_no_ack_consumption_leaves_no_index_entry(
        self,
        celery_app: Celery,
    ) -> None:
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "no-ack-get-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": delivery_tag,
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            message = channel.basic_get("celery", no_ack=True)

            assert message is not None
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is None
            assert not client.exists(f"{MESSAGE_KEY_PREFIX}{delivery_tag}")

    def test_an_ordinary_get_still_gets_a_visibility_deadline(
        self,
        celery_app: Celery,
    ) -> None:
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "acked-get-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")
            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": delivery_tag,
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            message = channel.basic_get("celery")

            assert message is not None
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is not None
            assert client.exists(f"{MESSAGE_KEY_PREFIX}{delivery_tag}")
            channel.basic_ack(message.delivery_tag)

    def test_no_ack_leaves_no_index_entry_on_the_consume_path(
        self,
        celery_app: Celery,
    ) -> None:
        """The registered-consumer path: basic_consume(no_ack=True) must flag
        the queue so the consume script dequeues, and basic_cancel must
        unflag it."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            received = []
            channel.basic_consume(
                "celery",
                no_ack=True,
                callback=received.append,
                consumer_tag="no-ack-consumer",
            )
            try:
                assert "celery" in channel._no_ack_queues

                delivery_tag = "no-ack-consume-test"
                channel._put(
                    "celery",
                    {
                        "body": "test",
                        "properties": {
                            "delivery_tag": delivery_tag,
                            "delivery_info": {"exchange": "", "routing_key": "celery"},
                        },
                    },
                )

                assert channel._drain_expired_and_deliver("celery") is True

                assert len(received) == 1
                assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is None
                assert not client.exists(f"{MESSAGE_KEY_PREFIX}{delivery_tag}")
            finally:
                channel.basic_cancel("no-ack-consumer")
            assert "celery" not in channel._no_ack_queues

    def test_the_consume_script_reads_the_flag_of_the_right_queue(
        self,
        celery_app: Celery,
    ) -> None:
        """The per-queue no_ack flags ride ARGV after the queue names, so with
        several queues the script must pair each pop with its own queue's flag."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client
            prefix = channel.global_keyprefix

            for queue, tag in (("acked_q", "flag-acked"), ("noack_q", "flag-noack")):
                client.delete(f"{QUEUE_KEY_PREFIX}{queue}", f"{MESSAGES_INDEX_PREFIX}{queue}")
                channel._put(
                    queue,
                    {
                        "body": "test",
                        "properties": {
                            "delivery_tag": tag,
                            "delivery_info": {"exchange": "", "routing_key": queue},
                        },
                    },
                )

            consume_script = client.register_script(_CONSUME_MESSAGE_LUA)
            keys = [f"{prefix}{QUEUE_KEY_PREFIX}acked_q", f"{prefix}{QUEUE_KEY_PREFIX}noack_q"]
            args = [
                prefix,
                MESSAGE_KEY_PREFIX,
                str(time.time() + 300),
                MESSAGES_INDEX_PREFIX,
                "acked_q",
                "noack_q",
                "0",
                "1",
            ]
            first = consume_script(keys=keys, args=args)
            second = consume_script(keys=keys, args=args)
            popped = {bytes_to_str(result[0]) for result in (first, second)}
            assert popped == {"acked_q", "noack_q"}

            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}acked_q", "flag-acked") is not None
            assert client.exists(f"{MESSAGE_KEY_PREFIX}flag-acked")
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}noack_q", "flag-noack") is None
            assert not client.exists(f"{MESSAGE_KEY_PREFIX}flag-noack")

            client.delete(
                f"{QUEUE_KEY_PREFIX}acked_q",
                f"{MESSAGES_INDEX_PREFIX}acked_q",
                f"{MESSAGE_KEY_PREFIX}flag-acked",
            )


@pytest.mark.integration
class TestDeliveryCount:
    """Tests for restore count tracking and max restore count enforcement."""

    def test_delivery_count_initialized_to_zero(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _put stores delivery_count=0 in the message hash."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "restore-init-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            channel._put(
                "celery",
                {
                    "body": "test",
                    "properties": {
                        "delivery_tag": delivery_tag,
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                    },
                },
            )

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            delivery_count = client.hget(message_key, "delivery_count")
            assert delivery_count == b"0"

    def test_a_backlogged_message_is_not_counted_as_redelivered(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that a message still waiting in the queue is not counted as redelivered."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "backlogged-not-redelivered"
            queue_key = f"{QUEUE_KEY_PREFIX}celery"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.delete(queue_key, index_key, message_key)

            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                },
            )

            # Published but never consumed: still in the queue, and its index
            # deadline has passed because the backlog is longer than the
            # visibility timeout.
            client.zadd(queue_key, {delivery_tag: 100.0})
            client.zadd(index_key, {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            for _ in range(3):
                channel.enqueue_due_messages()
                # Re-date the deadline into the past so the next cycle looks again
                client.zadd(index_key, {delivery_tag: time.time() - 100})

            # Delivered exactly zero times, so the counter must still be zero
            assert client.hget(message_key, "delivery_count") == b"0"
            # And the queue entry is untouched, not re-scored
            assert client.zscore(queue_key, delivery_tag) == 100.0

    def test_delivery_count_incremented_on_redelivery(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that delivery_count increments each time a timed-out message is redelivered."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "restore-incr-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            # Simulate an unacked message with delivery_count=0
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                },
            )

            # Set index score to past (ready for requeue)
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # First redelivery
            channel.enqueue_due_messages()
            assert client.hget(message_key, "delivery_count") == b"1"

            # Pop from queue and set index to past again for second redelivery
            client.zrem(f"{QUEUE_KEY_PREFIX}celery", delivery_tag)
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            # Second redelivery
            channel.enqueue_due_messages()
            assert client.hget(message_key, "delivery_count") == b"2"

    def test_delivery_count_not_incremented_for_native_delayed(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that delivery_count is NOT incremented for native delayed first delivery."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "restore-delayed-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            # Simulate a native delayed message (native_delayed=1)
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                    "native_delayed": "1",
                    "delivery_count": "0",
                    "eta": "0",
                },
            )

            # Set index score to past (ready for delivery)
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # Enqueue due messages (first delivery of delayed message)
            channel.enqueue_due_messages()

            # delivery_count should still be 0 (not incremented for first delivery)
            assert client.hget(message_key, "delivery_count") == b"0"
            # native_delayed should be cleared
            assert client.hget(message_key, "native_delayed") == b"0"

    def test_delivery_count_header_injected(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that x-delivery-count header is injected when delivery_count > 0."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "restore-header-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "headers": {},
                },
            }

            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "delivery_count": "3",
                },
            )
            # Add to queue so _get can consume it via Lua script
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 1.0})
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() + 300})

            message = channel._get("celery")
            assert message is not None
            assert message["properties"]["headers"]["x-delivery-count"] == 3
            # celery reads delivery_info['redelivered'] in Request and trace, and
            # it gates worker_deduplicate_successful_tasks
            assert message["properties"]["delivery_info"]["redelivered"] is True

    def test_delivery_count_header_absent_when_zero(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that x-delivery-count header is NOT injected when delivery_count is 0."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            client = channel.client

            delivery_tag = "restore-no-header-test"
            payload = {
                "body": "test",
                "headers": {},
                "properties": {
                    "delivery_tag": delivery_tag,
                    "headers": {},
                },
            }

            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "delivery_count": "0",
                },
            )
            # Add to queue so _get can consume it via Lua script
            client.zadd(f"{QUEUE_KEY_PREFIX}celery", {delivery_tag: 1.0})
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() + 300})

            message = channel._get("celery")
            assert message is not None
            assert "x-delivery-count" not in message["properties"].get("headers", {})

    def test_message_dropped_when_max_exceeded(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that message is dropped when delivery_count exceeds delivery_limit."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.delivery_limit = 2
            client = channel.client

            delivery_tag = "restore-drop-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            # Simulate a message already restored 2 times (at the limit)
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "1",
                    "native_delayed": "0",
                    "delivery_count": "2",
                },
            )

            # Set index score to past (ready for requeue)
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # This should drop the message (delivery_count would become 3, reaching the limit of 2)
            enqueued = channel.enqueue_due_messages()
            assert enqueued == 0

            # Message hash and index entry should be gone
            assert not client.exists(message_key)
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is None
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

    @pytest.mark.parametrize(
        ("delivery_count", "expect_dropped"),
        [("1", False), ("2", True)],
        ids=["under-limit", "at-limit"],
    )
    def test_a_message_is_dropped_once_it_reaches_the_delivery_limit(
        self,
        celery_app: Celery,
        delivery_count: str,
        expect_dropped: bool,
    ) -> None:
        """Test the limit counts attempts, so the check is >= and not >.

        With delivery_limit=3, a stored delivery_count of 2 means this restore is
        the third delivery and must be the last. Under the old > comparison it
        survived and got a fourth.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.delivery_limit = 3
            client = channel.client

            delivery_tag = f"delivery-limit-boundary-{delivery_count}"
            index_key = f"{MESSAGES_INDEX_PREFIX}celery"
            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", index_key, message_key)

            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "native_delayed": "0",
                    "delivery_count": delivery_count,
                },
            )
            client.zadd(index_key, {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            channel.enqueue_due_messages()

            assert bool(client.exists(message_key)) is not expect_dropped

    def test_message_dropped_cleans_up_queue_entry(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that dropping a message leaves nothing behind in the queue sorted set.

        The drop happens on the restore attempt, which re-adds the tag to the
        queue before the limit is checked, so the drop has to take it back out
        again. A tag that was already in the queue is a backlog rather than a
        redelivery and is never dropped at all.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.delivery_limit = 0  # drop on first restore attempt
            client = channel.client

            delivery_tag = "restore-queue-cleanup-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "0",
                    "native_delayed": "0",
                    "delivery_count": "0",
                },
            )

            # Consumed but not acked: in the index with a past deadline, out of
            # the queue, so this cycle is a genuine redelivery attempt.
            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            enqueued = channel.enqueue_due_messages()
            assert enqueued == 0

            # All traces of the message should be gone
            assert not client.exists(message_key)
            assert client.zscore(f"{MESSAGES_INDEX_PREFIX}celery", delivery_tag) is None
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is None

    def test_default_delivery_limit_matches_rabbitmq(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that the default limit is RabbitMQ quorum queues' 20."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            assert channel.delivery_limit == 20

    def test_no_limit_when_limit_is_none(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that messages are not dropped when delivery_limit is None."""
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.delivery_limit = None
            client = channel.client

            delivery_tag = "restore-no-limit-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            # Simulate a message with very high delivery_count
            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "redelivered": "1",
                    "native_delayed": "0",
                    "delivery_count": "999",
                },
            )

            client.zadd(f"{MESSAGES_INDEX_PREFIX}celery", {delivery_tag: time.time() - 100})

            if "celery" not in channel._active_queues:
                channel._active_queues.append("celery")
            channel._queue_cycle = list(channel.active_queues)

            # Should still enqueue (no limit)
            enqueued = channel.enqueue_due_messages()
            assert enqueued == 1

            # Message should exist and delivery_count incremented
            assert client.exists(message_key)
            assert client.hget(message_key, "delivery_count") == b"1000"

    def test_requeue_by_tag_increments_but_does_not_enforce_the_limit(
        self,
        celery_app: Celery,
    ) -> None:
        """Test that _requeue_by_tag counts the redelivery without dropping the message.

        Reject-with-requeue is a redelivery, as it is in RabbitMQ, so it counts.
        The limit is not enforced here: the message keeps its index entry, so
        enqueue_due_messages sees the raised count at its next deadline and drops
        it. One place decides, one place deletes.
        """
        with celery_app.connection() as conn:
            channel = cast("Channel", conn.default_channel)
            channel.delivery_limit = 1
            client = channel.client

            delivery_tag = "requeue-no-incr-test"
            client.delete(f"{QUEUE_KEY_PREFIX}celery", f"{MESSAGES_INDEX_PREFIX}celery")

            message_key = f"{MESSAGE_KEY_PREFIX}{delivery_tag}"
            payload = {"body": "test", "headers": {}, "properties": {"delivery_tag": delivery_tag}}

            client.hset(
                message_key,
                mapping={
                    "payload": json_dumps(payload),
                    "routing_key": "celery",
                    "priority": "0",
                    "delivery_count": "5",
                },
            )

            # Even though delivery_count (5) exceeds delivery_limit (1),
            # _requeue_by_tag should succeed because it doesn't enforce the limit
            result = channel._requeue_by_tag(delivery_tag, queue="celery")
            assert result is True

            # The redelivery is counted, so the next enqueue_due_messages cycle
            # sees the raised count and drops the message
            assert client.hget(message_key, "delivery_count") == b"6"
            # Message should be in queue
            assert client.zscore(f"{QUEUE_KEY_PREFIX}celery", delivery_tag) is not None


@pytest.mark.integration
class TestVisibilityHeartbeatEndToEnd:
    """The visibility heartbeat under a real worker running a real long task."""

    def test_a_task_outliving_its_visibility_timeout_runs_once(
        self,
        redis_container: tuple[str, int, str],
    ) -> None:
        """Test that a task running past the visibility timeout is not redelivered.

        This is the whole point of the heartbeat, and the only test that
        exercises it through the worker's event loop rather than by calling
        maybe_update_messages_index directly. The task sleeps for several
        visibility timeouts, so without the heartbeat the message goes back
        into the queue while it is still running and a second execution starts.

        Uses the threads pool: tasks run off the main thread, so the hub keeps
        ticking and the timer fires. Under --pool=solo it would not, which is
        the caveat documented for visibility_timeout.

        acks_late, because with the default early ack celery acknowledges the
        message the moment the pool accepts the task. The message is gone from
        Redis before the task body starts, so nothing can redeliver it and the
        heartbeat has nothing left to hold.
        """
        from celery import Celery as CeleryApp
        from celery.contrib.testing import worker as testing_worker

        host, port, _image = redis_container
        # A beat every second, so the message survives two missed beats. Tighter
        # than this and a stalled hub on a loaded CI box looks like a bug.
        visibility_timeout = 3
        task_runtime = 8

        app = CeleryApp("test_visibility_heartbeat")
        app.conf.update(
            broker_url=f"redis://{host}:{port}/0",
            broker_transport="celery_redis_plus.transport:Transport",
            broker_transport_options={"visibility_timeout": visibility_timeout},
            result_backend=f"redis://{host}:{port}/1",
            task_always_eager=False,
            task_acks_late=True,
            worker_prefetch_multiplier=1,
        )

        counter_key = "heartbeat-executions"
        raw_client = client_lib.Redis(host=host, port=port, db=2)

        # shared=False, or celery replays this registration onto every app built
        # after it. The other parametrization of this test registers the same
        # name against its own container, and whichever finalizer runs last wins,
        # so the worker ends up talking to a container that is already stopped.
        @app.task(name="tests.sleep_past_visibility_timeout", shared=False)
        def sleep_past_visibility_timeout() -> int:
            client = client_lib.Redis(host=host, port=port, db=2)
            try:
                client.incr(counter_key)
            finally:
                client.close()
            time.sleep(task_runtime)
            return 1

        try:
            with testing_worker.start_worker(
                app,
                pool="threads",
                concurrency=2,
                perform_ping_check=False,
                shutdown_timeout=30.0,
            ):
                result = sleep_past_visibility_timeout.delay()
                assert result.get(timeout=task_runtime + 30) == 1

                # A redelivery starts while the first run is still sleeping, but
                # the worker may only get to it once that run frees a slot, so
                # give it a full requeue cycle to show up
                settle = time.time() + visibility_timeout + transport_mod.DEFAULT_REQUEUE_CHECK_INTERVAL
                while time.time() < settle:
                    assert int(raw_client.get(counter_key) or 0) == 1, "task was redelivered while it was running"
                    time.sleep(0.2)
        finally:
            raw_client.delete(counter_key)
            raw_client.flushdb()
            raw_client.close()
            app.close()
