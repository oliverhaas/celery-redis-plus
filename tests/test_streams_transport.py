"""Tests for the Redis Streams transport."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from celery_redis_plus.constants import (
    CONSUMER_IDLE_CLEANUP_FACTOR,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_PRIORITY_STEPS,
    DELAYED_KEY_PREFIX,
    HEARTBEAT_INTERVAL_DIVISOR,
    SHUTDOWN_IDLE_MS,
    STREAM_KEY_PREFIX,
)
from celery_redis_plus.streams import priority_to_level
from celery_redis_plus.transport import PrefixedStrictRedis


@pytest.mark.unit
class TestStreamsConstants:
    """Tests for the streams transport constants."""

    def test_stream_key_prefix(self) -> None:
        """Test that per-(queue, level) streams use the stream: prefix."""
        assert STREAM_KEY_PREFIX == "stream:"

    def test_delayed_key_prefix(self) -> None:
        """Test that per-queue delayed sorted sets use the delayed: prefix."""
        assert DELAYED_KEY_PREFIX == "delayed:"

    def test_default_priority_steps(self) -> None:
        """Test the default priority step set (buckets in 0-255 space)."""
        assert DEFAULT_PRIORITY_STEPS == [0, 3, 6, 9]

    def test_default_consumer_group(self) -> None:
        """Test the default consumer group name used on every queue stream."""
        assert DEFAULT_CONSUMER_GROUP == "celery"

    def test_shutdown_idle_ms(self) -> None:
        """Test that shutdown idle time is far above any sane visibility timeout."""
        assert SHUTDOWN_IDLE_MS == 2**31

    def test_heartbeat_interval_divisor(self) -> None:
        """Test the divisor for deriving heartbeat_interval from visibility_timeout."""
        assert HEARTBEAT_INTERVAL_DIVISOR == 5

    def test_consumer_idle_cleanup_factor(self) -> None:
        """Test the idle factor after which stale consumers are deleted."""
        assert CONSUMER_IDLE_CLEANUP_FACTOR == 12


@pytest.mark.unit
class TestPriorityToLevel:
    """Tests for priority-to-level bucketing."""

    def test_priority_below_lowest_step(self) -> None:
        """Test that a priority below all steps buckets to the lowest step."""
        assert priority_to_level(1, [3, 6, 9]) == 3

    def test_priority_at_lowest_step(self) -> None:
        """Test that priority 0 buckets to step 0 with the default steps."""
        assert priority_to_level(0, DEFAULT_PRIORITY_STEPS) == 0

    def test_priority_at_exact_step(self) -> None:
        """Test that a priority exactly at a step buckets to that step."""
        assert priority_to_level(6, [0, 3, 6, 9]) == 6

    def test_priority_between_steps(self) -> None:
        """Test that a priority between two steps buckets to the lower step."""
        assert priority_to_level(5, [0, 3, 6, 9]) == 3

    def test_priority_above_highest_step(self) -> None:
        """Test that a priority above all steps buckets to the highest step."""
        assert priority_to_level(200, [0, 3, 6, 9]) == 9

    def test_priority_255_default_steps(self) -> None:
        """Test that the maximum priority buckets to the highest default step."""
        assert priority_to_level(255, DEFAULT_PRIORITY_STEPS) == 9

    def test_full_range_steps(self) -> None:
        """Test bucketing extremes with steps spanning the full 0-255 space."""
        steps = [0, 64, 128, 255]
        assert priority_to_level(0, steps) == 0
        assert priority_to_level(254, steps) == 128
        assert priority_to_level(255, steps) == 255

    def test_single_step_absorbs_all_priorities(self) -> None:
        """Test that a single step absorbs the whole 0-255 range."""
        assert priority_to_level(0, [5]) == 5
        assert priority_to_level(255, [5]) == 5


@pytest.mark.unit
class TestStreamsPrefixMixin:
    """Tests for the stream command additions to GlobalKeyPrefixMixin."""

    def test_prefix_xack(self) -> None:
        """Test XACK key prefixing (XACK key group id [id ...])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XACK", "stream:celery:9", "celery", "1-0"])
        assert args == ["XACK", "test:stream:celery:9", "celery", "1-0"]

    def test_prefix_xdel(self) -> None:
        """Test XDEL key prefixing (XDEL key id [id ...])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XDEL", "stream:celery:9", "1-0", "2-0"])
        assert args == ["XDEL", "test:stream:celery:9", "1-0", "2-0"]

    def test_prefix_xlen(self) -> None:
        """Test XLEN key prefixing (XLEN key)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XLEN", "stream:celery:0"])
        assert args == ["XLEN", "test:stream:celery:0"]

    def test_prefix_xpending(self) -> None:
        """Test XPENDING key prefixing (XPENDING key group [start end count [consumer]])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XPENDING", "stream:celery:9", "celery", "-", "+", "10", "worker1"])
        assert args == ["XPENDING", "test:stream:celery:9", "celery", "-", "+", "10", "worker1"]

    def test_prefix_xautoclaim(self) -> None:
        """Test XAUTOCLAIM key prefixing (XAUTOCLAIM key group consumer min-idle-time start)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(
            ["XAUTOCLAIM", "stream:celery:9", "celery", "worker1", "300000", "0-0", "COUNT", "10"],
        )
        assert args == ["XAUTOCLAIM", "test:stream:celery:9", "celery", "worker1", "300000", "0-0", "COUNT", "10"]

    def test_prefix_xclaim(self) -> None:
        """Test XCLAIM key prefixing (XCLAIM key group consumer min-idle-time id [id ...] [JUSTID])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XCLAIM", "stream:celery:9", "celery", "worker1", "0", "1-0", "JUSTID"])
        assert args == ["XCLAIM", "test:stream:celery:9", "celery", "worker1", "0", "1-0", "JUSTID"]

    def test_prefix_xrange(self) -> None:
        """Test XRANGE key prefixing (XRANGE key start end)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XRANGE", "stream:celery:9", "-", "+"])
        assert args == ["XRANGE", "test:stream:celery:9", "-", "+"]

    def test_prefix_xtrim(self) -> None:
        """Test XTRIM key prefixing (XTRIM key MAXLEN [~] threshold)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XTRIM", "dead_letters", "MAXLEN", "~", "10000"])
        assert args == ["XTRIM", "test:dead_letters", "MAXLEN", "~", "10000"]

    def test_prefix_xreadgroup(self) -> None:
        """Test XREADGROUP prefixes stream keys but not group, consumer, or IDs."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(
            [
                "XREADGROUP",
                "GROUP",
                "celery",
                "worker1",
                "COUNT",
                "1",
                "BLOCK",
                "1000",
                "STREAMS",
                "stream:celery:9",
                "stream:celery:0",
                ">",
                ">",
            ],
        )
        assert args == [
            "XREADGROUP",
            "GROUP",
            "celery",
            "worker1",
            "COUNT",
            "1",
            "BLOCK",
            "1000",
            "STREAMS",
            "test:stream:celery:9",
            "test:stream:celery:0",
            ">",
            ">",
        ]

    def test_prefix_xreadgroup_without_streams_keyword(self) -> None:
        """Test XREADGROUP without STREAMS keyword returns args unchanged."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XREADGROUP", "GROUP", "celery", "worker1", "stream:celery:9", ">"])
        assert args == ["XREADGROUP", "GROUP", "celery", "worker1", "stream:celery:9", ">"]

    def test_prefix_xgroup_create(self) -> None:
        """Test XGROUP CREATE prefixes the key at args[1] (XGROUP CREATE key group id [MKSTREAM])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP", "CREATE", "stream:celery:9", "celery", "0", "MKSTREAM"])
        assert args == ["XGROUP", "CREATE", "test:stream:celery:9", "celery", "0", "MKSTREAM"]

    def test_prefix_xgroup_delconsumer(self) -> None:
        """Test XGROUP DELCONSUMER prefixes the key (XGROUP DELCONSUMER key group consumer)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP", "DELCONSUMER", "stream:celery:9", "celery", "worker1"])
        assert args == ["XGROUP", "DELCONSUMER", "test:stream:celery:9", "celery", "worker1"]

    def test_prefix_xinfo_stream(self) -> None:
        """Test XINFO STREAM prefixes the key at args[1] (XINFO STREAM key)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO", "STREAM", "stream:celery:9"])
        assert args == ["XINFO", "STREAM", "test:stream:celery:9"]

    def test_prefix_xinfo_consumers(self) -> None:
        """Test XINFO CONSUMERS prefixes the key but not the group (XINFO CONSUMERS key group)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO", "CONSUMERS", "stream:celery:9", "celery"])
        assert args == ["XINFO", "CONSUMERS", "test:stream:celery:9", "celery"]

    def test_no_prefix_when_empty(self) -> None:
        """Test that an empty prefix leaves stream command keys unchanged."""
        client = PrefixedStrictRedis(connection_pool=MagicMock())

        args = client._prefix_args(["XACK", "stream:celery:9", "celery", "1-0"])
        assert args == ["XACK", "stream:celery:9", "celery", "1-0"]
