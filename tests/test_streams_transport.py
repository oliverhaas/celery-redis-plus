"""Tests for the Redis Streams transport."""

from __future__ import annotations

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
