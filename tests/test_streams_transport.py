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
from celery_redis_plus.streams import priority_to_level


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
