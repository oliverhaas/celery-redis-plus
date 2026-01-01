"""Tests for the signal handler that adds delay headers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from celery_redis_plus.constants import DELAY_HEADER
from celery_redis_plus.signals import add_delay_header


@pytest.mark.unit
class TestAddDelayHeader:
    """Tests for add_delay_header signal handler."""

    def test_no_headers(self) -> None:
        """Test that handler handles None headers gracefully."""
        # Should not raise
        add_delay_header(sender="task", headers=None)

    def test_no_eta(self) -> None:
        """Test that handler does nothing when no eta is present."""
        headers: dict[str, object] = {}
        add_delay_header(sender="task", headers=headers)
        assert DELAY_HEADER not in headers

    def test_eta_in_future_iso_string(self) -> None:
        """Test that delay header is added for future eta as ISO string."""
        future_eta = datetime.now(UTC) + timedelta(seconds=60)
        headers: dict[str, object] = {"eta": future_eta.isoformat()}

        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER in headers
        delay = headers[DELAY_HEADER]
        assert isinstance(delay, float)
        # Should be approximately 60 seconds (allow some tolerance for test execution time)
        assert 55 < delay < 65

    def test_eta_in_future_datetime(self) -> None:
        """Test that delay header is added for future eta as datetime."""
        future_eta = datetime.now(UTC) + timedelta(seconds=120)
        headers: dict[str, object] = {"eta": future_eta}

        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER in headers
        delay = headers[DELAY_HEADER]
        assert isinstance(delay, float)
        assert 115 < delay < 125

    def test_eta_in_past(self) -> None:
        """Test that delay header is not added for past eta."""
        past_eta = datetime.now(UTC) - timedelta(seconds=60)
        headers: dict[str, object] = {"eta": past_eta.isoformat()}

        add_delay_header(sender="task", headers=headers)

        # Should not add delay header for past eta
        assert DELAY_HEADER not in headers

    def test_eta_naive_datetime(self) -> None:
        """Test that naive datetime is treated as UTC."""
        future_eta = datetime.now(UTC) + timedelta(seconds=60)
        # Create naive datetime (no timezone)
        naive_eta = future_eta.replace(tzinfo=None)
        headers: dict[str, object] = {"eta": naive_eta}

        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER in headers

    def test_invalid_eta_string(self) -> None:
        """Test that invalid eta string is handled gracefully."""
        headers: dict[str, object] = {"eta": "not-a-date"}

        # Should not raise
        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER not in headers

    def test_invalid_eta_type(self) -> None:
        """Test that invalid eta type is handled gracefully."""
        headers: dict[str, object] = {"eta": 12345}  # Invalid type

        # Should not raise
        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER not in headers

    def test_eta_with_timezone_offset(self) -> None:
        """Test eta with non-UTC timezone."""
        # Create a datetime with +05:00 offset, 60 seconds in the future
        now_utc = datetime.now(UTC)
        future_utc = now_utc + timedelta(seconds=60)
        # Convert to +05:00 timezone
        tz_offset = timezone(timedelta(hours=5))
        future_with_offset = future_utc.astimezone(tz_offset)

        headers: dict[str, object] = {"eta": future_with_offset.isoformat()}

        add_delay_header(sender="task", headers=headers)

        assert DELAY_HEADER in headers
        delay = headers[DELAY_HEADER]
        assert isinstance(delay, float)
        assert 55 < delay < 65

    def test_all_other_kwargs_ignored(self) -> None:
        """Test that other kwargs are properly ignored."""
        future_eta = datetime.now(UTC) + timedelta(seconds=30)
        headers: dict[str, object] = {"eta": future_eta.isoformat()}

        # Call with various other kwargs that should be ignored
        add_delay_header(
            sender="my_task",
            body={"args": [], "kwargs": {}},
            exchange="celery",
            routing_key="celery",
            headers=headers,
            properties={"delivery_mode": 2},
            declare=[],
            retry_policy={"max_retries": 3},
            extra_kwarg="should be ignored",
        )

        assert DELAY_HEADER in headers
