"""Tests for signal handlers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest


@pytest.mark.unit
class TestConvertEtaToProperties:
    """Tests for the _convert_eta_to_properties signal handler."""

    def test_no_headers(self) -> None:
        """Test with no headers - should not modify properties."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        _convert_eta_to_properties(body={}, properties=properties)
        assert "eta" not in properties

    def test_no_eta_in_headers(self) -> None:
        """Test with headers but no eta - should not modify properties."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        _convert_eta_to_properties(body={}, properties=properties, headers={"foo": "bar"})
        assert "eta" not in properties

    def test_eta_iso_string_with_z(self) -> None:
        """Test eta as ISO string with Z suffix."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        eta_str = eta_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_str})

        assert "eta" in properties
        assert abs(properties["eta"] - eta_dt.timestamp()) < 0.001

    def test_eta_iso_string_with_offset(self) -> None:
        """Test eta as ISO string with timezone offset."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        eta_str = eta_dt.isoformat()

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_str})

        assert "eta" in properties
        assert abs(properties["eta"] - eta_dt.timestamp()) < 0.001

    def test_eta_iso_string_naive(self) -> None:
        """Test eta as naive ISO string (treated as UTC)."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_str = "2025-01-15T10:30:00"

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_str})

        assert "eta" in properties
        # Should be treated as UTC
        expected_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert abs(properties["eta"] - expected_dt.timestamp()) < 0.001

    def test_eta_datetime_object(self) -> None:
        """Test eta as datetime object."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_dt})

        assert "eta" in properties
        assert abs(properties["eta"] - eta_dt.timestamp()) < 0.001

    def test_eta_datetime_naive(self) -> None:
        """Test eta as naive datetime (treated as UTC)."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        # Create a naive datetime by removing tzinfo from a tz-aware datetime
        eta_dt_aware = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        eta_dt = eta_dt_aware.replace(tzinfo=None)  # naive

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_dt})

        assert "eta" in properties
        expected_dt = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert abs(properties["eta"] - expected_dt.timestamp()) < 0.001

    def test_eta_unix_timestamp_float(self) -> None:
        """Test eta as Unix timestamp float."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_timestamp = 1736938200.0  # Some timestamp

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_timestamp})

        assert "eta" in properties
        assert properties["eta"] == eta_timestamp

    def test_eta_unix_timestamp_int(self) -> None:
        """Test eta as Unix timestamp int."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        eta_timestamp = 1736938200

        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": eta_timestamp})

        assert "eta" in properties
        assert properties["eta"] == float(eta_timestamp)

    def test_invalid_eta_string_ignored(self) -> None:
        """Test invalid eta string is ignored."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": "not-a-date"})
        assert "eta" not in properties

    def test_none_eta_ignored(self) -> None:
        """Test None eta is ignored."""
        from celery_redis_plus.signals import _convert_eta_to_properties

        properties: dict[str, Any] = {}
        _convert_eta_to_properties(body={}, properties=properties, headers={"eta": None})
        assert "eta" not in properties
