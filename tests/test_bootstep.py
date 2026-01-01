"""Tests for the DelayedDeliveryBootstep."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from celery_redis_plus.bootstep import DelayedDeliveryBootstep


@pytest.mark.unit
class TestDelayedDeliveryBootstep:
    """Tests for DelayedDeliveryBootstep."""

    def test_init(self) -> None:
        """Test bootstep initialization."""
        consumer = MagicMock()
        bootstep = DelayedDeliveryBootstep(consumer)
        assert bootstep.consumer is consumer

    def test_start_no_connection(self) -> None:
        """Test start when consumer has no connection."""
        consumer = MagicMock()
        consumer.connection = None

        bootstep = DelayedDeliveryBootstep(consumer)

        # Should not raise
        bootstep.start(consumer)

    def test_start_no_transport(self) -> None:
        """Test start when connection has no transport."""
        consumer = MagicMock()
        consumer.connection.transport = None

        bootstep = DelayedDeliveryBootstep(consumer)

        # Should not raise
        bootstep.start(consumer)

    def test_start_transport_without_delayed_delivery_support(self) -> None:
        """Test start when transport doesn't support delayed delivery."""
        consumer = MagicMock()
        transport = MagicMock()
        transport.supports_native_delayed_delivery = False
        consumer.connection.transport = transport

        bootstep = DelayedDeliveryBootstep(consumer)
        bootstep.start(consumer)

        # Should not call setup method
        has_setup = hasattr(transport, "setup_native_delayed_delivery")
        assert not has_setup or not transport.setup_native_delayed_delivery.called

    def test_start_transport_with_delayed_delivery_support(self) -> None:
        """Test start when transport supports delayed delivery."""
        consumer = MagicMock()
        transport = MagicMock()
        transport.supports_native_delayed_delivery = True
        transport.setup_native_delayed_delivery = MagicMock()
        consumer.connection.transport = transport

        # Mock queues
        queue1 = MagicMock()
        queue1.name = "queue1"
        queue2 = MagicMock()
        queue2.name = "queue2"
        consumer.task_consumer.queues = [queue1, queue2]

        bootstep = DelayedDeliveryBootstep(consumer)
        bootstep.start(consumer)

        # Should call setup method with connection and queue names
        transport.setup_native_delayed_delivery.assert_called_once()
        call_args = transport.setup_native_delayed_delivery.call_args
        assert call_args[0][0] is consumer.connection
        assert call_args[0][1] == ["queue1", "queue2"]

    def test_start_no_task_consumer(self) -> None:
        """Test start when task_consumer is None."""
        consumer = MagicMock()
        transport = MagicMock()
        transport.supports_native_delayed_delivery = True
        transport.setup_native_delayed_delivery = MagicMock()
        consumer.connection.transport = transport
        consumer.task_consumer = None

        bootstep = DelayedDeliveryBootstep(consumer)
        bootstep.start(consumer)

        # Should call setup with empty queue list
        call_args = transport.setup_native_delayed_delivery.call_args
        assert call_args[0][1] == []

    def test_stop_no_connection(self) -> None:
        """Test stop when consumer has no connection."""
        consumer = MagicMock()
        consumer.connection = None

        bootstep = DelayedDeliveryBootstep(consumer)

        # Should not raise
        bootstep.stop(consumer)

    def test_stop_no_transport(self) -> None:
        """Test stop when connection has no transport."""
        consumer = MagicMock()
        consumer.connection.transport = None

        bootstep = DelayedDeliveryBootstep(consumer)

        # Should not raise
        bootstep.stop(consumer)

    def test_stop_with_teardown_method(self) -> None:
        """Test stop when transport has teardown method."""
        consumer = MagicMock()
        transport = MagicMock()
        transport.teardown_native_delayed_delivery = MagicMock()
        consumer.connection.transport = transport

        bootstep = DelayedDeliveryBootstep(consumer)
        bootstep.stop(consumer)

        transport.teardown_native_delayed_delivery.assert_called_once()

    def test_stop_without_teardown_method(self) -> None:
        """Test stop when transport has no teardown method."""
        consumer = MagicMock()
        transport = MagicMock(spec=[])  # No methods
        consumer.connection.transport = transport

        bootstep = DelayedDeliveryBootstep(consumer)

        # Should not raise
        bootstep.stop(consumer)


@pytest.mark.unit
class TestBootstepRequires:
    """Test bootstep requirements."""

    def test_requires_tasks(self) -> None:
        """Test that bootstep requires Tasks step."""
        assert "celery.worker.consumer.tasks:Tasks" in DelayedDeliveryBootstep.requires
