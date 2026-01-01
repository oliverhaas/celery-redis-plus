"""Bootstep for setting up native delayed delivery on worker startup."""

from __future__ import annotations

import logging
from typing import Any

from celery import bootsteps

logger = logging.getLogger(__name__)


class DelayedDeliveryBootstep(bootsteps.StartStopStep):
    """Bootstep that sets up native delayed delivery for the transport.

    This bootstep runs during worker startup and calls the transport's
    setup_native_delayed_delivery() method if it's available. This allows
    the transport to perform any necessary initialization for handling
    delayed messages (e.g., starting a background thread to move messages
    from delayed queues to ready queues).
    """

    requires = ("celery.worker.consumer.tasks:Tasks",)

    def __init__(self, parent: Any, **kwargs: Any) -> None:
        """Initialize the bootstep.

        Args:
            parent: The worker consumer instance.
            **kwargs: Additional keyword arguments.
        """
        self.consumer = parent
        super().__init__(parent, **kwargs)

    def start(self, parent: Any) -> None:
        """Start the delayed delivery setup.

        Called when the worker starts consuming. This method checks if the
        transport supports native delayed delivery and calls its setup method.

        Args:
            parent: The worker consumer instance.
        """
        consumer = parent
        connection = getattr(consumer, "connection", None)
        if connection is None:
            logger.warning("No connection available, skipping delayed delivery setup")
            return

        transport = getattr(connection, "transport", None)
        if transport is None:
            logger.warning("No transport available, skipping delayed delivery setup")
            return

        # Check if transport supports native delayed delivery
        if not getattr(transport, "supports_native_delayed_delivery", False):
            logger.debug("Transport does not support native delayed delivery")
            return

        # Get the list of queues the worker is consuming from
        task_consumer = getattr(consumer, "task_consumer", None)
        queues: list[str] = []
        if task_consumer is not None:
            task_queues = getattr(task_consumer, "queues", [])
            queues = [q.name for q in task_queues]

        # Call the transport's setup method
        setup_method = getattr(transport, "setup_native_delayed_delivery", None)
        if callable(setup_method):
            logger.info("Setting up native delayed delivery for queues: %s", queues)
            setup_method(connection, queues)

    def stop(self, parent: Any) -> None:
        """Stop the delayed delivery processing.

        Called when the worker stops. This method calls the transport's
        teardown method if available.

        Args:
            parent: The worker consumer instance.
        """
        consumer = parent
        connection = getattr(consumer, "connection", None)
        if connection is None:
            return

        transport = getattr(connection, "transport", None)
        if transport is None:
            return

        # Check if transport has a teardown method
        teardown_method = getattr(transport, "teardown_native_delayed_delivery", None)
        if callable(teardown_method):
            logger.info("Tearing down native delayed delivery")
            teardown_method()
