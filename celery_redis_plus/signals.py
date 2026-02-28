"""Signal handlers for celery-redis-plus.

This module provides signal handlers that integrate Celery with the
celery-redis-plus transport. The main responsibility is converting
Celery's eta (ISO datetime string in headers) to properties.eta
(Unix timestamp float) for the transport layer.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from celery.signals import before_task_publish

logger = logging.getLogger(__name__)


@before_task_publish.connect
def _convert_eta_to_properties(
    body: dict[str, Any],
    properties: dict[str, Any],
    **kwargs: Any,
) -> None:
    """Convert Celery's headers.eta to properties.eta for the transport.

    Celery stores eta as an ISO datetime string in headers. Our transport
    expects properties.eta as a Unix timestamp float (similar to priority).
    This signal handler bridges the two.

    Args:
        body: The message body (unused).
        properties: Message properties dict - we add 'eta' here.
        **kwargs: Additional signal arguments (headers, exchange, etc.).
    """
    headers = kwargs.get("headers", {})
    if not headers:
        return

    eta_value = headers.get("eta")
    if eta_value is None:
        return

    # Parse ISO datetime string to Unix timestamp
    if isinstance(eta_value, str):
        # Celery sends ISO format datetime strings
        try:
            # Try parsing with timezone info
            if eta_value.endswith("Z"):
                eta_value = eta_value[:-1] + "+00:00"
            eta_dt = datetime.fromisoformat(eta_value)
            # Ensure UTC timezone
            if eta_dt.tzinfo is None:
                eta_dt = eta_dt.replace(tzinfo=UTC)
            properties["eta"] = eta_dt.timestamp()
        except (ValueError, TypeError):  # fmt: skip
            logger.debug("Failed to parse ETA value %r, treating as immediate delivery", eta_value)
    elif isinstance(eta_value, datetime):
        # Already a datetime object
        if eta_value.tzinfo is None:
            eta_value = eta_value.replace(tzinfo=UTC)
        properties["eta"] = eta_value.timestamp()
    elif isinstance(eta_value, (int, float)):
        # Already a Unix timestamp
        properties["eta"] = float(eta_value)
