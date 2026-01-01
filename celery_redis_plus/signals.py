"""Signal handlers to add delay header for tasks with eta/countdown."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from celery.signals import before_task_publish

from .constants import DELAY_HEADER


@before_task_publish.connect
def add_delay_header(
    sender: str | None = None,
    body: dict[str, Any] | None = None,
    exchange: str | None = None,
    routing_key: str | None = None,
    headers: dict[str, Any] | None = None,
    properties: dict[str, Any] | None = None,
    declare: list[Any] | None = None,
    retry_policy: dict[str, Any] | None = None,
    **kwargs: Any,
) -> None:
    """Add x-celery-delay-seconds header based on eta in headers.

    This signal handler intercepts task publishing and calculates the delay
    in seconds from the 'eta' header (if present). The delay is added as
    x-celery-delay-seconds header for the transport to use.

    Args:
        sender: The task name.
        body: The task message body.
        exchange: The exchange to publish to.
        routing_key: The routing key.
        headers: Message headers (modified in place to add delay header).
        properties: Message properties.
        declare: Entities to declare.
        retry_policy: Retry policy for publishing.
        **kwargs: Additional keyword arguments.
    """
    if headers is None:
        return

    eta = headers.get("eta")
    if eta is None:
        return

    # Parse eta - it can be an ISO format string or datetime
    if isinstance(eta, str):
        # Parse ISO format datetime string
        # Celery uses ISO format with timezone: 2024-01-15T10:30:00+00:00
        try:
            eta_dt = datetime.fromisoformat(eta)
        except ValueError:
            return
    elif isinstance(eta, datetime):
        eta_dt = eta
    else:
        return

    # Ensure eta is timezone-aware
    if eta_dt.tzinfo is None:
        eta_dt = eta_dt.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    delay_seconds = (eta_dt - now).total_seconds()

    # Only add delay header if eta is in the future
    if delay_seconds > 0:
        headers[DELAY_HEADER] = delay_seconds
