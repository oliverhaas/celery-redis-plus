"""Fixes Celery's EventDispatcher losing buffered events sent from pool threads."""

from __future__ import annotations

from typing import Any

from celery.events.dispatcher import EventDispatcher
from celery.events.event import Event, group_from
from celery.utils.time import utcoffset


def _send(  # noqa: PLR0913
    self: Any,  # bound onto EventDispatcher at runtime, so its attributes are not statically visible
    type: str,  # noqa: A002
    blind: bool = False,
    utcoffset: Any = utcoffset,
    retry: bool = False,
    retry_policy: Any = None,
    Event: Any = Event,  # noqa: N803
    **fields: Any,
) -> Any:
    """Appends under the mutex, so flush cannot clear an event it never published."""
    if not self.enabled:
        return None
    groups, group = self.groups, group_from(type)
    if groups and group not in groups:
        return None
    if group not in self.buffer_group:
        return self.publish(
            type,
            fields,
            self.producer,
            blind=blind,
            Event=Event,
            retry=retry,
            retry_policy=retry_policy,
        )
    clock = self.clock.forward()
    event = Event(type, hostname=self.hostname, utcoffset=utcoffset(), pid=self.pid, clock=clock, **fields)
    with self.mutex:
        buf = self._group_buffer[group]
        buf.append(event)
        full = len(buf) >= self.buffer_limit
    # flush() takes the same mutex, which is not reentrant, so it has to be called unlocked.
    if full:
        self.flush()
    elif self.on_send_buffered:
        self.on_send_buffered()
    return None


def _flush(
    self: Any,
    errors: bool = True,
    groups: bool = True,
) -> None:
    """Clears each group buffer before publishing it, so concurrent appends survive the publish."""
    if errors:
        buf = list(self._outbound_buffer)
        try:
            with self.mutex:
                for event, routing_key, _ in buf:
                    self._publish(event, self.producer, routing_key)
        finally:
            self._outbound_buffer.clear()
    if groups:
        with self.mutex:
            for group, events in self._group_buffer.items():
                if not events:
                    continue
                # Snapshot and clear first. Upstream publishes the live list and only then does
                # events[:] = [], discarding whatever landed while the socket write released the GIL.
                batch = events[:]
                events[:] = []
                self._publish(batch, self.producer, f"{group}.multi")


def install() -> None:
    """Replace the dispatcher's send and flush. Idempotent."""
    if getattr(EventDispatcher, "_battle_patched", False):
        return
    EventDispatcher.send = _send  # ty: ignore[invalid-assignment]
    EventDispatcher.flush = _flush  # ty: ignore[invalid-assignment]
    EventDispatcher._battle_patched = True  # ty: ignore[unresolved-attribute]
