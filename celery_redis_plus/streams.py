"""Redis Streams transport with consumer groups for reliable point-to-point queues.

This transport is an alternative to the sorted-set transport in
``celery_redis_plus.transport``, built on Redis Streams with consumer groups:

1. XREADGROUP + Pending Entries List (PEL) for regular queues - delivering a message
   and registering it as pending is one atomic step; the broker itself tracks
   in-flight work
2. One stream per (queue, priority level) - full 0-255 priority support bucketed
   onto a small configurable step set
3. Native delayed delivery via a per-queue staging sorted set and a periodic Lua pump
4. Redis Streams for fanout exchanges - shared with the sorted-set transport

Requires Redis 7.0+ (or Valkey).
Supports both redis-py and valkey-py client libraries.

Configuration
=============
For Valkey, use the ``valkey-streams://`` URL scheme directly::

    broker_url = "valkey-streams://localhost:6379/0"

For Redis, set ``broker_transport`` with a standard ``redis://`` URL::

    broker_url = "redis://localhost:6379/0"
    broker_transport = "celery_redis_plus.streams:Transport"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


def priority_to_level(priority: int, steps: Sequence[int]) -> int:
    """Bucket a message priority onto a priority level.

    A message goes to the highest step <= its priority, or the lowest step
    if its priority is below all steps (RabbitMQ semantics, 0-255 space).

    Args:
        priority: Message priority (0-255, higher = more urgent).
        steps: Priority steps sorted ascending (e.g. [0, 3, 6, 9]).

    Returns:
        The step value used as the stream's priority level.
    """
    level = steps[0]
    for step in steps:
        if step <= priority:
            level = step
        else:
            break
    return level
