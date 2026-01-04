"""celery-redis-plus: Enhanced Redis transport for Celery.

This package provides an enhanced Redis transport for Celery with:
- Native delayed delivery integrated into sorted set scoring
- Improved reliability via BZMPOP + sorted sets
- Full 0-255 priority support (RabbitMQ semantics)
- Redis Streams for reliable fanout (replaces PUB/SUB)

Requires Redis 7.0+ (for BZMPOP) and Python 3.13+.
"""

from __future__ import annotations

from importlib.metadata import version

from .bootstep import DelayedDeliveryBootstep
from .transport import Transport

__all__ = ["DelayedDeliveryBootstep", "Transport", "__version__"]

__version__ = version("celery-redis-plus")
