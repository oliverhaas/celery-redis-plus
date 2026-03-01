"""celery-redis-plus: Enhanced Redis/Valkey transport for Celery.

This package provides an enhanced Redis/Valkey transport for Celery with:
- Native delayed delivery integrated into sorted set scoring
- Improved reliability via BZMPOP + sorted sets
- Full 0-255 priority support (RabbitMQ semantics)
- Redis Streams for reliable fanout (replaces PUB/SUB)

Requires Redis 7.0+ (or Valkey) for BZMPOP and Python 3.13+.
Supports both redis-py and valkey-py client libraries.
"""

from __future__ import annotations

from importlib.metadata import version

from .transport import Transport

__all__ = ["Transport", "__version__"]

__version__ = version("celery-redis-plus")


# Register valkey:// and valkeys:// URL schemes as transport aliases
# This allows using valkey://host:port/db URLs directly with Celery/Kombu
def _register_transport_aliases() -> None:
    """Register valkey transport aliases with kombu."""
    try:
        from kombu.transport import TRANSPORT_ALIASES

        TRANSPORT_ALIASES.setdefault("valkey", "celery_redis_plus.transport:Transport")
        TRANSPORT_ALIASES.setdefault("valkeys", "celery_redis_plus.transport:Transport")
    except ImportError:
        pass  # kombu not available


_register_transport_aliases()
