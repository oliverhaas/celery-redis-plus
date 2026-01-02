"""celery-redis-plus: Enhanced Redis transport for Celery.

This package provides an enhanced Redis transport for Celery that uses:
- BZMPOP + sorted sets for regular queues (priority support + reliability)
- Redis Streams for fanout exchanges (true broadcast with XREAD)
- Native delayed delivery integrated into sorted set scoring

Requirements:
- Redis 7.0+ (for BZMPOP command)
- Python 3.13+

Usage:
    from celery import Celery
    import celery_redis_plus  # Auto-registers signal handler for eta/countdown

    app = Celery('myapp')
    app.config_from_object({
        'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    })

    # For SSL/TLS connections:
    # app.config_from_object({
    #     'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    #     'broker_transport_options': {'ssl': True},
    # })

    @app.task
    def my_task():
        pass

    # Tasks with countdown/eta will use native Redis delayed delivery
    my_task.apply_async(countdown=60)

    # Full 0-255 priority support
    my_task.apply_async(priority=5)
"""

from __future__ import annotations

from importlib.metadata import version
from typing import Any

from .bootstep import DelayedDeliveryBootstep
from .constants import DELAY_HEADER
from .transport import Transport

__all__ = ["DELAY_HEADER", "DelayedDeliveryBootstep", "Transport", "__version__", "configure_app"]

__version__ = version("celery-redis-plus")


def configure_app(app: Any) -> None:
    """Configure a Celery app for celery-redis-plus.

    This function registers the DelayedDeliveryBootstep with the app,
    which will set up native delayed delivery when workers start.

    Args:
        app: The Celery application instance to configure.

    Example:
        >>> from celery import Celery
        >>> import celery_redis_plus
        >>>
        >>> app = Celery('myapp')
        >>> celery_redis_plus.configure_app(app)
    """
    app.steps["consumer"].add(DelayedDeliveryBootstep)


# Auto-register signal handler on import
# This ensures the delay header is added to all tasks with eta/countdown
from . import signals as _signals  # noqa: F401, E402
