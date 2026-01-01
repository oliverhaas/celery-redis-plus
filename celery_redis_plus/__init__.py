"""celery-redis-plus: Enhanced Redis transport for Celery with native delayed delivery.

This package provides a drop-in replacement Redis transport for Celery that
supports native delayed message delivery using Redis ZSETs. Tasks with
countdown or eta will use Redis-native delayed delivery instead of the
default Celery approach.

Usage:
    from celery import Celery
    import celery_redis_plus  # Auto-registers signal handler

    app = Celery('myapp')
    app.config_from_object({
        'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    })

    celery_redis_plus.configure_app(app)  # Registers bootstep

    @app.task
    def my_task():
        pass

    # Tasks with countdown/eta will use native Redis delayed delivery
    my_task.apply_async(countdown=60)
"""

from __future__ import annotations

from typing import Any

from .bootstep import DelayedDeliveryBootstep
from .constants import DELAY_HEADER
from .transport import Transport

__all__ = ["DELAY_HEADER", "DelayedDeliveryBootstep", "Transport", "configure_app"]

__version__ = "0.1.0a1"


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
