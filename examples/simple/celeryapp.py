"""Celery app configured with celery-redis-plus transport."""

from celery import Celery

app = Celery("example")

app.config_from_object(
    {
        "broker_url": "redis://localhost:6379/0",
        "broker_transport": "celery_redis_plus.transport:Transport",
        "result_backend": "redis://localhost:6379/1",
        # Import the package so the ETA signal handler is registered
        "include": ["celery_redis_plus", "tasks"],
    },
)
