# Celery Redis Plus

[![PyPI version](https://img.shields.io/pypi/v/celery-redis-plus.svg)](https://pypi.org/project/celery-redis-plus/)
[![CI](https://github.com/oliverhaas/celery-redis-plus/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/celery-redis-plus/actions/workflows/ci.yml)

Enhanced Redis/Valkey transport for Celery/Kombu with native delayed delivery, improved reliability, full priority support, and reliable fanout.

## Quick Example

```python
from celery import Celery
import celery_redis_plus  # Register valkey:// transport
from celery_redis_plus import DelayedDeliveryBootstep

app = Celery('myapp')
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
})
app.steps['consumer'].add(DelayedDeliveryBootstep)

@app.task
def my_task():
    print("Hello!")

# Native delayed delivery - stored in Redis, not worker memory
my_task.apply_async(countdown=120)

# Full priority support (0-255, RabbitMQ semantics)
my_task.apply_async(priority=90)
```

## Documentation

See the [full documentation](https://oliverhaas.github.io/celery-redis-plus/) for installation, configuration, and API reference.

## Supported Versions

|         | Python 3.13 | Python 3.14 |
|---------|:-----------:|:-----------:|
| Celery 5.5+ | ✓ | ✓ |

Requires Redis >= 7.0 (for BZMPOP) or Valkey (any version).

## License

MIT
