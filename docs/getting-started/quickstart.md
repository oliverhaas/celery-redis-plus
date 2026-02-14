# Quick Start

## Basic Setup

```python
from celery import Celery
from celery_redis_plus import DelayedDeliveryBootstep

app = Celery('myapp')

# For Valkey: use valkey:// URL scheme
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
})

# For Redis: use redis:// but with broker_transport,
# since the official Redis transport is otherwise used.
# app.config_from_object({
#     'broker_url': 'redis://localhost:6379/0',
#     'broker_transport': 'celery_redis_plus.transport:Transport',
# })

app.steps['consumer'].add(DelayedDeliveryBootstep)

@app.task
def my_task():
    print("Hello!")

# Use tasks as always
my_task.delay()

# Tasks with countdown/eta will use native delayed delivery
my_task.apply_async(countdown=120)

# Priority support (RabbitMQ semantics: higher = more important)
my_task.apply_async(priority=200)   # High priority
my_task.apply_async(priority=0)    # Low priority (default)
```

## Example Project

See [`examples/simple/`](https://github.com/oliverhaas/celery-redis-plus/tree/main/examples/simple) for a runnable example with Docker Compose, Flower, and tasks exercising all key features.

## Configuration

### Transport Options

Configure via Celery's `broker_transport_options`. Many options are the same as in the official Redis transport:

```python
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0', # or valkeys:// for ssl
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',        # Prefix for all Redis keys
        'visibility_timeout': 300,           # Seconds before unacked messages are reclaimed
        'stream_maxlen': 10000,              # Max messages per stream (approximate)
    },
})
```
