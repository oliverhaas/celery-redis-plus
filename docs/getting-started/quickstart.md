# Quick Start

## Basic Setup

```python
from celery import Celery
import celery_redis_plus  # noqa: F401 — registers valkey:// transport

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

## Streams Transport

The package also ships a second transport built on Redis Streams with consumer
groups. Select it with the `valkey-streams://` URL scheme:

```python
from celery import Celery
import celery_redis_plus  # noqa: F401 - registers the transports

app = Celery('myapp')

# For Valkey: use the valkey-streams:// URL scheme (valkeys-streams:// for SSL)
app.config_from_object({
    'broker_url': 'valkey-streams://localhost:6379/0',
})

# For Redis: use redis:// with broker_transport
# app.config_from_object({
#     'broker_url': 'redis://localhost:6379/0',
#     'broker_transport': 'celery_redis_plus.streams:Transport',
# })
```

Tasks, delayed delivery, and fanout work exactly as with the sorted set
transport. Two behavioral differences:

- Priorities are bucketed onto a small set of levels (default `[0, 3, 6, 9]`):
  a message goes to the highest level that is less than or equal to its
  priority, and levels are consumed highest first.
- `visibility_timeout` means "worker considered dead after this much
  heartbeat silence", not "maximum task duration". Workers heartbeat their
  in-flight messages every `visibility_timeout / 5` seconds, so the default
  of 300 seconds is safe even for tasks that run for hours, and messages
  from crashed workers are reclaimed within minutes.

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

The streams transport has its own options:

```python
app.config_from_object({
    'broker_url': 'valkey-streams://localhost:6379/0',
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',    # Prefix for all Redis keys
        'visibility_timeout': 300,       # Heartbeat silence before reclaim (NOT max task duration)
        'priority_steps': [0, 3, 6, 9],  # Priority buckets (one stream per level)
        'max_restore_count': 10,         # Drop after 10 involuntary redeliveries (shutdown handoffs count)
        'dead_letter_stream': 'dead',    # Copy dropped messages here first
    },
})
```
