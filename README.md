# Celery Redis Plus

[![PyPI version](https://img.shields.io/pypi/v/celery-redis-plus.svg)](https://pypi.org/project/celery-redis-plus/)
[![CI](https://github.com/oliverhaas/celery-redis-plus/actions/workflows/ci.yml/badge.svg)](https://github.com/oliverhaas/celery-redis-plus/actions/workflows/ci.yml)

Enhanced Redis/Valkey transports for Celery/Kombu with native delayed delivery, improved reliability, full priority support, and reliable fanout. Ships two broker transports: a sorted set transport (`valkey://`) and a Redis Streams transport with consumer groups (`valkey-streams://`).

## Quick Example

```python
from celery import Celery
import celery_redis_plus  # Register valkey:// and valkey-streams:// transports

app = Celery('myapp')
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
})

@app.task
def my_task():
    print("Hello!")

# Native delayed delivery - stored in Redis, not worker memory
my_task.apply_async(countdown=120)

# Full priority support (0-255, RabbitMQ semantics)
my_task.apply_async(priority=90)
```

## Choosing a Transport

|  | Sorted set (`valkey://`) | Streams (`valkey-streams://`) |
|---|---|---|
| Queue storage | Sorted sets + per-message hashes | One Redis Stream per (queue, priority level) |
| In-flight tracking | `messages_index` sorted sets + Lua bookkeeping | Broker-native Pending Entries List (PEL) |
| Priorities | Exact 0-255 ordering | 0-255 bucketed onto `priority_steps` (default `[0, 3, 6, 9]`) |
| `visibility_timeout` means | Time before an unacked message is requeued | Heartbeat silence before a worker counts as dead |
| Long-running tasks | Timeout must exceed the longest task | Heartbeat keeps tasks alive; the 300 s default is safe for hours-long tasks |
| Crash recovery latency | Up to `visibility_timeout` (sized for the longest task) | Minutes, independent of task duration |
| Dead-lettering | Not supported | Optional `dead_letter_stream` |

Pick the **streams transport** for long-running tasks, fast crash recovery, and dead-lettering.
Pick the **sorted set transport** when you need exact 256-level priority ordering.

```python
from celery import Celery
import celery_redis_plus  # Register transports

app = Celery('myapp')

# For Valkey (use valkeys-streams:// for SSL)
app.config_from_object({
    'broker_url': 'valkey-streams://localhost:6379/0',
})

# For Redis
# app.config_from_object({
#     'broker_url': 'redis://localhost:6379/0',
#     'broker_transport': 'celery_redis_plus.streams:Transport',
# })
```

Streams transport options (set via `broker_transport_options`):

| Option | Default | Meaning |
|--------|---------|---------|
| `priority_steps` | `[0, 3, 6, 9]` | Priority buckets in 0-255 space; one stream per (queue, level) |
| `visibility_timeout` | `300` | Seconds of heartbeat silence before in-flight messages are reclaimed |
| `heartbeat_interval` | `visibility_timeout / 5` | Cadence of the `XCLAIM ... JUSTID` heartbeat |
| `max_restore_count` | `None` | Involuntary-redelivery cap before drop/dead-letter (a graceful-shutdown handoff also counts as one; leave headroom for rolling restarts) |
| `dead_letter_stream` | `None` | Stream to copy poisoned messages to before dropping them (must not start with `stream:`, the queue namespace) |
| `consumer_group` | `"celery"` | Consumer group name on every queue stream |
| `consumer_name` | auto | Stable per-worker consumer identity (defaults to the worker nodename when available, else `hostname:pid`) |
| `global_keyprefix` | `""` | Prefix for all Redis keys |

## Documentation

See the [full documentation](https://oliverhaas.github.io/celery-redis-plus/) for installation, configuration, and API reference.

## Supported Versions

|         | Python 3.13 | Python 3.14 |
|---------|:-----------:|:-----------:|
| Celery 5.5+ | ✓ | ✓ |

Requires Redis >= 7.0 for the sorted set transport (BZMPOP) and Redis >= 6.2 for the streams transport (the XPENDING ... IDLE filter), or Valkey (any version).

## License

MIT
