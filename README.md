# celery-redis-plus

Enhanced Redis/Valkey transport for Celery with native delayed delivery, improved reliability, full priority support, and reliable fanout.

## Overview

`celery-redis-plus` is a replacement Redis/Valkey transport for Celery that addresses key limitations of the standard transport:

1. **Native Delayed Delivery** - Tasks with long `countdown` or `eta` are stored in Redis and delivered when due, instead of being held in worker memory.
2. **Improved Reliability** - Atomic message consumption via BZMPOP with improvements regarding visibility timeout ensures zero message loss.
3. **Full Priority Support** - All 256 priority levels (0-255) with RabbitMQ-compatible semantics (higher number = higher priority).
4. **Reliable Fanout** - Redis Streams replace lossy PUB/SUB for durable broadcast messaging.

## Installation

```bash
uv add celery-redis-plus
```

## Quick Start

```python
from celery import Celery
from celery_redis_plus import DelayedDeliveryBootstep

app = Celery('myapp')
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
})
app.steps['consumer'].add(DelayedDeliveryBootstep)

@app.task
def my_task():
    print("Hello!")

# Tasks with countdown/eta will use native Redis delayed delivery
my_task.apply_async(countdown=120)

# Priority support (RabbitMQ semantics: higher = more important)
my_task.apply_async(priority=90)   # High priority
my_task.apply_async(priority=0)    # Low priority (default)
```

## How It Works

### Sorted Set Queues

Queues use Redis sorted sets instead of lists. Messages are added with `ZADD` using a score that encodes priority and timestamp. Workers use `BZMPOP` to atomically pop the highest-priority, oldest message.

### Message Persistence

Messages are stored in per-message hashes before being added to the queue. When consumed, the hash persists until explicitly acknowledged. Combined with visibility timeout tracking, this ensures messages are never lost - even if a worker crashes in the instant after the message is pop'ed from the queue, the message can be recovered and requeued.

### Delayed Delivery

Delayed messages are stored in a sorted set with timestamps as scores. A background thread periodically checks for messages whose timestamp has passed and moves them to the normal queue.

### Stream-based Fanout

Fanout exchanges use Redis Streams. Messages are added with `XADD`, and each consumer uses `XREAD` tracking their own position. Old messages are trimmed based on `stream_maxlen`.

## Configuration

### Transport Options

Configure via Celery's `broker_transport_options`:

```python
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',        # Prefix for all Redis keys
        'visibility_timeout': 300,          # Seconds before unacked messages are reclaimed
        'stream_max_length': 10000,          # Max messages per stream (approximate)
    },
})
```

### SSL/TLS Connections

For secure connections to Redis, use the `ssl` transport option:

```python
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    'broker_transport_options': {
        'ssl': True,  # Use default SSL settings
    },
})

# Or with custom SSL options:
import ssl
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    'broker_transport_options': {
        'ssl': {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        },
    },
})
```

## API

- **`celery_redis_plus.Transport`** - Custom transport with sorted set queues, priority encoding, delayed delivery, and Redis Streams fanout.
- **`celery_redis_plus.DelayedDeliveryBootstep`** - Worker bootstep for background message processing and recovery.

## Requirements

We target recent versions for BZMPOP support and to simplify development.

- Python >= 3.13
- Celery >= 5.5.0
- Redis >= 7.0 (for BZMPOP) or Valkey (any version)

## Development

```bash
# Clone the repository
git clone https://github.com/oliverhaas/celery-redis-plus.git
cd celery-redis-plus

# Create virtual environment and install with development dependencies
uv venv
uv sync --group dev

# Run tests (requires Docker for integration tests)
uv run pytest

# Run linter
uv run ruff check

# Run type checker
uv run ty check
```

## Contributing

This package is intended as a temporary solution until these improvements are merged upstream into Celery/Kombu. Contributions are welcome! For consulting inquiries, contact ohaas@e1plus.de.

## License

MIT License - see [LICENSE](LICENSE) for details.
