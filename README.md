# celery-redis-plus

Enhanced Redis transport for Celery with improved reliability, priority support, and native delayed delivery.

## Overview

`celery-redis-plus` provides a drop-in replacement Redis transport for Celery that addresses three key limitations of the standard Redis transport:

1. **Native Delayed Delivery** - Tasks with `countdown` or `eta` are stored efficiently and delivered exactly when due
2. **Improved Reliability with Sorted Sets** - Uses BZMPOP (Redis 7.0+) for atomic, priority-aware message consumption
3. **Reliable Fanout with Redis Streams** - Replaces lossy PUB/SUB with durable consumer groups for fanout exchanges

## Features

### Native Delayed Delivery

When you schedule tasks with `countdown` or `eta`, messages are stored in a Redis sorted set and delivered exactly when they're due—no polling from workers required until delivery time.

```python
# Delivered after 60 seconds
my_task.apply_async(countdown=60)

# Delivered at a specific time
my_task.apply_async(eta=datetime.now() + timedelta(hours=1))
```

### Priority Support with Sorted Sets

Unlike the standard Redis transport which uses separate queues per priority level, `celery-redis-plus` uses a single sorted set per queue with a score that encodes both priority (0-255) and timestamp. This provides:

- **Full priority range**: All 256 priority levels (0-255) are supported
- **Atomic consumption**: BZMPOP ensures exactly-once delivery
- **FIFO within priority**: Messages with the same priority are delivered in order

Score formula: `(255 - priority) × 10¹⁰ + timestamp_ms + delay_ms`

### Reliable Fanout with Redis Streams

The standard Redis transport uses PUB/SUB for fanout exchanges, which means messages are lost if no subscribers are listening. `celery-redis-plus` uses Redis Streams with XREAD instead:

- **Durable messages**: Messages persist in the stream
- **True broadcast**: Every consumer receives every message (each tracks their own position)
- **Automatic cleanup**: Old messages are trimmed from streams based on `stream_maxlen`

## Installation

```bash
pip install celery-redis-plus
```

Or with uv:

```bash
uv add celery-redis-plus
```

## Quick Start

```python
from celery import Celery
import celery_redis_plus  # Auto-registers signal handler

app = Celery('myapp')
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
})

# Register the bootstep for worker-side delayed delivery processing
celery_redis_plus.configure_app(app)

@app.task
def my_task():
    print("Hello!")

# Tasks with countdown/eta will use native Redis delayed delivery
my_task.apply_async(countdown=60)  # Delivered after 60 seconds
my_task.apply_async(eta=datetime.now() + timedelta(hours=1))  # Delivered in 1 hour

# Priority support works out of the box
my_task.apply_async(priority=0)    # Highest priority
my_task.apply_async(priority=255)  # Lowest priority
```

## How It Works

### Sorted Set Queues

Regular queues use Redis sorted sets (ZSETs) instead of lists:

1. **Publishing**: Messages are added with `ZADD` using a score that encodes priority and timestamp
2. **Consuming**: Workers use `BZMPOP` to atomically pop the lowest-scored (highest priority, oldest) message
3. **Visibility timeout**: Unacknowledged messages are tracked and can be recovered

### Delayed Delivery

1. **Signal Handler**: When a task with `eta` or `countdown` is published, a signal handler calculates the delay
2. **Score Encoding**: The delay is added to the message's score, pushing it into the future
3. **Automatic Delivery**: Messages become "visible" when their score timestamp is reached
4. **Background Processing**: A worker-side bootstep handles message recovery and cleanup

### Stream-based Fanout

For fanout exchanges (broadcast patterns):

1. **Publishing**: Messages are added to Redis Streams with `XADD`
2. **Reading**: Each consumer uses `XREAD` to read messages, tracking their own position
3. **True broadcast**: Every consumer gets every message (no consumer groups)
4. **Cleanup**: Old messages are trimmed based on `stream_maxlen`

## Configuration

### Transport Options

Configure via Celery's `broker_transport_options`:

```python
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',        # Prefix for all Redis keys
        'visibility_timeout': 3600,          # Seconds before unacked messages are reclaimed
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

## Components

### `celery_redis_plus.Transport`

Custom Redis transport that extends `kombu.transport.redis.Transport` with:
- Sorted set queues with BZMPOP
- Priority encoding in scores
- Delayed delivery support
- Redis Streams for fanout

### `celery_redis_plus.DelayedDeliveryBootstep`

Celery bootstep that starts background threads for:
- Processing delayed messages
- Reclaiming timed-out messages
- Cleaning up acknowledged stream messages

### `celery_redis_plus.configure_app(app)`

Helper function to register the bootstep with a Celery application.

### `celery_redis_plus.DELAY_HEADER`

The header name used for delay information (`x-delay`).

## Requirements

- Python >= 3.13
- Celery >= 5.6.1
- Redis >= 7.0 (for BZMPOP support) or Valkey >= 7.0

## Compatibility

Tested with:
- Redis (latest)
- Valkey (latest)

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

## License

MIT License - see [LICENSE](LICENSE) for details.
