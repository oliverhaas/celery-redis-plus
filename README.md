# celery-redis-plus

Enhanced Redis transport for Celery with native delayed delivery support.

## Overview

`celery-redis-plus` provides a drop-in replacement Redis transport for Celery that implements native delayed message delivery using Redis ZSETs. When you schedule tasks with `countdown` or `eta`, messages are stored in a Redis sorted set and delivered exactly when they're due—no polling from workers required until delivery time.

## Features

- **Native delayed delivery**: Uses Redis ZSETs for efficient delayed message handling
- **Drop-in replacement**: Works with existing Celery applications with minimal changes
- **No upstream changes required**: Works without modifications to Celery or Kombu
- **Efficient**: Messages are moved to queues only when ready for delivery

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
```

## How It Works

1. **Signal Handler**: When a task with `eta` or `countdown` is published, a signal handler adds an `x-celery-delay-seconds` header to the message.

2. **Custom Transport**: The custom Redis transport intercepts messages with this header and stores them in a Redis ZSET (sorted set) with the delivery timestamp as the score.

3. **Background Processing**: A worker-side bootstep starts a background thread that periodically checks the ZSET and moves ready messages to the actual queue.

4. **Atomic Operations**: Message transfers use Lua scripts for atomicity, preventing race conditions.

## Components

### `celery_redis_plus.Transport`

Custom Redis transport that extends `kombu.transport.redis.Transport` with delayed delivery support.

### `celery_redis_plus.DelayedDeliveryBootstep`

Celery bootstep that starts the background thread for processing delayed messages.

### `celery_redis_plus.configure_app(app)`

Helper function to register the bootstep with a Celery application.

### `celery_redis_plus.DELAY_HEADER`

The header name used for delay information (`x-celery-delay-seconds`).

## Requirements

- Python >= 3.13
- Celery >= 5.6.1
- Redis >= 7.1.0

## Development

```bash
# Clone the repository
git clone https://github.com/yourusername/celery-redis-plus.git
cd celery-redis-plus

# Create virtual environment and install with development dependencies
uv venv
uv sync --extra dev

# Or with pip
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Run linter
ruff check .
```

## License

MIT License - see [LICENSE](LICENSE) for details.
