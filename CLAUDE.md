# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
# Create virtual environment and install with development dependencies (using uv)
uv venv
uv sync

# Run all tests
pytest

# Run a single test file
pytest tests/test_signals.py

# Run a specific test
pytest tests/test_signals.py::TestAddDelayHeader::test_eta_in_future_iso_string

# Run linter
ruff check .

# Run linter with auto-fix
ruff check . --fix

# Run type checker
ty check celery_redis_plus/ tests/
```

## Architecture

celery-redis-plus is a drop-in replacement Redis transport for Celery that implements native delayed message delivery using Redis ZSETs. Instead of workers polling for delayed tasks, messages are stored in a sorted set and moved to queues exactly when due.

### Message Flow

1. **Signal Handler** (`signals.py`): The `before_task_publish` signal intercepts tasks with `eta` or `countdown` and adds an `x-celery-delay-seconds` header with the computed delay
2. **Custom Transport** (`transport.py`): The `Channel._put` method checks for the delay header. If present, the message goes to a Redis ZSET (`{queue_name}:delayed`) with the delivery timestamp as score; otherwise, it publishes normally
3. **Background Thread**: The `Transport._delayed_delivery_loop` runs in a daemon thread, polling the ZSET every second and atomically moving ready messages to the queue using a Lua script

### Key Components

- **`Transport`** (extends `kombu.transport.redis.Transport`): Custom transport with `supports_native_delayed_delivery` flag. Creates background thread for delayed message processing
- **`Channel`** (extends `kombu.transport.redis.Channel`): Overrides `_put` to route delayed messages to ZSETs
- **`DelayedDeliveryBootstep`**: Celery consumer bootstep that calls `transport.setup_native_delayed_delivery()` on worker start and `teardown_native_delayed_delivery()` on stop
- **`configure_app(app)`**: Registers the bootstep with a Celery app via `app.steps["consumer"].add()`

### Configuration

The broker URL must use the custom transport path:
```
celery_redis_plus.transport:Transport://localhost:6379/0
```

### Constants

- `DELAY_HEADER`: `"x-celery-delay-seconds"` - header name for delay value
- `DELAYED_QUEUE_SUFFIX`: `":delayed"` - suffix for ZSET keys

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis (marked with `@pytest.mark.integration`). Unit tests mock the Redis client. The `celery_app` fixture uses an in-memory broker with `task_always_eager=True`.
