# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
# Create virtual environment and install with development dependencies (using uv)
uv venv
uv sync --group dev

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_transport.py

# Run a specific test
uv run pytest tests/test_transport.py::TestDelayedDeliveryQueues::test_message_with_long_delay_goes_to_delayed_queue

# Run linter
uv run ruff check

# Run linter with auto-fix
uv run ruff check --fix

# Run type checker
uv run ty check
```

## Architecture

celery-redis-plus is a drop-in replacement Redis transport for Celery that uses:
- BZMPOP + sorted sets for regular queues (priority support + reliability)
- Redis Streams with XREAD for fanout exchanges (true broadcast)
- Native delayed delivery integrated into sorted set scoring

### Message Flow

1. **Custom Transport** (`transport.py`): The `Channel._put` method parses the `eta` header (ISO datetime) to compute delay. Messages with long delays (> 60s) go to a separate delayed queue; short delays use timing in the sorted set score
2. **Two-Queue System**: `{queue}` for immediate/short delays, `{queue}:delayed` for long delays. A background thread moves ready messages from delayed to main queue
3. **Sorted Set Scoring**: Messages are stored in sorted sets with score = `(255 - priority) × 10¹⁰ + timestamp_ms`. Lower score = higher priority = delivered first

### Key Components

- **`Transport`** (extends `kombu.transport.virtual.Transport`): Custom transport with `supports_native_delayed_delivery` flag
- **`Channel`** (extends `kombu.transport.virtual.Channel`): Uses BZMPOP for consuming from sorted sets, XREAD for fanout streams
- **`DelayedDeliveryBootstep`**: Celery consumer bootstep (currently no-op since delay is handled inline via scoring)
- **`configure_app(app)`**: Registers the bootstep with a Celery app via `app.steps["consumer"].add()`

### Configuration

The broker URL must use the custom transport path:
```
celery_redis_plus.transport:Transport://localhost:6379/0
```

### Constants

- `PRIORITY_SCORE_MULTIPLIER`: `10¹⁰` - multiplier for priority in score calculation
- `DEFAULT_VISIBILITY_TIMEOUT`: `300` - seconds before unacked messages are restored
- `DEFAULT_DELAYED_CHECK_INTERVAL`: `60` - threshold in seconds for routing to delayed queue
- `DELAYED_QUEUE_SUFFIX`: `":delayed"` - suffix for delayed message queues

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis and Valkey (marked with `@pytest.mark.integration`). Unit tests mock the Redis client.
