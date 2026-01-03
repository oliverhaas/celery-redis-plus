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
uv run pytest tests/test_transport.py::TestDelayedMessageStorage::test_message_with_eta_goes_to_main_queue

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
- Unified requeue mechanism for both delayed and timed-out messages

### Message Flow

1. **Custom Transport** (`transport.py`): The `Channel._put` method parses the `eta` header (ISO datetime) to compute delay. All messages go to the main queue with score based on eta timestamp
2. **Single Queue System**: All messages go to `{queue}` with score = `(255 - priority) × 10¹⁰ + timestamp_ms`. Delayed messages have future timestamps, causing them to be delivered later
3. **Unified Requeue**: A single Lua script handles both delayed message delivery and visibility timeout restoration via the `messages_index` sorted set

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
- `DEFAULT_VISIBILITY_TIMEOUT`: `300` - seconds before unacked messages are requeued
- `DEFAULT_REQUEUE_CHECK_INTERVAL`: `60` - interval for checking messages to requeue
- `DEFAULT_REQUEUE_BATCH_LIMIT`: `1000` - max messages processed per requeue cycle

### Redis Keys

- `messages`: Hash storing `{delivery_tag: [message, exchange, routing_key]}`
- `messages_priority`: Hash storing `{delivery_tag: priority}` for requeue scoring
- `messages_index`: Sorted set storing `{delivery_tag: try_requeue_at}` for requeue timing
- `{queue}`: Sorted set storing messages with priority+timestamp scores

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis and Valkey (marked with `@pytest.mark.integration`). Unit tests mock the Redis client.
