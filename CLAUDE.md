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
2. **Single Queue System**: All messages go to `queue:{name}` with score = `(255 - priority) × 10¹³ + timestamp_ms`. Delayed messages have future timestamps, causing them to be delivered later
3. **Unified Requeue**: A single Lua script handles both delayed message delivery and visibility timeout restoration via the `messages_index` sorted set

### Key Components

- **`Transport`** (extends `kombu.transport.virtual.Transport`): Custom transport with `supports_native_delayed_delivery` flag. Delayed delivery is handled via `register_with_event_loop` which sets up periodic callbacks for processing delayed messages and updating visibility timeouts.
- **`Channel`** (extends `kombu.transport.virtual.Channel`): Uses BZMPOP for consuming from sorted sets, XREAD for fanout streams
- **`DelayedDeliveryBootstep`**: Celery consumer bootstep that integrates with the worker lifecycle


### Configuration

For Valkey, use the `valkey://` URL scheme:
```
valkey://localhost:6379/0
```

For Redis, use `broker_transport` with a standard `redis://` URL:
```python
broker_url = "redis://localhost:6379/0"
broker_transport = "celery_redis_plus.transport:Transport"
```

### Constants

- `PRIORITY_SCORE_MULTIPLIER`: `10¹³` - multiplier for priority in score calculation
- `QUEUE_KEY_PREFIX`: `queue:` - prefix for queue sorted sets (avoids collision with list-based queues)
- `MESSAGE_KEY_PREFIX`: `message:` - prefix for per-message hash keys
- `MESSAGES_INDEX_PREFIX`: `messages_index:` - prefix for per-queue message index sorted sets
- `DEFAULT_VISIBILITY_TIMEOUT`: `300` - seconds before unacked messages are requeued
- `DEFAULT_REQUEUE_CHECK_INTERVAL`: `60` - interval for checking messages to requeue
- `DEFAULT_REQUEUE_BATCH_LIMIT`: `1000` - max messages processed per requeue cycle

### Redis Keys

- `queue:{name}`: Sorted set storing delivery_tags with priority+timestamp scores (uses `queue:` prefix to avoid collision with list-based queues)
- `message:{delivery_tag}`: Hash storing message payload, routing_key, priority, and flags
- `messages_index:{name}`: Per-queue sorted set storing `{delivery_tag: queue_at}` for visibility timeout and delayed delivery
- `/{db}.{exchange}`: Redis Stream for fanout messages
- `_kombu.binding.{exchange}`: Set storing queue-exchange bindings

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis and Valkey (marked with `@pytest.mark.integration`). Unit tests mock the Redis client.
