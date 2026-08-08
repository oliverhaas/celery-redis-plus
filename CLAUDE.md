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
uv run pytest tests/test_transport.py::TestDelayedMessageStorage::test_message_with_eta_goes_to_messages_index

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
- `DEFAULT_DELIVERY_LIMIT`: `20` - max times a message may be delivered before being dropped (None = no limit)
- `DROPPED_REPORT_LIMIT`: `10` - max dropped messages named per queue per sweep in the error log (the drop deletes the hash, so the log line is the message's last trace)
- `DEFAULT_QUEUE_EXPIRES`: `None` - global fallback expiry (seconds) for queues declared without `x-expires`; when set, binding keys and fanout streams get TTLs too
- `MIN_QUEUE_EXPIRES`: `10_000` - floor under `x-expires`, in milliseconds
- `MIN_BINDING_LIFETIME`: `300` - floor under how long a binding survives without a refresh, in seconds

### Redis Keys

- `queue:{name}`: Sorted set storing delivery_tags with priority+timestamp scores (uses `queue:` prefix to avoid collision with list-based queues)
- `message:{delivery_tag}`: Hash storing message payload, routing_key, priority, flags, and `delivery_count`
- `messages_index:{name}`: Per-queue sorted set storing `{delivery_tag: queue_at}` for visibility timeout and delayed delivery

### Delivery Count

Messages track how many times they've been redelivered. The `delivery_count` field in the message hash is incremented by `enqueue_due_messages` on a visibility timeout restore and by `requeue_message` on reject-with-requeue or a worker shutdown restore. These are the same two paths RabbitMQ counts for `x-delivery-count`. A message that is merely backlogged in the queue past its deadline was never delivered, so it is not counted.

- `delivery_limit` transport option (default `20`, `None` disables): counts attempts, as RabbitMQ's quorum queue `delivery-limit` does. `delivery_count` is 0 on a first delivery, so the check is `delivery_count >= delivery_limit` and a limit of 3 allows a first delivery plus two redeliveries. Only `enqueue_due_messages` enforces it, so a requeued message is dropped at its next deadline rather than immediately. Drops are logged at ERROR with up to `DROPPED_REPORT_LIMIT` payload descriptions returned from the Lua script
- `enqueue_due_messages` returns a `SweepStats` NamedTuple `(enqueued, dropped, redelivered, orphaned)`; redeliveries and orphan cleanups are logged at INFO
- `x-delivery-count` header: injected into the payload's top-level `headers` map (the only place kombu's virtual `Message` reads headers from) when `delivery_count > 0`
- `delivery_info["redelivered"]`: set to `True` on the same condition. This is where kombu's Redis transport puts it and the only place celery looks, in `Request` and `trace`, for `worker_deduplicate_successful_tasks`. There is no `redelivered` hash field; both flags are derived from the counter at consume time
- `/{db}.{exchange}`: Redis Stream for fanout messages
- `_kombu.binding.{exchange}`: Sorted set storing queue-exchange bindings, scored with the unix time each binding goes stale

### Binding Lifetime

A binding is scored `x-expires` (in seconds, floored at `MIN_BINDING_LIFETIME`) past its last refresh, or `+inf` when its queue has no `x-expires`. `_queue_bind`, `_refresh_queue_expires` and `_put` all rescore (the latter two with `ZADD GT`, so a short window never pulls back a longer deadline); `get_table` runs `ZREMRANGEBYSCORE ... -inf now` ahead of its read, so cleanup rides the read path, and logs the members it prunes at INFO. `Channel._bindings` maps each queue to its `(exchange, member)` pairs so the refresh knows what to rescore, and is shared across a connection's channels like `_expires` is.

Fanout bindings are never written to the table: `_queue_bind` returns early after registering `_fanout_queues`, because fanout routing reads streams, not the binding table. The fanout branch also `DEL`s a binding key an earlier version left behind, since nothing reads or rescores it anymore.

kombu's own Redis transport writes this key as a plain set. `_queue_bind` converts one in place via `transport_convert_bindings.lua` (inherited members get `+inf`) and retries; `_delete` and `get_table` fall back to `SREM`/`SMEMBERS` on `WRONGTYPE` rather than converting.

Empty-binding-table publishes to durable direct exchanges raise `InconsistencyError` (kombu redeclares and retries); transient direct exchanges get an INFO log and kombu's drop, since their bindings vanish by design with their consumers. This holds regardless of `queue_expires`.

### Global queue_expires

The `queue_expires` transport option (seconds, default `None`) gives every queue declared without `x-expires` the global expiry (`_new_queue` stores it in `_expires`, so it rides the existing refresh machinery). When set, `_queue_bind`/`_put`/`_refresh_queue_expires` also `PEXPIRE` binding keys (GT, bootstrap in `_queue_bind` when the key has no TTL yet) and `_put_fanout` pexpires the stream. Message hashes are the exception: they follow `message_ttl`/`x-message-ttl` only, and an expired index leaves them unreachable, so full broker cleanup needs both options.

### no_ack consumption

Queues consumed with `no_ack=True` (pidbox control/reply) are tracked in `Channel._no_ack_queues`. Both consume paths dequeue such deliveries at pop time: the Lua consume script takes a per-queue no_ack flag (ZREM index + DEL hash instead of ZADD deadline), and `_slow_consume_read` mirrors it. Nothing ever acks these messages, so an index entry left behind would redeliver them on the next sweep.

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis and Valkey (marked with `@pytest.mark.integration`). Unit tests mock the Redis client.
