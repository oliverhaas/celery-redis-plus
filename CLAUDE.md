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
- `DEFAULT_MAX_RESTORE_COUNT`: `None` - max times a message can be restored via visibility timeout before being dropped (None = no limit)

### Redis Keys

- `queue:{name}`: Sorted set storing delivery_tags with priority+timestamp scores (uses `queue:` prefix to avoid collision with list-based queues)
- `message:{delivery_tag}`: Hash storing message payload, routing_key, priority, flags, and `restore_count`
- `messages_index:{name}`: Per-queue sorted set storing `{delivery_tag: queue_at}` for visibility timeout and delayed delivery

### Restore Count

Messages track how many times they've been involuntarily restored (visibility timeout expiry). The `restore_count` field in the message hash is incremented by the `enqueue_due_messages` Lua script on each timeout restore. It is NOT incremented for voluntary requeues (reject+requeue, worker shutdown).

- `max_restore_count` transport option: when set, messages exceeding this count are dropped
- `x-restore-count` header: injected into consumed messages when `restore_count > 0`
- `/{db}.{exchange}`: Redis Stream for fanout messages
- `_kombu.binding.{exchange}`: Set storing queue-exchange bindings

## Streams Transport (streams.py)

A second broker transport built on Redis Streams with consumer groups, selected via the `valkey-streams://` URL scheme (`valkeys-streams://` for SSL), or via `broker_transport = "celery_redis_plus.streams:Transport"` with a `redis://` URL. The registered `valkey+streams`/`valkeys+streams` aliases work only as `broker_transport` values, never in a bare URL: kombu splits bare URL schemes at `+` before alias lookup. Point-to-point queues only; fanout is shared with `transport.py` via `FanoutStreamsMixin`. Core idea: `XREADGROUP` delivers a message and registers it in the broker-native Pending Entries List (PEL) in one atomic step, so the broker itself tracks in-flight work.

### Streams Message Flow

1. **Publish** (`streams.Channel._put`): messages with `eta` further than `DEFAULT_REQUEUE_CHECK_INTERVAL` in the future go to `delayed:{queue}` (ZADD, member = serialized message, score = eta in ms). Everything else is XADDed to `stream:{queue}:{level}`, where level is the highest `priority_steps` step <= the message priority (`priority_to_level`)
2. **Consume cycle** (three phases):
   - Periodic reclaim: a read-only `XPENDING ... IDLE` discovery pass per level stream, paginated by exclusive id range with `min_idle_time = visibility_timeout * 1000`, finds entries idle long enough to belong to a dead worker. Delivery counts come from those same discovery pages. A single counting `XCLAIM` then takes only the surviving ids, so an entry this pass filters out never has its delivery counter bumped; `x-restore-count` = `times_delivered` is injected on redelivery
   - Non-blocking pass: `streams_consume.lua` runs `XREADGROUP COUNT 1` per queue, iterating level streams highest first (XREADGROUP without BLOCK is legal in Lua; fixes the per-stream COUNT problem)
   - Blocking wait only when all watched streams are empty: one `XREADGROUP ... BLOCK` with `COUNT 1` across all watched streams on the poller fd
3. **Ack** (`streams.QoS`): `streams_ack.lua` does XACK + XDEL atomically; streams shrink on every ack, no MAXLEN trimming. `reject(requeue=True)` XADDs a copy then XACK+XDELs the original in the same script; voluntary requeues therefore reset the delivery count
4. **Delayed pump**: `streams_move_delayed.lua` runs on the periodic requeue-check timer, moving due members from `delayed:{queue}` into their priority stream (members expired by x-message-ttl are dropped instead)

### Streams Heartbeat Semantics

- A hub timer every `visibility_timeout / HEARTBEAT_INTERVAL_DIVISOR` seconds runs `XCLAIM ... JUSTID` (batched per stream) over the channel's in-flight messages. JUSTID resets the idle clock WITHOUT bumping delivery counts
- **`visibility_timeout` means "worker considered dead after this much heartbeat silence", NOT maximum task duration.** The 300 s default is safe for a 6-hour task. The heartbeat runs in the worker main process event loop, so pod death stops it while a busy pool child does not
- Graceful shutdown: `streams.QoS.restore_unacked_once` reuses the executor-wait dance from `transport.py`/`signals.py`, then applies `XCLAIM ... IDLE SHUTDOWN_IDLE_MS JUSTID` to all in-flight messages so any peer reclaims them instantly. No payload movement, no re-add; the peer's counting XCLAIM bumps the delivery count, so each handoff costs one restore_count increment on surviving messages
- Poison messages: the PEL delivery count is the native restore count. Above `max_restore_count` the message is XACK+XDELed and dropped, optionally copied to `dead_letter_stream` first. Reject+requeue resets the count (fresh XADD); graceful-shutdown handoffs add one each, so size `max_restore_count` with headroom for rolling restarts

### Streams Constants

- `STREAM_KEY_PREFIX`: `stream:` - prefix for per-level queue streams
- `DELAYED_KEY_PREFIX`: `delayed:` - prefix for delayed-message sorted sets
- `DEFAULT_PRIORITY_STEPS`: `[0, 3, 6, 9]` - priority buckets in 0-255 space
- `DEFAULT_CONSUMER_GROUP`: `celery` - consumer group name on every queue stream
- `SHUTDOWN_IDLE_MS`: `2**31` - artificial idle set at graceful shutdown (~24 days)
- `HEARTBEAT_INTERVAL_DIVISOR`: `5` - heartbeat interval = visibility_timeout / 5
- `CONSUMER_IDLE_CLEANUP_FACTOR`: `12` - XGROUP DELCONSUMER after 12 x visibility_timeout idle (never deletes consumers with pending entries)

### Streams Redis Keys

- `stream:{queue}:{level}`: Stream per (queue, priority level); entry = single `payload` field with the serialized message
- `delayed:{queue}`: Sorted set of delayed messages; member = serialized message JSON, score = delivery time in ms
- Consumer group (default `celery`) on each queue stream, created lazily via `XGROUP CREATE ... MKSTREAM` (BUSYGROUP ignored, cached per channel)
- Fanout streams and `_kombu.binding.{exchange}` keys are shared with `transport.py`

### Streams Queue Management

- `_size` = sum of XLEN over the level streams + ZCARD of the delayed zset
- `_purge`/`_delete` = DEL the level streams and the delayed zset; consumer groups die with the stream and are recreated lazily
- x-expires: periodic PEXPIRE on the level streams and delayed zset (same `_update_expires_timer` pattern)
- x-message-ttl: enforced lazily at delivery; entry IDs encode creation time, older entries are XACK+XDELed and skipped

## Testing

Tests use pytest with fixtures in `conftest.py`. Integration tests use testcontainers for Redis and Valkey (marked with `@pytest.mark.integration`). Unit tests mock the Redis client.
