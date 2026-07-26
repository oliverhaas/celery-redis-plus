# API Reference

## Transport

### `celery_redis_plus.Transport`

Custom transport with sorted set queues, priority encoding, delayed delivery, and Redis Streams fanout. This is the default transport behind the `valkey://` scheme; for the consumer-group based streams transport see [Streams Transport](#streams-transport) below.

**Usage:**

```python
# For Valkey
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
})

# For Redis
app.config_from_object({
    'broker_url': 'redis://localhost:6379/0',
    'broker_transport': 'celery_redis_plus.transport:Transport',
})
```

**Features:**

- Sorted set queues with `BZMPOP` for atomic consumption
- Full 256-level priority support (0-255, higher = more important)
- Native delayed delivery using sorted set timestamps
- Redis Streams for reliable fanout messaging

## Configuration Options

### `broker_transport_options`

All options are passed via Celery's `broker_transport_options` configuration.

#### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `visibility_timeout` | `int` | `300` | Seconds before unacked messages are reclaimed |
| `global_keyprefix` | `str` | `""` | Prefix for all Redis keys |
| `stream_maxlen` | `int` | `10000` | Max messages per fanout stream (approximate) |

#### Message Storage Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `message_key_prefix` | `str` | `"message:"` | Prefix for per-message hash keys |
| `message_ttl` | `int` | `-1` | TTL in seconds for message hashes (-1 = no TTL) |

#### Connection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `socket_timeout` | `float` | `None` | Socket timeout in seconds |
| `socket_connect_timeout` | `float` | `None` | Socket connection timeout in seconds |
| `socket_keepalive` | `bool` | `None` | Enable TCP keepalive |
| `socket_keepalive_options` | `dict` | `None` | TCP keepalive options |
| `max_connections` | `int` | `10` | Maximum connections in pool |
| `health_check_interval` | `int` | `25` | Health check interval in seconds |
| `retry_on_timeout` | `bool` | `None` | Retry on timeout |
| `client_name` | `str` | `None` | Redis client name for `CLIENT SETNAME` |
| `credential_provider` | `object` | `None` | Redis credential provider for dynamic auth (e.g. token rotation) |
| `ssl` | `bool` or `dict` | `None` | SSL/TLS configuration |

#### Fanout Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fanout_prefix` | `bool` or `str` | `True` | Prefix for fanout streams (`True` uses `/{db}.`) |
| `fanout_patterns` | `bool` | `True` | Enable pattern-based fanout routing |

#### Advanced Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sep` | `str` | `"\x06\x16"` | Separator for binding key encoding |

### Example Configuration

```python
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',
        'visibility_timeout': 600,
        'stream_maxlen': 50000,
        'message_ttl': 259200,  # 3 days
        'max_connections': 20,
        'health_check_interval': 30,
    },
})
```

## Redis Keys

The transport uses the following Redis key patterns:

| Pattern | Type | Description |
|---------|------|-------------|
| `queue:{name}` | Sorted Set | Queue storing delivery tags with priority+timestamp scores |
| `message:{delivery_tag}` | Hash | Message payload, routing key, priority, and flags |
| `messages_index:{name}` | Sorted Set | Per-queue index tracking `{delivery_tag: queue_at}` for visibility timeout and delayed delivery |
| `/{db}.{exchange}` | Stream | Fanout messages |
| `_kombu.binding.{exchange}` | Set | Queue-exchange bindings |

## Constants

The following constants are used internally and define default behavior:

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_VISIBILITY_TIMEOUT` | `300` | Default visibility timeout (5 minutes) |
| `DEFAULT_REQUEUE_CHECK_INTERVAL` | `60` | Interval for checking messages to requeue |
| `DEFAULT_REQUEUE_BATCH_LIMIT` | `1000` | Max messages processed per requeue cycle |
| `DEFAULT_STREAM_MAXLEN` | `10000` | Default max length for fanout streams |
| `DEFAULT_MESSAGE_TTL` | `-1` | Default TTL for message hashes (no TTL) |
| `PRIORITY_SCORE_MULTIPLIER` | `10^13` | Multiplier for priority in score calculation |
| `QUEUE_KEY_PREFIX` | `"queue:"` | Prefix for queue sorted sets |
| `MESSAGE_KEY_PREFIX` | `"message:"` | Prefix for message hashes |
| `MESSAGES_INDEX_PREFIX` | `"messages_index:"` | Prefix for per-queue message index sorted sets |

## Streams Transport

### `celery_redis_plus.streams.Transport`

Second transport built on Redis Streams with consumer groups. `XREADGROUP`
delivers a message and registers it in the broker-native Pending Entries List
(PEL) in one atomic step, so the broker itself tracks in-flight work. Fanout is
shared with the sorted set transport.

**Usage:**

```python
# For Valkey (use valkeys-streams:// for SSL)
app.config_from_object({
    'broker_url': 'valkey-streams://localhost:6379/0',
})

# For Redis
app.config_from_object({
    'broker_url': 'redis://localhost:6379/0',
    'broker_transport': 'celery_redis_plus.streams:Transport',
})
```

**Features:**

- One stream per (queue, priority level), consumed through a consumer group via `XREADGROUP`
- Broker-native in-flight tracking: deliver and PEL-register happen in one atomic step
- Heartbeat via `XCLAIM ... JUSTID` keeps long-running tasks alive without bumping delivery counts
- Native delayed delivery via a staging sorted set and a Lua pump
- Poison message handling: delivery-count cap with optional dead-letter stream
- Ack removes entries with XACK + XDEL, so streams shrink on every ack

### Streams Transport Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `priority_steps` | `list[int]` | `[0, 3, 6, 9]` | Priority buckets in 0-255 space; one stream per (queue, level). A message goes to the highest step less than or equal to its priority |
| `visibility_timeout` | `int` | `300` | Seconds of heartbeat silence before a worker's in-flight messages are reclaimed by peers. NOT a task duration limit |
| `heartbeat_interval` | `float` | `visibility_timeout / 5` | Interval of the `XCLAIM ... JUSTID` heartbeat on in-flight messages |
| `max_restore_count` | `int` | `None` | Involuntary-redelivery cap; messages exceeding it are dropped (or dead-lettered). A graceful-shutdown handoff also adds one to the count (the peer reclaims it on its next reclaim pass), so leave headroom for rolling restarts |
| `dead_letter_stream` | `str` | `None` | Stream to copy poisoned messages to before dropping them (capped at about 10000 entries). Prefixed with `global_keyprefix` like every other key. Must not start with `stream:`, the queue namespace: a copy into a queue's own level stream would be redelivered and re-dead-lettered forever, so that value is rejected at connection setup |
| `consumer_group` | `str` | `"celery"` | Consumer group name used on every queue stream |
| `consumer_name` | `str` | `None` | Stable per-worker consumer identity; defaults to the worker nodename when available, else `hostname:pid` |
| `global_keyprefix` | `str` | `""` | Prefix for all Redis keys |
| `message_ttl` | `int` | `None` | Message TTL in seconds, enforced lazily at delivery (`None` = no TTL) |

Connection options (`socket_timeout`, `max_connections`, `ssl`, ...) and fanout
options (`stream_maxlen`, `fanout_prefix`, `fanout_patterns`) are identical to
the sorted set transport.

### Streams Redis Keys

| Pattern | Type | Description |
|---------|------|-------------|
| `stream:{queue}:{level}` | Stream | One per (queue, priority level); each entry holds the serialized message in a single `payload` field |
| `delayed:{queue}` | Sorted Set | Delayed messages; member = serialized message, score = absolute delivery time in ms |
| `/{db}.{exchange}` | Stream | Fanout messages (shared with the sorted set transport) |
| `_kombu.binding.{exchange}` | Set | Queue-exchange bindings (shared with the sorted set transport) |

A consumer group (default name `celery`) is created lazily on every queue stream
via `XGROUP CREATE ... MKSTREAM`. Consumer groups are deleted together with
their stream on purge/delete and recreated on next use. Consumers that are idle
longer than 12 x `visibility_timeout` and have no pending entries are removed
periodically with `XGROUP DELCONSUMER`.

### Streams Queue Length Reporting

`queue_declare` message counts, `SimpleQueue.qsize()` and `celery amqp
queue.declare` report the messages **available to be consumed**: `XLEN` summed
over the queue's level streams, minus that consumer group's pending (in-flight)
entries, plus the delayed sorted set. Messages a worker is currently processing
are not counted. This matches the sorted set transport, where consuming pops the
tag out of `queue:{name}`.

Two consequences worth knowing:

- `queue_delete(if_empty=True)` and `exchange_delete` will delete a queue whose
  messages are all in flight, destroying them. The sorted set transport has the
  same behavior. The worker path and auto-delete-on-close do not pass
  `if_empty`.
- A message whose consumer died stays pending until the reclaim sweep takes it,
  so it stays uncounted for that window. The sorted set transport counts such a
  message again as soon as its visibility timeout expires. If you need stranded
  work to be visible, alert on consumer group lag (`XPENDING`) rather than on
  queue length.

`queue_purge` reports what it actually destroyed, in-flight entries included, so
it can exceed a `queue_declare` count taken moments earlier.

### Streams Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `STREAM_KEY_PREFIX` | `"stream:"` | Prefix for per-level queue streams |
| `DELAYED_KEY_PREFIX` | `"delayed:"` | Prefix for delayed-message sorted sets |
| `DEFAULT_PRIORITY_STEPS` | `[0, 3, 6, 9]` | Default priority buckets |
| `DEFAULT_CONSUMER_GROUP` | `"celery"` | Default consumer group name |
| `SHUTDOWN_IDLE_MS` | `2^31` | Artificial idle applied at graceful shutdown so peers reclaim instantly |
| `HEARTBEAT_INTERVAL_DIVISOR` | `5` | Default heartbeat interval = `visibility_timeout / 5` |
| `CONSUMER_IDLE_CLEANUP_FACTOR` | `12` | Idle consumers removed after `12 x visibility_timeout` |
