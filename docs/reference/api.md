# API Reference

## Transport

### `celery_redis_plus.Transport`

Custom transport with sorted set queues, priority encoding, delayed delivery, and Redis Streams fanout.

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
| `messages_index_prefix` | `str` | `"messages_index:"` | Prefix for per-queue messages index sorted sets |

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
