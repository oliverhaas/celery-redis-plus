# API Reference

## Transport

### `celery_redis_plus.Transport`

Custom transport with sorted set queues, priority encoding, delayed delivery, and Redis Streams fanout.

**Usage:**

```python
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
})
```

**Features:**

- Sorted set queues with `BZMPOP` for atomic consumption
- Full 256-level priority support (0-255, higher = more important)
- Native delayed delivery using sorted set timestamps
- Redis Streams for reliable fanout messaging

## Bootstep

### `celery_redis_plus.DelayedDeliveryBootstep`

Worker bootstep for background message processing and recovery.

**Usage:**

```python
from celery_redis_plus import DelayedDeliveryBootstep

app.steps['consumer'].add(DelayedDeliveryBootstep)
```

**Responsibilities:**

- Background thread for checking and delivering delayed messages
- Recovery of messages that exceeded visibility timeout
- Cleanup of expired message hashes

## Configuration Options

### `broker_transport_options`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `global_keyprefix` | `str` | `""` | Prefix for all Redis keys |
| `visibility_timeout` | `int` | `300` | Seconds before unacked messages are reclaimed |
| `stream_max_length` | `int` | `10000` | Max messages per stream (approximate) |
| `ssl` | `bool` or `dict` | `None` | SSL/TLS configuration |

## Redis Keys

The transport uses the following Redis key patterns:

| Pattern | Type | Description |
|---------|------|-------------|
| `queue:{name}` | Sorted Set | Queue storing delivery tags with priority+timestamp scores |
| `message:{delivery_tag}` | Hash | Message payload, routing key, priority, and flags |
| `messages_index` | Sorted Set | Tracks `{delivery_tag: queue_at}` for visibility timeout and delayed delivery |
| `/{db}.{exchange}` | Stream | Fanout messages |
| `_kombu.binding.{exchange}` | Set | Queue-exchange bindings |
