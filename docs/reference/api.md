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
| `delivery_limit` | `int` or `None` | `20` | Delivery attempts before a message is dropped (`None` = no limit) |
| `blocking_timeout` | `int` | `10` | Seconds BZMPOP and XREAD block on the server per poll |
| `queue_expires` | `int` or `None` | `None` | Expiry in seconds for queues declared without `x-expires`; also puts TTLs on binding tables and fanout streams |
| `global_keyprefix` | `str` | `""` | Prefix for all Redis keys |
| `stream_maxlen` | `int` | `10000` | Max messages per fanout stream (approximate) |

!!! warning "`blocking_timeout` is not kombu's `polling_interval`"

    Both answer "how long to wait", but they are opposite mechanisms.
    `blocking_timeout` is how long the server holds the BZMPOP or XREAD open,
    during which a message is delivered the instant it arrives.
    `polling_interval` is kombu's sleep *between* unsuccessful polls, and this
    transport disables it, exactly as kombu's own Redis transport does. A sleep
    on top of a blocking read only delays a reply that is already on its way.

    Setting `polling_interval` in `broker_transport_options` still works: it is
    read as `blocking_timeout` and logs a deprecation warning, and the sleep
    stays off.

    Keep `blocking_timeout` below `socket_timeout` if you set one. The poll is
    an ordinary read on the connection, so a socket timeout shorter than the
    block turns every empty poll into a read timeout and a reconnect.

!!! note "`queue_expires` cleans up the broker, deployment-wide"

    With `queue_expires` set, queues and their message indexes carry TTLs (as
    if each queue had `x-expires`), and so do binding tables and fanout
    streams. Everything is refreshed by the same declares, publishes and
    periodic refreshes that keep queues alive, so an abandoned deployment's
    keys expire on their own. The exception is `message:{tag}` hashes: they
    only get a TTL from `message_ttl` (or a queue's `x-message-ttl`), and once
    a queue's index has expired no sweep can reach them again, so pair
    `queue_expires` with `message_ttl` if unconsumed payloads must not outlive
    their queue. A per-queue `x-expires` still wins over the global value, and
    the same 10-second floor applies.

    Set it in every process sharing the broker. A process running without the
    option never refreshes these TTLs, so routes it depends on could expire
    from under it (the durable-exchange redeclare path heals this, at the cost
    of a retry). Size it like `x-expires`: longer than the longest gap in which
    no worker, producer, or refresh timer touches the busiest queue.

!!! note "How `delivery_limit` counts"

    The counter follows RabbitMQ quorum queues: it counts delivery attempts,
    not redeliveries, so a message is dropped on its 20th delivery rather than
    after 20 redeliveries. Both involuntary redeliveries (the visibility
    timeout expiring) and voluntary ones (`reject(requeue=True)`, a worker
    handing messages back on shutdown) increment it. A message that is still
    sitting in its queue because no worker has got to it yet is a backlog, not
    a redelivery, and does not count.

    Consumed messages carry the current count in the `x-delivery-count` header
    and have `delivery_info["redelivered"]` set once it is above zero, which is
    what Celery's `worker_deduplicate_successful_tasks` reads.

    Dropped messages are deleted outright. There is no dead-letter queue yet,
    so set `delivery_limit: None` if you would rather have a poison message
    redeliver forever than disappear.

!!! note "Sizing `visibility_timeout`"

    Only unacknowledged messages have a deadline, so with Celery's default
    `task_acks_late = False` a running task is not covered at all: the message
    is acked the moment the pool accepts the task, before the task body starts,
    and nothing can redeliver it afterwards. That also means a task lost to a
    worker crash is not retried. The rest of this note applies to
    `task_acks_late = True`, where the message stays unacknowledged for the
    whole run, and to messages sitting in a worker's prefetch buffer.

    Consuming workers push the deadline forward every `visibility_timeout / 3`
    seconds, but that refresh is an event-loop timer and the event loop stops
    ticking while the worker drains and while it reconnects to the broker. A
    task that is still running across a broker reconnect or a shutdown drain
    gets no refresh, so `visibility_timeout` has to cover the longest task
    runtime plus the termination grace period plus however long a reconnect may
    take. If it does not, another worker picks the message up while the first
    one is still on it.

    Under `--pool=solo` there is no refresh at all. The solo pool runs each task
    inline on the main thread, so the event loop is frozen for the whole task
    and the timer never fires. Size `visibility_timeout` above your longest
    single task, the same way you would with no refresh mechanism. A lone solo
    worker gets away with it because its own requeue scan is frozen too, but a
    second worker of any pool type will reclaim the message and run it again.
    `prefork` and `threads` are unaffected: they execute tasks off the main
    thread, so the event loop keeps ticking and the refresh works.

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
| `_kombu.binding.{exchange}` | Sorted Set | Queue-exchange bindings, scored with the unix time each binding goes stale (`+inf` for a queue without `x-expires`) |

## Constants

The following constants are used internally and define default behavior:

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_VISIBILITY_TIMEOUT` | `300` | Default visibility timeout (5 minutes) |
| `DEFAULT_REQUEUE_CHECK_INTERVAL` | `60` | Interval for checking messages to requeue |
| `DEFAULT_REQUEUE_BATCH_LIMIT` | `1000` | Max messages processed per requeue cycle |
| `DEFAULT_STREAM_MAXLEN` | `10000` | Default max length for fanout streams |
| `DEFAULT_MESSAGE_TTL` | `-1` | Default TTL for message hashes (no TTL) |
| `DEFAULT_QUEUE_EXPIRES` | `None` | Default global queue expiry (queues persist) |
| `DROPPED_REPORT_LIMIT` | `10` | Max dropped messages named per queue per sweep in the error log |
| `PRIORITY_SCORE_MULTIPLIER` | `10^13` | Multiplier for priority in score calculation |
| `QUEUE_KEY_PREFIX` | `"queue:"` | Prefix for queue sorted sets |
| `MESSAGE_KEY_PREFIX` | `"message:"` | Prefix for message hashes |
| `MESSAGES_INDEX_PREFIX` | `"messages_index:"` | Prefix for per-queue message index sorted sets |
| `MIN_QUEUE_EXPIRES` | `10000` | Floor under `x-expires`, in milliseconds |
| `MIN_BINDING_LIFETIME` | `300` | Floor under how long a binding survives without a refresh, in seconds |
