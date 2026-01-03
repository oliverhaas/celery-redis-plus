# Complete Message Flow Walkthrough

## 1. Application Startup

### 1.1 Configuring the Application

```python
from celery import Celery
import celery_redis_plus

app = Celery('myapp')
app.config_from_object({
    'broker_url': 'celery_redis_plus.transport:Transport://localhost:6379/0',
})
celery_redis_plus.configure_app(app)  # Registers DelayedDeliveryBootstep
```

When the transport module is imported (`transport.py:1388-1396`):
- The `Transport.__init__` imports the `signals` module
- This triggers `@before_task_publish.connect` decorator in `signals.py:17`, registering `_convert_eta_to_properties`

### 1.2 Worker Startup

When a worker starts:

1. **Transport initialization** (`transport.py:1388-1396`):
   - Creates `MultiChannelPoller` to manage async I/O
   - Imports signals module (registers eta conversion handler)

2. **Channel creation** (`transport.py:647-681`):
   - Creates Redis connection pools (sync and async)
   - Initializes message handlers: `{"BZMPOP": _bzmpop_read, "XREAD": _xread_read}`
   - Pings Redis to verify connection
   - Registers channel with `MultiChannelPoller`

3. **Event loop registration** (`transport.py:1401-1430`):
   - `on_poll_init()`: Initial message recovery
   - `on_poll_start()`: Called every tick, registers BZMPOP/XREAD commands
   - `maybe_requeue_messages()`: Called every **60 seconds** (DEFAULT_REQUEUE_CHECK_INTERVAL)
   - `maybe_update_messages_index()`: Called every **visibility_timeout/3 seconds** (~100s default)

4. **Bootstep** (`bootstep.py:35-71`):
   - Checks `transport.supports_native_delayed_delivery` flag
   - Calls `setup_native_delayed_delivery()` (currently a no-op, event loop handles it)

---

## 2. Publishing a Message

### 2.1 Message WITHOUT eta (Immediate Delivery)

```python
my_task.apply_async()  # or my_task.delay()
```

**Flow:**

1. **Signal handler** (`signals.py:17-64`):
   - No eta in headers → does nothing

2. **Channel._put()** (`transport.py:1059-1108`):
   - `eta_timestamp = None` → uses `visible_at = now`
   - All messages go directly to the main queue:
     ```python
     queue_score = _queue_score(priority, visible_at)
     queue_at = now + visibility_timeout  # When to (re)queue if not acked
     ```

3. **Score calculation** (`transport.py:83-101`):
   ```python
   score = (255 - priority) * 10^10 + timestamp_ms
   ```
   - Priority 0 (default) → score ≈ 255 × 10¹⁰ + current_ms
   - Priority 255 (highest) → score ≈ 0 + current_ms

4. **Redis operations** (pipeline, atomic):
   ```
   HSET message:{delivery_tag} payload {json} routing_key {queue} priority {priority} redelivered 0 delayed 0
   EXPIRE message:{delivery_tag} {message_ttl}  # Default 3 days
   ZADD messages_index {delivery_tag: queue_at}
   ZADD {queue} {delivery_tag: queue_score}
   ```

---

### 2.2 Message WITH eta (Delayed Delivery)

**Native vs Celery-handled delays:**
- If `delay > DEFAULT_REQUEUE_CHECK_INTERVAL` (60s): Native delayed delivery via score
- If `delay <= 60s`: Treated as immediate, Celery's built-in eta logic handles the short delay

```python
my_task.apply_async(countdown=30)   # 30 second delay
my_task.apply_async(countdown=3600) # 1 hour delay
```

**Flow:**

1. **Signal handler** (`signals.py:42-64`):
   - Parses `headers.eta` (ISO datetime string)
   - Converts to Unix timestamp
   - Sets `properties["eta"] = timestamp`

2. **Channel._put()** (`transport.py:1059-1108`):
   - All messages (regardless of delay length) go to the main queue
   - Uses eta timestamp in the score:
     ```python
     visible_at = eta_timestamp if eta_timestamp > now else now
     queue_score = _queue_score(priority, visible_at)
     ```
   - Sets `queue_at = eta` (requeue mechanism delivers at eta time)

3. **Redis operations** (same structure as immediate):
   ```
   HSET message:{delivery_tag} payload {json} routing_key {queue} priority {priority} redelivered 0 delayed 1
   EXPIRE message:{delivery_tag} {message_ttl}  # Default 3 days
   ZADD messages_index {delivery_tag: queue_at}  # queue_at = eta
   ZADD {queue} {delivery_tag: queue_score}  # Score includes future timestamp
   ```

The message is in the queue but won't be popped until its score is the lowest (i.e., after the eta time passes).

---

### 2.3 Fanout Message (Broadcast)

```python
# Exchange type = fanout
app.send_task('broadcast_task', routing_key='', exchange='broadcast')
```

**Flow** (`transport.py:1110-1124`):

1. `_put_fanout()` called instead of `_put()`
2. Uses Redis Streams instead of sorted sets:
   ```
   XADD {keyprefix_fanout}/{exchange}/{routing_key} * uuid {uuid} payload {json}
   ```
3. `stream_maxlen` limits stream size (~10000 messages)
4. Every consumer independently reads via XREAD (no consumer groups)

---

## 3. Consuming a Message

### 3.1 Worker Polling

The `MultiChannelPoller.get()` method (`transport.py:536-563`) is the main loop:

1. **Register BZMPOP** for active queues (`transport.py:803-815`):
   ```
   BZMPOP {timeout} {numkeys} queue1 queue2 ... MIN
   ```
   - Blocks until message available or timeout
   - `MIN` pops lowest score (highest priority)

2. **Register XREAD** for fanout queues (`transport.py:845-886`):
   ```
   XREAD COUNT 1 BLOCK {timeout_ms} STREAMS stream1 stream2 ... id1 id2 ...
   ```

3. **Poll** for events using OS-level I/O multiplexing

### 3.2 Message Delivery (BZMPOP)

When BZMPOP returns (`transport.py:817-852`):

```python
result = client.parse_response(connection, "BZMPOP")
# result = (queue_name, [(delivery_tag, score)])

# Fetch message payload from per-message hash
message_key = f"message:{delivery_tag}"
payload_json = client.hget(message_key, "payload")
message = loads(payload_json)

# Deliver to worker
connection._deliver(message, queue)
```

The message is:
- **Removed from sorted set** (by BZMPOP)
- **Still in `message:{delivery_tag}` hash** (for recovery, with TTL)
- **Still in `messages_index`** (for visibility tracking)

### 3.3 Message Delivery (XREAD - Fanout)

When XREAD returns (`transport.py:888-943`):

```python
for stream, message_list in messages:
    for message_id, fields in message_list:
        # Update offset for this stream
        self._stream_offsets[stream] = message_id

        # Parse payload
        payload = loads(fields["payload"])

        # Generate new delivery tag
        delivery_tag = self._next_delivery_tag()

        # Mark as fanout (no ack needed in Redis)
        self.qos._fanout_tags.add(delivery_tag)

        connection._deliver(payload, queue_name)
```

---

## 4. Message Acknowledgment

### 4.1 Successful Processing (ack)

When task completes successfully (`transport.py:316-323`):

```python
def ack(self, delivery_tag):
    if delivery_tag in self._fanout_tags:
        # Fanout: just remove from local tracking
        self._fanout_tags.discard(delivery_tag)
    else:
        # Regular: clean up Redis
        self._remove_from_indices(delivery_tag).execute()
    super().ack(delivery_tag)
```

**Redis cleanup** (`transport.py:346-349`):
```
ZREM messages_index {delivery_tag}
DEL message:{delivery_tag}
```

### 4.2 Failed Processing (reject with requeue)

When task fails and should be retried (`transport.py:325-336`):

```python
def reject(self, delivery_tag, requeue=True):
    if delivery_tag in self._fanout_tags:
        # Fanout: no requeue support
        self._fanout_tags.discard(delivery_tag)
    else:
        if requeue:
            # Restore to front of queue
            self.restore_by_tag(delivery_tag, leftmost=True)
        else:
            self._remove_from_indices(delivery_tag).execute()
```

**Restore transaction** (`transport.py:381-409`):
```python
# In a WATCH/MULTI transaction:
message_key = f"message:{tag}"
payload = HGET message_key "payload"
priority = HGET message_key "priority"
routing_key = HGET message_key "routing_key"
score = 0 if leftmost else _queue_score(priority)
ZADD {routing_key} {tag: score}
# Update payload with redelivered flag
HSET message_key "payload" {payload_with_redelivered=True}
```

---

## 5. Message Recovery (Unified Requeue Mechanism)

### 5.1 Visibility Timeout and Delayed Messages - Unified Flow

Messages have a "visibility timeout" - if not acked within this time, they're considered lost. The same mechanism handles both unacked messages AND delayed messages.

**Key insight**: `messages_index` stores `queue_at` timestamps:
- For immediate messages: `queue_at = now + visibility_timeout`
- For delayed messages: `queue_at = eta` (requeue mechanism delivers at eta time)

**Heartbeat** (`transport.py:351-365`):
- Every `visibility_timeout/3` seconds (~100s)
- Updates `messages_index` score to extend the visibility window:
  ```
  ZADD messages_index {tag1: now + visibility_timeout, tag2: now + visibility_timeout, ...}
  ```
- Keeps messages "alive" while being processed

**Unified Requeue** (`transport.py:1013-1057`):
- Every 60 seconds (DEFAULT_REQUEUE_CHECK_INTERVAL), `requeue_messages()` runs
- Uses Lua script for atomic batch processing
- Processes messages with `queue_at <= now + requeue_check_interval`

### 5.2 Lua Script for Atomic Requeue

The Lua script (`transport.py:905-968`) handles both delayed messages and visibility timeout restoration:

```lua
local messages_index = KEYS[1]
local threshold = tonumber(ARGV[1])
local batch_limit = tonumber(ARGV[2])
local visibility_timeout = tonumber(ARGV[3])
local priority_multiplier = tonumber(ARGV[4])
local message_key_prefix = ARGV[5]
local global_keyprefix = ARGV[6]
local total_requeued = 0

-- Get current time in seconds and milliseconds
local time_result = redis.call('TIME')
local now_sec = tonumber(time_result[1])
local now_ms = now_sec * 1000 + math.floor(tonumber(time_result[2]) / 1000)

-- Get messages ready for requeue (score <= threshold)
local ready = redis.call('ZRANGEBYSCORE', messages_index, '-inf', threshold, 'LIMIT', 0, batch_limit)

for _, tag in ipairs(ready) do
    -- Build prefixed message key
    local message_key = global_keyprefix .. message_key_prefix .. tag

    -- Get fields from per-message hash
    local priority = redis.call('HGET', message_key, 'priority')
    if priority then
        priority = tonumber(priority)
        local routing_key = redis.call('HGET', message_key, 'routing_key')
        local eta = redis.call('HGET', message_key, 'eta')
        eta = eta and tonumber(eta) or 0

        -- Calculate queue score using eta if it's in the future, else use now
        local score_time_ms
        if eta > 0 and eta * 1000 > now_ms then
            score_time_ms = eta * 1000
        else
            score_time_ms = now_ms
        end
        local queue_score = (255 - priority) * priority_multiplier + score_time_ms

        -- Check if this is a native delayed message (first delivery) or a timed-out message (redelivery)
        local native_delayed = redis.call('HGET', message_key, 'native_delayed')
        if native_delayed and tonumber(native_delayed) == 1 then
            -- Native delayed message: clear the flag (this is the first delivery)
            redis.call('HSET', message_key, 'native_delayed', '0')
        else
            -- Timed-out message: mark as redelivered
            redis.call('HSET', message_key, 'redelivered', '1')
        end

        -- Add to the message's queue (with global prefix)
        local queue_key = global_keyprefix .. routing_key
        redis.call('ZADD', queue_key, 'NX', queue_score, tag)

        -- Update queue_at for next cycle (now + visibility_timeout)
        local new_queue_at = now_sec + visibility_timeout
        redis.call('ZADD', messages_index, new_queue_at, tag)
        total_requeued = total_requeued + 1
    else
        -- No message hash = message was already acked/deleted, clean up index
        redis.call('ZREM', messages_index, tag)
    end
end

return total_requeued
```

This ensures:
- Atomic batch processing (no race conditions)
- ZADD NX prevents duplicate enqueues (message already in queue stays untouched)
- Native delayed messages have `native_delayed` flag cleared on first delivery (not marked as redelivered)
- Timed-out messages are marked with `redelivered=1`
- Uses stored `eta` for queue score calculation when the eta is in the future
- Reads `routing_key` from hash to add message to the correct queue
- Batch limit of 1000 messages per cycle (DEFAULT_REQUEUE_BATCH_LIMIT)

### 5.3 Restore by Tag (Lua Script)

When a specific message needs restoration (e.g., on reject with requeue), a Lua script atomically:
1. Reads `priority` and `routing_key` (queue) from the per-message hash
2. Sets `redelivered=1` in the hash
3. Refreshes the TTL
4. Adds the message back to the queue with appropriate score (using global key prefix)

```lua
-- _restore_message_lua
local message_key = KEYS[1]
local leftmost = tonumber(ARGV[1]) == 1
local priority_multiplier = tonumber(ARGV[2])
local message_ttl = tonumber(ARGV[3])
local global_keyprefix = ARGV[4]

-- Get priority and routing_key (queue) from hash
local priority = redis.call('HGET', message_key, 'priority')
local routing_key = redis.call('HGET', message_key, 'routing_key')
if not priority or not routing_key then
    return 0
end

-- Mark as redelivered
redis.call('HSET', message_key, 'redelivered', '1')
redis.call('EXPIRE', message_key, message_ttl)

-- Calculate score (0 for front, normal for back)
local score
if leftmost then
    score = 0
else
    priority = tonumber(priority)
    local now_ms = redis.call('TIME')
    now_ms = tonumber(now_ms[1]) * 1000 + math.floor(tonumber(now_ms[2]) / 1000)
    score = (255 - priority) * priority_multiplier + now_ms
end

-- Add to queue (routing_key with global prefix)
local queue_key = global_keyprefix .. routing_key
local tag = string.match(message_key, ':(.+)$')
redis.call('ZADD', queue_key, score, tag)

return 1
```

The `redelivered` field in the hash is stored for debugging purposes only.

---

## Default Configuration Summary

| Setting | Default Value | Description |
|---------|---------------|-------------|
| **visibility_timeout** | `300` (5 min) | Time before unacked messages are requeued |
| **message_ttl** | `259200` (3 days) | TTL for per-message hashes (auto-cleanup) |
| **health_check_interval** | `25` (sec) | Redis connection health check interval |
| **stream_maxlen** | `10000` | Maximum fanout stream length (approximate) |
| **max_connections** | `10` | Redis connection pool size |
| **polling_interval** | `10` (sec) | Timeout for blocking BZMPOP/XREAD |
| **global_keyprefix** | `""` | Prefix for all Redis keys |
| **fanout_prefix** | `True` | Use `/{db}.` prefix for fanout streams |
| **fanout_patterns** | `True` | Include routing_key in stream keys |
| **socket_timeout** | `None` | Redis socket timeout |
| **socket_connect_timeout** | `None` | Connection establishment timeout |
| **socket_keepalive** | `None` | TCP keepalive setting |
| **retry_on_timeout** | `None` | Retry on socket timeout |
| **client_name** | `None` | Redis CLIENT SETNAME value |
| **ssl** | `False` | SSL/TLS configuration |

### Intervals

| Interval | Value | Constant | What it does |
|----------|-------|----------|--------------|
| **Requeue messages** | Every 60 sec | `DEFAULT_REQUEUE_CHECK_INTERVAL` | Requeue delayed and timed-out messages |
| **Update message index** | Every visibility_timeout/3 (~100 sec) | - | Heartbeat to keep messages alive |
| **Batch limit** | 1000 | `DEFAULT_REQUEUE_BATCH_LIMIT` | Max messages requeued per cycle |
| **Message TTL** | 3 days | `DEFAULT_MESSAGE_TTL` | Auto-expire orphaned message hashes |

### Key Names (with global_keyprefix)

| Key | Purpose |
|-----|---------|
| `{prefix}message:{delivery_tag}` | Hash: `{payload, routing_key, priority, redelivered, native_delayed, eta}` with TTL |
| `{prefix}messages_index` | Sorted set: `{delivery_tag: queue_at}` |
| `{prefix}{queue}` | Sorted set: main queue with priority+timestamp scores |
| `{prefix}_kombu.binding.{exchange}` | Set: exchange bindings |
| `{prefix}/{db}.{exchange}/{routing_key}` | Stream: fanout messages |
