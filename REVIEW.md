# celery-redis-plus Comprehensive Code Review

## Summary

Overall this is a well-engineered package with solid architecture. The Lua scripts
are correct, the FAST/SLOW consume mode is clever, and the shutdown/ack-drain
logic is thorough. The issues below range from potential production bugs to
hardening opportunities and minor inconsistencies.

### Analysis vs Standard Kombu Redis Transport

Many reported "bugs" actually match the standard Redis transport's behavior:
- **conn_or_acquire** creating new clients: same pattern in standard transport
- **_consume_fast_mode** per-channel: NOT a bug — BZMPOP watches ALL queue keys,
  so switching to SLOW mode doesn't miss messages in other queues
- **Single-pass hub drain**: standard transport doesn't drain at all
- **Client vs server clock**: standard transport also uses client-side time
- **Non-atomic pipeline in SLOW mode**: same as standard BRPOP pattern
- **Single message delivery per _xread_read**: same as standard _brpop_read
- **Re-declare ignoring TTL changes**: matches RabbitMQ semantics

**Actual bug fixed:** `_worker_pool` module-level global (issue #1)
**Dead code removed:** `_get_message_from_hash` (issue #31)

---

## Bugs / Correctness Issues

### 1. `_worker_pool` is a module-level global shared across all apps
**File:** `transport.py:152`
**Severity:** Medium

`_worker_pool` is a single module-level variable set by Celery signals. If two
Celery apps run in the same process (e.g., tests, or a publisher app + consumer
app), the second app's `worker_ready` signal overwrites the first's pool
reference. On `worker_shutdown`, it's set to `None`, which could make
`restore_unacked_once` for a still-running app skip the executor wait.

Consider keying by app/worker identity, or storing the pool reference on the
`QoS` or `Channel` instance instead.

### 2. `conn_or_acquire` creates a new client every call (connection leak risk)
**File:** `transport.py:1780-1785`

When called without a `client` argument, `conn_or_acquire` creates a brand new
`Redis` client each time via `_create_client()`. Unlike a proper connection pool
context manager, these clients are never explicitly closed. While they share the
pool, the `Redis()` wrapper objects accumulate.

The standard Redis transport's `conn_or_acquire` borrows/returns from a pool.
This implementation just creates new wrappers around the same pool, which works
but is wasteful.

### 3. `_consume_fast_mode` reset race in multi-queue scenarios
**File:** `transport.py:1087-1088`

When FAST mode returns nil (queue empty), `_consume_fast_mode` is set to `False`
globally for the channel, affecting ALL queues. If only one queue is empty while
others have messages, the next poll cycle will use SLOW (blocking BZMPOP) even
though FAST mode would have found messages immediately in other queues.

This could increase latency when some queues are hot and others are cold.

### 4. `_drain_hub_callbacks` only does one pass
**File:** `transport.py:438-456`

`_drain_hub_callbacks` calls `hub._pop_ready()` once. If a callback schedules
another callback (e.g., an ack callback triggers cleanup that schedules another
callback), those won't be drained. The atexit version in the memory notes
mentions "up to 10 passes" but that code was removed.

### 5. `enqueue_due_messages` Lua script uses `ZADD NX` but `_requeue_by_tag` also uses `ZADD NX`
**File:** `transport_requeue_message.lua:54`, `transport_enqueue_due_messages.lua:86`

Both use `ZADD NX` to avoid overwriting scores. However, the
`enqueue_due_messages` Lua uses server-side `TIME` for scoring while
`_requeue_by_tag` also uses server-side `TIME`. This is correct and consistent.
But the `_put` method uses client-side `time.time()` for scoring. If server and
client clocks diverge, messages published from the client could have scores that
are out of order relative to messages re-enqueued by the Lua scripts.

### 6. `_slow_consume_read` pipeline is not atomic with ZPOPMIN
**File:** `transport.py:1091-1138`

After BZMPOP pops a message, the pipeline does `ZADD XX` (update index) +
`HMGET` (fetch payload). If the connection drops between BZMPOP and the
pipeline, the message is lost from the queue but the index still has it. It will
eventually be re-enqueued by `enqueue_due_messages`, but there's a window equal
to the visibility timeout where the message is invisible.

This is acceptable for at-least-once semantics, but worth documenting.

### 7. `_xread_read` only delivers the first message from the first stream
**File:** `transport.py:1203-1242`

When XREAD returns messages from multiple streams or multiple messages from one
stream, only the first message is delivered (the method returns `True` after
delivering one). The `_stream_offsets` is only updated for that first message.
Subsequent messages from the same XREAD response are dropped.

With `COUNT 1` in `_xread_start`, this is currently safe. But if COUNT is ever
increased, messages will be lost.

### 8. `_new_queue` doesn't update `_expires` if queue already exists
**File:** `transport.py:1488`

`if x_expires is not None and queue not in self._expires` — if a queue is
re-declared with a different `x-expires`, the new value is silently ignored.
Same for `x-message-ttl`. This matches RabbitMQ behavior (re-declare must use
same args), but could be surprising.

---

## Robustness / Hardening

### 9. No dead letter queue (DLQ) support
When `max_restore_count` is exceeded, messages are silently dropped (DEL in Lua
script). There's no mechanism to route them to a dead letter queue for
inspection. This is a significant gap for production use.

### 10. No metrics or observability hooks
There are no counters for:
- Messages published/consumed/acked/rejected/restored
- Lua script execution times
- Queue depths
- Visibility timeout expiries
- Dropped messages (max_restore_count exceeded)

The `logger.error` for dropped messages is the only signal, and it's easy to
miss. Consider emitting Celery signals or structured log events.

### 11. `enqueue_due_messages` silently continues on per-queue errors
**File:** `transport.py:1345-1350`

Exceptions from the Lua script are caught and logged as warnings. If a queue
consistently fails (e.g., due to corrupt data), messages in that queue will
never be delivered and the only signal is a warning log. Consider escalating
after repeated failures.

### 12. `_ensure_consume_script_sha` doesn't handle cluster resharding
**File:** `transport.py:991-995`

`script_load` returns a SHA that's valid for the node it was loaded on. In a
Redis Cluster, different keys may hash to different nodes. The cached SHA may
not be valid on the node that holds a specific queue key. This would manifest
as NOSCRIPT errors that are handled (SHA is cleared and retried), but it means
every consume cycle in a cluster would reload the script.

### 13. No connection retry logic for `_put`
**File:** `transport.py:1394`

If the Redis connection drops during `_put`, the pipeline fails and the message
is lost from the publisher's perspective. The standard Redis transport has retry
logic via kombu's `ensure` mechanism. This transport doesn't wrap `_put` (or
most other methods) in retry logic.

### 14. `_delete` and `_purge` scan entire sorted sets
**File:** `transport.py:1515-1538, 1575-1589`

Both methods call `ZRANGE queue 0 -1` to get all tags, then delete each
message hash individually. For queues with millions of messages, this will:
- Use a lot of memory for the Python set
- Send millions of individual DEL commands in a pipeline
- Block Redis during the ZRANGE

Consider using `ZSCAN` with batched pipeline deletes.

### 15. `_warned_priority_clamp` is never reset
**File:** `transport.py:148`

Once set to `True`, the warning is permanently suppressed for the process
lifetime. If the user fixes their priority configuration, they'll never see
confirmation that the warning stopped. Minor, but for a long-running process
this could hide issues.

---

## Design / Architecture

### 16. `QoS.visibility_timeout` is a `cached_property` — never updated
**File:** `transport.py:518-520`

If `channel.visibility_timeout` changes after the QoS object is created (e.g.,
via dynamic transport options), the cached value is stale. This is unlikely in
practice but violates the principle of least surprise.

### 17. `active_queues` property re-computes set difference every call
**File:** `transport.py:1810-1812`

This property creates a new set every time it's called, which happens on every
poll cycle. For hot paths, consider caching and invalidating on
`basic_consume`/`basic_cancel`.

### 18. `_fanout_queues` mapping has unused routing_key
**File:** `transport.py:1508`

`self._fanout_queues[queue] = (exchange, routing_key.replace("#", "*"))` stores
the routing key, but it's never used. `_xread_start` and `_xread_read` only use
the exchange. The `routing_key.replace("#", "*")` transformation suggests it was
meant for pattern matching but that's not implemented for streams.

### 19. Signal handlers imported as side effect
**File:** `transport.py:1849`

`from . import signals as _signals` is imported inside `Transport.__init__` to
register signal handlers. This means signal handlers are registered as a side
effect of transport instantiation. If a user imports the module but doesn't
create a Transport, the `@before_task_publish.connect` decorator in `signals.py`
still fires at module import time (top-level decorator). The
`Transport.__init__` import is actually redundant since `signals.py` decorators
fire at import time.

### 20. Two separate clients (`client` and `subclient`) with separate connection pools
**File:** `transport.py:1799-1807`

Both `client` and `subclient` are `cached_property` that call
`_create_client(asynchronous=True)`, which means both share the same
`async_pool`. Since they share a pool, getting a connection for `subclient`
could exhaust the pool and block `client` (or vice versa). Consider using
separate pools for `client` and `subclient` to ensure both always have
connections available.

---

## Lua Script Issues

### 21. `transport_consume_message.lua` does O(N) ZRANGE peek across all queues
**File:** `transport_consume_message.lua:27-36`

For each consume attempt (up to 100), the script peeks at all queue keys with
`ZRANGE key 0 0 WITHSCORES`. With many queues, this is O(queues × attempts).
In the worst case (100 expired messages across many queues), this could be slow.

### 22. `enqueue_due_messages.lua` doesn't respect x-message-ttl for native delayed messages
**File:** `transport_enqueue_due_messages.lua:60-62`

When a native delayed message fires (`native_delayed == 1`), the script clears
the flag but doesn't set an EXPIRE on the message hash. The EXPIRE was set at
publish time in `_put`, but for very long delays (hours/days), the message
hash's TTL may have already expired, and the Lua script would find no hash (the
`priority` check on line 42 handles this, but the message is silently lost).

### 23. `transport_requeue_message.lua` tag extraction is fragile
**File:** `transport_requeue_message.lua:53`

`local tag = string.sub(message_key, #global_keyprefix + #message_key_prefix + 1)`
extracts the delivery tag by stripping prefixes from the Redis key. This
depends on the key format being exactly `{prefix}{message_prefix}{tag}`. If
the key format ever changes, this silently produces wrong tags.

---

## Testing Gaps

### 24. No unit tests for `_xread_start` and `_xread_read`
Fanout consume path has integration tests but no unit tests. Connection errors,
timeout behavior, and multi-stream scenarios aren't tested in isolation.

### 25. No tests for `close()` method
**File:** `transport.py:1591-1608`

Channel `close()` has significant logic (draining polls, cleaning up auto-delete
queues, disconnecting pools) but no dedicated tests.

### 26. No tests for `_connparams` with `socket://` URLs
**File:** `transport.py:1710-1726`

Unix domain socket connections are supported but not tested.

### 27. No tests for `_after_fork` cleanup
**File:** `transport.py:841-842`

The fork cleanup path isn't tested, though it's critical for `prefork` pool
mode.

### 28. No tests for concurrent `enqueue_due_messages` across multiple workers
The `enqueue_due_messages` Lua script uses `ZADD NX` to prevent double-enqueue,
but there's no test verifying this under concurrent execution.

### 29. Missing tests for `acks_late=True` behavior
The memory notes mention this is deferred. With `acks_late`, the ack happens
after task execution, which changes the shutdown timing and could produce
different behavior with the executor wait logic.

### 30. No tests for `credential_provider` with real auth
The credential provider tests use mocks. There are no integration tests with
actual dynamic auth (e.g., token rotation).

---

## Code Quality / Minor Issues

### 31. `_get_message_from_hash` is unused
**File:** `transport.py:889-909`

This method exists but is never called. It was likely replaced by the Lua
consume script. Dead code.

### 32. Inconsistent error handling in `_poll_error`
**File:** `transport.py:1248-1256`

`_poll_error` doesn't handle the response — it calls `parse_response` and
returns the result, but the return value is never used by the caller
(`handle_event` in `MultiChannelPoller`). If `parse_response` raises, the
exception propagates unhandled.

### 33. `_put` stores `eta: 0` when no eta is provided
**File:** `transport.py:1445`

`"eta": eta_timestamp or 0` stores 0 when there's no eta. The Lua scripts
check `eta > 0` to determine if eta is set. This works but is a sentinel value
pattern that could be confusing — `nil`/not-present would be cleaner.

### 34. `_queue_name` method could be a static method
**File:** `transport.py:876-883`

It doesn't use `self`.

### 35. `_message_key` and `_queue_key` could be static methods
**File:** `transport.py:864-874`

They only use class-level constants (`message_key_prefix`, `QUEUE_KEY_PREFIX`).

### 36. `OperationalError` is imported but never used
**File:** `tests/test_transport.py:16`

Unused import in the test file.

### 37. `_warned_expires_clamp` is set on the class, not the instance
**File:** `transport.py:1498`

`Channel._warned_expires_clamp = True` sets it on the class, which means it
persists across all instances and even across tests. Should use
`self.__class__._warned_expires_clamp = True` or document this as intentional.

### 38. `close()` checks `self._in_poll` as boolean but it can be `None` or a connection
**File:** `transport.py:1592`

`if self._in_poll:` — `_in_poll` is set to `None`, `False`, or a connection
object. The truthiness check works correctly in all cases, but `is not None`
would be more explicit.

### 39. `from_transport_options` doesn't include `ssl`
**File:** `transport.py:776-795`

SSL configuration is handled via `conninfo.ssl` and `transport_options.get("ssl")`,
but `ssl` is not in `from_transport_options`. This means it can't be set via
`broker_transport_options = {"ssl": True}` like other options. It works via
the URL scheme (`rediss://`) or `conninfo.ssl`, but the inconsistency is
confusing since the docstring lists it as a transport option.

### 40. `_process_credential_provider` instantiates with no arguments
**File:** `transport.py:1644-1645`

`credential_provider_cls = symbol_by_name(credential_provider)` followed by
`credential_provider = credential_provider_cls()` assumes the provider class
takes no constructor arguments. Many credential providers need configuration
(region, endpoint, etc.).

---

## Documentation

### 41. No documentation for transport options in code
The module docstring lists transport options but doesn't document their types
or valid ranges. For example, `visibility_timeout` accepts a float but no
documentation says what happens with 0 or negative values.

### 42. No changelog or migration guide
For users upgrading between versions, there's no documented upgrade path or
breaking change list.

### 43. `CLAUDE.md` mentions `x-restore-count` header but it's not documented for users
Users have no way to know this header exists or how to use it for monitoring.

---

## TODO List

- [x] **BUG**: Fix `_worker_pool` global state to be per-app/worker (issue #1) — **FIXED**: now `_worker_pools` dict keyed by `id(app)`
- [x] **NOT A BUG**: `_consume_fast_mode` per-channel (issue #3) — BZMPOP watches all queue keys, no latency issue
- [x] **NOT A BUG**: Single-pass `_drain_hub_callbacks` (issue #4) — standard transport doesn't drain at all
- [ ] **BUG**: Fix potential message loss for very long native delays exceeding message TTL (issue #22)
- [ ] **HARDENING**: Implement dead letter queue support for dropped messages (issue #9)
- [ ] **HARDENING**: Add metrics/observability (counters, signals) for key operations (issue #10)
- [ ] **HARDENING**: Add connection retry logic around `_put` and other write paths (issue #13)
- [ ] **HARDENING**: Use `ZSCAN` + batched delete for `_delete` and `_purge` on large queues (issue #14)
- [ ] **HARDENING**: Consider separate connection pools for `client` and `subclient` (issue #20)
- [ ] **HARDENING**: Escalate `enqueue_due_messages` after repeated per-queue failures (issue #11)
- [x] **NOT A BUG**: Client vs server TIME (issue #5) — standard transport also uses client-side time
- [ ] **DESIGN**: Make `active_queues` property cache the set instead of recomputing (issue #17)
- [x] **DESIGN**: Remove unused `_get_message_from_hash` method (issue #31) — **DONE**: removed dead code, updated tests to use `_get`
- [ ] **DESIGN**: Remove unused routing_key from `_fanout_queues` mapping (issue #18)
- [ ] **DESIGN**: Clean up redundant signal import in `Transport.__init__` (issue #19)
- [ ] **DESIGN**: Add `ssl` to `from_transport_options` or document the discrepancy (issue #39)
- [ ] **DESIGN**: Support credential provider constructor arguments (issue #40)
- [ ] **TEST**: Add unit tests for `_xread_start` and `_xread_read` (issue #24)
- [ ] **TEST**: Add unit tests for `close()` (issue #25)
- [ ] **TEST**: Add tests for `socket://` URL handling (issue #26)
- [ ] **TEST**: Add tests for `_after_fork` cleanup (issue #27)
- [ ] **TEST**: Add concurrent `enqueue_due_messages` test (issue #28)
- [ ] **TEST**: Add `acks_late=True` integration tests (issue #29)
- [x] **NOT AN ISSUE**: `OperationalError` import (issue #36) — IS used in test at line 4258
- [ ] **DOCS**: Document transport options with types and valid ranges (issue #41)
- [ ] **DOCS**: Create changelog/migration guide (issue #42)
- [ ] **DOCS**: Document `x-restore-count` header for users (issue #43)
- [x] **MINOR**: Make `_queue_name` a static method (issue #34) — **DONE**: added `@staticmethod` decorator
- [x] **MINOR**: Use `is not None` check in `close()` for `_in_poll` (issue #38) — **DONE**: fixed both `_in_poll` and `_in_fanout_poll` checks
- [ ] **MINOR**: Document client/server clock divergence for score calculation (issue #5)
- [ ] **MINOR**: Document SLOW mode pipeline non-atomicity (issue #6)
