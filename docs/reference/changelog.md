# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **BREAKING**: `_kombu.binding.{exchange}` is now a sorted set instead of a plain set, scored with the unix time each binding goes stale. Bindings were never removed: Redis cannot expire an individual member, and only `queue_delete` removes one, which reaches just the bindings the calling process declared itself. So the routing table of a long-lived exchange grew for the life of the deployment, and a celery control client, which binds a fresh reply queue per call and does not always get to unbind it, drove that growth. The deadline is `x-expires` after the last refresh and never less than `MIN_BINDING_LIFETIME` (300 seconds); a queue without `x-expires` is scored `+inf` and still only goes away on an explicit unbind. Declaring, refreshing and publishing all rescore, and `get_table` drops whatever has aged out before it reads, so cleanup rides the read path and nothing has to sweep. The first `_queue_bind` converts an inherited set in place, keeping every member and scoring it `+inf`. The conversion is one-way and the key name is shared with kombu's own Redis transport, so the two can no longer declare against the same exchange: run `celery.contrib.migrate.migrate_tasks` before deploying this version, or `DEL` the binding keys if you already did. See the [migration guide](../getting-started/migration.md)
- **BREAKING**: the involuntary-redelivery cap now follows RabbitMQ quorum queues. The `max_restore_count` transport option is now `delivery_limit`, the `restore_count` message hash field is now `delivery_count`, and the `x-restore-count` header is now `x-delivery-count`. The default changed from no limit to `20`, which is what RabbitMQ quorum queues have applied since 4.0, and the counter now counts delivery attempts rather than redeliveries, so a message is dropped on its 20th delivery. Set `delivery_limit: None` in `broker_transport_options` to keep the old unlimited behaviour. A message published by an older version has no `delivery_count` field, which reads as `0`, so it simply starts over
- `Channel.enqueue_due_messages` now returns a `SweepStats` NamedTuple `(enqueued, dropped, redelivered, orphaned)` instead of a bare count

### Added
- `blocking_timeout` transport option (default `10`), the seconds BZMPOP and XREAD block on the server per poll. This was `polling_interval`, which in kombu means the sleep *between* unsuccessful polls, so one attribute drove two opposite mechanisms: `kombu.transport.virtual.Transport.drain_events` slept 10 seconds after any poll that came up empty, and the sleep was clamped to the caller's drain timeout rather than skipped. kombu's own Redis transport sets `polling_interval = None` and keeps `brpop_timeout` separate for exactly this reason, and this transport now does the same. Setting `polling_interval` still works, read as `blocking_timeout` with a deprecation warning and with the sleep left disabled. Keep it below `socket_timeout` if you set one

### Documentation & Diagnostics
- Documented the `sep` transport option, which was accepted but never listed. A deployment migrating from the standard Redis transport has to carry over whatever `sep` it configured there, because `_kombu.binding.{exchange}` is the one piece of broker state the two transports share a key name for
- Added a "Carry over a custom `sep`" section to the migration guide covering both failure modes of a mismatch: kombu raising `ValueError: not enough values to unpack (expected 3, got 1)` on every publish, and this transport padding the member to `(member, "", "")` so routing silently matches nothing
- `get_table` now logs a warning (once per process) naming the exchange and the offending member when a binding does not split into three parts. Padding behaviour is unchanged, so nothing starts raising
- The requeue sweep now reports what it did. Messages dropped at the delivery limit are named in the error log (task name and id, up to 10 per queue per sweep); the drop deletes the message hash, so that log line is the last trace of the message. Redeliveries and orphaned index entries are counted and logged at INFO

### Fixed
- Publishing to a durable direct exchange whose binding table is empty now raises `InconsistencyError` instead of discarding the message. kombu made the empty table a silent no-op in 5.2 (PR #1404), which is right for topic and fanout but not for durable direct, where the binding is known to exist and, with `x-expires`, may simply have aged out. `InconsistencyError` is in `connection_errors`, so kombu redeclares the binding and retries. The visible symptom was pidbox replies vanishing after a control queue expired. A transient direct exchange keeps kombu's drop, with an INFO log: a pidbox reply exchange loses its bindings the moment its control client leaves, and the publisher redeclaring its own entities cannot recreate a binding that belonged to someone else, so raising there only churned through a pointless retry loop
- `x-expires` and `x-message-ttl` now apply to publishes made on a channel that did not declare the queue itself. kombu caches declarations per connection, so only the first channel to declare a queue ever sees its arguments, while any channel of that connection may be the one publishing. The TTL registries are now shared by all channels of a connection instead of being per-channel
- Acking a message now removes it from `queue:{name}` as well as from `messages_index:{name}`. A message whose visibility timeout had already restored it left the restored copy behind, so it was delivered again after being acked
- A consumed message always gets a visibility deadline. Both consume paths refreshed the index entry with `ZADD ... XX`, which is a no-op when the entry is gone, so such a message was never recovered if its worker died
- A queue backlog is no longer counted as a redelivery. `enqueue_due_messages` gates the counter on the `ZADD NX` result, so a message still sitting in its queue past its deadline is re-dated but neither counted nor dropped. Without this, a queue slower than `visibility_timeout` would have eaten its own backlog once `delivery_limit` gained a default
- `delivery_info["redelivered"]` and the `x-delivery-count` header are now derived from the delivery counter at consume time. `redelivered` used to be a hash field that was written but never read, so Celery's `worker_deduplicate_successful_tasks` never saw a redelivery. The header goes into the message's top-level `headers` map, which is where kombu reads headers from when it rebuilds a message; `properties["headers"]`, where it went first, never reaches the consumer
- Messages consumed with `no_ack` (pidbox control and reply queues, and `basic_get(..., no_ack=True)`) are now dequeued inside the atomic pop instead of being given a visibility deadline. Nothing ever acks a no_ack delivery, so its index entry and hash survived until the requeue sweep re-enqueued the message on its deadline, and a control command could fire a second time `visibility_timeout` later
- `x-expires` is now refreshed on connections that have no event loop. The refresh only ever ran off a timer inside a worker's hub, so a celery control client waiting for replies, a Flower event receiver and a gevent worker's synloop all let their own queues, and now their bindings, age out from under them. They drain events instead, so the drain path refreshes at the same interval the timer would have used
- The queue expires refresh timer now starts for queues declared before the event loop existed. `register_with_event_loop` never called `_update_expires_timer` after attaching the loop, so a worker that declared all its queues at startup refreshed none of their TTLs and its queues expired underneath it
- `QoS.restore_unacked_once` no longer shuts the worker thread pool down on broker reconnects. kombu calls it from `Channel.close()`, which also runs when the consumer reconnects, so every broker blip permanently disabled the pool (later `submit()` calls raised `RuntimeError` while the worker kept answering `inspect ping`). It is now gated on the worker blueprint having entered `CLOSE`/`TERMINATE`
- Reconnects no longer requeue messages whose tasks are still running. Those messages stay in `messages_index` and are redelivered on their visibility deadline instead
- Worker lookup no longer relies on `channel.connection.client.app`, which never resolves (kombu's `Connection` has no `app` attribute) and made the lookup raise `AttributeError` on every call

## [0.3.0] - 2026-02-14

### Added
- Queue TTL (`x-expires`): queues auto-expire when no worker refreshes them, via periodic PEXPIRE with dynamic interval (TTL/2)
- Message TTL (`x-message-ttl`): per-queue message expiry via shorter EXPIRE on message hashes
- `prepare_queue_arguments` override using kombu's `to_rabbitmq_queue_arguments` for RabbitMQ-compatible queue argument handling

### Changed
- Split global `messages_index` sorted set into per-queue `messages_index:{queue}` keys for scoped recovery, clean queue lifecycle, and correct `global_keyprefix` behavior with Lua scripts
- Renamed internal redis-specific naming to client-library-agnostic (`client_lib`, `_client_exceptions`) for better redis-py/valkey-py compatibility
- Default message TTL changed from 3 days to `-1` (no TTL); configurable via `message_ttl` channel attribute
- CI/CD: tag workflow now gates on CI success instead of running on every push

### Fixed
- `EXPIRE` and `PEXPIRE` commands now correctly prefixed when `global_keyprefix` is set
- `_bzmpop_read` and `_get` now skip expired message hashes and try the next message instead of raising `Empty`
- `x-expires` below minimum (10s) now clamped with warning instead of raising `ValueError`
- Removed redundant redis-specific getter functions (`get_redis_error_classes`, `get_redis_ConnectionError`, `_get_response_error`)

## [0.2.5] - 2026-02-14

### Fixed
- Fanout/broadcast (events, Flower) now works: added dedicated subclient for XREAD and fixed per-routing-key stream splitting

### Added
- Example project in `examples/simple/` demonstrating tasks, delayed delivery, priority, retries, and Flower

## [0.2.4] - 2026-01-31

### Added
- Migration support from standard Redis transport

### Fixed
- Simplified transport configuration in docs

## [0.2.3] - 2026-01-29

### Added
- Support for both redis-py and valkey-py client libraries (optional dependencies)
- `valkey://` and `valkeys://` URL scheme support for easier configuration
- SSL/TLS detection from `valkeys://` URL scheme
- Priority clamping for out-of-range values (clamps to 0-255 range with warning)

### Fixed
- Documentation site 404 by setting dev as default version

## [0.2.2] - 2025-01-22

### Changed
- Updated celery-types-ng to 0.25.4 and fixed typing errors

## [0.2.1] - 2025-01-21

### Changed
- Added `queue:` prefix to avoid collision with list-based queues

## [0.2.0] - 2025-01-20

### Added
- Native delayed delivery support
- Full priority support (0-255)
- Reliable fanout via Redis Streams
- Visibility timeout tracking

### Changed
- Switched from Redis lists to sorted sets for queues
- Improved message reliability with per-message hashes

## [0.1.0] - 2025-01-15

### Added
- Initial release
- Custom Kombu transport for Redis/Valkey
- Basic queue operations with sorted sets
