# Migrating from Standard Redis Transport

The two transports use different Redis data structures (list vs sorted set), so existing tasks won't be picked up after switching. Use Celery's built-in migration to move them:

```python
from celery import Celery
from celery.contrib.migrate import migrate_tasks

# Source: standard Redis transport (no broker_transport)
source_app = Celery("source")
source_app.conf.broker_url = "redis://localhost:6379/0"

# Destination: your app with celery-redis-plus
dest_app = Celery("dest")
dest_app.conf.update(
    broker_url="redis://localhost:6379/0",
    broker_transport="celery_redis_plus.transport:Transport",
)

# Migrate
with source_app.connection() as src, dest_app.connection() as dst:
    state = migrate_tasks(src, dst, app=dest_app, ack_messages=True)
    print(f"Migrated {state.count} tasks")
```

The `dest_app` is probably just the app of your new deployment and does not need to be explicitely defined, while `source_app` is a temporary dummy app for the old transports.

For most cases where a few minutes of task delays do not matter, the migration is fairly straight forward. Deploy new code with the backwards-compatible `source_app`, run the migration script, migration itself is already done, then remove the `source_app` or any other leftovers in the next deployment.

## Carry over a custom `sep`

Queues, message hashes and indices all use key prefixes that differ from the standard Redis transport, so the two transports cannot read each other's messages. That is what the migration above is for. Exchange bindings are the exception. Both transports store them in `_kombu.binding.{exchange}`, under the same key name, with members packed as `routing_key + sep + pattern + sep + queue`. It is the one piece of broker state that is shared rather than versioned by key name.

`sep` defaults to `"\x06\x16"` in both transports. If your deployment overrode it in `broker_transport_options`, the override **must** come along:

```python
broker_transport_options = {"sep": ":"}
```

Miss it and the two sides write bindings in two different formats into the same set, and each misreads the other's:

- The standard Redis transport splits without padding and unpacks 3-tuples, so it raises `ValueError: not enough values to unpack (expected 3, got 1)` on every `_lookup`, meaning on every publish, for as long as both versions run side by side.
- celery-redis-plus pads a short split to `(member, "", "")`, which matches no routing key. `_lookup` returns an empty set and kombu's `DirectExchange.deliver` drops the message with no exception, no warning and no deadletter.

The second case produces no error at all, so `get_table` logs a warning (once per process) naming the exchange and the offending member whenever a binding does not split into three parts.

The bindings are rewritten by `_queue_bind` on worker/producer startup, so fixing `sep` and restarting is normally enough. Stale members in the old format linger until the exchange is redeclared; `SREM` them from `_kombu.binding.{exchange}` if the warning persists.
