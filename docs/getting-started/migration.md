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

For most cases where a few minutes of task delays do not matter, the migration is fairly straight forward. Run the migration script while the old deployment is still the one talking to the broker, then deploy the new code, then remove the `source_app` or any other leftovers in the next deployment. Running the script after the new transport has already declared its queues fails with `WRONGTYPE`, for the reason in the next section.

## Run the migration before you deploy

Queues, message hashes and indices all use key prefixes that differ from the standard Redis transport, so the two transports cannot read each other's messages. That is what the migration above is for. Exchange bindings are the exception: both transports store them under the same key name, `_kombu.binding.{exchange}`, with members packed as `routing_key + sep + pattern + sep + queue`.

They do not store them in the same **type**. The standard transport uses a plain set. celery-redis-plus uses a sorted set, scored with the unix time each binding goes stale, so that a binding whose process is gone cleans itself up (see [Binding lifetime](#binding-lifetime)). The first `_queue_bind` this transport runs converts an inherited set in place, keeping every member and scoring it `+inf`, because this transport did not write those members and cannot know when they go stale.

The conversion is one-way, so **the two transports cannot both declare against the same exchange**. Once converted, the standard transport's `SADD` and `SMEMBERS` on that key raise `WRONGTYPE Operation against a key holding the wrong kind of value`, and since kombu declares queues when a consumer or producer is created, that hits the `source_app` in the script above too.

Run the migration script **before** deploying the new transport, from a process where only the old transport has touched the broker. If you already deployed and the script now fails with `WRONGTYPE`, delete the binding keys and let the old transport recreate them:

```
DEL _kombu.binding.celery
```

Losing binding members costs nothing: every live process rewrites its own bindings the next time it declares, and celery-redis-plus keeps rescoring them from then on.

## Carry over a custom `sep`

`sep` defaults to `"\x06\x16"` in both transports. If your deployment overrode it in `broker_transport_options`, the override **must** come along:

```python
broker_transport_options = {"sep": ":"}
```

Miss it and the two sides write bindings in two different formats into the same table, and each misreads the other's:

- The standard Redis transport splits without padding and unpacks 3-tuples, so it raises `ValueError: not enough values to unpack (expected 3, got 1)` on every `_lookup`, meaning on every publish.
- celery-redis-plus pads a short split to `(member, "", "")`, which matches no routing key. `_lookup` returns an empty set and kombu's `DirectExchange.deliver` drops the message with no exception, no warning and no deadletter.

The second case produces no error at all, so `get_table` logs a warning (once per process) naming the exchange and the offending member whenever a binding does not split into three parts.

The bindings are rewritten by `_queue_bind` on worker/producer startup, so fixing `sep` and restarting is normally enough. Members in the old format linger until they go stale; `ZREM` them from `_kombu.binding.{exchange}` if the warning persists.

## Binding lifetime

`_kombu.binding.{exchange}` is a sorted set scored with the unix time each binding goes stale, which is `x-expires` after its last refresh, and never less than `MIN_BINDING_LIFETIME` (300 seconds). A queue without `x-expires` is scored `+inf` and its binding only ever goes away on an explicit unbind.

Declaring, refreshing and publishing all rescore, so a binding lives exactly as long as some process keeps using its queue. `get_table` drops whatever has aged out before it reads, so cleanup rides the read path and nothing has to sweep. Redis cannot expire an individual member of a key, and only `queue_delete` removes a binding, which reaches just the bindings a process declared itself. Without the deadline the routing table of a long-lived exchange grows for the life of the deployment: a celery control client binds a fresh reply queue per call and does not always get to unbind it.

The window comes from `x-expires`, so lower it on a queue whose bindings should be reclaimed sooner. The floor is there because the processes that abandon bindings are the ones that cannot refresh them: a control client has no event loop, and the 10 second `x-expires` celery puts on its reply queue is shorter than the control call the binding has to outlive.
