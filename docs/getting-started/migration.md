# Migrating from Standard Redis Transport

The two transports use different Redis data structures (list vs sorted set), so tasks queued before the switch are not picked up after it. Celery's built-in migration moves them:

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

with source_app.connection() as src, dest_app.connection() as dst:
    state = migrate_tasks(src, dst, app=dest_app, ack_messages=True)
    print(f"Migrated {state.count} tasks")
```

In practice `dest_app` is just the app of your new deployment; `source_app` is a throwaway configured for the old transport.

Run this **before** deploying the new transport. Both transports store exchange bindings under `_kombu.binding.{exchange}`, but celery-redis-plus converts that key from a plain set to a sorted set on its first declare, and the conversion is one-way: afterwards the standard transport, including the `source_app` above, fails on it with `WRONGTYPE`. If that already happened, `DEL` the binding keys and move on; every live process rewrites its own bindings on its next declare:

```
DEL _kombu.binding.celery
```

If you overrode `sep` in `broker_transport_options`, carry the override over. With mismatched separators the two sides misread each other's bindings: the standard transport raises `ValueError` on every publish, celery-redis-plus silently drops the message (and logs a warning naming the offending member).

My own migration skipped all of this: hard-switch to the new transport, then requeue what the old deployment left behind with a couple of Django shell functions that pop the raw messages from the old list keys and re-send the tasks. If your tasks can sit for the few minutes that takes, it beats orchestrating two apps.
