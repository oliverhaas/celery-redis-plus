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
