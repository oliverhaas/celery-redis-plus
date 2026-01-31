# Quick Start

## Basic Setup

```python
from celery import Celery
from celery_redis_plus import DelayedDeliveryBootstep

app = Celery('myapp')

# For Valkey: use valkey:// URL scheme
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
})

# For Redis: use redis:// with broker_transport
# app.config_from_object({
#     'broker_url': 'redis://localhost:6379/0',
#     'broker_transport': 'celery_redis_plus.transport:Transport',
# })

app.steps['consumer'].add(DelayedDeliveryBootstep)

@app.task
def my_task():
    print("Hello!")

# Tasks with countdown/eta will use native delayed delivery
my_task.apply_async(countdown=120)

# Priority support (RabbitMQ semantics: higher = more important)
my_task.apply_async(priority=90)   # High priority
my_task.apply_async(priority=0)    # Low priority (default)
```

## Configuration

### Transport Options

Configure via Celery's `broker_transport_options`:

```python
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
    'broker_transport_options': {
        'global_keyprefix': 'myapp:',        # Prefix for all Redis keys
        'visibility_timeout': 300,           # Seconds before unacked messages are reclaimed
        'stream_maxlen': 10000,              # Max messages per stream (approximate)
    },
})
```

### SSL/TLS Connections

For secure connections, use `valkeys://` or set the `ssl` transport option:

```python
# Option 1: Use valkeys:// URL scheme
app.config_from_object({
    'broker_url': 'valkeys://localhost:6379/0',
})

# Option 2: Use ssl transport option
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
    'broker_transport_options': {
        'ssl': True,  # Use default SSL settings
    },
})

# Option 3: Custom SSL options
import ssl
app.config_from_object({
    'broker_url': 'valkey://localhost:6379/0',
    'broker_transport_options': {
        'ssl': {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        },
    },
})
```
