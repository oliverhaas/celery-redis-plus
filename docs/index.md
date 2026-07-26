# Celery Redis Plus

Enhanced Redis/Valkey transports for Celery/Kombu with native delayed delivery, improved reliability, full priority support, and reliable fanout. Ships two broker transports: a sorted set transport (`valkey://`) and a Redis Streams transport with consumer groups (`valkey-streams://`).

## Overview

`celery-redis-plus` is a replacement Redis/Valkey transport for Celery that addresses key limitations of the standard transport:

1. **Native Delayed Delivery** - Tasks with long `countdown` or `eta` are stored in Redis and delivered when due, instead of being held in worker memory.
2. **Improved Reliability** - Atomic message consumption via BZMPOP with improvements regarding visibility timeout ensures zero message loss.
3. **Full Priority Support** - All 256 priority levels (0-255) with RabbitMQ-compatible semantics (higher number = higher priority).
4. **Reliable Fanout** - Redis Streams replace lossy PUB/SUB for durable broadcast event messaging.
5. **Streams Transport** - An alternative queue transport (`valkey-streams://`) built on Redis Streams with consumer groups, where the broker natively tracks in-flight work and a worker heartbeat replaces task-duration-based visibility timeouts.

## Requirements

We target recent versions for BZMPOP support and to simplify development.

- Python >= 3.13
- Celery >= 5.5.0
- Redis >= 7.0 for the sorted set transport (BZMPOP) and Redis >= 6.2 for the streams transport (the XPENDING ... IDLE filter), or Valkey (any version)

## How It Works

### Sorted Set Queues

Queues use Redis sorted sets instead of lists. Messages are added with `ZADD` using a score that encodes priority and timestamp. Workers use `BZMPOP` to atomically pop the highest-priority, oldest message.

### Message Persistence

Messages are stored in per-message hashes before being added to the queue. When consumed, the hash persists until explicitly acknowledged. Combined with visibility timeout tracking, this ensures messages are never lost - even if a worker crashes in the instant after the message is pop'ed from the queue, the message can be recovered and requeued.

### Delayed Delivery

Delayed messages are stored in a sorted set with timestamps as scores. A periodic event loop callback checks for messages whose timestamp has passed and moves them to the normal queue.

### Stream-based Fanout

Fanout exchanges use Redis Streams. Messages are added with `XADD`, and each consumer uses `XREAD` tracking their own position. Old messages are trimmed based on `stream_maxlen`.

### Streams Transport (`valkey-streams://`)

The second transport stores each queue as a set of Redis Streams, one per priority
level, consumed through a consumer group. `XREADGROUP` delivers a message and
registers it in the broker-native Pending Entries List (PEL) in a single atomic
step, so the broker itself tracks in-flight work. Workers send a periodic
`XCLAIM ... JUSTID` heartbeat for their in-flight messages; if a worker dies, its
messages become reclaimable by peers after `visibility_timeout` seconds of
heartbeat silence. This means `visibility_timeout` no longer has to exceed the
longest task: the default of 300 seconds is safe for tasks that run for hours.
Delayed messages are staged in a `delayed:{queue}` sorted set and pumped into
their priority stream when due. Acknowledged messages are removed with
XACK + XDEL, so streams shrink on every ack.

## Contributing

This package is intended as a temporary solution until these improvements are hopefully merged upstream into Celery/Kombu. Contributions are welcome!

## License

MIT License - see [LICENSE](https://github.com/oliverhaas/celery-redis-plus/blob/main/LICENSE) for details.
