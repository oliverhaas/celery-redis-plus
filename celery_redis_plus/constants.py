"""Shared constants for celery-redis-plus."""

# Suffix for the messages index sorted set (tracks message visibility)
MESSAGES_INDEX_SUFFIX = ":index"

# Score multiplier for priority in sorted set queues.
# Formula: (255 - priority) * PRIORITY_SCORE_MULTIPLIER + timestamp_ms
# Higher priority number = lower score = popped first (matches RabbitMQ semantics).
# With 10^10, max score at priority 0 in 100 years: 255 * 10^10 + 4.9e12 ≈ 7.5e12
# This is well under IEEE 754 double precision safe integer limit (2^53 ≈ 9e15).
PRIORITY_SCORE_MULTIPLIER = 10**10

# Default priority value (lowest priority, matching RabbitMQ default)
DEFAULT_PRIORITY = 0

# Default visibility timeout in seconds (how long before unacked messages are restored)
DEFAULT_VISIBILITY_TIMEOUT = 300  # 5 minutes

# Default health check interval in seconds
DEFAULT_HEALTH_CHECK_INTERVAL = 25

# Default stream maximum length for fanout streams
DEFAULT_STREAM_MAXLEN = 10000

# Interval in seconds for requeue check (restores unacked messages and moves delayed messages)
DEFAULT_REQUEUE_CHECK_INTERVAL = 60

# Batch limit for requeue operations (max messages processed per queue per cycle)
DEFAULT_REQUEUE_BATCH_LIMIT = 1000

# Default TTL for per-message hashes in seconds (3 days)
# Messages are cleaned up on ack, but this TTL ensures orphaned messages are eventually removed
DEFAULT_MESSAGE_TTL = 3 * 24 * 60 * 60  # 259200 seconds

# Prefix for per-message hash keys
MESSAGE_KEY_PREFIX = "message:"
