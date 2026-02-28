"""Shared constants for celery-redis-plus."""

# Prefix for per-queue messages index sorted sets (tracks message visibility)
MESSAGES_INDEX_PREFIX = "messages_index:"

# Sorted set score: inverted priority scaled by this multiplier, plus timestamp_ms.
# Higher priority yields lower score, popped first (RabbitMQ semantics).
# Using 10^13 gives clean digit separation: PPP|tttttttttttt (3 priority + 13 timestamp digits).
# Max score ~2.6e15 is well under IEEE 754 exact integer limit (2^53 ≈ 9e15).
PRIORITY_SCORE_MULTIPLIER = 10**13

# Priority range (0-255, matching RabbitMQ semantics)
MIN_PRIORITY = 0
MAX_PRIORITY = 255

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

# Default TTL for per-message hashes in seconds
# -1 means no TTL (messages persist until acked or deleted)
# Set to a positive value (e.g., 259200 for 3 days) to auto-expire orphaned messages
DEFAULT_MESSAGE_TTL = -1

# Prefix for per-message hash keys
MESSAGE_KEY_PREFIX = "message:"

# Prefix for queue sorted set keys (avoids collision with list-based queues)
QUEUE_KEY_PREFIX = "queue:"

# Minimum allowed x-expires value in milliseconds (10 seconds)
# Celery's control/reply queues use 10s by default
MIN_QUEUE_EXPIRES = 10_000
