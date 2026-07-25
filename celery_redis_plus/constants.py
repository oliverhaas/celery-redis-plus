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

# Max XPENDING discovery pages walked per stream per reclaim call
DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT = 50

# Default TTL for per-message hashes in seconds
# -1 means no TTL (messages persist until acked or deleted)
# Set to a positive value (e.g., 259200 for 3 days) to auto-expire orphaned messages
DEFAULT_MESSAGE_TTL = -1

# Prefix for per-message hash keys
MESSAGE_KEY_PREFIX = "message:"

# Prefix for queue sorted set keys (avoids collision with list-based queues)
QUEUE_KEY_PREFIX = "queue:"

# Default max restore count (None = no limit, backwards compatible)
# When set to an integer, messages restored more than this many times are dropped
DEFAULT_MAX_RESTORE_COUNT: int | None = None

# Minimum allowed x-expires value in milliseconds (10 seconds)
# Celery's control/reply queues use 10s by default
MIN_QUEUE_EXPIRES = 10_000

# Prefix for per-(queue, priority level) stream keys: stream:{queue}:{level}
STREAM_KEY_PREFIX = "stream:"

# Prefix for per-queue delayed message sorted sets: delayed:{queue}
DELAYED_KEY_PREFIX = "delayed:"

# Priority buckets in the 0-255 space, ascending; see priority_to_level()
DEFAULT_PRIORITY_STEPS = [0, 3, 6, 9]

# Default consumer group name used on every queue stream
DEFAULT_CONSUMER_GROUP = "celery"

# Artificial idle time in ms set via XCLAIM on graceful shutdown (~24 days).
# Far above any sane visibility timeout, so peers reclaim the entries instantly.
SHUTDOWN_IDLE_MS = 2**31

# Heartbeat cadence divisor: heartbeat_interval = visibility_timeout / divisor
HEARTBEAT_INTERVAL_DIVISOR = 5

# Idle consumers with no pending entries are removed via XGROUP DELCONSUMER
# after this factor times the visibility timeout of idleness
CONSUMER_IDLE_CLEANUP_FACTOR = 12
