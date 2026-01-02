"""Shared constants for celery-redis-plus."""

# Header key for delay seconds in messages
DELAY_HEADER = "x-celery-delay-seconds"

# Suffix for the messages index sorted set (tracks message visibility)
MESSAGES_INDEX_SUFFIX = ":index"

# Score multiplier for priority in sorted set queues.
# Formula: (255 - priority) * PRIORITY_SCORE_MULTIPLIER + timestamp_ms
# Higher priority number = lower score = popped first (matches RabbitMQ semantics).
# This gives ~31 years of millisecond timestamps before priority levels collide.
PRIORITY_SCORE_MULTIPLIER = 10**12

# Default priority value (lowest priority, matching RabbitMQ default)
DEFAULT_PRIORITY = 0

# Default visibility timeout in seconds (how long before unacked messages are restored)
DEFAULT_VISIBILITY_TIMEOUT = 3600  # 1 hour

# Default health check interval in seconds
DEFAULT_HEALTH_CHECK_INTERVAL = 25

# Default stream maximum length for fanout streams
DEFAULT_STREAM_MAXLEN = 10000

# Default consumer group prefix
DEFAULT_CONSUMER_GROUP_PREFIX = "celery-redis-plus"
