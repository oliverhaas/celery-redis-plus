"""Shared constants for celery-redis-plus."""

# Header key for delay seconds in messages
DELAY_HEADER = "x-delay"

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

# Suffix for delayed message queues (stores messages until their eta)
DELAYED_QUEUE_SUFFIX = ":delayed"

# Interval in seconds for checking delayed messages (move ready ones to queue)
DEFAULT_DELAYED_CHECK_INTERVAL = 60
