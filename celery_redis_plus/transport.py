"""Custom Redis transport with native delayed delivery support."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from kombu.transport.redis import Channel as RedisChannel
from kombu.transport.redis import Transport as RedisTransport
from kombu.utils.json import dumps

from .constants import DELAY_HEADER, DELAYED_QUEUE_SUFFIX

if TYPE_CHECKING:
    from kombu import Connection

logger = logging.getLogger(__name__)


class Channel(RedisChannel):
    """Redis channel with native delayed delivery support.

    This channel overrides the _put method to intercept messages
    with the x-celery-delay-seconds header. Instead of publishing
    directly to the queue, delayed messages are added to a Redis ZSET
    with a score equal to the Unix timestamp when they should be delivered.
    """

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        """Put a message on the queue, handling delayed delivery if applicable.

        If the message contains an x-celery-delay-seconds header,
        the message is added to a delayed ZSET instead of the immediate queue.

        Args:
            queue: The queue name.
            message: The message dict (contains properties with headers).
            **kwargs: Additional keyword arguments.
        """
        # Extract headers from message properties
        properties = message.get("properties", {})
        headers = properties.get("headers", {}) if properties else {}

        delay_seconds = headers.get(DELAY_HEADER)

        if delay_seconds is not None and delay_seconds > 0:
            # Calculate delivery timestamp
            delivery_time = time.time() + float(delay_seconds)

            # Get the delayed queue key
            delayed_key = self._get_delayed_key(queue)

            # Serialize the message the same way the parent does
            serialized = dumps(message)

            # Add to ZSET with delivery timestamp as score
            with self.conn_or_acquire() as client:
                client.zadd(delayed_key, {serialized: delivery_time})

            logger.debug(
                "Added message to delayed queue %s with delivery time %s",
                delayed_key,
                delivery_time,
            )
        else:
            # No delay, publish normally
            super()._put(queue, message, **kwargs)

    def _get_delayed_key(self, queue_name: str) -> str:
        """Get the Redis key for the delayed queue ZSET.

        Args:
            queue_name: The base queue name.

        Returns:
            The delayed queue key.
        """
        return f"{queue_name}{DELAYED_QUEUE_SUFFIX}"


class Transport(RedisTransport):
    """Redis transport with native delayed delivery support.

    This transport extends the standard Redis transport to support
    native delayed message delivery using Redis ZSETs. Messages with
    an x-celery-delay-seconds header are stored in a ZSET with their
    delivery timestamp as the score. A background thread periodically
    moves ready messages to the actual queue.
    """

    Channel = Channel

    #: Flag indicating this transport supports native delayed delivery
    supports_native_delayed_delivery = True

    def __init__(self, client: Connection, **kwargs: Any) -> None:
        """Initialize the transport.

        Args:
            client: The Kombu connection.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(client, **kwargs)
        self._delayed_delivery_thread: threading.Thread | None = None
        self._delayed_delivery_stop_event: threading.Event | None = None
        self._monitored_queues: list[str] = []

    def setup_native_delayed_delivery(self, connection: Connection, queues: list[str]) -> None:
        """Set up native delayed delivery for the given queues.

        This method starts a background thread that periodically checks
        the delayed ZSETs and moves ready messages to the actual queues.

        Args:
            connection: The Kombu connection.
            queues: List of queue names to monitor for delayed messages.
        """
        if self._delayed_delivery_thread is not None:
            logger.warning("Delayed delivery already set up, skipping")
            return

        self._monitored_queues = list(queues)
        self._delayed_delivery_stop_event = threading.Event()

        self._delayed_delivery_thread = threading.Thread(
            target=self._delayed_delivery_loop,
            name="celery-redis-plus-delayed-delivery",
            daemon=True,
        )
        self._delayed_delivery_thread.start()
        logger.info("Started delayed delivery thread for queues: %s", queues)

    def teardown_native_delayed_delivery(self) -> None:
        """Stop the delayed delivery background thread."""
        if self._delayed_delivery_stop_event is not None:
            self._delayed_delivery_stop_event.set()

        if self._delayed_delivery_thread is not None:
            self._delayed_delivery_thread.join(timeout=5.0)
            self._delayed_delivery_thread = None
            logger.info("Stopped delayed delivery thread")

        self._delayed_delivery_stop_event = None
        self._monitored_queues = []

    def _delayed_delivery_loop(self) -> None:
        """Background loop that moves ready messages from delayed ZSETs to queues.

        This runs in a separate thread and periodically checks all monitored
        delayed queues. Messages whose delivery time has passed are moved
        to the actual queue using ZPOPMIN and LPUSH atomically via Lua script.
        """
        # Poll interval in seconds
        poll_interval = 1.0
        stop_event = self._delayed_delivery_stop_event
        assert stop_event is not None  # Set before thread starts

        while not stop_event.is_set():
            try:
                self._process_delayed_messages()
            except Exception:
                logger.exception("Error processing delayed messages")

            stop_event.wait(poll_interval)

    def _process_delayed_messages(self) -> None:
        """Process all ready messages from delayed queues.

        Checks each monitored queue's delayed ZSET and moves any messages
        whose delivery time has passed to the actual queue.
        """
        if not self._monitored_queues:
            return

        # Get a Redis client from a channel
        # We need to create a temporary channel to get the client
        try:
            channel = self.create_channel(self.client)
            with channel.conn_or_acquire() as client:
                current_time = time.time()
                for queue_name in self._monitored_queues:
                    delayed_key = f"{queue_name}{DELAYED_QUEUE_SUFFIX}"
                    self._move_ready_messages(client, delayed_key, queue_name, current_time)
        except Exception:
            logger.exception("Failed to process delayed messages")

    def _move_ready_messages(
        self,
        client: Any,
        delayed_key: str,
        queue_name: str,
        current_time: float,
    ) -> None:
        """Move ready messages from delayed ZSET to the queue.

        Uses a Lua script for atomicity to prevent race conditions.

        Args:
            client: The Redis client.
            delayed_key: The delayed queue ZSET key.
            queue_name: The target queue name.
            current_time: The current Unix timestamp.
        """
        # Lua script to atomically move ready messages
        # ZRANGEBYSCORE gets messages with score <= current_time
        # Then we ZREM and LPUSH each one
        lua_script = """
        local delayed_key = KEYS[1]
        local queue_key = KEYS[2]
        local current_time = tonumber(ARGV[1])
        local batch_size = tonumber(ARGV[2])

        local messages = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', current_time, 'LIMIT', 0, batch_size)

        local count = 0
        for i, message in ipairs(messages) do
            redis.call('ZREM', delayed_key, message)
            redis.call('LPUSH', queue_key, message)
            count = count + 1
        end

        return count
        """

        try:
            # Execute the Lua script
            moved_count = client.eval(lua_script, 2, delayed_key, queue_name, current_time, 100)
            if moved_count > 0:
                logger.debug("Moved %d messages from %s to %s", moved_count, delayed_key, queue_name)
        except Exception:
            logger.exception("Error moving messages from %s to %s", delayed_key, queue_name)
