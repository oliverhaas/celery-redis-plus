"""Enhanced Redis transport with BZMPOP priority queues, Streams fanout, and native delayed delivery.

This transport provides three key improvements over the standard Redis transport:
1. BZMPOP + sorted sets for regular queues - enables full 0-255 priority support and better reliability
2. Redis Streams for fanout exchanges - reliable consumer groups instead of lossy PUB/SUB
3. Native delayed delivery - delay integrated into sorted set score calculation

Requires Redis 7.0+ for BZMPOP support.

Connection String
=================
Connection string has the following format:

.. code-block::

    celery_redis_plus.transport:Transport://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/VIRTUALHOST]

Transport Options
=================
* ``visibility_timeout``: Time in seconds before unacked messages are restored (default: 300)
* ``stream_maxlen``: Maximum stream length for fanout streams (default: 10000)
* ``global_keyprefix``: Global prefix for all Redis keys
* ``socket_timeout``: Socket timeout in seconds
* ``socket_connect_timeout``: Socket connection timeout in seconds
* ``max_connections``: Maximum number of connections in pool
* ``health_check_interval``: Interval for health checks (default: 25)
* ``ssl``: Enable SSL/TLS connection. Set to ``True`` for default SSL settings,
  or a dict with SSL options (e.g., ``{'ssl_cert_reqs': ssl.CERT_REQUIRED}``)
"""

from __future__ import annotations

import functools
import numbers
import socket as socket_module
import uuid
from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from time import time
from typing import TYPE_CHECKING, Any

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.functional import accepts_argument
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.url import _parse_url
from vine import promise

from .constants import (
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_MESSAGE_TTL,
    DEFAULT_REQUEUE_BATCH_LIMIT,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_VISIBILITY_TIMEOUT,
    MESSAGE_KEY_PREFIX,
    PRIORITY_SCORE_MULTIPLIER,
)

if TYPE_CHECKING:
    from kombu import Connection

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None  # type: ignore[assignment]


logger = get_logger("kombu.transport.celery_redis_plus")
crit, warning = logger.critical, logger.warning

DEFAULT_PORT = 6379
DEFAULT_DB = 0

error_classes_t = namedtuple("error_classes_t", ("connection_errors", "channel_errors"))


def _queue_score(priority: int, timestamp: float | None = None) -> float:
    """Compute sorted set score for queue ordering.

    Higher priority number = higher priority = lower score = popped first.
    This matches RabbitMQ semantics where priority 255 is highest, 0 is lowest.
    Within same priority, earlier timestamp = lower score = popped first (FIFO).

    Args:
        priority: Message priority (0-255, higher is higher priority, matching RabbitMQ)
        timestamp: Unix timestamp in seconds (defaults to current time)

    Returns:
        Float score for ZADD
    """
    if timestamp is None:
        timestamp = time()
    # Invert priority so higher priority number = lower score = popped first
    # Multiply by large factor to leave room for millisecond timestamps
    return (255 - priority) * PRIORITY_SCORE_MULTIPLIER + int(timestamp * 1000)


def get_redis_error_classes() -> error_classes_t:
    """Return tuple of redis error classes."""
    from redis import exceptions

    # This exception changed name between redis-py versions
    DataError = getattr(exceptions, "InvalidData", exceptions.DataError)
    return error_classes_t(
        virtual.Transport.connection_errors  # type: ignore[attr-defined]
        + (
            InconsistencyError,
            socket_module.error,
            OSError,
            exceptions.ConnectionError,
            exceptions.BusyLoadingError,
            exceptions.AuthenticationError,
            exceptions.TimeoutError,
        ),
        virtual.Transport.channel_errors  # type: ignore[attr-defined]
        + (
            DataError,
            exceptions.InvalidResponse,
            exceptions.ResponseError,
        ),
    )


def get_redis_ConnectionError() -> type[Exception]:
    """Return the redis ConnectionError exception class."""
    from redis import exceptions

    return exceptions.ConnectionError


def _after_fork_cleanup_channel(channel: Channel) -> None:
    channel._after_fork()


class GlobalKeyPrefixMixin:
    """Mixin to provide common logic for global key prefixing.

    Overrides command execution to add prefixes to Redis keys.
    """

    global_keyprefix: str = ""

    PREFIXED_SIMPLE_COMMANDS = [
        "HDEL",
        "HGET",
        "HSET",
        "SADD",
        "SREM",
        "SMEMBERS",
        "ZADD",
        "ZCARD",
        "ZPOPMIN",
        "ZRANGEBYSCORE",
        "ZREM",
        "ZREVRANGEBYSCORE",
        "ZSCORE",
        "XADD",
    ]

    @staticmethod
    def _prefix_bzmpop_args(args: list[Any], prefix: str) -> list[Any]:
        """Prefix keys in BZMPOP command.

        BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
        """
        numkeys = int(args[1])
        keys_start = 2
        keys_end = 2 + numkeys
        pre_args = args[:keys_start]
        keys = [prefix + str(arg) for arg in args[keys_start:keys_end]]
        post_args = args[keys_end:]
        return pre_args + keys + post_args

    @staticmethod
    def _prefix_xread_args(args: list[Any], prefix: str) -> list[Any]:
        """Prefix keys in XREAD command.

        XREAD [COUNT n] [BLOCK ms] STREAMS <key1> ... <id1> ...
        """
        streams_idx = None
        for i, arg in enumerate(args):
            if arg in ("STREAMS", b"STREAMS"):
                streams_idx = i
                break
        if streams_idx is not None:
            after_streams = args[streams_idx + 1 :]
            num_streams = len(after_streams) // 2
            prefixed_keys = [prefix + str(k) for k in after_streams[:num_streams]]
            stream_ids = after_streams[num_streams:]
            return args[: streams_idx + 1] + prefixed_keys + stream_ids
        return args

    PREFIXED_COMPLEX_COMMANDS: dict[str, dict[str, int | None] | Any] = {
        "DEL": {"args_start": 0, "args_end": None},
        "WATCH": {"args_start": 0, "args_end": None},
        "BZMPOP": _prefix_bzmpop_args,
        "XREAD": _prefix_xread_args,
    }

    def _prefix_args(self, args: list[Any]) -> list[Any]:
        args = list(args)
        command = args.pop(0)

        if command in self.PREFIXED_SIMPLE_COMMANDS:
            args[0] = self.global_keyprefix + str(args[0])
        elif command in self.PREFIXED_COMPLEX_COMMANDS:
            spec = self.PREFIXED_COMPLEX_COMMANDS[command]
            if callable(spec):
                args = spec(args, self.global_keyprefix)
            else:
                # It's a dict with args_start/args_end
                args_start = spec["args_start"]
                args_end = spec["args_end"]

                pre_args = args[:args_start] if args_start and args_start > 0 else []
                post_args = args[args_end:] if args_end is not None else []

                args = pre_args + [self.global_keyprefix + str(arg) for arg in args[args_start:args_end]] + post_args

        return [command, *args]

    def parse_response(self, connection: Any, command_name: str, **options: Any) -> Any:
        """Parse a response from the Redis server."""
        ret = super().parse_response(connection, command_name, **options)  # type: ignore[misc]
        if command_name == "BZMPOP" and ret:
            # BZMPOP returns (key, [(member, score), ...])
            key, members = ret
            if isinstance(key, bytes):
                key = key.decode()
            key = key[len(self.global_keyprefix) :]
            return key, members
        return ret

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        return super().execute_command(*self._prefix_args(list(args)), **kwargs)  # type: ignore[misc]

    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> PrefixedRedisPipeline:
        return PrefixedRedisPipeline(
            self.connection_pool,  # type: ignore[attr-defined]
            self.response_callbacks,  # type: ignore[attr-defined]
            transaction,
            shard_hint,
            global_keyprefix=self.global_keyprefix,
        )


class PrefixedStrictRedis(GlobalKeyPrefixMixin, redis.Redis):  # type: ignore[misc]
    """Redis client that prefixes all keys."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.global_keyprefix = kwargs.pop("global_keyprefix", "")
        redis.Redis.__init__(self, *args, **kwargs)


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, redis.client.Pipeline):  # type: ignore[misc]
    """Redis pipeline that prefixes all keys."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.global_keyprefix = kwargs.pop("global_keyprefix", "")
        redis.client.Pipeline.__init__(self, *args, **kwargs)


class QoS(virtual.QoS):
    """Redis QoS with sorted set based message tracking.

    Messages are stored in a hash at publish time with visibility tracking
    in a separate sorted set. This allows recovery of messages from crashed
    workers based on their index scores.
    """

    restore_at_shutdown = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # For streams fanout: track delivery tags that came from fanout (no ack needed)
        self._fanout_tags: set[str] = set()

    def append(self, message: Any, delivery_tag: str) -> None:
        # Message is already stored in messages hash at publish time.
        # Just track it in _delivered for local state management.
        super().append(message, delivery_tag)

    def ack(self, delivery_tag: str) -> None:
        # Fanout messages don't need Redis cleanup (no consumer groups)
        if delivery_tag in self._fanout_tags:
            self._fanout_tags.discard(delivery_tag)
        else:
            # Regular sorted set message
            self._remove_from_indices(delivery_tag).execute()
        super().ack(delivery_tag)

    def reject(self, delivery_tag: str, requeue: bool = False) -> None:
        # Fanout messages: requeue not supported (fire-and-forget broadcast)
        if delivery_tag in self._fanout_tags:
            self._fanout_tags.discard(delivery_tag)
            super().ack(delivery_tag)
        else:
            # Regular sorted set message
            if requeue:
                self.restore_by_tag(delivery_tag, leftmost=True)
            else:
                self._remove_from_indices(delivery_tag).execute()
            super().ack(delivery_tag)

    @contextmanager
    def pipe_or_acquire(self, pipe: Any = None, client: Any = None):
        if pipe:
            yield pipe
        else:
            with self.channel.conn_or_acquire(client) as client:
                yield client.pipeline()

    def _remove_from_indices(self, delivery_tag: str, pipe: Any = None) -> Any:
        message_key = self.channel._message_key(delivery_tag)
        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(self.messages_index_key, delivery_tag).delete(message_key)

    def maybe_update_messages_index(self) -> None:
        """Update scores of delivered messages to now + visibility_timeout.

        Acts as a heartbeat to keep messages from being requeued by
        requeue_messages() while they are still being processed.

        Uses ZADD XX to only update existing entries, avoiding race conditions
        where a message is acked (removed from index) between checking
        _delivered and executing ZADD.
        """
        if not self._delivered:
            return
        queue_at = time() + self.visibility_timeout
        with self.channel.conn_or_acquire() as client, client.pipeline() as pipe:
            for tag in self._delivered:
                # Skip fanout messages (they don't use the index)
                if tag not in self._fanout_tags:
                    # XX = only update if member already exists (prevents re-adding acked messages)
                    pipe.zadd(self.messages_index_key, {tag: queue_at}, xx=True)
            pipe.execute()

    def requeue_messages(self) -> int:
        """Requeue messages whose queue_at time has passed.

        This unified method handles both:
        - Delayed messages that are now ready to be processed
        - Messages that were consumed but not acked (lost/timed out)

        Uses a Lua script for atomic, efficient batch processing.

        Returns:
            Number of messages requeued.
        """
        return self.channel.requeue_messages()

    def restore_by_tag(self, tag: str, client: Any = None, leftmost: bool = False) -> None:
        """Restore a message by its delivery tag using Lua script.

        The Lua script atomically reads the routing_key (queue) from the message
        hash and adds the message back to that queue.

        Args:
            tag: The message's delivery tag.
            client: Optional Redis client (unused, kept for API compatibility).
            leftmost: If True, restore to front of queue (score=0).
        """
        self.channel._restore_by_tag(tag, leftmost)

    @cached_property
    def messages_index_key(self) -> str:
        return self.channel.messages_index_key

    @cached_property
    def visibility_timeout(self) -> float:
        return self.channel.visibility_timeout


class MultiChannelPoller:
    """Async I/O poller for Redis transport."""

    eventflags = READ | ERR

    _in_protected_read = False
    after_read: set[Any]

    def __init__(self) -> None:
        self._channels: set[Channel] = set()
        self._fd_to_chan: dict[int, tuple[Channel, str]] = {}
        self._chan_to_sock: dict[tuple[Channel, Any, str], Any] = {}
        self.poller = poll()
        self.after_read = set()

    def close(self) -> None:
        for fd in self._chan_to_sock.values():
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def add(self, channel: Channel) -> None:
        self._channels.add(channel)

    def discard(self, channel: Channel) -> None:
        self._channels.discard(channel)

    def _on_connection_disconnect(self, connection: Any) -> None:
        try:
            self.poller.unregister(connection._sock)
        except (AttributeError, TypeError):
            pass

    def _register(self, channel: Channel, client: Any, cmd_type: str) -> None:
        if (channel, client, cmd_type) in self._chan_to_sock:
            self._unregister(channel, client, cmd_type)
        if client.connection._sock is None:
            client.connection.connect()
        sock = client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, cmd_type)
        self._chan_to_sock[(channel, client, cmd_type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel: Channel, client: Any, cmd_type: str) -> None:
        self.poller.unregister(self._chan_to_sock[(channel, client, cmd_type)])

    def _client_registered(self, channel: Channel, client: Any, cmd: str) -> bool:
        if getattr(client, "connection", None) is None:
            client.connection = client.connection_pool.get_connection("_")
        return client.connection._sock is not None and (channel, client, cmd) in self._chan_to_sock

    def _register_BZMPOP(self, channel: Channel) -> None:
        """Enable BZMPOP mode for channel."""
        ident = channel, channel.client, "BZMPOP"
        if not self._client_registered(channel, channel.client, "BZMPOP"):
            channel._in_poll = False
            self._register(*ident)
        if not channel._in_poll:
            channel._bzmpop_start()

    def _register_XREAD(self, channel: Channel) -> None:
        """Enable XREAD mode for channel (fanout streams)."""
        ident = channel, channel.client, "XREAD"
        if not self._client_registered(channel, channel.client, "XREAD"):
            channel._in_fanout_poll = False
            self._register(*ident)
        if not channel._in_fanout_poll:
            channel._xread_start()

    def on_poll_start(self) -> None:
        for channel in self._channels:
            if channel.active_queues:
                if channel.qos.can_consume():
                    self._register_BZMPOP(channel)
            if channel.active_fanout_queues:
                if channel.qos.can_consume():
                    self._register_XREAD(channel)

    def on_poll_init(self, poller: Any) -> None:
        self.poller = poller
        # Initial requeue check on startup
        self.maybe_requeue_messages()

    def maybe_requeue_messages(self) -> int:
        """Requeue messages whose queue_at time has passed.

        This unified method handles both delayed messages and timed-out messages.

        Returns:
            Total number of messages requeued across all channels.
        """
        total_requeued = 0
        for channel in self._channels:
            if channel.active_queues:
                total_requeued += channel.qos.requeue_messages()
        return total_requeued

    def maybe_update_messages_index(self) -> None:
        """Update message index scores to keep delivered messages alive."""
        for channel in self._channels:
            if channel.active_queues:
                channel.qos.maybe_update_messages_index()

    def on_readable(self, fileno: int) -> bool | None:
        chan, cmd_type = self._fd_to_chan[fileno]
        if chan.qos.can_consume():
            return chan.handlers[cmd_type]()
        return None

    def handle_event(self, fileno: int, event: int) -> tuple[Any, MultiChannelPoller] | None:
        if event & READ:
            return self.on_readable(fileno), self
        if event & ERR:
            chan, cmd_type = self._fd_to_chan[fileno]
            chan._poll_error(cmd_type)
        return None

    def get(self, callback: Any, timeout: float | None = None) -> None:
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:
                    if channel.qos.can_consume():
                        self._register_BZMPOP(channel)
                if channel.active_fanout_queues:
                    if channel.qos.can_consume():
                        self._register_XREAD(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            raise Empty()
        finally:
            self._in_protected_read = False
            while self.after_read:
                try:
                    fun = self.after_read.pop()
                except KeyError:
                    break
                else:
                    fun()

    @property
    def fds(self) -> dict[int, tuple[Channel, str]]:
        return self._fd_to_chan


class Channel(virtual.Channel):
    """Redis Channel with BZMPOP priority queues and Streams fanout.

    Uses:
    - BZMPOP + sorted sets for regular queues (priority support, reliability)
    - Redis Streams + consumer groups for fanout (reliable, not lossy)
    - Native delayed delivery via score calculation
    """

    QoS = QoS

    _client: Any = None
    _closing = False
    supports_fanout = True
    keyprefix_queue = "_kombu.binding.%s"
    keyprefix_fanout = "/{db}."
    sep = "\x06\x16"
    _in_poll = False
    _in_fanout_poll = False
    _fanout_queues: dict[str, tuple[str, str]] = {}

    # Message storage keys
    # Per-message hash keys use format: {message_key_prefix}{delivery_tag}
    message_key_prefix = MESSAGE_KEY_PREFIX
    message_ttl = DEFAULT_MESSAGE_TTL  # TTL for per-message hashes (3 days default)
    messages_index_key = "messages_index"

    # Visibility and timeout settings
    visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT
    socket_timeout: float | None = None
    socket_connect_timeout: float | None = None
    socket_keepalive: bool | None = None
    socket_keepalive_options: dict[str, Any] | None = None
    retry_on_timeout: bool | None = None
    max_connections = 10
    health_check_interval = DEFAULT_HEALTH_CHECK_INTERVAL
    client_name: str | None = None

    # Streams configuration
    stream_maxlen = DEFAULT_STREAM_MAXLEN

    # Global key prefix
    global_keyprefix = ""

    # Fanout settings
    fanout_prefix: bool | str = True
    fanout_patterns = True

    _async_pool: Any = None
    _pool: Any = None

    from_transport_options = virtual.Channel.from_transport_options + (
        "sep",
        "message_key_prefix",
        "message_ttl",
        "messages_index_key",
        "visibility_timeout",
        "fanout_prefix",
        "fanout_patterns",
        "global_keyprefix",
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "socket_keepalive_options",
        "max_connections",
        "health_check_interval",
        "retry_on_timeout",
        "client_name",
        "stream_maxlen",
    )

    connection_class = redis.Connection if redis else None
    connection_class_ssl = redis.SSLConnection if redis else None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._registered = False
        self._queue_cycle: list[str] = []
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues: set[str] = set()
        self.auto_delete_queues: set[str] = set()
        self._fanout_to_queue: dict[str, str] = {}
        self.handlers = {"BZMPOP": self._bzmpop_read, "XREAD": self._xread_read}
        # Track last-read stream ID per stream for fanout (start with $ = only new messages)
        self._stream_offsets: dict[str, str] = {}

        if self.fanout_prefix:
            if isinstance(self.fanout_prefix, str):
                self.keyprefix_fanout = self.fanout_prefix
        else:
            self.keyprefix_fanout = ""

        # Evaluate connection
        try:
            self.client.ping()
        except Exception:
            self._disconnect_pools()
            raise

        self.connection.cycle.add(self)
        self._registered = True

        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

    def _after_fork(self) -> None:
        self._disconnect_pools()

    def _disconnect_pools(self) -> None:
        pool = self._pool
        async_pool = self._async_pool

        self._async_pool = self._pool = None

        if pool is not None:
            pool.disconnect()

        if async_pool is not None:
            async_pool.disconnect()

    def _on_connection_disconnect(self, connection: Any) -> None:
        if self._in_poll is connection:
            self._in_poll = None  # type: ignore[assignment]
        if self._in_fanout_poll is connection:
            self._in_fanout_poll = None  # type: ignore[assignment]
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _message_key(self, delivery_tag: str) -> str:
        """Get the Redis key for a message's per-message hash."""
        return f"{self.message_key_prefix}{delivery_tag}"

    def _get_message_from_hash(self, message_key: str, client: Any) -> dict[str, Any] | None:
        """Fetch message payload from per-message hash.

        Args:
            message_key: The Redis key for the message hash.
            client: Redis client to use.

        Returns:
            The message dict, or None if not found.
        """
        payload_json = client.hget(message_key, "payload")
        if not payload_json:
            return None
        return loads(bytes_to_str(payload_json))  # type: ignore[call-arg]

    def _restore(self, message: Any, leftmost: bool = False) -> None:
        """Restore a message to its queue.

        This method is called by Kombu's virtual.Channel for message recovery.
        """
        self._restore_by_tag(message.delivery_tag, leftmost)

    def _restore_at_beginning(self, message: Any) -> None:
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue: str, *args: Any, **kwargs: Any) -> str:
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super().basic_consume(queue, *args, **kwargs)
        self._update_queue_cycle()
        return ret

    def basic_cancel(self, consumer_tag: str) -> Any:
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
                return connection.cycle.after_read.add(promise(self._basic_cancel, (consumer_tag,)))  # type: ignore[call-arg]
            return self._basic_cancel(consumer_tag)
        return None

    def _basic_cancel(self, consumer_tag: str) -> Any:
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return None
        try:
            self.active_fanout_queues.remove(queue)
        except KeyError:
            pass
        try:
            exchange, _ = self._fanout_queues[queue]
            self._fanout_to_queue.pop(exchange)
        except KeyError:
            pass
        ret = super().basic_cancel(consumer_tag)
        self._update_queue_cycle()
        return ret

    # --- BZMPOP (sorted set) methods for regular queues ---

    def _bzmpop_start(self, timeout: float | None = None) -> None:
        if timeout is None:
            timeout = self.connection.polling_interval or 1
        if not self._queue_cycle:
            return
        keys = list(self._queue_cycle)
        self._in_poll = self.client.connection

        command_args: list[Any] = ["BZMPOP", timeout or 0, len(keys), *keys, "MIN"]
        if self.global_keyprefix:
            command_args = self.client._prefix_args(command_args)

        self.client.connection.send_command(*command_args)

    def _bzmpop_read(self, **options: Any) -> bool:
        try:
            try:
                result = self.client.parse_response(self.client.connection, "BZMPOP", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                raise
            if result:
                dest, members = result
                dest = bytes_to_str(dest)
                delivery_tag, _score = members[0]
                delivery_tag = bytes_to_str(delivery_tag)
                message_key = self._message_key(delivery_tag)
                message = self._get_message_from_hash(message_key, self.client)
                if message:
                    self.connection._deliver(message, dest)
                    return True
                raise Empty()
            raise Empty()
        finally:
            self._in_poll = None  # type: ignore[assignment]

    # --- XREADGROUP (Streams) methods for fanout ---

    def _fanout_stream_key(self, exchange: str, routing_key: str = "") -> str:
        """Get stream key for fanout exchange."""
        if routing_key and self.fanout_patterns:
            return f"{self.keyprefix_fanout}{exchange}/{routing_key}"
        return f"{self.keyprefix_fanout}{exchange}"

    def _xread_start(self, timeout: float | None = None) -> None:
        """Start XREAD for fanout streams (true broadcast - every consumer gets every message)."""
        if timeout is None:
            timeout = self.connection.polling_interval or 1

        streams: dict[str, str] = {}

        for queue in self.active_fanout_queues:
            if queue in self._fanout_queues:
                exchange, routing_key = self._fanout_queues[queue]
                stream_key = self._fanout_stream_key(exchange, routing_key)
                # Use stored offset or "$" for only new messages
                offset = self._stream_offsets.get(stream_key, "$")
                streams[stream_key] = offset

        if not streams:
            return

        self._in_fanout_poll = self.client.connection

        # Build XREAD command
        stream_keys = list(streams.keys())
        stream_ids = [streams[k] for k in stream_keys]

        command_args: list[Any] = [
            "XREAD",
            "COUNT",
            "1",
            "BLOCK",
            str(int((timeout or 0) * 1000)),
            "STREAMS",
            *stream_keys,
            *stream_ids,
        ]

        if self.global_keyprefix:
            command_args = self.client._prefix_args(command_args)

        self.client.connection.send_command(*command_args)

    def _xread_read(self, **options: Any) -> bool:
        """Read messages from XREAD (fanout broadcast)."""
        try:
            try:
                messages = self.client.parse_response(self.client.connection, "XREAD", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                raise

            if not messages:
                raise Empty()

            for stream, message_list in messages:
                stream_str = bytes_to_str(stream) if isinstance(stream, bytes) else stream
                for message_id, fields in message_list:
                    message_id_str = bytes_to_str(message_id) if isinstance(message_id, bytes) else message_id

                    # Update offset for this stream
                    # Strip prefix if present for storing offset
                    offset_key = stream_str
                    prefix = self.global_keyprefix
                    if prefix and stream_str.startswith(prefix):
                        offset_key = stream_str[len(prefix) :]
                    self._stream_offsets[offset_key] = message_id_str

                    # Find which queue this stream belongs to
                    queue_name = None
                    for queue, (exchange, routing_key) in self._fanout_queues.items():
                        fanout_stream = self._fanout_stream_key(exchange, routing_key)
                        if stream_str.endswith(fanout_stream) or stream_str == fanout_stream:
                            queue_name = queue
                            break

                    if not queue_name:
                        continue

                    # Parse payload
                    payload_field = fields.get(b"payload") or fields.get("payload")
                    if not payload_field:
                        continue
                    payload = loads(bytes_to_str(payload_field))  # type: ignore[call-arg]

                    # Set delivery tag
                    delivery_tag = self._next_delivery_tag()
                    payload["properties"]["delivery_tag"] = delivery_tag

                    # Mark as fanout message (no ack needed)
                    self.qos._fanout_tags.add(delivery_tag)

                    # Deliver message
                    self.connection._deliver(payload, queue_name)
                    return True

            raise Empty()
        finally:
            self._in_fanout_poll = None  # type: ignore[assignment]

    def _poll_error(self, cmd_type: str, **options: Any) -> Any:
        return self.client.parse_response(self.client.connection, cmd_type)

    def _get(self, queue: str, timeout: float | None = None) -> dict[str, Any]:  # type: ignore[override]
        """Get single message from queue (synchronous)."""
        with self.conn_or_acquire() as client:
            result = client.zpopmin(queue, count=1)
            if result:
                delivery_tag, _score = result[0]
                delivery_tag = bytes_to_str(delivery_tag)
                message_key = self._message_key(delivery_tag)
                message = self._get_message_from_hash(message_key, client)
                if message:
                    return message
            raise Empty()

    def _size(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            return client.zcard(queue)

    # Lua script for requeuing messages whose queue_at time has passed.
    # This handles both delayed messages and messages that timed out (not acked).
    # Uses per-message hashes: message:{tag} with fields: payload, routing_key, priority, native_delayed, eta
    # For native_delayed messages: set native_delayed=0 (first delivery, not a redelivery)
    # For timed-out messages: set redelivered=1 (message was consumed but not acked)
    # Reads routing_key from hash to add message to the correct queue.
    # KEYS: [1] = messages_index
    # ARGV: [1] = threshold, [2] = batch_limit, [3] = visibility_timeout,
    #       [4] = priority_multiplier, [5] = message_key_prefix, [6] = global_keyprefix
    # Returns: total number of messages requeued
    _requeue_messages_lua = """
    local messages_index = KEYS[1]
    local threshold = tonumber(ARGV[1])
    local batch_limit = tonumber(ARGV[2])
    local visibility_timeout = tonumber(ARGV[3])
    local priority_multiplier = tonumber(ARGV[4])
    local message_key_prefix = ARGV[5]
    local global_keyprefix = ARGV[6]
    local total_requeued = 0

    -- Get current time in seconds and milliseconds
    local time_result = redis.call('TIME')
    local now_sec = tonumber(time_result[1])
    local now_ms = now_sec * 1000 + math.floor(tonumber(time_result[2]) / 1000)

    -- Get messages ready for requeue (score <= threshold)
    local ready = redis.call('ZRANGEBYSCORE', messages_index, '-inf', threshold, 'LIMIT', 0, batch_limit)

    for _, tag in ipairs(ready) do
        -- Build prefixed message key
        local message_key = global_keyprefix .. message_key_prefix .. tag

        -- Get fields from per-message hash
        local priority = redis.call('HGET', message_key, 'priority')
        if priority then
            priority = tonumber(priority)
            local routing_key = redis.call('HGET', message_key, 'routing_key')
            local eta = redis.call('HGET', message_key, 'eta')
            eta = eta and tonumber(eta) or 0

            -- Calculate queue score using eta if it's in the future, else use now
            local score_time_ms
            if eta > 0 and eta * 1000 > now_ms then
                score_time_ms = eta * 1000
            else
                score_time_ms = now_ms
            end
            local queue_score = (255 - priority) * priority_multiplier + score_time_ms

            -- Check if this is a native delayed message (first delivery) or a timed-out message (redelivery)
            local native_delayed = redis.call('HGET', message_key, 'native_delayed')
            if native_delayed and tonumber(native_delayed) == 1 then
                -- Native delayed message: clear the flag (this is the first delivery)
                redis.call('HSET', message_key, 'native_delayed', '0')
            else
                -- Timed-out message: mark as redelivered
                redis.call('HSET', message_key, 'redelivered', '1')
            end

            -- Add to the message's queue (with global prefix)
            local queue_key = global_keyprefix .. routing_key
            redis.call('ZADD', queue_key, 'NX', queue_score, tag)

            -- Update queue_at for next cycle (now + visibility_timeout)
            local new_queue_at = now_sec + visibility_timeout
            redis.call('ZADD', messages_index, new_queue_at, tag)
            total_requeued = total_requeued + 1
        else
            -- No message hash = message was already acked/deleted, clean up index
            redis.call('ZREM', messages_index, tag)
        end
    end

    return total_requeued
    """

    # Lua script for restoring a single message to its queue.
    # Sets redelivered=1 and adds to queue with appropriate score.
    # Uses routing_key from the hash as the queue name.
    # KEYS: [1] = message_key
    # ARGV: [1] = leftmost (1 or 0), [2] = priority_multiplier, [3] = message_ttl, [4] = global_keyprefix
    # Returns: 1 if restored, 0 if message not found
    _restore_message_lua = """
    local message_key = KEYS[1]
    local leftmost = tonumber(ARGV[1]) == 1
    local priority_multiplier = tonumber(ARGV[2])
    local message_ttl = tonumber(ARGV[3])
    local global_keyprefix = ARGV[4]

    -- Get priority and routing_key (queue) from hash
    local priority = redis.call('HGET', message_key, 'priority')
    local routing_key = redis.call('HGET', message_key, 'routing_key')
    if not priority or not routing_key then
        return 0
    end

    -- Mark as redelivered
    redis.call('HSET', message_key, 'redelivered', '1')
    redis.call('EXPIRE', message_key, message_ttl)

    -- Calculate score
    local score
    if leftmost then
        score = 0
    else
        priority = tonumber(priority)
        local now_ms = redis.call('TIME')
        now_ms = tonumber(now_ms[1]) * 1000 + math.floor(tonumber(now_ms[2]) / 1000)
        score = (255 - priority) * priority_multiplier + now_ms
    end

    -- Add to queue (routing_key with global prefix)
    local queue_key = global_keyprefix .. routing_key
    local tag = string.match(message_key, ':(.+)$')
    redis.call('ZADD', queue_key, score, tag)

    return 1
    """

    def requeue_messages(self) -> int:
        """Requeue messages whose queue_at time has passed.

        This unified method handles both:
        - Delayed messages that are now ready to be processed
        - Messages that were consumed but not acked (lost/timed out)

        Uses ZADD NX to avoid re-adding messages that are already in the queue.
        The Lua script reads routing_key from each message's hash to add it to the
        correct queue, and calculates new_queue_at = now + visibility_timeout.

        Returns:
            Number of messages requeued.
        """
        now = time()
        # Check messages that will need requeuing by next interval
        threshold = now + DEFAULT_REQUEUE_CHECK_INTERVAL

        with self.conn_or_acquire() as client:
            requeue_script = client.register_script(self._requeue_messages_lua)

            # Only pass messages_index as a key; routing_key is read from each message's hash
            keys = [self.messages_index_key]
            total_requeued = requeue_script(
                keys=keys,
                args=[
                    threshold,
                    DEFAULT_REQUEUE_BATCH_LIMIT,
                    self.visibility_timeout,
                    PRIORITY_SCORE_MULTIPLIER,
                    self.message_key_prefix,
                    self.global_keyprefix,
                ],
            )

        if total_requeued >= DEFAULT_REQUEUE_BATCH_LIMIT:
            warning(
                "Requeue hit batch limit of %d. There may be more messages waiting.",
                DEFAULT_REQUEUE_BATCH_LIMIT,
            )

        return total_requeued or 0

    def _restore_by_tag(self, delivery_tag: str, leftmost: bool = False) -> bool:
        """Restore a message to its queue using Lua script.

        The Lua script atomically reads the routing_key (queue) from the message
        hash and adds the message back to that queue. Sets the redelivered flag.

        Args:
            delivery_tag: The message's delivery tag.
            leftmost: If True, restore to front of queue (score=0).

        Returns:
            True if message was restored, False if not found.
        """
        message_key = self._message_key(delivery_tag)

        with self.conn_or_acquire() as client:
            restore_script = client.register_script(self._restore_message_lua)
            result = restore_script(
                keys=[message_key],
                args=[1 if leftmost else 0, PRIORITY_SCORE_MULTIPLIER, self.message_ttl, self.global_keyprefix],
            )
            return bool(result)

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        """Deliver message to queue using sorted set.

        All messages go directly to the queue with a score encoding priority and
        scheduled time. The messages_index tracks when to attempt (re)queue if the
        message is not acknowledged (queue_at = visible_at + visibility_timeout).

        Args:
            queue: Target queue name.
            message: Message dict with 'properties' containing optional 'eta'
                     (Unix timestamp float) for delayed delivery.
        """
        priority = self._get_message_priority(message, reverse=False)
        props = message["properties"]
        delivery_tag = props["delivery_tag"]

        now = time()

        # eta is a Unix timestamp (float) in properties, similar to priority
        # Native delayed delivery only applies if delay > requeue check interval.
        # Shorter delays are handled by Celery's built-in eta logic (immediate delivery).
        eta_timestamp: float | None = props.get("eta")
        is_native_delayed = eta_timestamp is not None and (eta_timestamp - now) > DEFAULT_REQUEUE_CHECK_INTERVAL
        visible_at = eta_timestamp if is_native_delayed else now

        # Queue score encodes priority and scheduled time
        queue_score = _queue_score(priority, visible_at)

        # queue_at: when to check if this message needs (re)queuing
        # For native delayed messages: queue_at = eta (requeue mechanism delivers at eta)
        # For immediate messages: queue_at = now + visibility_timeout (requeue if not acked)
        queue_at = eta_timestamp if is_native_delayed else now + self.visibility_timeout

        message_key = self._message_key(delivery_tag)

        with self.conn_or_acquire() as client, client.pipeline() as pipe:
            # Store message in per-message hash with individual fields
            # routing_key is used as the queue name for restore operations
            pipe.hset(
                message_key,
                mapping={
                    "payload": dumps(message),  # type: ignore[call-arg]
                    "routing_key": queue,
                    "priority": priority,
                    "redelivered": 0,
                    "native_delayed": 1 if is_native_delayed else 0,
                    "eta": eta_timestamp if eta_timestamp else 0,
                },
            )
            pipe.expire(message_key, self.message_ttl)
            pipe.zadd(self.messages_index_key, {delivery_tag: queue_at})
            if not is_native_delayed:
                pipe.zadd(queue, {delivery_tag: queue_score})
            pipe.execute()

    def _put_fanout(self, exchange: str, message: dict[str, Any], routing_key: str, **kwargs: Any) -> None:
        """Deliver fanout message using Redis Streams."""
        stream_key = self._fanout_stream_key(exchange, routing_key)
        message_uuid = str(uuid.uuid4())

        with self.conn_or_acquire() as client:
            client.xadd(
                name=stream_key,
                fields={"uuid": message_uuid, "payload": dumps(message)},  # type: ignore[call-arg]
                id="*",
                maxlen=self.stream_maxlen,
                approximate=True,
            )

    def _new_queue(self, queue: str, auto_delete: bool = False, **kwargs: Any) -> None:
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange: str, routing_key: str, pattern: str, queue: str) -> None:
        if self.typeof(exchange).type == "fanout":
            self._fanout_queues[queue] = (exchange, routing_key.replace("#", "*"))
        with self.conn_or_acquire() as client:
            client.sadd(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or "", pattern or "", queue or ""]),
            )

    def _delete(
        self,
        queue: str,
        exchange: str = "",
        routing_key: str = "",
        pattern: str = "",
        *args: Any,
        **kwargs: Any,
    ) -> None:  # type: ignore[override]
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get("client")) as client:
            client.srem(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or "", pattern or "", queue or ""]),
            )
            client.delete(queue)

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        with self.conn_or_acquire() as client:
            return bool(client.exists(queue))

    def get_table(self, exchange: str) -> list[tuple[str, ...]]:
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                return []
            return [tuple(bytes_to_str(val).split(self.sep)) for val in values]

    def _purge(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            size = client.zcard(queue)
            client.delete(queue)
            return size

    def close(self) -> None:
        self._closing = True
        if self._in_poll:
            try:
                self._bzmpop_read()
            except Empty:
                pass
        if self._in_fanout_poll:
            try:
                self._xread_read()
            except Empty:
                pass
        if not self.closed:
            self.connection.cycle.discard(self)

            client = self.__dict__.get("client")
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super().close()

    def _close_clients(self) -> None:
        try:
            client = self.__dict__["client"]
            connection, client.connection = client.connection, None
            connection.disconnect()
        except (KeyError, AttributeError, self.ResponseError):
            pass

    def _prepare_virtual_host(self, vhost: Any) -> int:
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == "/":
                vhost = DEFAULT_DB
            elif vhost.startswith("/"):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(f"Database is int between 0 and limit - 1, not {vhost}") from None
        return vhost

    def _connparams(self, asynchronous: bool = False) -> dict[str, Any]:
        conninfo = self.connection.client
        connparams: dict[str, Any] = {
            "host": conninfo.hostname or "127.0.0.1",
            "port": conninfo.port or self.connection.default_port,
            "virtual_host": conninfo.virtual_host,
            "username": conninfo.userid,
            "password": conninfo.password,
            "max_connections": self.max_connections,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "socket_keepalive": self.socket_keepalive,
            "socket_keepalive_options": self.socket_keepalive_options,
            "health_check_interval": self.health_check_interval,
            "retry_on_timeout": self.retry_on_timeout,
            "client_name": self.client_name,
        }

        conn_class = self.connection_class

        if hasattr(conn_class, "__init__"):
            classes = [conn_class]
            if hasattr(conn_class, "__bases__"):
                classes += list(conn_class.__bases__)
            for klass in classes:
                if accepts_argument(klass.__init__, "health_check_interval"):
                    break
            else:
                connparams.pop("health_check_interval")

        # Check for SSL configuration from URL scheme (rediss://) or transport_options
        ssl_config = conninfo.ssl
        if not ssl_config:
            # Fall back to transport_options for path-based transport URLs
            ssl_config = self.connection.client.transport_options.get("ssl")

        if ssl_config:
            try:
                if isinstance(ssl_config, dict):
                    connparams.update(ssl_config)
                connparams["connection_class"] = self.connection_class_ssl
            except TypeError:
                pass

        host = connparams["host"]
        if "://" in host:
            scheme, _, _, username, password, path, query = _parse_url(host)
            if scheme == "socket":
                connparams.update(
                    {
                        "connection_class": redis.UnixDomainSocketConnection,
                        "path": "/" + path,
                    },
                    **query,
                )
                connparams.pop("socket_connect_timeout", None)
                connparams.pop("socket_keepalive", None)
                connparams.pop("socket_keepalive_options", None)
            connparams["username"] = username
            connparams["password"] = password
            connparams.pop("host", None)
            connparams.pop("port", None)

        connparams["db"] = self._prepare_virtual_host(connparams.pop("virtual_host", None))

        channel = self
        connection_cls = connparams.get("connection_class") or self.connection_class

        if asynchronous:

            class Connection(connection_cls):  # type: ignore[valid-type, misc]
                def disconnect(self, *args: Any) -> None:
                    super().disconnect(*args)
                    if channel._registered:
                        channel._on_connection_disconnect(self)

            connection_cls = Connection

        connparams["connection_class"] = connection_cls
        return connparams

    def _create_client(self, asynchronous: bool = False) -> Any:
        if asynchronous:
            return self.Client(connection_pool=self.async_pool)
        return self.Client(connection_pool=self.pool)

    def _get_pool(self, asynchronous: bool = False) -> Any:
        params = self._connparams(asynchronous=asynchronous)
        self.keyprefix_fanout = self.keyprefix_fanout.format(db=params["db"])
        return redis.ConnectionPool(**params)

    def _get_client(self) -> Any:
        if redis.VERSION < (3, 2, 0):
            raise VersionMismatch(
                f"Redis transport requires redis-py versions 3.2.0 or later. You have {redis.__version__}",
            )

        if self.global_keyprefix:
            return functools.partial(PrefixedStrictRedis, global_keyprefix=self.global_keyprefix)

        return redis.Redis

    @contextmanager
    def conn_or_acquire(self, client: Any = None):
        if client:
            yield client
        else:
            yield self._create_client()

    @property
    def pool(self) -> Any:
        if self._pool is None:
            self._pool = self._get_pool()
        return self._pool

    @property
    def async_pool(self) -> Any:
        if self._async_pool is None:
            self._async_pool = self._get_pool(asynchronous=True)
        return self._async_pool

    @cached_property
    def client(self) -> Any:
        """Client used to publish messages, BZMPOP etc."""
        return self._create_client(asynchronous=True)

    def _update_queue_cycle(self) -> None:
        self._queue_cycle = list(self.active_queues)

    def _get_response_error(self) -> type[Exception]:
        from redis import exceptions

        return exceptions.ResponseError

    @property
    def active_queues(self) -> set[str]:
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues if queue not in self.active_fanout_queues}


class Transport(virtual.Transport):
    """Enhanced Redis Transport with priority queues, reliable fanout, and delayed delivery.

    Uses:
    - BZMPOP + sorted sets for regular queues (priority support, reliability)
    - Redis Streams + consumer groups for fanout (reliable, not lossy)
    - Integrated delayed delivery via score calculation

    Requires Redis 7.0+ for BZMPOP support.
    """

    Channel = Channel

    polling_interval = 10  # Timeout for blocking BZMPOP/XREADGROUP calls in seconds
    default_port = DEFAULT_PORT
    driver_type = "redis"
    driver_name = "redis"
    cycle: MultiChannelPoller

    #: Flag indicating this transport supports native delayed delivery
    supports_native_delayed_delivery = True

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(["direct", "topic", "fanout"]),
    )

    if redis:
        connection_errors, channel_errors = get_redis_error_classes()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if redis is None:
            raise ImportError("Missing redis library (pip install redis)")
        super().__init__(*args, **kwargs)

        # Import signals module to register signal handlers when transport is used
        from . import signals as _signals  # noqa: F401

        self.cycle = MultiChannelPoller()

    def driver_version(self) -> str:
        return redis.__version__

    def register_with_event_loop(self, connection: Connection, loop: Any) -> None:
        cycle = self.cycle
        cycle.on_poll_init(loop.poller)
        cycle_poll_start = cycle.on_poll_start
        add_reader = loop.add_reader
        on_readable = self.on_readable

        def _on_disconnect(connection: Any) -> None:
            if connection._sock:
                loop.remove(connection._sock)
            if cycle.fds:
                try:
                    loop.on_tick.remove(on_poll_start)
                except KeyError:
                    pass

        cycle._on_connection_disconnect = _on_disconnect  # type: ignore[method-assign]

        def on_poll_start() -> None:
            cycle_poll_start()
            [add_reader(fd, on_readable, fd) for fd in cycle.fds]

        loop.on_tick.add(on_poll_start)

        # Unified requeue check handles both delayed messages and timed-out messages
        loop.call_repeatedly(DEFAULT_REQUEUE_CHECK_INTERVAL, cycle.maybe_requeue_messages)

        # Heartbeat to keep in-flight messages alive
        visibility_timeout = connection.client.transport_options.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)  # type: ignore[attr-defined]
        loop.call_repeatedly(visibility_timeout / 3, cycle.maybe_update_messages_index)

    def on_readable(self, fileno: int) -> Any:
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)
