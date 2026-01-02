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
    DEFAULT_DELAYED_CHECK_INTERVAL,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_VISIBILITY_TIMEOUT,
    DELAY_HEADER,
    DELAYED_QUEUE_SUFFIX,
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


class MutexHeld(Exception):
    """Raised when another party holds the lock."""


@contextmanager
def Mutex(client: Any, name: str, expire: int):
    """Acquire redis lock in non blocking way.

    Raise MutexHeld if not successful.
    """
    lock = client.lock(name, timeout=expire)
    lock_acquired = False
    try:
        lock_acquired = lock.acquire(blocking=False)
        if lock_acquired:
            yield
        else:
            raise MutexHeld
    finally:
        if lock_acquired:
            try:
                lock.release()
            except redis.exceptions.LockNotOwnedError:
                # Lock expired
                pass


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
        self._vrestore_count = 0
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
        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(self.messages_index_key, delivery_tag).hdel(self.messages_key, delivery_tag)

    def maybe_update_messages_index(self) -> None:
        """Update scores of delivered messages to current time.

        Acts as a heartbeat to keep messages from being restored by
        restore_visible() while they are still being processed.
        """
        if not self._delivered:
            return
        now = time()
        with self.channel.conn_or_acquire() as client, client.pipeline() as pipe:
            for tag in self._delivered:
                # Skip fanout messages (they don't use the index)
                if tag not in self._fanout_tags:
                    pipe.zadd(self.messages_index_key, {tag: now})
            pipe.execute()

    def restore_visible(self, start: int = 0, num: int = 10, interval: int = 10) -> None:
        """Restore messages that have exceeded visibility timeout."""
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout
            try:
                with Mutex(client, self.messages_mutex_key, self.messages_mutex_expire):
                    visible = client.zrevrangebyscore(
                        self.messages_index_key,
                        ceil,
                        0,
                        start=num and start,
                        num=num,
                        withscores=True,
                    )
                    for tag, _score in visible or []:
                        tag_str = bytes_to_str(tag)
                        # Check if message is still in a queue before restoring
                        payload = client.hget(self.messages_key, tag_str)
                        if not payload:
                            # Message already acked, remove from index
                            client.zrem(self.messages_index_key, tag_str)
                            continue
                        _message, exchange, routing_key, _priority = loads(bytes_to_str(payload))  # type: ignore[call-arg]
                        # Check if delivery_tag is still in any target queue
                        queues = self.channel._lookup(exchange, routing_key)
                        in_queue = False
                        for queue in queues:
                            if client.zscore(queue, tag_str) is not None:
                                in_queue = True
                                break
                        if in_queue:
                            # Message still in queue, not yet consumed - skip
                            continue
                        # Message was consumed but not acked, restore it
                        self.restore_by_tag(tag_str, client)
            except MutexHeld:
                pass

    def restore_by_tag(self, tag: str, client: Any = None, leftmost: bool = False) -> None:
        def restore_transaction(pipe: Any) -> None:
            p = pipe.hget(self.messages_key, tag)
            pipe.multi()
            if p:
                message, exchange, routing_key, _priority = loads(bytes_to_str(p))  # type: ignore[call-arg]
                self.channel._do_restore_message(message, exchange, routing_key, pipe, leftmost, tag)

        with self.channel.conn_or_acquire(client) as client:
            client.transaction(restore_transaction, self.messages_key)

    @cached_property
    def messages_key(self) -> str:
        return self.channel.messages_key

    @cached_property
    def messages_index_key(self) -> str:
        return self.channel.messages_index_key

    @cached_property
    def messages_mutex_key(self) -> str:
        return self.channel.messages_mutex_key

    @cached_property
    def messages_mutex_expire(self) -> int:
        return self.channel.messages_mutex_expire

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
        for channel in self._channels:
            return channel.qos.restore_visible(num=10)
        return None

    def maybe_restore_messages(self) -> None:
        for channel in self._channels:
            if channel.active_queues:
                return channel.qos.restore_visible(num=10)
        return None

    def maybe_update_messages_index(self) -> None:
        """Update message index scores to keep delivered messages alive."""
        for channel in self._channels:
            if channel.active_queues:
                channel.qos.maybe_update_messages_index()

    def maybe_move_ready_delayed_messages(self) -> int:
        """Move delayed messages that are ready to their destination queues.

        Returns:
            Total number of messages moved across all channels.
        """
        total_moved = 0
        for channel in self._channels:
            if channel.active_queues:
                total_moved += channel.move_ready_delayed_messages()
        return total_moved

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
            self.maybe_restore_messages()
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
    messages_key = "messages"
    messages_index_key = "messages_index"
    messages_mutex_key = "messages_mutex"
    messages_mutex_expire = 300  # 5 minutes

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
        "messages_key",
        "messages_index_key",
        "messages_mutex_key",
        "messages_mutex_expire",
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

    def _do_restore_message(
        self,
        payload: dict[str, Any],
        exchange: str,
        routing_key: str,
        pipe: Any,
        leftmost: bool = False,
        delivery_tag: str | None = None,
    ) -> None:
        try:
            try:
                payload["headers"]["redelivered"] = True
                payload["properties"]["delivery_info"]["redelivered"] = True
            except KeyError:
                pass
            if delivery_tag is None:
                delivery_tag = payload["properties"]["delivery_tag"]
            for queue in self._lookup(exchange, routing_key):
                priority = self._get_message_priority(payload, reverse=False)
                score = 0 if leftmost else _queue_score(priority)
                pipe.zadd(queue, {delivery_tag: score})
                pipe.hset(self.messages_key, delivery_tag, dumps([payload, exchange, routing_key]))  # type: ignore[call-arg]
        except Exception:
            crit("Could not restore message: %r", payload, exc_info=True)

    def _restore(self, message: Any, leftmost: bool = False) -> None:
        tag = message.delivery_tag

        def restore_transaction(pipe: Any) -> None:
            payload = pipe.hget(self.messages_key, tag)
            pipe.multi()
            if payload:
                message, exchange, routing_key, _priority = loads(bytes_to_str(payload))  # type: ignore[call-arg]
                self._do_restore_message(message, exchange, routing_key, pipe, leftmost, tag)

        with self.conn_or_acquire() as client:
            client.transaction(restore_transaction, self.messages_key)

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
                payload = self.client.hget(self.messages_key, delivery_tag)
                if payload:
                    message, _exchange, _routing_key, _priority = loads(bytes_to_str(payload))  # type: ignore[call-arg]
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
                payload = client.hget(self.messages_key, delivery_tag)
                if payload:
                    message, _exchange, _routing_key, _priority = loads(bytes_to_str(payload))  # type: ignore[call-arg]
                    return message
            raise Empty()

    def _size(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            return client.zcard(queue)

    def move_ready_delayed_messages(self) -> int:
        """Move messages from delayed queues to ready queues when their eta has passed.

        For each active queue, checks the corresponding {queue}:delayed sorted set
        for messages with score <= current time. Those messages are moved to the
        main queue with the correct priority+timestamp score.

        Returns:
            Number of messages moved.
        """
        if not self.active_queues:
            return 0

        now = time()
        moved = 0

        with self.conn_or_acquire() as client:
            for queue in list(self.active_queues):
                delayed_queue = queue + DELAYED_QUEUE_SUFFIX

                # Get messages that are ready (score <= now)
                # Use ZRANGEBYSCORE to get messages with score <= current time
                ready_tags = client.zrangebyscore(delayed_queue, "-inf", now, withscores=False)

                if not ready_tags:
                    continue

                with client.pipeline() as pipe:
                    for tag_bytes in ready_tags:
                        tag = bytes_to_str(tag_bytes)

                        # Get message data to extract priority
                        payload = client.hget(self.messages_key, tag)
                        if not payload:
                            # Message was deleted, remove from delayed queue
                            pipe.zrem(delayed_queue, tag)
                            continue

                        _message, _exchange, _routing_key, priority = loads(bytes_to_str(payload))  # type: ignore[call-arg]

                        # Calculate new score with priority and current time
                        queue_score = _queue_score(priority, now)

                        # Move from delayed queue to ready queue
                        pipe.zrem(delayed_queue, tag)
                        pipe.zadd(queue, {tag: queue_score})
                        moved += 1

                    pipe.execute()

        return moved

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        """Deliver message to queue using sorted set.

        Messages with short delay (≤ DEFAULT_DELAYED_CHECK_INTERVAL) go to {queue}
        with a future timestamp in the score. The worker will honor the eta.
        Messages with longer delay go to {queue}:delayed with eta as score,
        and are moved to {queue} by a background process when ready.
        """
        priority = self._get_message_priority(message, reverse=False)
        props = message["properties"]
        delivery_tag = props["delivery_tag"]
        delivery_info = props["delivery_info"]
        exchange = delivery_info["exchange"]
        routing_key = delivery_info["routing_key"]

        # Check for delay header
        headers = props.get("headers", {})
        delay_seconds = headers.get(DELAY_HEADER, 0.0)
        if delay_seconds is None or delay_seconds < 0:
            delay_seconds = 0.0

        now = time()

        with self.conn_or_acquire() as client, client.pipeline() as pipe:
            if delay_seconds > DEFAULT_DELAYED_CHECK_INTERVAL:
                # Long delay: store in {queue}:delayed with eta timestamp as score
                # Background process will move it to main queue when ready
                eta_timestamp = now + delay_seconds
                delayed_queue = queue + DELAYED_QUEUE_SUFFIX
                # Store message with priority info for later retrieval
                pipe.hset(self.messages_key, delivery_tag, dumps([message, exchange, routing_key, priority]))  # type: ignore[call-arg]
                pipe.zadd(self.messages_index_key, {delivery_tag: now})
                pipe.zadd(delayed_queue, {delivery_tag: eta_timestamp})
            else:
                # Immediate or short delay: store in queue with priority+timestamp score
                # For short delays, worker will honor the eta when it receives the message
                queue_score = _queue_score(priority, now + delay_seconds)
                pipe.hset(self.messages_key, delivery_tag, dumps([message, exchange, routing_key, priority]))  # type: ignore[call-arg]
                pipe.zadd(self.messages_index_key, {delivery_tag: now})
                pipe.zadd(queue, {delivery_tag: queue_score})
            pipe.execute()

    def _put_fanout(self, exchange: str, message: dict[str, Any], routing_key: str, **kwargs: Any) -> None:
        """Deliver fanout message using Redis Streams."""
        import uuid

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

    polling_interval = 1  # Timeout for blocking BZMPOP/XREADGROUP calls in seconds
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

        # Register signal handlers on first transport init
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
        loop.call_repeatedly(10, cycle.maybe_restore_messages)

        visibility_timeout = connection.client.transport_options.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)  # type: ignore[attr-defined]
        loop.call_repeatedly(visibility_timeout / 3, cycle.maybe_update_messages_index)

        # Check for ready delayed messages every DEFAULT_DELAYED_CHECK_INTERVAL seconds
        loop.call_repeatedly(DEFAULT_DELAYED_CHECK_INTERVAL, cycle.maybe_move_ready_delayed_messages)

    def on_readable(self, fileno: int) -> Any:
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)

    def setup_native_delayed_delivery(self, connection: Connection, queues: list[str]) -> None:
        """Set up native delayed delivery (background processing via event loop callback)."""
        # Delayed delivery is now handled by cycle.maybe_move_ready_delayed_messages
        # which is registered in register_with_event_loop

    def teardown_native_delayed_delivery(self) -> None:
        """Tear down native delayed delivery (no-op, event loop handles cleanup)."""
