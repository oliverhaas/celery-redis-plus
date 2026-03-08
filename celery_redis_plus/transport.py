"""Enhanced Redis/Valkey transport with BZMPOP priority queues, Streams fanout, and native delayed delivery.

This transport provides three key improvements over the standard Redis transport:
1. BZMPOP + sorted sets for regular queues - enables full 0-255 priority support and better reliability
2. Redis Streams for fanout exchanges - reliable broadcast via XREAD instead of lossy PUB/SUB
3. Native delayed delivery - delay integrated into sorted set score calculation

Requires Redis 7.0+ (or Valkey) for BZMPOP support.
Supports both redis-py and valkey-py client libraries.

Configuration
=============
For Valkey, use the ``valkey://`` URL scheme directly::

    broker_url = "valkey://localhost:6379/0"

For Redis, set ``broker_transport`` with a standard ``redis://`` URL::

    broker_url = "redis://localhost:6379/0"
    broker_transport = "celery_redis_plus.transport:Transport"

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
* ``credential_provider``: A ``redis.credentials.CredentialProvider`` instance (or dotted
  import path string) for dynamic auth (e.g., AWS ElastiCache IAM, Azure Redis).
  Mutually exclusive with username/password in the broker URL.
"""

from __future__ import annotations

import functools
import numbers
import socket as socket_module
from contextlib import contextmanager, suppress
from pathlib import Path
from queue import Empty
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

import logging

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.transport import virtual
from kombu.transport.base import to_rabbitmq_queue_arguments  # type: ignore[attr-defined]
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.functional import accepts_argument
from kombu.utils.imports import symbol_by_name
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.url import _parse_url
from vine import promise

from .constants import (
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_MAX_RESTORE_COUNT,
    DEFAULT_MESSAGE_TTL,
    DEFAULT_REQUEUE_BATCH_LIMIT,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_VISIBILITY_TIMEOUT,
    MAX_PRIORITY,
    MESSAGE_KEY_PREFIX,
    MESSAGES_INDEX_PREFIX,
    MIN_PRIORITY,
    MIN_QUEUE_EXPIRES,
    PRIORITY_SCORE_MULTIPLIER,
    QUEUE_KEY_PREFIX,
)

if TYPE_CHECKING:
    from kombu import Connection

# Try to import redis-py or valkey-py (both have compatible APIs)
# Prefer redis-py if both are installed
client_lib: Any
_client_lib_name: str

try:
    import redis as client_lib

    _client_lib_name = "redis"
except ImportError:  # pragma: no cover
    try:
        import valkey as client_lib

        _client_lib_name = "valkey"
    except ImportError:
        raise ImportError(
            "celery-redis-plus requires either redis-py or valkey-py to be installed. "
            "Install with: pip install celery-redis-plus[redis] or pip install celery-redis-plus[valkey]",
        ) from None

# Import CredentialProvider from whichever client library is installed
CredentialProvider: type | None = getattr(
    getattr(client_lib, "credentials", None),
    "CredentialProvider",
    None,
)

# Exception classes (compatible between redis-py and valkey-py)
_client_exceptions = client_lib.exceptions
_DataError = getattr(_client_exceptions, "InvalidData", _client_exceptions.DataError)

_connection_errors = virtual.Transport.connection_errors + (
    InconsistencyError,
    socket_module.error,
    OSError,
    _client_exceptions.ConnectionError,
    _client_exceptions.BusyLoadingError,
    _client_exceptions.AuthenticationError,
    _client_exceptions.TimeoutError,
)
_channel_errors = virtual.Transport.channel_errors + (
    _DataError,
    _client_exceptions.InvalidResponse,
    _client_exceptions.ResponseError,
)

logger = logging.getLogger(__name__)


DEFAULT_PORT = 6379
DEFAULT_DB = 0

# Load Lua scripts at module init
_PACKAGE_DIR = Path(__file__).parent
_ENQUEUE_DUE_MESSAGES_LUA = (_PACKAGE_DIR / "transport_enqueue_due_messages.lua").read_text()
_REQUEUE_MESSAGE_LUA = (_PACKAGE_DIR / "transport_requeue_message.lua").read_text()
_CONSUME_MESSAGE_LUA = (_PACKAGE_DIR / "transport_consume_message.lua").read_text()


_warned_priority_clamp = False


def _queue_score(priority: int, timestamp: float | None = None) -> float:
    """Compute sorted set score for queue ordering.

    Higher priority number = higher priority = lower score = popped first.
    This matches RabbitMQ semantics where priority 255 is highest, 0 is lowest.
    Within same priority, earlier timestamp = lower score = popped first (FIFO).

    Args:
        priority: Message priority (0-255, higher is higher priority, matching RabbitMQ).
            Values outside this range are clamped with a warning.
        timestamp: Unix timestamp in seconds (defaults to current time)

    Returns:
        Float score for ZADD
    """
    global _warned_priority_clamp  # noqa: PLW0603
    if timestamp is None:
        timestamp = time()
    # Clamp priority to valid range (0-255)
    if priority < MIN_PRIORITY or priority > MAX_PRIORITY:
        if not _warned_priority_clamp:
            logger.warning(
                "Priority %d out of range (%d-%d), clamping to valid range."
                " This warning is shown once; other messages may also be affected.",
                priority,
                MIN_PRIORITY,
                MAX_PRIORITY,
            )
            _warned_priority_clamp = True
        priority = max(MIN_PRIORITY, min(MAX_PRIORITY, priority))
    # Invert priority so higher priority number = lower score = popped first
    # Multiply by large factor to leave room for millisecond timestamps
    return (MAX_PRIORITY - priority) * PRIORITY_SCORE_MULTIPLIER + int(timestamp * 1000)


def _after_fork_cleanup_channel(channel: Channel) -> None:
    channel._after_fork()


class GlobalKeyPrefixMixin:
    """Mixin to provide common logic for global key prefixing.

    Overrides command execution to add prefixes to Redis keys.
    """

    global_keyprefix: str = ""

    PREFIXED_SIMPLE_COMMANDS: ClassVar[list[str]] = [
        "EXISTS",
        "EXPIRE",
        "HDEL",
        "HGET",
        "HMGET",
        "HSET",
        "PEXPIRE",
        "PTTL",
        "SADD",
        "SREM",
        "SMEMBERS",
        "TTL",
        "ZADD",
        "ZCARD",
        "ZPOPMIN",
        "ZRANGE",
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

    PREFIXED_COMPLEX_COMMANDS: ClassVar[dict[str, dict[str, int | None] | Callable[..., list[Any]]]] = {
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
                args = cast("Callable[..., list[Any]]", spec)(args, self.global_keyprefix)
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


class PrefixedStrictRedis(GlobalKeyPrefixMixin, client_lib.Redis):
    """Redis/Valkey client that prefixes all keys."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.global_keyprefix = kwargs.pop("global_keyprefix", "")
        client_lib.Redis.__init__(self, *args, **kwargs)


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, client_lib.client.Pipeline):
    """Redis/Valkey pipeline that prefixes all keys."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.global_keyprefix = kwargs.pop("global_keyprefix", "")
        client_lib.client.Pipeline.__init__(self, *args, **kwargs)


class QoS(virtual.QoS):
    """Redis QoS with sorted set based message tracking.

    Messages are stored in a hash at publish time with visibility tracking
    in a separate sorted set. This allows recovery of messages from crashed
    workers based on their index scores. The base class append() is sufficient
    since messages are already persisted in Redis at publish time.
    """

    channel: Channel  # Narrow type from base class for our custom Channel
    restore_at_shutdown = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # For streams fanout: track delivery tags that came from fanout (no ack needed)
        self._fanout_tags: set[str] = set()

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
                queue = cast("dict", self._delivered)[delivery_tag].delivery_info["routing_key"]
                self.requeue_by_tag(delivery_tag, queue=queue, leftmost=True)
            else:
                self._remove_from_indices(delivery_tag).execute()
            super().ack(delivery_tag)

    @contextmanager
    def pipe_or_acquire(self, pipe: Any = None, client: Any = None) -> Generator[Any]:
        if pipe:
            yield pipe
        else:
            with self.channel.conn_or_acquire(client) as client:
                yield client.pipeline()

    def _remove_from_indices(self, delivery_tag: str, pipe: Any = None) -> Any:
        message_key = self.channel._message_key(delivery_tag)
        queue = cast("dict", self._delivered)[delivery_tag].delivery_info["routing_key"]
        index_key = self.channel._messages_index_key(queue)
        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(index_key, delivery_tag).delete(message_key)

    def maybe_update_messages_index(self) -> None:
        """Update scores of delivered messages to now + visibility_timeout.

        Acts as a heartbeat to keep messages from being enqueued by
        enqueue_due_messages() while they are still being processed.

        Uses ZADD XX to only update existing entries, avoiding race conditions
        where a message is acked (removed from index) between checking
        _delivered and executing ZADD.
        """
        if not self._delivered:
            return
        try:
            queue_at = time() + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL
            with self.channel.conn_or_acquire() as client, client.pipeline() as pipe:
                for tag, message in self._delivered.items():
                    # Skip fanout messages (they don't use the index)
                    if tag not in self._fanout_tags:
                        queue = message.delivery_info["routing_key"]
                        index_key = self.channel._messages_index_key(queue)
                        # XX = only update if member already exists (prevents re-adding acked messages)
                        pipe.zadd(index_key, {tag: queue_at}, xx=True)
                pipe.execute()
        except Exception:
            logger.warning("Failed to update messages index, will retry next cycle", exc_info=True)

    def enqueue_due_messages(self) -> int:
        """Enqueue messages due before the next requeue cycle.

        This unified method handles both:
        - Delayed messages that are now ready to be processed (first delivery)
        - Messages that were consumed but not acked (redelivery)

        Uses a Lua script for atomic, efficient batch processing.

        Returns:
            Number of messages enqueued.
        """
        return self.channel.enqueue_due_messages()

    def requeue_by_tag(
        self,
        tag: str,
        client: Any = None,
        queue: str | None = None,
        leftmost: bool = False,
    ) -> None:
        """Requeue a rejected message by its delivery tag using Lua script.

        The Lua script atomically reads the routing_key (queue) from the message
        hash and adds the message back to that queue.

        Args:
            tag: The message's delivery tag.
            client: Optional Redis client (unused, kept for API compatibility).
            queue: Queue name for per-queue message TTL lookup.
            leftmost: If True, requeue to front of queue (score=0).
        """
        self.channel._requeue_by_tag(tag, queue=queue, leftmost=leftmost)

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
        self._loop: Any = None
        self._expires_timer_entry: Any = None
        self._expires_timer_interval: float | None = None

    def close(self) -> None:
        for fd in self._chan_to_sock.values():
            with suppress(KeyError, ValueError):
                self.poller.unregister(fd)
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def add(self, channel: Channel) -> None:
        self._channels.add(channel)

    def discard(self, channel: Channel) -> None:
        self._channels.discard(channel)

    def _on_connection_disconnect(self, connection: Any) -> None:
        with suppress(AttributeError, TypeError):
            self.poller.unregister(connection._sock)

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
            client.connection = client.connection_pool.get_connection()
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
        ident = channel, channel.subclient, "XREAD"
        if not self._client_registered(channel, channel.subclient, "XREAD"):
            channel._in_fanout_poll = False
            self._register(*ident)
        if not channel._in_fanout_poll:
            channel._xread_start()

    def on_poll_start(self) -> None:
        for channel in self._channels:
            qos = channel.qos
            if qos is not None and channel.active_queues and qos.can_consume():
                self._register_BZMPOP(channel)
            if qos is not None and channel.active_fanout_queues and qos.can_consume():
                self._register_XREAD(channel)

    def on_poll_init(self, poller: Any) -> None:
        self.poller = poller
        # Initial enqueue check on startup
        self.maybe_enqueue_due_messages()

    def maybe_enqueue_due_messages(self) -> int:
        """Enqueue messages due before the next requeue cycle.

        This unified method handles both:
        - Delayed messages ready for first delivery
        - Timed-out messages that need redelivery

        Returns:
            Total number of messages enqueued across all channels.
        """
        total_enqueued = 0
        for channel in self._channels:
            qos = channel.qos
            if qos is not None and channel.active_queues:
                total_enqueued += cast("QoS", qos).enqueue_due_messages()
        return total_enqueued

    def maybe_update_messages_index(self) -> None:
        """Update message index scores to keep delivered messages alive."""
        for channel in self._channels:
            qos = channel.qos
            if qos is not None and channel.active_queues:
                cast("QoS", qos).maybe_update_messages_index()

    def maybe_refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on queue keys with x-expires TTL."""
        for channel in self._channels:
            channel._refresh_queue_expires()

    def _update_expires_timer(self) -> None:
        """Register or update the periodic PEXPIRE timer based on configured TTLs.

        Interval = min(all configured x-expires) / 2, so the TTL is refreshed
        ~2 times before it would expire.
        """
        min_ttl_ms: int | None = None
        for channel in self._channels:
            for ttl_ms in channel._expires.values():
                if min_ttl_ms is None or ttl_ms < min_ttl_ms:
                    min_ttl_ms = ttl_ms

        if min_ttl_ms is None:
            if self._expires_timer_entry is not None:
                self._expires_timer_entry.cancel()
                self._expires_timer_entry = None
                self._expires_timer_interval = None
            return

        interval = min_ttl_ms / 2 / 1000  # ms → seconds, divided by 2

        if self._expires_timer_interval == interval:
            return

        if self._expires_timer_entry is not None:
            self._expires_timer_entry.cancel()

        if self._loop is not None:
            self._expires_timer_entry = self._loop.call_repeatedly(
                interval,
                self.maybe_refresh_queue_expires,
            )
            self._expires_timer_interval = interval

    def on_readable(self, fileno: int) -> bool | None:
        chan, cmd_type = self._fd_to_chan[fileno]
        qos = chan.qos
        if qos is not None and qos.can_consume():
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
                qos = channel.qos
                if qos is not None and channel.active_queues and qos.can_consume():
                    self._register_BZMPOP(channel)
                if qos is not None and channel.active_fanout_queues and qos.can_consume():
                    self._register_XREAD(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            raise Empty
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
    - Redis Streams for fanout (true broadcast via XREAD)
    - Native delayed delivery via score calculation
    """

    QoS = QoS
    # qos is inherited from base class and will be an instance of our QoS
    connection: Transport  # Narrow type from base class for our custom Transport

    _client: Any = None
    supports_fanout = True
    keyprefix_queue = "_kombu.binding.%s"
    keyprefix_fanout = "/{db}."
    sep = "\x06\x16"
    _in_poll = None
    _in_fanout_poll = None
    _warned_expires_clamp = False
    max_priority = MAX_PRIORITY  # Override kombu's default of 9 to enable full 0-255 range

    # Message storage keys
    # Per-message hash keys use format: {message_key_prefix}{delivery_tag}
    message_key_prefix = MESSAGE_KEY_PREFIX
    message_ttl = DEFAULT_MESSAGE_TTL  # TTL for per-message hashes (-1 = no TTL)

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

    # Credential provider for dynamic auth (e.g. AWS ElastiCache IAM, Azure Redis)
    credential_provider = None

    # Max restore count (None = no limit)
    max_restore_count: int | None = DEFAULT_MAX_RESTORE_COUNT

    # Fanout settings
    fanout_prefix: bool | str = True
    fanout_patterns = True

    _async_pool: Any = None
    _pool: Any = None

    from_transport_options = virtual.Channel.from_transport_options + (
        "sep",
        "message_key_prefix",
        "message_ttl",
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
        "credential_provider",
        "max_restore_count",
    )

    connection_class = client_lib.Connection
    connection_class_ssl = client_lib.SSLConnection

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._registered = False
        self._queue_cycle: list[str] = []
        self.Client = self._get_client()
        self.ResponseError = _client_exceptions.ResponseError
        self.active_fanout_queues: set[str] = set()
        self.auto_delete_queues: set[str] = set()
        self._fanout_queues: dict[str, tuple[str, str]] = {}
        self.handlers = {"BZMPOP": self._bzmpop_read, "XREAD": self._xread_read}
        # Track last-read stream ID per stream for fanout (start with $ = only new messages)
        self._stream_offsets: dict[str, str] = {}
        # Per-queue TTL state from x-expires and x-message-ttl queue arguments
        self._expires: dict[str, int] = {}  # queue_name → TTL in ms
        self._message_ttls: dict[str, int] = {}  # queue_name → message TTL in ms
        # FAST/SLOW consume mode: FAST uses atomic Lua ZPOPMIN, SLOW uses blocking BZMPOP
        self._consume_fast_mode: bool = True
        self._consume_script_sha: str | None = None

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
            self._in_poll = None
        if self._in_fanout_poll is connection:
            self._in_fanout_poll = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _message_key(self, delivery_tag: str) -> str:
        """Get the Redis key for a message's per-message hash."""
        return f"{self.message_key_prefix}{delivery_tag}"

    def _queue_key(self, queue: str) -> str:
        """Get the Redis key for a queue's sorted set.

        Uses 'queue:' prefix to avoid collision with list-based queues
        from the standard redis transport.
        """
        return f"{QUEUE_KEY_PREFIX}{queue}"

    def _queue_name(self, queue_key: str) -> str:
        """Extract logical queue name from a Redis queue key.

        Strips the 'queue:' prefix if present.
        """
        if queue_key.startswith(QUEUE_KEY_PREFIX):
            return queue_key[len(QUEUE_KEY_PREFIX) :]
        return queue_key

    def _messages_index_key(self, queue: str) -> str:
        """Get the Redis key for a queue's messages index sorted set."""
        return f"{MESSAGES_INDEX_PREFIX}{queue}"

    def _get_message_from_hash(self, message_key: str, client: Any) -> dict[str, Any] | None:
        """Fetch message payload from per-message hash.

        Args:
            message_key: The Redis key for the message hash.
            client: Redis client to use.

        Returns:
            The message dict, or None if not found.
        """
        fields = client.hmget(message_key, "payload", "restore_count")
        payload_json = fields[0]
        if not payload_json:
            return None
        result: dict[str, Any] | None = loads(bytes_to_str(payload_json))
        if result is not None:
            restore_count = int(fields[1] or 0)
            if restore_count > 0:
                headers = result.setdefault("properties", {}).setdefault("headers", {})
                headers["x-restore-count"] = restore_count
        return result

    def _cleanup_expired_message(self, queue: str, delivery_tag: str, client: Any | None = None) -> None:
        """Remove messages_index entry for a message whose hash has expired."""
        if client is None:
            with self.conn_or_acquire() as client:
                client.zrem(self._messages_index_key(queue), delivery_tag)
        else:
            client.zrem(self._messages_index_key(queue), delivery_tag)

    def _drain_expired_and_deliver(self, queue: str) -> bool:
        """Pop messages from queue until one with a valid hash is found.

        Used after BZMPOP returns an expired message to avoid going back to
        blocking when there are still deliverable messages in the queue.

        Returns:
            True if a message was delivered, raises Empty otherwise.
        """
        queue_key = self._queue_key(queue)
        while True:
            result = self.client.zpopmin(queue_key, count=1)
            if not result:
                raise Empty
            delivery_tag, _score = result[0]
            delivery_tag = bytes_to_str(delivery_tag)
            message_key = self._message_key(delivery_tag)
            message = self._get_message_from_hash(message_key, self.client)
            if message:
                self.connection._deliver(message, queue)
                return True
            self._cleanup_expired_message(queue, delivery_tag, self.client)

    def _restore(self, message: Any, leftmost: bool = False) -> None:
        """Restore a message to its queue.

        This method is called by Kombu's virtual.Channel for message recovery.
        """
        queue = message.delivery_info.get("routing_key")
        self._requeue_by_tag(message.delivery_tag, queue=queue, leftmost=leftmost)

    def _restore_at_beginning(self, message: Any) -> None:
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue: str, *args: Any, **kwargs: Any) -> str:
        if queue in self._fanout_queues:
            self.active_fanout_queues.add(queue)
        ret = super().basic_consume(queue, *args, **kwargs)
        self._queue_cycle = list(self.active_queues)
        return ret

    def basic_cancel(self, consumer_tag: str) -> Any:
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
                return connection.cycle.after_read.add(promise(self._basic_cancel, (consumer_tag,)))
            return self._basic_cancel(consumer_tag)
        return None

    def _basic_cancel(self, consumer_tag: str) -> Any:
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return None
        with suppress(KeyError):
            self.active_fanout_queues.remove(queue)
        ret = super().basic_cancel(consumer_tag)
        self._queue_cycle = list(self.active_queues)
        return ret

    # --- BZMPOP (sorted set) methods for regular queues ---

    def _ensure_consume_script_sha(self) -> str:
        """Load and cache the consume_message Lua script SHA."""
        if self._consume_script_sha is None:
            self._consume_script_sha = self.client.script_load(_CONSUME_MESSAGE_LUA)
        return self._consume_script_sha

    def _parse_consume_result(self, result: list[Any]) -> tuple[str, dict[str, Any]]:
        """Parse the result from consume_message Lua script.

        Returns:
            Tuple of (queue_name, message_dict).
        """
        queue_name = bytes_to_str(result[0])
        payload_json = result[2]
        restore_count = int(result[3] or 0)
        message: dict[str, Any] = loads(bytes_to_str(payload_json))
        if restore_count > 0:
            headers = message.setdefault("properties", {}).setdefault("headers", {})
            headers["x-restore-count"] = restore_count
        return queue_name, message

    def _bzmpop_start(self, timeout: float | None = None) -> None:
        if timeout is None:
            timeout = self.connection.polling_interval or 1
        if not self._queue_cycle:
            return

        if self._consume_fast_mode:
            # FAST mode: send non-blocking EVALSHA (atomic ZPOPMIN + ZADD + HMGET)
            sha = self._ensure_consume_script_sha()
            keys = [f"{self.global_keyprefix}{self._queue_key(q)}" for q in self._queue_cycle]
            new_queue_at = time() + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL
            args = [
                self.global_keyprefix,
                self.message_key_prefix,
                str(new_queue_at),
                MESSAGES_INDEX_PREFIX,
                *self._queue_cycle,
            ]
            self.client.connection.send_command(
                "EVALSHA",
                sha,
                len(keys),
                *keys,
                *args,
            )
        else:
            # SLOW mode: send blocking BZMPOP
            keys = [self._queue_key(q) for q in self._queue_cycle]
            command_args: list[Any] = ["BZMPOP", timeout, len(keys), *keys, "MIN"]
            if self.global_keyprefix:
                command_args = self.client._prefix_args(command_args)
            self.client.connection.send_command(*command_args)

        self._in_poll = self.client.connection

    def _bzmpop_read(self, **options: Any) -> bool:
        if self._consume_fast_mode:
            return self._fast_consume_read(**options)
        return self._slow_consume_read(**options)

    def _fast_consume_read(self, **options: Any) -> bool:
        """Parse EVALSHA response from atomic Lua consume script.

        On success: delivers message, clears _in_poll, returns True.
        On empty: switches to SLOW mode, sends BZMPOP, raises Empty
        (keeps _in_poll set since BZMPOP is now pending).
        """
        try:
            try:
                result = self.client.parse_response(self.client.connection, "EVALSHA", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                self._in_poll = None
                raise
            except self.ResponseError as exc:
                if "NOSCRIPT" in str(exc):
                    # Script evicted from cache, reload on next tick
                    self._consume_script_sha = None
                    self._in_poll = None
                    raise Empty from None
                self._in_poll = None
                raise
        except Empty:
            raise
        except Exception:
            self._in_poll = None
            raise

        if result:
            queue_name, message = self._parse_consume_result(result)
            self._in_poll = None
            self.connection._deliver(message, queue_name)
            return True

        # Queue empty: switch to SLOW mode and send BZMPOP
        self._consume_fast_mode = False
        self._bzmpop_start()  # sends BZMPOP, keeps _in_poll set
        raise Empty

    def _slow_consume_read(self, **options: Any) -> bool:
        """Parse BZMPOP response with pipeline ZADD + HMGET.

        Safe because queue was just confirmed empty by FAST mode — any message
        BZMPOP returns was published after that, with queue_at far in the future.
        """
        try:
            try:
                result = self.client.parse_response(self.client.connection, "BZMPOP", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                raise
            if result:
                dest, members = result
                dest = bytes_to_str(dest)
                # Strip queue: prefix to get logical queue name for delivery
                dest = self._queue_name(dest)
                delivery_tag, _score = members[0]
                delivery_tag = bytes_to_str(delivery_tag)

                # Pipeline ZADD (refresh index) + HMGET (fetch message)
                index_key = self._messages_index_key(dest)
                message_key = self._message_key(delivery_tag)
                new_queue_at = time() + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL
                with self.client.pipeline(transaction=False) as pipe:
                    pipe.zadd(index_key, {delivery_tag: new_queue_at}, xx=True)
                    pipe.hmget(message_key, "payload", "restore_count")
                    results = pipe.execute()

                payload_json = results[1][0]
                if payload_json:
                    message: dict[str, Any] = loads(bytes_to_str(payload_json))
                    restore_count = int(results[1][1] or 0)
                    if restore_count > 0:
                        headers = message.setdefault("properties", {}).setdefault("headers", {})
                        headers["x-restore-count"] = restore_count
                    self._consume_fast_mode = True  # Switch back to FAST
                    self.connection._deliver(message, dest)
                    return True
                # Message hash expired (x-message-ttl) — clean up index
                self._cleanup_expired_message(dest, delivery_tag, self.client)
                # Try remaining messages synchronously via zpopmin
                return self._drain_expired_and_deliver(dest)
            raise Empty
        finally:
            self._in_poll = None

    # --- XREAD (Streams) methods for fanout ---

    def _fanout_stream_key(self, exchange: str) -> str:
        """Get stream key for fanout exchange.

        Fanout exchanges use a single stream per exchange (routing key is ignored).
        This is correct because fanout semantics deliver every message to every consumer,
        and XREAD does not support wildcard stream names.
        """
        return f"{self.keyprefix_fanout}{exchange}"

    def _xread_start(self, timeout: float | None = None) -> None:
        """Start XREAD for fanout streams (true broadcast - every consumer gets every message)."""
        if timeout is None:
            timeout = self.connection.polling_interval or 1

        streams: dict[str, str] = {}

        for queue in self.active_fanout_queues:
            if queue in self._fanout_queues:
                exchange, _routing_key = self._fanout_queues[queue]
                stream_key = self._fanout_stream_key(exchange)
                # Use stored offset or "$" for only new messages
                offset = self._stream_offsets.get(stream_key, "$")
                streams[stream_key] = offset

        if not streams:
            return

        self._in_fanout_poll = self.subclient.connection

        # Build XREAD command
        stream_keys = list(streams.keys())
        stream_ids = [streams[k] for k in stream_keys]

        command_args: list[Any] = [
            "XREAD",
            "COUNT",
            "1",
            "BLOCK",
            str(int(timeout * 1000)),
            "STREAMS",
            *stream_keys,
            *stream_ids,
        ]

        if self.global_keyprefix:
            command_args = self.subclient._prefix_args(command_args)

        self.subclient.connection.send_command(*command_args)

    def _xread_read(self, **options: Any) -> bool:
        """Read messages from XREAD (fanout broadcast)."""
        try:
            try:
                messages = self.subclient.parse_response(self.subclient.connection, "XREAD", **options)
            except self.connection_errors:
                self.subclient.connection.disconnect()
                raise

            if not messages:
                raise Empty

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
                    for queue, (exchange, _routing_key) in self._fanout_queues.items():
                        if offset_key == self._fanout_stream_key(exchange):
                            queue_name = queue
                            break

                    if not queue_name:
                        continue

                    # Parse payload
                    payload_field = fields.get(b"payload") or fields.get("payload")
                    if not payload_field:
                        continue
                    payload = loads(bytes_to_str(payload_field))

                    # Set delivery tag
                    delivery_tag = self._next_delivery_tag()
                    payload["properties"]["delivery_tag"] = delivery_tag

                    # Mark as fanout message (no ack needed)
                    if self.qos is not None:
                        cast("QoS", self.qos)._fanout_tags.add(delivery_tag)

                    # Deliver message
                    self.connection._deliver(payload, queue_name)
                    return True

            raise Empty
        finally:
            self._in_fanout_poll = None

    def _poll_error(self, cmd_type: str, **options: Any) -> Any:
        if cmd_type == "XREAD":
            client = self.subclient
        else:
            client = self.client
            # In FAST mode the pending command is EVALSHA, not BZMPOP
            if self._consume_fast_mode:
                cmd_type = "EVALSHA"
        return client.parse_response(client.connection, cmd_type)

    def _get(self, queue: str, timeout: float | None = None) -> dict[str, Any]:
        """Get single message from queue (synchronous).

        Uses the atomic consume Lua script (ZPOPMIN + ZADD index + HMGET).
        """
        with self.conn_or_acquire() as client:
            consume_script = client.register_script(_CONSUME_MESSAGE_LUA)
            queue_key = f"{self.global_keyprefix}{self._queue_key(queue)}"
            new_queue_at = time() + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL
            result = consume_script(
                keys=[queue_key],
                args=[
                    self.global_keyprefix,
                    self.message_key_prefix,
                    str(new_queue_at),
                    MESSAGES_INDEX_PREFIX,
                    queue,
                ],
            )
            if not result:
                raise Empty
            _, message = self._parse_consume_result(result)
            return message

    def _size(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            return int(client.zcard(self._queue_key(queue)))

    def enqueue_due_messages(self) -> int:
        """Enqueue messages due before the next requeue cycle.

        This unified method handles both:
        - Delayed messages that are now ready to be processed (first delivery)
        - Messages that were consumed but not acked (redelivery)

        Iterates over each active queue's per-queue messages index and runs
        a Lua script that atomically moves due messages into the queue.

        Returns:
            Number of messages enqueued.
        """
        if not self._queue_cycle:
            return 0

        now = time()
        threshold = now + DEFAULT_REQUEUE_CHECK_INTERVAL
        total_enqueued = 0

        max_restore = -1 if self.max_restore_count is None else self.max_restore_count

        with self.conn_or_acquire() as client:
            enqueue_script = client.register_script(_ENQUEUE_DUE_MESSAGES_LUA)

            for queue in self._queue_cycle:
                try:
                    # Pass prefixed key since EVALSHA doesn't auto-prefix KEYS
                    index_key = f"{self.global_keyprefix}{self._messages_index_key(queue)}"
                    result = enqueue_script(
                        keys=[index_key],
                        args=[
                            threshold,
                            DEFAULT_REQUEUE_BATCH_LIMIT,
                            self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL,
                            PRIORITY_SCORE_MULTIPLIER,
                            self.message_key_prefix,
                            self.global_keyprefix,
                            QUEUE_KEY_PREFIX,
                            max_restore,
                        ],
                    )
                    # Lua returns [enqueued_count, dropped_count]
                    enqueued = result[0] if result else 0
                    dropped = result[1] if result and len(result) > 1 else 0
                    if dropped:
                        logger.error(
                            "Queue %s: %d message(s) dropped after exceeding max restore count of %d.",
                            queue,
                            dropped,
                            self.max_restore_count,
                        )
                    if enqueued >= DEFAULT_REQUEUE_BATCH_LIMIT:
                        logger.warning(
                            "Queue %s hit enqueue batch limit of %d. There may be more messages waiting.",
                            queue,
                            DEFAULT_REQUEUE_BATCH_LIMIT,
                        )
                    total_enqueued += enqueued
                except Exception:
                    logger.warning(
                        "Failed to enqueue due messages for queue %s, will retry next cycle",
                        queue,
                        exc_info=True,
                    )

        return total_enqueued

    def _requeue_by_tag(self, delivery_tag: str, queue: str | None = None, leftmost: bool = False) -> bool:
        """Requeue a rejected message to its queue using Lua script.

        The Lua script atomically reads the routing_key (queue) from the message
        hash and adds the message back to that queue. Sets the redelivered flag.

        Args:
            delivery_tag: The message's delivery tag.
            queue: Queue name for per-queue message TTL lookup.
            leftmost: If True, requeue to front of queue (score=0).

        Returns:
            True if message was requeued, False if not found.
        """
        # Prefix key since EVALSHA doesn't auto-prefix KEYS
        message_key = f"{self.global_keyprefix}{self._message_key(delivery_tag)}"

        # Compute effective message TTL (respect per-queue x-message-ttl)
        effective_ttl = self.message_ttl
        if queue and queue in self._message_ttls:
            queue_ttl_s = max(1, -(-self._message_ttls[queue] // 1000))
            effective_ttl = queue_ttl_s if effective_ttl < 0 else min(effective_ttl, queue_ttl_s)

        with self.conn_or_acquire() as client:
            requeue_script = client.register_script(_REQUEUE_MESSAGE_LUA)
            result = requeue_script(
                keys=[message_key],
                args=[
                    1 if leftmost else 0,
                    PRIORITY_SCORE_MULTIPLIER,
                    effective_ttl,
                    self.global_keyprefix,
                    QUEUE_KEY_PREFIX,
                    self.message_key_prefix,
                    self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL,
                    MESSAGES_INDEX_PREFIX,
                ],
            )
            return bool(result)

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        """Store message hash and add to queue and messages_index.

        Immediate messages go to the queue sorted set with a score encoding priority
        and timestamp. Native delayed messages (delay > requeue check interval) go
        only to messages_index and are moved to the queue when due.
        The messages_index tracks when to attempt (re)queue if the message is not
        acknowledged (queue_at = eta for delayed, now + VT + RCI for immediate).

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
        # For immediate messages: queue_at = now + VT + RCI (requeue if not acked;
        #   +RCI compensates for the look-ahead threshold in enqueue_due_messages)
        queue_at = (
            eta_timestamp if is_native_delayed else now + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL
        )

        message_key = self._message_key(delivery_tag)

        with self.conn_or_acquire() as client, client.pipeline() as pipe:
            # Store message in per-message hash with individual fields
            # routing_key is used as the queue name for restore operations
            pipe.hset(
                message_key,
                mapping={
                    "payload": dumps(message),
                    "routing_key": queue,
                    "priority": priority,
                    "redelivered": 0,
                    "native_delayed": 1 if is_native_delayed else 0,
                    "eta": eta_timestamp or 0,
                    "restore_count": 0,
                },
            )
            effective_message_ttl = self.message_ttl
            if queue in self._message_ttls:
                queue_ttl_s = max(1, -(-self._message_ttls[queue] // 1000))
                if effective_message_ttl < 0:
                    effective_message_ttl = queue_ttl_s
                else:
                    effective_message_ttl = min(effective_message_ttl, queue_ttl_s)
            if effective_message_ttl >= 0:
                pipe.expire(message_key, max(1, effective_message_ttl))
            pipe.zadd(self._messages_index_key(queue), {delivery_tag: queue_at})
            if not is_native_delayed:
                pipe.zadd(self._queue_key(queue), {delivery_tag: queue_score})
            if queue in self._expires:
                ttl_ms = self._expires[queue]
                pipe.pexpire(self._queue_key(queue), ttl_ms)
                pipe.pexpire(self._messages_index_key(queue), ttl_ms)
            pipe.execute()

    def _put_fanout(self, exchange: str, message: dict[str, Any], routing_key: str, **kwargs: Any) -> None:
        """Deliver fanout message using Redis Streams."""
        stream_key = self._fanout_stream_key(exchange)

        with self.conn_or_acquire() as client:
            client.xadd(
                name=stream_key,
                fields={"payload": dumps(message)},
                id="*",
                maxlen=self.stream_maxlen,
                approximate=True,
            )

    def prepare_queue_arguments(self, arguments: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any] | None:
        return to_rabbitmq_queue_arguments(arguments, **kwargs)

    def _new_queue(self, queue: str, auto_delete: bool = False, **kwargs: Any) -> None:
        if auto_delete:
            self.auto_delete_queues.add(queue)
        arguments = kwargs.get("arguments") or {}
        x_expires = arguments.get("x-expires")
        if x_expires is not None and queue not in self._expires:
            x_expires = int(x_expires)
            if x_expires < MIN_QUEUE_EXPIRES:
                if not self._warned_expires_clamp:
                    logger.warning(
                        "x-expires %dms is below minimum %dms, clamping."
                        " This warning is shown once; other queues may also be affected.",
                        x_expires,
                        MIN_QUEUE_EXPIRES,
                    )
                    Channel._warned_expires_clamp = True
                x_expires = MIN_QUEUE_EXPIRES
            self._expires[queue] = x_expires
            self.connection.cycle._update_expires_timer()
        x_message_ttl = arguments.get("x-message-ttl")
        if x_message_ttl is not None and queue not in self._message_ttls:
            self._message_ttls[queue] = int(x_message_ttl)

    def _queue_bind(self, exchange: str, routing_key: str, pattern: str, queue: str) -> None:
        if self.typeof(exchange).type == "fanout":
            self._fanout_queues[queue] = (exchange, routing_key.replace("#", "*"))
        with self.conn_or_acquire() as client:
            client.sadd(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or "", pattern or "", queue or ""]),
            )

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        # kombu calls: _delete(queue, exchange, routing_key, pattern)
        exchange = args[0] if args else ""
        routing_key = args[1] if len(args) > 1 else ""
        pattern = args[2] if len(args) > 2 else ""  # noqa: PLR2004
        self.auto_delete_queues.discard(queue)
        had_expires = queue in self._expires
        self._expires.pop(queue, None)
        self._message_ttls.pop(queue, None)
        with self.conn_or_acquire(client=kwargs.get("client")) as client:
            client.srem(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or "", pattern or "", queue or ""]),
            )
            # Collect delivery tags from queue and index to clean up message hashes
            queue_key = self._queue_key(queue)
            index_key = self._messages_index_key(queue)
            tags = {bytes_to_str(t) for t in client.zrange(queue_key, 0, -1)}
            tags.update(bytes_to_str(t) for t in client.zrange(index_key, 0, -1))
            with client.pipeline() as pipe:
                pipe.delete(queue_key, index_key)
                for tag in tags:
                    pipe.delete(self._message_key(tag))
                pipe.execute()
        if had_expires:
            self.connection.cycle._update_expires_timer()

    def _refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on queue and index keys for queues with x-expires."""
        if not self._expires:
            return
        try:
            with self.conn_or_acquire() as client, client.pipeline() as pipe:
                for queue, ttl_ms in self._expires.items():
                    pipe.pexpire(self._queue_key(queue), ttl_ms)
                    pipe.pexpire(self._messages_index_key(queue), ttl_ms)
                pipe.execute()
        except Exception:
            logger.warning("Failed to refresh queue expires, will retry next cycle", exc_info=True)

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        with self.conn_or_acquire() as client:
            return bool(client.exists(self._queue_key(queue)))

    def get_table(self, exchange: str) -> list[tuple[str, str, str]]:
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                return []
            result: list[tuple[str, str, str]] = []
            binding_parts_count = 3  # routing_key, pattern, queue
            for val in values:
                parts = bytes_to_str(val).split(self.sep)
                # Ensure exactly 3 parts (routing_key, pattern, queue)
                while len(parts) < binding_parts_count:
                    parts.append("")
                result.append((parts[0], parts[1], parts[2]))
            return result

    def _purge(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            queue_key = self._queue_key(queue)
            size = int(client.zcard(queue_key))
            # Collect delivery tags from both queue and index to clean up message hashes.
            # Index may have tags not in queue (native delayed messages waiting for delivery).
            index_key = self._messages_index_key(queue)
            tags = {bytes_to_str(t) for t in client.zrange(queue_key, 0, -1)}
            tags.update(bytes_to_str(t) for t in client.zrange(index_key, 0, -1))
            with client.pipeline() as pipe:
                pipe.delete(queue_key, index_key)
                for tag in tags:
                    pipe.delete(self._message_key(tag))
                pipe.execute()
            return size

    def close(self) -> None:
        if self._in_poll:
            with suppress(Empty, *_connection_errors):
                self._bzmpop_read()
        if self._in_fanout_poll:
            with suppress(Empty, *_connection_errors):
                self._xread_read()
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
        for name in ("client", "subclient"):
            try:
                client = self.__dict__[name]
                connection, client.connection = client.connection, None
                connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError, _client_exceptions.ConnectionError) as exc:
                logger.debug("Error closing Redis %s (may be expected during shutdown): %s", name, exc)

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
        return int(vhost)

    def _process_credential_provider(
        self,
        credential_provider: Any,
        connparams: dict[str, Any],
    ) -> None:
        """Process credential_provider and update connparams in-place.

        Accepts a CredentialProvider instance or a dotted import path string.
        When set, static username/password are removed since they are mutually exclusive.
        """
        if credential_provider is None:
            return
        if isinstance(credential_provider, str):
            credential_provider_cls = symbol_by_name(credential_provider)
            credential_provider = credential_provider_cls()
        if CredentialProvider is not None and not isinstance(credential_provider, CredentialProvider):
            raise ValueError(
                "credential_provider must be an instance of "
                f"{CredentialProvider.__module__}.CredentialProvider (or a subclass)",
            )
        connparams["credential_provider"] = credential_provider
        connparams.pop("username", None)
        connparams.pop("password", None)

    def _connparams(self, asynchronous: bool = False) -> dict[str, Any]:  # noqa: PLR0912
        if self.connection.client is None:
            raise TypeError("Transport client must be set")
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

        if conn_class is not None and hasattr(conn_class, "__init__"):
            classes: list[type] = [conn_class]
            if hasattr(conn_class, "__bases__"):
                classes += list(conn_class.__bases__)
            for klass in classes:
                if accepts_argument(klass.__init__, "health_check_interval"):
                    break
            else:
                connparams.pop("health_check_interval")

        # Check for SSL configuration from URL scheme (rediss:// or valkeys://) or transport_options
        ssl_config = conninfo.ssl
        if not ssl_config:
            # Check if using valkeys:// transport (SSL variant of valkey://)
            transport_cls = getattr(self.connection, "transport_cls", None)
            if transport_cls == "valkeys":
                ssl_config = True
            else:
                # Fall back to transport_options for path-based transport URLs
                transport_options = self.connection.client.transport_options or {}
                ssl_config = transport_options.get("ssl")

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
                if path is None:
                    raise ValueError("socket:// URL must include a path")
                connparams.update(
                    {
                        "connection_class": client_lib.UnixDomainSocketConnection,
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

        self._process_credential_provider(self.credential_provider, connparams)

        channel = self
        connection_cls = connparams.get("connection_class") or self.connection_class

        if asynchronous:

            class Connection(connection_cls):
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

    _keyprefix_fanout_formatted = False

    def _get_pool(self, asynchronous: bool = False) -> Any:
        params = self._connparams(asynchronous=asynchronous)
        if not self._keyprefix_fanout_formatted:
            self.keyprefix_fanout = self.keyprefix_fanout.format(db=params["db"])
            self._keyprefix_fanout_formatted = True
        return client_lib.ConnectionPool(**params)

    _minimum_client_version: ClassVar[dict[str, tuple[int, ...]]] = {
        "redis": (7, 1, 0),
        "valkey": (6, 1, 0),
    }

    def _get_client(self) -> Any:
        min_version = self._minimum_client_version.get(_client_lib_name, (7, 1, 0))
        if min_version > client_lib.VERSION:
            min_version_str = ".".join(map(str, min_version))
            raise VersionMismatch(
                f"Transport requires {_client_lib_name} {min_version_str} or later. You have {client_lib.__version__}",
            )

        if self.global_keyprefix:
            return functools.partial(PrefixedStrictRedis, global_keyprefix=self.global_keyprefix)

        return client_lib.Redis

    @contextmanager
    def conn_or_acquire(self, client: Any = None) -> Generator[Any]:
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

    @cached_property
    def subclient(self) -> Any:
        """Dedicated client for XREAD fanout polling (needs its own connection)."""
        return self._create_client(asynchronous=True)

    @property
    def active_queues(self) -> set[str]:
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues if queue not in self.active_fanout_queues}


class Transport(virtual.Transport):
    """Enhanced Redis Transport with priority queues, reliable fanout, and delayed delivery.

    Uses:
    - BZMPOP + sorted sets for regular queues (priority support, reliability)
    - Redis Streams for fanout (true broadcast via XREAD)
    - Integrated delayed delivery via score calculation

    Requires Redis 7.0+ for BZMPOP support.
    """

    Channel = Channel

    polling_interval = 10  # Timeout for blocking BZMPOP/XREAD calls in seconds
    default_port = DEFAULT_PORT
    driver_type = _client_lib_name or "redis"
    driver_name = _client_lib_name or "redis"
    cycle: MultiChannelPoller

    #: Flag indicating this transport supports native delayed delivery
    supports_native_delayed_delivery = True

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(["direct", "topic", "fanout"]),
    )

    connection_errors = _connection_errors
    channel_errors = _channel_errors

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # Import signals module to register signal handlers when transport is used
        from . import signals as _signals  # noqa: F401

        self.cycle = MultiChannelPoller()

    def driver_version(self) -> str:
        return client_lib.__version__

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
                with suppress(KeyError):
                    loop.on_tick.remove(on_poll_start)

        cycle._on_connection_disconnect = _on_disconnect  # type: ignore[method-assign]

        def on_poll_start() -> None:
            cycle_poll_start()
            for fd in cycle.fds:
                add_reader(fd, on_readable, fd)

        loop.on_tick.add(on_poll_start)

        # Unified requeue check handles both delayed messages and timed-out messages
        loop.call_repeatedly(DEFAULT_REQUEUE_CHECK_INTERVAL, cycle.maybe_enqueue_due_messages)

        # Heartbeat to keep in-flight messages alive
        visibility_timeout = connection.client.transport_options.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)  # type: ignore[attr-defined]
        loop.call_repeatedly(visibility_timeout / 3, cycle.maybe_update_messages_index)

        # Store loop for dynamic timer registration (queue TTL refresh)
        cycle._loop = loop

    def on_readable(self, fileno: int) -> Any:  # type: ignore[override]
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)
