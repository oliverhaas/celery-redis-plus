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
* ``blocking_timeout``: Time in seconds BZMPOP and XREAD block on the server while a poll
  is outstanding (default: 10). Has to stay below ``socket_timeout``, if one is set, or
  every poll that finds nothing ends as a read timeout. This is kombu's ``brpop_timeout``
  under a name that also covers XREAD, and it is not kombu's ``polling_interval``, which is
  the sleep between unsuccessful polls and stays disabled here.
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
* ``delivery_limit``: Maximum number of times a message may be delivered before it is
  dropped (default: 20, ``None`` disables the limit). Named and counted after RabbitMQ
  quorum queues' ``delivery-limit`` policy: it counts attempts, so a limit of 3 allows a
  first delivery plus two redeliveries. A redelivery is a visibility timeout restore or a
  reject-with-requeue, the same two paths RabbitMQ counts. Consumed messages carry the
  count in the ``x-delivery-count`` header once it is above zero.
* ``sep``: Separator used inside ``_kombu.binding.{exchange}`` members
  (default: ``"\\x06\\x16"``, same as kombu's Redis transport). Members are packed exactly
  as kombu's Redis transport packs them, so a deployment migrating from it must carry over
  whatever ``sep`` it configured there or the members it left behind match no routing key
  -- see the migration guide. The key itself is a sorted set here rather than a set, so the
  two transports can no longer share it; the first bind converts an inherited set in place.

Binding lifetime
================
``_kombu.binding.{exchange}`` is a sorted set scored with the unix time each binding goes
stale, which is ``x-expires`` after its last refresh (at least ``MIN_BINDING_LIFETIME``).
A queue without ``x-expires`` is scored ``+inf`` and its binding only ever goes away on an
explicit unbind. Declaring, refreshing and publishing all rescore; ``get_table`` drops
whatever has aged out, so cleanup rides the read path and nothing has to sweep.
"""

from __future__ import annotations

import functools
import numbers
import socket as socket_module
import weakref
from contextlib import contextmanager, suppress
from pathlib import Path
from queue import Empty
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

import logging

from celery.bootsteps import (
    CLOSE,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
    TERMINATE,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
)
from celery.signals import worker_ready, worker_shutdown
from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.transport import virtual
from kombu.transport.base import (
    to_rabbitmq_queue_arguments,  # type: ignore[attr-defined]  # ty: ignore[unresolved-import]
)
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
    DEFAULT_BLOCKING_TIMEOUT,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_DELIVERY_LIMIT,
    DEFAULT_MESSAGE_TTL,
    DEFAULT_QUEUE_EXPIRES,
    DEFAULT_REQUEUE_BATCH_LIMIT,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_VISIBILITY_TIMEOUT,
    DROPPED_REPORT_LIMIT,
    MAX_PRIORITY,
    MESSAGE_KEY_PREFIX,
    MESSAGES_INDEX_PREFIX,
    MIN_BINDING_LIFETIME,
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


class SweepStats(NamedTuple):
    """What one requeue sweep did, per enqueue_due_messages call."""

    enqueued: int = 0
    dropped: int = 0
    redelivered: int = 0
    orphaned: int = 0


DEFAULT_PORT = 6379
DEFAULT_DB = 0

# Load Lua scripts at module init
_PACKAGE_DIR = Path(__file__).parent
_ENQUEUE_DUE_MESSAGES_LUA = (_PACKAGE_DIR / "transport_enqueue_due_messages.lua").read_text()
_REQUEUE_MESSAGE_LUA = (_PACKAGE_DIR / "transport_requeue_message.lua").read_text()
_CONSUME_MESSAGE_LUA = (_PACKAGE_DIR / "transport_consume_message.lua").read_text()
_ACK_MESSAGE_LUA = (_PACKAGE_DIR / "transport_ack_message.lua").read_text()
_CONVERT_BINDINGS_LUA = (_PACKAGE_DIR / "transport_convert_bindings.lua").read_text()


_warned_priority_clamp = False
_warned_polling_interval = False


def _is_wrongtype(exc: Exception) -> bool:
    """Whether the server rejected a command because the key holds another type."""
    return str(exc).startswith("WRONGTYPE")


# Per-app worker references.  WeakKeyDictionary so entries auto-clean
# when the Celery app is garbage-collected (no manual cleanup needed on crash).
# The value is the WorkController, which carries both the ``pool`` (for the
# executor wait) and the ``blueprint`` (to tell shutdown from reconnect).
_worker_owners: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()


@worker_ready.connect
def _on_worker_ready(sender: Any, **kwargs: Any) -> None:
    _worker_owners[sender.app] = sender


@worker_shutdown.connect
def _on_worker_shutdown(sender: Any, **kwargs: Any) -> None:
    _worker_owners.pop(sender.app, None)


# Celery's start_worker test helper uses TestWorkController which fires
# test_worker_started instead of worker_ready.  Handle both so the
# executor-wait logic works in integration tests too.
try:
    from celery.contrib.testing.worker import test_worker_started

    @test_worker_started.connect
    def _on_test_worker_started(sender: Any, worker: Any, **kwargs: Any) -> None:
        _worker_owners[worker.app] = worker
except ImportError:  # pragma: no cover
    pass


def _get_worker_owner_for_channel(channel: Channel) -> Any:
    """Look up the WorkController for the Celery app that owns this channel."""
    # kombu's Connection holds no back-reference to the Celery app, so the app
    # lookup is best-effort and the single-registration fallback is what
    # normally resolves.  Two or more workers leave nothing to disambiguate on.
    app = getattr(getattr(channel.connection, "client", None), "app", None)
    if app is not None and (owner := _worker_owners.get(app)) is not None:
        return owner
    if len(_worker_owners) == 1:
        return next(iter(_worker_owners.values()))
    return None


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
        "ZREMRANGEBYSCORE",
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
        ret = super().parse_response(connection, command_name, **options)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]
        if command_name == "BZMPOP" and ret:
            # BZMPOP returns (key, [(member, score), ...])
            key, members = ret
            if isinstance(key, bytes):
                key = key.decode()
            key = key[len(self.global_keyprefix) :]
            return key, members
        return ret

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        return super().execute_command(*self._prefix_args(list(args)), **kwargs)  # type: ignore[misc]  # ty: ignore[unresolved-attribute]

    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> PrefixedRedisPipeline:
        return PrefixedRedisPipeline(
            self.connection_pool,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            self.response_callbacks,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
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
        elif self._delivered is not None and delivery_tag in self._delivered:
            # Regular sorted set message — atomic Lua removes index entry + hash
            self._remove_from_indices(delivery_tag)
        super().ack(delivery_tag)

    def reject(self, delivery_tag: str, requeue: bool = False) -> None:
        # Fanout messages: requeue not supported (fire-and-forget broadcast)
        if delivery_tag in self._fanout_tags:
            self._fanout_tags.discard(delivery_tag)
            super().ack(delivery_tag)
        elif self._delivered is None or delivery_tag not in self._delivered:
            # Already restored/flushed (e.g., during shutdown cleanup)
            super().ack(delivery_tag)
        else:
            # Regular sorted set message
            if requeue:
                queue = self._delivered[delivery_tag].delivery_info["routing_key"]
                self.requeue_by_tag(delivery_tag, queue=queue, leftmost=True)
            else:
                self._remove_from_indices(delivery_tag)
            super().ack(delivery_tag)

    def _remove_from_indices(self, delivery_tag: str) -> None:
        """Atomically remove message from queue and index and delete its hash.

        Uses a Lua script so that a connection drop cannot leave an orphaned
        message hash (ZREM succeeds but DEL doesn't) or an orphaned index
        entry (DEL succeeds but ZREM doesn't).

        The queue entry is removed too: if enqueue_due_messages restored this
        message while its consumer was still working on it, the tag is back in
        the queue, and only this ZREM can cancel that restored copy before
        another worker pops it.
        """
        queue = cast("dict", self._delivered)[delivery_tag].delivery_info["routing_key"]
        # Prefix keys since EVALSHA doesn't auto-prefix
        index_key = f"{self.channel.global_keyprefix}{self.channel._messages_index_key(queue)}"
        message_key = f"{self.channel.global_keyprefix}{self.channel._message_key(delivery_tag)}"
        queue_key = f"{self.channel.global_keyprefix}{self.channel._queue_key(queue)}"
        with self.channel.conn_or_acquire() as client:
            ack_script = client.register_script(_ACK_MESSAGE_LUA)
            ack_script(keys=[index_key, message_key, queue_key], args=[delivery_tag])

    def restore_unacked_once(self, stderr: Any = None) -> None:
        """Restore unacked messages, waiting for threads and draining acks."""
        self._drain_hub_callbacks()

        # kombu calls this from Channel.close(), which also runs on broker
        # reconnects; Blueprint.stop() sets CLOSE before it stops a step, so
        # only a real shutdown reads CLOSE/TERMINATE here.  Reconnects fall
        # back to messages_index redelivery on the visibility deadline.
        owner = _get_worker_owner_for_channel(self.channel)
        blueprint = getattr(owner, "blueprint", None)
        if owner is not None and getattr(blueprint, "state", None) not in {CLOSE, TERMINATE}:
            return

        # Celery fires this BEFORE Pool.on_stop() waits for threads, so wait
        # here instead: finishing threads land their acks in hub._ready and the
        # second drain catches them, leaving only unfinished work to restore.
        if (
            (pool := getattr(owner, "pool", None)) is not None
            and (executor := getattr(pool, "executor", None)) is not None
            and hasattr(executor, "shutdown")
        ):
            executor.shutdown(wait=True)
            self._drain_hub_callbacks()

        super().restore_unacked_once(stderr)

    def _drain_hub_callbacks(self) -> None:
        """Execute pending hub callbacks to flush deferred acks.

        The hub's _ready set holds callbacks scheduled via call_soon().
        During graceful shutdown, worker threads may have completed tasks
        and scheduled ack callbacks that haven't fired yet. Processing
        them here ensures those delivery tags are marked dirty before
        the remaining _delivered entries are evaluated.
        """
        try:
            hub = self.channel.connection.cycle._loop
        except AttributeError:
            return
        if hub is None:
            return
        ready = hub._pop_ready()
        for cb in ready:
            with suppress(Exception):
                cb()

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

    def enqueue_due_messages(self) -> SweepStats:
        """Enqueue messages due before the next requeue cycle.

        This unified method handles both:
        - Delayed messages that are now ready to be processed (first delivery)
        - Messages that were consumed but not acked (redelivery)

        Uses a Lua script for atomic, efficient batch processing.

        Returns:
            SweepStats totalled across the channel's active queues.
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
        self._last_expires_refresh: float = 0.0

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
                total_enqueued += cast("QoS", qos).enqueue_due_messages().enqueued
        return total_enqueued

    def maybe_update_messages_index(self) -> None:
        """Update message index scores to keep delivered messages alive."""
        for channel in self._channels:
            qos = channel.qos
            if qos is not None and channel.active_queues:
                cast("QoS", qos).maybe_update_messages_index()

    def maybe_refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on queue keys with x-expires TTL.

        Every channel here belongs to one connection, so they share one expires
        registry and one server. The first channel that gets the refresh
        through does it for all of them; the rest are only tried if it fails.
        """
        for channel in self._channels:
            if channel._refresh_queue_expires():
                return

    def _expires_refresh_interval(self) -> float | None:
        """Seconds between refreshes, or None when no queue has an x-expires.

        Half the shortest configured TTL, so it is refreshed ~2 times before it
        would expire.
        """
        min_ttl_ms: int | None = None
        for channel in self._channels:
            for ttl_ms in channel._expires.values():
                if min_ttl_ms is None or ttl_ms < min_ttl_ms:
                    min_ttl_ms = ttl_ms
        if min_ttl_ms is None:
            return None
        return min_ttl_ms / 2 / 1000  # ms → seconds, divided by 2

    def maybe_refresh_queue_expires_without_loop(self) -> None:
        """Refresh from the drain path on a connection that has no event loop.

        The timer below only exists inside a worker's hub. A celery control
        client waiting for replies, a Flower event receiver and a gevent
        worker's synloop all drain events instead, and without this their
        queues and bindings age out from under them while they are still
        being used.
        """
        if self._loop is not None:
            return
        interval = self._expires_refresh_interval()
        if interval is None:
            return
        now = time()
        if now - self._last_expires_refresh < interval:
            return
        self._last_expires_refresh = now
        self.maybe_refresh_queue_expires()

    def _update_expires_timer(self) -> None:
        """Register or update the periodic PEXPIRE timer based on configured TTLs."""
        interval = self._expires_refresh_interval()

        if interval is None:
            if self._expires_timer_entry is not None:
                self._expires_timer_entry.cancel()
                self._expires_timer_entry = None
                self._expires_timer_interval = None
            return

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
        self.maybe_refresh_queue_expires_without_loop()
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
    _warned_queue_expires_clamp = False
    _warned_binding_sep = False
    max_priority = MAX_PRIORITY  # Override kombu's default of 9 to enable full 0-255 range

    # Message storage keys
    # Per-message hash keys use format: {message_key_prefix}{delivery_tag}
    message_key_prefix = MESSAGE_KEY_PREFIX
    message_ttl = DEFAULT_MESSAGE_TTL  # TTL for per-message hashes (-1 = no TTL)

    # Expiry in seconds for queues declared without x-expires; when set, binding
    # tables and fanout streams get TTLs too (None = queues persist)
    queue_expires: int | None = DEFAULT_QUEUE_EXPIRES

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
    delivery_limit: int | None = DEFAULT_DELIVERY_LIMIT

    # Fanout settings
    fanout_prefix: bool | str = True
    fanout_patterns = True

    _async_pool: Any = None
    _pool: Any = None

    from_transport_options = virtual.Channel.from_transport_options + (
        "sep",
        "message_key_prefix",
        "message_ttl",
        "queue_expires",
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
        "delivery_limit",
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
        # Queues consumed with no_ack=True; the consume script dequeues these at
        # pop time instead of giving them a deadline nothing would ever ack away
        self._no_ack_queues: set[str] = set()
        self._fanout_queues: dict[str, tuple[str, str]] = {}
        self.handlers = {"BZMPOP": self._bzmpop_read, "XREAD": self._xread_read}
        # Track last-read stream ID per stream for fanout (start with $ = only new messages)
        self._stream_offsets: dict[str, str] = {}
        # Per-queue TTL state from x-expires and x-message-ttl queue arguments.
        # Aliases of the transport-wide registries, not copies: kombu caches
        # declarations per connection (Connection.declared_entities), so only
        # the first channel to declare a queue sees its arguments, while any
        # channel of that connection may be the one publishing to it.
        self._expires: dict[str, int] = self.connection._expires  # queue_name → TTL in ms
        self._message_ttls: dict[str, int] = self.connection._message_ttls  # queue_name → message TTL in ms
        # queue_name → {(exchange, member)} for every binding declared on this
        # connection, so the refresh knows which members are still ours to keep
        # alive.  Shared for the same reason as the two registries above.
        self._bindings: dict[str, set[tuple[str, str]]] = self.connection._bindings
        # FAST/SLOW consume mode: FAST uses atomic Lua ZPOPMIN, SLOW uses blocking BZMPOP
        self._consume_fast_mode: bool = True
        self._consume_script_sha: str | None = None
        # Snapshot of the transport's blocking_timeout, so the consume paths
        # read one place and 0 (block forever) survives uncoerced
        self.blocking_timeout: float = self.connection.blocking_timeout

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

    def _next_queue_at(self) -> float:
        """The visibility deadline for a message delivered right now.

        DEFAULT_REQUEUE_CHECK_INTERVAL on top of the visibility timeout
        compensates for the look-ahead threshold in enqueue_due_messages, so
        a message is never restored before its full timeout has passed.
        """
        return time() + self.visibility_timeout + DEFAULT_REQUEUE_CHECK_INTERVAL

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

    def _cleanup_expired_message(self, queue: str, delivery_tag: str, client: Any | None = None) -> None:
        """Remove messages_index entry for a message whose hash has expired."""
        if client is None:
            with self.conn_or_acquire() as client:
                client.zrem(self._messages_index_key(queue), delivery_tag)
        else:
            client.zrem(self._messages_index_key(queue), delivery_tag)

    def _drain_expired_and_deliver(self, queue: str) -> bool:
        """Atomically pop, refresh index, and deliver from a single queue.

        Uses the consume Lua script which handles expired message hashes
        internally (cleans up index, tries next message, up to 100 attempts).
        This ensures the messages_index score is refreshed for visibility
        timeout tracking on the delivered message.

        Used after BZMPOP returns an expired message to avoid going back to
        blocking when there are still deliverable messages in the queue.

        Returns:
            True if a message was delivered, raises Empty otherwise.
        """
        consume_script = self.client.register_script(_CONSUME_MESSAGE_LUA)
        queue_key = f"{self.global_keyprefix}{self._queue_key(queue)}"
        result = consume_script(
            keys=[queue_key],
            args=[
                self.global_keyprefix,
                self.message_key_prefix,
                str(self._next_queue_at()),
                MESSAGES_INDEX_PREFIX,
                queue,
                "1" if queue in self._no_ack_queues else "0",
            ],
        )
        if not result:
            raise Empty
        _, message = self._parse_consume_result(result)
        self.connection._deliver(message, queue)
        return True

    def _restore(self, message: Any, leftmost: bool = False) -> None:
        """Restore a message to its queue.

        This method is called by Kombu's virtual.Channel for message recovery.
        """
        queue = message.delivery_info.get("routing_key")
        self._requeue_by_tag(message.delivery_tag, queue=queue, leftmost=leftmost)

    def _restore_at_beginning(self, message: Any) -> None:
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue: str, no_ack: bool, callback: Any, consumer_tag: str | None, **kwargs: Any) -> str:
        if queue in self._fanout_queues:
            self.active_fanout_queues.add(queue)
        if no_ack:
            self._no_ack_queues.add(queue)
        ret = super().basic_consume(queue, no_ack, callback, consumer_tag, **kwargs)
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
        self._no_ack_queues.discard(queue)
        ret = super().basic_cancel(consumer_tag)
        self._queue_cycle = list(self.active_queues)
        return ret

    # --- BZMPOP (sorted set) methods for regular queues ---

    def _ensure_consume_script_sha(self) -> str:
        """Load and cache the consume_message Lua script SHA."""
        if self._consume_script_sha is None:
            self._consume_script_sha = self.client.script_load(_CONSUME_MESSAGE_LUA)
        return self._consume_script_sha

    @staticmethod
    def _mark_redelivered(message: dict[str, Any], delivery_count: int) -> None:
        """Set the AMQP redelivery flags on a consumed message.

        Both are derived from the stored counter rather than a separate field.
        x-delivery-count goes into the payload's top-level headers dict, which
        is what kombu's virtual Message builds .headers from; a value in
        properties['headers'] never reaches a consumer or a task.
        delivery_info['redelivered'] is where kombu's own Redis transport puts
        it, and the only place celery looks, in Request and trace, for
        worker_deduplicate_successful_tasks.
        """
        if delivery_count <= 0:
            return
        message["headers"] = headers = message.get("headers") or {}
        headers["x-delivery-count"] = delivery_count
        properties = message.setdefault("properties", {})
        properties.setdefault("delivery_info", {})["redelivered"] = True

    def _parse_consume_result(self, result: list[Any]) -> tuple[str, dict[str, Any]]:
        """Parse the result from consume_message Lua script.

        Returns:
            Tuple of (queue_name, message_dict).
        """
        queue_name = bytes_to_str(result[0])
        payload_json = result[2]
        message: dict[str, Any] = loads(bytes_to_str(payload_json))
        self._mark_redelivered(message, int(result[3] or 0))
        return queue_name, message

    def _bzmpop_start(self, timeout: float | None = None) -> None:
        if timeout is None:
            timeout = self.blocking_timeout
        if not self._queue_cycle:
            return

        if self._consume_fast_mode:
            # FAST mode: send non-blocking EVALSHA (atomic ZPOPMIN + ZADD + HMGET)
            sha = self._ensure_consume_script_sha()
            keys = [f"{self.global_keyprefix}{self._queue_key(q)}" for q in self._queue_cycle]
            args = [
                self.global_keyprefix,
                self.message_key_prefix,
                str(self._next_queue_at()),
                MESSAGES_INDEX_PREFIX,
                *self._queue_cycle,
                *("1" if q in self._no_ack_queues else "0" for q in self._queue_cycle),
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

                # Pipeline ZADD (set index deadline) + HMGET (fetch message).
                # Not xx=True: a missing entry would leave the message delivered
                # with nothing tracking it, out of the queue and out of the index,
                # so a worker crash would lose it permanently and silently.
                # The pipeline is not transactional, so this can write an entry
                # for a message acked a moment earlier. That corrects itself: the
                # HMGET then returns no payload and _cleanup_expired_message ZREMs
                # the entry again.
                index_key = self._messages_index_key(dest)
                message_key = self._message_key(delivery_tag)
                with self.client.pipeline(transaction=False) as pipe:
                    if dest in self._no_ack_queues:
                        # Finished on delivery: drop the index entry and, after
                        # reading it, the hash, like the consume script does
                        pipe.zrem(index_key, delivery_tag)
                        pipe.hmget(message_key, "payload", "delivery_count")
                        pipe.delete(message_key)
                    else:
                        pipe.zadd(index_key, {delivery_tag: self._next_queue_at()})
                        pipe.hmget(message_key, "payload", "delivery_count")
                    results = pipe.execute()

                payload_json = results[1][0]
                if payload_json:
                    message: dict[str, Any] = loads(bytes_to_str(payload_json))
                    self._mark_redelivered(message, int(results[1][1] or 0))
                    self._consume_fast_mode = True  # Switch back to FAST
                    self.connection._deliver(message, dest)
                    return True
                # Message hash expired (x-message-ttl) — clean up index
                self._cleanup_expired_message(dest, delivery_tag, self.client)
                # Try remaining messages atomically via consume Lua script
                self._drain_expired_and_deliver(dest)
                self._consume_fast_mode = True  # Switch back to FAST
                return True
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
            timeout = self.blocking_timeout

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

    def basic_get(self, queue: str, no_ack: bool = False, **kwargs: Any) -> Any:
        """Get a single message synchronously.

        Reimplements the kombu base method so no_ack reaches _get: a no_ack
        get must dequeue the message inside the atomic pop rather than leave
        it with a visibility deadline nobody will ever ack away.
        """
        try:
            message = self.Message(self._get(queue, no_ack=no_ack), channel=self)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
        except Empty:
            return None
        return message

    def _get(self, queue: str, timeout: float | None = None, *, no_ack: bool = False) -> dict[str, Any]:
        """Get single message from queue (synchronous).

        Uses the atomic consume Lua script (ZPOPMIN + ZADD index + HMGET).
        """
        with self.conn_or_acquire() as client:
            consume_script = client.register_script(_CONSUME_MESSAGE_LUA)
            queue_key = f"{self.global_keyprefix}{self._queue_key(queue)}"
            result = consume_script(
                keys=[queue_key],
                args=[
                    self.global_keyprefix,
                    self.message_key_prefix,
                    str(self._next_queue_at()),
                    MESSAGES_INDEX_PREFIX,
                    queue,
                    "1" if no_ack else "0",
                ],
            )
            if not result:
                raise Empty
            _, message = self._parse_consume_result(result)
            return message

    def _size(self, queue: str) -> int:
        with self.conn_or_acquire() as client:
            return int(client.zcard(self._queue_key(queue)))

    def enqueue_due_messages(self) -> SweepStats:
        """Enqueue messages due before the next requeue cycle.

        This unified method handles both:
        - Delayed messages that are now ready to be processed (first delivery)
        - Messages that were consumed but not acked (redelivery)

        Iterates over each active queue's per-queue messages index and runs
        a Lua script that atomically moves due messages into the queue.

        Returns:
            SweepStats totalled across the active queues.
        """
        if not self._queue_cycle:
            return SweepStats()

        now = time()
        threshold = now + DEFAULT_REQUEUE_CHECK_INTERVAL
        totals = SweepStats()

        delivery_limit = -1 if self.delivery_limit is None else self.delivery_limit

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
                            delivery_limit,
                            DROPPED_REPORT_LIMIT,
                        ],
                    )
                    enqueued, dropped, redelivered, orphaned, dropped_payloads = result
                    if dropped:
                        # The Lua script deleted these hashes, so this log line
                        # is the only remaining trace of the messages
                        described = ", ".join(self._describe_message(payload) for payload in dropped_payloads)
                        if dropped > len(dropped_payloads):
                            described += ", ..."
                        logger.error(
                            "Queue %s: %d message(s) dropped after reaching the delivery limit of %d: %s",
                            queue,
                            dropped,
                            self.delivery_limit,
                            described,
                        )
                    if redelivered:
                        logger.info(
                            "Queue %s: %d message(s) redelivered after their visibility timeout expired.",
                            queue,
                            redelivered,
                        )
                    if orphaned:
                        logger.info(
                            "Queue %s: removed %d orphaned index entries (message already acked or expired).",
                            queue,
                            orphaned,
                        )
                    if enqueued >= DEFAULT_REQUEUE_BATCH_LIMIT:
                        logger.warning(
                            "Queue %s hit enqueue batch limit of %d. There may be more messages waiting.",
                            queue,
                            DEFAULT_REQUEUE_BATCH_LIMIT,
                        )
                    totals = SweepStats(
                        totals.enqueued + enqueued,
                        totals.dropped + dropped,
                        totals.redelivered + redelivered,
                        totals.orphaned + orphaned,
                    )
                except Exception:
                    logger.warning(
                        "Failed to enqueue due messages for queue %s, will retry next cycle",
                        queue,
                        exc_info=True,
                    )

        return totals

    @staticmethod
    def _describe_message(payload: bytes | str) -> str:
        """One-line description of a raw payload, for log lines about messages
        that no longer exist anywhere else."""
        try:
            message = loads(bytes_to_str(payload))
            headers = message.get("headers") or {}
            task = headers.get("task")
            task_id = headers.get("id")
            if task or task_id:
                return f"{task or '<unknown task>'} (id {task_id or '?'})"
            delivery_tag = (message.get("properties") or {}).get("delivery_tag")
            if delivery_tag:
                return f"<non-task message {delivery_tag}>"
        except Exception:
            logger.debug("Could not decode a dropped message payload for logging.", exc_info=True)
        return "<undecodable message>"

    def _requeue_by_tag(self, delivery_tag: str, queue: str | None = None, leftmost: bool = False) -> bool:
        """Requeue a rejected message to its queue using Lua script.

        The Lua script atomically reads the routing_key (queue) from the message
        hash and adds the message back to that queue, incrementing delivery_count.
        It does not enforce delivery_limit: the message keeps its index entry,
        so enqueue_due_messages drops it at the next deadline if it is over.

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
        queue_at = eta_timestamp if is_native_delayed else self._next_queue_at()

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
                    "native_delayed": 1 if is_native_delayed else 0,
                    "eta": eta_timestamp or 0,
                    "delivery_count": 0,
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
                # Publishing keeps the route alive (producers run no refresh
                # timer); GT never pulls back another channel's longer deadline
                stale_at = self._binding_stale_at(queue, now=now)
                binding_ttl_ms = self._binding_ttl_ms(queue)
                for exchange, member in self._bindings.get(queue, ()):
                    pipe.zadd(self._binding_key(exchange), {member: stale_at}, gt=True)
                    if binding_ttl_ms is not None:
                        pipe.pexpire(self._binding_key(exchange), binding_ttl_ms, gt=True)
            pipe.execute()

    def _put_fanout(self, exchange: str, message: dict[str, Any], routing_key: str, **kwargs: Any) -> None:
        """Deliver fanout message using Redis Streams."""
        stream_key = self._fanout_stream_key(exchange)
        global_expires_ms = self._global_expires_ms()

        with self.conn_or_acquire() as client, client.pipeline(transaction=False) as pipe:
            pipe.xadd(
                name=stream_key,
                fields={"payload": dumps(message)},
                id="*",
                maxlen=self.stream_maxlen,
                approximate=True,
            )
            if global_expires_ms is not None:
                # Only publishers can keep a broadcast stream alive; consumers
                # only read, and fanout is not durable anyway
                pipe.pexpire(stream_key, global_expires_ms)
            pipe.execute()

    def prepare_queue_arguments(self, arguments: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any] | None:
        return to_rabbitmq_queue_arguments(arguments, **kwargs)

    def _new_queue(self, queue: str, auto_delete: bool = False, **kwargs: Any) -> None:
        if auto_delete:
            self.auto_delete_queues.add(queue)
        arguments = kwargs.get("arguments") or {}
        x_expires = arguments.get("x-expires")
        if x_expires is None:
            x_expires = self._global_expires_ms()
        elif queue not in self._expires:
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
        if x_expires is not None and queue not in self._expires:
            self._expires[queue] = x_expires
            self.connection.cycle._update_expires_timer()
        x_message_ttl = arguments.get("x-message-ttl")
        if x_message_ttl is not None and queue not in self._message_ttls:
            self._message_ttls[queue] = int(x_message_ttl)

    def _global_expires_ms(self) -> int | None:
        """The queue_expires option in milliseconds, floored like x-expires is."""
        if self.queue_expires is None:
            return None
        expires_ms = int(self.queue_expires * 1000)
        if expires_ms < MIN_QUEUE_EXPIRES:
            if not self._warned_queue_expires_clamp:
                logger.warning(
                    "queue_expires %dms is below minimum %dms, clamping.",
                    expires_ms,
                    MIN_QUEUE_EXPIRES,
                )
                Channel._warned_queue_expires_clamp = True
            expires_ms = MIN_QUEUE_EXPIRES
        return expires_ms

    def _binding_key(self, exchange: str) -> str:
        return self.keyprefix_queue % (exchange,)

    def _binding_member(self, routing_key: str, pattern: str, queue: str) -> str:
        return self.sep.join([routing_key or "", pattern or "", queue or ""])

    def _binding_stale_at(self, queue: str, now: float | None = None) -> float:
        """Unix time the bindings of this queue go stale.

        Only a queue that expires can leave a binding behind that nobody wants:
        one that stays around has to keep its route, so it is scored +inf and
        goes away on an explicit unbind or not at all.

        The window never drops below MIN_BINDING_LIFETIME, because the processes
        that abandon bindings are the ones that cannot refresh them. A celery
        control client has no event loop, and the 10s x-expires on its reply
        queue is shorter than the control call the binding has to outlive.
        """
        expires_ms = self._expires.get(queue)
        if expires_ms is None:
            return float("inf")
        return (time() if now is None else now) + max(expires_ms / 1000, MIN_BINDING_LIFETIME)

    def _binding_ttl_ms(self, queue: str) -> int | None:
        """TTL to put on a binding key touched on behalf of this queue.

        Only with the global queue_expires option: a per-queue x-expires alone
        must not put a TTL on a table that may also hold the bindings of queues
        that never expire. The floor mirrors _binding_stale_at, so the key
        outlives its own members' staleness deadlines.
        """
        global_ms = self._global_expires_ms()
        if global_ms is None:
            return None
        expires_ms = self._expires.get(queue, global_ms)
        return max(expires_ms, MIN_BINDING_LIFETIME * 1000)

    def _touch_binding_key(self, client: Any, exchange: str, queue: str) -> None:
        """Give the binding key a TTL when the global queue_expires option is on.

        GT, so a queue with a short window cannot shrink a TTL that another
        queue's touch pushed further out. GT treats a key without a TTL as
        infinite and declines, so a key that has none yet gets one directly.
        """
        ttl_ms = self._binding_ttl_ms(queue)
        if ttl_ms is None:
            return
        key = self._binding_key(exchange)
        if not client.pexpire(key, ttl_ms, gt=True) and client.pttl(key) == -1:
            client.pexpire(key, ttl_ms)

    def _convert_binding_set(self, client: Any, exchange: str) -> None:
        """Turn a binding table left behind as a plain set into a sorted set."""
        script = client.register_script(_CONVERT_BINDINGS_LUA)
        # EVALSHA doesn't auto-prefix, so the key goes in already prefixed
        converted = script(keys=[f"{self.global_keyprefix}{self._binding_key(exchange)}"])
        logger.info(
            "Converted the binding table of exchange %r from a set to a sorted set, "
            "carrying over %s member(s) with no staleness deadline",
            exchange,
            converted,
        )

    def _queue_bind(self, exchange: str, routing_key: str, pattern: str, queue: str) -> None:
        if self.typeof(exchange).type == "fanout":
            # Fanout never reads the binding table (delivery is one XADD to
            # the stream), so drop any table an earlier version left behind
            self._fanout_queues[queue] = (exchange, routing_key.replace("#", "*"))
            with self.conn_or_acquire() as client:
                client.delete(self._binding_key(exchange))
            return
        member = self._binding_member(routing_key, pattern, queue)
        self._bindings.setdefault(queue, set()).add((exchange, member))
        with self.conn_or_acquire() as client:
            try:
                client.zadd(self._binding_key(exchange), {member: self._binding_stale_at(queue)})
            except _client_exceptions.ResponseError as exc:
                if not _is_wrongtype(exc):
                    raise
                self._convert_binding_set(client, exchange)
                client.zadd(self._binding_key(exchange), {member: self._binding_stale_at(queue)})
            self._touch_binding_key(client, exchange, queue)

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        # kombu calls: _delete(queue, exchange, routing_key, pattern)
        exchange = args[0] if args else ""
        routing_key = args[1] if len(args) > 1 else ""
        pattern = args[2] if len(args) > 2 else ""  # noqa: PLR2004
        self.auto_delete_queues.discard(queue)
        had_expires = queue in self._expires
        self._expires.pop(queue, None)
        self._message_ttls.pop(queue, None)
        member = self._binding_member(routing_key, pattern, queue)
        declared = self._bindings.get(queue)
        if declared is not None:
            declared.discard((exchange, member))
            if not declared:
                del self._bindings[queue]
        with self.conn_or_acquire(client=kwargs.get("client")) as client:
            try:
                client.zrem(self._binding_key(exchange), member)
            except _client_exceptions.ResponseError as exc:
                if not _is_wrongtype(exc):
                    raise
                # Table still a plain set from an older deployment. Unbinding is
                # no reason to convert it, so just remove the member in place.
                client.srem(self._binding_key(exchange), member)
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

    def _refresh_queue_expires(self) -> bool:
        """Refresh queue and index keys, and the bindings, for queues with x-expires.

        A binding lives exactly as long as some channel keeps rescoring it. The
        rescore also re-adds a member another process pruned while this one was
        stalled, so a queue that is still declared here keeps its route.

        Returns whether the refresh is done, so the caller can fall back to
        another channel when this one's connection is broken.
        """
        if not self._expires:
            return True
        try:
            with self.conn_or_acquire() as client, client.pipeline() as pipe:
                now = time()
                touch: set[tuple[str, str]] = set()
                for queue, ttl_ms in self._expires.items():
                    pipe.pexpire(self._queue_key(queue), ttl_ms)
                    pipe.pexpire(self._messages_index_key(queue), ttl_ms)
                    # GT, as in _put: never pull a deadline backwards that
                    # another channel pushed further out.
                    stale_at = self._binding_stale_at(queue, now=now)
                    for exchange, member in self._bindings.get(queue, ()):
                        pipe.zadd(self._binding_key(exchange), {member: stale_at}, gt=True)
                        touch.add((exchange, queue))
                pipe.execute()
                # Off the pipeline because the bootstrap needs the PTTL reply:
                # PEXPIRE GT declines on a key that lost its TTL
                for exchange, queue in touch:
                    self._touch_binding_key(client, exchange, queue)
        except Exception:
            logger.warning("Failed to refresh queue expires, will retry next cycle", exc_info=True)
            return False
        return True

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        with self.conn_or_acquire() as client:
            return bool(client.exists(self._queue_key(queue)))

    def _lookup(self, exchange: str, routing_key: str, default: str | None = None) -> list[str] | set[str]:
        """Find queues bound to an exchange, raising rather than dropping for durable direct.

        kombu returns an empty list when an exchange's binding table is empty and
        the publish is silently discarded. That was a deliberate change in kombu
        5.2 (PR #1404, closing #1063), replacing an InconsistencyError, because an
        empty table is the normal AMQP state for an exchange whose queues were all
        unbound.

        The reasoning holds for topic and fanout. It does not hold for a durable
        direct exchange, where the binding is known to exist, and it especially
        does not hold once bindings carry a staleness deadline, which they do
        here, because then an empty table also means the binding aged out.

        InconsistencyError is in this transport's connection_errors, so kombu's
        Connection.ensure reconnects, clears declared_entities, redeclares (which
        recreates the binding via ZADD) and retries. If it fails again the caller
        gets an OperationalError.

        A transient direct exchange gets kombu's drop instead, with an INFO log.
        Its bindings empty by design whenever its consumers go away: a pidbox
        reply exchange loses its binding the moment the control client leaves,
        and the publisher redeclaring its own entities could never recreate a
        binding that belonged to someone else, so the retry loop only churned.

        Raised here rather than in get_table like pre-5.2 kombu did, because
        exchange_delete, queue_unbind and list_bindings also call get_table and
        would then throw during teardown.
        """
        queues = super()._lookup(exchange, routing_key, default)
        # get_table runs a second time only on the miss path, to tell "no bindings
        # at all" apart from "bindings exist but none match this routing key".
        if not queues and self.typeof(exchange).type == "direct" and not self.get_table(exchange):
            if not self._exchange_is_durable(exchange):
                logger.info(
                    "Dropped message to transient exchange %r with routing key %r: binding table is empty.",
                    exchange,
                    routing_key,
                )
                return queues
            key = self._binding_key(exchange)
            msg = (
                f"Cannot route message for direct exchange {exchange!r}: binding table is empty. "
                f"Probably the key {key!r} has been removed from the Redis database, "
                f"or every binding in it went stale."
            )
            raise InconsistencyError(msg)
        return queues

    def _exchange_is_durable(self, exchange: str) -> bool:
        """Whether the exchange was declared durable.

        An exchange this process never declared has no state entry. Assume
        durable then, which keeps the raise-and-redeclare path the default for
        exchanges whose bindings are supposed to outlive their consumers.
        """
        entry = (self.state.exchanges or {}).get(exchange)
        if not entry:
            return True
        return bool(entry.get("durable", True))

    def _read_bindings(self, client: Any, exchange: str) -> Any:
        """Read the live bindings of an exchange, dropping the ones that aged out.

        Pruning rides the read path because nothing else can reach these members:
        a binding is only ever unbound by the process that declared it, and the
        ones that pile up are precisely the ones whose process is gone. The
        removal costs no extra round trip, and in steady state it removes
        nothing, so Redis has nothing to propagate.

        The pruned members are read back first and logged: dropping a binding
        silently reroutes messages, so the log line is the only way to tell an
        aged-out route from one that never existed.
        """
        key = self._binding_key(exchange)
        now = time()
        try:
            # Not a transaction: this runs on the publish path, and a bind landing
            # between the commands is indistinguishable from one landing just
            # after the read.
            with client.pipeline(transaction=False) as pipe:
                pipe.zrangebyscore(key, "-inf", now)
                pipe.zremrangebyscore(key, "-inf", now)
                pipe.zrange(key, 0, -1)
                stale, _removed, live = pipe.execute()
        except _client_exceptions.ResponseError as exc:
            if not _is_wrongtype(exc):
                raise
            # Table still a plain set from an older deployment or from kombu's
            # own Redis transport. Readable as it is; the next bind converts it.
            return client.smembers(key)
        if stale:
            logger.info(
                "Exchange %r: dropped %d abandoned binding(s): %s",
                exchange,
                len(stale),
                ", ".join(sorted(bytes_to_str(member) for member in stale)),
            )
        return live

    def get_table(self, exchange: str) -> list[tuple[str, str, str]]:
        with self.conn_or_acquire() as client:
            values = self._read_bindings(client, exchange)
            if not values:
                return []
            result: list[tuple[str, str, str]] = []
            binding_parts_count = 3  # routing_key, pattern, queue
            for val in values:
                member = bytes_to_str(val)
                parts = member.split(self.sep)
                if len(parts) != binding_parts_count:
                    # Almost always a `sep` mismatch: another transport (usually kombu's Redis
                    # transport, or an older deployment of this one) wrote the member with a
                    # different separator. Padding keeps publishing alive, but the binding
                    # matches no routing key, so messages for it are dropped silently.
                    if not Channel._warned_binding_sep:
                        logger.warning(
                            "Binding %r on exchange %r does not split into %s parts with the"
                            " configured sep %r, so it cannot be matched and messages routed to"
                            " it are dropped. This usually means the binding was written with a"
                            " different sep -- check that `sep` matches across every transport"
                            " sharing this broker."
                            " This warning is shown once; other bindings may also be affected.",
                            member,
                            exchange,
                            binding_parts_count,
                            self.sep,
                        )
                        Channel._warned_binding_sep = True
                    # Pad/truncate to exactly 3 parts (routing_key, pattern, queue)
                    parts = [*parts, "", ""][:binding_parts_count]
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
        # blocking_timeout 0 holds the poll open until a message arrives, so a
        # drain could hang forever; the sweep restores an unread pop instead
        if self._in_poll and self.blocking_timeout != 0:
            with suppress(Empty, *_connection_errors):
                self._bzmpop_read()
        if self._in_fanout_poll and self.blocking_timeout != 0:
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

    #: Seconds BZMPOP and XREAD block on the server while a poll is outstanding.
    blocking_timeout = DEFAULT_BLOCKING_TIMEOUT

    #: kombu's sleep between unsuccessful polls, disabled as in kombu's own Redis
    #: transport: a sleep on top of a blocking read delays a reply already on its way.
    polling_interval = None
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

        transport_options = self.client.transport_options if self.client else None
        blocking_timeout = (transport_options or {}).get("blocking_timeout")
        if blocking_timeout is None and self.polling_interval is not None:
            # polling_interval was this transport's block timeout before
            # blocking_timeout existed. kombu reads it in virtual.Transport
            # and also sleeps that long between unsuccessful polls, so keep
            # honouring the value but put the sleep back to off.
            global _warned_polling_interval  # noqa: PLW0603
            if not _warned_polling_interval:
                logger.warning(
                    "The polling_interval transport option is deprecated, use blocking_timeout."
                    " In kombu it is the sleep between unsuccessful polls, here it used to be"
                    " how long BZMPOP and XREAD block. Reading it as blocking_timeout=%s and"
                    " leaving the sleep disabled.",
                    self.polling_interval,
                )
                _warned_polling_interval = True
            blocking_timeout = self.polling_interval
        self.polling_interval = None
        if blocking_timeout is not None:
            self.blocking_timeout = blocking_timeout

        self.cycle = MultiChannelPoller()
        # Shared by every channel of this connection, see Channel.__init__
        self._bindings: dict[str, set[tuple[str, str]]] = {}
        self._expires: dict[str, int] = {}
        self._message_ttls: dict[str, int] = {}

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

        cycle._on_connection_disconnect = _on_disconnect  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

        def on_poll_start() -> None:
            cycle_poll_start()
            for fd in cycle.fds:
                add_reader(fd, on_readable, fd)

        loop.on_tick.add(on_poll_start)

        # Unified requeue check handles both delayed messages and timed-out messages
        loop.call_repeatedly(DEFAULT_REQUEUE_CHECK_INTERVAL, cycle.maybe_enqueue_due_messages)

        # Heartbeat to keep in-flight messages alive
        visibility_timeout = connection.client.transport_options.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        loop.call_repeatedly(visibility_timeout / 3, cycle.maybe_update_messages_index)

        # Store loop for dynamic timer registration (queue TTL refresh)
        cycle._loop = loop

        # _update_expires_timer no-ops while cycle._loop is None, and celery
        # declares every queue in the Tasks bootstep, which runs before asynloop
        # gets here. Without this call the timer only ever started by accident,
        # when a queue happened to be declared later at runtime.
        cycle._update_expires_timer()

    def on_readable(self, fileno: int) -> Any:  # type: ignore[override]  # ty: ignore[invalid-method-override]
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)
