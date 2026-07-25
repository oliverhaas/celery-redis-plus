"""Redis Streams transport with consumer groups, PEL reliability, and native delayed delivery.

This transport is a second broker transport alongside the sorted-set transport,
built on Redis Streams with consumer groups:
1. XREADGROUP + consumer groups for regular queues - delivery and Pending Entries
   List (PEL) registration happen in one atomic step, so the broker itself tracks
   in-flight work
2. One stream per (queue, priority level) - full 0-255 priority support bucketed
   onto configurable priority steps
3. Native delayed delivery - staging sorted set pumped into the priority streams
4. Redis Streams for fanout exchanges - reliable broadcast via XREAD (shared with
   the sorted-set transport)

Requires Redis 7.0+ (or Valkey) for consumer group and XAUTOCLAIM support.
Supports both redis-py and valkey-py client libraries.

Configuration
=============
For Valkey, use the ``valkey-streams://`` URL scheme::

    broker_url = "valkey-streams://localhost:6379/0"

Use ``valkeys-streams://`` to enable SSL.

As a convenience, the ``valkey+streams`` and ``valkeys+streams`` aliases can also
be used as ``broker_transport`` values with a plain ``valkey://`` URL (kombu
splits bare URL schemes at ``+`` before alias lookup, so the ``+`` forms only
work at the ``broker_transport`` level)::

    broker_url = "valkey://localhost:6379/0"
    broker_transport = "valkey+streams"

For Redis, set ``broker_transport`` with a standard ``redis://`` URL::

    broker_url = "redis://localhost:6379/0"
    broker_transport = "celery_redis_plus.streams:Transport"

Transport Options
=================
* ``priority_steps``: Priority buckets in 0-255 space (default: ``[0, 3, 6, 9]``)
* ``visibility_timeout``: Seconds of heartbeat silence before in-flight messages are
  reclaimed from a dead worker (default: 300)
* ``heartbeat_interval``: XCLAIM JUSTID heartbeat cadence in seconds
  (default: ``visibility_timeout / 5``)
* ``max_restore_count``: Delivery-count cap before poisoned messages are dropped or
  dead-lettered (default: None = no limit)
* ``dead_letter_stream``: Stream to copy poisoned messages to (default: None)
* ``consumer_group``: Consumer group name on every queue stream (default: "celery")
* ``consumer_name``: Stable per-worker consumer identity (default: the worker
  nodename when available, else ``hostname:pid``)
* ``global_keyprefix``: Global prefix for all Redis keys
* ``message_ttl``: Message TTL in seconds, enforced lazily at delivery (default: None)
* ``stream_maxlen``: Maximum stream length for fanout streams (default: 10000)
"""

from __future__ import annotations

import functools
import logging
import numbers
import os
import socket as socket_module
from contextlib import contextmanager, suppress
from pathlib import Path
from queue import Empty
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, cast

from kombu.exceptions import VersionMismatch
from kombu.transport import virtual
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
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_MAX_RESTORE_COUNT,
    DEFAULT_PRIORITY_STEPS,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_VISIBILITY_TIMEOUT,
    DELAYED_KEY_PREFIX,
    MAX_PRIORITY,
    STREAM_KEY_PREFIX,
)
from .signals import _get_worker_nodename_for_channel
from .transport import (
    DEFAULT_DB,
    DEFAULT_PORT,
    CredentialProvider,
    FanoutStreamsMixin,
    PrefixedStrictRedis,
    _channel_errors,
    _client_exceptions,
    _client_lib_name,
    _connection_errors,
    client_lib,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from kombu import Connection

logger = logging.getLogger(__name__)

# Load Lua scripts at module init
_PACKAGE_DIR = Path(__file__).parent
_STREAMS_CONSUME_LUA = (_PACKAGE_DIR / "streams_consume.lua").read_text()
_STREAMS_ACK_LUA = (_PACKAGE_DIR / "streams_ack.lua").read_text()


def priority_to_level(priority: int, steps: Sequence[int]) -> int:
    """Bucket a message priority onto a priority level.

    A message goes to the highest step <= its priority, or the lowest step
    if its priority is below all steps (RabbitMQ semantics, 0-255 space).

    Args:
        priority: Message priority (0-255, higher = more urgent).
        steps: Priority steps sorted ascending (e.g. [0, 3, 6, 9]).

    Returns:
        The step value used as the stream's priority level.
    """
    level = steps[0]
    for step in steps:
        if step <= priority:
            level = step
        else:
            break
    return level


def _after_fork_cleanup_channel(channel: Channel) -> None:
    channel._after_fork()


class QoS(virtual.QoS):
    """Streams QoS backed by the broker's Pending Entries List (PEL).

    XREADGROUP registers every delivered entry as pending in the consumer group
    atomically, so the broker itself tracks in-flight work. This class keeps the
    local metadata needed to ack (XACK+XDEL) later:
    delivery_tag -> (stream key, message id).
    """

    channel: Channel  # Narrow type from base class for our custom Channel
    restore_at_shutdown = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # delivery_tag -> (unprefixed stream key, stream message id) for in-flight messages
        self._in_flight: dict[str, tuple[str, str]] = {}
        # For streams fanout: track delivery tags that came from fanout (no ack needed)
        self._fanout_tags: set[str] = set()

    def ack(self, delivery_tag: str) -> None:
        # Fanout messages don't need Redis cleanup (no consumer groups)
        if delivery_tag in self._fanout_tags:
            self._fanout_tags.discard(delivery_tag)
        elif delivery_tag in self._in_flight:
            # Regular stream message: atomic Lua removes the PEL entry
            # (XACK) and the stream entry (XDEL) in one round trip
            self._ack_by_tag(delivery_tag)
        else:
            # Metadata lost (e.g. channel restart). The PEL entry stays and
            # will be reclaimed by a peer after visibility_timeout.
            logger.critical("Cannot ack message: no in-flight metadata for delivery_tag %r", delivery_tag)
        super().ack(delivery_tag)

    def _ack_by_tag(self, delivery_tag: str, requeue_payload: str = "") -> None:
        """Atomically XACK and XDEL a message's stream entry via Lua.

        When requeue_payload is non-empty, the script XADDs the copy to the
        stream tail before acking, so a connection drop cannot tear requeue
        and ack apart. The copy starts with a fresh delivery count, so
        voluntary requeues never count toward the poison cap.
        """
        stream_key, message_id = self._in_flight.pop(delivery_tag)
        # Prefix key since EVALSHA doesn't auto-prefix KEYS
        prefixed_stream_key = f"{self.channel.global_keyprefix}{stream_key}"
        with self.channel.conn_or_acquire() as client:
            ack_script = client.register_script(_STREAMS_ACK_LUA)
            ack_script(
                keys=[prefixed_stream_key],
                args=[self.channel.consumer_group, message_id, requeue_payload],
            )

    def reject(self, delivery_tag: str, requeue: bool = False) -> None:
        # Fanout messages: requeue not supported (fire-and-forget broadcast)
        if delivery_tag in self._fanout_tags:
            self._fanout_tags.discard(delivery_tag)
            super().ack(delivery_tag)
            return
        if delivery_tag not in self._in_flight:
            # Metadata lost (e.g. channel restart). The PEL entry stays and
            # will be reclaimed by a peer after visibility_timeout.
            logger.critical("Cannot reject message: no in-flight metadata for delivery_tag %r", delivery_tag)
            super().ack(delivery_tag)
            return
        requeue_payload = ""
        if requeue:
            if self._delivered is not None and delivery_tag in self._delivered:
                # Serialize the locally held message so the XADD copy, XACK,
                # and XDEL run in ONE atomic script. Never re-read the entry
                # from the stream: a re-read races against peer claims.
                requeue_payload = dumps(self._delivered[delivery_tag]._raw)
            else:
                logger.critical(
                    "Cannot requeue message: no delivered message for delivery_tag %r, acking without requeue",
                    delivery_tag,
                )
        self._ack_by_tag(delivery_tag, requeue_payload=requeue_payload)
        super().ack(delivery_tag)

    @cached_property
    def visibility_timeout(self) -> float:
        return self.channel.visibility_timeout


class MultiChannelPoller:
    """Async I/O poller for the streams transport."""

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

    def on_poll_start(self) -> None:
        """Start blocking reads for all consuming channels.

        Placeholder: XREADGROUP/XREAD registration is added with the consume
        cycle; until then there are no sockets to watch.
        """

    def on_poll_init(self, poller: Any) -> None:
        self.poller = poller
        # Initial pump on startup (delayed messages and reclaim)
        self.maybe_enqueue_due_messages()

    def maybe_enqueue_due_messages(self) -> int:
        """Move due delayed messages and reclaim timed-out deliveries.

        Placeholder: filled in with the delayed pump and reclaim; the periodic
        timer can already call it safely.

        Returns:
            Total number of messages moved across all channels.
        """
        return 0

    def maybe_heartbeat(self) -> None:
        """Send XCLAIM JUSTID heartbeats for in-flight messages.

        Placeholder: filled in with the heartbeat implementation; the periodic
        timer can already call it safely.
        """

    def maybe_refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on stream keys with x-expires TTL.

        Placeholder: filled in with queue TTL support; the periodic timer can
        already call it safely.
        """

    def _update_expires_timer(self) -> None:
        """Register or update the periodic PEXPIRE timer based on configured TTLs.

        Placeholder: filled in with queue TTL support.
        """

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
        """Drain events synchronously (no event loop).

        Placeholder: queue/fanout registration is added with the consume cycle;
        until then there is never anything to read, but after_read promises are
        still honored.
        """
        self._in_protected_read = True
        try:
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


class Channel(FanoutStreamsMixin, virtual.Channel):
    """Redis Channel with XREADGROUP priority streams and Streams fanout.

    Uses:
    - XREADGROUP + consumer groups for regular queues (PEL-based reliability)
    - One stream per (queue, priority level), consumed highest level first
    - Redis Streams for fanout (true broadcast via XREAD)
    - Native delayed delivery via a per-queue staging sorted set
    """

    QoS = QoS
    # qos is inherited from base class and will be an instance of our QoS
    connection: Transport  # Narrow type from base class for our custom Transport

    _client: Any = None
    keyprefix_queue = "_kombu.binding.%s"
    keyprefix_fanout = "/{db}."
    sep = "\x06\x16"
    _in_poll = None
    _in_fanout_poll = None
    max_priority = MAX_PRIORITY  # Override kombu's default of 9 to enable full 0-255 range

    # Message TTL in seconds enforced lazily at delivery (None = no TTL)
    message_ttl: int | None = None

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

    # Streams configuration (fanout)
    stream_maxlen = DEFAULT_STREAM_MAXLEN

    # Global key prefix
    global_keyprefix = ""

    # Credential provider for dynamic auth (e.g. AWS ElastiCache IAM, Azure Redis)
    credential_provider = None

    # Max restore count (None = no limit)
    max_restore_count: int | None = DEFAULT_MAX_RESTORE_COUNT

    # Dead-letter stream for poisoned messages (None = drop without copy)
    dead_letter_stream: str | None = None

    # Fanout settings
    fanout_prefix: bool | str = True
    fanout_patterns = True

    _async_pool: Any = None
    _pool: Any = None

    from_transport_options = virtual.Channel.from_transport_options + (
        "sep",
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
        "dead_letter_stream",
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
        self.handlers = {"XREADGROUP": self._xreadgroup_read, "XREAD": self._xread_read}
        # Track last-read stream ID per stream for fanout (start with $ = only new messages)
        self._stream_offsets: dict[str, str] = {}
        # Per-queue TTL state from x-expires and x-message-ttl queue arguments
        self._expires: dict[str, int] = {}  # queue_name -> TTL in ms
        self._message_ttls: dict[str, int] = {}  # queue_name -> message TTL in ms
        # Streams whose consumer group has already been created (XGROUP CREATE cache)
        self._ensured_groups: set[str] = set()

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

    def _stream_key(self, queue: str, level: int) -> str:
        """Get the Redis key for a queue's stream at a priority level."""
        return f"{STREAM_KEY_PREFIX}{queue}:{level}"

    def _delayed_key(self, queue: str) -> str:
        """Get the Redis key for a queue's delayed-message staging sorted set."""
        return f"{DELAYED_KEY_PREFIX}{queue}"

    def _stream_keys_for_queue(self, queue: str) -> list[str]:
        """Get all priority-level stream keys for a queue, highest level first."""
        return [self._stream_key(queue, level) for level in reversed(self.priority_steps)]

    def _ensure_group(self, stream_key: str) -> None:
        """Create the consumer group on a stream if not already ensured.

        Uses XGROUP CREATE with MKSTREAM so the stream is created together
        with the group, starting at id 0 so every entry already in the
        stream is deliverable. BUSYGROUP errors (group already exists) are
        ignored. Ensured stream keys are cached per channel to avoid a
        round-trip on every publish.
        """
        if stream_key in self._ensured_groups:
            return
        with self.conn_or_acquire() as client:
            try:
                # Transport-owned streams: "0" equals "$" at first creation, but makes re-created streams' pre-existing entries deliverable
                client.xgroup_create(stream_key, self.consumer_group, id="0", mkstream=True)
            except self.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise
        self._ensured_groups.add(stream_key)

    def _invalidate_group(self, queue: str | None = None) -> None:
        """Drop cached ensured-group state so _ensure_group re-creates groups.

        Called when a NOGROUP error reveals that a stream (and with it its
        consumer groups) was deleted out of band: a cross-process purge or
        queue delete, or an x-expires TTL firing. With a queue given, only
        that queue's level stream keys are discarded; with None the whole
        cache is cleared (used when the failing stream cannot be singled
        out, e.g. a blocking XREADGROUP armed across all watched queues).
        """
        if queue is None:
            self._ensured_groups.clear()
            return
        for stream_key in self._stream_keys_for_queue(queue):
            self._ensured_groups.discard(stream_key)

    def _put(self, queue: str, message: dict[str, Any], **kwargs: Any) -> None:
        """Publish a message to a queue's priority stream or the delayed zset.

        Immediate messages are XADDed to the stream for their priority level
        (consumer group ensured first, so the entry is registered in the PEL
        on first XREADGROUP). Native delayed messages (delay > requeue check
        interval) are ZADDed to the delayed zset as the full serialized
        message, scored by absolute delivery time in milliseconds; the
        delayed pump moves them into their priority stream when due.

        Args:
            queue: Target queue name.
            message: Message dict with 'properties' containing optional 'eta'
                     (Unix timestamp float) for delayed delivery.
        """
        now = time()

        # Shorter delays fall through to Celery's own eta logic (immediate delivery)
        eta_timestamp: float | None = message["properties"].get("eta")

        if eta_timestamp is not None and (eta_timestamp - now) > DEFAULT_REQUEUE_CHECK_INTERVAL:
            # Member is the self-contained serialized message; score is the
            # absolute delivery time in milliseconds
            delayed_key = self._delayed_key(queue)
            with self.conn_or_acquire() as client:
                client.zadd(delayed_key, {dumps(message): eta_timestamp * 1000})
                if queue in self._expires:
                    client.pexpire(delayed_key, self._expires[queue])
            return

        priority = self._get_message_priority(message, reverse=False)
        level = priority_to_level(priority, self.priority_steps)
        stream_key = self._stream_key(queue, level)
        # Group must exist before XADD so the entry is visible to XREADGROUP '>'
        self._ensure_group(stream_key)
        with self.conn_or_acquire() as client:
            client.xadd(name=stream_key, fields={"payload": dumps(message)}, id="*")
            if queue in self._expires:
                client.pexpire(stream_key, self._expires[queue])

    def _get(self, queue: str, timeout: float | None = None) -> dict[str, Any]:
        """Get a single message from a queue (synchronous).

        Uses the streams_consume Lua script: atomic XREADGROUP over the
        queue's priority streams (highest level first) with lazy
        message-TTL drop. The read registers the entry in the consumer
        group's PEL, and the QoS in-flight metadata is recorded before
        the message is returned so ack/reject can XACK+XDEL it.

        A NOGROUP reply means a stream (and with it its consumer group)
        was deleted out of band (cross-process purge, queue delete,
        x-expires expiry) while this channel held a warm _ensured_groups
        cache; the cache is invalidated, the groups are re-created, and
        the script is retried once.
        """
        stream_keys = self._stream_keys_for_queue(queue)
        # Ensure groups exist before the read. NOGROUP can still happen if
        # a stream is deleted out of band; that case is self-healed below.
        for stream_key in stream_keys:
            self._ensure_group(stream_key)

        # Effective message TTL in ms: min of channel-level message_ttl (seconds)
        # and per-queue x-message-ttl (ms); 0 = no TTL
        ttl_ms = 0 if self.message_ttl is None else int(self.message_ttl * 1000)
        queue_ttl_ms = self._message_ttls.get(queue)
        if queue_ttl_ms is not None:
            ttl_ms = queue_ttl_ms if ttl_ms == 0 else min(ttl_ms, queue_ttl_ms)

        with self.conn_or_acquire() as client:
            consume_script = client.register_script(_STREAMS_CONSUME_LUA)
            # Prefix keys manually since EVALSHA doesn't auto-prefix KEYS
            keys = [f"{self.global_keyprefix}{stream_key}" for stream_key in stream_keys]
            args = [
                self.consumer_group,
                self.consumer_name,
                ttl_ms,
            ]
            try:
                result = consume_script(keys=keys, args=args)
            except self.ResponseError as exc:
                if "NOGROUP" not in str(exc):
                    raise
                # Stream deleted out of band: drop the stale cache, recreate
                # the groups, and retry once. A second NOGROUP propagates.
                self._invalidate_group(queue)
                for stream_key in stream_keys:
                    self._ensure_group(stream_key)
                result = consume_script(keys=keys, args=args)
            if not result:
                raise Empty
            # Script returns [stream_key, entry_id, payload]; the key is prefixed
            hit_key = bytes_to_str(result[0])
            if self.global_keyprefix and hit_key.startswith(self.global_keyprefix):
                hit_key = hit_key[len(self.global_keyprefix) :]
            entry_id = bytes_to_str(result[1])
            message: dict[str, Any] = loads(bytes_to_str(result[2]))
            # Record PEL metadata (unprefixed stream key) before returning
            # so QoS ack/reject can XACK+XDEL the entry
            delivery_tag = message["properties"]["delivery_tag"]
            cast("QoS", self.qos)._in_flight[delivery_tag] = (hit_key, entry_id)
            return message

    @property
    def priority_steps(self) -> list[int]:
        """Priority buckets in 0-255 space, sorted ascending."""
        steps = self.connection.client.transport_options.get(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            "priority_steps",
            DEFAULT_PRIORITY_STEPS,
        )
        return sorted(int(step) for step in steps)

    @cached_property
    def consumer_group(self) -> str:
        """Consumer group name used on every queue stream."""
        return str(
            self.connection.client.transport_options.get(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
                "consumer_group",
                DEFAULT_CONSUMER_GROUP,
            ),
        )

    @cached_property
    def consumer_name(self) -> str:
        """Stable per-worker consumer identity on every queue stream.

        Resolution order: the ``consumer_name`` transport option, else the
        worker nodename recorded by the ``celeryd_after_setup`` signal, else
        ``hostname:pid``. Computed once per channel (kombu ``cached_property``);
        a stable name (never uuid-per-boot) ensures a restarted worker resumes
        its own PEL instead of leaking consumers and orphaning pending entries.
        """
        name = self.connection.client.transport_options.get(  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            "consumer_name",
        )
        if name:
            return str(name)
        nodename = _get_worker_nodename_for_channel(self)
        if nodename:
            return nodename
        return f"{socket_module.gethostname()}:{os.getpid()}"

    @property
    def active_queues(self) -> set[str]:
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues if queue not in self.active_fanout_queues}

    def _update_queue_cycle(self) -> None:
        """Rebuild the round-robin queue cycle from the watched non-fanout queues."""
        self._queue_cycle = list(self.active_queues)

    def basic_consume(self, queue: str, *args: Any, **kwargs: Any) -> str:
        if queue in self._fanout_queues:
            self.active_fanout_queues.add(queue)
        ret = super().basic_consume(queue, *args, **kwargs)
        self._update_queue_cycle()
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
        self._update_queue_cycle()
        return ret

    def _xreadgroup_start(self, timeout: float | None = None) -> None:
        """Send a blocking XREADGROUP over all watched level streams.

        Called when the non-blocking pass found every queue empty. Data on
        any stream wakes the poller fd; strict priority order is restored on
        the next non-blocking pass.
        """
        if timeout is None:
            timeout = self.connection.polling_interval or 1
        if not self._queue_cycle:
            return

        stream_keys: list[str] = []
        for queue in self._queue_cycle:
            for stream_key in self._stream_keys_for_queue(queue):
                self._ensure_group(stream_key)
                stream_keys.append(stream_key)

        command_args: list[Any] = [
            "XREADGROUP",
            "GROUP",
            self.consumer_group,
            self.consumer_name,
            "BLOCK",
            str(int(timeout * 1000)),
            "COUNT",
            "1",
            "STREAMS",
            *stream_keys,
            *([">"] * len(stream_keys)),
        ]

        if self.global_keyprefix:
            command_args = self.client._prefix_args(command_args)

        self.client.connection.send_command(*command_args)
        self._in_poll = self.client.connection

    def _xreadgroup_read(self, **options: Any) -> bool:
        """Parse the blocking XREADGROUP reply and deliver its entries.

        Every returned entry is already registered in this consumer's PEL
        (deliver + register was atomic inside Redis), so each one is
        delivered and recorded in QoS._in_flight; dropping any would strand
        it until reclaim.

        A NOGROUP reply means a watched stream (and its consumer group)
        was deleted out of band while the blocking read was armed; the
        whole ensured-group cache is invalidated and Empty is raised, so
        the next non-blocking pass re-creates the groups before touching
        the streams.
        """
        try:
            try:
                messages = self.client.parse_response(self.client.connection, "XREADGROUP", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                raise
            except self.ResponseError as exc:
                if "NOGROUP" not in str(exc):
                    raise
                # The failing stream cannot be singled out from the error,
                # so drop the whole cache; _ensure_group re-creates lazily
                self._invalidate_group()
                raise Empty from None

            if not messages:
                raise Empty

            delivered = False
            for stream, message_list in messages:
                stream_str = bytes_to_str(stream) if isinstance(stream, bytes) else stream
                # Reply stream names carry the global key prefix; strip so
                # _in_flight always stores unprefixed keys
                prefix = self.global_keyprefix
                if prefix and stream_str.startswith(prefix):
                    stream_str = stream_str[len(prefix) :]
                # stream:{queue}:{level} -> logical queue name
                queue_name = stream_str[len(STREAM_KEY_PREFIX) :].rsplit(":", 1)[0]
                for message_id, fields in message_list:
                    message_id_str = bytes_to_str(message_id) if isinstance(message_id, bytes) else message_id
                    payload_field = fields.get(b"payload") or fields.get("payload")
                    if not payload_field:
                        continue
                    message: dict[str, Any] = loads(bytes_to_str(payload_field))
                    delivery_tag = message["properties"]["delivery_tag"]
                    if self.qos is not None:
                        cast("QoS", self.qos)._in_flight[delivery_tag] = (stream_str, message_id_str)
                    self.connection._deliver(message, queue_name)
                    delivered = True

            if not delivered:
                raise Empty
            return True
        finally:
            self._in_poll = None

    def _poll_error(self, cmd_type: str, **options: Any) -> Any:
        if cmd_type == "XREAD":
            client = self.subclient
        else:
            client = self.client
            # Without a pending blocking read the last command on this socket
            # was the non-blocking EVALSHA consume pass
            if not self._in_poll:
                cmd_type = "EVALSHA"
        try:
            return client.parse_response(client.connection, cmd_type)
        except self.ResponseError as exc:
            if "NOGROUP" not in str(exc):
                raise
            # A watched stream (and its consumer group) was deleted out of
            # band; clear the cache so the next pass re-ensures the groups
            self._invalidate_group()
            raise Empty from None

    def close(self) -> None:
        if self._in_poll:
            with suppress(Empty, *_connection_errors):
                self._xreadgroup_read()
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

        # Check for SSL configuration from URL scheme (rediss://) or transport_options
        ssl_config = conninfo.ssl
        if not ssl_config:
            # Check if using an SSL streams scheme: the valkeys-streams URL scheme
            # or the valkeys+streams broker_transport alias
            transport_cls = getattr(self.connection, "transport_cls", None)
            if transport_cls in ("valkeys-streams", "valkeys+streams"):
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
        """Client used to publish messages, XREADGROUP etc."""
        return self._create_client(asynchronous=True)

    @cached_property
    def subclient(self) -> Any:
        """Dedicated client for XREAD fanout polling (needs its own connection)."""
        return self._create_client(asynchronous=True)


class Transport(virtual.Transport):
    """Redis Streams Transport with consumer groups, priority streams, and delayed delivery.

    Uses:
    - XREADGROUP + consumer groups for regular queues (PEL-based reliability)
    - One stream per (queue, priority level) for full 0-255 priority support
    - Redis Streams for fanout (true broadcast via XREAD)
    - Native delayed delivery via a staging sorted set

    Requires Redis 7.0+ (or Valkey) for consumer group and XAUTOCLAIM support.
    """

    Channel = Channel

    polling_interval = 10  # Timeout for blocking XREADGROUP/XREAD calls in seconds
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

        cycle._on_connection_disconnect = _on_disconnect  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

        def on_poll_start() -> None:
            cycle_poll_start()
            for fd in cycle.fds:
                add_reader(fd, on_readable, fd)

        loop.on_tick.add(on_poll_start)

        # Periodic pump: delayed messages, PEL reclaim, and consumer hygiene
        loop.call_repeatedly(DEFAULT_REQUEUE_CHECK_INTERVAL, cycle.maybe_enqueue_due_messages)

        # Store loop for dynamic timer registration (queue TTL refresh)
        cycle._loop = loop

    def on_readable(self, fileno: int) -> Any:  # type: ignore[override]  # ty: ignore[invalid-method-override]
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)
