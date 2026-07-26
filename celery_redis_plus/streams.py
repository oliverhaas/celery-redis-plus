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

Requires Redis 6.2+ (or Valkey) for the XPENDING IDLE filter and exclusive
stream ID ranges used by the reclaim pass.
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
  (default: ``visibility_timeout / 5``). Must be a positive finite number no
  greater than half of ``visibility_timeout``; an out-of-range value (including
  a non-positive, non-finite, or non-numeric ``visibility_timeout``) is
  overridden back to the derived default and a warning is logged
* ``max_restore_count``: Delivery-count cap before poisoned messages are dropped or
  dead-lettered (default: None = no limit)
* ``dead_letter_stream``: Stream to copy poisoned messages to (default: None).
  Capped at ``DEFAULT_STREAM_MAXLEN`` entries (approximate trim) and prefixed
  with ``global_keyprefix`` like every other key. Must not start with the
  ``stream:`` queue namespace: a poisoned message copied into a queue's own
  level stream would be redelivered and re-dead-lettered forever
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
import math
import numbers
import os
import socket as socket_module
from contextlib import ExitStack, contextmanager, suppress
from pathlib import Path
from queue import Empty
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, cast

from kombu.exceptions import VersionMismatch
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
    CONSUMER_IDLE_CLEANUP_FACTOR,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_HEALTH_CHECK_INTERVAL,
    DEFAULT_MAX_RESTORE_COUNT,
    DEFAULT_PRIORITY_STEPS,
    DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT,
    DEFAULT_REQUEUE_BATCH_LIMIT,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    DEFAULT_STREAM_MAXLEN,
    DEFAULT_UNBOUNDED_PREFETCH_DRAIN_LIMIT,
    DEFAULT_VISIBILITY_TIMEOUT,
    DELAYED_KEY_PREFIX,
    HEARTBEAT_INTERVAL_DIVISOR,
    MAX_PRIORITY,
    MIN_QUEUE_EXPIRES,
    SHUTDOWN_IDLE_MS,
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
    _collect_transport,
    _connection_errors,
    _drain_hub_callbacks,
    _get_worker_pool_for_channel,
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
_STREAMS_MOVE_DELAYED_LUA = (_PACKAGE_DIR / "streams_move_delayed.lua").read_text()
_STREAMS_CLEANUP_CONSUMERS_LUA = (_PACKAGE_DIR / "streams_cleanup_consumers.lua").read_text()


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


def _is_finite_positive(value: Any) -> bool:
    """Return True when value is a real, finite, strictly positive number."""
    # numbers.Real admits float("inf"), which passes every ordinary comparison a
    # timer interval is validated with and yields a timer that never fires.
    return isinstance(value, numbers.Real) and math.isfinite(value) and value > 0


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
        if self.channel._collected:
            # Connection lost, not a shutdown: the broker still owns this PEL
            # entry for a peer to reclaim. Not `closed`, which close() sets
            # before restore_unacked_once() and would drop the restore's acks.
            logger.debug("Skipping ack for delivery_tag %r: channel was collected", delivery_tag)
            self._fanout_tags.discard(delivery_tag)
            super().ack(delivery_tag)
            return
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
        if self.channel._collected:
            # See ack(): no client left to talk to. A peer reclaims the PEL
            # entry after the visibility timeout, requeue or not.
            logger.debug("Skipping reject for delivery_tag %r: channel was collected", delivery_tag)
            self._fanout_tags.discard(delivery_tag)
            super().ack(delivery_tag)
            return
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
                # Serialize the local copy so XADD, XACK, and XDEL run in one
                # script; re-reading the entry would race against peer claims.
                requeue_payload = dumps(self._delivered[delivery_tag]._raw)
            else:
                logger.critical(
                    "Cannot requeue message: no delivered message for delivery_tag %r, acking without requeue",
                    delivery_tag,
                )
        self._ack_by_tag(delivery_tag, requeue_payload=requeue_payload)
        super().ack(delivery_tag)

    def restore_unacked_once(self, stderr: Any = None) -> None:
        """Release in-flight messages for instant reclaim, waiting for threads first.

        Celery's shutdown order fires restore_unacked_once (during Consumer
        close) BEFORE Pool.on_stop() waits for threads.  By calling
        executor.shutdown(wait=True) here first, all threads complete and
        their ack callbacks land in hub._ready.  The second drain catches
        them, so only truly unfinished messages are released.
        executor.shutdown() is idempotent, so Pool.on_stop()'s later call
        is a no-op.

        Unlike the sorted set transport, no payload is re-added: the
        remaining PEL entries are XCLAIMed with an artificial idle time
        (SHUTDOWN_IDLE_MS, far above any sane visibility timeout), which
        makes them instantly reclaimable by a peer's reclaim pass
        (Channel._reclaim_and_deliver's XPENDING-IDLE discovery followed by
        a counting XCLAIM). JUSTID transfers no payloads and does not itself
        bump delivery counts; the peer's reclaim redelivery does, so each
        graceful handoff costs one restore_count increment on surviving
        messages.
        """
        _drain_hub_callbacks(self.channel)

        if (
            (pool := _get_worker_pool_for_channel(self.channel)) is not None
            and (executor := getattr(pool, "executor", None)) is not None
            and hasattr(executor, "shutdown")
        ):
            executor.shutdown(wait=True)
            _drain_hub_callbacks(self.channel)

        # Mirror virtual.QoS.restore_unacked_once guards (once-only via the
        # restored flag on _delivered) but replace its re-add restore path
        # with the atomic XCLAIM IDLE release.  No super() call.
        self._on_collect.cancel()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        self._flush()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        state = self._delivered

        if not self.restore_at_shutdown or not self.channel.do_restore:
            return
        if getattr(state, "restored", None):
            return

        try:
            if self._in_flight:
                by_stream: dict[str, list[tuple[str, str]]] = {}
                for delivery_tag, (stream_key, message_id) in self._in_flight.items():
                    by_stream.setdefault(stream_key, []).append((delivery_tag, message_id))

                released_tags: list[str] = []
                try:
                    with self.channel.conn_or_acquire() as client:
                        for stream_key, entries in by_stream.items():
                            message_ids = [message_id for _, message_id in entries]
                            try:
                                # Release only; the peer's reclaim redelivery bumps times_delivered by one.
                                client.xclaim(
                                    stream_key,
                                    self.channel.consumer_group,
                                    self.channel.consumer_name,
                                    0,
                                    message_ids,
                                    idle=SHUTDOWN_IDLE_MS,
                                    justid=True,
                                )
                            except Exception:
                                logger.warning(
                                    "Failed to release in-flight messages on %s for instant reclaim;"
                                    " peers will reclaim them after the visibility timeout",
                                    stream_key,
                                    exc_info=True,
                                )
                            else:
                                released_tags.extend(tag for tag, _ in entries)
                except Exception:
                    # Must not escape: this runs inside Channel.close(), and
                    # raising would abort that close and every later channel's.
                    logger.warning(
                        "Could not acquire a connection to release in-flight messages for instant"
                        " reclaim; peers will reclaim them after the visibility timeout",
                        exc_info=True,
                    )

                if released_tags:
                    logger.info(
                        "Released %d in-flight message(s) for instant reclaim by peers",
                        len(released_tags),
                    )
                    for tag in released_tags:
                        self._in_flight.pop(tag, None)
        finally:
            state.restored = True  # type: ignore[attr-defined]  # ty: ignore[invalid-assignment]

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
        # Rotation offset into each channel's queue cycle; see maybe_enqueue_due_messages.
        # Keyed by the channel itself, so discard() drops the entry with no id() reuse hazard.
        self._requeue_offsets: dict[Channel, int] = {}

    def close(self) -> None:
        for fd in self._chan_to_sock.values():
            with suppress(KeyError, ValueError):
                self.poller.unregister(fd)
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()
        self._requeue_offsets.clear()

    def add(self, channel: Channel) -> None:
        self._channels.add(channel)

    def discard(self, channel: Channel) -> None:
        self._channels.discard(channel)
        self._requeue_offsets.pop(channel, None)

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

    def _register_XREADGROUP(self, channel: Channel) -> bool:
        """Enable XREADGROUP mode for channel (queue streams).

        Loops the non-blocking EVALSHA consume pass inline within this one
        hub tick, draining every already-available message up to the
        channel's QoS prefetch headroom (or DEFAULT_REQUEUE_BATCH_LIMIT,
        whichever is smaller), instead of delivering only the first hit.
        With prefetch unbounded (worker_prefetch_multiplier=0) there is no
        headroom to bound the loop, so DEFAULT_UNBOUNDED_PREFETCH_DRAIN_LIMIT
        stands in for it and keeps one tick from monopolising the hub.

        Channel._consume_read's EVALSHA is synchronous: it sends and parses
        the reply inline and returns on the first hit, unlike the sorted-set
        transport's _register_BZMPOP, which sends and defers the read to
        on_readable so the blocking reply itself wakes the hub again the
        instant more data is available. Without this loop, a burst of N
        already-queued messages would drain at only one message per hub
        tick: nothing here would prompt the next tick to run any sooner than
        the hub's own poll_timeout otherwise allows (bound by the nearest
        scheduled timer, e.g. the requeue-check interval, not by anything
        this transport controls), turning what should be a near-instant
        drain into roughly N x poll_timeout. When every queue is empty, the
        final _consume_read call arms a blocking XREADGROUP and keeps
        _in_poll set until the reply is parsed by _xreadgroup_read. Exiting
        via the cap arms one here instead, so the remaining backlog is not
        left waiting on poll_timeout. That is the one case where the blocking
        path is entered with messages still queued, so _xreadgroup_read may
        deliver up to one lower-priority entry per level stream ahead of
        higher-priority ones; the next non-blocking pass restores order.

        Returns:
            True if the non-blocking pass delivered at least one message.
        """
        ident = channel, channel.client, "XREADGROUP"
        if not self._client_registered(channel, channel.client, "XREADGROUP"):
            channel._in_poll = False
            self._register(*ident)
        if channel._in_poll:
            return False

        qos = channel.qos
        delivered_any = False
        hit_cap = True
        for iteration in range(DEFAULT_REQUEUE_BATCH_LIMIT):
            remaining = qos.can_consume_max_estimate() if qos is not None else None
            if remaining is not None and remaining <= 0:
                hit_cap = False
                break
            if remaining is None and iteration >= DEFAULT_UNBOUNDED_PREFETCH_DRAIN_LIMIT:
                # Unbounded prefetch has no headroom to trim this loop, so a
                # deep backlog would block the hub for DEFAULT_REQUEUE_BATCH_LIMIT
                # synchronous EVALSHAs. Yield early; the next tick resumes.
                break
            try:
                channel._consume_read()
            except Empty:
                hit_cap = False
                break
            delivered_any = True
        if hit_cap:
            # More may still be waiting, but arming at zero headroom busy-spins:
            # on_readable refuses the fd that on_poll_start keeps re-adding.
            headroom = None if qos is None else qos.can_consume_max_estimate()
            if headroom is None or headroom > 0:
                channel._xreadgroup_start()
        return delivered_any

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
                self._register_XREADGROUP(channel)
            if qos is not None and channel.active_fanout_queues and qos.can_consume():
                self._register_XREAD(channel)

    def on_poll_init(self, poller: Any) -> None:
        self.poller = poller
        # Initial pump on startup (delayed messages and reclaim)
        self.maybe_enqueue_due_messages()

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
                if (
                    qos is not None
                    and channel.active_queues
                    and qos.can_consume()
                    and self._register_XREADGROUP(channel)
                ):
                    # The non-blocking pass delivered a message
                    return
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

    def maybe_enqueue_due_messages(self) -> int:
        """Run the periodic requeue cycle: delayed pump plus reclaim.

        For each channel's watched queues this first moves due delayed messages
        into their priority streams, then reclaims messages whose consumer has
        been idle past the visibility timeout (skipped when the channel is
        already at its QoS prefetch_count limit, since reclaim delivers
        directly into the channel and must respect the same limit as any
        other delivery path).

        A single budget of DEFAULT_REQUEUE_BATCH_LIMIT is shared per channel
        per cycle: it starts at the constant, the delayed pump for each queue
        is called with whatever budget remains as its own limit (rather than
        always spending the full constant), and reclaim then spends what the
        pump left over. This means the budget is genuinely shared, but a
        single busy queue processed first can still spend it all before later
        queues in the same cycle are served. To keep that from starving a
        queue permanently, the per-channel queue order rotates by one position
        every cycle (tracked in self._requeue_offsets): a queue that starts a
        cycle at the back moves one place forward each time, so it eventually
        leads and gets first claim on the budget.

        Also runs consumer hygiene: idle consumers with no pending entries are
        removed so consumer groups do not grow without bound.

        Returns:
            Total number of messages moved or reclaimed across all channels.
        """
        total = 0
        for channel in self._channels:
            qos = channel.qos
            if qos is None or not channel.active_queues:
                continue
            queues = channel._queue_cycle
            if not queues:
                continue
            offset = self._requeue_offsets.get(channel, 0) % len(queues)
            rotated_queues = queues[offset:] + queues[:offset]
            budget = DEFAULT_REQUEUE_BATCH_LIMIT
            for queue in rotated_queues:
                if budget <= 0:
                    logger.warning(
                        "Requeue cycle hit batch limit of %d. There may be more messages waiting.",
                        DEFAULT_REQUEUE_BATCH_LIMIT,
                    )
                    break
                try:
                    moved = channel._move_delayed(queue, limit=budget)
                    budget -= moved
                    total += moved
                    if budget > 0 and qos.can_consume():
                        reclaimed = channel._reclaim_and_deliver(queue, budget)
                        budget -= reclaimed
                        total += reclaimed
                except Exception:
                    logger.warning(
                        "Failed to process due messages for queue %s, will retry next cycle",
                        queue,
                        exc_info=True,
                    )
            self._requeue_offsets[channel] = (offset + 1) % len(queues)
            channel._cleanup_consumers()
        return total

    def maybe_heartbeat(self) -> None:
        """Reset PEL idle time on in-flight messages to keep them alive (XCLAIM JUSTID)."""
        for channel in self._channels:
            channel._heartbeat()

    def maybe_refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on stream keys with x-expires TTL."""
        for channel in self._channels:
            channel._refresh_queue_expires()

    def _update_expires_timer(self) -> None:
        """Register or update the periodic PEXPIRE timer based on configured TTLs.

        Interval = min(all configured x-expires) / 2, so the TTL is refreshed
        ~2 times before it would expire.

        With no TTLs configured, or with the event loop not registered yet,
        any existing timer is cancelled and both _expires_timer_entry and
        _expires_timer_interval are reset to None in the same branch. Without
        that reset, a queue declared before register_with_event_loop runs
        would leave _expires_timer_interval stale, and the interval-equality
        check below could then suppress the real registration once the loop
        does show up.
        """
        min_ttl_ms: int | None = None
        for channel in self._channels:
            for ttl_ms in channel._expires.values():
                if min_ttl_ms is None or ttl_ms < min_ttl_ms:
                    min_ttl_ms = ttl_ms

        if min_ttl_ms is None or self._loop is None:
            if self._expires_timer_entry is not None:
                self._expires_timer_entry.cancel()
            self._expires_timer_entry = None
            self._expires_timer_interval = None
            return

        interval = min_ttl_ms / 2 / 1000  # ms -> seconds, divided by 2

        if self._expires_timer_interval == interval:
            return

        if self._expires_timer_entry is not None:
            self._expires_timer_entry.cancel()

        self._expires_timer_entry = self._loop.call_repeatedly(
            interval,
            self.maybe_refresh_queue_expires,
        )
        self._expires_timer_interval = interval


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
    _warned_expires_clamp = False
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

    # Set only by _release_channel_on_collect() (in transport.py, shared by
    # both transports). A class attribute rather than an __init__ assignment
    # so it defaults to False on test doubles built via object.__new__.
    _collected: bool = False

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
        self._consume_script_sha: str | None = None
        # Track last-read stream ID per stream for fanout (start with $ = only new messages)
        self._stream_offsets: dict[str, str] = {}
        # Per-queue TTL state from x-expires and x-message-ttl queue arguments
        self._expires: dict[str, int] = {}  # queue_name -> TTL in ms
        self._message_ttls: dict[str, int] = {}  # queue_name -> message TTL in ms
        # Streams whose consumer group has already been created (XGROUP CREATE cache)
        self._ensured_groups: set[str] = set()
        # Streams already reported as uninspectable by consumer cleanup (warn-once)
        self._warned_cleanup_streams: set[str] = set()
        # Queues already reported as hitting the delayed-move limit (warn-once)
        self._warned_delayed_limit: set[str] = set()

        if self.dead_letter_stream is not None and str(self.dead_letter_stream).startswith(STREAM_KEY_PREFIX):
            message = (
                f"dead_letter_stream {self.dead_letter_stream!r} must not start with "
                f"{STREAM_KEY_PREFIX!r}: that namespace holds the queue level streams this "
                f"transport consumes from, so poisoned messages copied there would be "
                f"redelivered and re-dead-lettered forever."
            )
            raise ValueError(message)

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
        # Info, not warning: recovery is automatic. But not silent either, since
        # the same event drops whatever was pending on that stream, and an
        # operator seeing the redeliveries needs something to correlate them to.
        logger.info(
            "Consumer group cache invalidated for %s after a stream was deleted out of band "
            "(purge, queue delete, or x-expires); groups will be recreated on next use.",
            "all watched queues" if queue is None else f"queue {queue}",
        )
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

    def _effective_message_ttl_ms(self, queue: str) -> int:
        """Effective message TTL for a queue in milliseconds (0 = no TTL).

        Combines the channel-wide ``message_ttl`` transport option (seconds)
        with the queue's ``x-message-ttl`` argument (milliseconds) by taking
        the smaller of the two, and normalizes anything non-positive to 0.

        A non-positive ``message_ttl`` means "unset", never "expire
        immediately": ``-1`` is the sorted-set transport's own documented
        no-TTL sentinel (``DEFAULT_MESSAGE_TTL``), so a user migrating
        between the two transports carries it over. Without the
        normalization, ``min(-1000, queue_ttl_ms)`` would be negative and
        every ``ttl_ms > 0`` guard downstream (Lua and Python alike) would
        read it as "no TTL", silently disabling the queue's own
        ``x-message-ttl``.

        Args:
            queue: Queue name whose ``x-message-ttl`` is combined in.

        Returns:
            The effective TTL in milliseconds, or 0 when no TTL applies.
        """
        ttl_ms = 0
        if self.message_ttl is not None and self.message_ttl > 0:
            ttl_ms = int(self.message_ttl * 1000)
        queue_ttl_ms = self._message_ttls.get(queue)
        if queue_ttl_ms is not None:
            ttl_ms = queue_ttl_ms if ttl_ms <= 0 else min(ttl_ms, queue_ttl_ms)
        return max(ttl_ms, 0)

    def _deliver_in_flight(
        self,
        message: dict[str, Any],
        queue: str,
        delivery_tag: str,
        stream_key: str,
        entry_id: str,
    ) -> None:
        """Record a message's PEL metadata, deliver it, and drop the record if delivery raises.

        The QoS in-flight record has to exist before the delivery callback
        runs, since an ack can land inside that same call. But kombu's
        ``Consumer._receive_callback`` wraps only the decode section, so an
        exception from the user callback propagates straight back out of
        ``Transport._deliver``. A record left behind by such a raise is worse
        than useless: ``_heartbeat`` keeps issuing ``XCLAIM ... JUSTID`` for
        it every cycle, so the entry's PEL idle time never crosses the
        visibility timeout, and ``_own_in_flight_message_ids`` filters it out
        of this channel's own reclaim pass as well. Neither a peer nor this
        worker could ever take it again. Dropping the record on the way out
        hands the entry back to the visibility-timeout safety net, turning a
        permanent stall into an ordinary at-least-once redelivery.

        Args:
            message: Deserialized message to deliver.
            queue: Logical queue name the message was consumed from.
            delivery_tag: The message's delivery tag, used as the in-flight key.
            stream_key: Unprefixed stream key holding the entry.
            entry_id: Stream entry id, for the later XACK+XDEL.
        """
        qos = self.qos
        if qos is not None:
            cast("QoS", qos)._in_flight[delivery_tag] = (stream_key, entry_id)
        delivered = False
        try:
            self.connection._deliver(message, queue)
            delivered = True
        finally:
            if not delivered and qos is not None:
                cast("QoS", qos)._in_flight.pop(delivery_tag, None)

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

        ttl_ms = self._effective_message_ttl_ms(queue)

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

    def _has_queue(self, queue: str, **kwargs: Any) -> bool:
        """Return whether any level stream or the delayed zset exists for this queue."""
        with self.conn_or_acquire() as client:
            return bool(client.exists(*self._stream_keys_for_queue(queue), self._delayed_key(queue)))

    def _size(self, queue: str) -> int:
        """Return the number of messages waiting on a queue.

        Counts the level streams plus the delayed staging zset, and subtracts
        each stream's pending entries list. XREADGROUP registers a delivered
        entry in the PEL without removing it from the stream, so XLEN alone
        would keep counting messages that are already out with a consumer,
        and a fully consumed queue would never report empty. The sorted-set
        transport's _size does not count in-flight messages either (ZPOPMIN
        removes them), so this keeps the two transports' queue depths
        comparable.

        Args:
            queue: Queue name to measure.

        Returns:
            Messages available to be consumed, never negative.
        """
        stream_keys = self._stream_keys_for_queue(queue)
        with self.conn_or_acquire() as client, client.pipeline() as pipe:
            for stream_key in stream_keys:
                pipe.xlen(stream_key)
                pipe.xpending(stream_key, self.consumer_group)
            pipe.zcard(self._delayed_key(queue))
            # A stream that never had a consumer group answers XPENDING with
            # NOGROUP; that is an ordinary state, not an error worth raising
            results = pipe.execute(raise_on_error=False)

        total = 0
        for index in range(len(stream_keys)):
            length = results[index * 2]
            pending = results[index * 2 + 1]
            if isinstance(length, Exception):
                continue
            pending_count = pending.get("pending", 0) if isinstance(pending, dict) else 0
            total += max(int(length) - int(pending_count), 0)
        delayed = results[-1]
        if not isinstance(delayed, Exception):
            total += int(delayed)
        return total

    def _purge(self, queue: str) -> int:
        """Delete all messages from a queue: level streams and the delayed zset.

        Consumer groups die with their stream and are recreated lazily on next
        use, so the cached group entries for this queue are dropped as well.
        Deleting the level streams also destroys those consumer groups'
        pending entry lists, so any unacked, in-flight messages are discarded
        along with the queued ones.
        """
        stream_keys = self._stream_keys_for_queue(queue)
        delayed_key = self._delayed_key(queue)
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                for stream_key in stream_keys:
                    pipe.xlen(stream_key)
                pipe.zcard(delayed_key)
                counts = pipe.execute()
            client.delete(*stream_keys, delayed_key)
        for stream_key in stream_keys:
            self._ensured_groups.discard(stream_key)
        return sum(int(count) for count in counts)

    def _delete(self, queue: str, *args: Any, **kwargs: Any) -> None:
        # kombu calls: _delete(queue, exchange, routing_key, pattern)
        exchange = args[0] if args else ""
        routing_key = args[1] if len(args) > 1 else ""
        pattern = args[2] if len(args) > 2 else ""  # noqa: PLR2004
        self.auto_delete_queues.discard(queue)
        had_expires = queue in self._expires
        self._expires.pop(queue, None)
        self._message_ttls.pop(queue, None)
        stream_keys = self._stream_keys_for_queue(queue)
        delayed_key = self._delayed_key(queue)
        with self.conn_or_acquire(client=kwargs.get("client")) as client:
            client.srem(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or "", pattern or "", queue or ""]),
            )
            client.delete(*stream_keys, delayed_key)
        for stream_key in stream_keys:
            self._ensured_groups.discard(stream_key)
        if had_expires:
            self.connection.cycle._update_expires_timer()

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

    def _refresh_queue_expires(self) -> None:
        """Refresh PEXPIRE on level streams and the delayed zset for queues with x-expires."""
        if not self._expires:
            return
        try:
            with self.conn_or_acquire() as client, client.pipeline() as pipe:
                for queue, ttl_ms in self._expires.items():
                    for stream_key in self._stream_keys_for_queue(queue):
                        pipe.pexpire(stream_key, ttl_ms)
                    pipe.pexpire(self._delayed_key(queue), ttl_ms)
                pipe.execute()
        except Exception:
            logger.warning("Failed to refresh queue expires, will retry next cycle", exc_info=True)

    def _move_delayed(self, queue: str, limit: int = DEFAULT_REQUEUE_BATCH_LIMIT) -> int:
        """Move due delayed messages into their priority streams.

        Runs the streams_move_delayed Lua script: members of ``delayed:{queue}``
        whose delivery time has passed are XADDed to the stream matching their
        bucketed priority and removed from the zset. Members whose delivery time
        passed more than the effective message TTL ago are dropped instead
        (lazy x-message-ttl). The script reads the current time via Redis TIME
        rather than a caller-supplied timestamp, so a client clock ahead of the
        server cannot destroy live messages.

        Args:
            queue: Queue name whose delayed zset is pumped.
            limit: Maximum number of messages to move in this call. Defaults
                to DEFAULT_REQUEUE_BATCH_LIMIT; the requeue cycle instead
                passes its remaining shared budget here, so this queue's pump
                cannot alone spend more than what is left of the cycle's
                budget.

        Returns:
            Number of messages moved into streams.
        """
        steps = sorted(self.priority_steps)

        # Groups must exist before the XADD or consumers cannot read the entries
        for level in steps:
            self._ensure_group(self._stream_key(queue, level))

        message_ttl_ms = self._effective_message_ttl_ms(queue)

        # Prefix keys since EVALSHA doesn't auto-prefix KEYS
        delayed_key = f"{self.global_keyprefix}{self._delayed_key(queue)}"
        stream_keys = [f"{self.global_keyprefix}{self._stream_key(queue, level)}" for level in steps]

        with self.conn_or_acquire() as client:
            move_script = client.register_script(_STREAMS_MOVE_DELAYED_LUA)
            moved = int(
                move_script(
                    keys=[delayed_key, *stream_keys],
                    args=[
                        limit,
                        message_ttl_ms,
                        ",".join(str(step) for step in steps),
                    ],
                ),
            )

        if moved >= limit:
            # Warn once per queue, then drop to debug: a saturated queue hits
            # this every cycle. The else branch below re-arms the warning.
            log = logger.debug if queue in self._warned_delayed_limit else logger.warning
            self._warned_delayed_limit.add(queue)
            log(
                "Queue %s moved the maximum of %d delayed messages allowed this pass "
                "(the shared per-cycle requeue budget, or DEFAULT_REQUEUE_BATCH_LIMIT "
                "when called outside the requeue cycle). There may be more messages waiting.",
                queue,
                limit,
            )
        else:
            self._warned_delayed_limit.discard(queue)
        return moved

    def _heartbeat(self) -> None:
        """Reset the PEL idle clock on all in-flight messages.

        Groups the QoS in-flight map by stream and issues one XCLAIM per
        stream with min_idle_time=0 and justid=True. JUSTID resets the idle
        time without bumping the delivery count or transferring payloads, so
        a long-running task never counts toward the poison cap and its entry
        is never reclaimed by a peer while this worker is alive.

        A failure heartbeating one stream, NOGROUP or otherwise, is logged
        and does not stop the remaining streams from being heartbeated this
        cycle: letting one bad stream cancel every other stream's heartbeat
        would itself cause the spurious reclaims this method exists to
        prevent. A NOGROUP specifically means that stream (and its group,
        PEL included) was deleted out of band, so its cached ensure is
        dropped too (the next consume pass re-creates the group).

        Because min_idle_time=0 claims unconditionally, a worker that stalls
        past the visibility timeout (a GC pause, blocked I/O) and then
        recovers can claim an entry back from a peer that legitimately
        reclaimed it in the meantime, racing that peer for the same message.
        This is inherent to XCLAIM and to the at-least-once contract, not a
        bug in this method, and is not addressed here.
        """
        qos = self.qos
        if qos is None:
            return
        in_flight = cast("QoS", qos)._in_flight
        if not in_flight:
            return

        # One XCLAIM per stream: batch all message ids belonging to it
        ids_by_stream: dict[str, list[str]] = {}
        for stream_key, message_id in in_flight.values():
            ids_by_stream.setdefault(stream_key, []).append(message_id)

        # Stream keys in _in_flight are unprefixed; the prefixed client handles
        # global_keyprefix (XCLAIM is in PREFIXED_SIMPLE_COMMANDS).
        try:
            with self.conn_or_acquire() as client:
                for stream_key, message_ids in ids_by_stream.items():
                    try:
                        refreshed = client.xclaim(
                            stream_key,
                            self.consumer_group,
                            self.consumer_name,
                            0,
                            message_ids,
                            justid=True,
                        )
                        # Log missing ids, never prune them: _in_flight is what the ack
                        # path resolves a delivery tag through, and the task may still run.
                        refreshed_ids = {bytes_to_str(entry_id) for entry_id in refreshed}
                        missing = [message_id for message_id in message_ids if message_id not in refreshed_ids]
                        if missing:
                            logger.warning(
                                "Heartbeat found %d of %d in-flight ids no longer pending on stream %s "
                                "(already acked elsewhere, or the stream entry was deleted)",
                                len(missing),
                                len(message_ids),
                                stream_key,
                            )
                    except self.ResponseError as exc:
                        if "NOGROUP" in str(exc):
                            # Stream deleted out of band: drop the cached ensure
                            # and keep heartbeating the other streams
                            self._ensured_groups.discard(stream_key)
                        else:
                            logger.warning(
                                "Failed to heartbeat stream %s, will retry next cycle",
                                stream_key,
                                exc_info=True,
                            )
                        continue
                    except Exception:
                        # A failure on one stream must not abort the loop: the
                        # remaining streams still need their heartbeat this cycle.
                        logger.warning(
                            "Failed to heartbeat stream %s, will retry next cycle",
                            stream_key,
                            exc_info=True,
                        )
                        continue
        except Exception:
            logger.warning("Failed to heartbeat in-flight messages, will retry next cycle", exc_info=True)

    def _own_in_flight_message_ids(self, stream_key: str) -> set[str]:
        """Stream message ids already in-flight for stream_key, across this process.

        consumer_name is derived per process (see the property's docstring),
        so every consuming channel in this process shares one Redis consumer
        identity even though each channel keeps its own QoS instance and its
        own ``_in_flight`` table. Checking only this channel's table would let
        channel A's reclaim pass steal and re-deliver channel B's live
        message. This collects this channel's own in-flight ids plus those of
        every sibling channel reachable through the connection's cycle
        (MultiChannelPoller) that shares this channel's consumer_name, and
        falls back to just this channel's own table when the cycle is
        unreachable so a standalone channel still works.
        """
        cycle = getattr(self.connection, "cycle", None)
        channels: list[Channel] = [self] if cycle is None else list(cycle._channels)
        if self not in channels:
            channels.append(self)
        ids: set[str] = set()
        for channel in channels:
            if channel is not self and channel.consumer_name != self.consumer_name:
                continue
            channel_qos = cast("QoS", channel.qos) if channel.qos is not None else None
            if channel_qos is None:
                continue
            ids.update(message_id for key, message_id in channel_qos._in_flight.values() if key == stream_key)
        return ids

    def _reclaim_and_deliver(self, queue: str, budget: int) -> int:  # noqa: PLR0912, PLR0915
        """Reclaim messages idle past the visibility timeout and deliver them locally.

        Runs a discover-then-claim pass over each of the queue's priority
        level streams (highest level first):

        1. ``XPENDING ... IDLE min_idle_ms`` pages through the group's
           pending entries list (in id order, starting from "-") for entries
           idle past visibility_timeout, without claiming or incrementing
           anything. Being read-only, this is safe to run over every idle
           entry: one this pass ends up not taking simply stays parked in the
           PEL for a later pass to find again. Pages are walked with an
           exclusive cursor (next page's ``min`` is ``"(" + last_id``),
           terminating when a page returns fewer entries than requested
           (Redis only returns a short page once it has scanned to the end
           of the pending list). Capped at DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT
           pages per stream per call, since a PEL dominated by entries this
           pass ends up filtering out (own in-flight, expired, or over
           prefetch capacity) would otherwise scan the whole PEL every cycle
           for no progress; hitting the cap logs a warning and moves on, and
           a later pass picks up from wherever this one stopped.
        2. The discovered ids are filtered: ids already in-flight somewhere
           in this process (see _own_in_flight_message_ids) are dropped, and
           ids older than the effective x-message-ttl are acked away right
           here (XACK does not care which consumer currently owns an entry,
           so there is no need to claim an expired id first). The remainder
           is truncated to the smaller of the remaining budget and the
           channel's remaining QoS prefetch capacity
           (qos.can_consume_max_estimate(); unbounded prefetch skips this
           axis). Only the survivors are claimed.
        3. ``XCLAIM`` with the real min_idle_time (not 0) claims exactly the
           survivors, fetching their payloads and bumping their delivery
           count exactly once each. Using the real min_idle_time makes this
           race-safe against a competing reclaimer: if another worker
           claimed one of these ids between steps 1 and 3, its idle time was
           just reset, so this XCLAIM correctly does not take it. An id can
           also vanish from the stream between steps 1 and 3 (concurrent
           XDEL/trim); either way XCLAIM simply omits it rather than
           returning a placeholder, so the reply may legitimately be shorter
           than the survivor list.

        This keeps delivery counts honest: an entry this pass skips or never
        claims (own in-flight, lazily expired, or filtered out for lack of
        budget/prefetch capacity) never reaches XCLAIM, so its counter is
        never bumped. times_delivered is read once, in step 1, before this
        pass's own claim runs; restore_count = times_delivered with no
        adjustment, since step 3's XCLAIM adds exactly one more delivery for
        whichever ids it actually claims and the pre-claim count already
        excludes that increment (the first delivery, via XREADGROUP, is
        itself the message's 1st times_delivered; restore_count = 1 means
        this is its first restore).

        Messages exceeding max_restore_count are dropped, optionally copied to
        dead_letter_stream first. A payload that fails to parse is treated
        the same as a missing payload: logged, acked away so it cannot loop
        forever, and counted against budget.

        Actual deliveries respect the channel's QoS prefetch_count: once
        qos.can_consume() goes false (checked right after each delivery) the
        pass stops immediately, leaving any remaining discovered entries in
        the PEL for a later reclaim pass instead of flooding the channel
        past its limit. Callers are expected to skip invoking this method at
        all when qos.can_consume() is already false (see
        MultiChannelPoller.maybe_enqueue_due_messages).

        A NOGROUP from either XPENDING or XCLAIM means the stream (and with
        it its consumer group) was deleted out of band while this channel's
        _ensured_groups cache was warm; the cache is invalidated, the
        queue's groups are re-created, and the failing call is retried once.
        A second NOGROUP propagates to maybe_enqueue_due_messages, which
        logs and retries next cycle.

        Args:
            queue: Queue name to reclaim messages for.
            budget: Maximum number of claimed entries to process.

        Returns:
            Number of claimed entries processed (delivered or dropped); an
            entry recognized as already in-flight somewhere in this process,
            lazily expired, or left unclaimed for lack of budget/prefetch
            capacity, is not counted.
        """
        if budget <= 0:
            return 0

        min_idle_ms = int(self.visibility_timeout * 1000)
        qos = cast("QoS", self.qos) if self.qos is not None else None

        ttl_ms = self._effective_message_ttl_ms(queue)

        processed = 0
        prefetch_exhausted = False
        now_ms = 0
        with self.conn_or_acquire() as client:
            ack_script = client.register_script(_STREAMS_ACK_LUA)
            if ttl_ms > 0:
                # Server clock, not the caller's: a worker running ahead of the
                # server would delete un-expired messages every reclaim cycle.
                server_seconds, server_micros = client.time()
                now_ms = int(server_seconds) * 1000 + int(server_micros) // 1000
            for stream_key in self._stream_keys_for_queue(queue):
                cursor = "-"
                pages_walked = 0
                while processed < budget and not prefetch_exhausted:
                    if pages_walked >= DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT:
                        # A PEL dominated by filtered entries would otherwise
                        # walk in full every cycle for no progress.
                        logger.warning(
                            "Stream %s: reclaim discovery stopped after %d pages; more entries may be pending.",
                            stream_key,
                            pages_walked,
                        )
                        break
                    page_count = min(budget - processed, 100)
                    pending_kwargs: dict[str, Any] = {
                        "min": cursor,
                        "max": "+",
                        "count": page_count,
                        "idle": min_idle_ms,
                    }
                    try:
                        pending = client.xpending_range(stream_key, self.consumer_group, **pending_kwargs)
                    except self.ResponseError as exc:
                        if "NOGROUP" not in str(exc):
                            raise
                        # Stream deleted out of band (purge, queue delete, x-expires).
                        # Recreate the groups and retry once; a second NOGROUP propagates.
                        self._invalidate_group(queue)
                        for level_key in self._stream_keys_for_queue(queue):
                            self._ensure_group(level_key)
                        pending = client.xpending_range(stream_key, self.consumer_group, **pending_kwargs)
                    pages_walked += 1
                    if not pending:
                        break

                    page_ids = [bytes_to_str(entry["message_id"]) for entry in pending]
                    times_delivered = {
                        bytes_to_str(entry["message_id"]): int(entry["times_delivered"]) for entry in pending
                    }
                    last_id = page_ids[-1]
                    page_short = len(pending) < page_count

                    # Prefix key since EVALSHA doesn't auto-prefix KEYS
                    prefixed_stream = f"{self.global_keyprefix}{stream_key}"

                    # Filter before claiming: own in-flight ids must not have their
                    # delivery counter bumped, and XACK works regardless of owner.
                    own_ids = self._own_in_flight_message_ids(stream_key)
                    survivor_ids: list[str] = []
                    for message_id in page_ids:
                        if message_id in own_ids:
                            continue
                        # Lazy x-message-ttl drop: entry ids encode creation time in ms
                        if ttl_ms > 0 and int(message_id.split("-")[0]) < now_ms - ttl_ms:
                            processed += 1
                            ack_script(keys=[prefixed_stream], args=[self.consumer_group, message_id, ""])
                            continue
                        survivor_ids.append(message_id)

                    # Never claim more than we can deliver this pass; a None estimate
                    # means prefetch_count is unbounded.
                    capacity = qos.can_consume_max_estimate() if qos is not None else None
                    remaining_budget = budget - processed
                    take = remaining_budget if capacity is None else min(remaining_budget, capacity)
                    survivor_ids = survivor_ids[: max(take, 0)]
                    if not survivor_ids:
                        if page_short:
                            break
                        cursor = "(" + last_id
                        continue

                    # A real min_idle_time (not 0) is what makes this race-safe: a peer
                    # that claimed an id since discovery reset its idle, so it is skipped.
                    claim_kwargs: dict[str, Any] = {
                        "min_idle_time": min_idle_ms,
                        "message_ids": survivor_ids,
                    }
                    try:
                        claimed = client.xclaim(
                            stream_key,
                            self.consumer_group,
                            self.consumer_name,
                            **claim_kwargs,
                        )
                    except self.ResponseError as exc:
                        if "NOGROUP" not in str(exc):
                            raise
                        self._invalidate_group(queue)
                        for level_key in self._stream_keys_for_queue(queue):
                            self._ensure_group(level_key)
                        claimed = client.xclaim(
                            stream_key,
                            self.consumer_group,
                            self.consumer_name,
                            **claim_kwargs,
                        )

                    for message_id, fields in claimed:
                        if prefetch_exhausted:
                            break
                        message_id_str = bytes_to_str(message_id)
                        payload_field = fields.get(b"payload") or fields.get("payload")
                        if payload_field is None:
                            # Foreign or corrupt entry: ack it away so it cannot loop forever.
                            # Same treatment and same volume as its unparseable-payload
                            # sibling below: this discards an entry, which must never be
                            # silent even when the entry was never ours to begin with.
                            logger.warning(
                                "Stream %s: claimed entry %s has no payload field; acking it away.",
                                stream_key,
                                message_id_str,
                            )
                            processed += 1
                            ack_script(keys=[prefixed_stream], args=[self.consumer_group, message_id_str, ""])
                            continue

                        try:
                            message: dict[str, Any] = loads(bytes_to_str(payload_field))
                            delivery_tag = message["properties"]["delivery_tag"]
                        except (ValueError, TypeError, KeyError):
                            # Unparseable payload: ack it away so one bad entry cannot
                            # stall every reclaim pass for this queue forever
                            logger.warning(
                                "Stream %s: claimed entry %s has an unparseable payload; acking it away.",
                                stream_key,
                                message_id_str,
                            )
                            processed += 1
                            ack_script(keys=[prefixed_stream], args=[self.consumer_group, message_id_str, ""])
                            continue

                        processed += 1
                        # Unreachable: every claimed id came from this page's discovery.
                        # Fall back defensively rather than crash.
                        if message_id_str not in times_delivered:
                            logger.warning(
                                "Stream %s: claimed entry %s missing from XPENDING discovery reply; "
                                "assuming first delivery.",
                                stream_key,
                                message_id_str,
                            )
                        restore_count = times_delivered.get(message_id_str, 0)
                        if self.max_restore_count is not None and restore_count > self.max_restore_count:
                            if self.dead_letter_stream is not None:
                                # Copy to the dead-letter stream before dropping (approximate cap)
                                client.xadd(
                                    name=self.dead_letter_stream,
                                    fields={"payload": payload_field},
                                    id="*",
                                    maxlen=DEFAULT_STREAM_MAXLEN,
                                    approximate=True,
                                )
                            ack_script(keys=[prefixed_stream], args=[self.consumer_group, message_id_str, ""])
                            logger.warning(
                                "Queue %s: dropped message %s after exceeding max restore count of %d.",
                                queue,
                                message_id_str,
                                self.max_restore_count,
                            )
                            continue
                        if restore_count > 0:
                            headers = message.setdefault("properties", {}).setdefault("headers", {})
                            headers["x-restore-count"] = restore_count
                        self._deliver_in_flight(message, queue, delivery_tag, stream_key, message_id_str)
                        if qos is not None and not qos.can_consume():
                            prefetch_exhausted = True
                    if page_short or prefetch_exhausted:
                        break
                    cursor = "(" + last_id
                if processed >= budget or prefetch_exhausted:
                    break
        return processed

    def _cleanup_consumers(self) -> None:
        """Remove idle consumers with no pending entries from consumer groups.

        Consumers never expire in Redis, so every worker that ever connected
        leaves a consumer entry behind. This slow periodic pass deletes
        consumers idle longer than CONSUMER_IDLE_CLEANUP_FACTOR times the
        visibility timeout. Consumers with pending entries are never deleted
        (reclaim drains them first), and the channel's own consumer is kept.

        The read (XINFO CONSUMERS) and the delete (XGROUP DELCONSUMER) run
        inside one Lua script per stream (streams_cleanup_consumers.lua), so
        the pending/idle values a consumer is deleted on are exactly the
        values at delete time. Doing this as two separate round trips from
        the client would leave a window where a peer parked in a blocking
        XREADGROUP with zero pending is handed a fresh entry between the
        read and the delete; XGROUP DELCONSUMER would then remove that
        entry from the group PEL along with the consumer, unreachable by
        XREADGROUP, XPENDING, or XCLAIM, silently degrading at-least-once
        to at-most-once for that peer.
        """
        if not self._queue_cycle:
            return
        try:
            idle_threshold_ms = CONSUMER_IDLE_CLEANUP_FACTOR * self.visibility_timeout * 1000
            with self.conn_or_acquire() as client:
                cleanup_script = client.register_script(_STREAMS_CLEANUP_CONSUMERS_LUA)
                for queue in self._queue_cycle:
                    for stream_key in self._stream_keys_for_queue(queue):
                        # Prefix manually since EVALSHA doesn't auto-prefix KEYS
                        prefixed_stream_key = f"{self.global_keyprefix}{stream_key}"
                        try:
                            result = cleanup_script(
                                keys=[prefixed_stream_key],
                                args=[self.consumer_group, self.consumer_name, idle_threshold_ms],
                            )
                            # Ordinary replies are consumer names, so only an
                            # integer first element is the script's [-1, msg]
                            # sentinel. Match the shape, not result[0]: bytes
                            # would otherwise index to an int.
                            match result:
                                case [int(), message]:
                                    if isinstance(message, bytes):
                                        message = message.decode()
                                    # These conditions are persistent (a WRONGTYPE key
                                    # does not clear itself), so shout once per stream
                                    # and mutter afterwards rather than flooding every
                                    # cycle forever.
                                    log = (
                                        logger.debug
                                        if prefixed_stream_key in self._warned_cleanup_streams
                                        else logger.warning
                                    )
                                    self._warned_cleanup_streams.add(prefixed_stream_key)
                                    log(
                                        "Consumer cleanup could not inspect %s: %s",
                                        prefixed_stream_key,
                                        message,
                                    )
                                case _:
                                    pass
                        except self.ResponseError:
                            # One stream failing must not abort hygiene for the
                            # remaining streams and queues this cycle.
                            continue
        except Exception:
            logger.warning("Failed to clean up idle consumers, will retry next cycle", exc_info=True)

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

    def _rotate_queue_cycle(self, served_index: int) -> None:
        """Move the queue that just yielded, and everything before it, to the back.

        _consume_read scans _queue_cycle in order and returns on the first
        queue that yields, so without rotation a continuously busy queue at
        the head is served every single pass and every queue behind it
        starves. Rotating past the served position makes the next pass start
        at the following queue, which is the same fairness guarantee the
        requeue cycle already gets from MultiChannelPoller._requeue_offsets.

        Args:
            served_index: Index in _queue_cycle of the queue that yielded.
        """
        cycle = self._queue_cycle
        if len(cycle) < 2:  # noqa: PLR2004
            return
        split = (served_index + 1) % len(cycle)
        self._queue_cycle = cycle[split:] + cycle[:split]

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

    def _ensure_consume_script_sha(self) -> str:
        """Load and cache the streams_consume Lua script SHA."""
        if self._consume_script_sha is None:
            self._consume_script_sha = self.client.script_load(_STREAMS_CONSUME_LUA)
        return self._consume_script_sha

    def _consume_read(self, **options: Any) -> bool:
        """Non-blocking priority consume pass over all watched queues.

        Sends a raw EVALSHA of the streams_consume Lua script per queue in
        _queue_cycle order and parses each reply inline. The script walks
        the queue's level streams highest priority first with non-blocking
        XREADGROUP, so delivery and PEL registration are atomic in Redis.

        On the first hit: rotates _queue_cycle past the queue that yielded
        (so the next pass starts behind it and a busy queue cannot starve
        the ones after it), records the entry in QoS._in_flight, delivers
        the message, and returns True. When every queue is empty: arms a
        blocking XREADGROUP over all watched streams via _xreadgroup_start()
        (keeps _in_poll set) and raises Empty.

        A NOGROUP reply means the queue's stream (and its consumer group)
        was deleted out of band while this channel's _ensured_groups cache
        was warm; the cache is invalidated, the groups are re-created, and
        the EVALSHA is retried once for that queue.
        """
        for index, queue in enumerate(list(self._queue_cycle)):
            sha = self._ensure_consume_script_sha()
            stream_keys = self._stream_keys_for_queue(queue)
            for stream_key in stream_keys:
                self._ensure_group(stream_key)
            # Prefix keys manually since EVALSHA doesn't auto-prefix KEYS
            keys = [f"{self.global_keyprefix}{stream_key}" for stream_key in stream_keys]
            ttl_ms = self._effective_message_ttl_ms(queue)

            command_args: tuple[Any, ...] = (
                "EVALSHA",
                sha,
                len(keys),
                *keys,
                self.consumer_group,
                self.consumer_name,
                str(ttl_ms),
            )
            self.client.connection.send_command(*command_args)
            try:
                result = self.client.parse_response(self.client.connection, "EVALSHA", **options)
            except self.connection_errors:
                self.client.connection.disconnect()
                raise
            except self.ResponseError as exc:
                if "NOSCRIPT" in str(exc):
                    # Script evicted from cache, reload on next tick. Debug,
                    # not warning: a SCRIPT FLUSH or a server restart makes
                    # this expected, and it costs exactly one skipped tick.
                    logger.debug("Consume script evicted from the Redis script cache; reloading on the next pass.")
                    self._consume_script_sha = None
                    raise Empty from None
                if "NOGROUP" not in str(exc):
                    raise
                # Stream deleted out of band (purge, queue delete, x-expires).
                # Recreate the groups and retry once; a second NOGROUP propagates.
                self._invalidate_group(queue)
                for stream_key in stream_keys:
                    self._ensure_group(stream_key)
                self.client.connection.send_command(*command_args)
                result = self.client.parse_response(self.client.connection, "EVALSHA", **options)

            if result:
                result_stream = bytes_to_str(result[0])
                # KEYS were prefixed for EVALSHA; strip so _in_flight stores unprefixed keys
                if self.global_keyprefix and result_stream.startswith(self.global_keyprefix):
                    result_stream = result_stream[len(self.global_keyprefix) :]
                entry_id = bytes_to_str(result[1])
                message: dict[str, Any] = loads(bytes_to_str(result[2]))
                delivery_tag = message["properties"]["delivery_tag"]
                # Rotate before delivering so a raising delivery callback
                # cannot pin the cycle on the same queue forever
                self._rotate_queue_cycle(index)
                self._deliver_in_flight(message, queue, delivery_tag, result_stream, entry_id)
                return True

        # Every watched queue is empty: arm the blocking XREADGROUP (sets _in_poll)
        self._xreadgroup_start()
        raise Empty

    def _xreadgroup_start(self, timeout: float | None = None) -> None:
        """Send a blocking XREADGROUP over all watched level streams.

        Called when the non-blocking pass found every queue empty, or when it
        hit the per-tick drain cap with messages still available. Data on any
        stream wakes the poller fd; strict priority order is restored on the
        next non-blocking pass.
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

        Unlike the non-blocking pass, a raw XREADGROUP has no Lua wrapper
        to apply the lazy message-TTL drop, so expired entries are filtered
        here instead: without it a queue's ``x-message-ttl`` would be
        enforced on some ticks and ignored on others, purely by which leg
        happened to pick the entry up. The clock comes from the server, and
        the connection behind it is only acquired if a watched queue
        actually has a TTL.
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

            if not self._deliver_xreadgroup_entries(messages):
                raise Empty
            return True
        finally:
            self._in_poll = None

    def _deliver_xreadgroup_entries(self, messages: Sequence[Any]) -> bool:
        """Deliver the entries of a blocking XREADGROUP reply, dropping expired ones.

        Args:
            messages: The parsed XREADGROUP reply, a sequence of
                ``(stream_key, [(entry_id, fields), ...])`` pairs.

        Returns:
            True if at least one entry was delivered.
        """
        delivered = False
        with ExitStack() as stack:
            ack_script: Any = None
            now_ms = 0
            for stream, message_list in messages:
                stream_str = bytes_to_str(stream) if isinstance(stream, bytes) else stream
                # Reply stream names carry the global key prefix; strip so
                # _in_flight always stores unprefixed keys
                prefix = self.global_keyprefix
                if prefix and stream_str.startswith(prefix):
                    stream_str = stream_str[len(prefix) :]
                # stream:{queue}:{level} -> logical queue name
                queue_name = stream_str[len(STREAM_KEY_PREFIX) :].rsplit(":", 1)[0]
                ttl_ms = self._effective_message_ttl_ms(queue_name)
                for message_id, fields in message_list:
                    message_id_str = bytes_to_str(message_id) if isinstance(message_id, bytes) else message_id
                    payload_field = fields.get(b"payload") or fields.get("payload")
                    if not payload_field:
                        continue
                    if ttl_ms > 0:
                        if ack_script is None:
                            # Server clock, not the caller's: a worker running
                            # ahead of the server would drop live messages
                            client = stack.enter_context(self.conn_or_acquire())
                            ack_script = client.register_script(_STREAMS_ACK_LUA)
                            server_seconds, server_micros = client.time()
                            now_ms = int(server_seconds) * 1000 + int(server_micros) // 1000
                        # Entry ids encode creation time in ms
                        if int(message_id_str.split("-")[0]) < now_ms - ttl_ms:
                            ack_script(
                                keys=[f"{self.global_keyprefix}{stream_str}"],
                                args=[self.consumer_group, message_id_str, ""],
                            )
                            continue
                    message: dict[str, Any] = loads(bytes_to_str(payload_field))
                    delivery_tag = message["properties"]["delivery_tag"]
                    self._deliver_in_flight(message, queue_name, delivery_tag, stream_str, message_id_str)
                    delivered = True
        return delivered

    def _poll_error(self, cmd_type: str, **options: Any) -> Any:
        """Drain the error reply for a polled fd and clear the pending-read flag.

        Clearing the flag matters as much as draining the reply: the armed
        read is over either way, and a flag left set makes _register_XREAD /
        _register_XREADGROUP believe a blocking read is still parked on the
        socket, so neither re-arms one and the channel goes quiet until
        something else disturbs it. On the XREADGROUP side a stale flag also
        misroutes the next error reply, since the EVALSHA fallback below keys
        off exactly that flag.

        Args:
            cmd_type: The command whose reply is pending on the fd.
            **options: Passed through to parse_response.

        Returns:
            The parsed error reply.
        """
        is_fanout = cmd_type == "XREAD"
        if is_fanout:
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
        finally:
            if is_fanout:
                self._in_fanout_poll = None
            else:
                self._in_poll = None

    def close(self) -> None:
        if self._in_poll:
            with suppress(Empty, *_connection_errors):
                self._xreadgroup_read()
        if self._in_fanout_poll:
            with suppress(Empty, *_connection_errors):
                self._xread_read()
        already_closed = self.closed
        if not already_closed:
            self.connection.cycle.discard(self)

            client = self.__dict__.get("client")
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
        try:
            super().close()
        finally:
            if not already_closed:
                # After super().close() (which calls restore_unacked_once) on
                # purpose: conn_or_acquire() can lazily rebuild self._pool
                # during the restore, and that rebuilt pool must be
                # disconnected too, not just the one that existed before.
                self._disconnect_pools()
                self._close_clients()

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

    Requires Redis 6.2+ (or Valkey) for the XPENDING IDLE filter and exclusive
    stream ID ranges used by the reclaim pass.
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

    def _collect(self, connection: Connection) -> None:
        """Release channels for a lost connection, without restoring in-flight messages.

        kombu's ``Connection.collect()`` calls this instead of the normal
        ``close_connection``/``Channel.close`` path (the one a real
        ``Connection.close()`` uses) whenever a transport defines it. A collect
        means the broker connection was lost and celery is about to reconnect,
        not that the application asked to shut down, so this must never trigger
        QoS.restore_unacked_once(): a still-in-flight PEL entry stays owned by
        this worker and peers will pick it up on their own via the reclaim
        pass once the visibility timeout expires, same as any other in-flight
        message, if this process never reconnects.
        """
        _collect_transport(self)

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

        # Heartbeat keeps in-flight PEL entries alive while tasks are running.
        # visibility_timeout is validated first: it is the denominator below.
        transport_options = connection.client.transport_options  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
        visibility_timeout = transport_options.get("visibility_timeout", DEFAULT_VISIBILITY_TIMEOUT)
        if not _is_finite_positive(visibility_timeout):
            logger.warning(
                "visibility_timeout %r is not a positive finite number; falling back to the default of %s",
                visibility_timeout,
                DEFAULT_VISIBILITY_TIMEOUT,
            )
            visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        default_heartbeat_interval = visibility_timeout / HEARTBEAT_INTERVAL_DIVISOR
        # A configured interval must still leave headroom for a swallowed heartbeat,
        # so require at least 2 per visibility_timeout window (the default gives 5).
        max_safe_heartbeat_interval = visibility_timeout / 2
        heartbeat_interval = transport_options.get("heartbeat_interval", default_heartbeat_interval)
        if not (_is_finite_positive(heartbeat_interval) and heartbeat_interval <= max_safe_heartbeat_interval):
            logger.warning(
                "heartbeat_interval %r must be a positive finite number no greater than half of "
                "visibility_timeout %s to leave headroom against spurious reclaims of this "
                "worker's own live messages; falling back to the default of %s instead of "
                "honoring it",
                heartbeat_interval,
                visibility_timeout,
                default_heartbeat_interval,
            )
            heartbeat_interval = default_heartbeat_interval
        loop.call_repeatedly(heartbeat_interval, cycle.maybe_heartbeat)

        # Store loop for dynamic timer registration (queue TTL refresh), then arm
        # the timer immediately: a queue with x-expires may already have been
        # declared (the Tasks bootstep can run before this method does), and
        # nothing else re-triggers registration for it.
        cycle._loop = loop
        cycle._update_expires_timer()

    def on_readable(self, fileno: int) -> Any:  # type: ignore[override]  # ty: ignore[invalid-method-override]
        """Handle AIO event for one of our file descriptors."""
        return self.cycle.on_readable(fileno)
