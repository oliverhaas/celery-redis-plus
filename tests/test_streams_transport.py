"""Tests for the Redis Streams transport."""

from __future__ import annotations

import os
import socket
import time
import weakref
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kombu import Connection
from kombu.transport import TRANSPORT_ALIASES, get_transport_cls
from kombu.utils.json import dumps as json_dumps
from vine import promise

from celery_redis_plus import signals
from celery_redis_plus.constants import (
    CONSUMER_IDLE_CLEANUP_FACTOR,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_PRIORITY_STEPS,
    DELAYED_KEY_PREFIX,
    HEARTBEAT_INTERVAL_DIVISOR,
    SHUTDOWN_IDLE_MS,
    STREAM_KEY_PREFIX,
)
from celery_redis_plus.streams import (
    Channel,
    MultiChannelPoller,
    QoS,
    Transport,
    client_lib,
    priority_to_level,
)
from celery_redis_plus.transport import PrefixedStrictRedis, _client_exceptions


@pytest.mark.unit
class TestStreamsConstants:
    """Tests for the streams transport constants."""

    def test_stream_key_prefix(self) -> None:
        """Test that per-(queue, level) streams use the stream: prefix."""
        assert STREAM_KEY_PREFIX == "stream:"

    def test_delayed_key_prefix(self) -> None:
        """Test that per-queue delayed sorted sets use the delayed: prefix."""
        assert DELAYED_KEY_PREFIX == "delayed:"

    def test_default_priority_steps(self) -> None:
        """Test the default priority step set (buckets in 0-255 space)."""
        assert DEFAULT_PRIORITY_STEPS == [0, 3, 6, 9]

    def test_default_consumer_group(self) -> None:
        """Test the default consumer group name used on every queue stream."""
        assert DEFAULT_CONSUMER_GROUP == "celery"

    def test_shutdown_idle_ms(self) -> None:
        """Test that shutdown idle time is far above any sane visibility timeout."""
        assert SHUTDOWN_IDLE_MS == 2**31

    def test_heartbeat_interval_divisor(self) -> None:
        """Test the divisor for deriving heartbeat_interval from visibility_timeout."""
        assert HEARTBEAT_INTERVAL_DIVISOR == 5

    def test_consumer_idle_cleanup_factor(self) -> None:
        """Test the idle factor after which stale consumers are deleted."""
        assert CONSUMER_IDLE_CLEANUP_FACTOR == 12


@pytest.mark.unit
class TestPriorityToLevel:
    """Tests for priority-to-level bucketing."""

    def test_priority_below_lowest_step(self) -> None:
        """Test that a priority below all steps buckets to the lowest step."""
        assert priority_to_level(1, [3, 6, 9]) == 3

    def test_priority_at_lowest_step(self) -> None:
        """Test that priority 0 buckets to step 0 with the default steps."""
        assert priority_to_level(0, DEFAULT_PRIORITY_STEPS) == 0

    def test_priority_at_exact_step(self) -> None:
        """Test that a priority exactly at a step buckets to that step."""
        assert priority_to_level(6, [0, 3, 6, 9]) == 6

    def test_priority_between_steps(self) -> None:
        """Test that a priority between two steps buckets to the lower step."""
        assert priority_to_level(5, [0, 3, 6, 9]) == 3

    def test_priority_above_highest_step(self) -> None:
        """Test that a priority above all steps buckets to the highest step."""
        assert priority_to_level(200, [0, 3, 6, 9]) == 9

    def test_priority_255_default_steps(self) -> None:
        """Test that the maximum priority buckets to the highest default step."""
        assert priority_to_level(255, DEFAULT_PRIORITY_STEPS) == 9

    def test_full_range_steps(self) -> None:
        """Test bucketing extremes with steps spanning the full 0-255 space."""
        steps = [0, 64, 128, 255]
        assert priority_to_level(0, steps) == 0
        assert priority_to_level(254, steps) == 128
        assert priority_to_level(255, steps) == 255

    def test_single_step_absorbs_all_priorities(self) -> None:
        """Test that a single step absorbs the whole 0-255 range."""
        assert priority_to_level(0, [5]) == 5
        assert priority_to_level(255, [5]) == 5


@pytest.mark.unit
class TestStreamsPrefixMixin:
    """Tests for the stream command additions to GlobalKeyPrefixMixin."""

    def test_prefix_xack(self) -> None:
        """Test XACK key prefixing (XACK key group id [id ...])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XACK", "stream:celery:9", "celery", "1-0"])
        assert args == ["XACK", "test:stream:celery:9", "celery", "1-0"]

    def test_prefix_xdel(self) -> None:
        """Test XDEL key prefixing (XDEL key id [id ...])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XDEL", "stream:celery:9", "1-0", "2-0"])
        assert args == ["XDEL", "test:stream:celery:9", "1-0", "2-0"]

    def test_prefix_xlen(self) -> None:
        """Test XLEN key prefixing (XLEN key)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XLEN", "stream:celery:0"])
        assert args == ["XLEN", "test:stream:celery:0"]

    def test_prefix_xpending(self) -> None:
        """Test XPENDING key prefixing (XPENDING key group [start end count [consumer]])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XPENDING", "stream:celery:9", "celery", "-", "+", "10", "worker1"])
        assert args == ["XPENDING", "test:stream:celery:9", "celery", "-", "+", "10", "worker1"]

    def test_prefix_xautoclaim(self) -> None:
        """Test XAUTOCLAIM key prefixing (XAUTOCLAIM key group consumer min-idle-time start)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(
            ["XAUTOCLAIM", "stream:celery:9", "celery", "worker1", "300000", "0-0", "COUNT", "10"],
        )
        assert args == ["XAUTOCLAIM", "test:stream:celery:9", "celery", "worker1", "300000", "0-0", "COUNT", "10"]

    def test_prefix_xclaim(self) -> None:
        """Test XCLAIM key prefixing (XCLAIM key group consumer min-idle-time id [id ...] [JUSTID])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XCLAIM", "stream:celery:9", "celery", "worker1", "0", "1-0", "JUSTID"])
        assert args == ["XCLAIM", "test:stream:celery:9", "celery", "worker1", "0", "1-0", "JUSTID"]

    def test_prefix_xrange(self) -> None:
        """Test XRANGE key prefixing (XRANGE key start end)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XRANGE", "stream:celery:9", "-", "+"])
        assert args == ["XRANGE", "test:stream:celery:9", "-", "+"]

    def test_prefix_xtrim(self) -> None:
        """Test XTRIM key prefixing (XTRIM key MAXLEN [~] threshold)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XTRIM", "dead_letters", "MAXLEN", "~", "10000"])
        assert args == ["XTRIM", "test:dead_letters", "MAXLEN", "~", "10000"]

    def test_prefix_xreadgroup(self) -> None:
        """Test XREADGROUP prefixes stream keys but not group, consumer, or IDs."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(
            [
                "XREADGROUP",
                "GROUP",
                "celery",
                "worker1",
                "COUNT",
                "1",
                "BLOCK",
                "1000",
                "STREAMS",
                "stream:celery:9",
                "stream:celery:0",
                ">",
                ">",
            ],
        )
        assert args == [
            "XREADGROUP",
            "GROUP",
            "celery",
            "worker1",
            "COUNT",
            "1",
            "BLOCK",
            "1000",
            "STREAMS",
            "test:stream:celery:9",
            "test:stream:celery:0",
            ">",
            ">",
        ]

    def test_prefix_xreadgroup_without_streams_keyword(self) -> None:
        """Test XREADGROUP without STREAMS keyword returns args unchanged."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XREADGROUP", "GROUP", "celery", "worker1", "stream:celery:9", ">"])
        assert args == ["XREADGROUP", "GROUP", "celery", "worker1", "stream:celery:9", ">"]

    def test_prefix_xgroup_create(self) -> None:
        """Test XGROUP CREATE prefixes the key at args[1] (XGROUP CREATE key group id [MKSTREAM])."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP", "CREATE", "stream:celery:9", "celery", "0", "MKSTREAM"])
        assert args == ["XGROUP", "CREATE", "test:stream:celery:9", "celery", "0", "MKSTREAM"]

    def test_prefix_xgroup_delconsumer(self) -> None:
        """Test XGROUP DELCONSUMER prefixes the key (XGROUP DELCONSUMER key group consumer)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP", "DELCONSUMER", "stream:celery:9", "celery", "worker1"])
        assert args == ["XGROUP", "DELCONSUMER", "test:stream:celery:9", "celery", "worker1"]

    def test_prefix_xinfo_stream(self) -> None:
        """Test XINFO STREAM prefixes the key at args[1] (XINFO STREAM key)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO", "STREAM", "stream:celery:9"])
        assert args == ["XINFO", "STREAM", "test:stream:celery:9"]

    def test_prefix_xinfo_consumers(self) -> None:
        """Test XINFO CONSUMERS prefixes the key but not the group (XINFO CONSUMERS key group)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO", "CONSUMERS", "stream:celery:9", "celery"])
        assert args == ["XINFO", "CONSUMERS", "test:stream:celery:9", "celery"]

    def test_no_prefix_when_empty(self) -> None:
        """Test that an empty prefix leaves stream command keys unchanged."""
        client = PrefixedStrictRedis(connection_pool=MagicMock())

        args = client._prefix_args(["XACK", "stream:celery:9", "celery", "1-0"])
        assert args == ["XACK", "stream:celery:9", "celery", "1-0"]


@pytest.mark.unit
class TestStreamsChannelSetup:
    """Tests for streams Channel key helpers, configuration properties, and QoS setup."""

    def test_stream_key_format(self) -> None:
        """Test that _stream_key builds stream:{queue}:{level} keys."""
        channel = object.__new__(Channel)

        assert channel._stream_key("celery", 9) == "stream:celery:9"
        assert channel._stream_key("other_queue", 0) == "stream:other_queue:0"

    def test_delayed_key_format(self) -> None:
        """Test that _delayed_key builds delayed:{queue} keys."""
        channel = object.__new__(Channel)

        assert channel._delayed_key("celery") == "delayed:celery"

    def test_stream_keys_for_queue_highest_level_first(self) -> None:
        """Test that _stream_keys_for_queue returns all level streams, highest level first."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        assert channel._stream_keys_for_queue("celery") == [
            "stream:celery:9",
            "stream:celery:6",
            "stream:celery:3",
            "stream:celery:0",
        ]

    def test_priority_steps_default(self) -> None:
        """Test that priority_steps defaults to DEFAULT_PRIORITY_STEPS sorted ascending."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        assert channel.priority_steps == sorted(DEFAULT_PRIORITY_STEPS)
        assert channel.priority_steps == [0, 3, 6, 9]

    def test_priority_steps_transport_option_is_sorted(self) -> None:
        """Test that a priority_steps transport option is normalized to ascending order."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"priority_steps": [9, 0, 5]}
        channel.connection = mock_connection

        assert channel.priority_steps == [0, 5, 9]

    def test_consumer_group_default(self) -> None:
        """Test that consumer_group defaults to the celery group name."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        assert channel.consumer_group == "celery"

    def test_consumer_group_transport_option(self) -> None:
        """Test that the consumer_group transport option overrides the default."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"consumer_group": "mygroup"}
        channel.connection = mock_connection

        assert channel.consumer_group == "mygroup"

    @pytest.mark.parametrize("transport_cls", ["valkeys-streams", "valkeys+streams"])
    def test_connparams_ssl_for_valkeys_streams(self, transport_cls: str) -> None:
        """Test that valkeys-streams and the valkeys+streams alias enable the SSL connection class."""
        channel = object.__new__(Channel)
        channel.max_connections = 10
        channel.socket_timeout = None
        channel.socket_connect_timeout = None
        channel.socket_keepalive = None
        channel.socket_keepalive_options = None
        channel.health_check_interval = 25
        channel.retry_on_timeout = None
        channel.client_name = None
        channel.credential_provider = None

        mock_connection = MagicMock()
        mock_connection.client.hostname = "somehost"
        mock_connection.client.port = 6379
        mock_connection.client.virtual_host = "0"
        mock_connection.client.userid = None
        mock_connection.client.password = None
        mock_connection.client.ssl = None
        mock_connection.transport_cls = transport_cls
        channel.connection = mock_connection

        connparams = channel._connparams()

        assert connparams["connection_class"] is Channel.connection_class_ssl

    @pytest.mark.parametrize("transport_cls", ["valkey-streams", "valkey+streams"])
    def test_connparams_no_ssl_for_plain_scheme(self, transport_cls: str) -> None:
        """Test that valkey-streams and the valkey+streams alias keep the plain connection class."""
        channel = object.__new__(Channel)
        channel.max_connections = 10
        channel.socket_timeout = None
        channel.socket_connect_timeout = None
        channel.socket_keepalive = None
        channel.socket_keepalive_options = None
        channel.health_check_interval = 25
        channel.retry_on_timeout = None
        channel.client_name = None
        channel.credential_provider = None

        mock_connection = MagicMock()
        mock_connection.client.hostname = "somehost"
        mock_connection.client.port = 6379
        mock_connection.client.virtual_host = "0"
        mock_connection.client.userid = None
        mock_connection.client.password = None
        mock_connection.client.ssl = None
        mock_connection.client.transport_options = {}
        mock_connection.transport_cls = transport_cls
        channel.connection = mock_connection

        connparams = channel._connparams()

        assert connparams["connection_class"] is Channel.connection_class

    def test_qos_tracks_in_flight_and_fanout_tags(self) -> None:
        """Test that QoS initializes PEL in-flight and fanout tag tracking."""
        qos = QoS(MagicMock())

        assert qos._in_flight == {}
        assert qos._fanout_tags == set()

    def test_qos_visibility_timeout_from_channel(self) -> None:
        """Test that QoS visibility_timeout mirrors the channel's."""
        qos = object.__new__(QoS)
        mock_channel = MagicMock()
        mock_channel.visibility_timeout = 123.0
        qos.channel = mock_channel

        assert qos.visibility_timeout == 123.0


@pytest.mark.unit
class TestStreamsConsumerName:
    """Tests for the stable per-worker consumer name."""

    def test_consumer_name_falls_back_to_hostname_pid(self) -> None:
        """Test that consumer_name defaults to hostname:pid when no nodename is registered."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        with patch.object(signals, "_worker_nodenames", weakref.WeakKeyDictionary()):
            assert channel.consumer_name == f"{socket.gethostname()}:{os.getpid()}"

    def test_consumer_name_is_stable_across_calls(self) -> None:
        """Test that consumer_name does not change between calls (never uuid-per-boot)."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        assert channel.consumer_name == channel.consumer_name

    def test_consumer_name_uses_registered_worker_nodename(self) -> None:
        """Test that consumer_name resolves to the nodename recorded for the channel's app."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        registry = weakref.WeakKeyDictionary()
        registry[mock_connection.client.app] = "worker1@examplehost"
        with patch.object(signals, "_worker_nodenames", registry):
            assert channel.consumer_name == "worker1@examplehost"

    def test_consumer_name_falls_back_to_sole_registered_nodename(self) -> None:
        """Test the single-app fallback when the connection chain exposes no app."""
        channel = object.__new__(Channel)
        # SimpleNamespace has no .app attribute, unlike MagicMock, so the
        # registry lookup takes the AttributeError fallback path
        channel.connection = SimpleNamespace(client=SimpleNamespace(transport_options={}))

        # App stand-in must be weakref-able (SimpleNamespace is not); the local
        # strong reference keeps the weak registry entry alive during the test
        app = MagicMock()
        registry = weakref.WeakKeyDictionary()
        registry[app] = "worker1@examplehost"
        with patch.object(signals, "_worker_nodenames", registry):
            assert channel.consumer_name == "worker1@examplehost"

    def test_consumer_name_transport_option_override(self) -> None:
        """Test that the consumer_name transport option overrides the fallback."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"consumer_name": "worker-7"}
        channel.connection = mock_connection

        assert channel.consumer_name == "worker-7"

    def test_consumer_name_transport_option_overrides_nodename(self) -> None:
        """Test that the consumer_name transport option wins over a registered nodename."""
        channel = object.__new__(Channel)
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"consumer_name": "worker-7"}
        channel.connection = mock_connection

        registry = weakref.WeakKeyDictionary()
        registry[mock_connection.client.app] = "worker1@examplehost"
        with patch.object(signals, "_worker_nodenames", registry):
            assert channel.consumer_name == "worker-7"


@pytest.mark.unit
class TestStreamsTransportSetup:
    """Tests for streams Transport setup and valkey-streams scheme registration."""

    def test_transport_aliases_registered(self) -> None:
        """Test that importing the package registers the streams transport aliases."""
        # Importing celery_redis_plus.streams (top of this module) imports the
        # package, which registers the aliases at import time.
        assert TRANSPORT_ALIASES["valkey-streams"] == "celery_redis_plus.streams:Transport"
        assert TRANSPORT_ALIASES["valkeys-streams"] == "celery_redis_plus.streams:Transport"
        assert TRANSPORT_ALIASES["valkey+streams"] == "celery_redis_plus.streams:Transport"
        assert TRANSPORT_ALIASES["valkeys+streams"] == "celery_redis_plus.streams:Transport"

    def test_alias_resolves_to_streams_transport(self) -> None:
        """Test that kombu resolves the streams aliases to our Transport class."""
        assert get_transport_cls("valkey-streams") is Transport
        assert get_transport_cls("valkeys-streams") is Transport
        assert get_transport_cls("valkey+streams") is Transport
        assert get_transport_cls("valkeys+streams") is Transport

    def test_connection_with_streams_transport_name(self) -> None:
        """Test transport selection the way Celery's broker_transport setting does it.

        kombu splits scheme+rest:// URLs into (transport, sub-URL) before any
        alias lookup, so the streams transport is selected via an explicit
        transport name plus a plain broker URL.
        """
        conn = Connection("redis://somehost:6380/3", transport="valkey+streams")

        assert conn.transport_cls == "valkey+streams"
        assert conn.get_transport_cls() is Transport
        assert conn.hostname == "somehost"
        assert conn.port == 6380

    def test_connection_with_streams_url_scheme(self) -> None:
        """Test that a bare valkey-streams:// URL selects the streams transport.

        kombu splits bare URL schemes at '+' before alias lookup, so only the
        hyphen form works in a bare broker URL; the '+' aliases exist for
        broker_transport users.
        """
        conn = Connection("valkey-streams://somehost:6380/3")

        assert conn.transport_cls == "valkey-streams"
        assert conn.get_transport_cls() is Transport
        assert conn.hostname == "somehost"
        assert conn.port == 6380
        assert conn.virtual_host == "3"

    def test_supports_native_delayed_delivery_flag(self) -> None:
        """Test that the streams transport has the native delayed delivery flag."""
        assert Transport.supports_native_delayed_delivery is True

    def test_uses_streams_channel(self) -> None:
        """Test that the transport uses the streams Channel class."""
        assert Transport.Channel is Channel

    def test_implements_async_and_exchanges(self) -> None:
        """Test that transport implements async and all exchange types."""
        assert Transport.implements.asynchronous is True
        assert "direct" in Transport.implements.exchange_type
        assert "topic" in Transport.implements.exchange_type
        assert "fanout" in Transport.implements.exchange_type

    def test_driver_version(self) -> None:
        """Test that driver_version returns the client library version string."""
        transport = MagicMock(spec=Transport)
        transport.driver_version = Transport.driver_version
        version = transport.driver_version(transport)
        assert version == client_lib.__version__

    def test_init_creates_streams_poller(self) -> None:
        """Test that Transport.__init__ creates this module's MultiChannelPoller."""
        mock_client = MagicMock()
        mock_client.transport_options = {}
        transport = Transport(mock_client)

        assert isinstance(transport.cycle, MultiChannelPoller)

    def test_poller_add_and_discard_channel(self) -> None:
        """Test adding and removing channels from the poller."""
        poller = MultiChannelPoller()
        channel = MagicMock()

        poller.add(channel)
        assert channel in poller._channels

        poller.discard(channel)
        assert channel not in poller._channels

    def test_poller_periodic_placeholders_are_safe(self) -> None:
        """Test that timer callbacks are callable before the consume cycle exists."""
        poller = MultiChannelPoller()

        assert poller.maybe_enqueue_due_messages() == 0
        assert poller.maybe_heartbeat() is None
        assert poller.maybe_refresh_queue_expires() is None
        assert poller._update_expires_timer() is None


@pytest.mark.unit
class TestStreamsQueueCycle:
    """Tests for consume registration: active_queues, queue-cycle refresh, and fanout tracking."""

    def _make_channel(self) -> Channel:
        """Bare channel carrying exactly the state consume registration touches."""
        channel = object.__new__(Channel)
        channel._tag_to_queue = {}
        channel._active_queues = []
        channel._consumers = set()
        channel._queue_cycle = []
        channel._fanout_queues = {}
        channel.active_fanout_queues = set()
        mock_connection = MagicMock()
        mock_connection._callbacks = {}
        mock_connection.cycle._in_protected_read = False
        channel.connection = mock_connection
        return channel

    def test_active_queues_excludes_fanout(self) -> None:
        """Test that active_queues is the watched queue set minus fanout queues."""
        channel = object.__new__(Channel)
        channel._active_queues = ["q1", "q2", "bcast"]
        channel.active_fanout_queues = {"bcast"}

        assert channel.active_queues == {"q1", "q2"}

    def test_update_queue_cycle_reflects_active_queues(self) -> None:
        """Test that _update_queue_cycle rebuilds _queue_cycle from active_queues."""
        channel = object.__new__(Channel)
        channel._active_queues = ["q1", "bcast", "q2"]
        channel.active_fanout_queues = {"bcast"}
        channel._queue_cycle = []

        channel._update_queue_cycle()

        assert set(channel._queue_cycle) == {"q1", "q2"}
        assert len(channel._queue_cycle) == 2

    def test_basic_consume_updates_queue_cycle(self) -> None:
        """Test that basic_consume registers the queue in the round-robin cycle."""
        channel = self._make_channel()

        channel.basic_consume("celery", no_ack=True, callback=lambda *_a: None, consumer_tag="ctag-1")

        assert "celery" in channel._active_queues
        assert channel._queue_cycle == ["celery"]

    def test_basic_consume_fanout_queue_tracked_not_cycled(self) -> None:
        """Test that a fanout queue goes to active_fanout_queues, not the XREADGROUP cycle."""
        channel = self._make_channel()
        channel._fanout_queues["bcast"] = ("bcast_exchange", "")

        channel.basic_consume("bcast", no_ack=True, callback=lambda *_a: None, consumer_tag="ctag-f")

        assert "bcast" in channel.active_fanout_queues
        assert channel._queue_cycle == []

    def test_basic_cancel_updates_queue_cycle(self) -> None:
        """Test that basic_cancel removes the queue and rebuilds the cycle."""
        channel = self._make_channel()
        channel.basic_consume("celery", no_ack=True, callback=lambda *_a: None, consumer_tag="ctag-1")
        # Prime a stale cycle so the test proves basic_cancel rebuilds it
        channel._queue_cycle = ["stale-entry", "celery"]

        channel.basic_cancel("ctag-1")

        assert "celery" not in channel._active_queues
        assert channel._queue_cycle == []

    def test_basic_cancel_removes_fanout_queue(self) -> None:
        """Test that cancelling a fanout consumer stops XREAD tracking for its queue."""
        channel = self._make_channel()
        channel._fanout_queues["bcast"] = ("bcast_exchange", "")
        channel.basic_consume("bcast", no_ack=True, callback=lambda *_a: None, consumer_tag="ctag-f")
        channel.active_fanout_queues.add("bcast")  # no-op once basic_consume tracks fanout

        channel.basic_cancel("ctag-f")

        assert "bcast" not in channel.active_fanout_queues

    def test_basic_cancel_deferred_during_protected_read(self) -> None:
        """Test that basic_cancel defers via an after_read promise during a protected read."""
        channel = self._make_channel()
        channel.basic_consume("celery", no_ack=True, callback=lambda *_a: None, consumer_tag="ctag-1")
        channel.connection.cycle._in_protected_read = True

        channel.basic_cancel("ctag-1")

        # The cancel must not have run yet
        assert "celery" in channel._active_queues
        channel.connection.cycle.after_read.add.assert_called_once()
        deferred = channel.connection.cycle.after_read.add.call_args[0][0]
        assert isinstance(deferred, promise)
        assert deferred.fun == channel._basic_cancel
        assert deferred.args == ("ctag-1",)

        # Running the promise performs the deferred cancel
        deferred()
        assert "celery" not in channel._active_queues
        assert channel._queue_cycle == []


@pytest.mark.unit
class TestStreamsEnsureGroup:
    """Unit tests for Channel._ensure_group (lazy consumer group creation with BUSYGROUP cache)."""

    def test_ensure_group_creates_group_with_mkstream(self) -> None:
        """Test that _ensure_group creates the group at id 0 with MKSTREAM and caches the stream key."""
        channel = object.__new__(Channel)
        channel._ensured_groups = set()
        channel.ResponseError = _client_exceptions.ResponseError
        # consumer_group reads self.connection.client.transport_options, and a bare
        # object.__new__ instance has no connection, so mock the whole chain
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._ensure_group("stream:my_queue:0")

        mock_client.xgroup_create.assert_called_once_with(
            "stream:my_queue:0",
            DEFAULT_CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
        assert "stream:my_queue:0" in channel._ensured_groups

    def test_ensure_group_cached_key_skips_redis_call(self) -> None:
        """Test that a second _ensure_group call for the same stream key is a no-op."""
        channel = object.__new__(Channel)
        channel._ensured_groups = set()
        channel.ResponseError = _client_exceptions.ResponseError
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._ensure_group("stream:my_queue:0")
        channel._ensure_group("stream:my_queue:0")

        assert mock_client.xgroup_create.call_count == 1

    def test_ensure_group_ignores_busygroup_and_caches(self) -> None:
        """Test that a BUSYGROUP error (group already exists) is swallowed and the key is cached."""
        channel = object.__new__(Channel)
        channel._ensured_groups = set()
        channel.ResponseError = _client_exceptions.ResponseError
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = _client_exceptions.ResponseError(
            "BUSYGROUP Consumer Group name already exists",
        )
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        channel._ensure_group("stream:my_queue:0")

        assert "stream:my_queue:0" in channel._ensured_groups

    def test_ensure_group_reraises_other_response_errors(self) -> None:
        """Test that non-BUSYGROUP response errors propagate and the key is NOT cached."""
        channel = object.__new__(Channel)
        channel._ensured_groups = set()
        channel.ResponseError = _client_exceptions.ResponseError
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_client.xgroup_create.side_effect = _client_exceptions.ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with pytest.raises(_client_exceptions.ResponseError, match="WRONGTYPE"):
            channel._ensure_group("stream:my_queue:0")

        assert "stream:my_queue:0" not in channel._ensured_groups


@pytest.mark.unit
class TestStreamsPut:
    """Unit tests for Channel._put (XADD to priority stream / ZADD to delayed zset)."""

    def test_put_immediate_xadds_to_level_stream(self, global_keyprefix: str) -> None:
        """Test that an immediate message is XADDed to the level-0 stream with the group ensured first."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {}
        channel._ensure_group = MagicMock()
        # priority_steps reads self.connection.client.transport_options, and a bare
        # object.__new__ instance has no connection, so mock the whole chain
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        channel._ensure_group.assert_called_once_with("stream:my_queue:0")
        mock_client.xadd.assert_called_once_with(
            name="stream:my_queue:0",
            fields={"payload": json_dumps(message)},
            id="*",
        )
        mock_client.zadd.assert_not_called()
        mock_client.pexpire.assert_not_called()

    @pytest.mark.parametrize(
        ("priority", "expected_level"),
        [(0, 0), (2, 0), (3, 3), (5, 3), (6, 6), (8, 6), (9, 9), (200, 9)],
    )
    def test_put_priority_bucketing_selects_level_stream(self, priority: int, expected_level: int) -> None:
        """Test that message priority is bucketed onto the highest step <= priority."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._expires = {}
        channel._ensure_group = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "priority": priority,
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        channel._ensure_group.assert_called_once_with(f"stream:my_queue:{expected_level}")
        mock_client.xadd.assert_called_once_with(
            name=f"stream:my_queue:{expected_level}",
            fields={"payload": json_dumps(message)},
            id="*",
        )

    def test_put_native_delayed_zadds_with_eta_ms_score(self, global_keyprefix: str) -> None:
        """Test that a message with eta beyond the requeue check interval goes to the delayed zset."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {}
        channel._ensure_group = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        eta_timestamp = time.time() + 120  # far beyond the patched DEFAULT_REQUEUE_CHECK_INTERVAL (2s)
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "eta": eta_timestamp,
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        channel._ensure_group.assert_not_called()
        mock_client.xadd.assert_not_called()
        mock_client.zadd.assert_called_once()
        args, _kwargs = mock_client.zadd.call_args
        assert args[0] == "delayed:my_queue"
        assert args[1] == {json_dumps(message): eta_timestamp * 1000}
        mock_client.pexpire.assert_not_called()

    def test_put_short_eta_goes_to_stream(self, global_keyprefix: str) -> None:
        """Test that a short eta (below the requeue check interval) is published immediately."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {}
        channel._ensure_group = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        eta_timestamp = time.time() + 1  # below the patched DEFAULT_REQUEUE_CHECK_INTERVAL (2s)
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "eta": eta_timestamp,
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        mock_client.zadd.assert_not_called()
        mock_client.xadd.assert_called_once_with(
            name="stream:my_queue:0",
            fields={"payload": json_dumps(message)},
            id="*",
        )

    def test_put_immediate_applies_x_expires_pexpire(self, global_keyprefix: str) -> None:
        """Test that x-expires queues get PEXPIRE on the touched stream key."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {"my_queue": 30_000}
        channel._ensure_group = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        mock_client.pexpire.assert_called_once_with("stream:my_queue:0", 30_000)

    def test_put_delayed_applies_x_expires_pexpire(self, global_keyprefix: str) -> None:
        """Test that x-expires queues get PEXPIRE on the delayed zset for native delayed messages."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._expires = {"my_queue": 30_000}
        channel._ensure_group = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}
        channel.connection = mock_connection

        mock_client = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        eta_timestamp = time.time() + 120
        message = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag123",
                "eta": eta_timestamp,
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
                "headers": {},
            },
        }

        channel._put("my_queue", message)

        mock_client.pexpire.assert_called_once_with("delayed:my_queue", 30_000)
