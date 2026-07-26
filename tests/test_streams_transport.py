"""Tests for the Redis Streams transport."""

from __future__ import annotations

import logging
import os
import socket
import time
import weakref
from collections import OrderedDict
from pathlib import Path
from queue import Empty
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
from kombu import Connection
from kombu.asynchronous import Hub
from kombu.transport import TRANSPORT_ALIASES, get_transport_cls
from kombu.utils.eventio import ERR, READ
from kombu.utils.json import dumps as json_dumps
from vine import promise

import celery_redis_plus
from celery_redis_plus import signals
from celery_redis_plus.constants import (
    CONSUMER_IDLE_CLEANUP_FACTOR,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_PRIORITY_STEPS,
    DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT,
    DEFAULT_REQUEUE_BATCH_LIMIT,
    DEFAULT_VISIBILITY_TIMEOUT,
    DELAYED_KEY_PREFIX,
    HEARTBEAT_INTERVAL_DIVISOR,
    SHUTDOWN_IDLE_MS,
    STREAM_KEY_PREFIX,
)
from celery_redis_plus.streams import (
    _STREAMS_ACK_LUA,
    _STREAMS_CONSUME_LUA,
    DEFAULT_REQUEUE_CHECK_INTERVAL,
    Channel,
    MultiChannelPoller,
    QoS,
    Transport,
    client_lib,
    priority_to_level,
)
from celery_redis_plus.transport import (
    PrefixedStrictRedis,
    _client_exceptions,
    _connection_errors,
    _release_channel_on_collect,
)


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
        """Test XGROUP CREATE prefixes the key at args[0].

        redis-py's xgroup_create() sends the subcommand fused into the
        command name as a single "XGROUP CREATE" token (not "XGROUP" plus a
        separate "CREATE" argument), so the stream key is the first argument
        after the command name, not the second.
        """
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP CREATE", "stream:celery:9", "celery", "0", "MKSTREAM"])
        assert args == ["XGROUP CREATE", "test:stream:celery:9", "celery", "0", "MKSTREAM"]

    def test_prefix_xgroup_delconsumer(self) -> None:
        """Test XGROUP DELCONSUMER prefixes the key (single "XGROUP DELCONSUMER" command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP DELCONSUMER", "stream:celery:9", "celery", "worker1"])
        assert args == ["XGROUP DELCONSUMER", "test:stream:celery:9", "celery", "worker1"]

    def test_prefix_xinfo_stream(self) -> None:
        """Test XINFO STREAM prefixes the key at args[0] (single "XINFO STREAM" command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO STREAM", "stream:celery:9"])
        assert args == ["XINFO STREAM", "test:stream:celery:9"]

    def test_prefix_xinfo_consumers(self) -> None:
        """Test XINFO CONSUMERS prefixes the key but not the group (single command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO CONSUMERS", "stream:celery:9", "celery"])
        assert args == ["XINFO CONSUMERS", "test:stream:celery:9", "celery"]

    def test_prefix_xgroup_setid(self) -> None:
        """Test XGROUP SETID prefixes the key (single "XGROUP SETID" command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP SETID", "stream:celery:9", "celery", "0"])
        assert args == ["XGROUP SETID", "test:stream:celery:9", "celery", "0"]

    def test_prefix_xgroup_destroy(self) -> None:
        """Test XGROUP DESTROY prefixes the key (single "XGROUP DESTROY" command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP DESTROY", "stream:celery:9", "celery"])
        assert args == ["XGROUP DESTROY", "test:stream:celery:9", "celery"]

    def test_prefix_xgroup_createconsumer(self) -> None:
        """Test XGROUP CREATECONSUMER prefixes the key (single command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XGROUP CREATECONSUMER", "stream:celery:9", "celery", "worker1"])
        assert args == ["XGROUP CREATECONSUMER", "test:stream:celery:9", "celery", "worker1"]

    def test_prefix_xinfo_groups(self) -> None:
        """Test XINFO GROUPS prefixes the key (single "XINFO GROUPS" command name)."""
        client = PrefixedStrictRedis(connection_pool=MagicMock(), global_keyprefix="test:")

        args = client._prefix_args(["XINFO GROUPS", "stream:celery:9"])
        assert args == ["XINFO GROUPS", "test:stream:celery:9"]

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

        assert "celery" in channel._active_queues
        channel.connection.cycle.after_read.add.assert_called_once()
        deferred = channel.connection.cycle.after_read.add.call_args[0][0]
        assert isinstance(deferred, promise)
        assert deferred.fun == channel._basic_cancel
        assert deferred.args == ("ctag-1",)

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


@pytest.mark.unit
class TestStreamsInvalidateGroup:
    """Unit tests for Channel._invalidate_group (NOGROUP self-healing cache invalidation)."""

    def test_invalidate_group_discards_queue_stream_keys(self) -> None:
        """Test that invalidating a queue discards only that queue's level stream keys."""
        channel = object.__new__(Channel)
        channel._ensured_groups = {"stream:my_queue:9", "stream:my_queue:0", "stream:other:0"}
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:my_queue:9", "stream:my_queue:0"])

        channel._invalidate_group("my_queue")

        assert channel._ensured_groups == {"stream:other:0"}
        channel._stream_keys_for_queue.assert_called_once_with("my_queue")

    def test_invalidate_group_tolerates_uncached_keys(self) -> None:
        """Test that invalidating a queue with no cached keys is a harmless no-op."""
        channel = object.__new__(Channel)
        channel._ensured_groups = {"stream:other:0"}
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:my_queue:9", "stream:my_queue:0"])

        channel._invalidate_group("my_queue")

        assert channel._ensured_groups == {"stream:other:0"}

    def test_invalidate_group_without_queue_clears_cache(self) -> None:
        """Test that invalidating without a queue clears the whole ensured-group cache."""
        channel = object.__new__(Channel)
        channel._ensured_groups = {"stream:my_queue:0", "stream:other:0"}
        channel._stream_keys_for_queue = MagicMock()

        channel._invalidate_group()

        assert channel._ensured_groups == set()
        channel._stream_keys_for_queue.assert_not_called()


@pytest.mark.unit
class TestStreamsConsumeLua:
    """Tests for the streams_consume.lua script file and its module-level loading."""

    def test_script_file_loaded_at_import(self) -> None:
        """Test that streams_consume.lua exists in the package and is loaded into _STREAMS_CONSUME_LUA."""
        script_path = Path(celery_redis_plus.__file__).parent / "streams_consume.lua"
        assert script_path.is_file()
        assert script_path.read_text() == _STREAMS_CONSUME_LUA
        assert _STREAMS_CONSUME_LUA.strip()

    def test_script_consumes_via_xreadgroup(self) -> None:
        """Test that the script reads new entries via XREADGROUP (atomic deliver + PEL register)."""
        assert "XREADGROUP" in _STREAMS_CONSUME_LUA
        assert "'>'" in _STREAMS_CONSUME_LUA

    def test_script_lazy_drops_expired_entries_with_xack_xdel(self) -> None:
        """Test that expired entries (x-message-ttl) are dropped inside the script via XACK + XDEL."""
        assert "XACK" in _STREAMS_CONSUME_LUA
        assert "XDEL" in _STREAMS_CONSUME_LUA

    def test_script_returns_false_when_all_streams_empty(self) -> None:
        """Test that the script falls through to `return false` (nil to redis-py) on total miss."""
        assert _STREAMS_CONSUME_LUA.rstrip().endswith("return false")


@pytest.mark.unit
class TestStreamsGet:
    """Tests for the synchronous Channel._get consume path (streams_consume Lua script)."""

    def test_get_calls_consume_script_with_level_keys_highest_first(self, global_keyprefix: str) -> None:
        """Test _get passes prefixed level-stream KEYS (highest level first) and group/consumer/ttl ARGV."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(
            return_value=["stream:my_queue:9", "stream:my_queue:6", "stream:my_queue:3", "stream:my_queue:0"],
        )
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos

        payload = {"body": "test", "properties": {"delivery_tag": "tag123"}}
        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_script.return_value = [
            f"{global_keyprefix}stream:my_queue:9".encode(),
            b"1700000000000-0",
            json_dumps(payload).encode(),
        ]
        mock_client.register_script.return_value = mock_script
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", "celery", create=True),
            patch.object(Channel, "consumer_name", "testhost:4242", create=True),
        ):
            message = channel._get("my_queue")

        assert message == payload
        # Consumer groups are ensured for every level stream (avoids NOGROUP inside the script)
        assert channel._ensure_group.call_count == 4
        channel._ensure_group.assert_any_call("stream:my_queue:0")
        # KEYS: manually prefixed (EVALSHA does not auto-prefix), highest level first
        keys = mock_script.call_args.kwargs["keys"]
        assert keys == [
            f"{global_keyprefix}stream:my_queue:9",
            f"{global_keyprefix}stream:my_queue:6",
            f"{global_keyprefix}stream:my_queue:3",
            f"{global_keyprefix}stream:my_queue:0",
        ]
        # ARGV: group, consumer, message_ttl_ms (0 = no TTL); now is read
        # server-side via redis.call('TIME') inside the script
        args = mock_script.call_args.kwargs["args"]
        assert args[0] == "celery"
        assert args[1] == "testhost:4242"
        assert int(args[2]) == 0

    def test_get_raises_empty_on_nil(self) -> None:
        """Test _get raises Empty and records no metadata when the script returns nil."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:my_queue:9", "stream:my_queue:0"])
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos

        mock_client = MagicMock()
        mock_script = MagicMock(return_value=None)
        mock_client.register_script.return_value = mock_script
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", "celery", create=True),
            patch.object(Channel, "consumer_name", "testhost:4242", create=True),
            pytest.raises(Empty),
        ):
            channel._get("my_queue")

        assert mock_qos._in_flight == {}

    def test_get_records_in_flight_metadata_with_unprefixed_stream_key(self, global_keyprefix: str) -> None:
        """Test _get stores (unprefixed stream key, entry id) in qos._in_flight before returning."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(
            return_value=["stream:my_queue:9", "stream:my_queue:6", "stream:my_queue:3", "stream:my_queue:0"],
        )
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos

        payload = {"body": "test", "properties": {"delivery_tag": "tag123"}}
        mock_client = MagicMock()
        mock_script = MagicMock()
        # Script reply carries the PREFIXED key; metadata must store the UNPREFIXED key
        mock_script.return_value = [
            f"{global_keyprefix}stream:my_queue:6".encode(),
            b"1700000000123-5",
            json_dumps(payload).encode(),
        ]
        mock_client.register_script.return_value = mock_script
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", "celery", create=True),
            patch.object(Channel, "consumer_name", "testhost:4242", create=True),
        ):
            message = channel._get("my_queue")

        assert message["properties"]["delivery_tag"] == "tag123"
        assert mock_qos._in_flight == {"tag123": ("stream:my_queue:6", "1700000000123-5")}

    @pytest.mark.parametrize(
        ("message_ttl", "queue_ttl_ms", "expected_ttl_ms"),
        [
            (None, None, 0),
            (60, None, 60000),
            (None, 5000, 5000),
            (60, 5000, 5000),
            (2, 5000, 2000),
        ],
        ids=["no-ttl", "channel-only", "queue-only", "queue-smaller", "channel-smaller"],
    )
    def test_get_ttl_argv_min_rule(
        self,
        message_ttl: int | None,
        queue_ttl_ms: int | None,
        expected_ttl_ms: int,
    ) -> None:
        """Test the TTL ARGV: min of channel message_ttl (seconds) and per-queue x-message-ttl (ms)."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel.message_ttl = message_ttl
        channel._message_ttls = {} if queue_ttl_ms is None else {"my_queue": queue_ttl_ms}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:my_queue:0"])
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos

        mock_client = MagicMock()
        mock_script = MagicMock(return_value=None)
        mock_client.register_script.return_value = mock_script
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", "celery", create=True),
            patch.object(Channel, "consumer_name", "testhost:4242", create=True),
            pytest.raises(Empty),
        ):
            channel._get("my_queue")

        args = mock_script.call_args.kwargs["args"]
        assert int(args[2]) == expected_ttl_ms

    def test_get_nogroup_invalidates_and_retries_once(self) -> None:
        """Test _get self-heals NOGROUP (stream deleted out of band): invalidate, re-ensure, retry once."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:my_queue:9", "stream:my_queue:0"}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:my_queue:9", "stream:my_queue:0"])
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos

        payload = {"body": "test", "properties": {"delivery_tag": "tag123"}}
        mock_client = MagicMock()
        mock_script = MagicMock()
        # First call: NOGROUP (the stream and its group were deleted out of
        # band, e.g. cross-process purge or x-expires expiry, while this
        # channel's _ensured_groups cache was warm). Second call: a hit.
        mock_script.side_effect = [
            _client_exceptions.ResponseError(
                "NOGROUP No such key 'stream:my_queue:9' or consumer group 'celery' in XREADGROUP with GROUP option",
            ),
            [b"stream:my_queue:9", b"1700000000000-0", json_dumps(payload).encode()],
        ]
        mock_client.register_script.return_value = mock_script
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", "celery", create=True),
            patch.object(Channel, "consumer_name", "testhost:4242", create=True),
        ):
            message = channel._get("my_queue")

        assert message == payload
        assert mock_script.call_count == 2
        assert channel._ensure_group.call_count == 4
        # _invalidate_group discarded the stale cache entries (_ensure_group
        # is mocked here, so nothing re-adds them)
        assert channel._ensured_groups == set()
        assert mock_qos._in_flight == {"tag123": ("stream:my_queue:9", "1700000000000-0")}


@pytest.mark.unit
class TestStreamsQoSAck:
    """Tests for QoS.ack: atomic XACK + XDEL via streams_ack.lua."""

    def test_ack_calls_script_and_pops_in_flight(self, global_keyprefix: str) -> None:
        """Test ack runs streams_ack.lua with prefixed stream key and pops the in-flight entry."""
        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.closed = False
        mock_channel._collected = False
        mock_channel.global_keyprefix = global_keyprefix
        mock_channel.consumer_group = "celery"
        mock_channel.conn_or_acquire = MagicMock(return_value=mock_context)

        qos = object.__new__(QoS)
        qos.channel = mock_channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:0", "1700000000000-0")}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.ack("tag1")

        mock_client.register_script.assert_called_once_with(_STREAMS_ACK_LUA)
        mock_script.assert_called_once_with(
            keys=[f"{global_keyprefix}{STREAM_KEY_PREFIX}my_queue:0"],
            args=["celery", "1700000000000-0", ""],
        )
        assert "tag1" not in qos._in_flight
        qos._quick_ack.assert_called_once_with("tag1")

    def test_ack_fanout_tag_bypasses_stream_ack(self) -> None:
        """Test ack for fanout message discards the tag without touching Redis."""
        qos = object.__new__(QoS)
        qos.channel = MagicMock(closed=False, _collected=False)
        qos._fanout_tags = {"tag1"}
        qos._in_flight = {}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.ack("tag1")

        assert "tag1" not in qos._fanout_tags
        qos.channel.conn_or_acquire.assert_not_called()
        qos._quick_ack.assert_called_once_with("tag1")

    def test_ack_missing_metadata_logs_critical_and_acks(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test ack without in-flight metadata logs critical but still acks locally."""
        qos = object.__new__(QoS)
        qos.channel = MagicMock(closed=False, _collected=False)
        qos._fanout_tags = set()
        qos._in_flight = {}
        qos._delivered = {}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        with caplog.at_level(logging.CRITICAL, logger="celery_redis_plus.streams"):
            qos.ack("ghost-tag")

        assert "Cannot ack message" in caplog.text
        qos.channel.conn_or_acquire.assert_not_called()
        qos._quick_ack.assert_called_once_with("ghost-tag")

    def test_ack_after_channel_collected_does_not_raise(self) -> None:
        """N1 regression: acking a collected channel's in-flight tag must not raise.

        Simulates the state right after Transport._collect() has run: the
        channel is marked collected (and, incidentally, closed, since a
        genuine close() also happens to accompany a real collect), and
        kombu's Connection.collect() has severed channel.connection.client to
        None (Connection._do_close_transport does this unconditionally after
        any collect). Before the fix, this fell through to _ack_by_tag(),
        which calls conn_or_acquire() -> the .pool property -> _get_pool()
        -> _connparams(), and _connparams() raises TypeError when
        self.connection.client is None. The entry must stay in _in_flight so
        a peer reclaims it after the visibility timeout instead.

        Deliberately keys the assertion off channel._collected rather than
        channel.closed (F1, round 3): a genuine Channel.close() also sets
        `closed = True`, well before restore_unacked_once() runs, so a test
        that only set `closed` could no longer distinguish "collected" from
        "closing normally" once the guard uses the dedicated flag.
        """
        channel = object.__new__(Channel)
        channel.connection = MagicMock()
        channel.connection.client = None
        channel.closed = True
        channel._collected = True
        channel._pool = None
        channel._async_pool = None

        qos = object.__new__(QoS)
        qos.channel = channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:0", "1700000000000-0")}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.ack("tag1")

        qos._quick_ack.assert_called_once_with("tag1")
        assert "tag1" in qos._in_flight


@pytest.mark.unit
class TestStreamsQoSReject:
    """Tests for QoS.reject: plain ack or atomic requeue-copy via streams_ack.lua."""

    def test_reject_with_requeue_passes_payload_to_script(self, global_keyprefix: str) -> None:
        """Test reject with requeue sends the serialized message as the script's requeue payload."""
        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.closed = False
        mock_channel._collected = False
        mock_channel.global_keyprefix = global_keyprefix
        mock_channel.consumer_group = "celery"
        mock_channel.conn_or_acquire = MagicMock(return_value=mock_context)

        raw_payload = {
            "body": '{"task": "test"}',
            "properties": {
                "delivery_tag": "tag1",
                "delivery_info": {"exchange": "celery", "routing_key": "my_queue"},
            },
        }
        mock_message = MagicMock()
        mock_message._raw = raw_payload

        qos = object.__new__(QoS)
        qos.channel = mock_channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:9", "1700000000000-0")}
        qos._delivered = {"tag1": mock_message}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.reject("tag1", requeue=True)

        mock_client.register_script.assert_called_once_with(_STREAMS_ACK_LUA)
        mock_script.assert_called_once_with(
            keys=[f"{global_keyprefix}{STREAM_KEY_PREFIX}my_queue:9"],
            args=["celery", "1700000000000-0", json_dumps(raw_payload)],
        )
        assert "tag1" not in qos._in_flight
        qos._quick_ack.assert_called_once_with("tag1")

    def test_reject_without_requeue_plain_ack(self, global_keyprefix: str) -> None:
        """Test reject without requeue runs a plain ack (empty requeue payload)."""
        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.closed = False
        mock_channel._collected = False
        mock_channel.global_keyprefix = global_keyprefix
        mock_channel.consumer_group = "celery"
        mock_channel.conn_or_acquire = MagicMock(return_value=mock_context)

        qos = object.__new__(QoS)
        qos.channel = mock_channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:0", "1700000000000-0")}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.reject("tag1", requeue=False)

        mock_script.assert_called_once_with(
            keys=[f"{global_keyprefix}{STREAM_KEY_PREFIX}my_queue:0"],
            args=["celery", "1700000000000-0", ""],
        )
        assert "tag1" not in qos._in_flight
        qos._quick_ack.assert_called_once_with("tag1")

    def test_reject_fanout_tag_ignores_requeue(self) -> None:
        """Test reject for fanout message discards the tag; requeue unsupported for broadcast."""
        qos = object.__new__(QoS)
        qos.channel = MagicMock(closed=False, _collected=False)
        qos._fanout_tags = {"tag1"}
        qos._in_flight = {}
        qos._delivered = {}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.reject("tag1", requeue=True)

        assert "tag1" not in qos._fanout_tags
        qos.channel.conn_or_acquire.assert_not_called()
        qos._quick_ack.assert_called_once_with("tag1")

    def test_reject_missing_metadata_logs_critical_and_acks(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test reject without in-flight metadata logs critical but still acks locally."""
        qos = object.__new__(QoS)
        qos.channel = MagicMock(closed=False, _collected=False)
        qos._fanout_tags = set()
        qos._in_flight = {}
        qos._delivered = {}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        with caplog.at_level(logging.CRITICAL, logger="celery_redis_plus.streams"):
            qos.reject("ghost-tag", requeue=True)

        assert "Cannot reject message" in caplog.text
        qos.channel.conn_or_acquire.assert_not_called()
        qos._quick_ack.assert_called_once_with("ghost-tag")

    def test_reject_requeue_without_delivered_message_falls_back_to_plain_ack(self) -> None:
        """Test reject with requeue but no delivered message acks without a requeue copy."""
        mock_client = MagicMock()
        mock_script = MagicMock()
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.closed = False
        mock_channel._collected = False
        mock_channel.global_keyprefix = ""
        mock_channel.consumer_group = "celery"
        mock_channel.conn_or_acquire = MagicMock(return_value=mock_context)

        qos = object.__new__(QoS)
        qos.channel = mock_channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:0", "1700000000000-0")}
        qos._delivered = {}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.reject("tag1", requeue=True)

        mock_script.assert_called_once_with(
            keys=[f"{STREAM_KEY_PREFIX}my_queue:0"],
            args=["celery", "1700000000000-0", ""],
        )
        assert "tag1" not in qos._in_flight
        qos._quick_ack.assert_called_once_with("tag1")

    def test_reject_after_channel_collected_does_not_raise(self) -> None:
        """N1 regression: rejecting a collected channel's in-flight tag must not raise.

        See TestStreamsQoSAck.test_ack_after_channel_collected_does_not_raise:
        same collected-channel state, but through reject()'s _ack_by_tag()
        branch instead of ack()'s.
        """
        channel = object.__new__(Channel)
        channel.connection = MagicMock()
        channel.connection.client = None
        channel.closed = True
        channel._collected = True
        channel._pool = None
        channel._async_pool = None

        qos = object.__new__(QoS)
        qos.channel = channel
        qos._fanout_tags = set()
        qos._in_flight = {"tag1": (f"{STREAM_KEY_PREFIX}my_queue:0", "1700000000000-0")}
        qos._delivered = {"tag1": MagicMock()}
        qos._dirty = set()
        qos._quick_ack = MagicMock()

        qos.reject("tag1", requeue=True)

        qos._quick_ack.assert_called_once_with("tag1")
        assert "tag1" in qos._in_flight


@pytest.mark.unit
class TestStreamsXReadGroup:
    """Unit tests for blocking XREADGROUP start/read and poll error routing."""

    def test_xreadgroup_start_sends_blocking_command(self, global_keyprefix: str) -> None:
        """Test _xreadgroup_start sends XREADGROUP BLOCK COUNT 1 over all watched level streams."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel.connection = MagicMock()
        channel.connection.polling_interval = 3

        mock_client = MagicMock()
        mock_client._prefix_args.side_effect = lambda args: args
        channel.client = mock_client

        channel._xreadgroup_start()

        sent_args = mock_client.connection.send_command.call_args[0]
        assert list(sent_args) == [
            "XREADGROUP",
            "GROUP",
            "celery",
            "host:42",
            "BLOCK",
            "3000",
            "COUNT",
            "1",
            "STREAMS",
            "stream:q1:9",
            "stream:q1:0",
            ">",
            ">",
        ]
        assert channel._in_poll is mock_client.connection
        assert channel._ensure_group.call_count == 2
        if global_keyprefix:
            mock_client._prefix_args.assert_called_once()
        else:
            mock_client._prefix_args.assert_not_called()

    def test_xreadgroup_start_without_queues_sends_nothing(self) -> None:
        """Test _xreadgroup_start is a no-op when no queues are watched."""
        channel = object.__new__(Channel)
        channel._queue_cycle = []
        channel._in_poll = None
        channel.connection = MagicMock()
        channel.connection.polling_interval = 3
        mock_client = MagicMock()
        channel.client = mock_client

        channel._xreadgroup_start()

        mock_client.connection.send_command.assert_not_called()
        assert channel._in_poll is None

    def test_xreadgroup_read_delivers_entry(self, global_keyprefix: str) -> None:
        """Test _xreadgroup_read delivers an entry and records _in_flight with unprefixed key."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._in_poll = True
        channel.connection_errors = _connection_errors
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos
        mock_client = MagicMock()
        channel.client = mock_client
        mock_connection = MagicMock()
        channel.connection = mock_connection

        stream_name = f"{global_keyprefix}stream:q1:9".encode()
        payload = b'{"body": "test", "properties": {"delivery_tag": "tagA"}}'
        mock_client.parse_response.return_value = [(stream_name, [(b"1111-0", {b"payload": payload})])]

        result = channel._xreadgroup_read()

        assert result is True
        assert channel._in_poll is None
        assert mock_qos._in_flight == {"tagA": ("stream:q1:9", "1111-0")}
        mock_connection._deliver.assert_called_once_with(
            {"body": "test", "properties": {"delivery_tag": "tagA"}},
            "q1",
        )

    def test_xreadgroup_read_delivers_all_entries(self) -> None:
        """Test _xreadgroup_read delivers every returned entry (all are PEL-registered)."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._in_poll = True
        channel.connection_errors = _connection_errors
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos
        mock_client = MagicMock()
        channel.client = mock_client
        mock_connection = MagicMock()
        channel.connection = mock_connection

        mock_client.parse_response.return_value = [
            (b"stream:q1:9", [(b"1-0", {b"payload": b'{"body": "a", "properties": {"delivery_tag": "tagA"}}'})]),
            (b"stream:q2:0", [(b"2-0", {b"payload": b'{"body": "b", "properties": {"delivery_tag": "tagB"}}'})]),
        ]

        result = channel._xreadgroup_read()

        assert result is True
        assert mock_qos._in_flight == {"tagA": ("stream:q1:9", "1-0"), "tagB": ("stream:q2:0", "2-0")}
        assert mock_connection._deliver.call_count == 2
        delivered_queues = [call.args[1] for call in mock_connection._deliver.call_args_list]
        assert delivered_queues == ["q1", "q2"]
        assert channel._in_poll is None

    def test_xreadgroup_read_empty_reply_raises_empty(self) -> None:
        """Test _xreadgroup_read raises Empty on a nil reply (BLOCK timeout) and clears _in_poll."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._in_poll = True
        channel.connection_errors = _connection_errors
        mock_client = MagicMock()
        channel.client = mock_client
        mock_client.parse_response.return_value = None

        with pytest.raises(Empty):
            channel._xreadgroup_read()

        assert channel._in_poll is None

    def test_xreadgroup_read_connection_error_disconnects(self) -> None:
        """Test _xreadgroup_read disconnects the raw connection on connection errors."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._in_poll = True
        channel.connection_errors = _connection_errors
        mock_client = MagicMock()
        channel.client = mock_client
        mock_client.parse_response.side_effect = _client_exceptions.ConnectionError("connection lost")

        with pytest.raises(_client_exceptions.ConnectionError):
            channel._xreadgroup_read()

        mock_client.connection.disconnect.assert_called_once()
        assert channel._in_poll is None

    def test_xreadgroup_read_nogroup_invalidates_cache_and_raises_empty(self) -> None:
        """Test a NOGROUP reply (stream deleted out of band) clears the ensured-group cache and raises Empty."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._in_poll = True
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:q1:9", "stream:q1:0"}
        mock_client = MagicMock()
        channel.client = mock_client
        mock_client.parse_response.side_effect = _client_exceptions.ResponseError(
            "NOGROUP No such key 'stream:q1:9' or consumer group 'celery' in XREADGROUP with GROUP option",
        )

        with pytest.raises(Empty):
            channel._xreadgroup_read()

        # The next non-blocking pass re-runs _ensure_group per queue before
        # touching the streams, re-creating the groups
        assert channel._ensured_groups == set()
        assert channel._in_poll is None

    def test_poll_error_xread_uses_subclient(self) -> None:
        """Test _poll_error routes XREAD errors to the fanout subclient."""
        channel = object.__new__(Channel)
        channel._in_poll = None
        mock_subclient = MagicMock()
        channel.subclient = mock_subclient

        channel._poll_error("XREAD")

        mock_subclient.parse_response.assert_called_once_with(mock_subclient.connection, "XREAD")

    def test_poll_error_blocking_read_uses_xreadgroup(self) -> None:
        """Test _poll_error parses as XREADGROUP while a blocking read is pending."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        channel.client = mock_client
        channel._in_poll = mock_client.connection

        channel._poll_error("XREADGROUP")

        mock_client.parse_response.assert_called_once_with(mock_client.connection, "XREADGROUP")

    def test_poll_error_without_blocking_read_uses_evalsha(self) -> None:
        """Test _poll_error parses as EVALSHA when no blocking read is pending (non-blocking pass)."""
        channel = object.__new__(Channel)
        mock_client = MagicMock()
        channel.client = mock_client
        channel._in_poll = None

        channel._poll_error("XREADGROUP")

        mock_client.parse_response.assert_called_once_with(mock_client.connection, "EVALSHA")

    def test_poll_error_nogroup_invalidates_cache_and_raises_empty(self) -> None:
        """Test _poll_error on a NOGROUP reply clears the ensured-group cache and raises Empty."""
        channel = object.__new__(Channel)
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:q1:9", "stream:q1:0"}
        mock_client = MagicMock()
        channel.client = mock_client
        channel._in_poll = mock_client.connection
        mock_client.parse_response.side_effect = _client_exceptions.ResponseError(
            "NOGROUP No such key 'stream:q1:9' or consumer group 'celery' in XREADGROUP with GROUP option",
        )

        with pytest.raises(Empty):
            channel._poll_error("XREADGROUP")

        # The next non-blocking pass re-runs _ensure_group per queue,
        # re-creating the groups
        assert channel._ensured_groups == set()


@pytest.mark.unit
class TestStreamsConsumeRead:
    """Unit tests for the non-blocking EVALSHA consume pass (_consume_read)."""

    def test_consume_read_delivers_first_hit(self, global_keyprefix: str) -> None:
        """Test _consume_read delivers on the first Lua hit and records _in_flight."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos
        mock_client = MagicMock()
        channel.client = mock_client
        mock_connection = MagicMock()
        channel.connection = mock_connection

        payload = b'{"body": "test", "properties": {"delivery_tag": "tagA"}}'
        mock_client.parse_response.return_value = [f"{global_keyprefix}stream:q1:9".encode(), b"1111-0", payload]

        result = channel._consume_read()

        assert result is True
        sent_args = mock_client.connection.send_command.call_args[0]
        assert sent_args[0] == "EVALSHA"
        assert sent_args[1] == "sha123"
        assert sent_args[2] == 2
        assert sent_args[3] == f"{global_keyprefix}stream:q1:9"
        assert sent_args[4] == f"{global_keyprefix}stream:q1:0"
        assert sent_args[5] == "celery"
        assert sent_args[6] == "host:42"
        # ttl_ms; now is read server-side via redis.call('TIME') inside the script
        assert sent_args[7] == "0"
        assert channel._ensure_group.call_count == 2
        assert mock_qos._in_flight == {"tagA": ("stream:q1:9", "1111-0")}
        mock_connection._deliver.assert_called_once_with(
            {"body": "test", "properties": {"delivery_tag": "tagA"}},
            "q1",
        )
        channel._xreadgroup_start.assert_not_called()

    def test_consume_read_loads_script_when_sha_missing(self) -> None:
        """Test _consume_read loads and caches the Lua script SHA when unset."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = None
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_client = MagicMock()
        mock_client.script_load.return_value = "newsha"
        mock_client.parse_response.return_value = None
        channel.client = mock_client

        with pytest.raises(Empty):
            channel._consume_read()

        mock_client.script_load.assert_called_once()
        assert channel._consume_script_sha == "newsha"
        assert mock_client.connection.send_command.call_args[0][1] == "newsha"

    def test_consume_read_passes_effective_message_ttl(self) -> None:
        """Test _consume_read passes min(channel message_ttl, per-queue x-message-ttl) in ms."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = 10  # seconds -> 10000 ms
        channel._message_ttls = {"q1": 5000}  # ms, smaller -> wins
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_client = MagicMock()
        mock_client.parse_response.return_value = None
        channel.client = mock_client

        with pytest.raises(Empty):
            channel._consume_read()

        assert mock_client.connection.send_command.call_args[0][7] == "5000"

    def test_consume_read_total_miss_arms_blocking_read(self) -> None:
        """Test _consume_read polls every queue, then arms the blocking XREADGROUP and raises Empty."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1", "q2"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(side_effect=lambda q: [f"stream:{q}:9", f"stream:{q}:0"])
        channel._xreadgroup_start = MagicMock()
        mock_client = MagicMock()
        mock_client.parse_response.return_value = None
        channel.client = mock_client

        with pytest.raises(Empty):
            channel._consume_read()

        assert mock_client.connection.send_command.call_count == 2
        channel._xreadgroup_start.assert_called_once_with()

    def test_consume_read_noscript_resets_sha(self) -> None:
        """Test NOSCRIPT recovery: reset cached SHA, raise Empty, no blocking read armed."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_client = MagicMock()
        mock_client.parse_response.side_effect = _client_exceptions.ResponseError(
            "NOSCRIPT No matching script. Please use EVAL.",
        )
        channel.client = mock_client

        with pytest.raises(Empty):
            channel._consume_read()

        assert channel._consume_script_sha is None
        channel._xreadgroup_start.assert_not_called()

    def test_consume_read_connection_error_disconnects(self) -> None:
        """Test _consume_read disconnects the raw connection on connection errors."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_client = MagicMock()
        mock_client.parse_response.side_effect = _client_exceptions.ConnectionError("connection lost")
        channel.client = mock_client

        with pytest.raises(_client_exceptions.ConnectionError):
            channel._consume_read()

        mock_client.connection.disconnect.assert_called_once()
        channel._xreadgroup_start.assert_not_called()

    def test_consume_read_nogroup_reensures_and_retries_once(self) -> None:
        """Test NOGROUP recovery: drop cached groups, re-ensure, resend the EVALSHA once, deliver the hit."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = ""
        channel._queue_cycle = ["q1"]
        channel._in_poll = None
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._consume_script_sha = "sha123"
        channel.connection_errors = _connection_errors
        channel.ResponseError = _client_exceptions.ResponseError
        channel.consumer_group = "celery"
        channel.consumer_name = "host:42"
        channel._ensured_groups = {"stream:q1:9", "stream:q1:0"}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:q1:9", "stream:q1:0"])
        channel._xreadgroup_start = MagicMock()
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos
        mock_client = MagicMock()
        payload = b'{"body": "test", "properties": {"delivery_tag": "tagA"}}'
        # First parse: NOGROUP (stream deleted out of band while the cache
        # was warm). Second parse (after re-ensure + resend): a hit.
        mock_client.parse_response.side_effect = [
            _client_exceptions.ResponseError(
                "NOGROUP No such key 'stream:q1:9' or consumer group 'celery' in XREADGROUP with GROUP option",
            ),
            [b"stream:q1:9", b"1111-0", payload],
        ]
        channel.client = mock_client
        mock_connection = MagicMock()
        channel.connection = mock_connection

        result = channel._consume_read()

        assert result is True
        # The EVALSHA was sent twice: initial attempt + one retry
        assert mock_client.connection.send_command.call_count == 2
        # 2 upfront ensures + 2 re-ensures after the cache invalidation
        assert channel._ensure_group.call_count == 4
        # _invalidate_group discarded the stale cache entries (_ensure_group
        # is mocked here, so nothing re-adds them)
        assert channel._ensured_groups == set()
        assert mock_qos._in_flight == {"tagA": ("stream:q1:9", "1111-0")}
        mock_connection._deliver.assert_called_once_with(
            {"body": "test", "properties": {"delivery_tag": "tagA"}},
            "q1",
        )
        channel._xreadgroup_start.assert_not_called()


@pytest.mark.unit
class TestStreamsPoller:
    """Unit tests for the streams MultiChannelPoller registration and drain cycle."""

    def test_register_xreadgroup_registers_socket_and_runs_pass(self) -> None:
        """Test _register_XREADGROUP registers the client socket and runs the non-blocking pass."""
        poller = object.__new__(MultiChannelPoller)
        poller._fd_to_chan = {}
        poller._chan_to_sock = {}
        poller.poller = MagicMock()

        channel = MagicMock()
        channel._in_poll = None
        mock_sock = MagicMock()
        mock_sock.fileno.return_value = 7
        channel.client.connection._sock = mock_sock
        channel._consume_read.return_value = True

        result = poller._register_XREADGROUP(channel)

        assert result is True
        channel._consume_read.assert_called_once()
        assert poller._fd_to_chan[7] == (channel, "XREADGROUP")
        assert poller._chan_to_sock[(channel, channel.client, "XREADGROUP")] is mock_sock
        poller.poller.register.assert_called_once_with(mock_sock, poller.eventflags)
        assert channel._in_poll is False

    def test_register_xreadgroup_skips_pass_when_read_pending(self) -> None:
        """Test _register_XREADGROUP does not re-send while a blocking read is pending."""
        poller = object.__new__(MultiChannelPoller)
        poller._fd_to_chan = {}
        poller.poller = MagicMock()

        channel = MagicMock()
        mock_sock = MagicMock()
        channel.client.connection._sock = mock_sock
        channel._in_poll = channel.client.connection  # blocking XREADGROUP pending
        poller._chan_to_sock = {(channel, channel.client, "XREADGROUP"): mock_sock}

        result = poller._register_XREADGROUP(channel)

        assert result is False
        channel._consume_read.assert_not_called()
        poller.poller.register.assert_not_called()

    def test_register_xreadgroup_suppresses_empty(self) -> None:
        """Test _register_XREADGROUP returns False when the pass raises Empty (blocking read armed)."""
        poller = object.__new__(MultiChannelPoller)
        poller._fd_to_chan = {}
        poller._chan_to_sock = {}
        poller.poller = MagicMock()

        channel = MagicMock()
        channel._in_poll = None
        mock_sock = MagicMock()
        mock_sock.fileno.return_value = 7
        channel.client.connection._sock = mock_sock
        channel._consume_read.side_effect = Empty

        result = poller._register_XREADGROUP(channel)

        assert result is False
        channel._consume_read.assert_called_once()

    def test_on_poll_start_registers_active_channels(self) -> None:
        """Test on_poll_start registers queue channels for XREADGROUP and fanout channels for XREAD."""
        poller = object.__new__(MultiChannelPoller)
        queue_channel = MagicMock()
        queue_channel.active_queues = {"q1"}
        queue_channel.active_fanout_queues = set()
        queue_channel.qos.can_consume.return_value = True
        fanout_channel = MagicMock()
        fanout_channel.active_queues = set()
        fanout_channel.active_fanout_queues = {"fq"}
        fanout_channel.qos.can_consume.return_value = True
        poller._channels = {queue_channel, fanout_channel}
        poller._register_XREADGROUP = MagicMock()
        poller._register_XREAD = MagicMock()

        poller.on_poll_start()

        poller._register_XREADGROUP.assert_called_once_with(queue_channel)
        poller._register_XREAD.assert_called_once_with(fanout_channel)

    def test_handle_event_read_dispatches_handler(self) -> None:
        """Test handle_event dispatches READ events to the channel's registered handler."""
        poller = object.__new__(MultiChannelPoller)
        channel = MagicMock()
        channel.qos.can_consume.return_value = True
        handler = MagicMock(return_value=True)
        channel.handlers = {"XREADGROUP": handler}
        poller._fd_to_chan = {7: (channel, "XREADGROUP")}

        ret = poller.handle_event(7, READ)

        handler.assert_called_once_with()
        assert ret == (True, poller)

    def test_handle_event_err_routes_to_poll_error(self) -> None:
        """Test handle_event routes ERR events to channel._poll_error with the cmd type."""
        poller = object.__new__(MultiChannelPoller)
        channel = MagicMock()
        poller._fd_to_chan = {7: (channel, "XREADGROUP")}

        ret = poller.handle_event(7, ERR)

        channel._poll_error.assert_called_once_with("XREADGROUP")
        assert ret is None

    def test_get_returns_after_nonblocking_hit(self) -> None:
        """Test get() returns without polling when the non-blocking pass delivers."""
        poller = object.__new__(MultiChannelPoller)
        poller.after_read = set()
        channel = MagicMock()
        channel.active_queues = {"q1"}
        channel.active_fanout_queues = set()
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}
        poller._register_XREADGROUP = MagicMock(return_value=True)
        poller._register_XREAD = MagicMock()
        poller.poller = MagicMock()

        poller.get(MagicMock())

        poller._register_XREADGROUP.assert_called_once_with(channel)
        poller.poller.poll.assert_not_called()
        assert poller._in_protected_read is False

    def test_get_raises_empty_and_drains_after_read(self) -> None:
        """Test get() raises Empty on no events and drains deferred after_read callbacks."""
        poller = object.__new__(MultiChannelPoller)
        deferred = MagicMock()
        poller.after_read = {deferred}
        channel = MagicMock()
        channel.active_queues = {"q1"}
        channel.active_fanout_queues = set()
        channel.qos.can_consume.return_value = True
        poller._channels = {channel}
        poller._register_XREADGROUP = MagicMock(return_value=False)
        poller._register_XREAD = MagicMock()
        poller.poller = MagicMock()
        poller.poller.poll.return_value = []

        with pytest.raises(Empty):
            poller.get(MagicMock())

        # Asserting the registration pass and the poll (not just the drain)
        # keeps this test red against the Task 3 scaffold's placeholder get(),
        # which raises Empty immediately and never registers or polls
        poller._register_XREADGROUP.assert_called_once_with(channel)
        poller.poller.poll.assert_called_once()
        deferred.assert_called_once_with()
        assert not poller.after_read
        assert poller._in_protected_read is False


@pytest.mark.unit
class TestStreamsMoveDelayed:
    """Tests for the delayed pump: streams_move_delayed script call and poller wiring."""

    def _make_channel(self, global_keyprefix: str = "") -> tuple[Channel, MagicMock, MagicMock]:
        """Build a bare Channel with mocked client whose register_script returns a script mock."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        # priority_steps is a read-only property reading self.connection.client.transport_options,
        # and a bare object.__new__ instance has no connection, so mock the whole chain
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"priority_steps": [0, 3, 6, 9]}
        channel.connection = mock_connection
        channel.message_ttl = None
        channel._message_ttls = {}
        channel._ensure_group = MagicMock()

        mock_client = MagicMock()
        mock_script = MagicMock(return_value=0)
        mock_client.register_script.return_value = mock_script

        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)
        return channel, mock_client, mock_script

    def test_move_delayed_passes_prefixed_keys_ascending_and_steps_csv(self, global_keyprefix: str) -> None:
        """Test that _move_delayed passes prefixed delayed + stream keys (ascending) and steps CSV.

        DEVIATION from the brief: the Lua script reads the current time via
        Redis TIME rather than a caller-supplied now_ms ARGV slot (matching
        transport_enqueue_due_messages.lua and streams_consume.lua), so ARGV
        here is [batch_limit, message_ttl_ms, steps_csv] with no timestamp
        argument to assert on.
        """
        channel, mock_client, mock_script = self._make_channel(global_keyprefix)

        moved = channel._move_delayed("my_queue")

        assert moved == 0
        mock_client.register_script.assert_called_once()
        keys = mock_script.call_args.kwargs["keys"]
        args = mock_script.call_args.kwargs["args"]
        assert keys[0] == f"{global_keyprefix}delayed:my_queue"
        assert keys[1:] == [
            f"{global_keyprefix}stream:my_queue:0",
            f"{global_keyprefix}stream:my_queue:3",
            f"{global_keyprefix}stream:my_queue:6",
            f"{global_keyprefix}stream:my_queue:9",
        ]
        assert args[0] == DEFAULT_REQUEUE_BATCH_LIMIT
        assert args[1] == 0  # No message TTL configured
        assert args[2] == "0,3,6,9"

    def test_move_delayed_sorts_steps_ascending(self) -> None:
        """Test that unsorted priority_steps are sorted ascending for KEYS and the steps CSV."""
        channel, _mock_client, mock_script = self._make_channel()

        # The property getter already sorts, so feeding an unsorted transport option would
        # never reach _move_delayed unsorted. Patch the property itself to hand the method a
        # genuinely unsorted list and exercise its own sorted() guard.
        with patch.object(Channel, "priority_steps", new_callable=PropertyMock, return_value=[9, 0, 6, 3]):
            channel._move_delayed("my_queue")

        keys = mock_script.call_args.kwargs["keys"]
        args = mock_script.call_args.kwargs["args"]
        assert keys[1:] == [
            "stream:my_queue:0",
            "stream:my_queue:3",
            "stream:my_queue:6",
            "stream:my_queue:9",
        ]
        assert args[2] == "0,3,6,9"

    def test_move_delayed_message_ttl_args(self) -> None:
        """Test effective message TTL: per-queue x-message-ttl (ms) is min'd with channel message_ttl (s)."""
        # Per-queue TTL (5000 ms) is smaller than channel TTL (60 s): per-queue wins
        channel, _mock_client, mock_script = self._make_channel()
        channel.message_ttl = 60
        channel._message_ttls = {"my_queue": 5000}
        channel._move_delayed("my_queue")
        assert mock_script.call_args.kwargs["args"][1] == 5000

        # Only channel-wide TTL: seconds converted to ms
        channel, _mock_client, mock_script = self._make_channel()
        channel.message_ttl = 60
        channel._move_delayed("my_queue")
        assert mock_script.call_args.kwargs["args"][1] == 60000

    def test_move_delayed_returns_moved_count(self) -> None:
        """Test that _move_delayed returns the count reported by the Lua script."""
        channel, _mock_client, mock_script = self._make_channel()
        mock_script.return_value = 7

        assert channel._move_delayed("my_queue") == 7

    def test_move_delayed_passes_custom_limit_to_argv(self) -> None:
        """A custom limit kwarg is passed through as the Lua script's batch_limit ARGV slot.

        The requeue cycle passes its remaining shared budget here (Fix round 1,
        FIX4a) instead of always spending the full DEFAULT_REQUEUE_BATCH_LIMIT
        constant, so the value actually reaching Lua must reflect the argument.
        """
        channel, _mock_client, mock_script = self._make_channel()

        channel._move_delayed("my_queue", limit=42)

        assert mock_script.call_args.kwargs["args"][0] == 42

    def test_move_delayed_ensures_groups_for_all_levels(self) -> None:
        """Test that consumer groups are ensured (unprefixed keys) before the pump XADDs entries."""
        channel, _mock_client, _mock_script = self._make_channel()

        channel._move_delayed("my_queue")

        assert channel._ensure_group.call_args_list == [
            call("stream:my_queue:0"),
            call("stream:my_queue:3"),
            call("stream:my_queue:6"),
            call("stream:my_queue:9"),
        ]

    def test_maybe_enqueue_due_messages_calls_move_delayed_per_channel_queue(self) -> None:
        """Test that the periodic pump calls _move_delayed for every channel and active queue."""
        poller = MultiChannelPoller()
        channel_a = MagicMock()
        channel_a._queue_cycle = ["q1", "q2"]
        channel_a._move_delayed.side_effect = [2, 3]
        # Forward compatibility with Task 9: that task rewrites this pump to also call
        # channel._reclaim_and_deliver(queue, budget) and to subtract its return value
        # from an integer budget that is then compared with `if budget <= 0:`. An
        # unconfigured MagicMock return value would turn the budget into a MagicMock and
        # raise TypeError on that comparison, so pin it to a real int. This task's pump
        # body never reads the attribute, so the stub is inert until Task 9 lands.
        channel_a._reclaim_and_deliver.return_value = 0
        channel_b = MagicMock()
        channel_b._queue_cycle = ["q3"]
        channel_b._move_delayed.return_value = 5
        channel_b._reclaim_and_deliver.return_value = 0
        poller._channels = {channel_a, channel_b}

        total = poller.maybe_enqueue_due_messages()

        assert total == 10
        assert channel_a._move_delayed.call_args_list == [
            call("q1", limit=DEFAULT_REQUEUE_BATCH_LIMIT),
            call("q2", limit=DEFAULT_REQUEUE_BATCH_LIMIT - 2),
        ]
        channel_b._move_delayed.assert_called_once_with("q3", limit=DEFAULT_REQUEUE_BATCH_LIMIT)

    def test_maybe_enqueue_due_messages_survives_channel_errors(self) -> None:
        """Test that a failing queue is skipped with a warning and the pump continues."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel._queue_cycle = ["q1", "q2"]
        channel._move_delayed.side_effect = [ConnectionError("boom"), 4]
        # Same forward compatibility with Task 9's shared-budget rewrite as above:
        # without a real int here, `total` becomes a MagicMock and `assert total == 4`
        # silently fails once Task 9 wires reclaim into this pump.
        channel._reclaim_and_deliver.return_value = 0
        poller._channels = {channel}

        total = poller.maybe_enqueue_due_messages()

        assert total == 4
        assert channel._move_delayed.call_count == 2

    def test_requeue_timer_wired_to_maybe_enqueue_due_messages(self) -> None:
        """Test that register_with_event_loop registers the periodic delayed pump timer."""
        transport = object.__new__(Transport)
        cycle = MagicMock()
        transport.cycle = cycle
        loop = MagicMock()
        connection = MagicMock()
        connection.client.transport_options = {}

        transport.register_with_event_loop(connection, loop)

        # DEFAULT_REQUEUE_CHECK_INTERVAL is the streams module binding (patched to 2 in conftest),
        # the same value register_with_event_loop reads at call time
        intervals = {c.args[0]: c.args[1] for c in loop.call_repeatedly.call_args_list}
        assert intervals[DEFAULT_REQUEUE_CHECK_INTERVAL] is cycle.maybe_enqueue_due_messages


@pytest.mark.unit
class TestStreamsReclaim:
    """Unit tests for Channel._reclaim_and_deliver (XPENDING-IDLE discovery + XCLAIM)."""

    def test_reclaim_terminates_when_xpending_page_is_empty(self, global_keyprefix: str) -> None:
        """A stream with nothing pending returns 0 after a single XPENDING call.

        An empty page (no entries idle past visibility_timeout) ends the loop
        immediately: no XCLAIM, no delivery, no ack, and no further XPENDING
        call for this stream.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = []
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 0
        mock_client.xpending_range.assert_called_once_with(
            "stream:celery:0",
            "celery",
            min="-",
            max="+",
            count=100,
            idle=DEFAULT_VISIBILITY_TIMEOUT * 1000,
        )
        mock_client.xclaim.assert_not_called()
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_not_called()

    def test_reclaim_delivers_claimed_message_with_restore_count_header(self, global_keyprefix: str) -> None:
        """A claimed entry is delivered locally with x-restore-count = times_delivered (pre-claim, no subtraction)."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-reclaim-1",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        mock_client.xclaim.assert_called_once_with(
            "stream:celery:0",
            "celery",
            "worker1:123",
            min_idle_time=DEFAULT_VISIBILITY_TIMEOUT * 1000,
            message_ids=["1700000000000-0"],
        )
        delivered_message, delivered_queue = channel.connection._deliver.call_args[0]
        assert delivered_queue == "celery"
        assert delivered_message["properties"]["headers"]["x-restore-count"] == 1
        assert channel._qos._in_flight["tag-reclaim-1"] == ("stream:celery:0", "1700000000000-0")
        mock_ack_script.assert_not_called()

    def test_reclaim_missing_from_xpending_defaults_to_no_restore_count_header(self, global_keyprefix: str) -> None:
        """An id XCLAIM returns that is absent from the discovery-phase XPENDING map defaults to restore_count 0.

        Structurally this id always comes from survivor_ids, itself built from
        the same discovery page that populates the times_delivered map, so the
        lookup should never actually miss; this exercises the defensive
        fallback directly by having XCLAIM report back an id the discovery
        phase never saw.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-fresh",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 3,
            },
        ]
        # XCLAIM reports back an id the discovery phase never listed.
        mock_client.xclaim.return_value = [(b"1700000099999-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        delivered_message, _delivered_queue = channel.connection._deliver.call_args[0]
        assert "x-restore-count" not in delivered_message["properties"]["headers"]
        assert channel._qos._in_flight["tag-fresh"] == ("stream:celery:0", "1700000099999-0")

    def test_reclaim_restore_count_equals_pre_claim_times_delivered_exactly(self, global_keyprefix: str) -> None:
        """restore_count is exactly times_delivered from the discovery-phase XPENDING, with no subtraction.

        Pins the off-by-one that broke Fix round 2: that design queried
        XPENDING for delivery counts AFTER this pass's own XCLAIM had already
        bumped them, so it subtracted 1. Round 3 queries XPENDING BEFORE
        claiming, so the value read is already the pre-claim count and must
        be used as-is. Three entries with different times_delivered values
        are claimed in the same pass; each must produce a header equal to its
        discovery-phase times_delivered, unmodified.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        def payload_for(tag: str) -> bytes:
            return json_dumps(
                {
                    "body": '{"task": "test"}',
                    "properties": {
                        "delivery_tag": tag,
                        "delivery_info": {"exchange": "", "routing_key": "celery"},
                        "headers": {},
                    },
                },
            ).encode()

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000001-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
            {
                "message_id": b"1700000000002-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
            {
                "message_id": b"1700000000003-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 5,
            },
        ]
        mock_client.xclaim.return_value = [
            (b"1700000000001-0", {b"payload": payload_for("tag-1")}),
            (b"1700000000002-0", {b"payload": payload_for("tag-2")}),
            (b"1700000000003-0", {b"payload": payload_for("tag-5")}),
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 3
        delivered = {
            call.args[0]["properties"]["delivery_tag"]: call.args[0]["properties"]["headers"]["x-restore-count"]
            for call in channel.connection._deliver.call_args_list
        }
        assert delivered == {"tag-1": 1, "tag-2": 2, "tag-5": 5}

    def test_reclaim_pagination_advances_cursor_across_xpending_calls(self, global_keyprefix: str) -> None:
        """A full XPENDING page (len == count) advances the cursor to "(" + last_id and continues.

        Page 1 returns exactly as many raw entries as were requested (a "full"
        page), so the loop must not stop there even though only one of those
        entries is an actual survivor (the other is this process's own
        in-flight id, dropped for free and never counted). Page 2 then
        completes the budget, ending the loop from the budget check rather
        than a short page.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        # 1700000000001-0 is this process's own in-flight entry: filtered for
        # free, without ever reaching XCLAIM, and not counted against budget.
        channel._qos._in_flight = {"tag-own": ("stream:celery:0", "1700000000001-0")}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json_1 = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-a",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        payload_json_2 = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-b",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.side_effect = [
            [
                {
                    "message_id": b"1700000000001-0",
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 1,
                },
                {
                    "message_id": b"1700000000002-0",
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 2,
                },
            ],
            [
                {
                    "message_id": b"1700000000006-0",
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 2,
                },
            ],
        ]
        mock_client.xclaim.side_effect = [
            [(b"1700000000002-0", {b"payload": payload_json_1.encode()})],
            [(b"1700000000006-0", {b"payload": payload_json_2.encode()})],
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 2)

        assert result == 2
        assert mock_client.xpending_range.call_count == 2
        assert mock_client.xpending_range.call_args_list[0].kwargs["min"] == "-"
        assert mock_client.xpending_range.call_args_list[1].kwargs["min"] == "(1700000000002-0"
        assert channel.connection._deliver.call_count == 2

    def test_reclaim_advances_cursor_when_page_is_fully_own_in_flight(self, global_keyprefix: str) -> None:
        """A full page filtered down to zero survivors still advances the cursor rather than spinning.

        Every id on page 1 belongs to this process's own in-flight table, so
        the filter loop empties survivor_ids without ever consulting `take`.
        The page is full (len == count), so the loop must not treat this like
        a short page: it has to advance the cursor to "(" + last_id and issue
        a second XPENDING call rather than looping the same page forever. An
        implementation that forgets to advance the cursor here either spins
        on page 1 indefinitely or wrongly breaks out early; this test's mocked
        second call returns nothing further to claim, only reachable by
        actually advancing.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        own_ids = [f"170000000000{i}-0" for i in range(5)]
        channel._qos._in_flight = {
            f"tag-own-{i}": ("stream:celery:0", message_id) for i, message_id in enumerate(own_ids)
        }
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.side_effect = [
            [
                {
                    "message_id": message_id.encode(),
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 1,
                }
                for message_id in own_ids
            ],
            [],
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 5)

        assert result == 0
        assert mock_client.xpending_range.call_count == 2
        assert mock_client.xpending_range.call_args_list[0].kwargs["min"] == "-"
        assert mock_client.xpending_range.call_args_list[1].kwargs["min"] == f"({own_ids[-1]}"
        mock_client.xclaim.assert_not_called()
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_not_called()

    def test_reclaim_truncates_survivors_to_zero_when_prefetch_capacity_exhausted(
        self,
        global_keyprefix: str,
    ) -> None:
        """`take == 0` (no remaining prefetch capacity) drops genuine survivors without claiming them.

        Unlike the own-in-flight filter above, this entry is a real survivor
        of the per-id filter loop: it is not own in-flight and not expired.
        It is still discarded by the `survivor_ids[: max(take, 0)]` truncation
        because `qos.can_consume_max_estimate()` reports zero remaining
        capacity, so XCLAIM must never be called for it and it must not be
        counted against budget. The page is short, so the pass ends here
        rather than looping.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = 0
        channel.connection = MagicMock()
        channel.connection.cycle = None

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker2:999",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 0
        mock_client.xpending_range.assert_called_once()
        mock_client.xclaim.assert_not_called()
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_not_called()

    def test_reclaim_discovery_page_cap_stops_endless_pel_walk(
        self,
        global_keyprefix: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A PEL where every page is fully filtered out stops after DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT pages.

        The realistic worst case: this worker's own long-running in-flight
        messages dominate the PEL, so every discovery page comes back full
        but every entry on it is filtered out as own in-flight. Without a
        page cap this would walk the entire PEL, page by page, every single
        reclaim call, doing unbounded Redis work while processing nothing.
        The cap stops the walk at a fixed page count and logs a warning
        instead of continuing forever.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        class _EverythingIsOwn:
            """A set stand-in reporting every id as already in-flight in this process."""

            def __contains__(self, item: object) -> bool:
                return True

        channel._own_in_flight_message_ids = MagicMock(return_value=_EverythingIsOwn())

        def _full_page(*_args: object, **kwargs: Any) -> list[dict[str, object]]:
            count: int = kwargs["count"]
            return [
                {
                    "message_id": f"170000000{i:04d}-0".encode(),
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 1,
                }
                for i in range(count)
            ]

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.side_effect = _full_page
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with caplog.at_level(logging.WARNING, logger="celery_redis_plus.streams"):
            result = channel._reclaim_and_deliver("celery", 100_000)

        assert result == 0
        assert mock_client.xpending_range.call_count == DEFAULT_RECLAIM_DISCOVERY_PAGE_LIMIT
        mock_client.xclaim.assert_not_called()
        channel.connection._deliver.assert_not_called()
        assert any("discovery stopped" in record.getMessage() for record in caplog.records)

    def test_reclaim_own_in_flight_entry_does_not_corrupt_others_restore_count(self, global_keyprefix: str) -> None:
        """An own in-flight entry sharing a discovery page cannot corrupt a survivor's times_delivered.

        Fix round 2 fetched delivery counts via a separate, post-claim XPENDING
        query bounded to the claimed ID range specifically because an
        unbounded query could let this worker's own lower-ID in-flight entries
        displace the claimed ones and zero their restore counts. Round 3
        removes that whole failure mode structurally: times_delivered comes
        directly from the single discovery-phase page that produced the
        survivor ids, a flat per-id map, so an own in-flight entry sharing
        that same page is simply irrelevant to any other id's count.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {"tag-own": ("stream:celery:0", "1700000000001-0")}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-claimed",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        # Same page: this worker's own in-flight entry (lower id, filtered for
        # free) alongside the entry about to be claimed from a dead peer.
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000001-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 1000,
                "times_delivered": 1,
            },
            {
                "message_id": b"1700000000002-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 3,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000002-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        mock_client.xclaim.assert_called_once_with(
            "stream:celery:0",
            "celery",
            "worker1:123",
            min_idle_time=DEFAULT_VISIBILITY_TIMEOUT * 1000,
            message_ids=["1700000000002-0"],
        )
        delivered_message = channel.connection._deliver.call_args[0][0]
        assert delivered_message["properties"]["headers"]["x-restore-count"] == 3

    def test_reclaim_handles_xclaim_returning_fewer_entries_than_requested(
        self,
        global_keyprefix: str,
    ) -> None:
        """XCLAIM returning fewer entries than requested is handled without indexing off the end.

        Two survivor ids are sent to XCLAIM, but only one comes back (the
        other was claimed by a competing worker, or deleted from the stream
        entirely, between the XPENDING discovery and this XCLAIM). Round 3's
        design tolerates a short reply by construction: it iterates whatever
        (id, fields) pairs XCLAIM actually returns rather than indexing by
        position, so nothing needs to be requested defensively. The vanished
        id is neither delivered nor acked; it is simply not present to act on.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-live",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
            {
                "message_id": b"1700000000001-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        # Only the second survivor comes back; the first vanished in between.
        mock_client.xclaim.return_value = [(b"1700000000001-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        mock_client.xclaim.assert_called_once_with(
            "stream:celery:0",
            "celery",
            "worker1:123",
            min_idle_time=DEFAULT_VISIBILITY_TIMEOUT * 1000,
            message_ids=["1700000000000-0", "1700000000001-0"],
        )
        channel.connection._deliver.assert_called_once()
        delivered_message = channel.connection._deliver.call_args[0][0]
        assert delivered_message["properties"]["delivery_tag"] == "tag-live"
        mock_ack_script.assert_not_called()

    def test_reclaim_respects_budget(self, global_keyprefix: str) -> None:
        """Processing stops at the budget even when there is more to scan."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:9", "stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-budget",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 1)

        assert result == 1
        mock_client.xpending_range.assert_called_once()
        assert mock_client.xpending_range.call_args.args[0] == "stream:celery:9"
        assert mock_client.xpending_range.call_args.kwargs["count"] == 1
        channel.connection._deliver.assert_called_once()

    def test_reclaim_scans_all_level_streams(self, global_keyprefix: str) -> None:
        """All priority level streams are scanned, highest level first."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:9", "stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-level0",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.side_effect = [
            [],
            [
                {
                    "message_id": b"1700000000000-0",
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 2,
                },
            ],
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        scanned = [c.args[0] for c in mock_client.xpending_range.call_args_list]
        assert scanned == ["stream:celery:9", "stream:celery:0"]
        channel.connection._deliver.assert_called_once()

    def test_reclaim_drops_expired_message(self, global_keyprefix: str) -> None:
        """Entries older than the effective x-message-ttl are acked and skipped before ever being claimed."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {"celery": 60_000}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        # Server time is now mocked via client.time(), so the expiry cutoff is computed
        # from that fixed mock value instead of the real wall clock.
        mock_now_ms = 1_700_000_100_000
        expired_id = f"{mock_now_ms - 120_000}-0"
        mock_client = MagicMock()
        mock_client.time.return_value = (mock_now_ms // 1000, (mock_now_ms % 1000) * 1000)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": expired_id.encode(),
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_not_called()
        # Dropped in Step B, before claiming: XCLAIM is never invoked for it.
        mock_client.xclaim.assert_not_called()
        mock_ack_script.assert_called_once_with(
            keys=[f"{global_keyprefix}stream:celery:0"],
            args=["celery", expired_id, ""],
        )

    def test_reclaim_ttl_cutoff_uses_server_clock_not_worker_clock(self, global_keyprefix: str) -> None:
        """The TTL cutoff is computed from the server's clock (TIME), never the worker's local clock.

        The mocked server clock here is fixed at a point far behind the real
        wall clock (this machine's real time() return value). The claimed
        entry's id encodes a creation time that is recent relative to that
        mocked server clock (well inside the 60s TTL) but ancient relative to
        the worker's real clock (the gap between the mocked server time and
        actual now dwarfs the TTL many times over). If the cutoff were ever
        computed from time() instead of client.time(), this message would
        look wildly expired and get acked away instead of delivered; this
        test fails against that regression (Fix round 2, R2).
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {"celery": 60_000}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-server-clock",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        # Fixed server clock, meaningfully behind the real wall clock (this
        # test was written in 2026; a real time() call returns an epoch ms
        # value far larger than this).
        mock_server_now_ms = 1_700_000_100_000
        # 30s before the mocked server clock: within the 60s TTL relative to
        # that clock, but the entry id is a 2023 timestamp, hopelessly
        # "expired" relative to any real-world time() call.
        entry_id_ms = mock_server_now_ms - 30_000
        entry_id = f"{entry_id_ms}-0"
        assert entry_id_ms < int(time.time() * 1000) - 60_000, "test fixture must predate the real clock"
        mock_client = MagicMock()
        mock_client.time.return_value = (mock_server_now_ms // 1000, (mock_server_now_ms % 1000) * 1000)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": entry_id.encode(),
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
        ]
        mock_client.xclaim.return_value = [(entry_id.encode(), {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_called_once()
        mock_ack_script.assert_not_called()

    def test_reclaim_nogroup_from_xpending_reensures_and_retries_once(self, global_keyprefix: str) -> None:
        """A NOGROUP from the XPENDING discovery call re-ensures groups and retries once."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:celery:0"}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-nogroup",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        # First call: NOGROUP (the stream and its group were deleted out of
        # band, e.g. cross-process purge or x-expires expiry, while this
        # channel's _ensured_groups cache was warm). Second call: discovery.
        mock_client.xpending_range.side_effect = [
            _client_exceptions.ResponseError(
                "NOGROUP No such key 'stream:celery:0' or consumer group 'celery'",
            ),
            [
                {
                    "message_id": b"1700000000000-0",
                    "consumer": b"worker1:123",
                    "time_since_delivered": 400000,
                    "times_delivered": 1,
                },
            ],
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        assert mock_client.xpending_range.call_count == 2
        # _invalidate_group discarded the stale cache entry, then the queue's
        # groups were re-ensured (_ensure_group is mocked, so nothing re-adds)
        assert channel._ensured_groups == set()
        channel._ensure_group.assert_called_once_with("stream:celery:0")
        channel.connection._deliver.assert_called_once()

    def test_reclaim_nogroup_from_xclaim_reensures_and_retries_once(self, global_keyprefix: str) -> None:
        """A NOGROUP from the XCLAIM claim call also re-ensures groups and retries once.

        Discovery (XPENDING) can succeed while the group is still deleted out
        from under the claim itself (e.g. the group vanished between the two
        calls), so both calls need their own retry-once handling.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:celery:0"}
        channel._ensure_group = MagicMock()
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-nogroup-claim",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.side_effect = [
            _client_exceptions.ResponseError(
                "NOGROUP No such key 'stream:celery:0' or consumer group 'celery'",
            ),
            [(b"1700000000000-0", {b"payload": payload_json.encode()})],
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        assert mock_client.xclaim.call_count == 2
        assert channel._ensured_groups == set()
        channel._ensure_group.assert_called_once_with("stream:celery:0")
        channel.connection._deliver.assert_called_once()

    def test_reclaim_skips_own_in_flight_message(self, global_keyprefix: str) -> None:
        """An id already in this channel's QoS in-flight table is dropped before XCLAIM.

        It is this worker's own live message (e.g. a task still running past
        visibility_timeout on this same, healthy worker). XPENDING discovery
        finds it idle, but it is filtered out before the counting XCLAIM ever
        runs, so it is never claimed and its delivery count is never bumped.
        It must not be acked, not redelivered, and not counted against the
        budget (Fix round 1 FIX2, tightened in Fix round 2 R1, then carried
        into Fix round 3's XPENDING-discovery redesign).
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {"tag-own": ("stream:celery:0", "1700000000000-0")}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker2:999",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 0
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_not_called()
        mock_client.xclaim.assert_not_called()

    def test_reclaim_skips_in_flight_message_owned_by_sibling_channel(self, global_keyprefix: str) -> None:
        """The own-in-flight skip covers every sibling channel sharing this process's consumer_name.

        consumer_name is one identity per worker process (see the module
        docstring), so a second consuming channel in the same process is
        still this same consumer as far as Redis is concerned. Its live
        message must be just as protected from a sibling's reclaim pass as
        one delivered on this very channel (Fix round 2, R7): reached via
        ``self.connection.cycle`` (a ``MultiChannelPoller``), not just this
        channel's own QoS table.
        """
        poller = MultiChannelPoller()

        sibling = object.__new__(Channel)
        sibling.consumer_name = "worker1:123"
        sibling._qos = MagicMock()
        sibling._qos._in_flight = {"tag-sibling-own": ("stream:celery:0", "1700000000000-0")}
        poller._channels.add(sibling)

        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = poller
        poller._channels.add(channel)

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker2:999",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 0
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_not_called()
        mock_client.xclaim.assert_not_called()

    def test_reclaim_does_not_skip_in_flight_message_owned_by_other_consumer(
        self,
        global_keyprefix: str,
    ) -> None:
        """A sibling channel's in-flight table is consulted only when it shares this consumer_name.

        Guards the filter in _own_in_flight_message_ids itself: a channel
        reachable through the same connection cycle but identifying as a
        different Redis consumer (not expected in normal operation, since
        consumer_name is derived per process, but the code checks it
        explicitly) must not suppress delivery of an id that channel merely
        happens to have recorded under the same stream key.
        """
        poller = MultiChannelPoller()

        other_consumer = object.__new__(Channel)
        other_consumer.consumer_name = "worker2:456"
        other_consumer._qos = MagicMock()
        other_consumer._qos._in_flight = {"tag-other": ("stream:celery:0", "1700000000000-0")}
        poller._channels.add(other_consumer)

        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = poller
        poller._channels.add(channel)

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-not-owned",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_called_once()
        delivered_message = channel.connection._deliver.call_args[0][0]
        assert delivered_message["properties"]["delivery_tag"] == "tag-not-owned"
        mock_ack_script.assert_not_called()

    def test_reclaim_stops_delivering_once_prefetch_exhausted(self, global_keyprefix: str) -> None:
        """Delivering stops mid claimed-batch as soon as qos.can_consume() goes false.

        The remaining discovered entries are left untouched in the PEL (never
        claimed at all) for a later reclaim pass, instead of flooding the
        channel past its prefetch_count (Fix round 1, FIX3).

        Two ids come back from XPENDING discovery, but qos.can_consume_max_estimate()
        reports only 1 slot of remaining prefetch capacity. That truncates the
        survivor list to the first id before XCLAIM even runs, so the second
        id is never claimed for real and its delivery count is never bumped
        (Fix round 2, R1: the primary defense against phantom-bumping is this
        pre-claim truncation). The per-entry qos.can_consume() check after
        delivering the sole survivor is still exercised as the real-time
        backstop.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = 1
        channel._qos.can_consume.side_effect = [False]
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json_1 = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-first",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_client.register_script.return_value = MagicMock()
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000001-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
            {
                "message_id": b"1700000000002-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000001-0", {b"payload": payload_json_1.encode()})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        # Only the first id was claimed for real; the second never reached XCLAIM
        # at all, so it was never counted or delivered.
        mock_client.xclaim.assert_called_once_with(
            "stream:celery:0",
            "celery",
            "worker1:123",
            min_idle_time=DEFAULT_VISIBILITY_TIMEOUT * 1000,
            message_ids=["1700000000001-0"],
        )
        channel.connection._deliver.assert_called_once()
        delivered_message = channel.connection._deliver.call_args[0][0]
        assert delivered_message["properties"]["delivery_tag"] == "tag-first"
        assert channel._qos.can_consume.call_count == 1

    def test_reclaim_missing_payload_acks_and_skips(self, global_keyprefix: str) -> None:
        """A claimed entry with no payload field (foreign or corrupt) is acked away, never delivered."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"some-other-field": b"whatever"})]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_not_called()
        mock_ack_script.assert_called_once_with(
            keys=[f"{global_keyprefix}stream:celery:0"],
            args=["celery", "1700000000000-0", ""],
        )

    def test_reclaim_malformed_payload_acks_and_skips(self, global_keyprefix: str) -> None:
        """A claimed entry whose payload field fails to parse is acked away, never delivered.

        Distinct from the missing-payload branch above: the payload field is
        present here, but its bytes are not valid JSON, so loads() raises. Fix
        round 2, R4: an unparseable payload must not propagate an unhandled
        exception up to the poller and abort the whole reclaim pass for this
        queue forever; it is treated like a missing payload instead (logged,
        acked away, counted against budget), and the pass continues on to
        later entries in the same batch.
        """
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        good_payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-good",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 2,
            },
            {
                "message_id": b"1700000000001-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 1,
            },
        ]
        mock_client.xclaim.return_value = [
            (b"1700000000000-0", {b"payload": b"not-valid-json{{{"}),
            (b"1700000000001-0", {b"payload": good_payload_json.encode()}),
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        # Both entries count against budget: the malformed one acked away, the
        # well-formed one right after it still delivered normally, proving the
        # parse failure did not abort the rest of the batch.
        assert result == 2
        mock_ack_script.assert_called_once_with(
            keys=[f"{global_keyprefix}stream:celery:0"],
            args=["celery", "1700000000000-0", ""],
        )
        channel.connection._deliver.assert_called_once()
        delivered_message = channel.connection._deliver.call_args[0][0]
        assert delivered_message["properties"]["delivery_tag"] == "tag-good"

    def test_reclaim_returns_immediately_when_budget_not_positive(self) -> None:
        """budget <= 0 returns 0 immediately without acquiring a connection at all."""
        channel = object.__new__(Channel)
        channel.conn_or_acquire = MagicMock()

        result = channel._reclaim_and_deliver("celery", 0)

        assert result == 0
        channel.conn_or_acquire.assert_not_called()

    def test_maybe_enqueue_due_messages_pumps_and_reclaims_with_shared_budget(self) -> None:
        """The requeue cycle runs the delayed pump then reclaim per queue, sharing one budget."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos = MagicMock()
        channel.active_queues = {"q1", "q2"}
        channel._queue_cycle = ["q1", "q2"]
        channel._move_delayed.side_effect = [3, 5]
        channel._reclaim_and_deliver.side_effect = [2, 4]
        poller._channels.add(channel)

        total = poller.maybe_enqueue_due_messages()

        assert total == 14
        assert channel._move_delayed.call_args_list == [
            call("q1", limit=DEFAULT_REQUEUE_BATCH_LIMIT),
            call("q2", limit=DEFAULT_REQUEUE_BATCH_LIMIT - 3 - 2),
        ]
        assert channel._reclaim_and_deliver.call_args_list == [
            call("q1", DEFAULT_REQUEUE_BATCH_LIMIT - 3),
            call("q2", DEFAULT_REQUEUE_BATCH_LIMIT - 10),
        ]

    def test_maybe_enqueue_due_messages_continues_after_queue_error(self) -> None:
        """An error on one queue is logged and does not abort the cycle for later queues."""
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos = MagicMock()
        channel.active_queues = {"q1", "q2"}
        channel._queue_cycle = ["q1", "q2"]
        channel._move_delayed.side_effect = [RuntimeError("boom"), 1]
        channel._reclaim_and_deliver.return_value = 0
        poller._channels.add(channel)

        total = poller.maybe_enqueue_due_messages()

        assert total == 1
        assert channel._move_delayed.call_count == 2
        channel._reclaim_and_deliver.assert_called_once_with("q2", DEFAULT_REQUEUE_BATCH_LIMIT - 1)

    def test_maybe_enqueue_due_messages_skips_inactive_channels(self) -> None:
        """Channels without a QoS or without active queues are skipped entirely."""
        poller = MultiChannelPoller()
        no_qos_channel = MagicMock()
        no_qos_channel.qos = None
        idle_channel = MagicMock()
        idle_channel.qos = MagicMock()
        idle_channel.active_queues = set()
        poller._channels.add(no_qos_channel)
        poller._channels.add(idle_channel)

        total = poller.maybe_enqueue_due_messages()

        assert total == 0
        no_qos_channel._move_delayed.assert_not_called()
        idle_channel._move_delayed.assert_not_called()

    def test_maybe_enqueue_due_messages_skips_reclaim_when_cannot_consume(self) -> None:
        """Reclaim is skipped entirely once the channel is already at its prefetch_count limit.

        The delayed pump still runs (it does not deliver into the channel), but
        _reclaim_and_deliver, which delivers directly into the channel, must not
        be invoked at all while qos.can_consume() is already false (Fix round 1,
        FIX3).
        """
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos = MagicMock()
        channel.qos.can_consume.return_value = False
        channel.active_queues = {"q1"}
        channel._queue_cycle = ["q1"]
        channel._move_delayed.return_value = 3
        poller._channels.add(channel)

        total = poller.maybe_enqueue_due_messages()

        assert total == 3
        channel._move_delayed.assert_called_once_with("q1", limit=DEFAULT_REQUEUE_BATCH_LIMIT)
        channel._reclaim_and_deliver.assert_not_called()

    def test_maybe_enqueue_due_messages_rotates_queue_order_each_cycle(self) -> None:
        """The starting queue rotates by one position every cycle so no queue is permanently first.

        Three successive cycles over the same three queues must start at q0, then
        q1, then q2 in turn (Fix round 1, FIX4b). This fails against an un-rotated
        implementation that always iterates _queue_cycle from index 0.
        """
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos = MagicMock()
        channel.active_queues = {"q0", "q1", "q2"}
        channel._queue_cycle = ["q0", "q1", "q2"]
        channel._move_delayed.return_value = 0
        channel._reclaim_and_deliver.return_value = 0
        poller._channels.add(channel)

        poller.maybe_enqueue_due_messages()
        first_order = [c.args[0] for c in channel._move_delayed.call_args_list]
        channel._move_delayed.reset_mock()

        poller.maybe_enqueue_due_messages()
        second_order = [c.args[0] for c in channel._move_delayed.call_args_list]
        channel._move_delayed.reset_mock()

        poller.maybe_enqueue_due_messages()
        third_order = [c.args[0] for c in channel._move_delayed.call_args_list]

        assert first_order == ["q0", "q1", "q2"]
        assert second_order == ["q1", "q2", "q0"]
        assert third_order == ["q2", "q0", "q1"]

    def test_maybe_enqueue_due_messages_rotation_survives_queue_list_changes(self) -> None:
        """Rotation degrades gracefully, without IndexError or a permanently skipped queue.

        Simulates the queue list changing shape between cycles (basic_cancel then
        basic_consume changing channel._queue_cycle), which the stored offset must
        survive via modulo rather than crashing or being pinned to a stale index
        (Fix round 1, FIX4b).
        """
        poller = MultiChannelPoller()
        channel = MagicMock()
        channel.qos = MagicMock()
        channel.active_queues = {"q0", "q1", "q2"}
        channel._queue_cycle = ["q0", "q1", "q2"]
        channel._move_delayed.return_value = 0
        channel._reclaim_and_deliver.return_value = 0
        poller._channels.add(channel)

        poller.maybe_enqueue_due_messages()  # offset 0 -> 1
        poller.maybe_enqueue_due_messages()  # offset 1 -> 2

        # The queue list shrinks to a single, entirely different queue: the
        # stored offset (2) must not go out of range against the new length.
        channel._queue_cycle = ["q3"]
        channel._move_delayed.reset_mock()
        poller.maybe_enqueue_due_messages()

        assert channel._move_delayed.call_args_list == [call("q3", limit=DEFAULT_REQUEUE_BATCH_LIMIT)]

        # A second queue is added back: both must get to lead in turn across
        # cycles, proving neither is permanently skipped by the surviving offset.
        channel._queue_cycle = ["q3", "q4"]
        channel._move_delayed.reset_mock()
        poller.maybe_enqueue_due_messages()
        fourth_order = [c.args[0] for c in channel._move_delayed.call_args_list]
        channel._move_delayed.reset_mock()
        poller.maybe_enqueue_due_messages()
        fifth_order = [c.args[0] for c in channel._move_delayed.call_args_list]

        assert {fourth_order[0], fifth_order[0]} == {"q3", "q4"}


@pytest.mark.unit
class TestStreamsPoison:
    """Unit tests for poison message handling in Channel._reclaim_and_deliver."""

    def test_poison_message_dropped_when_exceeding_max_restore_count(
        self,
        global_keyprefix: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """restore_count above max_restore_count acks the entry away instead of delivering it."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = 2
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-poison",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 3,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with caplog.at_level(logging.WARNING, logger="celery_redis_plus.streams"):
            result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_not_called()
        mock_client.xadd.assert_not_called()
        mock_ack_script.assert_called_once_with(
            keys=[f"{global_keyprefix}stream:celery:0"],
            args=["celery", "1700000000000-0", ""],
        )
        assert any("max restore count" in record.getMessage() for record in caplog.records)

    def test_poison_message_copied_to_dead_letter_stream(self, global_keyprefix: str) -> None:
        """With dead_letter_stream set, the poisoned payload is XADDed there before the ack."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = 1
        channel.dead_letter_stream = "dead-letters"
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-dlq",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 4,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        channel.connection._deliver.assert_not_called()
        mock_client.xadd.assert_called_once_with(
            name="dead-letters",
            fields={"payload": payload_json.encode()},
            id="*",
            maxlen=10000,
            approximate=True,
        )
        mock_ack_script.assert_called_once_with(
            keys=[f"{global_keyprefix}stream:celery:0"],
            args=["celery", "1700000000000-0", ""],
        )

    def test_message_at_max_restore_count_still_delivered(self, global_keyprefix: str) -> None:
        """restore_count equal to max_restore_count is NOT poisoned (drop only when strictly above)."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = 3
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-at-max",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 3,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        delivered_message, _delivered_queue = channel.connection._deliver.call_args[0]
        assert delivered_message["properties"]["headers"]["x-restore-count"] == 3
        mock_ack_script.assert_not_called()

    def test_no_poison_check_when_max_restore_count_none(self, global_keyprefix: str) -> None:
        """max_restore_count None means unlimited restores: always delivered with the header."""
        channel = object.__new__(Channel)
        channel.global_keyprefix = global_keyprefix
        channel.visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
        channel.message_ttl = None
        channel._message_ttls = {}
        channel.max_restore_count = None
        channel.dead_letter_stream = None
        channel.consumer_group = "celery"
        channel.consumer_name = "worker1:123"
        channel._stream_keys_for_queue = MagicMock(return_value=["stream:celery:0"])
        channel._qos = MagicMock()
        channel._qos._in_flight = {}
        channel._qos.can_consume_max_estimate.return_value = None
        channel.connection = MagicMock()
        channel.connection.cycle = None

        payload_json = json_dumps(
            {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-unlimited",
                    "delivery_info": {"exchange": "", "routing_key": "celery"},
                    "headers": {},
                },
            },
        )
        mock_client = MagicMock()
        mock_client.time.return_value = (1700000100, 0)
        mock_ack_script = MagicMock()
        mock_client.register_script.return_value = mock_ack_script
        mock_client.xclaim.return_value = [(b"1700000000000-0", {b"payload": payload_json.encode()})]
        mock_client.xpending_range.return_value = [
            {
                "message_id": b"1700000000000-0",
                "consumer": b"worker1:123",
                "time_since_delivered": 400000,
                "times_delivered": 100,
            },
        ]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        result = channel._reclaim_and_deliver("celery", 100)

        assert result == 1
        delivered_message, _delivered_queue = channel.connection._deliver.call_args[0]
        assert delivered_message["properties"]["headers"]["x-restore-count"] == 100
        mock_ack_script.assert_not_called()


@pytest.mark.integration
class TestStreamsReclaimIntegration:
    """Integration tests for Channel._reclaim_and_deliver against real Redis/Valkey.

    These exercise the real client returned by Channel._get_client(), which is
    a plain redis-py/valkey-py client when global_keyprefix is falsy and a
    PrefixedStrictRedis when it is truthy. The round 3 fix removed a
    parse_response special case that only ever ran for the prefixed case, so
    this test runs under both (via the parametrized global_keyprefix fixture)
    to confirm both code paths actually execute and behave identically.
    """

    def test_reclaim_redelivers_idle_message_with_restore_count(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A message left idle in the PEL past visibility_timeout is reclaimed
        by a different consumer and redelivered with x-restore-count set to 1.

        Sequence: publish via one channel ("producer-worker"), consume it into
        the PEL without acking (simulating a worker that picked up the message
        then died before it could ack), wait past a short visibility_timeout,
        then reclaim via a second channel with a different consumer identity
        ("reclaimer-worker") standing in for a live peer.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "reclaim-integration-queue"

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": 0.3,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        reclaimer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": 0.3,
                "consumer_name": "reclaimer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-reclaim",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker" without acking, then
            # abandon it (simulates a worker dying before it could ack).
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-reclaim"

            # Let visibility_timeout (0.3s) elapse with a generous margin so the
            # entry is reliably reclaimable even under a slow container start or
            # a GC pause on a loaded CI host.
            time.sleep(1.0)

            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 1
        assert len(delivered) == 1
        # basic_consume wraps the raw dict _reclaim_and_deliver hands to
        # connection._deliver in a kombu Message before invoking the callback.
        delivered_message = delivered[0]
        assert delivered_message.delivery_tag == "tag-integration-reclaim"
        assert delivered_message.properties["headers"]["x-restore-count"] == 1


@pytest.mark.unit
class TestStreamsHeartbeat:
    """Unit tests for the XCLAIM JUSTID heartbeat that keeps in-flight messages alive."""

    def test_heartbeat_batches_ids_per_stream(self) -> None:
        """Test that _heartbeat groups in-flight message ids by stream, one XCLAIM per stream."""
        channel = object.__new__(Channel)
        mock_qos = MagicMock()
        mock_qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:9", "2222-0"),
            "tag3": ("stream:celery:0", "3333-0"),
        }
        channel._qos = mock_qos

        mock_client = MagicMock()
        # Every requested id comes back as still pending, so no "no longer
        # pending" warning fires here; that path is covered separately by
        # test_heartbeat_logs_ids_no_longer_pending_without_pruning_in_flight.
        mock_client.xclaim.side_effect = lambda *args, **_kwargs: args[4]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
        ):
            channel._heartbeat()

        assert mock_client.xclaim.call_count == 2
        mock_client.xclaim.assert_any_call(
            "stream:celery:9",
            "celery",
            "worker-1",
            0,
            ["1111-0", "2222-0"],
            justid=True,
        )
        mock_client.xclaim.assert_any_call("stream:celery:0", "celery", "worker-1", 0, ["3333-0"], justid=True)

    def test_heartbeat_uses_justid_and_zero_idle(self) -> None:
        """Test that XCLAIM is issued with min_idle_time=0 and justid=True (no delivery-count bump)."""
        channel = object.__new__(Channel)
        mock_qos = MagicMock()
        mock_qos._in_flight = {"tag1": ("stream:celery:3", "1111-0")}
        channel._qos = mock_qos

        mock_client = MagicMock()
        mock_client.xclaim.return_value = ["1111-0"]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
        ):
            channel._heartbeat()

        mock_client.xclaim.assert_called_once_with("stream:celery:3", "celery", "worker-1", 0, ["1111-0"], justid=True)

    def test_heartbeat_empty_in_flight_is_noop(self) -> None:
        """Test that _heartbeat does not touch Redis when nothing is in flight."""
        channel = object.__new__(Channel)
        mock_qos = MagicMock()
        mock_qos._in_flight = {}
        channel._qos = mock_qos
        channel.conn_or_acquire = MagicMock()

        channel._heartbeat()

        channel.conn_or_acquire.assert_not_called()

    def test_heartbeat_swallows_errors(self) -> None:
        """Test that connection failures are logged and swallowed so the periodic timer survives."""
        channel = object.__new__(Channel)
        channel.ResponseError = _client_exceptions.ResponseError
        mock_qos = MagicMock()
        mock_qos._in_flight = {"tag1": ("stream:celery:0", "1111-0")}
        channel._qos = mock_qos

        # conn_or_acquire itself fails (e.g. the connection cannot be
        # established at all), so there is no per-stream loop to isolate;
        # this exercises the outer guard.
        channel.conn_or_acquire = MagicMock(side_effect=ConnectionError("connection lost"))

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
            patch("celery_redis_plus.streams.logger") as mock_logger,
        ):
            channel._heartbeat()  # Must not raise

        mock_logger.warning.assert_called_once()

    def test_heartbeat_stream_error_does_not_abort_other_streams(self) -> None:
        """Test that a non-NOGROUP error heartbeating one stream still lets the rest heartbeat.

        Before this fix, any exception other than a NOGROUP ResponseError
        propagated out of the per-stream loop and was caught by the outer
        try/except, abandoning every remaining stream for that cycle. One
        unlucky stream would silently stop heartbeats for all others,
        causing exactly the spurious-reclaim failure this method exists to
        prevent.
        """
        channel = object.__new__(Channel)
        channel.ResponseError = _client_exceptions.ResponseError
        mock_qos = MagicMock()
        mock_qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:0", "2222-0"),
        }
        channel._qos = mock_qos

        mock_client = MagicMock()

        def _xclaim(stream_key: str, *args: object, **kwargs: object) -> list[object]:
            if stream_key == "stream:celery:9":
                raise ConnectionError("connection reset")
            return ["2222-0"]

        mock_client.xclaim.side_effect = _xclaim
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
            patch("celery_redis_plus.streams.logger") as mock_logger,
        ):
            channel._heartbeat()  # Must not raise

        # Both streams were attempted; the failure on the first did not
        # prevent the second from being heartbeated.
        assert mock_client.xclaim.call_count == 2
        mock_client.xclaim.assert_any_call("stream:celery:0", "celery", "worker-1", 0, ["2222-0"], justid=True)
        mock_logger.warning.assert_called_once()
        assert mock_logger.warning.call_args.kwargs.get("exc_info") is True

    def test_heartbeat_malformed_reply_does_not_abort_other_streams(self) -> None:
        """Test that an unusable XCLAIM reply for one stream still lets the rest heartbeat.

        The reply-processing step is inside the per-stream try, not after
        it: a client returning something non-iterable must be contained the
        same way a raising xclaim call is, or it reintroduces the
        abort-every-remaining-stream failure one step later.
        """
        channel = object.__new__(Channel)
        channel.ResponseError = _client_exceptions.ResponseError
        mock_qos = MagicMock()
        mock_qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:0", "2222-0"),
        }
        channel._qos = mock_qos

        mock_client = MagicMock()

        def _xclaim(stream_key: str, *args: object, **kwargs: object) -> object:
            if stream_key == "stream:celery:9":
                return None  # Not iterable: blows up building refreshed_ids
            return [b"2222-0"]

        mock_client.xclaim.side_effect = _xclaim
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
            patch("celery_redis_plus.streams.logger") as mock_logger,
        ):
            channel._heartbeat()  # Must not raise

        assert mock_client.xclaim.call_count == 2
        mock_client.xclaim.assert_any_call("stream:celery:0", "celery", "worker-1", 0, ["2222-0"], justid=True)
        mock_logger.warning.assert_called_once()
        assert mock_logger.warning.call_args.kwargs.get("exc_info") is True

    def test_heartbeat_logs_ids_no_longer_pending_without_pruning_in_flight(self) -> None:
        """Test that ids missing from the XCLAIM JUSTID reply are logged, not pruned.

        An id requested but absent from the reply is no longer in the PEL
        (already acked through another path, or its stream entry was
        deleted). That is observable and worth a warning, but _in_flight
        must not be pruned from it: _in_flight is what the ack path
        resolves a delivery tag through, and the local task for that id
        may still be executing.
        """
        channel = object.__new__(Channel)
        mock_qos = MagicMock()
        mock_qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:9", "2222-0"),
        }
        channel._qos = mock_qos

        mock_client = MagicMock()
        # Only 1111-0 comes back as still pending; 2222-0 was acked elsewhere.
        mock_client.xclaim.return_value = [b"1111-0"]
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
            patch("celery_redis_plus.streams.logger") as mock_logger,
        ):
            channel._heartbeat()

        assert mock_qos._in_flight == {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:9", "2222-0"),
        }
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args.args
        assert args[1:] == (1, 2, "stream:celery:9")

    def test_heartbeat_nogroup_invalidates_stream_and_continues(self) -> None:
        """Test a NOGROUP on one stream drops its cached ensure while other streams still heartbeat."""
        channel = object.__new__(Channel)
        channel.ResponseError = _client_exceptions.ResponseError
        channel._ensured_groups = {"stream:celery:9", "stream:celery:0"}
        mock_qos = MagicMock()
        mock_qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:0", "2222-0"),
        }
        channel._qos = mock_qos

        mock_client = MagicMock()

        def _xclaim(stream_key: str, *args: object, **kwargs: object) -> list[object]:
            if stream_key == "stream:celery:9":
                # The stream (and its group, PEL included) was deleted out of band
                raise _client_exceptions.ResponseError(
                    "NOGROUP No such key 'stream:celery:9' or consumer group 'celery'",
                )
            return ["2222-0"]

        mock_client.xclaim.side_effect = _xclaim
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_client)
        mock_context.__exit__ = MagicMock(return_value=False)
        channel.conn_or_acquire = MagicMock(return_value=mock_context)

        with (
            patch.object(Channel, "consumer_group", new_callable=PropertyMock, return_value="celery"),
            patch.object(Channel, "consumer_name", new_callable=PropertyMock, return_value="worker-1"),
        ):
            channel._heartbeat()  # Must not raise

        # The dead stream's cache entry is dropped so the next consume pass
        # re-creates the group; the healthy stream was still heartbeated
        assert channel._ensured_groups == {"stream:celery:0"}
        assert mock_client.xclaim.call_count == 2

    def test_maybe_heartbeat_calls_heartbeat_on_all_channels(self) -> None:
        """Test that the poller heartbeats every registered channel."""
        poller = MultiChannelPoller()
        channel1 = MagicMock()
        channel2 = MagicMock()
        poller._channels = {channel1, channel2}  # type: ignore[assignment]

        poller.maybe_heartbeat()

        channel1._heartbeat.assert_called_once_with()
        channel2._heartbeat.assert_called_once_with()

    @staticmethod
    def _heartbeat_timer_call(mock_loop: MagicMock, transport: Transport) -> Any:
        """Return the single call_repeatedly call that registers the heartbeat timer.

        Filters by identity of transport.cycle.maybe_heartbeat (not assert_any_call)
        and asserts there is exactly one such registration, so a stale duplicate
        registration would fail the test instead of passing unnoticed.
        """
        heartbeat_calls = [
            c for c in mock_loop.call_repeatedly.call_args_list if c.args[1] is transport.cycle.maybe_heartbeat
        ]
        assert len(heartbeat_calls) == 1
        return heartbeat_calls[0]

    def test_register_with_event_loop_registers_heartbeat_timer(self) -> None:
        """Test that the heartbeat timer defaults to visibility_timeout / HEARTBEAT_INTERVAL_DIVISOR."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {}

        transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == DEFAULT_VISIBILITY_TIMEOUT / HEARTBEAT_INTERVAL_DIVISOR

    def test_register_with_event_loop_heartbeat_derived_from_visibility_timeout(self) -> None:
        """Test that a custom visibility_timeout scales the default heartbeat interval."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100}

        transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0

    def test_register_with_event_loop_heartbeat_interval_override(self) -> None:
        """Test that the heartbeat_interval transport option overrides the derived default."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 7}

        transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 7

    def test_register_with_event_loop_clamps_heartbeat_interval_equal_to_visibility_timeout(self) -> None:
        """Test that heartbeat_interval == visibility_timeout is clamped rather than honored as-is.

        Honoring it would guarantee spurious reclaims of a still-live worker's
        messages (the heartbeat would tick no faster than entries go idle
        enough to reclaim), so this falls back to the safe derived default and
        logs a warning once instead of silently accepting a self-defeating value.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 100}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_clamps_heartbeat_interval_above_visibility_timeout(self) -> None:
        """Test that heartbeat_interval > visibility_timeout is also clamped, not just the equal case."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 500}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_honors_heartbeat_interval_at_half_visibility_timeout(self) -> None:
        """Test that heartbeat_interval == visibility_timeout / 2 is honored, not clamped.

        This is the boundary M3 fixed: the clamp must leave meaningful
        headroom (at least 2 heartbeats per visibility_timeout window)
        rather than only rejecting values that equal or exceed
        visibility_timeout outright.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 50}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 50
        mock_logger.warning.assert_not_called()

    def test_register_with_event_loop_clamps_heartbeat_interval_just_above_half_visibility_timeout(self) -> None:
        """Test that a heartbeat_interval leaving less than half of visibility_timeout as headroom is clamped.

        Before M3 the clamp only rejected values >= visibility_timeout, so a
        value like 0.9 * visibility_timeout passed straight through with
        essentially zero safety margin against a slow tick or GC pause.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 51}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_clamps_heartbeat_interval_zero(self) -> None:
        """Test that heartbeat_interval=0 falls back to the default instead of disabling heartbeats.

        kombu's Timer._reschedules guards call_repeatedly with
        `if lsince and lsince >= secs`, which is never true when secs is 0,
        so a zero interval would silently disable every heartbeat callback
        forever instead of raising or firing constantly.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": 0}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_clamps_heartbeat_interval_negative(self) -> None:
        """Test that a negative heartbeat_interval falls back to the default.

        A negative secs would fire the heartbeat callback on every hub tick
        (the opposite failure mode from zero), which is just as invalid a
        configuration as a too-large or zero value.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": -5}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_clamps_heartbeat_interval_nan(self) -> None:
        """Test that a non-finite heartbeat_interval (NaN) falls back to the default."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 100, "heartbeat_interval": float("nan")}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_visibility_timeout_zero_falls_back_to_default(self) -> None:
        """Test that visibility_timeout=0 is treated as invalid configuration, not honored.

        A zero visibility_timeout is nonsensical for this transport (every
        message would be immediately eligible for reclaim) and, left
        unguarded, would also poison the heartbeat_interval derivation
        (visibility_timeout / HEARTBEAT_INTERVAL_DIVISOR == 0, which is
        itself an invalid heartbeat_interval). It is rejected outright and
        replaced with DEFAULT_VISIBILITY_TIMEOUT.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": 0}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == DEFAULT_VISIBILITY_TIMEOUT / HEARTBEAT_INTERVAL_DIVISOR
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_visibility_timeout_negative_falls_back_to_default(self) -> None:
        """Test that a negative visibility_timeout is also treated as invalid configuration."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": -10}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == DEFAULT_VISIBILITY_TIMEOUT / HEARTBEAT_INTERVAL_DIVISOR
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_clamps_heartbeat_interval_infinite(self) -> None:
        """Test that an infinite heartbeat_interval falls back to the default.

        Unlike NaN, float("inf") satisfies `0 < value` and would only be
        rejected by the upper bound, so a configuration pairing it with an
        infinite visibility_timeout needs the explicit finiteness check.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {
            "visibility_timeout": 100,
            "heartbeat_interval": float("inf"),
        }

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == 20.0
        mock_logger.warning.assert_called_once()

    def test_register_with_event_loop_visibility_timeout_infinite_falls_back_to_default(self) -> None:
        """Test that an infinite visibility_timeout falls back to the default.

        float("inf") passes `isinstance(x, numbers.Real)` and `x > 0`, so
        without a finiteness check it would be honored and derive an
        infinite heartbeat_interval, registering a timer that never fires
        and leaving every in-flight entry to be reclaimed by peers.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        mock_loop = MagicMock()
        mock_connection = MagicMock()
        mock_connection.client.transport_options = {"visibility_timeout": float("inf")}

        with patch("celery_redis_plus.streams.logger") as mock_logger:
            transport.register_with_event_loop(mock_connection, mock_loop)

        call = self._heartbeat_timer_call(mock_loop, transport)
        assert call.args[0] == DEFAULT_VISIBILITY_TIMEOUT / HEARTBEAT_INTERVAL_DIVISOR
        mock_logger.warning.assert_called_once()


@pytest.mark.integration
class TestStreamsHeartbeatIntegration:
    """Integration tests for Channel._heartbeat against real Redis/Valkey.

    Exercises the real client returned by Channel._get_client(), which is a
    plain redis-py/valkey-py client when global_keyprefix is falsy and a
    PrefixedStrictRedis when it is truthy (via the parametrized
    global_keyprefix fixture), so both XCLAIM prefixing paths actually run.
    """

    def test_heartbeat_keeps_in_flight_message_alive_past_visibility_timeout(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A message held by a live worker survives repeated reclaim attempts
        as long as that worker keeps heartbeating it, even though the total
        elapsed time is well past visibility_timeout.

        Sequence: publish and consume via one channel ("producer-worker",
        simulating a long-running task that has not acked yet), set up a
        second channel with a different consumer identity
        ("reclaimer-worker") and its consumer group up front, then call
        _heartbeat() on the producer channel every half of a short
        visibility_timeout so the PEL idle clock never crosses the timeout.
        The reclaimer's channel construction (ping) and basic_consume (which
        issues an XGROUP CREATE round trip per priority stream) happen
        before the heartbeat loop, not in the window between the last
        heartbeat and the reclaim check below: if that setup ran after the
        loop instead, it would compete with visibility_timeout for time and
        could make this test fail against correct code on a slow container
        start or a loaded CI host. Only _reclaim_and_deliver itself runs in
        that window. The reclaim must find nothing: the heartbeat resets the
        idle clock before it ever qualifies as abandoned.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "heartbeat-integration-queue"
        visibility_timeout = 0.3

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        reclaimer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "reclaimer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-heartbeat",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker" and hold it, standing
            # in for a task that is still running (never acked).
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-heartbeat"

            # Build the reclaimer's channel and consumer group up front, so
            # that setup cost is paid before the heartbeat loop rather than
            # in the critical window between the last heartbeat and the
            # reclaim check below.
            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            # Heartbeat well past visibility_timeout, resetting idle every
            # half-period so it never crosses the timeout.
            for _ in range(6):
                time.sleep(visibility_timeout / 2)
                producer_channel._heartbeat()

            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 0
        assert delivered == []


@pytest.mark.unit
class TestStreamsShutdownRestore:
    """Graceful shutdown: executor wait, then XCLAIM IDLE release of in-flight messages."""

    def test_drain_hub_callbacks_module_function_runs_callbacks(self) -> None:
        """Module-level _drain_hub_callbacks pops hub._ready and runs every callback."""
        from celery_redis_plus.transport import _drain_hub_callbacks

        callback_fail = MagicMock(side_effect=RuntimeError("boom"))
        callback_ok = MagicMock()

        mock_channel = MagicMock()
        mock_channel.connection.cycle._loop._pop_ready.return_value = [callback_fail, callback_ok]

        _drain_hub_callbacks(mock_channel)

        # Both ran despite the first one raising (exceptions are suppressed)
        callback_fail.assert_called_once()
        callback_ok.assert_called_once()

    def test_drain_hub_callbacks_module_function_safe_without_hub(self) -> None:
        """Module-level _drain_hub_callbacks is a no-op without a hub or connection."""
        from celery_redis_plus.transport import _drain_hub_callbacks

        no_loop_channel = MagicMock()
        no_loop_channel.connection.cycle._loop = None
        _drain_hub_callbacks(no_loop_channel)  # Must not raise

        broken_channel = MagicMock(spec=[])  # Empty spec: no attributes at all
        _drain_hub_callbacks(broken_channel)  # AttributeError path, must not raise

    def _make_qos(self) -> tuple[QoS, MagicMock, MagicMock]:
        """Build a bare streams QoS with a mocked channel and client."""
        qos = object.__new__(QoS)
        qos._on_collect = MagicMock()
        qos._dirty = set()
        qos._delivered = OrderedDict()
        qos._delivered.restored = False
        qos._in_flight = {}

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_channel = MagicMock()
        mock_channel.do_restore = True
        mock_channel.consumer_group = "celery"
        mock_channel.consumer_name = "workerhost:1234"
        mock_channel.connection.cycle._loop = None  # No hub: drain step is a no-op
        mock_channel.conn_or_acquire.return_value = mock_client
        qos.channel = mock_channel
        return qos, mock_channel, mock_client

    def test_restore_unacked_once_xclaims_in_flight_per_stream(self) -> None:
        """In-flight entries are XCLAIMed with idle=SHUTDOWN_IDLE_MS and justid, grouped per stream."""
        qos, _mock_channel, mock_client = self._make_qos()
        qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:celery:9", "1111-1"),
            "tag3": ("stream:other:0", "2222-0"),
        }

        with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
            qos.restore_unacked_once()

        assert mock_client.xclaim.call_count == 2
        calls_by_stream = {call.args[0]: call for call in mock_client.xclaim.call_args_list}

        celery_call = calls_by_stream["stream:celery:9"]
        assert celery_call.args[1] == "celery"  # consumer group
        assert celery_call.args[2] == "workerhost:1234"  # consumer name
        assert celery_call.args[3] == 0  # min_idle_time
        assert celery_call.args[4] == ["1111-0", "1111-1"]  # ids grouped per stream
        assert celery_call.kwargs == {"idle": SHUTDOWN_IDLE_MS, "justid": True}

        other_call = calls_by_stream["stream:other:0"]
        assert other_call.args[4] == ["2222-0"]
        assert other_call.kwargs == {"idle": SHUTDOWN_IDLE_MS, "justid": True}

        assert qos._in_flight == {}

    def test_restore_unacked_once_waits_for_executor_before_xclaim(self) -> None:
        """executor.shutdown(wait=True) completes before any XCLAIM is sent."""
        qos, _mock_channel, mock_client = self._make_qos()
        qos._in_flight = {"tag1": ("stream:celery:0", "1111-0")}

        call_order: list[str] = []
        mock_executor = MagicMock()
        mock_executor.shutdown.side_effect = lambda **_kwargs: call_order.append("executor")
        mock_pool = MagicMock()
        mock_pool.executor = mock_executor
        mock_client.xclaim.side_effect = lambda *_args, **_kwargs: call_order.append("xclaim")

        with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=mock_pool):
            qos.restore_unacked_once()

        mock_executor.shutdown.assert_called_once_with(wait=True)
        assert call_order == ["executor", "xclaim"]

    def test_restore_unacked_once_does_not_re_add_messages(self) -> None:
        """Shutdown release never re-publishes payloads: no XADD, no _put, no base restore."""
        qos, mock_channel, mock_client = self._make_qos()
        qos._in_flight = {"tag1": ("stream:celery:0", "1111-0")}
        mock_message = MagicMock()
        mock_message.delivery_info = {"routing_key": "celery"}
        qos._delivered["tag1"] = mock_message
        qos.restore_unacked = MagicMock()

        with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
            qos.restore_unacked_once()

        qos.restore_unacked.assert_not_called()
        mock_client.xadd.assert_not_called()
        mock_channel._put.assert_not_called()
        mock_channel._restore.assert_not_called()
        mock_client.xclaim.assert_called_once()

    def test_restore_unacked_once_second_call_is_noop(self) -> None:
        """A second restore_unacked_once call does not XCLAIM again."""
        qos, _mock_channel, mock_client = self._make_qos()
        qos._in_flight = {"tag1": ("stream:celery:0", "1111-0")}

        with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
            qos.restore_unacked_once()
            assert mock_client.xclaim.call_count == 1
            assert qos._delivered.restored is True
            qos.restore_unacked_once()

        assert mock_client.xclaim.call_count == 1

    def test_restore_unacked_once_contains_connection_acquire_failure(self) -> None:
        """M1: a conn_or_acquire() failure must not escape restore_unacked_once.

        restore_unacked_once runs inside Channel.close(), which has already
        set self.closed = True; an uncaught exception here would abort the
        rest of that close and leak any later channels in the same
        close_connection loop. Since nothing could be released, _in_flight
        must stay untouched (a peer will reclaim it after the visibility
        timeout, same as any other unreleased in-flight message).
        """
        qos, mock_channel, _mock_client = self._make_qos()
        qos._in_flight = {"tag1": ("stream:celery:0", "1111-0")}
        mock_channel.conn_or_acquire.side_effect = RuntimeError("connection refused")

        with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
            qos.restore_unacked_once()  # Must not raise

        assert qos._in_flight == {"tag1": ("stream:celery:0", "1111-0")}
        assert qos._delivered.restored is True

    def test_restore_unacked_once_partial_xclaim_failure_keeps_failed_entries(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """M2/M3: one stream's XCLAIM failure does not block others, and only

        the entries that were actually released are cleared from _in_flight;
        the failed stream's entry survives for a later reclaim pass, and the
        summary log reports the real success count, not the pre-failure total.
        """
        qos, _mock_channel, mock_client = self._make_qos()
        qos._in_flight = {
            "tag1": ("stream:celery:9", "1111-0"),
            "tag2": ("stream:other:0", "2222-0"),
        }
        mock_client.xclaim.side_effect = [RuntimeError("boom"), None]

        with (
            patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None),
            caplog.at_level(logging.INFO, logger="celery_redis_plus.streams"),
        ):
            qos.restore_unacked_once()

        assert mock_client.xclaim.call_count == 2
        # stream:celery:9 failed and keeps its entry; stream:other:0 succeeded
        # and is cleared.
        assert qos._in_flight == {"tag1": ("stream:celery:9", "1111-0")}
        assert "Released 1 in-flight message" in caplog.text
        assert "Failed to release in-flight messages on stream:celery:9" in caplog.text


@pytest.mark.unit
class TestStreamsCollectVsClose:
    """Connection.collect() (a lost-connection reconnect) must release broker

    resources without running the QoS restore path; Connection.close() (a
    genuine shutdown) still must run it. Both transports share the same
    _collect/_collect_transport/_release_channel_on_collect implementation
    in transport.py; this class exercises the streams Transport/Channel
    wiring specifically.
    """

    def _make_bare_channel(self) -> tuple[Channel, MagicMock]:
        channel = object.__new__(Channel)
        channel._in_poll = None
        channel._in_fanout_poll = None
        channel.closed = False
        channel._fanout_queues = []
        channel._consumers = []
        channel._cycle = None
        channel._pool = None
        channel._async_pool = None
        channel.exchange_types = None
        channel.connection = MagicMock()
        channel.ResponseError = _client_exceptions.ResponseError
        mock_qos = MagicMock()
        channel._qos = mock_qos
        return channel, mock_qos

    def test_collect_releases_channels_without_restoring(self) -> None:
        """Transport._collect releases channels without calling restore_unacked_once."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        channel, mock_qos = self._make_bare_channel()
        transport._avail_channels = [channel]
        transport.channels = []

        transport._collect(connection=MagicMock())

        mock_qos.restore_unacked_once.assert_not_called()
        mock_qos._on_collect.cancel.assert_called_once()
        assert channel.closed is True
        transport.cycle.close.assert_called_once()

    def test_close_channel_still_restores_unacked(self) -> None:
        """A real Channel.close() (genuine shutdown) still calls restore_unacked_once.

        Regression guard: proves the collect-time fix did not also make the
        normal close path skip the restore.
        """
        channel, mock_qos = self._make_bare_channel()

        channel.close()

        mock_qos.restore_unacked_once.assert_called_once()
        assert channel.closed is True

    def test_collect_disconnects_pool_and_closes_clients(self) -> None:
        """_release_channel_on_collect actually releases pool/client resources.

        The other tests in this class use a channel with no cached pool or
        client, so _disconnect_pools()/_close_clients() run as no-ops and
        never prove any resource is actually released. This gives the channel
        a real pool and client stand-in and asserts both are torn down.
        """
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        channel, _mock_qos = self._make_bare_channel()
        mock_pool = MagicMock()
        channel._pool = mock_pool
        mock_client = MagicMock()
        mock_client_connection = MagicMock()
        mock_client.connection = mock_client_connection
        channel.__dict__["client"] = mock_client
        transport._avail_channels = [channel]
        transport.channels = []

        transport._collect(connection=MagicMock())

        mock_pool.disconnect.assert_called_once()
        assert channel._pool is None
        mock_client_connection.disconnect.assert_called_once()
        assert mock_client.connection is None

    def test_collect_releases_channels_from_channels_list(self) -> None:
        """Transport._collect also releases transport.channels, not just _avail_channels."""
        transport = object.__new__(Transport)
        transport.cycle = MagicMock()
        channel, mock_qos = self._make_bare_channel()
        transport._avail_channels = []
        transport.channels = [channel]

        transport._collect(connection=MagicMock())

        mock_qos.restore_unacked_once.assert_not_called()
        assert channel.closed is True

    def test_release_channel_on_collect_already_closed_is_noop(self) -> None:
        """A channel already collected (e.g. by a prior collect) is left untouched."""
        channel, mock_qos = self._make_bare_channel()
        channel.closed = True
        channel._collected = True
        channel._disconnect_pools = MagicMock()
        channel._close_clients = MagicMock()

        _release_channel_on_collect(channel)

        mock_qos._on_collect.cancel.assert_not_called()
        channel._disconnect_pools.assert_not_called()
        channel._close_clients.assert_not_called()

    def test_close_disconnects_a_pool_rebuilt_during_restore(self) -> None:
        """A pool lazily rebuilt by restore_unacked_once() during close() must not leak.

        N5 regression: _disconnect_pools()/_close_clients() now run AFTER
        super().close() (which calls restore_unacked_once()) specifically so
        that a pool conn_or_acquire() rebuilds mid-restore gets disconnected
        too, instead of only whatever pool existed before restore ran.
        """
        channel, mock_qos = self._make_bare_channel()
        rebuilt_pool = MagicMock()

        def fake_restore(stderr: Any = None) -> None:
            channel._pool = rebuilt_pool

        mock_qos.restore_unacked_once.side_effect = fake_restore

        channel.close()

        mock_qos.restore_unacked_once.assert_called_once()
        rebuilt_pool.disconnect.assert_called_once()
        assert channel._pool is None


@pytest.mark.integration
class TestStreamsShutdownRestoreIntegration:
    """Integration tests for QoS.restore_unacked_once against real Redis/Valkey.

    Exercises the real client returned by Channel._get_client() (plain
    redis-py/valkey-py when global_keyprefix is falsy, PrefixedStrictRedis
    when truthy, via the parametrized global_keyprefix fixture), so both
    XCLAIM prefixing paths actually run against a live server.
    """

    def test_restore_unacked_once_releases_message_for_instant_peer_reclaim(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """The graceful shutdown release lets a peer reclaim the message immediately.

        Sequence: publish and consume via one channel ("producer-worker") so
        the entry sits in its PEL, then call the real producer_channel.close()
        (the production path: a Consumer bootstep closing its channel while
        the message is still in flight), not restore_unacked_once() directly.
        close() runs virtual.Channel.close() (which reaches
        QoS.restore_unacked_once()) before _disconnect_pools()/_close_clients()
        tear the channel's pool down, so whatever pool conn_or_acquire() uses
        during the release is disconnected afterward rather than leaked. Then
        read XPENDING directly to confirm the entry's idle time is now far
        above visibility_timeout while
        times_delivered is untouched by the release itself, then confirm a
        second channel's reclaim pass picks the entry up right away, well
        inside a visibility_timeout deliberately set long enough that a
        natural timeout expiry could not explain the pickup.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "shutdown-integration-queue"
        visibility_timeout = 20.0

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        reclaimer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "reclaimer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-shutdown",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)

            # Consume into the PEL as "producer-worker", standing in for a
            # worker whose Consumer bootstep is closing while the message is
            # still in flight.
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-shutdown"

            producer_qos = cast("QoS", producer_channel.qos)
            stream_key, _message_id = producer_qos._in_flight["tag-integration-shutdown"]

            reclaimer_channel = cast("Channel", reclaimer_conn.channel())
            delivered: list[Any] = []
            reclaimer_channel.basic_consume(
                queue,
                no_ack=False,
                callback=delivered.append,
                consumer_tag="reclaimer-ctag",
            )

            # No real Celery worker pool is registered for this connection;
            # patching just skips the executor-wait branch (covered by unit
            # tests) so this test isolates the XCLAIM IDLE release itself.
            # close(), not restore_unacked_once() directly: exercises the
            # real production shutdown path (see docstring).
            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
                producer_channel.close()

            assert producer_channel.closed is True
            assert producer_qos._in_flight == {}

            with reclaimer_channel.conn_or_acquire() as client:
                pending_after = client.xpending_range(
                    stream_key,
                    reclaimer_channel.consumer_group,
                    min="-",
                    max="+",
                    count=10,
                )
            assert len(pending_after) == 1
            entry = pending_after[0]
            # Idle time is now enormous: far above visibility_timeout (20s = 20000ms).
            assert entry["time_since_delivered"] >= SHUTDOWN_IDLE_MS
            # The release itself never bumps times_delivered: still 1, the
            # count from the original XREADGROUP delivery.
            assert int(entry["times_delivered"]) == 1

            start = time.monotonic()
            processed = reclaimer_channel._reclaim_and_deliver(queue, budget=10)
            elapsed = time.monotonic() - start
        finally:
            producer_conn.close()
            reclaimer_conn.close()

        assert processed == 1
        assert len(delivered) == 1
        assert delivered[0].delivery_tag == "tag-integration-shutdown"
        # Reclaimed well inside the visibility_timeout window: this proves
        # the pickup came from the artificial idle release, not from
        # visibility_timeout naturally elapsing.
        assert elapsed < visibility_timeout / 2

    def test_deferred_ack_during_close_restore_window_still_fully_acks(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """A hub.call_soon()-deferred ack drained during close() leaves no PEL or stream entry.

        F1 regression (Fix round 3): QoS.ack/reject's collected-channel no-op
        guard was keyed off channel.closed, but virtual.Channel.close() sets
        `closed = True` for a genuine shutdown too, well before it reaches
        restore_unacked_once(). Graceful shutdown's _drain_hub_callbacks()
        runs inside that same restore_unacked_once() call, specifically to
        flush acks from tasks that finished just before shutdown (scheduled
        via hub.call_soon()). Keying the no-op off `closed` silently dropped
        every one of those acks, leaving the PEL entry and stream entry
        behind (and, via the sibling restore path, given an artificial
        SHUTDOWN_IDLE_MS idle time), so a peer's reclaim pass would
        immediately redeliver an already-completed task. Reproduces the
        reviewer's repro shape directly: a real kombu Hub, a message acked
        via hub.call_soon(message.ack), then a real Channel.close().
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "graceful-shutdown-ack-queue"
        visibility_timeout = 20.0

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        hub = Hub()
        try:
            producer_channel = cast("Channel", producer_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-graceful-shutdown-ack",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)
            # basic_get (not the raw _get) returns a real Message wired to
            # this channel, so message.ack() below exercises the exact same
            # call chain a completed task's deferred ack goes through in
            # production.
            consumed = producer_channel.basic_get(queue, no_ack=False)
            assert consumed is not None
            assert consumed.delivery_tag == "tag-graceful-shutdown-ack"

            producer_qos = cast("QoS", producer_channel.qos)
            stream_key, message_id = producer_qos._in_flight["tag-graceful-shutdown-ack"]

            # Wire a real Hub onto the channel's connection cycle, exactly as
            # Transport.register_with_event_loop() does in production, and
            # defer the ack the way a just-finished task's completion
            # callback does: call_soon(), not a direct call.
            producer_channel.connection.cycle._loop = hub
            hub.call_soon(consumed.ack)

            # No worker pool registered for this connection: isolates the
            # deferred-ack drain from the executor-wait branch (covered by
            # the test above and unit tests), so this test exercises only
            # the ack-during-restore-window guard.
            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=None):
                producer_channel.close()

            assert producer_channel.closed is True
            assert producer_qos._in_flight == {}

            inspector_conn = Connection(
                broker_url,
                transport="celery_redis_plus.streams:Transport",
                transport_options={
                    "visibility_timeout": visibility_timeout,
                    "consumer_name": "inspector-worker",
                    "global_keyprefix": global_keyprefix,
                },
            )
            try:
                inspector_channel = cast("Channel", inspector_conn.channel())
                with inspector_channel.conn_or_acquire() as client:
                    pending_after = client.xpending_range(
                        stream_key,
                        inspector_channel.consumer_group,
                        min="-",
                        max="+",
                        count=10,
                    )
                    stream_entries = client.xrange(stream_key, message_id, message_id)
                # XACK removed the PEL entry...
                assert pending_after == []
                # ...and XDEL removed the stream entry itself.
                assert stream_entries == []
            finally:
                inspector_conn.close()
        finally:
            hub.close()
            producer_conn.close()

    def test_connection_collect_does_not_release_pel_or_shutdown_executor(
        self,
        redis_container: tuple[str, int, str],
        clear_redis: None,
        global_keyprefix: str,
    ) -> None:
        """Connection.collect() must not touch in-flight PEL entries or the executor.

        collect() is kombu's reconnect-cleanup escape hatch (celery calls it from
        on_connection_error_after_connected after a lost broker connection, not
        on a genuine shutdown), so this drives the real Connection.collect(),
        not Transport._collect directly, to prove kombu's dispatch actually
        finds and calls our _collect hook. A worker pool is registered for
        this connection so a stray executor.shutdown() would be observable;
        the PEL entry must be left exactly as XREADGROUP delivered it, for a
        peer to reclaim only after the visibility timeout naturally elapses,
        same as any other unreleased in-flight message.

        kombu's Connection.collect() severs the transport from its owning
        Connection unconditionally (Connection._do_close_transport sets
        transport.client = None even when a _collect hook handled the
        channels), so producer_conn/producer_channel are unusable for
        anything afterward, by kombu's own design, not because of a defect
        here. The post-collect state is inspected through a separate,
        independent connection instead, exactly as a peer reclaiming after a
        real lost connection would.
        """
        host, port, _image = redis_container
        broker_url = f"redis://{host}:{port}/0"
        queue = "collect-integration-queue"
        visibility_timeout = 20.0

        producer_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "producer-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        inspector_conn = Connection(
            broker_url,
            transport="celery_redis_plus.streams:Transport",
            transport_options={
                "visibility_timeout": visibility_timeout,
                "consumer_name": "inspector-worker",
                "global_keyprefix": global_keyprefix,
            },
        )
        try:
            producer_channel = cast("Channel", producer_conn.channel())
            inspector_channel = cast("Channel", inspector_conn.channel())

            message = {
                "body": '{"task": "test"}',
                "properties": {
                    "delivery_tag": "tag-integration-collect",
                    "delivery_info": {"exchange": "", "routing_key": queue},
                    "headers": {},
                },
            }
            producer_channel._put(queue, message)
            consumed = producer_channel._get(queue)
            assert consumed["properties"]["delivery_tag"] == "tag-integration-collect"

            producer_qos = cast("QoS", producer_channel.qos)
            in_flight_before = dict(producer_qos._in_flight)
            stream_key, _message_id = in_flight_before["tag-integration-collect"]

            mock_executor = MagicMock()
            mock_pool = MagicMock()
            mock_pool.executor = mock_executor

            with patch("celery_redis_plus.streams._get_worker_pool_for_channel", return_value=mock_pool):
                producer_conn.collect()

            mock_executor.shutdown.assert_not_called()
            # The message is still owned by this worker: metadata untouched.
            assert producer_qos._in_flight == in_flight_before

            with inspector_channel.conn_or_acquire() as client:
                pending_after = client.xpending_range(
                    stream_key,
                    inspector_channel.consumer_group,
                    min="-",
                    max="+",
                    count=10,
                )
            assert len(pending_after) == 1
            entry = pending_after[0]
            # Idle time is small (a fraction of a second since XREADGROUP),
            # nowhere near the artificial SHUTDOWN_IDLE_MS a release would set.
            assert entry["time_since_delivered"] < SHUTDOWN_IDLE_MS
            assert int(entry["times_delivered"]) == 1
        finally:
            producer_conn.close()
            inspector_conn.close()
