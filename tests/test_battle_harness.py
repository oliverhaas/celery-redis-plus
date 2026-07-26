"""Unit tests for the battle/ harness (pure logic only; no docker, no redis)."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import math
import random
import subprocess
import threading
import time
from fnmatch import fnmatchcase
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from battle.orchestrator import verify
from battle.orchestrator.chaos import RESTART_VERIFY_ATTEMPTS, RESTART_VERIFY_INTERVAL, ChaosMonkey
from battle.orchestrator.producer import Producer
from battle.orchestrator.profiles import PROFILES, RunConfig, make_config, parse_duration
from battle.orchestrator.verify import (
    EARLY_DELIVERY_TOLERANCE,
    ID_SAMPLE_LIMIT,
    EventFolder,
    IdSample,
    LatencyFolder,
    LedgerData,
    RunSignals,
    TaskFolder,
    broker_is_empty,
    build_scorecard,
    check_broker_clean,
    evaluate_verdict,
    percentile,
    print_scorecard,
    read_ledger,
)

pytestmark = pytest.mark.unit


def _pin_for_restore(monkeypatch, module, name):
    """Queue monkeypatch's restore of module.name, to be called before anything mutates it."""
    monkeypatch.setattr(module, name, getattr(module, name))


def test_parse_duration_accepts_units():
    assert parse_duration("90s") == 90.0
    assert parse_duration("15m") == 900.0
    assert parse_duration("2h") == 7200.0
    assert parse_duration("120") == 120.0


def test_parse_duration_rejects_garbage():
    with pytest.raises(ValueError, match="duration"):
        parse_duration("soon")


def test_profiles_exist():
    assert set(PROFILES) == {"smoke", "chaos", "soak"}


def test_make_config_defaults():
    cfg = make_config()
    assert cfg.profile.name == "smoke"
    assert cfg.transport == "plus"
    assert cfg.broker_image == "redis:7"
    assert cfg.host_broker_url == "redis://127.0.0.1:6390/0"
    assert cfg.host_ledger_url == "redis://127.0.0.1:6391/0"


def test_make_config_profile_overrides():
    cfg = make_config("chaos", workers=8, rate=10.0, duration=60.0)
    assert cfg.profile.workers == 8
    assert cfg.profile.rate == 10.0
    assert cfg.profile.duration == 60.0
    assert cfg.profile.visibility_timeout == PROFILES["chaos"].visibility_timeout


def test_compose_env_carries_settings():
    cfg = make_config("chaos", transport="stock", broker="valkey")
    env = cfg.compose_env()
    assert env["BATTLE_TRANSPORT"] == "stock"
    assert env["BATTLE_BROKER_IMAGE"] == "valkey/valkey:8"
    assert env["BATTLE_VISIBILITY_TIMEOUT"] == "30"
    assert env["BATTLE_REQUEUE_INTERVAL"] == "10"


def test_compose_env_pins_concurrency_and_prefetch_instead_of_inheriting_them():
    cfg = make_config("chaos")
    env = cfg.compose_env()
    assert env["BATTLE_CONCURRENCY"] == str(PROFILES["chaos"].concurrency)
    assert env["BATTLE_PREFETCH"] == str(PROFILES["chaos"].prefetch)

    cfg = make_config("smoke", concurrency=7, prefetch=2)
    env = cfg.compose_env()
    assert env["BATTLE_CONCURRENCY"] == "7"
    assert env["BATTLE_PREFETCH"] == "2"


def test_compose_env_carries_pool_and_defaults_to_prefork():
    cfg = make_config("smoke")
    assert cfg.compose_env()["BATTLE_POOL"] == "prefork"


def test_compose_env_reflects_overridden_pool():
    cfg = make_config("chaos", pool="threads")
    assert cfg.compose_env()["BATTLE_POOL"] == "threads"


def test_compose_env_maps_acks_late_to_env_string():
    cfg = make_config("smoke", acks_late=True)
    assert cfg.compose_env()["BATTLE_ACKS_LATE"] == "1"

    cfg = make_config("smoke", acks_late=False)
    assert cfg.compose_env()["BATTLE_ACKS_LATE"] == "0"


def test_all_shipped_profiles_default_to_acks_late():
    assert all(profile.acks_late is True for profile in PROFILES.values())


def test_every_profile_rate_fits_its_execution_slots():
    """`slow` tasks dominate slot-time, and every interval kill parks a worker for its downtime
    plus a full boot; a profile asking for more than the rest can retire never drains.
    """
    measured_worker_boot_seconds = 10.5
    usable_share_of_ceiling = 0.8
    for profile in PROFILES.values():
        mean_slow_seconds = sum(profile.slow_range) / 2
        slot_seconds_per_task = profile.mix["slow"] * mean_slow_seconds
        outage_seconds_per_kill = profile.kill_downtime + measured_worker_boot_seconds
        workers_down = outage_seconds_per_kill / profile.kill_interval if profile.kill_interval else 0.0
        ceiling = (profile.workers - workers_down) * profile.concurrency / slot_seconds_per_task
        assert profile.rate <= usable_share_of_ceiling * ceiling, profile.name


def test_host_env_points_at_published_ports():
    cfg = make_config()
    env = cfg.host_env()
    assert env["BATTLE_BROKER_URL"] == "redis://127.0.0.1:6390/0"
    assert env["BATTLE_LEDGER_URL"] == "redis://127.0.0.1:6391/0"


def test_resolve_config_leaves_profile_default_when_no_ack_flag_passed():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run"])
    cfg = resolve_config(args)
    assert cfg.profile.acks_late is True


def test_resolve_config_no_acks_late_flag_sets_false():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--no-acks-late"])
    cfg = resolve_config(args)
    assert cfg.profile.acks_late is False


def test_resolve_config_concurrency_flag_overrides_the_profile():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos", "--concurrency", "3"])
    cfg = resolve_config(args)
    assert cfg.profile.concurrency == 3
    assert cfg.compose_env()["BATTLE_CONCURRENCY"] == "3"


def test_resolve_config_leaves_profile_concurrency_when_no_flag_passed():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos"])
    assert resolve_config(args).profile.concurrency == PROFILES["chaos"].concurrency


def test_resolve_config_visibility_timeout_flag_overrides_the_profile():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos", "--visibility-timeout", "120"])
    cfg = resolve_config(args)
    assert cfg.profile.visibility_timeout == 120
    assert cfg.compose_env()["BATTLE_VISIBILITY_TIMEOUT"] == "120"


def test_resolve_config_leaves_profile_visibility_timeout_when_no_flag_passed():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos"])
    expected = PROFILES["chaos"].visibility_timeout
    assert resolve_config(args).profile.visibility_timeout == expected


def test_no_delayed_flag_drops_countdown_tasks_and_reweights_the_rest():
    from battle.orchestrator.cli import build_parser, resolve_config
    from battle.orchestrator.producer import pick_type

    args = build_parser().parse_args(["run", "--profile", "chaos", "--no-delayed"])
    mix = resolve_config(args).profile.mix
    assert "delayed" not in mix
    assert set(mix) == {"fast", "slow", "cpu"}
    rng = random.Random(0)
    assert {pick_type(rng, mix) for _ in range(500)} == {"fast", "slow", "cpu"}


def test_drain_timeout_gives_stock_room_for_kombus_gated_restore_sweep():
    from battle.orchestrator.cli import build_parser, drain_timeout, resolve_config

    stock = drain_timeout(resolve_config(build_parser().parse_args(["run", "--transport", "stock"])))
    plus = drain_timeout(resolve_config(build_parser().parse_args(["run", "--transport", "plus"])))
    assert stock > plus
    profile = PROFILES["smoke"]
    assert stock >= 2 * profile.visibility_timeout + 200


def test_drain_timeout_flag_overrides_the_derived_value():
    from battle.orchestrator.cli import build_parser, drain_timeout, resolve_config

    args = build_parser().parse_args(["run", "--transport", "stock", "--drain-timeout", "600"])
    assert drain_timeout(resolve_config(args)) == 600


def test_resolve_config_pool_flag_overrides_the_profile():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos", "--pool", "threads"])
    cfg = resolve_config(args)
    assert cfg.profile.pool == "threads"
    assert cfg.compose_env()["BATTLE_POOL"] == "threads"


def test_resolve_config_leaves_profile_pool_when_no_flag_passed():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--profile", "chaos"])
    assert resolve_config(args).profile.pool == PROFILES["chaos"].pool


def test_resolve_config_acks_late_flag_sets_true():
    from battle.orchestrator.cli import build_parser, resolve_config

    args = build_parser().parse_args(["run", "--acks-late"])
    cfg = resolve_config(args)
    assert cfg.profile.acks_late is True


def _set_common_env(monkeypatch):
    monkeypatch.setenv("BATTLE_BROKER_URL", "redis://127.0.0.1:6390/0")
    monkeypatch.setenv("BATTLE_LEDGER_URL", "redis://127.0.0.1:6391/0")
    monkeypatch.setenv("BATTLE_VISIBILITY_TIMEOUT", "45")
    monkeypatch.setenv("BATTLE_CONCURRENCY", "3")
    monkeypatch.setenv("BATTLE_PREFETCH", "2")


def test_create_app_plus_config(monkeypatch):
    from battle.battle_app.app import create_app

    _set_common_env(monkeypatch)
    monkeypatch.setenv("BATTLE_TRANSPORT", "plus")
    app = create_app("producer")  # producer role never patches transport constants
    assert app.conf.broker_transport == "celery_redis_plus.transport:Transport"
    assert app.conf.broker_transport_options["visibility_timeout"] == 45
    assert app.conf.task_send_sent_event is True
    assert app.conf.worker_send_task_events is True
    assert app.conf.worker_concurrency == 3
    assert app.conf.worker_prefetch_multiplier == 2


def test_create_app_stock_config(monkeypatch):
    from battle.battle_app.app import create_app

    _set_common_env(monkeypatch)
    monkeypatch.setenv("BATTLE_TRANSPORT", "stock")
    app = create_app("producer")
    assert not app.conf.broker_transport
    assert app.conf.broker_transport_options["visibility_timeout"] == 45


def test_create_app_raises_broker_pool_limit_above_the_celery_default(monkeypatch):
    from battle.battle_app.app import create_app

    celery_default = 10
    _set_common_env(monkeypatch)
    monkeypatch.delenv("BATTLE_BROKER_POOL_LIMIT", raising=False)
    assert create_app("producer").conf.broker_pool_limit > celery_default


def test_create_app_broker_pool_limit_from_env(monkeypatch):
    from battle.battle_app.app import create_app

    _set_common_env(monkeypatch)
    monkeypatch.setenv("BATTLE_BROKER_POOL_LIMIT", "17")
    assert create_app("producer").conf.broker_pool_limit == 17


def test_create_app_worker_pool_defaults_to_prefork(monkeypatch):
    from battle.battle_app.app import create_app

    _set_common_env(monkeypatch)
    monkeypatch.delenv("BATTLE_POOL", raising=False)
    assert create_app("producer").conf.worker_pool == "prefork"


def test_create_app_worker_pool_from_env(monkeypatch):
    from battle.battle_app.app import create_app

    _set_common_env(monkeypatch)
    monkeypatch.setenv("BATTLE_POOL", "threads")
    assert create_app("producer").conf.worker_pool == "threads"


def test_create_app_worker_role_patches_requeue_interval(monkeypatch):
    import celery_redis_plus.constants as crp_constants
    import celery_redis_plus.transport as crp_transport
    from battle.battle_app.app import create_app

    _pin_for_restore(monkeypatch, crp_constants, "DEFAULT_REQUEUE_CHECK_INTERVAL")
    _pin_for_restore(monkeypatch, crp_transport, "DEFAULT_REQUEUE_CHECK_INTERVAL")
    _set_common_env(monkeypatch)
    monkeypatch.setenv("BATTLE_TRANSPORT", "plus")
    monkeypatch.setenv("BATTLE_REQUEUE_INTERVAL", "7")
    create_app("worker")
    assert crp_constants.DEFAULT_REQUEUE_CHECK_INTERVAL == 7
    assert crp_transport.DEFAULT_REQUEUE_CHECK_INTERVAL == 7


def test_compose_builds_command_with_config_env(monkeypatch):
    from unittest.mock import patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")
    monkeypatch.setenv("CUSTOM_VAR", "original")

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        compose.compose(cfg, "ps", check=False, capture=True)

    mock_run.assert_called_once()
    call_args = mock_run.call_args
    assert call_args[0][0] == ["docker", "compose", "-f", str(compose.COMPOSE_FILE), "ps"]
    env = call_args[1]["env"]
    assert env["BATTLE_TRANSPORT"] == "plus"
    assert env["BATTLE_BROKER_IMAGE"] == "redis:7"
    assert "CUSTOM_VAR" in env  # preserves host env


def test_every_compose_env_var_is_interpolated_by_the_compose_file():
    """A var set only on the host is silently dropped: the container falls back to its default."""
    from battle.orchestrator import compose

    text = compose.COMPOSE_FILE.read_text()

    for name in make_config("smoke").compose_env():
        assert f"${{{name}" in text, f"{name} is set for compose but never read by docker-compose.yml"


def test_event_patch_publishes_events_appended_during_the_publish():
    """Upstream clears the live buffer after publishing, so a pool thread's append is discarded."""
    from battle.battle_app.event_patch import _flush

    buffer = ["first", "second"]
    published = []

    def fake_publish(batch, producer, routing_key):
        buffer.append("appended-during-publish")
        published.append((list(batch), routing_key))

    dispatcher = SimpleNamespace(
        mutex=threading.Lock(),
        _group_buffer={"task": buffer},
        _outbound_buffer=[],
        producer=object(),
        _publish=fake_publish,
    )

    _flush(dispatcher)

    assert published == [(["first", "second"], "task.multi")]
    assert buffer == ["appended-during-publish"]


def test_event_patch_install_is_idempotent():
    from celery.events.dispatcher import EventDispatcher

    from battle.battle_app import event_patch

    original_send, original_flush = EventDispatcher.send, EventDispatcher.flush
    try:
        event_patch.install()
        patched_send = EventDispatcher.send
        event_patch.install()

        assert EventDispatcher.send is patched_send
        assert EventDispatcher.send is not original_send
    finally:
        EventDispatcher.send = original_send
        EventDispatcher.flush = original_flush
        EventDispatcher._battle_patched = False


def test_compose_respects_check_and_capture(monkeypatch):
    from unittest.mock import patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "output"
        result = compose.compose(cfg, "up", "-d", check=False, capture=True)

    mock_run.assert_called_once()
    assert mock_run.call_args[1]["check"] is False
    assert mock_run.call_args[1]["capture_output"] is True
    assert mock_run.call_args[1]["text"] is True
    assert result == mock_run.return_value


def test_docker_runs_command(monkeypatch):
    from unittest.mock import patch

    from battle.orchestrator import compose

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "container-id"
        result = compose.docker("ps", "-a", capture=True, timeout=30.0)

    mock_run.assert_called_once()
    assert mock_run.call_args[0][0] == ["docker", "ps", "-a"]
    assert mock_run.call_args[1]["capture_output"] is True
    assert mock_run.call_args[1]["text"] is True
    assert mock_run.call_args[1]["timeout"] == 30.0
    assert result == mock_run.return_value


def test_docker_captures_output_by_default():
    from unittest.mock import patch

    from battle.orchestrator import compose

    with patch("subprocess.run") as mock_run:
        compose.docker("kill", "-s", "KILL", "battle-worker-1", check=False)

    assert mock_run.call_args[1]["capture_output"] is True


def test_up_scales_workers(monkeypatch):
    from unittest.mock import patch

    from battle.orchestrator import compose

    cfg = make_config("chaos", workers=8)

    with patch.object(compose, "compose") as mock_compose:
        compose.up(cfg)

    mock_compose.assert_called_once()
    assert mock_compose.call_args[0][0] == cfg
    assert mock_compose.call_args[0][1:] == ("up", "-d", "--build", "--scale", "worker=8")


def test_down_stops_containers(monkeypatch):
    from unittest.mock import patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")

    with patch.object(compose, "compose") as mock_compose:
        compose.down(cfg)

    mock_compose.assert_called_once()
    assert mock_compose.call_args[0][0] == cfg
    assert mock_compose.call_args[0][1:] == ("down", "-t", "5", "-v")
    assert mock_compose.call_args[1]["check"] is False


def test_worker_names_builds_list():
    from battle.orchestrator import compose

    cfg = make_config("smoke", workers=2)
    names = compose.worker_names(cfg)
    assert names == ["battle-worker-1", "battle-worker-2"]

    cfg = make_config("chaos", workers=4)
    names = compose.worker_names(cfg)
    assert names == ["battle-worker-1", "battle-worker-2", "battle-worker-3", "battle-worker-4"]


def test_running_workers_parses_json_output(monkeypatch):
    from unittest.mock import MagicMock, patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")
    json_output = (
        '{"Service":"worker","Name":"battle-worker-1","State":"running"}\n'
        '{"Service":"worker","Name":"battle-worker-2","State":"running"}\n'
        '{"Service":"monitor","Name":"battle-monitor-1","State":"running"}\n'
        '{"Service":"redis-broker","Name":"battle-redis-broker-1","State":"running"}\n'
    )

    with patch.object(compose, "compose") as mock_compose:
        mock_result = MagicMock()
        mock_result.stdout = json_output
        mock_compose.return_value = mock_result

        names = compose.running_workers(cfg)

    assert names == {"battle-worker-1", "battle-worker-2"}
    assert mock_compose.call_args[0][0] == cfg
    assert mock_compose.call_args[0][1:] == ("ps", "--format", "json")
    assert mock_compose.call_args[1]["capture"] is True
    assert mock_compose.call_args[1]["check"] is False


def test_running_workers_handles_empty_output():
    from unittest.mock import MagicMock, patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")

    with patch.object(compose, "compose") as mock_compose:
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_compose.return_value = mock_result

        names = compose.running_workers(cfg)

    assert names == set()


def test_running_workers_filters_non_running_and_non_worker_services(monkeypatch):
    from unittest.mock import MagicMock, patch

    from battle.orchestrator import compose

    cfg = make_config("smoke")
    json_output = (
        '{"Service":"worker","Name":"battle-worker-1","State":"running"}\n'
        '{"Service":"worker","Name":"battle-worker-2","State":"exited"}\n'
        '{"Service":"monitor","Name":"battle-monitor-1","State":"running"}\n'
    )

    with patch.object(compose, "compose") as mock_compose:
        mock_result = MagicMock()
        mock_result.stdout = json_output
        mock_compose.return_value = mock_result

        names = compose.running_workers(cfg)

    assert names == {"battle-worker-1"}


def test_pick_type_is_deterministic_and_respects_mix():
    import random

    from battle.orchestrator.producer import pick_type

    rng = random.Random(7)
    picks = [pick_type(rng, {"fast": 0.65, "delayed": 0.20, "slow": 0.10, "cpu": 0.05}) for _ in range(2000)]
    counts = {t: picks.count(t) for t in ("fast", "delayed", "slow", "cpu")}
    assert set(counts) == {"fast", "delayed", "slow", "cpu"}
    assert counts["fast"] > counts["delayed"] > counts["slow"] > counts["cpu"]
    rng2 = random.Random(7)
    assert picks == [pick_type(rng2, {"fast": 0.65, "delayed": 0.20, "slow": 0.10, "cpu": 0.05}) for _ in range(2000)]


def test_pick_type_single_weight():
    import random

    from battle.orchestrator.producer import pick_type

    assert pick_type(random.Random(1), {"fast": 1.0}) == "fast"


def test_submit_one_increments_submitted_on_success():
    config = make_config("smoke", mix={"fast": 1.0})
    app = MagicMock()
    ledger = MagicMock()
    producer = Producer(config, app, ledger, random.Random(1), threading.Event())

    with patch("battle.orchestrator.producer.record_submission") as mock_record:
        producer._submit_one()

    assert producer.submitted == 1
    assert producer.errors == 0
    app.send_task.assert_called_once()
    mock_record.assert_called_once()


def test_submit_one_survives_send_task_error_and_continues():
    config = make_config("smoke", mix={"fast": 1.0})
    app = MagicMock()
    app.send_task.side_effect = RuntimeError("broker unreachable")
    ledger = MagicMock()
    producer = Producer(config, app, ledger, random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.producer.record_submission") as mock_record,
        patch("battle.orchestrator.producer.time.sleep"),
    ):
        producer._submit_one()
        producer._submit_one()

    assert producer.errors == 2
    assert producer.submitted == 0
    mock_record.assert_not_called()


def test_submit_one_survives_record_submission_error_and_continues():
    config = make_config("smoke", mix={"fast": 1.0})
    app = MagicMock()
    ledger = MagicMock()
    producer = Producer(config, app, ledger, random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.producer.record_submission", side_effect=RuntimeError("ledger down")),
        patch("battle.orchestrator.producer.time.sleep"),
    ):
        producer._submit_one()

    assert producer.errors == 1
    assert producer.submitted == 0
    app.send_task.assert_called_once()

    with patch("battle.orchestrator.producer.record_submission") as mock_record:
        producer._submit_one()

    assert producer.submitted == 1
    assert producer.errors == 1
    mock_record.assert_called_once()


def test_submit_one_survives_empty_mix_without_crashing():
    config = make_config("smoke", mix={})
    app = MagicMock()
    ledger = MagicMock()
    producer = Producer(config, app, ledger, random.Random(1), threading.Event())

    with patch("battle.orchestrator.producer.time.sleep"):
        producer._submit_one()

    assert producer.errors == 1
    assert producer.submitted == 0
    app.send_task.assert_not_called()


def test_run_stops_at_duration_when_every_send_fails():
    """Regression: the inner submit loop had no duration check, so a producer that could never
    reach its target rate ran forever and `battle run` blocked on `producer.is_alive()`.
    """
    config = make_config("smoke", duration=0.2, rate=1000.0, mix={"fast": 1.0})
    app = MagicMock()
    app.send_task.side_effect = RuntimeError("broker unreachable")
    producer = Producer(config, app, MagicMock(), random.Random(1), threading.Event())

    producer.start()
    producer.join(timeout=10.0)

    assert not producer.is_alive()
    assert producer.submitted == 0
    assert producer.errors > 0


def test_run_stops_at_duration_when_throughput_cannot_keep_up():
    config = make_config("smoke", duration=0.2, rate=10_000.0, mix={"fast": 1.0})
    app = MagicMock()
    app.send_task.side_effect = lambda *_args, **_kwargs: time.sleep(0.01)
    producer = Producer(config, app, MagicMock(), random.Random(1), threading.Event())

    with patch("battle.orchestrator.producer.record_submission"):
        producer.start()
        producer.join(timeout=10.0)

    assert not producer.is_alive()
    assert producer.submitted < 10_000 * config.profile.duration


def test_submitted_is_readable_while_the_producer_is_still_running():
    """`cli.py` prints `submitted=` every 10s from the polling loop, so it has to read live."""
    config = make_config("smoke", duration=2.0, rate=400.0, mix={"fast": 1.0})
    stop = threading.Event()
    producer = Producer(config, MagicMock(), MagicMock(), random.Random(1), stop)

    with patch("battle.orchestrator.producer.record_submission"):
        producer.start()
        time.sleep(0.3)
        mid_run = producer.submitted
        alive_at_read = producer.is_alive()
        stop.set()
        producer.join(timeout=10.0)

    assert alive_at_read
    assert mid_run > 0
    assert producer.submitted >= mid_run


def test_run_draws_the_same_task_sequence_for_a_seed():
    def submitted_types(seed):
        config = make_config("smoke", duration=0.4, rate=400.0)
        app = MagicMock()
        producer = Producer(config, app, MagicMock(), random.Random(seed), threading.Event())
        with patch("battle.orchestrator.producer.record_submission"):
            producer.start()
            producer.join(timeout=30.0)
        assert not producer.is_alive()
        return [call.args[0] for call in app.send_task.call_args_list]

    first, second = submitted_types(42), submitted_types(42)
    wall_clock_bound_count = min(len(first), len(second))

    assert wall_clock_bound_count > 0
    assert first[:wall_clock_bound_count] == second[:wall_clock_bound_count]


def test_pick_mode_deterministic_and_weighted():
    import random

    from battle.orchestrator.chaos import pick_mode

    weights = {"hard": 0.4, "cold": 0.2, "warm": 0.2, "grace": 0.2}
    rng = random.Random(3)
    picks = [pick_mode(rng, weights) for _ in range(1000)]
    assert set(picks) == {"hard", "cold", "warm", "grace"}
    assert picks.count("hard") > picks.count("cold")
    rng2 = random.Random(3)
    assert picks == [pick_mode(rng2, weights) for _ in range(1000)]


def test_pick_mode_single_mode():
    import random

    from battle.orchestrator.chaos import pick_mode

    assert pick_mode(random.Random(1), {"hard": 1.0}) == "hard"


def test_chaos_monkey_initializes_error_and_restart_tracking():
    config = make_config("smoke")
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    assert monkey.errors == 0
    assert monkey.restart_failures == []
    assert monkey.unexpected_deaths == []


def test_run_iteration_survives_kill_exception():
    config = make_config("smoke", kill_schedule=((0.0, "hard"),), kill_interval=None)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch.object(monkey, "_check_unexpected_deaths"),
        patch.object(monkey, "_kill", side_effect=RuntimeError("docker unreachable")),
    ):
        result = monkey._run_iteration(start=0.0, schedule=[(0.0, "hard")], next_interval_kill=None)

    assert monkey.errors == 1
    assert result is None


def test_run_iteration_survives_check_unexpected_deaths_exception():
    config = make_config("smoke", kill_schedule=(), kill_interval=None)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with patch.object(monkey, "_check_unexpected_deaths", side_effect=RuntimeError("docker ps failed")):
        result = monkey._run_iteration(start=0.0, schedule=[], next_interval_kill=None)

    assert monkey.errors == 1
    assert result is None


def test_run_survives_kill_exception_and_keeps_looping():
    config = make_config("smoke", kill_schedule=((0.0, "hard"), (0.0, "hard")), kill_interval=None)
    stop_event = MagicMock()
    stop_event.is_set.side_effect = [False, False, True]
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), stop_event)

    with (
        patch.object(monkey, "_check_unexpected_deaths"),
        patch.object(monkey, "_kill", side_effect=RuntimeError("docker unreachable")),
    ):
        monkey.run()

    assert monkey.errors == 2


@pytest.fixture
def no_restart_backoff():
    """Stubs the restart-verification backoff so the bounded poll loop runs instantly.

    It is a real `time.sleep`, deliberately not a `stop_event.wait`: setting `stop` must not be
    able to skip the wait a restarting container needs.
    """
    with patch("battle.orchestrator.chaos.time.sleep") as mock_sleep:
        yield mock_sleep


def _docker_reporting_exit_code(code="137"):
    """A `docker` double whose `inspect` reports `code` as the killed container's exit code."""
    mock = MagicMock()
    mock.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=f"{code}\n")
    return mock


def test_restart_and_verify_returns_true_when_container_recovers():
    config = make_config("smoke")
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.docker") as mock_docker,
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        result = monkey._restart_and_verify("battle-worker-1")

    assert result is True
    mock_docker.assert_called_once_with("start", "battle-worker-1", check=False)


def test_restart_and_verify_returns_false_when_container_never_recovers(no_restart_backoff):
    config = make_config("smoke")
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.docker"),
        patch("battle.orchestrator.chaos.running_workers", return_value=set()) as mock_running,
    ):
        result = monkey._restart_and_verify("battle-worker-1")

    assert result is False
    assert mock_running.call_count == RESTART_VERIFY_ATTEMPTS
    assert no_restart_backoff.call_count == RESTART_VERIFY_ATTEMPTS - 1


def test_restart_and_verify_keeps_its_backoff_once_the_run_is_stopped():
    """`_run_lifecycle` sets `stop` the moment the producer finishes, and backing off on
    `stop_event.wait` collapsed the five polls into one burst: a container that was still
    coming up got recorded as a restart failure, failing the whole run.
    """
    config = make_config("smoke")
    stop_event = threading.Event()
    stop_event.set()
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), stop_event)

    with (
        patch("battle.orchestrator.chaos.docker"),
        patch("battle.orchestrator.chaos.running_workers", side_effect=[set(), set(), {"battle-worker-1"}]),
        patch("battle.orchestrator.chaos.time.sleep") as mock_sleep,
    ):
        result = monkey._restart_and_verify("battle-worker-1")

    assert result is True
    assert mock_sleep.call_args_list == [call(RESTART_VERIFY_INTERVAL)] * 2


def test_check_unexpected_deaths_flags_new_death_once_and_does_not_reflag_while_still_dead(no_restart_backoff):
    config = make_config("smoke", workers=2)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-2"}),
        patch("battle.orchestrator.chaos.docker") as mock_docker,
    ):
        monkey._check_unexpected_deaths()
        monkey._check_unexpected_deaths()
        monkey._check_unexpected_deaths()

    assert monkey.unexpected_deaths == ["battle-worker-1"]
    assert monkey.restart_failures == ["battle-worker-1"]
    mock_docker.assert_called_once_with("start", "battle-worker-1", check=False)


def test_check_unexpected_deaths_counts_new_death_after_recovery():
    config = make_config("smoke", workers=1)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch(
            "battle.orchestrator.chaos.running_workers",
            side_effect=[set(), {"battle-worker-1"}, set()],
        ),
        patch("battle.orchestrator.chaos.docker"),
        patch.object(monkey, "_restart_and_verify", return_value=True),
    ):
        monkey._check_unexpected_deaths()  # dead -> flagged, restart succeeds
        monkey._check_unexpected_deaths()  # recovered -> unflagged
        monkey._check_unexpected_deaths()  # dead again -> genuinely new death

    assert monkey.unexpected_deaths == ["battle-worker-1", "battle-worker-1"]
    assert monkey.restart_failures == []


def test_kill_marks_restart_failed_and_keeps_existing_timeline_fields(no_restart_backoff):
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", _docker_reporting_exit_code()),
        patch("battle.orchestrator.chaos.running_workers", return_value=set()),
    ):
        monkey._kill("hard")

    assert len(monkey.timeline) == 1
    entry = monkey.timeline[0]
    assert entry["mode"] == "hard"
    assert entry["container"] == "battle-worker-1"
    assert "t_kill" in entry
    assert "t_restarted" in entry
    assert entry["sigkilled"] is True
    assert entry["restarted"] is False
    assert monkey.restart_failures == ["battle-worker-1"]


def test_kill_records_restarted_true_on_successful_restart():
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", _docker_reporting_exit_code()),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("hard")

    entry = monkey.timeline[0]
    assert entry["restarted"] is True
    assert monkey.restart_failures == []


def test_kill_reads_sigkilled_from_the_containers_exit_code():
    """A `grace` container was recorded as SIGKILLed purely on how long `docker stop` took, so one
    exiting a hair either side of the timeout landed in the wrong duplicate-attribution bucket.
    """
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())
    mock_docker = _docker_reporting_exit_code("137")

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", mock_docker),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("grace")

    assert monkey.timeline[0]["sigkilled"] is True
    assert monkey.errors == 0
    mock_docker.assert_any_call("inspect", "-f", "{{.State.ExitCode}}", "battle-worker-1", check=False, capture=True)


def test_kill_records_a_container_that_shut_itself_down_as_not_sigkilled():
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", _docker_reporting_exit_code("0")),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("grace")

    assert monkey.timeline[0]["sigkilled"] is False
    assert monkey.errors == 0


def test_kill_does_not_claim_a_sigkill_that_never_landed():
    """`docker kill -s KILL` runs with check=False, so `hard` claiming SIGKILL from control flow
    alone would record one even for a container that had already exited on its own.
    """
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", _docker_reporting_exit_code("143")),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("hard")

    assert monkey.timeline[0]["sigkilled"] is False


def test_kill_falls_back_to_the_kill_path_when_docker_cannot_report_an_exit_code():
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())
    mock_docker = MagicMock(return_value=subprocess.CompletedProcess(args=[], returncode=1, stdout=""))

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", mock_docker),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("hard")

    assert monkey.timeline[0]["sigkilled"] is True
    assert monkey.errors == 1


def test_kill_falls_back_when_the_exit_code_is_not_a_number():
    """`docker inspect` on a container it no longer knows prints `<no value>` and still exits 0."""
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", _docker_reporting_exit_code("<no value>")),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("hard")

    assert monkey.timeline[0]["sigkilled"] is True
    assert monkey.errors == 1


def test_kill_reads_the_exit_code_before_it_restarts_the_container():
    """A restart resets `.State.ExitCode` to 0, so the read has to happen while it still means
    something.
    """
    config = make_config("smoke", kill_downtime=0.0)
    monkey = ChaosMonkey(config, MagicMock(), random.Random(1), threading.Event())
    mock_docker = _docker_reporting_exit_code()

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1"]),
        patch("battle.orchestrator.chaos.docker", mock_docker),
        patch("battle.orchestrator.chaos.running_workers", return_value={"battle-worker-1"}),
    ):
        monkey._kill("hard")

    verbs = [call.args[0] for call in mock_docker.call_args_list]
    assert verbs.index("inspect") < verbs.index("start")


def test_kill_restart_failure_is_not_reflagged_as_unexpected_death_on_next_poll(no_restart_backoff):
    """A deliberate kill whose restart never succeeds produces exactly ONE restart-failed signal,
    and the still-down target is not mis-flagged as a new unexpected death on the next poll.
    """
    config = make_config("smoke", kill_downtime=0.0)
    rng = MagicMock()
    rng.choice.return_value = "battle-worker-1"
    monkey = ChaosMonkey(config, MagicMock(), rng, threading.Event())
    worker_1_never_comes_back = {"battle-worker-2"}
    mock_docker = _docker_reporting_exit_code()

    with (
        patch("battle.orchestrator.chaos.worker_names", return_value=["battle-worker-1", "battle-worker-2"]),
        patch("battle.orchestrator.chaos.docker", mock_docker),
        patch("battle.orchestrator.chaos.running_workers", return_value=worker_1_never_comes_back),
    ):
        monkey._kill("hard")
        monkey._check_unexpected_deaths()

    assert monkey.restart_failures == ["battle-worker-1"]
    assert monkey.unexpected_deaths == []
    mock_docker.assert_any_call("kill", "-s", "KILL", "battle-worker-1", check=False)
    start_calls = [call for call in mock_docker.call_args_list if call.args[0] == "start"]
    assert len(start_calls) == 1


class _FakePipeline:
    """Records what it was asked to queue, so a test can prove the reader batches."""

    def __init__(self, client):
        self._client = client
        self._queued = []

    def hmget(self, key, fields):
        self._queued.append(("hmget", key, fields))

    def lrange(self, key, start, end):
        self._queued.append(("lrange", key, (start, end)))

    def exists(self, key):
        self._queued.append(("exists", key, None))

    def sismember(self, key, member):
        self._queued.append(("sismember", key, member))

    def execute(self):
        self._client.queued_per_execute.append(len(self._queued))
        replies = [self._reply(command) for command in self._queued]
        self._queued = []
        return replies

    def _reply(self, command):
        name, key, argument = command
        if name == "hmget":
            row = self._client.hashes.get(key, {})
            return [row[field].encode() if field in row else None for field in argument]
        if name == "lrange":
            return [entry.encode() for entry in self._client.lists.get(key, [])]
        if name == "exists":
            return int(key in self._client.hashes)
        return int(argument in self._client.sets.get(key, ()))


class _FakeLedger:
    """In-memory ledger redis, populated through the same writes battle_app.ledger performs."""

    def __init__(self):
        self.hashes = {}
        self.lists = {}
        self.sets = {}
        self.queued_per_execute = []

    def submit(self, task_id, task_type="fast", eligible_at=100.0, sent_at=100.0):
        self.hashes[f"submitted:{task_id}"] = {
            "type": task_type,
            "priority": "5",
            "sent_at": f"{sent_at:.3f}",
            "eligible_at": f"{eligible_at:.3f}",
        }
        self.sets.setdefault("submitted_ids", set()).add(task_id)

    def executed(self, task_id, started_at, hostname="battle-worker-1"):
        self.lists.setdefault(f"executions:{task_id}", []).append(f"{hostname},{started_at:.3f}")
        self.sets.setdefault("executed_ids", set()).add(task_id)

    def event(self, event_type, task_id, event_ts=0.0, received_at=0.0):
        self.hashes[f"event:{event_type}:{task_id}"] = {
            "count": "1",
            "event_ts": f"{event_ts:.3f}",
            "received_at": f"{received_at:.3f}",
        }

    def kill(self, t_kill, *, sigkilled, mode="hard", container="battle-worker-1"):
        entry = {"mode": mode, "container": container, "t_kill": t_kill, "sigkilled": sigkilled}
        self.lists.setdefault("kills", []).append(json.dumps(entry))

    def scan_iter(self, match, count=10):
        keys = sorted([*self.hashes, *self.lists, *self.sets])
        return (key.encode() for key in keys if fnmatchcase(key, match))

    def lrange(self, key, start, end):
        assert (start, end) == (0, -1)
        return [entry.encode() for entry in self.lists.get(key, [])]

    def scard(self, key):
        return len(self.sets.get(key, ()))

    def sdiffstore(self, dest, keys):
        result = set(self.sets.get(keys[0], set())).difference(*(self.sets.get(key, set()) for key in keys[1:]))
        if result:
            self.sets[dest] = result
        else:
            self.sets.pop(dest, None)
        return len(result)

    def sscan_iter(self, key, *args, **kwargs):
        return (member.encode() for member in sorted(self.sets.get(key, ())))

    def pipeline(self, transaction=False):
        return _FakePipeline(self)


def _scorecard(ledger, *, drain_ok=True, config=None, signals=None, broker_clean=None, transport="plus"):
    data = read_ledger(ledger, visibility_timeout=30)
    base = {"acks_late": True, "transport": "plus", "max_duplicates_per_kill": 2.0}
    return build_scorecard(config or base, data, drain_ok, signals or RunSignals(), broker_clean, transport)


def test_percentile_basics():
    values = sorted(float(v) for v in range(1, 101))
    assert percentile(values, 50) == 50.0
    assert percentile(values, 99) == 99.0
    assert percentile(values, 100) == 100.0


def _folder(hard=(), soft=(), visibility_timeout=30, margin=5.0):
    return TaskFolder(list(hard), list(soft), visibility_timeout, margin)


def _execs(*started_at, hostname="battle-worker-1"):
    return [(hostname, at) for at in started_at]


def test_task_folder_separates_exactly_once_from_never_executed():
    folder = _folder()

    folder.add("a", _execs(101.0))
    folder.add("b", [])

    assert folder.stats.submitted == 2
    assert folder.stats.exactly_once == 1
    assert folder.stats.missing.sample == ["b"]
    assert folder.stats.failed.count == 0


def test_task_folder_files_a_reported_failure_apart_from_a_missing_task():
    folder = _folder()

    folder.add("blew-up", [], failed=True)
    folder.add("gone", [], failed=False)

    assert folder.stats.failed.sample == ["blew-up"]
    assert folder.stats.missing.sample == ["gone"]


def test_task_folder_duplicate_inside_hard_kill_window_is_hard_kill():
    first_execution, hard_kill, vt_redelivery = 100.0, 101.0, 131.0
    folder = _folder(hard=[hard_kill])

    folder.add("a", _execs(first_execution, vt_redelivery))

    assert folder.stats.duplicates_hard_kill.sample == ["a"]
    assert folder.stats.duplicates_soft_kill.count == 0
    assert folder.stats.duplicates_unattributed.count == 0


def test_task_folder_duplicate_inside_soft_kill_window_is_soft_kill():
    """A warm/cold/grace kill can abandon in-flight work just as a SIGKILL can."""
    first_execution, soft_kill, vt_redelivery = 100.0, 101.0, 131.0
    folder = _folder(soft=[soft_kill])

    folder.add("a", _execs(first_execution, vt_redelivery))

    assert folder.stats.duplicates_soft_kill.sample == ["a"]
    assert folder.stats.duplicates_hard_kill.count == 0


def test_task_folder_duplicate_bracketing_both_hard_and_soft_kill_counts_as_hard():
    """Hard wins so a hard kill is never masked by a nearby soft one."""
    first_execution, kill, vt_redelivery = 100.0, 101.0, 131.0
    folder = _folder(hard=[kill], soft=[kill])

    folder.add("a", _execs(first_execution, vt_redelivery))

    assert folder.stats.duplicates_hard_kill.sample == ["a"]
    assert folder.stats.duplicates_soft_kill.count == 0


def test_task_folder_duplicate_without_any_kill_is_unattributed():
    folder = _folder(hard=[500.0])

    folder.add("a", _execs(100.0, 131.0))

    assert folder.stats.duplicates_unattributed.sample == ["a"]


def test_task_folder_duplicate_at_the_edge_of_the_vt_window_is_still_hard_kill():
    hard_kill, margin, visibility_timeout = 101.0, 5.0, 30
    folder = _folder(hard=[hard_kill], visibility_timeout=visibility_timeout, margin=margin)

    folder.add("a", _execs(100.0, hard_kill + visibility_timeout + margin))

    assert folder.stats.duplicates_hard_kill.sample == ["a"]


def test_task_folder_duplicate_landing_after_the_vt_window_is_unattributed():
    """A redelivery too late to be that kill's doing is a real duplicate, not an excused one."""
    hard_kill, margin, visibility_timeout = 101.0, 5.0, 30
    folder = _folder(hard=[hard_kill], visibility_timeout=visibility_timeout, margin=margin)

    folder.add("a", _execs(100.0, hard_kill + visibility_timeout + margin + 0.1))

    assert folder.stats.duplicates_unattributed.sample == ["a"]


def test_task_folder_duplicate_is_not_excused_by_a_kill_that_happened_after_it():
    folder = _folder(hard=[500.0], visibility_timeout=3600)

    folder.add("a", _execs(100.0, 110.0))

    assert folder.stats.duplicates_unattributed.sample == ["a"]


def test_task_folder_keeps_the_hostname_of_every_unattributed_duplicate_execution():
    """The ledger redis is torn down before anyone reads the scorecard, so the container that ran
    an unexplained re-execution is only nameable from what is captured here.
    """
    folder = _folder()

    folder.add("a", [("battle-worker-1", 100.0), ("battle-worker-4", 999.0)])

    assert folder.diagnostics == {"a": [["battle-worker-1", 100.0], ["battle-worker-4", 999.0]]}


def test_task_folder_caps_duplicate_diagnostics_at_the_sample_limit():
    folder = _folder()

    for index in range(ID_SAMPLE_LIMIT * 2):
        folder.add(f"dupe-{index:04d}", _execs(100.0, 999.0))

    assert folder.stats.duplicates_unattributed.count == ID_SAMPLE_LIMIT * 2
    assert len(folder.diagnostics) == ID_SAMPLE_LIMIT


def test_id_sample_keeps_the_count_after_the_sample_stops_growing():
    """The whole restructure is worthless if a bucket can still put a 3M-id list on the host."""
    sample = IdSample()

    for index in range(ID_SAMPLE_LIMIT * 4):
        sample.add(f"task-{index:04d}")

    assert sample.count == ID_SAMPLE_LIMIT * 4
    assert len(sample.sample) == ID_SAMPLE_LIMIT


def test_latency_folder_counts_first_executions_past_vt():
    folder = LatencyFolder(visibility_timeout=30)

    folder.add("a", "fast", 100.0, 100.5)
    folder.add("b", "fast", 100.0, 140.0)  # recovered via VT (40s > 30s VT)
    folder.add("c", "delayed", 110.0, 110.2)

    result = folder.result()
    assert result["first_exec_past_vt"] == 1
    assert result["per_type"]["fast"]["count"] == 2
    assert result["per_type"]["delayed"]["p50"] == pytest.approx(0.2, abs=0.01)


def test_latency_folder_reports_the_minimum_alongside_the_percentiles():
    """Percentiles cannot show an early delivery: a negative latency only pulls p50 down."""
    folder = LatencyFolder(visibility_timeout=30)

    folder.add("a", "fast", 100.0, 100.5)
    folder.add("b", "fast", 100.0, 103.0)

    result = folder.result()
    assert result["overall"]["min"] == pytest.approx(0.5)
    assert result["per_type"]["fast"]["min"] == pytest.approx(0.5)


def test_latency_folder_flags_a_countdown_delivered_before_its_due_time():
    """Native delayed delivery is a headline feature, and a regression that fires countdown
    messages immediately improves every other number the harness measures.
    """
    folder = LatencyFolder(visibility_timeout=30)

    folder.add("early", "delayed", 110.0, 100.2)
    folder.add("on_time", "delayed", 110.0, 110.4)

    result = folder.result()
    assert result["early_deliveries"] == 1
    assert result["early_deliveries_sample"] == ["early"]
    assert result["overall"]["min"] == pytest.approx(-9.8)


def test_latency_folder_does_not_flag_sub_tolerance_clock_jitter():
    folder = LatencyFolder(visibility_timeout=30)

    folder.add("a", "delayed", 110.0, 110.0 - EARLY_DELIVERY_TOLERANCE / 2)

    assert folder.result()["early_deliveries"] == 0


def test_latency_folder_caps_the_early_delivery_sample():
    folder = LatencyFolder(visibility_timeout=30)

    for index in range(ID_SAMPLE_LIMIT * 2):
        folder.add(f"early-{index:04d}", "delayed", 110.0, 100.0)

    result = folder.result()
    assert result["early_deliveries"] == ID_SAMPLE_LIMIT * 2
    assert len(result["early_deliveries_sample"]) == ID_SAMPLE_LIMIT


def test_event_folder_counts_only_expected_events_but_times_all_of_them():
    folder = EventFolder()

    folder.add("task-sent", 100.0, 100.1, expected=True)
    folder.add("task-sent", 100.0, 100.5, expected=False)

    result = folder.result({"task-sent": 2, "task-received": 0, "task-started": 0, "task-succeeded": 0})
    assert result["task-sent"]["seen"] == 1
    assert result["task-sent"]["loss_pct"] == pytest.approx(50.0)
    assert result["task-sent"]["delay_max"] == pytest.approx(0.5)


def test_event_folder_reports_no_loss_when_nothing_was_expected():
    result = EventFolder().result(dict.fromkeys(("task-sent", "task-received", "task-started", "task-succeeded"), 0))

    assert result["task-sent"] == {
        "expected": 0,
        "seen": 0,
        "loss_pct": 0.0,
        "delay_p50": math.nan,
        "delay_p95": math.nan,
        "delay_max": math.nan,
    }


def test_read_ledger_parses_ids_with_colons_and_sorts_executions_by_timestamp():
    ledger = _FakeLedger()
    ledger.submit("a:b", "fast", eligible_at=100.5)
    ledger.executed("a:b", 131.25, "battle-worker-1")
    ledger.executed("a:b", 101.125, "battle-worker-2")
    ledger.submit("plain", "delayed", eligible_at=111.0)
    ledger.executed("plain", 102.0)
    ledger.event("task-sent", "a:b", 100.0, 100.1)
    ledger.kill(500.0, sigkilled=True)

    data = read_ledger(ledger, visibility_timeout=30)

    assert data.tasks.submitted == 2
    assert data.tasks.duplicates_unattributed.sample == ["a:b"]
    assert data.duplicate_diagnostics == {"a:b": [["battle-worker-2", 101.125], ["battle-worker-1", 131.25]]}
    assert data.events["task-sent"]["seen"] == 1
    assert data.kills == [
        {"mode": "hard", "container": "battle-worker-1", "t_kill": 500.0, "sigkilled": True},
    ]


def test_read_ledger_on_an_empty_ledger_yields_empty_aggregates():
    data = read_ledger(_FakeLedger(), visibility_timeout=30)

    assert data.tasks.submitted == 0
    assert data.tasks.unexpected.count == 0
    assert data.kills == []
    assert data.latency["overall"]["count"] == 0
    assert data.events["task-sent"]["expected"] == 0


def test_read_ledger_finds_an_execution_that_was_never_submitted():
    ledger = _FakeLedger()
    ledger.submit("real")
    ledger.executed("real", 100.5)
    ledger.executed("ghost", 400.0)

    data = read_ledger(ledger, visibility_timeout=30)

    assert data.tasks.unexpected.count == 1
    assert data.tasks.unexpected.sample == ["ghost"]


def test_read_ledger_splits_unexecuted_tasks_on_the_workers_own_failure_event():
    ledger = _FakeLedger()
    ledger.submit("blew-up")
    ledger.event("task-failed", "blew-up", 100.0, 100.2)
    ledger.submit("gone")

    data = read_ledger(ledger, visibility_timeout=30)

    assert data.tasks.failed.sample == ["blew-up"]
    assert data.tasks.missing.sample == ["gone"]


def test_read_ledger_leaves_failure_events_out_of_the_loss_rows():
    """task-failed has no expected set to measure loss against, so a row for it would report a
    meaningless 0/0 and hide the count behind an `expected & seen` intersection.
    """
    ledger = _FakeLedger()
    ledger.submit("a")
    ledger.event("task-failed", "a", 100.0, 100.1)

    events = read_ledger(ledger, visibility_timeout=30).events

    assert "task-failed" not in events
    assert events["task-sent"] == {
        "expected": 1,
        "seen": 0,
        "loss_pct": 100.0,
        "delay_p50": math.nan,
        "delay_p95": math.nan,
        "delay_max": math.nan,
    }


def _bulk_ledger(task_count, *, execute=True):
    ledger = _FakeLedger()
    for index in range(task_count):
        task_id = f"task-{index:05d}"
        ledger.submit(task_id)
        if execute:
            ledger.executed(task_id, 100.5)
    return ledger


def test_read_ledger_never_queues_more_than_one_batch_into_a_pipeline(monkeypatch):
    """The old reader's peak RSS was one pipeline holding a command for every key in the ledger.
    Batching is the whole reason the reader is now flat in task count.
    """
    scan_batch, commands_per_key = 4, 2
    monkeypatch.setattr(verify, "SCAN_BATCH", scan_batch)
    ledger = _bulk_ledger(10)

    read_ledger(ledger, visibility_timeout=30)

    assert max(ledger.queued_per_execute) <= scan_batch * commands_per_key
    assert len(ledger.queued_per_execute) >= 3


def test_read_ledger_pipeline_size_does_not_grow_with_the_ledger(monkeypatch):
    monkeypatch.setattr(verify, "SCAN_BATCH", 4)
    small, large = _bulk_ledger(8), _bulk_ledger(40)

    read_ledger(small, visibility_timeout=30)
    read_ledger(large, visibility_timeout=30)

    assert len(large.queued_per_execute) > len(small.queued_per_execute)
    assert max(large.queued_per_execute) == max(small.queued_per_execute)


def test_read_ledger_caps_id_samples_without_capping_their_counts():
    ledger = _bulk_ledger(ID_SAMPLE_LIMIT * 3, execute=False)

    data = read_ledger(ledger, visibility_timeout=30)

    assert data.tasks.missing.count == ID_SAMPLE_LIMIT * 3
    assert len(data.tasks.missing.sample) == ID_SAMPLE_LIMIT


def _golden_ledger():
    """One task per outcome the scorecard distinguishes, plus a kill of each severity."""
    ledger = _FakeLedger()
    ledger.submit("t-ok", "fast", eligible_at=100.0)
    ledger.executed("t-ok", 100.5)
    ledger.submit("t-dupe-hard", "fast", eligible_at=100.0)
    ledger.executed("t-dupe-hard", 100.0, "battle-worker-1")
    ledger.executed("t-dupe-hard", 131.0, "battle-worker-2")
    ledger.submit("t-dupe-soft", "slow", eligible_at=200.0)
    ledger.executed("t-dupe-soft", 200.0, "battle-worker-1")
    ledger.executed("t-dupe-soft", 231.0, "battle-worker-3")
    ledger.submit("t-dupe-none", "fast", eligible_at=300.0)
    ledger.executed("t-dupe-none", 300.0, "battle-worker-1")
    ledger.executed("t-dupe-none", 999.0, "battle-worker-2")
    ledger.submit("t-failed", "fast", eligible_at=100.0)
    ledger.event("task-failed", "t-failed", 100.0, 100.2)
    ledger.submit("t-missing", "delayed", eligible_at=110.0)
    ledger.submit("t-early", "delayed", eligible_at=110.0)
    ledger.executed("t-early", 100.2)
    ledger.executed("t-ghost", 400.0)
    ledger.event("task-sent", "t-ok", 100.0, 100.1)
    ledger.event("task-sent", "t-missing", 100.0, 100.4)
    ledger.event("task-succeeded", "t-ok", 100.6, 100.9)
    ledger.kill(101.0, sigkilled=True)
    ledger.kill(201.0, sigkilled=False, mode="warm", container="battle-worker-2")
    return ledger


def test_scorecard_task_buckets_match_the_pre_streaming_reader():
    """Pinned against the aggregates the whole-ledger-in-memory reader produced for this fixture,
    so the streaming restructure is provably a memory-shape change and not a semantics one.
    """
    scorecard = _scorecard(_golden_ledger())

    assert scorecard["tasks"] == {
        "submitted": 7,
        "exactly_once": 2,
        "failed": 1,
        "failed_sample": ["t-failed"],
        "lost": 1,
        "lost_sample": ["t-missing"],
        "pending": 0,
        "pending_sample": [],
        "duplicates_hard_kill": 1,
        "duplicates_hard_kill_sample": ["t-dupe-hard"],
        "duplicates_soft_kill": 1,
        "duplicates_soft_kill_sample": ["t-dupe-soft"],
        "duplicates_unattributed": 1,
        "duplicates_unattributed_sample": ["t-dupe-none"],
        "unexpected": 1,
        "unexpected_sample": ["t-ghost"],
    }
    assert scorecard["duplicate_diagnostics"] == {
        "t-dupe-none": [["battle-worker-1", 300.0], ["battle-worker-2", 999.0]],
    }


def test_scorecard_latency_matches_the_pre_streaming_reader():
    scorecard = _scorecard(_golden_ledger())

    assert scorecard["latency"] == {
        "overall": {"count": 5, "min": -9.799999999999997, "p50": 0.0, "p95": 0.5, "p99": 0.5, "max": 0.5},
        "per_type": {
            "delayed": {
                "count": 1,
                "min": -9.799999999999997,
                "p50": -9.799999999999997,
                "p95": -9.799999999999997,
                "p99": -9.799999999999997,
                "max": -9.799999999999997,
            },
            "fast": {"count": 3, "min": 0.0, "p50": 0.0, "p95": 0.5, "p99": 0.5, "max": 0.5},
            "slow": {"count": 1, "min": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0},
        },
        "first_exec_past_vt": 0,
        "early_deliveries": 1,
        "early_deliveries_sample": ["t-early"],
    }


def test_scorecard_event_stats_match_the_pre_streaming_reader():
    scorecard = _scorecard(_golden_ledger())

    assert scorecard["events"] == {
        "task-sent": {
            "expected": 7,
            "seen": 2,
            "loss_pct": 71.42857142857143,
            "delay_p50": 0.09999999999999432,
            "delay_p95": 0.4000000000000057,
            "delay_max": 0.4000000000000057,
        },
        "task-received": {
            "expected": 6,
            "seen": 0,
            "loss_pct": 100.0,
            "delay_p50": math.nan,
            "delay_p95": math.nan,
            "delay_max": math.nan,
        },
        "task-started": {
            "expected": 6,
            "seen": 0,
            "loss_pct": 100.0,
            "delay_p50": math.nan,
            "delay_p95": math.nan,
            "delay_max": math.nan,
        },
        "task-succeeded": {
            "expected": 6,
            "seen": 1,
            "loss_pct": 83.33333333333333,
            "delay_p50": 0.30000000000001137,
            "delay_p95": 0.30000000000001137,
            "delay_max": 0.30000000000001137,
        },
    }


def test_scorecard_verdict_failures_match_the_pre_streaming_reader():
    scorecard = _scorecard(
        _golden_ledger(),
        config={"acks_late": True, "transport": "plus", "kill_interval": 7.0, "max_duplicates_per_kill": 2.0},
        broker_clean={"queues": {}, "indices": {}, "message_hashes": 0},
    )

    assert scorecard["verdict"]["passed"] is False
    assert scorecard["verdict"]["failures"] == [
        "1 tasks lost (first: ['t-missing'])",
        "1 tasks failed in the worker, not lost by the transport (first: ['t-failed'])",
        "1 duplicates not attributable to any kill (first: ['t-dupe-none'])",
        "1 executions never submitted (producer_errors=0, each of which loses one submission record)",
        "1 tasks executed before their eligibility time (first: ['t-early'])",
    ]


_TASK_BUCKETS = (
    "failed",
    "lost",
    "pending",
    "duplicates_hard_kill",
    "duplicates_soft_kill",
    "duplicates_unattributed",
    "unexpected",
)


def _minimal_scorecard(*, submitted=10, exactly_once=10, kills=None, **buckets):
    """Buckets are given as id lists and expanded into the count + `_sample` pair build_scorecard
    stores, so a test cannot state a count that disagrees with the ids beside it.
    """
    tasks = {"submitted": submitted, "exactly_once": exactly_once}
    for name in _TASK_BUCKETS:
        ids = buckets.get(name, [])
        tasks[name] = len(ids)
        tasks[f"{name}_sample"] = list(ids)
    return {
        "config": {"kill_interval": 20.0, "kill_schedule": (), "max_duplicates_per_kill": 2.0},
        "kills": [{"mode": "hard", "sigkilled": True}] if kills is None else kills,
        "tasks": tasks,
        "drain_ok": True,
        "unexpected_deaths": [],
        "restart_failures": [],
        "producer_errors": 0,
        "chaos_errors": 0,
        "chaos_join_timed_out": False,
        "latency": {
            "overall": {"count": 10, "min": 0.1, "p50": 0.2, "p95": 0.3, "p99": 0.4, "max": 0.5},
            "per_type": {},
            "first_exec_past_vt": 0,
            "early_deliveries": 0,
            "early_deliveries_sample": [],
        },
        "broker_clean": {"queues": {}, "indices": {}, "message_hashes": 0},
        "verdict": {"mode": "enforced"},
    }


def test_verdict_passes_clean_run():
    passed, failures = evaluate_verdict(_minimal_scorecard())
    assert passed
    assert failures == []


def test_verdict_fails_on_loss_and_orphans():
    card = _minimal_scorecard(lost=["x"])
    card["broker_clean"] = {"queues": {}, "indices": {}, "message_hashes": 3}
    passed, failures = evaluate_verdict(card)
    assert not passed
    assert any("lost" in f for f in failures)
    assert any("orphan" in f for f in failures)


def test_verdict_reports_the_full_lost_count_beside_a_truncated_sample():
    """A capped sample must never be mistaken for the number of tasks the transport dropped."""
    card = _minimal_scorecard(lost=_ids(ID_SAMPLE_LIMIT, "lost"))
    card["tasks"]["lost"] = 4000

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any(f.startswith("4000 tasks lost") for f in failures)


def test_verdict_fails_when_tasks_are_still_pending_after_a_drain_timeout():
    """A drain timeout is the likeliest way a real run fails, and `pending` is the bucket its
    unexecuted tasks land in once `drain_ok` is False.
    """
    scorecard = _minimal_scorecard(pending=["task-1", "task-2"])
    scorecard["drain_ok"] = False

    passed, failures = evaluate_verdict(scorecard)

    assert not passed
    assert "2 tasks still pending after drain timeout" in failures
    assert "drain did not complete before timeout" in failures


def test_verdict_fails_when_no_tasks_were_submitted():
    """An empty run satisfies every count-is-zero check, so absence of load must fail on its own."""
    passed, failures = evaluate_verdict(_minimal_scorecard(submitted=0))
    assert not passed
    assert any("no tasks were submitted" in f for f in failures)


def test_verdict_fails_when_a_kill_scheduling_profile_produced_no_kills():
    passed, failures = evaluate_verdict(_minimal_scorecard(kills=[]))
    assert not passed
    assert any("chaos injected no kills" in f for f in failures)


def test_verdict_fails_when_an_explicit_kill_schedule_produced_no_kills():
    card = _minimal_scorecard(kills=[])
    card["config"] = {"kill_interval": None, "kill_schedule": ((25.0, "warm"),)}
    passed, failures = evaluate_verdict(card)
    assert not passed
    assert any("chaos injected no kills" in f for f in failures)


def test_verdict_allows_no_kills_when_the_profile_schedules_none():
    card = _minimal_scorecard(kills=[])
    card["config"] = {"kill_interval": None, "kill_schedule": ()}
    passed, failures = evaluate_verdict(card)
    assert passed
    assert failures == []


def test_verdict_fails_on_unattributed_duplicates_but_not_hard_or_soft_kill_ones():
    card = _minimal_scorecard(
        duplicates_hard_kill=["a"],
        duplicates_soft_kill=["b"],
        duplicates_unattributed=["c"],
        kills=[{"sigkilled": True}] * 4,
    )
    passed, failures = evaluate_verdict(card)
    assert not passed
    assert any("1 duplicates not attributable to any kill" in f and "c" in f for f in failures)


def test_verdict_ignores_hard_and_soft_kill_duplicates_below_the_rate_ceiling():
    card = _minimal_scorecard(
        duplicates_hard_kill=["a", "b"],
        duplicates_soft_kill=["c", "d"],
        kills=[{"sigkilled": True}] * 4,
    )
    passed, failures = evaluate_verdict(card)
    assert passed
    assert failures == []


def test_verdict_fails_when_attributed_duplicates_outrun_the_per_kill_ceiling():
    """Attribution alone excuses ~78% of arbitrarily placed duplicates, so a systematic
    duplication regression would otherwise be absorbed by the kill windows entirely.
    """
    card = _minimal_scorecard(duplicates_hard_kill=_ids(7, "dupe"), kills=[{"sigkilled": True}] * 3)

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any("7 kill-attributed duplicates over 3 kills is 2.33 per kill" in f for f in failures)
    assert any("above the 2.00 ceiling" in f for f in failures)


def test_verdict_counts_soft_kill_duplicates_towards_the_per_kill_ceiling():
    card = _minimal_scorecard(
        duplicates_hard_kill=_ids(4, "hard"),
        duplicates_soft_kill=_ids(3, "soft"),
        kills=[{"sigkilled": True}] * 3,
    )

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any("7 kill-attributed duplicates" in f for f in failures)


def test_verdict_allows_a_duplicate_rate_exactly_at_the_ceiling():
    card = _minimal_scorecard(duplicates_hard_kill=_ids(6, "dupe"), kills=[{"sigkilled": True}] * 3)

    passed, failures = evaluate_verdict(card)

    assert passed, failures


def test_verdict_duplicate_rate_rule_does_not_divide_by_zero_without_kills():
    """The soak profile kills nothing, and no kills is already its own failure where it matters."""
    card = _minimal_scorecard(duplicates_hard_kill=_ids(9, "dupe"), kills=[])
    card["config"] = {"kill_interval": None, "kill_schedule": (), "max_duplicates_per_kill": 2.0}

    passed, failures = evaluate_verdict(card)

    assert passed, failures


def test_verdict_skips_the_duplicate_rate_rule_when_no_ceiling_is_configured():
    card = _minimal_scorecard(duplicates_hard_kill=_ids(9, "dupe"))
    card["config"]["max_duplicates_per_kill"] = None

    passed, failures = evaluate_verdict(card)

    assert passed, failures


@pytest.mark.parametrize(("duplicates", "kills"), [(22, 36), (11, 38)])
def test_verdict_passes_both_recorded_chaos_runs_under_the_new_rate_ceiling(duplicates, kills):
    """battle/results/20260725-133010 and -142148. The first predates the hard/soft split, so
    every one of its duplicates is counted as attributed: the worst case for this rule.
    """
    card = _minimal_scorecard(
        duplicates_hard_kill=_ids(duplicates, "dupe"),
        kills=[{"sigkilled": True}] * kills,
    )

    passed, failures = evaluate_verdict(card)

    assert passed, failures


def test_chaos_profile_duplicate_ceiling_clears_both_recorded_runs():
    ceiling = PROFILES["chaos"].max_duplicates_per_kill

    assert ceiling is not None
    assert ceiling / 3 > 22 / 36
    assert ceiling < PROFILES["chaos"].concurrency


def test_verdict_fails_on_chaos_iteration_errors():
    """A chaos thread erroring on every poll injects nothing, which otherwise reads as clean."""
    card = _minimal_scorecard()
    card["chaos_errors"] = 42
    passed, failures = evaluate_verdict(card)
    assert not passed
    assert any("chaos iterations failed" in f for f in failures)


def test_verdict_fails_when_the_chaos_thread_outlived_its_join():
    """The kill timeline is then incomplete, so nothing else on this scorecard can be trusted to
    attribute duplicates. Defence in depth: 180s covers a worst-case warm iteration.
    """
    scorecard = _minimal_scorecard()
    scorecard["chaos_join_timed_out"] = True

    passed, failures = evaluate_verdict(scorecard)

    assert not passed
    assert "chaos thread outlived its join timeout; the kill timeline may be incomplete" in failures


def test_verdict_passes_a_scorecard_recorded_before_the_chaos_join_signal_existed():
    scorecard = _minimal_scorecard()
    del scorecard["chaos_join_timed_out"]

    passed, failures = evaluate_verdict(scorecard)

    assert passed
    assert failures == []


def test_verdict_fails_on_restart_failures():
    card = _minimal_scorecard()
    card["restart_failures"] = ["battle-worker-2"]
    passed, failures = evaluate_verdict(card)
    assert not passed
    assert any("battle-worker-2" in f for f in failures)


def test_verdict_ignores_producer_errors():
    card = _minimal_scorecard()
    card["producer_errors"] = 17
    passed, failures = evaluate_verdict(card)
    assert passed
    assert failures == []


def test_verdict_names_producer_errors_when_reporting_executions_never_submitted():
    """The producer ledgers a submission only after a successful send, so each producer error
    yields exactly one executed-but-never-submitted id. Reporting that count without the counter
    that explains it reads as the transport having invented a task.
    """
    card = _minimal_scorecard(unexpected=["ghost"])
    card["producer_errors"] = 3

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any("executions never submitted" in f and "producer_errors=3" in f for f in failures)


def test_verdict_reports_a_worker_side_failure_separately_from_transport_loss():
    """A task whose body raised was acked and discarded by Celery, so it looks exactly like a
    lost message. Reporting it as loss indicts the transport for a worker-side ledger outage.
    """
    card = _minimal_scorecard(lost=["gone"], failed=["blew-up"])

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any("1 tasks lost" in f and "gone" in f for f in failures)
    assert any("failed in the worker" in f and "blew-up" in f for f in failures)


def test_verdict_fails_when_a_task_executed_before_it_was_eligible():
    card = _minimal_scorecard()
    card["latency"]["early_deliveries"] = 1
    card["latency"]["early_deliveries_sample"] = ["too-soon"]

    passed, failures = evaluate_verdict(card)

    assert not passed
    assert any("before their eligibility time" in f and "too-soon" in f for f in failures)


def test_build_scorecard_files_a_task_that_reported_failure_as_failed_not_lost():
    ledger = _FakeLedger()
    ledger.submit("blew-up")
    ledger.event("task-failed", "blew-up", 100.0, 100.2)
    ledger.submit("gone")

    scorecard = _scorecard(ledger)

    assert scorecard["tasks"]["failed_sample"] == ["blew-up"]
    assert scorecard["tasks"]["lost_sample"] == ["gone"]


def test_build_scorecard_keeps_a_failed_task_out_of_pending_after_a_drain_timeout():
    """A drain timeout turns unexecuted tasks into `pending`, but a task that already reported
    failure is never going to run: it belongs in `failed` either way.
    """
    ledger = _FakeLedger()
    ledger.submit("blew-up")
    ledger.event("task-failed", "blew-up", 100.0, 100.2)
    ledger.submit("queued")

    scorecard = _scorecard(ledger, drain_ok=False)

    assert scorecard["tasks"]["failed_sample"] == ["blew-up"]
    assert scorecard["tasks"]["pending_sample"] == ["queued"]
    assert scorecard["tasks"]["lost"] == 0


def test_build_scorecard_calls_a_drain_timeout_against_an_empty_broker_lost():
    """A drain that times out with nothing left on the broker has nothing left to redeliver, so
    hedging those tasks as `pending` would understate a measured loss.
    """
    ledger = _FakeLedger()
    ledger.submit("gone")

    scorecard = _scorecard(ledger, drain_ok=False, broker_clean=_BROKER_EMPTY)

    assert scorecard["tasks"]["lost_sample"] == ["gone"]
    assert scorecard["tasks"]["pending"] == 0


def test_build_scorecard_keeps_tasks_pending_when_the_broker_still_holds_messages():
    ledger = _FakeLedger()
    ledger.submit("queued")

    scorecard = _scorecard(ledger, drain_ok=False, broker_clean=_BROKER_DIRTY)

    assert scorecard["tasks"]["pending_sample"] == ["queued"]
    assert scorecard["tasks"]["lost"] == 0


def test_build_scorecard_keeps_the_pending_hedge_under_early_ack():
    """Early ack drops the message before the task body runs, so an empty broker is the normal
    state for work still executing and cannot stand in for a drain.
    """
    ledger = _FakeLedger()
    ledger.submit("running")

    scorecard = _scorecard(
        ledger,
        drain_ok=False,
        config={"acks_late": False, "transport": "plus", "max_duplicates_per_kill": 2.0},
        broker_clean=_BROKER_EMPTY,
    )

    assert scorecard["tasks"]["pending_sample"] == ["running"]
    assert scorecard["tasks"]["lost"] == 0


def test_build_scorecard_ignores_a_failure_event_for_a_task_that_did_execute():
    """A failure event only reclassifies a task with no execution record; one that ran (and was
    retried, or failed after recording) still counts as executed.
    """
    ledger = _FakeLedger()
    ledger.submit("a")
    ledger.executed("a", 101.0)
    ledger.event("task-failed", "a", 100.0, 100.2)

    scorecard = _scorecard(ledger)

    assert scorecard["tasks"]["failed"] == 0
    assert scorecard["tasks"]["exactly_once"] == 1


def test_build_scorecard_carries_thread_signals_into_the_scorecard():
    ledger = _FakeLedger()
    ledger.submit("a")
    ledger.executed("a", 101.0)
    signals = RunSignals(
        unexpected_deaths=["battle-worker-1"],
        restart_failures=["battle-worker-2"],
        producer_errors=3,
        chaos_errors=4,
        chaos_join_timed_out=True,
    )

    scorecard = _scorecard(ledger, signals=signals)

    assert scorecard["unexpected_deaths"] == ["battle-worker-1"]
    assert scorecard["restart_failures"] == ["battle-worker-2"]
    assert scorecard["producer_errors"] == 3
    assert scorecard["chaos_errors"] == 4
    assert scorecard["chaos_join_timed_out"] is True


def test_build_scorecard_empty_run_does_not_pass():
    """Regression: every verdict check was list-emptiness, so a zero-load run reported PASS."""
    scorecard = _scorecard(
        _FakeLedger(),
        config={"acks_late": True, "transport": "plus", "kill_interval": 20.0, "kill_schedule": ()},
        broker_clean={"queues": {}, "indices": {}, "message_hashes": 0},
    )

    assert scorecard["tasks"]["submitted"] == 0
    assert scorecard["verdict"]["passed"] is False
    assert any("no tasks were submitted" in f for f in scorecard["verdict"]["failures"])
    assert any("chaos injected no kills" in f for f in scorecard["verdict"]["failures"])


def test_build_scorecard_default_ledger_data_is_printable_and_verdictable():
    """`LedgerData()` is the shape the interrupt path can end up with; it must not KeyError."""
    scorecard = build_scorecard(
        {"acks_late": True, "transport": "plus"},
        LedgerData(),
        drain_ok=True,
        signals=RunSignals(),
        broker_clean=None,
        transport="plus",
    )

    assert scorecard["verdict"]["passed"] is False
    print_scorecard(scorecard)


def test_build_scorecard_acks_late_false_is_report_only_even_for_plus_transport():
    ledger = _FakeLedger()
    ledger.submit("a")
    ledger.executed("a", 101.0)

    scorecard = _scorecard(ledger, config={"acks_late": False})

    assert scorecard["verdict"]["mode"] == "report-only"
    assert scorecard["verdict"]["passed"] is None


def test_build_scorecard_acks_late_true_is_enforced_for_plus_transport():
    ledger = _FakeLedger()
    ledger.submit("a")
    ledger.executed("a", 101.0)

    scorecard = _scorecard(ledger)

    assert scorecard["verdict"]["mode"] == "enforced"
    assert isinstance(scorecard["verdict"]["passed"], bool)


def test_build_scorecard_duplicate_diagnostics_only_carries_unattributed_ones():
    """The ledger redis is torn down after the run, so an unattributed duplicate's container and
    timestamps have to be captured in the scorecard itself or they're gone for good.
    """
    ledger = _FakeLedger()
    ledger.submit("attributed")
    ledger.executed("attributed", 100.0, "battle-worker-1")
    ledger.executed("attributed", 131.0, "battle-worker-2")
    ledger.submit("unattributed")
    ledger.executed("unattributed", 100.0, "battle-worker-1")
    ledger.executed("unattributed", 999.0, "battle-worker-3")
    ledger.kill(101.0, sigkilled=True)

    scorecard = _scorecard(ledger)

    assert scorecard["tasks"]["duplicates_hard_kill_sample"] == ["attributed"]
    assert scorecard["tasks"]["duplicates_unattributed_sample"] == ["unattributed"]
    assert scorecard["duplicate_diagnostics"] == {
        "unattributed": [["battle-worker-1", 100.0], ["battle-worker-3", 999.0]],
    }


def _printable_scorecard(**overrides):
    card = _minimal_scorecard()
    card["events"] = {"task-sent": {"seen": 10, "expected": 10, "loss_pct": 0.0, "delay_p95": 0.05}}
    card["verdict"] = {"mode": "enforced", "passed": True, "failures": []}
    card.update(overrides)
    return card


def test_print_scorecard_reports_a_soak_summary_that_collected_no_samples(capsys):
    """`summarize_samples` returns a short dict for an empty file (a soak run shorter than one
    sample interval, or one where every sample raised). Dereferencing the long shape used to
    raise KeyError *before* the scorecard was saved, losing the whole run.
    """
    card = _printable_scorecard(soak={"samples": 0, "skipped": 0, "errors": 3})

    print_scorecard(card)

    out = capsys.readouterr().out
    assert "soak: no samples collected" in out
    assert "3 sampling errors" in out


def test_print_scorecard_still_renders_a_full_soak_summary(capsys):
    card = _printable_scorecard(
        soak={
            "samples": 2,
            "skipped": 0,
            "errors": 0,
            "mem_start": {"battle-worker-1": 2 * 1024 * 1024},
            "mem_end": {"battle-worker-1": 3 * 1024 * 1024},
            "mem_max": {"battle-worker-1": 4 * 1024 * 1024},
            "redis_mem_start": 1024 * 1024,
            "redis_mem_end": 2 * 1024 * 1024,
            "throughput_per_interval": {"min": 10, "mean": 15.0, "max": 20},
        },
    )

    print_scorecard(card)

    out = capsys.readouterr().out
    assert "soak: 2 samples" in out
    assert "redis mem: 1.0MB -> 2.0MB" in out
    assert "battle-worker-1: 2.0 -> 3.0MB (max 4.0MB)" in out


def test_print_scorecard_renders_what_build_scorecard_actually_produces(capsys):
    """C1 was a print/build shape mismatch, so pin the two together on a real scorecard rather
    than on a hand-built dict that can drift away from the producer.
    """
    scorecard = _scorecard(
        _golden_ledger(),
        broker_clean={"queues": {}, "indices": {}, "message_hashes": 0},
    )

    print_scorecard(scorecard)

    out = capsys.readouterr().out
    assert "failed(worker-side)=1" in out
    assert "dupes(UNATTRIBUTED)=1" in out
    assert "latency: min=-9.80s" in out
    assert "DELIVERED EARLY: 1 (first: ['t-early'])" in out
    assert "verdict: FAIL" in out
    assert any("failed in the worker" in line for line in out.splitlines())


def test_print_scorecard_prints_bucket_counts_not_sample_lengths(capsys):
    """A 4000-task loss printed as `lost=50` would understate the run by two orders of magnitude."""
    card = _printable_scorecard()
    card["tasks"]["lost"] = 4000
    card["tasks"]["lost_sample"] = _ids(50, "lost")

    print_scorecard(card)

    assert "lost=4000" in capsys.readouterr().out


def test_print_scorecard_reports_an_unavailable_soak_summary(capsys):
    card = _printable_scorecard(soak={"samples": 0, "skipped": 0, "errors": 1, "error": "no such file"})

    print_scorecard(card)

    assert "soak: unavailable (no such file)" in capsys.readouterr().out


def _broker_client(*, queues, indices, message_keys, depths):
    client = MagicMock()
    client.scan_iter.side_effect = lambda match, count: {  # noqa: ARG005
        "queue:*": queues,
        "messages_index:*": indices,
        "message:*": message_keys,
    }[match]
    client.zcard.side_effect = lambda key: depths[key]
    return client


def test_check_broker_clean_reports_a_settled_broker_as_empty():
    client = _broker_client(
        queues=[b"queue:battle"],
        indices=[b"messages_index:battle"],
        message_keys=[],
        depths={b"queue:battle": 0, b"messages_index:battle": 0},
    )

    result = check_broker_clean(client, "plus")

    assert result == {"queues": {}, "indices": {}, "message_hashes": 0}
    assert broker_is_empty(result)


def test_check_broker_clean_reports_orphaned_keys():
    client = _broker_client(
        queues=[b"queue:battle"],
        indices=[b"messages_index:battle"],
        message_keys=[b"message:t1", b"message:t2"],
        depths={b"queue:battle": 3, b"messages_index:battle": 0},
    )

    result = check_broker_clean(client, "plus")

    assert result == {"queues": {"queue:battle": 3}, "indices": {}, "message_hashes": 2}
    assert not broker_is_empty(result)


def _stock_broker_client(*, lists, unacked, unacked_index):
    client = MagicMock()
    client.scan_iter.side_effect = lambda count, _type: iter(lists)  # noqa: ARG005
    client.llen.side_effect = lambda key: lists[key]
    client.hlen.side_effect = lambda key: {"unacked": unacked}[key]
    client.zcard.side_effect = lambda key: {"unacked_index": unacked_index}[key]
    return client


def test_check_broker_clean_reports_a_settled_stock_broker_as_empty():
    client = _stock_broker_client(lists={b"celery": 0}, unacked=0, unacked_index=0)

    result = check_broker_clean(client, "stock")

    assert result == {"queues": {}, "indices": {}, "message_hashes": 0}
    assert broker_is_empty(result)


def test_check_broker_clean_reads_kombus_lists_and_unacked_structures_for_the_stock_transport():
    """Stock strands a message in `unacked`, not in a `queue:` zset, so scanning only the plus key
    shapes reported a settled broker while messages were still held.
    """
    client = _stock_broker_client(
        lists={b"celery": 2, b"celery\x06\x169": 1, b"celery\x06\x166": 0},
        unacked=4,
        unacked_index=4,
    )

    result = check_broker_clean(client, "stock")

    assert result == {
        "queues": {"celery": 2, "celery\x06\x169": 1},
        "indices": {"unacked_index": 4},
        "message_hashes": 4,
    }
    assert not broker_is_empty(result)


def test_check_broker_clean_flags_a_stock_broker_holding_only_unacked_messages():
    """The interesting stock leak: every queue list is drained but the safety structures are not."""
    client = _stock_broker_client(lists={b"celery": 0}, unacked=3, unacked_index=3)

    result = check_broker_clean(client, "stock")

    assert result == {"queues": {}, "indices": {"unacked_index": 3}, "message_hashes": 3}
    assert not broker_is_empty(result)


def _ids(count, prefix="task"):
    return [f"{prefix}-{index}" for index in range(count)]


def _drain_ledger(*, submitted_ids, executed_ids):
    """Ledger backed by real id sets, so SCARD and SDIFFSTORE agree with one another."""
    sets = {"submitted_ids": set(submitted_ids), "executed_ids": set(executed_ids)}
    ledger = MagicMock()
    ledger.scard.side_effect = lambda key: len(sets[key])
    ledger.sdiffstore.side_effect = lambda dest, keys: len(sets[keys[0]] - sets[keys[1]])  # noqa: ARG005
    return ledger


_BROKER_EMPTY = {"queues": {}, "indices": {}, "message_hashes": 0}
_BROKER_DIRTY = {"queues": {}, "indices": {}, "message_hashes": 4}


def test_drain_returns_true_when_the_ledger_caught_up_and_the_broker_is_empty():
    from battle.orchestrator import cli

    ledger = _drain_ledger(submitted_ids=_ids(10), executed_ids=_ids(10))

    with patch.object(cli, "check_broker_clean", return_value=_BROKER_EMPTY):
        assert cli._drain(make_config("smoke"), ledger, MagicMock()) is True


def test_drain_waits_for_the_broker_even_once_every_execution_is_ledgered():
    """The ledger is written before the acks_late ack, so the leak check that follows `_drain`
    could otherwise trip on keys that were still on their way out.
    """
    from battle.orchestrator import cli

    ledger = _drain_ledger(submitted_ids=_ids(10), executed_ids=_ids(10))

    with (
        patch.object(cli, "check_broker_clean", return_value=_BROKER_DIRTY) as mock_check,
        patch.object(cli, "time") as mock_time,
    ):
        mock_time.monotonic.side_effect = [0.0, 1.0, 1e9]
        result = cli._drain(make_config("smoke"), ledger, MagicMock())

    assert result is False
    assert mock_check.call_count == 1


def test_drain_returns_false_on_timeout_when_executions_never_catch_up():
    from battle.orchestrator import cli

    ledger = _drain_ledger(submitted_ids=_ids(10), executed_ids=_ids(3))

    with (
        patch.object(cli, "check_broker_clean") as mock_check,
        patch.object(cli, "time") as mock_time,
    ):
        mock_time.monotonic.side_effect = [0.0, 1.0, 1e9]
        result = cli._drain(make_config("smoke"), ledger, MagicMock())

    assert result is False
    mock_check.assert_not_called()


def test_drain_is_not_fooled_by_an_execution_that_was_never_submitted():
    """The producer ledgers a submission only after a successful send, so a ledger blip leaves
    an id in executed_ids and not in submitted_ids. Comparing cardinalities let that ghost mask
    a real task still queued, which the stock baseline then reported as loss.
    """
    from battle.orchestrator import cli

    config = make_config("smoke", transport="stock")
    ledger = _drain_ledger(submitted_ids=_ids(100), executed_ids=[*_ids(99), "ghost"])

    with (
        patch.object(cli, "check_broker_clean") as mock_check,
        patch.object(cli, "time") as mock_time,
    ):
        mock_time.monotonic.side_effect = [0.0, 1.0, 1e9]
        result = cli._drain(config, ledger, MagicMock())

    assert result is False
    mock_check.assert_not_called()


def test_drain_gates_the_stock_transport_on_its_own_broker_keys():
    """A stock run that reads "done" in the ledger can still be holding messages in `unacked`,
    which is exactly the state the loss measurement has to tell apart from a destroyed message.
    """
    from battle.orchestrator import cli

    config = make_config("smoke", transport="stock")
    ledger = _drain_ledger(submitted_ids=_ids(10), executed_ids=_ids(10))

    with (
        patch.object(cli, "check_broker_clean", return_value=_BROKER_DIRTY) as mock_check,
        patch.object(cli, "time") as mock_time,
    ):
        mock_time.monotonic.side_effect = [0.0, 1.0, 1e9]
        result = cli._drain(config, ledger, MagicMock())

    assert result is False
    assert mock_check.call_args.args[1] == "stock"


def test_config_info_carries_the_profiles_kill_scheduling():
    from battle.orchestrator.cli import _config_info

    assert _config_info(make_config("chaos"))["kill_interval"] == 7.0
    smoke = _config_info(make_config("smoke"))
    assert smoke["kill_interval"] is None
    assert smoke["kill_schedule"] == ((25.0, "warm"), (50.0, "hard"))


def test_config_info_records_the_sizing_knobs():
    """An A/B pair sized differently is not a comparison, so the scorecard has to carry the sizing."""
    from battle.orchestrator.cli import _config_info

    info = _config_info(make_config("chaos"))
    assert info["workers"] == PROFILES["chaos"].workers
    assert info["concurrency"] == PROFILES["chaos"].concurrency
    assert info["prefetch"] == PROFILES["chaos"].prefetch
    assert info["pool"] == PROFILES["chaos"].pool


def test_collect_signals_snapshots_current_values_without_accumulating():
    from battle.orchestrator.cli import _collect_signals

    signals = RunSignals()
    producer = MagicMock(errors=1)
    chaos = MagicMock(errors=2, unexpected_deaths=["battle-worker-1"], restart_failures=["battle-worker-2"])

    _collect_signals(signals, producer, chaos)
    _collect_signals(signals, producer, chaos)

    assert signals.producer_errors == 1
    assert signals.chaos_errors == 2
    assert signals.unexpected_deaths == ["battle-worker-1"]
    assert signals.restart_failures == ["battle-worker-2"]


def test_parse_mem_units():
    from battle.orchestrator.sampler import parse_mem

    assert parse_mem("512B") == 512
    assert parse_mem("1KiB") == 1024
    assert parse_mem("12.5MiB") == int(12.5 * 1024 * 1024)
    assert parse_mem("1.2GiB") == int(1.2 * 1024 * 1024 * 1024)


def test_summarize_samples(tmp_path):
    import json as json_mod

    from battle.orchestrator.sampler import summarize_samples

    rows = [
        {"t": 0.0, "mem": {"battle-worker-1": 100}, "redis_mem": 50, "executed": 0},
        {"t": 30.0, "mem": {"battle-worker-1": 120}, "redis_mem": 60, "executed": 900},
        {"t": 60.0, "mem": {"battle-worker-1": 110}, "redis_mem": 55, "executed": 1800},
    ]
    path = tmp_path / "soak.jsonl"
    path.write_text("\n".join(json_mod.dumps(r) for r in rows) + "\n")
    summary = summarize_samples(path)
    assert summary["samples"] == 3
    assert summary["skipped"] == 0
    assert summary["mem_start"]["battle-worker-1"] == 100
    assert summary["mem_end"]["battle-worker-1"] == 110
    assert summary["mem_max"]["battle-worker-1"] == 120
    assert summary["throughput_per_interval"]["mean"] == pytest.approx(900.0)


def test_summarize_samples_empty_file_reports_zero_samples(tmp_path):
    from battle.orchestrator.sampler import summarize_samples

    path = tmp_path / "empty.jsonl"
    path.write_text("")
    assert summarize_samples(path) == {"samples": 0, "skipped": 0}


def test_summarize_samples_skips_unparseable_trailing_line(tmp_path):
    import json as json_mod

    from battle.orchestrator.sampler import summarize_samples

    rows = [
        {"t": 0.0, "mem": {"battle-worker-1": 100}, "redis_mem": 50, "executed": 0},
        {"t": 30.0, "mem": {"battle-worker-1": 120}, "redis_mem": 60, "executed": 900},
    ]
    path = tmp_path / "soak.jsonl"
    text = "\n".join(json_mod.dumps(r) for r in rows) + "\n"
    text += '{"t": 60.0, "mem": {"battle-worker-1": 1'  # truncated trailing line
    path.write_text(text)

    summary = summarize_samples(path)

    assert summary["samples"] == 2
    assert summary["skipped"] == 1
    assert summary["mem_end"]["battle-worker-1"] == 120


def test_parse_mem_rejects_garbage():
    from battle.orchestrator.sampler import parse_mem

    with pytest.raises(ValueError, match="unparseable"):
        parse_mem("lots")


def test_container_memory_filters_workers_and_skips_unparseable():
    from battle.orchestrator.sampler import _container_memory

    mock_result = MagicMock(returncode=0)
    mock_result.stdout = (
        "battle-worker-1\t12.3MiB / 256MiB\nbattle-ledger\t5MiB / 100MiB\nbattle-worker-2\tbadvalue / 256MiB\n"
    )
    with patch("battle.orchestrator.sampler.docker", return_value=mock_result) as mock_docker:
        memory = _container_memory()

    assert memory == {"battle-worker-1": int(12.3 * 1024 * 1024)}
    mock_docker.assert_called_once_with(
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}\t{{.MemUsage}}",
        check=False,
        capture=True,
        timeout=30,
    )


def test_container_memory_raises_on_nonzero_returncode():
    from battle.orchestrator.sampler import _container_memory

    mock_result = MagicMock(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon")
    with (
        patch("battle.orchestrator.sampler.docker", return_value=mock_result),
        pytest.raises(RuntimeError, match="docker stats failed"),
    ):
        _container_memory()


def test_container_memory_raises_when_no_worker_rows():
    from battle.orchestrator.sampler import _container_memory

    mock_result = MagicMock(returncode=0, stdout="battle-ledger\t5MiB / 100MiB\n")
    with (
        patch("battle.orchestrator.sampler.docker", return_value=mock_result),
        pytest.raises(RuntimeError, match="no battle-worker"),
    ):
        _container_memory()


def test_sampler_sample_collects_depths_and_counts():
    from pathlib import Path

    from battle.orchestrator.sampler import Sampler

    config = make_config("soak")
    broker = MagicMock()
    broker.scan_iter.side_effect = lambda match, count: {  # noqa: ARG005
        "queue:*": [b"queue:a", b"queue:b"],
        "messages_index:*": [b"messages_index:a"],
        "message:*": [b"message:1", b"message:2", b"message:3"],
    }[match]
    broker.zcard.side_effect = lambda key: {b"queue:a": 3, b"queue:b": 2, b"messages_index:a": 4}[key]
    broker.info.return_value = {"used_memory": 999}
    broker.dbsize.return_value = 42
    ledger = MagicMock()
    ledger.scard.return_value = 7
    sampler = Sampler(config, broker, ledger, Path("unused"), threading.Event())

    with patch("battle.orchestrator.sampler._container_memory", return_value={"battle-worker-1": 100}):
        row = sampler._sample()

    assert row["queue_depth"] == 5
    assert row["index_depth"] == 4
    assert row["message_keys"] == 3
    assert row["redis_mem"] == 999
    assert row["dbsize"] == 42
    assert row["executed"] == 7
    assert row["mem"] == {"battle-worker-1": 100}


def test_sample_and_write_survives_exception_and_counts_error():
    from pathlib import Path

    from battle.orchestrator.sampler import Sampler

    config = make_config("soak")
    sampler = Sampler(config, MagicMock(), MagicMock(), Path("unused"), threading.Event())
    out = MagicMock()

    with patch.object(sampler, "_sample", side_effect=RuntimeError("redis down")):
        sampler._sample_and_write(out)

    assert sampler.errors == 1
    out.write.assert_not_called()


def test_sample_and_write_counts_docker_error_but_keeps_redis_side_metrics():
    import json
    from pathlib import Path

    from battle.orchestrator.sampler import Sampler

    config = make_config("soak")
    broker = MagicMock()
    broker.scan_iter.return_value = []
    broker.info.return_value = {"used_memory": 1}
    broker.dbsize.return_value = 0
    ledger = MagicMock()
    ledger.scard.return_value = 0
    sampler = Sampler(config, broker, ledger, Path("unused"), threading.Event())
    out = MagicMock()

    mock_result = MagicMock(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon")
    with patch("battle.orchestrator.sampler.docker", return_value=mock_result):
        sampler._sample_and_write(out)

    assert sampler.errors == 1
    row = json.loads(out.write.call_args[0][0])
    assert row["mem"] == {}
    assert row["redis_mem"] == 1
    assert "queue_depth" in row


def test_run_writes_one_line_per_interval_until_stopped(tmp_path):
    import json as json_mod

    from battle.orchestrator.sampler import Sampler

    config = make_config("soak")
    stop_event = MagicMock()
    stop_event.wait.side_effect = [False, False, True]
    out_path = tmp_path / "soak.jsonl"
    sampler = Sampler(config, MagicMock(), MagicMock(), out_path, stop_event)

    with patch.object(sampler, "_sample", return_value={"t": 1.0, "mem": {}, "executed": 0}):
        sampler.run()

    lines = out_path.read_text().splitlines()
    assert len(lines) == 2
    assert sampler.errors == 0
    for line in lines:
        row = json_mod.loads(line)  # a row that serialized to garbage must fail this, not just len()
        assert row == {"t": 1.0, "mem": {}, "executed": 0}


def test_run_survives_unopenable_path_without_raising():
    from battle.orchestrator.sampler import Sampler

    config = make_config("soak")
    bad_path = MagicMock()
    bad_path.open.side_effect = OSError("disk full")
    sampler = Sampler(config, MagicMock(), MagicMock(), bad_path, threading.Event())

    sampler.run()

    assert sampler.errors == 1


def test_finish_sampler_noop_when_sampling_was_disabled():
    from battle.orchestrator.cli import _finish_sampler

    scorecard: dict = {}
    _finish_sampler(None, None, scorecard)
    assert "soak" not in scorecard


def test_finish_sampler_attaches_summary_on_success(tmp_path):
    from battle.orchestrator.cli import _finish_sampler

    soak_path = tmp_path / "soak.jsonl"
    soak_path.write_text('{"t": 0.0, "mem": {}, "redis_mem": 1, "executed": 0}\n')
    scorecard: dict = {}

    _finish_sampler(MagicMock(), soak_path, scorecard)

    assert scorecard["soak"]["samples"] == 1


def test_finish_sampler_survives_missing_soak_file_and_still_annotates_scorecard(tmp_path):
    from battle.orchestrator.cli import _finish_sampler

    sampler = MagicMock()
    sampler.errors = 7
    soak_path = tmp_path / "never-created.jsonl"
    scorecard: dict = {"verdict": {"mode": "enforced"}}

    _finish_sampler(sampler, soak_path, scorecard)

    sampler.join.assert_called_once_with(timeout=10.0)
    assert scorecard["soak"]["samples"] == 0
    assert "error" in scorecard["soak"]
    assert scorecard["soak"]["skipped"] == 0
    assert scorecard["soak"]["errors"] == 7
    assert scorecard["verdict"]["mode"] == "enforced"


def test_start_sampler_returns_none_when_sampling_disabled():
    from battle.orchestrator.cli import _start_sampler

    config = make_config()  # smoke profile: sample_interval=None
    stop = threading.Event()

    with patch("battle.orchestrator.cli.Sampler") as mock_sampler_cls:
        result = _start_sampler(config, stop, broker_client=MagicMock(), ledger_client=MagicMock())

    assert result == (None, None)
    mock_sampler_cls.assert_not_called()


def test_start_sampler_swallows_mkdir_failure_and_returns_none_none():
    from battle.orchestrator.cli import _start_sampler

    config = make_config("soak")
    stop = threading.Event()
    broken_results_dir = MagicMock()
    broken_results_dir.mkdir.side_effect = OSError("disk full")

    with patch("battle.orchestrator.cli.RESULTS_DIR", broken_results_dir):
        result = _start_sampler(config, stop, broker_client=MagicMock(), ledger_client=MagicMock())

    assert result == (None, None)


def test_start_sampler_swallows_thread_start_failure_and_returns_none_none(tmp_path):
    from battle.orchestrator.cli import _start_sampler

    config = make_config("soak")
    stop = threading.Event()

    with (
        patch("battle.orchestrator.cli.RESULTS_DIR", tmp_path),
        patch("battle.orchestrator.cli.Sampler") as mock_sampler_cls,
    ):
        mock_sampler_cls.return_value.start.side_effect = RuntimeError("can't start new thread")
        result = _start_sampler(config, stop, broker_client=MagicMock(), ledger_client=MagicMock())

    assert result == (None, None)


@contextlib.contextmanager
def _stubbed_lifecycle(cli, *, chaos_alive=False):
    """Stubs everything `_run_lifecycle` reaches out to, leaving only its own sequencing."""
    with (
        patch.object(cli, "_wait_for_workers") as wait_for_workers,
        patch("battle.battle_app.app.create_app") as create_app,
        patch.object(cli, "Producer") as producer,
        patch.object(cli, "ChaosMonkey") as chaos,
        patch.object(cli, "_start_sampler", return_value=(None, None)),
        patch.object(cli, "_drain", return_value=True),
        patch.object(cli, "_run_verification", return_value={}),
    ):
        producer.return_value.is_alive.return_value = False
        chaos.return_value.is_alive.return_value = chaos_alive
        yield SimpleNamespace(
            wait_for_workers=wait_for_workers,
            create_app=create_app,
            producer=producer,
            chaos=chaos,
        )


def _lifecycle(cli, signals=None, stop=None):
    return cli._run_lifecycle(
        make_config("smoke"),
        stop if stop is not None else threading.Event(),
        signals if signals is not None else RunSignals(),
        ledger_client=MagicMock(),
        broker_client=MagicMock(),
    )


def test_run_lifecycle_flushes_the_broker_before_anything_builds_a_producer_app():
    """The flush wipes `_kombu.binding.celery`. A Celery app that had already declared the binding
    does not redeclare it, and kombu then drops every publish with no warning and no exception.
    """
    from battle.orchestrator import cli

    order: list[str] = []
    ledger_client = MagicMock()
    broker_client = MagicMock()
    broker_client.flushall.side_effect = lambda: order.append("flush")

    with _stubbed_lifecycle(cli) as stubs:
        stubs.wait_for_workers.side_effect = lambda _config: order.append("wait")
        stubs.create_app.side_effect = lambda _role: order.append("create_app")
        cli._run_lifecycle(
            make_config("smoke"),
            threading.Event(),
            RunSignals(),
            ledger_client=ledger_client,
            broker_client=broker_client,
        )

    assert order == ["flush", "wait", "create_app"]
    ledger_calls = [name for name, _, _ in ledger_client.mock_calls]
    assert ledger_calls.index("flushall") < ledger_calls.index("set")


def test_run_lifecycle_flags_a_chaos_thread_that_outlived_its_join():
    """A chaos thread still killing containers during the drain leaves its in-flight kill out of
    `data.kills`, turning that kill's duplicates into unattributed ones and skewing the per-kill
    ceiling's denominator. Silently.
    """
    from battle.orchestrator import cli

    signals = RunSignals()

    with _stubbed_lifecycle(cli, chaos_alive=True):
        _lifecycle(cli, signals)

    assert signals.chaos_join_timed_out is True


def test_run_lifecycle_does_not_flag_a_chaos_thread_that_stopped_in_time():
    from battle.orchestrator import cli

    signals = RunSignals()

    with _stubbed_lifecycle(cli, chaos_alive=False) as stubs:
        _lifecycle(cli, signals)

    assert signals.chaos_join_timed_out is False
    stubs.chaos.return_value.join.assert_called_once_with(timeout=cli.CHAOS_JOIN_TIMEOUT)


def test_run_lifecycle_joins_the_chaos_thread_when_the_run_is_interrupted():
    """The chaos thread outlives a Ctrl-C, and its `docker start` would then race the teardown's
    `docker compose down`, leaving a container running outside the compose project.
    """
    from battle.orchestrator import cli

    stop = threading.Event()

    with _stubbed_lifecycle(cli) as stubs:
        chaos = stubs.chaos.return_value
        chaos.is_alive.side_effect = [True, False]
        stubs.producer.return_value.is_alive.return_value = True
        stubs.producer.return_value.join.side_effect = KeyboardInterrupt
        with pytest.raises(KeyboardInterrupt):
            _lifecycle(cli, stop=stop)

    chaos.join.assert_called_once_with(cli.CHAOS_JOIN_TIMEOUT)
    assert stop.is_set()


def test_run_lifecycle_does_not_wait_on_the_chaos_thread_twice_on_the_happy_path():
    from battle.orchestrator import cli

    with _stubbed_lifecycle(cli, chaos_alive=False) as stubs:
        _lifecycle(cli)

    assert stubs.chaos.return_value.join.call_count == 1


def test_cmd_run_keyboard_interrupt_verifies_with_the_signals_the_lifecycle_had_collected():
    """The lifecycle owns the producer and chaos threads but not `signals`, so counters it
    refreshed before the interrupt still reach verification and the saved scorecard.
    """
    from battle.orchestrator import cli

    args = cli.build_parser().parse_args(["run"])

    def fake_run_lifecycle(config, stop, signals, *, ledger_client, broker_client):
        signals.unexpected_deaths.append("battle-worker-3")
        signals.chaos_errors = 2
        raise KeyboardInterrupt

    fake_scorecard = {"unexpected_deaths": ["battle-worker-3"], "verdict": {"mode": "enforced", "passed": True}}

    with (
        patch.object(cli.compose, "up"),
        patch.object(cli.compose, "down"),
        patch.object(cli, "redis") as mock_redis,
        patch.object(cli, "_run_lifecycle", side_effect=fake_run_lifecycle),
        patch.object(cli, "_run_verification", return_value=fake_scorecard) as mock_verify,
        patch.object(cli, "print_scorecard"),
        patch.object(cli, "_save_scorecard") as mock_save,
    ):
        mock_redis.Redis.from_url.return_value = MagicMock()
        result = cli.cmd_run(args)

    signals = mock_verify.call_args.kwargs["signals"]
    assert signals.unexpected_deaths == ["battle-worker-3"]
    assert signals.chaos_errors == 2
    assert mock_save.call_args[0][0]["unexpected_deaths"] == ["battle-worker-3"]
    assert result == 0


def _stored_run_config(cli, profile="smoke"):
    """The `run_config` blob a `--keep-up` run leaves in the ledger for `battle verify`."""
    import json

    client = MagicMock()
    client.get.return_value = json.dumps(cli._config_info(make_config(profile))).encode()
    return client


def test_cmd_verify_reports_work_that_has_not_run_yet_as_pending_not_lost():
    """`battle verify` drains nothing, so a task that has not executed *yet* is pending. Passing
    drain_ok=True failed a live --keep-up stack with thousands of "lost" tasks that were queued.
    """
    import argparse

    from battle.orchestrator import cli

    with (
        patch.object(cli, "redis") as mock_redis,
        patch.object(cli, "_run_verification", return_value={"verdict": {}}) as mock_verify,
        patch.object(cli, "print_scorecard"),
        patch.object(cli, "_save_scorecard"),
    ):
        mock_redis.Redis.from_url.return_value = _stored_run_config(cli)
        result = cli.cmd_verify(argparse.Namespace(transport=None))

    assert result == 0
    assert mock_verify.call_args.kwargs["drain_ok"] is False


def test_cmd_verify_restores_the_pool_and_mix_the_run_actually_used():
    """The recomputed scorecard is the only surviving record once the stack is down, and a
    `--pool threads --no-delayed` run used to read back as prefork with countdowns in the mix.
    """
    import argparse

    from battle.orchestrator import cli

    stored = dataclasses.replace(
        make_config("chaos", pool="threads", mix={"fast": 0.65, "slow": 0.1, "cpu": 0.05}),
        drain_timeout=600.0,
    )

    with (
        patch.object(cli, "redis") as mock_redis,
        patch.object(cli, "_run_verification", return_value={"verdict": {}}),
        patch.object(cli, "print_scorecard"),
        patch.object(cli, "_save_scorecard") as mock_save,
    ):
        client = MagicMock()
        client.get.return_value = json.dumps(cli._config_info(stored)).encode()
        mock_redis.Redis.from_url.return_value = client
        cli.cmd_verify(argparse.Namespace(transport=None))

    config = mock_save.call_args.args[1]
    assert config.profile.pool == "threads"
    assert "delayed" not in config.profile.mix
    assert cli.drain_timeout(config) == 600.0


def test_cmd_verify_returns_one_when_the_ledger_holds_no_run_config():
    import argparse

    from battle.orchestrator import cli

    with patch.object(cli, "redis") as mock_redis:
        mock_redis.Redis.from_url.return_value.get.return_value = None
        result = cli.cmd_verify(argparse.Namespace(transport=None))

    assert result == 1


def test_cmd_run_saves_the_scorecard_before_printing_it():
    """`compose.down -v` in the `finally` has already destroyed the ledger redis by this point,
    so the saved JSON is the only surviving record of the run; a print that raises must not take
    it down with it.
    """
    from battle.orchestrator import cli

    args = cli.build_parser().parse_args(["run"])
    order: list[str] = []

    with (
        patch.object(cli.compose, "up"),
        patch.object(cli.compose, "down"),
        patch.object(cli, "redis"),
        patch.object(cli, "_run_lifecycle", return_value={"verdict": {"mode": "enforced", "passed": True}}),
        patch.object(cli, "print_scorecard", side_effect=lambda card: order.append("print")),  # noqa: ARG005
        patch.object(cli, "_save_scorecard", side_effect=lambda card, config: order.append("save")),  # noqa: ARG005
    ):
        result = cli.cmd_run(args)

    assert order == ["save", "print"]
    assert result == 0


def test_cmd_run_tears_down_the_stack_when_bring_up_fails_partway():
    from battle.orchestrator import cli

    args = cli.build_parser().parse_args(["run"])

    with (
        patch.object(cli.compose, "up", side_effect=RuntimeError("worker-7 failed to start")),
        patch.object(cli.compose, "down") as mock_down,
        patch.object(cli, "redis"),
        patch.object(cli, "_run_lifecycle") as mock_lifecycle,
    ):
        with pytest.raises(RuntimeError, match="worker-7"):
            cli.cmd_run(args)

        mock_down.assert_called_once()
        mock_lifecycle.assert_not_called()


def _compare_card(
    *,
    transport="plus",
    profile="smoke",
    pool="prefork",
    passed=True,
    mode=None,
    lost=None,
    pending=None,
    duplicates_unattributed=None,
    drain_ok=True,
    events=None,
):
    return {
        "config": {"transport": transport, "profile": profile, "pool": pool},
        "drain_ok": drain_ok,
        "tasks": {
            "submitted": 1799,
            "exactly_once": 1799,
            "lost": lost or [],
            "pending": pending or [],
            "duplicates_hard_kill": [],
            "duplicates_soft_kill": [],
            "duplicates_unattributed": duplicates_unattributed or [],
        },
        "latency": {
            "overall": {"p50": 0.1236, "p95": 0.456, "p99": 0.789, "max": 1.234},
            "first_exec_past_vt": 3,
        },
        "events": events
        if events is not None
        else {
            event_type: {"seen": 1799, "expected": 1799, "loss_pct": 0.0}
            for event_type in ("task-sent", "task-received", "task-started", "task-succeeded")
        },
        "verdict": {"mode": mode or ("enforced" if transport == "plus" else "report-only"), "passed": passed},
    }


def _row_values(out: str, label: str) -> list[str]:
    """The two value columns of the row whose label starts with `label`.

    Sliced by column width, not by whitespace: an event-loss cell contains a space of its own.
    """
    from battle.orchestrator.cli import _VALUE_WIDTH

    line = next(line for line in out.splitlines() if line.strip().startswith(label))
    return [line[-2 * _VALUE_WIDTH - 1 : -_VALUE_WIDTH - 1].strip(), line[-_VALUE_WIDTH:].strip()]


def test_compare_row_pads_and_right_aligns_columns():
    from battle.orchestrator.cli import _compare_row

    row = _compare_row("metric", "plus/smoke", "stock/smoke")
    assert row == "  metric                                            plus/smoke               stock/smoke"


def test_cmd_compare_prints_header_metrics_and_report_only_verdict(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(transport="plus", passed=True)))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(transport="stock", passed=None)))

    result = cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert result == 0
    assert "plus/smoke" in out
    assert "stock/smoke" in out
    assert "submitted" in out
    assert "event loss task-succeeded (%)" in out
    assert "True" in out
    assert "None" in out


def test_cmd_compare_formats_float_metrics_with_two_decimals(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card()))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert "0.12" in out
    assert "0.1236" not in out


def test_cmd_compare_reports_list_lengths_not_task_ids(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(lost=["task-1", "task-2"])))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(duplicates_unattributed=["task-3"])))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert "task-1" not in out
    assert "task-3" not in out
    assert _row_values(out, "lost") == ["2", "0"]
    assert _row_values(out, "duplicates (hard-kill)") == ["0", "0"]
    assert _row_values(out, "duplicates (soft-kill)") == ["0", "0"]
    assert _row_values(out, "duplicates (unattributed)") == ["0", "1"]


def test_cmd_compare_puts_a_capped_scorecard_beside_a_pre_cap_one(tmp_path, capsys):
    """Buckets now arrive as counts; scorecards recorded before the cap stored id lists. Both
    have to render the same number, or an A/B against a recorded run reads as all dashes.
    """
    import argparse
    import json

    from battle.orchestrator import cli

    capped = _compare_card()
    capped["tasks"].update({"lost": 4000, "lost_sample": _ids(ID_SAMPLE_LIMIT, "lost")})
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(capped))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(lost=["task-1", "task-2"])))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))

    assert _row_values(capsys.readouterr().out, "lost") == ["4000", "2"]


def test_cmd_compare_surfaces_pending_and_drain_ok_after_timeout(tmp_path, capsys):
    """A timed-out drain must not render identically to a clean run (Important finding 1)."""
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(lost=[], pending=["task-1"] * 500, drain_ok=False)))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert _row_values(out, "lost") == ["0", "0"]
    assert _row_values(out, "pending") == ["500", "0"]
    assert _row_values(out, "drain ok") == ["False", "True"]


def test_cmd_compare_verdict_mode_row_distinguishes_enforced_from_report_only(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(transport="plus", mode="enforced", passed=True)))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(transport="stock", mode="report-only", passed=None)))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert _row_values(out, "verdict mode") == ["enforced", "report-only"]


def test_cmd_compare_pool_row_reflects_configured_pool(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(pool="prefork")))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(pool="threads")))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert _row_values(out, "pool") == ["prefork", "threads"]


def test_cmd_compare_event_loss_shows_dash_when_nothing_measured(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    unmeasured = {
        event_type: {"seen": 0, "expected": 0, "loss_pct": 0.0}
        for event_type in ("task-sent", "task-received", "task-started", "task-succeeded")
    }
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(events=unmeasured)))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert _row_values(out, "event loss task-succeeded") == ["-", "1799/1799 (0.00%)"]


def test_cmd_compare_keeps_a_full_size_event_loss_row_inside_its_column(tmp_path, capsys):
    """`seen/expected (loss%)` is the widest cell the table renders. At the chaos profile's task
    count it overflowed the value column and shunted every event row out of alignment.
    """
    import argparse
    import json

    from battle.orchestrator import cli

    at_full_scale = {
        event_type: {"seen": 2_969_999, "expected": 2_970_000, "loss_pct": 0.0}
        for event_type in ("task-sent", "task-received", "task-started", "task-succeeded")
    }
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(_compare_card(events=at_full_scale)))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card(events=at_full_scale)))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert len({len(line) for line in out.splitlines()}) == 1


def test_cmd_compare_separates_worker_side_failures_from_lost_tasks(tmp_path, capsys):
    """The A/B table is the branch's headline deliverable, so it must not roll a worker-side
    task failure into the `lost` column that indicts the transport.
    """
    import argparse
    import json

    from battle.orchestrator import cli

    card_a = _compare_card()
    card_a["tasks"]["failed"] = ["blew-up", "blew-up-too"]
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(card_a))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert _row_values(out, "failed (worker-side)") == ["2", "-"]
    assert _row_values(out, "lost") == ["0", "0"]


def test_cmd_compare_missing_scorecard_file_returns_one(tmp_path, capsys):
    import argparse
    import json

    from battle.orchestrator import cli

    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    result = cli.cmd_compare(
        argparse.Namespace(scorecard_a=str(tmp_path / "missing.json"), scorecard_b=str(path_b)),
    )
    out = capsys.readouterr().out

    assert result == 1
    assert "missing.json" in out


@pytest.mark.parametrize(
    "contents",
    [
        '{"sampled_at": 1}\n{"sampled_at": 2}\n',
        '{"sampled_at": 1}\n',
    ],
    ids=["multi-line", "single-line-parses-as-valid-json"],
)
def test_cmd_compare_non_scorecard_json_returns_one(tmp_path, capsys, contents):
    """battle/results/ also holds *-soak.jsonl sample files; picking one must fail cleanly."""
    import argparse
    import json

    from battle.orchestrator import cli

    path_a = tmp_path / "20260101-plus-smoke-soak.jsonl"
    path_a.write_text(contents)
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    result = cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert result == 1
    assert "soak.jsonl" in out


def test_cmd_compare_missing_key_renders_dash_and_completes_table(tmp_path, capsys):
    """A card missing a key must render `-` for that row and still print every later row."""
    import argparse
    import json

    from battle.orchestrator import cli

    card_a = _compare_card()
    del card_a["latency"]
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(card_a))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    result = cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert result == 0
    assert _row_values(out, "latency p50 (s)") == ["-", "0.12"]
    assert _row_values(out, "first exec past VT") == ["-", "3"]
    assert "verdict mode" in out  # table reached its last row instead of aborting mid-print


def test_cmd_compare_renders_dash_for_a_pre_split_two_bucket_scorecard(tmp_path, capsys):
    """battle/results/ already holds runs saved before the hard/soft/unattributed split (still
    keyed by duplicates_expected/duplicates_unexpected); comparing against one of those must not
    raise a KeyError, and the new rows have nothing to show for that side.
    """
    import argparse
    import json

    from battle.orchestrator import cli

    old_style = _compare_card()
    old_style["tasks"] = {
        "submitted": 1799,
        "exactly_once": 1799,
        "lost": [],
        "pending": [],
        "duplicates_expected": [],
        "duplicates_unexpected": ["task-3"],
    }
    path_a = tmp_path / "a.json"
    path_a.write_text(json.dumps(old_style))
    path_b = tmp_path / "b.json"
    path_b.write_text(json.dumps(_compare_card()))

    result = cli.cmd_compare(argparse.Namespace(scorecard_a=str(path_a), scorecard_b=str(path_b)))
    out = capsys.readouterr().out

    assert result == 0
    assert _row_values(out, "duplicates (hard-kill)") == ["-", "0"]
    assert _row_values(out, "duplicates (soft-kill)") == ["-", "0"]
    assert _row_values(out, "duplicates (unattributed)") == ["-", "0"]


def test_cmd_down_removes_the_stack_without_any_stored_run_state():
    """`battle down` is the escape hatch after a run that left no `run_config` behind, so it has
    to resolve a config of its own rather than read one.
    """
    import argparse

    from battle.orchestrator import cli

    with patch.object(cli.compose, "down") as mock_down:
        result = cli.cmd_down(argparse.Namespace())

    assert result == 0
    assert isinstance(mock_down.call_args.args[0], RunConfig)


@pytest.mark.parametrize(
    ("argv", "handler"),
    [
        (["run"], "cmd_run"),
        (["verify"], "cmd_verify"),
        (["compare", "a.json", "b.json"], "cmd_compare"),
        (["down"], "cmd_down"),
    ],
)
def test_main_dispatches_every_subcommand_and_returns_its_exit_code(argv, handler):
    from battle.orchestrator import cli

    with patch.object(cli, handler, return_value=7) as mock_handler:
        result = cli.main(argv)

    assert result == 7
    assert mock_handler.call_args.args[0].command == argv[0]


def test_main_rejects_an_argv_with_no_subcommand():
    from battle.orchestrator import cli

    with pytest.raises(SystemExit):
        cli.main([])
