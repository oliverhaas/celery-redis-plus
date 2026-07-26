from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Profile:
    name: str
    duration: float  # seconds of load generation
    workers: int
    rate: float  # tasks per second
    visibility_timeout: int
    requeue_interval: int
    kill_interval: float | None  # mean seconds between kills; None = use kill_schedule only
    acks_late: bool = True
    concurrency: int = 4  # prefork children per worker container
    prefetch: int = 4  # worker_prefetch_multiplier
    # "prefork" or "threads"; only the threads pool exposes `pool.executor`, which is what
    # the transport's graceful-shutdown fix (restore_unacked_once) waits on before restoring.
    pool: str = "prefork"
    # Patch Celery's EventDispatcher so pool threads cannot lose buffered events. Off by default so
    # the harness measures stock Celery; turn it on to show the loss is the dispatcher's, not ours.
    event_patch: bool = False
    kill_schedule: tuple[tuple[float, str], ...] = ()  # explicit (offset_seconds, mode) kills
    kill_weights: dict[str, float] = field(default_factory=dict)
    kill_downtime: float = 5.0  # seconds a killed container stays down before restart
    # Ceiling on kill-attributed duplicates per kill; None disables the rule. Recorded chaos runs
    # sat at 0.61 and 0.29, and one hard kill can abandon at most `concurrency`, so 2.0 sits between.
    max_duplicates_per_kill: float | None = 2.0
    grace_timeout: int = 10  # `docker stop -t` value for grace mode
    warm_timeout: int = 120  # max seconds to wait for warm shutdown before escalating to SIGKILL
    sample_interval: float | None = None  # soak sampler period; None = disabled
    mix: dict[str, float] = field(
        default_factory=lambda: {"fast": 0.65, "delayed": 0.20, "slow": 0.10, "cpu": 0.05},
    )
    delayed_countdown: tuple[float, float] = (5.0, 30.0)
    slow_range: tuple[float, float] = (1.0, 5.0)
    cpu_range: tuple[int, int] = (20_000, 100_000)


PROFILES: dict[str, Profile] = {
    "smoke": Profile(
        name="smoke",
        duration=90.0,
        workers=2,
        rate=20.0,
        visibility_timeout=15,
        requeue_interval=5,
        kill_interval=None,
        kill_schedule=((25.0, "warm"), (50.0, "hard")),
        delayed_countdown=(5.0, 15.0),
    ),
    "chaos": Profile(
        name="chaos",
        duration=5400.0,
        # 256 slots at a weighted ~0.30s of slot-time each, less the at-most-14% of the fleet
        # parked by kill downtime plus a 10.5s boot, so a >=735/s floor on the effective ceiling.
        workers=16,
        rate=550.0,
        visibility_timeout=30,
        requeue_interval=10,
        # A floor on the gap, not a period: the chaos thread blocks through shutdown, downtime, and
        # restart before drawing the next one, so real kills land further apart and cost less.
        kill_interval=7.0,
        concurrency=16,
        kill_weights={"hard": 0.4, "cold": 0.2, "warm": 0.2, "grace": 0.2},
    ),
    "soak": Profile(
        name="soak",
        duration=4 * 3600.0,
        workers=4,
        rate=30.0,
        visibility_timeout=300,
        requeue_interval=60,
        kill_interval=300.0,
        kill_weights={"hard": 0.3, "cold": 0.2, "warm": 0.3, "grace": 0.2},
        grace_timeout=30,
        sample_interval=30.0,
    ),
}

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)([smh]?)$")
_DURATION_FACTORS = {"": 1.0, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(text: str) -> float:
    match = _DURATION_RE.match(text.strip())
    if match is None:
        raise ValueError(f"invalid duration: {text!r} (expected e.g. 90s, 15m, 2h)")
    return float(match.group(1)) * _DURATION_FACTORS[match.group(2)]


@dataclass(frozen=True)
class RunConfig:
    profile: Profile
    transport: str  # "plus" | "stock"
    broker: str  # "redis" | "valkey"
    seed: int
    broker_port: int = 6390
    ledger_port: int = 6391
    keep_up: bool = False
    # Seconds to wait for outstanding tasks after load stops; None derives it from the profile.
    drain_timeout: float | None = None

    @property
    def broker_image(self) -> str:
        return "redis:7" if self.broker == "redis" else "valkey/valkey:8"

    @property
    def host_broker_url(self) -> str:
        return f"redis://127.0.0.1:{self.broker_port}/0"

    @property
    def host_ledger_url(self) -> str:
        return f"redis://127.0.0.1:{self.ledger_port}/0"

    def compose_env(self) -> dict[str, str]:
        """Env vars consumed by docker-compose.yml variable interpolation."""
        return {
            "BATTLE_BROKER_IMAGE": self.broker_image,
            "BATTLE_BROKER_PORT": str(self.broker_port),
            "BATTLE_LEDGER_PORT": str(self.ledger_port),
            "BATTLE_TRANSPORT": self.transport,
            "BATTLE_VISIBILITY_TIMEOUT": str(self.profile.visibility_timeout),
            "BATTLE_REQUEUE_INTERVAL": str(self.profile.requeue_interval),
            "BATTLE_ACKS_LATE": "1" if self.profile.acks_late else "0",
            "BATTLE_CONCURRENCY": str(self.profile.concurrency),
            "BATTLE_PREFETCH": str(self.profile.prefetch),
            "BATTLE_POOL": self.profile.pool,
            "BATTLE_EVENT_PATCH": "1" if self.profile.event_patch else "0",
        }

    def host_env(self) -> dict[str, str]:
        """Env vars for host-side create_app() (producer role) and ledger access."""
        return {
            "BATTLE_TRANSPORT": self.transport,
            "BATTLE_BROKER_URL": self.host_broker_url,
            "BATTLE_LEDGER_URL": self.host_ledger_url,
            "BATTLE_VISIBILITY_TIMEOUT": str(self.profile.visibility_timeout),
            "BATTLE_REQUEUE_INTERVAL": str(self.profile.requeue_interval),
        }


def make_config(
    profile: str = "smoke",
    transport: str = "plus",
    broker: str = "redis",
    seed: int = 42,
    **overrides: object,
) -> RunConfig:
    base = PROFILES[profile]
    if overrides:
        base = dataclasses.replace(base, **overrides)
    return RunConfig(profile=base, transport=transport, broker=broker, seed=seed)
