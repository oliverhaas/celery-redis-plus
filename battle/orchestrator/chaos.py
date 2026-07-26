from __future__ import annotations

import json
import subprocess
import threading
import time
from typing import TYPE_CHECKING

from battle.orchestrator.compose import docker, running_workers, worker_names

if TYPE_CHECKING:
    import random

    import redis

    from battle.orchestrator.profiles import RunConfig

RESTART_VERIFY_ATTEMPTS = 5  # bounded polls to confirm a restarted container actually came back up
RESTART_VERIFY_INTERVAL = 1.0  # seconds between restart-verification polls
SIGKILL_EXIT_CODE = 137  # 128 + SIGKILL(9): what docker records for a container it had to kill


def container_exit_code(name: str) -> int | None:
    """The exit code docker recorded for a stopped container, or None when it cannot say."""
    result = docker("inspect", "-f", "{{.State.ExitCode}}", name, check=False, capture=True)
    if result.returncode != 0:
        return None
    try:
        return int(result.stdout.strip())
    except ValueError:
        return None


def pick_mode(rng: random.Random, weights: dict[str, float]) -> str:
    roll = rng.random() * sum(weights.values())
    acc = 0.0
    for mode, weight in weights.items():
        acc += weight
        if roll <= acc:
            return mode
    return next(reversed(weights))


class ChaosMonkey(threading.Thread):
    """Kills worker containers on a seeded schedule and records a timeline in the ledger."""

    def __init__(
        self,
        config: RunConfig,
        ledger_client: redis.Redis,
        rng: random.Random,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="battle-chaos", daemon=True)
        self.config = config
        self.ledger = ledger_client
        self.rng = rng
        self.stop_event = stop_event
        self.timeline: list[dict] = []
        self.unexpected_deaths: list[str] = []
        self.restart_failures: list[str] = []
        self.errors = 0
        # Cleared once observed running again, so a later death for it counts as genuinely new.
        self._flagged_dead: set[str] = set()

    def run(self) -> None:
        profile = self.config.profile
        start = time.monotonic()
        schedule = list(profile.kill_schedule)
        next_interval_kill: float | None = None
        if profile.kill_interval is not None:
            next_interval_kill = start + profile.kill_interval * self.rng.uniform(0.5, 1.5)
        while not self.stop_event.is_set():
            next_interval_kill = self._run_iteration(start, schedule, next_interval_kill)
            self.stop_event.wait(1.0)

    def _run_iteration(
        self,
        start: float,
        schedule: list[tuple[float, str]],
        next_interval_kill: float | None,
    ) -> float | None:
        """Runs one poll/kill decision. Any unexpected error is counted and swallowed here so a
        transient docker/redis hiccup does not take down the whole chaos thread for the rest of the run.
        """
        profile = self.config.profile
        try:
            now = time.monotonic()
            self._check_unexpected_deaths()
            if schedule and now - start >= schedule[0][0]:
                _, mode = schedule.pop(0)
                self._kill(mode)
            elif next_interval_kill is not None and now >= next_interval_kill:
                self._kill(pick_mode(self.rng, self.config.profile.kill_weights))
                next_interval_kill = time.monotonic() + profile.kill_interval * self.rng.uniform(0.5, 1.5)  # ty: ignore[unsupported-operator]
        except Exception as exc:
            print(f"chaos: iteration failed (continuing): {exc}", flush=True)
            self.errors += 1
        return next_interval_kill

    def _check_unexpected_deaths(self) -> None:
        missing = set(worker_names(self.config)) - running_workers(self.config)
        # Containers that came back running are no longer "flagged"; a later death for them is new.
        self._flagged_dead &= missing
        new_deaths = missing - self._flagged_dead
        for name in sorted(new_deaths):
            print(f"chaos: UNEXPECTED DEATH of {name}; restarting it", flush=True)
            self.unexpected_deaths.append(name)
            self._flagged_dead.add(name)
            if self._restart_and_verify(name):
                print(f"chaos: {name} recovered after unexpected death", flush=True)
            else:
                print(f"chaos: RESTART FAILED for {name} after unexpected death", flush=True)
                self.restart_failures.append(name)

    def _restart_and_verify(self, name: str) -> bool:
        """Issues `docker start` and polls (bounded) for the container to actually come back up."""
        docker("start", name, check=False)
        for attempt in range(RESTART_VERIFY_ATTEMPTS):
            if name in running_workers(self.config):
                return True
            if attempt < RESTART_VERIFY_ATTEMPTS - 1:
                # Not stop_event.wait: `stop` is set when the producer finishes, which would
                # collapse these polls and fail a container that is merely still booting.
                time.sleep(RESTART_VERIFY_INTERVAL)
        return False

    def _was_sigkilled(self, name: str, *, believed: bool) -> bool:
        """Whether the container actually died by SIGKILL, from the only thing that observed it.

        `believed` is what the kill path did, kept as the fallback so an unanswerable docker
        neither crashes the chaos thread nor quietly rewrites the timeline.
        """
        exit_code = container_exit_code(name)
        if exit_code is None:
            self.errors += 1
            print(f"chaos: cannot read exit code of {name}; recording sigkilled={believed}", flush=True)
            return believed
        return exit_code == SIGKILL_EXIT_CODE

    def _kill(self, mode: str) -> None:
        profile = self.config.profile
        name = self.rng.choice(worker_names(self.config))
        sigkilled = False
        t_kill = time.time()
        if mode == "hard":
            docker("kill", "-s", "KILL", name, check=False)
            sigkilled = True
        elif mode == "cold":
            docker("kill", "-s", "QUIT", name, check=False)
            try:
                docker("wait", name, timeout=60)
            except subprocess.TimeoutExpired:
                docker("kill", "-s", "KILL", name, check=False)
                sigkilled = True
        elif mode == "warm":
            docker("kill", "-s", "TERM", name, check=False)
            try:
                docker("wait", name, timeout=profile.warm_timeout)
            except subprocess.TimeoutExpired:
                print(
                    f"chaos: warm shutdown of {name} exceeded {profile.warm_timeout}s; escalating to SIGKILL",
                    flush=True,
                )
                docker("kill", "-s", "KILL", name, check=False)
                sigkilled = True
        elif mode == "grace":
            stop_started = time.monotonic()
            docker("stop", "-t", str(profile.grace_timeout), name, check=False)
            # Fallback only, for an unreadable exit code below: a stop that ran the full grace
            # period probably ended in the SIGKILL.
            sigkilled = time.monotonic() - stop_started >= profile.grace_timeout - 0.5
        self.stop_event.wait(profile.kill_downtime)
        # After the downtime (the container has exited for every mode) and before the restart,
        # which resets the exit code to 0.
        sigkilled = self._was_sigkilled(name, believed=sigkilled)
        restarted = self._restart_and_verify(name)
        t_restarted = time.time()
        entry = {
            "mode": mode,
            "container": name,
            "t_kill": t_kill,
            "t_restarted": t_restarted,
            "sigkilled": sigkilled,
            "restarted": restarted,
        }
        self.timeline.append(entry)
        self.ledger.rpush("kills", json.dumps(entry))
        if not restarted:
            # Already-known-dead, so the next poll does not re-report this as an unexpected death.
            self._flagged_dead.add(name)
            print(f"chaos: RESTART FAILED for {name} after {mode} kill", flush=True)
            self.restart_failures.append(name)
        print(
            f"chaos: {mode} kill of {name} (down {t_restarted - t_kill:.1f}s, sigkilled={sigkilled})",
            flush=True,
        )
