from __future__ import annotations

import threading
import time
import uuid
from typing import TYPE_CHECKING

from battle.battle_app.ledger import record_submission

if TYPE_CHECKING:
    import random

    import redis
    from celery import Celery

    from .profiles import RunConfig


def pick_type(rng: random.Random, mix: dict[str, float]) -> str:
    roll = rng.random() * sum(mix.values())
    acc = 0.0
    for task_type, weight in mix.items():
        acc += weight
        if roll <= acc:
            return task_type
    return next(reversed(mix))


class Producer(threading.Thread):
    """Submits tasks at a sustained rate and ledgers every submission."""

    def __init__(
        self,
        config: RunConfig,
        app: Celery,
        ledger_client: redis.Redis,
        rng: random.Random,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="battle-producer", daemon=True)
        self.config = config
        self.app = app
        self.ledger = ledger_client
        self.rng = rng
        self.stop_event = stop_event
        self.submitted = 0
        self.errors = 0

    def run(self) -> None:
        profile = self.config.profile
        start = time.monotonic()
        while not self.stop_event.is_set():
            elapsed = time.monotonic() - start
            if elapsed >= profile.duration:
                break
            target = int(elapsed * profile.rate)
            # Also checked here: below-`rate` throughput never lets `submitted` reach `target`,
            # so the outer duration check would never be re-evaluated and the thread never exit.
            while (
                self.submitted < target and not self.stop_event.is_set() and time.monotonic() - start < profile.duration
            ):
                self._submit_one()
            time.sleep(0.05)

    def _submit_one(self) -> None:
        try:
            profile = self.config.profile
            task_type = pick_type(self.rng, profile.mix)
            task_id = str(uuid.uuid4())
            priority = self.rng.randint(0, 9)
            countdown: float | None = None
            args: tuple[float | int, ...] = ()
            if task_type == "delayed":
                countdown = self.rng.uniform(*profile.delayed_countdown)
            elif task_type == "slow":
                args = (self.rng.uniform(*profile.slow_range),)
            elif task_type == "cpu":
                args = (self.rng.randint(*profile.cpu_range),)
            sent_at = time.time()
            self.app.send_task(
                f"battle.{task_type}",
                args=args,
                task_id=task_id,
                priority=priority,
                countdown=countdown,
            )
            # Ledger AFTER a successful send: a crash between send and ledger surfaces as an
            # "unexpected execution" in verification rather than a phantom loss.
            record_submission(self.ledger, task_id, task_type, priority, sent_at, sent_at + (countdown or 0.0))
        except Exception as exc:
            # Nothing usable came out of this iteration (mix pick, send, or ledger write failed):
            # count it and keep the producer thread alive instead of letting one bad task kill it.
            print(f"producer: submission failed, dropping this task: {exc}", flush=True)
            self.errors += 1
            time.sleep(0.5)
            return
        self.submitted += 1
