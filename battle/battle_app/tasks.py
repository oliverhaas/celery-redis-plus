from __future__ import annotations

import hashlib
import time
from typing import Any

from . import ledger


def register_tasks(app: Any) -> None:
    @app.task(name="battle.fast", bind=True)
    def fast(self: Any) -> None:
        started = time.time()
        ledger.record_execution(self.request.id, self.request.hostname, started)

    @app.task(name="battle.delayed", bind=True)
    def delayed(self: Any) -> None:
        started = time.time()
        ledger.record_execution(self.request.id, self.request.hostname, started)

    @app.task(name="battle.slow", bind=True)
    def slow(self: Any, seconds: float) -> None:
        started = time.time()
        time.sleep(seconds)
        ledger.record_execution(self.request.id, self.request.hostname, started)

    @app.task(name="battle.cpu", bind=True)
    def cpu(self: Any, iterations: int) -> None:
        started = time.time()
        value = b"battle"
        for _ in range(iterations):
            value = hashlib.sha256(value).digest()
        ledger.record_execution(self.request.id, self.request.hostname, started)
