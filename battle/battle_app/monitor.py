from __future__ import annotations

import os
import time

import redis

from . import ledger
from .app import create_app


def main() -> None:
    app = create_app("monitor")
    client = redis.Redis.from_url(os.environ["BATTLE_LEDGER_URL"])
    print("monitor: starting event capture", flush=True)
    while True:
        try:
            with app.connection() as connection:
                receiver = app.events.Receiver(
                    connection,
                    handlers={"*": lambda event: ledger.record_event(client, event)},
                )
                receiver.capture(limit=None, timeout=None, wakeup=False)
        except Exception as exc:
            print(f"monitor: capture failed ({exc!r}); reconnecting in 1s", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
