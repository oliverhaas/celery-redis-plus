from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .profiles import RunConfig

BATTLE_DIR = Path(__file__).resolve().parent.parent
COMPOSE_FILE = BATTLE_DIR / "docker-compose.yml"


def compose(
    config: RunConfig,
    *args: str,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    env = os.environ.copy()
    env.update(config.compose_env())
    return subprocess.run(cmd, env=env, check=check, capture_output=capture, text=True)


def docker(
    *args: str,
    check: bool = True,
    # Captured by default, unlike `compose`, whose build and teardown output is worth showing:
    # every caller here is a container-lifecycle command whose echoed name is pure log noise.
    capture: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=check, capture_output=capture, text=True, timeout=timeout)


def up(config: RunConfig) -> None:
    compose(config, "up", "-d", "--build", "--scale", f"worker={config.profile.workers}")


def down(config: RunConfig) -> None:
    compose(config, "down", "-t", "5", "-v", check=False)


def worker_names(config: RunConfig) -> list[str]:
    return [f"battle-worker-{i}" for i in range(1, config.profile.workers + 1)]


def running_workers(config: RunConfig) -> set[str]:
    result = compose(config, "ps", "--format", "json", capture=True, check=False)
    names: set[str] = set()
    for line_raw in result.stdout.splitlines():
        line = line_raw.strip()
        if not line:
            continue
        item = json.loads(line)
        if item.get("Service") == "worker" and item.get("State") == "running":
            names.add(item["Name"])
    return names
