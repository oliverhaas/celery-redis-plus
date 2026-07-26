from __future__ import annotations

from .app import create_app
from .tasks import register_tasks

app = create_app("worker")
register_tasks(app)
