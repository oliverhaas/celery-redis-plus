# Simple Example

Minimal Celery app using the celery-redis-plus transport.

## What it demonstrates

- Basic task execution
- Delayed delivery via `countdown` and `eta`
- Task priority (0-9)
- Automatic retries on failure
- Event monitoring via Flower (exercises Redis Streams fanout)

## Setup

```bash
# 1. Start Redis
docker compose up -d

# 2. Install dependencies (from repo root)
cd ../..
uv venv && uv sync --group dev
cd examples/simple

# 3. Install Flower for event monitoring
uv pip install flower

# 4. Start the worker (with -E to enable events)
uv run celery -A celeryapp worker --loglevel=info -E

# 5. In another terminal, start Flower
uv run celery -A celeryapp flower

# 6. In another terminal, send tasks
uv run python run.py
```

## Expected output

```
============================================================
celery-redis-plus example
============================================================

1) Basic task: add(2, 3)
   result = 5

2) Delayed task: add(10, 20) with countdown=3s
   waiting for delayed result...
   result = 30

3) ETA task: multiply(6, 7) with eta=...
   waiting for eta result...
   result = 42

4) Priority tasks: three add() calls with different priorities
   low  (priority=0): 2
   mid  (priority=5): 4
   high (priority=9): 6

5) Flaky task (retries up to 3 times):
   result = succeeded after retries

============================================================
All tasks completed successfully!
============================================================
```

## Flower dashboard

Open http://localhost:5555 in your browser while running tasks. You should see:

- **Workers tab** — the worker and its status
- **Tasks tab** — each task as it's sent, received, and completed
- **Monitor tab** — live graphs of task throughput and latency

This exercises the fanout/broadcast code path (Redis Streams with XREAD) since
Celery events are delivered via fanout exchanges.

## Cleanup

```bash
docker compose down
```
