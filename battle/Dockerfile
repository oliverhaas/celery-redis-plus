FROM python:3.13-slim

WORKDIR /app

# Install celery-redis-plus from the local source tree (build context is the repo root)
COPY pyproject.toml README.md LICENSE ./
COPY celery_redis_plus ./celery_redis_plus
RUN pip install --no-cache-dir ".[redis]"

COPY battle/battle_app ./battle_app

# Exec form is REQUIRED: celery must be PID 1 to receive SIGTERM/SIGQUIT/SIGKILL.
CMD ["celery", "-A", "battle_app.worker", "worker", "--loglevel=warning"]
