"""Example tasks demonstrating celery-redis-plus features."""

import random

from celeryapp import app


@app.task
def add(x, y):
    return x + y


@app.task(bind=True, max_retries=3)
def flaky_task(self):
    """Task that fails randomly to demonstrate retry behaviour."""
    if random.random() < 0.5:
        raise self.retry(countdown=1)
    return "succeeded after retries"


@app.task
def multiply(x, y):
    return x * y
