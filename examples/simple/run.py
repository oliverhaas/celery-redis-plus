"""Send example tasks and print results.

Start the worker first:
    celery -A celeryapp worker --loglevel=info

Then run this script:
    python run.py
"""

from datetime import UTC, datetime, timedelta

from tasks import add, flaky_task, multiply

TIMEOUT = 30


def main():
    print("=" * 60)
    print("celery-redis-plus example")
    print("=" * 60)

    # --- Basic task ---
    print("\n1) Basic task: add(2, 3)")
    result = add.delay(2, 3)
    print(f"   result = {result.get(timeout=TIMEOUT)}")

    # --- Delayed task (countdown) ---
    print("\n2) Delayed task: add(10, 20) with countdown=3s")
    result = add.apply_async(args=(10, 20), countdown=3)
    print("   waiting for delayed result...")
    print(f"   result = {result.get(timeout=TIMEOUT)}")

    # --- Delayed task (eta) ---
    eta = datetime.now(tz=UTC) + timedelta(seconds=3)
    print(f"\n3) ETA task: multiply(6, 7) with eta={eta.isoformat()}")
    result = multiply.apply_async(args=(6, 7), eta=eta)
    print("   waiting for eta result...")
    print(f"   result = {result.get(timeout=TIMEOUT)}")

    # --- Priority tasks ---
    print("\n4) Priority tasks: three add() calls with different priorities")
    low = add.apply_async(args=(1, 1), priority=0)
    mid = add.apply_async(args=(2, 2), priority=5)
    high = add.apply_async(args=(3, 3), priority=9)
    print(f"   low  (priority=0): {low.get(timeout=TIMEOUT)}")
    print(f"   mid  (priority=5): {mid.get(timeout=TIMEOUT)}")
    print(f"   high (priority=9): {high.get(timeout=TIMEOUT)}")

    # --- Retry task ---
    print("\n5) Flaky task (retries up to 3 times):")
    result = flaky_task.delay()
    print(f"   result = {result.get(timeout=TIMEOUT)}")

    print("\n" + "=" * 60)
    print("All tasks completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
