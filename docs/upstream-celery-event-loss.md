# Upstream: Celery drops buffered task events under the threads pool

Found while battle-testing this transport. It is **not** a transport bug: stock kombu Redis and
celery-redis-plus lose the same events at the same rate, and prefork loses none.

## Which repo

**`celery/celery`**, not `kombu`. The buffer and the flush both live in
[`celery/events/dispatcher.py`](https://github.com/celery/celery/blob/main/celery/events/dispatcher.py).
Kombu only supplies the `Producer` that `_publish` calls. Nothing in kombu needs to change.

Verified against celery 5.6.3 / kombu 5.6.2. Line numbers below are from celery 5.6.3; the code has
been like this for many releases, so re-check the line numbers against `main` before you start.

## The bug

`EventDispatcher.send` appends to the shared group buffer **without holding `self.mutex`** (line 189):

```python
buf = self._group_buffer[group]
buf.append(event)
```

`EventDispatcher.flush` publishes that same list object and then clears it in place (lines 210-213):

```python
with self.mutex:
    for group, events in self._group_buffer.items():
        self._publish(events, self.producer, '%s.multi' % group)
        events[:] = []  # list.clear
```

`_publish` serializes the list and then writes it to a socket. The socket write releases the GIL, so
other threads run during it. Every event appended in that window is neither in the payload just
serialized nor left in the buffer, because `events[:] = []` clears the whole list. It is silently
destroyed.

Only threads *other than the flushing thread* can lose events, which makes the damage selective:

| event | emitted by | lost |
|---|---|---|
| `task-sent` | producer process | 0% |
| `task-received` | worker main/hub thread | 0% |
| `task-started` | pool thread | 58-70% |
| `task-succeeded` | pool thread | 18-37% |

`task-received` is emitted by the same thread that runs `flush()` (see
`celery/worker/strategy.py`), so it can never append during its own publish. Prefork is immune
because children return results up the result pipe and the parent dispatches every event from one
thread. Only the threads pool (`celery/concurrency/thread.py`, via `apply_target`, which runs
`accept_callback` and `callback` inline on the pool thread) is exposed.

The split between `task-started` and `task-succeeded` is measured but unexplained; the reproduction
below loses both at an equal rate. Do not claim a cause for it in the issue.

## Reproduction

`scratchpad/upstream_repro.py` in this session, reproduced here in full. Only needs `pip install
celery`. It uses no broker: the producer is stubbed so the test is deterministic and fast.

```python
"""Buffered task events are dropped when the worker pool is threads. Standalone: pip install celery."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from celery import Celery
from celery.events.dispatcher import EventDispatcher

TASKS, POOL, PUBLISH_SECONDS = 4000, 16, 0.0005
TYPES = ("task-received", "task-started", "task-succeeded")

sent = dict.fromkeys(TYPES, 0)
sent_lock = threading.Lock()


class CountingProducer:
    """Stands in for kombu's Producer. The sleep models a socket write, which releases the GIL."""

    def __init__(self):
        self.published = dict.fromkeys(TYPES, 0)
        self.lock = threading.Lock()

    def publish(self, body, **kwargs):
        # Order matters: kombu serializes the body (reads the list) and only then writes to the
        # socket. Anything appended during that write is not in the payload.
        batch = list(body) if isinstance(body, list) else [body]
        time.sleep(PUBLISH_SECONDS)
        with self.lock:
            for event in batch:
                self.published[event["type"]] += 1


def main():
    app = Celery(broker="memory://")
    dispatcher = EventDispatcher(app=app, connection=app.connection(), enabled=False, buffer_group=["task"])
    dispatcher.producer = CountingProducer()
    dispatcher.enabled = True

    def emit(event_type):
        with sent_lock:
            sent[event_type] += 1
        dispatcher.send(event_type, uuid="x")

    def run_task():
        emit("task-started")
        time.sleep(0.02)
        emit("task-succeeded")

    with ThreadPoolExecutor(max_workers=POOL) as executor:
        futures = []
        for _ in range(TASKS):
            emit("task-received")
            futures.append(executor.submit(run_task))
            dispatcher.flush()
        for future in futures:
            future.result()
    dispatcher.flush()

    print(f"{'event':16s} {'sent':>6s} {'published':>10s} {'lost':>6s}")
    for event_type in TYPES:
        got = dispatcher.producer.published[event_type]
        lost = sent[event_type] - got
        print(f"{event_type:16s} {sent[event_type]:6d} {got:10d} {lost:6d} ({lost / sent[event_type]:6.2%})")


if __name__ == "__main__":
    main()
```

Measured on celery 5.6.3:

```
event              sent  published   lost
task-received      4000       4000      0 ( 0.00%)
task-started       4000       2155   1845 (46.12%)
task-succeeded     4000       2076   1924 (48.10%)
```

With the fix below applied, all three report `lost 0 (0.00%)`.

**Watch the ordering in `publish`.** An earlier version of this script read `body` *after* the sleep
and reported zero loss, because the appends made during the sleep were still counted. The snapshot
has to happen before the sleep, as serialization does in reality. Getting this backwards is the
easiest way to convince yourself there is no bug.

## The fix

Two changes, both in `celery/events/dispatcher.py`.

**1. `flush()` (lines 209-213): snapshot and clear before publishing**, so an append that arrives
during the publish lands in the live list and goes out with the next flush instead of being wiped.

```diff
         if groups:
             with self.mutex:
                 for group, events in self._group_buffer.items():
-                    self._publish(events, self.producer, '%s.multi' % group)
-                    events[:] = []  # list.clear
+                    batch = events[:]
+                    events[:] = []  # list.clear
+                    self._publish(batch, self.producer, '%s.multi' % group)
```

**2. `send()` (lines 188-193): append under the mutex.** Change 1 leaves a much smaller window
between `events[:]` and `events[:] = []`; this closes it completely.

```diff
-                buf = self._group_buffer[group]
-                buf.append(event)
-                if len(buf) >= self.buffer_limit:
+                with self.mutex:
+                    buf = self._group_buffer[group]
+                    buf.append(event)
+                    buffer_full = len(buf) >= self.buffer_limit
+                if buffer_full:
                     self.flush()
                 elif self.on_send_buffered:
                     self.on_send_buffered()
```

`self.mutex` is a plain `threading.Lock` and is **not reentrant**, and `flush()` takes it. So
`self.flush()` must be called *after* the `with self.mutex:` block has exited, exactly as written
above. Getting this wrong deadlocks the worker instantly.

Keep `_publish` inside the mutex. It is not just protecting the buffer: kombu `Producer` objects are
not thread-safe, and the mutex is what serializes access to `self.producer`.

Change 1 alone removes essentially all the loss. Change 2 is what makes it airtight. Submit both;
if a reviewer objects to the extra locking in the hot path, change 1 is the fallback.

### Optional, mention but do not bundle

`flush()` has no `if events:` guard, so an idle worker publishes empty `task.multi` batches. On a
live 16-worker fleet that was 2,554 empty messages per minute. Adding `if not events: continue` is a
harmless win but it is a separate concern from the data loss; offer it as a follow-up so it cannot
stall the fix.

## Test to add

Find where the dispatcher is already tested:

```bash
grep -rl "EventDispatcher" t/unit/
```

Add a test that a concurrent append is not lost. This is deterministic, no threads or sleeps needed:
the fake producer appends *during* the publish, which is exactly the race.

```python
def test_flush_keeps_events_appended_during_publish(self):
    dispatcher = EventDispatcher(app=self.app, enabled=False, buffer_group=['task'])
    dispatcher.enabled = True
    buffer = dispatcher._group_buffer['task']
    buffer.extend(['first', 'second'])
    published = []

    def fake_publish(batch, producer, routing_key, **kwargs):
        buffer.append('appended-during-publish')
        published.append((list(batch), routing_key))

    dispatcher._publish = fake_publish
    dispatcher.flush()

    assert published == [(['first', 'second'], 'task.multi')]
    assert buffer == ['appended-during-publish']
```

Against unpatched celery this fails on the last assertion: the buffer comes back empty because the
event was destroyed. Run it before applying the fix to confirm it actually catches the bug.

## Step by step

```bash
git clone https://github.com/celery/celery.git
cd celery
python -m venv .venv && source .venv/bin/activate
pip install -e '.[test]'

git checkout -b fix-eventdispatcher-buffered-event-loss

# apply the two diffs above to celery/events/dispatcher.py, add the test, then:
grep -rl "EventDispatcher" t/unit/          # find the test module
python -m pytest t/unit/events/ -q          # adjust path to whatever the grep found

git add celery/events/dispatcher.py t/unit/events/
git commit -m "fix(events): don't drop buffered events appended during publish"
git push -u origin fix-eventdispatcher-buffered-event-loss
```

Open the PR against `main`. Celery asks contributors to add a `Fixes #<issue>` line, so file the
issue first and reference it.

## Issue draft

> **Title:** Buffered task events are silently dropped when the worker uses the threads pool
>
> `EventDispatcher.send` appends to `_group_buffer[group]` without holding `self.mutex`, while
> `EventDispatcher.flush` publishes that same list and then clears it in place with `events[:] = []`.
> `_publish` serializes the list and then writes to a socket, which releases the GIL, so any event a
> pool thread appends during that write is neither published nor retained. It is dropped silently.
>
> Only threads other than the flushing thread can lose events, so the loss is selective:
> `task-received` (emitted on the main thread) loses nothing, while `task-started` and
> `task-succeeded` (emitted on pool threads via `apply_target`) lose 18-70%. Prefork is unaffected
> because the parent dispatches all events from one thread.
>
> Measured on a 16-worker fleet under sustained load: `task-started` arrived at 42% of
> `task-received` and `task-succeeded` at 82%, confirmed by tapping the broker, so the events are
> never published rather than lost downstream. Event-driven monitoring (Flower and friends) badly
> understates started tasks and shows tasks that appear to start and never finish.
>
> **Reproduce** (no broker needed, `pip install celery`): <paste the script above>
>
> ```
> task-received      4000 sent   4000 published      0 ( 0.00%)
> task-started       4000 sent   2155 published   1845 (46.12%)
> task-succeeded     4000 sent   2076 published   1924 (48.10%)
> ```
>
> **Probable fix:** in `flush()`, snapshot and clear each group buffer before publishing it, and take
> `self.mutex` around the append in `send()`. With both applied the script reports 0 lost.
> celery 5.6.3, kombu 5.6.2, Python 3.13, Linux.

## PR draft

> **Title:** fix(events): don't drop buffered events appended during publish
>
> Fixes #<issue>.
>
> `flush()` published the live group buffer and then cleared it with `events[:] = []`. Because
> `_publish` releases the GIL on the socket write, events appended by other threads during the
> publish were neither in the payload nor left in the buffer, so they were dropped silently. Under
> the threads pool this cost 18-70% of `task-started` and `task-succeeded`; prefork was unaffected
> because it dispatches all events from one thread.
>
> - `flush()` now snapshots and clears each buffer before publishing, so a concurrent append goes out
>   with the next flush.
> - `send()` now appends under `self.mutex`, closing the remaining window. `flush()` is still called
>   outside the lock, since the mutex is not reentrant.
>
> `_publish` stays inside the mutex: it also serializes access to the kombu producer, which is not
> thread-safe.
>
> Adds a regression test where the fake producer appends during the publish; it fails on `main`.
