# battle/ (production-like battle testing)

Kills real Celery worker containers (prefork or threads) while pushing sustained load, then
verifies exactly-once execution, fanout event delivery, latency (including
visibility-timeout restore delays), and broker cleanliness against an
independent ledger Redis. Broker cleanliness is transport-aware: plus is
scanned for `queue:`, `messages_index:` and `message:` keys, stock for kombu's
queue lists and its `unacked` / `unacked_index` pair. Local-only; the live
harness never runs in CI or pytest.

## Requirements

Docker + docker compose v2, and the repo venv (`uv sync --group dev`).

## Usage

    uv run python -m battle run --profile smoke              # 90s load phase; see below
    uv run python -m battle run --profile chaos              # 90 min, kills >=7s apart
    uv run python -m battle run --profile soak --duration 4h # long-run stability
    uv run python -m battle run --profile chaos --transport stock --seed 42
    uv run python -m battle compare battle/results/A.json battle/results/B.json
    uv run python -m battle down                              # teardown

The `smoke` profile's load phase runs for 90s, but a full `run` invocation
takes several minutes end to end: `compose up --build`, then up to a 120s
worker-ready wait, the 90s load phase, and up to a 75s drain before
verification and teardown.

Flags: `--profile smoke|chaos|soak`, `--transport plus|stock`,
`--pool prefork|threads`, `--broker redis|valkey`, `--workers N`,
`--concurrency N`, `--rate T/S`, `--duration 90s|15m|2h`, `--seed N`,
`--acks-late` / `--no-acks-late`, `--keep-up`, `--dry-run`.

`--pool` matters more than its one line suggests. The transport's
graceful-shutdown fix in `restore_unacked_once` waits on `pool.executor`, and
only Celery's threads pool has one, so a prefork run does not exercise it at
all. Threads also moves the ack itself: `celery.concurrency.base.apply_target`
invokes the completion callback inline, so under threads `basic_ack` runs on
the worker thread rather than on the main process after a result pipe read.

## Sizing

Each profile fixes its own `workers`, `concurrency`, and `prefetch`, and all
three land in the scorecard, so `battle compare` shows immediately when an A/B
pair was not sized the same way.

Sustainable throughput is `workers x concurrency / weighted-slot-time`. The
shipped mix costs about 0.30s of slot-time per task, essentially all of it the
10% `slow` share, which sleeps rather than burns CPU:

| profile | slots | raw ceiling | with kills | rate |
|---|---|---|---|---|
| smoke | 2x4 = 8 | ~27/s | n/a (scheduled kills) | 20/s |
| chaos | 16x16 = 256 | ~853/s | >=735/s | 550/s |
| soak | 4x4 = 16 | ~53/s | ~53/s | 30/s |

Kills cost real capacity. A SIGKILLed worker is unavailable for
`kill_downtime` plus a full boot, measured at 10.5s to consuming again at
concurrency 12 (the Celery banner at 0.4s and forked children at 0.7s are both
too early to use as a readiness signal). Back-to-back at `chaos`'s 7s
`kill_interval` that would park 2.2 of 16 workers, a 14% capacity loss. That
is the worst case: `kill_interval` is the gap the chaos thread waits *after*
finishing a kill, so shutdown, downtime, and restart all push the real cadence
out further and park less of the fleet than the table assumes.

The binding constraint is worker-container memory, not CPU. Idle, prefork
copy-on-write puts a container at roughly `28 + 21.3 * concurrency` MiB
(measured 49 / 286 / 540 MiB at concurrency 1 / 12 / 24). Under sustained load
it runs about 20% above that formula, because each child also holds its
prefetched messages and task state: the 16x16 `chaos` fleet measured a mean of
443 MiB per container (min 378, max 563) mid-run, so 6.9 GB rather than the
5.9 GB the formula predicts. The ledger Redis reached 3.52 GB at 2.97M tasks,
or about 1.24 KiB per task. Both figures are prefork; the threads pool
(`--pool threads`) is the reason that distinction matters. The producer is
not a constraint; a single submitting thread sustains ~1400 tasks/s against a
local broker, and threading it measures *slower* because `send_task` holds the
GIL through sub-millisecond I/O.

## Kill modes

- `hard`: SIGKILL, no cleanup; recovery only via visibility timeout
- `cold`: SIGQUIT, cold shutdown; restores unacked, exits after an up-to-8s
  soft-shutdown window (`BATTLE_SOFT_SHUTDOWN_TIMEOUT`). Still fast relative
  to `warm`, but not immediate
- `warm`: SIGTERM, finishes running tasks (escalates to SIGKILL after warm_timeout)
- `grace`: `docker stop -t N`, SIGTERM then SIGKILL; models k8s grace periods

## Ack mode

Ack mode is the harness's most consequential setting. `Profile.acks_late`
defaults to `True` (override with `--acks-late` / `--no-acks-late`). With late
ack, a task killed mid-execution is redelivered after the visibility timeout
and re-executed exactly once. With early ack (`--no-acks-late`), Celery
acknowledges a message *before* the task body runs, so a task killed
mid-execution is lost by design, and no broker can recover it. Runs with
`--no-acks-late` are therefore always forced into `report-only` verdict mode
(see below); this is a documented contrast, not a bug. On the `smoke`
profile this lost 4 of 1799 tasks with `--no-acks-late` in a measured run,
versus 0 with the default; kill timing is wall-clock, so the exact count of
tasks caught mid-flight is not reproducible run to run.

## Verdict (plus transport)

Enforced only for the `plus` transport with late ack. Stock-transport runs and
early-ack runs are report-only baselines, never pass/fail. Every one of these
must hold, or the run fails:

- **Tasks**: the run submitted something at all; zero lost; zero still pending
  after the drain timeout; zero worker-side failures; no executions for task
  ids that were never submitted; no task executed before its eligibility time.
- **Duplicates**: none unattributable to a kill, *and* no more than
  `max_duplicates_per_kill` (2.0) attributed duplicates per kill.
- **Chaos**: the profile's kills actually landed; no chaos iteration errored;
  no unexpected worker deaths; every killed worker came back; and the chaos
  thread joined before its timeout. A thread that outlives the join leaves an
  in-flight kill out of the timeline, which both strands its duplicates as
  unattributed and shortens the per-kill denominator.
- **Cleanup**: the drain completed before its timeout, and broker Redis ends
  with no `queue:*`/`messages_index:*` entries and no `message:*` hashes.

The per-kill rate ceiling exists because attribution alone stopped being
selective: the excusal window is ~35s wide and chaos kills land ~19s apart, so
a systematic duplication regression could hide inside the windows. Recorded
chaos runs sat at 0.61, 0.29, and 0.45 duplicates per kill, the last over 290
kills, which is the only one of the three with a denominator worth trusting.

The event-loss column is not a fanout-reliability measurement, and the verdict
never inspects it. Nothing in the harness kills the monitor, so a killed worker
cannot cost an event. Two unrelated effects land in this column, and only one of
them is small.

Under `--pool prefork` it picks up the verifier's own `SCAN` racing the
monitor's writes: Redis promises nothing about keys added mid-iteration. The
3M-task run below missed exactly 1 `task-succeeded` of 2,969,873 across 290
kills. Had the loss been caused by the kills it would have scaled with them; at
0.4 events per 100 kills it plainly does not.

Under `--pool threads` it picks up a much larger upstream Celery bug, measured
at 70% of `task-started` and 37% of `task-succeeded`. `EventDispatcher.send`
appends to the shared group buffer without holding `self.mutex`, while
`EventDispatcher.flush` publishes that same list object and then clears it in
place with `events[:] = []`. The publish is a real socket write and releases the
GIL, so every event a pool thread appends during it is discarded unpublished. A
60s tap on the broker's fanout confirms the events never reach the wire, and a
sample of the ledger agrees with the tap to within a percentage point.

The loss is therefore selective by emitting thread, which is what identifies it.
`task-sent` (producer process) and `task-received` (worker main thread) lose
nothing, because the main thread cannot append while it is inside its own
publish. Only the two pool-thread events lose. A standalone reproduction
mimicking that thread shape, where a hub thread emits `task-received`,
dispatches to a pool, and flushes while pool threads emit the other two, loses
0.00% of `task-received` and 46-48% of both pool-thread events, so the mechanism
and the main-thread immunity both reproduce directly. The measured split between
`task-started` and `task-succeeded` does not reproduce and remains unexplained.
Prefork is
immune because its children return results up the pipe and the parent dispatches
every event from one thread.

`--event-patch` takes the mutex around the append and has `flush` snapshot the
buffer before publishing. Two matched 5-minute `plus`/threads runs differing
only in that flag put the diagnosis beyond doubt: `task-started` went from
72.625% loss to 0.000% and `task-succeeded` from 35.578% to 0.000%, with both
runs delivering every task exactly once. The patch is harness-local and exists
to attribute the loss, not to ship.

None of this is transport-attributable: stock kombu Redis reproduces it
identically, and task delivery is unaffected in both cases. It costs
observability, not work. Expect event-based dashboards on a threads-pool worker
to understate started tasks badly and to show tasks that appear to start and
never finish.

Verified live: a default (`acks_late=True`) `chaos` run submitted 2,969,873
tasks over 90 minutes at 16 workers x 16 concurrency, absorbing 290 kills (116
hard, 67 warm, 58 cold, 49 grace). Zero lost, zero pending, zero worker-side
failures, and zero duplicates that could not be attributed to a kill. The 129
duplicates that did occur were all attributed to hard kills, a rate of 0.45 per
kill against the 2.0 ceiling. 1373 first executions landed more than a
visibility timeout after eligibility, so the restore path carried real traffic
rather than being incidental. Verdict PASS.

Scorecards land in `battle/results/` (gitignored). A/B runs should use the
same `--seed` and profile. Because that directory does not survive a clean
checkout, `uv run python -m battle.orchestrator.summarize <scorecard>.json`
appends the headline numbers to the tracked `battle/RESULTS.md`.

## Measured task loss

The headline A/B. Four 90-minute `chaos` runs at 16 workers x 16 concurrency,
seed 42, one per transport on each pool. Three of them use
`--no-delayed --drain-timeout 600`: countdown tasks are excluded so nothing sits
unacked in a worker timer, and the drain window is six times kombu's restore
sweep, so an outstanding task at the end is a lost task rather than a slow one.
The plus prefork run predates those flags and ran the default mix against a
profile-derived drain.

| metric | plus threads | stock threads | plus prefork | stock prefork |
|---|---|---|---|---|
| submitted | 2,969,929 | 2,969,967 | 2,969,873 | 2,969,952 |
| **never executed** | **0** | **27** | **0** | **17** |
| drain | clean | TIMED OUT | clean | TIMED OUT |
| kills (of which hard SIGKILL) | 320 (128) | 335 (133) | 290 (116) | 304 (122) |
| **losses per hard kill** | **0.000** | **0.203** | **0.000** | **0.139** |
| duplicates, hard / soft / unattributed | 28 / 0 / 0 | 109 / 17 / 0 | 129 / 0 / 0 | 43 / 0 / 12 |
| verdict | PASS | report-only | PASS | report-only |

Stock loses roughly one task per five to seven SIGKILLs. The rate holds between
0.14 and 0.20 per hard kill across five stock runs, on both pools, across a 150s
and a 600s drain window, and with and without countdown tasks in the mix, which
is what identifies it as a fixed race window rather than an artefact of the
measurement.

Duplicate *counts* do not separate the transports, and the threads pair alone
would mislead: plus prefork produced the most of the four. Attribution does
separate them. Both plus runs left zero duplicates that could not be pinned to a
kill, while stock prefork left 12 unattributable, and stock threads duplicated
across 17 soft kills, which are shutdowns that restore their own unacked messages
and should not need a redelivery at all.

There is a window in kombu's consume path that accounts for this. `BRPOP`
removes the message from the queue, then the transport JSON-decodes it,
constructs a `Message`, and only then records it as unacked through
`QoS.append`. Between the pop and that record the message exists solely in
worker memory: it is off the broker and the broker holds no evidence it ever
existed, so a SIGKILL there destroys it and no visibility timeout can recover
it. Both pools lose, because all of those steps run on the hub thread either way.
Prefork measures somewhat lower (0.14 and 0.18) than threads (0.17 to 0.20),
which the window's position does not explain and this harness does not resolve.

`transport_consume_message.lua` closes it. The `ZPOPMIN` and the
`messages_index` rescore happen inside one script, so the message is recorded
as in-flight by the same indivisible operation that dequeues it, and its index
entry is rescored rather than removed. There is no instant at which a consumed
message is unrecoverable.

### Destroyed, not stranded

The table above establishes that stock loses tasks, not where they went. The
stock-aware broker scan answers that: on both stock runs, at the 600s timeout the
broker reads `{'queues': {}, 'indices': {}, 'message_hashes': 0}`.

Three things separate destroyed from stranded, and both pools show all three:

- **Nothing is on the broker.** Every queue list, `unacked`, and
  `unacked_index` is empty, and no `unacked` entry belongs to an
  already-executed task, so there is no in-flight ack to mistake for a leak.
  Nothing is waiting on a restore sweep because there is nothing left to
  restore.
- **Every one has `task-sent` and no `task-received`.** Celery emits
  `task-received` after `QoS.append`, so its absence places the death before
  the unacked record while the message was already off the queue. Event loss
  does not explain it. On threads, `task-received` loss ran at 0.03%, so 27
  independent misses is not a number that happens; on prefork it ran at 0.00%
  across all four event types, which leaves the 17 misses no measurement noise
  to hide in at all.
- **They cluster on the SIGKILLs.** All 27 land within 0.04s of one on threads,
  all 17 within 0.098s on prefork. Soft kills lose nothing, and 13 of 16 and 9
  of 16 containers respectively are implicated, so this is a fleet-wide race and
  not one sick worker.

The type split points at the consume path rather than the task body: fast 34,
slow 8, cpu 2 across the two runs, against an 81.5 / 12.3 / 6.2% arrival mix.
Losses track how often a task type arrives, not how long it runs.

## Measured A/B example

A second, much smaller pair, kept because it separates the transports on
latency shape rather than on correctness. One measured pair at final HEAD,
comparing this transport (`plus`) against
stock kombu Redis on the same seed 42, `smoke` profile, and kill schedule (a
warm kill at 25s, a SIGKILL at 50s):

| metric | plus | stock |
|---|---|---|
| submitted | 1799 | 1799 |
| exactly once | 1799 | 1799 |
| lost / pending / failed | 0 / 0 / 0 | 0 / 0 / 0 |
| duplicates, hard / soft / unattributed | 0 / 0 / 0 | 0 / 0 / 0 |
| latency min / p50 / p95 | 0.00s / 1.93s / 42.74s | 0.00s / 0.86s / 10.56s |
| latency p99 / max | 56.16s / 65.77s | 72.82s / 81.27s |
| first exec past VT | 292 | 71 |
| event loss, all four types | 0.00% | 0.00% |
| verdict | PASS | report-only |

Both transports delivered every task exactly once here, so this pair separates
them on latency shape rather than on correctness. Stock shows the better
median and p95; celery-redis-plus the better p99 and max. `first exec past VT`
counts first executions that landed more than one visibility timeout after
eligibility, which is a proxy for how much work went through the restore path
rather than a count of restore operations.

An earlier run of this same pair recorded 2 unattributable duplicates on the
stock side. That did not reproduce. Both tables are single runs of a
wall-clock-timed kill schedule, so neither is a statistical result, and
neither should be read as a general property of either transport. The kill
schedule is identical by construction; the kill *timing* is not, so the
measured downtimes still differ between the two sides.
