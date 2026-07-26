# Battle run results

One row per run, appended by `battle.orchestrator.summarize`. Trimmed to the runs that
carry a conclusion; superseded, smoke-sized, and event-only runs were dropped along with
their scorecards. Stock-transport and early-ack runs are report-only, never pass/fail.

Both `lost` and `pending` count a submitted task with no execution recorded; the split is
whether anything could still redeliver it. A drain that completed, or one that timed out
against an empty broker, files them under `lost`; a broker still holding messages, or an
early-ack run where an empty broker proves nothing, files them under `pending`. Every row
here was scored with the transport-aware broker scan, so a stock drain timeout against an
empty broker credits `lost`.

The four rows are one 90-minute `chaos` run per transport on each pool, all at 16 workers
x 16 concurrency with `--seed 42`, plus the flags below.

| row | what it establishes | added flags |
|---|---|---|
| 013617 | plus absorbs 320 kills on threads with nothing lost | `--pool threads --no-delayed --drain-timeout 600` |
| 115927 | stock loses 27 of 2.97M on threads, 0.203 per hard SIGKILL | `--transport stock --pool threads --no-delayed --drain-timeout 600` |
| 172344 | plus absorbs 290 kills on prefork with nothing lost | (none) |
| 131910 | stock loses 17 of 2.97M on prefork, 0.139 per hard SIGKILL | `--transport stock --no-delayed --drain-timeout 600` |

Three of the four exclude countdown tasks and wait six times kombu's restore sweep, so an
outstanding task at the end is a lost task rather than a slow one, and both stock runs ended
against a broker measured empty. Row 172344 predates those flags and ran the default mix
against a profile-derived drain. The 115927 stamp is a rescore under the final classifier of
the run that started at 09:41; only its `lost`/`pending` labels differ from the original.

| finished | transport | pool | profile | submitted | exactly once | lost | pending | failed | dupes h/s/u | kills | p50 | p95 | p99 | max | past VT | event loss | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 172344 | plus | prefork | chaos | 2969873 | 2969744 | **0** | 0 | 0 | 129/0/0 | 290 | 0.00s | 0.42s | 1.28s | 82.81s | 1373 | 0.000% | PASS |
| 013617 | plus | threads | chaos | 2969929 | 2969901 | **0** | 0 | 0 | 28/0/0 | 320 | 0.00s | 1.23s | 1.78s | 90.99s | 2807 | 61.354% | PASS |
| 115927 | stock | threads | chaos | 2969967 | 2969814 | **27** | 0 | 0 | 109/17/0 | 335 | 0.00s | 1.05s | 1.61s | 97.24s | 2446 | 48.934% | report-only |
| 131910 | stock | prefork | chaos | 2969952 | 2969880 | **17** | 0 | 0 | 43/0/12 | 304 | 0.00s | 1.06s | 1.62s | 68.08s | 2099 | 0.000% | report-only |
