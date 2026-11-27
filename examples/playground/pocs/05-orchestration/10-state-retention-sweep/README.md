# 10-state-retention-sweep — bound `state.redb` history growth

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `[state.retention]`, `rocky state retention sweep`,
> end-of-run auto-sweep

## What it shows

`run_history`, `dag_snapshots` and `quality_history` in `state.redb` grow until
pruned. `[state.retention]` controls the prune, run either by
`rocky state retention sweep` (`--dry-run` to plan only) or by an auto-sweep at
the end of `rocky run`, throttled by `sweep_interval_seconds`.

Operational tables (schema cache, watermarks, partition records, loaded files,
branches, idempotency keys, grace periods, check history) are **never** swept,
whatever the configuration says. A swept run record takes its resume checkpoint
with it, unless that checkpoint still needs watermark recovery or still owes
post-copy checks.

Both paths share one planner, so the dry run's `runs_deleted` is what apply
removes (modulo concurrent writers).

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3` for the small JSON probes in `run.sh`

## Run

```bash
./run.sh
```

It does three runs with auto-sweep parked (`sweep_interval_seconds = 3600`, in a
copied `rocky.no-auto.toml`), a dry run, a manual sweep, then three runs with the
live `rocky.toml` (`sweep_interval_seconds = 0`).

## Expected output

```text
==> Three runs with auto-sweep parked (sweep_interval_seconds = 3600)
    history rows after 3 runs: 3

==> 'rocky state retention sweep --dry-run'  (plan only — no deletes)
    plan: runs_deleted=2  runs_kept=1  domains=['history', 'lineage', 'audit']  duration_ms=0
    history rows after dry-run: 3  (unchanged — dry-run is a no-op)

==> 'rocky state retention sweep'  (apply)
    apply: runs_deleted=2  runs_kept=1  lineage_deleted=0  audit_deleted=0
    history rows after apply: 1  (down to min_runs_kept = 1)

==> Three more runs WITH auto-sweep (sweep_interval_seconds = 0)
    after run 4: history rows = 1
...
```

## Config knobs

```toml
[state.retention]
max_age_days = 365            # default
min_runs_kept = 100           # default — the N most recent rows in each
                              # domain are preserved unconditionally
applies_to = ["history", "lineage", "audit"]  # domains to sweep
sweep_interval_seconds = 3600 # default — minimum gap between end-of-run
                              # auto-sweeps; 0 means "every run"
sweep_budget_ms = 5000        # soft budget; exceeding flips the per-run
                              # log line from debug to warn
```

Set `applies_to = []` to turn the sweep off (manual and automatic) without
removing the block.
