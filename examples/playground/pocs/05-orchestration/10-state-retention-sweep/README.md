# 10-state-retention-sweep — bound `state.redb` history growth

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `[state.retention]`, `rocky state retention sweep`,
> end-of-run auto-sweep

## What it shows

Rocky tracks every run in `state.redb`: `run_history`, `dag_snapshots`,
and `quality_history` grow monotonically until something prunes them.
The `[state.retention]` config block governs that prune, with two
delivery paths exercised by this POC:

1. **Manual** — `rocky state retention sweep` (with `--dry-run` to plan
   without deleting) explicitly prunes the three history domains.
2. **Automatic** — `rocky run` end-of-run auto-sweep, throttled by
   `sweep_interval_seconds` so a project that runs every minute doesn't
   pay the sweep cost on every invocation.

Operational tables (schema cache, watermarks, partition records,
branches, idempotency keys, grace periods, run progress, check history)
are **never** swept regardless of configuration.

## Why it's distinctive

- **No external cron.** A long-running project is a single config block
  away from a self-bounded state store; auto-sweep amortizes the cost
  end-of-run.
- **Dry-run plan equals apply count.** Both paths share the same planner,
  so the dry-run JSON's `runs_deleted` is exactly what the apply path
  will remove (modulo concurrent writers).
- **Operational tables are explicitly off-limits.** Retention only ever
  touches history domains; the schema cache and watermark map stay
  intact, so the next run isn't paying a cold-cache penalty.

## Layout

```
.
├── README.md         this file
├── rocky.toml        [state.retention] knobs + DuckDB pipeline
├── run.sh            manual + auto-sweep walkthrough
└── data/seed.sql     raw__orders.orders bootstrap
```

## Prerequisites

- `rocky` ≥ 1.23.0 on PATH (`rocky state retention sweep` landed in 1.22.0,
  end-of-run auto-sweep followed in 1.23.0)
- `duckdb` CLI for seeding (`brew install duckdb`)
- `python3` for the small JSON probes in `run.sh`

## Run

```bash
./run.sh
```

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
    after run 5: history rows = 1
    after run 6: history rows = 1
```

## What happened

1. **Three runs with auto-sweep parked** — `run.sh` copies `rocky.toml`
   into `rocky.no-auto.toml` with `sweep_interval_seconds = 3600` so the
   run loop's auto-sweep skips silently. After three replication runs,
   `run_history` carries three rows.
2. **Dry-run** — `rocky state retention sweep --dry-run` reports the
   plan (`runs_deleted = 2`, `runs_kept = 1`) without touching the
   state store. The probe confirms the table is still at 3.
3. **Apply** — `rocky state retention sweep` runs the same plan for
   real and drops the table to `min_runs_kept = 1`.
4. **Auto-sweep on `rocky run`** — Switching back to the live
   `rocky.toml` (`sweep_interval_seconds = 0`), three further runs each
   trigger an end-of-run sweep. `run_history` stays flat at 1.

## Config knobs

```toml
[state.retention]
max_age_days = 365            # default
min_runs_kept = 100           # default — the N most recent successful runs
                              # are preserved unconditionally
applies_to = ["history", "lineage", "audit"]  # domains to sweep
sweep_interval_seconds = 3600 # default — minimum gap between end-of-run
                              # auto-sweeps; 0 means "every run"
sweep_budget_ms = 5000        # soft budget; exceeding flips the per-run
                              # log line from debug to warn
```

Set `applies_to = []` to disable auto-sweep without removing the manual
subcommand.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/state.rs`,
  `engine/crates/rocky-core/src/retention.rs`
- Sibling POC: [`04-checkpoint-resume`](../04-checkpoint-resume/) —
  state-store usage for resumable runs; this POC is the operational
  housekeeping counterpart.
