# 05-optimize-recommendations — Cost-based materialization recommendations

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky optimize`, `rocky profile-storage`, `rocky compact`

## What it shows

Three administrative commands that recommend optimizations after building
some run history:

- `rocky optimize` — Reads execution history from the state store and
  recommends materialization strategy changes (e.g., "this view is queried
  100x/day; promote to a table").
- `rocky profile-storage <model>` — Suggests column encodings + compression.
- `rocky compact <model>` — Generates `OPTIMIZE`/`VACUUM` SQL for table
  compaction. This is a **warehouse-only** maintenance op — see the note
  under Status.

## Why it's distinctive

- **Auto-tuning suggestions** based on actual execution history. dbt has no
  equivalent built into core.

## Layout

```
README.md      # this file
rocky.toml     # DuckDB replication pipeline + [cost] block
run.sh         # runs validate + run, then the three admin commands
data/seed.sql  # raw__events.events — 200 rows
expected/      # captured JSON (gitignored): run/optimize/profile/compact
```

## Status

- `rocky optimize` and `rocky profile-storage` are implemented, parse
  `rocky.toml` + state, and **run without error** against any populated
  state store, including the local one. With a nearly-empty state store the
  recommendations are minimal, so the POC just documents the commands and
  their JSON shape.
- `rocky compact` generates `OPTIMIZE`/`VACUUM` SQL, which is a warehouse
  maintenance operation. On the **DuckDB** dialect the engine's dialect guard
  rejects it (regardless of `--dry-run`):

  ```
  Error: failed to generate compaction SQL for 'events'
  Caused by:
      maintenance operation 'OPTIMIZE' is not supported for dialect 'duckdb'
  ```

  `run.sh` runs the `compact` step to demonstrate this guard (it captures the
  error into `expected/compact.json` and continues). Compaction is exercised
  for real against Databricks/Snowflake, not DuckDB.

## Run

```bash
./run.sh
```

## Expected output

`run.sh` prints, in order:

- the seed row count (`200`),
- `rocky validate` JSON (config valid),
- `run` log lines building `poc.staging__events.events`,
- `=== rocky optimize ===` followed by a `recommendations` array,
- `=== rocky profile-storage ===` followed by a `profile_sql` string and a
  per-column `recommendations` array,
- `=== rocky compact events (expected to fail on DuckDB) ===` followed by the
  dialect-guard error above,
- `POC complete: optimize + profile-storage emitted JSON; compact hit the
  DuckDB dialect guard (warehouse-only).`

Full JSON for each command is captured under `expected/`.
