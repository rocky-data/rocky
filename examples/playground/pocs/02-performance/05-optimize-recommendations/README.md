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
- `rocky compact <catalog.schema.table>` — Generates `OPTIMIZE`/`VACUUM`
  SQL for table compaction. It takes a fully qualified table, not a bare
  model name. This is a **warehouse-only** maintenance op. See the note
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
  maintenance operation. `run.sh` calls it with the bare name `events`, so
  the engine refuses before it looks at the dialect:

  ```
  Error: refusing to plan a compaction of 'events': `rocky compact` takes a fully qualified table (catalog.schema.table). A bare name cannot say whether it means a physical table or a model, and this plan regenerates OPTIMIZE/VACUUM against whatever it names
  ```

  With a fully qualified table, the **DuckDB** dialect guard rejects it
  (with or without `--dry-run`):

  ```
  $ rocky -c rocky.toml compact poc.staging__events.events
  Error: failed to generate compaction SQL for 'poc.staging__events.events'

  Caused by:
      maintenance operation 'OPTIMIZE' is not supported for dialect 'duckdb'
  ```

  `run.sh` captures the error into `expected/compact.json` and continues.
  Compaction runs for real against Databricks, not DuckDB.

## Run

```bash
./run.sh
```

## Expected output

`run.sh` prints, in order:

- `rocky validate` JSON (config valid),
- `run` log lines building `poc.staging__events.events`,
- `=== rocky optimize ===` followed by a `recommendations` array,
- `=== rocky profile-storage ===` followed by a `profile_sql` string and a
  per-column `recommendations` array,
- `=== rocky compact events (expected to fail) ===` followed by the
  bare-name refusal above,
- `POC complete: optimize + profile-storage emitted JSON; compact refused the
  bare table name (it needs catalog.schema.table).`

Full JSON for each command is captured under `expected/`.
