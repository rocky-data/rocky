# 05-optimize-recommendations — Cost-based materialization recommendations

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky optimize`, `rocky profile-storage`, `rocky compact`

## What it shows

Three administrative commands, run after building some history (200 rows in `raw__events.events`):

- `rocky optimize`: recommends materialization strategy changes from execution history in the state store.
- `rocky profile-storage <model>`: suggests column encodings and compression.
- `rocky compact <catalog.schema.table>`: generates `OPTIMIZE`/`VACUUM` SQL for compaction. **Warehouse-only.**

## Status

- `optimize` and `profile-storage` run without error against any populated state store, including the local one. With a nearly-empty store the recommendations are minimal.
- `run.sh` calls `compact` with the bare name `events`, so the engine refuses before it looks at the dialect:

  ```
  Error: refusing to plan a compaction of 'events': `rocky compact` takes a fully qualified table (catalog.schema.table). A bare name cannot say whether it means a physical table or a model, and this plan regenerates OPTIMIZE/VACUUM against whatever it names
  ```

  With a fully qualified table, the **DuckDB** dialect guard rejects it (with or without `--dry-run`):

  ```
  $ rocky -c rocky.toml compact poc.staging__events.events
  Error: failed to generate compaction SQL for 'poc.staging__events.events'

  Caused by:
      maintenance operation 'OPTIMIZE' is not supported for dialect 'duckdb'
  ```

  `run.sh` captures the error into `expected/compact.json` and continues. Compaction runs for real against Databricks, not DuckDB.

## Run

```bash
./run.sh
```

## Expected output

`validate` JSON, `run` log lines building `poc.staging__events.events`, an `optimize` `recommendations` array, a `profile-storage` `profile_sql` string with per-column `recommendations`, the bare-name refusal above, then:

`POC complete: optimize + profile-storage emitted JSON; compact refused the bare table name (it needs catalog.schema.table).`

Full JSON for each command is under `expected/`.
