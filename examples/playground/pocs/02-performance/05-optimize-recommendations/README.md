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
- `rocky compact --dry-run` — Generates `OPTIMIZE`/`VACUUM` SQL for table
  compaction.

## Why it's distinctive

- **Auto-tuning suggestions** based on actual execution history. dbt has no
  equivalent built into core.

## Status

These commands are implemented and parse rocky.toml + state. They work
against any populated state store, including the local one. With an
empty state store the recommendations are minimal — the POC documents
the commands and shows they run without error.

## Run

```bash
./run.sh
```
