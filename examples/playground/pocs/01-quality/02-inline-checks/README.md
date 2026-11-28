# 02-inline-checks — Built-in `[checks]` running inside `rocky run`

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[pipeline.NAME.checks]` row_count, column_match (freshness and null_rate are commented out in `rocky.toml`)

## What it shows

Pipeline-level data quality checks, configured in `[pipeline.NAME.checks]` in `rocky.toml`, run inline during `rocky run` and report in its JSON output. They are separate from `rocky test` model assertions and apply to every replicated table.

## Run

```bash
./run.sh
```

## Expected output

The run discovers `raw__events.events`, replicates it to `poc.staging__events.events` and checks the target. The log shows:

```
running batched checks concurrently  row_count_tables:1 freshness_tables:0
batched checks complete              tables:1
```

`expected/run.json` carries one `check_results` entry per table:

```json
"check_results": [
  {
    "asset_key": ["duckdb", "events", "events"],
    "checks": [
      { "name": "column_match", "passed": true, "severity": "error", "missing": [], "extra": [] },
      { "name": "row_count",    "passed": true, "severity": "error", "source_count": 500, "target_count": 500 }
    ]
  }
]
```

`row_count` compares source and target counts; `column_match` looks for missing or extra columns (list them with `duckdb poc.duckdb "PRAGMA table_info('staging__events.events')"`). `run.sh` does not assert the check results; it only writes them.
