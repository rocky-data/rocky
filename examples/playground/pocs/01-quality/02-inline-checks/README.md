# 02-inline-checks — Built-in `[checks]` running inside `rocky run`

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[pipeline.NAME.checks]` row_count, column_match (freshness and null_rate are commented out in `rocky.toml`)

## What it shows

How to enable Rocky's built-in pipeline-level data quality checks via the
`[checks]` block in `rocky.toml`. Checks run inline during `rocky run` and
report results in the JSON output, distinct from the `rocky test` (model
assertion) flow.

## Why it's distinctive

- **Pipeline-level**, not per-model: checks live in `rocky.toml`, not in
  individual model files. Configure once, applies to every replicated table.
- Runs inline with the pipeline (no second pass).

## Layout

```
.
├── README.md
├── rocky.toml          [pipeline.poc.checks] enables row_count + column_match
├── run.sh
└── data/seed.sql       raw__events.events seed (replication is pattern-discovery based; no model files)
```

## Run

```bash
./run.sh
```

## What happened

`rocky -c rocky.toml run` discovers `raw__events.events`, replicates it to
`poc.staging__events.events`, and runs the configured checks against the
target. The JSON output's `check_results` array contains one entry per
table.

## Expected output

`rocky run`'s local (DuckDB) execution path runs the configured checks inline
and populates `check_results` in the JSON output. The run log shows the
batched-checks phase:

```
running batched checks concurrently  row_count_tables:1 freshness_tables:0
batched checks complete              tables:1
```

and `expected/run.json` carries one `check_results` entry per table:

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

Both checks pass. `row_count` compares the source and target row counts
(500 == 500). `column_match` finds no missing and no extra columns. You can
list the target columns with
`duckdb poc.duckdb "PRAGMA table_info('staging__events.events')"`.
`run.sh` does not assert the check results. It only writes them to
`expected/run.json`.
