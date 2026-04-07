# 02-inline-checks — Built-in `[checks]` running inside `rocky run`

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[pipeline.NAME.checks]` row_count, column_match, freshness, null_rate

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
├── models/             single SQL replication model
└── data/seed.sql
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

## Note on current binary support

`rocky run`'s local execution path (added in rocky 0.1.x for DuckDB) does
end-to-end materialization. The full `[checks]` integration is part of the
Databricks run path; for the DuckDB path the checks block is parsed but
checks are not yet executed inline. This POC ships with the config so the
schema is documented; once check support lands in `run_local.rs` the
`check_results` array will be populated automatically.
