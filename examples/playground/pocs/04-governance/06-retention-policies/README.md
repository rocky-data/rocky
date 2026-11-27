# 06-retention-policies — declarative data-retention policies

> **Category:** 04-governance
> **Credentials:** none (DuckDB primary path; Databricks / Snowflake
> narrative-only in the "for real" section below)
> **Runtime:** < 5s
> **Rocky features:** `retention = "<N>[dy]"` sidecar key,
> `RetentionPolicy`, `rocky retention-status` (`--model`, `--drift`),
> `GovernanceAdapter::apply_retention_policy`

## What it shows

A model sidecar can declare `retention = "<N>[dy]"`, parsed at project-load time
into `RetentionPolicy { duration_days: u32 }`. The grammar is strict: `30d` and
`1y` pass; `-3d`, `90` and `abc` are rejected (in `rocky validate` and the LSP).
Years are a flat 365 days (no leap years).

| Model | Sidecar `retention` | `configured_days` |
|---|---|---|
| `page_views_30d` | `"30d"` | 30 |
| `ledger_archive_1y` | `"1y"` | 365 |
| `ephemeral_summary` | *(absent)* | `null` |

After a successful DAG, `rocky run` calls
`GovernanceAdapter::apply_retention_policy` on every model with a policy.
`rocky retention-status` is a static report (no warehouse probe without
`--drift`).

## DuckDB behaviour

On DuckDB the apply is **silently skipped** (`NoopGovernanceAdapter` returns
`Ok(())`: no warning, no error). `run.sh` tolerates a non-zero exit from
`rocky run` with `|| true` (DuckDB replication exits non-zero on "no discovery
adapter").

## Run

```bash
cd examples/playground/pocs/04-governance/06-retention-policies
./run.sh
```

It validates, runs, then calls `rocky retention-status` four ways (table, JSON,
`--model ledger_archive_1y`, `--drift`).

## Expected output (abridged)

```
--- rocky retention-status -o table ---
MODEL                                    CONFIGURED       WAREHOUSE        IN SYNC
----------------------------------------------------------------------------------
ephemeral_summary                        -                -                yes
ledger_archive_1y                        365 days         -                no
page_views_30d                           30 days          -                no
```

Unset `configured_days` and `warehouse_days` are omitted from JSON, not emitted
as `null`. Treat missing keys as `null`.

`in_sync` compares the two. Without `--drift`, `warehouse_days` is always
absent, so only unconfigured models are `in_sync`. `--drift` filters to models
with a policy and probes the warehouse: Databricks and Snowflake read the real
value. DuckDB and BigQuery return nothing, so values match the non-drift report.

## Apply retention for real on Databricks / Snowflake

Swap the `[adapter]` block to Databricks or Snowflake:

- **Databricks (Delta).** Sets both `delta.logRetentionDuration` and
  `delta.deletedFileRetentionDuration` TBLPROPERTIES in one `ALTER TABLE`.
- **Snowflake.** Emits
  `ALTER TABLE <db>.<schema>.<table> SET DATA_RETENTION_TIME_IN_DAYS = <N>`.
  Snowflake enforces its edition cap (Standard 90, Enterprise 365) server-side,
  and Rocky surfaces any rejection.
- **BigQuery / DuckDB.** No first-class retention knob. On DuckDB the apply is a
  silent no-op. An adapter that keeps the trait default returns a "not
  supported" error, logged as a warning.

Apply is best-effort: failures emit `warn!` and the pipeline continues.
