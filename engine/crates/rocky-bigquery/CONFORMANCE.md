# BigQuery adapter conformance

This document maps the conformance categories in
`rocky-adapter-sdk::conformance::test_specs` to the live drivers that
exercise the BigQuery adapter against a real GCP project.

The built-in suite does little on its own. `run_conformance` executes three
dialect checks (`format_table_ref`, `watermark_where`, `row_hash`) only when
it gets a live dialect. It reports every other spec as `Skipped`.
`rocky test-adapter --adapter <name>` passes no dialect, so it skips all of
them. The real coverage comes from two places:

- the smoke drivers under
  `examples/playground/pocs/07-adapters/05-bigquery-native-queries/live/`;
- the `#[ignore]`-gated live tests in `tests/` (`batch_describe_live.rs`,
  `bisection_live.rs`, `dialect_sweep_live.rs`). They need
  `BIGQUERY_TEST_PROJECT` and GCP credentials.

## Coverage matrix

| Category | Test | Status | Receipt |
|---|---|---|---|
| Connection | `connect` | ✅ live | every smoke driver authenticates and runs at least one query |
| DDL | `create_table` | ✅ live | `live/run.sh` (full-refresh CTAS), `live/merge/run.sh` (bootstrap) |
| DDL | `drop_table` | ✅ live | `live/drift/run.sh` stage 3 (`drop_and_recreate` action) |
| DDL | `create_catalog` | ⚪ N/A | `BigQueryDialect::create_catalog_sql` returns `None`; BQ projects can't be created via SQL. Documented in adapter source. |
| DDL | `create_schema` | ✅ live | exercised inside `live/<strategy>/run.sh` cleanup (`bq rm -r -f -d`) and via `auto_create_schemas` on the replication path |
| DML | `insert_into` | ✅ live | `live/drift/run.sh` (incremental replication) |
| DML | `merge_into` | ✅ live | `live/merge/run.sh` (bootstrap + UPSERT); positive MERGE probe in `tests/dialect_sweep_live.rs` |
| Query | `describe_table` | ✅ live | `live/drift/run.sh` (existence probe + drift detection); covered by `BigQueryAdapter::describe_table` integration test |
| Query | `table_exists_true` / `table_exists_false` | ✅ live | `live/merge/run.sh` first-run bootstrap probes target absence; subsequent run probes target presence |
| Query | `execute_query` | ✅ live | every smoke driver round-trips at least one `SELECT` |
| Types | `type_string`, `type_integer`, `type_float`, `type_boolean`, `type_date`, `type_timestamp`, `type_null` | 🟡 implicit | not exercised by a dedicated type-coverage driver. The shipped smokes use STRING (`name`), INT64 (`id`, `score`), TIMESTAMP (`_updated_at`), NUMERIC (`score` after widening). FLOAT64, BOOL, DATE, and explicit NULL are not live-exercised yet. |
| Dialect | `format_table_ref` | ✅ unit + live | unit tests in `dialect.rs::tests` plus implicit coverage in every smoke (every query references three-part names) |
| Dialect | `watermark_where` | ✅ unit | unit-tested in `dialect.rs`. Not exercised by a live transformation pipeline today. |
| Dialect | `row_hash` | ✅ unit + live | `BigQueryDialect::row_hash_expr` in `dialect.rs`; `tests/bisection_live.rs` runs the checksum-bisection diff on it |
| Governance | `set_tags` | 🟡 partial — wired | `BigQueryGovernanceAdapter::set_tags` executes `ALTER SCHEMA ... SET OPTIONS(labels=[...])` for `TagTarget::Schema` and `ALTER TABLE ... SET OPTIONS(labels=[...])` for `TagTarget::Table`. `TagTarget::Catalog` (project-level labels) stays a warn-and-return because BQ projects do not support labels via SQL; that path needs the Resource Manager API. Adapter now wired through the CLI registry (was `NoopGovernanceAdapter` until the wiring landed). |
| Governance | `get_grants` | ⚪ no-op by design | same as `set_tags` — IAM grants are REST-only, not SQL-issuable. |
| BatchChecks | `batch_row_counts` / `batch_freshness` | ⚪ not implemented | `BigQueryBatchCheckAdapter` declares `supports_row_counts() == false` and `supports_freshness() == false`, so the runner never calls them and uses one query per table instead (#1719). |
| BatchChecks | `batch_describe_schema` | ✅ unit + live | unit tests in `batch.rs`; `tests/batch_describe_live.rs` checks it against `describe_table` on a real dataset. No smoke driver calls it. Not in `test_specs`. |
| Discovery | `discover` | ✅ live | `live/discover/run.sh` |

Legend: ✅ exercised live · 🟡 implicit / partial · ⚪ N/A or by design

## Open findings (documented limitations)

These limitations hold today. None breaks the workflow when the caller knows
about it.

1. **Model SQL bodies skip env-var substitution.** Sidecar TOMLs and
   `rocky.toml` resolve `${VAR}`. The `.sql` file is read raw. Engine-wide,
   not BQ-specific. Workaround: hardcode the catalog, or substitute it
   before the run (the live drivers use a `__GCP_PROJECT__` placeholder).
2. **Time-interval `time_column` must be TIMESTAMP on BigQuery.** The
   partition filter uses `'YYYY-MM-DD HH:MM:SS'` literals (the
   `SqlDialect::timestamp_literal` default, which BigQuery does not
   override). BigQuery refuses to coerce them to a DATE column.
   Workaround: model SQL uses `TIMESTAMP_TRUNC(...)`.
3. **`bytes_scanned` can read zero for a query with no source.** BigQuery
   exempts constant queries (such as the full-refresh smoke's literal
   `SELECT`) from the 10 MB minimum bill. Models that scan a source report
   non-zero values through the `jobs.get` enrichment path (PR #330).

Resolved since this file was first written: `auto_create_schemas` now
applies on transformation pipelines (#448). A `merge` model with no
`update_columns` now resolves the column set from the model's output
columns (#1007).

## Status: not experimental

`BigQueryAdapter` does not override `WarehouseAdapter::is_experimental`, so
it inherits the default (`false`). Rocky prints no experimental warning for
it. The public adapter table still lists BigQuery as Beta.

The gate to leave experimental was: "Live smokes green, or every gap is
documented in this file." Every dialect surface with a live smoke driver
passed. The remaining gaps are the limitations above.
