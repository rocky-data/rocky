# Release Smoke Test Checklist

Manual pre-release verification for the Rocky CLI. Run through this checklist
with trial warehouse credentials before tagging any `engine-v*` release.

**Target time:** ~30 minutes for a full pass (DuckDB + all GA warehouses).

---

## Prerequisites

- [ ] Rocky binary built from the release candidate commit:
  ```bash
  cd engine && cargo build --release
  export PATH="$PWD/target/release:$PATH"
  rocky --version   # confirm expected version
  ```
- [ ] DuckDB CLI installed (for seeding the playground database)
- [ ] Environment variables set for each warehouse you plan to test:

  | Warehouse | Required env vars |
  |---|---|
  | Databricks | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` (or `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` for OAuth M2M) |
  | Snowflake | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` (or `SNOWFLAKE_PRIVATE_KEY_PATH` for key-pair JWT) |
  | Fivetran | `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, `FIVETRAN_DESTINATION_ID` |
  | BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON) |

- [ ] A scratch catalog/schema you can write to in each warehouse (tests create
  and drop tables)

---

## 1. Local / DuckDB (no credentials needed)

Baseline sanity check using the playground example. Every release must pass this
section.

### 1.1 Validate config

```bash
cd examples/playground/pocs/00-foundations/00-playground-default
rocky validate -c rocky.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON output contains `"valid": true` |
| **FAIL** | Non-zero exit; `"valid": false` with `errors` array |
| **Time** | <5 s |

### 1.2 Compile models

```bash
rocky compile --models models/
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON shows all models compiled, `diagnostics` is empty or warnings only |
| **FAIL** | Non-zero exit; `diagnostics` contains errors (severity `error`) |
| **Time** | <5 s |

### 1.3 Run local tests (DuckDB in-memory)

```bash
rocky test --models models/
```

| | |
|---|---|
| **PASS** | Exit code 0; `tests_passed > 0`, `tests_failed == 0` |
| **FAIL** | Non-zero exit; `tests_failed > 0` with assertion details |
| **Time** | <10 s |

### 1.4 CI pipeline (compile + test)

```bash
rocky ci --models models/
```

| | |
|---|---|
| **PASS** | Exit code 0; both compile and test phases succeed |
| **FAIL** | Non-zero exit; check which phase failed in the output |
| **Time** | <10 s |

### 1.5 Seed the DuckDB database

```bash
duckdb playground.duckdb < data/seed.sql
```

| | |
|---|---|
| **PASS** | Exit code 0; no SQL errors |
| **FAIL** | SQL syntax or file-not-found errors |
| **Time** | <5 s |

### 1.6 Discover sources

```bash
rocky -c rocky.toml discover
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON `sources` array is non-empty; tables listed under each source |
| **FAIL** | Non-zero exit; empty sources; connection error to DuckDB file |
| **Time** | <5 s |

### 1.7 Plan (dry-run)

```bash
rocky -c rocky.toml plan --filter source=demo
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON contains generated SQL statements; no execution errors |
| **FAIL** | Non-zero exit; filter matches nothing (check `source` component name in seed data) |
| **Time** | <5 s |

### 1.8 Run pipeline

```bash
rocky -c rocky.toml run --filter source=demo
```

| | |
|---|---|
| **PASS** | Exit code 0; `materializations` array shows tables with `status: "ok"` |
| **FAIL** | Non-zero exit; materializations with `status: "error"` |
| **Time** | <10 s |

### 1.9 State (watermarks)

```bash
rocky -c rocky.toml state
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON contains watermark entries from the run in 1.8 |
| **FAIL** | Non-zero exit; empty watermarks (state file not found or not written) |
| **Time** | <5 s |

### 1.10 Doctor

```bash
rocky doctor -c rocky.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; `overall` is `"healthy"` or `"warning"`; no `"critical"` checks |
| **FAIL** | Non-zero exit; `overall` is `"critical"` |
| **Time** | <5 s |

### 1.11 Lineage

```bash
rocky lineage <model_name> --models models/
```

Replace `<model_name>` with a model from the playground that has upstream
dependencies.

| | |
|---|---|
| **PASS** | Exit code 0; JSON shows `upstream` and `downstream` nodes |
| **FAIL** | Non-zero exit; model not found |
| **Time** | <5 s |

### 1.12 Snapshot (SCD2)

Use the dedicated snapshot POC:

```bash
cd examples/playground/pocs/01-quality/05-snapshot-scd2
duckdb poc.duckdb < data/seed.sql
rocky snapshot -c rocky.toml --dry-run
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON shows `steps` with generated MERGE SQL, all `status: "dry_run"` |
| **FAIL** | Non-zero exit; pipeline type mismatch error |
| **Time** | <5 s |

### 1.13 Seed (CSV loading)

If the playground POC has a `seeds/` directory with CSV files:

```bash
rocky seed -c rocky.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; `tables_loaded > 0`, `tables_failed == 0` |
| **FAIL** | Non-zero exit; CSV parse errors or DDL execution failures |
| **Time** | <10 s |

---

## 2. Databricks (GA)

Requires `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and auth credentials.
Use a dedicated test `rocky.toml` that points at your scratch catalog/schema.

### 2.1 Discover

```bash
rocky -c rocky-databricks.toml discover
```

| | |
|---|---|
| **PASS** | Exit code 0; sources listed with tables from the Fivetran or manual source |
| **FAIL** | 401/403 auth error; timeout; empty sources when connectors exist |
| **Time** | <30 s |

### 2.2 Plan

```bash
rocky -c rocky-databricks.toml plan --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; JSON shows SQL statements for the filtered source |
| **FAIL** | Filter matches nothing; SQL generation error |
| **Time** | <15 s |

### 2.3 Run

```bash
rocky -c rocky-databricks.toml run --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; materializations succeed; drift detection reports clean or safely-widened columns |
| **FAIL** | SQL execution errors; auth failures; `error_rate_abort_pct` threshold exceeded |
| **Time** | 1-5 min (depends on table count and warehouse size) |

### 2.4 Seed

```bash
rocky seed -c rocky-databricks.toml --seeds seeds/
```

| | |
|---|---|
| **PASS** | Exit code 0; tables created in the target catalog/schema with correct row counts |
| **FAIL** | DDL errors; INSERT failures; type inference mismatches |
| **Time** | <30 s |

### 2.5 Snapshot

Requires a `type = "snapshot"` pipeline in the config with Databricks adapter.

```bash
rocky snapshot -c rocky-databricks.toml --dry-run
```

| | |
|---|---|
| **PASS** | Exit code 0; generated MERGE SQL uses Databricks dialect (Delta MERGE syntax) |
| **FAIL** | Pipeline type mismatch; missing unique_key or updated_at |
| **Time** | <10 s |

### 2.6 Doctor

```bash
rocky doctor -c rocky-databricks.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; adapter check shows healthy connection to Databricks |
| **FAIL** | Adapter check fails with connection or auth errors |
| **Time** | <15 s |

### 2.7 State

```bash
rocky -c rocky-databricks.toml state
```

| | |
|---|---|
| **PASS** | Exit code 0; watermarks from run in 2.3 are visible |
| **FAIL** | Empty state (state file path mismatch) |
| **Time** | <5 s |

---

## 3. Snowflake (GA)

Requires `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and one of the auth methods
(password, key-pair, or OAuth).

### 3.1 Discover

```bash
rocky -c rocky-snowflake.toml discover
```

| | |
|---|---|
| **PASS** | Exit code 0; sources and tables enumerated |
| **FAIL** | Auth failure (check account identifier format); network timeout |
| **Time** | <30 s |

### 3.2 Plan

```bash
rocky -c rocky-snowflake.toml plan --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; SQL uses Snowflake dialect (quoted identifiers, Snowflake types) |
| **FAIL** | Dialect mismatch; filter matches nothing |
| **Time** | <15 s |

### 3.3 Run

```bash
rocky -c rocky-snowflake.toml run --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; materializations succeed |
| **FAIL** | SQL execution errors; warehouse suspended (auto-resume may take time); auth token expired |
| **Time** | 1-5 min |

### 3.4 Seed

```bash
rocky seed -c rocky-snowflake.toml --seeds seeds/
```

| | |
|---|---|
| **PASS** | Exit code 0; tables created with correct row counts |
| **FAIL** | DDL errors; Snowflake type mapping issues |
| **Time** | <30 s |

### 3.5 Snapshot

```bash
rocky snapshot -c rocky-snowflake.toml --dry-run
```

| | |
|---|---|
| **PASS** | Exit code 0; MERGE SQL uses Snowflake syntax |
| **FAIL** | Pipeline type mismatch |
| **Time** | <10 s |

### 3.6 Doctor

```bash
rocky doctor -c rocky-snowflake.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; healthy adapter connection |
| **FAIL** | Connection or auth errors |
| **Time** | <15 s |

---

## 4. Fivetran (connector integration)

Fivetran is a source adapter, not a warehouse. It integrates with Databricks or
Snowflake as the target warehouse. These tests verify connector discovery and
sync detection.

Requires `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, and `FIVETRAN_DESTINATION_ID`.

### 4.1 Discover (Fivetran source)

```bash
rocky -c rocky-fivetran.toml discover
```

| | |
|---|---|
| **PASS** | Exit code 0; `sources` array contains connectors with `source_type: "fivetran"`; each source lists tables and `last_sync_at` timestamps |
| **FAIL** | 401 auth error (check API key/secret); empty sources (wrong destination ID) |
| **Time** | <30 s |

### 4.2 Plan (Fivetran -> warehouse)

```bash
rocky -c rocky-fivetran.toml plan --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; SQL references source tables discovered from Fivetran schemas |
| **FAIL** | Schema pattern mismatch; filter matches nothing |
| **Time** | <15 s |

### 4.3 Run (Fivetran -> warehouse)

```bash
rocky -c rocky-fivetran.toml run --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; tables replicated from Fivetran-synced source schemas to target |
| **FAIL** | Source tables not yet synced by Fivetran; warehouse auth errors |
| **Time** | 1-5 min |

---

## 5. BigQuery (experimental)

**Status: experimental.** BigQuery adapter is under active development. Failures
in this section do not block a release but should be documented.

Requires `GOOGLE_APPLICATION_CREDENTIALS` pointing to a service account JSON
with BigQuery read/write access.

### 5.1 Discover

```bash
rocky -c rocky-bigquery.toml discover
```

| | |
|---|---|
| **PASS** | Exit code 0; datasets and tables listed |
| **FAIL** | Auth error; API not enabled; empty results |
| **Time** | <30 s |

### 5.2 Plan

```bash
rocky -c rocky-bigquery.toml plan --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; SQL uses BigQuery dialect |
| **FAIL** | Dialect errors; unsupported features |
| **Time** | <15 s |

### 5.3 Run

```bash
rocky -c rocky-bigquery.toml run --filter <key>=<value>
```

| | |
|---|---|
| **PASS** | Exit code 0; materializations succeed |
| **FAIL** | Quota errors; type mapping issues; experimental feature gaps |
| **Time** | 1-5 min |

### 5.4 Doctor

```bash
rocky doctor -c rocky-bigquery.toml
```

| | |
|---|---|
| **PASS** | Exit code 0; adapter connection healthy |
| **FAIL** | Auth or connectivity errors |
| **Time** | <15 s |

---

## 6. Cross-cutting checks

These verify features that apply across all adapters.

### 6.1 JSON output parseable

For every command tested above, verify the JSON output is valid:

```bash
rocky -c rocky.toml discover --output json | python3 -c "import sys,json; json.load(sys.stdin)"
```

| | |
|---|---|
| **PASS** | No Python error; valid JSON |
| **FAIL** | `json.decoder.JSONDecodeError` — indicates tracing output leaking into stdout |
| **Time** | <5 s per command |

### 6.2 Table output mode

Spot-check that `--output table` renders without panics:

```bash
rocky -c rocky.toml discover --output table
rocky -c rocky.toml state --output table
rocky doctor -c rocky.toml --output table
```

| | |
|---|---|
| **PASS** | Human-readable table printed to stdout; no panics or stack traces |
| **FAIL** | Panic; formatting error; garbled output |
| **Time** | <5 s per command |

### 6.3 Version and help

```bash
rocky --version
rocky --help
rocky run --help
```

| | |
|---|---|
| **PASS** | Version matches the release candidate; help text renders cleanly |
| **FAIL** | Wrong version; help text truncated or missing subcommands |
| **Time** | <5 s |

### 6.4 Exit codes

Verify non-zero exits for expected failures:

```bash
rocky validate -c /nonexistent/rocky.toml; echo "exit: $?"
rocky run -c rocky.toml --filter source=NONEXISTENT; echo "exit: $?"
```

| | |
|---|---|
| **PASS** | Non-zero exit codes (1) for both commands |
| **FAIL** | Exit code 0 on known-bad input |
| **Time** | <5 s |

---

## Sign-off

| Section | Status | Tester | Notes |
|---|---|---|---|
| 1. DuckDB (local) | | | |
| 2. Databricks | | | |
| 3. Snowflake | | | |
| 4. Fivetran | | | |
| 5. BigQuery (experimental) | | | |
| 6. Cross-cutting | | | |

**Release candidate version:** _______________

**Date:** _______________

**Verdict:** [ ] GO / [ ] NO-GO

If NO-GO, list blocking issues:

1. _______________
2. _______________
3. _______________
