# Release Smoke Test Checklist

Manual check of the Rocky CLI before you tag an `engine-v*` release. It covers what CI cannot: live warehouses. Run it with trial credentials. A full pass takes about 30 minutes. For the tag and CI steps, see the `rocky-release` skill.

## Prerequisites

```bash
cd engine && cargo build --release
export PATH="$PWD/target/release:$PATH"
rocky --version   # confirm the release candidate version
```

- DuckDB CLI installed (it seeds the playground database).
- A scratch catalog or schema in each warehouse. The tests create and drop tables.
- Environment variables for each warehouse you test:

| Warehouse | Required env vars |
|---|---|
| Databricks | `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` (or `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` for OAuth M2M) |
| Snowflake | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` (or `SNOWFLAKE_PRIVATE_KEY_PATH` for key-pair JWT) |
| Fivetran | `FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, `FIVETRAN_DESTINATION_ID` |
| BigQuery | `GOOGLE_APPLICATION_CREDENTIALS` (service account JSON) |

Every step passes on exit code 0 plus the condition shown. A non-zero exit fails the step.

## 1. Local / DuckDB (no credentials; every release must pass)

Run 1.1 to 1.4 in `examples/playground/pocs/00-foundations/00-playground-default`, then 1.5 to 1.13 there too unless a step says otherwise.

| # | Command | Pass when |
|---|---|---|
| 1.1 | `rocky -c rocky.toml validate` | JSON has `"valid": true` |
| 1.2 | `rocky compile --models models/` | `diagnostics` is empty or warnings only |
| 1.3 | `rocky test --models models/` | `tests_passed > 0`, `tests_failed == 0` |
| 1.4 | `rocky ci --models models/` | compile and test phases both succeed |
| 1.5 | `duckdb playground.duckdb < data/seed.sql` | no SQL errors |
| 1.6 | `rocky -c rocky.toml discover` | `sources` is non-empty, with tables |
| 1.7 | `rocky -c rocky.toml plan --filter source=demo` | JSON has the generated SQL. If the filter matches nothing, check the `source` name in the seed data. |
| 1.8 | `rocky -c rocky.toml run --filter source=demo` | every entry in `materializations` has `status: "ok"` |
| 1.9 | `rocky -c rocky.toml state` | watermarks from 1.8 are listed |
| 1.10 | `rocky -c rocky.toml doctor` | `overall` is `"healthy"` or `"warning"`, never `"critical"` |
| 1.11 | `rocky lineage <model_name> --models models/` | JSON has `upstream` and `downstream` nodes. Pick a model with upstream dependencies. |
| 1.12 | In `examples/playground/pocs/01-quality/05-snapshot-scd2`: `duckdb poc.duckdb < data/seed_v1.sql`, then `rocky -c rocky.toml snapshot --dry-run` | `steps` hold MERGE SQL, all `status: "dry_run"` |
| 1.13 | `rocky -c rocky.toml seed` (only if the POC has a `seeds/` directory) | `tables_loaded > 0`, `tables_failed == 0` |
| 1.14 | Browser UI, see below | see below |

### 1.14 Browser UI

Use a binary built with the UI embedded: `just build-engine-ui` from the repo root, or the release archive. A plain `cargo build --release` does not embed it, and `serve --ui` then refuses to start.

```bash
rocky playground /tmp/ui-smoke
rocky --config /tmp/ui-smoke/rocky.toml serve --ui --models /tmp/ui-smoke/models --token t --token-scope read-only --port 18080
```

In a second shell, open `http://127.0.0.1:18080/login?t=t` in a browser.

Pass when the page loads at `/ui/` and shows the project, and `curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:18080/api/v1/meta` prints `401`. Fail when `serve --ui` refuses to start, the page is blank, or the API answers without the token.

## 2. Warehouses (GA: Databricks, Snowflake)

Use a dedicated `rocky-<warehouse>.toml` that points at the scratch catalog or schema. Replace `<key>=<value>` with a filter that matches a real source.

| # | Command | Pass when |
|---|---|---|
| W.1 | `rocky -c rocky-<warehouse>.toml discover` | sources and tables are listed. Fail on 401/403, timeout, empty sources. |
| W.2 | `rocky -c rocky-<warehouse>.toml plan --filter <key>=<value>` | SQL uses the warehouse dialect. Databricks: Delta MERGE. Snowflake: quoted identifiers. |
| W.3 | `rocky -c rocky-<warehouse>.toml run --filter <key>=<value>` | materializations succeed. Databricks: drift reports clean or safely widened columns. Fail if `error_rate_abort_pct` is exceeded. Allow 1-5 min. |
| W.4 | `rocky -c rocky-<warehouse>.toml seed --seeds seeds/` | tables exist with correct row counts |
| W.5 | `rocky -c rocky-<warehouse>.toml snapshot --dry-run` (needs a `type = "snapshot"` pipeline) | MERGE SQL uses the warehouse dialect |
| W.6 | `rocky -c rocky-<warehouse>.toml doctor` | the adapter check is healthy |
| W.7 | `rocky -c rocky-<warehouse>.toml state` (Databricks only) | watermarks from W.3 are listed |

Snowflake needs `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` and one auth method. A suspended warehouse can slow W.3.

## 3. Fivetran (source adapter, with a Databricks or Snowflake target)

Config: `rocky-fivetran.toml`.

| # | Command | Pass when |
|---|---|---|
| 3.1 | `rocky -c rocky-fivetran.toml discover` | `sources` hold connectors with `source_type: "fivetran"`, tables and `last_sync_at`. Fail on 401 or a wrong destination ID. |
| 3.2 | `rocky -c rocky-fivetran.toml plan --filter <key>=<value>` | SQL references the discovered source tables |
| 3.3 | `rocky -c rocky-fivetran.toml run --filter <key>=<value>` | tables replicate to the target. Fail if Fivetran has not synced yet. |

## 4. BigQuery (experimental; failures do not block a release, but record them)

Config: `rocky-bigquery.toml`.

| # | Command | Pass when |
|---|---|---|
| 4.1 | `rocky -c rocky-bigquery.toml discover` | datasets and tables are listed |
| 4.2 | `rocky -c rocky-bigquery.toml plan --filter <key>=<value>` | SQL uses the BigQuery dialect |
| 4.3 | `rocky -c rocky-bigquery.toml run --filter <key>=<value>` | materializations succeed |
| 4.4 | `rocky -c rocky-bigquery.toml doctor` | the adapter connection is healthy |

## 5. Cross-cutting checks

| # | Command | Pass when |
|---|---|---|
| 5.1 | `rocky -c rocky.toml discover --output json \| python3 -c "import sys,json; json.load(sys.stdin)"`, for every command above | valid JSON. A `JSONDecodeError` means tracing leaks into stdout. |
| 5.2 | `rocky -c rocky.toml discover --output table`, also `state` and `doctor` | a readable table, no panic |
| 5.3 | `rocky --version`, `rocky --help`, `rocky run --help` | version matches the candidate; help lists every subcommand |
| 5.4 | `rocky -c /nonexistent/rocky.toml validate; echo "exit: $?"` and `rocky -c rocky.toml run --filter source=NONEXISTENT; echo "exit: $?"` | both exit non-zero (1) |

## Sign-off

| Section | Status | Tester | Notes |
|---|---|---|---|
| 1. DuckDB (local, with the browser UI) | | | |
| 2. Databricks and Snowflake | | | |
| 3. Fivetran | | | |
| 4. BigQuery (experimental) | | | |
| 5. Cross-cutting | | | |

Release candidate version: _______________ · Date: _______________

Verdict: [ ] GO / [ ] NO-GO

If NO-GO, list the blocking issues:

1. _______________
2. _______________
