# 19-import-dbt-unit-tests — `rocky import-dbt` unit-test bridge + `data_tests:` alias

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`, manifest path, `manifest.unit_tests` → `[[test]]` sidecars

## What it shows

1. **`data_tests:` as an alias for `tests:`** on column-level YAML tests (the dbt 1.7+ spelling). `schema.yml` uses `data_tests:` only, so the demo fails if the alias breaks.
2. **`manifest.unit_tests` → Rocky `[[test]]` sidecars** (manifest path). Each entry becomes a `[[test]]` block on the matching model's sidecar, with `ref(...)` / `source(...)` wrappers on `given.input` stripped to bare references. CSV / SQL fixtures are out of scope and surface as `UnsupportedUnitTestFormat` warnings.

[`03-import-dbt-validate/`](../03-import-dbt-validate/) covers the regex path (`--no-manifest`) and the legacy `tests:` key.

## Run

```bash
./run.sh
```

No credentials or dbt toolchain needed: the manifest is hand-authored (`dbt_project/target/manifest.json`, dbt-1.8 schema v12). Output goes to `imported/`.

## Expected output

```text
=== rocky import-dbt --manifest (emit Rocky repo + walk manifest.unit_tests) ===
{
  "version": "<rocky-version>",
  "command": "import-dbt",
  ...
  "unit_tests_found": 3,
  "unit_tests_converted": 2,
  "unit_tests_skipped": 1,
  ...
}

=== Emitted models/stg_orders.toml (data_tests: alias + unit test) ===
name = "stg_orders"
...
[[tests]]
type = "unique"
column = "order_id"

[[test]]
name = "stamps_status_when_completed"
[[test.given]]
ref = "raw.orders"
[[test.given.rows]]
amount = 50
customer_id = 100
order_id = 1
status = "Completed"
# ... more given.rows ...
[test.expect]
ordered = false
[[test.expect.rows]]
status = "completed"
# ...

=== Assertions ===
ok  All assertions passed.
```

The `unit_tests_found / unit_tests_converted / unit_tests_skipped` counters appear in the `--output json` payload (`ImportDbtOutput`) and in `MIGRATION-NOTES.md`. The CSV fixture bumps `unit_tests_skipped` by one.

## Related

- Migration guide: [`/guides/migrate-from-dbt/`](https://rocky-data.dev/guides/migrate-from-dbt/) — Section 9 covers both features.
- Source: `engine/crates/rocky-compiler/src/import/{dbt.rs, dbt_manifest.rs, dbt_tests.rs, dbt_sources.rs}`
- [`14-import-dbt-failure-modes/`](../14-import-dbt-failure-modes/) — out-of-scope dbt features (Jinja, non-canonical tests)
