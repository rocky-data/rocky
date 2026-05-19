# 19-import-dbt-unit-tests — `rocky import-dbt` unit-test bridge + `data_tests:` alias

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`, manifest path, `manifest.unit_tests` → `[[test]]` sidecars

## What it shows

Two pieces of `rocky import-dbt` coverage added in engine v1.39.0:

1. **`data_tests:` accepted as an alias for `tests:`** on column-level YAML tests.
   dbt 1.7+ renamed the column-test key from `tests:` to `data_tests:` (the legacy
   spelling still works on the dbt side). Before v1.39.0 the importer only saw the
   legacy key, so any modern dbt project converted to zero column tests. This POC's
   `schema.yml` uses `data_tests:` exclusively to pin the alias in a runnable demo.

2. **`manifest.unit_tests` → Rocky `[[test]]` sidecars.** The manifest path now walks
   `manifest.unit_tests` and emits each entry as a `[[test]]` block on the matching
   model's sidecar TOML. `ref('upstream')` / `source('a', 'b')` wrappers on
   `given.input` are stripped to bare references. CSV / SQL fixtures are deliberately
   out of scope and surface as `UnsupportedUnitTestFormat` warnings.

## Why it's distinctive vs `03-import-dbt-validate/`

POC 03 exercises the regex path (`--no-manifest`) and the canonical-four column-test
mapping with the legacy `tests:` key. This POC exercises the **manifest** path on a
deliberately dbt-1.8-shaped input: `data_tests:` for column tests + a `unit_tests`
block in `target/manifest.json` covering happy path, source-wrapped input, and a
deliberately unsupported CSV fixture.

## Layout

```
.
├── README.md         this file
├── run.sh            end-to-end demo (no creds, no toolchain — uses a hand-authored manifest)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── stg_orders.sql
│   │   ├── fct_revenue.sql
│   │   └── schema.yml          # data_tests: alias (column tests)
│   └── target/
│       └── manifest.json       # nodes + unit_tests block (dbt-1.8 schema v12)
└── imported/                   # output dir (regenerated each run, gitignored)
```

## Run

```bash
./run.sh
```

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

## What happened

1. `rocky import-dbt --manifest` reads `target/manifest.json`, walks `nodes` (models)
   and `unit_tests`, and emits a Rocky repo under `imported/`.
2. `apply_dbt_tests` (called from both the regex and manifest paths) reads
   `dbt_project/models/schema.yml`, recognises `data_tests:` via the new serde alias,
   and emits canonical-four `[[tests]]` blocks on `stg_orders.toml`.
3. `apply_dbt_unit_tests` walks `manifest.unit_tests`, strips `ref(...)` / `source(...)`
   wrappers on `given.input`, and emits `[[test]]` blocks on the matching model
   sidecar. The CSV-format fixture surfaces as an `UnsupportedUnitTestFormat`
   warning and bumps `unit_tests_skipped` by one.
4. New counters (`unit_tests_found / unit_tests_converted / unit_tests_skipped`) show
   up on the `--output json` payload (`ImportDbtOutput`) and in `MIGRATION-NOTES.md`.

## Related

- Migration guide: [`/guides/migrate-from-dbt/`](https://rocky-data.dev/guides/migrate-from-dbt/) — Section 9 covers both features.
- Source: `engine/crates/rocky-compiler/src/import/{dbt.rs, dbt_manifest.rs, dbt_tests.rs, dbt_sources.rs}`
- Sibling POCs:
  - [`03-import-dbt-validate/`](../03-import-dbt-validate/) — regex path, legacy `tests:` key
  - [`14-import-dbt-failure-modes/`](../14-import-dbt-failure-modes/) — out-of-scope dbt features (Jinja, non-canonical tests)
