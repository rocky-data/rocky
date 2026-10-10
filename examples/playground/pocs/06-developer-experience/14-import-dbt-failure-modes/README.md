# 14-import-dbt-failure-modes — `rocky import-dbt` against deliberately bad inputs

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`

## What it shows

`rocky import-dbt` is opinionated about what it translates: canonical
generic tests + `{{ ref }}` / `{{ source }}` / `{{ config }}` translate
cleanly, a growing set of common dbt constructs now **map** to native
Rocky equivalents, and genuinely runtime-only Jinja stays out of scope.
In particular, every raw import path refuses references to dbt's
`is_incremental` macro, including callable aliases, because Rocky cannot
preserve its bootstrap semantics without compiled SQL. This POC throws
other edge cases at the importer in one go and verifies each is handled
cleanly — distinguishing the constructs that map from the ones that warn
or are refused:

- A model with `{% if target.name == 'prod' %}` (`stg_orders`) —
  **refused**. The raw importer cannot evaluate Jinja control flow, and
  stripping the tags would apply the conditional body unconditionally.
  It lands under "Failed models" with the fix named: run
  `dbt compile --full-refresh` and import with the manifest.
- A model with `{% for %}` loops (`stg_loop`) — **refused** for the same
  reason, rather than half-rendered into broken SQL.
- A model with `{{ var('cutoff') }}` (`stg_variables`) — **mapped** to
  Rocky's native `@var(cutoff)` per-run variable marker, surfaced as an
  informational `MappedConstruct` warning (supply the value with
  `rocky run --var cutoff=...`, or an inline `@var(name, default)`).
- `schema.yml` with `dbt_utils.accepted_range` on `stg_variables` —
  **mapped** to a native `[[tests]]` block of type `in_range` on the
  model sidecar; no longer surfaced as an `UnsupportedTest` warning.
- `snapshots/orders_snapshot.sql` — **imported** as a
  `type = "snapshot"` model that keeps dbt's metadata column names. Two
  `MappedConstruct` warnings say what to check before the first
  `rocky run`.
- `dbt_packages/` and `tests/` (singular tests) trees — silently ignored.

The POC's `run.sh` asserts each of these end-to-end. The happy-path
counterpart that does compile end-to-end is
[`03-import-dbt-validate`](../03-import-dbt-validate/).

## Why it's distinctive vs `03-import-dbt-validate`

`03-import-dbt-validate` shows the **happy path** (clean translation,
canonical tests, the materialization mapping). This POC is the
**failure-mode counterpart**: it documents exactly what the importer
will and won't do when fed a dbt project that uses features outside
the supported set, so users can predict the importer's behaviour
before pointing it at a real codebase.

## Layout

```
.
├── README.md
├── run.sh
├── dbt_project/                            Deliberately bad dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── schema.yml                      tests on stg_variables; accepted_range → in_range
│   │   ├── stg_orders.sql                  {% if target.name == 'prod' %} (refused)
│   │   ├── stg_variables.sql               {{ var('cutoff') }} (mapped to @var)
│   │   └── stg_loop.sql                    {% for %} loop (refused)
│   ├── snapshots/orders_snapshot.sql       Imported as a snapshot model
│   ├── dbt_packages/dbt_utils/macros/star.sql   Out of scope — silently ignored
│   └── tests/assert_revenue_positive.sql   Out of scope — singular test
└── imported/                               Regenerated each run (gitignored)
```

## Run

```bash
./run.sh
```

## Expected output

```
=== rocky import-dbt (regex path, deliberately bad inputs) ===
{
  "version": "...",
  "command": "import-dbt",
  "import_method": "Regex",
  "imported": 2,
  "warnings": 3,
  "failed": 2,
  "tests_found": 3,
  "tests_converted": 3,
  "tests_converted_custom": 1,
  "tests_skipped": 0,
  "imported_models": ["stg_variables", "orders_snapshot"],
  "warning_details": [
    { "model": "stg_variables", "category": "MappedConstruct",
      "message": "contains {{ var() }} — mapped to Rocky's `@var()` per-run variable marker", ... },
    { "model": "orders_snapshot", "category": "MappedConstruct",
      "message": "dbt snapshot imported as a `type = \"snapshot\"` model; ...", ... },
    { "model": "orders_snapshot", "category": "MappedConstruct",
      "message": "snapshot targets warehouse.snapshots.orders_snapshot as configured; ...", ... }
  ],
  "failed_details": [
    { "name": "stg_orders",
      "reason": "raw import cannot evaluate Jinja control flow; run `dbt compile --full-refresh` and import with the manifest" },
    { "name": "stg_loop",
      "reason": "raw import cannot evaluate Jinja control flow; run `dbt compile --full-refresh` and import with the manifest" }
  ],
  ...
}

=== imported/MIGRATION-NOTES.md (Known limitations + Warnings sections) ===
## Known limitations
...
## Warnings
- `stg_variables` — MappedConstruct: contains {{ var() }} — mapped to Rocky's `@var()` per-run variable marker
- `orders_snapshot` — MappedConstruct: dbt snapshot imported as a `type = "snapshot"` model; ...
- `orders_snapshot` — MappedConstruct: snapshot targets warehouse.snapshots.orders_snapshot as configured; ...

## Failed models
- `stg_orders` — raw import cannot evaluate Jinja control flow; run `dbt compile --full-refresh` and import with the manifest
- `stg_loop` — raw import cannot evaluate Jinja control flow; run `dbt compile --full-refresh` and import with the manifest

=== Emitted models/stg_variables.sql ({{ var() }} mapped to @var()) ===
SELECT ...
FROM raw.orders
WHERE customer_id > @var(cutoff)

=== Assertions ===
ok  All assertions passed.
```
