# 14-import-dbt-failure-modes — `rocky import-dbt` against deliberately bad inputs

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`

## What it shows

How `rocky import-dbt` handles dbt features at or beyond the edge of what it translates, so you can predict it before pointing it at a real codebase. [`03-import-dbt-validate`](../03-import-dbt-validate/) is the happy path.

Canonical generic tests and `{{ ref }}` / `{{ source }}` / `{{ config }}` translate cleanly, and some other constructs **map** to native Rocky equivalents. Runtime-only Jinja is out of scope. Every raw import path refuses references to dbt's `is_incremental` macro, including callable aliases, because Rocky cannot preserve its bootstrap semantics without compiled SQL.

| Input | Result |
|---|---|
| `{% if target.name == 'prod' %}` (`stg_orders`) | **Refused**: the raw importer cannot evaluate Jinja control flow, and stripping the tags would apply the body unconditionally. Listed under "Failed models" with the fix: run `dbt compile --full-refresh` and import with the manifest. |
| `{% for %}` loop (`stg_loop`) | **Refused**, same reason (it would half-render into broken SQL). |
| `{{ var('cutoff') }}` (`stg_variables`) | **Mapped** to `@var(cutoff)`, with an informational `MappedConstruct` warning. Supply it with `rocky run --var cutoff=...` or an inline `@var(name, default)`. |
| `dbt_utils.accepted_range` in `schema.yml` | **Mapped** to a `[[tests]]` block of type `in_range` on the sidecar; no `UnsupportedTest` warning. |
| `snapshots/orders_snapshot.sql` | **Imported** as a `type = "snapshot"` model that keeps dbt's metadata column names. Two `MappedConstruct` warnings say what to check before the first `rocky run`. |
| `dbt_packages/` and `tests/` (singular tests) | Silently ignored. |

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
