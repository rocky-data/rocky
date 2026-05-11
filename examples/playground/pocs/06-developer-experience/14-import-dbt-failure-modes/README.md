# 14-import-dbt-failure-modes — `rocky import-dbt` against deliberately bad inputs

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`

## What it shows

`rocky import-dbt` is opinionated about what it translates: canonical
generic tests + `{{ ref }}` / `{{ source }}` / `{{ config }}` /
`is_incremental()` translate cleanly; everything else is deliberately
out of scope. This POC throws all the out-of-scope cases at the importer
in one go and verifies the importer handles each one cleanly:

- A model with `{% if target.name == 'prod' %}` — surfaced as a
  `JinjaControlFlow` warning, body emitted verbatim with a
  `-- TODO: dbt-jinja-not-translated` marker.
- A model with `{{ var('cutoff') }}` — surfaced as an
  `UnsupportedMacro` warning, the `{{ var() }}` expression rewritten
  to a TODO placeholder.
- `schema.yml` with `dbt_utils.accepted_range` — surfaced as an
  `UnsupportedTest` warning, not stubbed in the emitted toml.
- `snapshots/`, `dbt_packages/`, and `tests/` (singular tests) trees —
  silently ignored. The importer walks `models/` only.

The POC's `run.sh` asserts each of these end-to-end. The emitted SQL
deliberately contains TODO-replaced fragments that won't `rocky compile`
cleanly — the whole point is that out-of-scope dbt features need a
manual follow-up pass. The happy-path counterpart that does compile
end-to-end is [`03-import-dbt-validate`](../03-import-dbt-validate/).

## Why it's distinctive vs `03-import-dbt-validate`

`03-import-dbt-validate` shows the **happy path** (clean translation,
canonical tests, the `view → ephemeral` mapping). This POC is the
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
│   │   ├── schema.yml                      dbt_utils.accepted_range (unsupported test)
│   │   ├── stg_orders.sql                  {% if target.name == 'prod' %}
│   │   └── stg_variables.sql               {{ var('cutoff') }}
│   ├── snapshots/orders_snapshot.sql       Out of scope — silently ignored
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
  "failed": 0,
  "tests_found": 3,
  "tests_converted": 2,
  "tests_skipped": 1,
  "warning_details": [
    {
      "model": "stg_orders",
      "category": "JinjaControlFlow",
      "message": "contains Jinja control flow ({% if %}, {% for %}) — replaced with TODO comments",
      "suggestion": "consider using manifest.json import path for full Jinja resolution"
    },
    {
      "model": "stg_variables",
      "category": "UnsupportedMacro",
      "message": "contains {{ var() }} — not supported, replaced with TODO",
      "suggestion": "replace with a literal value or use manifest.json import path"
    },
    {
      "model": "stg_orders",
      "category": "UnsupportedTest",
      "message": "dbt test 'dbt_utils.accepted_range' on column 'amount' is outside the supported set ...",
      "suggestion": "rewrite as a Rocky `[[tests]]` of type `expression` or as a custom check in a quality pipeline"
    }
  ],
  ...
}

=== imported/MIGRATION-NOTES.md (Known limitations + Warnings sections) ===
## Known limitations
- **dbt generic tests outside the canonical four** ...
- **Singular tests** in `tests/` (custom SQL) — copy and rewrite manually.
- **dbt macros / `dbt_packages/`** — Rocky has no Jinja runtime. ...
- **`{% if %}` / `{% for %}` / `{{ var() }}`** ...
## Warnings
- `stg_orders` — JinjaControlFlow: ...
- `stg_variables` — UnsupportedMacro: ...
- `stg_orders` — UnsupportedTest: ...

=== Emitted models/stg_orders.sql (target.name branch flagged) ===
-- TODO: dbt-jinja-not-translated — see MIGRATION-NOTES.md
SELECT ...
FROM raw.orders
/* TODO: unsupported Jinja block */
WHERE updated_at >= '2026-01-01'
/* TODO: unsupported Jinja block */

=== Assertions ===
ok  All assertions passed.
```
