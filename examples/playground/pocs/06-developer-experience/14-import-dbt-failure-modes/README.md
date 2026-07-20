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

- A model with `{% if target.name == 'prod' %}` — still out of scope:
  surfaced as a `JinjaControlFlow` warning, body emitted verbatim with a
  `-- TODO: dbt-jinja-not-translated` marker.
- A model with `{{ var('cutoff') }}` — now **mapped** to Rocky's native
  `@var(cutoff)` per-run variable marker, surfaced as an informational
  `MappedConstruct` warning (supply the value with
  `rocky run --var cutoff=...`, or an inline `@var(name, default)`).
- `schema.yml` with `dbt_utils.accepted_range` — now **mapped** to a
  native `[[tests]]` block of type `in_range` on the model sidecar; no
  longer surfaced as an `UnsupportedTest` warning.
- A model with `{% for %}` loops (`stg_loop`) — **refused** rather than
  half-rendered into broken SQL; it lands under "Failed models".
- `snapshots/`, `dbt_packages/`, and `tests/` (singular tests) trees —
  silently ignored. The importer walks `models/` only.

The POC's `run.sh` asserts each of these end-to-end. The `{% if %}`
emission deliberately contains a TODO-replaced fragment that won't
`rocky compile` cleanly; the point is that genuinely runtime-only Jinja
needs a manual follow-up pass, while constructs like `var()` and
`accepted_range` now arrive as native Rocky. The happy-path counterpart
that does compile end-to-end is
[`03-import-dbt-validate`](../03-import-dbt-validate/).

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
│   │   ├── schema.yml                      dbt_utils.accepted_range (mapped to in_range)
│   │   ├── stg_orders.sql                  {% if target.name == 'prod' %}
│   │   ├── stg_variables.sql               {{ var('cutoff') }} (mapped to @var)
│   │   └── stg_loop.sql                    {% for %} loop (refused)
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
  "warnings": 2,
  "failed": 1,
  "tests_found": 3,
  "tests_converted": 3,
  "tests_converted_custom": 1,
  "tests_skipped": 0,
  "warning_details": [
    {
      "model": "stg_variables",
      "category": "MappedConstruct",
      "message": "contains {{ var() }} — mapped to Rocky's `@var()` per-run variable marker",
      "suggestion": "supply values at run time with `rocky run --var name=value`, or rely on an inline default `@var(name, default)`"
    },
    {
      "model": "stg_orders",
      "category": "JinjaControlFlow",
      "message": "contains Jinja control flow ({% if %}) — emitted with TODO markers; the conditional body is applied unconditionally, so review the result",
      "suggestion": "use the manifest import path (`dbt compile`) for faithful Jinja resolution"
    }
  ],
  "failed_details": [
    {
      "name": "stg_loop",
      "reason": "contains unsupported Jinja control flow ({% for %} or {% set %}) ..."
    }
  ],
  ...
}

=== imported/MIGRATION-NOTES.md (Known limitations + Warnings sections) ===
## Known limitations
- **dbt generic tests outside the canonical four** ...
- **Singular tests** in `tests/` (custom SQL) — copy and rewrite manually.
- **dbt macros / `dbt_packages/`** — Rocky has no Jinja runtime. ...
- **Raw Jinja control flow** — unresolved Jinja that references the `is_incremental` macro, including callable aliases, is refused on every raw import path. With `--no-manifest`, `{% for %}` / `{% set %}` models are also refused. Other `{% if %}` bodies are emitted with `# TODO: dbt-jinja-not-translated` comments and must be reviewed.

## Warnings
- `stg_variables` — MappedConstruct: {{ var() }} mapped to `@var()` ...
- `stg_orders` — JinjaControlFlow: ...
## Failed models
- `stg_loop` — contains unsupported Jinja control flow ({% for %} or {% set %}) ...

=== Emitted models/stg_orders.sql (target.name branch flagged) ===
-- TODO: dbt-jinja-not-translated — see MIGRATION-NOTES.md
SELECT ...
FROM raw.orders
/* TODO: unsupported Jinja block */
WHERE updated_at >= '2026-01-01'
/* TODO: unsupported Jinja block */

=== Emitted models/stg_variables.sql ({{ var() }} mapped to @var()) ===
SELECT ...
FROM raw.orders
WHERE customer_id > @var(cutoff)

=== Assertions ===
ok  All assertions passed.
```
