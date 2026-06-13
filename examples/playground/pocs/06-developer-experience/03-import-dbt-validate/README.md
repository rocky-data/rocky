# 03-import-dbt-validate — `rocky import-dbt` + `validate-migration`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`, `rocky validate-migration`

## What it shows

Two commands for migrating off dbt:

1. `rocky import-dbt --dbt-project <path>` — converts a dbt project (with
   refs, sources, configs) into Rocky models. Supports either manifest.json
   or regex-based imports.
2. `rocky validate-migration --dbt-project <path>` — without running anything
   on a warehouse, reports which dbt models map cleanly and which need
   manual attention.

## Why it's distinctive vs `rocky/examples/dbt-migration`

That example shows hand-written before/after equivalents. This POC
demonstrates the **automated** import path on a real (small) dbt project.

`dbt_project/models/schema.yml` uses the legacy `tests:` key. As of engine
v1.39.0 the importer also accepts the dbt-1.7+ `data_tests:` spelling on the
same column tests; swap one for the other and the import is identical.
See sibling POC `19-import-dbt-unit-tests/` for the manifest path that also
imports `manifest.unit_tests` entries onto Rocky `[[test]]` sidecars.

## Layout

```
.
├── README.md
├── run.sh
├── dbt_project/        Minimal dbt project
│   ├── dbt_project.yml
│   └── models/
│       ├── stg_orders.sql
│       └── fct_revenue.sql
└── imported/           Output dir (regenerated each run)
```

## Run

```bash
./run.sh
```
