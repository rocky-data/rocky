# 03-import-dbt-validate — `rocky import-dbt` + `validate-migration`

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky import-dbt`, `rocky validate-migration`

## What it shows

- `rocky import-dbt --dbt-project <path>` converts a dbt project (refs, sources, configs) into Rocky models, from `manifest.json` or by regex.
- `rocky validate-migration --dbt-project <path>` reports which dbt models map cleanly and which need manual attention, without running anything on a warehouse.

`dbt_project/models/schema.yml` uses the legacy `tests:` key. The importer also accepts the dbt-1.7+ `data_tests:` spelling on the same column tests, with an identical import. For the manifest path, which also imports `manifest.unit_tests`, see `19-import-dbt-unit-tests/`.

## Run

```bash
./run.sh
```

The emitted Rocky repo goes to `imported/` and logs to `expected/`; both are regenerated each run.
