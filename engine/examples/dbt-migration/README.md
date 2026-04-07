# dbt Migration

A side-by-side comparison of a dbt project and its Rocky equivalent, demonstrating how to migrate from dbt to Rocky.

## Before (dbt)

```
dbt-project/
  dbt_project.yml
  models/
    staging/
      stg_customers.sql       # Jinja {{ ref() }}, {{ config() }}
    marts/
      fct_orders.sql           # Jinja {{ ref() }}, {{ config() }}
```

## After (Rocky)

```
rocky-project/
  rocky.toml
  models/
    stg_customers.sql          # Plain SQL, no Jinja
    stg_customers.toml         # Config lives in sidecar TOML
    fct_orders.rocky           # Rocky DSL — pipeline syntax
    fct_orders.toml            # Config lives in sidecar TOML
```

## Key Differences

| Concept | dbt | Rocky |
|---------|-----|-------|
| Config | `{{ config(materialized='table') }}` in SQL | Separate `.toml` sidecar file |
| References | `{{ ref('model_name') }}` | Bare name `from stg_customers` |
| Templating | Jinja2 | None — compiled DSL or plain SQL |
| Incremental | `{% if is_incremental() %}` blocks | `type = "incremental"` in `.toml` + timestamp column |
| Schema tests | YAML `schema.yml` files | Contracts (`.contract.toml`) or AI-generated tests |
| CLI | `dbt run`, `dbt test` | `rocky run`, `rocky plan` |

## Migration Steps

1. **Remove Jinja from SQL** -- Replace `{{ ref('model') }}` with the bare model name. Remove `{{ config(...) }}` blocks entirely.

2. **Create sidecar `.toml` files** -- Move materialization config, tags, and descriptions into `.toml` files alongside each model.

3. **Convert to Rocky DSL (optional)** -- Models can stay as `.sql` files. Optionally rewrite complex models in `.rocky` for better readability and NULL-safety.

4. **Create `rocky.toml`** -- Define your adapter, target, and execution settings. Rocky uses a single config file instead of `profiles.yml` + `dbt_project.yml`.

5. **Validate** -- Run `rocky plan` to preview generated SQL without executing.

```bash
cd rocky-project
rocky plan --config rocky.toml
rocky run --config rocky.toml
```

## NULL-Safety Bonus

In dbt SQL: `WHERE status != 'cancelled'` silently drops NULL rows.
In Rocky DSL: `where status != "cancelled"` compiles to `WHERE status IS DISTINCT FROM 'cancelled'`, preserving NULLs.
