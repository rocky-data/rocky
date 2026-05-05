# 07-config-layering — three-layer config + env-var substitution

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** env-var substitution, `_defaults.toml` inheritance, model sidecar overrides

## What it shows

Rocky resolves a model's target (`catalog.schema.table`) by walking three
config layers, with `${VAR}` / `${VAR:-default}` substitution at every layer:

1. **`rocky.toml`** — pipeline-level config (adapter, target template, etc.)
2. **`models/_defaults.toml`** — directory-level defaults applied to every
   sibling sidecar.
3. **`models/<name>.toml`** — per-model sidecar, overrides anything from the
   layers above.

This POC runs `rocky compile` twice over the same source tree — once with
no env vars set, once with overrides — and prints the resolved targets so
the precedence rules are visible side-by-side.

## Why it's distinctive

- Env-var substitution works at **every** config layer — including per-model
  sidecars, which closes a dbt-migration ergonomics gap (FR-001 Option A).
  A Dagster asset factory can now point each asset at its own
  catalog/schema/table by setting env vars on the rocky subprocess.
- The same source files materialize to different physical tables based on
  the orchestrator's environment, with no codegen or templating step.

## Layout

```
.
├── README.md             this file
├── rocky.toml            Layer 1 — pipeline + adapter (uses ${ROCKY_DUCKDB_PATH})
├── run.sh                end-to-end demo: two runs, with/without env vars
└── models/
    ├── _defaults.toml    Layer 2 — directory defaults (catalog, schema, strategy)
    ├── orders_summary.sql + .toml   Demonstrates Layer 2 inheritance
    └── customer_facts.sql  + .toml  Demonstrates Layer 3 env-var override
```

## Run

```bash
./run.sh
```

## Expected output

```text
=== Run 1: NO env vars set — every layer falls back to its default ===
Resolved model targets (defaults):
  customer_facts     -> customer_warehouse.marts.customer_facts
  orders_summary     -> poc_warehouse.public.orders_summary

=== Run 2: env vars set — orchestrator-style overrides flow into every layer ===
Resolved model targets (overrides):
  customer_facts     -> customer_prod.gold.customer_facts_v2
  orders_summary     -> prod_warehouse.silver.orders_summary
```

## Precedence rules

For a sidecar field (e.g. `target.catalog`):

1. Explicit value in the sidecar (after env-var substitution) wins.
2. Otherwise, the matching value from `_defaults.toml` (also after env-var
   substitution).
3. Otherwise, filename inference for `name` and `target.table` (defaults to
   the SQL file's stem).

`${VAR}` is **required** — an unset var is a load-time error. `${VAR:-default}`
falls back to `default` when the var is unset or empty.

## Related

- Canonical config reference: `.claude/skills/rocky-config/SKILL.md` at the
  monorepo root.
- The substitution helper lives at
  `engine/crates/rocky-core/src/config.rs::substitute_env_vars`.
- The sidecar / `_defaults.toml` loader lives at
  `engine/crates/rocky-core/src/models.rs`.
