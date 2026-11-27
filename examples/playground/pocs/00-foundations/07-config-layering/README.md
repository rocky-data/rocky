# 07-config-layering — three-layer config + env-var substitution

> **Category:** 00-foundations
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** env-var substitution, `_defaults.toml` inheritance, model sidecar overrides

## What it shows

Rocky resolves a model's target (`catalog.schema.table`) through three config layers, with `${VAR}` / `${VAR:-default}` substitution at every layer:

1. `rocky.toml`: pipeline-level config
2. `models/_defaults.toml`: directory defaults for sibling sidecars
3. `models/<name>.toml`: per-model sidecar, overrides the layers above

`run.sh` compiles the same source twice (no env vars, then overrides) and prints the resolved targets. `orders_summary` shows Layer 2 inheritance; `customer_facts` shows the Layer 3 override.

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

1. An explicit sidecar value (after substitution) wins.
2. Otherwise the `_defaults.toml` value (also after substitution).
3. Otherwise filename inference for `name` and `target.table` (the SQL file's stem).

`${VAR}` is **required**: an unset var is a load-time error. `${VAR:-default}` falls back to `default` when the var is unset or empty.

## Related

- Config reference: `.agents/skills/rocky-config/SKILL.md` at the monorepo root.
- Source: `engine/crates/rocky-core/src/config.rs::substitute_env_vars`, `engine/crates/rocky-core/src/models.rs`
