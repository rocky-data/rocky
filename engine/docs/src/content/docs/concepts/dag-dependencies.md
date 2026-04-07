---
title: DAG & Dependencies
description: How Rocky resolves model execution order and parallel execution
sidebar:
  order: 5
---

Rocky builds a directed acyclic graph (DAG) from model dependencies to determine execution order. Models declare their upstream dependencies explicitly, and Rocky uses topological sorting to produce a valid execution plan with parallel execution layers.

## Declaring dependencies

Each model declares what it depends on using the `depends_on` field in its TOML configuration:

```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]
```

This means `fct_orders` cannot run until both `stg_orders` and `dim_customers` have completed.

## Topological sort

Rocky uses Kahn's algorithm to produce a topological ordering of models. The output is deterministic: when multiple models have no remaining dependencies (i.e., they are tied), they are sorted alphabetically.

## Execution layers

Models are grouped into layers. All models in a layer can run in parallel because their dependencies have been satisfied by earlier layers.

Example dependency graph:

```
stg_customers ──→ dim_customers ──┐
                                  ├──→ fct_orders
stg_orders ───────────────────────┘
```

This produces three execution layers:

```
Layer 0: stg_customers, stg_orders     (no dependencies, run in parallel)
Layer 1: dim_customers                  (depends on stg_customers)
Layer 2: fct_orders                     (depends on stg_orders + dim_customers)
```

Rocky executes all models in Layer 0 concurrently, waits for them to finish, then executes Layer 1, and so on.

## Validation

Rocky validates the DAG at `rocky validate` time, catching problems before any SQL is executed.

### Cycle detection

Circular dependencies are detected and reported with the full cycle path:

```toml
# model_a.toml
name = "model_a"
depends_on = ["model_b"]

# model_b.toml
name = "model_b"
depends_on = ["model_a"]
```

```
Error: Circular dependency detected: model_a → model_b → model_a
```

### Unknown dependencies

References to models that don't exist are caught:

```toml
name = "fct_orders"
depends_on = ["stg_orders", "nonexistent_model"]
```

```
Error: Unknown dependency "nonexistent_model" in model "fct_orders"
```

## How it differs from dbt

dbt uses Jinja's `{{ ref('model_name') }}` macro inside SQL to create implicit dependencies. The dependency graph is extracted by parsing Jinja templates:

```sql
-- dbt model
SELECT *
FROM {{ ref('stg_orders') }}
JOIN {{ ref('dim_customers') }} USING (customer_id)
```

Rocky uses explicit `depends_on` declarations in TOML:

```toml
depends_on = ["stg_orders", "dim_customers"]
```

The differences:

| | dbt | Rocky |
|---|---|---|
| Declaration | Implicit via `{{ ref() }}` in SQL | Explicit `depends_on` in TOML |
| When validated | During parsing/compilation | At `rocky validate` time |
| SQL purity | SQL mixed with Jinja | Pure SQL, no template language |
| Editor support | Requires dbt LSP for `ref()` | Standard SQL tooling works |

Rocky's approach means the DAG is validated early and independently of SQL parsing. You can run `rocky validate` to check the entire dependency graph without connecting to any warehouse.
