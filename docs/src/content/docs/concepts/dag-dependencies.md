---
title: DAG & Dependencies
description: How Rocky works out the order models run in, and which of them run at the same time.
sidebar:
  order: 5
---

Rocky builds a DAG from your model dependencies, then runs the models in that
order. A DAG is a directed acyclic graph: nodes with one-way edges and no cycles.

Edges come from two places, merged:

- An explicit `depends_on` list in a model's TOML.
- A bare table name in the model's SQL that matches another model in the project. Rocky infers that edge and reports it as diagnostic `I001`.

Rocky then topologically sorts the merged set. The result is an execution plan
with layers that run in parallel.

## Declaring dependencies

Each model names what it depends on with `depends_on` in its TOML:

```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]
```

`fct_orders` cannot start until both `stg_orders` and `dim_customers` finish.

## Topological sort

Rocky sorts with Kahn's algorithm. The order is deterministic: when several
models are ready at the same time, Rocky sorts those alphabetically.

## Execution layers

Rocky groups models into layers. Every model in a layer can run at the same time,
because earlier layers already satisfied its dependencies.

Take this graph:

```
stg_customers ──→ dim_customers ──┐
                                  ├──→ fct_orders
stg_orders ───────────────────────┘
```

It produces three layers:

```
Layer 0: stg_customers, stg_orders     (no dependencies, run in parallel)
Layer 1: dim_customers                  (depends on stg_customers)
Layer 2: fct_orders                     (depends on stg_orders + dim_customers)
```

Rocky runs all of Layer 0 at once, waits for it to finish, then runs Layer 1, and
so on.

### What `--parallel N` bounds

`--parallel N` caps how many nodes run at once. On `rocky run` it defaults to 4.

Under `rocky run --dag` it applies **only when you pass it**. Leave it off and
every node in a layer runs at once, as it always has.

The flag bounds *nodes*, not warehouse queries. A `--dag` sub-run builds one
partition at a time, so the number is not multiplied inside a node. But it is
also not a ceiling on every query. A replication node takes its table fan-out
from that pipeline's `[execution] concurrency`, which defaults to 32, and
`--parallel` does not govern that on either path.

So `rocky run --dag --parallel 1` runs one node at a time, and the replication
node inside it may still copy several tables at once.

## Validation

`rocky validate` checks the whole DAG before any SQL runs. It needs no warehouse
connection.

### Cycle detection

Rocky reports a circular dependency as the set of models in the cycle:

```toml
# model_a.toml
name = "model_a"
depends_on = ["model_b"]

# model_b.toml
name = "model_b"
depends_on = ["model_a"]
```

```
Error: DAG error: circular dependency detected involving: ["model_a", "model_b"]
```

### Unknown dependencies

Rocky catches a reference to a model that does not exist:

```toml
name = "fct_orders"
depends_on = ["stg_orders", "nonexistent_model"]
```

```
Error: DAG error: unknown dependency 'nonexistent_model' referenced by 'fct_orders'
```

When the unknown name is close to a real model name, the message adds a
`— did you mean '<model>'?` suggestion.

## External table references

Not every table reference becomes a dependency. Rocky decides from how the
reference is qualified in the SQL.

| SQL reference | Classification | DAG behavior |
|---|---|---|
| `stg_orders` (matches a Rocky model) | Model dependency | Execution edge in DAG |
| `stg_orders` (no matching model) | External reference | Ignored by DAG |
| `dbt_fivetran.stg_facebook_ads__ad_history` | Two-part external | Ignored by DAG |
| `analytics.dbt_fivetran.stg_facebook_ads__ad_history` | Three-part external | Ignored by DAG |

Rocky reads from an external table. It does not manage, build, or schedule it.

That split is what makes hybrid projects work. A Rocky model can read a table
another tool produced (a dbt package, a Fivetran connector, a hand-written ETL
job) without converting or importing it. External tables still show up in
column-level [lineage](/reference/glossary/#lineage). They are left out of
execution planning.

```sql
-- stg_orders is a Rocky model -> DAG dependency
-- dbt_fivetran.stg_facebook_ads__ad_history is external -> no dependency
SELECT
    o.order_id,
    f.ad_name
FROM stg_orders o
JOIN dbt_fivetran.stg_facebook_ads__ad_history f
    ON o.campaign_id = f.campaign_id
```

See [Using Rocky with dbt Packages](/guides/using-dbt-packages/) for the full
guide to this pattern.
