---
title: Shadow Mode
description: Run a change against copies of your tables, then diff them against production
sidebar:
  order: 14
---

Shadow mode writes a pipeline's output to shadow tables instead of production tables, or alongside them. You get to see what a change does to the data before it reaches the tables people query. Use it for new logic, schema migrations, and adapter upgrades.

## What a shadow run does

```
  rocky run --shadow
        │
        │ 1. Rocky rewrites every target name
        │    with the default _rocky_shadow suffix
        ▼
  the pipeline runs as usual, writing to the shadow targets
        │
        ▼
  analytics.marts.fct_revenue_rocky_shadow   the shadow table
  analytics.marts.fct_revenue                production, untouched
        │
        │ 2. rocky compare reads both tables:
        │    row counts, schema, optional sampled rows
        ▼
  PASS / WARN / FAIL, with the diffs
```

:::caution[Where shadow isolation applies]
Isolation covers a plain `rocky run` over a **transformation** pipeline, and a
**replication** pipeline in either mode — `--shadow-suffix` or `--shadow-schema`
(or a branch). Everywhere else Rocky refuses the flag instead of running without
isolation:

- **`rocky run --dag`** refuses `--shadow` and `--branch` outright. The DAG runs
  each model as its own sub-run. A model's read of an upstream built by the same
  run is therefore not redirected to that upstream's shadow target. The
  downstream shadow table would be built from production data while the run
  reported success. Run the shadow pipeline without `--dag`.
- **Snapshot and load pipelines** refuse it. Rocky does not rewrite their
  targets.
- **Seeds** cause a `--dag` run to be refused along with the rest. `rocky seed`
  itself has no shadow mode and always writes its configured target.

A stored `rocky plan --shadow` carries its routing into `rocky apply`.
:::

## Models a shadow run refuses

Shadow and branch runs reject `content_addressed`, `time_interval` and
`ephemeral` models.

- `content_addressed` and `time_interval` models persist object-storage or
  partition-state identities. Rewriting the warehouse target alone cannot
  isolate those yet.
- An `ephemeral` model is neither materialized nor inlined into its consumers.
  The consumer would read the production table, and no rewrite could redirect
  that read. Give the model a materialized strategy to shadow it.

Rocky also refuses a derived shadow target that matches a configured production
target, or that matches another selected model's shadow target.

Rocky does **not** yet check whether a derived shadow target is already occupied
by an object it does not know about. Suppose a table with the derived name
already exists and is not a Rocky model target — a source, a seed, or an ad-hoc
table. A full-refresh model will replace it. Prefer a dedicated shadow schema
that you own.

## How Rocky redirects a read to a shadow table

A model that reads another selected model's table is routed to that upstream's
shadow target. This holds whether or not the model declares the dependency in
`depends_on`. Rocky matches on the upstream's configured
`catalog.schema.table`, so it redirects a physical read too.

Sometimes a reference could resolve to more than one selected upstream. Two
models may have targets that share a table name in different catalogs, read as a
bare or partially qualified name. Rocky refuses the run rather than guess which
one to read.

### Identifier case

Matching follows the warehouse's own rule for identifier case, per component.

| Warehouse | Case is part of object identity | What Rocky matches |
|---|---|---|
| DuckDB, Databricks, Trino | No | `Orders` and `orders` name one table, and either spelling is redirected |
| BigQuery | Yes | The reference must match exactly |
| Snowflake | Yes, and an unquoted reference is read as upper case | The reference must name the same object the target does |

On BigQuery and Snowflake, a model reading `raw.Orders` is **not** redirected to
the shadow of a model whose target is `raw.orders`. It never read that table.

Where a reference matches a routed upstream **only if case is ignored**, Rocky
refuses the run rather than guess. Redirecting it could read a table the model
never named. Leaving it alone would read production while the model writes its
shadow. Spell the reference exactly as the upstream's configured target — on
Snowflake, see the note below, where spelling it the same is not enough.

Deciding whether two *targets* collide is the opposite question. Rocky answers
it conservatively on every warehouse. Two selected models whose targets differ
only by identifier case are always treated as one object, and Rocky refuses the
run. Case sensitivity is connection state Rocky cannot observe: a Snowflake
account may set `QUOTED_IDENTIFIERS_IGNORE_CASE`, and a BigQuery dataset may be
created `is_case_insensitive`. Assuming the two targets are distinct could let
both models write the same shadow table with no error. Rename one target so the
two differ by more than case.

:::caution[Snowflake: quoting is a second identity axis]
Snowflake reads an *unquoted* identifier as upper case, and Rocky writes every
Snowflake target double-quoted. A target configured as `main.orders` is the
object `"main"."orders"`. A model's `FROM main.orders` names `MAIN.ORDERS`.
Those are two different tables with the same text.

Rocky resolves the reference the way Snowflake resolves it before matching. So
an upper-case target read by an unquoted reference routes normally — that is the
idiomatic Snowflake project. It also routes when the reference is written in a
different case, such as `FROM main.orders` against `MAIN.ORDERS`; that spelling
used to be refused. A lower-case or mixed-case target read by an unquoted
reference is refused instead of redirected.

Two ways to fix a refusal, either one:

- quote every component of the reference so it spells the target exactly —
  `FROM "main"."orders"`;
- or leave every component unquoted and spell the configured target in upper
  case — `MAIN.ORDERS` — so the reference resolves onto it.

A half-quoted reference such as `"main".orders` needs both: the quoted part must
match the target's spelling, and the unquoted part must be upper case in the
target. A name that is a reserved word (`ORDER`, `SELECT`, …) has only the first
remedy — it cannot be read unquoted at all.

On an account with Snowflake's default `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`,
a lower-case target read unquoted could not be read on a plain run either. The
refusal replaces a silent wrong read on a shape that was already broken. Two
settings can make the two spellings one object:
`QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` on the account, and
`CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE` on a catalog-linked database. Rocky
can observe the first on a connection, but that answer describes one request and
does not govern the next, so it asks for an unambiguous spelling instead.
:::

### CTE names on Snowflake

The same rule decides whether a CTE hides a bare table name. Rocky now folds an
unquoted CTE alias and an unquoted reference to upper case before comparing them,
and leaves a quoted one as written — the way Snowflake reads them under its
default `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`. Four pairs change answer:

| CTE alias | Reference | Before | Now |
|---|---|---|---|
| `orders` | `ORDERS` | free | hidden |
| `Orders` | `orders` | free | hidden |
| `"orders"` | `orders` | hidden | free |
| `orders` | `"orders"` | hidden | free |

An unquoted alias with an identically spelled unquoted reference still hides it,
which is the ordinary shape and does not change.

In a shadow or branch run the freed reference goes to the matcher, which routes
it or refuses it. `--defer` has no matcher and no refusal: the freed reference is
a table reference, and a bare name that matches a model name is that model, so
`--defer` qualifies it to that model's target.

With `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` a double-quoted identifier folds to
upper case too, so `WITH "orders"` does hide `FROM orders` and Rocky's answer is
wrong. On `--defer` that is silent, because nothing on that path can refuse.
Rocky can *observe* the setting on a connection, but that answer describes one
request and does not govern the next one, so it cannot decide this. The rule
before this one had the mirror of that problem under the default setting, so the
error now falls on an opt-out configuration rather than the common one. Tracked
in issue #1622.

## Shadow target rewriting

### Suffix mode (default)

Rocky appends a suffix to the table name. The default suffix is `_rocky_shadow`.

```
production: analytics.marts.fct_revenue
shadow:     analytics.marts.fct_revenue_rocky_shadow
```

### Schema override mode

```
production: analytics.marts.fct_revenue
shadow:     analytics.rocky_shadow.fct_revenue
```

Schema override keeps the table name clean and groups all shadow tables together.

### The two modes on a replication pipeline

Replication supports both modes, and they isolate differently. Under
`--shadow-suffix` the copy writes `<table><suffix>` **in the pipeline's own
target schema**, and reads the unsuffixed production source. Under
`--shadow-schema` the whole target schema moves and the table names stay as they
are. Prefer the schema override when you want the shadow objects kept away from
production tables rather than sitting beside them.

## Comparison engine

`rocky compare` reads the shadow tables back and diffs them against production.
It finds the targets differently per pipeline type: replication discovers them
from the source, and transformation reads them off its models. It then compares
each pair the same way. `rocky branch compare` does the same for a branch's
shadow schema.

The comparison evaluates three dimensions:

### Row count

Rocky counts the rows on each side and reports the difference:

```
shadow:     148,203 rows
production: 148,205 rows
diff:       -2 rows (-0.001%)
verdict:    PASS (within 1% warn threshold)
```

### Schema diff

Rocky compares column names, types, and order:

| Diff type | Description |
|-----------|-------------|
| `ColumnAdded` | Column in shadow but not production |
| `ColumnRemoved` | Column in production but not shadow |
| `ColumnTypeDiff` | Same column, different type |
| `ColumnOrderDiff` | Same columns, different order |

### Sample comparison

Rocky hashes a sample of rows from each side and compares the hashes. This finds value differences that the row count misses, because two tables can hold the same number of rows and different data.

## Thresholds

Set the pass, warn, and fail thresholds:

| Threshold | Default | Description |
|-----------|---------|-------------|
| `row_count_diff_pct_warn` | 0.01 (1%) | Warn if row count differs by more than this |
| `row_count_diff_pct_fail` | 0.05 (5%) | Fail if row count differs by more than this |
| `allow_column_order_diff` | true | Whether column reordering is acceptable |

## Verdicts

| Verdict | Meaning |
|---------|---------|
| **Pass** | All comparisons within thresholds |
| **Warn** | Minor differences detected (e.g., row count within warn threshold, column order change) |
| **Fail** | Significant differences (e.g., row count beyond fail threshold, missing columns, type changes) |

## Use cases

- **Schema migrations**: check that a column rename does not change the output
- **Logic changes**: compare the old and the new calculation, row for row
- **Adapter testing**: run a new warehouse adapter beside the production one
- **dbt migration**: compare Rocky's output against dbt's output with `rocky validate-migration`
