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
        │ 2. rocky run compares both tables:
        │    row counts and schema
        ▼
  PASS / WARN / FAIL, with the diffs
        │
        ▼
  Rocky drops the shadow table unless you pass --keep-shadow
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
- **Quality, snapshot and load pipelines** refuse it. Rocky does not rewrite
  their targets. A quality run would check the tables it lists in production,
  and a quarantine mode would write there too. Rocky stops before it reads or
  writes anything, and the message names the pipeline and the flag.
- **Seeds** cause a `--dag` run to be refused along with the rest. `rocky seed`
  itself has no shadow mode and always writes its configured target.

A stored `rocky plan --shadow` carries its routing into `rocky apply`.
The apply run compares before it cleans up, as a direct shadow run does.

To quality-check a branch, list the branch's tables in a quality pipeline. Set
`schema` to the branch schema, `branch__<name>`, and run the pipeline without
the flag. Its quarantine tables then land in the branch schema, and production
stays untouched.
:::

## Who owns a shadow object, and how long it lives

The two modes answer this differently, and everything below follows from it.

```
  rocky run --shadow          the objects belong to THIS RUN
      built  ─▶  compared  ─▶  dropped
      A name that is already taken is refused, not overwritten.

  rocky run --branch <name>   the objects belong to the BRANCH
      built  ─▶  compared  ─▶  kept
      The next run on that branch replaces them. That is the point.
```

An unnamed shadow run is disposable. It creates its objects and drops them after a completed comparison, whether the verdict is `pass`, `warn`, `fail` or `no_baseline`. A failed verdict makes the run exit non-zero, and the run output keeps the comparison result. A second run starts clean.

Pass `--keep-shadow` to keep the objects, for a separate `rocky compare` or to inspect a failed comparison. The next one-off shadow run refuses those occupied names, so remove them first.

A model write error or a comparison query error can leave the target's state uncertain. Rocky may keep those objects. The next shadow run refuses the occupied name and prints the `DROP` that clears it.

Rocky will not delete an object it did not create. Before writing, it checks every derived shadow target in the warehouse. If one exists, the run is refused and names the object, the model and the `DROP` statement.

A named branch is a persistent workspace. Its objects survive the run, and the next run on that branch replaces them. Rocky does **not** check them for prior occupancy and tracks no per-object ownership there. Give a branch a namespace nothing else writes to.

Cleanup is best effort. A failed drop is a run warning, not a failure, because the comparison already happened.

## Models a shadow run refuses

Shadow and branch runs reject `content_addressed`, `time_interval` and the
incremental family (`incremental`, `merge`, `delete_insert`, `microbatch`).
An `ephemeral` model is skipped, not refused: it builds nothing, and the reads
inside its inlined SQL are routed to shadow targets like any other read.

- `content_addressed` and `time_interval` models persist object-storage or
  partition-state identities. Rewriting the warehouse target alone cannot
  isolate those yet.
- An **incremental** model appends to whatever its target already holds. A
  shadow target starts empty while production holds full history, so the first
  run compares a partial table against a complete one, and every later run
  appends again and drifts further. The comparison would get worse, not better.
  Run the model as `full_refresh` to shadow it, or compare it in production
  through `rocky compare`.

Rocky also refuses a derived shadow target that matches a configured production
target, or that matches another selected model's shadow target.

## How Rocky redirects a read to a shadow table

A model that reads another selected model's table is routed to that upstream's
shadow target. This holds whether or not the model declares the dependency in
`depends_on`. Rocky matches on the upstream's configured
`catalog.schema.table`, so it redirects a physical read too.

A reference could resolve to more than one selected upstream, for example two targets that share a table name in different catalogs, read by a bare or partial name. Rocky refuses the run rather than guess.

### Identifier case

Matching follows the warehouse's own rule for identifier case, per component.

| Warehouse | Case is part of object identity | What Rocky matches |
|---|---|---|
| DuckDB, Databricks, Trino | No | `Orders` and `orders` name one table, and either spelling is redirected |
| BigQuery | Yes | The reference must match exactly |
| Snowflake | Yes, and an unquoted reference is read as upper case | The reference must name the same object the target does |

On BigQuery and Snowflake, a model reading `raw.Orders` is **not** redirected to
the shadow of a model whose target is `raw.orders`. It never read that table.

Where a reference matches a routed upstream **only if case is ignored**, Rocky refuses the run. Redirecting it could read a table the model never named. Leaving it alone would read production while the model writes its shadow. Spell the reference exactly as the upstream's configured target.

Two *targets* that differ only by identifier case are always treated as one object, and Rocky refuses the run, on every warehouse. Case sensitivity is connection state Rocky cannot observe (Snowflake `QUOTED_IDENTIFIERS_IGNORE_CASE`, BigQuery `is_case_insensitive`), and assuming the targets differ could let both models write one shadow table. Rename one target so they differ by more than case.

:::caution[Snowflake: quoting is a second identity axis]
Snowflake reads an *unquoted* identifier as upper case, and Rocky writes every Snowflake target double-quoted. A target `main.orders` is the object `"main"."orders"`. A model's `FROM main.orders` names `MAIN.ORDERS`. Those are two tables.

Rocky resolves the reference the way Snowflake does before matching. An upper-case target read by an unquoted reference routes normally, in any spelling case. A lower-case or mixed-case target read by an unquoted reference is refused. Fix it either way:

- quote every component so the reference spells the target exactly: `FROM "main"."orders"`
- or leave every component unquoted and spell the configured target in upper case: `MAIN.ORDERS`

A half-quoted reference such as `"main".orders` needs both. A reserved word (`ORDER`, `SELECT`, …) has only the first remedy.

With the default `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`, a lower-case target read unquoted could not be read on a plain run either, so the refusal replaces a silent wrong read. `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` on the account, or `CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE` on a catalog-linked database, can make the two spellings one object. Rocky sees the first only per request, so it asks for an unambiguous spelling.
:::

### CTE names on Snowflake

A CTE can hide a bare table name. Rocky folds an unquoted CTE alias and an unquoted reference to upper case before comparing them, and leaves a quoted one as written, as Snowflake does under the default `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`. With `TRUE`, a quoted identifier folds to upper case too. Rocky cannot observe the setting for the statement it decides, so it refuses the pairs that differ between settings:

| CTE alias | Reference | `FALSE` (default) | `TRUE` | Rocky |
|---|---|---|---|---|
| `orders` | `orders` | hidden | hidden | hidden |
| `Orders` | `orders` | hidden | hidden | hidden |
| `"orders"` | `orders` | free | hidden | refuses |
| `orders` | `"orders"` | free | hidden | refuses |
| `"Orders"` | `"orders"` | free | hidden | refuses |

The refusal applies only when the reference would otherwise name an upstream that the command redirects: a deferred model on `--defer`, a routed upstream on a shadow or branch run, an upstream on `branch promote`, or a recorded upstream on `rocky replay --execute`. Spell the CTE alias and the reference alike (both unquoted, or both quoted with the same case), or rename the CTE.

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

Under `--shadow-suffix` the copy writes `<table><suffix>` **in the pipeline's own target schema** and reads the unsuffixed production source. Under `--shadow-schema` the whole target schema moves and table names stay. Prefer the schema override to keep shadow objects away from production tables.

## Comparison engine

`rocky run --shadow` compares its targets with production before cleanup.
The result appears in text output and in JSON under `shadow_comparison`.
`rocky compare` performs the same comparison on objects you kept with
`--keep-shadow`. It fails if no tables were selected.
When production has no target, Rocky reports `no_baseline` for that target.
It does not fail the run. Rocky drops that shadow unless you use `--keep-shadow`.
A failed threshold does fail the run, after Rocky drops the shadow objects it created.
If Rocky cannot confirm whether production exists, the comparison errors before a verdict and may leave the shadow.
Do not combine `--shadow` with `--resume` or `--resume-latest`.
Remove any retained shadow objects, then restart with `--shadow` without a resume flag.

It finds the targets differently per pipeline type: replication discovers them
from the source, and transformation reads them off its models. It then compares
each pair the same way. `rocky branch compare` does the same for a branch's
shadow schema.

The comparison evaluates two dimensions: the row count and the schema.

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

### No value comparison yet

The comparison reads no rows. Two tables can hold the same number of rows and
different data, and the verdict cannot see that. For a row-level diff, use
[`rocky preview diff --algorithm bisection`](/concepts/preview-internals/#--algorithm-bisection).

## Thresholds

`rocky run --shadow` uses the defaults below. `rocky compare` takes others as
JSON, for example `--thresholds '{"row_count_diff_pct_fail": 0.10}'`.

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
| **Error** | Rocky could not read a table or its schema on one side. The count it could not read is `null`, and `reasons` in the JSON says why. Rocky counts an error as a failure. |

## Use cases

- **Schema migrations**: check that a column rename does not change the output
- **Logic changes**: compare the old and the new calculation, row for row
- **Adapter testing**: run a new warehouse adapter beside the production one
- **dbt migration**: compare Rocky's output against dbt's output with `rocky validate-migration`
