---
title: Shadow Mode
description: Run a change against copies of your tables, then diff them against production
sidebar:
  order: 14
---

Shadow mode writes a pipeline's output to shadow tables instead of production tables, or alongside them, so you see what a change does before it reaches the tables people query. Use it for new logic, schema migrations and adapter upgrades.

## What a shadow run does

```
  rocky run --shadow
     1. rewrite every target name with the _rocky_shadow suffix
     2. run the pipeline as usual into the shadow targets
          analytics.marts.fct_revenue_rocky_shadow   shadow
          analytics.marts.fct_revenue                production, untouched
     3. compare both tables: row counts and schema ──▶ PASS / WARN / FAIL
     4. drop the shadow table unless you pass --keep-shadow
```

:::caution[Where shadow isolation applies]
Isolation covers a plain `rocky run` over a **transformation** pipeline, and a **replication** pipeline with `--shadow-suffix`, `--shadow-schema` or a branch. Everywhere else Rocky refuses the flag instead of running without isolation:

- **`rocky run --dag`** refuses `--shadow` and `--branch`. The DAG runs each model as its own sub-run, so a read of an upstream built by the same run is not redirected to its shadow target. Run the shadow pipeline without `--dag`. A `--dag` run is also refused for seeds. `rocky seed` has no shadow mode and always writes its configured target.
- **Quality, snapshot and load pipelines** refuse it, because Rocky does not rewrite their targets. The message names the pipeline and the flag.

A stored `rocky plan --shadow` carries its routing into `rocky apply`, which compares before it cleans up. To quality-check a branch, list the branch's tables in a quality pipeline, set `schema` to `branch__<name>`, and run it without the flag.
:::

## Who owns a shadow object, and how long it lives

```
  rocky run --shadow          the objects belong to THIS RUN
      built  ─▶  compared  ─▶  dropped
      A name that is already taken is refused, not overwritten.

  rocky run --branch <name>   the objects belong to the BRANCH
      built  ─▶  compared  ─▶  kept
      The next run on that branch replaces them. That is the point.
```

An unnamed shadow run drops its objects after a completed comparison, whatever the verdict (`pass`, `warn`, `fail`, `no_baseline`). A failed verdict makes the run exit non-zero, and the run output keeps the result. Pass `--keep-shadow` to keep the objects for a separate `rocky compare`. The next one-off shadow run refuses those occupied names, so remove them first.

A model write error or a comparison query error can leave the target's state uncertain, and Rocky may keep those objects. Rocky never deletes an object it did not create: before writing, it checks every derived shadow target. If one exists, the run is refused and names the object, the model and the `DROP` statement.

A named branch is a persistent workspace. Rocky does **not** check its objects for prior occupancy and tracks no per-object ownership there. Give a branch a namespace nothing else writes to. Cleanup is best effort: a failed drop is a run warning, not a failure.

## Models a shadow run refuses

Shadow and branch runs reject `content_addressed`, `time_interval` and the incremental family (`incremental`, `merge`, `delete_insert`, `microbatch`). An `ephemeral` model is skipped, not refused: its inlined reads are routed to shadow targets like any other read.

- `content_addressed` and `time_interval` models persist object-storage or partition-state identities. Rewriting the warehouse target alone cannot isolate them yet.
- An **incremental** model appends to whatever its target holds. A shadow target starts empty while production holds full history, so the comparison is partial and drifts further each run. Run the model as `full_refresh` to shadow it, or compare it in production with `rocky compare`.

Rocky also refuses a derived shadow target that matches a configured production target, or another selected model's shadow target.

## How Rocky redirects a read to a shadow table

A model that reads another selected model's table is routed to that upstream's shadow target, whether or not it declares `depends_on`. Rocky matches on the upstream's configured `catalog.schema.table`, so a physical read is redirected too. A reference that could resolve to more than one selected upstream (two targets sharing a table name in different catalogs, read by a bare or partial name) is refused rather than guessed.

### Identifier case

Matching follows the warehouse's own rule for identifier case, per component.

| Warehouse | Case is part of object identity | What Rocky matches |
|---|---|---|
| DuckDB, Databricks, Trino | No | `Orders` and `orders` name one table, and either spelling is redirected |
| BigQuery | Yes | The reference must match exactly |
| Snowflake | Yes, and an unquoted reference is read as upper case | The reference must name the same object the target does |

On BigQuery and Snowflake, a model reading `raw.Orders` is **not** redirected to the shadow of a model whose target is `raw.orders`: it never read that table. A reference that matches a routed upstream **only if case is ignored** is refused, because redirecting could read a table the model never named and leaving it alone would read production. Spell the reference exactly as the upstream's configured target.

Two *targets* that differ only by case are always one object, and Rocky refuses the run on every warehouse. Case sensitivity is connection state Rocky cannot observe (Snowflake `QUOTED_IDENTIFIERS_IGNORE_CASE`, BigQuery `is_case_insensitive`). Rename one target.

:::caution[Snowflake: quoting is a second identity axis]
Snowflake reads an *unquoted* identifier as upper case, and Rocky writes every Snowflake target double-quoted. A target `main.orders` is `"main"."orders"`, while `FROM main.orders` names `MAIN.ORDERS`. Rocky resolves the reference as Snowflake does. An upper-case target read unquoted routes normally in any spelling case. A lower-case or mixed-case target read unquoted is refused. Fix it either way:

- quote every component so the reference spells the target exactly: `FROM "main"."orders"`
- or leave every component unquoted and spell the configured target in upper case: `MAIN.ORDERS`

A half-quoted reference such as `"main".orders` needs both. A reserved word (`ORDER`, `SELECT`, …) has only the first remedy. `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE` on the account, or `CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE` on a catalog-linked database, can make the two spellings one object. Rocky sees that only per request, so it asks for an unambiguous spelling.
:::

### CTE names on Snowflake

A CTE can hide a bare table name. Rocky folds an unquoted CTE alias and an unquoted reference to upper case before comparing them, and leaves a quoted one as written (Snowflake's default `QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`). With `TRUE`, a quoted identifier folds to upper case too. Rocky cannot observe the setting, so it refuses the pairs that differ between settings:

| CTE alias | Reference | `FALSE` (default) | `TRUE` | Rocky |
|---|---|---|---|---|
| `orders` | `orders` | hidden | hidden | hidden |
| `Orders` | `orders` | hidden | hidden | hidden |
| `"orders"` | `orders` | free | hidden | refuses |
| `orders` | `"orders"` | free | hidden | refuses |
| `"Orders"` | `"orders"` | free | hidden | refuses |

The refusal applies only when the reference would otherwise name an upstream the command redirects: a deferred model on `--defer`, a routed upstream on a shadow or branch run, an upstream on `branch promote`, or a recorded upstream on `rocky replay --execute`. Spell the CTE alias and the reference alike, or rename the CTE.

## Shadow target rewriting

| Mode | Production | Shadow |
|---|---|---|
| Suffix (default `_rocky_shadow`) | `analytics.marts.fct_revenue` | `analytics.marts.fct_revenue_rocky_shadow` |
| Schema override | `analytics.marts.fct_revenue` | `analytics.rocky_shadow.fct_revenue` |

Schema override keeps table names clean and groups all shadow tables together. Prefer it to keep shadow objects away from production tables.

### The two modes on a replication pipeline

On a replication pipeline, `--shadow-suffix` writes `<table><suffix>` **in the pipeline's own target schema** and reads the unsuffixed production source. `--shadow-schema` moves the whole target schema and keeps table names.

## Comparison engine

`rocky run --shadow` compares its targets with production before cleanup. The result appears in text output and in JSON under `shadow_comparison`. `rocky compare` does the same on objects you kept with `--keep-shadow`, and fails if no tables were selected. `rocky branch compare` does it for a branch's shadow schema. Replication discovers the targets from the source, and transformation reads them off its models.

- No production target: the verdict is `no_baseline` for that target. It does not fail the run.
- A failed threshold fails the run, after Rocky drops the shadow objects it created.
- If Rocky cannot confirm whether production exists, the comparison errors before a verdict and may leave the shadow.
- Do not combine `--shadow` with `--resume` or `--resume-latest`. Remove retained shadow objects, then restart without a resume flag.

The comparison covers two dimensions: row count and schema.

### Row count

Rocky counts the rows on each side:

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

The comparison reads no rows: two tables can hold the same number of rows and different data. For a row-level diff, use [`rocky preview diff --algorithm bisection`](/concepts/preview-internals/#--algorithm-bisection).

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
