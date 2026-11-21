# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. The direction was ruled on 2026-11-21: staged publish is opt-in, and the default stays in place. This note is the design for that ruling.

**Scope:** Issue #2612. A transformation run writes each model to production as it finishes. A later failure leaves production mixing new and old outputs.

---

## Context

### The problem

`rocky run` writes each model to its production target when that model finishes. Checks run after model execution (`ChecksConfig` in `rocky-core/src/config.rs`). So a failed downstream model, or a failed check, leaves the upstream models already published. Consumers read a mix that no single run produced. The #2612 reproduction shows it on DuckDB: `stg_orders` holds the new text column, and `customer_orders` keeps its old columns.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe. A failed `SELECT` keeps the old table. It says nothing about several tables changing together. That is the gap here.
- `SwapNames::for_target` (`rocky-core/src/replace.rs`) already reserves a stage name beside each target, in the same schema.
- Shadow and branch runs rewrite each model's upstream references to the shadow targets (`apply_shadow_rewrite` in `commands/run.rs`). A downstream model then reads its staged upstream, not production.

### Why the branch flow alone does not close the gap

The nearest path today is `rocky run --branch <name>`, then `rocky branch promote`. It has three limits.

**1. Promote can stop after a partial write.** `run_promote_apply` (`commands/branch.rs`) checks every destination first. It reads each branch source with `SELECT * FROM <source> LIMIT 0`. Then it runs one autocommit statement per target, in order, and stops at the first failure. Each statement is a copy: `CREATE OR REPLACE TABLE prod.t AS SELECT * FROM branch.t`. A copy evaluates the source again at write time. The `LIMIT 0` check cannot see an error that appears only when rows are read.

The test `promote_reports_all_targets_after_a_partial_write` (`commands/branch.rs`) proves this. Its second branch source is a view that does `CAST('bad' AS INTEGER)`. The `LIMIT 0` check passes. Then:

```
step 1  first   replaced            (production now holds the new rows)
step 2  second  not confirmed       (the copy failed while reading rows)
step 3  third   not attempted       (production table never created)
```

The test asserts that `marts.first` holds the new value and `marts.third` does not exist. Production is a mix after an exit that reports failure.

**2. Promote covers two strategies only.** It refuses every strategy except `full_refresh` and `view` ("branch promotion is undefined for {other}"). A branch run refuses `incremental`, `merge`, `delete_insert`, `microbatch` and `snapshot`, because a branch target starts empty.

**3. It is two manual commands**, which a scheduled `rocky run` does not use.

## Decision

A new opt-in mode, `publish = "staged"`. The default, `"in_place"`, keeps today's behaviour exactly.

### D1 — Stage, test, then swap

```
build    every selected model  ->  <target>__rocky_stage   (production untouched)
check    every check           ->  runs against the stages
gate     any model or check failed?  ->  stop; production untouched
swap     every stage           ->  production, by rename, in dependency order
clean    drop the old tables the swap moved aside
```

- **Build.** The shadow rewrite points each downstream model at its upstream's stage.
- **Check.** A failed check blocks the publish, just like a failed model.
- **Gate.** On any failure Rocky publishes nothing and keeps the stages for inspection.
- **Swap.** Each stage moves into place **by rename, not by copy**. A rename does not evaluate the stage again, so the failure in the test above cannot recur here. Views are re-created against the production names after the tables, as promote does today.
- **Clean.** Rocky drops the moved-aside tables only after every swap succeeded.

Where the warehouse allows it, steps 4 and 5 run in one transaction. See D2.

### D2 — Per-warehouse atomicity

Staged publish always closes one gap. A failed model or check no longer publishes anything. Whether the **swap itself** is atomic depends on the warehouse.

**Grounded in tree** (the ADR-REPLACE and ADR-DUCKLAKE spikes):

| Warehouse | What the swap can do |
|---|---|
| PostgreSQL, Redshift | The replace already runs as one implicit transaction (`rocky-postgres` connector). Several renames in one transaction is the planned path. Redshift needs a spike. |
| SQL Server | The replace already runs `sp_rename` inside one transaction. Several targets in one transaction is the planned path. |
| DuckLake | A transaction that mixes DDL and DML rolls back fully. A transaction that writes the lake and the main database is refused. |
| DuckDB over Iceberg REST | One transaction can write two **existing** tables atomically. It cannot create two tables, and it cannot include a rename or a drop. So the swap there is an in-place overwrite of existing tables, and a first build of two new tables cannot be atomic. |
| Trino | Multi-statement transactions are refused ("only supports writes using autocommit"). Each table swaps by two renames. A reader can get "table not found" between them. No multi-table atomicity. |

**Not established in tree, needs a spike before a claim:**

| Warehouse | Assumed until a spike says otherwise |
|---|---|
| DuckDB (native file) | Likely one transaction across several renames. Not yet probed. |
| Snowflake, Databricks, BigQuery, ClickHouse, Spark | One object at a time. Each object's replace is atomic (ADR-REPLACE), but no multi-table swap is assumed. |

Where no transaction can span the swap, a failure **during** the swap can still leave a mix. Staged publish moves the failure window from "any model or any check" to "the rename statements". Those statements are short and evaluate nothing. Rocky reports every target as `published`, `not confirmed` or `not published`, the three outcomes the promote test checks. The run output says whether the swap was one transaction or per object.

### D3 — Strategy scope (v1)

v1 stages `full_refresh` and `view` models, the set promote already handles. A staged run that selects any other strategy refuses at plan time and names the model. `materialized_view` and `dynamic_table` are a follow-up: a shadow run builds them, but how each warehouse renames them is not probed. A stage for `incremental`, `merge`, `delete_insert`, `microbatch` or `snapshot` must start as a copy of the current target. That is a zero-copy clone on some warehouses and a full copy on others. It is a cost decision and a follow-up.

### D4 — Config key

The key sits under the pipeline's existing `[execution]` table. It is an enum, not a bool, so a later mode can be added.

```toml
[pipeline.marts.execution]
publish = "staged"     # default: "in_place"
```

A `rocky run --publish staged|in_place` flag overrides it for one run. The names are proposed and change if review asks.

## Consequences

What changes for the user who opts in:

- A failed model or check leaves production exactly as before the run. The run exits non-zero and lists the kept stages.
- Readers see the previous outputs until the swap.
- Storage peaks at about two copies of each rebuilt table.
- The run needs rights to create and rename tables in each target schema. A stage name is reserved beside each target, as ADR-REPLACE already does.
- The run JSON gains a publish section with a disposition per target. That is an output-schema change and goes through `just codegen`.
- On Trino and the one-object-at-a-time warehouses, the guarantee is narrower. It is "nothing publishes unless everything built and passed". It is not "all or nothing at the swap".

A user who does not opt in sees no change.

### What this does not close

- Atomic multi-table publish on warehouses without multi-statement DDL transactions.
- Incremental-family models under staged publish (D3).
- Two concurrent staged runs on one target. ADR-REPLACE already does not support two concurrent runs.

## Alternatives considered

- **Make staged the default.** Not chosen. The 2026-11-21 ruling keeps in place as the default.
- **Point users at the branch flow.** It does not close the gap (see Context).
- **Publish through views that point at versioned tables.** Every consumer would read through a view, and the swap would still be one object at a time on most warehouses.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"`. The second model fails, and both production tables keep their old shape.
- A failing check blocks the publish with every model built.
- A scripted adapter that fails at each swap step reports the right disposition for every target.
- A spike per warehouse in the "needs a spike" table before its row is stated as fact.

## Least confident

Whether DuckDB native, Redshift and SQL Server really run several renames in one transaction, since only single-table replaces are probed in tree.
Whether v1 without incremental-family models is useful enough, since many production pipelines are mostly incremental.

## Missing

A per-warehouse spike of a two-table rename swap inside one transaction, with a failure injected between the renames, would settle the D2 table.
