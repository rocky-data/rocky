# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. The direction was ruled on 2026-11-21: staged publish is opt-in, and the default stays in place. This note is the design for that ruling.

**Scope:** Issue #2612. A transformation run writes each model to production as it finishes. A later failure leaves production mixing new and old outputs.

---

## Context

### The problem

`rocky run` writes each model to its production target when that model finishes. The config docs say checks run after model execution (the `checks` field of `TransformationPipelineConfig`). So a failed downstream model, check or test leaves the upstream models already published. Consumers read a mix that no single run produced. The #2612 reproduction shows it on DuckDB: `stg_orders` holds the new text column, and `customer_orders` keeps its old columns.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe, not several tables changing together.
- `SwapNames::for_target` (`rocky-core/src/replace.rs`) already reserves a stage name beside each target.
- Shadow and branch runs point each model's upstream references at the shadow targets (`apply_shadow_rewrite` in `commands/run.rs`).

### Why the branch flow alone does not close the gap

The nearest path today is `rocky run --branch <name>`, then `rocky branch promote`. It has three limits.

**1. Promote can stop after a partial write.** `run_promote_apply` (`commands/branch.rs`) checks every destination first. It reads each branch source with `SELECT * FROM <source> LIMIT 0`. Then it runs one autocommit statement per target, in order, and stops at the first failure. Each statement is a copy: `CREATE OR REPLACE TABLE prod.t AS SELECT * FROM branch.t`. A copy evaluates the source again at write time. The `LIMIT 0` check cannot see an error that appears only when rows are read.

The test `promote_reports_all_targets_after_a_partial_write` (`commands/branch.rs`) proves this. Its second branch source is a view that does `CAST('bad' AS INTEGER)`. The `LIMIT 0` check passes. Then:

```
step 1  first   replaced            (production now holds the new rows)
step 2  second  not confirmed       (the copy failed while reading rows)
step 3  third   not attempted       (production table never created)
```

The test asserts `marts.first` holds the new value and `marts.third` does not exist.

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
- **Check.** Pipeline checks and error-severity tests (the `--tests enforce` set that the #2609 promote gate reads) run against the stages. A failure blocks the publish, just like a failed model.
- **Gate.** On any failure Rocky publishes nothing. It drops the stages it built in this run. A stage left behind would make the next replace refuse with `LeftoverStage` (`rocky-core/src/replace.rs`), so one failed scheduled run would block every later one.
- **Swap.** The stage is already a built table, so the swap never runs model SQL again. The failure in the test above cannot recur here. The swap is a rename where the warehouse allows it. DuckDB over Iceberg is the exception: it overwrites in place from the stage (D2). Views are re-created against the production names after the tables, as promote does today.
- **Swap failure.** Rocky stops and reports. It does not reverse the swaps already done in v1, because a reverse rename can fail too. The failed target keeps its old table through the ADR-REPLACE recovery.
- **Clean.** Rocky drops the moved-aside tables only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction. See D2.

### D2 — Per-warehouse atomicity

Staged publish always closes one gap. A failed model or check no longer publishes anything. Whether the **swap itself** is atomic depends on the warehouse.

**Grounded in tree** (the ADR-REPLACE and ADR-DUCKLAKE spikes):

| Warehouse | What the swap can do |
|---|---|
| PostgreSQL, Redshift | One table's replace runs as one implicit transaction (`rocky-postgres` connector). |
| SQL Server | One table's replace runs `sp_rename` inside one transaction. |
| DuckLake | A transaction that mixes DDL and DML rolls back fully. A transaction that writes the lake and the main database is refused. |
| DuckDB over Iceberg REST | One transaction can write two **existing** tables atomically. It cannot create two tables, and it cannot include a rename or a drop. So the swap there is an in-place overwrite of existing tables, and a first build of two new tables cannot be atomic. |
| Trino | Multi-statement transactions are refused ("only supports writes using autocommit"). Each table swaps by two renames. A reader can get "table not found" between them. No multi-table atomicity. |

**Not established in tree, needs a spike before a claim:**

| Warehouse | Assumed until a spike says otherwise |
|---|---|
| DuckDB (native file), PostgreSQL, Redshift, SQL Server | Several renames in one transaction. Only single-table replaces are probed. |
| Snowflake, Databricks, BigQuery, ClickHouse, Spark | One object at a time. Each object's replace is atomic (ADR-REPLACE), but no multi-table swap is assumed. |

Where no transaction can span the swap, a failure **during** the swap can still leave a mix. Staged publish shrinks the failure window from "any model or check" to "the swap statements", which run no model SQL. Rocky reports every target as `published`, `not confirmed` or `not published`, the three outcomes the promote test checks. The run output says whether the swap was one transaction or per object.

### D3 — Strategy scope (v1)

v1 stages `full_refresh` and `view` models, the set promote already handles. A staged run that selects any other strategy refuses at plan time and names the model. `materialized_view` and `dynamic_table` are a follow-up: a shadow run builds them, but how each warehouse renames them is not probed. A stage for `incremental`, `merge`, `delete_insert`, `microbatch` or `snapshot` must start as a copy of the current target. That is a clone or a full copy, a cost decision left to a follow-up.

### D4 — Config key

The key sits under the pipeline's existing `[execution]` table. It is an enum, not a bool, so a later mode can be added.

```toml
[pipeline.marts.execution]
publish = "staged"     # default: "in_place"
```

A `rocky run --publish staged|in_place` flag overrides it for one run. Other pipeline types share `ExecutionConfig`; they refuse `publish = "staged"` in v1. The names are proposed.

## Consequences

What changes for the user who opts in:

- A failed model, check or test leaves production as before the run. The run exits non-zero.
- A failed swap stops the run with some targets new and some old, and reports each one.
- Readers see the previous outputs until the swap.
- Storage peaks at about two copies of each rebuilt table.
- The run needs rights to create and rename tables in each target schema.
- The run JSON gains a publish section with a disposition per target. That is an output-schema change and goes through `just codegen`.
A user who does not opt in sees no change.

### What this does not close

- Atomic multi-table publish on warehouses without multi-statement DDL transactions.
- Incremental-family models under staged publish (D3).
- Two concurrent staged runs on one target. ADR-REPLACE already does not support two concurrent runs.

## Alternatives considered

- **Make staged the default.** Not chosen. The 2026-11-21 ruling keeps in place as the default.
- **Point users at the branch flow.** It does not close the gap (see Context).
- **Publish through views over versioned tables.** Still one object at a time on most warehouses.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"`. The second model fails, and both production tables keep their old shape.
- A failing check blocks the publish with every model built.
- A scripted adapter that fails at each swap step reports the right disposition for every target.
- A spike per warehouse in the "needs a spike" table before its row is stated as fact.

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server run several renames in one transaction. Only single-table replaces are probed.
- Whether v1 without incremental-family models is useful enough, since many pipelines are mostly incremental.

## Missing

A per-warehouse spike of a two-table rename swap inside one transaction, with a failure injected between the renames, would settle the D2 table.
