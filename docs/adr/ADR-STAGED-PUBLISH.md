# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. Ruled 2026-11-21: staged publish is opt-in; the default stays in place.

**Scope:** Issue #2612.

---

## Context

### The problem

`rocky run` writes each model to its production target when that model finishes. A failed downstream model leaves the upstream models published, a mix no single run produced. The #2612 reproduction shows it on DuckDB: `stg_orders` holds the new text column, and `customer_orders` keeps its old columns.

What runs after a write today:

- The transformation run path never reads `pipeline.checks` (`commands/run_local.rs`). The doc comment on that field ("run after model execution") has no call site there.
- Per model, right after its write, `run.rs` runs the volume check, then the model's `[[tests]]`.
- The tests run only under `[run] tests_on_run`, which defaults to `off` (`rocky-core/src/config.rs`). By default, no test runs.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe, not several tables changing together.
- `SwapNames::for_target` (`rocky-core/src/replace.rs`) reserves a stage and a backup name beside each target.
- Swap by rename is built only on Trino. `SqlDialect::rename_table_sql` refuses by default (`rocky-core/src/traits.rs`), and only `rocky-trino` overrides it.
- Shadow and branch runs point each model's upstream references at the shadow targets (`apply_shadow_rewrite` in `commands/run.rs`).

### Why the branch flow alone does not close the gap

The nearest path today is `rocky run --branch <name>`, then `rocky branch promote`. It has three limits.

**1. Promote can stop after a partial write.** `run_promote_apply` (`commands/branch.rs`) checks every destination first. It reads each branch source with `SELECT * FROM <source> LIMIT 0`. Then it writes one target at a time and stops at the first failure. Each write copies: `CREATE OR REPLACE TABLE prod.t AS SELECT * FROM branch.t`, or on a staged-swap dialect (Trino), `promote_swap`, which builds its stage from the same `SELECT`. Either way the source is evaluated again at write time, which `LIMIT 0` cannot fully test.

The test `promote_reports_all_targets_after_a_partial_write` (`commands/branch.rs`) proves this. Its second source is a view doing `CAST('bad' AS INTEGER)`, which passes `LIMIT 0`:

```
step 1  first   replaced            (production now holds the new rows)
step 2  second  not confirmed       (the copy failed while reading rows)
step 3  third   not attempted       (production table never created)
```

**2. Promote covers two strategies only**, `full_refresh` and `view`. A branch run also refuses the incremental strategies, because a branch target starts empty.

**3. It is two manual commands**, unused by a scheduled `rocky run`.

## Decision

A new opt-in mode, `publish = "staged"`. The default, `"in_place"`, keeps today's behaviour exactly.

### D1 — Stage, test, then swap

```
build    every selected model  ->  <target>__rocky_stage   (production untouched)
test     per model, on its stage: volume check, then [[tests]]
gate     any failure?  ->  drop this run's stages; production untouched
swap     every stage   ->  production, in dependency order
clean    drop the old tables the swap moved aside
```

- **Build.** Downstream models read their upstream's stage, through the shadow rewrite.
- **Test.** The same per-model checks as today, run against the stage. With `tests_on_run = "off"`, the default, the gate sees only model failures and volume breaches. Staged publish keeps that default.
- **Gate.** On any failure Rocky publishes nothing and drops the stages it built in this run. A stage left behind would make the next replace refuse with `LeftoverStage`.
- **Swap.** The stage is already a built table, so the swap never runs model SQL again. It reuses ADR-REPLACE's `StagedSwap` renames per target. That needs `rename_table_sql` on every adapter that opts in: new work everywhere except Trino. DuckDB over Iceberg cannot rename, so it overwrites in place from the stage (D2). Views are re-created against the production names after the tables.
- **Clean.** Rocky drops the moved-aside tables only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction.

### D2 — Per-warehouse atomicity

A failed model or test no longer publishes anything, on every warehouse. Whether the **swap itself** is atomic depends on the warehouse.

**Grounded in tree** (the ADR-REPLACE and ADR-DUCKLAKE spikes):

| Warehouse | What is proven |
|---|---|
| PostgreSQL, Redshift | One table's replace runs as one implicit transaction. |
| SQL Server | One table's replace runs `sp_rename` inside one transaction. |
| DuckLake | A transaction that mixes DDL and DML rolls back fully. Writing the lake and the main database in one transaction is refused. |
| DuckDB over Iceberg REST | One transaction can write two **existing** tables atomically. It cannot create two tables, rename or drop. A first build of two new tables cannot be atomic. |
| Trino | Multi-statement transactions are refused. Each table swaps by two renames, with a "table not found" window between them. |

**Needs a spike before a claim:**

| Warehouse | Assumed until a spike says otherwise |
|---|---|
| DuckDB (native), PostgreSQL, Redshift, SQL Server | Several renames in one transaction. |
| Snowflake, Databricks, BigQuery, ClickHouse, Spark | One object at a time. No multi-table swap is assumed. |

Where no transaction spans the swap, a failure during it still leaves a mix, but in a window of statements that run no model SQL. Rocky reports every target as `published`, `not confirmed` or `not published`, and says whether the swap was one transaction or per object.

### D3 — Recovery after a partial swap

After a swap stops partway, the warehouse holds:

```
swapped targets     new table   + <target>__rocky_old    (backup)
failed target       old table   (if StagedSwap's rollback succeeds)
unswapped targets   old table   + <target>__rocky_stage  (stage)
```

The next replace of any of them refuses (`LeftoverBackup`, `LeftoverStage`). Production stays mixed until someone acts.

**Proposed rule.** Before the first swap, Rocky records the run's stage and backup names in the state store. The next staged run of that pipeline reads the record first. It drops each recorded backup beside a swapped target and each recorded stage beside an unswapped one. Then it rebuilds and publishes every selected model as normal. A table at a working name that is not in the record still refuses, as ADR-REPLACE requires. An in-place run never drops working tables. It refuses as today, and the message names the `DROP` to run by hand.

**Decision for Hugo:** this rule drops a backup automatically, so the pre-run copy of a swapped target is lost. ADR-REPLACE's default keeps it and refuses.

### D4 — Strategy scope (v1)

v1 stages `full_refresh` and `view` models. It refuses `incremental`, `merge`, `time_interval`, `delete_insert`, `microbatch`, `snapshot`, `materialized_view`, `dynamic_table` and `content_addressed`. `ephemeral` models have no object, so they are skipped. The incremental family needs a stage copied from the target first, a cost decision for a follow-up.

**One refused model refuses the whole run** at plan time, and the message names it. A pipeline that mixes strategies cannot use staged publish in v1.

### D5 — Config key

An enum under the pipeline's `[execution]` table:

```toml
[pipeline.marts.execution]
publish = "staged"     # default: "in_place"
```

A `rocky run --publish staged|in_place` flag overrides it for one run. Other pipeline types share `ExecutionConfig` and refuse `"staged"` in v1. The names are proposed.

## Consequences

For a user who opts in:

- A failed model or test leaves production as before the run, and the run exits non-zero.
- A failed swap leaves some targets new and some old, reported per target, until the next staged run (D3).
- Readers see the previous outputs until the swap.
- Storage peaks at about two copies of each rebuilt table.
- The run needs rights to create and rename tables in each target schema.
- The run JSON gains a per-target publish section, an output-schema change through `just codegen`.

Nothing changes for anyone else.

### Open questions

- **Unverified:** after a model builds, `run.rs` records it as `Built` for the skip gate. If a staged model is recorded that way and the gate then fails, a later `--skip-unchanged` run could skip it, leaving production on the old version. The publish must record `Built` only after its swap.
- Atomic multi-table publish where no multi-statement DDL transaction exists.
- Two concurrent staged runs on one target, which ADR-REPLACE already does not support.

## Alternatives considered

- **Make staged the default.** Not chosen: the 2026-11-21 ruling keeps in place as the default.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"`. The second model fails, and both production tables keep their old shape.
- A failing test under `tests_on_run = "enforce"` blocks the publish.
- A scripted adapter that fails at each swap step reports every target correctly, and the next staged run recovers (D3).

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server commit several renames in one transaction. Only single-table replaces are probed.
- Whether v1 is useful when one incremental model refuses the whole run.

## Missing

A per-warehouse spike of a two-table rename swap in one transaction, with a failure injected between the renames, would settle the D2 table.
