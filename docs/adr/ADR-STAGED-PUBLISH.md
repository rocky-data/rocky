# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. Ruled 2026-11-21: staged publish is opt-in; the default stays in place.

**Scope:** Issue #2612.

---

## Context

### The problem

`rocky run` writes each model to production when it finishes. A failed downstream model leaves the upstream models published, a mix no single run produced (the #2612 DuckDB recipe).

What runs after a write today:

- The transformation run path never reads `pipeline.checks` (`commands/run_local.rs`); that field's doc comment has no call site there.
- Per model, right after its write, `run.rs` runs the volume check, then the model's `[[tests]]`.
- The tests run only under `[run] tests_on_run`, which defaults to `off` (`rocky-core/src/config.rs`). So by default no test runs.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe, not several tables changing together.
- `SwapNames::for_target` (`rocky-core/src/replace.rs`) reserves a stage and a backup name beside each target.
- Swap by rename is built only on Trino. `SqlDialect::rename_table_sql` refuses by default (`rocky-core/src/traits.rs`), and only `rocky-trino` overrides it.
- Shadow runs point upstream references at shadow targets (`apply_shadow_rewrite`).

### Why the branch flow alone does not close the gap

The nearest path is `rocky run --branch <name>`, then `rocky branch promote`.

**1. Promote can stop after a partial write.** `run_promote_apply` (`commands/branch.rs`) probes each source with `LIMIT 0`, then writes one target at a time and stops at the first failure. Each write copies, by `CREATE OR REPLACE TABLE prod.t AS SELECT * FROM branch.t` or, on Trino, by `promote_swap` from the same `SELECT`. So the source is evaluated again at write time.

The test `promote_reports_all_targets_after_a_partial_write` (`commands/branch.rs`) proves this. Its second source, a view doing `CAST('bad' AS INTEGER)`, passes `LIMIT 0`:

```
step 1  first   replaced            (production now holds the new rows)
step 2  second  not confirmed       (the copy failed while reading rows)
step 3  third   not attempted       (production table never created)
```

**2. Promote covers only `full_refresh` and `view`.** Branch runs refuse incremental strategies.

**3. It is two manual commands**, unused by a scheduled `rocky run`.

## Decision

A new opt-in mode, `publish = "staged"`. The default, `"in_place"`, is unchanged.

### D1 — Stage, test, then swap

```
build    every selected model  ->  <target>__rocky_stage   (production untouched)
test     per model, on its stage: volume check, then [[tests]]
gate     any failure?  ->  drop this run's stages; production untouched
swap     every stage   ->  production, in dependency order
clean    drop the old tables the swap moved aside
```

- **Build.** Downstream models read their upstream's stage, through the shadow rewrite.
- **Test.** Today's per-model checks, on the stage. Under the default `tests_on_run = "off"`, the gate sees only model failures and volume breaches.
- **Gate.** On any failure Rocky publishes nothing and drops this run's stages, so no `LeftoverStage` blocks the next run.
- **Swap.** A table's stage is already built, so its swap never runs model SQL again. Views are not: they are re-created against the production names after the tables, which binds their SQL again, so a view can still fail at swap time. Tables reuse ADR-REPLACE's `StagedSwap` renames, which needs `rename_table_sql` on every adapter but Trino. DuckDB over Iceberg cannot rename, so it overwrites in place from the stage.
- **Clean.** Rocky drops the moved-aside tables only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction.

### D2 — Per-warehouse atomicity

A failed model or test publishes nothing, on every warehouse. Whether the **swap itself** is atomic varies.

**Grounded in tree** (the ADR-REPLACE and ADR-DUCKLAKE spikes):

| Warehouse | What is proven |
|---|---|
| PostgreSQL, Redshift | One table's replace runs as one implicit transaction. |
| SQL Server | One table's replace runs `sp_rename` inside one transaction. |
| DuckLake | A DDL-plus-DML transaction rolls back fully. Lake plus main database in one transaction is refused. |
| DuckDB over Iceberg REST | One transaction can write two **existing** tables. It cannot create two tables, rename or drop. |
| Trino | No multi-statement transactions. Two renames per table, with a "table not found" window. |

**Needs a spike before a claim:**

| Warehouse | Assumed until a spike says otherwise |
|---|---|
| DuckDB (native), PostgreSQL, Redshift, SQL Server | Several renames in one transaction. |
| Snowflake, Databricks, BigQuery, ClickHouse, Spark | One object at a time. No multi-table swap is assumed. |

Where no transaction spans the swap, a failure during it still leaves a mix. Rocky reports each target as `published`, `not confirmed` or `not published`.

### D3 — Recovery after a partial swap

**The record.** Before the build, Rocky writes a publish record to the state store, keyed by pipeline and run id. It lists every target with its stage and backup names; stages exist from the build step. Rocky clears the record only after a clean run: every swap and every drop succeeded.

**Trace.** Targets A, B and C. A swaps. B fails between `move_aside` and `swap_in`, and its restore fails too (`ReplaceError::RestoreFailed`). C is never attempted.

```
after A swaps   A = new         A__rocky_old = old A
after B fails   B is missing    B__rocky_old = old B   B__rocky_stage = new B
C untouched     C = old         C__rocky_stage = new C
```

Production mixes new A with old C; B does not resolve.

**The next staged run, by default, restores first.**

1. It reads its pipeline's newest unclean record and acts only on names in it.
2. **B:** a backup whose target is missing is restored, never dropped, as `StagedSwap::execute` already does.
3. **A:** new A is renamed to its recorded stage name, then `A__rocky_old` back to A.
4. Only after every restore passed, it drops the recorded stages of A, B and C.
5. Production is the consistent pre-run state. The run then proceeds as normal under a new record.

If any restore fails, the run stops and drops nothing. A working table not in the record still refuses, as ADR-REPLACE requires. A record is matched to its run by run id, and each recorded table is checked against the warehouse's table identity where one exists, so a stale record cannot reach another run's tables. An in-place run never drops working tables; it refuses and names the `DROP` to run by hand.

**Decisions for Hugo:**

- **(a) Restore by default, or drop.** Restore costs two renames per swapped target and briefly puts old outputs back. Dropping the backups before the rebuild keeps new A and is faster, but if the rebuild fails, the pre-run copy of A is lost and production stays mixed.
- **(b) The record format.** Run id alone is simple but trusts names. A table identity is safer, but not every warehouse exposes one; the fallback must be chosen.

### D4 — Strategy scope (v1)

v1 stages `full_refresh` and `view` models. It refuses `incremental`, `merge`, `time_interval`, `delete_insert`, `microbatch`, `snapshot`, `materialized_view`, `dynamic_table` and `content_addressed`. `ephemeral` models have no object, so they are skipped. The incremental family needs a stage copied from the target, a follow-up.

**One refused model refuses the whole run** at plan time, naming it. A pipeline that mixes strategies cannot use v1.

### D5 — Config key

An enum under the pipeline's `[execution]` table:

```toml
[pipeline.marts.execution]
publish = "staged"     # default: "in_place"
```

`rocky run --publish staged|in_place` overrides it. Other pipeline types share `ExecutionConfig` and refuse `"staged"`. Names are proposed.

## Consequences

For a user who opts in:

- A failed model or test leaves production as before the run, and the run exits non-zero.
- A failed swap leaves a mix, reported per target. It lasts until the next staged run succeeds, which is not guaranteed. D3 assumes that run can restore.
- Storage peaks at two copies of each rebuilt table.
- The run needs rights to create and rename tables in each target schema.
- The run JSON gains a per-target publish section (`just codegen`).

Nothing changes for anyone else.

### Open questions

- **Unverified:** `run.rs` records a built model as `Built` for the skip gate. If that persists after a failed gate, a later `--skip-unchanged` run could skip the model and leave production old. `Built` should be recorded only after the swap.
- Two concurrent staged runs on one target (unsupported, as in ADR-REPLACE).

## Alternatives considered

- **Make staged the default.** Not chosen: the 2026-11-21 ruling keeps in place as the default.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"` leaves both production tables old.
- A failing test under `tests_on_run = "enforce"` blocks the publish.
- A scripted adapter failing at each swap step: correct reports, and the D3 trace recovers.

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server commit several renames in one transaction. Only single-table replaces are probed.
- Whether v1 is useful when one incremental model refuses the whole run.

## Missing

A per-warehouse spike of a two-table rename swap in one transaction, with a failure injected between the renames, would settle the D2 table.
