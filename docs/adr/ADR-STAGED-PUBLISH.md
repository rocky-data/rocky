# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. Ruled 2026-11-21: opt-in; the default stays in place.

**Scope:** Issue #2612.

---

## Context

### The problem

`rocky run` writes each model to production when it finishes. A failed downstream model leaves the upstream models published, a mix no single run produced (the #2612 DuckDB recipe).

What runs after a write today:

- The transformation path never reads `pipeline.checks` (`commands/run_local.rs`).
- Per model, right after its write, `run.rs` runs the volume check, then the model's `[[tests]]`.
- Tests run only under `[run] tests_on_run`, default `off`, so by default none run.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe, not several tables changing together.
- `SwapNames::for_target` reserves stage and backup names beside each target.
- Rename is built only on Trino: `SqlDialect::rename_table_sql` refuses by default.
- Shadow runs point upstream references at shadow targets (`apply_shadow_rewrite`).

### Why the branch flow alone does not close the gap

The nearest path is `rocky run --branch <name>`, then `rocky branch promote`.

**1. Promote can stop after a partial write.** `run_promote_apply` (`commands/branch.rs`) probes each source with `LIMIT 0`, then writes one target at a time and stops at the first failure. Each write copies, by `CREATE OR REPLACE TABLE prod.t AS SELECT * FROM branch.t` or, on Trino, by `promote_swap` from the same `SELECT`. So the source is evaluated again at write time.

The test `promote_reports_all_targets_after_a_partial_write` proves it. Its second source, a view doing `CAST('bad' AS INTEGER)`, passes `LIMIT 0`:

```
first   replaced        (new rows in production)
second  not confirmed   (the copy failed reading rows)
third   not attempted
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

- **Build.** Downstream models read upstream stages (shadow rewrite).
- **Test.** Today's per-model checks, on the stage. Under the default `tests_on_run = "off"`, the gate sees only model failures and volume breaches.
- **Gate.** On any failure Rocky publishes nothing and drops this run's stages, so no `LeftoverStage` blocks the next run.
- **Swap.** A table's swap never runs model SQL again. Views do: they are re-created against production after the tables, so a view can still fail here. Tables reuse `StagedSwap` renames, which need `rename_table_sql` on every adapter but Trino. DuckDB over Iceberg overwrites in place from the stage instead.
- **Clean.** Moved-aside tables are dropped only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction.

### D2 — Per-warehouse atomicity

A failed model or test publishes nothing anywhere. Whether the **swap** is atomic varies.

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

**The record.** Before the build, Rocky writes a publish record to the state store, keyed by pipeline and run id. It holds a phase per target: `built`, `swapped`, `cleaned`. Rocky writes each phase as an intent before the step starts, and marks it done after. The record is cleared only after every target reaches `cleaned`.

**Ownership.** Working names (`<t>__rocky_stage`, `<t>__rocky_old`) carry no run id, so a name does not prove this run made the table. Recovery acts only where the warehouse state matches the recorded phase exactly (below). Any other combination refuses and names the `DROP` to run by hand.

**Recovery uses its own probes,** not `StagedSwap::execute`, which would refuse B's stage with `LeftoverStage`. It runs the same existence probe on target, stage and backup, then:

| Recorded phase | Warehouse state | Action |
|---|---|---|
| `built` | target old, stage present | drop the stage |
| swap started | target missing, backup, stage | rename backup to target, then drop the stage |
| swap started | target present, backup, no stage | the swap finished: treat as `swapped` |
| `swapped`, some target not swapped | target new, backup present | restore: drop the new target, rename the backup to target |
| `swapped`, every target swapped | target new, backup present | the swap finished: drop the backup |
| `cleaned` | target new, nothing beside it | nothing |

Restore is the default. Views have no backup; once re-created, they are `swapped`.

**Trace.** Targets A, B and C. A swaps. B fails between `move_aside` and `swap_in`, and its restore fails too (`ReplaceError::RestoreFailed`). C is never attempted.

```
A   swapped        A = new       A__rocky_old = old A
B   swap started   B missing     B__rocky_old = old B   B__rocky_stage = new B
C   built          C = old       C__rocky_stage = new C
```

The next staged run first recovers: it drops new A and renames `A__rocky_old` back, restores old B, and drops the stages of B and C. Production is then old A, B and C. It then rebuilds under a new record. If any recovery step fails, it stops.

**Clean-step failure.** If every swap succeeded but a backup drop failed (the `drop_backup` warning), the swap finished: the next run finishes the clean step and does not restore.

An in-place run never drops working tables.

**Decided 2026-11-22:**

- **Restore, not roll forward.** After a partial swap, Rocky restores each target from its backup. Cost: a reverse rename and a drop per swapped target, and the run's new data is thrown away.
- **Refuse what does not match.** v1 refuses any table it cannot match to a recorded phase. Cost: a manual `DROP` after an unusual failure. A run id in working names would bind tables to runs, but it would change ADR-REPLACE's reserved names and need a schema listing; not in v1.

### D4 — Strategy scope (v1)

v1 stages `full_refresh` and `view` models. It refuses `incremental`, `merge`, `time_interval`, `delete_insert`, `microbatch`, `snapshot`, `materialized_view`, `dynamic_table` and `content_addressed`. `ephemeral` models are skipped. The incremental family needs a stage copied from the target, a follow-up.

**One refused model refuses the whole run** at plan time, naming it. 

### D5 — Config key

An enum under the pipeline's `[execution]` table:

```toml
[pipeline.marts.execution]
publish = "staged"     # default: "in_place"
```

`rocky run --publish staged|in_place` overrides it. Other pipeline types share `ExecutionConfig` and refuse `"staged"`. Names are proposed.

## Consequences

For a user who opts in (nobody else sees a change):

- A failed model or test leaves production as before the run, and the run exits non-zero.
- A failed swap leaves a mix, reported per target, until the next staged run recovers it to the pre-run state. D3 assumes recovery succeeds; if a restore step fails, a person must act.
- Storage peaks at two copies per rebuilt table. It needs create and rename rights per target schema.
- The run JSON gains a per-target publish section (`just codegen`).


### Open questions

- **Unverified:** `run.rs` marks a built model `Built` for the skip gate. If that persists past a failed gate, `--skip-unchanged` could skip the model and leave production old.
- Two concurrent staged runs on one target: unsupported.

## Alternatives considered

- **Make staged the default.** Not chosen: the 2026-11-21 ruling keeps in place.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"` leaves both production tables old.
- A failing test under `tests_on_run = "enforce"` blocks the publish.
- A scripted adapter failing at each swap step: correct reports, and the D3 trace restores.

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server commit several renames in one transaction.
- Whether v1 is useful when one incremental model refuses the run.

## Missing

A per-warehouse spike of a two-table rename swap in one transaction, with a failure injected between the renames, would settle the D2 table.
