# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. Ruled 2026-11-21: opt-in; the default stays in place.

**Scope:** Issue #2612.

---

## Context

### The problem

`rocky run` writes each model to production when it finishes. A failed downstream model leaves the upstream models published, a mix no single run produced (the #2612 DuckDB recipe).

What runs after a write today:

- The transformation path never reads `pipeline.checks` (`commands/run_local.rs`).
- Per model, right after its write, `run.rs` runs the volume check, then the model's `[[tests]]`. The volume check is only `[product.output.volume]`, on a product's output model.
- `[run] tests_on_run` defaults to `enforce` (#2716). So by default an error-severity test failure fails the model.

### What is already true

- [`ADR-REPLACE.md`](ADR-REPLACE.md) makes **one table's** replace safe, not several tables changing together.
- `SwapNames::for_target` reserves stage and backup names beside each target.
- Rename is built only on Trino: `SqlDialect::rename_table_sql` refuses by default.
- Shadow runs point upstream references at shadow targets (`apply_shadow_rewrite`, which routes to `shadow::shadow_target`).
- A transformation run holds the state store's writer lock for its whole life. `run_transformation` opens the `StateStore` before any model runs, and the store keeps the advisory lock on `<state path>.redb.lock` until it drops (`state::try_acquire_writer_lock`).
- Under `concurrency_control = "cas"` the periodic state uploader is off (`RemoteStateSession::start_periodic_uploader`). The remote state object then changes only at the end of a run.

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
recover  an open publish record of this pipeline?  ->  D3 first
build    every selected model  ->  <target>__rocky_stage   (production untouched)
test     per model, on its stage: volume check, then [[tests]]
gate     any failure?  ->  drop this run's stages, clear the record
swap     every stage   ->  production, in dependency order
         a swap fails?  ->  restore every swapped target now (D3)
clean    drop the old tables the swap moved aside, clear the record
```

- **Build.** Downstream models read upstream stages. This needs new routing: `apply_shadow_rewrite` routes to `shadow_target`, not to the `SwapNames` stage. An upstream this run does not build (not selected) has no stage, so its readers read its production table, which this run does not change. The skip gate is off under staged, as it is for shadow and branch runs (`SkipGateConfig::is_active`). The rewrite runs before the gate decides, so a skipped upstream would leave its readers pointing at a stage that does not exist.
- **Test.** Today's per-model checks, on the stage. Under the default `tests_on_run = "enforce"`, the gate sees model failures, volume breaches and error-severity test failures.
- **Gate.** On any failure Rocky publishes nothing. It drops each stage whose identity (D3) is this run, then clears the record. So no `LeftoverStage` and no open record blocks the next run. If a drop fails, the record stays and the next run finishes it (D3, rows 1 and 2).
- **Containment.** `[resilience] contain_failures` (default `true`, #2617) still decides what builds: a failed model withholds its downstream closure, and unrelated models still build and test. Under staged, the gate then publishes none of them. Containment only makes the report complete.
- **Swap.** A table's swap never runs model SQL again. Views do: they are re-created against production after the tables, so a view can still fail here. Tables reuse `StagedSwap` renames, which need `rename_table_sql` on every adapter but Trino. DuckDB over Iceberg overwrites in place (D3).
- **Restore at once.** When a swap step fails, the same run restores every target it swapped, in reverse order, then exits non-zero. A restore needs no new data and no rebuild, so there is no reason to wait. The next staged run recovers only what a crash or a failed restore left.
- **Clean.** Moved-aside tables are dropped only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction.

**What the rest of the run records** for a model built to its stage but not published (the gate stopped the run, or a restore put the old table back):

- **State store.** The model's `ModelExecution` gets status `not_published`, never `success`. The status is written in `output.rs` from the materialization; a volume breach or a failed test already writes `failed` there. Every reader compares against `"success"`: the skip gate needs a latest `success` execution (`skip_gate.rs`, clause C), `--retry-failed` keeps out only models with a `success` row (`run_retry.rs`), and `latest_success_executions` (`backfill.rs`) and `plan_cost.rs` look for `success`. So a later run rebuilds the model, and `--retry-failed` reruns it. The run's status is `Failure` or `PartialFailure`, as today.
- **Hooks.** Today `on_after_model_run` fires right after a model's write, when its volume check and tests passed (`execute_models_phase`). Under staged it fires for each model only after the whole run is published. A model that failed fires `on_model_error`, as today. A model that passed but was not published fires neither.
- **Governance.** The transformation path applies per-model `[governance.tags]` after the model phase, and only when no model failed (`run_transformation`). Under staged it runs after the publish, on production names. A gated or restored run applies none. The transformation path applies no grants itself; what a rename does to grants the warehouse holds is a D2 spike item.

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
| Snowflake, ClickHouse | `ALTER TABLE … SWAP WITH …` (Snowflake) and `EXCHANGE TABLES … AND …` (ClickHouse, `Atomic` engine) swap two names in one statement. One table then has no missing window. Neither spans several tables. After it, the stage name holds the old table, so recovery must read identity, not names. |
| Databricks, BigQuery, Spark | One object at a time. No multi-table swap is assumed. |
| DuckDB over Iceberg REST | One transaction overwriting more than two existing tables. |

The spike also checks, per warehouse, what moves with a rename:

- **Dependent views.** PostgreSQL and Redshift bind a view to the table, not to its name. Renaming the target to `__rocky_old` carries every dependent view with it. A view Rocky does not manage then reads the old table, and the backup drop fails on the dependency (`CASCADE` would drop the view). Redshift late-binding views (`WITH NO SCHEMA BINDING`) bind by name.
- **Grants and tags.** A stage is a new table, so it may lack the grants and tags of the target. Some warehouses copy them on request (for example Snowflake `COPY GRANTS`).
- **The identity tag (D3).** It must survive a rename and a swap. A `CREATE OR REPLACE` of the target by any writer must clear it or change it.

Where no transaction spans the swap, a failure during it leaves a mix until the run restores it. Rocky reports each target as `published`, `not confirmed` or `not published`.

### D3 — Recovery after a partial swap

This section says what the run records, who may act on it, and what recovery does in each state.

**The record.** Before the build, Rocky writes a publish record for the pipeline. It holds the run id, an owner (host, process, lease expiry) and one entry per target. An entry holds the target kind (table, view, or Iceberg table), whether the target existed before the run, a phase, and for a view its old definition. Phases are `built`, `swapping`, `swapped`, `cleaning` and `cleaned`. Rocky writes `swapping` and `cleaning` as intents before the step starts, and the next phase after it ends. The run is **published** once every entry is `swapped`. The record is cleared when every entry is `cleaned`, after the gate drops the stages, or after a restore completes.

**Where the record lives, and when it is durable.** A step starts only after its intent is durable. A failed record write stops the run before the step.

- Local state: a state-store table. Each write is one redb commit, which returns only when durable.
- Remote state: the state object changes only at the end of a run, and not at all if the process dies. So the record is a separate object beside the state object, written with a conditional put at every phase change (the create-once pattern of the freeze markers, `freeze_marker.rs`). The end-of-run upload is not relied on. A backend with no conditional put wired (today `valkey`) refuses `publish = "staged"`.

**Who may act on a record.**

- Same host, same state path: a second run cannot open the state store while the first holds the writer lock. So it cannot act on a live run's record.
- Across hosts, on remote state: the lock is per host. So the record carries a lease. The owner renews it with a conditional put on its own version. Another run treats an unexpired lease as live and refuses, naming the owner. After expiry, a run takes the lease with a conditional put. The old owner's next record write then fails, and it stops before its next step. Residual: a step the old owner started before expiry can still finish.
- This lease covers only the publish record. It is not the state-blob lease that ADR-CONCURRENCY reserves as `concurrency_control = "lease"`; the state blob stays under CAS.

**Table identity.** Working names carry no run id (decided below). So Rocky tags each stage with the run id, as a table property, or a comment where the warehouse has no properties. The tag moves with the table on rename. Before any drop or rename, recovery reads it:

- a stage, or a target Rocky swapped in, is this record's only if its tag equals the record's run id;
- a backup is the old table only if its tag is not the record's run id.

Any other combination refuses and names the `DROP` or `ALTER` to run by hand. Existence probes alone cannot tell an old table from a new one; the tag can.

**In-place runs while a record is open.** An `in_place` run of a pipeline with an open record refuses. It names the run id and the fix: run once with `--publish staged`, which recovers first.

**What recovery does.** Recovery uses its own probes, not `StagedSwap::execute`, which would refuse with `LeftoverStage`. Per entry, it probes target, stage and backup and reads their tags. "Run" means the tag equals the record's run id; "old" means it does not. Every state not listed refuses.

Tables that existed before the run:

| # | Phase | Warehouse state | Action |
|---|---|---|---|
| 1 | `built` | target old, stage run | drop the stage |
| 2 | `built` | target old, no stage | nothing (the gate dropped it) |
| 3 | `swapping` | target old, no backup, stage run | the step never ran, or its transaction rolled back: drop the stage |
| 4 | `swapping` | no target, backup old, stage run | rename the backup to target, drop the stage |
| 5 | `swapping` | target run, backup old, no stage | the swap ran; its done-mark was lost: treat as row 6 or 7 |
| 6 | `swapped`, not published | target run, backup old | restore: drop the target, rename the backup to target |
| 7 | `swapped` or `cleaning`, published | target run, backup old or gone | finish the clean: drop the backup; no restore |
| 8 | `cleaned` | target run, nothing beside it | nothing |
| 9 | `swapped`, not published | target old, no backup, stage run | the record ran ahead of a rolled-back transaction: drop the stage |

Tables created by this run (no target before):

| # | Phase | Warehouse state | Action |
|---|---|---|---|
| 10 | `built` or `swapping` | no target, stage run or none | drop the stage if present |
| 11 | `swapping` or `swapped`, not published | target run, no stage | restore: drop the target (the pre-run state had no table) |
| 12 | published | target run | nothing to clean: clear the entry |

A one-statement swap (Snowflake `SWAP WITH`, ClickHouse `EXCHANGE TABLES`) leaves the old table under the stage name. Its rows wait for the D2 spike; until then, that state refuses.

Views have no backup. The record holds each view's old definition, read before the view is re-created. Restore re-creates the view from it, and drops a view the run created. An adapter that cannot read a view's definition refuses `staged` for that view at plan time.

DuckDB over an Iceberg REST catalog has no rename, and its in-place overwrite keeps no backup (`recover_interrupted_swap` returns early for `InPlaceOverwrite`). So v1 stages an Iceberg target only when every target exists and one transaction overwrites all of them from their stages. That transaction either commits or rolls back, so there is nothing to restore. Recovery only drops stages. Any other Iceberg pipeline refuses `staged` at plan time.

**Trace: staged, then in place, then staged.** This is the sequence that existence probes alone get wrong.

```
S1 (staged)    A swapped, then the process dies    A = new(S1)  A__rocky_old = old
in place run   record open  ->  refused            nothing written
S2 (staged)    recovers first: row 6               A = old, record cleared
```

Without the refusal, the in-place run writes newer A. S2 would then drop it as "new(S1)". With the tag, that A carries no S1 tag, so S2 refuses instead. The tag also covers a write Rocky did not make: another tool, or an older binary.

**Trace: a crash mid-swap.** Targets A, B and C. A swaps. The process dies after B's move-aside, before its swap-in. C is never attempted.

```
A   swapped     A = new        A__rocky_old = old A
B   swapping    B missing      B__rocky_old = old B   B__rocky_stage = new B
C   built       C = old        C__rocky_stage = new C
```

The next staged run first recovers: row 6 for A, row 4 for B, row 1 for C. Production is then old A, B and C. It then rebuilds under a new record. If any recovery step fails, it stops.

If the run had not died, it would have done the same restore at once, before exiting non-zero.

**Clean-step failure.** If every swap succeeded but a backup drop failed (the `drop_backup` warning), the run is published. The next run finishes the clean step and does not restore (row 7).

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
- A failed swap restores at once: production is as before the run, views included. A target this run created is dropped. If the process dies or a restore step fails, the next staged run recovers. If recovery cannot match a table, a person must act.
- `--skip-unchanged` has no effect under staged.
- While a record is open, `in_place` runs of that pipeline refuse.
- Storage peaks at two copies per rebuilt table. It needs create and rename rights per target schema, and the right to set a table property or comment.
- The run JSON gains a per-target publish section (`just codegen`).

### Open questions

Proposed in this revision, for Hugo to confirm. None changes the two 2026-11-22 decisions.

- **First-time targets.** A restore drops a table this run created, because the pre-run state had none. The alternative is to keep it and report it.
- **New v1 refusals.** `staged` refuses on `valkey` state, on a view whose definition the adapter cannot read, and on any Iceberg pipeline that one transaction cannot overwrite whole.
- **Skip gate off.** Turning it off is the simple answer. Running the gate before the rewrite would keep it, at the cost of routing decided per run.

## Alternatives considered

- **Make staged the default.** Not chosen: the 2026-11-21 ruling keeps in place.
- **Roll views forward.** Simpler than recording their definitions, but it breaks "production as before the run" for views. Not chosen: it would contradict the restore decision.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"` leaves both production tables old.
- A failing test under the default `tests_on_run = "enforce"` blocks the publish. The next staged run starts with no open record.
- A scripted adapter killed after each intent write and after each step: recovery reaches the row the D3 tables name, and the crash trace restores.
- Remote state: a run killed mid-swap, with no end-of-run upload, leaves a record that a run on another host recovers.
- Staged, then in place, then staged: the in-place run refuses; a write by another tool makes S2 refuse, not drop.
- Restore with a view and with a first-time target: the old view definition is back, and the new table is gone.
- Concurrent recovery: two hosts on one remote state; one takes the lease, the other refuses; a lease taken after expiry stops the old owner at its next step.
- A model built but not published: its `ModelExecution` is not `success`, and `--retry-failed` reruns it.

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server commit several renames in one transaction.
- Whether every warehouse can carry the identity tag through a rename, and clears it on any `CREATE OR REPLACE`. If one cannot, staged must refuse there.
- Whether v1 is useful when one incremental model refuses the run, and `--skip-unchanged` is off.
- The lease timings (renew interval, expiry), and the residual step an expired owner can still finish.

## Missing

A per-warehouse spike would settle the D2 tables: a two-table rename swap in one transaction, with a failure injected between the renames, plus dependent views, grants, tags and the identity tag across the rename.
