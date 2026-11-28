# ADR-STAGED-PUBLISH — Opt-in staged publish for transformation runs

**Status:** Proposed. Ruled 2026-11-21: opt-in; the default stays in place. Ruled 2026-11-28: v1 scope is option A, local state on one host, with manual recovery (D3).

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
check    an open publish record of this pipeline?  ->  refuse (D3)
build    every selected model  ->  <target>__rocky_stage  (production untouched)
test     per model, on its stage: volume check, then [[tests]]
gate     any failure?  ->  drop this run's stages, clear the record
swap     every stage   ->  production, in dependency order
         a swap fails?  ->  restore every swapped target now (D3)
clean    drop the old tables the swap moved aside, clear the record
```

- **Build.** Downstream models read upstream stages. This needs new routing: `apply_shadow_rewrite` routes to `shadow_target`, not to the `SwapNames` stage. An upstream this run does not build (not selected) has no stage, so its readers read its production table, which this run does not change. The skip gate is off under staged, as it is for shadow and branch runs (`SkipGateConfig::is_active`). The rewrite runs before the gate decides, so a skipped upstream would leave its readers pointing at a stage that does not exist.
- **Test.** Today's per-model checks, on the stage. Under the default `tests_on_run = "enforce"`, the gate sees model failures, volume breaches and error-severity test failures.
- **Gate.** On any failure Rocky publishes nothing. It drops each stage whose identity (D3) is this run, then clears the record. So no `LeftoverStage` and no open record blocks the next run. If a drop fails, the record stays, and runs refuse until `rocky publish restore` finishes it (D3, rows 1 and 2).
- **Containment.** `[resilience] contain_failures` (default `true`, #2617) still decides what builds: a failed model withholds its downstream closure, and unrelated models still build and test. Under staged, the gate then publishes none of them. Containment only makes the report complete.
- **Swap.** A table's swap never runs model SQL again. Views do: they are re-created against production after the tables, so a view can still fail here. Tables reuse `StagedSwap` renames, which need `rename_table_sql` on every adapter but Trino. DuckDB over Iceberg overwrites in place (D3).
- **Restore at once.** When a swap step fails, the same run restores every target it swapped, in reverse order, then exits non-zero. A restore needs no new data and no rebuild, so there is no reason to wait. A crash or a failed restore leaves the record open; a person then runs `rocky publish restore` or `rocky publish abandon` (D3).
- **Clean.** Moved-aside tables are dropped only after every swap succeeded.

Where the warehouse allows it, the swap and clean steps run in one transaction.

**What the rest of the run records** for a model built to its stage but not published (the gate stopped the run, or a restore put the old table back):

- **State store.** The model's `ModelExecution` gets status `not_published`, never `success`. The status is written in `output.rs` from the materialization; a volume breach or a failed test already writes `failed` there. Every reader compares against `"success"`: the skip gate needs a latest `success` execution (`skip_gate.rs`, clause C), `--retry-failed` keeps out only models with a `success` row (`run_retry.rs`), and `latest_success_executions` (`backfill.rs`) and `plan_cost.rs` look for `success`. So a later run rebuilds the model, and `--retry-failed` reruns it. The run's status is `Failure` or `PartialFailure`, as today.
- **Hooks.** Today `on_after_model_run` fires right after a model's write, when its volume check and tests passed (`execute_models_phase`). Under staged it fires for each model only after the whole run is published. A model that failed fires `on_model_error`, as today. A model that passed but was not published fires neither.
- **Governance.** The transformation path applies per-model `[governance.tags]` after the model phase, and only when no model failed (`run_transformation`). Under staged it runs after the publish, on production names. A gated or restored run applies none. The mask, classification, retention and role-grant reconcile runs only after the model phase of a replication `rocky run --all` or `--models` (`run_with_direct_gate`), which cannot be staged (D5). What a rename does to grants the warehouse holds is a D2 spike item.

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

This section says what the run records, who may act on it, and what a person runs after a crash.

**Scope (ruled 2026-11-28, option A).** v1 runs on local state on one host. After a crash, no run recovers on its own: every run refuses until a person runs `rocky publish restore` or `rocky publish abandon`. No step drops a production table by its production name.

**The record.** Before the build, Rocky writes a publish record for the pipeline. It holds the run id and one entry per target. An entry holds the target kind (table, view, or Iceberg table), whether the target existed before the run, a phase, and for a view its old and new definitions. The phases are:

```
built ──► swapping ──► swapped ──► cleaning ──► cleaned      (published)
               │            │
               └────────────┴──► restoring ──► restored      (not published)
```

Rocky writes `swapping`, `cleaning` and `restoring` as intents before the step starts, and the next phase after the step ends. The run is **published** once every entry is `swapped`. The run clears the record when every entry is `cleaned`, when the gate has dropped every stage, or when every swapped entry is `restored`.

**Where the record lives, and when it is durable.** The record is a state-store table. Each write is one redb commit, which returns only when durable. A step starts only after its intent is durable. A failed record write stops the run before the step.

`publish = "staged"` refuses on remote state (`[state] backend` other than `local`). A remote state object is not written durably before each step, so it cannot hold a durable intent.

**Who may act on a record.** A transformation run holds the state store's writer lock for its whole life. A second run, or a `rocky publish` command, cannot open the store while the first holds the lock. So only one process on the host can act on a record. One host and one local state path are what make this true.

The lock does not cover everything yet:

- `rocky backfill` and `rocky branch promote` open the store read-only today and take no writer lock. v1 makes each take the writer lock before it checks for an open record, and refuse when the store cannot be read.
- A second state file for the same targets (`[state] namespacing = "pipeline"`, `--state-namespace`, `--state-path`), or a second host with its own local state, has its own lock. Only the identity tag guards against it. v1 refuses `staged` when state namespacing is on.
- A statement sent before a crash can still be running in the warehouse. `restore` refuses while the warehouse reports a statement of the record's run id still running, where the adapter can tell (a D2 spike item); elsewhere this is a residual of option A.

**Table identity.** Working names carry no run id (decided below). So Rocky tags each stage with the run id, as a table property, or a comment where the warehouse has no properties. The tag moves with the table on rename. Before any drop or rename, `restore` reads it:

- a stage, or a target Rocky swapped in, is this record's only if its tag equals the record's run id;
- a backup is the old table only if its tag is not the record's run id.

Any other combination refuses and names the `DROP` or `ALTER` to run by hand. Existence probes alone cannot tell an old table from a new one; the tag can.

**Runs while a record is open.** Every run of a pipeline with an open record refuses, staged or in place. It names the run id and the two commands. Model-only runs (`rocky run --model`), `rocky backfill` and `rocky branch promote` name no pipeline but can write the same targets. v1 makes each of them refuse when an open record lists one of its targets. The tag covers any writer Rocky cannot see.

**Restore never drops by the production name.** A restore moves the run's table aside first: it renames the target to its stage name, renames the backup to the target, then drops the table under the stage name after its tag check. A target this run created is renamed to its stage name, then dropped. The production name only ever receives a rename.

**`rocky publish restore`.** It takes the writer lock, then works per entry. It probes target, stage and backup and reads their tags. "Run" means the tag equals the record's run id; "old" means it does not. It works in three passes:

1. Resolve each entry whose done-mark was lost. A `swapping` entry whose target carries the run's tag is `swapped` (rows 5 and 15). A `restoring` entry whose old table or view is back is `restored` (rows 12 and 20).
2. Decide once, for the whole record, whether the run is published.
3. Act on each entry by the rows below. Every state not listed refuses.

A published record is not rolled back: `restore` only finishes the clean (row 7). When every entry is closed, it clears the record.

**`rocky publish abandon`.** It clears the record and changes nothing in the warehouse. It prints each stage, backup and target with its tag, so a person can drop or rename what is left. It is the way out when `restore` refuses.

Tables that existed before the run:

| # | Phase | Warehouse state | Action |
|---|---|---|---|
| 1 | `built` | target old, stage run | drop the stage |
| 2 | `built` | target old, no stage | nothing (the gate dropped it) |
| 3 | `swapping` | target old, no backup, stage run | the step never ran, or its transaction rolled back: drop the stage |
| 4 | `swapping` | no target, backup old, stage run | rename the backup to target, drop the stage |
| 5 | `swapping` | target run, backup old, no stage | the swap ran and its done-mark was lost: it is `swapped` |
| 6 | `swapped`, not published | target run, backup old | restore: write `restoring`, rename the target to stage, rename the backup to target, drop the stage |
| 7 | `swapped` or `cleaning`, published | target run, backup old or gone | finish the clean: drop the backup; no restore |
| 8 | `cleaned` | target run, nothing beside it | nothing |
| 9 | `swapped`, not published | target old, no backup, stage run | the record ran ahead of a rolled-back transaction: drop the stage |
| 10 | `restoring` | target run, backup old, no stage | the restore never ran: as row 6 |
| 11 | `restoring` | no target, backup old, stage run | the move-aside ran: rename the backup to target, drop the stage |
| 12 | `restoring` | target old, stage run or none, no backup | the restore ran and its done-mark was lost: drop the stage if present; it is `restored` |
| 13 | `restored` | target old, nothing beside it | nothing |

Tables created by this run (no target before):

| # | Phase | Warehouse state | Action |
|---|---|---|---|
| 14 | `built` or `swapping` | no target, stage run or none | drop the stage if present |
| 15 | `swapping`, `swapped` (not published) or `restoring` | target run, no stage | restore: write `restoring` if not yet written, rename the target to stage, drop the stage |
| 16 | `restoring` or `restored` | no target, stage run or none | drop the stage if present; nothing else |
| 17 | published | target run | nothing to clean |

A one-statement swap (Snowflake `SWAP WITH`, ClickHouse `EXCHANGE TABLES`) leaves the old table under the stage name. Its rows wait for the D2 spike; until then, that state refuses.

**Views.** A view has no backup. The record holds its old definition, read before the swap, and the new one. The view carries the run's tag where the warehouse allows a comment on a view. Elsewhere "run" means its current definition equals the new one, and "old" that it equals the old one.

| # | Phase | Warehouse state | Action |
|---|---|---|---|
| 18 | `built` or `swapping` | view old, or no view when created by this run | the step never ran: nothing |
| 18a | `restoring` | view run | the restore never ran: as row 19 |
| 18b | `restored` | view old, or no view when created by this run | nothing |
| 19 | `swapping` or `swapped`, not published | view run | restore: write `restoring`, re-create the old definition, or drop a view this run created |
| 20 | `restoring` | view old, or no view when created by this run | the restore ran: it is `restored` |
| 21 | published | view run | nothing |

A view holds no data, so re-creating or dropping it by its production name loses nothing. An adapter that cannot read a view's definition refuses `staged` for that view at plan time.

**DuckDB over an Iceberg REST catalog.** DuckDB-Iceberg refuses a rename inside a write transaction, and its in-place overwrite keeps no backup (`recover_interrupted_swap` returns early for `InPlaceOverwrite`). So v1 stages an Iceberg pipeline only when every target of the pipeline is an existing table in one Iceberg catalog. One transaction then overwrites all of them from their stages. It either commits or rolls back, so there is nothing to restore, and `restore` only drops stages. Any other pipeline with an Iceberg target refuses `staged` at plan time, including one that mixes Iceberg and other targets: its Iceberg commit could not be restored.

**Trace: staged, then in place, then staged.** This is the sequence that existence probes alone get wrong.

```
S1 staged     A swapped, B built, dies   A = new(S1)   A__rocky_old = old A
                                         B = old B     B__rocky_stage = new(S1)
in place      record open  ->  refused   nothing written
S2 staged     record open  ->  refused   nothing written
restore       row 6 for A, row 1 for B   A = old A, B = old B, record cleared
```

Without the refusal, the in-place run writes newer A. `restore` would then move it aside as "new(S1)". With the tag, that A carries no S1 tag, so `restore` refuses instead. The tag also covers a write Rocky did not make: another tool, or an older binary.

**Trace: a crash mid-swap.** Targets A, B and C. A swaps. The process dies after B's move-aside, before its swap-in. C is never attempted.

```
A   swapped     A = new        A__rocky_old = old A
B   swapping    B missing      B__rocky_old = old B   B__rocky_stage = new B
C   built       C = old        C__rocky_stage = new C
```

Every later run refuses. `rocky publish restore` applies row 6 for A, row 4 for B and row 1 for C. A restore killed half-way resumes by rows 10 to 13 on the next `restore`. Production is then old A, B and C, and the record is clear. If any step fails, `restore` stops and the record stays.

If the run had not died, it would have done the same restore at once, before exiting non-zero.

**Clean-step failure.** If every swap succeeded but a backup drop failed (the `drop_backup` warning), the run is published. The record stays open, and `restore` finishes the clean step without a rollback (row 7).

**Decided 2026-11-22:**

- **Restore, not roll forward.** After a partial swap, Rocky restores each target from its backup. Cost: two renames and a drop per swapped target, and the run's new data is thrown away.
- **Refuse what does not match.** v1 refuses any table it cannot match to a recorded phase. Cost: a manual `DROP` after an unusual failure. A run id in working names would bind tables to runs, but it would change ADR-REPLACE's reserved names and need a schema listing; not in v1.

**Decided 2026-11-28 (option A):** local state on one host; after a crash every run refuses until `restore` or `abandon`; no step drops a production table by its production name. Cost: a person must act after every crash, and remote-state pipelines cannot use `staged` in v1.

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

`rocky publish restore` and `rocky publish abandon` (D3) take the same `--config` and pipeline selection as `rocky run`. Names are proposed.

## Consequences

For a user who opts in (nobody else sees a change):

- A failed model or test leaves production as before the run, and the run exits non-zero.
- A failed swap restores at once: production is as before the run, views included. A target this run created is dropped.
- After a crash or a failed restore, every run of the pipeline refuses until a person runs `rocky publish restore` or `rocky publish abandon`. A scheduled pipeline stops publishing until then.
- `staged` needs local state on one host. A pipeline on remote state cannot use it in v1.
- `--skip-unchanged` has no effect under staged.
- While a record is open, model-only runs, backfills and promotes that write one of its targets also refuse.
- Storage peaks at two copies per rebuilt table. It needs create and rename rights per target schema, and the right to set a table property or comment.
- The run JSON gains a per-target publish section (`just codegen`).

### Open questions

Proposed in this revision, for Hugo to confirm. None changes the 2026-11-22 or 2026-11-28 decisions.

- **First-time targets.** A restore drops a table this run created, because the pre-run state had none. The alternative is to keep it and report it.
- **New v1 refusals.** `staged` refuses on remote state, on a view whose definition the adapter cannot read, and on any pipeline with an Iceberg target that one transaction cannot overwrite whole. Model-only runs, `rocky backfill` and `rocky branch promote` refuse while an open record lists their target.
- **Command names.** `rocky publish restore` and `rocky publish abandon`.
- **Leftovers after `abandon`.** `abandon` leaves stages and backups with no record. A later staged run must refuse on any leftover stage or backup of its targets, not let `StagedSwap::execute` restore a backup on its own.
- **One transaction killed before `COMMIT`.** The record must mark entries `swapped` only after the warehouse commits, or `restore` must read a fully marked record whose targets are old as not published.
- **An untagged stage.** A kill between creating a stage and tagging it leaves a table no row matches; it refuses today.
- **Views by name.** A restore re-creates or drops a view by its production name. A view holds no data, so this is read as outside the "never drop by name" rule.
- **Unpublished models downstream.** How `not_published` reaches the run JSON, the SDK and Dagster (which emits one result per materialization) is not settled.
- **Run ids.** "Old" means "tag differs from the run id", so run ids must be unique for everything that tags the warehouse.
- **Skip gate off.** Turning it off is the simple answer. Running the gate before the rewrite would keep it, at the cost of routing decided per run.

## Alternatives considered

- **Make staged the default.** Not chosen: the 2026-11-21 ruling keeps in place.
- **Roll views forward.** Simpler than recording their definitions, but it breaks "production as before the run" for views. Not chosen: it would contradict the restore decision.
- **B: full scope with fencing.** Remote state, a lease on the record across hosts, and automatic recovery by the next run. Rejected 2026-11-28: the warehouse cannot stop a late statement from a run that stalled past its lease, so that run could still drop a table another run had restored. Not sized.
- **C: park #2612.** Rejected 2026-11-28: production keeps its mixed-version risk with no opt-in way out.
- **Automatic recovery by the next run on one host.** Rejected with option A: a person decides between restore and abandon after each crash.

## Validation

- DuckDB: the #2612 recipe under `publish = "staged"` leaves both production tables old.
- A failing test under the default `tests_on_run = "enforce"` blocks the publish. The next staged run starts with no open record.
- A scripted adapter killed after each intent write and after each step, restore steps included: every later run refuses; `rocky publish restore` reaches the row the D3 tables name, and the crash trace restores.
- No step issues a `DROP` on a production table name: a check over the scripted adapter's statements.
- `rocky publish abandon` clears the record, changes no table and lists what is left.
- Staged, then in place, then staged: both later runs refuse; a write by another tool makes `restore` refuse, not move it aside.
- Restore with a view and with a first-time target: the old view definition is back, and the new table is gone.
- `publish = "staged"` on remote state refuses at plan time.
- A model built but not published: its `ModelExecution` is not `success`, and `--retry-failed` reruns it.

## Least confident

- Whether DuckDB native, PostgreSQL, Redshift and SQL Server commit several renames in one transaction.
- Whether every warehouse can carry the identity tag through a rename, and clears it on any `CREATE OR REPLACE`. If one cannot, staged must refuse there.
- Whether v1 is useful when one incremental model refuses the run, `--skip-unchanged` is off, and a person must act after every crash.

## Missing

A per-warehouse spike would settle the D2 tables: a two-table rename swap in one transaction, with a failure injected between the renames, plus dependent views, grants, tags and the identity tag across the rename.

