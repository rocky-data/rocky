# ADR-REPLACE — Staged replace: a full refresh never leaves a missing table

**Status:** Accepted (advisor-ruled 2026-11-11, Hugo to confirm)

**Scope:** Closes audit finding **RD-009** (package A-09). Sets the replace contract that the open-storage work (L-1 Iceberg through DuckDB, L-2 DuckLake) builds on.

---

## Context

### The problem (RD-009)

A full refresh replaces a table's contents with the result of a `SELECT`. On most warehouses Rocky emits one atomic statement, `CREATE OR REPLACE TABLE t AS …`. If the `SELECT` fails, the warehouse keeps the old table.

Trino has no portable `CREATE OR REPLACE`. Its dialect sets `SqlDialect::full_refresh_needs_predrop()` to `true` (`rocky-trino/src/dialect.rs`), and every full refresh runs two autocommit statements:

```
DROP TABLE IF EXISTS t      -- commits
CREATE TABLE t AS SELECT …  -- fails  →  t is gone, old data is gone
```

The spike (below, probe J) reproduces this exactly on DuckDB-Iceberg too: drop, then a failing CTAS, leaves no table.

### Every site that drops before it creates

| Site | Dialects | What happens on a failed create |
|---|---|---|
| `sql_gen::generate_transformation_sql_with_warehouse`, `FullRefresh` arm (pre-drop when `full_refresh_needs_predrop`) | Trino | Target missing |
| `commands/run.rs` replication copy, pre-drop before `full_refresh` | Trino | Target missing |
| `commands/run.rs` replication drift `DriftAction::DropAndRecreate` (`drift::generate_drop_table_sql`, then the full-refresh CTAS) | **All** | Target missing |
| `commands/branch.rs::promote_pre_drop_sql` (branch promote of a table) | Trino | Production table missing |
| `quarantine.rs`, `QuarantineMode::Drop`, `StatementRole::PredropValid` | Trino | Valid table missing |
| `seeds.rs`, `SeedStrategy::DropAndRecreate` (drop, `CREATE TABLE`, then batched `INSERT`s) | **All** | Seed missing or partial |

Already safe, and kept as they are:

- Databricks, Snowflake, BigQuery, DuckDB, ClickHouse, Spark: `CREATE OR REPLACE TABLE … AS`, one statement.
- PostgreSQL and Redshift: `DROP …; CREATE … AS` sent as one simple-query string, which the server runs as one implicit transaction (`rocky-postgres/src/connector.rs` module docs).
- SQL Server: stage, then `sp_rename` inside one transaction (`rocky-sqlserver/src/dialect.rs::create_table_as`).
- The `drop_existing_kind` table/view switch: already refuses unless the adapter offers `WarehouseAdapter::atomic_drop_and_create`.

### Spike: what the engines actually do (2026-11-11)

Run locally, credential-free: Lakekeeper (REST catalog) + PostgreSQL + SeaweedFS (S3 API) in Docker, DuckDB 2.0.0 (the version Rocky bundles: crate `duckdb 1.20000.x`) with the `iceberg` and `httpfs` extensions, and `trinodb/trino` 484. Each probe ran in a fresh namespace and a fresh connection, then checked the table from a third connection.

**DuckDB → Iceberg REST catalog**

| Probe | Result |
|---|---|
| `CREATE SCHEMA`, CTAS, `INSERT`, `INSERT … BY NAME`, `UPDATE`, `DELETE`, `MERGE` | Work |
| `CREATE OR REPLACE TABLE … AS` | Refused: "CREATE OR REPLACE not supported in DuckDB-Iceberg" |
| `BEGIN; DELETE; INSERT; COMMIT` | Atomic. One catalog commit; the current snapshot jumps past the empty intermediate snapshot |
| Same, with the `INSERT` failing | Old rows kept |
| `BEGIN; ALTER … ADD/DROP COLUMN; DELETE; INSERT BY NAME; COMMIT` | Atomic, including drop and re-add of a column under a new type |
| Same, failing `INSERT` after the ALTERs | Old schema and rows kept |
| `BEGIN; DROP TABLE t; CREATE TABLE t …` | Refused: "Cannot create table deleted within a transaction" |
| Any transaction with `RENAME` or `DROP` | Refused: cannot commit atomically when it mixes table updates with rename/drop |
| Transaction creating two tables | Refused |
| Transaction writing two existing tables | Works |
| Autocommit `DROP`, then failing CTAS | **Table gone** (RD-009 reproduced) |
| Four concurrent appends to one table | All four commit |
| `ALTER COLUMN TYPE` INT→BIGINT | Works; INT→VARCHAR refused |

Version floor: DuckDB 1.2 cannot attach an Iceberg REST catalog; DuckDB 1.4.1 attaches but has no `DELETE` on it; 1.5.0 and 2.0.0 pass every probe above.

**Trino 484**

| Probe | `memory` connector | `iceberg` connector (REST) |
|---|---|---|
| `CREATE OR REPLACE TABLE … AS` | Refused: "This connector does not support replacing tables" | Works, and replaces a different schema |
| `CREATE OR REPLACE` with a failing `SELECT` | n/a | Old table kept (atomic) |
| `ALTER TABLE a RENAME TO b` | Works | Works |
| Rename onto an existing name | Refused: "already exists" | Refused |
| `ALTER TABLE IF EXISTS missing RENAME …` | No-op | No-op |
| Multi-statement transaction | Refused: "Catalog only supports writes using autocommit" | — |
| `system.metadata.catalogs.connector_name` | Names the connector of every catalog | |

The `hive` connector (file metastore) behaves like `memory`: `CREATE OR REPLACE` is refused, rename works. Trino's Hive connector on AWS Glue is documented to refuse table rename; there the swap fails at the move-aside step with the target untouched (see Consequences).

## Decision

**One line:** every full refresh goes through one replace contract. A dialect either replaces atomically in one call, or Rocky runs a staged swap whose every failure point leaves the old table, or the new one, under the target name or a name Rocky restores from.

### D1 — A typed replace capability replaces `full_refresh_needs_predrop`

`SqlDialect::full_refresh_needs_predrop() -> bool` is removed. A dialect declares, per target catalog, how it replaces a table:

```rust
pub enum TableReplace {
    /// `create_table_as` already replaces atomically (CREATE OR REPLACE,
    /// or a script the warehouse runs as one transaction).
    Atomic,
    /// No atomic replace: Rocky builds a stage, then swaps by rename.
    StagedSwap,
}

fn table_replace(&self, catalog: &str) -> TableReplace;            // default Atomic
fn rename_table_sql(&self, from: &str, to: &str, if_exists: bool)  // default refuses
    -> AdapterResult<String>;
```

- Keyed on the catalog, because one dialect can serve catalogs with different capabilities (D3 is the first case).
- Call sites `match` on it with no `_` arm, so a third variant fails to compile until every site handles it.
- Removing the boolean, rather than adding beside it, made every former call site fail to compile until it was moved onto this contract.

Trino returns `StagedSwap` for every catalog. Every other shipped dialect keeps the default `Atomic`.

### D2 — The staged swap (`rocky-core/src/replace.rs`)

Names: `<table>__rocky_stage` and `<table>__rocky_old`, beside the target. A name longer than 128 characters is shortened with a stable hash (the same scheme SQL Server's `__rocky_new` uses). A target whose own name ends in either suffix is refused before any statement runs.

`StagedSwap::statements()` is the happy path, four statements:

```
1. CREATE TABLE stage AS <select>                     (never replaces: a leftover stage fails it)
2. ALTER TABLE IF EXISTS target RENAME TO backup      (never overwrites: a leftover backup fails it)
3. ALTER TABLE stage RENAME TO target
4. DROP TABLE IF EXISTS backup
```

Run one after another, a failure at any step leaves the old table at the target name or the backup name, never neither. `rocky plan`, `emit_sql` and `estimate` show this list, so their signatures do not change.

`StagedSwap::execute()` is what `rocky run`, promote and quarantine `drop` use. It adds:

```
probe target, backup, stage
  backup and no target   → restore: rename backup → target, report it
  backup and target      → refuse (Rocky cannot prove it made the backup)
  stage exists           → refuse (another run, or a crash; never dropped)
no target                → plain CREATE TABLE target AS <select>; no rename
1 fails, "already exists"→ refuse, keep the stage (another run made it after the probe,
                           or the probe could not see it); target untouched
1 fails otherwise        → drop the stage (the probe proved it was ours); target untouched
2 fails                  → drop the stage; target untouched
3 fails                  → rename backup → target (rollback)
3 and rollback fail      → error names the exact RENAME that restores the table
4 fails                  → success with a warning naming the DROP; the next run refuses
                           until the backup is gone
```

Recovery is also wired into the existence probes. `recover_interrupted_swap` runs before the model executor and the replication copy probe whether a target exists. Without it, a run that died between steps 2 and 3 would make the next run bootstrap a fresh table beside the backup that holds the old data.

Every restore and every cleanup warning is reported in `MaterializationOutput.notes`. Nothing is ever dropped on the strength of a name alone: the only drops are the stage this run proved absent before building it, and the backup this run made in step 2.

### D3 — DuckDB over an Iceberg REST catalog (consumed by L-1)

`CREATE OR REPLACE` is unavailable there, and rename cannot share a transaction with writes. The replace is an in-place transactional overwrite of the existing table:

```
BEGIN
  ALTER TABLE t ADD/DROP COLUMN …   -- align the schema to the new SELECT
  DELETE FROM t
  INSERT INTO t BY NAME <select>
COMMIT
```

The spike proves this is one atomic catalog commit, and that a failure at any statement keeps the old rows and schema. A first run (no table) is a plain CTAS. L-1 adds it as a third `TableReplace` variant for catalogs the DuckDB adapter attaches as Iceberg, so every call site has to handle it.

### D4 — Every drop-then-create site moves onto the contract

| Site | Now |
|---|---|
| `sql_gen` `FullRefresh` | `full_refresh_swap()` → the swap's statements; `run` executes the same swap |
| Replication `full_refresh` of an existing target | Staged swap built from the bootstrap (non-replacing) CTAS aimed at the stage, held-back bound kept |
| Replication drift `DropAndRecreate` | No `DROP`. The rebuild is a full refresh of an existing target: atomic replace, or the swap |
| Branch promote | `promote_swap()` checks the recorded statement is exactly the replace, then executes the swap. New plans record no pre-drop; a legacy plan's `pre_drop_statement` is never run |
| Quarantine `drop` | The valid table is written by the swap (`QuarantinePlan::valid_swap`), run with `StagedSwap::execute` |
| Quarantine `tag` | Still refused on a swap dialect (it rewrites its own source) |
| Seeds | Not changed here; see What this does not close |

## Advisor ruling (2026-11-11, Hugo to confirm)

A fresh read-only Fable agent ruled on the open questions of the draft. Recorded as given; deviations are marked.

- **Q1 — Trino connectors that can replace atomically:** always staged swap on Trino now. A per-catalog config key (default swap) is a follow-up; never probe `system.metadata.catalogs`, because a probe makes `rocky plan` differ from `rocky run` and the connector name does not prove the capability.
- **Q2 — Recovery scope:** restore only when the target is missing; never on "exists but empty"; no age rule; report every recovery. Recovery runs inside the existence probes. The draft's "target and backup → drop the backup" was unsafe (a bootstrap after a crash would make the backup the only full copy) and is replaced by a refusal.
- **Q3 — Seeds:** follow-up PR under this ADR.
- **Q4 — Name collisions:** reserve the suffixes; never drop a stage or backup Rocky cannot prove it made. The ruling also asked for a compile-time coded diagnostic and a state-store swap-intent record. **Deviation:** the refusal is at runtime, in `SwapNames::for_target`, before any statement; a new compiler code would collide with parallel compiler work, and the runtime is the only consumer of the names. The intent record is replaced by the stricter rule above (refuse every leftover), which needs no state and also acts as the per-target lock, since step 1 never replaces.
- **Q5 — Shape:** `table_replace` keyed on the catalog; a `StagedSwap` type with `statements()` and `execute()`; the executor skips the statement loop the way `atomic_create_done` does; the stage carries the model's table options.
- **Q6 — Drift:** removing the `DROP` is safe on Databricks, Snowflake, DuckDB, ClickHouse and Spark, with one exception stated below.

## Consequences

- RD-009 closes for full refresh, replication copy, drift rebuild, branch promote and quarantine `drop`, on every adapter Rocky ships.
- On Trino, every full refresh of an existing table has a short window between the two renames where the target name does not resolve. A reader in that window gets "table not found". No data is lost.
- On Trino, a full refresh costs two renames and three existence probes more than before.
- On a Trino catalog that refuses rename (Hive on Glue), a full refresh of an existing table now fails at the move-aside step with the target untouched, where it used to drop and recreate. A first build still works there: with no target, the swap is a plain `CREATE TABLE … AS` and renames nothing. Opting such a catalog into a non-atomic drop-and-create, or into `CREATE OR REPLACE` for Iceberg and Delta Lake catalogs, is the Q1 follow-up config key.
- A Trino Iceberg table rebuilt by the swap starts a new snapshot history, as the old drop-and-create did. `CREATE OR REPLACE` (the follow-up key) would keep it.
- Drift rebuild without the `DROP`:
  - BigQuery refuses `CREATE OR REPLACE` when the existing table's partitioning or clustering differs from the new one. A rebuild that worked by dropping now fails with BigQuery's error and the old table kept. That is the intended fail-closed outcome.
  - Spark: `CREATE OR REPLACE` is atomic only on catalogs that stage the replace (Delta, Iceberg). Rocky's Spark dialect always writes `USING delta` or `USING iceberg`.
  - ClickHouse: `CREATE OR REPLACE` is atomic on the `Atomic` database engine, ClickHouse's default.
- `rocky plan` on Trino shows the four swap statements instead of `DROP` + `CREATE`.

- A run that stops after step 3 (or whose step-4 drop fails) leaves the backup beside the live target. Every later replace of that table then refuses with `LeftoverBackup` until someone drops the backup by hand. That availability cost is the price of never dropping a table Rocky cannot prove it made (Q4).

### What this does not close

- **No intent record.** Ownership of `<t>__rocky_old` is inferred from its name. A table at the backup name with no target (a user's table, or a backup whose drop failed long ago and whose target was later dropped) is restored as the target. For a full refresh it is then replaced; for an incremental or merge model it becomes the live target. Q2 ruled this acceptable: restoring is the only action that never loses data. A state-store intent record would close it.
- **Two concurrent runs on one target are not supported.** The swap does not lock. Data is never lost, but a run can fail with a false error, and a recovery that races another run's swap-in can leave a stage that later runs refuse (`LeftoverStage`) until it is dropped by hand. A concurrent build that collides with another run's stage keeps that stage (`StageTaken`).

- **Seeds** still drop, create and batch-insert on every dialect (`seeds.rs`, `SeedStrategy::DropAndRecreate`). A failed seed leaves the seed table missing or partial. The CSV is in the repository, so availability is lost, not data. Follow-up PR under this ADR.
- **`drop_existing_kind`** (a table/view switch the model opts into) is unchanged: it already refuses unless the adapter offers `atomic_drop_and_create`.

## Alternatives considered

- **Keep the pre-drop and document it.** Rejected: a failed `SELECT` destroys the only copy of the data.
- **Trino `START TRANSACTION`.** Rejected: most connectors refuse multi-statement writes ("only supports writes using autocommit").
- **Swap by `INSERT OVERWRITE` / `DELETE` + `INSERT` on Trino.** Not atomic under autocommit, and a schema change still needs a rebuild.

## Validation

- Unit (`rocky-core`, `rocky-trino`): the swap's statements; every failure point of `execute` against a scripted adapter (build, move aside, swap in, rollback, drop backup); the refusals (leftover stage, leftover backup, reserved name, a stage the probe could not see); a retryable probe failure runs nothing; a first build renames nothing; recovery of a missing target from its backup; no statement drops the target.
- Run path (`rocky-cli`, Trino's dialect over DuckDB): a transformation full refresh swaps, a failing `SELECT` keeps the old table, an interrupted swap is recovered and reported in `notes`, a first build creates the target directly.
- CLI: Trino promote renders the swap and refuses a tampered statement; a legacy plan's pre-drop is never run (DuckDB, live).
- Live: the Trino conformance harness runs a full refresh whose `SELECT` fails against a real coordinator and asserts the old rows survive, then a crash-between-renames recovery.
- `scripts/mutation-check.sh` on the rollback and on the leftover-backup refusal.
