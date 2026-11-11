//! Staged replace: a full refresh never leaves the target missing
//! (ADR-REPLACE, audit finding RD-009).
//!
//! A dialect whose [`SqlDialect::table_replace`] is
//! [`TableReplace::StagedSwap`] has no atomic `CREATE OR REPLACE`. Rocky then
//! replaces a table in four steps:
//!
//! ```text
//! 1. CREATE TABLE <stage> AS <select>          fail → target untouched
//! 2. ALTER TABLE IF EXISTS <target> RENAME TO <backup>
//!                                              fail → target untouched
//! 3. ALTER TABLE <stage> RENAME TO <target>    fail → rename the backup back
//! 4. DROP TABLE IF EXISTS <backup>             fail → warning; the new table is live
//! ```
//!
//! [`StagedSwap::statements`] is that list, for `rocky plan` and for callers
//! that run statements one after another. Run that way, every failure still
//! leaves the old table under the target name or under the backup name.
//!
//! [`StagedSwap::execute`] adds what a plain list cannot:
//!
//! - With no target (and no backup to restore), it skips the swap and runs a
//!   plain `CREATE TABLE <target> AS <select>`. A first build needs no
//!   rename, so it works on a catalog that cannot rename tables.
//! - It probes first. A backup with no target is restored (a previous run
//!   stopped between steps 2 and 3). A backup beside a live target, or a
//!   leftover stage, is refused: Rocky cannot prove it made that table, so it
//!   never drops or overwrites it.
//! - It rolls back step 2 when step 3 fails. If that also fails, the error
//!   names the exact `ALTER TABLE … RENAME` that restores the table.
//!
//! [`recover_interrupted_swap`] is the same restore for code that probes a
//! target before deciding to bootstrap it. Without it, a run that died
//! between steps 2 and 3 would make the next run build a fresh table beside
//! the backup that holds the old data.

use std::fmt::Write as _;

use rocky_ir::TableRef;

use crate::traits::{AdapterError, ExecutionStats, SqlDialect, TableReplace, WarehouseAdapter};

/// Suffix of the table a staged swap builds before it swaps it in.
pub const STAGE_SUFFIX: &str = "__rocky_stage";

/// Suffix the old table carries between the two renames of a staged swap.
pub const BACKUP_SUFFIX: &str = "__rocky_old";

/// The longest stage or backup name Rocky builds. 128 characters is the
/// lowest common identifier limit of the metastores Trino fronts (Hive
/// metastore, Glue); a longer target table name is shortened with a stable
/// hash.
pub const MAX_WORKING_NAME_CHARS: usize = 128;

/// Errors from a staged replace.
#[derive(Debug, thiserror::Error)]
pub enum ReplaceError {
    /// The target's own name ends in a suffix staged replace reserves.
    #[error(
        "table '{table}' ends in '{suffix}', a suffix Rocky reserves for the working tables of a \
         staged replace (ADR-REPLACE); rename the target"
    )]
    ReservedName { table: String, suffix: &'static str },

    /// A table is already at the stage name.
    #[error(
        "a table already exists at the staging name {stage}. Rocky did not create it in this run, \
         so it will not drop it. It is either left over from a run that stopped mid-replace, or \
         another run is replacing {target} right now. Once no run is writing {target}, check \
         {stage} and drop it: DROP TABLE {stage}"
    )]
    LeftoverStage { target: String, stage: String },

    /// A table is at the backup name while the target exists too.
    #[error(
        "a table already exists at the backup name {backup} beside the live {target}. Rocky did \
         not create it in this run, so it will not drop it. A previous replace most likely \
         finished but could not drop its backup. Check {backup} and drop it: DROP TABLE {backup}"
    )]
    LeftoverBackup { target: String, backup: String },

    /// Probing whether a table exists failed with a retryable error, so its
    /// absence cannot be assumed.
    #[error("could not check whether {table} exists: {source}")]
    Probe {
        table: String,
        #[source]
        source: AdapterError,
    },

    /// The dialect cannot render a statement the swap needs.
    #[error("cannot render the staged replace of {target}: {source}")]
    Render {
        target: String,
        #[source]
        source: AdapterError,
    },

    /// Building the stage failed. The target is untouched.
    #[error("building {stage} failed; {target} is untouched: {source}")]
    Build {
        target: String,
        stage: String,
        #[source]
        source: AdapterError,
    },

    /// Building the stage failed because a table already sits at the stage
    /// name: another run made it after this run's probe, or the probe could
    /// not see it. Rocky did not make it, so it is kept.
    #[error(
        "building {stage} failed because a table already exists there; Rocky did not create it \
         in this run, so it kept it. {target} is untouched. Another run may be replacing {target} \
         right now; once none is, check {stage} and drop it: DROP TABLE {stage}. Error: {source}"
    )]
    StageTaken {
        target: String,
        stage: String,
        #[source]
        source: AdapterError,
    },

    /// Creating a missing target failed. Nothing was renamed or dropped.
    #[error("creating {target} failed: {source}")]
    Create {
        target: String,
        #[source]
        source: AdapterError,
    },

    /// Moving the old table to the backup name failed. The target is untouched.
    #[error("moving {target} aside failed; {target} is untouched: {source}")]
    MoveAside {
        target: String,
        #[source]
        source: AdapterError,
    },

    /// Renaming the stage to the target failed, and the old table was
    /// renamed back. The target still holds the old data.
    #[error(
        "renaming {stage} to {target} failed; the old table was restored to {target}: {source}"
    )]
    SwapIn {
        target: String,
        stage: String,
        #[source]
        source: AdapterError,
    },

    /// Renaming the stage to the target failed and so did the rollback. The
    /// old data is safe under the backup name; the target name is empty.
    #[error(
        "renaming the new table to {target} failed ({swap_error}), and restoring the old table \
         failed too ({restore_error}). The old data is safe in {backup}; {target} does not exist. \
         Restore it with: {restore_sql}"
    )]
    RestoreFailed {
        target: String,
        backup: String,
        restore_sql: String,
        swap_error: String,
        restore_error: String,
    },

    /// Restoring a backup left by an interrupted swap failed.
    #[error(
        "{target} is missing and {backup} holds the table an interrupted replace moved aside, \
         but restoring it failed: {source}. Restore it with: {restore_sql}"
    )]
    Recover {
        target: String,
        backup: String,
        restore_sql: String,
        #[source]
        source: AdapterError,
    },
}

/// The name `table` gets with `suffix` appended, shortened with a stable hash
/// when it would pass [`MAX_WORKING_NAME_CHARS`].
///
/// Identifiers are validated ASCII before they reach here, so bytes are
/// characters.
#[must_use]
pub fn working_name(table: &str, suffix: &str) -> String {
    let plain = format!("{table}{suffix}");
    if plain.len() <= MAX_WORKING_NAME_CHARS {
        return plain;
    }
    // FNV-1a: stable across releases and platforms, no dependency.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in table.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    let keep = MAX_WORKING_NAME_CHARS - 1 - 16 - suffix.len();
    let prefix: String = table.chars().take(keep).collect();
    format!("{prefix}_{hash:016x}{suffix}")
}

/// The reserved suffix `table` ends in, if any. Compared without ASCII case:
/// most catalogs fold unquoted names.
#[must_use]
pub fn reserved_suffix(table: &str) -> Option<&'static str> {
    let lower = table.to_ascii_lowercase();
    [STAGE_SUFFIX, BACKUP_SUFFIX]
        .into_iter()
        .find(|suffix| lower.ends_with(suffix))
}

/// The target of a staged swap and the two working tables beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapNames {
    /// The table being replaced.
    pub target: TableRef,
    /// Where the new table is built.
    pub stage: TableRef,
    /// Where the old table waits between the two renames.
    pub backup: TableRef,
}

impl SwapNames {
    /// The working tables for `target`, in its catalog and schema.
    ///
    /// # Errors
    ///
    /// [`ReplaceError::ReservedName`] when the target's own name ends in a
    /// reserved suffix: its working tables could collide with another
    /// target's.
    pub fn for_target(target: &TableRef) -> Result<Self, ReplaceError> {
        if let Some(suffix) = reserved_suffix(&target.table) {
            return Err(ReplaceError::ReservedName {
                table: target.full_name(),
                suffix,
            });
        }
        let sibling = |suffix: &str| TableRef {
            catalog: target.catalog.clone(),
            schema: target.schema.clone(),
            table: working_name(&target.table, suffix),
        };
        Ok(Self {
            target: target.clone(),
            stage: sibling(STAGE_SUFFIX),
            backup: sibling(BACKUP_SUFFIX),
        })
    }
}

/// A rendered staged swap. Build one with [`StagedSwap::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedSwap {
    names: SwapNames,
    target_sql: String,
    stage_sql: String,
    backup_sql: String,
    build: String,
    create: String,
    move_aside: String,
    swap_in: String,
    restore: String,
    drop_backup: String,
}

/// What a successful [`StagedSwap::execute`] did besides the swap.
#[derive(Debug, Clone, Default)]
pub struct SwapOutcome {
    /// Statistics of the statement that built the stage.
    pub stats: ExecutionStats,
    /// Set when the run first restored a backup an interrupted run left.
    pub recovered: Option<String>,
    /// Set when the swap succeeded but the backup could not be dropped.
    pub warning: Option<String>,
}

fn format_ref(dialect: &dyn SqlDialect, table: &TableRef) -> Result<String, AdapterError> {
    dialect.format_table_ref(&table.catalog, &table.schema, &table.table)
}

impl StagedSwap {
    /// Render the swap of `names.target`. `build` must create
    /// `names.stage` and must not replace an existing table there (a plain
    /// `CREATE TABLE … AS`, as [`SqlDialect::create_table_as_new`] renders),
    /// so a stage another run is filling makes this run fail, not take it
    /// over. `create` is the same statement aimed at `names.target`, run
    /// instead of the swap when the target does not exist.
    ///
    /// # Errors
    ///
    /// [`ReplaceError::Render`] when the dialect cannot format a name or
    /// render a rename.
    pub fn new(
        dialect: &dyn SqlDialect,
        names: SwapNames,
        build: String,
        create: String,
    ) -> Result<Self, ReplaceError> {
        let render = |source| ReplaceError::Render {
            target: names.target.full_name(),
            source,
        };
        let target_sql = format_ref(dialect, &names.target).map_err(render)?;
        let stage_sql = format_ref(dialect, &names.stage).map_err(render)?;
        let backup_sql = format_ref(dialect, &names.backup).map_err(render)?;
        let move_aside = dialect
            .rename_table_sql(&target_sql, &backup_sql, true)
            .map_err(render)?;
        let swap_in = dialect
            .rename_table_sql(&stage_sql, &target_sql, false)
            .map_err(render)?;
        let restore = dialect
            .rename_table_sql(&backup_sql, &target_sql, false)
            .map_err(render)?;
        let drop_backup = dialect.drop_table_sql(&backup_sql);
        Ok(Self {
            names,
            target_sql,
            stage_sql,
            backup_sql,
            build,
            create,
            move_aside,
            swap_in,
            restore,
            drop_backup,
        })
    }

    /// The target and working table names.
    #[must_use]
    pub fn names(&self) -> &SwapNames {
        &self.names
    }

    /// The statement that builds the stage.
    #[must_use]
    pub fn build_statement(&self) -> &str {
        &self.build
    }

    /// The four statements of the swap, in order: build the stage, move the
    /// target aside, rename the stage in, drop the backup.
    ///
    /// Run one after another, a failure at any step leaves the old table at
    /// the target name or at the backup name, never neither. A leftover stage
    /// or backup makes the next run's build or move-aside fail rather than
    /// overwrite it.
    #[must_use]
    pub fn statements(&self) -> Vec<String> {
        vec![
            self.build.clone(),
            self.move_aside.clone(),
            self.swap_in.clone(),
            self.drop_backup.clone(),
        ]
    }

    /// Run the swap with its probes and rollback. See the module docs.
    ///
    /// # Errors
    ///
    /// Every [`ReplaceError`] variant says what state the target is in.
    pub async fn execute(
        &self,
        warehouse: &dyn WarehouseAdapter,
    ) -> Result<SwapOutcome, ReplaceError> {
        let mut outcome = SwapOutcome::default();
        let mut target_exists = exists(warehouse, &self.names.target).await?;
        if exists(warehouse, &self.names.backup).await? {
            if target_exists {
                return Err(ReplaceError::LeftoverBackup {
                    target: self.target_sql.clone(),
                    backup: self.backup_sql.clone(),
                });
            }
            outcome.recovered = Some(self.restore_backup(warehouse).await?);
            target_exists = true;
        }
        if exists(warehouse, &self.names.stage).await? {
            return Err(ReplaceError::LeftoverStage {
                target: self.target_sql.clone(),
                stage: self.stage_sql.clone(),
            });
        }

        if !target_exists {
            // Nothing to replace: build the target directly. A plain CREATE
            // fails rather than replace a table the probe missed, and a
            // failure leaves nothing this run must clean up.
            outcome.stats = warehouse
                .execute_statement_with_stats(&self.create)
                .await
                .map_err(|source| ReplaceError::Create {
                    target: self.target_sql.clone(),
                    source,
                })?;
            return Ok(outcome);
        }

        match warehouse.execute_statement_with_stats(&self.build).await {
            Ok(stats) => outcome.stats = stats,
            Err(source) if says_already_exists(&source) => {
                // The stage appeared after the probe (a concurrent run), or
                // the probe could not see it. Either way it is not this
                // run's table, so it is kept.
                return Err(ReplaceError::StageTaken {
                    target: self.target_sql.clone(),
                    stage: self.stage_sql.clone(),
                    source,
                });
            }
            Err(source) => {
                // The probe saw no stage and the build did not collide with
                // one, so whatever a failed build left there is this run's.
                self.drop_stage_quietly(warehouse).await;
                return Err(ReplaceError::Build {
                    target: self.target_sql.clone(),
                    stage: self.stage_sql.clone(),
                    source,
                });
            }
        }

        if let Err(source) = warehouse.execute_statement(&self.move_aside).await {
            self.drop_stage_quietly(warehouse).await;
            return Err(ReplaceError::MoveAside {
                target: self.target_sql.clone(),
                source,
            });
        }

        if let Err(swap_error) = warehouse.execute_statement(&self.swap_in).await {
            return match warehouse.execute_statement(&self.restore).await {
                Ok(()) => {
                    self.drop_stage_quietly(warehouse).await;
                    Err(ReplaceError::SwapIn {
                        target: self.target_sql.clone(),
                        stage: self.stage_sql.clone(),
                        source: swap_error,
                    })
                }
                Err(restore_error) => Err(ReplaceError::RestoreFailed {
                    target: self.target_sql.clone(),
                    backup: self.backup_sql.clone(),
                    restore_sql: self.restore.clone(),
                    swap_error: swap_error.to_string(),
                    restore_error: restore_error.to_string(),
                }),
            };
        }

        if let Err(e) = warehouse.execute_statement(&self.drop_backup).await {
            let mut msg = String::new();
            let _ = write!(
                msg,
                "{} was replaced, but its old copy {} could not be dropped ({e}). The next \
                 replace refuses until it is gone: {}",
                self.target_sql, self.backup_sql, self.drop_backup
            );
            tracing::warn!(target = %self.target_sql, "{msg}");
            outcome.warning = Some(msg);
        }
        Ok(outcome)
    }

    async fn restore_backup(
        &self,
        warehouse: &dyn WarehouseAdapter,
    ) -> Result<String, ReplaceError> {
        warehouse
            .execute_statement(&self.restore)
            .await
            .map_err(|source| ReplaceError::Recover {
                target: self.target_sql.clone(),
                backup: self.backup_sql.clone(),
                restore_sql: self.restore.clone(),
                source,
            })?;
        let msg = format!(
            "restored {} from {}, which an interrupted replace had left",
            self.target_sql, self.backup_sql
        );
        tracing::warn!(target = %self.target_sql, "{msg}");
        Ok(msg)
    }

    async fn drop_stage_quietly(&self, warehouse: &dyn WarehouseAdapter) {
        let drop = warehouse.dialect().drop_table_sql(&self.stage_sql);
        if let Err(e) = warehouse.execute_statement(&drop).await {
            tracing::warn!(stage = %self.stage_sql, error = %e, "could not drop the stage of a failed replace");
        }
    }
}

/// Whether a failed `CREATE TABLE` collided with an existing table. Trino
/// reports `TABLE_ALREADY_EXISTS` as "Table '…' already exists"; other
/// engines use the same words.
fn says_already_exists(error: &AdapterError) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("already exists")
}

/// Whether `table` exists. A retryable probe failure is an error (absence
/// cannot be assumed); any other failure, a genuine "not found" among them,
/// reads as absent. The same rule as the run path's existence probes.
async fn exists(warehouse: &dyn WarehouseAdapter, table: &TableRef) -> Result<bool, ReplaceError> {
    match warehouse.describe_table(table).await {
        Ok(_) => Ok(true),
        Err(e) if warehouse.classify_failure(&e).is_retryable() => Err(ReplaceError::Probe {
            table: table.full_name(),
            source: e,
        }),
        Err(_) => Ok(false),
    }
}

/// Restore `target` from its staged-swap backup when a previous run stopped
/// between moving the old table aside and renaming the new one in.
///
/// Call it before an existence probe that would bootstrap a missing target.
/// It does nothing on a dialect that replaces atomically, and nothing when
/// the target exists. It never drops a table. Returns a message to report
/// when it restored one.
///
/// # Errors
///
/// A retryable probe failure, or a failed restore (the error names the
/// statement that restores it by hand).
pub async fn recover_interrupted_swap(
    warehouse: &dyn WarehouseAdapter,
    target: &TableRef,
) -> Result<Option<String>, ReplaceError> {
    let dialect = warehouse.dialect();
    match dialect.table_replace(&target.catalog) {
        TableReplace::Atomic => return Ok(None),
        TableReplace::StagedSwap => {}
    }
    // A target with a reserved name never had a swap, so it has no backup.
    let Ok(names) = SwapNames::for_target(target) else {
        return Ok(None);
    };
    if exists(warehouse, &names.target).await? || !exists(warehouse, &names.backup).await? {
        return Ok(None);
    }
    let swap = StagedSwap::new(dialect, names, String::new(), String::new())?;
    swap.restore_backup(warehouse).await.map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn working_names_append_the_suffix() {
        assert_eq!(working_name("orders", STAGE_SUFFIX), "orders__rocky_stage");
        assert_eq!(working_name("orders", BACKUP_SUFFIX), "orders__rocky_old");
    }

    #[test]
    fn long_working_names_are_shortened_and_stay_distinct() {
        let long = "t".repeat(200);
        let other = format!("{}x", "t".repeat(199));
        let a = working_name(&long, STAGE_SUFFIX);
        let b = working_name(&other, STAGE_SUFFIX);
        assert_eq!(a.len(), MAX_WORKING_NAME_CHARS);
        assert!(a.ends_with(STAGE_SUFFIX));
        assert_ne!(a, b, "the hash separates names that share the kept prefix");
        assert_eq!(a, working_name(&long, STAGE_SUFFIX), "stable");
    }

    #[test]
    fn a_target_with_a_reserved_suffix_is_refused() {
        for table in ["orders__rocky_stage", "ORDERS__ROCKY_OLD"] {
            let target = TableRef {
                catalog: "c".into(),
                schema: "s".into(),
                table: table.into(),
            };
            assert!(matches!(
                SwapNames::for_target(&target),
                Err(ReplaceError::ReservedName { .. })
            ));
        }
    }

    #[test]
    fn working_tables_sit_beside_the_target() {
        let target = TableRef {
            catalog: "lake".into(),
            schema: "marts".into(),
            table: "orders".into(),
        };
        let names = SwapNames::for_target(&target).unwrap();
        assert_eq!(names.stage.full_name(), "lake.marts.orders__rocky_stage");
        assert_eq!(names.backup.full_name(), "lake.marts.orders__rocky_old");
    }
}
