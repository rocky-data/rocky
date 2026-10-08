//! The Delta backend of a table publish (RV1-P3, experimental).
//!
//! [`DeltaTablePublisher`] moves each model's content-addressed Delta table
//! to the output a pointer names, in **one Delta commit per table**. Delta
//! has no multi-table commit, so a publish over several tables is not
//! atomic: readers can see some tables moved and others not until the last
//! commit lands. [`rocky_core::table_publish`] records which tables moved.
//!
//! What it can publish today:
//!
//! - `content_addressed` outputs of unpartitioned tables Rocky writes.
//!
//! What it refuses before anything is written:
//!
//! - partitioned outputs (more than one file, or a folded partition hash);
//! - `delta_observed` versions: they name a table version, not files Rocky
//!   wrote, so there is nothing to point a commit at;
//! - every other version kind, and a table with no configured writer.

use std::collections::BTreeMap;

use rocky_core::environments::EnvPointer;
use rocky_core::state::OutputVersion;
use rocky_core::table_publish::{TableMoved, TablePointerBackend};

use super::UniformWriter;

/// A [`TablePointerBackend`] over Delta UniForm tables that Rocky's
/// content-addressed writer owns. One [`UniformWriter`] per table.
pub struct DeltaTablePublisher {
    writers: BTreeMap<String, UniformWriter>,
    sync_iceberg: bool,
}

impl Default for DeltaTablePublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl DeltaTablePublisher {
    /// A publisher with no tables. After each commit it runs the Iceberg
    /// metadata sync (`MSCK REPAIR TABLE ... SYNC METADATA`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            writers: BTreeMap::new(),
            sync_iceberg: true,
        }
    }

    /// Add the writer of one table, keyed by its `catalog.schema.table`.
    #[must_use]
    pub fn with_table(mut self, writer: UniformWriter) -> Self {
        self.writers.insert(writer.config().fqtn(), writer);
        self
    }

    /// Skip the Iceberg metadata sync after each commit. The caller then
    /// syncs the tables itself.
    #[must_use]
    pub fn without_iceberg_sync(mut self) -> Self {
        self.sync_iceberg = false;
        self
    }

    /// The writer and the files of a pointer this publisher can move.
    fn resolve<'a>(
        &'a self,
        pointer: &'a EnvPointer,
    ) -> Result<(&'a UniformWriter, &'a [String]), String> {
        let Some(version) = pointer.version.known() else {
            return Err("this binary cannot read the recorded version".to_string());
        };
        match version {
            OutputVersion::ContentAddressed {
                table,
                blake3,
                files,
                ..
            } => {
                // An unpartitioned output records one file, and its blake3 is
                // that file's hash. Anything else is a partitioned output.
                if files.len() != 1 || files[0] != *blake3 {
                    return Err(format!(
                        "`{table}` is a partitioned output ({} files); partitioned outputs \
                         cannot be published yet",
                        files.len()
                    ));
                }
                let writer = self
                    .writers
                    .get(table)
                    .ok_or_else(|| format!("no Delta table writer is configured for `{table}`"))?;
                Ok((writer, files.as_slice()))
            }
            OutputVersion::DeltaObserved { table, .. } => Err(format!(
                "`{table}` recorded a delta_observed version, which names a table version and no \
                 files Rocky wrote; only content_addressed outputs can be published to Delta"
            )),
            OutputVersion::WarehouseJob { table, .. } => Err(format!(
                "`{table}` recorded a warehouse job id, not a Delta version"
            )),
            OutputVersion::Unversioned { reason } => {
                Err(format!("the output is unversioned ({reason:?})"))
            }
        }
    }
}

#[async_trait::async_trait]
impl TablePointerBackend for DeltaTablePublisher {
    fn check(&self, pointer: &EnvPointer) -> Result<(), String> {
        self.resolve(pointer).map(|_| ())
    }

    async fn move_table(&self, pointer: &EnvPointer) -> Result<TableMoved, String> {
        let (writer, files) = self.resolve(pointer)?;
        let table = writer.config().fqtn();
        let state = writer.discover().await.map_err(|e| e.to_string())?;
        let outcome = writer
            .restore_content_addressed(files, state)
            .await
            .map_err(|e| e.to_string())?;
        if !outcome.committed {
            return Ok(TableMoved::AlreadyCurrent {
                table,
                table_version: outcome.table_version,
            });
        }
        let warning = if self.sync_iceberg {
            writer.sync_iceberg_metadata().await.err().map(|e| {
                format!("the Delta commit landed, but the Iceberg metadata sync failed: {e}")
            })
        } else {
            None
        };
        Ok(TableMoved::Moved {
            table,
            table_version: outcome.table_version,
            warning,
        })
    }
}
