use rocky_core::secret_registry::render_placeholders as render;
use thiserror::Error;

#[derive(Error)]
pub enum UniformWriterError {
    #[error("object store: {}", render(&.0.to_string()))]
    ObjectStore(#[from] object_store::Error),

    #[error("parquet: {}", render(&.0.to_string()))]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow: {}", render(&.0.to_string()))]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("json: {}", render(&.0.to_string()))]
    Json(#[from] serde_json::Error),

    #[error("io: {}", render(&.0.to_string()))]
    Io(#[from] std::io::Error),

    #[error("delta log parse: {}", render(&.0.to_string()))]
    DeltaLog(String),

    #[error("sql client: {}", render(&.0.to_string()))]
    Sql(String),

    #[error(
        "row tracking is enabled on this table; phase 1 writer does not support it. \
         See arc 1 wave 2 phase 3 for the rowTracking-aware writer surface."
    )]
    RowTrackingUnsupported,

    #[error(
        "partitioned tables are not supported by phase 1; partition columns: {}. \
         See arc 1 wave 2 phase 2 for partitioned-table support.",
        format!("{:?}", .0.iter().map(|c| render(c)).collect::<Vec<_>>())
    )]
    PartitionedUnsupported(Vec<String>),

    #[error(
        "deletion vectors are enabled on this table; UniForm + deletionVectors is rejected by \
         Delta itself. This writer requires UniForm and so cannot operate on a DV table."
    )]
    DeletionVectorsUnsupported,

    #[error("retry budget exhausted on conditional log put: {}", render(&.0.to_string()))]
    CondPutRetryExhausted(String),

    /// The table's `_delta_log` holds a Delta checkpoint. Rocky's log reader
    /// replays only the `<20-digit>.json` commits, so it cannot see every
    /// live file. A replace that misses a live file leaves stale rows, so the
    /// write refuses (RV1-D8, #2269).
    #[error(
        "table `{table}` has a Delta checkpoint (`{checkpoint}`). Rocky reads only the JSON \
         commits in `_delta_log`, so it cannot list every live file, and it refuses to replace \
         the table. Another engine wrote this checkpoint; tables that only Rocky writes never \
         get one. To recover, drop the table, create it again on an empty storage prefix, and \
         run the model again. Rocky then writes the whole output in one commit.",
        table = render(table),
        checkpoint = render(checkpoint)
    )]
    CheckpointPresent { table: String, checkpoint: String },

    /// The table's latest protocol or metadata turns on a Delta feature the
    /// writer does not implement, or its log carries a commit shape Rocky
    /// cannot replay (coordinated commits, an unknown action).
    #[error(
        "table `{table}` uses a Delta feature Rocky's content-addressed writer does not support: \
         {feature}. Rocky refuses to commit to it, because the commit could corrupt the table or \
         bypass its commit coordinator. Turn the feature off, or create the table again without \
         it, then run the model again.",
        table = render(table),
        feature = render(feature)
    )]
    UnsupportedTableFeature { table: String, feature: String },

    /// The table's schema, partitioning or protocol changed between
    /// `discover()` and the commit (for example a concurrent `ALTER TABLE`).
    /// The prepared files no longer match the table, so nothing is committed.
    #[error(
        "table `{table}` changed while Rocky was writing it ({what}); no commit was written. \
         Run the model again so Rocky reads the new table shape.",
        table = render(table),
        what = render(what)
    )]
    TableChangedDuringWrite { table: String, what: String },

    /// The table sets `delta.appendOnly=true`, so a replace commit cannot
    /// `remove` the files of the earlier build.
    #[error(
        "table `{table}` sets `delta.appendOnly=true`, so Rocky cannot remove the files of the \
         earlier build. Run `ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = \
         'false')`, then run the model again.",
        table = render(table)
    )]
    AppendOnlyTable { table: String },

    /// A table publish (RV1-P3) cannot make the table serve an earlier
    /// output: a file of that output is gone, or was written for another
    /// schema. Nothing is committed.
    #[error(
        "table `{table}` cannot be published at the recorded version: {detail}. No commit was \
         written. Run the model again to build a version that can be published.",
        table = render(table),
        detail = render(detail)
    )]
    PublishSourceUnavailable { table: String, detail: String },

    /// The PUT of a table-publish commit (RV1-P3) failed with an error that
    /// does not say whether the commit was stored (for example a timeout
    /// after the request was sent). The commit may have landed.
    #[error(
        "table `{table}`: the write of commit {version} failed, and it may have landed: \
         {detail}. Publish again: a table that already serves the version writes no commit.",
        table = render(table),
        detail = render(detail)
    )]
    CommitOutcomeUnknown {
        table: String,
        version: u64,
        detail: String,
    },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and of every wrapped error (#1919).
impl std::fmt::Debug for UniformWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "UniformWriterError", self)
    }
}

pub type Result<T> = std::result::Result<T, UniformWriterError>;

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "s3cr3t-value-123";

    fn register() {
        rocky_core::secret_registry::register_substitution("RV_UW_ERR_SECRET", SECRET);
    }

    fn table() -> String {
        format!("cat.{SECRET}.orders")
    }

    /// Every variant that embeds a table name or free text built from a
    /// resolved value (#1919).
    fn leaky() -> Vec<UniformWriterError> {
        vec![
            UniformWriterError::CheckpointPresent {
                table: table(),
                checkpoint: format!("{SECRET}.checkpoint.parquet"),
            },
            UniformWriterError::UnsupportedTableFeature {
                table: table(),
                feature: format!("feature-{SECRET}"),
            },
            UniformWriterError::TableChangedDuringWrite {
                table: table(),
                what: format!("what-{SECRET}"),
            },
            UniformWriterError::AppendOnlyTable { table: table() },
            UniformWriterError::PublishSourceUnavailable {
                table: table(),
                detail: format!("detail-{SECRET}"),
            },
            UniformWriterError::CommitOutcomeUnknown {
                table: table(),
                version: 3,
                detail: format!("detail-{SECRET}"),
            },
            UniformWriterError::PartitionedUnsupported(vec![format!("col_{SECRET}")]),
            UniformWriterError::DeltaLog(format!("log-{SECRET}")),
            UniformWriterError::Sql(format!("sql-{SECRET}")),
            UniformWriterError::CondPutRetryExhausted(format!("put-{SECRET}")),
            UniformWriterError::Io(std::io::Error::other(format!("io-{SECRET}"))),
        ]
    }

    #[test]
    fn display_and_debug_print_a_resolved_value_as_its_name() {
        register();
        for err in leaky() {
            let shown = err.to_string();
            let debug = format!("{err:?}");
            assert!(!shown.contains(SECRET), "Display leaks: {shown}");
            assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        }
    }
}
