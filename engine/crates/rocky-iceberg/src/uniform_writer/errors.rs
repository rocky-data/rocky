use thiserror::Error;

#[derive(Debug, Error)]
pub enum UniformWriterError {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("delta log parse: {0}")]
    DeltaLog(String),

    #[error("sql client: {0}")]
    Sql(String),

    #[error(
        "row tracking is enabled on this table; phase 1 writer does not support it. \
         See arc 1 wave 2 phase 3 for the rowTracking-aware writer surface."
    )]
    RowTrackingUnsupported,

    #[error(
        "partitioned tables are not supported by phase 1; partition columns: {0:?}. \
         See arc 1 wave 2 phase 2 for partitioned-table support."
    )]
    PartitionedUnsupported(Vec<String>),

    #[error(
        "deletion vectors are enabled on this table; UniForm + deletionVectors is rejected by \
         Delta itself. This writer requires UniForm and so cannot operate on a DV table."
    )]
    DeletionVectorsUnsupported,

    #[error("retry budget exhausted on conditional log put: {0}")]
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
         run the model again. Rocky then writes the whole output in one commit."
    )]
    CheckpointPresent { table: String, checkpoint: String },

    /// The table's latest protocol or metadata turns on a Delta feature the
    /// writer does not implement, or its log carries a commit shape Rocky
    /// cannot replay (coordinated commits, an unknown action).
    #[error(
        "table `{table}` uses a Delta feature Rocky's content-addressed writer does not support: \
         {feature}. Rocky refuses to commit to it, because the commit could corrupt the table or \
         bypass its commit coordinator. Turn the feature off, or create the table again without \
         it, then run the model again."
    )]
    UnsupportedTableFeature { table: String, feature: String },

    /// The table's schema, partitioning or protocol changed between
    /// `discover()` and the commit (for example a concurrent `ALTER TABLE`).
    /// The prepared files no longer match the table, so nothing is committed.
    #[error(
        "table `{table}` changed while Rocky was writing it ({what}); no commit was written. \
         Run the model again so Rocky reads the new table shape."
    )]
    TableChangedDuringWrite { table: String, what: String },

    /// The table sets `delta.appendOnly=true`, so a replace commit cannot
    /// `remove` the files of the earlier build.
    #[error(
        "table `{table}` sets `delta.appendOnly=true`, so Rocky cannot remove the files of the \
         earlier build. Run `ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = \
         'false')`, then run the model again."
    )]
    AppendOnlyTable { table: String },
}

pub type Result<T> = std::result::Result<T, UniformWriterError>;
