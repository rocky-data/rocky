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
}

pub type Result<T> = std::result::Result<T, UniformWriterError>;
