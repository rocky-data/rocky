//! Spark Connect client: runs one SQL statement per `ExecutePlan` call and
//! collects the Arrow batches it streams back.
//!
//! The gRPC channel is opened on first use and reused. Every statement of
//! one client shares a session id, so session state (temporary views, `SET`
//! options) carries over between statements, as it does in one PySpark
//! session.

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use rocky_core::traits::QueryResult;
use tokio::sync::OnceCell;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::config::SparkConfig;
use crate::proto::{self, ExecutePlanRequest, ExecutePlanResponse, execute_plan_response};

/// How long opening the TCP (and TLS) connection may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors from the Spark adapter.
#[derive(Debug, thiserror::Error)]
pub enum SparkError {
    /// The `[adapter]` block is invalid.
    #[error("spark configuration: {0}")]
    Config(String),

    /// The connection could not be opened. Nothing reached the server, so the
    /// statement certainly did not run.
    #[error("cannot connect to the Spark Connect server: {0}")]
    Connect(String),

    /// The server (or the gRPC layer) failed the call. `message` is the
    /// server's error text, which starts with Spark's error class
    /// (`[TABLE_OR_VIEW_NOT_FOUND] …`).
    #[error("spark: {message}")]
    Status { code: tonic::Code, message: String },

    /// The statement did not finish within the configured timeout. The
    /// server may still be running it.
    #[error("spark statement did not finish within {secs}s")]
    Timeout { secs: u64 },

    /// The server sent an Arrow batch Rocky could not read.
    #[error("spark returned an unreadable Arrow batch: {0}")]
    Arrow(String),
}

/// Spark error classes that mean "the object does not exist".
const MISSING_OBJECT_CLASSES: &[&str] = &[
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "[SCHEMA_NOT_FOUND]",
    "[DELTA_TABLE_NOT_FOUND]",
    "[DELTA_MISSING_DELTA_TABLE]",
];

impl SparkError {
    /// Whether the server said the table, view or schema does not exist.
    #[must_use]
    pub fn is_missing_object(&self) -> bool {
        match self {
            Self::Status { message, .. } => {
                MISSING_OBJECT_CLASSES.iter().any(|c| message.contains(c))
            }
            Self::Config(_) | Self::Connect(_) | Self::Timeout { .. } | Self::Arrow(_) => false,
        }
    }

    /// Whether retrying is safe and may succeed: only a connection that
    /// never opened. A call that failed after it was sent may have run on
    /// the server, so it is not retried.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connect(_) => true,
            Self::Config(_) | Self::Status { .. } | Self::Timeout { .. } | Self::Arrow(_) => false,
        }
    }
}

/// A Spark Connect client bound to one session.
pub struct SparkClient {
    config: SparkConfig,
    session_id: String,
    bearer: Option<MetadataValue<Ascii>>,
    channel: OnceCell<Channel>,
}

impl std::fmt::Debug for SparkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkClient")
            .field("config", &self.config)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl SparkClient {
    /// Build a client. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SparkError::Config`] when the token holds characters an HTTP
    /// header cannot carry.
    pub fn new(config: SparkConfig) -> Result<Self, SparkError> {
        let bearer = config
            .token
            .as_ref()
            .map(|t| {
                MetadataValue::try_from(format!("Bearer {t}"))
                    .map_err(|_| SparkError::Config("token is not a valid header value".into()))
            })
            .transpose()?;
        Ok(Self {
            config,
            session_id: uuid::Uuid::new_v4().to_string(),
            bearer,
            channel: OnceCell::new(),
        })
    }

    /// The connection settings.
    #[must_use]
    pub fn config(&self) -> &SparkConfig {
        &self.config
    }

    /// The Spark Connect session id every statement of this client uses.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn channel(&self) -> Result<Channel, SparkError> {
        self.channel
            .get_or_try_init(|| async {
                let mut endpoint = Endpoint::from_shared(self.config.endpoint_url())
                    .map_err(|e| SparkError::Config(format!("invalid endpoint: {e}")))?
                    .connect_timeout(CONNECT_TIMEOUT);
                if self.config.use_ssl() {
                    endpoint = endpoint
                        .tls_config(ClientTlsConfig::new().with_webpki_roots())
                        .map_err(|e| SparkError::Config(format!("TLS: {e}")))?;
                }
                endpoint
                    .connect()
                    .await
                    .map_err(|e| SparkError::Connect(error_chain(&e)))
            })
            .await
            .cloned()
    }

    /// Run one statement and collect its Arrow batches (none for a command
    /// that returns no rows).
    ///
    /// # Errors
    ///
    /// See [`SparkError`].
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, SparkError> {
        let timeout = self.config.timeout;
        match tokio::time::timeout(timeout, self.execute_inner(sql)).await {
            Ok(result) => result,
            Err(_) => Err(SparkError::Timeout {
                secs: timeout.as_secs(),
            }),
        }
    }

    async fn execute_inner(&self, sql: &str) -> Result<Vec<RecordBatch>, SparkError> {
        let mut grpc = tonic::client::Grpc::new(self.channel().await?);
        grpc.ready()
            .await
            .map_err(|e| SparkError::Connect(error_chain(&e)))?;

        let operation_id = uuid::Uuid::new_v4().to_string();
        let mut request = tonic::Request::new(proto::sql_request(
            &self.session_id,
            &self.config.user_id,
            &operation_id,
            concat!("rocky/", env!("CARGO_PKG_VERSION")),
            sql,
        ));
        if let Some(bearer) = &self.bearer {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
        let codec: tonic_prost::ProstCodec<ExecutePlanRequest, ExecutePlanResponse> =
            tonic_prost::ProstCodec::default();
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(proto::EXECUTE_PLAN_PATH);
        let mut stream = grpc
            .server_streaming(request, path, codec)
            .await
            .map_err(|s| status_error(&s))?
            .into_inner();

        // The stream ends with the call's gRPC status: `None` here means the
        // server closed it with OK, after every result. A stream cut short
        // (server crash, dropped connection) ends with an error status
        // instead, which `message()` returns as `Err`. The `ResultComplete`
        // marker is only sent to reattachable executions, which Rocky does
        // not request, so its absence is not a signal.
        let mut batches = Vec::new();
        while let Some(message) = stream.message().await.map_err(|s| status_error(&s))? {
            match message.response_type {
                Some(execute_plan_response::ResponseType::ArrowBatch(batch)) => {
                    decode_arrow_batch(&batch.data, &mut batches)?;
                }
                // Progress, metrics and command results Rocky does not read.
                Some(execute_plan_response::ResponseType::ResultComplete(_)) | None => {}
            }
        }
        Ok(batches)
    }

    /// Run one statement and return its rows as strings (`NULL` as JSON
    /// null), the shape every other adapter's `QueryResult` has.
    ///
    /// # Errors
    ///
    /// See [`SparkError`].
    pub async fn query(&self, sql: &str) -> Result<QueryResult, SparkError> {
        let batches = self.execute(sql).await?;
        batches_to_query_result(&batches)
    }

    /// `SELECT 1`: proves the server is reachable and accepts the session.
    ///
    /// # Errors
    ///
    /// See [`SparkError`].
    pub async fn ping(&self) -> Result<(), SparkError> {
        self.execute("SELECT 1").await.map(|_| ())
    }
}

fn status_error(status: &tonic::Status) -> SparkError {
    SparkError::Status {
        code: status.code(),
        message: status.message().to_string(),
    }
}

/// The error and its sources, joined: tonic's top-level transport error is
/// just "transport error", with the useful part (connection refused, DNS,
/// TLS) in its source.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut text = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        let part = inner.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        source = inner.source();
    }
    text
}

/// Decode one `ArrowBatch.data` payload: an Arrow IPC stream.
fn decode_arrow_batch(data: &[u8], out: &mut Vec<RecordBatch>) -> Result<(), SparkError> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
        .map_err(|e| SparkError::Arrow(e.to_string()))?;
    for batch in reader {
        out.push(batch.map_err(|e| SparkError::Arrow(e.to_string()))?);
    }
    Ok(())
}

/// Render batches as a [`QueryResult`]: every non-null cell as a display
/// string, a null as JSON null. Columns come from the first batch.
///
/// Timestamps render in the two shapes the rest of Rocky parses back
/// (`parse_timestamp_cell`): a `TIMESTAMP` (an instant, which Spark sends
/// tagged with the session time zone) as RFC 3339 in UTC,
/// `2024-05-01T10:00:00.123456+00:00`; a `TIMESTAMP_NTZ` (no zone) as
/// `2024-05-01 10:00:00.123456`.
///
/// # Errors
///
/// [`SparkError::Arrow`] when a column type has no display form.
pub fn batches_to_query_result(batches: &[RecordBatch]) -> Result<QueryResult, SparkError> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let columns = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let options = FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.f"))
        .with_timestamp_tz_format(Some("%Y-%m-%dT%H:%M:%S%.f%:z"));
    let mut rows = Vec::new();
    for batch in batches {
        let arrays = batch
            .columns()
            .iter()
            .map(as_utc)
            .collect::<Result<Vec<_>, _>>()?;
        let formatters = arrays
            .iter()
            .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| SparkError::Arrow(e.to_string()))?;
        // `logical_nulls`, not `is_null`: a `NullArray` (the type of a bare
        // `NULL` literal) has no validity buffer, so `is_null` says false for
        // every row of it.
        let nulls: Vec<_> = arrays.iter().map(|a| a.logical_nulls()).collect();
        for row in 0..batch.num_rows() {
            let cells = formatters
                .iter()
                .zip(&nulls)
                .map(|(fmt, nulls)| {
                    if nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(fmt.value(row).to_string())
                    }
                })
                .collect();
            rows.push(cells);
        }
    }
    Ok(QueryResult { columns, rows })
}

/// A zoned timestamp column re-tagged as UTC (`+00:00`). The stored values
/// are UTC instants whatever the tag says, so this changes only how they
/// display, and avoids resolving a named zone such as `Etc/UTC` (which needs
/// a time-zone database Rocky does not link).
fn as_utc(array: &arrow::array::ArrayRef) -> Result<arrow::array::ArrayRef, SparkError> {
    use arrow::datatypes::DataType;

    let DataType::Timestamp(unit, Some(_)) = array.data_type() else {
        return Ok(array.clone());
    };
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Timestamp(*unit, Some("+00:00".into())))
        .build()
        .map_err(|e| SparkError::Arrow(e.to_string()))?;
    Ok(arrow::array::make_array(data))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ],
        )
        .unwrap()
    }

    fn ipc(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        buf
    }

    #[test]
    fn arrow_ipc_payload_round_trips_to_rows_with_nulls() {
        let mut out = Vec::new();
        decode_arrow_batch(&ipc(&batch()), &mut out).unwrap();
        decode_arrow_batch(&ipc(&batch()), &mut out).unwrap();
        let result = batches_to_query_result(&out).unwrap();
        assert_eq!(result.columns, vec!["n", "s"]);
        assert_eq!(result.rows.len(), 4);
        assert_eq!(
            result.rows[0],
            vec![serde_json::json!("1"), serde_json::Value::Null]
        );
        assert_eq!(
            result.rows[1],
            vec![serde_json::Value::Null, serde_json::json!("x")]
        );
    }

    #[test]
    fn timestamps_render_in_the_shapes_rocky_parses_and_null_literals_are_null() {
        use arrow::array::{NullArray, TimestampMicrosecondArray};
        use arrow::datatypes::TimeUnit;

        // 2024-05-01T10:00:00.123456Z
        let micros = 1_714_557_600_123_456_i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ltz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into())),
                true,
            ),
            Field::new(
                "ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("n", DataType::Null, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("Etc/UTC"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)])),
                Arc::new(NullArray::new(1)),
            ],
        )
        .unwrap();
        let result = batches_to_query_result(&[batch]).unwrap();
        assert_eq!(
            result.rows[0],
            vec![
                serde_json::json!("2024-05-01T10:00:00.123456+00:00"),
                serde_json::json!("2024-05-01 10:00:00.123456"),
                serde_json::Value::Null,
            ]
        );
        let ltz: chrono::DateTime<chrono::Utc> =
            result.rows[0][0].as_str().unwrap().parse().unwrap();
        assert_eq!(ltz.timestamp_micros(), micros);
    }

    #[test]
    fn garbage_payload_is_an_arrow_error() {
        let mut out = Vec::new();
        let err = decode_arrow_batch(&[1, 2, 3], &mut out).unwrap_err();
        assert!(matches!(err, SparkError::Arrow(_)), "{err}");
    }

    #[test]
    fn missing_object_and_transient_classification() {
        let missing = SparkError::Status {
            code: tonic::Code::Internal,
            message: "[TABLE_OR_VIEW_NOT_FOUND] The table or view `a`.`b` cannot be found.".into(),
        };
        assert!(missing.is_missing_object());
        assert!(!missing.is_transient());
        let syntax = SparkError::Status {
            code: tonic::Code::Internal,
            message: "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'.".into(),
        };
        assert!(!syntax.is_missing_object());
        // A call that reached the server is never retried, even when gRPC
        // reports it unavailable: the statement may have run.
        let dropped = SparkError::Status {
            code: tonic::Code::Unavailable,
            message: "connection reset".into(),
        };
        assert!(!dropped.is_transient());
        assert!(SparkError::Connect("refused".into()).is_transient());
        assert!(!SparkError::Timeout { secs: 1 }.is_transient());
    }

    #[tokio::test]
    async fn connection_refused_is_a_connect_error() {
        // Port 1 on loopback: nothing listens there.
        let config =
            SparkConfig::new(Some("127.0.0.1:1"), None, None, Duration::from_secs(10)).unwrap();
        let client = SparkClient::new(config).unwrap();
        let err = client.execute("SELECT 1").await.unwrap_err();
        assert!(matches!(err, SparkError::Connect(_)), "{err}");
        assert!(err.is_transient());
    }
}
