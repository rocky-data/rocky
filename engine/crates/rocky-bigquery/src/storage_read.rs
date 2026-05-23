//! BigQuery Storage Read API client — Arrow IPC over gRPC.
//!
//! Backs [`crate::connector::BigQueryAdapter::fetch_arrow_batch`].
//! The Storage Read API is the canonical Arrow path for BigQuery: a
//! `CreateReadSession` call against an existing table (or a query job's
//! anonymous destination table) hands back N read streams; iterating
//! each stream's `ReadRows` server-streaming RPC yields Arrow IPC
//! schema + record-batch messages directly. No JSON round-trip, no
//! type-fidelity loss.
//!
//! For the trait's single-`RecordBatch` PoC contract, this module
//! requests `max_stream_count = 1` so the entire result lands on one
//! stream, decodes every `ArrowRecordBatch` into the workspace
//! `arrow 58` types, and concatenates into one batch via
//! `arrow::compute::concat_batches`.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use googleapis_tonic_google_cloud_bigquery_storage_v1::google::cloud::bigquery::storage::v1::{
    CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession, big_query_read_client,
    read_rows_response, read_session,
};
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::debug;

use crate::auth::{AuthError, BigQueryAuth};

/// Errors produced by the Storage Read API path.
#[derive(Debug, thiserror::Error)]
pub enum StorageReadError {
    #[error("authentication error: {0}")]
    Auth(#[from] AuthError),

    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status: {0}")]
    Status(#[from] tonic::Status),

    #[error("invalid bearer token header: {0}")]
    InvalidToken(String),

    #[error("ReadSession returned no streams (empty table or denied)")]
    NoStreams,

    #[error("ReadSession missing Arrow schema bytes")]
    MissingSchema,

    #[error("Arrow IPC decode error: {0}")]
    Arrow(String),
}

/// gRPC endpoint for the BigQuery Storage Read API. The Storage Read
/// API is regionless from the client's perspective — the
/// `CreateReadSession` request carries the parent project (and the
/// destination table reference carries the dataset, which in turn pins
/// the region on the server side).
const STORAGE_READ_ENDPOINT: &str = "https://bigquerystorage.googleapis.com";

/// Fetch a single workspace `arrow 58` `RecordBatch` from `table_ref`
/// (project/dataset/table) by opening a one-stream Arrow Storage Read
/// session and concatenating all `ReadRowsResponse` chunks.
///
/// `selected_columns` is optional; pass `&[]` to project all columns.
/// `project_id` is the *billing* project for the read session — it
/// must have `bigquery.readSessions.create` on it, and need not equal
/// the project that owns the table.
pub async fn fetch_arrow_record_batch(
    auth: &BigQueryAuth,
    http_client: &reqwest::Client,
    billing_project: &str,
    table_ref: &StorageTableRef<'_>,
    selected_columns: &[String],
) -> Result<RecordBatch, StorageReadError> {
    let token = auth.get_token(http_client).await?;
    let channel = build_channel().await?;
    let mut client = make_authed_client(channel, &token)?;

    let parent = format!("projects/{billing_project}");
    let session_template = ReadSession {
        table: format!(
            "projects/{}/datasets/{}/tables/{}",
            table_ref.project, table_ref.dataset, table_ref.table
        ),
        data_format: DataFormat::Arrow as i32,
        read_options: Some(read_session::TableReadOptions {
            selected_fields: selected_columns.to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    };

    debug!(
        target: "rocky_bigquery::storage_read",
        table = %session_template.table,
        "creating Storage Read session"
    );

    let session = client
        .create_read_session(CreateReadSessionRequest {
            parent,
            read_session: Some(session_template),
            // 1 stream so the caller gets the entire result on one
            // server-streaming RPC. Multi-stream parallelism is a
            // follow-up (would land as a separate streaming-variant
            // trait method per the WarehouseAdapter doc).
            max_stream_count: 1,
            preferred_min_stream_count: 1,
        })
        .await?
        .into_inner();

    let stream_name = session
        .streams
        .first()
        .ok_or(StorageReadError::NoStreams)?
        .name
        .clone();

    // Pull the IPC schema once, up front. Each subsequent record-batch
    // message is just the IPC body — we splice the schema bytes in
    // front of each batch so `StreamReader` decodes cleanly.
    let schema_bytes: Bytes = match session.schema.ok_or(StorageReadError::MissingSchema)? {
        read_session::Schema::ArrowSchema(s) => Bytes::from(s.serialized_schema),
        read_session::Schema::AvroSchema(_) => {
            // We requested Arrow, so this branch is unreachable in
            // practice; surface defensively.
            return Err(StorageReadError::Arrow(
                "ReadSession returned Avro schema despite DataFormat::Arrow request".into(),
            ));
        }
    };

    let mut stream = client
        .read_rows(ReadRowsRequest {
            read_stream: stream_name,
            offset: 0,
        })
        .await?
        .into_inner();

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut decoded_schema: Option<Arc<Schema>> = None;
    while let Some(msg) = stream.next().await {
        let resp = msg?;
        let Some(rows) = resp.rows else { continue };
        let batch_bytes = match rows {
            read_rows_response::Rows::ArrowRecordBatch(b) => b.serialized_record_batch,
            read_rows_response::Rows::AvroRows(_) => continue,
        };
        if batch_bytes.is_empty() {
            continue;
        }

        let mut buf = BytesMut::with_capacity(schema_bytes.len() + batch_bytes.len());
        buf.put_slice(&schema_bytes);
        buf.put_slice(&batch_bytes);

        let reader = StreamReader::try_new(Cursor::new(buf.freeze()), None)
            .map_err(|e| StorageReadError::Arrow(e.to_string()))?;
        if decoded_schema.is_none() {
            decoded_schema = Some(reader.schema());
        }
        for b in reader {
            batches.push(b.map_err(|e| StorageReadError::Arrow(e.to_string()))?);
        }
    }

    match batches.len() {
        0 => {
            // No rows: hand back an empty batch with the declared schema
            // so downstream consumers always get a typed shape.
            let schema = decoded_schema
                .or_else(|| {
                    StreamReader::try_new(Cursor::new(schema_bytes.clone()), None)
                        .ok()
                        .map(|r| r.schema())
                })
                .ok_or_else(|| StorageReadError::Arrow("could not decode IPC schema".into()))?;
            RecordBatch::try_new(schema, Vec::new())
                .map_err(|e| StorageReadError::Arrow(e.to_string()))
        }
        1 => Ok(batches.into_iter().next().expect("len == 1")),
        _ => {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, batches.iter())
                .map_err(|e| StorageReadError::Arrow(e.to_string()))
        }
    }
}

/// Resolved (project, dataset, table) handle pointing at a BigQuery
/// table that the Storage Read API can open a session against.
/// Borrowed view — no allocations on the call path.
#[derive(Debug, Clone)]
pub struct StorageTableRef<'a> {
    pub project: &'a str,
    pub dataset: &'a str,
    pub table: &'a str,
}

async fn build_channel() -> Result<Channel, StorageReadError> {
    let tls = ClientTlsConfig::new().with_webpki_roots();
    let channel = Endpoint::from_static(STORAGE_READ_ENDPOINT)
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(channel)
}

/// Wrap `channel` in a `BigQueryReadClient` that attaches a
/// `authorization: Bearer <token>` header to every RPC. The token is
/// snapshotted at client-construction time; callers are expected to
/// mint a fresh client per `fetch_arrow_record_batch` invocation so a
/// long-lived adapter doesn't keep replaying a server-expired token.
fn make_authed_client(
    channel: Channel,
    token: &str,
) -> Result<
    big_query_read_client::BigQueryReadClient<
        tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
    >,
    StorageReadError,
> {
    let bearer = MetadataValue::try_from(format!("Bearer {token}"))
        .map_err(|e| StorageReadError::InvalidToken(e.to_string()))?;
    let interceptor = AuthInterceptor { bearer };
    Ok(big_query_read_client::BigQueryReadClient::with_interceptor(
        channel,
        interceptor,
    ))
}

/// Stateless interceptor: clones the pre-built `Authorization` header
/// onto every outgoing request. Bears no reference to the underlying
/// `BigQueryAuth` because the token is sealed in at construction.
#[derive(Clone)]
pub struct AuthInterceptor {
    bearer: MetadataValue<tonic::metadata::Ascii>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
        req.metadata_mut()
            .insert("authorization", self.bearer.clone());
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_is_https() {
        // Regression: the Storage Read API requires TLS; an http://
        // endpoint silently 404s on GCP load balancers.
        assert!(STORAGE_READ_ENDPOINT.starts_with("https://"));
    }

    #[test]
    fn storage_table_ref_borrows_cleanly() {
        let p = String::from("p");
        let d = String::from("d");
        let t = String::from("t");
        let r = StorageTableRef {
            project: &p,
            dataset: &d,
            table: &t,
        };
        assert_eq!(r.project, "p");
        assert_eq!(r.dataset, "d");
        assert_eq!(r.table, "t");
    }
}
