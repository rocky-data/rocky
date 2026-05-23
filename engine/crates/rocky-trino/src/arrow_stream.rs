//! Spooled-protocol Arrow result fetch for `TrinoClient`.
//!
//! Implements the Trino [spooling protocol] extension to request
//! `QueryResults.data` as a sequence of Apache Arrow IPC segments rather
//! than the default inline-JSON row encoding. Each Arrow segment is a
//! self-contained IPC stream that decodes directly into one or more
//! `arrow::record_batch::RecordBatch` values; the multi-batch result set
//! is concatenated into a single batch via `arrow::compute::concat_batches`,
//! matching the contract documented on
//! [`WarehouseAdapter::fetch_arrow_batch`](rocky_core::traits::WarehouseAdapter::fetch_arrow_batch).
//!
//! [spooling protocol]: https://trino.io/docs/current/client/client-protocol.html
//!
//! # Version-gating
//!
//! Apache Arrow IPC is a **proposed** encoding for the spooling protocol —
//! see upstream PR [`trinodb/trino#26365`][arrow-pr] (closed stale Nov 2025,
//! revival discussion ongoing). The shipping Trino release (481 as of
//! May 2026) advertises only `json`, `json+lz4`, and `json+zstd` as
//! supported spooling encodings. Until Arrow encoding lands upstream the
//! coordinator falls back to inline-JSON `data` when the client requests
//! `arrow`/`arrow+zstd`; this module detects that fallback and surfaces
//! a clear [`TrinoError::ArrowEncodingUnavailable`] so callers can route
//! around it (or feature-gate the call) rather than silently degrading
//! to the JSON path. The wire negotiation is in place so the Rocky
//! adapter is ready for the encoding the day upstream merges.
//!
//! [arrow-pr]: https://github.com/trinodb/trino/pull/26365

use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::ipc::reader::StreamReader;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use tokio::time::Instant;
use tracing::{Instrument, debug, info_span};

use crate::connector::{TrinoClient, TrinoError};

/// Header advertising client support for the spooling protocol.
///
/// Trino opts the client into the segmented `data` response shape only
/// when this token appears in the comma-separated capabilities list.
const CLIENT_CAPABILITIES_HEADER: &str = "x-trino-client-capabilities";
const CLIENT_CAPABILITIES_VALUE: &str = "SPOOLING";

/// Header listing the spooled-segment encodings the client can decode,
/// in preference order. The server selects the first it supports and
/// holds that choice for the duration of the query.
///
/// We list both `arrow+zstd` (preferred — zstd halves the on-wire
/// size of a typical Arrow IPC stream) and `arrow` (uncompressed
/// fallback). When neither is honoured the server falls back to the
/// inline-JSON shape, which this module surfaces as
/// [`TrinoError::ArrowEncodingUnavailable`] rather than silently
/// degrading.
const SPOOLED_ACCEPT_ENCODING_HEADER: &str = "x-trino-spooled-segments-accept-encoding";
const SPOOLED_ACCEPT_ENCODING_VALUE: &str = "arrow+zstd,arrow";

/// Encoding strings the spec uses for Arrow IPC segments.
///
/// Matches the encoding identifiers from the upstream proposal
/// (see `protocol.spooling.encoding.arrow.enabled` / `.arrow+zstd.enabled`).
const ARROW_ENCODING_PREFIX: &str = "arrow";

impl TrinoClient {
    /// Execute `sql` and aggregate every spooled Arrow segment into a
    /// single [`RecordBatch`].
    ///
    /// Drives the `/v1/statement` state machine identically to
    /// [`TrinoClient::execute`], but threads the spooling-protocol
    /// negotiation headers and decodes the `QueryResults.data` payload
    /// as Apache Arrow IPC streams instead of inline JSON rows.
    ///
    /// # Errors
    ///
    /// - [`TrinoError::ArrowEncodingUnavailable`] when the coordinator
    ///   doesn't honour an Arrow encoding — the inline-JSON fallback is
    ///   visible on the first poll that ships data and is reported here
    ///   rather than silently row-converted.
    /// - [`TrinoError::ArrowDecode`] when an IPC payload fails to parse
    ///   or `concat_batches` rejects a schema mismatch across segments.
    /// - All other variants of [`TrinoError`] surface unchanged.
    pub async fn execute_arrow(&self, sql: &str) -> Result<RecordBatch, TrinoError> {
        let span = info_span!("statement.execute_arrow", adapter = "trino");
        async move { self.execute_arrow_inner(sql).await }
            .instrument(span)
            .await
    }

    async fn execute_arrow_inner(&self, sql: &str) -> Result<RecordBatch, TrinoError> {
        let deadline = Instant::now() + self.config().timeout;
        let coordinator = self.config().coordinator_url.clone();
        let submit_url = format!("{coordinator}/v1/statement");
        debug!(coordinator = %coordinator, "submitting Trino statement (spooled+arrow)");

        let headers = self.spooled_arrow_headers()?;
        let resp = self
            .http_client()
            .post(&submit_url)
            .headers(headers.clone())
            .body(sql.to_string())
            .send()
            .await?;
        let mut current = parse_spooled_response(resp).await?;

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut last_state: String;
        let mut attempt = 0_usize;
        loop {
            // Drain whatever segments / inline data the current page
            // carries. If `data` arrived as inline JSON rows the server
            // doesn't support Arrow — surface a clear error rather than
            // emit a synthetic batch.
            if let Some(data) = current.data.take() {
                match data {
                    SpooledData::Inline(_) => return Err(TrinoError::ArrowEncodingUnavailable),
                    SpooledData::Segments(segments) => {
                        for seg in segments {
                            let batch = self.fetch_segment(&seg).await?;
                            batches.push(batch);
                        }
                    }
                }
            }
            last_state = current.stats.state.clone();

            let Some(next) = current.next_uri.clone() else {
                if current.stats.state == "FINISHED" {
                    return concat_arrow_batches(&batches);
                }
                return Err(query_failed(&current));
            };

            if Instant::now() >= deadline {
                return Err(TrinoError::Timeout {
                    timeout_secs: self.config().timeout.as_secs(),
                    last_state,
                });
            }
            if !same_origin(&coordinator, &next) {
                return Err(TrinoError::UntrustedNextUri {
                    coordinator: coordinator.clone(),
                    next,
                });
            }

            tokio::time::sleep(poll_delay(attempt)).await;
            attempt += 1;

            let resp = self
                .http_client()
                .get(&next)
                .headers(headers.clone())
                .send()
                .await?;
            current = parse_spooled_response(resp).await?;
        }
    }

    /// Header set for the spooled-Arrow path. Reuses the base auth +
    /// catalog/schema headers from the standard `execute` path and adds
    /// the two spooling-negotiation headers on top.
    fn spooled_arrow_headers(&self) -> Result<HeaderMap, TrinoError> {
        let mut headers = self.base_headers()?;
        headers.insert(
            HeaderName::from_static(CLIENT_CAPABILITIES_HEADER),
            HeaderValue::from_static(CLIENT_CAPABILITIES_VALUE),
        );
        headers.insert(
            HeaderName::from_static(SPOOLED_ACCEPT_ENCODING_HEADER),
            HeaderValue::from_static(SPOOLED_ACCEPT_ENCODING_VALUE),
        );
        Ok(headers)
    }

    async fn fetch_segment(&self, seg: &Segment) -> Result<RecordBatch, TrinoError> {
        // Defensive: encoding metadata MUST start with "arrow" — if the
        // server somehow negotiated a non-Arrow spooled encoding (e.g.
        // server-side bug, mis-configured `protocol.spooling.encoding.*`)
        // refuse to decode rather than panic in `StreamReader::try_new`.
        if !seg.metadata.encoding.starts_with(ARROW_ENCODING_PREFIX) {
            return Err(TrinoError::ArrowEncodingUnavailable);
        }
        let bytes = match &seg.body {
            SegmentBody::Inline { data } => {
                // Inline Arrow IPC bytes are base64-encoded so the JSON
                // envelope can carry them. The spec encodes them as
                // standard (not URL-safe) base64.
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(data.as_bytes())
                    .map_err(|e| TrinoError::ArrowDecode(format!("inline base64: {e}")))?
            }
            SegmentBody::Spooled { uri } => {
                let resp = self.http_client().get(uri).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(TrinoError::HttpStatus {
                        status: status.as_u16(),
                        message: body,
                    });
                }
                resp.bytes().await?.to_vec()
            }
        };
        let batch = decode_arrow_ipc(&bytes)?;
        // Best-effort ack so the coordinator can release the spooled
        // segment. Failures here are non-fatal — the next polled page
        // will surface any coordinator-side complaint via `stats.state`.
        if let Some(ack) = seg.ack_uri.as_deref() {
            let _ = self.http_client().get(ack).send().await;
        }
        Ok(batch)
    }
}

/// Decode a single Arrow IPC stream payload into one concatenated
/// `RecordBatch`. The IPC stream MAY contain multiple batches; the
/// adapter contract (single-batch return) requires concatenation.
fn decode_arrow_ipc(bytes: &[u8]) -> Result<RecordBatch, TrinoError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| TrinoError::ArrowDecode(format!("StreamReader::try_new: {e}")))?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TrinoError::ArrowDecode(format!("reading IPC stream: {e}")))?;
    if batches.is_empty() {
        // An Arrow IPC stream carrying only a schema (no batches) is
        // legal but unusable here — surface as an empty batch with the
        // declared schema rather than fabricating column-less data.
        return RecordBatch::try_new(schema, Vec::new())
            .map_err(|e| TrinoError::ArrowDecode(format!("empty-batch construction: {e}")));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().expect("len==1"));
    }
    concat_batches(&schema, batches.iter())
        .map_err(|e| TrinoError::ArrowDecode(format!("concat_batches: {e}")))
}

/// Concatenate every per-segment `RecordBatch` into a single result
/// batch. Empty input maps to the documented `NoBatches` failure mode
/// — every other adapter (DuckDB) treats DDL / zero-row queries the
/// same way.
fn concat_arrow_batches(batches: &[RecordBatch]) -> Result<RecordBatch, TrinoError> {
    match batches.len() {
        0 => Err(TrinoError::ArrowDecode(
            "query returned no Arrow segments (DDL or empty result)".into(),
        )),
        1 => Ok(batches[0].clone()),
        _ => {
            let schema = batches[0].schema();
            concat_batches(&schema, batches.iter())
                .map_err(|e| TrinoError::ArrowDecode(format!("final concat: {e}")))
        }
    }
}

async fn parse_spooled_response(
    resp: reqwest::Response,
) -> Result<SpooledQueryResults, TrinoError> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(TrinoError::HttpStatus {
            status: status.as_u16(),
            message: body,
        });
    }
    resp.json::<SpooledQueryResults>()
        .await
        .map_err(TrinoError::Http)
}

fn query_failed(qr: &SpooledQueryResults) -> TrinoError {
    if let Some(err) = qr.error.as_ref() {
        TrinoError::QueryFailed {
            state: qr.stats.state.clone(),
            error_code: err.error_code.unwrap_or(-1),
            error_name: err.error_name.clone().unwrap_or_default(),
            message: err.message.clone().unwrap_or_else(|| "<no message>".into()),
        }
    } else {
        TrinoError::QueryFailed {
            state: qr.stats.state.clone(),
            error_code: -1,
            error_name: String::new(),
            message: format!(
                "Trino spooled query reached terminal state {} with no error block",
                qr.stats.state
            ),
        }
    }
}

/// Same-origin check shared with the JSON polling path.
///
/// Kept module-local to avoid widening `connector.rs`'s public surface
/// — this is the only other call site.
fn same_origin(base: &str, candidate: &str) -> bool {
    let Ok(base) = url::Url::parse(base) else {
        return false;
    };
    let Ok(cand) = url::Url::parse(candidate) else {
        return false;
    };
    base.scheme() == cand.scheme()
        && base.host_str().is_some()
        && base.host_str() == cand.host_str()
        && base.port_or_known_default() == cand.port_or_known_default()
}

/// Polling delay ladder mirroring `connector::poll_delay` — duplicated
/// here so this module doesn't widen `connector.rs`'s public surface
/// for a 6-line helper.
fn poll_delay(attempt: usize) -> Duration {
    const POLL_DELAY_STEPS_MS: [u64; 5] = [50, 100, 250, 500, 1000];
    const MAX_POLL_DELAY_MS: u64 = 5000;
    let delay_ms = if attempt < POLL_DELAY_STEPS_MS.len() {
        POLL_DELAY_STEPS_MS[attempt]
    } else {
        let extra = attempt - POLL_DELAY_STEPS_MS.len();
        (1000u64 * (1u64 << extra.min(3))).min(MAX_POLL_DELAY_MS)
    };
    Duration::from_millis(delay_ms)
}

// -- Wire types -------------------------------------------------------------
//
// The spooled response shape differs from the inline-JSON one only in
// the `data` field: a `Vec<Segment>` rather than a `Vec<Vec<JsonValue>>`.
// Everything else (id, nextUri, columns, stats, error) is identical.
// We re-declare the envelope locally rather than reusing `QueryResults`
// from connector.rs because the `data` variant carries Arrow-specific
// segment metadata that `QueryResults` doesn't need to know about.

#[derive(Debug, Clone, Deserialize)]
struct SpooledQueryResults {
    #[allow(dead_code)]
    id: String,
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    data: Option<SpooledData>,
    stats: QueryStats,
    error: Option<QueryError>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryStats {
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryError {
    message: Option<String>,
    error_code: Option<i64>,
    error_name: Option<String>,
}

/// Either a row-of-rows JSON payload (server fell back from Arrow) or
/// an array of spooled-segment descriptors (Arrow path honoured).
///
/// Trino's spooled-protocol responses encode `data` as a heterogeneous
/// JSON value: a 2-D array when the server didn't / can't honour the
/// requested encoding, and an array of `{type, uri, ackUri, ...}`
/// objects when it did. `serde(untagged)` discriminates on shape — the
/// inline-row variant catches the `[[..], [..]]` shape and short-circuits
/// to `ArrowEncodingUnavailable`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SpooledData {
    /// Inline JSON rows — server didn't honour Arrow encoding. The
    /// `Vec<Vec<JsonValue>>` body is what the JSON polling path expects;
    /// here it's a discriminator only (we surface `ArrowEncodingUnavailable`
    /// immediately on seeing this variant), but the field has to round-trip
    /// through serde so the untagged dispatch picks the right arm.
    Inline(#[allow(dead_code)] Vec<Vec<serde_json::Value>>),
    /// Spooled segments with Arrow IPC payloads.
    Segments(Vec<Segment>),
}

/// Spooled-segment descriptor.
///
/// Each segment carries either an inline base64-encoded body (small
/// segments the coordinator opts not to spill to object storage) or a
/// `uri` to fetch the raw Arrow IPC bytes from. `ackUri` releases the
/// segment on the server side after the client has consumed it.
#[derive(Debug, Clone, Deserialize)]
struct Segment {
    #[serde(rename = "ackUri")]
    ack_uri: Option<String>,
    metadata: SegmentMetadata,
    #[serde(flatten)]
    body: SegmentBody,
}

#[derive(Debug, Clone, Deserialize)]
struct SegmentMetadata {
    /// Spooling-encoding identifier — e.g. `arrow`, `arrow+zstd`,
    /// `json+lz4`. Used to gate the decoder.
    encoding: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum SegmentBody {
    /// Base64-encoded Arrow IPC payload embedded directly in the JSON
    /// envelope. Used for small segments the coordinator opts not to
    /// spill to the spooling object store.
    Inline { data: String },
    /// HTTP URI returning the raw Arrow IPC stream bytes. Coordinator
    /// signs the URI when the spooling backend requires presigning
    /// (S3, GCS, ABFS) so the client doesn't need credentials of its
    /// own.
    Spooled { uri: String },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use base64::Engine;
    use wiremock::matchers::{header, headers, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::{TrinoAuth, TrinoClientConfig};

    /// Encode a single `RecordBatch` as an Arrow IPC stream — the shape
    /// the coordinator would ship per spooled segment.
    fn encode_ipc(batch: &RecordBatch) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name = Arc::new(StringArray::from(vec!["alice", "bob", "carol"]));
        RecordBatch::try_new(schema, vec![id, name]).unwrap()
    }

    fn test_basic_auth() -> TrinoAuth {
        crate::test_helpers::test_basic_auth()
    }

    #[tokio::test]
    async fn execute_arrow_decodes_inline_segment() {
        // Inline segment (base64 Arrow IPC bytes embedded in the JSON
        // envelope) — exercises the decode path without needing a
        // second HTTP endpoint for segment fetches.
        let batch = make_batch();
        let ipc = encode_ipc(&batch);
        let inline_b64 = base64::engine::general_purpose::STANDARD.encode(&ipc);

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_000000_00001_arrow",
                "data": [
                    {
                        "type": "inline",
                        "data": inline_b64,
                        "metadata": {"encoding": "arrow"}
                    }
                ],
                "stats": {"state": "FINISHED"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, test_basic_auth());
        let got = client
            .execute_arrow("SELECT id, name FROM users")
            .await
            .unwrap();
        assert_eq!(got.num_rows(), 3);
        assert_eq!(got.num_columns(), 2);
        assert_eq!(got.schema().field(0).name(), "id");
        assert_eq!(got.schema().field(1).name(), "name");
    }

    #[tokio::test]
    async fn execute_arrow_follows_segment_uri_and_acks() {
        // Spooled segment served via a second URI — exercises the
        // separate `GET <segment_uri>` fetch + the best-effort `ackUri`
        // release.
        let batch = make_batch();
        let ipc = encode_ipc(&batch);

        let server = MockServer::start().await;
        let seg_uri = format!("{}/v1/segment/abc", server.uri());
        let ack_uri = format!("{}/v1/segment/abc/ack", server.uri());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_000000_00002_arrow",
                "data": [
                    {
                        "type": "spooled",
                        "uri": seg_uri,
                        "ackUri": ack_uri,
                        "metadata": {"encoding": "arrow+zstd"}
                    }
                ],
                "stats": {"state": "FINISHED"}
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/segment/abc"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(ipc)
                    .insert_header("content-type", "application/vnd.apache.arrow.stream"),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/v1/segment/abc/ack"))
            .respond_with(ResponseTemplate::new(200))
            // ack is best-effort but in the happy path it does fire.
            .expect(1)
            .mount(&server)
            .await;

        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, test_basic_auth());
        let got = client.execute_arrow("SELECT * FROM users").await.unwrap();
        assert_eq!(got.num_rows(), 3);
    }

    #[tokio::test]
    async fn execute_arrow_errors_when_server_falls_back_to_inline_rows() {
        // Coordinator without Arrow support (current shipping behaviour
        // — Trino 481 supports only json / json+lz4 / json+zstd) drops
        // the negotiated encoding and ships the regular inline-JSON
        // row shape. The adapter MUST surface this as a clear error
        // rather than silently degrading.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_000000_00003_fallback",
                "data": [[1, "alice"], [2, "bob"]],
                "stats": {"state": "FINISHED"}
            })))
            .mount(&server)
            .await;

        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, test_basic_auth());
        let err = client.execute_arrow("SELECT 1").await.unwrap_err();
        assert!(
            matches!(err, TrinoError::ArrowEncodingUnavailable),
            "expected ArrowEncodingUnavailable, got {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_arrow_concats_segments_across_polls() {
        // Multi-page response: first poll ships segment A, second poll
        // (via nextUri) ships segment B + closes the query. The two
        // batches must concat into a single 6-row RecordBatch.
        let server = MockServer::start().await;

        let batch_a = make_batch();
        let batch_b = {
            let schema = batch_a.schema();
            let id = Arc::new(Int32Array::from(vec![10, 20, 30])) as Arc<_>;
            let name = Arc::new(StringArray::from(vec!["dave", "eve", "frank"])) as Arc<_>;
            RecordBatch::try_new(schema, vec![id, name]).unwrap()
        };
        let ipc_a = encode_ipc(&batch_a);
        let ipc_b = encode_ipc(&batch_b);
        let b64_a = base64::engine::general_purpose::STANDARD.encode(&ipc_a);
        let b64_b = base64::engine::general_purpose::STANDARD.encode(&ipc_b);
        let next = format!("{}/v1/statement/q/1/2", server.uri());

        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_multi",
                "nextUri": next,
                "data": [
                    {"type": "inline", "data": b64_a, "metadata": {"encoding": "arrow"}}
                ],
                "stats": {"state": "RUNNING"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/statement/q/1/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_multi",
                "data": [
                    {"type": "inline", "data": b64_b, "metadata": {"encoding": "arrow"}}
                ],
                "stats": {"state": "FINISHED"}
            })))
            .mount(&server)
            .await;

        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, test_basic_auth());
        let got = client.execute_arrow("SELECT * FROM users").await.unwrap();
        assert_eq!(got.num_rows(), 6);
    }

    #[tokio::test]
    async fn execute_arrow_sends_spooling_negotiation_headers() {
        // Pin the wire-level negotiation: the spooling-capability and
        // accept-encoding headers MUST appear on the POST /v1/statement
        // request so the coordinator knows to ship Arrow segments. The
        // mock returns no body shape that exercises the decoder — we
        // only care that the request matched.
        let server = MockServer::start().await;
        // wiremock's `header()` matcher splits the request value on `,`
        // before comparison — `arrow+zstd,arrow` arrives as two items —
        // so we match the split list via `headers(key, vec![...])`.
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .and(header(
                CLIENT_CAPABILITIES_HEADER,
                CLIENT_CAPABILITIES_VALUE,
            ))
            .and(headers(
                SPOOLED_ACCEPT_ENCODING_HEADER,
                vec!["arrow+zstd", "arrow"],
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_headers",
                "stats": {"state": "FINISHED"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = TrinoClientConfig::new(server.uri()).with_timeout(Duration::from_secs(5));
        let client = TrinoClient::new(cfg, test_basic_auth());
        // The query terminates with `FINISHED` + zero batches, which
        // maps to `ArrowDecode("query returned no Arrow segments...")`
        // — the strict header match is the receipt we care about; the
        // mock would 404 if the headers weren't present.
        let _ = client.execute_arrow("SELECT 1").await;
    }

    #[test]
    fn decode_arrow_ipc_round_trips_a_simple_batch() {
        let batch = make_batch();
        let ipc = encode_ipc(&batch);
        let decoded = decode_arrow_ipc(&ipc).unwrap();
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 2);
    }

    #[test]
    fn decode_arrow_ipc_surfaces_garbage_as_error() {
        let err = decode_arrow_ipc(b"not arrow ipc").unwrap_err();
        assert!(matches!(err, TrinoError::ArrowDecode(_)));
    }
}
