//! ClickHouse HTTP interface client.
//!
//! One statement per request: the SQL is the `POST` body, the session
//! settings are URL parameters, and the credentials are the
//! `X-ClickHouse-User` / `X-ClickHouse-Key` headers (never the URL, which
//! proxies and server logs record). The HTTP interface refuses a
//! multi-statement body ("Multi-statements are not allowed"), so a dialect
//! that needs several statements returns them as separate strings.
//!
//! Settings sent with every statement:
//!
//! | Setting | Why |
//! |---|---|
//! | `wait_end_of_query=1` | The server buffers the response, so a failure surfaces as an HTTP error status, never as an error appended to a `200` body. |
//! | `max_execution_time` | The configured timeout, enforced by the server too. |
//! | `session_timezone=UTC` | String ↔ `DateTime` conversions (time windows, watermarks, `Date` promotion) happen in UTC whatever the server's own time zone. |
//! | `date_time_output_format=iso` | `DateTime` cells come back as RFC 3339 UTC (`2026-01-02T03:04:05Z`), the shape Rocky's watermark reader parses. |
//! | `join_use_nulls=1` | An outer join's unmatched side is `NULL`, as in standard SQL and as the compiler's nullability inference assumes, not the column type's default value. |
//! | `mutations_sync=2` | A mutation (`ALTER TABLE … UPDATE/DELETE`) returns once it is applied. |
//!
//! `session_timezone` needs ClickHouse 23.6 or later; an older server
//! refuses every statement with "Unknown setting", loudly.
//!
//! Result cells come back as JSON (`JSONCompact`) and are normalized to the
//! shape the PostgreSQL adapter returns: `NULL` is JSON `null`, every other
//! scalar is its text form (`"42"`, `"true"`, `"2026-01-02T03:04:05Z"`), and
//! an array / map / tuple is its compact JSON text.

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue};
use rocky_core::traits::QueryResult;
use serde::Deserialize;
use thiserror::Error;
use tracing::debug;

use crate::config::ChConfig;

/// Errors surfaced by the ClickHouse connector.
#[derive(Debug, Error)]
pub enum ChError {
    /// The adapter configuration is invalid.
    #[error("invalid clickhouse configuration: {0}")]
    Config(String),

    /// The request could not be sent or the response could not be read.
    #[error("clickhouse HTTP transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// The client-side deadline passed. The server may still have run the
    /// statement, so this is never retried.
    #[error("clickhouse statement timed out after {secs}s")]
    Timeout { secs: u64 },

    /// The server rejected the statement.
    #[error("clickhouse error{}: {message}", code_suffix(*.code, .name.as_deref(), *.status))]
    Server {
        /// The `X-ClickHouse-Exception-Code` (e.g. `60` for `UNKNOWN_TABLE`).
        code: Option<i32>,
        /// The error name the message ends with (`UNKNOWN_TABLE`).
        name: Option<String>,
        /// HTTP status.
        status: u16,
        message: String,
    },

    /// The response body did not have the expected shape.
    #[error("clickhouse response could not be decoded: {0}")]
    Decode(String),

    /// `describe_table` found no columns: the table does not exist.
    #[error("table {database}.{table} does not exist")]
    NotFound { database: String, table: String },
}

fn code_suffix(code: Option<i32>, name: Option<&str>, status: u16) -> String {
    match (code, name) {
        (Some(c), Some(n)) => format!(" {c} ({n})"),
        (Some(c), None) => format!(" {c}"),
        (None, _) => format!(" (HTTP {status})"),
    }
}

/// `UNKNOWN_TABLE`.
pub const UNKNOWN_TABLE: i32 = 60;
/// `UNKNOWN_DATABASE`.
pub const UNKNOWN_DATABASE: i32 = 81;
/// `TOO_MANY_SIMULTANEOUS_QUERIES`: refused before running.
pub const TOO_MANY_SIMULTANEOUS_QUERIES: i32 = 202;
/// `AUTHENTICATION_FAILED`.
pub const AUTHENTICATION_FAILED: i32 = 516;

impl ChError {
    /// Whether retrying the statement is safe and may succeed: the request
    /// never reached the server (connection refused), or the server refused
    /// it before running it (`TOO_MANY_SIMULTANEOUS_QUERIES`, or a `503`
    /// with no ClickHouse error code, i.e. a proxy or load balancer).
    ///
    /// Deliberately narrow. A timeout or a dropped connection mid-request is
    /// not here: ClickHouse has no transactions, so an `INSERT` the client
    /// lost track of may have landed, and running it again would duplicate
    /// rows.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            ChError::Transport(e) => e.is_connect(),
            ChError::Server {
                code: Some(TOO_MANY_SIMULTANEOUS_QUERIES),
                ..
            } => true,
            ChError::Server {
                code: None,
                status: 503,
                ..
            } => true,
            _ => false,
        }
    }

    /// Whether the error proves the table (or its database) is absent.
    #[must_use]
    pub fn is_missing_object(&self) -> bool {
        matches!(
            self,
            ChError::NotFound { .. }
                | ChError::Server {
                    code: Some(UNKNOWN_TABLE | UNKNOWN_DATABASE),
                    ..
                }
        )
    }
}

/// HTTP client for one ClickHouse endpoint. Cheap to share: `reqwest`
/// pools connections internally.
pub struct ChClient {
    config: ChConfig,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct JsonCompact {
    #[serde(default)]
    meta: Vec<JsonMeta>,
    #[serde(default)]
    data: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct JsonMeta {
    name: String,
}

#[derive(Deserialize)]
struct Summary {
    #[serde(default)]
    written_rows: Option<String>,
}

impl ChClient {
    /// Build a client. Reads `ca_cert` from disk when set.
    ///
    /// # Errors
    ///
    /// Returns [`ChError::Config`] when the CA certificate cannot be read or
    /// parsed, or the HTTP client cannot be built.
    pub fn new(config: ChConfig) -> Result<Self, ChError> {
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            // The statement deadline plus a margin, so the server's own
            // `max_execution_time` error normally arrives first.
            .timeout(config.timeout + Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(30));
        if let Some(path) = &config.ca_cert {
            let pem = std::fs::read(path)
                .map_err(|e| ChError::Config(format!("cannot read ca_cert '{path}': {e}")))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| ChError::Config(format!("ca_cert '{path}' is not PEM: {e}")))?;
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        let http = builder
            .build()
            .map_err(|e| ChError::Config(format!("cannot build the HTTP client: {e}")))?;
        Ok(Self { config, http })
    }

    /// The connection configuration.
    #[must_use]
    pub fn config(&self) -> &ChConfig {
        &self.config
    }

    fn headers(&self) -> Result<HeaderMap, ChError> {
        let mut headers = HeaderMap::new();
        let value = |v: &str, what: &str| {
            HeaderValue::from_str(v).map_err(|_| {
                ChError::Config(format!(
                    "{what} contains characters an HTTP header cannot carry"
                ))
            })
        };
        headers.insert("X-ClickHouse-User", value(&self.config.user, "username")?);
        if let Some(password) = &self.config.password {
            let mut key = value(password, "password")?;
            key.set_sensitive(true);
            headers.insert("X-ClickHouse-Key", key);
        }
        Ok(headers)
    }

    async fn send(&self, sql: &str, format: Option<&str>) -> Result<(HeaderMap, String), ChError> {
        let sql = sql.trim().trim_end_matches(';').trim_end();
        let secs = self.config.timeout.as_secs().max(1);
        let mut params: Vec<(&str, String)> = vec![
            ("database", self.config.database.clone()),
            ("wait_end_of_query", "1".into()),
            ("max_execution_time", secs.to_string()),
            ("session_timezone", "UTC".into()),
            ("date_time_output_format", "iso".into()),
            ("join_use_nulls", "1".into()),
            ("mutations_sync", "2".into()),
        ];
        if let Some(format) = format {
            params.push(("default_format", format.into()));
        }
        debug!(sql, "clickhouse statement");
        let response = self
            .http
            .post(self.config.base_url())
            .query(&params)
            .headers(self.headers()?)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| self.map_transport(e))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.text().await.map_err(|e| self.map_transport(e))?;
        if !status.is_success() {
            return Err(server_error(status.as_u16(), &headers, &body));
        }
        Ok((headers, body))
    }

    fn map_transport(&self, e: reqwest::Error) -> ChError {
        if e.is_timeout() {
            ChError::Timeout {
                secs: self.config.timeout.as_secs(),
            }
        } else {
            ChError::Transport(e)
        }
    }

    /// Run a statement, discarding any result. Returns the rows the server
    /// reports written (`X-ClickHouse-Summary`), when it reports any.
    ///
    /// # Errors
    ///
    /// Returns [`ChError`] on transport failure, server rejection or
    /// timeout.
    pub async fn execute(&self, sql: &str) -> Result<Option<u64>, ChError> {
        let (headers, _) = self.send(sql, None).await?;
        Ok(written_rows(&headers))
    }

    /// Run a query and return its rows. A statement that returns no result
    /// set (DDL, `INSERT`) yields an empty [`QueryResult`].
    ///
    /// # Errors
    ///
    /// Returns [`ChError`] on transport failure, server rejection, timeout,
    /// or a body that is not `JSONCompact`.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, ChError> {
        let (_, body) = self.send(sql, Some("JSONCompact")).await?;
        parse_json_compact(&body)
    }

    /// `SELECT 1`.
    ///
    /// # Errors
    ///
    /// See [`Self::execute`].
    pub async fn ping(&self) -> Result<(), ChError> {
        self.execute("SELECT 1").await.map(|_| ())
    }
}

fn written_rows(headers: &HeaderMap) -> Option<u64> {
    let raw = headers.get("X-ClickHouse-Summary")?.to_str().ok()?;
    let summary: Summary = serde_json::from_str(raw).ok()?;
    summary.written_rows?.parse().ok()
}

/// Build a [`ChError::Server`] from an error response. The code comes from
/// `X-ClickHouse-Exception-Code`, falling back to the `Code: N.` prefix of
/// the body; the name from the body's `(NAME)` suffix.
fn server_error(status: u16, headers: &HeaderMap, body: &str) -> ChError {
    let message = body.trim().to_string();
    let code = headers
        .get("X-ClickHouse-Exception-Code")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
        .or_else(|| {
            message
                .strip_prefix("Code: ")?
                .split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        });
    ChError::Server {
        code,
        name: error_name(&message),
        status,
        message,
    }
}

/// The `UPPER_SNAKE` name in the last `(…)` group of a ClickHouse message
/// that holds nothing else (the server appends it after the text), e.g. `UNKNOWN_TABLE` in
/// `Code: 60. DB::Exception: … (UNKNOWN_TABLE) (version 26.1 …)`.
fn error_name(message: &str) -> Option<String> {
    message
        .match_indices('(')
        .filter_map(|(i, _)| {
            let rest = &message[i + 1..];
            let name = &rest[..rest.find(')')?];
            (!name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && name.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
            .then(|| name.to_string())
        })
        .next_back()
}

fn parse_json_compact(body: &str) -> Result<QueryResult, ChError> {
    if body.trim().is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    }
    let parsed: JsonCompact =
        serde_json::from_str(body).map_err(|e| ChError::Decode(e.to_string()))?;
    Ok(QueryResult {
        columns: parsed.meta.into_iter().map(|m| m.name).collect(),
        rows: parsed
            .data
            .into_iter()
            .map(|row| row.into_iter().map(normalize_cell).collect())
            .collect(),
    })
}

/// Text form for every scalar, compact JSON text for a composite; `NULL`
/// stays JSON `null`.
fn normalize_cell(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Null => Value::Null,
        Value::String(_) => value,
        Value::Bool(b) => Value::String(b.to_string()),
        Value::Number(n) => Value::String(n.to_string()),
        Value::Array(_) | Value::Object(_) => Value::String(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_compact_cells_normalize_to_text() {
        let body = r#"{"meta":[{"name":"id","type":"Int64"},{"name":"ok","type":"Bool"},
            {"name":"x","type":"Nullable(Float64)"},{"name":"a","type":"Array(Int32)"},
            {"name":"ts","type":"DateTime"}],
            "data":[["42", true, null, [1,2], "2026-01-02T03:04:05Z"],
                    [7, false, 1.5, [], "2026-01-02T03:04:06Z"]],
            "rows":2}"#;
        let r = parse_json_compact(body).unwrap();
        assert_eq!(r.columns, ["id", "ok", "x", "a", "ts"]);
        assert_eq!(
            r.rows[0],
            vec![
                serde_json::json!("42"),
                serde_json::json!("true"),
                serde_json::Value::Null,
                serde_json::json!("[1,2]"),
                serde_json::json!("2026-01-02T03:04:05Z"),
            ]
        );
        assert_eq!(r.rows[1][0], serde_json::json!("7"));
        assert_eq!(r.rows[1][2], serde_json::json!("1.5"));
        assert!(parse_json_compact("").unwrap().rows.is_empty());
        assert!(parse_json_compact("not json").is_err());
    }

    #[test]
    fn server_errors_carry_code_and_name() {
        let body = "Code: 60. DB::Exception: Unknown table expression identifier 't1.nope'. \
                    Maybe you meant system.one? In scope SELECT * FROM t1.nope. (UNKNOWN_TABLE) \
                    (version 26.10.1.1496 (official build))";
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-ClickHouse-Exception-Code",
            HeaderValue::from_static("60"),
        );
        let err = server_error(404, &headers, body);
        let ChError::Server { code, name, .. } = &err else {
            panic!("{err:?}");
        };
        assert_eq!(*code, Some(60));
        assert_eq!(name.as_deref(), Some("UNKNOWN_TABLE"));
        assert!(err.is_missing_object());
        assert!(!err.is_transient());
        // No header: the body prefix still carries the code.
        let err = server_error(
            404,
            &HeaderMap::new(),
            "Code: 81. DB::Exception: Database nope does not exist. (UNKNOWN_DATABASE) (version 1)",
        );
        assert!(err.is_missing_object(), "{err}");
        assert!(err.to_string().contains("81 (UNKNOWN_DATABASE)"), "{err}");
    }

    #[test]
    fn transient_errors_are_the_ones_that_never_ran() {
        let server = |code: Option<i32>, status: u16| ChError::Server {
            code,
            name: None,
            status,
            message: String::new(),
        };
        assert!(server(Some(TOO_MANY_SIMULTANEOUS_QUERIES), 500).is_transient());
        assert!(server(None, 503).is_transient());
        assert!(!server(Some(UNKNOWN_TABLE), 404).is_transient());
        assert!(!server(Some(AUTHENTICATION_FAILED), 403).is_transient());
        assert!(!server(Some(241), 500).is_transient());
        assert!(!ChError::Timeout { secs: 1 }.is_transient());
    }

    #[test]
    fn summary_header_reports_written_rows() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-ClickHouse-Summary",
            HeaderValue::from_static(r#"{"read_rows":"5","written_rows":"3"}"#),
        );
        assert_eq!(written_rows(&headers), Some(3));
        assert_eq!(written_rows(&HeaderMap::new()), None);
    }
}
