//! Native PostgreSQL wire-protocol client with a small connection pool.
//!
//! Every statement runs over the **simple query protocol**
//! (`Client::simple_query` / `batch_execute`). That choice is load-bearing:
//!
//! - One call may carry several `;`-separated statements, and the server runs
//!   them as **one implicit transaction** (PostgreSQL docs, "Multiple
//!   Statements in a Simple Query"). The dialect relies on this to make
//!   multi-statement writes atomic on a pooled connection: a full refresh
//!   (`DROP TABLE IF EXISTS …; CREATE TABLE … AS …`), a `time_interval`
//!   overwrite and a `delete_insert` (`DELETE …; INSERT …`, via
//!   `SqlDialect::delete_insert_statements`) are each one string, so either
//!   every statement commits or none does — and no other task's statement
//!   can land between them.
//! - Results come back as text, which is the shape every other Rocky adapter
//!   already returns in [`QueryResult`] cells.
//!
//! A connection that returns any error is dropped instead of going back to
//! the pool, so a session left in an aborted transaction is never reused.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rocky_core::traits::QueryResult;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tracing::debug;

use crate::config::{Flavor, PgConfig, SslMode};
use crate::tls::{RustlsConnect, Verification, client_config};

/// Errors from the PostgreSQL / Redshift connector.
#[derive(thiserror::Error)]
pub enum PgError {
    /// Invalid adapter configuration.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// TLS setup failed before a connection was attempted.
    #[error("TLS setup failed: {0}")]
    Tls(String),
    /// Could not open a connection (network, TLS handshake, authentication).
    #[error("connection to {host}:{port} failed: {message}")]
    Connect {
        host: String,
        port: u16,
        message: String,
        /// SQLSTATE when the server answered (e.g. `28P01` bad password).
        sqlstate: Option<String>,
    },
    /// The server rejected a statement.
    #[error("{message} (SQLSTATE {sqlstate})")]
    Query {
        sqlstate: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
    /// The connection dropped mid-statement or the client failed locally.
    #[error("connection error: {0}")]
    Transport(String),
    /// A statement exceeded the configured timeout on the client side.
    #[error("statement exceeded the {secs}s timeout")]
    Timeout { secs: u64 },
    /// A described table does not exist (or is not visible to this role).
    #[error("table {schema}.{table} not found")]
    NotFound { schema: String, table: String },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "PgError", self)
    }
}

impl PgError {
    /// The server's SQLSTATE, when there is one.
    #[must_use]
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            PgError::Query { sqlstate, .. } => Some(sqlstate),
            PgError::Connect { sqlstate, .. } => sqlstate.as_deref(),
            _ => None,
        }
    }

    /// Whether retrying the same statement may succeed.
    ///
    /// Grounded in the SQLSTATE classes PostgreSQL documents (Appendix A):
    /// class 08 (connection exception), `40001` serialization failure,
    /// `40P01` deadlock, `53300` too many connections, `57P01`/`57P02`/
    /// `57P03` server shutting down / crash / cannot connect now. Redshift
    /// reuses these codes.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            PgError::Transport(_) => true,
            // The client stopped waiting, but the server may still commit
            // the statement. A write retried after that could apply twice
            // (an append, a partition overwrite), so a timeout fails closed.
            PgError::Timeout { .. } => false,
            PgError::Connect { sqlstate, .. } => match sqlstate.as_deref() {
                // Authentication failures (class 28) and unknown database
                // (3D000) will not heal on retry.
                Some(code) => is_transient_sqlstate(code),
                // No server answer at all: a network failure.
                None => true,
            },
            PgError::Query { sqlstate, .. } => is_transient_sqlstate(sqlstate),
            PgError::Config(_) | PgError::Tls(_) | PgError::NotFound { .. } => false,
        }
    }

    /// Whether the error says the referenced relation or schema is absent.
    #[must_use]
    pub fn is_missing_object(&self) -> bool {
        match self {
            PgError::NotFound { .. } => true,
            // 42P01 undefined_table, 3F000 invalid_schema_name.
            PgError::Query { sqlstate, .. } => matches!(sqlstate.as_str(), "42P01" | "3F000"),
            _ => false,
        }
    }

    /// Whether the error is an authentication / authorization failure.
    #[must_use]
    pub fn is_auth(&self) -> bool {
        // Class 28 invalid authorization; 42501 insufficient_privilege.
        self.sqlstate()
            .is_some_and(|c| c.starts_with("28") || c == "42501")
    }
}

fn is_transient_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || matches!(
            code,
            "40001" | "40P01" | "53300" | "57P01" | "57P02" | "57P03"
        )
}

/// `tokio_postgres::Error`'s `Display` omits its cause ("error performing
/// TLS handshake"); the cause is what an operator needs ("invalid peer
/// certificate: UnknownIssuer").
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

fn map_query_error(err: &tokio_postgres::Error) -> PgError {
    if let Some(db) = err.as_db_error() {
        return PgError::Query {
            sqlstate: db.code().code().to_string(),
            message: db.message().to_string(),
            detail: db.detail().map(str::to_string),
            hint: db.hint().map(str::to_string),
        };
    }
    PgError::Transport(error_chain(err))
}

/// Pooled PostgreSQL / Redshift client.
pub struct PgClient {
    config: PgConfig,
    tls: Option<RustlsConnect>,
    idle: Mutex<Vec<Client>>,
    permits: Arc<Semaphore>,
}

/// A connection checked out of the pool. Returned on [`PooledConn::release`];
/// dropped (closed) otherwise.
struct PooledConn<'a> {
    client: Option<Client>,
    pool: &'a PgClient,
    _permit: OwnedSemaphorePermit,
}

impl PooledConn<'_> {
    fn client(&self) -> &Client {
        self.client.as_ref().expect("client present until release")
    }

    /// Put a healthy connection back in the pool.
    fn release(mut self) {
        if let Some(client) = self.client.take()
            && !client.is_closed()
        {
            self.pool
                .idle
                .lock()
                .expect("idle pool lock poisoned")
                .push(client);
        }
    }
}

impl PgClient {
    /// Build a client. No connection is opened until the first statement.
    ///
    /// # Errors
    ///
    /// Returns [`PgError::Tls`] when the TLS configuration cannot be built
    /// (an unreadable `sslrootcert`).
    pub fn new(config: PgConfig) -> Result<Self, PgError> {
        let tls = match config.sslmode {
            SslMode::Disable => None,
            SslMode::Prefer | SslMode::Require => {
                Some(RustlsConnect::new(client_config(Verification::None, None)?))
            }
            SslMode::VerifyFull => Some(RustlsConnect::new(client_config(
                Verification::Full,
                config.sslrootcert.as_deref(),
            )?)),
        };
        let permits = Arc::new(Semaphore::new(config.max_connections));
        Ok(Self {
            config,
            tls,
            idle: Mutex::new(Vec::new()),
            permits,
        })
    }

    /// The configuration this client connects with.
    #[must_use]
    pub fn config(&self) -> &PgConfig {
        &self.config
    }

    async fn connect(&self) -> Result<Client, PgError> {
        let cfg = &self.config;
        let mut pg = tokio_postgres::Config::new();
        pg.host(&cfg.host)
            .port(cfg.port)
            .dbname(&cfg.database)
            .user(&cfg.user)
            .application_name("rocky")
            .connect_timeout(cfg.timeout);
        if let Some(pw) = &cfg.password {
            pg.password(pw);
        }
        pg.ssl_mode(match cfg.sslmode {
            SslMode::Disable => tokio_postgres::config::SslMode::Disable,
            SslMode::Prefer => tokio_postgres::config::SslMode::Prefer,
            SslMode::Require | SslMode::VerifyFull => tokio_postgres::config::SslMode::Require,
        });

        let connect_err = |e: tokio_postgres::Error| PgError::Connect {
            // The message prints, so a resolved `${VAR}` host shows as
            // `${NAME}` (#1919).
            host: rocky_core::secret_registry::render_placeholders(&cfg.host),
            port: cfg.port,
            sqlstate: e.code().map(|c| c.code().to_string()),
            message: rocky_core::secret_registry::render_placeholders(
                &e.as_db_error()
                    .map_or_else(|| error_chain(&e), |db| db.message().to_string()),
            ),
        };

        let client = match &self.tls {
            None => {
                let (client, conn) = pg.connect(NoTls).await.map_err(connect_err)?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!(error = %e, "postgres connection closed with error");
                    }
                });
                client
            }
            Some(tls) => {
                let (client, conn) = pg.connect(tls.clone()).await.map_err(connect_err)?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        debug!(error = %e, "postgres connection closed with error");
                    }
                });
                client
            }
        };

        // Session settings every statement relies on: UTC + ISO output so
        // timestamp cells parse the same way on every server, and the
        // statement timeout so a runaway query is cancelled server-side.
        let timeout_ms = cfg.timeout.as_millis().min(i32::MAX as u128);
        let mut setup = format!(
            "SET timezone TO 'UTC'; SET datestyle TO 'ISO, MDY'; SET statement_timeout TO {timeout_ms}"
        );
        // The Postgres dialect encodes literals under the standard rule
        // (`LiteralEscape::Standard`); pin the setting that makes it true so
        // a server configured with `standard_conforming_strings = off`
        // cannot turn a backslash into an escape. Redshift's lexer is the
        // backslash one and its dialect says so.
        if cfg.flavor == Flavor::Postgres {
            setup.push_str("; SET standard_conforming_strings = on");
        }
        client
            .batch_execute(&setup)
            .await
            .map_err(|e| map_query_error(&e))?;
        Ok(client)
    }

    async fn checkout(&self) -> Result<PooledConn<'_>, PgError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| PgError::Transport("connection pool closed".into()))?;
        let reused = {
            let mut idle = self.idle.lock().expect("idle pool lock poisoned");
            let mut found = None;
            while let Some(client) = idle.pop() {
                if !client.is_closed() {
                    found = Some(client);
                    break;
                }
            }
            found
        };
        let client = match reused {
            Some(c) => c,
            None => self.connect().await?,
        };
        Ok(PooledConn {
            client: Some(client),
            pool: self,
            _permit: permit,
        })
    }

    /// Client-side deadline: the server-side `statement_timeout` plus a
    /// grace period, so a dead TCP peer cannot hang a run forever.
    fn deadline(&self) -> Duration {
        self.config.timeout + Duration::from_secs(30)
    }

    /// Run one or more `;`-separated statements as a single implicit
    /// transaction. Returns the row count of the last command.
    ///
    /// # Errors
    ///
    /// Returns [`PgError`] on connection failure, server rejection, or
    /// timeout. The whole string is rolled back on any failure.
    pub async fn execute(&self, sql: &str) -> Result<Option<u64>, PgError> {
        let conn = self.checkout().await?;
        debug!(flavor = ?self.config.flavor, sql, "postgres execute");
        let fut = conn.client().simple_query(sql);
        let messages = match tokio::time::timeout(self.deadline(), fut).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(map_query_error(&e)),
            Err(_) => {
                return Err(PgError::Timeout {
                    secs: self.deadline().as_secs(),
                });
            }
        };
        conn.release();
        Ok(messages.iter().rev().find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        }))
    }

    /// Run a query and return the last result set as text cells.
    ///
    /// `NULL` is JSON `null`; every other value is the server's text form.
    /// Booleans are normalized to `true` / `false` (the server sends `t` /
    /// `f`) when the column is known to be boolean — see
    /// [`Self::query_typed`].
    ///
    /// # Errors
    ///
    /// Returns [`PgError`] on connection failure, server rejection, or
    /// timeout.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, PgError> {
        let conn = self.checkout().await?;
        debug!(flavor = ?self.config.flavor, sql, "postgres query");
        let fut = conn.client().simple_query(sql);
        let messages = match tokio::time::timeout(self.deadline(), fut).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(map_query_error(&e)),
            Err(_) => {
                return Err(PgError::Timeout {
                    secs: self.deadline().as_secs(),
                });
            }
        };
        conn.release();
        Ok(messages_to_result(&messages))
    }

    /// [`Self::query`] plus per-column type names, read by preparing the
    /// statement first (one extra round trip). Used where a cell's meaning
    /// depends on its type (boolean `t`/`f`, `timestamptz` offsets). Falls
    /// back to untyped text when the statement cannot be prepared (e.g. a
    /// multi-statement string).
    ///
    /// # Errors
    ///
    /// Returns [`PgError`] on connection failure, server rejection, or
    /// timeout.
    pub async fn query_typed(&self, sql: &str) -> Result<QueryResult, PgError> {
        let types: Option<Vec<String>> = {
            let conn = self.checkout().await?;
            match conn.client().prepare(sql).await {
                Ok(stmt) => {
                    let names = stmt
                        .columns()
                        .iter()
                        .map(|c| c.type_().name().to_string())
                        .collect();
                    conn.release();
                    Some(names)
                }
                Err(e) => {
                    // A rejected prepare is still a rejected query; the
                    // simple-protocol run below reports the real error.
                    debug!(error = %e, "prepare failed; returning untyped cells");
                    None
                }
            }
        };
        let mut result = self.query(sql).await?;
        if let Some(types) = types
            && types.len() == result.columns.len()
        {
            for row in &mut result.rows {
                for (cell, ty) in row.iter_mut().zip(&types) {
                    normalize_cell(cell, ty);
                }
            }
        }
        Ok(result)
    }

    /// Run `sql` and report whether it succeeded, discarding the result.
    /// Used by `ping`.
    ///
    /// # Errors
    ///
    /// See [`Self::execute`].
    pub async fn ping(&self) -> Result<(), PgError> {
        self.execute("SELECT 1").await.map(|_| ())
    }

    /// The warehouse flavor.
    #[must_use]
    pub fn flavor(&self) -> Flavor {
        self.config.flavor
    }
}

fn messages_to_result(messages: &[SimpleQueryMessage]) -> QueryResult {
    // A multi-statement string yields one RowDescription per result set;
    // keep the last one, matching what a caller of a single SELECT expects.
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    for message in messages {
        match message {
            SimpleQueryMessage::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name().to_string()).collect();
                rows.clear();
            }
            SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                }
                rows.push(
                    (0..row.len())
                        .map(|i| {
                            row.get(i).map_or(serde_json::Value::Null, |s| {
                                serde_json::Value::String(s.to_string())
                            })
                        })
                        .collect(),
                );
            }
            SimpleQueryMessage::CommandComplete(_) => {}
            _ => {}
        }
    }
    QueryResult { columns, rows }
}

/// Rewrite a text cell into the shape the rest of Rocky reads, by its
/// PostgreSQL type name.
fn normalize_cell(cell: &mut serde_json::Value, type_name: &str) {
    let serde_json::Value::String(text) = cell else {
        return;
    };
    match type_name {
        "bool" => {
            let normalized = match text.as_str() {
                "t" => "true",
                "f" => "false",
                _ => return,
            };
            *cell = serde_json::Value::String(normalized.to_string());
        }
        // Under `TIME ZONE 'UTC'` the server prints `2026-01-02 03:04:05.25+00`.
        // RFC 3339 is what `DateTime<Utc>::from_str` reads.
        "timestamptz" => {
            if let Some(rfc) = timestamptz_to_rfc3339(text) {
                *cell = serde_json::Value::String(rfc);
            }
        }
        _ => {}
    }
}

fn timestamptz_to_rfc3339(text: &str) -> Option<String> {
    // Offset forms: `+00`, `+05:30`, `-03`.
    let split = text.rfind(['+', '-']).filter(|&i| i > 10)?;
    let (local, offset) = text.split_at(split);
    let offset = if offset.len() == 3 {
        format!("{offset}:00")
    } else {
        offset.to_string()
    };
    let parsed =
        chrono::DateTime::parse_from_str(&format!("{local}{offset}"), "%Y-%m-%d %H:%M:%S%.f%:z")
            .ok()?;
    Some(
        parsed
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_debug_prints_a_resolved_value_as_its_name() {
        const SECRET: &str = "s3cr3t-value-123";
        rocky_core::secret_registry::register_substitution("RV_PG_DBG", SECRET);
        let err = PgError::Connect {
            host: format!("db-{SECRET}"),
            port: 5432,
            message: "refused".into(),
            sqlstate: None,
        };
        let debug = format!("{err:?}");
        assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_PG_DBG}"), "{debug}");
    }

    #[test]
    fn transient_classification_follows_sqlstate_classes() {
        let q = |code: &str| PgError::Query {
            sqlstate: code.into(),
            message: String::new(),
            detail: None,
            hint: None,
        };
        assert!(q("40001").is_transient());
        assert!(q("40P01").is_transient());
        assert!(q("08006").is_transient());
        assert!(q("57P01").is_transient());
        assert!(!q("42P01").is_transient());
        assert!(!q("23505").is_transient());
        let auth = PgError::Connect {
            host: "h".into(),
            port: 1,
            message: "password authentication failed".into(),
            sqlstate: Some("28P01".into()),
        };
        assert!(!auth.is_transient());
        assert!(auth.is_auth());
        let net = PgError::Connect {
            host: "h".into(),
            port: 1,
            message: "refused".into(),
            sqlstate: None,
        };
        assert!(net.is_transient());
    }

    #[test]
    fn missing_object_codes() {
        let q = |code: &str| PgError::Query {
            sqlstate: code.into(),
            message: String::new(),
            detail: None,
            hint: None,
        };
        assert!(q("42P01").is_missing_object());
        assert!(q("3F000").is_missing_object());
        assert!(!q("42703").is_missing_object());
        assert!(
            PgError::NotFound {
                schema: "s".into(),
                table: "t".into()
            }
            .is_missing_object()
        );
    }

    #[test]
    fn timestamptz_cells_become_rfc3339() {
        assert_eq!(
            timestamptz_to_rfc3339("2026-09-15 10:00:00.25+00").as_deref(),
            Some("2026-09-15T10:00:00.250Z")
        );
        assert_eq!(
            timestamptz_to_rfc3339("2026-09-15 10:00:00+05:30").as_deref(),
            Some("2026-09-15T04:30:00Z")
        );
        assert_eq!(timestamptz_to_rfc3339("not a timestamp"), None);
    }

    #[test]
    fn bool_cells_normalize_only_for_bool_columns() {
        let mut cell = serde_json::json!("t");
        normalize_cell(&mut cell, "bool");
        assert_eq!(cell, serde_json::json!("true"));
        let mut text = serde_json::json!("t");
        normalize_cell(&mut text, "text");
        assert_eq!(text, serde_json::json!("t"));
    }
}
