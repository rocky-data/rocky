//! Native TDS client (`tiberius`, rustls) with a small connection pool.
//!
//! Statements run through `sp_executesql` (`Client::execute`), queries as a
//! plain SQL batch (`Client::simple_query`). Either way one call may carry
//! several `;`-separated statements; the dialect wraps every multi-statement
//! write in an explicit `BEGIN TRANSACTION … COMMIT` (SQL Server does not run
//! a batch as one implicit transaction the way PostgreSQL's simple query
//! protocol does).
//!
//! Every session runs with `SET XACT_ABORT ON`, so a run-time error rolls the
//! whole transaction back. A few errors (a missing object at deferred name
//! resolution, for one) end the statement without dooming the transaction;
//! that is why a connection that returns ANY error is dropped instead of
//! going back to the pool. Closing the connection makes the server roll back
//! whatever it left open, so no later statement can run inside it.
//!
//! The other session settings are the ones ODBC and ADO.NET set at login
//! (`ANSI_NULLS`, `QUOTED_IDENTIFIER`, …), pinned explicitly because a bare
//! TDS login inherits the server's `user options`, plus `DATEFORMAT ymd` so a
//! `'2026-01-02'` literal compared with a `DATETIME` column reads year-month-
//! day whatever the login's language.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::TryStreamExt;
use rocky_core::traits::QueryResult;
use tiberius::{AuthMethod, Client, ColumnData, EncryptionLevel, FromSql, QueryItem};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::debug;

use crate::auth::TokenProvider;
use crate::config::{Auth, Encrypt, SqlServerConfig};

type Conn = Client<Compat<TcpStream>>;

/// Errors from the SQL Server connector.
#[derive(thiserror::Error)]
pub enum SqlServerError {
    /// Invalid adapter configuration.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// Acquiring an Entra ID token failed.
    #[error("authentication failed: {0}")]
    Auth(String),
    /// Could not open a session (network, TLS, login).
    #[error("connection to {host}:{port} failed: {message}")]
    Connect {
        host: String,
        port: u16,
        message: String,
        /// The server's error number when it answered (e.g. 18456 login
        /// failed).
        number: Option<u32>,
    },
    /// The server rejected a statement.
    #[error("{message} (SQL Server error {number}, state {state}, class {class})")]
    Query {
        number: u32,
        state: u8,
        class: u8,
        message: String,
    },
    /// The connection dropped mid-statement or the client failed locally.
    #[error("connection error: {0}")]
    Transport(String),
    /// The server did not answer within the configured timeout.
    #[error("statement exceeded the {secs}s timeout")]
    Timeout { secs: u64 },
    /// A described table does not exist (or is not visible to this login).
    #[error("table {schema}.{table} not found")]
    NotFound { schema: String, table: String },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for SqlServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SqlServerError({self})")
    }
}

/// Error numbers Microsoft documents as transient for Azure SQL Database
/// ("Transient fault error codes", learn.microsoft.com/azure/azure-sql/
/// database/troubleshoot-common-errors-issues), plus 1205 (deadlock victim,
/// the transaction was rolled back) and 233 / 10053 / 10054 / 10060 (the
/// transport dropped).
const TRANSIENT_NUMBERS: &[u32] = &[
    233, 926, 1205, 4060, 4221, 10053, 10054, 10060, 10928, 10929, 40143, 40197, 40501, 40540,
    40613, 42108, 42109, 49918, 49919, 49920,
];

impl SqlServerError {
    /// The server's error number, when there is one.
    #[must_use]
    pub fn number(&self) -> Option<u32> {
        match self {
            SqlServerError::Query { number, .. } => Some(*number),
            SqlServerError::Connect { number, .. } => *number,
            _ => None,
        }
    }

    /// Whether retrying the same statement may succeed.
    ///
    /// Every multi-statement write is one transaction under `XACT_ABORT`, so
    /// a failure on these numbers left nothing behind. A client-side timeout
    /// is never retried: the server may still commit.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            SqlServerError::Transport(_) => true,
            SqlServerError::Timeout { .. } => false,
            SqlServerError::Connect { number, .. } => match number {
                Some(n) => TRANSIENT_NUMBERS.contains(n),
                None => true,
            },
            SqlServerError::Query { number, .. } => TRANSIENT_NUMBERS.contains(number),
            SqlServerError::Config(_)
            | SqlServerError::Auth(_)
            | SqlServerError::NotFound { .. } => false,
        }
    }

    /// Whether the error says the referenced object is absent.
    ///
    /// 208 "Invalid object name", 3701 "Cannot drop … because it does not
    /// exist", 4902 "Cannot find the object … because it does not exist or
    /// you do not have permissions", 2706 "Table … does not exist", 15151
    /// "Cannot find the object".
    #[must_use]
    pub fn is_missing_object(&self) -> bool {
        match self {
            SqlServerError::NotFound { .. } => true,
            SqlServerError::Query { number, .. } => {
                matches!(number, 208 | 2706 | 3701 | 4902 | 15151)
            }
            _ => false,
        }
    }

    /// Whether the error is a login or permission failure.
    ///
    /// 18456 login failed, 229 / 230 / 262 / 297 / 300 permission denied,
    /// 4060 cannot open the database.
    #[must_use]
    pub fn is_auth(&self) -> bool {
        matches!(self, SqlServerError::Auth(_))
            || self
                .number()
                .is_some_and(|n| matches!(n, 18456 | 229 | 230 | 262 | 297 | 300 | 916))
    }
}

fn map_error(err: tiberius::error::Error, timeout: Duration) -> SqlServerError {
    match err {
        tiberius::error::Error::Server(token) => SqlServerError::Query {
            number: token.code(),
            state: token.state(),
            class: token.class(),
            message: token.message().to_string(),
        },
        tiberius::error::Error::Io {
            kind: std::io::ErrorKind::TimedOut,
            ..
        } => SqlServerError::Timeout {
            secs: timeout.as_secs(),
        },
        other => SqlServerError::Transport(other.to_string()),
    }
}

/// Pooled SQL Server client.
pub struct SqlServerClient {
    config: SqlServerConfig,
    tokens: TokenProvider,
    idle: Mutex<Vec<Conn>>,
    permits: Arc<Semaphore>,
}

/// A connection checked out of the pool. Returned on [`Pooled::release`];
/// dropped (closed) otherwise — which is what makes the server roll back
/// anything an error left open.
struct Pooled<'a> {
    conn: Option<Conn>,
    pool: &'a SqlServerClient,
    _permit: OwnedSemaphorePermit,
}

impl Pooled<'_> {
    fn conn(&mut self) -> &mut Conn {
        self.conn
            .as_mut()
            .expect("connection present until release")
    }

    fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool
                .idle
                .lock()
                .expect("idle pool lock poisoned")
                .push(conn);
        }
    }
}

/// Session settings applied to every new connection (see the module docs).
pub const SESSION_SETUP: &str = "SET XACT_ABORT ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; \
     SET ANSI_WARNINGS ON; SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; \
     SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; SET DATEFORMAT ymd";

impl SqlServerClient {
    /// Build a client. No connection is opened until the first statement.
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Auth`] when the token provider cannot be built.
    pub fn new(config: SqlServerConfig) -> Result<Self, SqlServerError> {
        let permits = Arc::new(Semaphore::new(config.max_connections));
        let tokens = TokenProvider::new(config.timeout)?;
        Ok(Self {
            config,
            tokens,
            idle: Mutex::new(Vec::new()),
            permits,
        })
    }

    /// The configuration this client connects with.
    #[must_use]
    pub fn config(&self) -> &SqlServerConfig {
        &self.config
    }

    async fn tiberius_config(&self) -> Result<tiberius::Config, SqlServerError> {
        let cfg = &self.config;
        let mut tc = tiberius::Config::new();
        tc.host(&cfg.host);
        tc.port(cfg.port);
        tc.database(&cfg.database);
        tc.application_name("rocky");
        tc.handshake_timeout(Some(cfg.timeout));
        tc.command_timeout(Some(cfg.timeout));
        tc.encryption(match cfg.encrypt {
            Encrypt::Mandatory => EncryptionLevel::Required,
            Encrypt::Strict => EncryptionLevel::Strict,
            Encrypt::Optional => EncryptionLevel::Off,
        });
        if cfg.trust_server_certificate {
            tc.trust_cert();
        } else {
            tc.trust_webpki_roots();
            if let Some(ca) = &cfg.ca_cert {
                tc.trust_cert_ca(ca);
            }
        }
        match &cfg.auth {
            Auth::SqlPassword { user, password } => {
                tc.authentication(AuthMethod::sql_server(user, password));
            }
            Auth::AccessToken(_) | Auth::ServicePrincipal { .. } => {
                let token = self
                    .tokens
                    .token(cfg)
                    .await?
                    .ok_or_else(|| SqlServerError::Auth("no access token".into()))?;
                tc.authentication(AuthMethod::aad_token(token));
            }
        }
        Ok(tc)
    }

    async fn open(
        &self,
        tc: tiberius::Config,
        host: &str,
        port: u16,
    ) -> Result<Conn, tiberius::error::Error> {
        let tcp = tokio::time::timeout(self.config.timeout, TcpStream::connect((host, port)))
            .await
            .map_err(|_| tiberius::error::Error::Io {
                kind: std::io::ErrorKind::TimedOut,
                message: format!("TCP connect timed out after {:?}", self.config.timeout),
            })??;
        tcp.set_nodelay(true)?;
        Client::connect(tc, tcp.compat_write()).await
    }

    async fn connect(&self) -> Result<Conn, SqlServerError> {
        let cfg = &self.config;
        let connect_err = |host: &str, port: u16, e: tiberius::error::Error| {
            let number = match &e {
                tiberius::error::Error::Server(t) => Some(t.code()),
                _ => None,
            };
            let message = match &e {
                tiberius::error::Error::Server(t) => t.message().to_string(),
                other => other.to_string(),
            };
            SqlServerError::Connect {
                host: rocky_core::secret_registry::render_placeholders(host),
                port,
                message: rocky_core::secret_registry::render_placeholders(&message),
                number,
            }
        };
        let mut client = match self
            .open(self.tiberius_config().await?, &cfg.host, cfg.port)
            .await
        {
            Ok(c) => c,
            // Azure SQL's `Redirect` connection policy answers the first
            // login with the node to talk to; follow it once.
            Err(tiberius::error::Error::Routing { host, port }) => {
                debug!(%host, port, "sqlserver login redirected");
                let mut tc = self.tiberius_config().await?;
                tc.host(&host);
                tc.port(port);
                self.open(tc, &host, port)
                    .await
                    .map_err(|e| connect_err(&host, port, e))?
            }
            Err(e) => return Err(connect_err(&cfg.host, cfg.port, e)),
        };
        client
            .simple_query(SESSION_SETUP)
            .await
            .map_err(|e| map_error(e, cfg.timeout))?
            .into_results()
            .await
            .map_err(|e| map_error(e, cfg.timeout))?;
        Ok(client)
    }

    async fn checkout(&self) -> Result<Pooled<'_>, SqlServerError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| SqlServerError::Transport("connection pool closed".into()))?;
        let reused = self.idle.lock().expect("idle pool lock poisoned").pop();
        let conn = match reused {
            Some(c) => c,
            None => self.connect().await?,
        };
        Ok(Pooled {
            conn: Some(conn),
            pool: self,
            _permit: permit,
        })
    }

    /// Run one or more statements. Returns the last non-zero row count the
    /// statements reported.
    ///
    /// # Errors
    ///
    /// [`SqlServerError`] on connection failure, server rejection, or
    /// timeout. The connection is discarded on any error.
    pub async fn execute(&self, sql: &str) -> Result<Option<u64>, SqlServerError> {
        let mut pooled = self.checkout().await?;
        debug!(sql, "sqlserver execute");
        let wrapped = exec_wrapped(sql);
        let result = pooled
            .conn()
            .execute(wrapped.as_deref().unwrap_or(sql), &[])
            .await
            .map_err(|e| map_error(e, self.config.timeout))?;
        pooled.release();
        // A script reports a count per statement, and 0 for statements that
        // touch no rows (DDL, `sp_rename`, `COMMIT`); the last non-zero count
        // is the write's (`SELECT INTO`, or the INSERT after a DELETE).
        let counts = result.rows_affected();
        Ok(counts
            .iter()
            .rev()
            .find(|n| **n > 0)
            .or(counts.last())
            .copied())
    }

    /// Run a query and return its last result set as text cells (`NULL` is
    /// JSON `null`).
    ///
    /// # Errors
    ///
    /// [`SqlServerError`] on connection failure, server rejection, or
    /// timeout. The connection is discarded on any error.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, SqlServerError> {
        let mut pooled = self.checkout().await?;
        debug!(sql, "sqlserver query");
        let timeout = self.config.timeout;
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        {
            let mut stream = pooled
                .conn()
                .simple_query(sql)
                .await
                .map_err(|e| map_error(e, timeout))?;
            while let Some(item) = stream.try_next().await.map_err(|e| map_error(e, timeout))? {
                match item {
                    // A batch yields one metadata item per result set; keep
                    // the last set, as a caller of a single SELECT expects.
                    QueryItem::Metadata(meta) => {
                        columns = meta
                            .columns()
                            .iter()
                            .map(|c| c.name().to_string())
                            .collect();
                        rows.clear();
                    }
                    QueryItem::Row(row) => {
                        rows.push(row.cells().map(|(_, data)| cell_to_json(data)).collect());
                    }
                }
            }
        }
        pooled.release();
        Ok(QueryResult { columns, rows })
    }

    /// Open (or reuse) a session and run `SELECT 1`.
    ///
    /// # Errors
    ///
    /// See [`Self::query`].
    pub async fn ping(&self) -> Result<(), SqlServerError> {
        self.query("SELECT 1").await.map(|_| ())
    }
}

/// `EXEC(N'…')` around a statement T-SQL requires to start its own batch
/// (`CREATE [OR ALTER] VIEW`), `None` for anything else.
///
/// Statements run through `sp_executesql`, whose batch does not start with
/// the statement text as far as that rule is concerned: a bare `CREATE
/// VIEW` there fails with "Incorrect syntax near the keyword 'VIEW'"
/// (verified against SQL Server 2022). The nested `EXEC` gives it a batch
/// of its own.
#[must_use]
pub fn exec_wrapped(stmt: &str) -> Option<String> {
    let trimmed = stmt.trim().trim_end_matches(';').trim_end();
    let words: Vec<String> = trimmed
        .split_whitespace()
        .take(4)
        .map(str::to_ascii_uppercase)
        .collect();
    let is_view = matches!(
        words
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice(),
        ["CREATE", "VIEW", ..] | ["CREATE", "OR", "ALTER", "VIEW"]
    );
    is_view.then(|| format!("EXEC(N'{}')", trimmed.replace('\'', "''")))
}

/// One cell as the text shape the rest of Rocky reads (the same shape the
/// PostgreSQL adapter returns): numbers and booleans as their text,
/// `DATETIME2` as `YYYY-MM-DD HH:MM:SS[.fffffff]`, `DATETIMEOFFSET` as RFC 3339
/// in UTC, GUIDs upper-case (as SQL Server prints them), binary as `0x…`.
#[must_use]
pub fn cell_to_json(data: &ColumnData<'static>) -> serde_json::Value {
    use serde_json::Value;
    let text = |s: String| Value::String(s);
    match data {
        ColumnData::U8(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::I16(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::I32(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::I64(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::F32(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::F64(v) => v.map_or(Value::Null, |n| text(n.to_string())),
        ColumnData::Bit(v) => v.map_or(Value::Null, |b| text(b.to_string())),
        ColumnData::String(v) => v.as_ref().map_or(Value::Null, |s| text(s.to_string())),
        ColumnData::Guid(v) => v.map_or(Value::Null, |g| text(g.to_string().to_uppercase())),
        ColumnData::Binary(v) => v.as_ref().map_or(Value::Null, |b| {
            let mut out = String::with_capacity(2 + b.len() * 2);
            out.push_str("0x");
            for byte in b.iter() {
                out.push_str(&format!("{byte:02X}"));
            }
            text(out)
        }),
        ColumnData::Numeric(v) => v.map_or(Value::Null, |n| text(numeric_text(n))),
        ColumnData::Xml(v) => v
            .as_ref()
            .map_or(Value::Null, |x| text(x.as_ref().clone().into_string())),
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => {
            match chrono::NaiveDateTime::from_sql(data) {
                Ok(Some(dt)) => text(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
                _ => Value::Null,
            }
        }
        ColumnData::Date(_) => match chrono::NaiveDate::from_sql(data) {
            Ok(Some(d)) => text(d.format("%Y-%m-%d").to_string()),
            _ => Value::Null,
        },
        ColumnData::Time(_) => match chrono::NaiveTime::from_sql(data) {
            Ok(Some(t)) => text(t.format("%H:%M:%S%.f").to_string()),
            _ => Value::Null,
        },
        ColumnData::DateTimeOffset(_) => {
            match chrono::DateTime::<chrono::FixedOffset>::from_sql(data) {
                Ok(Some(dt)) => text(
                    dt.with_timezone(&chrono::Utc)
                        .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                ),
                _ => Value::Null,
            }
        }
    }
}

/// `DECIMAL` / `NUMERIC` as plain decimal text: `12.50`, `-0.5`, `7`.
fn numeric_text(n: tiberius::numeric::Numeric) -> String {
    if n.scale() == 0 {
        return n.value().to_string();
    }
    // tiberius' Display: sign, integer part, `.`, fraction zero-padded to
    // the scale.
    n.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn transient_and_missing_classification() {
        let q = |number: u32| SqlServerError::Query {
            number,
            state: 1,
            class: 16,
            message: String::new(),
        };
        assert!(q(1205).is_transient());
        assert!(q(40613).is_transient());
        assert!(!q(208).is_transient());
        assert!(!q(2627).is_transient());
        assert!(q(208).is_missing_object());
        assert!(q(3701).is_missing_object());
        assert!(!q(207).is_missing_object());
        assert!(
            !SqlServerError::Timeout { secs: 1 }.is_transient(),
            "a timeout may have committed; never retried"
        );
        let login = SqlServerError::Connect {
            host: "h".into(),
            port: 1,
            message: "Login failed for user 'x'.".into(),
            number: Some(18456),
        };
        assert!(!login.is_transient());
        assert!(login.is_auth());
        let refused = SqlServerError::Connect {
            host: "h".into(),
            port: 1,
            message: "refused".into(),
            number: None,
        };
        assert!(refused.is_transient());
    }

    #[test]
    fn create_view_runs_in_its_own_batch() {
        assert_eq!(
            exec_wrapped("CREATE OR ALTER VIEW [m].[v] AS\nSELECT 'a' AS x;").as_deref(),
            Some("EXEC(N'CREATE OR ALTER VIEW [m].[v] AS\nSELECT ''a'' AS x')")
        );
        assert_eq!(
            exec_wrapped("  create   view [m].[v] AS SELECT 1").as_deref(),
            Some("EXEC(N'create   view [m].[v] AS SELECT 1')")
        );
        assert_eq!(exec_wrapped("SELECT 1"), None);
        assert_eq!(exec_wrapped("CREATE TABLE t (a INT)"), None);
    }

    #[test]
    fn cells_render_as_text() {
        use serde_json::json;
        assert_eq!(cell_to_json(&ColumnData::I32(Some(7))), json!("7"));
        assert_eq!(cell_to_json(&ColumnData::I64(None)), json!(null));
        assert_eq!(cell_to_json(&ColumnData::Bit(Some(true))), json!("true"));
        assert_eq!(
            cell_to_json(&ColumnData::String(Some(Cow::Borrowed("héllo")))),
            json!("héllo")
        );
        assert_eq!(
            cell_to_json(&ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(1250, 2)
            ))),
            json!("12.50")
        );
        assert_eq!(
            cell_to_json(&ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(-5, 1)
            ))),
            json!("-0.5")
        );
        assert_eq!(
            cell_to_json(&ColumnData::Numeric(Some(
                tiberius::numeric::Numeric::new_with_scale(42, 0)
            ))),
            json!("42")
        );
        assert_eq!(
            cell_to_json(&ColumnData::Binary(Some(Cow::Borrowed(&[0xde, 0xad])))),
            json!("0xDEAD")
        );
    }
}
