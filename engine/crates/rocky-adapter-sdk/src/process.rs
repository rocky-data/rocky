//! Process adapter: out-of-process adapter communication via JSON-RPC over stdio.
//!
//! Rocky spawns an adapter as a child process and communicates via JSON lines
//! on stdin/stdout (same pattern as LSP). This allows adapters to be written
//! in any language (Python, Go, Node, Rust, etc.).
//!
//! # Protocol
//!
//! Each message is a single JSON object per line (JSON Lines format).
//! The adapter reads from stdin and writes to stdout. Stderr is for logging.
//!
//! ## Request (Rocky -> Adapter)
//! ```json
//! { "jsonrpc": "2.0", "id": 1, "method": "execute_statement", "params": { "sql": "..." } }
//! ```
//!
//! ## Response (Adapter -> Rocky)
//! ```json
//! { "jsonrpc": "2.0", "id": 1, "result": { "ok": true } }
//! ```
//!
//! ## Error Response
//! ```json
//! { "jsonrpc": "2.0", "id": 1, "error": { "code": "EXECUTION_FAILED", "message": "..." } }
//! ```

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::Mutex;

#[cfg(unix)]
use crate::child::signal_group;
use crate::child::{read_bounded_line, stop_child};
use crate::child_stdin::ChildStdinWriter;
use crate::manifest::AdapterManifest;
use crate::traits::{
    AdapterError, AdapterResult, ColumnInfo, QueryResult, SqlDialect, TableRef, WarehouseAdapter,
};

// ---------------------------------------------------------------------------
// JSON-RPC protocol types
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 request (Rocky -> Adapter).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

impl RpcRequest {
    /// Create a new JSON-RPC request.
    pub fn new(id: u64, method: impl Into<String>, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// JSON-RPC 2.0 response (Adapter -> Rocky).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

/// JSON-RPC error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Limits and typed errors
// ---------------------------------------------------------------------------

/// Default for [`ProcessLimits::init_timeout`]: 60 seconds.
pub const DEFAULT_INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Default for [`ProcessLimits::call_timeout`]: 1 hour. A call can run
/// warehouse SQL, so this bounds a hung adapter, not a slow query.
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Default for [`ProcessLimits::stop_grace`]: 5 seconds.
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(5);

/// Default for [`ProcessLimits::max_response_bytes`]: 64 MiB.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Bounds on one adapter process: how long Rocky waits and how much it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessLimits {
    /// How long the `initialize` exchange in [`ProcessAdapter::spawn`] may take.
    pub init_timeout: Duration,
    /// How long one [`ProcessAdapter::call`] may take, request write and
    /// response read together.
    pub call_timeout: Duration,
    /// How long a stopping adapter has to exit after `SIGTERM` (Unix) before
    /// its process group gets `SIGKILL`. Also bounds the `shutdown` call that
    /// `close` sends.
    pub stop_grace: Duration,
    /// The most bytes Rocky reads for one response line, newline excluded.
    pub max_response_bytes: usize,
}

impl Default for ProcessLimits {
    fn default() -> Self {
        Self {
            init_timeout: DEFAULT_INIT_TIMEOUT,
            call_timeout: DEFAULT_CALL_TIMEOUT,
            stop_grace: DEFAULT_STOP_GRACE,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

/// A process adapter failure that stopped the adapter process.
///
/// Returned inside an [`AdapterError`]; reach it with
/// `err.inner().downcast_ref::<ProcessAdapterError>()`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProcessAdapterError {
    /// The adapter did not answer within the limit, so Rocky stopped it.
    #[error("adapter did not answer `{method}` within {after:?}; the adapter process was stopped")]
    Timeout {
        /// The JSON-RPC method that was in flight.
        method: String,
        /// The limit that expired.
        after: Duration,
    },
    /// The adapter's response line was longer than the limit, so Rocky
    /// stopped it instead of buffering the rest.
    #[error(
        "adapter response to `{method}` is longer than {max_bytes} bytes; the adapter process was stopped"
    )]
    ResponseTooLarge {
        /// The JSON-RPC method that was in flight.
        method: String,
        /// The limit that was exceeded.
        max_bytes: usize,
    },
    /// An earlier timeout or oversized response stopped the adapter process.
    #[error("the adapter process was stopped after an earlier failure; spawn a new adapter")]
    Stopped,
}

// ---------------------------------------------------------------------------
// Process adapter
// ---------------------------------------------------------------------------

/// An adapter that communicates with an external process via JSON-RPC over stdio.
///
/// The external process must implement the Rocky adapter protocol:
/// 1. Read JSON-RPC requests from stdin (one per line)
/// 2. Write JSON-RPC responses to stdout (one per line)
/// 3. Use stderr for logging
///
/// The process adapter implements `WarehouseAdapter` by translating each
/// method call into a JSON-RPC request/response exchange.
///
/// Every exchange is bounded by [`ProcessLimits`]. On Unix the adapter leads
/// its own process group, so stopping it also stops what it started. The
/// adapter is stopped on a timeout, on an oversized response, in `close`, and
/// when the `ProcessAdapter` is dropped.
pub struct ProcessAdapter {
    /// The child process.
    child: Mutex<Child>,
    /// Writer to the child's stdin. A write to an adapter that has exited
    /// returns an error instead of killing Rocky with SIGPIPE (#2370).
    stdin: ChildStdinWriter,
    /// Buffered reader from the child's stdout.
    stdout: Mutex<BufReader<ChildStdout>>,
    /// Serializes the request/response pair in [`ProcessAdapter::call`].
    ///
    /// The wire protocol is request-id-keyed in theory but the underlying
    /// child process is single-threaded — it reads one request, writes one
    /// response, then loops. Concurrent callers that interleave writes/reads
    /// can therefore swap responses (caller A reads B's reply). Holding this
    /// lock across the entire write→read pair makes `call` linearizable.
    call_lock: Mutex<()>,
    /// Set once the child was stopped after a timeout or an oversized
    /// response. The stream may then hold half a message, so every later
    /// call fails with [`ProcessAdapterError::Stopped`].
    stopped: AtomicBool,
    /// Monotonically increasing request ID.
    next_id: AtomicU64,
    /// Time and size bounds for this adapter.
    limits: ProcessLimits,
    /// The adapter's manifest, received during initialization.
    manifest: AdapterManifest,
    /// A minimal dialect proxy for the process adapter.
    dialect: ProcessDialect,
}

impl std::fmt::Debug for ProcessAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessAdapter")
            .field("manifest", &self.manifest)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// Stops the adapter's whole process group if it is still running.
///
/// `Drop` cannot wait, so there is no `SIGTERM` grace here: the group gets
/// `SIGKILL`. `kill_on_drop` on the `Command` still covers the adapter itself
/// where there is no process group.
impl Drop for ProcessAdapter {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // `id()` is `None` once tokio has reaped the child. While it is
            // `Some`, the child is unreaped, so its id still names its group.
            if let Some(pgid) = self.child.get_mut().id() {
                let _ = signal_group(pgid, libc::SIGKILL);
            }
        }
    }
}

impl ProcessAdapter {
    /// Spawn an adapter process and initialize it, with [`ProcessLimits::default`].
    ///
    /// 1. Starts the child process
    /// 2. Sends an `initialize` request with the provided config
    /// 3. Reads the manifest from the response
    pub async fn spawn(
        command: &str,
        args: &[&str],
        config: &serde_json::Value,
    ) -> Result<Self, AdapterError> {
        Self::spawn_with_limits(command, args, config, ProcessLimits::default()).await
    }

    /// Spawn an adapter process and initialize it, bounded by `limits`.
    ///
    /// An `initialize` exchange that runs past `limits.init_timeout`, or
    /// answers with a line longer than `limits.max_response_bytes`, stops
    /// the process and returns a [`ProcessAdapterError`]. Any other failure
    /// after the spawn also stops the process.
    pub async fn spawn_with_limits(
        command: &str,
        args: &[&str],
        config: &serde_json::Value,
        limits: ProcessLimits,
    ) -> Result<Self, AdapterError> {
        let mut command_builder = Command::new(command);
        for (key, _) in std::env::vars_os() {
            if key
                .to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("DAGSTER_PIPES_")
            {
                command_builder.env_remove(key);
            }
        }
        command_builder
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit()) // let adapter log to stderr
            .kill_on_drop(true);
        // Lead a new process group, so stopping the adapter reaches what it
        // started, and a signal sent to its group never reaches Rocky.
        #[cfg(unix)]
        command_builder.process_group(0);
        let mut child = command_builder.spawn().map_err(|e| {
            AdapterError::msg(format!("failed to spawn adapter process '{command}': {e}"))
        })?;

        match initialize(&mut child, config, limits).await {
            Ok((stdin, stdout, manifest)) => {
                let dialect = ProcessDialect {
                    name: manifest.dialect.clone(),
                };
                Ok(Self {
                    child: Mutex::new(child),
                    stdin,
                    stdout: Mutex::new(stdout),
                    call_lock: Mutex::new(()),
                    stopped: AtomicBool::new(false),
                    // `initialize` used id 1.
                    next_id: AtomicU64::new(2),
                    limits,
                    manifest,
                    dialect,
                })
            }
            Err(e) => {
                // A failed initialize leaves no `ProcessAdapter` whose `Drop`
                // would stop the group, so stop it here.
                stop_child(&mut child, limits.stop_grace).await;
                Err(e)
            }
        }
    }

    /// Get the adapter's manifest.
    pub fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    /// The limits this adapter was spawned with.
    pub fn limits(&self) -> &ProcessLimits {
        &self.limits
    }

    /// Send a JSON-RPC request and wait for the response.
    ///
    /// **Concurrency:** this method serializes concurrent callers. The
    /// underlying child process is single-threaded — it reads one request
    /// and writes one response per loop iteration — so two concurrent
    /// `call`s would otherwise interleave on stdin/stdout and could swap
    /// responses (caller A reads B's reply). [`Self::call_lock`] is held
    /// across the entire write→read pair to keep the protocol
    /// linearizable. If you need parallel adapter calls, spawn multiple
    /// [`ProcessAdapter`] instances rather than sharing one.
    ///
    /// **Limits:** the write→read pair must finish within
    /// [`ProcessLimits::call_timeout`], and the response line must fit in
    /// [`ProcessLimits::max_response_bytes`]. Otherwise the adapter process
    /// is stopped and this returns a [`ProcessAdapterError`]; every later
    /// call returns [`ProcessAdapterError::Stopped`].
    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, AdapterError> {
        self.call_within(method, params, self.limits.call_timeout)
            .await
    }

    async fn call_within(
        &self,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, AdapterError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = RpcRequest::new(id, method, params);

        let mut request_line = serde_json::to_vec(&request).map_err(AdapterError::new)?;
        request_line.push(b'\n');

        // Hold `call_lock` across the entire write→read pair so concurrent
        // callers cannot interleave on stdin/stdout. Without this, two
        // overlapping calls can swap responses: caller A writes id=N,
        // caller B writes id=M, then A reads M's reply and the existing
        // id mismatch surfaces as a hard error rather than corruption.
        let _guard = self.call_lock.lock().await;

        if self.stopped.load(Ordering::SeqCst) {
            return Err(AdapterError::new(ProcessAdapterError::Stopped));
        }

        let exchanged = {
            let mut reader = self.stdout.lock().await;
            bounded_exchange(
                &self.stdin,
                &mut *reader,
                request_line,
                method,
                timeout,
                self.limits.max_response_bytes,
            )
            .await
        };
        let line = match exchanged {
            Ok(line) => line,
            Err(ExchangeFailure::Stop(e)) => {
                self.stopped.store(true, Ordering::SeqCst);
                let mut child = self.child.lock().await;
                stop_child(&mut child, self.limits.stop_grace).await;
                return Err(AdapterError::new(e));
            }
            Err(ExchangeFailure::Other(e)) => return Err(e),
        };

        let response: RpcResponse = serde_json::from_str(line.trim())
            .map_err(|e| AdapterError::msg(format!("failed to parse adapter response: {e}")))?;

        // Verify response ID matches request ID. With `call_lock` held this
        // should be impossible to violate; the check stays as a defence
        // against an adapter that emits a response with a wrong id.
        if response.id != id {
            return Err(AdapterError::msg(format!(
                "response ID mismatch: expected {id}, got {}",
                response.id
            )));
        }

        if let Some(err) = response.error {
            return Err(AdapterError::msg(format!("{}: {}", err.code, err.message)));
        }

        response
            .result
            .ok_or_else(|| AdapterError::msg("adapter returned empty result"))
    }
}

/// Take the child's pipes, send `initialize` with request id 1, and read the
/// manifest, bounded by `limits`.
async fn initialize(
    child: &mut Child,
    config: &serde_json::Value,
    limits: ProcessLimits,
) -> Result<(ChildStdinWriter, BufReader<ChildStdout>, AdapterManifest), AdapterError> {
    let child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| AdapterError::msg("failed to capture adapter stdin"))?;
    let child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| AdapterError::msg("failed to capture adapter stdout"))?;
    let stdin = ChildStdinWriter::new(child_stdin)
        .map_err(|e| AdapterError::msg(format!("failed to set up adapter stdin: {e}")))?;
    let mut stdout = BufReader::new(child_stdout);

    let init_request = RpcRequest::new(1, "initialize", serde_json::json!({ "config": config }));
    let mut request_line = serde_json::to_vec(&init_request).map_err(AdapterError::new)?;
    request_line.push(b'\n');

    let line = bounded_exchange(
        &stdin,
        &mut stdout,
        request_line,
        "initialize",
        limits.init_timeout,
        limits.max_response_bytes,
    )
    .await
    .map_err(ExchangeFailure::into_error)?;

    let response: RpcResponse = serde_json::from_str(line.trim())
        .map_err(|e| AdapterError::msg(format!("failed to parse adapter response: {e}")))?;

    if let Some(err) = response.error {
        return Err(AdapterError::msg(format!(
            "adapter initialization failed: {} ({})",
            err.message, err.code
        )));
    }

    let result = response
        .result
        .ok_or_else(|| AdapterError::msg("adapter returned empty initialize response"))?;

    let manifest: AdapterManifest =
        serde_json::from_value(result.get("manifest").cloned().unwrap_or(result.clone()))
            .map_err(|e| AdapterError::msg(format!("failed to parse adapter manifest: {e}")))?;

    Ok((stdin, stdout, manifest))
}

// ---------------------------------------------------------------------------
// Bounded exchange and process-group stop
// ---------------------------------------------------------------------------

/// Why one request/response exchange did not produce a line.
enum ExchangeFailure {
    /// Timeout or oversized response: the stream may hold half a message,
    /// so the caller stops the adapter.
    Stop(ProcessAdapterError),
    /// Any other failure; the stream is left as it is.
    Other(AdapterError),
}

impl ExchangeFailure {
    fn into_error(self) -> AdapterError {
        match self {
            Self::Stop(e) => AdapterError::new(e),
            Self::Other(e) => e,
        }
    }
}

/// Write one request line and read one response line, within `timeout`,
/// reading at most `max_bytes` (newline excluded) for the response.
///
/// The timeout covers the write too: a child that does not read its stdin
/// fills the pipe, and the write then blocks.
async fn bounded_exchange<R: AsyncBufRead + Unpin>(
    stdin: &ChildStdinWriter,
    reader: &mut R,
    request_line: Vec<u8>,
    method: &str,
    timeout: Duration,
    max_bytes: usize,
) -> Result<String, ExchangeFailure> {
    let exchange = async {
        stdin.write_all(request_line).await.map_err(|e| {
            ExchangeFailure::Other(AdapterError::msg(format!(
                "failed to write to adapter: {e}"
            )))
        })?;
        match read_bounded_line(reader, max_bytes).await {
            Ok(Some(line)) if line.is_empty() => Err(ExchangeFailure::Other(AdapterError::msg(
                "adapter process closed stdout (process may have crashed)",
            ))),
            Ok(Some(line)) => Ok(line),
            Ok(None) => Err(ExchangeFailure::Stop(
                ProcessAdapterError::ResponseTooLarge {
                    method: method.to_string(),
                    max_bytes,
                },
            )),
            Err(e) => Err(ExchangeFailure::Other(AdapterError::msg(format!(
                "failed to read from adapter: {e}"
            )))),
        }
    };
    match tokio::time::timeout(timeout, exchange).await {
        Ok(result) => result,
        Err(_elapsed) => Err(ExchangeFailure::Stop(ProcessAdapterError::Timeout {
            method: method.to_string(),
            after: timeout,
        })),
    }
}

// ---------------------------------------------------------------------------
// WarehouseAdapter implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl WarehouseAdapter for ProcessAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.call("execute_statement", serde_json::json!({ "sql": sql }))
            .await?;
        Ok(())
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let resp = self
            .call("execute_query", serde_json::json!({ "sql": sql }))
            .await?;
        serde_json::from_value(resp)
            .map_err(|e| AdapterError::msg(format!("failed to parse query result: {e}")))
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let resp = self
            .call(
                "describe_table",
                serde_json::json!({
                    "catalog": table.catalog,
                    "schema": table.schema,
                    "table": table.table,
                }),
            )
            .await?;

        let columns: Vec<ColumnInfo> =
            serde_json::from_value(resp.get("columns").cloned().unwrap_or(resp))
                .map_err(|e| AdapterError::msg(format!("failed to parse describe result: {e}")))?;

        Ok(columns)
    }

    async fn table_exists(&self, table: &TableRef) -> AdapterResult<bool> {
        let resp = self
            .call(
                "table_exists",
                serde_json::json!({
                    "catalog": table.catalog,
                    "schema": table.schema,
                    "table": table.table,
                }),
            )
            .await?;

        resp.get("exists")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| AdapterError::msg("table_exists response missing 'exists' field"))
    }

    async fn close(&self) -> AdapterResult<()> {
        // Send shutdown, ignore errors (process may already be gone).
        let _ = self
            .call_within("shutdown", serde_json::json!({}), self.limits.stop_grace)
            .await;

        // Give the adapter the grace to exit, then stop its whole group.
        let mut child = self.child.lock().await;
        stop_child(&mut child, self.limits.stop_grace).await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Process dialect (proxy for dialect calls)
// ---------------------------------------------------------------------------

/// A minimal SqlDialect that delegates dialect operations back to the process adapter.
///
/// For pure dialect operations (SQL generation), this uses local implementations
/// since they don't require round-trips. The process adapter is primarily for
/// execution operations.
#[derive(Debug, Clone)]
struct ProcessDialect {
    name: String,
}

impl SqlDialect for ProcessDialect {
    fn name(&self) -> &str {
        &self.name
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // Default three-part name; adapters can override via process calls if needed.
        Ok(format!("{catalog}.{schema}.{table}"))
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        _target: &str,
        _source_sql: &str,
        _keys: &[String],
        _update_cols: Option<&[String]>,
    ) -> AdapterResult<String> {
        Err(AdapterError::not_supported("merge_into"))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn create_schema_sql(&self, _catalog: &str, _schema: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn row_hash_expr(&self, columns: &[String]) -> String {
        format!("MD5(CONCAT({}))", columns.join(", "))
    }

    fn tablesample_clause(&self, _percent: u32) -> Option<String> {
        None
    }

    /// Renders the SELECT list, refusing metadata columns.
    ///
    /// The five in-tree dialects validate all three fields of a metadata
    /// column before splicing them into `CAST({value} AS {type}) AS {name}`.
    /// This impl cannot: the SDK deliberately does not depend on `rocky-sql`
    /// (see the "not yet in the SDK" list in `traits.rs`), so the shared
    /// validators are out of reach, and re-implementing the fragment scanner
    /// here would give the same rule two bodies that can drift.
    ///
    /// Splicing them unvalidated is the defect #1594 closes, so this refuses
    /// instead — the same fail-closed shape `merge_into` above already uses.
    /// Nothing reaches it today: `crate::traits::MetadataColumn` is a separate
    /// struct from `rocky_ir::MetadataColumn`, no in-tree code converts one to
    /// the other, and the conformance harness drives only `format_table_ref`.
    /// When the SDK and IR types unify (the prerequisite named in
    /// `traits.rs`), route this through `rocky_ir::MetadataColumn::new`.
    fn select_clause(
        &self,
        columns: &crate::traits::ColumnSelection,
        metadata: &[crate::traits::MetadataColumn],
    ) -> AdapterResult<String> {
        if !metadata.is_empty() {
            return Err(AdapterError::not_supported(
                "select_clause with metadata columns (the process adapter cannot validate them)",
            ));
        }
        let mut sql = String::from("SELECT ");
        match columns {
            crate::traits::ColumnSelection::All => sql.push('*'),
            crate::traits::ColumnSelection::Explicit(cols) => sql.push_str(&cols.join(", ")),
        }
        Ok(sql)
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
    }

    fn insert_overwrite_partition(
        &self,
        _target: &str,
        _partition_filter: &str,
        _select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // Process adapters need to declare time_interval support themselves.
        // The default impl errors loudly so users get a clear "this adapter
        // doesn't support time_interval" rather than silent corruption.
        Err(AdapterError::not_supported("insert_overwrite_partition"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_request_serialization() {
        let req = RpcRequest::new(
            1,
            "execute_statement",
            serde_json::json!({"sql": "SELECT 1"}),
        );
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"method\":\"execute_statement\""));
        assert!(json.contains("\"sql\":\"SELECT 1\""));
    }

    #[test]
    fn test_rpc_request_deserialization() {
        let json = r#"{"jsonrpc":"2.0","id":42,"method":"describe_table","params":{"catalog":"c","schema":"s","table":"t"}}"#;
        let req: RpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, 42);
        assert_eq!(req.method, "describe_table");
        assert_eq!(req.params["catalog"], "c");
    }

    #[test]
    fn test_rpc_response_success() {
        let resp = RpcResponse {
            jsonrpc: "2.0".into(),
            id: 1,
            result: Some(serde_json::json!({"ok": true})),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"result\""));
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_rpc_response_error() {
        let resp = RpcResponse {
            jsonrpc: "2.0".into(),
            id: 1,
            result: None,
            error: Some(RpcError {
                code: "EXECUTION_FAILED".into(),
                message: "Table already exists".into(),
            }),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("\"result\""));
        assert!(json.contains("\"error\""));
        assert!(json.contains("EXECUTION_FAILED"));
    }

    #[test]
    fn test_rpc_response_deserialization_success() {
        let json =
            r#"{"jsonrpc":"2.0","id":1,"result":{"columns":["id","name"],"rows":[[1,"foo"]]}}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_none());
        let result = resp.result.unwrap();
        assert_eq!(result["columns"][0], "id");
    }

    #[test]
    fn test_rpc_response_deserialization_error() {
        let json =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":"NOT_FOUND","message":"Table not found"}}"#;
        let resp: RpcResponse = serde_json::from_str(json).unwrap();
        assert!(resp.result.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, "NOT_FOUND");
    }

    #[test]
    fn test_process_dialect_format_table_ref() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        let result = d.format_table_ref("cat", "sch", "tbl").unwrap();
        assert_eq!(result, "cat.sch.tbl");
    }

    #[test]
    fn test_process_dialect_create_table_as() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        let sql = d.create_table_as("my_table", "SELECT 1");
        assert!(sql.contains("CREATE OR REPLACE TABLE my_table"));
        assert!(sql.contains("SELECT 1"));
    }

    /// The SDK deliberately has no `rocky-sql` dependency, so this dialect
    /// cannot run the validators the five in-tree dialects run before
    /// splicing a metadata column into `CAST({} AS {}) AS {}`. It refuses
    /// instead of splicing unvalidated text (#1594). Nothing reaches it
    /// today; the refusal is what keeps that true if something does.
    #[test]
    fn test_process_dialect_refuses_metadata_columns() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        assert_eq!(
            d.select_clause(&crate::traits::ColumnSelection::All, &[])
                .unwrap(),
            "SELECT *"
        );
        let meta = [crate::traits::MetadataColumn {
            name: "_loaded_by".into(),
            data_type: "STRING".into(),
            value: "NULL".into(),
        }];
        assert!(
            d.select_clause(&crate::traits::ColumnSelection::All, &meta)
                .is_err(),
            "an unvalidatable metadata column must be refused, not spliced"
        );
    }

    #[test]
    fn test_process_dialect_watermark_where_no_prior() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        let sql = d.watermark_where("_fivetran_synced", None).unwrap();
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > TIMESTAMP '1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn test_process_dialect_watermark_where_with_prior() {
        use chrono::TimeZone;
        let d = ProcessDialect {
            name: "test".into(),
        };
        let prior = chrono::Utc.with_ymd_and_hms(2026, 4, 17, 9, 30, 0).unwrap();
        let sql = d.watermark_where("_fivetran_synced", Some(&prior)).unwrap();
        // Source-side semantics: literal substitution, no MAX subquery.
        assert_eq!(
            sql,
            "WHERE _fivetran_synced > TIMESTAMP '2026-04-17 09:30:00'"
        );
    }

    #[test]
    fn test_process_dialect_row_hash() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        let expr = d.row_hash_expr(&["col1".into(), "col2".into()]);
        assert!(expr.contains("MD5"));
        assert!(expr.contains("col1, col2"));
    }

    /// A spawned adapter must not inherit the outer Pipes session.
    #[cfg(unix)]
    #[tokio::test]
    async fn spawned_adapter_does_not_inherit_pipes_environment() {
        const KEY: &str = "DAGSTER_PIPES_ADAPTER_PROBE";
        struct Restore(Option<std::ffi::OsString>);
        impl Drop for Restore {
            fn drop(&mut self) {
                // SAFETY: restore this test's process-global value before returning.
                unsafe {
                    match self.0.take() {
                        Some(value) => std::env::set_var(KEY, value),
                        None => std::env::remove_var(KEY),
                    }
                }
            }
        }
        let _restore = Restore(std::env::var_os(KEY));
        // SAFETY: no other test uses this probe name.
        unsafe { std::env::set_var(KEY, "outer") };
        let script = r#"
import json, os, sys
if os.environ.get('DAGSTER_PIPES_ADAPTER_PROBE'):
    sys.exit(7)
request = json.loads(sys.stdin.readline())
manifest = {
    'name': 'probe', 'version': '0.0.0', 'sdk_version': '0.0.0',
    'dialect': 'probe',
    'capabilities': {'warehouse': True, 'discovery': False, 'governance': False,
                     'batch_checks': False, 'create_catalog': False,
                     'create_schema': False, 'merge': False, 'tablesample': False,
                     'file_load': False},
    'auth_methods': [], 'config_schema': {},
}
print(json.dumps({'jsonrpc': '2.0', 'id': request['id'],
                  'result': {'manifest': manifest}}), flush=True)
"#;
        let adapter = ProcessAdapter::spawn("python3", &["-c", script], &serde_json::json!({}))
            .await
            .expect("adapter should initialize without outer Pipes environment");
        drop(adapter);
    }

    /// Spawn a tiny Python-based JSON-RPC echo adapter and fire two calls
    /// concurrently. The call lock keeps each response paired with its id.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_concurrent_calls_do_not_swap_ids() {
        // The adapter:
        //   1. on `initialize`, writes back a manifest.
        //   2. on `echo`, sleeps to widen the race window, then echoes the
        //      caller's `id` + a marker copied from `params.tag`.
        let script = r#"
import json, sys, time
def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
manifest = {
    "name": "echo", "version": "0.0.0", "sdk_version": "0.0.0",
    "dialect": "echo",
    "capabilities": {
        "warehouse": True, "discovery": False, "governance": False,
        "batch_checks": False, "create_catalog": False, "create_schema": False,
        "merge": False, "tablesample": False, "file_load": False,
    },
    "auth_methods": [], "config_schema": {},
}
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    method = req.get("method")
    rid = req.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": rid, "result": {"manifest": manifest}})
    elif method == "echo":
        # Sleep to widen the race window so an unsynchronized
        # implementation will reliably interleave.
        time.sleep(0.05)
        tag = req.get("params", {}).get("tag")
        emit({"jsonrpc": "2.0", "id": rid, "result": {"id_seen": rid, "tag": tag}})
    elif method == "shutdown":
        emit({"jsonrpc": "2.0", "id": rid, "result": {}})
        break
    else:
        emit({"jsonrpc": "2.0", "id": rid,
              "error": {"code": "UNKNOWN_METHOD", "message": method or ""}})
"#;
        let adapter =
            match ProcessAdapter::spawn("python3", &["-c", script], &serde_json::json!({})).await {
                Ok(a) => a,
                Err(e) => {
                    // Skip silently if python3 isn't installed in this
                    // environment — we don't want to fail CI on a missing
                    // tool the test only uses to emulate an adapter.
                    eprintln!("skipping: spawn failed (python3 unavailable?): {e}");
                    return;
                }
            };

        // Fire two calls concurrently. Each carries a distinct `tag` so we
        // can assert each result was returned to the right caller.
        let a = adapter.call("echo", serde_json::json!({"tag": "alpha"}));
        let b = adapter.call("echo", serde_json::json!({"tag": "beta"}));
        let (ra, rb) = tokio::join!(a, b);

        let ra = ra.expect("call A succeeded");
        let rb = rb.expect("call B succeeded");

        assert_eq!(ra["tag"], "alpha", "call A must receive its own tag");
        assert_eq!(rb["tag"], "beta", "call B must receive its own tag");

        // The id_seen field round-trips the request id; assert each
        // caller's response carries its own id, not the other caller's.
        let ida = ra["id_seen"].as_u64().expect("id_seen on A");
        let idb = rb["id_seen"].as_u64().expect("id_seen on B");
        assert_ne!(ida, idb, "request ids must be distinct");

        let _ = adapter.close().await;
    }

    /// A Python adapter for the limit tests.
    ///
    /// - `initialize`: answers with a manifest, unless `ROCKY_TEST_SILENT_INIT`
    ///   is in the config, then never answers.
    /// - `ping`: answers `{"pong": true}`.
    /// - `spawn`: starts `sleep 1000` and answers with its pid and the
    ///   adapter's own pid.
    /// - `hang`: never answers.
    /// - `big`: answers with one line of `params.size` bytes.
    ///
    /// The adapter ignores `SIGTERM`, and so does the `sleep` it starts (an
    /// ignored signal stays ignored across `exec`), so only a `SIGKILL` to the
    /// whole group stops both.
    #[cfg(unix)]
    const LIMITS_ADAPTER: &str = r#"
import json, os, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
def emit(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()
manifest = {
    "name": "limits", "version": "0.0.0", "sdk_version": "0.0.0",
    "dialect": "limits",
    "capabilities": {
        "warehouse": True, "discovery": False, "governance": False,
        "batch_checks": False, "create_catalog": False, "create_schema": False,
        "merge": False, "tablesample": False, "file_load": False,
    },
    "auth_methods": [], "config_schema": {},
}
for line in sys.stdin:
    req = json.loads(line)
    method, rid, params = req.get("method"), req.get("id"), req.get("params", {})
    if method == "initialize":
        if params.get("config", {}).get("ROCKY_TEST_SILENT_INIT"):
            while True:
                time.sleep(1000)
        emit({"jsonrpc": "2.0", "id": rid, "result": {"manifest": manifest}})
    elif method == "ping":
        emit({"jsonrpc": "2.0", "id": rid, "result": {"pong": True}})
    elif method == "spawn":
        p = subprocess.Popen(["sleep", "1000"])
        emit({"jsonrpc": "2.0", "id": rid,
              "result": {"adapter": os.getpid(), "descendant": p.pid}})
    elif method == "hang":
        while True:
            time.sleep(1000)
    elif method == "big":
        sys.stdout.write("x" * params["size"] + "\n")
        sys.stdout.flush()
    elif method == "shutdown":
        emit({"jsonrpc": "2.0", "id": rid, "result": {}})
        break
"#;

    #[cfg(unix)]
    fn python3_available() -> bool {
        std::process::Command::new("python3")
            .args(["-c", "pass"])
            .status()
            .is_ok_and(|s| s.success())
    }

    #[cfg(unix)]
    fn test_limits() -> ProcessLimits {
        ProcessLimits {
            init_timeout: Duration::from_secs(5),
            call_timeout: Duration::from_secs(1),
            stop_grace: Duration::from_millis(200),
            max_response_bytes: 1024,
        }
    }

    /// Whether `pid` has gone: no such process, or a zombie its new parent
    /// has not reaped yet.
    #[cfg(unix)]
    fn process_is_gone(pid: u64) -> bool {
        let out = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .expect("run ps");
        let stat = String::from_utf8_lossy(&out.stdout);
        let stat = stat.trim();
        stat.is_empty() || stat.starts_with('Z')
    }

    #[cfg(unix)]
    async fn wait_until_gone(pid: u64) -> bool {
        for _ in 0..100 {
            if process_is_gone(pid) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Ask the adapter to start a descendant; returns `(adapter, descendant)`.
    #[cfg(unix)]
    async fn spawn_descendant(adapter: &ProcessAdapter) -> (u64, u64) {
        let pids = adapter
            .call("spawn", serde_json::json!({}))
            .await
            .expect("spawn call");
        let adapter_pid = pids["adapter"].as_u64().expect("adapter pid");
        let descendant = pids["descendant"].as_u64().expect("descendant pid");
        assert!(!process_is_gone(descendant), "descendant should be running");
        (adapter_pid, descendant)
    }

    /// A call the adapter never answers fails with `Timeout` after
    /// `call_timeout`, and the adapter and its descendant are both gone,
    /// even though both ignore `SIGTERM`.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_call_past_the_timeout_stops_the_whole_process_group() {
        if !python3_available() {
            eprintln!("skipping: python3 unavailable");
            return;
        }
        let run = async {
            let adapter = ProcessAdapter::spawn_with_limits(
                "python3",
                &["-c", LIMITS_ADAPTER],
                &serde_json::json!({}),
                test_limits(),
            )
            .await
            .expect("adapter should initialize");

            // A normal call still works under the limits.
            let pong = adapter.call("ping", serde_json::json!({})).await.unwrap();
            assert_eq!(pong["pong"], true);

            let (adapter_pid, descendant) = spawn_descendant(&adapter).await;

            let err = adapter
                .call("hang", serde_json::json!({}))
                .await
                .expect_err("a hung call must time out");
            match err.inner().downcast_ref::<ProcessAdapterError>() {
                Some(ProcessAdapterError::Timeout { method, after }) => {
                    assert_eq!(method, "hang");
                    assert_eq!(*after, Duration::from_secs(1));
                }
                other => panic!("expected a Timeout, got {other:?} ({err})"),
            }
            assert!(wait_until_gone(adapter_pid).await, "adapter still running");
            assert!(
                wait_until_gone(descendant).await,
                "the adapter's descendant survived the timeout"
            );

            // The stream may hold half a message, so later calls refuse.
            let err = adapter
                .call("ping", serde_json::json!({}))
                .await
                .expect_err("a stopped adapter must refuse calls");
            assert!(matches!(
                err.inner().downcast_ref::<ProcessAdapterError>(),
                Some(ProcessAdapterError::Stopped)
            ));
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the timeout test itself hung");
    }

    /// A response line longer than `max_response_bytes` fails with
    /// `ResponseTooLarge`, and the adapter is stopped.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_oversized_response_is_refused_and_stops_the_adapter() {
        if !python3_available() {
            eprintln!("skipping: python3 unavailable");
            return;
        }
        let run = async {
            let adapter = ProcessAdapter::spawn_with_limits(
                "python3",
                &["-c", LIMITS_ADAPTER],
                &serde_json::json!({}),
                ProcessLimits {
                    call_timeout: Duration::from_secs(10),
                    ..test_limits()
                },
            )
            .await
            .expect("adapter should initialize");
            let (adapter_pid, descendant) = spawn_descendant(&adapter).await;

            let err = adapter
                .call("big", serde_json::json!({ "size": 4096 }))
                .await
                .expect_err("an oversized response must be refused");
            match err.inner().downcast_ref::<ProcessAdapterError>() {
                Some(ProcessAdapterError::ResponseTooLarge { method, max_bytes }) => {
                    assert_eq!(method, "big");
                    assert_eq!(*max_bytes, 1024);
                }
                other => panic!("expected ResponseTooLarge, got {other:?} ({err})"),
            }
            assert!(wait_until_gone(adapter_pid).await, "adapter still running");
            assert!(
                wait_until_gone(descendant).await,
                "descendant still running"
            );
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the size-cap test itself hung");
    }

    /// An adapter that never answers `initialize` fails `spawn` with a
    /// `Timeout`, instead of blocking `rocky adapter list` forever.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_silent_initialize_times_out() {
        if !python3_available() {
            eprintln!("skipping: python3 unavailable");
            return;
        }
        let run = async {
            let err = ProcessAdapter::spawn_with_limits(
                "python3",
                &["-c", LIMITS_ADAPTER],
                &serde_json::json!({ "ROCKY_TEST_SILENT_INIT": true }),
                ProcessLimits {
                    init_timeout: Duration::from_millis(500),
                    ..test_limits()
                },
            )
            .await
            .expect_err("a silent initialize must time out");
            assert!(
                matches!(
                    err.inner().downcast_ref::<ProcessAdapterError>(),
                    Some(ProcessAdapterError::Timeout { method, .. }) if method == "initialize"
                ),
                "expected an initialize Timeout, got {err}"
            );
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the init-timeout test itself hung");
    }

    /// Dropping a `ProcessAdapter` without `close` stops its whole group.
    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_the_adapter_stops_its_descendants() {
        if !python3_available() {
            eprintln!("skipping: python3 unavailable");
            return;
        }
        let run = async {
            let adapter = ProcessAdapter::spawn_with_limits(
                "python3",
                &["-c", LIMITS_ADAPTER],
                &serde_json::json!({}),
                test_limits(),
            )
            .await
            .expect("adapter should initialize");
            let (adapter_pid, descendant) = spawn_descendant(&adapter).await;
            drop(adapter);
            assert!(wait_until_gone(adapter_pid).await, "adapter still running");
            assert!(
                wait_until_gone(descendant).await,
                "the adapter's descendant survived the drop"
            );
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the drop test itself hung");
    }

    /// An adapter that exits on `SIGTERM` is not made to wait out the grace:
    /// the stop path sees the leader exit and returns at once. The adapters
    /// above ignore `SIGTERM`, so they reach `SIGKILL` even when the exit
    /// check never fires. With a 5 s grace, a check that never fires would
    /// take about 6 s here and fail the bound.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_leader_that_exits_on_sigterm_does_not_wait_out_the_grace() {
        if !python3_available() {
            eprintln!("skipping: python3 unavailable");
            return;
        }
        let run = async {
            // The same adapter, minus the line that ignores `SIGTERM`.
            let script =
                LIMITS_ADAPTER.replace("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n", "");
            assert_ne!(script, LIMITS_ADAPTER, "the SIGTERM line should be gone");
            let adapter = ProcessAdapter::spawn_with_limits(
                "python3",
                &["-c", &script],
                &serde_json::json!({}),
                ProcessLimits {
                    call_timeout: Duration::from_secs(1),
                    stop_grace: Duration::from_secs(5),
                    ..test_limits()
                },
            )
            .await
            .expect("adapter should initialize");

            let started = std::time::Instant::now();
            let err = adapter
                .call("hang", serde_json::json!({}))
                .await
                .expect_err("a hung call must time out");
            let elapsed = started.elapsed();
            assert!(
                matches!(
                    err.inner().downcast_ref::<ProcessAdapterError>(),
                    Some(ProcessAdapterError::Timeout { .. })
                ),
                "expected a Timeout, got {err}"
            );
            assert!(
                elapsed < Duration::from_millis(2500),
                "the stop path waited out the grace: {elapsed:?}"
            );
            eprintln!("sigterm-exit call returned in {elapsed:?}");
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the grace test itself hung");
    }
}
