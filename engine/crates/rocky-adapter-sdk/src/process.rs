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
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

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
pub struct ProcessAdapter {
    /// The child process.
    child: Mutex<Child>,
    /// Buffered writer to the child's stdin.
    stdin: Mutex<BufWriter<ChildStdin>>,
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
    /// Monotonically increasing request ID.
    next_id: AtomicU64,
    /// The adapter's manifest, received during initialization.
    manifest: AdapterManifest,
    /// A minimal dialect proxy for the process adapter.
    dialect: ProcessDialect,
}

impl std::fmt::Debug for ProcessAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessAdapter")
            .field("manifest", &self.manifest)
            .finish_non_exhaustive()
    }
}

impl ProcessAdapter {
    /// Spawn an adapter process and initialize it.
    ///
    /// 1. Starts the child process
    /// 2. Sends an `initialize` request with the provided config
    /// 3. Reads the manifest from the response
    pub async fn spawn(
        command: &str,
        args: &[&str],
        config: &serde_json::Value,
    ) -> Result<Self, AdapterError> {
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit()) // let adapter log to stderr
            .spawn()
            .map_err(|e| {
                AdapterError::msg(format!("failed to spawn adapter process '{command}': {e}"))
            })?;

        let child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| AdapterError::msg("failed to capture adapter stdin"))?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| AdapterError::msg("failed to capture adapter stdout"))?;

        let stdin = Mutex::new(BufWriter::new(child_stdin));
        let stdout = Mutex::new(BufReader::new(child_stdout));
        let next_id = AtomicU64::new(1);

        // Send initialize request.
        let init_request = RpcRequest::new(
            next_id.fetch_add(1, Ordering::SeqCst),
            "initialize",
            serde_json::json!({ "config": config }),
        );

        let request_json = serde_json::to_string(&init_request).map_err(AdapterError::new)?;

        {
            let mut writer = stdin.lock().await;
            writer
                .write_all(request_json.as_bytes())
                .await
                .map_err(|e| AdapterError::msg(format!("failed to write to adapter stdin: {e}")))?;
            writer
                .write_all(b"\n")
                .await
                .map_err(|e| AdapterError::msg(format!("failed to write newline: {e}")))?;
            writer
                .flush()
                .await
                .map_err(|e| AdapterError::msg(format!("failed to flush adapter stdin: {e}")))?;
        }

        // Read initialize response.
        let response = {
            let mut reader = stdout.lock().await;
            let mut line = String::new();
            reader.read_line(&mut line).await.map_err(|e| {
                AdapterError::msg(format!("failed to read from adapter stdout: {e}"))
            })?;
            let resp: RpcResponse = serde_json::from_str(line.trim())
                .map_err(|e| AdapterError::msg(format!("failed to parse adapter response: {e}")))?;
            resp
        };

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

        let dialect = ProcessDialect {
            name: manifest.dialect.clone(),
        };

        Ok(Self {
            child: Mutex::new(child),
            stdin,
            stdout,
            call_lock: Mutex::new(()),
            next_id,
            manifest,
            dialect,
        })
    }

    /// Get the adapter's manifest.
    pub fn manifest(&self) -> &AdapterManifest {
        &self.manifest
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
    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, AdapterError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = RpcRequest::new(id, method, params);

        let request_json = serde_json::to_string(&request).map_err(AdapterError::new)?;

        // Hold `call_lock` across the entire write→read pair so concurrent
        // callers cannot interleave on stdin/stdout. Without this, two
        // overlapping calls can swap responses: caller A writes id=N,
        // caller B writes id=M, then A reads M's reply and the existing
        // id mismatch surfaces as a hard error rather than corruption.
        let _guard = self.call_lock.lock().await;

        // Write request.
        {
            let mut writer = self.stdin.lock().await;
            writer
                .write_all(request_json.as_bytes())
                .await
                .map_err(|e| AdapterError::msg(format!("failed to write to adapter: {e}")))?;
            writer
                .write_all(b"\n")
                .await
                .map_err(|e| AdapterError::msg(format!("failed to write newline: {e}")))?;
            writer
                .flush()
                .await
                .map_err(|e| AdapterError::msg(format!("failed to flush adapter: {e}")))?;
        }

        // Read response.
        let mut line = String::new();
        {
            let mut reader = self.stdout.lock().await;
            reader
                .read_line(&mut line)
                .await
                .map_err(|e| AdapterError::msg(format!("failed to read from adapter: {e}")))?;
        }

        if line.is_empty() {
            return Err(AdapterError::msg(
                "adapter process closed stdout (process may have crashed)",
            ));
        }

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
        let _ = self.call("shutdown", serde_json::json!({})).await;

        // Wait for the child to exit.
        let mut child = self.child.lock().await;
        let _ = child.kill().await;

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

    fn select_clause(
        &self,
        columns: &crate::traits::ColumnSelection,
        metadata: &[crate::traits::MetadataColumn],
    ) -> AdapterResult<String> {
        let mut sql = String::from("SELECT ");
        match columns {
            crate::traits::ColumnSelection::All => sql.push('*'),
            crate::traits::ColumnSelection::Explicit(cols) => sql.push_str(&cols.join(", ")),
        }
        for mc in metadata {
            sql.push_str(&format!(
                ", CAST({} AS {}) AS {}",
                mc.value, mc.data_type, mc.name
            ));
        }
        Ok(sql)
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        Ok(format!(
            "WHERE {timestamp_col} > (SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01') FROM {target_ref})"
        ))
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

    #[test]
    fn test_process_dialect_watermark_where() {
        let d = ProcessDialect {
            name: "test".into(),
        };
        let sql = d
            .watermark_where("_fivetran_synced", "target.schema.table")
            .unwrap();
        assert!(sql.contains("WHERE _fivetran_synced >"));
        assert!(sql.contains("target.schema.table"));
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

    /// Spawn a tiny Python-based JSON-RPC echo adapter and fire two
    /// `call`s concurrently. Without [`ProcessAdapter::call_lock`] the two
    /// requests/responses can interleave on stdin/stdout and the per-call
    /// id-mismatch guard turns the race into a hard error. With the lock
    /// in place each caller deterministically reads its own response, so
    /// both ids match.
    ///
    /// Unix-only because the test relies on `python3` being on `PATH`,
    /// which is true for the engine CI matrix (ubuntu) but not Windows.
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
}
