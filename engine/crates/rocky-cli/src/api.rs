//! HTTP API routes for `rocky serve`.
//!
//! Exposes the compiler's semantic graph and the run/quality state store as
//! a REST API:
//! ```text
//! GET  /api/v1/health                          → liveness (auth-exempt)
//! GET  /api/v1/meta                             → engine + config fingerprint
//! GET  /api/v1/models                           → list all models (dashboard-only)
//! GET  /api/v1/models/:name                     → model details (dashboard-only)
//! GET  /api/v1/models/:name/lineage             → column-level lineage
//! GET  /api/v1/models/:name/lineage/:column     → trace single column
//! GET  /api/v1/models/:name/history             → per-model run history
//! GET  /api/v1/models/:name/metrics             → per-model quality snapshots
//! GET  /api/v1/runs                             → project run history
//! GET  /api/v1/compile                          → full compile result
//! GET  /api/v1/dag                              → full unified DAG
//! GET  /api/v1/dag/layers                       → execution layers (dashboard-only)
//! GET  /api/v1/dag/status                       → latest DAG run status (server-only)
//! POST /api/v1/compile                          → trigger recompilation
//! ```
//!
//! ## Contract (`/api/v1`)
//!
//! The refitted read routes — `compile`, `lineage` (+ column), `dag`,
//! `runs`, `models/:name/history`, `models/:name/metrics` — serve the
//! **canonical typed output cores** (`compile_output`, `lineage_output`,
//! `dag_output`, …). Their response bytes are byte-for-byte identical to
//! `rocky <verb> --output json` (pretty-printed, trailing newline) so an
//! embedder on the HTTP API and a consumer on the SDK/MCP see the same data.
//! (`compile`'s wall-clock `compile_timings` are the one non-deterministic
//! field; everything else is deterministic given the same project + state.)
//!
//! `models`, `models/:name`, and `dag/layers` render the dashboard's
//! ad-hoc shapes and are **explicitly out of the `/api/v1` value contract**
//! for now — they have no canonical CLI counterpart (promoting them is
//! net-new construction, deferred). `health`, `POST /compile`, and
//! `dag/status` are server-lifecycle, also out of contract.
//!
//! All routes except `/api/v1/health` require a Bearer token when one is
//! configured on [`ServerState`]; see [`rocky_server::auth`]. Error
//! responses carry the [`ErrorEnvelope`] body (`{code, message,
//! remediation_hint}`), never an empty body.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use rocky_server::auth::{build_cors_layer, require_bearer_token};
use rocky_server::dashboard;
use rocky_server::state::ServerState;

use crate::commands::{
    column_lineage_output, compile_output, dag_output, history_runs_output, lineage_output,
    metrics_output, model_history_output, schemas_hash,
};
use crate::output::{
    ColumnLineageOutput, CompileOutput, DagOutput, ErrorEnvelope, HistoryOutput, LineageOutput,
    MetaOutput, MetricsOutput, ModelHistoryOutput,
};

/// Bind config for [`serve`].
///
/// Defaults bind to `127.0.0.1:8080` (loopback only) — the LAN-leak class
/// of bug doesn't reproduce unless the operator opts into a non-loopback
/// host *and* configures a Bearer token.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Bind host. Defaults to `127.0.0.1`. A non-loopback host (e.g.
    /// `0.0.0.0`) requires `auth_token` to be `Some`.
    pub host: String,
    /// Listen port.
    pub port: u16,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

/// Build the axum router with all API routes.
pub fn router(state: Arc<ServerState>) -> Router {
    let cors = build_cors_layer(&state.allowed_origins);

    Router::new()
        .route("/", get(dashboard::dashboard))
        .route("/dashboard", get(dashboard::dashboard))
        .route("/api/v1/health", get(health))
        .route("/api/v1/meta", get(meta))
        .route("/api/v1/models", get(list_models))
        .route("/api/v1/models/{name}", get(get_model))
        .route("/api/v1/models/{name}/lineage", get(model_lineage))
        .route("/api/v1/models/{name}/lineage/{column}", get(trace_column))
        .route("/api/v1/models/{name}/history", get(model_history))
        .route("/api/v1/models/{name}/metrics", get(model_metrics))
        .route("/api/v1/runs", get(list_runs))
        .route("/api/v1/compile", get(compile_status))
        .route("/api/v1/compile", post(trigger_compile))
        .route("/api/v1/dag", get(full_dag))
        .route("/api/v1/dag/layers", get(dag_layers))
        .route("/api/v1/dag/status", get(dag_status))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ))
        .layer(cors)
        .with_state(state)
}

/// Start the HTTP server.
///
/// # Errors
///
/// Returns an error when:
/// - the bind host is non-loopback (e.g. `0.0.0.0`) and no Bearer token
///   is configured on `state` — exposing the API on the LAN without
///   auth would leak model SQL, file paths, and run history;
/// - the listener fails to bind (port in use, permissions, etc.);
/// - the axum runtime returns an error.
pub async fn serve(state: Arc<ServerState>, config: ServeConfig) -> anyhow::Result<()> {
    if !is_loopback(&config.host) && state.auth_token.is_none() {
        anyhow::bail!(
            "rocky serve refuses to bind {host} without a Bearer token. \
             Pass --token <secret> (or set ROCKY_SERVE_TOKEN), or bind to \
             127.0.0.1 (the default).",
            host = config.host,
        );
    }

    let app = router(state);
    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!(addr = %bind_addr, "rocky serve listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Returns `true` when `host` resolves to loopback only.
///
/// Accepts the textual forms we actually emit / accept on the CLI:
/// `127.0.0.1`, `::1`, `localhost`. Anything else (including `0.0.0.0`
/// and external addresses) is treated as non-loopback.
fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

// --- Response + error plumbing ---

/// A JSON response body serialized with the SAME formatting that
/// `rocky <verb> --output json` emits — pretty-printed (2-space indent)
/// with a trailing newline — so `GET /api/v1/*` response bytes are
/// byte-for-byte identical to the CLI's stdout. See
/// [`crate::output::print_json`], whose default (non-compact) branch this
/// mirrors.
struct PrettyJson<T>(T);

impl<T: Serialize> IntoResponse for PrettyJson<T> {
    fn into_response(self) -> Response {
        match serde_json::to_string_pretty(&self.0) {
            Ok(mut body) => {
                body.push('\n');
                let mut resp = body.into_response();
                resp.headers_mut()
                    .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                resp
            }
            Err(e) => ApiError::internal(format!("serializing response: {e}")).into_response(),
        }
    }
}

/// A structured API error: an HTTP status *class* plus the [`ErrorEnvelope`]
/// body carrying a stable `code`, a human `message`, and an optional
/// `remediation_hint`. Replaces the bare, empty-bodied `StatusCode` errors so
/// embedders get an actionable, machine-switchable failure on every route.
struct ApiError {
    status: StatusCode,
    envelope: ErrorEnvelope,
}

impl ApiError {
    fn new(status: StatusCode, code: &str, message: impl Into<String>, hint: Option<&str>) -> Self {
        Self {
            status,
            envelope: ErrorEnvelope {
                code: code.to_string(),
                message: message.into(),
                remediation_hint: hint.map(str::to_string),
            },
        }
    }

    /// `503` — no compile result is available yet (the initial compile
    /// hasn't finished, or it failed). Distinct from [`ApiError::engine_busy`],
    /// which means "a run is holding the state lock".
    fn engine_not_ready() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "engine_not_ready",
            "the engine has no compile result available yet",
            Some("retry after the initial compile completes, or POST /api/v1/compile"),
        )
    }

    /// `503` — the state store is locked by a running job (retryable). A
    /// concurrent `rocky run` (or an API mutation job) holds the redb flock
    /// for the duration of the run; the three state-backed read routes are
    /// unavailable until it finishes.
    fn engine_busy() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "engine_busy",
            "the state store is locked by a running job",
            Some("state locked by a running job; retry"),
        )
    }

    /// `404` — the named model is not in the compiled graph.
    fn model_not_found(name: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "model_not_found",
            format!("model '{name}' not found"),
            Some("check the model name against GET /api/v1/models"),
        )
    }

    /// `500` — an unexpected internal error.
    fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            message,
            None,
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, PrettyJson(self.envelope)).into_response()
    }
}

/// Map an error from a state-backed read core onto the API error surface.
///
/// A redb lock contention ([`StateError::Busy`] / [`StateError::LockHeldByOther`]),
/// which happens when a concurrent `rocky run` (or an API mutation job) holds
/// the state flock, becomes a **retryable `503 engine_busy`** — never a `500`.
/// An embedder must be able to tell "the engine broke" from "retry in a
/// moment". Everything else is a genuine `500`.
///
/// [`StateError::Busy`]: rocky_core::state::StateError::Busy
/// [`StateError::LockHeldByOther`]: rocky_core::state::StateError::LockHeldByOther
fn map_state_err(err: anyhow::Error) -> ApiError {
    use rocky_core::state::StateError;
    match err.downcast::<StateError>() {
        Ok(StateError::Busy { .. } | StateError::LockHeldByOther { .. }) => ApiError::engine_busy(),
        Ok(other) => ApiError::internal(other.to_string()),
        Err(other) => ApiError::internal(other.to_string()),
    }
}

/// Map a blocking-task join failure (panic / cancel) onto a `500`.
fn map_join_err(err: &tokio::task::JoinError) -> ApiError {
    ApiError::internal(format!("blocking task failed: {err}"))
}

/// Resolve the state-store path for a read route the same way the CLI does.
fn state_path_for(state: &ServerState) -> std::path::PathBuf {
    rocky_core::state::resolve_state_path(None, &state.models_dir).path
}

/// The `/api/v1` routes this build serves — the feature-detection surface
/// exposed via `GET /api/v1/meta`.
fn api_v1_routes() -> Vec<String> {
    [
        "GET /api/v1/health",
        "GET /api/v1/meta",
        "GET /api/v1/models",
        "GET /api/v1/models/{name}",
        "GET /api/v1/models/{name}/lineage",
        "GET /api/v1/models/{name}/lineage/{column}",
        "GET /api/v1/models/{name}/history",
        "GET /api/v1/models/{name}/metrics",
        "GET /api/v1/runs",
        "GET /api/v1/compile",
        "POST /api/v1/compile",
        "GET /api/v1/dag",
        "GET /api/v1/dag/layers",
        "GET /api/v1/dag/status",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Coarse capability tokens so embedders feature-detect a build's surface
/// without version-sniffing. The canonical read routes carry the same
/// `*Output` shapes the CLI `--output json` emits.
fn capabilities() -> Vec<String> {
    [
        "compile",
        "lineage",
        "column_lineage",
        "dag",
        "history",
        "metrics",
        "meta",
        "error_envelope",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

// --- Route handlers ---

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

/// `GET /api/v1/meta` — engine + config fingerprint for feature detection.
///
/// Every field is computed at request time — never a baked literal — so the
/// schema version, the schema-set hash, and the config hash track the live
/// engine and the on-disk config even across a long-running sidecar.
async fn meta(State(state): State<Arc<ServerState>>) -> PrettyJson<MetaOutput> {
    // Per-request hash of the resolved `rocky.toml` (contents + path). Reveals
    // a config that drifted out from under a running sidecar; `None` when no
    // config was resolved at bind time.
    let config_hash = state.config_path.as_deref().and_then(|path| {
        std::fs::read(path).ok().map(|contents| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(path.to_string_lossy().as_bytes());
            hasher.update(b"\0");
            hasher.update(&contents);
            hasher.finalize().to_hex().to_string()
        })
    });

    PrettyJson(MetaOutput {
        engine_version: env!("CARGO_PKG_VERSION").to_string(),
        state_schema_version: rocky_core::state::current_schema_version(),
        schemas_hash: schemas_hash(),
        config_hash,
        capabilities: capabilities(),
        routes: api_v1_routes(),
    })
}

/// `GET /api/v1/models` — dashboard-only model list (out of `/api/v1` contract).
async fn list_models(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    let models: Vec<serde_json::Value> = result
        .semantic_graph
        .models
        .iter()
        .map(|(name, schema)| {
            serde_json::json!({
                "name": name,
                "columns": schema.columns.len(),
                "has_star": schema.has_star,
                "upstream": schema.upstream,
                "downstream": schema.downstream,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "models": models,
        "count": models.len(),
    })))
}

/// `GET /api/v1/models/:name` — dashboard-only model detail (out of contract).
async fn get_model(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    let schema = result
        .semantic_graph
        .model_schema(&name)
        .ok_or_else(|| ApiError::model_not_found(&name))?;

    let typed_cols = result.type_check.typed_models.get(&name);

    let model = result
        .project
        .model(&name)
        .ok_or_else(|| ApiError::model_not_found(&name))?;

    Ok(Json(serde_json::json!({
        "name": name,
        "sql": model.sql,
        "file_path": model.file_path,
        "columns": schema.columns,
        "typed_columns": typed_cols,
        "has_star": schema.has_star,
        "upstream": schema.upstream,
        "downstream": schema.downstream,
    })))
}

/// `GET /api/v1/models/:name/lineage` — canonical [`LineageOutput`].
///
/// Serves the in-process `compile_result` through the same `lineage_output`
/// core the CLI's `rocky lineage <name> --output json` calls, so the bytes
/// match (including the normalized `transform: "direct"`, not the raw
/// `"Direct"` the ad-hoc handler used to leak).
async fn model_lineage(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<PrettyJson<LineageOutput>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;
    let output = lineage_output(result, &name).map_err(|_| ApiError::model_not_found(&name))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/models/:name/lineage/:column` — canonical [`ColumnLineageOutput`].
///
/// Mirrors `rocky lineage <name> --column <column>` (the default upstream
/// trace, `downstream = false`).
async fn trace_column(
    State(state): State<Arc<ServerState>>,
    Path((name, column)): Path<(String, String)>,
) -> Result<PrettyJson<ColumnLineageOutput>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;
    let output = column_lineage_output(result, &name, &column, false)
        .map_err(|_| ApiError::model_not_found(&name))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/compile` — canonical [`CompileOutput`].
///
/// Recompiles from disk through the same `compile_output` core the CLI's
/// `rocky compile --output json` calls (with default flags: no `--model`,
/// `--expand-macros`, `--target-dialect`, or `--with-seed`). The only
/// non-deterministic field is the wall-clock `compile_timings`.
async fn compile_status(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<CompileOutput>, ApiError> {
    let config_path = state.config_path.clone();
    let models_dir = state.models_dir.clone();
    let contracts_dir = state.contracts_dir.clone();
    let state_path = state_path_for(&state);

    let output = tokio::task::spawn_blocking(move || {
        compile_output(
            config_path.as_deref(),
            &state_path,
            &models_dir,
            contracts_dir.as_deref(),
            None,  // model_filter
            false, // do_expand_macros
            None,  // target_dialect
            false, // with_seed
            None,  // cache_ttl_override
        )
    })
    .await
    .map_err(|e| map_join_err(&e))?
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(PrettyJson(output))
}

async fn trigger_compile(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    state.recompile().await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "recompiled" })),
    )
}

/// `GET /api/v1/dag` — canonical [`DagOutput`].
///
/// Builds the unified DAG from disk through the same `dag_output` core the
/// CLI's `rocky dag --output json` calls (no column lineage). Requires a
/// resolved config; `engine_not_ready` when none was bound.
async fn full_dag(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<DagOutput>, ApiError> {
    let Some(config_path) = state.config_path.clone() else {
        return Err(ApiError::engine_not_ready());
    };
    let models_dir = state.models_dir.clone();
    let contracts_dir = state.contracts_dir.clone();
    let state_path = state_path_for(&state);

    let output = tokio::task::spawn_blocking(move || {
        dag_output(
            &config_path,
            &state_path,
            &models_dir,
            None, // seeds_dir → falls back to <config_dir>/seeds, matching `rocky dag`
            contracts_dir.as_deref(),
            false, // include_column_lineage
            None,  // cache_ttl_override
        )
    })
    .await
    .map_err(|e| map_join_err(&e))?
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(PrettyJson(output))
}

/// `GET /api/v1/runs` — canonical [`HistoryOutput`] (project run history).
///
/// Mirrors `rocky history --output json` (no `--model`, `--since`, or
/// `--audit`). A lock-held state read surfaces as `503 engine_busy`.
async fn list_runs(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<HistoryOutput>, ApiError> {
    // The redb open + scan are sync work that can sleep on the state flock
    // (see `StateStore::open_redb_with_retry`); move it off the async runtime.
    let state_path = state_path_for(&state);
    let output = tokio::task::spawn_blocking(move || history_runs_output(&state_path, None, false))
        .await
        .map_err(|e| map_join_err(&e))?
        .map_err(map_state_err)?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/models/:name/history` — canonical [`ModelHistoryOutput`].
///
/// Mirrors `rocky history --model <name> --output json` (no rolling stats).
async fn model_history(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<PrettyJson<ModelHistoryOutput>, ApiError> {
    let state_path = state_path_for(&state);
    let output = tokio::task::spawn_blocking(move || {
        // `window` (20) is the CLI default; it is unused when rolling_stats
        // is false, so the output matches `rocky history --model <name>`.
        model_history_output(&state_path, &name, None, false, 20)
    })
    .await
    .map_err(|e| map_join_err(&e))?
    .map_err(map_state_err)?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/models/:name/metrics` — canonical [`MetricsOutput`].
///
/// Mirrors `rocky metrics <name> --output json` (no `--trend`, `--column`,
/// or `--alerts`).
async fn model_metrics(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<PrettyJson<MetricsOutput>, ApiError> {
    let state_path = state_path_for(&state);
    let output =
        tokio::task::spawn_blocking(move || metrics_output(&state_path, &name, false, None, false))
            .await
            .map_err(|e| map_join_err(&e))?
            .map_err(map_state_err)?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/dag/layers` — dashboard-only execution layers (out of contract).
async fn dag_layers(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    Ok(Json(serde_json::json!({
        "layers": result.project.layers,
        "total_models": result.project.model_count(),
    })))
}

/// `GET /api/v1/dag/status` — latest DAG execution status (server-only).
///
/// Returns `503 engine_not_ready` when no DAG run has been recorded yet.
/// Serializes the [`DagStatus`][rocky_core::dag_status::DagStatus] struct;
/// pinned out of the `/api/v1` value contract (no CLI counterpart, and the
/// in-process executor that populates it is future work).
async fn dag_status(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match state.dag_status.get().await {
        Some(status) => Ok(Json(serde_json::json!(status))),
        None => Err(ApiError::engine_not_ready()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn simple_project_models() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models")
    }

    fn test_state() -> Arc<ServerState> {
        // Skip the spawned initial compile — tests hit the router directly
        // and `recompile()` explicitly where a warmed graph is needed.
        ServerState::new(simple_project_models(), None, None)
    }

    /// Spawn the router on an ephemeral loopback port and return its base URL.
    async fn spawn_router(state: Arc<ServerState>) -> String {
        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// Reference bytes for a canonical output: exactly what
    /// `crate::output::print_json` emits by default (pretty + trailing `\n`).
    fn reference_bytes<T: Serialize>(output: &T) -> String {
        serde_json::to_string_pretty(output).unwrap() + "\n"
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_compile_and_list_models() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 3);
    }

    #[tokio::test]
    async fn test_get_model_detail() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models/raw_orders"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["name"], "raw_orders");
        assert!(body["sql"].as_str().unwrap().contains("SELECT"));
    }

    #[tokio::test]
    async fn test_model_not_found_returns_error_envelope() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models/nonexistent"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "model_not_found");
        assert!(body.remediation_hint.is_some());
    }

    #[tokio::test]
    async fn test_dag_layers() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/dag/layers"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["total_models"], 3);
        assert_eq!(body["layers"].as_array().unwrap().len(), 3);
    }

    // --- /meta ---

    #[tokio::test]
    async fn test_meta_is_computed_not_literal() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/meta")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: MetaOutput = resp.json().await.unwrap();
        assert_eq!(body.engine_version, env!("CARGO_PKG_VERSION"));
        // Computed from the live getter, never a baked literal.
        assert_eq!(
            body.state_schema_version,
            rocky_core::state::current_schema_version()
        );
        // Derived from the live schema registry.
        assert_eq!(body.schemas_hash, schemas_hash());
        assert!(!body.schemas_hash.is_empty());
        assert!(body.routes.iter().any(|r| r == "GET /api/v1/meta"));
        // No config bound in this fixture (models-only ServerState).
        assert!(body.config_hash.is_none());
    }

    // --- Golden API-vs-CLI parity (the load-bearing contract tests) ---
    //
    // Each asserts the API response bytes are byte-for-byte identical to what
    // the canonical core + `print_json` (pretty + trailing newline) produce —
    // i.e. identical to `rocky <verb> --output json`. Because `rocky <verb>`
    // and the handler call the SAME core, this pins the handler's core wiring,
    // its default args, and its serialization formatting against drift.

    #[tokio::test]
    async fn parity_lineage() {
        let state = test_state();
        state.recompile().await;
        // Reference from the in-process compile_result via the CLI core.
        let reference = {
            let lock = state.compile_result.read().await;
            let result = lock.as_ref().unwrap();
            reference_bytes(&lineage_output(result, "customer_orders").unwrap())
        };
        let base = spawn_router(state).await;
        let resp = reqwest::get(format!("{base}/api/v1/models/customer_orders/lineage"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();
        assert_eq!(api, reference, "GET /lineage must match `rocky lineage`");
        // Regression guard for the specific drift the refit fixes: the raw
        // enum `"Direct"` must never leak; the CLI core normalizes to `"direct"`.
        assert!(!api.contains("\"Direct\""));
    }

    #[tokio::test]
    async fn parity_column_lineage() {
        let state = test_state();
        state.recompile().await;
        let reference = {
            let lock = state.compile_result.read().await;
            let result = lock.as_ref().unwrap();
            reference_bytes(
                &column_lineage_output(result, "customer_orders", "customer_id", false).unwrap(),
            )
        };
        let base = spawn_router(state).await;
        let resp = reqwest::get(format!(
            "{base}/api/v1/models/customer_orders/lineage/customer_id"
        ))
        .await
        .unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();
        assert_eq!(api, reference);
    }

    #[tokio::test]
    async fn parity_compile_modulo_timings() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state.clone()).await;
        let resp = reqwest::get(format!("{base}/api/v1/compile"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();

        let state_path = state_path_for(&state);
        let reference = reference_bytes(
            &compile_output(
                None,
                &state_path,
                &simple_project_models(),
                None,
                None,
                false,
                None,
                false,
                None,
            )
            .unwrap(),
        );

        // `compile_timings` is wall-clock and inherently non-deterministic;
        // normalize it away, then require byte-equality on everything else.
        let normalize = |s: &str| -> serde_json::Value {
            let mut v: serde_json::Value = serde_json::from_str(s).unwrap();
            v["compile_timings"] = serde_json::json!(null);
            v
        };
        assert_eq!(normalize(&api), normalize(&reference));
    }

    #[tokio::test]
    async fn parity_dag() {
        let (_dir, models_dir, config_path) = minimal_dag_project();
        let state = ServerState::new(models_dir.clone(), None, Some(config_path.clone()));
        let base = spawn_router(state).await;
        let resp = reqwest::get(format!("{base}/api/v1/dag")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();

        let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;
        let reference = reference_bytes(
            &dag_output(
                &config_path,
                &state_path,
                &models_dir,
                None,
                None,
                false,
                None,
            )
            .unwrap(),
        );
        assert_eq!(api, reference, "GET /dag must match `rocky dag`");
    }

    #[tokio::test]
    async fn parity_runs_history_metrics_on_empty_state() {
        // A hermetic, empty state store. Both the API and the CLI core resolve
        // the same path via `resolve_state_path`, so they read identical bytes.
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;
        // Create + init the (empty) store so open_read_only succeeds.
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());

        let state = ServerState::new(models_dir.clone(), None, None);
        let base = spawn_router(state).await;

        // /runs
        let resp = reqwest::get(format!("{base}/api/v1/runs")).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(&history_runs_output(&state_path, None, false).unwrap())
        );

        // /models/{name}/history
        let resp = reqwest::get(format!("{base}/api/v1/models/some_model/history"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(
                &model_history_output(&state_path, "some_model", None, false, 20).unwrap()
            )
        );

        // /models/{name}/metrics
        let resp = reqwest::get(format!("{base}/api/v1/models/some_model/metrics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(
                &metrics_output(&state_path, "some_model", false, None, false).unwrap()
            )
        );
    }

    /// Minimal on-disk transformation project (rocky.toml + one model) for the
    /// `/dag` parity test. Mirrors the playground's config shape.
    fn minimal_dag_project() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();

        let mut sql = std::fs::File::create(models_dir.join("raw.sql")).unwrap();
        write!(sql, "SELECT 1 AS id").unwrap();
        let mut toml = std::fs::File::create(models_dir.join("raw.toml")).unwrap();
        write!(
            toml,
            "name = \"raw\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"raw\"\n"
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        let mut cfg = std::fs::File::create(&config_path).unwrap();
        write!(
            cfg,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n"
        )
        .unwrap();

        (dir, models_dir, config_path)
    }

    /// The three state-backed read routes must remap a redb lock contention
    /// to the retryable `503 engine_busy`, never a bare `500`. A concurrent
    /// `rocky run` holds the flock for the whole run; an embedder needs the
    /// "retry" signal, not "engine broke". Everything else stays a `500`.
    #[test]
    fn engine_busy_remaps_lock_contention() {
        use rocky_core::state::StateError;

        let busy = anyhow::Error::from(StateError::Busy {
            path: "x".to_string(),
        });
        let mapped = map_state_err(busy);
        assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.envelope.code, "engine_busy");
        assert_eq!(
            mapped.envelope.remediation_hint.as_deref(),
            Some("state locked by a running job; retry")
        );

        let held = anyhow::Error::from(StateError::LockHeldByOther {
            path: "x".to_string(),
        });
        assert_eq!(map_state_err(held).envelope.code, "engine_busy");

        // A genuine, non-lock failure stays a 500.
        let other = map_state_err(anyhow::anyhow!("disk exploded"));
        assert_eq!(other.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(other.envelope.code, "internal_error");
    }

    // --- Auth (moved from rocky-server) ---

    fn test_state_with_token(token: &str) -> Arc<ServerState> {
        ServerState::with_auth(
            simple_project_models(),
            None,
            None,
            Some(token.to_string()),
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn auth_rejects_request_without_token() {
        let base = spawn_router(test_state_with_token("s3cret")).await;
        let resp = reqwest::get(format!("{base}/api/v1/models")).await.unwrap();
        assert_eq!(resp.status(), 401);
        // The 401 carries the structured envelope, not an empty body.
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "unauthorized");
    }

    #[tokio::test]
    async fn auth_rejects_request_with_wrong_token() {
        let base = spawn_router(test_state_with_token("s3cret")).await;
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{base}/api/v1/models"))
            .bearer_auth("wrong")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }

    #[tokio::test]
    async fn auth_accepts_request_with_correct_token() {
        let state = test_state_with_token("s3cret");
        state.recompile().await;
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{base}/api/v1/models"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn health_endpoint_is_auth_exempt() {
        let base = spawn_router(test_state_with_token("s3cret")).await;
        // No bearer token — health must still respond 200 so liveness probes
        // work without provisioning the secret to the prober.
        let resp = reqwest::get(format!("{base}/api/v1/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn serve_refuses_non_loopback_without_token() {
        let state = ServerState::with_auth(simple_project_models(), None, None, None, Vec::new());
        let result = serve(
            state,
            ServeConfig {
                host: "0.0.0.0".to_string(),
                port: 0,
            },
        )
        .await;
        let err = result.expect_err("expected 0.0.0.0 without token to be rejected");
        assert!(
            format!("{err}").contains("Bearer token"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn serve_allows_loopback_without_token() {
        // The historical default (loopback, no token) must keep working.
        let state = ServerState::with_auth(simple_project_models(), None, None, None, Vec::new());
        let task = tokio::spawn(async move {
            serve(
                state,
                ServeConfig {
                    host: "127.0.0.1".to_string(),
                    port: 0,
                },
            )
            .await
        });
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut Box::pin(task)).await;
        // Timeout = serve is happily running. An immediate Ok/Err would mean
        // the loopback check fired; that would be the bug.
        assert!(early.is_err(), "serve exited too quickly: {early:?}");
    }
}
