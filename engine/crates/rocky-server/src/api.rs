//! HTTP API routes for `rocky serve`.
//!
//! Exposes the compiler's semantic graph as a REST API:
//! ```text
//! GET  /api/v1/health                          → health check
//! GET  /api/v1/models                          → list all models
//! GET  /api/v1/models/:name                    → model details
//! GET  /api/v1/models/:name/lineage            → column-level lineage
//! GET  /api/v1/models/:name/lineage/:column    → trace single column
//! GET  /api/v1/compile                         → full compile result
//! GET  /api/v1/dag                             → full DAG
//! GET  /api/v1/dag/layers                      → execution layers
//! GET  /api/v1/dag/status                      → latest DAG run status
//! POST /api/v1/compile                         → trigger recompilation
//! ```
//!
//! All routes except `/api/v1/health` require a Bearer token when one is
//! configured on [`ServerState`]; see [`crate::auth`].

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::auth::{build_cors_layer, require_bearer_token};
use crate::dashboard;
use crate::state::ServerState;

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

/// `GET /api/v1/dag/status` — latest DAG execution status.
///
/// Returns 503 Service Unavailable when no DAG run has been recorded yet.
/// Otherwise the body matches the [`DagStatus`][rocky_core::dag_status::DagStatus]
/// struct (completed_at + the full DagExecutionResult).
async fn dag_status(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    match state.dag_status.get().await {
        Some(status) => Ok(Json(serde_json::json!(status))),
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
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

// --- Route handlers ---

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

async fn list_models(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

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

async fn get_model(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let schema = result
        .semantic_graph
        .model_schema(&name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let typed_cols = result.type_check.typed_models.get(&name);

    let model = result.project.model(&name).ok_or(StatusCode::NOT_FOUND)?;

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

async fn model_lineage(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let schema = result
        .semantic_graph
        .model_schema(&name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let edges: Vec<_> = result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| *e.target.model == *name || *e.source.model == *name)
        .collect();

    Ok(Json(serde_json::json!({
        "model": name,
        "columns": schema.columns,
        "upstream": schema.upstream,
        "downstream": schema.downstream,
        "edges": edges,
    })))
}

async fn trace_column(
    State(state): State<Arc<ServerState>>,
    Path((name, column)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let _ = result
        .semantic_graph
        .model_schema(&name)
        .ok_or(StatusCode::NOT_FOUND)?;

    let trace = result.semantic_graph.trace_column(&name, &column);

    Ok(Json(serde_json::json!({
        "model": name,
        "column": column,
        "trace": trace,
    })))
}

async fn compile_status(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(Json(serde_json::json!({
        "models": result.project.model_count(),
        "diagnostics": result.diagnostics,
        "has_errors": result.has_errors,
        "layers": result.project.layers.len(),
    })))
}

async fn trigger_compile(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    state.recompile().await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "recompiled" })),
    )
}

async fn full_dag(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let nodes: Vec<serde_json::Value> = result
        .project
        .dag_nodes
        .iter()
        .map(|n| {
            serde_json::json!({
                "name": n.name,
                "depends_on": n.depends_on,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "edges": result.semantic_graph.edges,
        "execution_order": result.project.execution_order,
    })))
}

async fn list_runs(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // The redb open + scan are sync work that can sleep on the state
    // flock (see `StateStore::open_redb_with_retry`); move it off the
    // async runtime. Mirrors the pattern at `lsp.rs:468` (PR #263).
    let state_path = rocky_core::state::resolve_state_path(None, &state.models_dir).path;
    let runs = tokio::task::spawn_blocking(move || {
        let store = rocky_core::state::StateStore::open_read_only(&state_path)
            .map_err(|e| e.to_string())?;
        store.list_runs(50).map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let run_entries: Vec<serde_json::Value> = runs
        .iter()
        .map(|r| {
            serde_json::json!({
                "run_id": r.run_id,
                "started_at": r.started_at.to_rfc3339(),
                "finished_at": r.finished_at.to_rfc3339(),
                "status": format!("{:?}", r.status),
                "models_executed": r.models_executed.len(),
                "trigger": format!("{:?}", r.trigger),
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "runs": run_entries,
        "count": run_entries.len(),
    })))
}

async fn model_history(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let state_path = rocky_core::state::resolve_state_path(None, &state.models_dir).path;
    let history_name = name.clone();
    let history = tokio::task::spawn_blocking(move || {
        let store = rocky_core::state::StateStore::open_read_only(&state_path)
            .map_err(|e| e.to_string())?;
        store
            .get_model_history(&history_name, 50)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let entries: Vec<serde_json::Value> = history
        .iter()
        .map(|e| {
            serde_json::json!({
                "model_name": e.model_name,
                "started_at": e.started_at.to_rfc3339(),
                "finished_at": e.finished_at.to_rfc3339(),
                "duration_ms": e.duration_ms,
                "rows_affected": e.rows_affected,
                "status": e.status,
                "sql_hash": e.sql_hash,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "model": name,
        "history": entries,
        "count": entries.len(),
    })))
}

async fn model_metrics(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let state_path = rocky_core::state::resolve_state_path(None, &state.models_dir).path;
    let trend_name = name.clone();
    let snapshots = tokio::task::spawn_blocking(move || {
        let store = rocky_core::state::StateStore::open_read_only(&state_path)
            .map_err(|e| e.to_string())?;
        store
            .get_quality_trend(&trend_name, 20)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let entries: Vec<serde_json::Value> = snapshots
        .iter()
        .map(|s| {
            serde_json::json!({
                "timestamp": s.timestamp.to_rfc3339(),
                "run_id": s.run_id,
                "row_count": s.metrics.row_count,
                "null_rates": s.metrics.null_rates,
                "freshness_lag_seconds": s.metrics.freshness_lag_seconds,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "model": name,
        "metrics": entries,
        "count": entries.len(),
    })))
}

async fn dag_layers(
    State(state): State<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    Ok(Json(serde_json::json!({
        "layers": result.project.layers,
        "total_models": result.project.model_count(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_state() -> Arc<ServerState> {
        // Use the simple_project fixture
        let models_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        // Skip the spawned initial compile — tests hit the router directly
        // without needing a warmed `compile_result`, and the spawn is cheap
        // to absorb at test end.
        ServerState::new(models_dir, None, None)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = test_state();
        let app = router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/health"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_compile_and_list_models() {
        let state = test_state();
        // Manually compile
        state.recompile().await;

        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/models"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["count"], 3);
    }

    #[tokio::test]
    async fn test_get_model_detail() {
        let state = test_state();
        state.recompile().await;

        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/models/raw_orders"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["name"], "raw_orders");
        assert!(body["sql"].as_str().unwrap().contains("SELECT"));
    }

    #[tokio::test]
    async fn test_model_not_found() {
        let state = test_state();
        state.recompile().await;

        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/models/nonexistent"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }

    #[tokio::test]
    async fn test_dag_layers() {
        let state = test_state();
        state.recompile().await;

        let app = router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/dag/layers"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["total_models"], 3);
        assert_eq!(body["layers"].as_array().unwrap().len(), 3);
    }

    fn test_state_with_token(token: &str) -> Arc<ServerState> {
        let models_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        ServerState::with_auth(models_dir, None, None, Some(token.to_string()), Vec::new())
    }

    #[tokio::test]
    async fn auth_rejects_request_without_token() {
        let state = test_state_with_token("s3cret");
        let app = router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/api/v1/models"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }

    #[tokio::test]
    async fn auth_rejects_request_with_wrong_token() {
        let state = test_state_with_token("s3cret");
        let app = router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/api/v1/models"))
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
        let app = router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/api/v1/models"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn health_endpoint_is_auth_exempt() {
        let state = test_state_with_token("s3cret");
        let app = router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // No bearer token — health must still respond 200 so liveness
        // probes work without provisioning the secret to the prober.
        let resp = reqwest::get(format!("http://{addr}/api/v1/health"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn serve_refuses_non_loopback_without_token() {
        let models_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        let state = ServerState::with_auth(models_dir, None, None, None, Vec::new());
        let result = serve(
            state,
            ServeConfig {
                host: "0.0.0.0".to_string(),
                port: 0,
            },
        )
        .await;
        let err = result.expect_err("expected 0.0.0.0 without token to be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("Bearer token"), "unexpected error: {msg}");
    }

    #[tokio::test]
    async fn serve_allows_loopback_without_token() {
        // Sanity check: the historical default (loopback, no token)
        // must keep working — refusal applies only to non-loopback.
        let models_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../rocky-compiler/tests/fixtures/simple_project/models");
        let state = ServerState::with_auth(models_dir, None, None, None, Vec::new());
        // Bind on a random port; serve runs forever, so race a short
        // timeout against it. We only need to confirm it didn't bail
        // immediately on the loopback check.
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
        // Timeout = serve is happily running. An immediate Ok/Err would
        // mean the loopback check fired; that would be the bug.
        assert!(early.is_err(), "serve exited too quickly: {early:?}");
    }
}
