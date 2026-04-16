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

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::cors::CorsLayer;

use crate::dashboard;
use crate::state::ServerState;

/// Build the axum router with all API routes.
pub fn router(state: Arc<ServerState>) -> Router {
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
        .layer(CorsLayer::permissive())
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
pub async fn serve(state: Arc<ServerState>, port: u16) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port, "rocky serve listening");
    axum::serve(listener, app).await?;
    Ok(())
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
    let state_path = state.models_dir.join(".rocky-state.redb");
    let store = rocky_core::state::StateStore::open(&state_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let runs = store
        .list_runs(50)
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
    let state_path = state.models_dir.join(".rocky-state.redb");
    let store = rocky_core::state::StateStore::open(&state_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let history = store
        .get_model_history(&name, 50)
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
    let state_path = state.models_dir.join(".rocky-state.redb");
    let store = rocky_core::state::StateStore::open(&state_path)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let snapshots = store
        .get_quality_trend(&name, 20)
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
        Arc::new(ServerState {
            models_dir,
            contracts_dir: None,
            config_path: None,
            compile_result: tokio::sync::RwLock::new(None),
            dag_status: rocky_core::dag_status::DagStatusStore::new(),
        })
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
}
