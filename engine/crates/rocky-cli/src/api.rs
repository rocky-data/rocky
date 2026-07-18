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
//! POST /api/v1/jobs/run                         → submit a run job    → 202 {job_id}
//! POST /api/v1/jobs/plan                        → submit a plan job   → 202 {job_id}
//! POST /api/v1/jobs/apply                       → submit an apply job → 202 {job_id}
//! GET  /api/v1/jobs/:id                          → job status (+ embedded result when done)
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
//! remediation_hint}`), never an empty body — including the router-level
//! fallbacks (`404 route_not_found` for an unmatched path,
//! `405 method_not_allowed` for a known path hit with the wrong method) and
//! malformed path parameters (`400 bad_request`). The one exception: a
//! request the HTTP stack rejects before it reaches the router (e.g.
//! malformed HTTP or an aborted body read) is answered below this layer and
//! carries no envelope.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use rocky_core::state::PersistedJob;
use rocky_server::auth::{build_cors_layer, require_bearer_token};
use rocky_server::dashboard;
use rocky_server::state::ServerState;

use crate::commands::{
    column_lineage_output, compile_output, dag_output, history_runs_output, lineage_output,
    metrics_output, model_history_output, schemas_hash,
};
use crate::output::{
    ColumnLineageOutput, CompileOutput, DagOutput, ErrorEnvelope, HistoryOutput, JobKind, JobState,
    JobStatus, LineageOutput, MetaOutput, MetricsOutput, ModelHistoryOutput,
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
///
/// ## Adding a route? Two rules, both load-bearing
///
/// 1. Register it **before** the `.layer(require_bearer_token)` call below.
///    `Router::layer` only wraps what is already in the router, so a route
///    (or fallback) appended after that call would silently **dodge auth**.
///    The probe test `every_declared_route_is_auth_wrapped_except_health`
///    fails on such a route — do not weaken it.
/// 2. Mirror it in [`api_v1_routes`]. That table anchors `/meta.routes` and
///    the generated OpenAPI paths; the probe test
///    `every_declared_route_is_registered_on_the_router` fails when the two
///    drift.
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
        .route("/api/v1/jobs/run", post(submit_run))
        .route("/api/v1/jobs/plan", post(submit_plan))
        .route("/api/v1/jobs/apply", post(submit_apply))
        .route("/api/v1/jobs/{id}", get(get_job))
        // Envelope fallbacks (see the module doc's error contract). Both are
        // registered BEFORE the auth layer so they are auth-wrapped like every
        // route: with a token configured, an unauthenticated probe of an
        // unknown path is a 401, not a route-existence oracle.
        .fallback(fallback_route_not_found)
        // Applies to the method routers registered ABOVE this line.
        .method_not_allowed_fallback(fallback_method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ))
        .layer(cors)
        .with_state(state)
}

/// Start the HTTP server.
///
/// Before the router serves, persisted jobs stranded in a non-terminal state
/// by a previous sidecar process are swept to `failed` (see
/// [`sweep_interrupted_jobs`]) so embedders polling `GET /api/v1/jobs/{id}`
/// until a terminal state never poll a dead job forever.
///
/// # Errors
///
/// Returns an error when:
/// - the bind host is non-loopback (e.g. `0.0.0.0`) and no Bearer token
///   is configured on `state` — exposing the API on the LAN without
///   auth would leak model SQL, file paths, and run history;
/// - the listener fails to bind (port in use, permissions, etc.);
/// - the axum runtime returns an error.
pub async fn serve(
    state: Arc<ServerState>,
    config: ServeConfig,
    shutdown: rocky_core::schedule::Drain,
) -> anyhow::Result<()> {
    if !is_loopback(&config.host) && state.auth_token.is_none() {
        anyhow::bail!(
            "rocky serve refuses to bind {host} without a Bearer token. \
             Pass --token <secret> (or set ROCKY_SERVE_TOKEN), or bind to \
             127.0.0.1 (the default).",
            host = config.host,
        );
    }

    // Startup reconciliation, ONCE, before the router serves: any job this
    // process submits from here on can never be swept. Best-effort — a locked
    // state store (e.g. a concurrent CLI run) only defers the sweep to the
    // next restart, it never blocks serving.
    let sweep_path = state_path_for(&state);
    match tokio::task::spawn_blocking(move || sweep_interrupted_jobs(&sweep_path)).await {
        Ok(Ok(0)) => {}
        Ok(Ok(swept)) => {
            tracing::warn!(
                swept,
                "marked jobs interrupted by a previous engine shutdown as failed"
            );
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "job-record sweep failed; stale `running` records may linger");
        }
        Err(e) => {
            tracing::warn!(error = %e, "job-record sweep task failed");
        }
    }

    let app = router(state);
    let bind_addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!(addr = %bind_addr, "rocky serve listening");
    // Graceful shutdown: on `shutdown` (SIGTERM/ctrl-c, shared with the scheduler
    // loop) axum stops accepting and drains in-flight requests before returning.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.signalled().await })
        .await?;
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
                running_job_id: None,
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
    ///
    /// When the lock holder is a mutation job submitted through THIS sidecar,
    /// `running_job_id` carries its id so the embedder can poll
    /// `GET /api/v1/jobs/{id}` instead of blind-retrying. An external writer
    /// (e.g. a concurrent CLI `rocky run`) has no job id, so it stays `None`.
    fn engine_busy(running_job_id: Option<String>) -> Self {
        let mut err = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "engine_busy",
            "the state store is locked by a running job",
            Some("state locked by a running job; retry"),
        );
        err.envelope.running_job_id = running_job_id;
        err
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

    /// `404` — no job with this id (neither in-memory nor persisted).
    fn job_not_found(id: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "job_not_found",
            format!("job '{id}' not found"),
            Some("use the job_id returned by POST /api/v1/jobs/{run|plan|apply}"),
        )
    }

    /// `409` — a `run`/`apply` job already holds the single-mutating-job permit.
    /// Carries the holder's `job_id` in `running_job_id` so the embedder can
    /// poll it. Distinct from `503 engine_busy` (redb lock contention), which is
    /// the cross-process backstop, not this app-level guard.
    fn mutation_in_progress(running_job_id: &str) -> Self {
        let mut err = Self::new(
            StatusCode::CONFLICT,
            "mutation_in_progress",
            "another run/apply job is already in progress on this project",
            Some("wait for the running job to finish (poll GET /api/v1/jobs/{id}), then resubmit"),
        );
        err.envelope.running_job_id = Some(running_job_id.to_string());
        err
    }

    /// `400` — the request body could not be parsed.
    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message, None)
    }

    /// `404` — no route matches the request path. Deliberately distinct from
    /// the resource-level 404s (`model_not_found`, `job_not_found`) so a
    /// client — and the route-registration probe test — can tell "no such
    /// route" from "route exists, resource doesn't".
    fn route_not_found(path: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "route_not_found",
            format!("no route matches '{path}'"),
            Some("list the routes this build serves via GET /api/v1/meta"),
        )
    }

    /// `405` — the path exists, but not for this method.
    fn method_not_allowed(method: &Method, path: &str) -> Self {
        Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            format!("method {method} is not supported for '{path}'"),
            Some("check the method against the route list at GET /api/v1/meta"),
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
/// `running_job_id` is the mutation permit's current holder
/// ([`MutationPermit::running_job`]); when this sidecar's own mutation job is
/// what holds the lock, the `503` carries its id so the embedder can poll the
/// job instead of blind-retrying. Only the busy arm forwards it.
///
/// [`StateError::Busy`]: rocky_core::state::StateError::Busy
/// [`StateError::LockHeldByOther`]: rocky_core::state::StateError::LockHeldByOther
/// [`MutationPermit::running_job`]: rocky_server::jobs::MutationPermit::running_job
fn map_state_err(err: anyhow::Error, running_job_id: Option<String>) -> ApiError {
    use rocky_core::state::StateError;
    match err.downcast::<StateError>() {
        Ok(StateError::Busy { .. } | StateError::LockHeldByOther { .. }) => {
            ApiError::engine_busy(running_job_id)
        }
        Ok(other) => ApiError::internal(other.to_string()),
        Err(other) => ApiError::internal(other.to_string()),
    }
}

/// Router-level fallback: an unmatched path answers the enveloped
/// `404 route_not_found` instead of axum's default empty body. Registered
/// before the auth layer in [`router`], so it is auth-wrapped like every
/// route.
async fn fallback_route_not_found(uri: Uri) -> ApiError {
    ApiError::route_not_found(uri.path())
}

/// Method-router fallback: a known path hit with an unsupported method
/// answers the enveloped `405 method_not_allowed` instead of axum's default
/// empty body.
async fn fallback_method_not_allowed(method: Method, uri: Uri) -> ApiError {
    ApiError::method_not_allowed(&method, uri.path())
}

/// [`Path`] wrapper whose rejection is the [`ErrorEnvelope`]: a malformed path
/// parameter (e.g. an invalid percent-encoded UTF-8 sequence) answers the
/// enveloped `400 bad_request` instead of axum's plain-text default, keeping
/// the module-level "every error carries the envelope" contract. Use this —
/// not bare [`Path`] — in `/api/v1` handlers.
struct ApiPath<T>(T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Path::<T>::from_request_parts(parts, state).await {
            Ok(Path(value)) => Ok(Self(value)),
            Err(rejection) => {
                // Preserve axum's status class (deserialization → 400,
                // missing-params wiring bug → 500); only the body changes
                // shape, from plain text to the envelope.
                let code = if rejection.status() == StatusCode::BAD_REQUEST {
                    "bad_request"
                } else {
                    "internal_error"
                };
                Err(ApiError::new(
                    rejection.status(),
                    code,
                    rejection.body_text(),
                    None,
                ))
            }
        }
    }
}

/// Map a blocking-task join failure (panic / cancel) onto a `500`.
fn map_join_err(err: &tokio::task::JoinError) -> ApiError {
    ApiError::internal(format!("blocking task failed: {err}"))
}

/// Resolve the state-store path for a read route the same way the CLI does.
pub(crate) fn state_path_for(state: &ServerState) -> std::path::PathBuf {
    rocky_core::state::resolve_state_path(None, &state.models_dir).path
}

/// The `/api/v1` routes this build serves — the feature-detection surface
/// exposed via `GET /api/v1/meta`.
///
/// Exposed to the crate so the OpenAPI generator ([`crate::commands::export_openapi`])
/// can assert its `paths` table covers exactly this route set — the anti-drift
/// guard that keeps the generated document honest when a route is added here.
pub(crate) fn api_v1_routes() -> Vec<String> {
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
        "POST /api/v1/jobs/run",
        "POST /api/v1/jobs/plan",
        "POST /api/v1/jobs/apply",
        "GET /api/v1/jobs/{id}",
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
        "jobs",
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
    ApiPath(name): ApiPath<String>,
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
    ApiPath(name): ApiPath<String>,
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
    ApiPath((name, column)): ApiPath<(String, String)>,
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
        .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/models/:name/history` — canonical [`ModelHistoryOutput`].
///
/// Mirrors `rocky history --model <name> --output json` (no rolling stats).
async fn model_history(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
) -> Result<PrettyJson<ModelHistoryOutput>, ApiError> {
    let state_path = state_path_for(&state);
    let output = tokio::task::spawn_blocking(move || {
        // `window` (20) is the CLI default; it is unused when rolling_stats
        // is false, so the output matches `rocky history --model <name>`.
        model_history_output(&state_path, &name, None, false, 20)
    })
    .await
    .map_err(|e| map_join_err(&e))?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/models/:name/metrics` — canonical [`MetricsOutput`].
///
/// Mirrors `rocky metrics <name> --output json` (no `--trend`, `--column`,
/// or `--alerts`).
async fn model_metrics(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
) -> Result<PrettyJson<MetricsOutput>, ApiError> {
    let state_path = state_path_for(&state);
    let output =
        tokio::task::spawn_blocking(move || metrics_output(&state_path, &name, false, None, false))
            .await
            .map_err(|e| map_join_err(&e))?
            .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
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

// --- Job model (POST /api/v1/jobs/{run|plan|apply}, GET /api/v1/jobs/{id}) ---

/// Optional JSON body for a job submission. Every field is optional; an empty
/// body runs the verb with its defaults (all pipelines, no filter). Unknown
/// fields are ignored so an embedder on a newer client stays forward-compatible.
///
/// Derives `JsonSchema` (crate-visible) so the OpenAPI generator can emit the
/// `POST /api/v1/jobs/{run|plan|apply}` request-body schema from this single
/// source of truth rather than a hand-copied duplicate. It is deliberately not
/// registered in [`super::commands::export_schemas`], so it stays out of the
/// Pydantic/TypeScript codegen cascade.
#[derive(Debug, Default, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub(crate) struct JobRequest {
    /// `--filter <component=value>` for `run`/`plan`.
    filter: Option<String>,
    /// `--pipeline <name>` for `run`/`plan`.
    pipeline: Option<String>,
    /// `--model <name>` for `run`/`plan` (single-model execution).
    model: Option<String>,
    /// The positional `<plan_id>` for `apply`.
    plan_id: Option<String>,
}

/// Mint an opaque, collision-resistant job id for this sidecar.
fn new_job_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let mut hasher = blake3::Hasher::new();
    hasher.update(&nanos.to_le_bytes());
    hasher.update(&seq.to_le_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    format!("job_{}", &hasher.finalize().to_hex()[..24])
}

/// The persisted string form of a lifecycle state.
pub(crate) fn job_state_str(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Succeeded => "succeeded",
        JobState::Failed => "failed",
    }
}

/// Build the API presentation type from the durable record. Unknown persisted
/// `kind`/`state` strings (only reachable from a malformed record) fall back to
/// safe defaults rather than failing the read.
fn job_status_from(job: PersistedJob) -> JobStatus {
    JobStatus {
        kind: JobKind::parse(&job.kind).unwrap_or(JobKind::Run),
        state: JobState::parse(&job.state).unwrap_or(JobState::Failed),
        job_id: job.job_id,
        submitted_at: job.submitted_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        principal: job.principal,
        error: job.error,
        result: job.result,
    }
}

/// Read and validate the advisory `X-Rocky-Principal` header.
///
/// Spoofable by construction under the single-shared-secret auth ceiling, so it
/// is recorded for audit only — never an authorization input. Validated against
/// the engine's principal charset (`^[a-zA-Z0-9_ \-.@]+$`); a malformed value is
/// a `400`, an absent one is `None`.
fn principal_from_headers(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(raw) = headers.get("x-rocky-principal") else {
        return Ok(None);
    };
    let value = raw
        .to_str()
        .map_err(|_| ApiError::bad_request("X-Rocky-Principal must be valid ASCII"))?;
    if value.is_empty() {
        return Ok(None);
    }
    let valid = value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '-' | '.' | '@'));
    if !valid {
        return Err(ApiError::bad_request(
            "X-Rocky-Principal contains characters outside ^[a-zA-Z0-9_ \\-.@]+$",
        ));
    }
    Ok(Some(value.to_string()))
}

/// Mark every persisted job stranded in a non-terminal state (`running` /
/// `queued`) as `failed` with the error `"interrupted by engine restart"`,
/// returning how many records were reconciled.
///
/// A job's terminal-state write lives in the background task of the process
/// that accepted the submission (see [`submit_job`]); when a sidecar dies
/// mid-job, nothing will ever finish the persisted record — even if the
/// orphaned subprocess completes, its outcome is never recorded — so an
/// embedder following the documented poll-until-terminal contract would poll
/// forever. [`serve`] runs this sweep **once, before the router serves**,
/// which is what makes it safe: a job submitted by the current process cannot
/// exist yet, so only records from a previous process are ever swept.
///
/// A missing state file is a no-op (nothing was ever persisted).
pub(crate) fn sweep_interrupted_jobs(state_path: &std::path::Path) -> anyhow::Result<usize> {
    if !state_path.exists() {
        return Ok(0);
    }
    let store = rocky_core::state::StateStore::open(state_path)?;
    let mut swept = 0;
    for job in store.list_jobs()? {
        if matches!(job.state.as_str(), "succeeded" | "failed") {
            continue;
        }
        let mut done = job;
        done.state = job_state_str(JobState::Failed).to_string();
        done.finished_at = Some(chrono::Utc::now().to_rfc3339());
        done.error = Some("interrupted by engine restart".to_string());
        store.record_job(&done)?;
        swept += 1;
    }
    Ok(swept)
}

/// Persist a job record (brief open-write-close on the blocking pool).
///
/// Remote `[state]` integrity (S1, #1089): unlike the governance ledgers
/// (`policy_decisions`, `tombstones`) — whose non-run write seams bracket
/// themselves with a download-before / upload-after remote sync — the `jobs`
/// table is deliberately **not** synced here. `serve` is long-running, so a
/// per-job remote round-trip would be impractical, and a job launched on one
/// pod is meaningless to another. `jobs` is therefore listed in
/// [`rocky_core::state::LOCAL_ONLY_TABLE_NAMES`], which keeps it node-local on
/// BOTH sync legs: the end-of-run/periodic upload strips it from the remote
/// snapshot, and the run-start download (`state_sync::download_state`)
/// explicitly PRESERVES the local-only tables across the wholesale file replace
/// — snapshotting them before the download and splicing them back afterward.
/// So this write stays node-local and is not reverted by another pod's
/// run-download. (Merely being stripped on upload would NOT be enough on its
/// own: without the download-side preservation, a run-download's wholesale file
/// replace would still wipe the local `jobs` rows.)
pub(crate) async fn persist_job(
    state_path: std::path::PathBuf,
    job: PersistedJob,
) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let store = rocky_core::state::StateStore::open(&state_path)?;
        store.record_job(&job)?;
        Ok(())
    })
    .await?
}

/// `POST /api/v1/jobs/run` — submit a run job (mutating; takes the permit).
async fn submit_run(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    submit_job(JobKind::Run, state, &headers, &body).await
}

/// `POST /api/v1/jobs/plan` — submit a plan job (non-mutating; no permit).
async fn submit_plan(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    submit_job(JobKind::Plan, state, &headers, &body).await
}

/// `POST /api/v1/jobs/apply` — submit an apply job (mutating; takes the permit).
async fn submit_apply(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    submit_job(JobKind::Apply, state, &headers, &body).await
}

/// Shared submission path for all three job kinds.
///
/// Returns `202 {job_id}` after (1) taking the mutation permit for `run`/`apply`
/// — a second attempt while one is held is a `409 mutation_in_progress` — and
/// (2) persisting the `running` record **before** spawning the subprocess, so a
/// crash immediately after submission still reports honest status on restart.
/// The actual `rocky <kind>` runs as a subprocess in a background task so the
/// server never holds a long-lived write handle.
async fn submit_job(
    kind: JobKind,
    state: Arc<ServerState>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Response, ApiError> {
    let request: JobRequest = if body.is_empty() {
        JobRequest::default()
    } else {
        serde_json::from_slice(body)
            .map_err(|e| ApiError::bad_request(format!("invalid job request body: {e}")))?
    };
    let principal = principal_from_headers(headers)?;
    let job_id = new_job_id();

    // Layer 1: the app-level single-mutating-job guard. `plan` never takes it.
    // A second `run`/`apply` while one is held returns the holder's id as a
    // `409 mutation_in_progress` rather than colliding on the redb flock.
    let permit = if kind.mutates() {
        match state.mutation_permit.try_acquire(&job_id) {
            Ok(guard) => Some(guard),
            Err(running_job_id) => return Err(ApiError::mutation_in_progress(&running_job_id)),
        }
    } else {
        None
    };

    let now = chrono::Utc::now().to_rfc3339();
    let record = PersistedJob {
        job_id: job_id.clone(),
        kind: kind.verb().to_string(),
        state: job_state_str(JobState::Running).to_string(),
        submitted_at: now.clone(),
        started_at: Some(now),
        finished_at: None,
        principal,
        error: None,
        result: None,
    };

    let state_path = state_path_for(&state);
    let config_path = state.config_path.clone();

    // Register + persist the `running` record BEFORE spawning the subprocess and
    // BEFORE returning 202, so a crash immediately after submission still reports
    // honest status on restart (the durability done-criterion). Persistence is
    // best-effort under lock contention — the in-memory registry is authoritative
    // for the live session, and embedders reconcile via /runs.
    state.jobs.upsert(record.clone()).await;
    if let Err(e) = persist_job(state_path.clone(), record.clone()).await {
        tracing::warn!(error = %e, job_id = %job_id,
            "could not persist initial job record; in-memory only until it settles");
    }

    // Background task: run the subprocess, then record the terminal state. The
    // permit is moved in and released when the task ends.
    let task_state = state.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let (final_state, result, error) =
            execute_job_subprocess(kind, config_path, state_path.clone(), request).await;

        let mut done = record;
        done.state = job_state_str(final_state).to_string();
        done.finished_at = Some(chrono::Utc::now().to_rfc3339());
        done.result = result;
        done.error = error;
        task_state.jobs.upsert(done.clone()).await;
        if let Err(e) = persist_job(state_path, done.clone()).await {
            tracing::warn!(error = %e, job_id = %done.job_id,
                "could not persist terminal job record; /runs is the reconcile surface");
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        PrettyJson(serde_json::json!({ "job_id": job_id })),
    )
        .into_response())
}

/// Spawn `rocky <kind> --output json` as a subprocess and collect its outcome.
///
/// Runs as a subprocess (matching the SDK's pattern) so the server never holds
/// a long-lived redb write handle for the job's duration. The subprocess
/// acquires the state flock for the run; the server's own reads/writes stay
/// brief. The verbatim stdout JSON becomes the embedded `result`.
async fn execute_job_subprocess(
    kind: JobKind,
    config_path: Option<std::path::PathBuf>,
    state_path: std::path::PathBuf,
    request: JobRequest,
) -> (JobState, Option<serde_json::Value>, Option<String>) {
    let exe = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            return (
                JobState::Failed,
                None,
                Some(format!("could not resolve the rocky binary: {e}")),
            );
        }
    };

    let mut cmd = tokio::process::Command::new(exe);
    cmd.arg("--output").arg("json");
    if let Some(config) = &config_path {
        cmd.arg("--config").arg(config);
    }
    cmd.arg("--state-path").arg(&state_path);
    cmd.arg(kind.verb());
    match kind {
        JobKind::Run | JobKind::Plan => {
            if let Some(filter) = &request.filter {
                cmd.arg("--filter").arg(filter);
            }
            if let Some(pipeline) = &request.pipeline {
                cmd.arg("--pipeline").arg(pipeline);
            }
            if let Some(model) = &request.model {
                cmd.arg("--model").arg(model);
            }
        }
        JobKind::Apply => {
            if let Some(plan_id) = &request.plan_id {
                cmd.arg(plan_id);
            }
        }
    }

    let output = match cmd.output().await {
        Ok(output) => output,
        Err(e) => {
            return (
                JobState::Failed,
                None,
                Some(format!("failed to spawn `rocky {}`: {e}", kind.verb())),
            );
        }
    };

    // The canonical output is emitted on stdout; embed it verbatim when parseable.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let result = serde_json::from_str::<serde_json::Value>(stdout.trim()).ok();

    if output.status.success() {
        (JobState::Succeeded, result, None)
    } else {
        // Surface the last few stderr lines (the actionable tail) as the error.
        let stderr = String::from_utf8_lossy(&output.stderr);
        let lines: Vec<&str> = stderr.lines().collect();
        let tail = lines[lines.len().saturating_sub(10)..].join("\n");
        let msg = if tail.trim().is_empty() {
            format!("`rocky {}` exited with {}", kind.verb(), output.status)
        } else {
            tail
        };
        (JobState::Failed, result, Some(msg))
    }
}

/// `GET /api/v1/jobs/{id}` — job status, with the embedded canonical result once
/// terminal.
///
/// Reads the in-memory registry first; on a miss (e.g. after a sidecar restart)
/// it falls back to the durable `jobs` state table, so a job launched before a
/// crash still reports its last-persisted status instead of a spurious `404`.
async fn get_job(
    State(state): State<Arc<ServerState>>,
    ApiPath(id): ApiPath<String>,
) -> Result<PrettyJson<JobStatus>, ApiError> {
    if let Some(record) = state.jobs.get(&id).await {
        return Ok(PrettyJson(job_status_from(record)));
    }

    let state_path = state_path_for(&state);
    if !state_path.exists() {
        return Err(ApiError::job_not_found(&id));
    }

    let lookup_id = id.clone();
    let record = tokio::task::spawn_blocking(move || {
        let store = rocky_core::state::StateStore::open_read_only(&state_path)?;
        store.get_job(&lookup_id)
    })
    .await
    .map_err(|e| map_join_err(&e))?
    .map_err(|e| map_state_err(anyhow::Error::from(e), state.mutation_permit.running_job()))?;

    match record {
        Some(record) => {
            // Warm the cache so subsequent reads are hot.
            state.jobs.upsert(record.clone()).await;
            Ok(PrettyJson(job_status_from(record)))
        }
        None => Err(ApiError::job_not_found(&id)),
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

    /// GET `url`, retrying while the endpoint returns a *documented-retryable*
    /// `503 engine_busy`. The three state-backed read routes open the redb
    /// store read-only, and redb takes an unconditional `flock` on open; the
    /// store's open-retry budget is only ~250ms (tuned for the LSP-vs-CLI
    /// keystroke race), which a loaded CI runner can exhaust — so the handler
    /// correctly returns the retryable 503 rather than blocking forever. Real
    /// embedders back off and retry on that 503; the parity assertion should
    /// too, instead of demanding 200 on the first request.
    async fn get_retrying_on_busy(url: &str) -> reqwest::Response {
        for _ in 0..40 {
            let resp = reqwest::get(url).await.unwrap();
            if resp.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return resp;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("{url} still returning 503 engine_busy after ~2s of retries");
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
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/runs")).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(&history_runs_output(&state_path, None, false).unwrap())
        );

        // /models/{name}/history
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/models/some_model/history")).await;
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(
                &model_history_output(&state_path, "some_model", None, false, 20).unwrap()
            )
        );

        // /models/{name}/metrics
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/models/some_model/metrics")).await;
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
        let mapped = map_state_err(busy, None);
        assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.envelope.code, "engine_busy");
        assert_eq!(
            mapped.envelope.remediation_hint.as_deref(),
            Some("state locked by a running job; retry")
        );
        // No permit holder known → no job id on the envelope.
        assert!(mapped.envelope.running_job_id.is_none());

        let held = anyhow::Error::from(StateError::LockHeldByOther {
            path: "x".to_string(),
        });
        assert_eq!(map_state_err(held, None).envelope.code, "engine_busy");

        // A genuine, non-lock failure stays a 500.
        let other = map_state_err(anyhow::anyhow!("disk exploded"), None);
        assert_eq!(other.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(other.envelope.code, "internal_error");
    }

    /// When this sidecar's own mutation job holds the permit, the `503
    /// engine_busy` carries its `running_job_id` (as the [`ErrorEnvelope`]
    /// field doc promises) so the embedder polls the job instead of
    /// blind-retrying. Only the busy arm forwards it — a genuine `500` never
    /// names a job even when one is running.
    #[test]
    fn engine_busy_names_the_permit_holding_job() {
        use rocky_core::state::StateError;

        // The handler wiring: the id comes from `mutation_permit.running_job()`.
        let permit = rocky_server::jobs::MutationPermit::new();
        let guard = permit.try_acquire("job_holder").expect("permit is free");

        let busy = anyhow::Error::from(StateError::Busy {
            path: "x".to_string(),
        });
        let mapped = map_state_err(busy, permit.running_job());
        assert_eq!(mapped.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.envelope.code, "engine_busy");
        assert_eq!(
            mapped.envelope.running_job_id.as_deref(),
            Some("job_holder")
        );

        // A non-lock failure ignores the holder entirely.
        let other = map_state_err(anyhow::anyhow!("disk exploded"), permit.running_job());
        assert_eq!(other.envelope.code, "internal_error");
        assert!(other.envelope.running_job_id.is_none());

        drop(guard);
        // Permit released → nothing to name.
        let busy = anyhow::Error::from(StateError::Busy {
            path: "x".to_string(),
        });
        assert!(
            map_state_err(busy, permit.running_job())
                .envelope
                .running_job_id
                .is_none()
        );
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
            rocky_core::schedule::Drain::new(),
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
                rocky_core::schedule::Drain::new(),
            )
            .await
        });
        let early =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut Box::pin(task)).await;
        // Timeout = serve is happily running. An immediate Ok/Err would mean
        // the loopback check fired; that would be the bug.
        assert!(early.is_err(), "serve exited too quickly: {early:?}");
    }

    // --- Job model (POST /api/v1/jobs/*, GET /api/v1/jobs/{id}) ---
    //
    // The full happy path (a real `rocky plan` subprocess emitting a PlanOutput)
    // is proven by the live reachability transcript, not here: in a cargo test
    // `std::env::current_exe()` is the test harness, not the `rocky` binary, so
    // these tests exercise every deterministic API-layer guarantee that returns
    // *before* the subprocess spawns (the 409 permit guard, principal
    // validation, 404) plus the durable-restart read, which needs no subprocess.

    /// A second `run`/`apply` submission while the permit is held is a
    /// `409 mutation_in_progress` carrying the holder's `job_id` — never a redb
    /// collision, never a queue.
    #[tokio::test]
    async fn job_second_mutation_returns_409_with_running_job_id() {
        let state = test_state();
        // Simulate a run/apply already in flight by holding the permit.
        let held = state
            .mutation_permit
            .try_acquire("job_incumbent")
            .expect("permit is free");
        let base = spawn_router(state).await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/api/v1/jobs/apply"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "mutation_in_progress");
        assert_eq!(body.running_job_id.as_deref(), Some("job_incumbent"));

        drop(held);
    }

    /// The permit-relevant contract for each kind: `run`/`apply` mutate (take
    /// the permit), `plan` does not (so a submitted plan is never blocked by a
    /// held permit). Asserted directly to avoid spawning the test harness as a
    /// bogus `rocky` subprocess.
    #[test]
    fn job_kind_mutation_and_verb_semantics() {
        assert!(JobKind::Run.mutates());
        assert!(JobKind::Apply.mutates());
        assert!(!JobKind::Plan.mutates());
        assert_eq!(JobKind::Run.verb(), "run");
        assert_eq!(JobKind::Plan.verb(), "plan");
        assert_eq!(JobKind::Apply.verb(), "apply");
    }

    /// An unknown job id is a structured `404 job_not_found`.
    #[tokio::test]
    async fn job_get_unknown_returns_404() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/jobs/does_not_exist"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "job_not_found");
    }

    /// A malformed `X-Rocky-Principal` is rejected before any work starts.
    #[tokio::test]
    async fn job_invalid_principal_returns_400() {
        let base = spawn_router(test_state()).await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/api/v1/jobs/plan"))
            .header("X-Rocky-Principal", "bad/slash")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "bad_request");
    }

    /// A [`PersistedJob`] fixture in the given lifecycle `state`.
    fn persisted_job(id: &str, state: &str) -> rocky_core::state::PersistedJob {
        rocky_core::state::PersistedJob {
            job_id: id.to_string(),
            kind: "run".to_string(),
            state: state.to_string(),
            submitted_at: "2026-07-07T00:00:00Z".to_string(),
            started_at: Some("2026-07-07T00:00:00Z".to_string()),
            finished_at: None,
            principal: None,
            error: None,
            result: None,
        }
    }

    /// The restart contract: a job persisted as `running` by a sidecar that
    /// died mid-job can never reach a terminal state on its own (the terminal
    /// write lived in the dead process's background task), so the startup
    /// sweep marks it failed — and a **freshly restarted** server (empty
    /// in-memory registry, durable-table fallback) reports that honestly. An
    /// embedder following the poll-until-terminal contract now terminates
    /// instead of polling a permanent `running` forever.
    #[tokio::test]
    async fn job_interrupted_by_restart_is_swept_to_failed() {
        use rocky_core::state::StateStore;

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;

        // A job that was "running" when the (previous) sidecar died.
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_job(&persisted_job("job_inflight", "running"))
                .unwrap();
        }

        // The "restart": `serve` runs the sweep once, before the router serves.
        let swept = sweep_interrupted_jobs(&state_path).unwrap();
        assert_eq!(swept, 1);

        // A brand-new server (empty in-memory registry) reads the durable record.
        let state = ServerState::new(models_dir, None, None);
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/jobs/job_inflight"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: JobStatus = resp.json().await.unwrap();
        assert_eq!(body.job_id, "job_inflight");
        assert!(
            matches!(body.state, JobState::Failed),
            "a killed-mid-job record must be swept to terminal `failed` on restart, got {:?}",
            body.state
        );
        assert_eq!(body.error.as_deref(), Some("interrupted by engine restart"));
        assert!(
            body.finished_at.is_some(),
            "the sweep stamps the terminal timestamp"
        );
        assert!(body.result.is_none());
    }

    /// The sweep reconciles ONLY non-terminal records: `running` and `queued`
    /// flip to `failed` with the documented error, while terminal
    /// `succeeded`/`failed` history is untouched. A missing state file is a
    /// no-op, not an error (fresh project, nothing ever persisted).
    #[test]
    fn sweep_marks_only_nonterminal_jobs_failed() {
        use rocky_core::state::StateStore;

        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join(".rocky-state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_job(&persisted_job("job_run", "running"))
                .unwrap();
            store.record_job(&persisted_job("job_q", "queued")).unwrap();
            store
                .record_job(&persisted_job("job_ok", "succeeded"))
                .unwrap();
            store
                .record_job(&persisted_job("job_bad", "failed"))
                .unwrap();
        }

        let swept = sweep_interrupted_jobs(&state_path).unwrap();
        assert_eq!(swept, 2, "exactly the running + queued records are swept");

        let store = StateStore::open(&state_path).unwrap();
        for id in ["job_run", "job_q"] {
            let job = store.get_job(id).unwrap().expect("record present");
            assert_eq!(job.state, "failed", "{id} must be terminal after sweep");
            assert_eq!(job.error.as_deref(), Some("interrupted by engine restart"));
            assert!(job.finished_at.is_some());
        }
        // Terminal history is preserved verbatim.
        let ok = store.get_job("job_ok").unwrap().unwrap();
        assert_eq!(ok.state, "succeeded");
        assert!(ok.error.is_none());
        let bad = store.get_job("job_bad").unwrap().unwrap();
        assert_eq!(bad.state, "failed");
        assert!(
            bad.error.is_none(),
            "an already-failed job keeps its own error"
        );
        drop(store);

        // Idempotent: a second sweep finds nothing non-terminal.
        assert_eq!(sweep_interrupted_jobs(&state_path).unwrap(), 0);
        // A state file that never existed is a clean no-op.
        assert_eq!(
            sweep_interrupted_jobs(&dir.path().join("nope.redb")).unwrap(),
            0
        );
    }

    /// A running job in the CURRENT process is never swept: the sweep runs
    /// once at startup, before the router serves, so any job submitted through
    /// this process's router postdates it by construction. Simulated here by
    /// sweeping first (the startup) and then registering a running job exactly
    /// the way `submit_job` does (registry + durable record) — it must still
    /// report `running`.
    #[tokio::test]
    async fn job_running_in_current_process_is_not_swept() {
        use rocky_core::state::StateStore;

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;

        // Startup: nothing persisted yet, nothing to sweep.
        assert_eq!(sweep_interrupted_jobs(&state_path).unwrap(), 0);

        // A live submission in THIS process, after the sweep.
        let state = ServerState::new(models_dir, None, None);
        let record = persisted_job("job_live", "running");
        state.jobs.upsert(record.clone()).await;
        StateStore::open(&state_path)
            .unwrap()
            .record_job(&record)
            .unwrap();

        let base = spawn_router(state).await;
        let resp = reqwest::get(format!("{base}/api/v1/jobs/job_live"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: JobStatus = resp.json().await.unwrap();
        assert!(
            matches!(body.state, JobState::Running),
            "a job submitted after startup must stay honestly `running`, got {:?}",
            body.state
        );
        assert!(body.error.is_none());
    }

    // --- Route-table ↔ router anti-drift probes (FIX for silent drift) ---

    /// Substitute the `{param}` placeholders in an `api_v1_routes()` entry
    /// with probe values so the declared route can actually be requested.
    fn probe_url(base: &str, path: &str) -> String {
        let path = path
            .replace("{name}", "probe_model")
            .replace("{column}", "probe_column")
            .replace("{id}", "probe_job");
        format!("{base}{path}")
    }

    /// Every route `api_v1_routes()` declares must actually be served by
    /// `router()` — the two tables are maintained by hand side by side, and
    /// this probe is the only executable link between them. A
    /// declared-but-unregistered path would answer with the fallback's
    /// distinctive `route_not_found` code, and an unregistered method with a
    /// `405`; any other answer (2xx, resource-level 404, 400, 503) proves the
    /// route + method are registered. The canary at the end asserts an
    /// UNdeclared path really does hit the fallback, so the discriminator
    /// cannot silently rot.
    #[tokio::test]
    async fn every_declared_route_is_registered_on_the_router() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        // An initialized (empty) state store so the state-backed reads answer.
        let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let state = ServerState::new(models_dir, None, None);
        state.recompile().await;
        let base = spawn_router(state).await;

        let client = reqwest::Client::new();
        for entry in api_v1_routes() {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
            // The malformed principal short-circuits the POST /jobs/* routes
            // with a 400 BEFORE any permit/persist/subprocess side effect —
            // the probe needs "the route answered", not a submitted job. GET
            // routes ignore the header.
            let resp = client
                .request(method, probe_url(&base, path))
                .header("X-Rocky-Principal", "bad/slash")
                .send()
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                405,
                "{entry}: declared method is not registered on the router"
            );
            if resp.status() == 404 {
                let body: ErrorEnvelope = resp.json().await.unwrap();
                assert_ne!(
                    body.code, "route_not_found",
                    "{entry}: declared path is not registered on the router"
                );
            }
        }

        // Canary: an undeclared path must hit the fallback discriminator.
        let resp = reqwest::get(format!("{base}/api/v1/definitely-not-a-route"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "route_not_found");
    }

    /// With a token configured, every declared route except the auth-exempt
    /// `GET /api/v1/health` must reject a token-less request with a `401` —
    /// this pins the requirement that routes are registered BEFORE the
    /// `.layer(require_bearer_token)` call in `router()` (a route appended
    /// after it would silently dodge auth). The fallback is wrapped too: an
    /// unknown path without a token is a `401`, never a route-existence
    /// oracle.
    #[tokio::test]
    async fn every_declared_route_is_auth_wrapped_except_health() {
        let base = spawn_router(test_state_with_token("s3cret")).await;
        let client = reqwest::Client::new();

        for entry in api_v1_routes() {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
            let resp = client
                .request(method, probe_url(&base, path))
                .send()
                .await
                .unwrap();
            if entry == "GET /api/v1/health" {
                assert_eq!(resp.status(), 200, "{entry}: health is auth-exempt");
            } else {
                assert_eq!(
                    resp.status(),
                    401,
                    "{entry}: must sit behind the auth layer"
                );
            }
        }

        let resp = reqwest::get(format!("{base}/api/v1/definitely-not-a-route"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "the 404 fallback sits behind auth too");
    }

    // --- Envelope fallbacks (unknown path / wrong method / bad path param) ---

    /// An unmatched path answers the enveloped `404 route_not_found`, not
    /// axum's default empty body — the documented "every error carries the
    /// envelope" contract.
    #[tokio::test]
    async fn unknown_path_returns_enveloped_404() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/no-such-route"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "route_not_found");
        assert!(body.message.contains("/api/v1/no-such-route"));
        assert!(body.remediation_hint.is_some());
    }

    /// A known path hit with an unsupported method answers the enveloped
    /// `405 method_not_allowed`, not axum's default empty-body 405.
    #[tokio::test]
    async fn wrong_method_returns_enveloped_405() {
        let base = spawn_router(test_state()).await;
        let client = reqwest::Client::new();
        // /api/v1/meta only serves GET.
        let resp = client
            .post(format!("{base}/api/v1/meta"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 405);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "method_not_allowed");
        assert!(body.message.contains("POST"));
    }

    /// A malformed path parameter (invalid percent-encoded UTF-8) answers the
    /// enveloped `400 bad_request` through the [`ApiPath`] wrapper, not
    /// axum's plain-text rejection.
    #[tokio::test]
    async fn invalid_path_param_returns_enveloped_400() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;
        // `%FF` percent-decodes to invalid UTF-8 → Path<String> rejection.
        let resp = reqwest::get(format!("{base}/api/v1/models/%FF"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "bad_request");
    }
}
