//! HTTP API routes for `rocky serve`.
//!
//! Exposes the compiler's semantic graph and the run/quality state store as
//! a REST API:
//! ```text
//! GET  /api/v1/health                          → liveness (auth-exempt)
//! GET  /api/v1/meta                             → engine + config fingerprint
//! GET  /api/v1/project                          → the project served (typed, no CLI twin)
//! GET  /api/v1/models                           → list all models (dashboard-only)
//! GET  /api/v1/models/:name                     → model details (dashboard-only)
//! GET  /api/v1/models/:name/lineage             → column-level lineage
//! GET  /api/v1/models/:name/lineage/:column     → trace single column
//! GET  /api/v1/models/:name/history             → per-model run history
//! GET  /api/v1/models/:name/metrics             → per-model quality snapshots
//! GET  /api/v1/runs                             → project run history
//! GET  /api/v1/products                         → every product (= rocky product list)
//! GET  /api/v1/products/:name                   → one product (= rocky product status)
//! GET  /api/v1/review/queue                     → pending escalations (= rocky review --queue)
//! GET  /api/v1/review/:plan_id/status           → one plan's sign-off (= rocky review --status)
//! GET  /api/v1/compile                          → full compile result
//! GET  /api/v1/dag                              → full unified DAG
//! GET  /api/v1/dag/layers                       → execution layers (typed, no CLI twin)
//! GET  /api/v1/dag/status                       → latest DAG run status (typed, no CLI twin)
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
//! `health`, `models`, `models/:name`, `dag/layers` and `dag/status` have
//! **no CLI counterpart** — no verb lists compiled models with their graph
//! edges, and the DAG status is populated by the in-process executor only.
//! They still serve typed payloads (`HealthOutput`, `ModelListOutput`,
//! `ModelDetailOutput`, `DagLayersOutput`, `DagStatusOutput`) exported
//! through the same schema registry as every CLI output, so the OpenAPI
//! document and the generated bindings describe them; `/meta` advertises
//! the `estate` capability for them. Only `POST /compile` is still an
//! ad-hoc server-lifecycle body.
//!
//! All routes except `/api/v1/health` require a Bearer token when one is
//! configured on [`ServerState`]; see [`rocky_server::auth`]. A token
//! configured with `--token-scope read-only` authenticates the same way but
//! is refused `403 forbidden_read_only_token` on every request whose HTTP
//! method is not safe (`GET`, `HEAD`, `OPTIONS`) — so every route below that
//! serves `POST` (or any method added later) is unreachable with one. Error
//! responses carry the [`ErrorEnvelope`] body (`{code, message,
//! remediation_hint}`), never an empty body — including the router-level
//! fallbacks (`404 route_not_found` for an unmatched path,
//! `405 method_not_allowed` for a known path hit with the wrong method), the
//! scope refusal above, and malformed path parameters (`400 bad_request`).
//! The one exception: a
//! request the HTTP stack rejects before it reaches the router (e.g.
//! malformed HTTP or an aborted body read) is answered below this layer and
//! carries no envelope.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use rocky_core::state::PersistedJob;
use rocky_server::auth::{build_cors_layer, require_bearer_token};
use rocky_server::state::ServerState;

use crate::commands::audit::{
    compute_audit, compute_audit_for, compute_audit_scorecard, parse_window, plan_file_path,
    resolve_product_scope,
};
use crate::commands::product::{
    ProductJournalOutput, ProductListOutput, ProductStatusOutput, product_journal_in,
    product_list_in, product_names_in, product_status_in,
};
use crate::commands::review::{
    ReviewMarkerState, compute_review_queue, compute_review_status, review_marker_state,
};
use crate::commands::{BriefSince, compute_brief};
use crate::commands::{
    PolicyShowMarkers, assemble_policy_show, load_policy_show_markers, policy_show_config,
    policy_show_remote_backend, policy_show_unconsulted_ledger, read_policy_show_ledger,
};
use crate::commands::{
    ScheduleStatusError, column_lineage_output, compile_output, dag_output, history_runs_output,
    lineage_output, metrics_output, model_history_output, schedule_status_output, schemas_hash,
};
use crate::output::{
    AuditForOutput, AuditOutput, AuditScorecardOutput, BriefOutput, PolicyRulesOutput,
    ProjectOutput, ReviewOutput, ReviewQueueOutput, ReviewStatusOutput, ScorecardDimension,
};
use crate::output::{
    ColumnLineageOutput, CompileOutput, DagExecutionOutput, DagLayersOutput, DagNodeResultOutput,
    DagNodeStatusOutput, DagOutput, DagStatusOutput, ErrorEnvelope, HealthOutput, HistoryOutput,
    JobKind, JobState, JobStatus, LineageOutput, MetaOutput, MetricsOutput, ModelColumnOutput,
    ModelDetailOutput, ModelHistoryOutput, ModelListEntry, ModelListOutput, ScheduleSpoolOutput,
    ScheduleStatusOutput, TypedColumnOutput, cap_model_sql,
};

/// Bind config for [`serve`].
///
/// Defaults bind to `127.0.0.1:8080` (loopback only) — the LAN-leak class
/// of bug doesn't reproduce unless the operator opts into a non-loopback
/// host *and* configures a Bearer token.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Bind host. Defaults to `127.0.0.1`. A non-loopback host (e.g.
    /// `0.0.0.0`) requires `ServerState::auth` to be `Some`.
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
///
/// A route registered with a non-safe method (`post`/`put`/`patch`/`delete`)
/// is additionally counted by `router_registers_no_undeclared_mutating_route`,
/// so it cannot land without appearing in [`api_v1_routes`] — which is what
/// the read-scope test enumerates. The read-scope *guarantee* does not depend
/// on either table: `rocky_server::auth` refuses on the HTTP method before
/// routing, so a new mutating route is covered with no edit there — provided
/// it is registered above the layer (rule 1) and its method is genuinely not
/// safe. A mutating `GET` would pass; see `rocky_server::auth::is_safe_method`
/// for the two stated limits.
pub fn router(state: Arc<ServerState>) -> Router {
    let cors = build_cors_layer(&state.allowed_origins);

    let api = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/meta", get(meta))
        .route("/api/v1/project", get(project))
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
        .route("/api/v1/schedule", get(schedule_status))
        .route("/api/v1/schedule/spool", get(schedule_spool))
        .route("/api/v1/policy", get(policy_show))
        .route("/api/v1/products", get(list_products))
        .route("/api/v1/products/{name}", get(get_product))
        .route("/api/v1/products/{name}/journal", get(product_journal))
        .route("/api/v1/review/queue", get(review_queue))
        .route("/api/v1/review/{plan_id}", get(review_diff))
        .route("/api/v1/review/{plan_id}/status", get(review_status))
        .route("/api/v1/models/{name}/rows", get(model_rows))
        .route("/api/v1/brief", get(governor_brief))
        .route("/api/v1/audit", get(audit_ledger))
        .route("/api/v1/audit/scorecard", get(audit_scorecard))
        .route("/api/v1/custody/{subject}", get(custody_chain))
        // Webhook ingress. Registered BEFORE the auth layer like every route, but
        // the middleware PREFIX-exempts `/api/v1/hooks/trigger/{pipeline}` from
        // the Bearer token (see `rocky_server::auth`): the handler authenticates
        // with its own `X-Rocky-Signature` HMAC instead. The exemption is scoped
        // to a single segment so it can never widen to another route.
        .route("/api/v1/hooks/trigger/{pipeline}", post(webhook_trigger))
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
        .with_state(state.clone());

    // The browser UI's files are public (they carry no data, and the page
    // must load before it has a token to send), so they sit OUTSIDE the
    // bearer layer above. They exist only with `--ui`; without it there is
    // no `/ui` path, and the API fallback answers as before.
    let app = if state.ui.is_some() {
        api.merge(crate::ui::ui_router(state.clone()))
    } else {
        api
    };

    app
        // The `Host`/`Origin` guard is outermost so it precedes routing for
        // every request, UI files included. It is a no-op without `--ui`.
        .layer(middleware::from_fn_with_state(
            state,
            rocky_server::auth::require_known_host,
        ))
        // A body over the limit is refused by the extractors with a bare
        // `413`; this rewrites it into the envelope. Every mode, every route.
        .layer(middleware::map_response(
            crate::ui::envelope_payload_too_large,
        ))
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::ui::MAX_REQUEST_BODY_BYTES,
        ))
        // OUTERMOST, and it must stay last: on the response path a layer runs
        // after everything applied before it, so this sees the 413 envelope,
        // the 421 host refusal, the 401, the 404/405 fallbacks and every
        // handler that builds its own Response — including `trigger_compile`,
        // which hand-builds a body carrying `config_error`. A filter installed
        // at a responder type would miss those while looking correct (#1897).
        .layer(middleware::from_fn(
            crate::secret_filter::redact_response_secrets,
        ))
}

/// Start the HTTP server.
///
/// Before the router serves, persisted jobs left IN FLIGHT — `running` or
/// `queued` — by a previous sidecar process are swept to `failed` (see
/// [`sweep_interrupted_jobs`]).
///
/// A record in any other non-terminal state is deliberately left alone. It was
/// written by a version this one does not know, and rewriting it would destroy
/// a payload this version cannot reproduce. Nothing is stranded either way:
/// `job_status_from` renders an unrecognized state as terminal `failed`, so a
/// poller on `GET /api/v1/jobs/{id}` sees a terminal state whether or not the
/// sweep touched the record.
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
    ready: rocky_core::schedule::Drain,
) -> anyhow::Result<()> {
    if !is_loopback(&config.host) && state.auth.is_none() {
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
    // Readiness: the startup job sweep has completed and the listener is bound, so
    // it is now safe for the resident scheduler (if any) to take its first tick —
    // any job it submits from here on is past the sweep and cannot be clobbered,
    // and no scheduled child runs before the server is actually up. A bind failure
    // returns above via `?` without raising this, so the scheduler — which awaits
    // readiness against shutdown — exits without ticking.
    ready.signal();
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
pub(crate) fn is_loopback(host: &str) -> bool {
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
    /// Seconds for a `Retry-After` header, on the refusals where the caller
    /// should come back rather than give up. `None` on every other error, so
    /// the header appears only where it means something.
    retry_after_seconds: Option<u32>,
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
            retry_after_seconds: None,
        }
    }

    /// Attach a `Retry-After`, in seconds, to a refusal the caller should
    /// retry.
    fn retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
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
    /// `500` — a `rocky.toml` the engine could not read or parse. Deliberately
    /// not the retryable `503`: retrying will not make an invalid config parse.
    fn config_invalid(message: &str) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "config_invalid",
            message,
            Some("fix the config and retry; `rocky validate` reports the specific error"),
        )
    }

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

    /// `404` — no spec file and no state record under this product name.
    fn product_not_found(name: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "product_not_found",
            format!("product '{name}' not found"),
            Some("list the products this project knows at GET /api/v1/products"),
        )
    }

    /// `404` — no persisted plan file under this id.
    fn plan_not_found(plan_id: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "plan_not_found",
            format!("plan '{plan_id}' not found"),
            Some("list the pending plans at GET /api/v1/review/queue"),
        )
    }

    /// `409` — a review marker exists for the plan but is malformed, or
    /// names another plan. An integrity fault of custody files, not a server
    /// fault; the CLI refuses the same way.
    fn review_marker_malformed(plan_id: &str, reason: &str) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "review_marker_malformed",
            format!("review marker for plan '{plan_id}' is invalid: {reason}"),
            Some("re-approve with `rocky review <plan-id> --approve` to rewrite it atomically"),
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
        // The resident scheduler holds the permit for a whole tick under a
        // SENTINEL id, not a job id — its runs are recorded per-demand under
        // generated submission ids, so `GET /api/v1/jobs/scheduler` would 404.
        // Hand those clients a truthful message and NO pollable id rather than
        // pointing them at a route that cannot resolve.
        if running_job_id == crate::commands::scheduler::SCHEDULER_PERMIT_HOLDER {
            return Self::new(
                StatusCode::CONFLICT,
                "mutation_in_progress",
                "a scheduled run is in progress on this project",
                Some("wait for the scheduler tick to finish, then resubmit"),
            );
        }
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

    /// `409` — `products/<name>.toml` exists but the spec loader rejects it,
    /// so the product's output model cannot be resolved. An integrity fault of
    /// a custody file, like a malformed review marker, not a server fault.
    fn product_spec_invalid(name: &str, code: &str, reason: &str) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "product_spec_invalid",
            format!("product '{name}' has a spec the loader rejects ({code}): {reason}"),
            Some("fix products/<name>.toml until `rocky product verify <name>` accepts it"),
        )
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

    /// `404` — webhook ingress is not available: either `serve` is running
    /// without `--scheduler` (nothing would consume a demand), or no
    /// `ROCKY_WEBHOOK_SECRET` is configured on a non-loopback bind (fail-closed).
    /// A distinct code from `route_not_found` so the route-registration probe
    /// still recognizes the route as served, and so callers do not treat it as a
    /// generic missing route.
    fn webhook_disabled() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "webhook_disabled",
            "webhook ingress is not enabled on this server",
            Some(
                "run `rocky serve --scheduler` and set ROCKY_WEBHOOK_SECRET \
                 (required unless bound to loopback)",
            ),
        )
    }

    /// `401` — the `X-Rocky-Signature` HMAC did not verify (missing, malformed,
    /// or wrong). Distinct code from the Bearer middleware's `unauthorized` so a
    /// caller — and the auth-wrapping probe — can tell the webhook's own HMAC
    /// rejection from a Bearer-token rejection.
    fn webhook_bad_signature() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "invalid_signature",
            "the X-Rocky-Signature HMAC did not verify",
            Some("sign the raw request body: HMAC-SHA256 hex, header `X-Rocky-Signature`"),
        )
    }

    /// `404` — the target pipeline is not defined in the resolved config.
    /// Returned only after the HMAC verifies, so an unauthenticated caller
    /// cannot use it to enumerate pipeline names.
    fn pipeline_not_found(name: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "pipeline_not_found",
            format!("pipeline '{name}' is not defined in the project config"),
            Some("check the pipeline name against the project's rocky.toml"),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = self.retry_after_seconds;
        let mut response = (self.status, PrettyJson(self.envelope)).into_response();
        if let Some(seconds) = retry_after
            && let Ok(value) = HeaderValue::from_str(&seconds.to_string())
        {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        response
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
/// Map a schedule-status failure to its honest status.
///
/// A `rocky.toml` the engine cannot read is **not** a transient busy condition,
/// so it must not borrow the retryable `503` — a client would back off and
/// retry forever against a config that will never parse on its own. Nor is it a
/// `200` with an empty pipeline list: that asserts "nothing is scheduled" when
/// the truth is "we cannot tell what is scheduled". State-store failures keep
/// the existing mapping, including `503` on flock contention.
fn map_schedule_err(err: ScheduleStatusError, running_job_id: Option<String>) -> ApiError {
    match err {
        ScheduleStatusError::ConfigInvalid(message) => ApiError::config_invalid(&message),
        ScheduleStatusError::State(e) => map_state_err(e, running_job_id),
    }
}

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

/// Run a request-local read of the state store off the async runtime, one at
/// a time per process.
///
/// Every state-backed read opens the store for the request and drops it with
/// the response, and redb takes an exclusive `flock` on the file for the life
/// of a handle. Two requests opening at once therefore race: the loser polls
/// the lock five times, 50 ms apart (`StateStore::open_redb_with_retry`), and
/// answers `503 engine_busy` when it loses every poll. Two browser tabs
/// refreshing together manage that routinely — measured with
/// `scripts/serve-ceiling.py`: 76 of 1,982 requests at two clients, none at
/// one — so the ceiling of one process was one viewer. A poll cannot tell "a
/// `rocky run` holds the store" from "the request next to me is reading it";
/// this process can. Its own reads queue on one permit and never contend with
/// each other, so the only `503 engine_busy` left is the one that status is
/// for: another process holding the store.
///
/// The permit is taken inside the blocking task, not in the handler: a client
/// that disconnects drops the handler's future, not the running read, and the
/// next read must not start until this one has closed the store.
async fn store_read<T, F>(state: &ServerState, read: F) -> Result<T, ApiError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let permit = Arc::clone(&state.store_access)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("the state-store read queue is closed".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let _held = permit;
        read()
    })
    .await
    .map_err(|e| map_join_err(&e))
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

/// Query-string extractor whose rejection is the error envelope (`400
/// bad_request`) rather than axum's plain-text body, so a malformed query
/// string fails the same way a malformed path parameter does.
struct ApiQuery<T>(T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Query(value)) => Ok(Self(value)),
            Err(rejection) => Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                rejection.body_text(),
                None,
            )),
        }
    }
}

/// Map a blocking-task join failure (panic / cancel) onto a `500`.
fn map_join_err(err: &tokio::task::JoinError) -> ApiError {
    ApiError::internal(format!("blocking task failed: {err}"))
}

/// Resolve the state-store path for a read route the same way the CLI does.
pub(crate) fn state_path_for(state: &ServerState) -> std::path::PathBuf {
    // An explicit `--state-path` is a hard override — honor it verbatim. Without
    // this, a `rocky --state-path <p> serve --scheduler` resolved the default
    // instead, so the scheduler's cursors, claims, and child run history landed
    // in a different file than the one the operator selected (and than a
    // `rocky tick` on the same project would use), silently re-firing occurrences.
    match &state.state_path {
        Some(explicit) => explicit.clone(),
        None => rocky_core::state::resolve_state_path(None, &state.models_dir).path,
    }
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
        "GET /api/v1/project",
        "GET /api/v1/models",
        "GET /api/v1/models/{name}",
        "GET /api/v1/models/{name}/lineage",
        "GET /api/v1/models/{name}/lineage/{column}",
        "GET /api/v1/models/{name}/history",
        "GET /api/v1/models/{name}/metrics",
        "GET /api/v1/models/{name}/rows",
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
        "GET /api/v1/schedule",
        "GET /api/v1/schedule/spool",
        "GET /api/v1/policy",
        "GET /api/v1/products",
        "GET /api/v1/products/{name}",
        "GET /api/v1/products/{name}/journal",
        "GET /api/v1/review/queue",
        "GET /api/v1/review/{plan_id}",
        "GET /api/v1/review/{plan_id}/status",
        "GET /api/v1/brief",
        "GET /api/v1/audit",
        "GET /api/v1/audit/scorecard",
        "GET /api/v1/custody/{subject}",
        "POST /api/v1/hooks/trigger/{pipeline}",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Coarse capability tokens so embedders feature-detect a build's surface
/// without version-sniffing. The canonical read routes carry the same
/// `*Output` shapes the CLI `--output json` emits. `estate` says the five
/// server-only routes (`/health`, `/models`, `/models/{name}`, `/dag/layers`,
/// `/dag/status`) answer with their typed, schema-exported payloads.
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
        "schedule",
        "policy",
        "webhooks",
        "estate",
        "products",
        "review",
        "governor",
        "audit",
        "journal",
        "review_diff",
        "samples",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

// --- Route handlers ---

/// `GET /api/v1/health` — auth-exempt liveness probe, typed [`HealthOutput`].
async fn health() -> PrettyJson<HealthOutput> {
    PrettyJson(HealthOutput {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// `GET /api/v1/project` — canonical [`ProjectOutput`]: the project this
/// sidecar serves, as the retired dashboard at `/` used to show it.
///
/// Config-derived fields come from the bound `rocky.toml` (empty lists and
/// `config_error` when it does not load; `config_path: null` when none is
/// bound), the counts from the in-memory compile result, and `last_run`
/// from the state store the server resolved, namespace included. Bounded by
/// construction: no model names, one run. Reads only.
///
/// A compile that produced no result is its own state (#1823):
/// `compile_error` carries the reason, `models_compiled` is absent, and
/// `diagnostics.has_errors` is `true`. Before this, a background compile
/// that failed was logged and the route read the absent result as a clean
/// project — no diagnostics, `has_errors: false` — which is what the SPA
/// then showed for a project whose models could not be read at all.
async fn project(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ProjectOutput>, ApiError> {
    let (name, config_path, pipelines, adapters, config_error) = match state.config_path.as_deref()
    {
        None => ("rocky".to_string(), None, Vec::new(), Vec::new(), None),
        Some(path) => {
            let name = path
                .parent()
                .and_then(|dir| dir.file_name())
                .map(|dir| dir.to_string_lossy().to_string())
                .unwrap_or_else(|| "rocky".to_string());
            let shown = Some(path.display().to_string());
            match rocky_core::config::load_rocky_config(path) {
                Ok(config) => {
                    let pipelines = config
                        .pipelines
                        .iter()
                        .map(|(pipeline, cfg)| crate::output::ProjectPipelineOutput {
                            name: pipeline.clone(),
                            pipeline_type: pipeline_type_label(cfg).to_string(),
                        })
                        .collect();
                    let adapters = config
                        .adapters
                        .iter()
                        .map(|(adapter, cfg)| crate::output::ProjectAdapterOutput {
                            name: adapter.clone(),
                            adapter_type: cfg.adapter_type.clone(),
                        })
                        .collect();
                    (name, shown, pipelines, adapters, None)
                }
                Err(e) => (name, shown, Vec::new(), Vec::new(), Some(format!("{e:#}"))),
            }
        }
    };

    // The failure guard is held across the result read, in the writers'
    // order (failure, then result), so this route sees the pair a single
    // recompile published — never an earlier failure beside a newer result.
    let failure_guard = state.compile_failure.read().await;
    let compile_error = failure_guard.clone();
    let (models_compiled, diagnostics) = if compile_error.is_some() {
        // The last compile produced no result, and `publish_failure` dropped
        // the previous one with it, so there are no counts to show. Zero
        // counts, and `has_errors` true: this is not a clean project.
        (
            None,
            crate::output::ProjectDiagnosticsOutput {
                total: 0,
                warnings: 0,
                has_errors: true,
            },
        )
    } else {
        let lock = state.compile_result.read().await;
        match lock.as_ref() {
            Some(result) => (
                Some(result.semantic_graph.models.len() as u64),
                crate::output::ProjectDiagnosticsOutput {
                    total: result.diagnostics.len() as u64,
                    warnings: result
                        .diagnostics
                        .iter()
                        .filter(|d| d.severity == rocky_compiler::diagnostic::Severity::Warning)
                        .count() as u64,
                    has_errors: result.has_errors,
                },
            ),
            None => (
                None,
                crate::output::ProjectDiagnosticsOutput {
                    total: 0,
                    warnings: 0,
                    has_errors: false,
                },
            ),
        }
    };
    drop(failure_guard);

    let state_path = state_path_for(&state);
    let last_run =
        store_read(
            &state,
            move || -> anyhow::Result<Option<crate::output::ProjectRunOutput>> {
                if !state_path.exists() {
                    return Ok(None);
                }
                let store = rocky_core::state::StateStore::open_read_only(&state_path)?;
                Ok(store.list_runs(1)?.into_iter().next().map(|run| {
                    crate::output::ProjectRunOutput {
                        run_id: run.run_id,
                        started_at: run.started_at.to_rfc3339(),
                        finished_at: run.finished_at.to_rfc3339(),
                        status: format!("{:?}", run.status),
                        models_executed: run.models_executed.len() as u64,
                        trigger: format!("{:?}", run.trigger),
                    }
                }))
            },
        )
        .await?
        .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;

    Ok(PrettyJson(ProjectOutput {
        name,
        config_path,
        config_error,
        pipelines,
        adapters,
        compile_error,
        models_compiled,
        diagnostics,
        last_run,
    }))
}

/// A pipeline's kind by its config variant, without exposing the enum.
fn pipeline_type_label(cfg: &rocky_core::config::PipelineConfig) -> &'static str {
    match cfg {
        rocky_core::config::PipelineConfig::Replication(_) => "replication",
        rocky_core::config::PipelineConfig::Transformation(_) => "transformation",
        rocky_core::config::PipelineConfig::Quality(_) => "quality",
        rocky_core::config::PipelineConfig::Snapshot(_) => "snapshot",
        rocky_core::config::PipelineConfig::Load(_) => "load",
    }
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

/// `GET /api/v1/models` — the compiled model list, typed [`ModelListOutput`].
///
/// No CLI counterpart: no verb lists compiled models with their graph edges.
/// Sorted by model name; bounded by the project's model count.
async fn list_models(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ModelListOutput>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    let mut models: Vec<ModelListEntry> = result
        .semantic_graph
        .models
        .iter()
        .map(|(name, schema)| ModelListEntry {
            name: name.clone(),
            columns: schema.columns.len(),
            has_star: schema.has_star,
            upstream: schema.upstream.clone(),
            downstream: schema.downstream.clone(),
        })
        .collect();
    // The graph iterates in topological order; the list contract is by name.
    models.sort_by(|a, b| a.name.cmp(&b.name));

    let count = models.len();
    Ok(PrettyJson(ModelListOutput { models, count }))
}

/// `GET /api/v1/models/:name` — one model's detail, typed [`ModelDetailOutput`].
///
/// No CLI counterpart. The SQL text is capped at
/// [`crate::output::MODEL_DETAIL_SQL_CAP_BYTES`]; a cut is reported, never
/// silent.
async fn get_model(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
) -> Result<PrettyJson<ModelDetailOutput>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    let schema = result
        .semantic_graph
        .model_schema(&name)
        .ok_or_else(|| ApiError::model_not_found(&name))?;

    let typed_columns = result.type_check.typed_models.get(&name).map(|cols| {
        cols.iter()
            .map(TypedColumnOutput::from_typed_column)
            .collect()
    });

    let model = result
        .project
        .model(&name)
        .ok_or_else(|| ApiError::model_not_found(&name))?;

    let (sql, sql_truncated) = cap_model_sql(&model.sql);

    Ok(PrettyJson(ModelDetailOutput {
        name,
        sql,
        sql_truncated,
        sql_bytes: model.sql.len(),
        file_path: model.file_path.display().to_string(),
        columns: schema
            .columns
            .iter()
            .map(|c| ModelColumnOutput {
                name: c.name.clone(),
            })
            .collect(),
        typed_columns,
        has_star: schema.has_star,
        upstream: schema.upstream.clone(),
        downstream: schema.downstream.clone(),
    }))
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

    let output = store_read(&state, move || {
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
    .await?
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(PrettyJson(output))
}

/// `POST /api/v1/compile` — recompile in place.
///
/// Reports `status: "recompiled"` when the compile produced a result and the
/// project config loaded or is absent; `status: "recompiled_degraded"` with
/// `config_error` when a `rocky.toml` is present and could not be read; and
/// `status: "compile_failed"` with `compile_error` when the compile produced
/// no result at all (#1823) — with `config_error` beside it when both hold.
///
/// It used to answer `"recompiled"` unconditionally, so an SDK caller could
/// not distinguish a project that declares no masks and no freshness from
/// one whose config failed to parse — the compile silently ran with empty
/// project inputs and the route still said success (#1625). And it still
/// said `"recompiled"` when the compile itself failed: `recompile` returns
/// only the config's reason, and a failed compile was logged and dropped,
/// so the caller that asked for a compile was told it had one (#1823).
/// `serve` still compiles rather than refusing (a resident server must not
/// go dark mid-edit), but it no longer calls either outcome the same thing.
async fn trigger_compile(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    // This invocation's own outcome, not a re-read of the shared fields: a
    // concurrent recompile (the watcher, another request) could otherwise
    // publish its result between this compile and the read, and the caller
    // would be told the other compile's outcome.
    let rocky_server::state::RecompileOutcome {
        config_error,
        compile_error,
    } = state.recompile().await;
    let mut body = serde_json::Map::new();
    let status = match (&compile_error, &config_error) {
        (Some(_), _) => "compile_failed",
        (None, Some(_)) => "recompiled_degraded",
        (None, None) => "recompiled",
    };
    body.insert("status".into(), serde_json::Value::String(status.into()));
    if let Some(reason) = compile_error {
        body.insert("compile_error".into(), serde_json::Value::String(reason));
    }
    if let Some(reason) = config_error {
        body.insert("config_error".into(), serde_json::Value::String(reason));
    }
    (StatusCode::OK, Json(serde_json::Value::Object(body)))
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
    // Only a models dir the operator actually named is a whole-project
    // override, the API analogue of `rocky dag --models`. `serve` without
    // `--models` falls back to `models` internally, and passing that default on
    // as an override replaced every transformation pipeline's own directory —
    // reproducing #1261 over HTTP on projects `rocky dag` handled correctly.
    let models_dir = state
        .models_dir_is_explicit
        .then(|| state.models_dir.clone());
    let contracts_dir = state.contracts_dir.clone();
    let state_path = state_path_for(&state);

    let output = store_read(&state, move || {
        dag_output(
            &config_path,
            &state_path,
            models_dir.as_deref(),
            None, // seeds_dir → falls back to <config_dir>/seeds, matching `rocky dag`
            contracts_dir.as_deref(),
            false, // include_column_lineage
            None,  // cache_ttl_override
        )
    })
    .await?
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
    let output = store_read(&state, move || {
        history_runs_output(&state_path, None, false)
    })
    .await?
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
    let output = store_read(&state, move || {
        // `window` (20) is the CLI default; it is unused when rolling_stats
        // is false, so the output matches `rocky history --model <name>`.
        model_history_output(&state_path, &name, None, false, 20)
    })
    .await?
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
    let output = store_read(&state, move || {
        metrics_output(&state_path, &name, false, None, false)
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/dag/layers` — the execution layers, typed [`DagLayersOutput`].
///
/// No CLI counterpart. Bounded by the project's model count.
async fn dag_layers(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<DagLayersOutput>, ApiError> {
    let lock = state.compile_result.read().await;
    let result = lock.as_ref().ok_or_else(ApiError::engine_not_ready)?;

    Ok(PrettyJson(DagLayersOutput {
        layers: result.project.layers.clone(),
        total_models: result.project.model_count(),
    }))
}

/// `GET /api/v1/dag/status` — latest DAG execution status, typed
/// [`DagStatusOutput`].
///
/// Returns `503 engine_not_ready` when no DAG run has been recorded yet.
/// Projects the [`DagStatus`][rocky_core::dag_status::DagStatus] the
/// in-process executor records; there is no CLI counterpart.
async fn dag_status(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<DagStatusOutput>, ApiError> {
    match state.dag_status.get().await {
        Some(status) => Ok(PrettyJson(dag_status_output(&status))),
        None => Err(ApiError::engine_not_ready()),
    }
}

/// Project the executor's [`DagStatus`][rocky_core::dag_status::DagStatus]
/// onto the served [`DagStatusOutput`], field for field.
fn dag_status_output(status: &rocky_core::dag_status::DagStatus) -> DagStatusOutput {
    use rocky_core::dag_executor::NodeStatus;

    let result = &status.result;
    DagStatusOutput {
        completed_at: status.completed_at,
        result: DagExecutionOutput {
            nodes: result
                .nodes
                .iter()
                .map(|n| DagNodeResultOutput {
                    id: n.id.clone(),
                    kind: n.kind.clone(),
                    label: n.label.clone(),
                    // Exhaustive on purpose: a new executor status must be
                    // given a served rendering here, not fall through.
                    status: match n.status {
                        NodeStatus::Pending => DagNodeStatusOutput::Pending,
                        NodeStatus::Running => DagNodeStatusOutput::Running,
                        NodeStatus::Completed => DagNodeStatusOutput::Completed,
                        NodeStatus::Failed => DagNodeStatusOutput::Failed,
                        NodeStatus::Skipped => DagNodeStatusOutput::Skipped,
                    },
                    layer: n.layer,
                    duration_ms: n.duration_ms,
                    error: n.error.clone(),
                })
                .collect(),
            total_layers: result.total_layers,
            total_nodes: result.total_nodes,
            completed: result.completed,
            failed: result.failed,
            skipped: result.skipped,
            duration_ms: result.duration_ms,
        },
    }
}

/// The project root the product routes read `products/` under: the directory
/// of the bound `rocky.toml`. `503 engine_not_ready` when no config is bound —
/// a models-only sidecar has no product surface.
fn project_root_for(state: &ServerState) -> Result<std::path::PathBuf, ApiError> {
    let config = state
        .config_path
        .as_deref()
        .ok_or_else(ApiError::engine_not_ready)?;
    Ok(match config.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    })
}

/// `GET /api/v1/products` — canonical [`ProductListOutput`].
///
/// The same bytes as `rocky product list --output json` for the project the
/// bound config names. Reads `products/` and the state store; never writes.
async fn list_products(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ProductListOutput>, ApiError> {
    let root = project_root_for(&state)?;
    let state_path = state_path_for(&state);
    let output = store_read(&state, move || product_list_in(&root, Some(&state_path)))
        .await?
        .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/products/:name` — canonical [`ProductStatusOutput`].
///
/// The same bytes as `rocky product status <name> --output json`. A name
/// with neither a spec file nor a state record is `404 product_not_found`;
/// a spec that exists but does not parse is `200` with `spec_error` set,
/// exactly as the CLI reports it.
async fn get_product(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
) -> Result<PrettyJson<ProductStatusOutput>, ApiError> {
    // A name is only ever looked up in the computed product set before any
    // path is built from it, so a traversal cannot reach the filesystem.
    // This guard refuses anything that is not a bare identifier before
    // opening anything: a spec's own `product.name` must be one to exist.
    if !is_bare_product_name(&name) {
        return Err(ApiError::product_not_found(&name));
    }
    let root = project_root_for(&state)?;
    let state_path = state_path_for(&state);
    let lookup = name.clone();
    let output = store_read(&state, move || -> anyhow::Result<Option<_>> {
        let known = product_names_in(&root, Some(&state_path))?;
        if !known.iter().any(|known| known == &lookup) {
            return Ok(None);
        }
        product_status_in(&root, Some(&state_path), &lookup).map(Some)
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    match output {
        Some(status) => Ok(PrettyJson(status)),
        None => Err(ApiError::product_not_found(&name)),
    }
}

/// `GET /api/v1/products/{name}/journal` — canonical [`ProductJournalOutput`].
///
/// The same bytes as `rocky product journal <name> --output json`: the
/// product's fulfillment journal rows in append order, read through the
/// store function the loop reads through. A name that is not a bare
/// identifier, or that neither a spec file nor a store record knows, is
/// `404 product_not_found`. A known product with no rows is an empty
/// journal, not a refusal.
async fn product_journal(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
) -> Result<PrettyJson<ProductJournalOutput>, ApiError> {
    if !is_bare_product_name(&name) {
        return Err(ApiError::product_not_found(&name));
    }
    let root = project_root_for(&state)?;
    let state_path = state_path_for(&state);
    let lookup = name.clone();
    let output = store_read(&state, move || {
        product_journal_in(&root, Some(&state_path), &lookup)
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    match output {
        Some(journal) => Ok(PrettyJson(journal)),
        None => Err(ApiError::product_not_found(&name)),
    }
}

/// `GET /api/v1/review/queue` — canonical [`ReviewQueueOutput`].
///
/// The same bytes as `rocky review --queue --output json` for the project the
/// bound config names, modulo the clock: `staleness_seconds` and the `score`
/// it feeds derive from the request instant. Compiles the project once per
/// call to rank by blast radius, exactly as the CLI does. Reads only.
async fn review_queue(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ReviewQueueOutput>, ApiError> {
    let root = project_root_for(&state)?;
    let config = state
        .config_path
        .clone()
        .ok_or_else(ApiError::engine_not_ready)?;
    let state_path = state_path_for(&state);
    let models_dir = state.models_dir.clone();
    let output = store_read(&state, move || {
        compute_review_queue(&root, &config, &state_path, &models_dir)
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/review/:plan_id/status` — canonical [`ReviewStatusOutput`].
///
/// The same bytes as `rocky review <plan-id> --status --output json`. A plan
/// id that is not 64 lower-case hex characters, or has no plan file, is
/// `404 plan_not_found`. A marker that exists but is malformed or names
/// another plan is `409 review_marker_malformed`, with the CLI's own reason.
async fn review_status(
    State(state): State<Arc<ServerState>>,
    ApiPath(plan_id): ApiPath<String>,
) -> Result<PrettyJson<ReviewStatusOutput>, ApiError> {
    if !is_plan_id(&plan_id) {
        return Err(ApiError::plan_not_found(&plan_id));
    }
    let root = project_root_for(&state)?;
    let lookup = plan_id.clone();
    let output = store_read(&state, move || -> anyhow::Result<ReviewStatusLookup> {
        if !plan_file_path(&root, &lookup).is_file() {
            return Ok(ReviewStatusLookup::NoPlan);
        }
        if let ReviewMarkerState::Invalid { reason } = review_marker_state(&root, &lookup) {
            return Ok(ReviewStatusLookup::MalformedMarker { reason });
        }
        compute_review_status(&root, &lookup)
            .map(|status| ReviewStatusLookup::Found(Box::new(status)))
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    match output {
        ReviewStatusLookup::Found(status) => Ok(PrettyJson(*status)),
        ReviewStatusLookup::NoPlan => Err(ApiError::plan_not_found(&plan_id)),
        ReviewStatusLookup::MalformedMarker { reason } => {
            Err(ApiError::review_marker_malformed(&plan_id, &reason))
        }
    }
}

/// The three ways a status lookup can end, decided on the blocking side so
/// the handler maps each to its documented status code.
enum ReviewStatusLookup {
    /// Boxed: the payload dwarfs the two refusal arms.
    Found(Box<ReviewStatusOutput>),
    NoPlan,
    MalformedMarker {
        reason: String,
    },
}

/// A plan id is the 64 lower-case hex characters of a blake3 digest. Anything
/// else names no plan file, so the route answers 404 without touching disk.
fn is_plan_id(candidate: &str) -> bool {
    candidate.len() == 64
        && candidate
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// --- The live-compute routes: the review diff and the samples endpoint ---

/// How long `GET /api/v1/review/{plan_id}` waits for the diff permit before
/// refusing. A diff is local and ends in hundreds of milliseconds, so a short
/// wait beats a refusal the caller would only retry into.
const REVIEW_DIFF_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Largest `limit` the samples route accepts. The CLI takes any `u32`; a
/// browser-reachable route does not, because the rows cross the wire and sit
/// in a page.
const MAX_SAMPLE_LIMIT: u32 = 500;

/// Default `limit` when the caller names none — the CLI's own default.
const DEFAULT_SAMPLE_LIMIT: u32 = 20;

/// The per-request consent header for a warehouse-executing read.
///
/// A header, never a query parameter. The danger of a money-spending `GET` is
/// that a `GET` is issued by things that are not the user: a browser prefetch,
/// a link scanner, an `<img src>`, a restored tab. None of those can set a
/// custom request header, and cross-origin script cannot either without a
/// preflight this server does not grant. The header is what makes `GET` the
/// right method here, not a ritual on top of it.
const CONSENT_HEADER: &str = "x-rocky-allow-warehouse";

/// Query string of `GET /api/v1/models/{name}/rows`.
#[derive(Debug, Deserialize)]
struct SampleQuery {
    /// Rows to return, `1..=500`. Defaults to 20, the CLI's default.
    limit: Option<u32>,
    /// Preview one named CTE of the model instead of its output.
    cte: Option<String>,
    /// Which pipeline's adapter to run against; required only when the project
    /// declares more than one.
    pipeline: Option<String>,
}

/// `GET /api/v1/review/{plan_id}` — the review diff, canonical [`ReviewOutput`].
///
/// The same bytes as `rocky review <plan-id> --output json`: the plan's kind,
/// the breaking-change findings against `HEAD`, and `approved: false`. It never
/// writes the review marker — approving stays in the terminal, and `approve` is
/// hard-coded `false` here rather than exposed.
///
/// The base is `HEAD` and is not a parameter. The CLI's `--base <ref>` reaches
/// `git` as a subprocess argument, and refusing a ref from a query string is
/// cheaper than validating one. A plan records no base of its own
/// (`PersistedPlan` carries no such field), so there is nothing else this could
/// resolve to today.
///
/// Refusals: an id that is not 64 lower-case hex, or that names no plan file,
/// is `404 plan_not_found` before anything is read — which also keeps the
/// `404` path from creating `.rocky/plans/`, as `read_plan` would on a miss. A
/// plan the CLI refuses to review is `409 plan_not_reviewable`.
async fn review_diff(
    State(state): State<Arc<ServerState>>,
    ApiPath(plan_id): ApiPath<String>,
) -> Result<PrettyJson<ReviewOutput>, ApiError> {
    if !is_plan_id(&plan_id) {
        return Err(ApiError::plan_not_found(&plan_id));
    }
    let root = project_root_for(&state)?;
    let config = state
        .config_path
        .clone()
        .ok_or_else(ApiError::engine_not_ready)?;
    if !plan_file_path(&root, &plan_id).is_file() {
        return Err(ApiError::plan_not_found(&plan_id));
    }

    // Whether the plan is review-gated at all, asked of the same predicate the
    // CLI's own guard asks — never read out of its error message, which would
    // drift the moment the wording changed.
    let plan = crate::plan_store::read_plan(&root, &plan_id)
        .map_err(|e| ApiError::internal(format!("{e:#}")))?;
    if !crate::commands::plan_is_reviewable(&plan) {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "plan_not_reviewable",
            format!(
                "plan '{plan_id}' is a {} plan; review applies to AI-authored plans, \
                 agent-authored run plans, backfills, and gc / restore / compact / archive plans",
                plan.kind
            ),
            Some("a human-authored run plan is not review-gated, so there is no sign-off to take"),
        ));
    }

    // Admission: one diff at a time, with a short wait. A caller that waits out
    // the window is refused with `Retry-After` rather than queueing further.
    let _permit = match tokio::time::timeout(
        REVIEW_DIFF_WAIT,
        Arc::clone(&state.review_diffs).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        _ => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "engine_busy",
                "another review diff is in flight",
                Some("retry in a moment; one diff runs at a time"),
            )
            .retry_after(2));
        }
    };

    // The store gate, for the state read `compute_review` does internally, so
    // this compile does not race the queue's. Held across the compile, the same
    // trade `GET /api/v1/review/queue` already makes.
    let _store = Arc::clone(&state.store_access)
        .acquire_owned()
        .await
        .map_err(|_| ApiError::internal("the state-store read queue is closed".to_string()))?;

    // `compute_review` is `async` for its marker-writing path, which
    // `approve = false` never enters; its compiles are synchronous work on this
    // thread, which the permit above bounds to one worker at a time.
    match crate::commands::compute_review(&root, &config, &plan_id, "HEAD", false).await {
        Ok(output) => Ok(PrettyJson(output)),
        // The reviewability refusal is already handled above, so anything left
        // here is a genuine failure of this server.
        Err(e) => Err(ApiError::internal(format!("{e:#}"))),
    }
}

/// `GET /api/v1/models/{name}/rows` — a bounded, masked sample, canonical
/// [`PreviewRowsOutput`].
///
/// The same bytes as `rocky preview rows <model> --limit <n> [--cte <c>]
/// [--pipeline <p>] --output json`, through the same `compute_preview_rows`
/// core, so the gate, the masking refusal and the executed SQL cannot differ
/// between the two callers.
///
/// Three bounds a browser-reachable route needs and the CLI does not:
///
/// * **Consent.** A remote adapter needs [`CONSENT_HEADER`] on every request. A
///   local DuckDB adapter needs none, exactly as the CLI needs no
///   `--allow-warehouse` there.
/// * **A row cap.** `limit` is `1..=500`; anything else is `400`.
/// * **A timeout.** The whole call is bounded by the state's sample timeout
///   ([`rocky_server::state::DEFAULT_SAMPLE_TIMEOUT`], 30 seconds), the
///   compile included: the blocking stage of the sample runs on the blocking
///   pool, so the deadline is observed while it runs (#1816). Only one sample
///   runs at a time — a second is refused at once rather than queued behind
///   a call that may take the whole 30 seconds — and the permit rides with
///   the blocking stage, so a compile the route stopped waiting for keeps the
///   lane until it returns.
///
/// The response carries `Cache-Control: no-store`: the body is warehouse rows,
/// and `GET` is the one method a browser, a proxy or a service worker caches
/// without being asked.
///
/// Ad-hoc SQL (`--sql-file`) is not exposed. A compiled model's `SELECT` is a
/// different threat model from arbitrary SQL over HTTP, whatever the token.
async fn model_rows(
    State(state): State<Arc<ServerState>>,
    ApiPath(name): ApiPath<String>,
    ApiQuery(query): ApiQuery<SampleQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let config = state
        .config_path
        .clone()
        .ok_or_else(ApiError::engine_not_ready)?;

    let limit = query.limit.unwrap_or(DEFAULT_SAMPLE_LIMIT);
    if limit == 0 || limit > MAX_SAMPLE_LIMIT {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("limit must be between 1 and {MAX_SAMPLE_LIMIT}, got {limit}"),
            Some("pass a smaller `limit`, or omit it for the default of 20"),
        ));
    }

    let consented = headers
        .get(CONSENT_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    // One sample at a time, refused immediately rather than queued: waiting
    // behind a call that may run the full timeout is worse for the caller than
    // a refusal it can retry.
    let Ok(permit) = Arc::clone(&state.warehouse_samples).try_acquire_owned() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "engine_busy",
            "another sample is in flight",
            Some("retry in a moment; one sample runs at a time"),
        )
        .retry_after(5));
    };

    let timeout = state.sample_timeout();
    let sample = tokio::time::timeout(
        timeout,
        crate::commands::compute_preview_rows(
            &config,
            &name,
            query.cte.as_deref(),
            limit,
            consented,
            query.pipeline.as_deref(),
            &state.models_dir,
            // Ad-hoc SQL is never reachable over HTTP.
            None,
            // The permit goes with the sample, into its blocking stage: a
            // compile that outlives the deadline keeps the lane until it
            // returns (#1816).
            Some(permit),
        ),
    )
    .await;

    match sample {
        Ok(Ok(output)) => {
            let mut response = PrettyJson(output).into_response();
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
            Ok(response)
        }
        Ok(Err(failure)) => Err(sample_failure_to_api_error(failure)),
        Err(_) => Err(ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "sample_timeout",
            format!("the sample did not finish within {timeout:?}"),
            Some("the warehouse may still be running the query; narrow the model or lower `limit`"),
        )),
    }
}

/// Map a `PreviewFailure` onto the API error surface, one `error_kind` at a
/// time.
///
/// The masking refusal is renamed here, which the CLI's single
/// `unmaskable_column` kind does not do: `inline_mask_expr` can express a mask
/// only on `databricks`, `snowflake` and `duckdb`, so on any other adapter
/// **every** sample of a model with a masked column is refused, permanently.
/// That caller needs to be told the adapter cannot express the strategy, which
/// they can act on, rather than a bare "unmaskable column" they will read as a
/// bug. The refusal itself is unchanged and stays fail-closed: nothing is ever
/// served unmasked.
fn sample_failure_to_api_error(failure: crate::commands::PreviewFailure) -> ApiError {
    let status = match failure.kind.as_str() {
        "model_not_found" => StatusCode::NOT_FOUND,
        "warehouse_gated" => StatusCode::FORBIDDEN,
        "invalid_model_name" | "invalid_cte_name" | "invalid_arguments" => StatusCode::BAD_REQUEST,
        "unsupported_model_kind"
        | "compile_error"
        | "cte_error"
        | "cte_masking_unverified"
        | "adhoc_masking_blocked"
        | "unmaskable_column" => StatusCode::UNPROCESSABLE_ENTITY,
        "upstream_not_materialized" | "missing_catalog" => StatusCode::CONFLICT,
        "config_error" | "pipeline_error" => StatusCode::SERVICE_UNAVAILABLE,
        // The blocking stage of the sample did not complete (a panic on the
        // blocking pool): this server's fault, not the adapter's.
        "internal_error" => StatusCode::INTERNAL_SERVER_ERROR,
        // An adapter that would not connect, or would not answer, failed
        // upstream of this server.
        _ => StatusCode::BAD_GATEWAY,
    };
    let hint = match failure.kind.as_str() {
        "warehouse_gated" => {
            Some("set the `X-Rocky-Allow-Warehouse: true` header to run this against the warehouse")
        }
        "unmaskable_column" => Some(
            "this adapter cannot express the column's mask strategy; change the strategy, or sample from an adapter that can",
        ),
        "upstream_not_materialized" => Some("run the pipeline first, then sample"),
        _ => None,
    };
    let code = match failure.kind.as_str() {
        // The one rename: the CLI's kind names the column, the route's names
        // what the caller can do about it.
        "unmaskable_column" => "masking_unsupported_by_adapter",
        other => other,
    };
    ApiError::new(status, code, failure.message, hint)
}

// --- The governor routes: brief, scorecard, custody ---

/// Query string of `GET /api/v1/brief`.
#[derive(Debug, Deserialize)]
struct BriefQuery {
    /// `last` | `24h` | `7d`. Defaults to `7d`.
    since: Option<String>,
}

/// Query string of `GET /api/v1/audit/scorecard`.
#[derive(Debug, Deserialize)]
struct ScorecardQuery {
    /// `principal` | `rule` | `scope`. Defaults to `principal`.
    by: Option<String>,
    /// `all`, or a `<N>d` / `<N>h` duration. Defaults to `all`.
    window: Option<String>,
}

/// Longest custody subject the route reads. Model names are short
/// identifiers, run ids and plan ids are fixed-width, and a decision-only
/// custody id (`freeze:…`, `draft:…`, `autoapply:…`) is one of those with a
/// prefix; anything longer is not a subject the ledger can hold.
const MAX_CUSTODY_SUBJECT_BYTES: usize = 512;

/// `GET /api/v1/brief` — canonical [`BriefOutput`].
///
/// The same bytes as `rocky brief --since <since> --output json`, except
/// `generated_at` and, for the relative windows, `since_timestamp`, which
/// derive from the request instant. The route never advances the digest
/// cursor: `since=last` reads the cursor as it stands, so a screen that
/// refreshes does not consume a Slack hook's `--since last` window. The
/// default is `7d`, the MCP tool's default rather than the CLI's `last`,
/// because a route that cannot advance the cursor has no window of its own
/// under `last`, and a first-ever `last` spans all of recorded history.
async fn governor_brief(
    State(state): State<Arc<ServerState>>,
    ApiQuery(query): ApiQuery<BriefQuery>,
) -> Result<PrettyJson<BriefOutput>, ApiError> {
    let since = match query.since.as_deref().unwrap_or("7d") {
        "last" => BriefSince::Last,
        "24h" => BriefSince::Hours24,
        "7d" => BriefSince::Days7,
        other => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("unknown since window '{other}'"),
                Some("pass since=last, since=24h or since=7d"),
            ));
        }
    };
    let root = project_root_for(&state)?;
    let config = state
        .config_path
        .clone()
        .ok_or_else(ApiError::engine_not_ready)?;
    let state_path = state_path_for(&state);
    let output = store_read(&state, move || {
        compute_brief(&root, &state_path, &config, since, chrono::Utc::now())
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/audit/scorecard` — canonical [`AuditScorecardOutput`].
///
/// The same bytes as `rocky audit --scorecard --by <by> --window <window>
/// --output json`, except `window_start` for a duration window, which derives
/// from the request instant. Reads the state store only, so it needs no bound
/// config. A malformed `window` is `400` with the CLI's own message; a ledger
/// that cannot be read is `200` with `availability: unavailable`, because the
/// core fails closed inside the payload and the CLI prints exactly that.
async fn audit_scorecard(
    State(state): State<Arc<ServerState>>,
    ApiQuery(query): ApiQuery<ScorecardQuery>,
) -> Result<PrettyJson<AuditScorecardOutput>, ApiError> {
    let by = match query.by.as_deref().unwrap_or("principal") {
        "principal" => ScorecardDimension::Principal,
        "rule" => ScorecardDimension::Rule,
        "scope" => ScorecardDimension::Scope,
        other => {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("unknown scorecard dimension '{other}'"),
                Some("pass by=principal, by=rule or by=scope"),
            ));
        }
    };
    // Validated here so a usage error is a 400; the only other failure the
    // core can return is then a read failure, which stays a 500.
    parse_window(query.window.as_deref(), chrono::Utc::now()).map_err(|e| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("{e:#}"),
            Some("pass window=all or a <N>d / <N>h duration such as window=30d"),
        )
    })?;
    let state_path = state_path_for(&state);
    let window = query.window;
    let output = store_read(&state, move || {
        compute_audit_scorecard(&state_path, by, window.as_deref())
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/custody/{subject}` — canonical [`AuditForOutput`].
///
/// The same bytes as `rocky audit --for <subject> --output json`: the custody
/// chain for a model name, a run id, a plan id, or a decision-only custody id
/// such as `freeze:global`. A subject nothing references is `200` with
/// `resolved: false`, as the CLI. Compiles the project once per request for
/// the blast radius, as the CLI does. The subject touches disk only when it is
/// 64 hex characters, through the non-creating plan-path probe.
async fn custody_chain(
    State(state): State<Arc<ServerState>>,
    ApiPath(subject): ApiPath<String>,
) -> Result<PrettyJson<AuditForOutput>, ApiError> {
    if subject.len() > MAX_CUSTODY_SUBJECT_BYTES {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!(
                "subject is {} bytes; the limit is {MAX_CUSTODY_SUBJECT_BYTES}",
                subject.len()
            ),
            Some("pass a model name, a run id, a plan id or a custody id"),
        ));
    }
    let root = project_root_for(&state)?;
    let config = state
        .config_path
        .clone()
        .ok_or_else(ApiError::engine_not_ready)?;
    let state_path = state_path_for(&state);
    let models_dir = state.models_dir.clone();
    let output = store_read(&state, move || {
        compute_audit_for(&root, &config, &state_path, &models_dir, &subject)
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    Ok(PrettyJson(output))
}

/// Query string of `GET /api/v1/audit`.
#[derive(Debug, Deserialize)]
struct AuditQuery {
    /// A product name: list only the rows about its output model.
    product: Option<String>,
}

/// The three ways a ledger lookup can end, decided on the blocking side so
/// the handler maps each to its documented status code.
enum AuditLookup {
    /// Boxed: the payload dwarfs the two refusal arms.
    Found(Box<AuditOutput>),
    NoProduct,
    SpecInvalid {
        code: String,
        reason: String,
    },
}

/// `GET /api/v1/audit` — canonical [`AuditOutput`].
///
/// The same bytes as `rocky audit --output json`: every recorded policy
/// decision, oldest first. With `?product=<name>`, the same bytes as
/// `rocky audit --product <name> --output json`: only the rows about that
/// product's output model, resolved from `products/<name>.toml` through the
/// loader `rocky product status` uses. Unfiltered, it reads the state store
/// only and needs no bound config. A name that is not a bare identifier, or
/// has no spec file, is `404 product_not_found`; a spec the loader rejects is
/// `409 product_spec_invalid` with the loader's code and reason.
async fn audit_ledger(
    State(state): State<Arc<ServerState>>,
    ApiQuery(query): ApiQuery<AuditQuery>,
) -> Result<PrettyJson<AuditOutput>, ApiError> {
    let product_name = query.product.clone().unwrap_or_default();
    let scope = match query.product {
        None => None,
        Some(name) => {
            // The spec path is built only from a bare identifier, the same
            // guard the product routes apply before opening anything.
            if !is_bare_product_name(&name) {
                return Err(ApiError::product_not_found(&name));
            }
            Some((project_root_for(&state)?, name))
        }
    };
    let state_path = state_path_for(&state);
    let output = store_read(&state, move || -> anyhow::Result<AuditLookup> {
        let product = match scope {
            None => None,
            Some((root, name)) => match resolve_product_scope(&root, &name) {
                Ok(scope) => Some(scope),
                Err(reject) if reject.code == "spec-file-missing" => {
                    return Ok(AuditLookup::NoProduct);
                }
                Err(reject) => {
                    return Ok(AuditLookup::SpecInvalid {
                        code: reject.code.to_string(),
                        reason: reject.message,
                    });
                }
            },
        };
        compute_audit(&state_path, product).map(|output| AuditLookup::Found(Box::new(output)))
    })
    .await?
    .map_err(|e| map_state_err(e, state.mutation_permit.running_job()))?;
    match output {
        AuditLookup::Found(output) => Ok(PrettyJson(*output)),
        AuditLookup::NoProduct => Err(ApiError::product_not_found(&product_name)),
        AuditLookup::SpecInvalid { code, reason } => Err(ApiError::product_spec_invalid(
            &product_name,
            &code,
            &reason,
        )),
    }
}

/// The rule a product name must satisfy to exist at all: a bare identifier,
/// ASCII letter or `_` first, then ASCII letters, digits or `_`. It is the
/// spec parser's `product-name-invalid` rule, so a name that fails it can
/// name no spec file and no state record, and the route can answer 404
/// without touching the filesystem or the store.
fn is_bare_product_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    match bytes.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// `GET /api/v1/schedule` — a read-only scheduler snapshot.
///
/// Reports stored cursors, claims and tick-lock state. It deliberately does
/// **not** evaluate demand — `rocky tick --dry-run` is the evaluation — so it
/// stays side-effect free and `O(pipelines)` rather than `O(runs × pipelines)`.
async fn schedule_status(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ScheduleStatusOutput>, ApiError> {
    let Some(config_path) = state.config_path.clone() else {
        return Err(ApiError::engine_not_ready());
    };
    let state_path = state_path_for(&state);
    // Anchor `.rocky` to the config's directory (the project root), not the
    // process cwd — the same derivation the reconciler uses, so a `serve
    // --scheduler` and a cron `rocky tick` are reported against one tick lock.
    let rocky_dir = crate::commands::scheduler::rocky_dir_for_config(&config_path);
    let running_job_id = state.mutation_permit.running_job();

    let output = store_read(&state, move || {
        schedule_status_output(&config_path, &state_path, &rocky_dir, chrono::Utc::now())
    })
    .await?
    .map_err(|e| map_schedule_err(e, running_job_id))?;
    Ok(PrettyJson(output))
}

/// `GET /api/v1/schedule/spool`: the webhook demands accepted but not yet
/// consumed — the same bytes as `rocky state schedule spool --output json`.
///
/// [`schedule_status`] reports claims, which exist only once a tick has picked
/// a demand up, so a queued demand appears nowhere in `GET /api/v1/schedule`.
/// This is the other half.
///
/// Fail-closed: a spool directory that is present but unreadable is a `500`,
/// never an empty list. An absent spool is `200` with nothing pending — no
/// webhook has ever been accepted for this project.
///
/// Takes no state-store permit. The spool is plain files under `.rocky`, so
/// this read never touches redb and cannot be blocked by a running job; the
/// filesystem work runs on a blocking thread.
async fn schedule_spool(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<ScheduleSpoolOutput>, ApiError> {
    let Some(config_path) = state.config_path.clone() else {
        return Err(ApiError::engine_not_ready());
    };

    let output =
        tokio::task::spawn_blocking(move || crate::commands::compute_schedule_spool(&config_path))
            .await
            .map_err(|e| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("the spool read panicked: {e}"),
                    None,
                )
            })?
            .map_err(|e| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "spool_unreadable",
                    e.to_string(),
                    Some(
                        "inspect the spool directory's permissions — queued webhook \
                 demands cannot be counted while it is unreadable",
                    ),
                )
            })?;

    Ok(PrettyJson(output))
}

/// `GET /api/v1/policy`: the policy plane, the same bytes as
/// `rocky policy show --output json`.
///
/// Three reads, each fail-closed: `rocky.toml` (a file that does not parse is
/// `500 config_invalid`; a missing one is the default posture), the decision
/// ledger under the store permit, and the durable freeze markers when
/// `[state]` keeps them. A source that exists but cannot be read is a `500`,
/// never an empty list.
async fn policy_show(
    State(state): State<Arc<ServerState>>,
) -> Result<PrettyJson<PolicyRulesOutput>, ApiError> {
    let Some(config_path) = state.config_path.clone() else {
        return Err(ApiError::engine_not_ready());
    };
    let (policy, state_cfg) = policy_show_config(&config_path)
        .map_err(|e| ApiError::config_invalid(&format!("{e:#}")))?;
    // With no `[policy]` block the enforcement gate returns NotConfigured
    // before it reads a freeze source. The route reads neither too, so it
    // cannot list a freeze the engine would not honour. Same branch the CLI
    // takes in `compute_policy_show`, so the two cannot answer differently.
    let (ledger, markers) = if policy.is_some() {
        let state_path = state_path_for(&state);
        let remote = policy_show_remote_backend(&state_cfg);
        let running_job_id = state.mutation_permit.running_job();
        let ledger = store_read(&state, move || read_policy_show_ledger(&state_path, remote))
            .await?
            .map_err(|e| map_state_err(e, running_job_id))?;
        let markers = load_policy_show_markers(&state_cfg)
            .await
            .map_err(|e| ApiError::internal(format!("{e:#}")))?;
        (ledger, markers)
    } else {
        (
            policy_show_unconsulted_ledger(),
            PolicyShowMarkers::NotConsulted,
        )
    };
    Ok(PrettyJson(assemble_policy_show(
        policy.as_ref(),
        ledger,
        markers,
    )))
}

// --- Webhook ingress (POST /api/v1/hooks/trigger/{pipeline}) ---

/// The hex HMAC-SHA256 of the raw body, keyed on `ROCKY_WEBHOOK_SECRET`.
const WEBHOOK_SIGNATURE_HEADER: &str = "x-rocky-signature";
/// An optional caller-supplied delivery id. When present the demand deduplicates
/// on the id (with a 24h consumed-tombstone); when absent it deduplicates on the
/// body hash (no tombstone — an identical body after consumption fires again).
const WEBHOOK_DELIVERY_HEADER: &str = "x-rocky-delivery";

/// The result of the blocking pipeline-validate + spool-accept.
enum WebhookAccept {
    Accepted(String),
    Duplicate,
    PipelineNotFound,
    Failed(String),
}

/// `POST /api/v1/hooks/trigger/{pipeline}` — accept a webhook demand.
///
/// This route is **prefix-exempt** from the Bearer middleware and authenticates
/// with its own `X-Rocky-Signature` HMAC over the raw body. The demand is
/// written durably (`fsync`'d) before the `202`, so an accepted webhook is never
/// lost across a crash; the resident reconciler consumes it *at most once* on a
/// later tick. Fail-closed: the route is dark when `--scheduler` is off or when
/// no secret is set on a non-loopback bind.
async fn webhook_trigger(
    State(state): State<Arc<ServerState>>,
    Path(pipeline): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(ingress) = state.webhook.as_ref() else {
        // `serve` without `--scheduler`: no reconciler to consume a demand.
        return Err(ApiError::webhook_disabled());
    };
    // Fail-closed: a non-loopback bind with no secret must not accept unsigned
    // webhooks. Loopback with no secret is the dev convenience path (no HMAC).
    if ingress.secret.is_none() && !ingress.bind_is_loopback {
        return Err(ApiError::webhook_disabled());
    }

    // 1. HMAC FIRST — before revealing anything about the target — so an
    //    unauthenticated caller can neither enumerate pipelines nor exhaust the
    //    rate limiter for legitimate senders.
    if let Some(secret) = ingress.secret.as_deref() {
        let provided = headers
            .get(WEBHOOK_SIGNATURE_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !rocky_core::hooks::webhook::verify_signature(secret, &body, provided) {
            return Err(ApiError::webhook_bad_signature());
        }
    }

    // 2. Rate limit — a flood guard on authenticated traffic, BEFORE any spool
    //    write so an over-limit request never yields a `202` without a file.
    if let rocky_server::webhook_ingress::RateDecision::Limited { retry_after } =
        ingress.rate_limiter.check(std::time::Instant::now())
    {
        return Ok(too_many_requests_response(retry_after));
    }

    // 3. Determine dedup kind + token from the delivery header, and hash the body.
    let body_hash = blake3::hash(&body).to_hex().to_string();
    let (kind, token) = match headers
        .get(WEBHOOK_DELIVERY_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        Some(delivery) => (rocky_core::schedule::WebhookKind::Id, delivery.to_string()),
        None => (rocky_core::schedule::WebhookKind::Body, body_hash.clone()),
    };

    // 4. Validate the pipeline + durably spool — both are blocking FS/parse work.
    let config_path = state.config_path.clone();
    let rocky_dir = ingress.rocky_dir.clone();
    let pipeline_for_task = pipeline.clone();
    let now = chrono::Utc::now();
    let outcome = tokio::task::spawn_blocking(move || {
        let Some(config_path) = config_path.as_deref() else {
            return WebhookAccept::Failed("no project config is bound to this server".to_string());
        };
        let config = match rocky_core::config::load_rocky_config(config_path) {
            Ok(c) => c,
            Err(e) => return WebhookAccept::Failed(format!("config load failed: {e}")),
        };
        if !config.pipelines.contains_key(&pipeline_for_task) {
            return WebhookAccept::PipelineNotFound;
        }
        match rocky_core::schedule::accept(
            &rocky_dir,
            &pipeline_for_task,
            kind,
            &token,
            &body_hash,
            now,
        ) {
            Ok(rocky_core::schedule::AcceptOutcome::Created(uid)) => WebhookAccept::Accepted(uid),
            Ok(rocky_core::schedule::AcceptOutcome::Duplicate) => WebhookAccept::Duplicate,
            Err(e) => WebhookAccept::Failed(format!("spooling the demand failed: {e}")),
        }
    })
    .await
    .map_err(|e| map_join_err(&e))?;

    match outcome {
        WebhookAccept::Accepted(uid) => {
            tracing::info!(pipeline = %pipeline, demand_uid = %uid, "webhook demand accepted");
            Ok((
                StatusCode::ACCEPTED,
                PrettyJson(serde_json::json!({ "demand": "accepted", "demand_uid": uid })),
            )
                .into_response())
        }
        WebhookAccept::Duplicate => Ok((
            StatusCode::ACCEPTED,
            PrettyJson(serde_json::json!({ "demand": "duplicate" })),
        )
            .into_response()),
        WebhookAccept::PipelineNotFound => Err(ApiError::pipeline_not_found(&pipeline)),
        WebhookAccept::Failed(message) => Err(ApiError::internal(message)),
    }
}

/// Build a `429` with the `Retry-After` header and the standard error envelope.
fn too_many_requests_response(retry_after: std::time::Duration) -> Response {
    let secs = retry_after.as_secs().max(1);
    let envelope = ErrorEnvelope {
        code: "rate_limited".to_string(),
        message: "too many webhook requests; retry after the indicated delay".to_string(),
        remediation_hint: Some("respect the Retry-After header".to_string()),
        running_job_id: None,
    };
    let mut resp = (StatusCode::TOO_MANY_REQUESTS, PrettyJson(envelope)).into_response();
    if let Ok(value) = HeaderValue::from_str(&secs.to_string()) {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, value);
    }
    resp
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
    /// `--expect-spec-digest <hex>` for `apply` — the approved-spec digest
    /// the caller expects the plan to be bound to. The engine's gate is
    /// fail-closed both ways: a product-bound plan REFUSES an apply without
    /// this, and passing it against an unbound plan refuses too. The value
    /// must come from the caller's independently approved product-spec
    /// snapshot, never read back from the plan itself.
    expect_spec_digest: Option<String>,
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
/// Strip resolved `${VAR}` values from a job's terminal outcome, before it is
/// cached or written (#1897).
///
/// A pure function on purpose. The alternative is scrubbing inside the spawn
/// path, and nothing in `cargo test -p rocky-cli --lib` can reach that: the
/// subprocess is `current_exe`, which is the test harness. Here the seam is
/// callable directly, so the behaviour is pinned by unit test rather than by
/// an end-to-end run nobody can drive.
///
/// It is also the chokepoint rather than one caller: the spawn path is the only
/// writer today, and a second one would otherwise be unscrubbed.
///
/// `result` is the child's stdout verbatim — a whole `RunOutput` carrying
/// targets, model names and attempt trails. It is rewritten as text and
/// re-parsed; if the rewrite breaks the JSON (a registered value in a
/// non-string position), the payload is REPLACED rather than stored, because
/// an unparseable blob on disk is worse than a named absence.
pub(crate) fn scrub_job_outcome(
    result: Option<serde_json::Value>,
    error: Option<String>,
) -> (Option<serde_json::Value>, Option<String>, u32) {
    let scrub = |text: &str| {
        crate::secret_filter::redact_truncated_tail(&crate::secret_filter::redact(text))
    };

    let withheld =
        || serde_json::json!({ "redaction": "result_withheld_unparseable_after_redaction" });

    let result = result.map(|value| {
        // A registered value inside an object KEY cannot be rewritten safely.
        // Two distinct keys whose names both carry one collapse to the same
        // replacement, and re-parsing then keeps only one of them — a silently
        // truncated record that still looks complete. Withhold instead.
        if crate::secret_filter::any_key_carries_a_value(&value) {
            return withheld();
        }
        let Ok(text) = serde_json::to_string(&value) else {
            return serde_json::json!({ "redaction": "result_unserializable" });
        };
        match serde_json::from_str::<serde_json::Value>(&scrub(&text)) {
            Ok(scrubbed) => scrubbed,
            Err(_) => withheld(),
        }
    });
    let error = error.map(|text| scrub(&text));

    // THE FINAL CHECK, and its absence was a defect. Everything above is
    // replacement GENERATION, and #1920 established that no generator can be
    // trusted: a fallback marker can itself be a registered value, and two
    // overlapping replacements can concatenate into one. The response filter
    // ends with this check for exactly that reason; the durable path had no
    // equivalent, so a surviving value was stamped trusted and written to
    // disk (#1897).
    let survives = result
        .as_ref()
        .and_then(|v| serde_json::to_string(v).ok())
        .is_some_and(|t| crate::secret_filter::any_value_survives(&t))
        || error
            .as_deref()
            .is_some_and(crate::secret_filter::any_value_survives);
    if survives {
        // The fallback must not be the thing that leaked. Both the marker and
        // this message are fixed text an operator can register, and this is
        // the LAST step — nothing scans what it returns — so each is checked
        // and dropped entirely if it would carry a value. Storing nothing is
        // always available and cannot leak, which is what makes this
        // terminate (#1897).
        let safe = |v: &serde_json::Value| {
            serde_json::to_string(v)
                .ok()
                .is_some_and(|t| !crate::secret_filter::any_value_survives(&t))
        };

        // Prefer the descriptive marker. If its own text is registered, fall
        // back to one whose SERIALIZED form is shorter than the floor.
        //
        // The invariant is about the serialized bytes, not about tokens.
        // `any_value_survives` scans a string; token boundaries do not exist
        // at that layer, so a short-token marker can still contain a
        // registerable substring spanning them — the same concatenation
        // fallacy the comment above warns about. `{"h":1}` is SEVEN bytes, so
        // every substring of it is shorter than the eight-byte floor and the
        // registry cannot hold any of them. True by construction, not by odds.
        //
        // `SHORT_MARKER_IS_UNREGISTERABLE` in the tests pins the length, so
        // changing the marker or lowering the floor fails there rather than
        // silently making this claim false.
        let short = serde_json::json!({ "h": 1 });
        let marker = withheld();
        let result = Some(if safe(&marker) { marker } else { short });

        // Same rule for the note: `held` is four bytes raw and six quoted,
        // both under the floor.
        const NOTE: &str = "withheld: a resolved value survived redaction";
        let error = Some(if crate::secret_filter::any_value_survives(NOTE) {
            "held".to_string()
        } else {
            NOTE.to_string()
        });
        return (result, error, rocky_core::state::CURRENT_REDACTION_VERSION);
    }

    (result, error, rocky_core::state::CURRENT_REDACTION_VERSION)
}

fn job_status_from(job: PersistedJob) -> JobStatus {
    // A record written before the scrub existed may hold an unredacted result
    // or error, and nothing distinguishes a safe legacy string from one
    // carrying a resolved value. Lifecycle fields still answer — a poller
    // waiting for a terminal state is not left hanging — but the two payload
    // fields are withheld (#1897).
    let legacy = job.redaction_is_legacy();
    JobStatus {
        kind: JobKind::parse(&job.kind).unwrap_or(JobKind::Run),
        state: JobState::parse(&job.state).unwrap_or(JobState::Failed),
        job_id: job.job_id,
        submitted_at: job.submitted_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        principal: job.principal,
        error: if legacy { None } else { job.error },
        result: if legacy { None } else { job.result },
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

/// Mark every persisted job stranded [in flight](rocky_core::state::PersistedJob::is_in_flight)
/// — `running` or `queued` — as `failed` with the error
/// `"interrupted by engine restart"`, returning how many records were
/// reconciled.
///
/// A record in any OTHER non-terminal state is left untouched; see the loop for
/// why that is both safe and necessary.
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
        // IN FLIGHT, not "not terminal". The two are deliberately not
        // complements: an unrecognized `state` is neither. Sweeping those was a
        // DOWNGRADE DATA-LOSS path — `state` is a plain string precisely so a
        // newer sidecar can add a state, so a newer binary's terminal
        // `"cancelled"` record, carrying a real result, reads as non-terminal
        // here and the clear below would delete that result. That contradicts
        // the preservation contract on `MIN_TRUSTED_REDACTION_VERSION`.
        //
        // Skipping them costs nothing ON THE HTTP SURFACE: `job_status_from`
        // renders `JobState::parse(&job.state).unwrap_or(JobState::Failed)`,
        // the only parse site in the workspace, and every route renders
        // through it. So an unrecognized state ALREADY reads as terminal
        // `failed` to a polling embedder whether or not this sweep runs.
        //
        // NOT "nothing observable anywhere", which is a claim this cannot
        // support: `StateStore::{get_job,list_jobs}` and `JobRegistry::get`
        // return the raw record, so an in-process Rust consumer sees the
        // stored string itself. That is the intended outcome. The state
        // belongs to the version that wrote it, and leaving it verbatim is
        // the only answer this version can give honestly.
        if !job.is_in_flight() {
            continue;
        }
        // Through the scrub, and RE-STAMPED. This is a terminal writer: it
        // mutates `error` and persists, so without the scrub it wrote an
        // unchecked string while inheriting the record's existing trusted
        // stamp — a durable record claiming current redaction while holding a
        // registered value. The message is a fixed literal, but an operator
        // can register any string, including this one (#1897).
        //
        // The inherited `result` goes too, and the reason is stronger than
        // "a previous process wrote it":
        //
        // **AT THIS POINT IN STARTUP THE VERIFIER IS NOT TRUSTWORTHY.** The
        // registry is populated by `substitute_env_vars_inner`, which runs
        // only on a config LOAD. `build_serve_state` does not load — it
        // derives paths — so in plain `rocky serve` this sweep is the first
        // thing after the bind check and the registry may be EMPTY, or
        // partially filled by the watcher's initial compile, which is a race
        // rather than an ordering. Scheduler mode is NOT reliably better:
        // `resolved_poll_interval` returns without loading config when an
        // explicit interval is supplied (`serve.rs`), so that path can reach
        // here with an empty registry too.
        //
        // `any_value_survives` against an empty registry answers "nothing
        // survived" and would stamp the record current — a false assertion
        // produced by the check meant to prevent one.
        //
        // **WHAT THE STAMP SPEAKS FOR IS `result` AND `error`** — that is the
        // pair `job_status_from` withholds on a legacy record, and the pair
        // this sweep replaces with process-local values. It does NOT speak for
        // `principal`, which is inherited here: caller-supplied, advisory,
        // served unconditionally, redacted on the wire by the response filter,
        // and durable-record exposure disclosed under #1919.
        //
        // The literal below is EXPLICIT on every field rather than
        // `..job`/`let mut done = job`, so adding a field to `PersistedJob`
        // stops compiling here instead of being inherited silently.
        let (_, error, version) =
            scrub_job_outcome(None, Some("interrupted by engine restart".to_string()));
        let done = rocky_core::state::PersistedJob {
            state: job_state_str(JobState::Failed).to_string(),
            finished_at: Some(chrono::Utc::now().to_rfc3339()),
            result: None,
            error,
            redaction_version: Some(version),
            job_id: job.job_id,
            kind: job.kind,
            submitted_at: job.submitted_at,
            started_at: job.started_at,
            principal: job.principal,
        };
        // The sweep writes directly rather than through `persist_job`, so it
        // needs the same sink check.
        store.record_job(&sanitize_for_storage(done))?;
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
/// Check the record that is actually about to be WRITTEN, and withhold its
/// payload if a registered value survives in it.
///
/// **The layer is the point.** `scrub_job_outcome` checks the `result` and
/// `error` FIELDS; what reaches disk is the SERIALIZED RECORD, produced later
/// by `serde_json::to_vec` inside `record_job`. Three things slip through that
/// gap, and the first two were reported against the field-level check:
///
/// ```text
/// escaping     register `ABCDEFGH\"`; return the raw error `ABCDEFGH"`.
///              The raw scan misses it; serializing the record escapes the
///              quote and writes exactly the registered bytes.
///
/// other fields `principal` comes from a caller-supplied X-Rocky-Principal
///              header and was never scanned at all. Nor is any field added
///              to this struct in future.
///
/// field seams  a value can span two fields — `null,"result":null` sits
///              across the `error` value and the `result` key that follows
///              it. No single field contains it; the record does.
/// ```
///
/// Checking the serialized record closes the class rather than those three
/// instances: the thing checked is the thing written.
///
/// **Withholding replaces the record, it does not patch it.** Blanking
/// `result` and `error` would not help when the survivor is in `principal`, so
/// the fields that are kept are named explicitly and everything else is
/// dropped. That is also why this is a full literal rather than `..job`: a
/// field added to `PersistedJob` stops compiling here instead of being
/// forwarded into a record this function claims to have cleaned.
///
/// **Why it terminates.** The withheld record can itself trip the check — an
/// operator may register `"held"` padded into something over the length floor.
/// It is not re-scanned, and does not need to be: every field kept is
/// generated by this process (`new_job_id`, a fixed `kind` verb, a
/// `job_state_str` literal, `Utc::now` timestamps) and the two replacements
/// are fixed. A collision therefore requires an operator to register a value
/// this process produced, which discloses nothing they did not already hold.
/// No caller-supplied byte reaches the store either way.
///
/// Scope: the DURABLE record. The in-memory cache is served through
/// `job_status_from` and therefore through the outermost response filter, so
/// the wire is covered by #1920 regardless; this is the stored-record
/// guarantee, which is what #1944 adds.
pub(crate) fn sanitize_for_storage(job: PersistedJob) -> PersistedJob {
    let Ok(serialized) = serde_json::to_string(&job) else {
        // Unserializable here means `record_job` would fail too; leave it for
        // that to report rather than silently altering the record.
        return job;
    };
    if !crate::secret_filter::any_value_survives(&serialized) {
        return job;
    }

    PersistedJob {
        // Kept: generated by this process, never by a caller.
        job_id: job.job_id,
        kind: job.kind,
        state: job.state,
        submitted_at: job.submitted_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        // Dropped: caller-supplied, and advisory rather than load-bearing.
        principal: None,
        // The same fallbacks `scrub_job_outcome` uses, and safe for the same
        // reason: `{"h":1}` serializes to seven bytes, below the eight-byte
        // floor, so it has no substring the registry could hold.
        error: Some("held".to_string()),
        result: Some(serde_json::json!({ "h": 1 })),
        redaction_version: Some(rocky_core::state::CURRENT_REDACTION_VERSION),
    }
}

pub(crate) async fn persist_job(
    state: &ServerState,
    state_path: std::path::PathBuf,
    job: PersistedJob,
) -> anyhow::Result<()> {
    // The write takes its turn at the process's store gate like every other
    // open in this process; see `store_read`.
    let permit = Arc::clone(&state.store_access).acquire_owned().await?;
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let _held = permit;
        let store = rocky_core::state::StateStore::open(&state_path)?;
        // Checked HERE, on the record about to be serialized, not on the
        // fields that composed it.
        store.record_job(&sanitize_for_storage(job))?;
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
        // Stamped at creation; the terminal write re-stamps after scrubbing.
        redaction_version: Some(rocky_core::state::CURRENT_REDACTION_VERSION),
    };

    let state_path = state_path_for(&state);
    let config_path = state.config_path.clone();

    // Register + persist the `running` record BEFORE spawning the subprocess and
    // BEFORE returning 202, so a crash immediately after submission still reports
    // honest status on restart (the durability done-criterion). Persistence is
    // best-effort under lock contention — the in-memory registry is authoritative
    // for the live session, and embedders reconcile via /runs.
    //
    // Order matters for cancellation, not just durability. Axum drops this future
    // if the client disconnects, so every `.await` here is a point the handler can
    // simply stop at. The job cache never evicts an in-flight record, so caching
    // `running` before an await that might not return would strand a slot for the
    // life of the process — and a client that disconnects mid-submission in a loop
    // would rebuild the unbounded growth the cache bound exists to stop. Persisting
    // first and caching last leaves no await between the cache write and the spawn
    // below, so the record is cached only once its terminal owner is guaranteed:
    // either both happen or neither does. (A durable record briefly visible with no
    // cache entry is fine — a concurrent read serves it and declines to cache it,
    // exactly as it would for any in-flight record it did not launch.)
    if let Err(e) = persist_job(&state, state_path.clone(), record.clone()).await {
        tracing::warn!(error = %e, job_id = %job_id,
            "could not persist initial job record; in-memory only until it settles");
    }

    // Background task: run the subprocess, then record the terminal state. The
    // permit is moved in and released when the task ends. Nothing between the
    // cache write and `tokio::spawn` may await.
    state.jobs.upsert(record.clone()).await;
    let task_state = state.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let (final_state, result, error) =
            execute_job_subprocess(kind, config_path, state_path.clone(), request).await;

        let mut done = record;
        done.state = job_state_str(final_state).to_string();
        done.finished_at = Some(chrono::Utc::now().to_rfc3339());
        // Before the cache and before the write, so neither holds a resolved
        // value even briefly, and a reader that races the persist sees the
        // same bytes a restart would (#1897).
        let (result, error, version) = scrub_job_outcome(result, error);
        done.result = result;
        done.error = error;
        done.redaction_version = Some(version);
        task_state.jobs.upsert(done.clone()).await;
        if let Err(e) = persist_job(&task_state, state_path, done.clone()).await {
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

/// Build the full `rocky` argv (minus the binary path) for a job subprocess.
///
/// Pure function extracted from [`execute_job_subprocess`] so the flag
/// threading is unit-testable without spawning anything — in a cargo test
/// `std::env::current_exe()` is the test harness, so the spawn itself is
/// proven by the live reachability transcript, while THIS seam pins exactly
/// which request fields become which flags (notably `expect_spec_digest` →
/// `--expect-spec-digest` on `apply`).
fn job_subprocess_args(
    kind: JobKind,
    config_path: Option<&std::path::Path>,
    state_path: &std::path::Path,
    request: &JobRequest,
) -> Vec<std::ffi::OsString> {
    use std::ffi::OsString;
    let mut args: Vec<OsString> = vec!["--output".into(), "json".into()];
    if let Some(config) = config_path {
        args.push("--config".into());
        args.push(config.into());
    }
    args.push("--state-path".into());
    args.push(state_path.into());
    args.push(kind.verb().into());
    match kind {
        JobKind::Run | JobKind::Plan => {
            if let Some(filter) = &request.filter {
                args.push("--filter".into());
                args.push(filter.into());
            }
            if let Some(pipeline) = &request.pipeline {
                args.push("--pipeline".into());
                args.push(pipeline.into());
            }
            if let Some(model) = &request.model {
                args.push("--model".into());
                args.push(model.into());
            }
        }
        JobKind::Apply => {
            if let Some(plan_id) = &request.plan_id {
                args.push(plan_id.into());
            }
            // FF-WP1 (finding 5): the HTTP apply surface carries the caller's
            // spec-digest expectation through to the engine's fail-closed
            // gate. Without it, a product-bound plan is refused by the engine
            // (the fail-safe); with it, the subprocess crosses the same
            // generic gate a CLI apply does.
            if let Some(digest) = &request.expect_spec_digest {
                args.push("--expect-spec-digest".into());
                args.push(digest.into());
            }
        }
    }
    args
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
    for arg in job_subprocess_args(kind, config_path.as_deref(), &state_path, &request) {
        cmd.arg(arg);
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
    let record = store_read(&state, move || {
        let store = rocky_core::state::StateStore::open_read_only(&state_path)?;
        store.get_job(&lookup_id)
    })
    .await?
    .map_err(|e| map_state_err(anyhow::Error::from(e), state.mutation_permit.running_job()))?;

    match record {
        Some(record) => {
            // Warm the cache so subsequent reads are hot — but only for a
            // finished record.
            //
            // Reaching here with a record that still reads as in flight means it
            // is not one this process is running: every job launched here is
            // cached from submission until it finishes, so a live job's read is
            // a cache hit and never falls through. Whatever else it is — most
            // likely a terminal write that lost to lock contention, since
            // persistence is best-effort — this process will never observe it
            // transition. That is the decisive part, and it holds whether the
            // record is stale or genuinely live somewhere else: the job cache
            // never evicts an in-flight record, so caching one whose completion
            // we will never see parks a slot for the life of the process, and
            // repeating that rebuilds the unbounded growth the capacity bound
            // exists to stop. Re-reading instead also lets the answer improve if
            // a later write settles, rather than pinning `running` in memory.
            if !record.is_in_flight() {
                state.jobs.upsert(record.clone()).await;
            }
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

    use rocky_server::auth::{ServeToken, is_safe_method};

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

    /// #1897. The secret filter must be the LAST layer `router()` applies.
    ///
    /// On the response path a tower layer runs after everything applied
    /// before it, so "last applied" is what makes the filter outermost —
    /// and outermost is the whole claim. A layer added after it would see
    /// the response *after* filtering and could reintroduce anything, and a
    /// filter moved earlier would stop covering the 413 envelope, the 421
    /// host refusal, the auth failures and the fallbacks.
    ///
    /// Scanned from the source for the same reason
    /// [`router_registers_no_undeclared_route`] is: there is no runtime API
    /// that reports a tower stack's order, and the invariant is about the
    /// code someone will edit.
    #[test]
    fn the_secret_filter_is_the_outermost_layer() {
        let source = include_str!("api.rs");
        let start = source
            .find("pub fn router(state: Arc<ServerState>) -> Router {")
            .expect("router() must be findable by its exact signature");
        let body = &source[start..];
        let end = body.find("\n}\n").expect("router() must end at column 0");
        let body: String = body[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        let filter = "crate::secret_filter::redact_response_secrets";
        let at = body
            .rfind(filter)
            .expect("router() must apply the secret filter");
        let last_layer = body
            .rfind(".layer(")
            .expect("router() must apply at least one layer");

        assert!(
            at > last_layer,
            "the secret filter must be inside the LAST `.layer(` call in \
             router(). A layer applied after it wraps OUTSIDE it on the \
             response path, so its output would never be filtered."
        );
    }

    /// #1897. A reported defect — HEAD requests turned into manufactured
    /// `500`s — **did not reproduce**, and this is the test that says so.
    ///
    /// The claim was that axum strips the HEAD body inside the router, leaving
    /// the outermost filter to parse `""` and fail closed. It strips at the
    /// top-level `RouteFuture`, outside the layer stack, so the filter sees
    /// the full body and answers normally. With the guard reverted this test
    /// still passes, which is why the guard is documented as precautionary
    /// rather than as a fix.
    ///
    /// Only the STATUS is asserted. `reqwest` strips a HEAD response body
    /// client-side, so asserting the body is empty would say nothing about
    /// what the server sent.
    #[tokio::test]
    async fn a_head_request_is_not_turned_into_a_manufactured_error() {
        rocky_core::secret_registry::register_substitution(
            "ROCKY_HEAD_PROBE",
            "HEAD-PROBE-VALUE-8e26660e",
        );

        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.redb");
        let base = spawn_router(pinned_server(dir.path().join("models"), None, &state_path)).await;

        // PRECONDITIONS, asserted rather than assumed. Codex reported this
        // defect as firing "whenever a secret has been registered", and the
        // filter returns early on an empty registry — so a green result with
        // an empty registry would be vacuous. `spawn_router` builds the fully
        // composed `router(state)`, with the filter as its last layer
        // (pinned by `the_secret_filter_is_the_outermost_layer`).
        assert!(
            !rocky_core::secret_registry::is_empty(),
            "PRECONDITION: the registry must be non-empty, or the filter \
             returns before it can see this response"
        );

        let response = reqwest::Client::new()
            .head(format!("{base}/api/v1/meta"))
            .send()
            .await
            .expect("request");

        assert!(
            response.status().is_success(),
            "HEAD must answer as the GET would, not with a manufactured \
             error: {}",
            response.status()
        );
    }

    /// #1897. The filter covers a handler that does not use `PrettyJson`.
    ///
    /// `trigger_compile` hand-builds its body with axum's `Json` and inserts
    /// `config_error` verbatim — the resolved-value leak path. This is the
    /// route a filter installed at the `PrettyJson` responder would have
    /// missed while looking correct, so it is pinned end to end through the
    /// real router rather than by calling `redact` directly.
    #[tokio::test]
    async fn a_hand_built_response_body_is_filtered_too() {
        let secret = "OUTERMOST-PROBE-SECRET-8e26660e";
        rocky_core::secret_registry::register_substitution("ROCKY_OUTERMOST_PROBE", secret);

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("rocky.toml");
        // Valid TOML before substitution, broken after: the parse error then
        // echoes the line with the RESOLVED value in it.
        std::fs::write(
            &config,
            format!("[adapter]\ntype = \"duckdb\"\npath = {secret}\n"),
        )
        .unwrap();
        let state_path = dir.path().join("state.redb");
        let base = spawn_router(pinned_server(
            dir.path().join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        let body = reqwest::Client::new()
            .post(format!("{base}/api/v1/compile"))
            .send()
            .await
            .expect("request")
            .text()
            .await
            .expect("body");

        assert!(
            !body.contains(secret),
            "POST /api/v1/compile hand-builds its body with axum::Json, so it \
             bypasses PrettyJson entirely. The resolved value must still be \
             gone. Body length: {} bytes",
            body.len()
        );
        assert!(
            body.contains("${ROCKY_OUTERMOST_PROBE}"),
            "and the variable must be named, or the operator cannot tell what \
             failed. Body length: {} bytes",
            body.len()
        );
    }

    /// #1897 PR2. The child's stdout is scrubbed before it is cached or
    /// written, so neither the in-memory record nor the durable one holds a
    /// resolved value.
    #[test]
    fn a_job_result_is_scrubbed_before_it_is_stored() {
        let secret = "JOBSCRUB-PROBE-8e26660e-VALUE";
        rocky_core::secret_registry::register_substitution("ROCKY_JOBSCRUB", secret);

        let child_stdout = serde_json::json!({
            "version": "1",
            "command": "run",
            "materializations": [{ "target": format!("{secret}.marts.orders") }],
        });
        let (result, error, version) =
            scrub_job_outcome(Some(child_stdout), Some(format!("failed at {secret}")));

        let rendered = serde_json::to_string(&result.expect("a result")).expect("serializes");
        assert!(
            !rendered.contains(secret),
            "the stored result still holds it: {rendered}"
        );
        assert!(rendered.contains("${ROCKY_JOBSCRUB}"), "{rendered}");

        let error = error.expect("an error");
        assert!(
            !error.contains(secret),
            "the stored error still holds it: {error}"
        );
        assert_eq!(version, rocky_core::state::CURRENT_REDACTION_VERSION);
    }

    /// A result the rewrite would break is REPLACED, not stored. An
    /// unparseable blob on disk is worse than a named absence, and it would
    /// fail every later read rather than this one write.
    #[test]
    fn a_result_that_cannot_survive_the_rewrite_is_withheld_not_corrupted() {
        // 10 digits: above the floor, and it lands in a NUMERIC position.
        let numeric = "1234509876";
        rocky_core::secret_registry::register_substitution("ROCKY_JOBSCRUB_NUM", numeric);

        let child_stdout = serde_json::json!({ "max_downstreams": 1234509876u64 });
        let (result, _, _) = scrub_job_outcome(Some(child_stdout), None);

        // Withheld as the marker, or as nothing at all when the marker's own
        // text is registered by another test — the registry is process-global,
        // so the exact fallback depends on ordering. The property is that the
        // original payload is gone and nothing registered survived.
        let stored = serde_json::to_string(&result).expect("serializes");
        assert!(
            !stored.contains("1234509876"),
            "the numeric value survived into the stored result"
        );
        assert!(!crate::secret_filter::any_value_survives(&stored));
    }

    /// Codex E, the regression this PR introduced and the worst of the round.
    ///
    /// `state` is a plain string so a newer sidecar can add a state. Such a
    /// state reads as non-terminal here, so the sweep used to claim it — and
    /// once the sweep started clearing `result`, claiming it DELETED a newer
    /// binary's data on a downgrade.
    ///
    /// The record must come back byte-for-byte, and the sweep must not count it.
    #[test]
    fn the_sweep_leaves_an_unrecognized_state_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.redb");

        // Terminal in a NEWER binary, unrecognized here, and carrying a real
        // result — exactly the downgrade case.
        let mut newer = persisted_job("from-a-newer-binary", "cancelled");
        newer.finished_at = Some("2026-07-07T00:00:10Z".to_string());
        newer.result = Some(serde_json::json!({ "tables_copied": 3 }));
        assert!(
            !newer.is_terminal() && !newer.is_in_flight(),
            "PRECONDITION: the state must fall in the gap between the two \
             predicates, or this test is not exercising the downgrade case"
        );
        {
            let store = rocky_core::state::StateStore::open(&state_path).unwrap();
            store.record_job(&newer).unwrap();
        }

        assert_eq!(
            sweep_interrupted_jobs(&state_path).expect("sweep"),
            0,
            "a state this version does not recognise must not be swept"
        );

        let store = rocky_core::state::StateStore::open(&state_path).unwrap();
        let after = store
            .get_job("from-a-newer-binary")
            .unwrap()
            .expect("record");
        assert_eq!(
            after, newer,
            "the record must survive the sweep unchanged — the result above is \
             the newer binary's data and this version cannot reproduce it"
        );
    }

    /// Codex A, the layer above the field check. A value that only appears
    /// once the RECORD is serialized.
    ///
    /// Register the ESCAPED form. The raw error does not contain it, so the
    /// field-level scan passes — and then `serde_json` escapes the quote while
    /// serializing the record, writing exactly the registered bytes.
    #[test]
    fn a_value_that_appears_only_after_record_serialization_is_caught() {
        let escaped = "SINKESC-8e26660e\\\"";
        rocky_core::secret_registry::register_substitution("ROCKY_SINK_ESCAPED", escaped);

        let raw = "SINKESC-8e26660e\"";
        assert!(
            !raw.contains(escaped),
            "PRECONDITION: the raw text must NOT contain the registered form, \
             or the field-level scan would already catch it"
        );

        let mut job = persisted_job("sink-esc", "failed");
        job.error = Some(raw.to_string());
        assert!(
            serde_json::to_string(&job)
                .expect("serializes")
                .contains(escaped),
            "PRECONDITION: serializing the record must produce the registered \
             form, or this test proves nothing"
        );

        let held = sanitize_for_storage(job);
        let after = serde_json::to_string(&held).expect("serializes");
        assert!(
            !crate::secret_filter::any_value_survives(&after),
            "a value appearing only in the serialized record must be caught"
        );
    }

    /// Codex A, second instance. `principal` comes from a caller-supplied
    /// header and was never scanned. Withholding the PAYLOAD does not help
    /// here, which is why the sink replaces the record instead of blanking two
    /// fields.
    #[test]
    fn a_value_in_a_non_outcome_field_is_caught_at_the_sink() {
        // Inside the header's own charset, so this is a value a caller can send.
        let secret = "SINKPRINCIPAL-8e26660e";
        rocky_core::secret_registry::register_substitution("ROCKY_SINK_PRINCIPAL", secret);

        let mut job = persisted_job("sink-principal", "running");
        job.principal = Some(secret.to_string());

        let held = sanitize_for_storage(job);
        assert_eq!(held.principal, None, "a caller-supplied field is dropped");
        let after = serde_json::to_string(&held).expect("serializes");
        assert!(
            !crate::secret_filter::any_value_survives(&after),
            "a registered value in ANY field must not reach the store"
        );
    }

    /// Codex B. A value that no single field contains and the RECORD does,
    /// because serialization composes them.
    ///
    /// `principal` and `error` are adjacent, so the span below sits across the
    /// END of the principal value and the `error` key that follows it. A
    /// field-level check cannot see it: it is in neither field, nor in the
    /// `(result, error)` tuple the earlier test serializes, which carries
    /// neither field names nor order.
    ///
    /// **The span carries its own entropy on purpose.** A structural span like
    /// `null,"result":null` would also work here and would be far worse: the
    /// registry is process-global and monotonic, so registering a generic
    /// structural string permanently changes every later test whose record has
    /// those fields empty — including the principal test above, whose sink
    /// would then fire for the wrong reason while still passing.
    #[test]
    fn a_value_spanning_two_record_fields_is_caught_at_the_sink() {
        let principal = "SPANBOUNDARY-4f1a77c3";
        let span = "4f1a77c3\",\"error\":null";
        assert!(
            span.len() >= rocky_core::secret_registry::SECRET_LENGTH_FLOOR,
            "PRECONDITION: the span must be registerable"
        );
        rocky_core::secret_registry::register_substitution("ROCKY_SINK_SPAN", span);

        let mut job = persisted_job("sink-span", "failed");
        job.principal = Some(principal.to_string());
        job.error = None;
        assert!(
            !principal.contains(span),
            "PRECONDITION: no single field may contain the span, or this is \
             not testing the seam"
        );
        let serialized = serde_json::to_string(&job).expect("serializes");
        assert!(
            serialized.contains(span),
            "PRECONDITION: the record must actually compose the span, or the \
             field order this test depends on has changed"
        );

        let held = sanitize_for_storage(job);
        let after = serde_json::to_string(&held).expect("serializes");
        assert!(
            !crate::secret_filter::any_value_survives(&after),
            "a value spanning two fields must be caught at the record level"
        );
    }

    /// The ordinary case passes through untouched, or every record would be
    /// withheld and the tests above would pass vacuously.
    #[test]
    fn a_clean_record_is_stored_unchanged() {
        let mut job = persisted_job("sink-clean", "succeeded");
        job.principal = Some("an-operator".to_string());
        job.result = Some(serde_json::json!({ "tables_copied": 3 }));

        assert_eq!(
            sanitize_for_storage(job.clone()),
            job,
            "a clean record must not be altered at the sink"
        );
    }

    /// #1897. The last-resort marker is unregisterable BY LENGTH.
    ///
    /// `any_value_survives` scans serialized bytes, so the property has to be
    /// about those bytes and not about token boundaries — a marker of short
    /// tokens can still contain a registerable substring spanning them. A
    /// marker whose whole serialized form is shorter than the floor has no
    /// substring the registry could hold.
    ///
    /// This fails if the marker grows or the floor shrinks, which is the point:
    /// the claim in `scrub_job_outcome` stops being true at exactly that moment.
    #[test]
    fn the_last_resort_marker_is_shorter_than_the_secret_floor() {
        let short = serde_json::json!({ "h": 1 });
        let serialized = serde_json::to_string(&short).expect("serializes");
        assert_eq!(serialized, r#"{"h":1}"#);
        assert!(
            serialized.len() < rocky_core::secret_registry::SECRET_LENGTH_FLOOR,
            "the last-resort marker serializes to {} bytes, which is not below \
             the {}-byte floor — it can therefore contain a registered value",
            serialized.len(),
            rocky_core::secret_registry::SECRET_LENGTH_FLOOR
        );
        // The note's fallback, quoted as it appears in a JSON body.
        assert!(
            serde_json::to_string("held").expect("serializes").len()
                < rocky_core::secret_registry::SECRET_LENGTH_FLOOR
        );
    }

    /// Codex C, the blocking one. `scrub_job_outcome` stamped without ever
    /// checking whether a value survived its own rewriting.
    ///
    /// The trigger is the fallback marker: register the marker's own text and
    /// a value that forces the marker to be produced. The marker is
    /// constructed AFTER all rewriting, so nothing scanned it — it was
    /// stamped current and written to disk carrying a registered value.
    #[test]
    fn a_value_surviving_the_scrub_is_withheld_rather_than_stamped() {
        // Forces the unparseable path: 10 digits in a numeric position.
        rocky_core::secret_registry::register_substitution("ROCKY_SURV_NUM", "1029384756");
        // And the marker's own text is registerable.
        rocky_core::secret_registry::register_substitution(
            "ROCKY_SURV_MARKER",
            "result_withheld_unparseable_after_redaction",
        );

        let child = serde_json::json!({ "duration_ms": 1029384756u64 });
        let (result, error, _) = scrub_job_outcome(Some(child), None);

        // The marker's own text is registered, so the descriptive marker
        // cannot be used — but the reader still gets a POSITIVE signal, not a
        // bare null. The fallback is safe because its WHOLE SERIALIZED FORM is
        // shorter than SECRET_LENGTH_FLOOR, so it has no substring the
        // registry could hold. Not because its tokens are short: the check
        // scans serialized bytes, where token boundaries do not exist.
        let result = result.expect("a withheld result is still a signal, never null");
        assert_eq!(
            result,
            serde_json::json!({ "h": 1 }),
            "when the descriptive marker is unusable, the one that is shorter \
             than the floor is used"
        );
        let result = Some(result);
        let stored = serde_json::to_string(&(result, error)).expect("serializes");
        assert!(
            !crate::secret_filter::any_value_survives(&stored),
            "a registered value survived into the stored record"
        );
    }

    /// Codex C, non-blocking. Two distinct object KEYS whose names both carry
    /// a registered value rewrite to the SAME replacement; re-parsing then
    /// keeps only one, so the record silently loses a field while still
    /// parsing cleanly.
    #[test]
    fn a_result_whose_keys_carry_a_value_is_withheld_not_silently_truncated() {
        let shared = "KEYCOLLIDE-8e26660e";
        rocky_core::secret_registry::register_substitution("ROCKY_KEYCOLLIDE", shared);

        let child = serde_json::json!({
            format!("{shared}_copied"): 1,
            format!("{shared}_failed"): 2,
        });
        let (result, _, _) = scrub_job_outcome(Some(child), None);

        // Withheld either as the marker or, if the marker text is itself
        // registered by another test in this process, as nothing at all. The
        // property is that the ORIGINAL keys are gone, not which fallback was
        // chosen — the registry is process-global, so asserting the exact
        // shape would depend on test ordering.
        let stored = serde_json::to_string(&result).expect("serializes");
        assert!(
            !stored.contains("_copied") && !stored.contains("_failed"),
            "a key collision must withhold, not store a record missing a field"
        );
        assert!(!crate::secret_filter::any_value_survives(&stored));
    }

    /// Codex A/D. The restart sweep mutates `error` and persists. Before this
    /// it kept the record's existing trusted stamp, so a durable record could
    /// claim current redaction while holding an unchecked string.
    #[test]
    fn the_restart_sweep_scrubs_and_restamps() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.redb");
        {
            let store = rocky_core::state::StateStore::open(&state_path).unwrap();
            let mut running = persisted_job("swept-1", "running");
            // A pre-existing result this process cannot re-verify.
            running.result = Some(serde_json::json!({ "from": "a previous process" }));
            store.record_job(&running).unwrap();
        }

        let swept = sweep_interrupted_jobs(&state_path).expect("sweep");
        assert_eq!(swept, 1);

        let store = rocky_core::state::StateStore::open(&state_path).unwrap();
        let done = store.get_job("swept-1").unwrap().expect("record");
        assert_eq!(
            done.redaction_version,
            Some(rocky_core::state::CURRENT_REDACTION_VERSION),
            "the sweep must RE-STAMP, not inherit"
        );
        assert!(
            done.result.is_none(),
            "an inherited result cannot be re-verified by this process, so it \
             must not be carried forward under a current stamp"
        );
        assert!(!done.redaction_is_legacy());
    }

    /// #1897 PR2. A record written before the scrub existed serves its
    /// lifecycle fields and withholds both payload fields — nothing
    /// distinguishes a safe legacy string from one carrying a resolved value.
    ///
    /// Legacy is ABSENT **or** BELOW the floor. Pinning both means the
    /// comparison is live from the start, instead of the first tightening
    /// needing a second marker.
    #[test]
    fn a_legacy_job_record_serves_status_and_withholds_its_payload() {
        let base = PersistedJob {
            job_id: "job-legacy".to_string(),
            kind: "run".to_string(),
            state: "failed".to_string(),
            submitted_at: "2026-09-11T00:00:00Z".to_string(),
            started_at: Some("2026-09-11T00:00:01Z".to_string()),
            finished_at: Some("2026-09-11T00:00:02Z".to_string()),
            principal: Some("someone".to_string()),
            error: Some("PRE-FIX-ERROR-TEXT".to_string()),
            result: Some(serde_json::json!({ "pre": "fix" })),
            redaction_version: None,
        };

        for version in [None, Some(0)] {
            let job = PersistedJob {
                redaction_version: version,
                ..base.clone()
            };
            let status = job_status_from(job);
            assert!(
                status.result.is_none(),
                "legacy {version:?} served a result"
            );
            assert!(status.error.is_none(), "legacy {version:?} served an error");
            // The lifecycle half still answers, so a poller waiting for a
            // terminal state is not left hanging.
            assert_eq!(status.job_id, "job-legacy");
            assert!(status.finished_at.is_some());
        }

        // At or above the floor, INCLUDING a version this binary does not know:
        // the monotonic-strictness contract says a newer rule redacts at least
        // as hard, so refusing it would lose data that is not at risk.
        for version in [
            Some(rocky_core::state::MIN_TRUSTED_REDACTION_VERSION),
            Some(999),
        ] {
            let job = PersistedJob {
                redaction_version: version,
                ..base.clone()
            };
            let status = job_status_from(job);
            assert!(
                status.result.is_some(),
                "trusted {version:?} withheld a result"
            );
            assert!(
                status.error.is_some(),
                "trusted {version:?} withheld an error"
            );
        }
    }

    /// Reference bytes for a canonical output: exactly what
    /// `crate::output::print_json` emits by default (pretty + trailing `\n`).
    fn reference_bytes<T: Serialize>(output: &T) -> String {
        serde_json::to_string_pretty(output).unwrap() + "\n"
    }

    /// The state path a test should pin, computed **directly**.
    ///
    /// Deliberately not `resolve_state_path`: that resolver prefers a
    /// CWD-relative `.rocky-state.redb` whenever one exists, and the cwd is
    /// global to this whole test binary, so calling it from a test is exactly
    /// what makes a test repointable by its neighbours.
    fn pinned_state_path(models_dir: &std::path::Path) -> PathBuf {
        models_dir.join(rocky_core::state::STATE_FILE_NAME)
    }

    /// A `ServerState` whose state path is **pinned** to `state_path`.
    ///
    /// `ServerState::new` hardcodes the override to `None`, and
    /// `state_path_for` then re-resolves through `resolve_state_path(None, ..)`
    /// at *request* time — so an unpinned test server reads whatever the cwd
    /// probe finds when the request lands, not what the test set up.
    ///
    /// Takes the path rather than deriving it so the caller controls ordering:
    /// tests that must populate or sweep the store *before* the server exists
    /// (the spawned `recompile()` contends for the same file) need the path
    /// first and the server last.
    fn pinned_server(
        models_dir: PathBuf,
        config_path: Option<PathBuf>,
        state_path: &std::path::Path,
    ) -> Arc<ServerState> {
        ServerState::with_auth(
            models_dir,
            None,
            config_path,
            None,
            Vec::new(),
            Some(state_path.to_path_buf()),
        )
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: HealthOutput = resp.json().await.unwrap();
        assert_eq!(body.status, "ok");
        assert_eq!(body.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn test_compile_and_list_models() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let body: ModelListOutput = serde_json::from_str(&text).unwrap();
        assert_eq!(body.count, 3);
        assert_eq!(
            body.models.len(),
            body.count,
            "count must equal the list length"
        );
        // Pretty-printed like every canonical route.
        assert_eq!(text, reference_bytes(&body));
        let customer_orders = body
            .models
            .iter()
            .find(|m| m.name == "customer_orders")
            .expect("the fixture's customer_orders model is listed");
        assert!(customer_orders.upstream.contains(&"raw_orders".to_string()));
        assert!(!customer_orders.has_star);
    }

    #[tokio::test]
    async fn test_get_model_detail() {
        let state = test_state();
        state.recompile().await;
        let state_for_detail = state.clone();
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models/raw_orders"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let body: ModelDetailOutput = serde_json::from_str(&text).unwrap();
        assert_eq!(body.name, "raw_orders");
        assert!(body.sql.contains("SELECT"));
        assert!(!body.sql_truncated, "a fixture-sized model is never cut");
        assert_eq!(body.sql_bytes, body.sql.len());
        assert!(body.file_path.ends_with("raw_orders.sql"));
        assert!(
            body.columns.iter().any(|c| c.name == "order_id"),
            "inferred columns carry the projection: {:?}",
            body.columns
        );
        // Pretty-printed like every canonical route.
        assert_eq!(text, reference_bytes(&body));
        // The typed columns carry the structured type and its label together,
        // and the label is exactly the type's own rendering.
        let typed = body
            .typed_columns
            .as_ref()
            .expect("the type checker produces columns for a fixture model");
        assert!(!typed.is_empty());
        for column in typed {
            assert_eq!(column.data_type_display, column.data_type.to_string());
        }
        // The served type is the checker's own, field for field.
        let state_cols: Vec<TypedColumnOutput> = {
            let lock = state_for_detail.compile_result.read().await;
            lock.as_ref().unwrap().type_check.typed_models["raw_orders"]
                .iter()
                .map(TypedColumnOutput::from_typed_column)
                .collect()
        };
        assert_eq!(
            serde_json::to_value(typed).unwrap(),
            serde_json::to_value(&state_cols).unwrap()
        );
    }

    /// The one estate route whose size is not bounded by the model count
    /// carries an explicit cap, and a cut is reported, never silent.
    #[tokio::test]
    async fn model_detail_caps_long_sql_and_says_so() {
        use crate::output::MODEL_DETAIL_SQL_CAP_BYTES;

        // A copy of the fixture project with one model padded past the cap.
        // The padding is a SQL comment that ends on a multi-byte character,
        // so the cut also has to land on a char boundary.
        let tmp = tempfile::tempdir().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        for entry in std::fs::read_dir(simple_project_models()).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), models.join(entry.file_name())).unwrap();
        }
        let padded = models.join("raw_orders.sql");
        let mut sql = std::fs::read_to_string(&padded).unwrap();
        let line = "-- ééééééééééééééééééééééééééééééééééééééééééééé\n";
        while sql.len() <= MODEL_DETAIL_SQL_CAP_BYTES + 4096 {
            sql.push_str(line);
        }
        std::fs::write(&padded, &sql).unwrap();

        let state = ServerState::new(models, None, None);
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models/raw_orders"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: ModelDetailOutput = resp.json().await.unwrap();
        assert!(
            body.sql_truncated,
            "a model past the cap must say it was cut"
        );
        // The loader trims the file's surrounding whitespace before it stores
        // the SQL, so the reported length is the trimmed source's.
        let stored = sql.trim();
        assert_eq!(body.sql_bytes, stored.len(), "the full length is reported");
        assert!(body.sql_bytes > MODEL_DETAIL_SQL_CAP_BYTES);
        assert!(body.sql.len() <= MODEL_DETAIL_SQL_CAP_BYTES);
        assert!(
            stored.starts_with(&body.sql),
            "the served text is a prefix of the source"
        );
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
        let body: DagLayersOutput = resp.json().await.unwrap();
        assert_eq!(body.total_models, 3);
        assert_eq!(body.layers.len(), 3);
        let listed: usize = body.layers.iter().map(Vec::len).sum();
        assert_eq!(
            listed, body.total_models,
            "every model sits in exactly one layer"
        );
    }

    /// The model list is sorted by name, whatever order the graph iterates in.
    #[tokio::test]
    async fn model_list_is_sorted_by_name() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/models")).await.unwrap();
        let body: ModelListOutput = resp.json().await.unwrap();
        let names: Vec<&str> = body.models.iter().map(|m| m.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "{names:?}");
    }

    /// `/dag/status` projects the executor's record field for field: the
    /// served JSON equals the executor's own serialization of the same
    /// record, across every node status.
    #[test]
    fn dag_status_projection_equals_the_executor_serialization() {
        use rocky_core::dag_executor::{DagExecutionResult, NodeResult, NodeStatus};
        use rocky_core::dag_status::DagStatus;

        let statuses = [
            NodeStatus::Pending,
            NodeStatus::Running,
            NodeStatus::Completed,
            NodeStatus::Failed,
            NodeStatus::Skipped,
        ];
        let nodes = statuses
            .iter()
            .enumerate()
            .map(|(i, status)| NodeResult {
                id: format!("n{i}"),
                kind: "model".into(),
                label: format!("node {i}"),
                status: status.clone(),
                layer: i,
                duration_ms: i as u64 * 7,
                error: matches!(status, NodeStatus::Failed).then(|| "boom".to_string()),
            })
            .collect();
        let status = DagStatus {
            completed_at: chrono::Utc::now(),
            result: DagExecutionResult {
                nodes,
                total_layers: 5,
                total_nodes: 5,
                completed: 1,
                failed: 1,
                skipped: 1,
                duration_ms: 70,
            },
        };

        assert_eq!(
            serde_json::to_value(dag_status_output(&status)).unwrap(),
            serde_json::to_value(&status).unwrap()
        );
    }

    // --- /products ---

    /// Both product routes answer with the CLI's bytes for the same project:
    /// `rocky product list --output json` and `rocky product status <name>
    /// --output json`, pretty-printed. An unknown name is the documented 404.
    #[tokio::test]
    async fn product_routes_match_the_cli_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) =
            crate::commands::product::tests::api_fixture_project(dir.path());
        // Approve once so the store holds records beside the spec file.
        crate::commands::product::product_approve_in(&root, &state_path, "revenue_daily")
            .expect("approves");
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/products"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected = product_list_in(&root, Some(&state_path)).unwrap();
        assert_eq!(text, reference_bytes(&expected));
        let list: ProductListOutput = serde_json::from_str(&text).unwrap();
        assert_eq!(list.count, 1);
        assert_eq!(list.products[0].name, "revenue_daily");
        assert_eq!(
            list.products[0].fulfill_state.as_deref(),
            Some("spec_approved")
        );

        let resp = reqwest::get(format!("{base}/api/v1/products/revenue_daily"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected = product_status_in(&root, Some(&state_path), "revenue_daily").unwrap();
        assert_eq!(text, reference_bytes(&expected));

        let resp = reqwest::get(format!("{base}/api/v1/products/nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "product_not_found");
        assert!(err.remediation_hint.is_some());

        // A traversal-shaped or non-identifier name is a 404 before any path
        // is built from it: the router refuses shapes that do not match one
        // segment (`route_not_found`), and the handler's identifier guard
        // refuses the rest (`product_not_found`). Either way nothing on
        // disk is touched.
        for shape in [
            "..",
            "..%2F..%2Fetc%2Fpasswd",
            "a%5Cb",
            "a.b",
            "-x",
            "a%23b",
        ] {
            let resp = reqwest::get(format!("{base}/api/v1/products/{shape}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 404, "{shape}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert!(
                err.code == "product_not_found" || err.code == "route_not_found",
                "{shape}: {}",
                err.code
            );
        }
    }

    /// A product the store knows but whose spec file is gone is still a
    /// product: listed, and served, with `spec_present = false`.
    #[tokio::test]
    async fn product_routes_serve_a_product_only_the_store_knows() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) =
            crate::commands::product::tests::api_fixture_project(dir.path());
        crate::commands::product::product_approve_in(&root, &state_path, "revenue_daily")
            .expect("approves");
        std::fs::remove_file(root.join("products/revenue_daily.toml")).unwrap();
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        let list: ProductListOutput = reqwest::get(format!("{base}/api/v1/products"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(list.count, 1);
        assert!(!list.products[0].spec_present);

        let resp = reqwest::get(format!("{base}/api/v1/products/revenue_daily"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let status: ProductStatusOutput = resp.json().await.unwrap();
        assert!(!status.spec_present);
        assert!(status.approval.is_some());
    }

    /// A store that cannot be opened for reading — here, a file that is not
    /// a redb database at all — is the documented 500, with the envelope, on
    /// both routes. Nothing is repaired or rewritten on the way.
    #[tokio::test]
    async fn product_routes_report_an_unreadable_store_as_500() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) =
            crate::commands::product::tests::api_fixture_project(dir.path());
        std::fs::write(&state_path, b"this is not a redb database").unwrap();
        let before = std::fs::read(&state_path).unwrap();
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        for path in ["/api/v1/products", "/api/v1/products/revenue_daily"] {
            let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
            assert_eq!(resp.status(), 500, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "internal_error", "{path}");
        }
        assert_eq!(
            std::fs::read(&state_path).unwrap(),
            before,
            "the file is left alone"
        );
    }

    /// A models-only sidecar has no bound config, so it has no product
    /// surface: the documented 503, with the envelope.
    #[tokio::test]
    async fn product_routes_need_a_bound_config() {
        let base = spawn_router(test_state()).await;
        for path in ["/api/v1/products", "/api/v1/products/anything"] {
            let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
            assert_eq!(resp.status(), 503, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "engine_not_ready", "{path}");
        }
    }

    // --- /review ---

    /// A project with two pending escalations, one reviewed plan, and one
    /// decision-only ledger row (a plan id with no plan file). Returns the
    /// root, config, state path, and the three plan ids in that order:
    /// pending A, pending B, reviewed C.
    fn review_fixture(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf, [String; 3]) {
        use crate::commands::review::{record_plan_review_escalation, write_test_review_marker};
        use crate::plan_store::{PlanKind, write_plan};
        use rocky_core::config::{PolicyCapability, PolicyPrincipal};

        let (root, config, state_path) = crate::commands::product::tests::api_fixture_project(dir);
        let plan = |tag: &str| {
            write_plan(
                &root,
                PlanKind::Run,
                &serde_json::json!({ "models": [tag] }),
            )
            .expect("plan written")
        };
        let a = plan("orders");
        let b = plan("customers");
        let c = plan("revenue");
        for (plan_id, model, capability) in [
            (&a, "orders", PolicyCapability::SchemaChangeAdditive),
            (&b, "customers", PolicyCapability::SchemaChangeBreaking),
            (&c, "revenue", PolicyCapability::SchemaChangeAdditive),
        ] {
            record_plan_review_escalation(
                &state_path,
                plan_id,
                PolicyPrincipal::Agent,
                capability,
                model,
                // These fixtures model an ORDINARY row, whose `model` is
                // already the graph key — no separate model set.
                Vec::new(),
                "test escalation",
            );
        }
        // C is signed off, so it leaves the queue but keeps a status.
        write_test_review_marker(&root, &c);
        // A decision-only row: a plan id no file backs, counted, not listed.
        record_plan_review_escalation(
            &state_path,
            &"0".repeat(64),
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            "ghost",
            Vec::new(),
            "decision-only custody row",
        );
        (root, config, state_path, [a, b, c])
    }

    /// Strip the two clock-derived fields from every pending entry and return
    /// their `staleness_seconds` values, so two payloads taken a moment apart
    /// can be compared exactly on everything else.
    fn without_clock(mut queue: serde_json::Value) -> (serde_json::Value, Vec<i64>) {
        let mut staleness = Vec::new();
        if let Some(pending) = queue["pending"].as_array_mut() {
            for entry in pending {
                let obj = entry.as_object_mut().expect("entry object");
                staleness.push(
                    obj.remove("staleness_seconds")
                        .and_then(|v| v.as_i64())
                        .expect("staleness_seconds"),
                );
                obj.remove("score").expect("score");
            }
        }
        (queue, staleness)
    }

    /// `/review/queue` answers with the CLI's bytes for the same project,
    /// modulo the clock: every field equal except `staleness_seconds` and
    /// `score`, and the staleness values within five seconds.
    #[tokio::test]
    async fn review_queue_matches_the_cli_modulo_the_clock() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, [a, b, c]) = review_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config.clone()),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/review/queue"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let served: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(served["total"], 2, "{text}");
        assert_eq!(served["excluded_non_plan_rows"], 1);
        let listed: Vec<&str> = served["pending"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["plan_id"].as_str().unwrap())
            .collect();
        assert!(listed.contains(&a.as_str()) && listed.contains(&b.as_str()));
        assert!(
            !listed.contains(&c.as_str()),
            "a reviewed plan leaves the queue"
        );
        // Pretty-printed with a trailing newline, like every canonical route.
        assert!(text.ends_with("}\n"), "{text}");

        let expected =
            compute_review_queue(&root, &config, &state_path, &root.join("models")).unwrap();
        let (served_json, served_stale) = without_clock(served);
        let (expected_json, expected_stale) =
            without_clock(serde_json::to_value(&expected).unwrap());
        assert_eq!(served_json, expected_json);
        for (s, e) in served_stale.iter().zip(&expected_stale) {
            assert!((s - e).abs() <= 5, "staleness drifted: {s} vs {e}");
        }
    }

    /// A project whose pipeline names a REMOTE adapter, so the samples route's
    /// consent gate is the thing under test rather than DuckDB's exemption.
    /// The gate runs before any compile and before the adapter is built, so
    /// this needs no credentials and reaches no warehouse.
    fn remote_adapter_project(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
        let root = dir.join("remote");
        std::fs::create_dir_all(root.join("models")).unwrap();
        let config = root.join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"databricks\"\nhost = \"example.invalid\"\n\
             http_path = \"/sql/1.0/warehouses/x\"\ntoken = \"unused\"\n\n\
             [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.main.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = root.join("models/.rocky-state.redb");
        (root, config, state_path)
    }

    /// The samples route's three refusals that need no warehouse: the row cap,
    /// the consent gate on a remote adapter, and an unknown model.
    #[tokio::test]
    async fn samples_refuses_a_bad_limit_a_missing_consent_and_an_unknown_model() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) = remote_adapter_project(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;
        let client = reqwest::Client::new();

        // The cap is checked before anything else, so it answers even on a
        // project whose adapter would refuse.
        for limit in ["0", "501", "100000"] {
            let resp = client
                .get(format!("{base}/api/v1/models/orders/rows?limit={limit}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400, "limit={limit}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "bad_request", "limit={limit}");
        }

        // A remote adapter without the consent header is refused BEFORE the
        // compile, so an unknown model still answers 403 rather than 404 —
        // which is the proof that the gate runs first.
        let resp = client
            .get(format!("{base}/api/v1/models/orders/rows"))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(status, 403, "{}: {}", err.code, err.message);
        assert_eq!(err.code, "warehouse_gated");

        // Consent is exactly the literal `true`, case-insensitively. With it the
        // request gets past the gate and fails later, on the project; without
        // it the gate refuses first, whatever else is wrong.
        for (value, consents) in [
            ("true", true),
            ("TRUE", true),
            ("false", false),
            ("1", false),
        ] {
            let resp = client
                .get(format!("{base}/api/v1/models/orders/rows"))
                .header("x-rocky-allow-warehouse", value)
                .send()
                .await
                .unwrap();
            if consents {
                assert_ne!(resp.status(), 403, "consent header {value:?} was ignored");
            } else {
                assert_eq!(resp.status(), 403, "consent header {value:?} was accepted");
            }
        }
    }

    /// #1816. The sample deadline could not fire during the compile. Config
    /// loading and the compiler run inside `compute_preview_rows` are
    /// synchronous, and a Tokio timeout is checked only between polls, so a
    /// compile that took a minute held the request, a runtime worker and the
    /// sample permit for the minute, and `504 sample_timeout` was never sent
    /// at 30 seconds. The blocking stage now runs on the blocking pool, where
    /// the deadline can be observed; the permit rides with it, so a compile
    /// the route stopped waiting for still keeps a second sample out until it
    /// returns.
    ///
    /// A test hook holds the blocking stage for longer than the deadline. The
    /// clock is the discriminator: on the pre-fix shape the answer arrives
    /// after the whole hold, not after the deadline.
    #[tokio::test]
    async fn the_sample_deadline_fires_during_a_slow_compile_and_the_permit_outlives_it() {
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) = remote_adapter_project(dir.path());
        let hold = Duration::from_millis(1500);
        *crate::commands::PREPARE_HOLD_FOR_TEST.lock().unwrap() = Some((config.clone(), hold));
        let state = pinned_server(root.join("models"), Some(config), &state_path);
        state.set_sample_timeout(Duration::from_millis(100));
        let base = spawn_router(Arc::clone(&state)).await;
        let client = reqwest::Client::new();

        let started = Instant::now();
        let resp = client
            .get(format!("{base}/api/v1/models/orders/rows"))
            .header("x-rocky-allow-warehouse", "true")
            .send()
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(resp.status(), 504, "answered after {elapsed:?}");
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "sample_timeout");
        assert!(
            elapsed < hold,
            "the deadline waited for the compile: answered after {elapsed:?}, \
             and the compile holds for {hold:?}"
        );

        // The compile the route stopped waiting for still holds the permit,
        // so a second sample is refused rather than compiled beside it...
        let second = client
            .get(format!("{base}/api/v1/models/orders/rows"))
            .header("x-rocky-allow-warehouse", "true")
            .send()
            .await
            .unwrap();
        assert_eq!(second.status(), 503);
        let err: ErrorEnvelope = second.json().await.unwrap();
        assert_eq!(err.code, "engine_busy");

        // ...and releases it when the compile returns, not never.
        let deadline = Instant::now() + Duration::from_secs(10);
        while state.warehouse_samples.available_permits() == 0 {
            assert!(
                Instant::now() < deadline,
                "the permit never came back after the orphaned compile"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// One sample at a time: the second is refused at once, with `Retry-After`,
    /// rather than queued behind a call that may run the full 30 seconds.
    #[tokio::test]
    async fn a_second_concurrent_sample_is_refused_immediately_with_retry_after() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) = remote_adapter_project(dir.path());
        let state = pinned_server(root.join("models"), Some(config), &state_path);
        let held = Arc::clone(&state.warehouse_samples)
            .try_acquire_owned()
            .expect("the permit starts free");
        let base = spawn_router(state).await;

        let resp = reqwest::Client::new()
            .get(format!("{base}/api/v1/models/orders/rows"))
            .header("x-rocky-allow-warehouse", "true")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            Some("5"),
            "a refusal the caller should retry carries how long to wait"
        );
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_busy");
        drop(held);
    }

    /// The diff answers the bytes `compute_review` produces for the same plan,
    /// and refuses a plan whose kind is never review-gated with the CLI's own
    /// reason. The fixture is not a git repository, so the base compile finds
    /// no `HEAD` and the findings come back absent — which both sides report
    /// identically, and which is the parity under test.
    #[tokio::test]
    async fn review_diff_matches_the_core_and_refuses_an_ungated_kind() {
        use crate::plan_store::{PlanKind, write_plan};

        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, [human_authored, ..]) = review_fixture(dir.path());
        // An AI-authored plan is review-gated by kind; the fixture's own plans
        // are `Run` with the default human principal, which is not.
        let gated = write_plan(
            &root,
            PlanKind::AiAuthored,
            &serde_json::json!({ "models": ["orders"] }),
        )
        .expect("plan written");
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config.clone()),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/review/{gated}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected = crate::commands::compute_review(&root, &config, &gated, "HEAD", false)
            .await
            .expect("the core computes the same diff");
        assert_eq!(text, reference_bytes(&expected));
        let diff: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(diff["approved"], false, "the route must never approve");

        // A kind the review flow does not gate: the CLI refuses it, so the
        // route answers 409 rather than pretending there is a diff.
        let resp = reqwest::get(format!("{base}/api/v1/review/{human_authored}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "plan_not_reviewable");
    }

    /// The review diff refuses the same two shapes the status route does, and
    /// a 404 must not create the plans directory — `read_plan` would.
    #[tokio::test]
    async fn review_diff_refuses_unknown_plans_without_creating_anything() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = review_fixture(dir.path());
        let plans_dir = root.join(".rocky/plans");
        let before: Vec<_> = std::fs::read_dir(&plans_dir)
            .map(|entries| entries.filter_map(Result::ok).map(|e| e.path()).collect())
            .unwrap_or_default();
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        for shape in ["not-hex", &"f".repeat(64), &"A".repeat(64)] {
            let resp = reqwest::get(format!("{base}/api/v1/review/{shape}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 404, "{shape}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "plan_not_found", "{shape}");
        }

        let after: Vec<_> = std::fs::read_dir(&plans_dir)
            .map(|entries| entries.filter_map(Result::ok).map(|e| e.path()).collect())
            .unwrap_or_default();
        assert_eq!(before, after, "a 404 wrote to the plans directory");
    }

    /// The diff permit admits one caller; a second waits and is then refused
    /// with `Retry-After`, never queued indefinitely.
    #[tokio::test]
    async fn a_second_concurrent_review_diff_is_refused_after_its_wait() {
        use crate::plan_store::{PlanKind, write_plan};

        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = review_fixture(dir.path());
        // A plan the route would otherwise answer 200 for, so the 503 below is
        // the permit refusing and not the reviewability guard.
        let a = write_plan(
            &root,
            PlanKind::AiAuthored,
            &serde_json::json!({ "models": ["orders"] }),
        )
        .expect("plan written");
        let state = pinned_server(root.join("models"), Some(config), &state_path);
        let held = Arc::clone(&state.review_diffs)
            .try_acquire_owned()
            .expect("the permit starts free");
        let base = spawn_router(state).await;

        let started = std::time::Instant::now();
        let resp = reqwest::get(format!("{base}/api/v1/review/{a}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
        assert!(
            started.elapsed() >= REVIEW_DIFF_WAIT,
            "the caller was refused before its wait elapsed"
        );
        assert_eq!(
            resp.headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            Some("2"),
        );
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_busy");
        drop(held);
    }

    /// Both live-compute routes are advertised, so an embedder can feature-
    /// detect them instead of probing.
    #[tokio::test]
    async fn meta_advertises_the_live_compute_capabilities() {
        let base = spawn_router(test_state()).await;
        let meta: serde_json::Value = reqwest::get(format!("{base}/api/v1/meta"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let caps = meta["capabilities"].as_array().unwrap();
        for capability in ["review_diff", "samples"] {
            assert!(
                caps.iter().any(|c| c == capability),
                "missing {capability}: {caps:?}"
            );
        }
    }

    /// `/review/{plan_id}/status` answers with the CLI's bytes for a pending
    /// and a reviewed plan, and the three refusals carry their codes.
    #[tokio::test]
    async fn review_status_matches_the_cli_and_refuses_with_its_codes() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, [a, _b, c]) = review_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        for (plan_id, reviewed) in [(&a, false), (&c, true)] {
            let resp = reqwest::get(format!("{base}/api/v1/review/{plan_id}/status"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{plan_id}");
            let text = resp.text().await.unwrap();
            let expected = compute_review_status(&root, plan_id).unwrap();
            assert_eq!(text, reference_bytes(&expected));
            let status: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(status["reviewed"], reviewed, "{plan_id}");
        }

        // Not a plan id at all, and a well-formed id with no plan file.
        for shape in ["not-hex", "..%2F..", &"f".repeat(64)] {
            let resp = reqwest::get(format!("{base}/api/v1/review/{shape}/status"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 404, "{shape}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert!(
                err.code == "plan_not_found" || err.code == "route_not_found",
                "{shape}: {}",
                err.code
            );
        }

        // A marker that is not a review marker: the documented 409, with the
        // CLI's own reason in the message.
        let marker = crate::commands::apply::review_marker_path(&root, &a);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, b"not json").unwrap();
        let resp = reqwest::get(format!("{base}/api/v1/review/{a}/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "review_marker_malformed");
        assert!(err.message.contains("does not parse"), "{}", err.message);
    }

    /// Without a bound config there is no project root: the documented 503
    /// on both review routes.
    #[tokio::test]
    async fn review_routes_need_a_bound_config() {
        let base = spawn_router(test_state()).await;
        let id = "a".repeat(64);
        for path in [
            "/api/v1/review/queue".to_string(),
            format!("/api/v1/review/{id}/status"),
        ] {
            let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
            assert_eq!(resp.status(), 503, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "engine_not_ready", "{path}");
        }
    }

    // --- The governor routes: brief, scorecard, custody ---

    /// The review fixture plus what the governor projections need: one run
    /// an hour ago that executed `orders`, and a decision-only `freeze:global`
    /// row half an hour ago. Both sit well inside every relative window.
    fn governor_fixture(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf, [String; 3]) {
        use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
        use rocky_core::state::{
            ModelExecution, PolicyDecisionRecord, RunRecord, RunStatus, RunTrigger, SessionSource,
            StateStore,
        };

        let (root, config, state_path, plans) = review_fixture(dir);
        let now = chrono::Utc::now();
        let started = now - chrono::Duration::hours(1);
        let store = StateStore::open(&state_path).expect("store");
        store
            .record_run(&RunRecord {
                run_id: "run-orders-1".to_string(),
                started_at: started,
                finished_at: started + chrono::Duration::minutes(1),
                status: RunStatus::Success,
                models_executed: vec![ModelExecution {
                    model_name: "orders".to_string(),
                    started_at: started,
                    finished_at: started + chrono::Duration::seconds(10),
                    duration_ms: 10_000,
                    rows_affected: Some(10),
                    status: "success".to_string(),
                    sql_hash: "h".to_string(),
                    skip_hash: None,
                    upstream_freshness: None,
                    bytes_scanned: None,
                    bytes_written: None,
                    tenant: None,
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: None,
                    env_hash: None,
                    hash_scheme: None,
                    output_column_hashes: None,
                    attempts: Vec::new(),
                }],
                trigger: RunTrigger::Manual,
                config_hash: "c".to_string(),
                triggering_identity: None,
                session_source: SessionSource::Cli,
                git_commit: None,
                git_branch: None,
                idempotency_key: None,
                target_catalog: None,
                hostname: "host".to_string(),
                rocky_version: "0.0.0-test".to_string(),
                check_outcomes: Vec::new(),
                pipeline: None,
                submission_id: None,
                check_gate_failed: false,
                verify_after_failed: false,
            })
            .expect("run recorded");
        store
            .record_policy_decision(&PolicyDecisionRecord {
                keys_recorded: false,
                models: Vec::new(),
                timestamp: now - chrono::Duration::minutes(30),
                plan_id: "freeze:global".to_string(),
                principal: PolicyPrincipal::Human,
                capability: PolicyCapability::Apply,
                model: "*".to_string(),
                effect: PolicyEffect::Deny,
                rule_id: Some(0),
                reason: "test freeze".to_string(),
                verify_after: Vec::new(),
                auto_apply: None,
            })
            .expect("decision recorded");
        drop(store);
        (root, config, state_path, plans)
    }

    /// Strip the clock-derived fields of a digest and return its
    /// `generated_at` instant, so two digests a moment apart compare exactly
    /// on everything else.
    fn brief_without_clock(
        mut brief: serde_json::Value,
    ) -> (serde_json::Value, chrono::DateTime<chrono::Utc>) {
        let obj = brief.as_object_mut().expect("brief object");
        let generated_at = obj
            .remove("generated_at")
            .and_then(|v| v.as_str().map(str::to_string))
            .expect("generated_at");
        obj.remove("since_timestamp");
        let at = chrono::DateTime::parse_from_rfc3339(&generated_at)
            .expect("rfc3339")
            .with_timezone(&chrono::Utc);
        (brief, at)
    }

    fn rfc3339(value: &serde_json::Value) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(value.as_str().expect("timestamp string"))
            .expect("rfc3339")
            .with_timezone(&chrono::Utc)
    }

    /// `/brief` answers with the CLI's bytes for each window, modulo the two
    /// clock fields, defaults to `7d`, never advances the cursor, and refuses
    /// an unknown window with the envelope.
    /// Sixty-four concurrent reads of the store-backed routes all answer
    /// `200`. Every one of them opens the store for the request, and redb's
    /// exclusive flock made two of this process's own reads race for it: the
    /// loser polled five times and answered `503 engine_busy`. The load test
    /// (`scripts/serve-ceiling.py`) measured that at two clients, so the
    /// in-process read queue exists; this pins it. Without the queue this
    /// test fails on the first run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_store_reads_never_answer_busy() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;
        let routes = [
            "/api/v1/runs",
            "/api/v1/schedule",
            "/api/v1/audit",
            "/api/v1/products",
            "/api/v1/brief",
            "/api/v1/audit/scorecard",
        ];
        let client = reqwest::Client::new();
        let tasks: Vec<_> = (0..64)
            .map(|n| {
                let client = client.clone();
                let url = format!("{base}{}", routes[n % routes.len()]);
                tokio::spawn(async move {
                    let status = client.get(&url).send().await.unwrap().status().as_u16();
                    (url, status)
                })
            })
            .collect();
        let mut failed = Vec::new();
        for task in tasks {
            let (url, status) = task.await.unwrap();
            if status != 200 {
                failed.push((url, status));
            }
        }
        assert!(
            failed.is_empty(),
            "concurrent reads answered non-200: {failed:?}"
        );
    }

    #[tokio::test]
    async fn brief_matches_the_cli_modulo_the_clock_and_never_moves_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config.clone()),
            &state_path,
        ))
        .await;

        for (since, mode) in [
            ("last", BriefSince::Last),
            ("24h", BriefSince::Hours24),
            ("7d", BriefSince::Days7),
        ] {
            let resp = reqwest::get(format!("{base}/api/v1/brief?since={since}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{since}");
            let text = resp.text().await.unwrap();
            assert!(text.ends_with("}\n"), "{text}");
            let served: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(served["since_mode"], since);
            assert_eq!(served["runs"]["availability"], "available", "{since}");
            assert_eq!(
                served["escalations"]["availability"], "available",
                "{since}"
            );
            let expected =
                compute_brief(&root, &state_path, &config, mode, chrono::Utc::now()).unwrap();
            let (served_json, served_at) = brief_without_clock(served);
            let (expected_json, expected_at) =
                brief_without_clock(serde_json::to_value(&expected).unwrap());
            assert_eq!(served_json, expected_json, "{since}");
            assert!(
                (expected_at - served_at).num_seconds().abs() <= 5,
                "{since}: generated_at drifted"
            );
        }

        // No query at all is the MCP tool's `7d`, not the CLI's `last`.
        let served: serde_json::Value = reqwest::get(format!("{base}/api/v1/brief"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(served["since_mode"], "7d");

        // Three reads, one of them `last`: the cursor is still unset.
        let cursor = rocky_core::state::StateStore::open_read_only(&state_path)
            .unwrap()
            .get_last_brief_at()
            .unwrap();
        assert!(
            cursor.is_none(),
            "a read must never advance the brief cursor: {cursor:?}"
        );

        let resp = reqwest::get(format!("{base}/api/v1/brief?since=yesterday"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "bad_request");
        assert!(err.message.contains("yesterday"), "{}", err.message);
        assert!(err.remediation_hint.is_some());
    }

    /// `/audit/scorecard` answers with the CLI's bytes for every grouping,
    /// defaults to `principal` over `all`, differs only in `window_start` for
    /// a duration window, and refuses the two usage errors with the envelope.
    #[tokio::test]
    async fn scorecard_matches_the_cli_bytes_and_refuses_bad_queries() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        for (by, dimension) in [
            ("principal", ScorecardDimension::Principal),
            ("rule", ScorecardDimension::Rule),
            ("scope", ScorecardDimension::Scope),
        ] {
            let resp = reqwest::get(format!("{base}/api/v1/audit/scorecard?by={by}&window=all"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{by}");
            let text = resp.text().await.unwrap();
            let expected = compute_audit_scorecard(&state_path, dimension, Some("all")).unwrap();
            assert_eq!(text, reference_bytes(&expected), "{by}");
            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(json["by"], by);
            assert_eq!(json["availability"], "available", "{by}");
        }

        let text = reqwest::get(format!("{base}/api/v1/audit/scorecard"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let expected =
            compute_audit_scorecard(&state_path, ScorecardDimension::Principal, None).unwrap();
        assert_eq!(text, reference_bytes(&expected));

        let resp = reqwest::get(format!("{base}/api/v1/audit/scorecard?window=30d"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let mut served: serde_json::Value = resp.json().await.unwrap();
        let mut expected = serde_json::to_value(
            compute_audit_scorecard(&state_path, ScorecardDimension::Principal, Some("30d"))
                .unwrap(),
        )
        .unwrap();
        let served_start = served
            .as_object_mut()
            .unwrap()
            .remove("window_start")
            .expect("window_start");
        let expected_start = expected
            .as_object_mut()
            .unwrap()
            .remove("window_start")
            .expect("window_start");
        assert_eq!(served, expected);
        assert!(
            (rfc3339(&expected_start) - rfc3339(&served_start))
                .num_seconds()
                .abs()
                <= 5
        );

        for query in ["by=team", "window=fortnight"] {
            let resp = reqwest::get(format!("{base}/api/v1/audit/scorecard?{query}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 400, "{query}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "bad_request", "{query}");
            assert!(err.remediation_hint.is_some(), "{query}");
        }
        // The window refusal carries the CLI's own message.
        let err: ErrorEnvelope =
            reqwest::get(format!("{base}/api/v1/audit/scorecard?window=fortnight"))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
        assert!(err.message.contains("fortnight"), "{}", err.message);
    }

    /// `/custody/{subject}` answers with the CLI's bytes for a model, a run,
    /// a plan on disk, a decision-only custody id and a subject nothing
    /// references; an over-long subject is refused before anything is read.
    #[tokio::test]
    async fn custody_matches_the_cli_bytes_for_every_subject_kind() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, [a, _b, _c]) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config.clone()),
            &state_path,
        ))
        .await;
        let models = root.join("models");

        for (subject, kind, resolved) in [
            ("orders", "model", true),
            ("run-orders-1", "run", true),
            (a.as_str(), "plan", true),
            ("freeze:global", "plan", true),
            ("nothing_here", "model", false),
        ] {
            let resp = reqwest::get(format!("{base}/api/v1/custody/{subject}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{subject}");
            let text = resp.text().await.unwrap();
            let expected =
                compute_audit_for(&root, &config, &state_path, &models, subject).unwrap();
            assert_eq!(text, reference_bytes(&expected), "{subject}");
            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(json["subject_kind"], kind, "{subject}");
            assert_eq!(json["resolved"], resolved, "{subject}");
        }

        let long = "m".repeat(MAX_CUSTODY_SUBJECT_BYTES + 1);
        let resp = reqwest::get(format!("{base}/api/v1/custody/{long}"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "bad_request");

        // No subject at all is no route.
        let resp = reqwest::get(format!("{base}/api/v1/custody/"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
    }

    /// `/audit` answers with the CLI's bytes, whole and scoped to a product,
    /// and the product refusals carry their codes.
    #[tokio::test]
    async fn audit_ledger_matches_the_cli_bytes_whole_and_scoped() {
        use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
        use rocky_core::state::{PolicyDecisionRecord, StateStore};

        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = governor_fixture(dir.path());
        // One row about the fixture product's output model, `revenue_daily`.
        StateStore::open(&state_path)
            .unwrap()
            .record_policy_decision(&PolicyDecisionRecord {
                keys_recorded: false,
                models: Vec::new(),
                timestamp: chrono::Utc::now() - chrono::Duration::minutes(5),
                plan_id: "plan-revenue-daily".to_string(),
                principal: PolicyPrincipal::Agent,
                capability: PolicyCapability::Apply,
                model: "revenue_daily".to_string(),
                effect: PolicyEffect::Allow,
                rule_id: None,
                reason: "test".to_string(),
                verify_after: Vec::new(),
                auto_apply: None,
            })
            .unwrap();
        // A spec the loader rejects, beside the fixture's valid one.
        std::fs::write(root.join("products/broken.toml"), b"not = [toml").unwrap();
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/audit")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert_eq!(
            text,
            reference_bytes(&compute_audit(&state_path, None).unwrap())
        );
        let whole: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(whole.get("product").is_none(), "{text}");
        assert!(whole["decisions"].as_array().unwrap().len() >= 6, "{text}");

        let resp = reqwest::get(format!("{base}/api/v1/audit?product=revenue_daily"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let scope = resolve_product_scope(&root, "revenue_daily").unwrap();
        assert_eq!(
            text,
            reference_bytes(&compute_audit(&state_path, Some(scope)).unwrap())
        );
        let scoped: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(scoped["product"]["output_model"], "revenue_daily");
        assert_eq!(scoped["decisions"].as_array().unwrap().len(), 1, "{text}");
        assert_eq!(scoped["decisions"][0]["plan_id"], "plan-revenue-daily");

        // No spec, a traversal shape, and a name with a space: 404 each,
        // the last two before any path is built.
        for name in ["nope", "..%2Fx", "not%20a%20name"] {
            let resp = reqwest::get(format!("{base}/api/v1/audit?product={name}"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 404, "{name}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "product_not_found", "{name}");
        }
        let resp = reqwest::get(format!("{base}/api/v1/audit?product=broken"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "product_spec_invalid");
        assert!(err.message.contains("broken"), "{}", err.message);
    }

    /// `/products/{name}/journal` answers with the CLI's bytes: empty for a
    /// product known by its spec alone, one row after an approval; an
    /// unknown or traversal-shaped name is 404.
    #[tokio::test]
    async fn product_journal_matches_the_cli_bytes_and_refuses_unknown_names() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path) =
            crate::commands::product::tests::api_fixture_project(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/products/revenue_daily/journal"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected = product_journal_in(&root, Some(&state_path), "revenue_daily")
            .unwrap()
            .expect("known by its spec");
        assert_eq!(text, reference_bytes(&expected));
        let empty: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(empty["count"], 0, "{text}");
        assert_eq!(empty["product_id"], "product:revenue_daily");

        // One approval appends one row, which the route shows byte for byte.
        crate::commands::product::product_approve_in(&root, &state_path, "revenue_daily")
            .expect("approved");
        let resp = reqwest::get(format!("{base}/api/v1/products/revenue_daily/journal"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected = product_journal_in(&root, Some(&state_path), "revenue_daily")
            .unwrap()
            .expect("known");
        assert_eq!(text, reference_bytes(&expected));
        let one: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(one["count"], 1, "{text}");
        assert_eq!(one["rows"][0]["seq"], 1);
        assert_eq!(one["rows"][0]["to_state"], "spec_approved");
        assert!(
            one["rows"][0]["spec_digest"]
                .as_str()
                .unwrap()
                .starts_with("sha256:"),
            "{text}"
        );

        for name in ["nope", "..%2F..", "not%20a%20name"] {
            let resp = reqwest::get(format!("{base}/api/v1/products/{name}/journal"))
                .await
                .unwrap();
            assert_eq!(resp.status(), 404, "{name}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert!(
                err.code == "product_not_found" || err.code == "route_not_found",
                "{name}: {}",
                err.code
            );
        }
    }

    /// Without a bound config there is no `products/` to know a name by.
    #[tokio::test]
    async fn product_journal_needs_a_bound_config() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/api/v1/products/revenue_daily/journal"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_not_ready");
    }

    /// Unfiltered, the ledger needs no config; a product filter needs the
    /// project root the config names.
    #[tokio::test]
    async fn audit_ledger_needs_a_bound_config_only_for_a_product() {
        let dir = tempfile::tempdir().unwrap();
        let (root, _config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(root.join("models"), None, &state_path)).await;

        let resp = reqwest::get(format!("{base}/api/v1/audit")).await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.text().await.unwrap(),
            reference_bytes(&compute_audit(&state_path, None).unwrap())
        );

        let resp = reqwest::get(format!("{base}/api/v1/audit?product=revenue_daily"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_not_ready");
    }

    /// The brief and the custody chain need a project root; the scorecard
    /// reads the state store only and answers without a config.
    #[tokio::test]
    async fn governor_routes_need_a_bound_config_except_the_scorecard() {
        let dir = tempfile::tempdir().unwrap();
        let (root, _config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(root.join("models"), None, &state_path)).await;

        for path in ["/api/v1/brief", "/api/v1/custody/orders"] {
            let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
            assert_eq!(resp.status(), 503, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "engine_not_ready", "{path}");
        }

        let resp = reqwest::get(format!("{base}/api/v1/audit/scorecard"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let expected =
            compute_audit_scorecard(&state_path, ScorecardDimension::Principal, None).unwrap();
        assert_eq!(text, reference_bytes(&expected));
    }

    /// `/dag/status` projects the executor's record field for field, with
    /// the node status rendered in `snake_case` as the executor serializes it.
    #[tokio::test]
    async fn dag_status_is_typed_and_snake_case() {
        use rocky_core::dag_executor::{DagExecutionResult, NodeResult, NodeStatus};

        let state = test_state();
        let base = spawn_router(state.clone()).await;

        // Nothing recorded yet: the documented 503, with the envelope.
        let resp = reqwest::get(format!("{base}/api/v1/dag/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_not_ready");

        state
            .dag_status
            .set(DagExecutionResult {
                nodes: vec![
                    NodeResult {
                        id: "raw_orders".into(),
                        kind: "model".into(),
                        label: "raw_orders".into(),
                        status: NodeStatus::Completed,
                        layer: 0,
                        duration_ms: 12,
                        error: None,
                    },
                    NodeResult {
                        id: "customer_orders".into(),
                        kind: "model".into(),
                        label: "customer_orders".into(),
                        status: NodeStatus::Failed,
                        layer: 1,
                        duration_ms: 3,
                        error: Some("boom".into()),
                    },
                ],
                total_layers: 2,
                total_nodes: 2,
                completed: 1,
                failed: 1,
                skipped: 0,
                duration_ms: 15,
            })
            .await;

        let resp = reqwest::get(format!("{base}/api/v1/dag/status"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        let body: DagStatusOutput = serde_json::from_str(&text).unwrap();
        assert_eq!(body.result.total_nodes, 2);
        assert_eq!(body.result.failed, 1);
        assert_eq!(body.result.nodes[0].status, DagNodeStatusOutput::Completed);
        assert_eq!(body.result.nodes[1].status, DagNodeStatusOutput::Failed);
        assert_eq!(body.result.nodes[1].error.as_deref(), Some("boom"));
        assert!(body.result.nodes[0].error.is_none());
        // The wire rendering is the executor's `snake_case`, not the variant name.
        assert!(text.contains("\"status\": \"completed\""), "{text}");
        assert!(!text.contains("Completed"), "{text}");
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
        // The five typed estate routes are advertised as one capability.
        assert!(
            body.capabilities.iter().any(|c| c == "estate"),
            "{:?}",
            body.capabilities
        );
        // So are the product, review and governor routes, and all seven are
        // registered.
        for capability in ["products", "review", "governor", "audit", "journal"] {
            assert!(
                body.capabilities.iter().any(|c| c == capability),
                "{capability}: {:?}",
                body.capabilities
            );
        }
        for route in [
            "GET /api/v1/products",
            "GET /api/v1/products/{name}",
            "GET /api/v1/products/{name}/journal",
            "GET /api/v1/review/queue",
            "GET /api/v1/review/{plan_id}/status",
            "GET /api/v1/brief",
            "GET /api/v1/audit/scorecard",
            "GET /api/v1/custody/{subject}",
            "GET /api/v1/audit",
            "GET /api/v1/project",
        ] {
            assert!(body.routes.iter().any(|r| r == route), "{route} missing");
        }
        // No config bound in this fixture (models-only ServerState).
        assert!(body.config_hash.is_none());
    }

    // --- The browser UI: `rocky serve --ui` ---

    /// A `--ui` server: a read-only token, an in-memory file set standing in
    /// for the embedded one, and the given allowed hosts and origins.
    fn ui_state(allowed_hosts: &[&str], allowed_origins: &[&str]) -> Arc<ServerState> {
        use rocky_server::auth::{ServeToken, TokenScope};
        use rocky_server::ui::{InMemoryAssets, UiConfig};

        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "index.html".to_string(),
            b"<!doctype html><div id=root></div>".to_vec(),
        );
        files.insert(
            "assets/index-abc123.js".to_string(),
            b"console.log('rocky')".to_vec(),
        );
        ServerState::with_auth_and_webhook(
            simple_project_models(),
            false,
            None,
            None,
            Some(ServeToken {
                secret: "s3cret".to_string(),
                scope: TokenScope::ReadOnly,
            }),
            allowed_origins.iter().map(ToString::to_string).collect(),
            None,
            None,
            Some(UiConfig {
                bind_host: "127.0.0.1".to_string(),
                allowed_hosts: allowed_hosts.iter().map(ToString::to_string).collect(),
                assets: Arc::new(InMemoryAssets(files)),
            }),
        )
    }

    /// The UI files are public and carry every security header; a client
    /// route deep-links to the shell; the API behind them still needs the
    /// token, and the read-only token still cannot mutate.
    #[tokio::test]
    async fn ui_files_are_public_carry_the_headers_and_the_api_stays_behind_the_token() {
        let base = spawn_router(ui_state(&[], &[])).await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let resp = client.get(format!("{base}/ui/")).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.headers()["content-type"], "text/html; charset=utf-8");
        assert_eq!(resp.headers()["cache-control"], "no-cache");
        for (name, value) in rocky_server::ui::UI_SECURITY_HEADERS {
            assert_eq!(
                resp.headers().get(*name).and_then(|v| v.to_str().ok()),
                Some(*value),
                "{name}"
            );
        }
        assert!(resp.text().await.unwrap().contains("id=root"));

        let resp = client
            .get(format!("{base}/ui/assets/index-abc123.js"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()["content-type"],
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            resp.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
        assert!(resp.headers().contains_key("content-security-policy"));

        let resp = client
            .get(format!("{base}/ui/estate"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "a client route gets the shell");
        assert_eq!(resp.headers()["content-type"], "text/html; charset=utf-8");

        let resp = client
            .get(format!("{base}/ui/assets/missing.js"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "asset_not_found");

        let resp = client.get(format!("{base}/ui")).send().await.unwrap();
        assert_eq!(resp.status(), 308);
        assert_eq!(resp.headers()["location"], "/ui/");

        let resp = client
            .get(format!("{base}/api/v1/meta"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401, "the API stays behind the token");
        let resp = client
            .get(format!("{base}/api/v1/meta"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/api/v1/jobs/run"))
            .bearer_auth("s3cret")
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403, "the UI token never reaches a mutation");
    }

    /// `/api/v1/project` reads the bound config, the compile result and the
    /// state store the server resolved: the run recorded under the pinned
    /// store shows, and the lists carry the config's pipelines and adapters.
    #[tokio::test]
    async fn project_route_reads_the_config_the_compile_and_the_resolved_store() {
        let dir = tempfile::tempdir().unwrap();
        let (root, config, state_path, _) = governor_fixture(dir.path());
        let base = spawn_router(pinned_server(
            root.join("models"),
            Some(config.clone()),
            &state_path,
        ))
        .await;

        let resp = reqwest::get(format!("{base}/api/v1/project"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let text = resp.text().await.unwrap();
        assert!(text.ends_with("}\n"), "{text}");
        let project: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            project["name"],
            root.file_name().unwrap().to_string_lossy().as_ref()
        );
        assert_eq!(project["config_path"], config.display().to_string());
        assert!(project.get("config_error").is_none(), "{text}");
        assert!(
            !project["pipelines"].as_array().unwrap().is_empty(),
            "the fixture config declares a pipeline: {text}"
        );
        assert!(
            !project["adapters"].as_array().unwrap().is_empty(),
            "the fixture config declares an adapter: {text}"
        );
        assert_eq!(project["last_run"]["run_id"], "run-orders-1", "{text}");
        assert_eq!(project["last_run"]["status"], "Success");
        assert_eq!(project["last_run"]["models_executed"], 1);
        assert!(project["diagnostics"]["total"].is_number());

        // No config bound: still 200, with nothing config-derived.
        let base = spawn_router(test_state()).await;
        let project: serde_json::Value = reqwest::get(format!("{base}/api/v1/project"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(project["name"], "rocky");
        assert!(project.get("config_path").is_none());
        assert_eq!(project["pipelines"], serde_json::json!([]));
        assert_eq!(project["adapters"], serde_json::json!([]));

        // A config that does not load: the reason, and empty lists.
        let bad = dir.path().join("broken").join("rocky.toml");
        std::fs::create_dir_all(bad.parent().unwrap()).unwrap();
        std::fs::write(&bad, "this = [is not toml").unwrap();
        let base = spawn_router(pinned_server(root.join("models"), Some(bad), &state_path)).await;
        let project: serde_json::Value = reqwest::get(format!("{base}/api/v1/project"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(project["name"], "broken");
        assert!(project["config_error"].is_string(), "{project}");
        assert_eq!(project["pipelines"], serde_json::json!([]));
    }

    /// #1823. The background compile failed — the project's `models` entry
    /// is a dangling symlink, which the walker refuses since #1817 — and the
    /// route read the absent result as a clean project: `diagnostics: []`,
    /// `has_errors: false`. Now the failure is its own state on the wire,
    /// with the reason, and it clears when a later compile produces a result.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_project_route_reports_a_failed_compile_rather_than_a_clean_project() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("broken-models");
        std::fs::create_dir_all(&root).unwrap();
        let config = root.join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\npath = \"probe.duckdb\"\n\n\
             [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.main.target]\nadapter = \"default\"\n",
        )
        .unwrap();
        let models = root.join("models");
        std::os::unix::fs::symlink(root.join("gone"), &models).unwrap();
        let state_path = root.join("state.redb");
        let state = pinned_server(models.clone(), Some(config), &state_path);
        let base = spawn_router(Arc::clone(&state)).await;

        // The initial compile runs in the background; wait for it to fail.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let project = loop {
            let project: serde_json::Value = reqwest::get(format!("{base}/api/v1/project"))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if project.get("compile_error").is_some() {
                break project;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the failed compile never reached the route: {project}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        assert!(
            project.get("config_error").is_none(),
            "precondition: the config loads, so the only failure is the compile's: {project}"
        );
        let reason = project["compile_error"].as_str().expect("a string reason");
        assert!(
            reason.contains("models"),
            "the reason names what could not be read: {reason}"
        );
        assert_eq!(
            project["diagnostics"]["has_errors"],
            serde_json::json!(true),
            "a project that did not compile is not clean: {project}"
        );
        assert!(
            project.get("models_compiled").is_none(),
            "no count from a compile that produced no result: {project}"
        );
        assert_eq!(project["diagnostics"]["total"], 0, "{project}");

        // Asking for a compile says the compile failed, not "recompiled".
        let client = reqwest::Client::new();
        let triggered: serde_json::Value = client
            .post(format!("{base}/api/v1/compile"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(triggered["status"], "compile_failed", "{triggered}");
        assert!(
            triggered["compile_error"]
                .as_str()
                .is_some_and(|r| r.contains("models")),
            "{triggered}"
        );
        assert!(triggered.get("config_error").is_none(), "{triggered}");

        // Repair the project and recompile through the route: the state clears.
        std::fs::remove_file(&models).unwrap();
        std::fs::create_dir(&models).unwrap();
        std::fs::write(models.join("users.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models.join("users.toml"),
            "name = \"users\"\n\n[target]\ncatalog = \"probe\"\nschema = \"main\"\ntable = \"users\"\n",
        )
        .unwrap();
        let triggered: serde_json::Value = client
            .post(format!("{base}/api/v1/compile"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(triggered["status"], "recompiled", "{triggered}");
        assert!(triggered.get("compile_error").is_none(), "{triggered}");
        let project: serde_json::Value = reqwest::get(format!("{base}/api/v1/project"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(
            project.get("compile_error").is_none(),
            "a compile that produced a result clears the failure: {project}"
        );
        assert_eq!(project["models_compiled"], 1, "{project}");
        assert_eq!(
            project["diagnostics"]["has_errors"],
            serde_json::json!(false)
        );
        let listed: serde_json::Value = reqwest::get(format!("{base}/api/v1/models"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(listed["models"][0]["name"], "users", "{listed}");

        // Break it again. The failure is recorded and the previous result is
        // dropped with it: the model routes answer "not ready" rather than
        // serve the users model as current.
        std::fs::remove_dir_all(&models).unwrap();
        std::os::unix::fs::symlink(root.join("gone"), &models).unwrap();
        let triggered: serde_json::Value = client
            .post(format!("{base}/api/v1/compile"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(triggered["status"], "compile_failed", "{triggered}");
        let resp = reqwest::get(format!("{base}/api/v1/models")).await.unwrap();
        assert_eq!(
            resp.status(),
            503,
            "the previous compile's models are not served as current"
        );
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "engine_not_ready");
        let project: serde_json::Value = reqwest::get(format!("{base}/api/v1/project"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(project["compile_error"].is_string(), "{project}");
        assert!(project.get("models_compiled").is_none(), "{project}");
    }

    /// The server-rendered dashboard is retired: `/dashboard` is no route in
    /// any mode, `/` redirects to the page with `--ui` and is no route
    /// without it.
    #[tokio::test]
    async fn the_dashboard_is_retired_and_root_redirects_only_with_ui() {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let base = spawn_router(test_state()).await;
        for path in ["/dashboard", "/"] {
            let resp = client.get(format!("{base}{path}")).send().await.unwrap();
            assert_eq!(resp.status(), 404, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "route_not_found", "{path}");
        }
        let base = spawn_router(ui_state(&[], &[])).await;
        let resp = client.get(format!("{base}/")).send().await.unwrap();
        assert_eq!(resp.status(), 308);
        assert_eq!(resp.headers()["location"], "/ui/");
        // `/dashboard` is no route in `--ui` mode either; this state holds a
        // token, so the auth-wrapped fallback answers 401 to a token-less
        // probe rather than confirming the path's absence.
        let resp = client
            .get(format!("{base}/dashboard"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        let resp = client
            .get(format!("{base}/dashboard"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "route_not_found");
    }

    /// Without `--ui` there is no `/ui` path at all.
    #[tokio::test]
    async fn without_ui_there_is_no_ui_route() {
        let base = spawn_router(test_state()).await;
        let resp = reqwest::get(format!("{base}/ui/")).await.unwrap();
        assert_eq!(resp.status(), 404);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "route_not_found");
    }

    /// The host guard: a foreign `Host` is 421 and a foreign or opaque
    /// `Origin` is 403, on UI files and API alike, before routing; the
    /// server's own names, an allowed host and an allowed origin pass; a
    /// request with no `Origin` passes; without `--ui` the guard is off.
    #[tokio::test]
    async fn ui_mode_refuses_foreign_hosts_and_origins_before_routing() {
        let base = spawn_router(ui_state(&["ui.internal"], &["https://app.example"])).await;
        let client = reqwest::Client::new();
        // The liveness route is the one exemption: a kubelet probes it with
        // the pod IP as `Host`, which no `--allowed-host` can anticipate.
        for (name, value) in [
            ("host", "10.244.0.7:8080"),
            ("origin", "http://evil.example"),
        ] {
            let resp = client
                .get(format!("{base}/api/v1/health"))
                .header(name, value)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "health must answer any {name}");
        }
        // A request that names nobody is refused, not waved through: HTTP/1.0
        // without a Host header, sent by hand because every client adds one.
        {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let addr = base.trim_start_matches("http://").to_string();
            for (path, expected) in [("/ui/", "421"), ("/api/v1/health", "200")] {
                let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
                stream
                    .write_all(format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes())
                    .await
                    .unwrap();
                let mut buf = Vec::new();
                stream.read_to_end(&mut buf).await.unwrap();
                let head = String::from_utf8_lossy(&buf);
                let status = head.split(' ').nth(1).unwrap_or("");
                assert_eq!(status, expected, "{path} without a Host header: {head}");
            }
        }
        for path in ["/ui/"] {
            let resp = client
                .get(format!("{base}{path}"))
                .header("host", "evil.example")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 421, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "host_not_allowed", "{path}");

            let resp = client
                .get(format!("{base}{path}"))
                .header("origin", "http://evil.example")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 403, "{path}");
            let err: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(err.code, "origin_not_allowed", "{path}");

            let resp = client
                .get(format!("{base}{path}"))
                .header("origin", "null")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 403, "{path}: the opaque origin never passes");
        }
        for (name, value) in [
            ("host", "localhost:9"),
            ("host", "127.0.0.1:9"),
            ("host", "ui.internal"),
            ("origin", "https://app.example"),
            ("origin", "http://127.0.0.1:9"),
            ("origin", "http://localhost:5173"),
        ] {
            let resp = client
                .get(format!("{base}/api/v1/health"))
                .header(name, value)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200, "{name}: {value}");
        }
        let resp = client
            .get(format!("{base}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "no Origin is an ordinary request");

        let base = spawn_router(test_state()).await;
        let resp = client
            .get(format!("{base}/api/v1/health"))
            .header("host", "evil.example")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "without --ui the guard is off");
    }

    /// A body over the limit is refused with the envelope before any
    /// handler; a body under it reaches the handler. Every mode.
    #[tokio::test]
    async fn oversized_bodies_are_413_with_the_envelope() {
        let base = spawn_router(test_state_with_token("s3cret")).await;
        let client = reqwest::Client::new();
        let big = vec![b'x'; crate::ui::MAX_REQUEST_BODY_BYTES + 1];
        let resp = client
            .post(format!("{base}/api/v1/jobs/run"))
            .bearer_auth("s3cret")
            .body(big)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 413);
        let err: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(err.code, "payload_too_large");
        assert!(err.remediation_hint.is_some());

        let small = vec![b'x'; 100 * 1024];
        let resp = client
            .post(format!("{base}/api/v1/jobs/run"))
            .bearer_auth("s3cret")
            .body(small)
            .send()
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            413,
            "a body under the limit reaches the handler"
        );
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
        let state_path = pinned_state_path(&models_dir);
        let state = pinned_server(models_dir.clone(), Some(config_path.clone()), &state_path);
        let base = spawn_router(state).await;
        let resp = reqwest::get(format!("{base}/api/v1/dag")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();

        let reference = reference_bytes(
            &dag_output(
                &config_path,
                &state_path,
                // `pinned_server` builds state the way `serve` without
                // `--models` does, so the reference is `rocky dag` without
                // `--models` too. Passing an explicit dir on this side only
                // would compare two different commands.
                None,
                None,
                None,
                false,
                None,
            )
            .unwrap(),
        );
        assert_eq!(api, reference, "GET /dag must match `rocky dag`");
    }

    /// A project whose transformation pipeline declares a **custom** model root,
    /// so the conventional `models/` directory does not exist at all.
    fn custom_root_dag_project() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let transforms = dir.path().join("transforms");
        std::fs::create_dir_all(&transforms).unwrap();

        std::fs::write(transforms.join("stg.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            transforms.join("stg.toml"),
            "name = \"stg\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"stg\"\n",
        )
        .unwrap();
        // A real column dependency, so column lineage has an edge to assert on
        // rather than only a `Result::is_ok` to check.
        std::fs::write(transforms.join("fct.sql"), "SELECT id FROM stg").unwrap();
        std::fs::write(
            transforms.join("fct.toml"),
            "name = \"fct\"\ndepends_on = [\"stg\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"fct\"\n",
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();

        (dir, config_path)
    }

    /// `GET /api/v1/dag` must match `rocky dag` on a project whose models live
    /// somewhere other than `models/`.
    ///
    /// This is the case the original `parity_dag` structurally could not see. It
    /// passed `models = "models/**"` — the conventional root — so the server's
    /// "override every pipeline with my models dir" behavior picked the same
    /// directory the pipeline had declared anyway, and passing `true` on both
    /// sides of the comparison agreed on the same wrong thing. Here the override
    /// would point at a `models/` that does not exist, so it can only produce an
    /// empty graph (#1261).
    ///
    /// The non-emptiness assertion is load-bearing and comes first: parity
    /// between two empty DAGs is exactly the failure being tested for.
    #[tokio::test]
    async fn parity_dag_custom_models_root() {
        let (dir, config_path) = custom_root_dag_project();
        // What `serve` without `--models` holds: the conventional default,
        // which for this project is a directory that was never created.
        let default_models_dir = dir.path().join("models");
        let state_path = pinned_state_path(dir.path());
        let state = pinned_server(default_models_dir, Some(config_path.clone()), &state_path);
        let base = spawn_router(state).await;
        let resp = reqwest::get(format!("{base}/api/v1/dag")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let api = resp.text().await.unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&api).unwrap();
        let stg = parsed["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["label"] == "stg")
            .expect("the custom-root model must reach the served DAG");

        // Finding 1: the node existing is not enough. Enrichment reads a flat
        // name-keyed model list, and deriving that from the fallback `models/`
        // while building the graph per pipeline produced a correctly-shaped node
        // whose target and strategy were silently `null`.
        assert_eq!(
            stg["target"]["table"], "stg",
            "node must carry its target, not just its name"
        );
        assert_eq!(stg["target"]["schema"], "s");
        assert!(
            !stg["strategy"].is_null(),
            "node must carry its materialization strategy"
        );

        let reference = reference_bytes(
            &dag_output(&config_path, &state_path, None, None, None, false, None).unwrap(),
        );
        assert_eq!(api, reference, "GET /dag must match `rocky dag`");

        // A single custom root is still a root the compiler can read, so
        // `--column-lineage` must keep working here. Asserting on the EDGES,
        // not on `is_ok`: an empty lineage list is also `Ok`, so a bare
        // success check would have passed against the pre-fix code that
        // compiled the wrong directory.
        let lineage = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("one model root must still compile column lineage")
            .column_lineage;
        assert!(
            lineage
                .iter()
                .any(|e| e.source.model == "stg" && e.target.model == "fct"),
            "expected a stg->fct column edge, got {lineage:?}"
        );
    }

    /// Column lineage must be compiled from the same glob-selected model
    /// objects as the DAG, not by re-reading their whole base directory.
    ///
    /// Before #1293 the graph correctly excluded `customers` after full-glob
    /// matching was added, but the lineage path compiled `models/` again. A
    /// valid excluded model leaked an edge into the output; making that same
    /// sidecar malformed erased the selected `orders` lineage instead.
    #[tokio::test]
    async fn dag_column_lineage_uses_only_glob_selected_models() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();

        for (name, sql, depends) in [
            // A bare run variable must be substituted before dependency and
            // lineage parsing, just like the normal directory compiler does.
            ("orders", "SELECT @var(seed) AS id", ""),
            (
                "orders_rollup",
                "SELECT id FROM orders",
                "depends_on = [\"orders\"]\n",
            ),
            (
                "customers",
                "SELECT id FROM orders",
                "depends_on = [\"orders\"]\n",
            ),
        ] {
            std::fs::write(models.join(format!("{name}.sql")), sql).unwrap();
            std::fs::write(
                models.join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n{depends}\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/ord*.sql\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = pinned_state_path(dir.path());

        let output = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("glob-selected DAG lineage must compile");
        let transformation_models: std::collections::HashSet<&str> = output
            .nodes
            .iter()
            .filter(|node| node.kind == "transformation")
            .map(|node| node.label.as_str())
            .collect();
        assert_eq!(
            transformation_models,
            std::collections::HashSet::from(["orders", "orders_rollup"])
        );
        assert!(
            output.column_lineage.iter().any(|edge| {
                edge.source.model == "orders" && edge.target.model == "orders_rollup"
            }),
            "selected-model lineage must remain visible: {:?}",
            output.column_lineage
        );
        for edge in &output.column_lineage {
            assert!(
                transformation_models.contains(edge.source.model.as_str())
                    && transformation_models.contains(edge.target.model.as_str()),
                "lineage endpoint absent from DAG: {edge:?}"
            );
        }

        std::fs::write(models.join("customers.toml"), "name = [\n").unwrap();
        let after_malformed = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("a malformed excluded sidecar must not erase selected lineage");
        assert!(
            after_malformed.column_lineage.iter().any(|edge| {
                edge.source.model == "orders" && edge.target.model == "orders_rollup"
            }),
            "selected lineage disappeared after excluded sidecar broke: {:?}",
            after_malformed.column_lineage
        );
    }

    /// A project with no transformation pipeline at all.
    fn replication_only_project() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter.local]\ntype = \"duckdb\"\n\n\
             [pipeline.load]\ntype = \"replication\"\nstrategy = \"full_refresh\"\n\n\
             [pipeline.load.source]\nadapter = \"local\"\n\n\
             [pipeline.load.source.schema_pattern]\nprefix = \"raw__\"\n\
             separator = \"__\"\ncomponents = [\"source\"]\n\n\
             [pipeline.load.target]\nadapter = \"local\"\n\
             catalog_template = \"warehouse\"\nschema_template = \"analytics\"\n",
        )
        .unwrap();
        (dir, config_path)
    }

    /// Zero model roots is "no lineage exists", not "lineage is unavailable".
    ///
    /// Folding the zero case into the several-roots refusal made
    /// `rocky dag --column-lineage` fail on every replication-only project with
    /// a complaint about "different model directories" it does not have — a
    /// regression against a call that previously succeeded with an empty list.
    #[tokio::test]
    async fn dag_column_lineage_allows_project_with_no_models() {
        let (dir, config_path) = replication_only_project();
        let state_path = pinned_state_path(dir.path());

        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("a project with no transformation models must not refuse lineage");
        assert!(out.column_lineage.is_empty());
    }

    /// #1320: a compile failure must be distinguishable from "no lineage".
    ///
    /// The tolerance is deliberate and stays — a lineage-only problem must not
    /// break `rocky dag` itself. What it may not do is report the failure as
    /// an *answer*: a consumer reading zero edges off a project Rocky cannot
    /// parse would conclude there is nothing to trace.
    #[tokio::test]
    async fn dag_column_lineage_says_so_when_the_compile_failed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("transforms");
        std::fs::create_dir_all(&root).unwrap();

        // Valid sidecar, SQL the parser cannot read.
        std::fs::write(root.join("broken.sql"), "this is not sql at all ;;; ((").unwrap();
        std::fs::write(
            root.join("broken.toml"),
            "name = \"broken\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"broken\"\n",
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = pinned_state_path(dir.path());

        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("a lineage-only compile failure must not fail `rocky dag`");

        assert!(out.column_lineage.is_empty());
        assert!(
            out.column_lineage_unavailable.is_some(),
            "an unparseable project must not report zero edges as an answer"
        );

        // And `rocky dag` without `--column-lineage` is untouched: the
        // tolerance this guards is that a lineage problem cannot break the DAG.
        let plain = dag_output(&config_path, &state_path, None, None, None, false, None)
            .expect("the DAG itself must still build");
        assert!(
            plain.column_lineage_unavailable.is_none(),
            "not asking for lineage is not the same as lineage being unavailable"
        );
    }

    /// The control: a project that genuinely has no lineage says nothing is
    /// wrong, so the flag above means what it claims.
    #[tokio::test]
    async fn dag_column_lineage_reports_available_when_it_really_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("transforms");
        std::fs::create_dir_all(&root).unwrap();
        // One model, no upstream — compiles fine, produces no lineage edge.
        std::fs::write(root.join("solo.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            root.join("solo.toml"),
            "name = \"solo\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"solo\"\n",
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = pinned_state_path(dir.path());

        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("a healthy project must compile");
        assert!(
            out.column_lineage_unavailable.is_none(),
            "a compile that succeeded must report its empty lineage as an answer"
        );
    }

    /// `--column-lineage` must not fail on the ordinary nested layout: staging
    /// models one level down feeding marts at the root.
    ///
    /// The lineage compile now uses the DAG's OWN already-loaded model set
    /// (`compile_preloaded_models`) rather than re-reading the models
    /// directory, so `stg` is no longer invisible to it and the compiler no
    /// longer reports `unknown dependency 'stg'` for this shape.
    ///
    /// A previous revision surfaced that error instead of swallowing it and so
    /// turned this — the single most common project shape — into a hard
    /// failure. That tolerance is still asserted, but the edge itself is now
    /// pinned too: checking node labels alone would stay green if the lineage
    /// silently regressed to the old root-only read.
    #[tokio::test]
    async fn dag_column_lineage_tolerates_nested_model_layout() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("transforms");
        std::fs::create_dir_all(root.join("staging")).unwrap();

        std::fs::write(root.join("staging").join("stg.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            root.join("staging").join("stg.toml"),
            "name = \"stg\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"stg\"\n",
        )
        .unwrap();
        std::fs::write(root.join("fct.sql"), "SELECT id FROM stg").unwrap();
        std::fs::write(
            root.join("fct.toml"),
            "name = \"fct\"\ndepends_on = [\"stg\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"fct\"\n",
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"transforms/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = pinned_state_path(dir.path());

        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("the ordinary nested layout must not fail `--column-lineage`");
        // Both models are in the DAG even though the compile cannot see one.
        let labels: Vec<&str> = out.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.contains(&"stg"), "nested model missing: {labels:?}");
        assert!(labels.contains(&"fct"), "root model missing: {labels:?}");

        // The point of compiling the DAG's own model set: the nested `stg` is
        // visible to lineage, so the `stg -> fct` edge exists. Labels alone
        // would not catch a regression to the old root-only read.
        let edges: Vec<(&str, &str)> = out
            .edges
            .iter()
            .map(|e| (e.from.as_str(), e.to.as_str()))
            .collect();
        assert!(
            edges
                .iter()
                .any(|(f, t)| f.contains("stg") && t.contains("fct")),
            "the nested model must contribute a lineage edge, not just a node: {edges:?}"
        );
    }

    /// A configured-but-empty root is not a second root.
    ///
    /// Counting roots by what *resolves* rather than by what actually yields
    /// models refuses a project whose only models live under one directory,
    /// merely because a sibling pipeline points at an empty one.
    #[tokio::test]
    async fn dag_column_lineage_ignores_roots_that_contribute_nothing() {
        let (dir, config_path) = custom_root_dag_project();
        // A second transformation pipeline whose directory exists but is empty.
        std::fs::create_dir_all(dir.path().join("unused")).unwrap();
        let mut cfg = std::fs::read_to_string(&config_path).unwrap();
        cfg.push_str(
            "\n[pipeline.empty]\ntype = \"transformation\"\nmodels = \"unused/**\"\n\n\
             [pipeline.empty.target.governance]\nauto_create_schemas = true\n",
        );
        std::fs::write(&config_path, cfg).unwrap();

        let state_path = pinned_state_path(dir.path());
        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("an empty root must not count as a second root");
        assert!(
            !out.column_lineage.is_empty(),
            "the contributing root's lineage must still be compiled"
        );
    }

    /// Two transformation pipelines whose model roots differ.
    fn split_root_dag_project() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        // `fct` in the gold root reads `stg` from the silver root, so the
        // fixture exercises a cross-PIPELINE edge, not just two islands.
        for (sub, model, sql, depends) in [
            ("silver", "stg", "SELECT 1 AS id", ""),
            (
                "gold",
                "fct",
                "SELECT id FROM stg",
                "depends_on = [\"stg\"]\n",
            ),
        ] {
            let root = dir.path().join(sub);
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join(format!("{model}.sql")), sql).unwrap();
            std::fs::write(
                root.join(format!("{model}.toml")),
                format!(
                    "name = \"{model}\"\n{depends}\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{model}\"\n"
                ),
            )
            .unwrap();
        }

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.silver]\ntype = \"transformation\"\nmodels = \"silver/**\"\n\n\
             [pipeline.silver.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.gold]\ntype = \"transformation\"\nmodels = \"gold/**\"\n\n\
             [pipeline.gold.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();

        (dir, config_path)
    }

    /// Split model roots now yield real column lineage.
    ///
    /// This used to assert *empty*, on the reasoning that the compiler reads
    /// one root and none covers both. That reasoning expired: lineage compiles
    /// the DAG's own model set rather than re-reading a directory, so a set
    /// spanning several roots is as compilable as one from a single root. The
    /// old behaviour deferred correctness to #1262, which has since closed —
    /// leaving cross-pipeline lineage silently dropped for no live reason.
    ///
    /// The fixture is built for exactly this: `fct` in the gold root reads
    /// `stg` from the silver root.
    #[tokio::test]
    async fn dag_column_lineage_spans_split_model_roots() {
        let (dir, config_path) = split_root_dag_project();
        let state_path = pinned_state_path(dir.path());

        let out = dag_output(&config_path, &state_path, None, None, None, true, None)
            .expect("split model roots must not fail `--column-lineage`");
        assert!(
            out.column_lineage_unavailable.is_none(),
            "several roots is no longer a reason lineage cannot be computed: {:?}",
            out.column_lineage_unavailable
        );
        assert!(
            out.column_lineage
                .iter()
                .any(|e| e.source.model == "stg" && e.target.model == "fct"),
            "the cross-ROOT column edge must be reported, not dropped: {:?}",
            out.column_lineage
        );

        // The DAG itself — the thing #1261 is actually about — is complete, with
        // a node from each root and the cross-pipeline edge between them.
        let labels: Vec<&str> = out.nodes.iter().map(|n| n.label.as_str()).collect();
        assert!(labels.contains(&"stg"), "silver root missing: {labels:?}");
        assert!(labels.contains(&"fct"), "gold root missing: {labels:?}");
        assert!(
            out.edges
                .iter()
                .any(|e| e.from.contains("stg") && e.to.contains("fct")),
            "the cross-pipeline edge must survive: {:?}",
            out.edges
        );
    }

    /// GET `url`, retrying while the endpoint returns a *documented-retryable*
    /// `503 engine_busy`. The three state-backed read routes open the redb
    /// store read-only, and redb takes an unconditional `flock` on open; the
    /// store's open-retry budget is only ~250ms (tuned for the LSP-vs-CLI
    /// keystroke race), which a loaded CI runner can exhaust — so the handler
    /// correctly returns the retryable 503 rather than blocking forever. Real
    /// embedders back off and retry on that 503; the parity assertion should
    /// too, instead of demanding 200 on the first request.
    ///
    /// The contender is not only an external process. `ServerState::with_auth`
    /// spawns `recompile()`, which opens the same store read-only via
    /// `load_cached_source_schemas`, and `open_redb_with_retry` calls
    /// `Database::create` for read-only opens too — so redb's in-process
    /// `DatabaseAlreadyOpen` guard fires between two read-only opens in a
    /// single test process. Rocky's own advisory lock is taken only for
    /// `OpenMode::ReadWrite`, so it does not mediate this.
    ///
    /// The rule: **any** api test whose request can fall through to a durable
    /// read must go through this helper. A test that pre-populates
    /// `state.jobs` and returns from the in-memory registry does not open the
    /// store and is exempt.
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
        // A hermetic, empty state store. The API and the CLI core share ONE
        // pinned path, so they read identical bytes without either side
        // re-resolving.
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);
        // Create + init the (empty) store so open_read_only succeeds.
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());

        let state = pinned_server(models_dir.clone(), None, &state_path);
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

    /// A transformation project whose pipeline declares an every-minute cron
    /// schedule, plus a pinned state path and a `ServerState` bound to the
    /// config. The state store is created up front so the read path sees a real
    /// (empty) store rather than the missing-file branch.
    fn scheduled_project() -> (tempfile::TempDir, PathBuf, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
             [pipeline.sales]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.sales.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.sales.schedule]\ncron = \"* * * * *\"\ntimezone = \"UTC\"\n",
        )
        .unwrap();

        let state_path = pinned_state_path(&models_dir);
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let state = ServerState::with_auth(
            models_dir,
            None,
            Some(config_path.clone()),
            None,
            Vec::new(),
            Some(state_path),
        );
        (dir, config_path, state)
    }

    #[tokio::test]
    async fn schedule_status_reports_a_configured_cron() {
        let (_dir, _config, state) = scheduled_project();
        let base = spawn_router(state).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/schedule")).await;
        assert_eq!(resp.status(), 200);
        let body: ScheduleStatusOutput = resp.json().await.unwrap();

        assert_eq!(body.timezone, "UTC");
        assert_eq!(body.counts.scheduled, 1);
        assert_eq!(body.counts.enabled, 1);
        let p = &body.pipelines[0];
        assert_eq!(p.pipeline, "sales");
        assert_eq!(p.cron.as_deref(), Some("* * * * *"));
        // No tick has run, so the cron has no anchor yet — reported honestly
        // rather than as a fabricated next fire.
        assert!(p.awaiting_first_anchor);
        assert!(p.next_fire_at.is_none());
        assert!(p.last_evaluated_at.is_none());
    }

    /// The bytes the endpoint returns are exactly what the backing function
    /// produces — the endpoint adds no reshaping.
    #[tokio::test]
    async fn schedule_status_matches_the_backing_output() {
        let (_dir, config_path, state) = scheduled_project();
        let state_path = state_path_for(&state);
        let base = spawn_router(state).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/schedule")).await;
        let api = resp.json::<ScheduleStatusOutput>().await.unwrap();

        let rocky_dir = config_path.parent().unwrap().join(".rocky");
        let reference =
            schedule_status_output(&config_path, &state_path, &rocky_dir, chrono::Utc::now())
                .unwrap();
        // `now` differs by the request latency; compare the stable parts.
        assert_eq!(api.timezone, reference.timezone);
        assert_eq!(api.counts.scheduled, reference.counts.scheduled);
        assert_eq!(api.pipelines.len(), reference.pipelines.len());
        assert_eq!(api.pipelines[0].cron, reference.pipelines[0].cron);
    }

    /// The route returns the producer's document, whole. Unlike
    /// `schedule_status`, this output carries no `now`, so every byte is
    /// comparable — a handler that reshaped, filtered or fabricated any part
    /// of it fails here.
    #[tokio::test]
    async fn schedule_spool_matches_the_backing_output() {
        let (dir, config_path, state) = scheduled_project();
        let rocky_dir = dir.path().join(".rocky");

        // Two queued demands and one file that will not parse, so the
        // comparison covers `pending`, `skipped` and `counts` at once.
        for (token, at) in [
            ("delivery-2", "2026-09-10T11:00:00Z"),
            ("delivery-1", "2026-09-10T10:00:00Z"),
        ] {
            rocky_core::schedule::spool::accept(
                &rocky_dir,
                "sales",
                rocky_core::schedule::spool::WebhookKind::Id,
                token,
                "deadbeef",
                chrono::DateTime::parse_from_rfc3339(at)
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            )
            .unwrap();
        }
        std::fs::write(rocky_dir.join("pending-demands/notjson"), b"{ not json").unwrap();

        let base = spawn_router(state).await;
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/schedule/spool")).await;
        assert_eq!(resp.status(), 200);
        // Raw bytes, not a parsed value: `PrettyJson` is `to_string_pretty`
        // plus a newline, so the response is byte-comparable with the
        // producer's own serialisation. Comparing parsed values would accept a
        // route that reordered or reformatted the document.
        let api = resp.text().await.unwrap();

        let reference = crate::commands::compute_schedule_spool(&config_path).unwrap();
        let reference_bytes = serde_json::to_string_pretty(&reference).unwrap() + "\n";

        assert_eq!(
            api, reference_bytes,
            "the route did not return the producer's bytes"
        );
        assert_eq!(reference.counts.pending, 2);
        assert_eq!(reference.counts.skipped, 1);
    }

    /// An unreadable spool is `500 spool_unreadable`, never `200` with an
    /// empty queue. The producer fails closed; this pins that the route does
    /// not undo it.
    #[cfg(unix)]
    #[tokio::test]
    async fn schedule_spool_reports_an_unreadable_spool_as_500() {
        let (dir, _config_path, state) = scheduled_project();
        let rocky_dir = dir.path().join(".rocky");
        std::fs::create_dir_all(&rocky_dir).unwrap();
        // Present (a dangling symlink), but impossible to enumerate.
        std::os::unix::fs::symlink(
            dir.path().join("nowhere"),
            rocky_dir.join("pending-demands"),
        )
        .unwrap();

        let base = spawn_router(state).await;
        let resp = reqwest::Client::new()
            .get(format!("{base}/api/v1/schedule/spool"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            500,
            "an unreadable spool must not answer 200"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["code"], "spool_unreadable");
    }

    const POLICY_PROJECT_CONFIG: &str = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
         [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
         [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
         [policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n\n\
         [[policy.rules]]\nprincipal = \"agent\"\ncapability = \"apply\"\n\
         scope = { contracted = true }\neffect = \"deny\"\n";

    /// A project with a `[policy]` block and one freeze recorded through the
    /// real `rocky policy freeze` path.
    fn policy_project(dir: &std::path::Path) -> (PathBuf, PathBuf, PathBuf) {
        let root = dir.join("project");
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, POLICY_PROJECT_CONFIG).unwrap();
        let state_path = dir.join("state.redb");
        crate::commands::run_policy_freeze(
            &config_path,
            &state_path,
            Some(rocky_core::config::PolicyPrincipal::Agent),
            Some("model=fct_*".to_string()),
            Some("incident 42".to_string()),
            false,
            true,
        )
        .unwrap();
        (models_dir, config_path, state_path)
    }

    /// `GET /api/v1/policy` answers with the CLI's bytes: the rule with its
    /// position, the default posture, the freeze in force, and which sources
    /// were read.
    #[tokio::test]
    async fn policy_show_matches_the_cli_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let (models_dir, config_path, state_path) = policy_project(dir.path());
        let state = pinned_server(models_dir, Some(config_path.clone()), &state_path);
        let base = spawn_router(state).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/policy")).await;
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert_eq!(
            body,
            reference_bytes(
                &crate::commands::compute_policy_show(&config_path, &state_path)
                    .await
                    .unwrap()
            )
        );
        assert!(body.contains("\"source\": \"ledger\""), "{body}");
        assert!(body.contains("incident 42"), "{body}");
        assert!(body.contains("\"id\": 0"), "{body}");
    }

    /// The same byte parity on the OTHER branch: a project with no `[policy]`
    /// block, where neither freeze source is consulted.
    ///
    /// That branch is written twice, once in `compute_policy_show` and once in
    /// this route, because the route reads the ledger under the store permit.
    /// Two copies can drift, and the configured-branch parity test above would
    /// not notice. This is the test that would.
    #[tokio::test]
    async fn policy_show_matches_the_cli_bytes_without_a_policy_block() {
        let dir = tempfile::tempdir().unwrap();
        // A freeze is recorded FIRST, against a config that has a [policy]
        // block, then the block is removed. So the ledger genuinely holds a
        // freeze that neither caller may report as in force.
        let (models_dir, config_path, state_path) = policy_project(dir.path());
        let no_policy = POLICY_PROJECT_CONFIG
            .split("[policy]")
            .next()
            .expect("the fixture has a [policy] block to cut at")
            .to_string();
        std::fs::write(&config_path, &no_policy).unwrap();

        let state = pinned_server(models_dir, Some(config_path.clone()), &state_path);
        let base = spawn_router(state).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/policy")).await;
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert_eq!(
            body,
            reference_bytes(
                &crate::commands::compute_policy_show(&config_path, &state_path)
                    .await
                    .unwrap()
            )
        );
        assert!(
            body.contains("\"not_consulted\""),
            "neither source is consulted without a [policy] block: {body}"
        );
        assert!(
            !body.contains("incident 42"),
            "the recorded freeze must not be reported in force: {body}"
        );
    }

    /// A state store path that is there but cannot be read is a `500`, never
    /// a `200` with no freezes: an empty list would claim nothing is frozen
    /// for a plane whose freezes could not be read at all.
    #[cfg(unix)]
    #[tokio::test]
    async fn policy_show_refuses_an_unreadable_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, POLICY_PROJECT_CONFIG).unwrap();
        let state_path = dir.path().join("state.redb");
        std::os::unix::fs::symlink(dir.path().join("gone.redb"), &state_path).unwrap();
        let state = pinned_server(models_dir, Some(config_path), &state_path);
        let base = spawn_router(state).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/policy")).await;
        assert_eq!(resp.status(), 500);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("cannot be read"),
            "{body}"
        );
    }

    /// A `rocky.toml` the engine cannot parse is a `500 config_invalid`, never a
    /// retryable `503` (retry will not fix a broken config) and never a `200`
    /// with an empty list (which would falsely claim "nothing is scheduled").
    #[tokio::test]
    async fn schedule_status_on_unparseable_config_is_config_invalid() {
        let (dir, config_path, _state) = scheduled_project();
        std::fs::write(&config_path, "this is not valid toml {{{").unwrap();
        // Rebuild the server AFTER corrupting the config so the bound path
        // points at the broken file.
        let models_dir = dir.path().join("models");
        let state_path = pinned_state_path(&models_dir);
        let state = ServerState::with_auth(
            models_dir,
            None,
            Some(config_path),
            None,
            Vec::new(),
            Some(state_path),
        );
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/schedule"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 500);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "config_invalid");
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

    /// An explicit `--state-path` must win over the conventional
    /// `<models>/.rocky-state.redb`. Without this, `rocky --state-path <p> serve
    /// --scheduler` put the scheduler's cursors, claims, and child run history in
    /// a different file than the operator selected — so switching to it from a
    /// `rocky tick` timer silently re-fired occurrences against fresh state.
    // `with_auth` spawns the initial compile, so this needs a runtime.
    #[tokio::test]
    async fn state_path_for_honors_an_explicit_override() {
        let models = simple_project_models();
        let explicit = std::path::PathBuf::from("/tmp/rocky-explicit-state.redb");

        let overridden = ServerState::with_auth(
            models.clone(),
            None,
            None,
            None,
            Vec::new(),
            Some(explicit.clone()),
        );
        assert_eq!(
            state_path_for(&overridden),
            explicit,
            "an explicit --state-path is a hard override",
        );

        // Without an override the conventional resolution is unchanged.
        let default_state =
            ServerState::with_auth(models.clone(), None, None, None, Vec::new(), None);
        assert_eq!(
            state_path_for(&default_state),
            rocky_core::state::resolve_state_path(None, &models).path,
            "no override ⇒ historical behavior",
        );
    }

    fn test_state_with_token(token: &str) -> Arc<ServerState> {
        test_state_with_scoped_token(ServeToken::full(token))
    }

    /// A server whose configured token carries an explicit
    /// [`rocky_server::auth::TokenScope`].
    fn test_state_with_scoped_token(token: ServeToken) -> Arc<ServerState> {
        ServerState::with_auth(
            simple_project_models(),
            None,
            None,
            Some(token),
            Vec::new(),
            None,
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
        let state =
            ServerState::with_auth(simple_project_models(), None, None, None, Vec::new(), None);
        let result = serve(
            state,
            ServeConfig {
                host: "0.0.0.0".to_string(),
                port: 0,
            },
            rocky_core::schedule::Drain::new(),
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
        let state =
            ServerState::with_auth(simple_project_models(), None, None, None, Vec::new(), None);
        let task = tokio::spawn(async move {
            serve(
                state,
                ServeConfig {
                    host: "127.0.0.1".to_string(),
                    port: 0,
                },
                rocky_core::schedule::Drain::new(),
                // Pre-signal readiness so this smoke test starts serving without a
                // separate trigger; it asserts the loopback path keeps running.
                {
                    let r = rocky_core::schedule::Drain::new();
                    r.signal();
                    r
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

    /// FF-WP1 (finding 5) — the apply job's argv threading, pinned at the
    /// extracted pure seam: `expect_spec_digest` becomes the trailing
    /// `--expect-spec-digest <hex>` pair after the positional plan id; without
    /// the field the flag is absent (so a product-bound plan is refused by
    /// the engine's fail-closed gate); run/plan jobs never carry it.
    #[test]
    fn job_apply_argv_threads_expect_spec_digest() {
        let state = std::path::Path::new("/tmp/state.redb");
        let with = JobRequest {
            plan_id: Some("abc123".to_string()),
            expect_spec_digest: Some("sha256:feed".to_string()),
            ..JobRequest::default()
        };
        let args = job_subprocess_args(JobKind::Apply, None, state, &with);
        let args: Vec<String> = args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let flag_pos = args
            .iter()
            .position(|a| a == "--expect-spec-digest")
            .expect("the flag is appended for apply");
        assert_eq!(args[flag_pos + 1], "sha256:feed");
        let plan_pos = args.iter().position(|a| a == "abc123").unwrap();
        assert!(
            plan_pos < flag_pos,
            "the positional plan id precedes the flag: {args:?}"
        );

        // Without the field: no flag — the engine gate stays the fail-safe.
        let without = JobRequest {
            plan_id: Some("abc123".to_string()),
            ..JobRequest::default()
        };
        let args = job_subprocess_args(JobKind::Apply, None, state, &without);
        assert!(
            !args
                .iter()
                .any(|a| a.to_string_lossy() == "--expect-spec-digest"),
            "no expectation, no flag: {args:?}"
        );

        // Run/plan jobs never thread the flag even if the field is set.
        let stray = JobRequest {
            expect_spec_digest: Some("sha256:feed".to_string()),
            ..JobRequest::default()
        };
        for kind in [JobKind::Run, JobKind::Plan] {
            let args = job_subprocess_args(kind, None, state, &stray);
            assert!(
                !args
                    .iter()
                    .any(|a| a.to_string_lossy() == "--expect-spec-digest"),
                "{kind:?} must not thread the apply-only flag: {args:?}"
            );
        }
    }

    /// FF-WP1 (finding 5) — a malformed `expect_spec_digest` (non-string) in
    /// the HTTP body is a structured `400` REJECTED before any permit,
    /// subprocess, or job record — while a well-typed body is accepted past
    /// body parsing (here surfacing as the held-permit 409, which proves
    /// parsing succeeded).
    #[tokio::test]
    async fn job_apply_rejects_malformed_expect_spec_digest_body() {
        let state = test_state();
        let held = state
            .mutation_permit
            .try_acquire("job_incumbent")
            .expect("permit is free");
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        // Non-string digest → 400 from body deserialization (before the
        // permit check — a 409 here would mean the malformed body parsed).
        let resp = client
            .post(format!("{base}/api/v1/jobs/apply"))
            .json(&serde_json::json!({ "plan_id": "abc", "expect_spec_digest": 123 }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "bad_request");
        assert!(
            body.message.contains("invalid job request body"),
            "the 400 names the body failure: {}",
            body.message
        );

        // Control: the well-typed body parses and reaches the permit guard.
        let resp = client
            .post(format!("{base}/api/v1/jobs/apply"))
            .json(&serde_json::json!({
                "plan_id": "abc",
                "expect_spec_digest": "sha256:feed"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            409,
            "a well-typed body parses and reaches the (held) permit guard"
        );

        drop(held);
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
            // A fixture for lifecycle tests, so it is stamped current: an
            // unstamped one would read as pre-redaction and have its payload
            // withheld, which is a different behaviour than these tests mean
            // to exercise.
            redaction_version: Some(rocky_core::state::CURRENT_REDACTION_VERSION),
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
        let state_path = pinned_state_path(&models_dir);

        // A job that was "running" when the (previous) sidecar died.
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_job(&persisted_job("job_inflight", "running"))
                .unwrap();
        }

        // The "restart": `serve` runs the sweep once, before the router serves.
        // The server is built only AFTER the sweep — its spawned `recompile()`
        // opens the same store, and a concurrent open would make this
        // `unwrap()` a new flake.
        let swept = sweep_interrupted_jobs(&state_path).unwrap();
        assert_eq!(swept, 1);

        // A brand-new server (empty in-memory registry) reads the durable record.
        let state = pinned_server(models_dir, None, &state_path);
        assert!(
            state.jobs.get("job_inflight").await.is_none(),
            "the sweep is registry-independent: it reconciles the durable table, \
             and a restarted server starts with a cold cache"
        );
        let base = spawn_router(state).await;

        // This GET misses the in-memory registry and falls through to a durable
        // read, so it races the `recompile()` this server spawned — see
        // `get_retrying_on_busy`. Its sibling
        // `job_running_in_current_process_is_not_swept` upserts first, returns
        // from the registry, and never opens the store, which is why only this
        // one flaked.
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/jobs/job_inflight")).await;
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

    /// The mechanism behind the `job_interrupted_by_restart_is_swept_to_failed`
    /// flake, pinned deterministically.
    ///
    /// redb's `Database::create` is a single-open guard and
    /// `open_redb_with_retry` uses it for read-only opens too, while Rocky's
    /// advisory lock is taken only for `OpenMode::ReadWrite`. So two opens in
    /// **one process** contend, with no second process involved: a durable read
    /// blocked that way must surface as the retryable `503 engine_busy`, never
    /// as a hard failure. Under full-suite load the server's spawned
    /// `recompile()` plays the role the explicit `held` store plays here.
    #[tokio::test]
    async fn state_backed_read_under_held_store_is_retryable_503() {
        use rocky_core::state::StateStore;

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = models_dir.join(".rocky-state.redb");

        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_job(&persisted_job("job_held", "succeeded"))
                .unwrap();
        }

        // Acquire BEFORE the server exists: nothing else holds the store yet,
        // so this acquisition cannot itself lose the race.
        let held = StateStore::open(&state_path).unwrap();

        let state = ServerState::with_auth(
            models_dir,
            None,
            None,
            None,
            Vec::new(),
            Some(state_path.clone()),
        );
        let base = spawn_router(state).await;

        let resp = reqwest::get(format!("{base}/api/v1/jobs/job_held"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            503,
            "a durable read blocked by an in-process open must be retryable"
        );
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "engine_busy");

        drop(held);
        let resp = get_retrying_on_busy(&format!("{base}/api/v1/jobs/job_held")).await;
        assert_eq!(
            resp.status(),
            200,
            "the same read must succeed once the contender releases"
        );
    }

    /// The sweep reconciles ONLY in-flight records: `running` and `queued`
    /// flip to `failed` with the documented error, while terminal
    /// `succeeded`/`failed` history is untouched. An unrecognized state is
    /// neither, and `the_sweep_leaves_an_unrecognized_state_untouched` covers
    /// that third case. A missing state file is a
    /// no-op, not an error (fresh project, nothing ever persisted).
    #[test]
    fn sweep_marks_only_in_flight_jobs_failed() {
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
        let state_path = pinned_state_path(&models_dir);

        // Startup: nothing persisted yet, nothing to sweep.
        assert_eq!(sweep_interrupted_jobs(&state_path).unwrap(), 0);

        // A live submission in THIS process, after the sweep.
        let state = pinned_server(models_dir, None, &state_path);
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

    /// The correctness leg of the job cache's capacity bound: a record the
    /// cache has evicted is still served, byte for byte, from the durable
    /// `jobs` table. Bounding the cache may cost a redb read — it must never
    /// cost an answer, and a miss must not answer differently from a hit.
    #[tokio::test]
    async fn evicted_job_is_served_identically_from_the_durable_table() {
        use rocky_core::state::StateStore;
        use rocky_server::jobs::DEFAULT_JOB_CACHE_CAPACITY;

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);

        // A finished job recorded the way the job paths do it: durable record
        // first, then the in-memory cache.
        let mut record = persisted_job("job_evicted", "succeeded");
        record.finished_at = Some("2026-07-07T00:00:10Z".to_string());
        record.result = Some(serde_json::json!({ "status": "success" }));
        {
            let store = StateStore::open(&state_path).unwrap();
            store.record_job(&record).unwrap();
        }

        let state = pinned_server(models_dir, None, &state_path);
        state.jobs.upsert(record).await;
        let base = spawn_router(state.clone()).await;

        // The hot answer, straight from the cache.
        let hot = reqwest::get(format!("{base}/api/v1/jobs/job_evicted"))
            .await
            .unwrap();
        assert_eq!(hot.status(), 200);
        let hot = hot.text().await.unwrap();

        // Exactly one capacity's worth of newer finished jobs displaces it.
        for i in 0..DEFAULT_JOB_CACHE_CAPACITY {
            state
                .jobs
                .upsert(persisted_job(&format!("filler{i}"), "succeeded"))
                .await;
        }
        assert!(
            state.jobs.get("job_evicted").await.is_none(),
            "the record under test must actually have been evicted"
        );

        // The cold answer, through the durable fallback. This read opens the
        // store and so races the server's spawned `recompile()` — the same
        // contention `job_interrupted_by_restart_is_swept_to_failed` documents.
        let cold = get_retrying_on_busy(&format!("{base}/api/v1/jobs/job_evicted")).await;
        assert_eq!(cold.status(), 200);
        assert_eq!(
            cold.text().await.unwrap(),
            hot,
            "a cache miss must return exactly what the hit returned"
        );
    }

    /// A submission abandoned mid-flight must leave nothing behind in the job
    /// cache.
    ///
    /// Axum drops the handler future when a client disconnects, so submission
    /// can stop at any `.await`. Because the cache never evicts an in-flight
    /// record, caching `running` before an await that may never return would
    /// strand a slot for the life of the process, and a client looping
    /// submit-then-disconnect would rebuild the unbounded growth the capacity
    /// bound exists to stop.
    ///
    /// Driven by polling the future directly and dropping it at its first
    /// suspension — which is the durable write, the same point a disconnect
    /// would realistically land on. Under the previous ordering (cache, then
    /// persist, then spawn) that first poll had already cached `running` and
    /// this assertion fails.
    #[tokio::test]
    async fn a_submission_abandoned_mid_flight_caches_nothing() {
        use std::task::{Context, Waker};

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);
        let state = pinned_server(models_dir, None, &state_path);

        let headers = HeaderMap::new();
        let body = Bytes::new();
        {
            let future = submit_job(JobKind::Plan, state.clone(), &headers, &body);
            let mut future = std::pin::pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            assert!(
                future.as_mut().poll(&mut cx).is_pending(),
                "submission must suspend on the durable write before it is cached"
            );
            // The disconnect: the handler future is dropped where it suspended.
        }

        assert_eq!(
            state.jobs.entry_count().await,
            0,
            "an abandoned submission must cache nothing: the cache never evicts \
             an in-flight record, so a `running` entry with no terminal owner \
             would hold a slot for the life of the process"
        );

        // The permit is released by the dropped guard, so the slot is reusable.
        assert_eq!(
            state.mutation_permit.running_job(),
            None,
            "an abandoned submission must not hold the mutation permit"
        );
    }

    /// The cross-crate stand-in for enum exhaustiveness.
    ///
    /// `rocky-core` classifies a job's lifecycle from the persisted **string**,
    /// because it cannot see this crate's [`JobState`]. So adding a variant
    /// there compiles cleanly while `is_in_flight` silently reads it as
    /// finished — and therefore evictable from the job cache, even if the new
    /// state means live work. This test can see both sides, and its exhaustive
    /// `match` stops compiling the moment a variant is added, forcing the new
    /// state to be classified deliberately on both predicates.
    ///
    /// It also pins the asymmetry: the two predicates are complements for the
    /// four known states and deliberately are NOT for anything else — an
    /// unrecognized string is neither in flight (so it can never defeat the
    /// cache's capacity bound) nor terminal (so this version never CLAIMS such
    /// a job finished). The gap between them is the set of records this version
    /// must not touch; `the_sweep_leaves_an_unrecognized_state_untouched` pins
    /// that the restart sweep honours it.
    #[test]
    fn every_job_state_is_classified_for_both_the_cache_and_the_sweep() {
        for state in [
            JobState::Queued,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
        ] {
            // Exhaustive on purpose: a new variant breaks compilation here.
            let (in_flight, terminal) = match state {
                JobState::Queued | JobState::Running => (true, false),
                JobState::Succeeded | JobState::Failed => (false, true),
            };
            let job = persisted_job("j", job_state_str(state));
            assert_eq!(
                job.is_in_flight(),
                in_flight,
                "{state:?} must classify as in_flight={in_flight}"
            );
            assert_eq!(
                job.is_terminal(),
                terminal,
                "{state:?} must classify as terminal={terminal}"
            );
        }

        // The deliberate asymmetry, which no `JobState` variant can express.
        let unknown = persisted_job("j", "cancelled");
        assert!(
            !unknown.is_in_flight(),
            "an unrecognized state must be evictable, or it could defeat the cache bound"
        );
        assert!(
            !unknown.is_terminal(),
            "an unrecognized state must not read as terminal TO THIS \
             PREDICATE, which classifies the stored string and makes no claim \
             about a state this version does not know. `job_status_from` \
             separately renders it as `failed` — a rendering fallback at the \
             API boundary, not this classification"
        );
    }

    /// A durable record that still reads as in flight must be served but NOT
    /// cached. Persisting is best-effort, so a job whose terminal write lost to
    /// lock contention leaves `running` behind durably; caching that on a miss
    /// would park a record the cache is never allowed to evict, and repeating it
    /// would rebuild the very unbounded growth the capacity bound exists to stop.
    #[tokio::test]
    async fn a_stale_in_flight_durable_record_is_served_but_not_cached() {
        use rocky_core::state::StateStore;

        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);

        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_job(&persisted_job("job_zombie", "running"))
                .unwrap();
        }

        let state = pinned_server(models_dir, None, &state_path);
        let base = spawn_router(state.clone()).await;

        let resp = get_retrying_on_busy(&format!("{base}/api/v1/jobs/job_zombie")).await;
        assert_eq!(resp.status(), 200);
        let body: JobStatus = resp.json().await.unwrap();
        assert!(
            matches!(body.state, JobState::Running),
            "the durable record is still reported honestly, got {:?}",
            body.state
        );

        assert!(
            state.jobs.get("job_zombie").await.is_none(),
            "an in-flight record read from the durable table must not be cached: \
             the cache never evicts in-flight records, so caching it would strand \
             a slot for the life of the process"
        );
    }

    // --- Route-table ↔ router anti-drift probes (FIX for silent drift) ---

    /// Substitute the `{param}` placeholders in an `api_v1_routes()` entry
    /// with probe values so the declared route can actually be requested.
    fn probe_url(base: &str, path: &str) -> String {
        let path = path
            .replace("{name}", "probe_model")
            .replace("{column}", "probe_column")
            .replace("{id}", "probe_job")
            .replace("{pipeline}", "probe_pipeline");
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
        let state_path = pinned_state_path(&models_dir);
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let state = pinned_server(models_dir, None, &state_path);
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

    /// With a token configured, every declared route must reject a token-less
    /// request with a `401` from the Bearer middleware — EXCEPT the two known
    /// exemption classes, which carry their own auth (or none):
    ///
    /// - `GET /api/v1/health` — always exempt (liveness probes need no token).
    /// - `POST /api/v1/hooks/trigger/{pipeline}` — prefix-exempt because it
    ///   authenticates with an `X-Rocky-Signature` HMAC instead of the Bearer
    ///   token. A token-less probe must therefore NOT get the Bearer `401`; it
    ///   must reach the handler, which on this webhook-disabled test state
    ///   answers `404 webhook_disabled` — proving it bypassed the Bearer layer.
    ///
    /// This pins that routes are registered BEFORE the
    /// `.layer(require_bearer_token)` call (a route appended after it would
    /// silently dodge auth) and that the webhook exemption did not accidentally
    /// widen: every OTHER declared route still hard-`401`s. The fallback is
    /// wrapped too: an unknown path without a token is a `401`, never a
    /// route-existence oracle.
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
            } else if entry == "POST /api/v1/hooks/trigger/{pipeline}" {
                // HMAC-authed, so the Bearer layer must NOT reject it. On this
                // webhook-disabled state it reaches the handler → 404
                // webhook_disabled, never the Bearer 401.
                assert_eq!(
                    resp.status(),
                    404,
                    "{entry}: HMAC-exempt route must reach its handler, not the Bearer 401"
                );
                let body: ErrorEnvelope = resp.json().await.unwrap();
                assert_eq!(
                    body.code, "webhook_disabled",
                    "{entry}: exempt route reached the handler (not a Bearer or fallback answer)"
                );
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

        // The exemption must not widen: a traversal under the webhook prefix is
        // NOT exempt (the post-prefix remainder has a `/`), so a token-less
        // request still hits the Bearer `401`, never the webhook handler.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/../jobs/run"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            401,
            "a traversal under the webhook prefix must stay behind Bearer auth"
        );
    }

    // --- Read-scoped token: refused on every mutating route ---

    /// A `ServerState` with a scoped token AND a pinned state path, so a test
    /// can inspect the durable `jobs` table afterwards.
    fn pinned_scoped_server(
        token: ServeToken,
        models_dir: PathBuf,
        state_path: &std::path::Path,
    ) -> Arc<ServerState> {
        ServerState::with_auth(
            models_dir,
            None,
            None,
            Some(token),
            Vec::new(),
            Some(state_path.to_path_buf()),
        )
    }

    /// The declared routes a read-scoped token must be refused on: every
    /// `api_v1_routes()` entry whose method is not safe, minus the webhook
    /// ingress, which is Bearer-exempt (it carries its own HMAC) and has its
    /// own test below.
    ///
    /// The safe/unsafe split calls [`is_safe_method`] — the production
    /// predicate — rather than restating the method list, so a test can never
    /// pass by agreeing with a copy of the rule instead of the rule.
    fn declared_mutating_routes() -> Vec<String> {
        api_v1_routes()
            .into_iter()
            .filter(|entry| {
                let (method, _) = entry.split_once(' ').expect("entries are 'METHOD /path'");
                let method = Method::from_bytes(method.as_bytes()).expect("declared method");
                !is_safe_method(&method)
            })
            .filter(|entry| entry != "POST /api/v1/hooks/trigger/{pipeline}")
            .collect()
    }

    /// **Bar item 1.** A read-scoped token is refused `403` on every mutating
    /// route the router serves, and is refused *before* any side effect: no
    /// job record is persisted and the mutation permit is never taken.
    ///
    /// Honest scope of the enumeration: axum's `Router` exposes no route
    /// iterator, so this walks the hand-maintained `api_v1_routes()` table,
    /// not the router itself. Two other tests close the two directions that
    /// leaves open:
    ///
    /// - table → router: `every_declared_route_is_registered_on_the_router`
    ///   proves each declared entry is really served.
    /// - router → table: `router_registers_no_undeclared_mutating_route`
    ///   (below) narrows the other direction with a source-text count. It is
    ///   a heuristic, not a proof — its own doc lists what it cannot see.
    ///
    /// And the guarantee itself does not rest on any of the three: the
    /// middleware refuses on the HTTP **method**, before routing, without
    /// consulting the path. These tests are evidence, not the mechanism.
    #[tokio::test]
    async fn read_scoped_token_is_forbidden_on_every_declared_mutating_route() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let state = pinned_scoped_server(ServeToken::read_only("s3cret"), models_dir, &state_path);
        let base = spawn_router(state.clone()).await;
        let client = reqwest::Client::new();

        let mutating = declared_mutating_routes();
        assert!(
            mutating.len() >= 4,
            "the mutating surface should not have silently emptied; got {mutating:?}"
        );

        for entry in &mutating {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
            let resp = client
                .request(method, probe_url(&base, path))
                .bearer_auth("s3cret")
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                403,
                "{entry}: a read-scoped token must never reach a mutating route"
            );
            let body: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(
                body.code, "forbidden_read_only_token",
                "{entry}: refusal must carry the scope envelope, not some other error"
            );
            assert!(body.remediation_hint.is_some(), "{entry}");
        }

        // Refused at a seam upstream of the handler, not merely answered 403:
        // no permit was taken and nothing was persisted. Without this the test
        // would still pass if a job were submitted and *then* rejected.
        assert_eq!(
            state.mutation_permit.running_job(),
            None,
            "a refused request must not have taken the mutation permit"
        );
        let store = rocky_core::state::StateStore::open(&state_path).unwrap();
        assert!(
            store.list_jobs().unwrap().is_empty(),
            "a refused request must not have persisted a job record"
        );
    }

    /// **Bar item 1, router→table direction.** No mutating route may be
    /// registered on `router()` without appearing in `api_v1_routes()` — the
    /// table the test above enumerates.
    ///
    /// This is a **source-text guard, not a programmatic enumeration**: axum's
    /// `Router` has no public route iterator, so the honest thing to say is
    /// that this reads `router()`'s own source and counts. It fails when a new
    /// `post`/`put`/`patch`/`delete` registration lands without a matching
    /// declared entry, which is exactly "a new non-safe-method route appeared
    /// without being considered".
    ///
    /// It is deliberately count-based rather than a path parser: rustfmt is
    /// free to reflow `.route("...", post(handler))` across lines, which would
    /// break literal extraction, but not the count. The guard is kept honest
    /// from the other side by refusing the router-builder forms that could
    /// register a mutating method without one of those four words appearing.
    ///
    /// What it does NOT prove, stated so nobody leans on it further than it
    /// reaches. It counts text, so it can be defeated by a helper function
    /// that returns a `post(...)` router from outside `router()`, and it can
    /// miscount if an identifier such as `post_process(` appears in the body.
    /// It classifies methods, not effects, so a mutating `GET` reads as safe.
    /// Two named holes ARE closed: comment lines are stripped before counting,
    /// and the declaration table is asserted duplicate-free below — without
    /// that, a repeated declared entry could balance an undeclared
    /// registration and the counts would agree while a route went unprobed.
    ///
    /// The real enforcement is `rocky_server::auth`, which never reads this
    /// table or this file.
    #[test]
    fn router_registers_no_undeclared_mutating_route() {
        let source = include_str!("api.rs");
        let start = source
            .find("pub fn router(state: Arc<ServerState>) -> Router {")
            .expect("router() must be findable by its exact signature");
        let body = &source[start..];
        let end = body.find("\n}\n").expect("router() must end at column 0");
        // Comment lines are stripped so prose mentioning a verb can't inflate
        // the count — the count must reflect code.
        let body: String = body[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        // Forms this counter cannot classify. Each could mount a mutating
        // method invisibly, so their presence fails the test rather than
        // silently weakening it — someone must come here and decide.
        let unclassifiable = [
            ".nest(",
            ".nest_service(",
            ".merge(",
            ".route_service(",
            ".fallback_service(",
            ".any(",
            ".on(",
            "MethodFilter",
        ];

        // The one merge this guard understands: the browser UI's router,
        // merged AFTER the bearer layer because its files are public. It is
        // counted below by the same rules, and its bar is stricter — zero
        // mutating registrations, because a `post(` there would be an
        // unauthenticated mutation, not merely an undeclared one.
        let ui_merge = ".merge(crate::ui::ui_router(state.clone()))";
        assert!(
            body.contains(ui_merge),
            "router() must merge the UI router by exactly `{ui_merge}` so this \
             guard can find and check it"
        );
        let body = body.replace(ui_merge, "");
        for form in unclassifiable {
            assert!(
                !body.contains(form),
                "router() uses `{form}`, which this guard cannot classify. \
                 Either express the route with a plain method combinator, or \
                 teach this test (and re-check the read-scope enumeration)."
            );
        }

        let ui_source = include_str!("ui.rs");
        let ui_start = ui_source
            .find("pub(crate) fn ui_router(state: Arc<ServerState>) -> Router {")
            .expect("ui_router() must be findable by its exact signature");
        let ui_body = &ui_source[ui_start..];
        let ui_end = ui_body
            .find("\n}\n")
            .expect("ui_router() must end at column 0");
        let ui_body: String = ui_body[..ui_end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        for form in unclassifiable {
            assert!(
                !ui_body.contains(form),
                "ui_router() uses `{form}`, which this guard cannot classify"
            );
        }
        let ui_mutating: usize = ["post(", "put(", "patch(", "delete("]
            .iter()
            .map(|verb| ui_body.matches(verb).count())
            .sum();
        assert_eq!(
            ui_mutating, 0,
            "ui_router() is merged outside the bearer layer, so it may register \
             safe methods only"
        );

        let registered: usize = ["post(", "put(", "patch(", "delete("]
            .iter()
            .map(|verb| body.matches(verb).count())
            .sum();

        // Counting only works against a table with no repeats: a duplicated
        // declared entry would inflate `declared` and let an undeclared
        // registration balance it out unnoticed.
        let all = api_v1_routes();
        let unique: std::collections::HashSet<&String> = all.iter().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "api_v1_routes() must not repeat an entry — a duplicate would let \
             an undeclared mutating route hide inside the count"
        );

        // `declared_mutating_routes()` drops the webhook route, which IS a
        // registered `post(`, so add it back for the comparison.
        let declared = declared_mutating_routes().len() + 1;
        assert_eq!(
            registered, declared,
            "router() registers {registered} non-safe-method route(s) but \
             api_v1_routes() declares {declared}. A mutating route that is not \
             declared is one the read-scope test never probes — add it to \
             api_v1_routes()."
        );
    }

    /// **Bar item 2.** A read-scoped token is *not* a broken token: every
    /// declared read route still answers normally. The discriminator is the
    /// same one the auth-wrapping probe uses — a `401` would mean the token
    /// stopped authenticating, a `403` would mean the scope check over-fired.
    /// Any other status proves the request reached its handler.
    #[tokio::test]
    async fn read_scoped_token_still_serves_every_declared_read_route() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let state_path = pinned_state_path(&models_dir);
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let state = pinned_scoped_server(ServeToken::read_only("s3cret"), models_dir, &state_path);
        state.recompile().await;
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        for entry in api_v1_routes() {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let probe_method = Method::from_bytes(method.as_bytes()).unwrap();
            if !is_safe_method(&probe_method) {
                continue;
            }
            let resp = client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    probe_url(&base, path),
                )
                .bearer_auth("s3cret")
                .send()
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                401,
                "{entry}: read scope must still authenticate"
            );
            assert_ne!(
                resp.status(),
                403,
                "{entry}: the scope check must not fire on a safe method"
            );
        }

        // A concrete 200 with the real body, so "not 401/403" can't be
        // satisfied by some unrelated failure on every route.
        let resp = client
            .get(format!("{base}/api/v1/meta"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: MetaOutput = resp.json().await.unwrap();
        assert!(!body.routes.is_empty());
    }

    /// **Bar item 3.** A full-scope token still reaches the mutating routes —
    /// the change is opt-in and breaks no existing deployment.
    ///
    /// The malformed `X-Rocky-Principal` short-circuits `POST /jobs/*` with a
    /// `400` *before* the permit, the persist, and the subprocess spawn, so
    /// this asserts reachability without actually launching Rocky.
    #[tokio::test]
    async fn full_scope_token_still_reaches_every_mutating_route() {
        let state = test_state_with_scoped_token(ServeToken::full("s3cret"));
        state.recompile().await;
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        for entry in declared_mutating_routes() {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
            let resp = client
                .request(method, probe_url(&base, path))
                .bearer_auth("s3cret")
                .header("X-Rocky-Principal", "bad/slash")
                .send()
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                403,
                "{entry}: a full-scope token must keep reaching mutating routes"
            );
            assert_ne!(resp.status(), 401, "{entry}: the token must authenticate");
        }
    }

    /// **Bar item 4.** No token configured (the loopback-only default) is
    /// untouched: a mutating request still succeeds with no `Authorization`
    /// header at all. A scope only ever restricts a *configured* token.
    #[tokio::test]
    async fn no_token_configured_leaves_mutating_routes_open() {
        let state = test_state();
        state.recompile().await;
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{base}/api/v1/compile"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "loopback-only mode must keep serving mutating routes without a token"
        );
    }

    /// **Bar item 5.** The webhook ingress is unaffected by scope. It is
    /// Bearer-exempt because it authenticates with its own `X-Rocky-Signature`
    /// HMAC, and the scope check sits *after* that exemption — so a
    /// read-scoped token must not turn a webhook `POST` into a `403`.
    ///
    /// On this webhook-disabled state the handler answers `404
    /// webhook_disabled`, which is the proof it was reached.
    #[tokio::test]
    async fn webhook_ingress_is_unaffected_by_token_scope() {
        let base = spawn_router(test_state_with_scoped_token(ServeToken::read_only(
            "s3cret",
        )))
        .await;
        let client = reqwest::Client::new();

        // With the token…
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/orders"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            404,
            "the HMAC-exempt route must reach its handler"
        );
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "webhook_disabled");

        // …and without it, exactly as before this change.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/orders"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "webhook_disabled");
    }

    /// **Bar item 6.** Fail-closed on methods nobody enumerated. The refusal
    /// is an allowlist of three safe methods, so a verb the router does not
    /// serve, a WebDAV verb, and a bare extension token are all refused —
    /// refused for being absent from the allowlist, not for being present on
    /// some list of known-mutating verbs.
    ///
    /// `PUT`/`PATCH`/`DELETE` here are the important ones: they match no route,
    /// so without the scope check they would answer `405`/`404`. Getting `403`
    /// proves the middleware refused them ahead of the router.
    #[tokio::test]
    async fn odd_and_unknown_methods_are_refused_for_a_read_scoped_token() {
        let base = spawn_router(test_state_with_scoped_token(ServeToken::read_only(
            "s3cret",
        )))
        .await;
        let client = reqwest::Client::new();

        for verb in ["PUT", "PATCH", "DELETE", "TRACE", "PROPFIND", "FROB", "get"] {
            let method = reqwest::Method::from_bytes(verb.as_bytes()).unwrap();
            let resp = client
                .request(method, format!("{base}/api/v1/meta"))
                .bearer_auth("s3cret")
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                403,
                "{verb}: an un-enumerated method must be refused, not routed"
            );
            let body: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(body.code, "forbidden_read_only_token", "{verb}");
        }
    }

    /// A wrong token is `401`, never `403`, whatever the configured scope: the
    /// scope check runs only after authentication succeeds, so the `403` can
    /// never become an oracle telling an unauthenticated caller which routes
    /// mutate.
    #[tokio::test]
    async fn scope_check_never_precedes_authentication() {
        let base = spawn_router(test_state_with_scoped_token(ServeToken::read_only(
            "s3cret",
        )))
        .await;
        let client = reqwest::Client::new();

        for (label, req) in [
            ("no token", client.post(format!("{base}/api/v1/jobs/run"))),
            (
                "wrong token",
                client
                    .post(format!("{base}/api/v1/jobs/run"))
                    .bearer_auth("wrong"),
            ),
        ] {
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 401, "{label}: must be 401, not 403");
            let body: ErrorEnvelope = resp.json().await.unwrap();
            assert_eq!(body.code, "unauthorized", "{label}");
        }
    }

    /// The exempt path set bypasses the scope check as well as the token (it
    /// returns first), so it must stay safe-method-only. `/api/v1/health`
    /// serves `GET` alone, so a `POST` to it reaches the router's `405` — it
    /// never reaches a handler that could mutate. This pins that: a mutating
    /// handler mounted on an exempt path would answer 2xx here.
    #[tokio::test]
    async fn exempt_paths_expose_no_mutating_handler() {
        let base = spawn_router(test_state_with_scoped_token(ServeToken::read_only(
            "s3cret",
        )))
        .await;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/api/v1/health"))
            .bearer_auth("s3cret")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            405,
            "an auth-exempt path must serve no mutating method — it bypasses \
             the scope check too"
        );
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

    // --- Webhook ingress (POST /api/v1/hooks/trigger/{pipeline}) -------------

    const WEBHOOK_TEST_CONFIG: &str = r#"
[adapter.db]
type = "duckdb"

[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
"#;

    /// A `ServerState` with webhook ingress configured. Returns the state and the
    /// temp dir whose `.rocky/pending-demands` the spool lands in (kept alive by
    /// the caller so it can inspect the spool).
    fn webhook_state(
        secret: Option<&str>,
        loopback: bool,
        rps: f64,
    ) -> (Arc<ServerState>, tempfile::TempDir) {
        webhook_state_with_token(secret, loopback, rps, None)
    }

    /// [`webhook_state`] with a configured Bearer token, so a test can show
    /// what the token's scope does — and does not — reach.
    fn webhook_state_with_token(
        secret: Option<&str>,
        loopback: bool,
        rps: f64,
        token: Option<ServeToken>,
    ) -> (Arc<ServerState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, WEBHOOK_TEST_CONFIG).unwrap();
        let ingress = rocky_server::webhook_ingress::WebhookIngress {
            secret: secret.map(String::from),
            bind_is_loopback: loopback,
            rocky_dir: dir.path().join(".rocky"),
            rate_limiter: rocky_server::webhook_ingress::WebhookRateLimiter::new(rps),
        };
        let state = ServerState::with_auth_and_webhook(
            simple_project_models(),
            false, // models_dir_is_explicit — irrelevant to webhook ingress
            None,
            Some(config_path),
            token,
            Vec::new(),
            None,
            Some(ingress),
            None,
        );
        (state, dir)
    }

    fn sign(secret: &str, body: &[u8]) -> String {
        rocky_core::hooks::webhook::compute_signature_bytes(secret, body)
    }

    fn spool_file_count(dir: &tempfile::TempDir) -> usize {
        rocky_core::schedule::spool::list_pending_files(&dir.path().join(".rocky"))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn webhook_404_without_scheduler() {
        // `test_state` has `webhook: None` — ingress disabled.
        let base = spawn_router(test_state()).await;
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "webhook_disabled");
    }

    #[tokio::test]
    async fn webhook_404_without_secret_on_non_loopback() {
        let (state, _dir) = webhook_state(None, false, 100.0);
        let base = spawn_router(state).await;
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let body: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(body.code, "webhook_disabled");
    }

    #[tokio::test]
    async fn webhook_loopback_without_secret_accepts_unsigned() {
        let (state, dir) = webhook_state(None, true, 100.0);
        let base = spawn_router(state).await;
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["demand"], "accepted");
        assert_eq!(spool_file_count(&dir), 1);
    }

    /// **The limit of the read-scoped token, pinned rather than described.**
    ///
    /// The webhook route is Bearer-exempt, so the token's scope has no say over
    /// it — and on `serve --scheduler` bound to loopback with no
    /// `ROCKY_WEBHOOK_SECRET`, the handler accepts an UNSIGNED `POST` (the
    /// documented dev convenience at `webhook_trigger`) and durably spools a
    /// demand the resident reconciler will run.
    ///
    /// So in that one configuration a read-scoped token is not, on its own, a
    /// browser-safety guarantee: same-origin script can spool work without any
    /// token at all. That is pre-existing behaviour, not something the scope
    /// changed — the same request succeeds identically with no token
    /// configured, which the assertion below shows. It is pinned here because
    /// the surrounding prose claims a read-scoped token keeps a browser off the
    /// warehouse, and this is the configuration where that claim needs its
    /// caveat. Setting `ROCKY_WEBHOOK_SECRET` closes it.
    #[tokio::test]
    async fn read_scope_does_not_reach_the_unsigned_loopback_webhook() {
        let (state, dir) =
            webhook_state_with_token(None, true, 100.0, Some(ServeToken::read_only("s3cret")));
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        // No Authorization header at all: the route is Bearer-exempt, so the
        // scope check never runs and the unsigned POST is accepted.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            202,
            "unsigned loopback webhook stays accepted — the scope does not gate it"
        );
        assert_eq!(spool_file_count(&dir), 1, "and it durably spooled work");

        // Identical with the read-scoped token presented, confirming the token
        // is simply not consulted here — this is not a scope refusal turning
        // into an acceptance.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .bearer_auth("s3cret")
            .header("X-Rocky-Delivery", "second")
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
        assert_eq!(spool_file_count(&dir), 2);
    }

    #[tokio::test]
    async fn webhook_valid_hmac_accepts_without_bearer() {
        let (state, dir) = webhook_state(Some("s3cret"), false, 100.0);
        let base = spawn_router(state).await;
        let body = br#"{"event":"sync.done"}"#;
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .header("X-Rocky-Signature", sign("s3cret", body))
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            202,
            "a valid HMAC is accepted with no Bearer token"
        );
        assert_eq!(spool_file_count(&dir), 1);
    }

    #[tokio::test]
    async fn webhook_invalid_and_missing_hmac_are_401_and_write_nothing() {
        let (state, dir) = webhook_state(Some("s3cret"), false, 100.0);
        let base = spawn_router(state).await;
        let client = reqwest::Client::new();

        // Wrong signature.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .header("X-Rocky-Signature", "deadbeef")
            .body("payload")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        let env: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(env.code, "invalid_signature");

        // Missing signature header entirely.
        let resp = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .body("payload")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);

        assert_eq!(
            spool_file_count(&dir),
            0,
            "a rejected webhook spools nothing"
        );
    }

    #[tokio::test]
    async fn webhook_unknown_pipeline_is_404_only_after_auth() {
        let (state, dir) = webhook_state(Some("s3cret"), false, 100.0);
        let base = spawn_router(state).await;
        let body = b"x";
        // A VALID signature but an unknown pipeline → 404 pipeline_not_found.
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/ghost"))
            .header("X-Rocky-Signature", sign("s3cret", body))
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);
        let env: ErrorEnvelope = resp.json().await.unwrap();
        assert_eq!(env.code, "pipeline_not_found");
        assert_eq!(spool_file_count(&dir), 0);

        // An UNAUTHENTICATED request to the same unknown pipeline gets 401, NOT
        // 404 — so the endpoint is not a pipeline-name enumeration oracle.
        let resp = reqwest::Client::new()
            .post(format!("{base}/api/v1/hooks/trigger/ghost"))
            .header("X-Rocky-Signature", "bad")
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            401,
            "auth is checked before pipeline existence"
        );
    }

    #[tokio::test]
    async fn webhook_same_delivery_id_twice_is_duplicate() {
        let (state, dir) = webhook_state(Some("s3cret"), false, 100.0);
        let base = spawn_router(state).await;
        let body = b"x";
        let client = reqwest::Client::new();
        let send = || {
            client
                .post(format!("{base}/api/v1/hooks/trigger/raw"))
                .header("X-Rocky-Signature", sign("s3cret", body))
                .header("X-Rocky-Delivery", "evt-42")
                .body(body.to_vec())
                .send()
        };
        let first: serde_json::Value = send().await.unwrap().json().await.unwrap();
        assert_eq!(first["demand"], "accepted");
        let second: serde_json::Value = send().await.unwrap().json().await.unwrap();
        assert_eq!(second["demand"], "duplicate");
        assert_eq!(
            spool_file_count(&dir),
            1,
            "a redelivered id is a single file"
        );
    }

    #[tokio::test]
    async fn webhook_over_rate_limit_is_429_with_retry_after_and_no_spool() {
        // Capacity 1: the first request is allowed, the second is limited.
        let (state, dir) = webhook_state(Some("s3cret"), false, 1.0);
        let base = spawn_router(state).await;
        let body = b"x";
        let client = reqwest::Client::new();

        let ok = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .header("X-Rocky-Signature", sign("s3cret", body))
            .header("X-Rocky-Delivery", "evt-a")
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 202);
        assert_eq!(spool_file_count(&dir), 1);

        let limited = client
            .post(format!("{base}/api/v1/hooks/trigger/raw"))
            .header("X-Rocky-Signature", sign("s3cret", body))
            .header("X-Rocky-Delivery", "evt-b")
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(limited.status(), 429);
        assert!(
            limited.headers().contains_key("retry-after"),
            "a 429 must carry Retry-After"
        );
        assert_eq!(
            spool_file_count(&dir),
            1,
            "an over-limit request writes no spool file"
        );
    }
}
