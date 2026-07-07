//! Authentication and CORS configuration for the HTTP API.
//!
//! `rocky serve` ships behind a Bearer-token auth middleware and a
//! configurable CORS allowlist. The defaults are deliberately tight:
//!
//! - When `auth_token` is `Some`, every request to `/api/v1/*`, `/`, or
//!   `/dashboard` must carry an `Authorization: Bearer <token>` header
//!   that matches via constant-time comparison. `/api/v1/health` is
//!   always auth-exempt so liveness probes work.
//! - When `auth_token` is `None`, the server refuses to start unless the
//!   bind host is `127.0.0.1` / `localhost` (loopback only). This keeps
//!   the LAN-leak class of bug from regressing on a forgotten `--host`
//!   override.
//!
//! CORS defaults mirror the auth posture: an empty allowlist means
//! same-origin only (no `Access-Control-Allow-Origin` header). Origins
//! are configured via `--allowed-origin` on the CLI or `[serve]
//! allowed_origins = [...]` in `rocky.toml` (Phase 2 — the CLI flag is
//! the source of truth today).

use std::sync::Arc;

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderName, HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::state::ServerState;

/// Path prefixes that bypass the auth middleware. `/api/v1/health` is
/// the canonical liveness probe — orchestrators and load balancers need
/// to hit it without a token.
const AUTH_EXEMPT_PATHS: &[&str] = &["/api/v1/health"];

/// Bearer-token auth middleware.
///
/// Extracts the configured token from [`ServerState`] and requires every
/// non-exempt request to carry `Authorization: Bearer <token>`. Token
/// comparison is constant-time so timing oracles can't be used to leak
/// the token byte-by-byte.
///
/// When no token is configured (loopback-only deployments) the middleware
/// is a no-op — but `rocky serve` refuses to bind a non-loopback host
/// without one, so the no-op path is safe.
///
/// A rejected request carries the same `{code, message, remediation_hint}`
/// error envelope shape the `/api/v1` handlers use, with `code:"unauthorized"`
/// — never an empty body.
pub async fn require_bearer_token(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    if AUTH_EXEMPT_PATHS.contains(&request.uri().path()) {
        return next.run(request).await;
    }

    let Some(expected) = state.auth_token.as_deref() else {
        // No token configured → loopback-only mode.
        return next.run(request).await;
    };

    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));

    let Some(provided) = provided else {
        return unauthorized_response();
    };

    if !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        return unauthorized_response();
    }

    next.run(request).await
}

/// Build a `401` response carrying the structured `unauthorized` error
/// envelope + a `WWW-Authenticate: Bearer` challenge.
///
/// The body mirrors `rocky_cli::output::ErrorEnvelope`'s shape by hand: the
/// typed struct lives downstream in `rocky-cli` (the router's crate), so the
/// middleware — which must reject before any handler runs — emits the same
/// `{code, message, remediation_hint}` JSON directly.
fn unauthorized_response() -> Response {
    let body = serde_json::json!({
        "code": "unauthorized",
        "message": "missing or invalid bearer token",
        "remediation_hint": "supply `Authorization: Bearer <token>`",
    });
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
        Json(body),
    )
        .into_response()
}

/// Constant-time byte comparison. Returns `true` only if both slices have
/// the same length *and* every byte matches; runtime is independent of
/// the position of the first mismatch.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Build the CORS layer from a configured allowlist.
///
/// - Empty allowlist → no `Access-Control-Allow-Origin` header is sent
///   (same-origin only). The dashboard at `/` is server-rendered HTML
///   and doesn't need cross-origin XHR, so this is the safe default.
/// - Non-empty allowlist → only the listed origins are accepted. We
///   restrict methods to `GET, POST, OPTIONS` and headers to
///   `Authorization, Content-Type` so the surface stays minimal.
///
/// Origins must be valid `Origin`-header values (e.g.
/// `http://localhost:5173`). Invalid entries are dropped with a
/// `tracing::warn`.
pub fn build_cors_layer(allowed_origins: &[String]) -> CorsLayer {
    if allowed_origins.is_empty() {
        return CorsLayer::new();
    }

    let parsed: Vec<HeaderValue> = allowed_origins
        .iter()
        .filter_map(|o| match HeaderValue::from_str(o) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(origin = %o, error = %e, "invalid CORS origin, ignoring");
                None
            }
        })
        .collect();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            HeaderName::from_static("authorization"),
            HeaderName::from_static("content-type"),
        ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_strings() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hello!"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn cors_layer_empty_allowlist_is_minimal() {
        // Doesn't panic; the layer rejects cross-origin without an
        // explicit allow_origin call.
        let _layer = build_cors_layer(&[]);
    }

    #[test]
    fn cors_layer_drops_invalid_origins() {
        // Invalid origin strings (those containing control chars) should
        // be filtered out without panicking.
        let _layer = build_cors_layer(&[
            "http://localhost:5173".to_string(),
            "bad\norigin".to_string(),
        ]);
    }
}
