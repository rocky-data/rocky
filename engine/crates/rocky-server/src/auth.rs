//! Authentication and CORS configuration for the HTTP API.
//!
//! `rocky serve` ships behind a Bearer-token auth middleware and a
//! configurable CORS allowlist. The defaults are deliberately tight:
//!
//! - When `auth` is `Some`, every request to `/api/v1/*`, `/`, or
//!   `/dashboard` must carry an `Authorization: Bearer <token>` header
//!   that matches via constant-time comparison. `/api/v1/health` is
//!   always auth-exempt so liveness probes work.
//! - When `auth` is `None`, the server refuses to start unless the
//!   bind host is `127.0.0.1` / `localhost` (loopback only). This keeps
//!   the LAN-leak class of bug from regressing on a forgotten `--host`
//!   override.
//! - A configured token also carries a [`TokenScope`]. `Full` is the
//!   historical all-or-nothing token. `ReadOnly` authenticates exactly the
//!   same way but is refused `403` on any request whose HTTP method is not
//!   safe — see [`is_safe_method`]. A browser UI can hold a `ReadOnly`
//!   token without one leak of that token reaching a warehouse mutation
//!   *through the token*.
//!
//! One limit worth stating, because the scope does not cover it: the webhook
//! ingress is Bearer-exempt, so a `ReadOnly` token has no say over it. On
//! `serve --scheduler` bound to loopback with no `ROCKY_WEBHOOK_SECRET`, that
//! route accepts an unsigned `POST` and spools work for the resident
//! reconciler — reachable by same-origin script with no token at all. That is
//! pre-existing behaviour the scope neither adds nor removes, but it means a
//! read-scoped token is not by itself a browser-safety guarantee in that one
//! configuration. Setting `ROCKY_WEBHOOK_SECRET` closes it. Pinned by
//! `rocky_cli::api::tests::read_scope_does_not_reach_the_unsigned_loopback_webhook`.
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

/// What a configured `rocky serve` Bearer token is allowed to do.
///
/// Deliberately an enum rather than an `is_readonly: bool`: a new privilege
/// tier has to come to the `match` in [`require_bearer_token`], which has no
/// `_ =>` arm, so it cannot be added without deciding what it may mutate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TokenScope {
    /// Every route the router serves, mutating included. The historical
    /// (and default) behaviour of a configured token.
    #[default]
    Full,
    /// Safe HTTP methods only. Authenticates normally; any other method is
    /// refused `403` before it reaches the router.
    ReadOnly,
}

impl TokenScope {
    /// The spellings accepted on `--token-scope` and in
    /// `ROCKY_SERVE_TOKEN_SCOPE`, in CLI-flag order.
    pub const VALUE_NAMES: &'static [&'static str] = &["full", "read-only"];
}

impl std::str::FromStr for TokenScope {
    type Err = UnknownTokenScope;

    /// Exact, lowercase, hyphenated — one spelling per scope. Anything else
    /// is an error rather than a silent fall back to [`TokenScope::Full`]:
    /// the flag is validated by clap, but `ROCKY_SERVE_TOKEN_SCOPE` is not,
    /// and a typo there must not quietly hand out a full-scope token.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full" => Ok(Self::Full),
            "read-only" => Ok(Self::ReadOnly),
            other => Err(UnknownTokenScope(other.to_string())),
        }
    }
}

/// A `--token-scope` / `ROCKY_SERVE_TOKEN_SCOPE` value that names no scope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unknown token scope `{0}` (expected one of: {expected})",
    expected = TokenScope::VALUE_NAMES.join(", ")
)]
pub struct UnknownTokenScope(pub String);

/// The Bearer token `rocky serve` was configured with, and the scope it grants.
///
/// The secret and its scope are one value, not two fields on [`ServerState`],
/// so a scope can never be configured without a token to attach it to. That
/// shape is what makes "the operator asked for read-only but no token was set,
/// so everything stayed mutable" unrepresentable rather than a silent
/// fail-open.
#[derive(Clone)]
pub struct ServeToken {
    /// The expected Bearer secret. Compared in constant time.
    pub secret: String,
    /// What this token may do.
    pub scope: TokenScope,
}

impl ServeToken {
    /// A full-scope token — the historical all-or-nothing behaviour.
    pub fn full(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
            scope: TokenScope::Full,
        }
    }

    /// A read-scoped token: authenticates, but only safe methods pass.
    pub fn read_only(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
            scope: TokenScope::ReadOnly,
        }
    }
}

/// Hand-written so the secret never reaches a log line, a panic message, or a
/// `#[derive(Debug)]` on any struct that comes to hold a `ServeToken`.
impl std::fmt::Debug for ServeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeToken")
            .field("secret", &"<redacted>")
            .field("scope", &self.scope)
            .finish()
    }
}

/// Whether `method` is an HTTP *safe* method — one defined to be read-only.
///
/// This is the whole derivation behind [`TokenScope::ReadOnly`]. It is an
/// explicit allowlist of three methods, so it is fail-closed by construction:
/// a method nobody anticipated — `PROPFIND`, a future verb, an arbitrary
/// extension token hyper hands through — is refused because it is not on the
/// list, not permitted because it is not on a list of known-mutating verbs.
///
/// Deriving the refusal from the method rather than from the path is what
/// makes a route added tomorrow safe with no edit here: `POST /api/v1/<new>`
/// is refused for a read-scoped token the moment it exists, because the
/// middleware runs before the router and never consults the path.
///
/// State that precisely, because it has two real limits and neither is
/// covered by the method check:
///
/// 1. It holds only for routes **inside** the wrapped router. A route
///    registered after `Router::layer` in `rocky_cli::api::router` is outside
///    this middleware entirely — that is why the router's own doc makes
///    registration order rule 1, and why
///    `every_declared_route_is_auth_wrapped_except_health` probes it.
/// 2. It classifies the **method**, not the handler. A `GET` handler with a
///    mutating effect would pass, and one class of write already does.
///
///    **Corrected 2026-08-29** — the earlier note here claimed "no `GET`
///    handler performs a durable write, only `get_job`'s in-memory cache
///    upsert". That was false, and it was false because the audit read
///    handler bodies and stopped there. The write is one call deeper:
///    `StateStore::open_inner` (`rocky-core/src/state.rs`) begins a redb
///    write transaction and commits it on EVERY open — the `OpenMode`
///    argument gates the migration/stamp logic *inside* the transaction, not
///    whether one is opened. So `open_read_only` commits a write transaction
///    too, and every `GET` that reads the state store does likewise.
///
///    What that does and does not mean — stated more carefully than the
///    first correction, which was itself too categorical:
///    - It is NOT a warehouse mutation and NOT a logical-record change. No
///      run, plan, product or history row is created or altered.
///    - It CAN change the database layout. Read-only mode skips the version
///      stamp, but it still opens every table eagerly, so against an older
///      store missing a newer table a `GET` durably creates that table while
///      leaving the version stamp untouched. Calling that "no semantic state
///      change" was an over-claim in the other direction.
///    - It IS a write transaction, so a read-scoped `GET` serializes against
///      real writers. Polled by a browser, that is a contention surface.
///    - It is PRE-EXISTING `serve` behaviour, not introduced by the token
///      scope. The scope does not make it worse; the earlier claim simply
///      described it wrongly.
///
///    Tracked as #1545; do not restate the old claim. Adding a genuinely
///    mutating `GET` would still break this silently, so don't.
///
/// `TRACE` is deliberately absent even though RFC 9110 classifies it as safe:
/// nothing routes it, and refusing it keeps the cross-site-tracing shape off
/// the list entirely.
pub fn is_safe_method(method: &Method) -> bool {
    *method == Method::GET || *method == Method::HEAD || *method == Method::OPTIONS
}

/// Exact paths that bypass the Bearer-token middleware. `/api/v1/health` is
/// the canonical liveness probe — orchestrators and load balancers need
/// to hit it without a token.
///
/// This is an exact-match set. The one *class* of prefix exemption is the
/// webhook ingress (see [`is_webhook_trigger_path`]), which carries its own
/// HMAC authentication instead of the Bearer token.
///
/// **An entry here bypasses the [`TokenScope`] check as well as the token**,
/// because the exemption returns before either runs. The method-derived
/// refusal therefore does not cover these paths. Keep this set to routes that
/// serve safe methods only — a mutating route added here would be reachable by
/// a read-scoped token (and by no token at all). The webhook prefix is the
/// deliberate exception: it is a `POST`, and it is expected to authenticate
/// itself with an `X-Rocky-Signature` HMAC over the raw body instead of the
/// Bearer token.
///
/// "Expected to" is exact. The handler verifies an HMAC **only when a secret
/// is configured** (`ROCKY_WEBHOOK_SECRET`). On a loopback bind with no
/// secret it accepts unsigned `POST`s as a documented dev convenience, and
/// this exemption means the Bearer token and its scope have no say over that
/// — so in that one configuration the webhook is a mutating route reachable
/// with no credential. Pre-existing, unchanged here, and pinned by
/// `rocky_cli::api::tests::read_scope_does_not_reach_the_unsigned_loopback_webhook`.
const AUTH_EXEMPT_PATHS: &[&str] = &["/api/v1/health"];

/// The single-segment prefix under which webhook ingress routes live. A request
/// to `<PREFIX><pipeline>` is exempt from the Bearer token because the handler
/// verifies an `X-Rocky-Signature` HMAC over the raw body instead.
const WEBHOOK_TRIGGER_PREFIX: &str = "/api/v1/hooks/trigger/";

/// Whether `path` is a webhook-ingress route eligible for the HMAC-auth
/// exemption from the Bearer middleware.
///
/// Path shape only — this says nothing about whether the handler will
/// actually check an HMAC. See [`AUTH_EXEMPT_PATHS`] for when it does not.
///
/// The exemption is deliberately the **tightest** possible: the path must be a
/// single non-empty segment directly under [`WEBHOOK_TRIGGER_PREFIX`] — the
/// remainder after the prefix must be non-empty and contain no `/`. This means
/// none of the classic bypass shapes are ever exempted, because each contains a
/// `/` in the post-prefix remainder:
///
/// - traversal: `/api/v1/hooks/trigger/../jobs` → remainder `../jobs`
/// - percent-encoded traversal: `/api/v1/hooks/trigger/%2e%2e/jobs`
/// - a trailing/extra segment: `/api/v1/hooks/trigger/x/y`
/// - a bare/double slash: `/api/v1/hooks/trigger/` or `//`
///
/// axum's router matches these against the same raw `uri().path()` this checks
/// (matchit does not collapse `..`), so a path that is not exempted here is also
/// not routed to the single-segment `{pipeline}` handler — the middleware view
/// and the router view cannot diverge into a bypass.
fn is_webhook_trigger_path(path: &str) -> bool {
    match path.strip_prefix(WEBHOOK_TRIGGER_PREFIX) {
        Some(rest) => !rest.is_empty() && !rest.contains('/'),
        None => false,
    }
}

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
/// An authenticated request is then checked against the token's
/// [`TokenScope`]. A [`TokenScope::ReadOnly`] token is refused `403` unless
/// its method is safe ([`is_safe_method`]). The order matters: authenticate
/// first, so a wrong token is always `401` and the `403` never becomes an
/// oracle for which routes exist or mutate.
///
/// ```text
/// request
///   ├─ exempt path? ────────────────► handler   (health, HMAC webhook)
///   ├─ no token configured? ────────► handler   (loopback-only mode)
///   ├─ token missing / wrong? ──────► 401 unauthorized
///   ├─ scope Full ──────────────────► handler
///   └─ scope ReadOnly
///        ├─ GET / HEAD / OPTIONS ───► handler
///        └─ anything else ──────────► 403 forbidden_read_only_token
/// ```
///
/// A rejected request carries the same `{code, message, remediation_hint}`
/// error envelope shape the `/api/v1` handlers use — never an empty body.
pub async fn require_bearer_token(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    // Health is exact-exempt; webhook ingress is prefix-exempt because it
    // carries its own HMAC auth. Both bypass the Bearer token here — and, by
    // returning first, the scope check below too. See `AUTH_EXEMPT_PATHS`.
    if AUTH_EXEMPT_PATHS.contains(&path) || is_webhook_trigger_path(path) {
        return next.run(request).await;
    }

    let Some(token) = state.auth.as_ref() else {
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

    if !constant_time_eq(provided.as_bytes(), token.secret.as_bytes()) {
        return unauthorized_response();
    }

    // Authenticated. Now: is this token allowed to do what it is asking?
    // Exhaustive on purpose — no `_ =>` arm, so a future scope variant fails
    // to compile here until someone decides what it may mutate.
    match token.scope {
        TokenScope::Full => {}
        TokenScope::ReadOnly => {
            if !is_safe_method(request.method()) {
                return forbidden_read_only_response();
            }
        }
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

/// Build a `403` response for a read-scoped token that tried to mutate.
///
/// `403`, not `401`: the credential was accepted, so re-authenticating with
/// the same token will never help, and no `WWW-Authenticate` challenge is
/// sent. Body shape mirrors [`unauthorized_response`] (and, through it,
/// `rocky_cli::output::ErrorEnvelope`) so every error on this API carries the
/// same `{code, message, remediation_hint}` envelope.
fn forbidden_read_only_response() -> Response {
    let body = serde_json::json!({
        "code": "forbidden_read_only_token",
        "message": "this bearer token is read-only; only GET, HEAD, and OPTIONS are permitted",
        "remediation_hint": "use a full-scope token (`rocky serve --token-scope full`, the default) for mutating routes",
    });
    (StatusCode::FORBIDDEN, Json(body)).into_response()
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
///
/// The advertised method list is deliberately **not** scope-aware. It says
/// `GET, POST, OPTIONS` whatever [`TokenScope`] the token carries, and omits
/// `HEAD`. That is a capability mismatch, not a bypass: this layer sits
/// outside [`require_bearer_token`], so it answers the preflight `OPTIONS`
/// itself and never reaches a handler, and the browser's subsequent real
/// `POST` still passes through the scope check and earns its `403`. Narrowing
/// the advertisement per scope would only move the refusal earlier, at the
/// cost of a second place where the safe-method list is written down — and a
/// second place is how the two drift.
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
    fn webhook_exemption_matches_a_single_pipeline_segment() {
        assert!(is_webhook_trigger_path("/api/v1/hooks/trigger/orders"));
        assert!(is_webhook_trigger_path(
            "/api/v1/hooks/trigger/my-pipeline.v2"
        ));
        // A percent-encoded slash in the pipeline name is one segment at the
        // raw-path level (axum decodes it only when extracting the param), and
        // still routes solely to the webhook handler — so exempting it is safe.
        assert!(is_webhook_trigger_path(
            "/api/v1/hooks/trigger/my%2fpipeline"
        ));
    }

    #[test]
    fn webhook_exemption_never_widens_to_another_route() {
        // Every classic bypass shape has a `/` in the post-prefix remainder, so
        // none is exempted — they fall through to the Bearer middleware.
        assert!(!is_webhook_trigger_path("/api/v1/hooks/trigger/../jobs"));
        assert!(!is_webhook_trigger_path(
            "/api/v1/hooks/trigger/../../jobs/run"
        ));
        assert!(!is_webhook_trigger_path(
            "/api/v1/hooks/trigger/%2e%2e/jobs"
        ));
        assert!(!is_webhook_trigger_path("/api/v1/hooks/trigger/x/y"));
        assert!(!is_webhook_trigger_path("/api/v1/hooks/trigger//"));
        // Empty pipeline segment and the bare prefix are not exempt.
        assert!(!is_webhook_trigger_path("/api/v1/hooks/trigger/"));
        assert!(!is_webhook_trigger_path("/api/v1/hooks/trigger"));
        // Unrelated routes are never exempt.
        assert!(!is_webhook_trigger_path("/api/v1/jobs/run"));
        assert!(!is_webhook_trigger_path("/api/v1/health"));
        // A path that merely embeds the prefix later is not exempt.
        assert!(!is_webhook_trigger_path(
            "/api/v1/jobs/../hooks/trigger/orders"
        ));
    }

    /// The safe-method allowlist, stated as an allowlist: three methods pass,
    /// and everything else — mutating verbs, RFC-safe-but-unrouted `TRACE`,
    /// a WebDAV verb, a bare extension token, and a lowercase `get` (HTTP
    /// methods are case-sensitive) — does not. The failures matter more than
    /// the passes: they are what "fail-closed on an unknown method" means.
    #[test]
    fn safe_methods_are_an_allowlist_not_a_denylist() {
        for m in [Method::GET, Method::HEAD, Method::OPTIONS] {
            assert!(is_safe_method(&m), "{m} is a safe method");
        }
        for m in [
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::TRACE,
            Method::CONNECT,
        ] {
            assert!(!is_safe_method(&m), "{m} must not pass a read-only scope");
        }
        for raw in [b"PROPFIND".as_slice(), b"FROB", b"get", b"Get", b"MKCOL"] {
            let m = Method::from_bytes(raw).unwrap();
            assert!(
                !is_safe_method(&m),
                "{m}: a method nobody enumerated must be refused, not permitted"
            );
        }
    }

    /// Scope parsing is exact and fails closed. `--token-scope` is validated by
    /// clap, but `ROCKY_SERVE_TOKEN_SCOPE` is not — a typo there must be an
    /// error, never a quiet fall back to `Full`.
    #[test]
    fn token_scope_parses_exactly_two_spellings() {
        assert_eq!("full".parse::<TokenScope>().unwrap(), TokenScope::Full);
        assert_eq!(
            "read-only".parse::<TokenScope>().unwrap(),
            TokenScope::ReadOnly
        );
        for bad in ["readonly", "read_only", "Read-Only", "FULL", "", "ro"] {
            assert!(
                bad.parse::<TokenScope>().is_err(),
                "`{bad}` must not resolve to a scope"
            );
        }
        // Every spelling the CLI advertises must actually parse.
        for name in TokenScope::VALUE_NAMES {
            assert!(name.parse::<TokenScope>().is_ok(), "{name}");
        }
    }

    /// The historical behaviour is the default: a token with no scope named is
    /// a full-scope token, so existing deployments are unchanged.
    #[test]
    fn default_scope_is_full() {
        assert_eq!(TokenScope::default(), TokenScope::Full);
        assert_eq!(ServeToken::full("s").scope, TokenScope::Full);
        assert_eq!(ServeToken::read_only("s").scope, TokenScope::ReadOnly);
    }

    /// `ServeToken`'s hand-written `Debug` must never print the secret — it is
    /// one `tracing::debug!` away from a log file otherwise.
    #[test]
    fn serve_token_debug_redacts_the_secret() {
        let rendered = format!("{:?}", ServeToken::read_only("hunter2"));
        assert!(!rendered.contains("hunter2"), "secret leaked: {rendered}");
        assert!(rendered.contains("ReadOnly"), "scope should be visible");
    }

    /// The `403` body carries the same `{code, message, remediation_hint}`
    /// envelope every other error on this API uses.
    #[tokio::test]
    async fn forbidden_response_carries_the_error_envelope() {
        let resp = forbidden_read_only_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        // A rejected credential earns a challenge; an accepted-but-insufficient
        // one does not — re-sending the same token cannot help.
        assert!(resp.headers().get(header::WWW_AUTHENTICATE).is_none());
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "forbidden_read_only_token");
        assert!(body["message"].is_string());
        assert!(body["remediation_hint"].is_string());
    }

    /// The auth-exempt set bypasses the scope check as well as the token,
    /// because the exemption returns before either runs. Nothing enforces that
    /// its entries are read-only — so pin the set itself: it must stay at the
    /// liveness probe, and the one prefix exemption must stay the HMAC-authed
    /// webhook. A new entry here is a decision, not a detail.
    #[test]
    fn auth_exempt_set_is_pinned() {
        assert_eq!(AUTH_EXEMPT_PATHS, &["/api/v1/health"]);
        assert_eq!(WEBHOOK_TRIGGER_PREFIX, "/api/v1/hooks/trigger/");
    }

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
