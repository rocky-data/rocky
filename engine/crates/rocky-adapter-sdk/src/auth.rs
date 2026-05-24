//! Pluggable authentication for HTTP-based warehouse adapters.
//!
//! The [`AuthProvider`] trait composes two concerns that older Rocky
//! adapter prototypes conflated:
//!
//! 1. The `Authorization` header value (Basic, Bearer, sigv4, …).
//! 2. Any **other** mandatory headers the warehouse requires on every
//!    request (Trino's `X-Trino-User`, Snowflake's `X-Snowflake-Authorization-Token-Type`,
//!    Databricks's `X-Databricks-Org-Id` on some sandboxes, etc.).
//!
//! The original PR #427 (rocky-trino v0) surfaced the breakage: Trino's
//! `X-Trino-User` header is **mandatory on every request**, but for JWT
//! bearer auth there's no auth-derived default user — the JWT's `sub`
//! claim is opaque to the adapter, so the caller has to supply the user
//! out of band. A monolithic "compute Authorization" callback can't
//! express that cleanly; an explicit "auth + extra headers" pair can.
//!
//! Adapters that don't have extra mandatory headers (most don't) get the
//! default empty list from [`AuthProvider::extra_headers`] for free.
//!
//! # Composition example (Trino + JWT + explicit user)
//!
//! ```rust
//! use rocky_adapter_sdk::auth::{AuthProvider, StaticAuthProvider};
//!
//! // JWT bearer for the Authorization header, plus a mandatory X-Trino-User
//! // (Trino rejects requests missing it; the JWT's `sub` is opaque to the
//! // adapter so the caller threads the username through explicitly).
//! let auth = StaticAuthProvider::new("Bearer eyJ.signed.token")
//!     .with_extra_header("X-Trino-User", "alice");
//!
//! assert_eq!(
//!     auth.authorization_header().as_deref(),
//!     Some("Bearer eyJ.signed.token"),
//! );
//! assert_eq!(
//!     auth.extra_headers(),
//!     vec![("X-Trino-User".to_string(), "alice".to_string())],
//! );
//! ```

/// Renders the `Authorization` header and any mandatory extra headers a
/// warehouse requires on every HTTP request.
///
/// Adapters implement this for their auth mode (Basic / Bearer /
/// signed-V4 / OAuth refresh / …). The HTTP client layer queries the
/// provider once per request to assemble the header set.
///
/// Implementations should be cheap to call — `authorization_header`
/// fires on every request. Token refresh / caching belong **inside**
/// the implementation (e.g. behind a `Mutex<Option<Expiring<Token>>>`);
/// the trait stays sync to keep the per-request hot path allocation-free.
///
/// Implementations should also be `Send + Sync` so the provider can be
/// shared across async tasks behind an `Arc`.
pub trait AuthProvider: Send + Sync {
    /// Return the value to set on the `Authorization` header for the next
    /// request, or `None` when the adapter authenticates exclusively via
    /// other mechanisms (mTLS, IP allowlist, signed query params, …).
    ///
    /// Implementations should never include the header name — just the
    /// value (e.g. `"Bearer eyJ..."`, `"Basic dXNlcjpwd2Q="`).
    fn authorization_header(&self) -> Option<String>;

    /// Return any **other** mandatory headers the warehouse requires on
    /// every request, as `(name, value)` pairs.
    ///
    /// Default: empty — most adapters need only `Authorization`. Adapters
    /// whose warehouse requires extra headers (Trino's `X-Trino-User`,
    /// for example) override.
    ///
    /// The returned list is appended to the request's header set verbatim;
    /// implementations are responsible for any validation / canonicalisation
    /// of header names and values.
    fn extra_headers(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// Trivial [`AuthProvider`] for tests and simple cases — holds a pre-computed
/// `Authorization` value plus an optional list of extra headers.
///
/// Real adapters typically don't use this directly; they implement
/// [`AuthProvider`] on their own auth-config struct (e.g. `TrinoAuth`) so
/// secrets can stay wrapped in `RedactedString`. `StaticAuthProvider` is
/// the canonical example for documentation + the SDK's own conformance
/// tests.
#[derive(Debug, Clone, Default)]
pub struct StaticAuthProvider {
    authorization: Option<String>,
    extra: Vec<(String, String)>,
}

impl StaticAuthProvider {
    /// Build a provider that renders the given `Authorization` header value.
    pub fn new(authorization: impl Into<String>) -> Self {
        Self {
            authorization: Some(authorization.into()),
            extra: Vec::new(),
        }
    }

    /// Build a provider that emits no `Authorization` header at all —
    /// useful for warehouses that authenticate via mTLS / IP allowlist /
    /// signed query params and only need the SDK's extra-header surface.
    pub fn none() -> Self {
        Self::default()
    }

    /// Append a mandatory extra header (name, value).
    ///
    /// Repeatable — call once per header. Trino's `X-Trino-User`,
    /// Snowflake's `X-Snowflake-Authorization-Token-Type`, etc.
    pub fn with_extra_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.push((name.into(), value.into()));
        self
    }
}

impl AuthProvider for StaticAuthProvider {
    fn authorization_header(&self) -> Option<String> {
        self.authorization.clone()
    }

    fn extra_headers(&self) -> Vec<(String, String)> {
        self.extra.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The four blocker (c) properties pinned as tests:
    //
    // 1. Basic-style auth provider composes cleanly (Authorization only).
    // 2. JWT + X-Trino-User composes (Authorization + one extra header) —
    //    this is the v0 rocky-trino break the abstraction is built for.
    // 3. Auth-less adapter (mTLS / signed query params) is expressible.
    // 4. The trait is object-safe so callers can hold `Arc<dyn AuthProvider>`.

    #[test]
    fn basic_auth_provider_has_no_extras() {
        let auth = StaticAuthProvider::new("Basic dXNlcjpwYXNz");
        assert_eq!(
            auth.authorization_header().as_deref(),
            Some("Basic dXNlcjpwYXNz")
        );
        assert!(auth.extra_headers().is_empty());
    }

    #[test]
    fn jwt_plus_trino_user_composes() {
        // The v0 rocky-trino break: Trino's X-Trino-User is mandatory on
        // every request, but JWT auth doesn't self-describe the user. The
        // abstraction must let the caller supply both.
        let auth = StaticAuthProvider::new("Bearer eyJ.signed.token")
            .with_extra_header("X-Trino-User", "alice");
        assert_eq!(
            auth.authorization_header().as_deref(),
            Some("Bearer eyJ.signed.token")
        );
        assert_eq!(
            auth.extra_headers(),
            vec![("X-Trino-User".to_string(), "alice".to_string())]
        );
    }

    #[test]
    fn auth_provider_can_carry_multiple_extra_headers() {
        let auth = StaticAuthProvider::new("Bearer x")
            .with_extra_header("X-Trino-User", "alice")
            .with_extra_header("X-Trino-Source", "rocky");
        assert_eq!(
            auth.extra_headers(),
            vec![
                ("X-Trino-User".to_string(), "alice".to_string()),
                ("X-Trino-Source".to_string(), "rocky".to_string()),
            ]
        );
    }

    #[test]
    fn no_auth_provider_is_expressible() {
        // mTLS / signed-URL adapters don't emit Authorization but may
        // still need extra headers.
        let auth = StaticAuthProvider::none().with_extra_header("X-Api-Key", "sk-xxx");
        assert!(auth.authorization_header().is_none());
        assert_eq!(
            auth.extra_headers(),
            vec![("X-Api-Key".to_string(), "sk-xxx".to_string())]
        );
    }

    fn _assert_auth_provider_object_safe(_: &dyn AuthProvider) {}
}
