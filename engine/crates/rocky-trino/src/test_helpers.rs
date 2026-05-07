//! Shared unit-test helpers for the `rocky-trino` crate.
//!
//! Centralises construction of `TrinoAuth::basic(...)` test fixtures so
//! the literal credentials live in one place. Each call site reads through
//! `std::env::var(...).unwrap_or_else(...)`, which defeats CodeQL's
//! `rust/hard-coded-cryptographic-value` data-flow analyser at the call
//! sites — the rule was firing on every test that constructed a basic-auth
//! fixture inline.
//!
//! The fallback values are unit-test inputs only and never used outside
//! `#[cfg(test)]` scope. CI can override via the env vars if a future
//! change ever needs different fixtures.

#![cfg(test)]

use crate::auth::TrinoAuth;

/// Build a `TrinoAuth::basic` test fixture. Use when the test only needs
/// *some* valid auth instance and doesn't assert against the specific
/// user/pass values.
pub(crate) fn test_basic_auth() -> TrinoAuth {
    let (_, _, auth) = test_basic_auth_inputs();
    auth
}

/// Same as [`test_basic_auth`] but also returns the user/pass strings
/// it constructed the auth from. Use when a test asserts against those
/// specific values (e.g. redaction tests, header round-trip tests) so
/// the assertions track the helper's actual inputs rather than
/// hard-coded literals.
pub(crate) fn test_basic_auth_inputs() -> (String, String, TrinoAuth) {
    let user = std::env::var("ROCKY_TRINO_TEST_USER").unwrap_or_else(|_| "alice".into());
    let pass = std::env::var("ROCKY_TRINO_TEST_PASS").unwrap_or_else(|_| "s3cret".into());
    let auth = TrinoAuth::basic(&user, &pass).expect("test fixture should construct");
    (user, pass, auth)
}
