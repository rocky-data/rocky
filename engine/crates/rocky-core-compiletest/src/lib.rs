//! Compile-fail (`trybuild`) guards for `rocky-core`'s `#[must_use]` boundaries.
//!
//! This crate has no runtime code. It exists only to host the trybuild suite in
//! `tests/` in isolation from `rocky-core`'s heavyweight dev-dependencies —
//! chiefly `rocky-duckdb`, which trybuild would otherwise recompile in its
//! private `target/tests/trybuild/` directory (a redundant multi-minute DuckDB
//! build that exhausted CI runner disk). See `tests/must_use_guard.rs`.
