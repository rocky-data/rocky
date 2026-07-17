//! Compile-fail guard for the `#[must_use] StateAuthority` boundary.
//!
//! The remote-state seam migration relies on one compiler mechanism: a
//! discarding `download_state(...)?;` in statement position must raise
//! `unused_must_use`, so under `-D warnings` a caller can never silently drop
//! the download's authority. This pins that the lint actually fires *through
//! the `?` operator* — the proof of the mechanism, not a nice-to-have.
//!
//! Lives in its own crate (`rocky-core-compiletest`) rather than in
//! `rocky-core`: `trybuild` copies the host crate's dev-dependencies into the
//! private project it builds under `target/tests/trybuild/`, so hosting this in
//! `rocky-core` dragged its `rocky-duckdb` dev-dependency into that build and
//! recompiled the whole DuckDB C++ library a second time — minutes of
//! wall-clock and enough extra disk to exhaust CI runners. This crate depends
//! only on `rocky-core`, so the guard compiles in seconds.
//!
//! Regenerate the expected diagnostics after a rustc bump changes the wording:
//! `TRYBUILD=overwrite cargo test -p rocky-core-compiletest --test must_use_guard`.

#[test]
fn discarded_authority_fails_to_compile() {
    trybuild::TestCases::new().compile_fail("tests/compile_fail/*.rs");
}
