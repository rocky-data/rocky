//! Compile-fail guard for the `#[must_use] StateAuthority` boundary.
//!
//! The remote-state seam migration relies on one compiler mechanism: a
//! discarding `download_state(...)?;` in statement position must raise
//! `unused_must_use`, so under `-D warnings` a caller can never silently drop
//! the download's authority. This pins that the lint actually fires *through
//! the `?` operator* — the proof of the mechanism, not a nice-to-have.
//!
//! Regenerate the expected diagnostics after a rustc bump changes the wording:
//! `TRYBUILD=overwrite cargo test -p rocky-core --test must_use_guard`.

#[test]
fn discarded_authority_fails_to_compile() {
    trybuild::TestCases::new().compile_fail("tests/compile_fail/*.rs");
}
