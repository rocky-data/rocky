//! Compile-fail guard for the `SqlDialect::literal_escape` requirement.
//!
//! The dialect-owned literal encoder rests on one claim: a new warehouse
//! adapter cannot inherit another warehouse's lexer rule by staying silent,
//! because the trait method has no default body. Every implementation in the
//! tree already supplies the method, so *adding* a default would leave the
//! whole suite green and nothing would notice. This is what notices.
//!
//! The case implements every other required `SqlDialect` method and omits only
//! `literal_escape`, so its `E0046` can have exactly one cause. Give the trait
//! a default and the case compiles, and this test fails.
//!
//! Lives in `rocky-core-compiletest` for the same reason `must_use_guard` does:
//! `trybuild` copies the host crate's dev-dependencies into the project it
//! builds, and hosting this in `rocky-core` would drag `rocky-duckdb` — the
//! whole DuckDB C++ library — into that build a second time.
//!
//! Regenerate the expected diagnostic after a rustc bump changes the wording:
//! `TRYBUILD=overwrite cargo test -p rocky-core-compiletest --test literal_escape_required`.

#[test]
fn a_dialect_without_literal_escape_fails_to_compile() {
    trybuild::TestCases::new().compile_fail("tests/compile_fail/missing_literal_escape.rs");
}
