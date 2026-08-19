//! The structural half of the route-inventory invariant: the raw plan
//! writers are not nameable from outside `rocky-cli`.
//!
//! Each `tests/compile_fail/*.rs` case names one privatized symbol and
//! must fail with the privacy error (E0603). The positive control
//! (`read_plan` from the same module, plus the governed propose helper)
//! compiles — proving the failures come from the privatization, not from
//! a broken import path that would make every case "pass" vacuously.
//!
//! Regenerate expected diagnostics after a rustc bump:
//! `TRYBUILD=overwrite cargo test -p rocky-cli-compiletest`.

#[test]
fn plan_writers_are_not_nameable_externally() {
    let cases = trybuild::TestCases::new();
    // Positive control first: the module path and the sanctioned symbols
    // resolve, so a failing writer case fails on privacy alone.
    cases.pass("tests/compile_pass/sanctioned_surface.rs");
    cases.compile_fail("tests/compile_fail/*.rs");
}
