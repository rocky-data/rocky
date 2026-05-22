//! Salsa incremental-computation spike for the Rocky DSL parse pipeline.
//!
//! This module is an opt-in, parallel API. It wraps the existing
//! [`crate::parser::parse`] entry point in a [salsa](https://docs.rs/salsa)
//! tracked query so that repeated parses of the same source text are
//! served from a memoized cache, while a `set_text` mutation invalidates
//! the cache and re-runs the parser.
//!
//! The main compile pipeline (in `rocky-compiler`) is **not** touched by
//! this spike. Wiring salsa into the LSP / compiler entry points is
//! follow-up work; this module exists to prove the framework's behaviour
//! against real Rocky parser inputs.
//!
//! # Example
//!
//! ```ignore
//! use rocky_lang::incremental::{RockyDatabase, SourceFile, parse_file};
//!
//! let db = RockyDatabase::default();
//! let src = SourceFile::new(&db, "from users".to_string());
//! let parsed = parse_file(&db, src);
//! assert!(parsed.is_ok());
//! ```
//!
//! # Safety
//!
//! [`ParsedAst`] carries an `unsafe impl salsa::Update` because
//! [`crate::ast::RockyFile`] doesn't itself implement
//! [`salsa::Update`] — and the spike scope explicitly excludes editing
//! `ast.rs`. The impl is sound (`maybe_update` overwrites in place and
//! returns "changed"; salsa only calls it after deciding to re-run the
//! query) and isolated to the spike module; production migration will
//! eventually derive `salsa::Update` on the AST types directly. See the
//! site-level `SAFETY:` comment on the impl below.

use std::sync::Arc;

use crate::ast::RockyFile;
use crate::error::ParseError;
use crate::parser;

/// Input source text for a single `.rocky` file.
///
/// Storing only the text (not the path) keeps the spike minimal — the
/// path is content-irrelevant for parse memoization, so threading it
/// through would just add a field. Production wiring (follow-up PR) can
/// extend this struct with `path: PathBuf` plus any other inputs the
/// compiler wants memoized against.
#[salsa::input]
pub struct SourceFile {
    /// The Rocky DSL source text. Mutating via [`salsa::Setter::set_text`]
    /// bumps the input revision and invalidates all downstream tracked
    /// queries that read this input.
    #[returns(ref)]
    pub text: String,
}

/// Memoizable handle to a parsed Rocky DSL AST.
///
/// Wraps `Arc<RockyFile>` so we can implement [`salsa::Update`] without
/// touching [`crate::ast::RockyFile`] itself (out-of-scope for the
/// spike). The `Arc` makes the value cheap to clone out of the salsa
/// cache and means `Arc::ptr_eq` is a robust way to assert memoization
/// returned the same instance.
#[derive(Clone)]
pub struct ParsedAst(pub Arc<RockyFile>);

impl PartialEq for ParsedAst {
    /// Compare by `Arc` pointer identity — two `ParsedAst`s are equal
    /// iff they point at the same allocation. This is the conservative
    /// choice given [`RockyFile`] doesn't implement structural
    /// `PartialEq`: distinct allocations are reported "not equal",
    /// which over-invalidates downstream salsa queries but is never
    /// incorrect. Deriving structural equality on `RockyFile` is a
    /// follow-up.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ParsedAst {}

// SAFETY: salsa::Update requires that `maybe_update(old, new)`:
//   (1) writes `new` through `old` (the pointer is valid + aligned for
//       a `Self` because salsa allocates the storage), and
//   (2) returns whether the value changed.
// We unconditionally write and return `true`. Returning `true`
// over-approximates: it tells salsa "assume the AST changed", so
// downstream tracked queries that depend on this output may invalidate
// when they could have short-circuited. That is a *performance* loss,
// never a soundness one — and `maybe_update` is only invoked after salsa
// has already decided to re-execute the query, so the over-approximation
// matters only for transitively-dependent queries (of which the spike
// has none). `RockyFile` has no `PartialEq`, so a content equality
// check isn't cheaply available; deriving it is a follow-up.
unsafe impl salsa::Update for ParsedAst {
    unsafe fn maybe_update(old_pointer: *mut Self, new_value: Self) -> bool {
        // SAFETY: salsa guarantees `old_pointer` points at valid,
        // properly-aligned storage of size `Self` owned by the cache.
        unsafe {
            std::ptr::write(old_pointer, new_value);
        }
        true
    }
}

/// Parse a [`SourceFile`] into a [`ParsedAst`].
///
/// # Memoization
///
/// Calling `parse_file` twice with the same `SourceFile` and an
/// unchanged `text` returns the cached [`ParsedAst`] from the second
/// call onward; the underlying [`parser::parse`] is invoked exactly
/// once per revision. Mutating the input via [`salsa::Setter::set_text`]
/// bumps the revision and forces a re-parse on the next call.
///
/// # Errors
///
/// Returns `Err(Arc<String>)` when the wrapped parser rejects the
/// input. The error is mapped to a stringly-typed `Arc` because
/// [`ParseError`] doesn't implement `Clone` (and salsa requires
/// [`salsa::Update`] on cached values). Production wiring can either
/// add `Clone` to `ParseError` or route diagnostics through a salsa
/// accumulator — both are follow-up work; this spike keeps the error
/// path simple.
#[salsa::tracked]
pub fn parse_file(db: &dyn salsa::Database, src: SourceFile) -> Result<ParsedAst, Arc<String>> {
    // Counter wrapped in a Mutex (per spike spec) so the unit test can
    // assert the parser body was invoked exactly the expected number of
    // times. Lives behind cfg(test) to keep the production path free of
    // observer overhead.
    #[cfg(test)]
    {
        let mut count = tests::PARSE_INVOCATIONS.lock().unwrap();
        *count += 1;
    }

    let text = src.text(db);
    parser::parse(text)
        .map(|ast| ParsedAst(Arc::new(ast)))
        .map_err(|e: ParseError| Arc::new(e.to_string()))
}

/// Concrete salsa database for the Rocky DSL incremental parse pipeline.
///
/// Single-purpose: the [`SourceFile`] input + [`parse_file`] tracked
/// query. Follow-up PRs that wire the LSP / compiler will likely grow
/// this struct (additional inputs, accumulators for diagnostics) — the
/// minimal shape here is deliberately a strict subset of what production
/// will need.
#[salsa::db]
#[derive(Clone, Default)]
pub struct RockyDatabase {
    storage: salsa::Storage<Self>,
}

#[salsa::db]
impl salsa::Database for RockyDatabase {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use salsa::Setter;

    use super::*;

    /// Module-scoped counter, bumped each time the [`parse_file`] body
    /// runs. The tests below reset it before each scenario and assert
    /// the exact invocation count after each call group, giving us the
    /// memoization receipt for the PR body.
    pub(super) static PARSE_INVOCATIONS: Mutex<usize> = Mutex::new(0);

    /// Serialize the two tests in this module — they share a single
    /// process-global counter, so running them on different threads
    /// would interleave reset / assert and produce flaky failures.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn reset_counter() {
        *PARSE_INVOCATIONS.lock().unwrap() = 0;
    }

    fn invocation_count() -> usize {
        *PARSE_INVOCATIONS.lock().unwrap()
    }

    #[test]
    fn parse_file_memoizes_across_repeated_calls() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let mut db = RockyDatabase::default();
        let src = SourceFile::new(&db, "from orders\nwhere status == \"active\"".to_string());

        // First call: cold cache, parser runs.
        let first = parse_file(&db, src);
        assert!(first.is_ok(), "first parse should succeed");
        assert_eq!(
            invocation_count(),
            1,
            "first call must invoke the parser exactly once",
        );

        // Second call with identical input: salsa serves from cache,
        // the parser body never runs.
        let second = parse_file(&db, src);
        assert!(second.is_ok(), "second parse should succeed");
        assert_eq!(
            invocation_count(),
            1,
            "second call with unchanged input must NOT re-invoke the parser \
             (this is the memoization receipt)",
        );

        // Returned `Arc<RockyFile>` instances point at the same memoized
        // value across the two calls.
        let (a, b) = (first.unwrap(), second.unwrap());
        assert!(
            Arc::ptr_eq(&a.0, &b.0),
            "memoized Arc should be shared across calls",
        );

        // Mutating the input bumps the revision; the next call must
        // re-invoke the parser. Without this assertion, "count == 1"
        // would be consistent with the parser never having run at all.
        src.set_text(&mut db).to("from orders".to_string());
        let third = parse_file(&db, src);
        assert!(third.is_ok(), "post-mutation parse should succeed");
        assert_eq!(
            invocation_count(),
            2,
            "input mutation must invalidate the cache and re-run the parser",
        );
    }

    #[test]
    fn parse_file_propagates_errors() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let db = RockyDatabase::default();
        // An empty source is `ParseError::EmptyFile` — the cleanest
        // way to exercise the error path without depending on
        // grammar specifics.
        let src = SourceFile::new(&db, String::new());

        let result = parse_file(&db, src);
        assert!(result.is_err(), "empty DSL should surface a parse error");
        assert_eq!(
            invocation_count(),
            1,
            "the error path also counts as one parser invocation",
        );
    }
}
