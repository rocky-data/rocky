//! Salsa incremental-computation pipeline for the Rocky DSL parser.
//!
//! This module wraps the existing [`crate::parser::parse`] entry point in
//! a [salsa](https://docs.rs/salsa) tracked query so repeated parses of
//! the same source text are served from a memoized cache, while a
//! `set_text` mutation invalidates the cache and re-runs the parser.
//!
//! [`RockyDatabase`] is also the file-system entry point: callers load
//! `.rocky` files via [`read_source`], which reads from disk and
//! deduplicates [`SourceFile`] inputs by canonical path. LSP wiring
//! (follow-up PR) translates `textDocument/didOpen` + `didChange` into
//! `read_source` + [`SourceFile::set_text`] calls; the CLI compile flow
//! (follow-up PR) loads files via [`read_source`] before invoking
//! [`parse_file`].
//!
//! The main compile pipeline in `rocky-compiler` is **not** touched by
//! this module yet — those call sites migrate in a follow-up PR once the
//! framework's API surface is pinned for Rocky's needs.
//!
//! # Example
//!
//! ```ignore
//! use std::path::PathBuf;
//! use rocky_lang::incremental::{RockyDatabase, read_source, parse_file};
//!
//! let mut db = RockyDatabase::default();
//! let src = read_source(&mut db, PathBuf::from("models/users.rocky"))?;
//! let parsed = parse_file(&db, src);
//! assert!(parsed.is_ok());
//! # Ok::<_, std::io::Error>(())
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::ast::RockyFile;
use crate::error::ParseError;
use crate::parser;

/// A `.rocky` source file as a salsa input.
///
/// Carries both the on-disk `path` and the current `text`. The path is
/// the dedup key for [`read_source`] — two calls with the same canonical
/// path return the same `SourceFile` handle, so downstream tracked
/// queries (notably [`parse_file`]) see one cache entry per logical file.
///
/// Mutating the text via [`salsa::Setter::set_text`] bumps the input
/// revision and invalidates downstream tracked queries; mutating the
/// path is intentionally not part of the public surface (callers should
/// drop the input and load a new one instead).
#[salsa::input(debug)]
pub struct SourceFile {
    /// Canonical on-disk path. Set once by [`read_source`] (or by direct
    /// `SourceFile::new` in tests) and not expected to change — LSP
    /// wiring replaces the contents via `set_text`, not the path.
    #[returns(ref)]
    pub path: PathBuf,

    /// The Rocky DSL source text. Mutating via [`salsa::Setter::set_text`]
    /// bumps the input revision and invalidates all downstream tracked
    /// queries that read this input.
    #[returns(ref)]
    pub text: String,
}

/// Parse a [`SourceFile`] into an [`Arc<RockyFile>`].
///
/// # Memoization
///
/// Calling `parse_file` twice with the same `SourceFile` and an
/// unchanged `text` returns the cached [`Arc<RockyFile>`] from the
/// second call onward; the underlying [`parser::parse`] is invoked
/// exactly once per revision. Mutating the input via
/// [`salsa::Setter::set_text`] bumps the revision and forces a re-parse
/// on the next call.
///
/// # Errors
///
/// Returns `Err(Arc<String>)` when the wrapped parser rejects the
/// input. The error is mapped to a stringly-typed `Arc` because
/// [`ParseError`] doesn't implement `Clone` (and salsa requires
/// [`salsa::Update`] on cached values). A follow-up PR routes
/// diagnostics through a salsa accumulator instead of this `Arc<String>`
/// shape.
#[salsa::tracked]
pub fn parse_file(
    db: &dyn salsa::Database,
    src: SourceFile,
) -> Result<Arc<RockyFile>, Arc<String>> {
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
        .map(Arc::new)
        .map_err(|e: ParseError| Arc::new(e.to_string()))
}

/// Concrete salsa database for the Rocky DSL incremental pipeline.
///
/// Owns the salsa `Storage` plus a [`HashMap`] keyed by canonical
/// [`PathBuf`] that tracks every [`SourceFile`] input loaded via
/// [`read_source`]. The map is the dedup primitive: a second
/// `read_source` call with the same path returns the original handle so
/// downstream tracked queries (e.g. [`parse_file`]) hit the cache.
///
/// The map sits behind the `&mut RockyDatabase` exclusivity already
/// required by salsa input mutation, so no internal locking is needed
/// — `read_source` takes `&mut db`. If a future caller needs to insert
/// inputs from inside a tracked query (under `&dyn salsa::Database`),
/// the map can be swapped for a `DashMap` without churning the public
/// API.
#[salsa::db]
#[derive(Clone, Default)]
pub struct RockyDatabase {
    storage: salsa::Storage<Self>,
    /// Path → input dedup map. See struct-level docs.
    files: HashMap<PathBuf, SourceFile>,
}

#[salsa::db]
impl salsa::Database for RockyDatabase {}

/// Error returned by [`read_source`] when the file cannot be loaded.
///
/// Wraps [`std::io::Error`] alongside the offending path so callers
/// can render a useful diagnostic. We deliberately do *not* fold this
/// into a salsa accumulator yet — diagnostics routing is a follow-up
/// PR; here the caller (LSP / CLI) decides how to surface the failure.
#[derive(Debug, thiserror::Error)]
#[error("failed to read source file {path}: {source}")]
pub struct ReadSourceError {
    /// The path the caller asked for, before canonicalization (so the
    /// error message matches what they typed even if the file is
    /// missing and canonicalization would have failed too).
    pub path: PathBuf,
    /// Underlying I/O failure from [`std::fs::read_to_string`] or
    /// [`Path::canonicalize`].
    #[source]
    pub source: std::io::Error,
}

impl ReadSourceError {
    fn new(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self {
            path: path.into(),
            source,
        }
    }
}

/// Load a `.rocky` source file from disk into a salsa [`SourceFile`] input.
///
/// On first call for a given canonical path, reads the file via
/// [`std::fs::read_to_string`] and creates a fresh [`SourceFile`] input.
/// Subsequent calls with the same path return the cached input handle —
/// disk is **not** re-read. Callers that need to refresh the contents
/// (LSP `textDocument/didChange`, a file-watcher event) mutate the
/// existing input via [`salsa::Setter::set_text`].
///
/// Paths are canonicalized before dedup so `./models/foo.rocky` and
/// `models/foo.rocky` map to the same input.
///
/// # Errors
///
/// Returns [`ReadSourceError`] when the path cannot be canonicalized
/// (e.g. file missing) or when [`std::fs::read_to_string`] fails. The
/// salsa database is not mutated on the error path.
pub fn read_source(db: &mut RockyDatabase, path: PathBuf) -> Result<SourceFile, ReadSourceError> {
    // Canonicalize first so the dedup key matches the file the FS sees,
    // regardless of how the caller spelled the path. The canonical
    // example (`salsa/examples/lazy-input/main.rs`) does the same.
    // `canonicalize` requires the file to exist, so this also surfaces
    // ENOENT before we attempt the read.
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(err) => return Err(ReadSourceError::new(path, err)),
    };

    if let Some(existing) = db.files.get(&canonical) {
        return Ok(*existing);
    }

    let text = match std::fs::read_to_string(&canonical) {
        Ok(text) => text,
        Err(err) => return Err(ReadSourceError::new(canonical, err)),
    };

    let source = SourceFile::new(db, canonical.clone(), text);
    db.files.insert(canonical, source);
    Ok(source)
}

/// Look up a previously-loaded [`SourceFile`] by canonical path.
///
/// Returns `None` if the path has never been passed to [`read_source`]
/// (or the canonical form doesn't match anything in the dedup map).
/// Useful for LSP `didChange` handlers that already know the path and
/// want to mutate the existing input rather than re-load from disk.
pub fn lookup_source(db: &RockyDatabase, path: &Path) -> Option<SourceFile> {
    let canonical = path.canonicalize().ok()?;
    db.files.get(&canonical).copied()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use salsa::Setter;
    use tempfile::TempDir;

    use super::*;

    /// Module-scoped counter, bumped each time the [`parse_file`] body
    /// runs. The tests below reset it before each scenario and assert
    /// the exact invocation count after each call group, giving us the
    /// memoization receipt for the PR body.
    pub(super) static PARSE_INVOCATIONS: Mutex<usize> = Mutex::new(0);

    /// Serialize the tests in this module — they share a single
    /// process-global counter, so running them on different threads
    /// would interleave reset / assert and produce flaky failures.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn reset_counter() {
        *PARSE_INVOCATIONS.lock().unwrap() = 0;
    }

    fn invocation_count() -> usize {
        *PARSE_INVOCATIONS.lock().unwrap()
    }

    /// Build a `SourceFile` directly (no disk) for the legacy
    /// memoization tests. Path is a sentinel value — the parser
    /// doesn't read it, and the dedup map is bypassed (no `read_source`
    /// here), so it just needs to be a valid `PathBuf`.
    fn in_memory_source(db: &RockyDatabase, text: &str) -> SourceFile {
        SourceFile::new(db, PathBuf::from("<test>"), text.to_string())
    }

    #[test]
    fn parse_file_memoizes_across_repeated_calls() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let mut db = RockyDatabase::default();
        let src = in_memory_source(&db, "from orders\nwhere status == \"active\"");

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
            Arc::ptr_eq(&a, &b),
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
        let src = in_memory_source(&db, "");

        let result = parse_file(&db, src);
        assert!(result.is_err(), "empty DSL should surface a parse error");
        assert_eq!(
            invocation_count(),
            1,
            "the error path also counts as one parser invocation",
        );
    }

    /// `read_source` loads the file contents from disk into the
    /// `SourceFile` salsa input, and `parse_file` sees those contents.
    #[test]
    fn read_source_loads_file_contents_from_disk() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let tmp = TempDir::new().expect("create tempdir");
        let path = tmp.path().join("model.rocky");
        std::fs::write(&path, "from orders").expect("write fixture");

        let mut db = RockyDatabase::default();
        let src = read_source(&mut db, path.clone()).expect("read_source should succeed");

        // The salsa input round-trips the on-disk contents.
        assert_eq!(src.text(&db), "from orders");
        // And the input is keyed by the canonical (not the input) path,
        // so the public accessor sees the canonical form.
        assert_eq!(
            src.path(&db),
            &path.canonicalize().expect("canonicalize fixture path"),
        );

        // Downstream tracked query sees the same content.
        let parsed = parse_file(&db, src).expect("parse should succeed");
        assert!(
            !parsed.pipeline.is_empty(),
            "loaded source should produce at least one parsed pipeline step",
        );
    }

    /// Two `read_source` calls with the same path return the same
    /// `SourceFile` handle (dedup), so `parse_file` hits the salsa
    /// cache on the second call.
    #[test]
    fn read_source_dedupes_by_path() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let tmp = TempDir::new().expect("create tempdir");
        let path = tmp.path().join("dedup.rocky");
        std::fs::write(&path, "from orders").expect("write fixture");

        let mut db = RockyDatabase::default();
        let first = read_source(&mut db, path.clone()).expect("first read");
        let second = read_source(&mut db, path.clone()).expect("second read");
        assert_eq!(
            first, second,
            "two read_source calls with the same path must return the same SourceFile handle",
        );

        // Memoization receipt: parser body fires exactly once across
        // two parse_file calls keyed by the deduped input.
        let _ = parse_file(&db, first);
        let _ = parse_file(&db, second);
        assert_eq!(
            invocation_count(),
            1,
            "dedup must keep parse_file on the same cache entry across two read_source calls",
        );
    }

    /// Two distinct paths produce two distinct `SourceFile` handles —
    /// the negative half of the dedup invariant.
    #[test]
    fn read_source_distinct_paths_distinct_inputs() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let tmp = TempDir::new().expect("create tempdir");
        let path_a = tmp.path().join("a.rocky");
        let path_b = tmp.path().join("b.rocky");
        std::fs::write(&path_a, "from orders").expect("write a");
        std::fs::write(&path_b, "from users").expect("write b");

        let mut db = RockyDatabase::default();
        let a = read_source(&mut db, path_a).expect("read a");
        let b = read_source(&mut db, path_b).expect("read b");
        assert_ne!(a, b, "distinct paths must yield distinct SourceFile inputs",);

        // Two cache entries, two parser invocations.
        let _ = parse_file(&db, a);
        let _ = parse_file(&db, b);
        assert_eq!(
            invocation_count(),
            2,
            "two distinct SourceFile inputs must produce two parser invocations",
        );
    }

    /// Mutating a loaded `SourceFile` via `set_text` bumps the
    /// revision and invalidates downstream tracked queries. This is
    /// the canonical LSP `didChange` shape.
    #[test]
    fn set_text_after_read_source_invalidates_downstream_queries() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();
        let tmp = TempDir::new().expect("create tempdir");
        let path = tmp.path().join("mutate.rocky");
        std::fs::write(&path, "from orders").expect("write fixture");

        let mut db = RockyDatabase::default();
        let src = read_source(&mut db, path).expect("read_source");

        let _ = parse_file(&db, src).expect("initial parse");
        assert_eq!(invocation_count(), 1, "cold-cache parse runs once");

        // Cache hit on the unchanged input.
        let _ = parse_file(&db, src).expect("memoized parse");
        assert_eq!(invocation_count(), 1, "unchanged input must hit the cache");

        // LSP `didChange`-style mutation. The on-disk file is not
        // re-read by `read_source` — the caller pushes the new buffer
        // contents straight at the existing input.
        src.set_text(&mut db).to("from users".to_string());
        let _ = parse_file(&db, src).expect("post-mutation parse");
        assert_eq!(
            invocation_count(),
            2,
            "set_text must invalidate the cache and re-run the parser",
        );
    }

    /// A missing file surfaces an `io::Error`-backed `ReadSourceError`;
    /// no panic, no salsa input materialized.
    #[test]
    fn read_source_missing_file_returns_err() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let tmp = TempDir::new().expect("create tempdir");
        let path = tmp.path().join("does-not-exist.rocky");

        let mut db = RockyDatabase::default();
        let err = read_source(&mut db, path.clone()).expect_err("missing file must error");
        assert_eq!(err.path, path, "error should preserve the caller's path");
        assert_eq!(
            err.source.kind(),
            std::io::ErrorKind::NotFound,
            "underlying io::Error should be NotFound",
        );
        assert!(
            db.files.is_empty(),
            "failed read must not mutate the dedup map",
        );
    }
}
