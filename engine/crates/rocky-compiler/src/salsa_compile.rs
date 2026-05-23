//! Salsa-tracked compile pipeline.
//!
//! Wraps the existing [`crate::compile`] orchestration in a salsa
//! incremental-computation layer so that repeated compiles against the
//! same `RockyDatabase` only re-run work for files whose inputs changed.
//!
//! This is the compiler-side half of the Salsa migration (the parser
//! side shipped earlier with `rocky_lang::incremental::parse_file`).
//! The two layers share the same [`RockyDatabase`] from
//! [`rocky_lang::incremental`]; the database we re-export here is just
//! that database, so a single in-process db can serve both pipelines.
//!
//! # Granularity
//!
//! Two tracked queries:
//!
//! - [`file_typecheck`] — keyed on a [`SourceFile`] salsa input, runs
//!   parse + lower (for `.rocky` files) and extracts the per-file
//!   output column list with no cross-model context. Body of this query
//!   fires once per file per revision and re-runs only when the file's
//!   parsed AST (or text) changes. Backdates: when `set_text` produces
//!   an identical AST, downstream callers reuse the cached
//!   [`FileTypecheck`] without re-entering the body.
//!
//! - [`source_signature`] — keyed on a [`SourceFile`], returns a stable
//!   `u64` hash of the parsed AST. Used by the orchestrator to detect
//!   "this file's AST is the same after a `set_text`" so cross-file
//!   typecheck can be skipped for unchanged inputs.
//!
//! The whole-project orchestration ([`compile_with_db`]) stays plain —
//! it loads the project, calls [`file_typecheck`] per `.rocky` file
//! through the database, then runs the existing cross-model passes
//! (join-key check, contracts, blast-radius, classification) over the
//! assembled per-file outputs. Cross-model passes are cheap on the
//! current 22-crate workspace's project sizes so they're not yet
//! tracked.
//!
//! # What this does NOT cover (yet)
//!
//! - Cross-model type propagation isn't itself memoized — when an
//!   upstream `.sql` model's text changes, dependents do re-typecheck
//!   even though salsa would short-circuit a `parse_file` call. The
//!   per-file query is the receipt; full transitive invalidation is a
//!   follow-up that requires routing `compute_model_typecheck`'s
//!   cross-cutting context (SemanticGraph, source schemas, mask config)
//!   into salsa.
//! - `.sql` models are loaded straight from disk via the existing
//!   `Project::load` path. The salsa pipeline currently only memoizes
//!   `.rocky` files (the surface the parser-side migration wired up).
//!   Extending to `.sql` requires routing the SQL parser through a
//!   tracked query too.
//! - Diagnostics still flow through the `Vec<Diagnostic>` shape on
//!   `CompileResult`. The salsa Accumulator-driven path is the next
//!   migration step.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use rocky_lang::ast::RockyFile;
use rocky_lang::incremental::{SourceFile, parse_file};

pub use rocky_lang::incremental::{ReadSourceError, RockyDatabase, lookup_source, read_source};

/// Per-file typecheck output, salsa-tracked.
///
/// Carries the lowered SQL string, the parsed AST handle, and the
/// extracted output column names. Cross-model type propagation
/// happens in the orchestrator — this struct is intentionally local
/// (no upstream-dependent fields) so its identity is determined solely
/// by the input file's AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTypecheck {
    /// Lowered SQL string (for `.rocky`) or the original SQL (for `.sql`).
    pub sql: String,
    /// Output column names extracted from the file in isolation. The
    /// orchestrator stitches these into typed columns once upstream
    /// types are known.
    pub output_columns: Vec<String>,
    /// Diagnostics produced by parse + lower. Cross-model diagnostics
    /// are appended later by the orchestrator.
    pub diagnostics: Vec<String>,
}

/// Parse + lower a `.rocky` source file into a [`FileTypecheck`].
///
/// # Memoization
///
/// The body of this query runs **exactly once** per distinct
/// `SourceFile` revision. A second call with an unchanged input
/// returns the cached `Arc<FileTypecheck>` without re-entering the
/// body. When the file's text is mutated via
/// [`salsa::Setter::set_text`], salsa marks the input as changed; on
/// the next call the body re-runs the parser, the lowering, and the
/// column extraction.
///
/// # Backdating
///
/// Because [`RockyFile`] and all its transitive AST nodes derive
/// structural `PartialEq`, salsa is able to **backdate** this query:
/// if `set_text` happens to produce a token-equivalent AST, the
/// downstream tracked-value comparison short-circuits the
/// dependent-query re-run. Callers observing `file_typecheck` results
/// across an identical-AST edit see the same `Arc<FileTypecheck>`
/// returned (pointer-equal in practice).
#[salsa::tracked]
pub fn file_typecheck(db: &dyn salsa::Database, src: SourceFile) -> Arc<FileTypecheck> {
    // Test-only invocation counter — gives integration tests a way to
    // assert the body really did (or did not) run on a given call.
    // Behind cfg(test) so the production path doesn't pay for the
    // Mutex traffic.
    #[cfg(test)]
    {
        let mut count = tests::TYPECHECK_INVOCATIONS.lock().unwrap();
        *count += 1;
    }

    match parse_file(db, src) {
        Ok(ast) => match rocky_lang::lower::lower_to_sql(&ast) {
            Ok(sql) => {
                let output_columns = extract_output_columns(&ast);
                Arc::new(FileTypecheck {
                    sql,
                    output_columns,
                    diagnostics: Vec::new(),
                })
            }
            Err(err) => Arc::new(FileTypecheck {
                sql: String::new(),
                output_columns: Vec::new(),
                diagnostics: vec![format!("lower error: {err}")],
            }),
        },
        Err(err) => Arc::new(FileTypecheck {
            sql: String::new(),
            output_columns: Vec::new(),
            diagnostics: vec![format!("parse error: {err}")],
        }),
    }
}

/// Stable hash of a parsed file's AST.
///
/// Salsa-tracked so the hash is computed only once per AST revision.
/// Used by the orchestrator to detect "this file's AST is structurally
/// identical to the previous compile" — the cross-model typecheck can
/// then skip dependents.
#[salsa::tracked]
pub fn source_signature(db: &dyn salsa::Database, src: SourceFile) -> u64 {
    match parse_file(db, src) {
        Ok(ast) => hash_rocky_file(&ast),
        // Errors hash to a sentinel so two failures with different
        // error texts don't both look "unchanged"; callers checking
        // the signature should fold the parse error in separately.
        Err(_) => 0,
    }
}

/// Extract output column names from a parsed `.rocky` file in isolation.
///
/// Looks at the final `select` step's column list, falling back to an
/// empty list if the pipeline doesn't terminate in a `select` (the
/// orchestrator then treats it as `SELECT *` and handles upstream type
/// propagation separately).
fn extract_output_columns(file: &RockyFile) -> Vec<String> {
    use rocky_lang::ast::{PipelineStep, SelectItem};

    // Walk pipeline steps backwards looking for the last `select`.
    // `select` overrides the column set; everything before it is
    // either passed-through (`from`) or column-adding (`derive`),
    // which doesn't matter for the per-file output names.
    for step in file.pipeline.iter().rev() {
        if let PipelineStep::Select(items) = step {
            return items
                .iter()
                .filter_map(|item| match item {
                    SelectItem::Column(name) => Some(name.clone()),
                    SelectItem::QualifiedColumn(_, name) => Some(name.clone()),
                    // `Star` is an upstream-dependent pattern — the
                    // orchestrator resolves it from inferred upstream
                    // columns, not from the local AST.
                    SelectItem::Star => None,
                })
                .collect();
        }
    }
    Vec::new()
}

/// Hash a parsed file's AST for backdating purposes.
fn hash_rocky_file(file: &RockyFile) -> u64 {
    let mut hasher = DefaultHasher::new();
    // RockyFile derives Debug; we hash the Debug shape as a cheap
    // structural fingerprint. Not cryptographic — only used as a
    // change-detection signal.
    format!("{file:?}").hash(&mut hasher);
    hasher.finish()
}

/// Load a `.rocky` file's [`FileTypecheck`] through the salsa database.
///
/// Convenience wrapper combining [`read_source`] + [`file_typecheck`].
/// Used by callers that have a file path and want the cached typecheck
/// without manually managing the `SourceFile` handle.
pub fn typecheck_path(
    db: &mut RockyDatabase,
    path: PathBuf,
) -> Result<Arc<FileTypecheck>, ReadSourceError> {
    let src = read_source(db, path)?;
    Ok(file_typecheck(db, src))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    pub(super) static TYPECHECK_INVOCATIONS: Mutex<usize> = Mutex::new(0);

    /// Serialize the salsa-tracked tests in this crate — they share
    /// the process-global counter so concurrent execution would race.
    pub(super) static TEST_LOCK: Mutex<()> = Mutex::new(());

    pub(crate) fn reset_counter() {
        *TYPECHECK_INVOCATIONS.lock().unwrap() = 0;
    }

    pub(crate) fn invocation_count() -> usize {
        *TYPECHECK_INVOCATIONS.lock().unwrap()
    }

    use std::path::PathBuf;
    use std::sync::Arc;

    use salsa::Setter;
    use tempfile::TempDir;

    use super::{FileTypecheck, RockyDatabase, file_typecheck, read_source, source_signature};
    use rocky_lang::incremental::SourceFile;

    fn write_model(dir: &TempDir, name: &str, body: &str) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, body).expect("write model");
        path
    }

    /// First-compile baseline: typecheck body fires once per file.
    #[test]
    fn first_compile_runs_typecheck_once_per_file() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "a.rocky", "from orders\nselect { id }");
        let b = write_model(&tmp, "b.rocky", "from users\nselect { name }");
        let c = write_model(&tmp, "c.rocky", "from products\nselect { sku }");

        let mut db = RockyDatabase::default();
        let src_a = read_source(&mut db, a).expect("read a");
        let src_b = read_source(&mut db, b).expect("read b");
        let src_c = read_source(&mut db, c).expect("read c");

        let _ = file_typecheck(&db, src_a);
        let _ = file_typecheck(&db, src_b);
        let _ = file_typecheck(&db, src_c);

        assert_eq!(
            invocation_count(),
            3,
            "cold cache: three distinct files = three typecheck invocations",
        );
    }

    /// Second compile with no input changes: typecheck body does NOT
    /// re-fire. This is the headline memoization receipt.
    #[test]
    fn unchanged_inputs_skip_typecheck() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "a.rocky", "from orders\nselect { id }");
        let b = write_model(&tmp, "b.rocky", "from users\nselect { name }");

        let mut db = RockyDatabase::default();
        let src_a = read_source(&mut db, a).expect("read a");
        let src_b = read_source(&mut db, b).expect("read b");

        // First compile.
        let first_a = file_typecheck(&db, src_a);
        let first_b = file_typecheck(&db, src_b);
        assert_eq!(invocation_count(), 2, "cold-cache baseline");

        // Second compile, identical inputs — body must not re-run.
        let second_a = file_typecheck(&db, src_a);
        let second_b = file_typecheck(&db, src_b);
        assert_eq!(
            invocation_count(),
            2,
            "unchanged inputs: body must not re-run (memoization receipt)",
        );

        // Returned Arcs point at the same memoized value across calls.
        assert!(
            Arc::ptr_eq(&first_a, &second_a),
            "memoized FileTypecheck Arc should be shared across calls for src_a",
        );
        assert!(
            Arc::ptr_eq(&first_b, &second_b),
            "memoized FileTypecheck Arc should be shared across calls for src_b",
        );
    }

    /// Set a file's text to a string-identical value. Salsa's input
    /// equality check on `String` short-circuits at the input layer —
    /// the revision does not bump, `parse_file` is not re-invoked, and
    /// neither is `file_typecheck`. Tight invariant: the body must
    /// fire exactly **zero** additional times after a no-op `set_text`.
    ///
    /// A looser bound (e.g. `count <= 3`) would silently pass even if
    /// salsa eagerly invalidated — which is exactly the failure mode
    /// this test rules out.
    #[test]
    fn identical_text_set_does_not_invalidate() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "a.rocky", "from orders\nselect { id }");
        let b = write_model(&tmp, "b.rocky", "from users\nselect { name }");

        let mut db = RockyDatabase::default();
        let src_a = read_source(&mut db, a).expect("read a");
        let src_b = read_source(&mut db, b).expect("read b");

        let _ = file_typecheck(&db, src_a);
        let _ = file_typecheck(&db, src_b);
        assert_eq!(invocation_count(), 2, "cold-cache baseline");

        // Set src_a's text to its current value — String equality
        // matches, so salsa must NOT bump the input revision.
        src_a
            .set_text(&mut db)
            .to("from orders\nselect { id }".to_string());

        let _ = file_typecheck(&db, src_a);
        let _ = file_typecheck(&db, src_b);

        assert_eq!(
            invocation_count(),
            2,
            "no-op set_text must NOT trigger any additional typecheck \
             body invocations (string-equal input short-circuits at \
             the salsa input layer)",
        );
    }

    /// Edit one file to genuinely change its AST: only that file's
    /// body re-runs; independent files are NOT re-typechecked.
    ///
    /// This is the per-file invalidation invariant — the whole reason
    /// to ship per-file tracked queries instead of one whole-project
    /// query.
    #[test]
    fn unrelated_files_skip_typecheck_after_one_edit() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "a.rocky", "from orders\nselect { id }");
        let b = write_model(&tmp, "b.rocky", "from users\nselect { name }");
        let c = write_model(&tmp, "c.rocky", "from products\nselect { sku }");

        let mut db = RockyDatabase::default();
        let src_a = read_source(&mut db, a).expect("read a");
        let src_b = read_source(&mut db, b).expect("read b");
        let src_c = read_source(&mut db, c).expect("read c");

        // Cold-cache baseline.
        let _ = file_typecheck(&db, src_a);
        let _ = file_typecheck(&db, src_b);
        let _ = file_typecheck(&db, src_c);
        assert_eq!(invocation_count(), 3, "cold-cache baseline");

        // Edit src_a to a genuinely different AST.
        src_a
            .set_text(&mut db)
            .to("from orders\nselect { id, total }".to_string());

        // Re-typecheck all three.
        let _ = file_typecheck(&db, src_a);
        let _ = file_typecheck(&db, src_b);
        let _ = file_typecheck(&db, src_c);

        assert_eq!(
            invocation_count(),
            4,
            "only src_a's typecheck body re-runs; src_b + src_c stay cached",
        );
    }

    /// `source_signature` is salsa-tracked and stable across no-op
    /// `set_text` calls. Backdating-friendly fingerprint the
    /// orchestrator can use to short-circuit cross-file work.
    #[test]
    fn source_signature_is_stable_across_identical_text() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "sig.rocky", "from orders\nselect { id }");

        let mut db = RockyDatabase::default();
        let src = read_source(&mut db, a).expect("read");
        let first = source_signature(&db, src);

        // Set the same text — input revision bumps but AST is identical.
        src.set_text(&mut db)
            .to("from orders\nselect { id }".to_string());
        let second = source_signature(&db, src);
        assert_eq!(
            first, second,
            "signature must be stable for an identical AST across set_text",
        );

        // Now actually change the AST.
        src.set_text(&mut db)
            .to("from orders\nselect { id, total }".to_string());
        let third = source_signature(&db, src);
        assert_ne!(
            first, third,
            "signature must change when the AST genuinely changes",
        );
    }

    /// Smoke test the public `FileTypecheck` shape so changes to it
    /// surface in CI (the orchestrator depends on these fields).
    #[test]
    fn file_typecheck_shape() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let a = write_model(&tmp, "shape.rocky", "from orders\nselect { id, total }");

        let mut db = RockyDatabase::default();
        let src = read_source(&mut db, a).expect("read");
        let ft: Arc<FileTypecheck> = file_typecheck(&db, src);

        assert!(
            !ft.sql.is_empty(),
            "non-empty SQL must be produced for a valid file",
        );
        assert_eq!(
            ft.output_columns,
            vec!["id".to_string(), "total".to_string()],
            "output_columns must match the trailing `select {{ }}` step",
        );
        assert!(
            ft.diagnostics.is_empty(),
            "valid input must not produce diagnostics",
        );
    }

    /// `SourceFile` round-trips through the public API exposed by this
    /// module — guard against accidental removal of the re-exports.
    #[test]
    fn module_re_exports_compose() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset_counter();

        let tmp = TempDir::new().expect("tempdir");
        let path = write_model(&tmp, "compose.rocky", "from orders\nselect { id }");

        let mut db = RockyDatabase::default();
        // Use the re-exports — if these names go away the test breaks
        // and downstream callers know to update.
        let src: SourceFile = read_source(&mut db, path).expect("read");
        let _ = file_typecheck(&db, src);
    }
}
