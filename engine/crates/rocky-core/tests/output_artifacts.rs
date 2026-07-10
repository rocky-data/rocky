//! Integration tests for the [`OUTPUT_ARTIFACTS`] redb table — the
//! content-addressed artifact ledger.
//!
//! These tests pin the **query shape** the reclamation path (`rocky gc`
//! refcounting) builds on, against the live `StateStore` API. The unit
//! tests in `state.rs` cover correctness of individual methods; this
//! file demonstrates the full end-to-end shape:
//!
//! 1. Multiple runs record artifacts to the same physical state file.
//! 2. "Which (run, model) tuples reference hash X?" is answered via
//!    `list_artifacts_by_hash`.
//! 3. The query result is enough to answer "is this file orphaned?" —
//!    the refcount predicate `rocky gc` evaluates.
//!
//! Note: there's no end-to-end test against a live warehouse here, a
//! known coverage gap — content-addressed writes need Databricks Delta
//! UniForm, and the playground POC is DuckDB-only, so live verification
//! of this path runs against a warehouse sandbox rather than in CI.

use chrono::Utc;
use rocky_core::state::{ArtifactRecord, StateStore};
use tempfile::TempDir;

fn open_fresh_store() -> (StateStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");
    let store = StateStore::open(&path).unwrap();
    (store, dir)
}

fn artifact(
    run_id: &str,
    model: &str,
    hash: &str,
    path: &str,
    commit: u64,
    size: u64,
) -> ArtifactRecord {
    ArtifactRecord {
        blake3_hash: hash.to_string(),
        run_id: run_id.to_string(),
        model_name: model.to_string(),
        file_path: path.to_string(),
        commit_version: commit,
        size_bytes: size,
        written_at: Utc::now(),
    }
}

/// Three runs against the same content-addressed table, where the
/// second run produces identical content (same blake3) to the first.
/// Phase 6 refcount sweep on the shared hash must see *both* references
/// — pulling it would orphan a file still in use by run-1.
#[test]
fn phase6_query_sees_shared_hash_across_runs() {
    let (store, _dir) = open_fresh_store();

    // Run 1: writes hash-A
    store
        .record_artifact(&artifact(
            "run-1",
            "fct_orders",
            "blake3:A",
            "s3://bucket/prefix/A.parquet",
            1,
            1024,
        ))
        .unwrap();
    // Run 2: writes hash-A again (idempotent recipe → same content
    // hash; the writer dedupes the bytes via cond-put, but the
    // *reference* still needs to count).
    store
        .record_artifact(&artifact(
            "run-2",
            "fct_orders",
            "blake3:A",
            "s3://bucket/prefix/A.parquet",
            2,
            1024,
        ))
        .unwrap();
    // Run 3: writes hash-B (different content)
    store
        .record_artifact(&artifact(
            "run-3",
            "fct_orders",
            "blake3:B",
            "s3://bucket/prefix/B.parquet",
            3,
            2048,
        ))
        .unwrap();

    // Phase 6 query: "which runs reference blake3:A?" — answer must
    // include both run-1 and run-2.
    let refs_a = store.list_artifacts_by_hash("blake3:A").unwrap();
    // Note: the (run-1, fct_orders, A.parquet) and (run-2, fct_orders,
    // A.parquet) tuples differ in run_id, so they live in two distinct
    // rows; record_artifact keyed on the run_id prefix is what gives
    // us this. If the spike had keyed on (model, file_path) only,
    // we'd have lost run-1's reference when run-2 wrote — exactly the
    // bug Phase 6 refcount would silently mis-attribute.
    assert_eq!(
        refs_a.len(),
        2,
        "blake3:A must be referenced by both run-1 and run-2",
    );
    let run_ids: std::collections::BTreeSet<String> =
        refs_a.iter().map(|a| a.run_id.clone()).collect();
    assert!(run_ids.contains("run-1"));
    assert!(run_ids.contains("run-2"));

    // Different hash → singleton.
    let refs_b = store.list_artifacts_by_hash("blake3:B").unwrap();
    assert_eq!(refs_b.len(), 1);
    assert_eq!(refs_b[0].run_id, "run-3");

    // Phase 6 sanity-check: total count.
    assert_eq!(store.count_artifacts().unwrap(), 3);
}

/// A partitioned content-addressed model produces multiple artifacts
/// per run (one per partition group). The ledger must record each
/// independently so Phase 6 can refcount them per-partition.
#[test]
fn partitioned_run_produces_independent_artifacts() {
    let (store, _dir) = open_fresh_store();
    // One run, one model, three partition groups → three artifacts.
    for (i, part) in ["eu", "us", "apac"].iter().enumerate() {
        store
            .record_artifact(&artifact(
                "run-part",
                "fct_orders",
                &format!("blake3:{part}"),
                &format!("s3://bucket/prefix/region={part}/{part}.parquet"),
                (i + 1) as u64,
                1024,
            ))
            .unwrap();
    }

    let in_run = store.list_artifacts_for_run("run-part").unwrap();
    assert_eq!(in_run.len(), 3);

    // Each partition's hash is independently queryable.
    let eu_refs = store.list_artifacts_by_hash("blake3:eu").unwrap();
    assert_eq!(eu_refs.len(), 1);
    assert_eq!(eu_refs[0].model_name, "fct_orders");

    let us_refs = store.list_artifacts_by_hash("blake3:us").unwrap();
    assert_eq!(us_refs.len(), 1);
}

/// The ledger survives `StateStore::open` round-trips. Critical for
/// Phase 6, which will run as a separate sub-command (`rocky vacuum`?
/// `rocky compact`?) against a state file written by previous `rocky
/// run` processes.
#[test]
fn ledger_persists_across_separate_store_opens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");

    {
        let store = StateStore::open(&path).unwrap();
        store
            .record_artifact(&artifact(
                "writer-process",
                "m",
                "h",
                "s3://b/p/x.parquet",
                1,
                100,
            ))
            .unwrap();
        // Store drops here — file lock released, batch flushed.
    }

    // Simulated "Phase 6 vacuum process" reads the ledger.
    let vacuum_store = StateStore::open(&path).unwrap();
    let refs = vacuum_store.list_artifacts_by_hash("h").unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].run_id, "writer-process");
}
