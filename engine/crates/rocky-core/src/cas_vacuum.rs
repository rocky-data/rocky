//! Refcount-gated physical deletion of content-addressed artifact bytes.
//!
//! [`delete_unreferenced_artifacts`] is the object-store delete path for the
//! `OUTPUT_ARTIFACTS` ledger. It deletes a file only once its content hash
//! has no ledger reference left:
//!
//! ```text
//! candidates ──► partition_vacuum_candidates ──► safe_to_delete ──► retire row ──► delete bytes
//!                (refcount == 1: the row is      (re-counted in      (refcount   (object store)
//!                 the hash's only reference)      the write txn)      now 0)
//!                          │
//!                          └──► still_referenced / lookup error ──► kept (bytes untouched)
//! ```
//!
//! The ledger row is retired **before** the bytes are deleted. A failed
//! delete therefore leaves an orphan byte (safe, reclaimable later), never a
//! live ledger row pointing at missing bytes.
//!
//! Scope: the refcount sees Rocky-ledger references only. It does not prove a
//! file is gone from its table's Delta log. A caller must establish that
//! separately (see `rocky gc`'s removal-proof oracle) before handing a
//! candidate here. No CLI command calls this today: `rocky gc` eviction is
//! ledger-only and `[gc] physical_delete = true` remains a hard error.

use crate::object_store::ObjectStoreProvider;
use crate::state::{ArtifactRecord, StateStore, partition_vacuum_candidates};

/// What [`delete_unreferenced_artifacts`] did with each candidate.
#[derive(Debug, Default)]
pub struct VacuumDeleteReport {
    /// Ledger row retired and bytes deleted.
    pub deleted: Vec<ArtifactRecord>,
    /// Bytes kept: another ledger row still shares the hash, the refcount
    /// lookup failed, or the row changed before it could be retired.
    pub kept: Vec<ArtifactRecord>,
    /// Hashes whose refcount lookup failed (their candidates are in `kept`).
    pub lookup_errors: Vec<String>,
    /// Ledger row retired but the object-store delete failed. The bytes are
    /// an orphan: unreferenced, safe to delete on a later sweep.
    pub delete_errors: Vec<(ArtifactRecord, String)>,
    /// The ledger retire itself errored; nothing was deleted for these.
    pub retire_errors: Vec<(ArtifactRecord, String)>,
}

/// Delete the bytes of every candidate whose content hash has no other
/// ledger reference, retiring its ledger row first.
///
/// `candidates` are live ledger rows the caller has already chosen to retire
/// (by age, retention, or a Delta-log removal proof). `provider` must be
/// rooted where [`ArtifactRecord::file_path`] is relative to: the bucket
/// root for the content-addressed writer.
///
/// Fail-closed throughout: a candidate is deleted only when
/// [`partition_vacuum_candidates`] puts it in `safe_to_delete` AND
/// [`StateStore::retire_artifact_if_sole_reference`] confirms, inside its
/// write transaction, that the row is still the hash's sole reference.
pub async fn delete_unreferenced_artifacts(
    store: &StateStore,
    provider: &ObjectStoreProvider,
    candidates: Vec<ArtifactRecord>,
) -> VacuumDeleteReport {
    let partition = partition_vacuum_candidates(candidates, |h| store.refcount_for_hash(h));

    let mut report = VacuumDeleteReport {
        kept: partition.still_referenced,
        lookup_errors: partition.lookup_errors,
        ..VacuumDeleteReport::default()
    };

    for record in partition.safe_to_delete {
        match store.retire_artifact_if_sole_reference(&record) {
            Ok(true) => match provider.delete(&record.file_path).await {
                Ok(()) => report.deleted.push(record),
                Err(e) => {
                    tracing::warn!(
                        file_path = %record.file_path,
                        error = %e,
                        "vacuum: ledger row retired but the object delete failed; bytes are an orphan"
                    );
                    report.delete_errors.push((record, e.to_string()));
                }
            },
            Ok(false) => report.kept.push(record),
            Err(e) => report.retire_errors.push((record, e.to_string())),
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn temp_store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        (store, dir)
    }

    fn artifact(run_id: &str, hash: &str, path: &str) -> ArtifactRecord {
        ArtifactRecord {
            blake3_hash: hash.to_string(),
            run_id: run_id.to_string(),
            model_name: "m".to_string(),
            file_path: path.to_string(),
            commit_version: 1,
            size_bytes: 4,
            written_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn referenced_file_survives_and_unreferenced_file_is_deleted() {
        let (store, _dir) = temp_store();
        let provider = ObjectStoreProvider::in_memory();

        // `shared` bytes: written by run-1, reused by run-2 (same hash, same
        // content-addressed path). `solo` bytes: only run-1 references them.
        let shared_path = "warehouse/t/shared.parquet";
        let solo_path = "warehouse/t/solo.parquet";
        provider
            .put(shared_path, Bytes::from_static(b"PAR1"))
            .await
            .unwrap();
        provider
            .put(solo_path, Bytes::from_static(b"PAR1"))
            .await
            .unwrap();
        let shared_run1 = artifact("run-1", "h-shared", shared_path);
        let shared_run2 = artifact("run-2", "h-shared", shared_path);
        let solo = artifact("run-1", "h-solo", solo_path);
        store.record_artifact(&shared_run1).unwrap();
        store.record_artifact(&shared_run2).unwrap();
        store.record_artifact(&solo).unwrap();

        // Retire run-1: both of its rows are candidates.
        let report =
            delete_unreferenced_artifacts(&store, &provider, vec![shared_run1, solo]).await;

        assert_eq!(report.deleted.len(), 1, "{report:?}");
        assert_eq!(report.deleted[0].blake3_hash, "h-solo");
        assert_eq!(report.kept.len(), 1, "{report:?}");
        assert_eq!(report.kept[0].blake3_hash, "h-shared");
        assert!(report.delete_errors.is_empty() && report.retire_errors.is_empty());

        // The bytes a live ledger row (run-2) still references survive.
        assert!(provider.exists(shared_path).await.unwrap());
        assert_eq!(store.refcount_for_hash("h-shared").unwrap(), 2);
        // The unreferenced bytes are gone, and so is their last ledger row.
        assert!(!provider.exists(solo_path).await.unwrap());
        assert_eq!(store.refcount_for_hash("h-solo").unwrap(), 0);
    }

    #[tokio::test]
    async fn a_candidate_with_no_ledger_row_is_kept() {
        // Refcount 0 means the ledger never saw these bytes: absence of
        // evidence, not proof they are unreferenced. Keep them.
        let (store, _dir) = temp_store();
        let provider = ObjectStoreProvider::in_memory();
        let path = "warehouse/t/unknown.parquet";
        provider
            .put(path, Bytes::from_static(b"PAR1"))
            .await
            .unwrap();

        let report =
            delete_unreferenced_artifacts(&store, &provider, vec![artifact("r", "h-x", path)])
                .await;

        assert!(report.deleted.is_empty());
        assert_eq!(report.kept.len(), 1);
        assert!(provider.exists(path).await.unwrap());
    }

    #[test]
    fn retire_refuses_while_another_row_shares_the_hash() {
        // The write-txn re-count: a reference that appears after the
        // partition pass must still hold the bytes.
        let (store, _dir) = temp_store();
        let a = artifact("run-1", "h", "p/h.parquet");
        store.record_artifact(&a).unwrap();
        store
            .record_artifact(&artifact("run-2", "h", "p/h.parquet"))
            .unwrap();

        assert!(!store.retire_artifact_if_sole_reference(&a).unwrap());
        assert_eq!(store.refcount_for_hash("h").unwrap(), 2);

        // A hash mismatch at the row's key retires nothing either.
        let wrong = artifact("run-1", "other", "p/h.parquet");
        assert!(!store.retire_artifact_if_sole_reference(&wrong).unwrap());
        assert_eq!(store.refcount_for_hash("h").unwrap(), 2);
    }
}
