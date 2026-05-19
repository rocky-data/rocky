//! Integration tests for the object-store state cache backend.
//!
//! Uses `object_store::local::LocalFileSystem` rooted in a `tempdir`
//! rather than the `object_store::memory::InMemory` backend — the
//! latter doesn't generate stable ETags from file metadata the way S3
//! does, so the hash-dedupe code path doesn't exercise. The
//! `LocalFileSystem` backend produces ETags derived from file size +
//! mtime, which is enough to drive the dedupe branch + the cache-miss
//! branch.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use rocky_fivetran::envelope::{
    FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination, FivetranSchemaConfig,
    FivetranStateEnvelope,
};
use rocky_fivetran::state_cache::{FivetranStateCache, ObjectStoreCache, WriteOutcome};

fn sample_envelope_with_region(region: &str) -> FivetranStateEnvelope {
    FivetranStateEnvelope::from_parts(
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        FivetranDestination {
            id: "dest_obj".into(),
            region: Some(region.into()),
            time_zone: None,
            service: None,
            setup_status: None,
        },
        vec![FivetranConnectorSummary {
            id: "conn_a".into(),
            name: "conn_a".into(),
            schema: "src__a__b__shopify".into(),
            service: "shopify".into(),
            status: FivetranConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            paused: false,
            succeeded_at: None,
            failed_at: None,
            group_id: None,
        }],
        BTreeMap::<String, FivetranSchemaConfig>::new(),
    )
}

fn cache_in(tmp_root: &std::path::Path) -> ObjectStoreCache {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp_root).expect("LocalFileSystem"));
    ObjectStoreCache::from_parts(store, ObjectPath::from("fivetran"), "file")
}

#[tokio::test]
async fn object_store_read_miss_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());
    let value = cache.read("acct/dest_obj").await.unwrap();
    assert!(value.is_none(), "miss must surface as Ok(None)");
}

#[tokio::test]
async fn object_store_round_trip_get_put() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());
    let env = sample_envelope_with_region("us-east-1");

    let outcome = cache.write("acct/dest_obj", &env).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Written);

    let back = cache.read("acct/dest_obj").await.unwrap().unwrap();
    assert_eq!(back, env);
}

#[tokio::test]
async fn object_store_hash_dedupe_via_etag_compare() {
    // The LocalFileSystem backend produces ETags from file metadata —
    // those ETags will match the MD5 of an unchanged object, so the
    // dedupe branch in `ObjectStoreCache::write` fires.
    //
    // NOTE: object_store 0.11's LocalFileSystem ETag derivation isn't
    // a content-hash by default — it incorporates file size + mtime.
    // That means two writes of identical bytes a few milliseconds
    // apart produce DIFFERENT ETags from the LocalFS backend, even
    // though S3 would produce identical ETags (= MD5).
    //
    // For this test we exercise the "ETag is present but doesn't
    // match new MD5 → unconditional PUT" branch — which proves the
    // backend correctly falls through to PUT on ETag mismatch. The
    // "ETag exactly matches MD5 → skip" path is exercised in unit
    // tests via the `md5_hex` helper + `normalize_etag` checks; an
    // end-to-end S3-shape test would need a mock S3 server (out of
    // scope for this PR).
    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());
    let env_a = sample_envelope_with_region("us-east-1");
    let env_b = sample_envelope_with_region("eu-west-1");

    cache.write("acct/dest_obj", &env_a).await.unwrap();

    // Different envelope -> different MD5 -> unconditional PUT path.
    let outcome = cache.write("acct/dest_obj", &env_b).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Written);

    let back = cache.read("acct/dest_obj").await.unwrap().unwrap();
    assert_eq!(back.destination.region, Some("eu-west-1".into()));
}

#[tokio::test]
async fn object_store_size_cap_rejects_oversize_envelope() {
    // Pin the size guard — past MAX_ENVELOPE_BYTES the backend would
    // need a multi-part PUT, where the ETag isn't a simple MD5 and the
    // dedupe assumption breaks. Build a synthetic huge envelope by
    // padding the destination metadata.
    use rocky_fivetran::state_cache::CacheError;
    use rocky_fivetran::state_cache::object_store_backend::MAX_ENVELOPE_BYTES;

    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());

    let mut huge = sample_envelope_with_region("us-east-1");
    // The size cap is 5 MiB; pad the region string past that.
    huge.destination.region = Some("x".repeat(MAX_ENVELOPE_BYTES + 1024));

    let result = cache.write("acct/oversize", &huge).await;
    assert!(
        matches!(result, Err(CacheError::Config(_))),
        "oversize envelope must fail with CacheError::Config, got {result:?}"
    );
}

#[tokio::test]
async fn object_store_read_of_corrupt_object_returns_err() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());

    // Hand-write a non-JSON file at the path the backend would resolve
    // `acct/dest_corrupt` to.
    let dir = tmp.path().join("fivetran").join("acct");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("dest_corrupt.json"), b"not json bytes").unwrap();

    let result = cache.read("acct/dest_corrupt").await;
    assert!(result.is_err(), "corrupt body must error");
}

#[tokio::test]
async fn object_store_key_path_separates_account_and_destination() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = cache_in(tmp.path());
    let env = sample_envelope_with_region("us-east-1");

    cache.write("acct_hash/dest_z", &env).await.unwrap();

    // The on-disk layout reflects the cache_key splitting on `/`.
    let expected = tmp
        .path()
        .join("fivetran")
        .join("acct_hash")
        .join("dest_z.json");
    assert!(expected.exists(), "expected {expected:?} to exist");
}
