//! Gate tests for the run-level idempotency claim on an OBJECT-STORE state
//! backend (S3 / GCS), over an in-memory store installed through the
//! `provider_for_uri` test seam.
//!
//! These cover issue #2458: take-over, finalize and sweep are conditional
//! writes. Each test holds `remote_testing::serial_guard()` because the
//! provider override is process-global.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rocky_core::config::IdempotencyConfig;
use rocky_core::idempotency::{
    FinalOutcome, IdempotencyBackend, IdempotencyCheck, IdempotencyEntry, IdempotencyState,
    ObjectStoreScheme, object_store_idempotency_path, testing,
};
use rocky_core::object_store::ObjectStoreProvider;
use rocky_core::state_sync::remote_testing::{install_global, serial_guard};
use tokio::sync::Barrier;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 21, 8, 0, 0).unwrap()
}

fn cfg(in_flight_ttl_hours: u32) -> IdempotencyConfig {
    IdempotencyConfig {
        in_flight_ttl_hours,
        ..IdempotencyConfig::default()
    }
}

fn backend() -> IdempotencyBackend {
    IdempotencyBackend::ObjectStore {
        scheme: ObjectStoreScheme::S3,
        uri: "s3://test".into(),
    }
}

async fn read_entry(provider: &ObjectStoreProvider, key: &str) -> IdempotencyEntry {
    let path = object_store_idempotency_path(key);
    let body = provider.get(path.to_str().unwrap()).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// An in-memory store that makes the first `n` full reads wait for each
/// other. Both readers have then seen the same version before either writes,
/// which is the interleaving behind #2458.
#[derive(Debug)]
struct RendezvousStore {
    inner: InMemory,
    remaining: AtomicUsize,
    barrier: Barrier,
}

impl RendezvousStore {
    fn new(readers: usize) -> Self {
        Self {
            inner: InMemory::new(),
            remaining: AtomicUsize::new(0),
            barrier: Barrier::new(readers),
        }
    }

    /// Make the next `n` full reads meet at the barrier.
    fn arm(&self, n: usize) {
        self.remaining.store(n, Ordering::SeqCst);
    }
}

impl fmt::Display for RendezvousStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RendezvousStore")
    }
}

#[async_trait]
impl ObjectStore for RendezvousStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options.clone()).await;
        if !options.head
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            self.barrier.wait().await;
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A shared in-memory "bucket" for the backend under test.
struct Bucket {
    provider: ObjectStoreProvider,
    store: Arc<RendezvousStore>,
    _guard: rocky_core::state_sync::remote_testing::GlobalOverrideGuard,
    _serial: rocky_core::state_sync::remote_testing::SerialGuard,
}

fn bucket(readers: usize) -> Bucket {
    let serial = serial_guard();
    let store = Arc::new(RendezvousStore::new(readers));
    let provider = ObjectStoreProvider::from_store(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        "s3",
        "test",
        "",
    );
    let guard = install_global(provider.clone());
    Bucket {
        provider,
        store,
        _guard: guard,
        _serial: serial,
    }
}

/// A claim that crashes is adopted only after its own expiry.
#[tokio::test]
async fn a_crashed_claim_is_adopted_only_after_it_expires() {
    let bucket = bucket(2);
    let b = backend();
    let first = b
        .check_and_claim_at(None, "k", "run-a", &cfg(24), t0())
        .await
        .unwrap();
    assert!(matches!(first, IdempotencyCheck::Proceed));

    let early = b
        .check_and_claim_at(None, "k", "run-b", &cfg(24), t0() + Duration::hours(23))
        .await
        .unwrap();
    assert!(
        matches!(early, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-a"),
        "{early:?}"
    );

    let late = b
        .check_and_claim_at(None, "k", "run-b", &cfg(24), t0() + Duration::hours(25))
        .await
        .unwrap();
    assert!(
        matches!(late, IdempotencyCheck::AdoptStale { ref prior_run_id } if prior_run_id == "run-a"),
        "{late:?}"
    );
    assert_eq!(read_entry(&bucket.provider, "k").await.run_id, "run-b");
}

/// The sweep judges a claim by the expiry stamped on it, not by the
/// `in_flight_ttl_hours` of the config it runs under.
#[tokio::test]
async fn the_sweep_keeps_a_claim_whose_own_expiry_is_ahead_after_the_config_ttl_drops() {
    let bucket = bucket(2);
    let b = backend();
    b.check_and_claim_at(None, "k", "run-a", &cfg(30), t0())
        .await
        .unwrap();

    let ten_hours = t0() + Duration::hours(10);
    assert_eq!(b.sweep_expired_at(None, ten_hours).await.unwrap(), 0);
    let second = b
        .check_and_claim_at(None, "k", "run-b", &cfg(6), ten_hours)
        .await
        .unwrap();
    assert!(
        matches!(second, IdempotencyCheck::SkipInFlight { .. }),
        "{second:?}"
    );

    let swept = b
        .sweep_expired_at(None, t0() + Duration::hours(31))
        .await
        .unwrap();
    assert_eq!(swept, 1);
    let path = object_store_idempotency_path("k");
    assert!(
        !bucket
            .provider
            .exists(path.to_str().unwrap())
            .await
            .unwrap()
    );
}

/// Two runs read the same expired claim, then both try to take it over.
/// Exactly one wins. The other sees the winner's fresh claim and skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_adopters_of_one_expired_claim_exactly_one_wins() {
    let bucket = bucket(2);
    let b = backend();
    b.check_and_claim_at(None, "k", "run-a", &cfg(24), t0())
        .await
        .unwrap();

    // Both adopters read the expired claim before either one writes.
    bucket.store.arm(2);
    let now = t0() + Duration::hours(25);
    let (left, right) = tokio::join!(
        async {
            b.check_and_claim_at(None, "k", "run-b", &cfg(24), now)
                .await
        },
        async {
            b.check_and_claim_at(None, "k", "run-c", &cfg(24), now)
                .await
        },
    );
    let verdicts = [left.unwrap(), right.unwrap()];
    let winners = verdicts
        .iter()
        .filter(|v| matches!(v, IdempotencyCheck::AdoptStale { .. }))
        .count();
    let skipped = verdicts
        .iter()
        .filter(|v| matches!(v, IdempotencyCheck::SkipInFlight { .. }))
        .count();
    assert_eq!((winners, skipped), (1, 1), "{verdicts:?}");

    let held = read_entry(&bucket.provider, "k").await;
    assert!(
        held.run_id == "run-b" || held.run_id == "run-c",
        "the stored claim belongs to the winner: {held:?}"
    );
    assert_eq!(held.state, IdempotencyState::InFlight);
}

/// A slow original run finishes after its claim was taken over. Its finalize
/// must not overwrite the new owner's claim.
#[tokio::test]
async fn finalize_by_a_non_owner_is_refused() {
    let bucket = bucket(2);
    let b = backend();
    b.check_and_claim_at(None, "k", "run-a", &cfg(24), t0())
        .await
        .unwrap();
    b.check_and_claim_at(None, "k", "run-b", &cfg(24), t0() + Duration::hours(25))
        .await
        .unwrap();

    b.finalize_at(
        None,
        "k",
        "run-a",
        FinalOutcome::Succeeded,
        &cfg(24),
        t0() + Duration::hours(26),
    )
    .await
    .unwrap();
    let held = read_entry(&bucket.provider, "k").await;
    assert_eq!(held.run_id, "run-b");
    assert_eq!(held.state, IdempotencyState::InFlight);

    // The owner's finalize lands, and later runs skip as completed.
    b.finalize_at(
        None,
        "k",
        "run-b",
        FinalOutcome::Succeeded,
        &cfg(24),
        t0() + Duration::hours(27),
    )
    .await
    .unwrap();
    assert_eq!(
        read_entry(&bucket.provider, "k").await.state,
        IdempotencyState::Succeeded
    );
    let later = b
        .check_and_claim_at(None, "k", "run-d", &cfg(24), t0() + Duration::hours(28))
        .await
        .unwrap();
    assert!(
        matches!(later, IdempotencyCheck::SkipCompleted { ref run_id } if run_id == "run-b"),
        "{later:?}"
    );
}

/// The sweep read an expired claim. Before it deleted, another run took the
/// claim over. The sweep must leave the new claim alone.
#[tokio::test]
async fn the_sweep_does_not_delete_a_claim_re_adopted_between_read_and_delete() {
    let bucket = bucket(2);
    let b = backend();
    b.check_and_claim_at(None, "k", "run-a", &cfg(24), t0())
        .await
        .unwrap();
    let after_expiry = t0() + Duration::hours(25);

    let seen_by_sweep = testing::observe_claim(&bucket.provider, "k")
        .await
        .unwrap()
        .expect("the claim exists");

    // A run adopts the expired claim after the sweep read it.
    let verdict = b
        .check_and_claim_at(None, "k", "run-b", &cfg(24), after_expiry)
        .await
        .unwrap();
    assert!(
        matches!(verdict, IdempotencyCheck::AdoptStale { .. }),
        "{verdict:?}"
    );

    let deleted = testing::sweep_observed(&bucket.provider, &seen_by_sweep, after_expiry)
        .await
        .unwrap();
    assert!(!deleted, "the sweep must skip a claim that changed");
    assert_eq!(read_entry(&bucket.provider, "k").await.run_id, "run-b");
}
