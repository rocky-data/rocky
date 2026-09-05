//! Cross-pod remote-state integration tests (WP-01 rig).
//!
//! These drive the REAL `download_state` / `upload_state` paths from outside
//! the crate, against a shared fault-injectable in-memory "S3" installed via
//! the process-global provider override
//! (`state_sync::remote_testing::install_global`). Each test holds
//! `remote_testing::serial_guard()` because the override is process-global.
//!
//! Requires the `test-support` feature (on by default for test targets via
//! rocky-core's self-referential dev-dependency).

use chrono::Utc;
use rocky_core::fault_store::FaultOp;
use rocky_core::state_sync::{self, StateAuthority, remote_testing};
use rocky_core::test_harness::CrossPodHarness;
use rocky_ir::WatermarkState;

fn wm_now() -> WatermarkState {
    let now = Utc::now();
    WatermarkState {
        last_value: now,
        updated_at: now,
    }
}

/// The rig's core promise: state written by pod A round-trips to pod B
/// through the real upload → shared remote → download chain.
#[tokio::test]
async fn pod_b_sees_pod_a_watermark() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // Pod A: open the store (writer lock, like a run), record a watermark,
    // release, and push state to the shared remote.
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.orders", &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_a).await.expect("pod A upload");

    // Pod B: pull the remote and read pod A's watermark.
    let authority = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B download");
    assert_eq!(
        authority,
        StateAuthority::Authoritative,
        "a restored remote object is Authoritative"
    );
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_watermark("cat.sch.orders").unwrap().is_some(),
        "pod B must see the watermark pod A uploaded"
    );
}

/// PINS CURRENT BEHAVIOR (RD-002): interleaved writers on a shared remote
/// state object are last-writer-wins — the second upload wholesale-replaces
/// the first, silently dropping pod A's watermark.
///
/// This test is deliberately GREEN against today's engine: it documents the
/// lost-update window, it does not endorse it. When compare-and-swap /
/// conditional-put publication lands (a later WP-01 PR; see also #1120), this
/// test MUST flip to a RED baseline: pod B's blind upload gets REFUSED
/// (its ETag/generation is stale), and the assertions below invert —
/// `from_a` survives and pod B is told to re-sync.
#[tokio::test]
async fn interleaved_writers_last_writer_wins_documented() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // Both pods sync from the same (empty) remote — neither sees the other.
    assert_eq!(
        harness.download(&harness.pod_a).await.unwrap(),
        StateAuthority::FreshStart,
        "an empty remote is a genuine fresh start"
    );
    assert_eq!(
        harness.download(&harness.pod_b).await.unwrap(),
        StateAuthority::FreshStart
    );

    // Pod A writes and uploads first...
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.from_a", &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_a).await.expect("pod A upload");

    // ...then pod B — which never saw A's upload — writes and uploads.
    {
        let store = harness.open_store(&harness.pod_b);
        store.set_watermark("cat.sch.from_b", &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_b).await.expect("pod B upload");

    // A fresh download now serves pod B's file wholesale: pod A's watermark
    // is gone from the shared remote.
    assert_eq!(
        harness.download(&harness.pod_a).await.unwrap(),
        StateAuthority::Authoritative
    );
    let store = harness.open_store(&harness.pod_a);
    assert!(
        store.get_watermark("cat.sch.from_b").unwrap().is_some(),
        "the last writer's row is served"
    );
    assert!(
        store.get_watermark("cat.sch.from_a").unwrap().is_none(),
        "RD-002 (documented, not endorsed): the interleaved writer's row is \
         silently lost under last-writer-wins"
    );
}

/// The reason the global override exists: the shared provider must be
/// reachable from a `std::thread::spawn`ed OS thread — the shape of
/// rocky-cli's `block_on_state_sync`, which runs state futures on a dedicated
/// runtime thread. The crate-private thread-local seam alone can never
/// satisfy this (the override would be unset on the new thread, so the
/// upload would try to build a real S3 client).
#[tokio::test]
async fn global_override_visible_across_threads() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    {
        let store = harness.open_store(&harness.pod_a);
        store
            .set_watermark("cat.sch.cross_thread", &wm_now())
            .unwrap();
    }

    // Upload from a spawned OS thread with its own runtime.
    let cfg = harness.pod_a.cfg.clone();
    let path = harness.pod_a.state_path.clone();
    let uploaded = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build upload runtime")
            .block_on(state_sync::upload_state(&cfg, &path, false))
    })
    .join()
    .expect("upload thread panicked");
    uploaded.expect("upload from a spawned thread must resolve the shared provider");

    // The spawned thread hit the SAME shared store this thread's handle
    // wraps: its Put registered on the shared fault counters...
    assert!(
        harness.faults.count(FaultOp::Put) >= 1,
        "the cross-thread upload must land on the shared FaultingStore"
    );

    // ...and this thread can download what that thread uploaded.
    assert_eq!(
        harness
            .download(&harness.pod_b)
            .await
            .expect("pod B download"),
        StateAuthority::Authoritative
    );
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_watermark("cat.sch.cross_thread")
            .unwrap()
            .is_some(),
        "state uploaded from a foreign thread round-trips to pod B"
    );
}

/// Scheduler tables (`schedule_state`, `schedule_claims`) are LOCAL-ONLY by
/// design: `rocky tick` state is per-machine, so the upload strips them
/// fail-closed and a download re-merges them from the local file rather than
/// overwriting them from the remote. This pins the whole cycle — a committed
/// tick cursor + claim never reach the remote (so a second host inherits none),
/// and survive the owning pod's own upload → download round-trip.
#[tokio::test]
async fn scheduler_state_and_claims_stay_local_across_sync_cycle() {
    use rocky_core::schedule::{ClaimCas, ClaimRecord, ScheduleStateMutation, ScheduleStateRecord};

    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    // The store API is key-opaque, so any consistent claim key works here.
    let claim_key = "raw\u{1f}cron\u{1f}2026-05-02T03:00:00+00:00";

    // Pod A commits one replicated row (a watermark) and two local-only rows
    // (a schedule cursor + a claim), then releases the writer lock — the state a
    // real tick leaves between passes.
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.orders", &wm_now()).unwrap();
        store
            .put_schedule_state(
                "raw",
                &ScheduleStateRecord {
                    last_fire_logical_ts: Some("2026-05-02T03:00:00+00:00".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let claim = ClaimRecord::new_submitted("sub-1".to_string(), Utc::now());
        assert!(matches!(
            store
                .schedule_claim_cas(claim_key, None, &claim, "raw", &ScheduleStateMutation::None)
                .unwrap(),
            ClaimCas::Won
        ));
    }
    harness.upload(&harness.pod_a).await.expect("pod A upload");

    // A second host that pulls this remote inherits the watermark but NO
    // scheduler state — it would keep an independent cursor (the documented
    // "one scheduler instance per project" invariant, pinned in code).
    let _ = harness
        .download(&harness.pod_b)
        .await
        .expect("pod B download");
    {
        let store = harness.open_store(&harness.pod_b);
        assert!(
            store.get_watermark("cat.sch.orders").unwrap().is_some(),
            "replicated state crosses hosts"
        );
        assert!(
            store.get_schedule_state("raw").unwrap().is_none(),
            "scheduler cursor is local-only — never uploaded"
        );
        assert!(
            store.get_schedule_claim(claim_key).unwrap().is_none(),
            "scheduler claim is local-only — never uploaded"
        );
    }

    // Pod A's own sync cycle re-merges its scheduler rows from the local file
    // (the download replaces the replicated tables but preserves local-only
    // ones), so a committed tick write survives the child run's upload/download.
    let _ = harness
        .download(&harness.pod_a)
        .await
        .expect("pod A download");
    {
        let store = harness.open_store(&harness.pod_a);
        let cursor = store
            .get_schedule_state("raw")
            .unwrap()
            .expect("cursor survives the owner's own sync cycle");
        assert_eq!(
            cursor.last_fire_logical_ts.as_deref(),
            Some("2026-05-02T03:00:00+00:00")
        );
        assert!(store.get_schedule_claim(claim_key).unwrap().is_some());
        assert!(store.get_watermark("cat.sch.orders").unwrap().is_some());
    }
}

// ---------------------------------------------------------------------------
// `[cache.schemas] replicate` — the schema cache crosses pods only when the
// project opts in (#1620).
// ---------------------------------------------------------------------------

/// Seed one schema-cache entry into `pod`'s local store.
fn seed_schema_cache(harness: &CrossPodHarness, pod: &rocky_core::test_harness::Pod, table: &str) {
    let store = harness.open_store(pod);
    let key = rocky_core::schema_cache::schema_cache_key("cat", "staging", table);
    let entry = rocky_core::schema_cache::SchemaCacheEntry {
        columns: vec![rocky_core::schema_cache::StoredColumn {
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: false,
        }],
        cached_at: Utc::now(),
    };
    store.write_schema_cache_entry(&key, &entry).unwrap();
}

/// How many schema-cache rows `pod`'s local store holds.
fn schema_cache_len(harness: &CrossPodHarness, pod: &rocky_core::test_harness::Pod) -> usize {
    harness.open_store(pod).list_schema_cache().unwrap().len()
}

/// THE DEFAULT, UNCHANGED. With `replicate = false` — every project that has
/// not opted in — the schema cache stays node-local: pod A's cached warehouse
/// types must not reach pod B, so a fresh clone never inherits another
/// machine's stale types.
#[tokio::test]
async fn schema_cache_stays_local_by_default() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    seed_schema_cache(&harness, &harness.pod_a, "orders");
    harness
        .upload_replicating(&harness.pod_a, false)
        .await
        .expect("pod A upload");

    let _authority = harness
        .download_replicating(&harness.pod_b, false)
        .await
        .expect("pod B download");
    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        0,
        "the default posture must not replicate the schema cache"
    );
}

/// THE FIX. `[cache.schemas] replicate = true` makes the schema cache travel:
/// pod A's cached types reach pod B through the real upload → remote →
/// download chain. Before #1620 this test failed — the setting parsed and
/// changed neither leg.
#[tokio::test]
async fn schema_cache_crosses_pods_when_replicate_is_enabled() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    seed_schema_cache(&harness, &harness.pod_a, "orders");
    harness
        .upload_replicating(&harness.pod_a, true)
        .await
        .expect("pod A upload");

    let _authority = harness
        .download_replicating(&harness.pod_b, true)
        .await
        .expect("pod B download");
    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        1,
        "replicate = true must carry the schema cache across pods"
    );
}

/// Both legs are required. Uploading with `replicate = true` but downloading
/// with it off still leaves pod B empty — the download leg overwrites the
/// staged remote cache from pod B's own (empty) local table. This pins that
/// the fix is not accidentally one-sided.
#[tokio::test]
async fn replicating_upload_alone_does_not_reach_the_other_pod() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    seed_schema_cache(&harness, &harness.pod_a, "orders");
    harness
        .upload_replicating(&harness.pod_a, true)
        .await
        .expect("pod A upload");

    let _authority = harness
        .download_replicating(&harness.pod_b, false)
        .await
        .expect("pod B download");
    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        0,
        "a non-replicating download keeps its own local cache"
    );
}

/// HONEST FAILURE. A remote object written before anyone opted in carries NO
/// `schema_cache` table at all. Turning `replicate = true` on must not fail
/// that download — an absent table is not an error, and pod B simply has an
/// empty cache afterwards.
#[tokio::test]
async fn replicating_download_tolerates_a_remote_with_no_schema_cache_table() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // Pod A uploads under the OLD posture, so the remote has no schema_cache.
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.orders", &wm_now()).unwrap();
    }
    seed_schema_cache(&harness, &harness.pod_a, "orders");
    harness
        .upload_replicating(&harness.pod_a, false)
        .await
        .expect("pod A upload");

    // Pod B now opts in and downloads that older remote.
    let authority = harness
        .download_replicating(&harness.pod_b, true)
        .await
        .expect("an absent remote schema_cache table must not fail the download");
    assert_eq!(authority, StateAuthority::Authoritative);

    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_watermark("cat.sch.orders").unwrap().is_some(),
        "the replicated tables must still arrive"
    );
    assert_eq!(
        store.list_schema_cache().unwrap().len(),
        0,
        "no remote cache to inherit → empty, not an error"
    );
}

/// Replication must never DESTROY a warm local cache. When there is no remote
/// object at all, the download rebuilds the local file from the local-only
/// tables — and `schema_cache` stays in that set even under `replicate = true`,
/// or opting in would wipe this machine's cache every time the remote is
/// missing.
#[tokio::test]
async fn replicating_download_keeps_the_local_cache_when_the_remote_is_absent() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // A local store with a warm cache, and NOTHING uploaded to the remote.
    seed_schema_cache(&harness, &harness.pod_b, "orders");
    assert_eq!(schema_cache_len(&harness, &harness.pod_b), 1);

    let authority = harness
        .download_replicating(&harness.pod_b, true)
        .await
        .expect("an absent remote object is not a download failure");
    assert_eq!(authority, StateAuthority::FreshStart);

    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        1,
        "opting into replication must not discard the local cache when the remote is absent"
    );
}

/// The CAS upload leg carries the posture too. `upload_state_cas` is a SECOND
/// upload path, reached only under `concurrency_control = "cas"`, and it read
/// the same hard-coded constant. Wiring the plain upload and not this one would
/// have let a CAS project set `replicate = true` and replicate nothing —
/// silently, because every other leg would look wired.
#[tokio::test]
async fn schema_cache_crosses_pods_through_the_cas_upload_leg() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    let cas_cfg = rocky_core::config::StateConfig {
        concurrency_control: rocky_core::config::ConcurrencyControl::Cas,
        ..harness.pod_a.cfg.clone()
    };
    seed_schema_cache(&harness, &harness.pod_a, "orders");

    let mut session = state_sync::RemoteStateSession::new(
        &cas_cfg,
        &harness.pod_a.state_path,
        state_sync::FinalizeDurability::Durable,
        true,
    );
    let _authority = session.acquire().await.expect("acquire");
    session.finalize().await.expect("CAS finalize uploads");

    let _authority = harness
        .download_replicating(&harness.pod_b, true)
        .await
        .expect("pod B download");
    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        1,
        "the CAS upload leg must honour replicate = true"
    );
}

/// The CAS leg's default is unchanged: with `replicate = false` the cache
/// stays local, exactly as before the posture existed.
#[tokio::test]
async fn cas_upload_leg_keeps_the_schema_cache_local_by_default() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    let cas_cfg = rocky_core::config::StateConfig {
        concurrency_control: rocky_core::config::ConcurrencyControl::Cas,
        ..harness.pod_a.cfg.clone()
    };
    seed_schema_cache(&harness, &harness.pod_a, "orders");

    let mut session = state_sync::RemoteStateSession::new(
        &cas_cfg,
        &harness.pod_a.state_path,
        state_sync::FinalizeDurability::Durable,
        false,
    );
    let _authority = session.acquire().await.expect("acquire");
    session.finalize().await.expect("CAS finalize uploads");

    let _authority = harness
        .download_replicating(&harness.pod_b, true)
        .await
        .expect("pod B download");
    assert_eq!(
        schema_cache_len(&harness, &harness.pod_b),
        0,
        "a non-replicating CAS upload must not publish the schema cache"
    );
}

/// The mid-run periodic uploader is the THIRD upload path — it snapshots the
/// live store on a cadence rather than at the end of the run, and it read the
/// same hard-coded constant. A run that ends by crashing replicates only what
/// this leg pushed, so a posture it ignored would be a posture that silently
/// applied to some uploads and not others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_cache_crosses_pods_through_the_periodic_uploader() {
    use std::sync::Arc;
    use std::time::Duration;

    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    let store = Arc::new(rocky_core::state::StateStore::open(&harness.pod_a.state_path).unwrap());
    let key = rocky_core::schema_cache::schema_cache_key("cat", "staging", "orders");
    let entry = rocky_core::schema_cache::SchemaCacheEntry {
        columns: vec![rocky_core::schema_cache::StoredColumn {
            name: "id".into(),
            data_type: "BIGINT".into(),
            nullable: false,
        }],
        cached_at: Utc::now(),
    };
    store.write_schema_cache_entry(&key, &entry).unwrap();

    let mut session = state_sync::RemoteStateSession::new(
        &harness.pod_a.cfg,
        &harness.pod_a.state_path,
        state_sync::FinalizeDurability::ConfigDefault,
        true,
    );
    let _authority = session.acquire().await.expect("acquire");
    session.start_periodic_uploader(Arc::downgrade(&store), Duration::from_millis(40));

    // Wait for a tick to publish, then drain cooperatively.
    let mut replicated = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let probe = state_sync::download_state(&harness.pod_b.cfg, &harness.pod_b.state_path, true)
            .await
            .expect("pod B download");
        let _ = probe;
        if schema_cache_len(&harness, &harness.pod_b) == 1 {
            replicated = true;
            break;
        }
    }
    // Consume the session BEFORE asserting, so a failing assertion cannot fire
    // the Drop tripwire during unwind and abort the process.
    session.abandon("test complete").await;
    drop(store);

    assert!(
        replicated,
        "the periodic uploader must honour replicate = true"
    );
}
