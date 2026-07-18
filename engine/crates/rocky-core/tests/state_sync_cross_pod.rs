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
            .block_on(state_sync::upload_state(&cfg, &path))
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
