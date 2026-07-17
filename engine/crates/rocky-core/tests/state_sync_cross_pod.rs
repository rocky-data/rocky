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
use rocky_core::state_sync::{self, remote_testing};
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
    harness
        .download(&harness.pod_b)
        .await
        .expect("pod B download");
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
    harness.download(&harness.pod_a).await.unwrap();
    harness.download(&harness.pod_b).await.unwrap();

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
    harness.download(&harness.pod_a).await.unwrap();
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
    harness
        .download(&harness.pod_b)
        .await
        .expect("pod B download");
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store
            .get_watermark("cat.sch.cross_thread")
            .unwrap()
            .is_some(),
        "state uploaded from a foreign thread round-trips to pod B"
    );
}
