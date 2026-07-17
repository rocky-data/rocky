//! Writer-lock contention integration tests for remote-state downloads
//! (WP-01 rig).
//!
//! Out-of-crate mirror of the in-crate finding-B unit coverage
//! (`state_sync.rs::download_publish_fails_closed_when_writer_lock_held`),
//! driven through the cross-pod harness: a download must serialize its atomic
//! publish behind the SAME advisory writer lock a live `rocky run` holds —
//! waiting (bounded retries) for a short-lived writer, failing closed on one
//! that never releases, and never leaking its staging file.
//!
//! Requires the `test-support` feature (on by default for test targets via
//! rocky-core's self-referential dev-dependency).

use std::time::Duration;

use chrono::Utc;
use rocky_core::fault_store::FaultOp;
use rocky_core::state_sync::{self, remote_testing};
use rocky_core::test_harness::{CrossPodHarness, with_held_writer};
use rocky_ir::WatermarkState;

fn wm_now() -> WatermarkState {
    let now = Utc::now();
    WatermarkState {
        last_value: now,
        updated_at: now,
    }
}

/// Seed the shared remote with a pod-A watermark so pod-B downloads have
/// something to publish.
async fn seed_remote(harness: &CrossPodHarness, key: &str) {
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark(key, &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_a).await.expect("seed upload");
}

/// Any leftover download artifacts (staging redb scratch or atomic-download
/// temp files) in `dir`, beyond the state file itself.
fn leaked_download_artifacts(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .expect("read pod dir")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".rocky-state-download-") || name.ends_with(".tmp"))
        .collect()
}

/// A download racing a SHORT-LIVED writer must not fail: it retries the
/// publish lock and completes once the writer releases within the retry
/// budget.
#[tokio::test]
async fn download_publish_waits_for_writer_release() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    seed_remote(&harness, "cat.sch.seeded").await;

    // Hold pod B's writer lock (as a live run would), start a concurrent
    // download, and only release once the download has provably fetched the
    // remote object — so its publish-lock attempts genuinely contend.
    let cfg = harness.pod_b.cfg.clone();
    let path = harness.pod_b.state_path.clone();
    let faults = harness.faults.clone();
    let gets_before = faults.count(FaultOp::Get);
    // Deliberately yields the still-running download's `JoinHandle` out of the
    // held-writer scope: the download must OUTLIVE the lock to prove it waits.
    #[allow(clippy::async_yields_async)]
    let download = with_held_writer(&harness.pod_b.state_path, async move {
        let download = tokio::spawn(async move { state_sync::download_state(&cfg, &path).await });
        // Handshake: wait for the download's remote GET to land on the shared
        // store, then keep the lock a beat longer so at least one publish-lock
        // attempt fails while we hold it.
        while faults.count(FaultOp::Get) == gets_before {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        download
    })
    .await;

    // Writer released — the download's remaining lock retries can publish.
    assert_eq!(
        download
            .await
            .expect("download task join")
            .expect("download must succeed once the writer releases within the retry budget"),
        rocky_core::state_sync::StateAuthority::Authoritative,
        "the seeded remote object restores as Authoritative"
    );

    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_watermark("cat.sch.seeded").unwrap().is_some(),
        "the published download must carry the seeded remote watermark"
    );
    assert!(
        leaked_download_artifacts(harness.pod_b.dir.path()).is_empty(),
        "a successful download must clean up its staging artifacts"
    );
}

/// A writer that NEVER releases must fail the download closed (finding B):
/// the publish-lock retries exhaust, the download returns `Err`, the live
/// writer's file is untouched, and no staging file is left behind.
#[tokio::test]
async fn download_fails_closed_when_writer_never_releases() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    seed_remote(&harness, "cat.sch.seeded").await;

    // Give pod B live local state so we can prove the held writer's file
    // survives the failed publish.
    {
        let store = harness.open_store(&harness.pod_b);
        store.set_watermark("cat.sch.local_b", &wm_now()).unwrap();
    }

    // Run the WHOLE download while the writer lock is held: every publish
    // attempt inside the retry budget fails, so the download fails closed.
    let cfg = harness.pod_b.cfg.clone();
    let path = harness.pod_b.state_path.clone();
    let result = with_held_writer(&harness.pod_b.state_path, async move {
        state_sync::download_state(&cfg, &path).await
    })
    .await;
    assert!(
        result.is_err(),
        "the download must fail closed while the writer lock never releases"
    );

    // Fail-closed means fail-clean: no staging redb / temp file leaked...
    assert!(
        leaked_download_artifacts(harness.pod_b.dir.path()).is_empty(),
        "a failed download must remove its staging file, found: {:?}",
        leaked_download_artifacts(harness.pod_b.dir.path())
    );

    // ...and the live writer's state was never clobbered by the remote.
    let store = harness.open_store(&harness.pod_b);
    assert!(
        store.get_watermark("cat.sch.local_b").unwrap().is_some(),
        "the held writer's local state must survive the failed publish"
    );
    assert!(
        store.get_watermark("cat.sch.seeded").unwrap().is_none(),
        "the remote content must NOT have been published over the live writer"
    );
}
