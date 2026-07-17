//! Live-S3 rung for the remote-state rig (WP-01).
//!
//! `#[ignore]`-gated probes against a REAL S3 bucket — the honest top rung
//! above the in-memory harness. They validate the two assumptions the
//! deterministic rig takes on faith: that `upload_state`/`download_state`
//! round-trip through a real AWS endpoint, and that the bucket honors the
//! atomic create-if-absent precondition (`If-None-Match: *`) that
//! conditional-put publication will build on.
//!
//! Reads connection params from env so no bucket name or credential is ever
//! committed:
//!   ROCKY_TEST_S3_BUCKET — bucket name (no scheme)
//!   ROCKY_TEST_S3_PREFIX — key prefix to sandbox test objects under
//!   ROCKY_TEST_S3_REGION — bucket region (exported to AWS_REGION if unset)
//! plus the standard AWS credential chain (env vars / profile / SSO / IMDS).
//!
//! Run with:
//!   ROCKY_TEST_S3_BUCKET=<bucket> ROCKY_TEST_S3_PREFIX=<prefix> \
//!   ROCKY_TEST_S3_REGION=<region> \
//!     cargo test -p rocky-core --features test-support \
//!     --test state_sync_s3_live -- --ignored --nocapture

use std::sync::{Mutex, MutexGuard, Once, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use chrono::Utc;
use rocky_core::config::{PolicyPrincipal, StateBackend, StateConfig, StateUploadFailureMode};
use rocky_core::freeze_marker::{
    FreezeMarker, FreezeMarkerError, UnfreezeMarker, load_active_marker_freezes,
    write_freeze_marker, write_unfreeze_marker,
};
use rocky_core::object_store::{ObjectStoreProvider, PutIfNotExistsOutcome};
use rocky_core::state::StateStore;
use rocky_core::state_sync::{download_state, upload_state};
use rocky_ir::WatermarkState;

/// Serializes the live tests within this binary: they mutate the process
/// environment (AWS_REGION) once and share one bucket prefix.
static LIVE_SERIAL: Mutex<()> = Mutex::new(());

/// Newtype so the guard can be held across the tests' `.await` points (the
/// whole point of the serialization) without tripping
/// `clippy::await_holding_lock` — contenders are other tests' OS threads
/// blocking in `lock()`, never tasks on this test's runtime.
struct LiveSerial(#[allow(dead_code)] MutexGuard<'static, ()>);

fn live_serial() -> LiveSerial {
    LiveSerial(LIVE_SERIAL.lock().unwrap_or_else(PoisonError::into_inner))
}

struct LiveS3 {
    bucket: String,
    prefix: String,
}

/// Read the live-S3 params, exporting `ROCKY_TEST_S3_REGION` to `AWS_REGION`
/// (once, only if no region is already set) so the standard builder env chain
/// resolves. Returns `None` when the env gate is not configured.
fn live_s3_from_env() -> Option<LiveS3> {
    let bucket = std::env::var("ROCKY_TEST_S3_BUCKET").ok()?;
    let prefix = std::env::var("ROCKY_TEST_S3_PREFIX").ok()?;
    let region = std::env::var("ROCKY_TEST_S3_REGION").ok()?;

    static REGION_INIT: Once = Once::new();
    REGION_INIT.call_once(|| {
        if std::env::var("AWS_REGION").is_err() && std::env::var("AWS_DEFAULT_REGION").is_err() {
            // SAFETY: test-only env mutation, serialized by `Once` and by the
            // callers all holding `live_serial()` for their whole duration —
            // no other thread reads the environment while this write runs.
            unsafe { std::env::set_var("AWS_REGION", &region) };
        }
    });

    Some(LiveS3 {
        bucket,
        prefix: prefix.trim_matches('/').to_string(),
    })
}

/// Process-unique suffix so parallel/aborted runs never collide on a key.
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{}", std::process::id(), nanos)
}

fn wm_now() -> WatermarkState {
    let now = Utc::now();
    WatermarkState {
        last_value: now,
        updated_at: now,
    }
}

/// `upload_state` → real bucket → `download_state` round-trip: a watermark
/// written on one "pod" is readable after a download into a fresh dir.
#[tokio::test]
#[ignore]
async fn live_round_trip() {
    let _serial = live_serial();
    let Some(live) = live_s3_from_env() else {
        eprintln!("SKIP live_round_trip: ROCKY_TEST_S3_* env not set");
        return;
    };
    // A per-run prefix sandbox so cleanup can sweep everything it wrote.
    let run_prefix = format!("{}/round-trip-{}", live.prefix, unique_suffix());
    let cfg = StateConfig {
        backend: StateBackend::S3,
        s3_bucket: Some(live.bucket.clone()),
        s3_prefix: Some(run_prefix.clone()),
        on_upload_failure: StateUploadFailureMode::Fail,
        ..StateConfig::default()
    };

    // "Pod A": write a watermark and upload.
    let up_dir = tempfile::tempdir().unwrap();
    let up_path = up_dir.path().join(".rocky-state.redb");
    {
        let store = StateStore::open(&up_path).unwrap();
        store.set_watermark("cat.sch.live", &wm_now()).unwrap();
    }
    upload_state(&cfg, &up_path).await.expect("live upload");

    // "Pod B": download into a fresh dir and read it back.
    let down_dir = tempfile::tempdir().unwrap();
    let down_path = down_dir.path().join(".rocky-state.redb");
    assert_eq!(
        download_state(&cfg, &down_path)
            .await
            .expect("live download"),
        rocky_core::state_sync::StateAuthority::Authoritative,
        "the just-uploaded object must restore as Authoritative"
    );
    {
        let store = StateStore::open(&down_path).unwrap();
        assert!(
            store.get_watermark("cat.sch.live").unwrap().is_some(),
            "the watermark must round-trip through the real bucket"
        );
    }

    // Best-effort cleanup: sweep the per-run prefix.
    let provider = ObjectStoreProvider::from_uri(&format!("s3://{}/{run_prefix}", live.bucket))
        .expect("cleanup provider");
    match provider.list("").await {
        Ok(keys) => {
            for key in keys {
                if let Err(e) = provider.delete(&key).await {
                    eprintln!("cleanup: failed to delete {key}: {e}");
                }
            }
        }
        Err(e) => eprintln!("cleanup: failed to list {run_prefix}: {e}"),
    }
}

/// The atomic create-if-absent primitive against a real bucket: the first
/// `put_if_not_exists` claims the key, the second reports the conflict, and
/// the loser's bytes never land.
#[tokio::test]
#[ignore]
async fn live_put_if_not_exists_conflict() {
    let _serial = live_serial();
    let Some(live) = live_s3_from_env() else {
        eprintln!("SKIP live_put_if_not_exists_conflict: ROCKY_TEST_S3_* env not set");
        return;
    };
    let provider = ObjectStoreProvider::from_uri(&format!("s3://{}/{}", live.bucket, live.prefix))
        .expect("live provider");
    let key = format!("conflict-probe-{}", unique_suffix());

    let first = provider
        .put_if_not_exists(&key, Bytes::from_static(b"first"))
        .await
        .expect("first conditional put");
    assert_eq!(first, PutIfNotExistsOutcome::Created, "first writer claims");

    let second = provider
        .put_if_not_exists(&key, Bytes::from_static(b"second"))
        .await
        .expect("second conditional put must resolve to an outcome, not an error");
    assert_eq!(
        second,
        PutIfNotExistsOutcome::AlreadyExists,
        "second writer must see the conflict"
    );

    let stored = provider.get(&key).await.expect("read back");
    assert_eq!(
        stored.as_ref(),
        b"first",
        "the losing writer's bytes must never land"
    );

    provider.delete(&key).await.expect("cleanup delete");
}

/// The freeze-marker LIST + Create roundtrip against a REAL bucket — the
/// OR-set projection over a real (unordered, paginated) object listing, not
/// InMemory-only: freeze A and B, unfreeze lifting A, and the active set
/// projects exactly {B}. A duplicate Create on an existing marker key must
/// report the collision rather than overwrite — the create-once primitive
/// the un-erasable kill switch stands on.
#[tokio::test]
#[ignore]
async fn live_freeze_marker_list_create_roundtrip() {
    let _serial = live_serial();
    let Some(live) = live_s3_from_env() else {
        eprintln!("SKIP live_freeze_marker_list_create_roundtrip: ROCKY_TEST_S3_* env not set");
        return;
    };
    // A per-run prefix sandbox so cleanup can sweep everything it wrote.
    let run_prefix = format!("{}/freeze-markers-{}", live.prefix, unique_suffix());
    let provider = ObjectStoreProvider::from_uri(&format!("s3://{}/{run_prefix}", live.bucket))
        .expect("live provider");

    let marker = |id: &str| FreezeMarker {
        freeze_id: id.to_string(),
        principal: PolicyPrincipal::Agent,
        scope: "any".to_string(),
        reason: format!("live marker probe {id}"),
        created_at: Utc::now(),
    };
    write_freeze_marker(&provider, &marker("live-a"))
        .await
        .expect("create live-a");
    write_freeze_marker(&provider, &marker("live-b"))
        .await
        .expect("create live-b");
    write_unfreeze_marker(
        &provider,
        &UnfreezeMarker {
            unfreeze_id: "live-u".to_string(),
            lifts: vec!["live-a".to_string()],
            principal: Some(PolicyPrincipal::Agent),
            reason: Some("lift live-a".to_string()),
            created_at: Utc::now(),
        },
    )
    .await
    .expect("create live-u");

    let active = load_active_marker_freezes(&provider)
        .await
        .expect("project the active set against the real bucket listing");
    let ids: Vec<&str> = active.iter().map(|m| m.freeze_id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["live-b"],
        "the OR-set projection must lift live-a and keep live-b"
    );

    // Duplicate Create on live-a's still-present key: refused, never an
    // overwrite.
    let err = write_freeze_marker(&provider, &marker("live-a"))
        .await
        .expect_err("a duplicate Create on an existing marker key must be refused");
    assert!(
        matches!(err, FreezeMarkerError::IdCollision { .. }),
        "the collision must surface as the hard id-collision error, got: {err}"
    );

    // Best-effort cleanup: sweep the per-run prefix (all three markers).
    match provider.list("").await {
        Ok(keys) => {
            for key in keys {
                if let Err(e) = provider.delete(&key).await {
                    eprintln!("cleanup: failed to delete {key}: {e}");
                }
            }
        }
        Err(e) => eprintln!("cleanup: failed to list {run_prefix}: {e}"),
    }
}
