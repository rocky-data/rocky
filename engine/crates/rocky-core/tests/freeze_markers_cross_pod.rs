//! Cross-pod freeze-marker integration tests (WP-01 rig).
//!
//! Drives the REAL marker write/read paths ([`rocky_core::freeze_marker`])
//! against the shared fault-injectable in-memory "S3" the PR-1 rig installs
//! via the process-global provider override, resolving the durable tier the
//! way production does ([`rocky_core::state_sync::durable_tier_provider`]).
//! Each test holds `remote_testing::serial_guard()` because the override is
//! process-global.
//!
//! What is pinned here (see docs/adr/ADR-CONCURRENCY.md, D3 + Validation
//! Rung 2):
//! - markers live OUTSIDE `state.redb`: a stale-base whole-blob overwrite
//!   (RD-002 last-writer-wins, still live until CAS lands) cannot erase a
//!   freeze marker;
//! - the deterministic half of the kill-switch drill: a freeze landing
//!   between a run's download and its upload survives the upload;
//! - a LIST/GET transport failure surfaces as `Err` — never an empty active
//!   set;
//! - the tiered backend's marker read is pinned to the durable S3 leg, with
//!   zero Valkey traffic;
//! - the create → LIST → project roundtrip through the real write path, on a
//!   schema-version-INDEPENDENT key layout beside the `v{N}/state.redb`
//!   object.
//!
//! Requires the `test-support` feature (on by default for test targets via
//! rocky-core's self-referential dev-dependency).

use chrono::Utc;
use rocky_core::config::{PolicyPrincipal, StateBackend, StateConfig};
use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::freeze_marker::{self, FreezeMarker, UnfreezeMarker};
use rocky_core::object_store::ObjectStoreProvider;
use rocky_core::redacted::RedactedString;
use rocky_core::state_sync::{durable_tier_provider, remote_testing};
use rocky_core::test_harness::CrossPodHarness;
use rocky_ir::WatermarkState;

fn wm_now() -> WatermarkState {
    let now = Utc::now();
    WatermarkState {
        last_value: now,
        updated_at: now,
    }
}

fn marker(id: &str, principal: PolicyPrincipal, scope: &str) -> FreezeMarker {
    FreezeMarker {
        freeze_id: id.to_string(),
        principal,
        scope: scope.to_string(),
        reason: format!("test freeze {id}"),
        created_at: Utc::now(),
    }
}

/// Resolve the durable-tier provider for `cfg` exactly the way every
/// production marker reader/writer does — through `durable_tier_provider`,
/// so the process-global override intercepts it.
fn provider_for(cfg: &StateConfig) -> ObjectStoreProvider {
    durable_tier_provider(cfg)
        .expect("durable tier resolves")
        .expect("an s3-like backend has a durable object tier")
}

/// Markers live OUTSIDE `state.redb`: a pod that full-uploads from a stale
/// base wholesale-replaces the shared blob (RD-002 last-writer-wins, still
/// live until CAS), but the freeze marker beside it is untouched and still
/// projects active.
#[tokio::test]
async fn stale_base_blob_overwrite_leaves_marker_intact() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // Pod A publishes a baseline; pod B downloads it — pod B's upload base
    // is stale the moment anything else lands on the remote.
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.from_a", &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_a).await.expect("pod A upload");
    assert_eq!(
        harness
            .download(&harness.pod_b)
            .await
            .expect("pod B download"),
        rocky_core::state_sync::StateAuthority::Authoritative,
        "pod B's base is the restored remote object"
    );

    // A freeze marker lands (another pod's `rocky policy freeze`), via the
    // REAL create-once write path on the durable-tier provider.
    let provider = provider_for(&harness.pod_b.cfg);
    freeze_marker::write_freeze_marker(
        &provider,
        &marker("f-stale", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("marker create");

    // Pod B mutates and full-uploads from its stale base — the blob is
    // wholesale-replaced.
    {
        let store = harness.open_store(&harness.pod_b);
        store.set_watermark("cat.sch.from_b", &wm_now()).unwrap();
    }
    harness
        .upload(&harness.pod_b)
        .await
        .expect("pod B stale-base upload");

    // The marker is separate from `state.redb`: still listed, still active.
    let keys = harness
        .provider
        .list("freeze")
        .await
        .expect("list marker prefix");
    assert_eq!(
        keys,
        vec!["freeze/f-stale.json".to_string()],
        "the stale-base blob overwrite must leave the marker object intact"
    );
    let active = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .expect("project active set");
    assert_eq!(active.len(), 1, "the freeze must still project active");
    assert_eq!(active[0].freeze_id, "f-stale");
}

/// The deterministic half of the kill-switch drill: a freeze marker created
/// BETWEEN a run's start-of-run download and its end-of-run upload survives
/// that upload and projects active — the marker write path
/// (`put_if_not_exists` on a unique key) never contends with the blob
/// upload. (The fence-denial half is driven at the CLI level in
/// rocky-cli/tests/freeze_marker_enforcement.rs; the live half is the
/// operational drill.)
#[tokio::test]
async fn freeze_between_download_and_upload_survives() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // The "run": start-of-run download (fresh start on an empty remote),
    // then local mutation while in flight.
    assert_eq!(
        harness
            .download(&harness.pod_a)
            .await
            .expect("start-of-run download"),
        rocky_core::state_sync::StateAuthority::FreshStart,
        "an empty remote is a genuine fresh start"
    );
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.mid_run", &wm_now()).unwrap();
    }

    // The kill switch engages between the run's download and its upload.
    let provider = provider_for(&harness.pod_a.cfg);
    freeze_marker::write_freeze_marker(
        &provider,
        &marker("f-drill", PolicyPrincipal::Agent, "any"),
    )
    .await
    .expect("freeze marker create");

    // The run's end-of-run upload fires anyway (this rig has no fence).
    harness
        .upload(&harness.pod_a)
        .await
        .expect("end-of-run upload");

    let active = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .expect("project active set");
    assert_eq!(
        active.len(),
        1,
        "the freeze must survive the racing run's upload"
    );
    assert_eq!(active[0].freeze_id, "f-drill");
    assert_eq!(active[0].principal, Some(PolicyPrincipal::Agent));
}

/// Fail-closed transport posture: a LIST failure — or a GET failure on a
/// listed key — propagates as `Err`. An error is never an empty active set:
/// the caller cannot distinguish a network blip from a suppressed marker.
#[tokio::test]
async fn list_failure_surfaces_transport_error() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let provider = provider_for(&harness.pod_a.cfg);

    // A marker exists, so a swallowed error WOULD look like an empty set.
    freeze_marker::write_freeze_marker(&provider, &marker("f-net", PolicyPrincipal::Agent, "any"))
        .await
        .expect("marker create");

    harness.faults.arm(FaultOp::List, FaultMode::FailAll);
    assert!(
        freeze_marker::load_active_marker_freezes(&provider)
            .await
            .is_err(),
        "a LIST transport failure must be an Err, never an empty active set"
    );
    harness.faults.clear();

    harness.faults.arm(FaultOp::Get, FaultMode::FailAll);
    assert!(
        freeze_marker::load_active_marker_freezes(&provider)
            .await
            .is_err(),
        "a GET transport failure on a listed key must be an Err, never a dropped marker"
    );
    harness.faults.clear();

    // Recovery: the same call sees the marker once the transport heals.
    let active = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .expect("recovered projection");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].freeze_id, "f-net");
}

/// Tiered backends resolve markers to the durable S3 leg ONLY — Valkey is
/// never read or written for markers, so a stale (or poisoned) cache can
/// never shadow a freeze. The Valkey URL here is unroutable (TEST-NET-1):
/// any attempted Valkey traffic would fail, so success + the durable store's
/// call counters prove the read went to the durable tier.
#[tokio::test]
async fn tiered_marker_read_bypasses_valkey() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();
    let tiered_cfg = StateConfig {
        backend: StateBackend::Tiered,
        valkey_url: Some(RedactedString::new("redis://192.0.2.1:1".into())),
        s3_bucket: Some("test".into()),
        ..StateConfig::default()
    };

    let provider = durable_tier_provider(&tiered_cfg)
        .expect("tiered durable tier resolves")
        .expect("tiered maps to its durable S3 leg");
    freeze_marker::write_freeze_marker(
        &provider,
        &marker("f-tiered", PolicyPrincipal::Human, "layer=gold"),
    )
    .await
    .expect("marker create on the durable leg");

    let lists_before = harness.faults.count(FaultOp::List);
    let active = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .expect("tiered marker read must succeed with zero Valkey traffic");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].freeze_id, "f-tiered");
    assert_eq!(active[0].scope, "layer=gold");
    assert!(
        harness.faults.count(FaultOp::List) > lists_before,
        "the marker LIST must land on the durable object store, not a cache"
    );
}

/// Create → LIST → project roundtrip via the real write path, beside a real
/// state upload: the state object is schema-versioned (`v{N}/state.redb`)
/// while every marker key is a version-INDEPENDENT sibling — a mixed-version
/// fleet reads one active set.
#[tokio::test]
async fn marker_create_roundtrip_lists_and_projects() {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

    // A real state upload, so the version-segmented state key exists beside
    // the markers.
    {
        let store = harness.open_store(&harness.pod_a);
        store.set_watermark("cat.sch.orders", &wm_now()).unwrap();
    }
    harness.upload(&harness.pod_a).await.expect("state upload");

    let provider = provider_for(&harness.pod_a.cfg);
    freeze_marker::write_freeze_marker(&provider, &marker("f-a", PolicyPrincipal::Agent, "any"))
        .await
        .expect("create f-a");
    freeze_marker::write_freeze_marker(
        &provider,
        &marker("f-b", PolicyPrincipal::Human, "layer=gold"),
    )
    .await
    .expect("create f-b");
    freeze_marker::write_unfreeze_marker(
        &provider,
        &UnfreezeMarker {
            unfreeze_id: "u-1".to_string(),
            lifts: vec!["f-a".to_string()],
            principal: Some(PolicyPrincipal::Agent),
            reason: Some("lift f-a".to_string()),
            created_at: Utc::now(),
        },
    )
    .await
    .expect("create u-1");

    // OR-set projection against the real listing: f-a is lifted, f-b stays.
    let active = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .expect("project active set");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].freeze_id, "f-b");
    assert_eq!(active[0].principal, Some(PolicyPrincipal::Human));
    assert_eq!(active[0].scope, "layer=gold");

    // Key layout: the state object carries the schema-version segment; the
    // marker keys are flat `freeze/<id>.json` / `unfreeze/<id>.json` with NO
    // `v{N}/` segment.
    let keys = harness.provider.list("").await.expect("list root");
    assert!(
        keys.iter()
            .any(|k| k.starts_with('v') && k.contains("state.redb")),
        "the state object must live under its schema-version segment, got {keys:?}"
    );
    assert!(
        keys.contains(&"freeze/f-a.json".to_string())
            && keys.contains(&"freeze/f-b.json".to_string())
            && keys.contains(&"unfreeze/u-1.json".to_string()),
        "marker keys must be flat version-independent siblings, got {keys:?}"
    );
    assert!(
        keys.iter()
            .filter(|k| k.contains("freeze"))
            .all(|k| k.starts_with("freeze/") || k.starts_with("unfreeze/")),
        "no marker key may carry a schema-version segment, got {keys:?}"
    );
}
