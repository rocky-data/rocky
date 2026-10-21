//! Gate tests for the run-level idempotency claim on the LOCAL state backend.
//!
//! A child connector run takes the same claim as `rocky run --idempotency-key`.
//! These tests drive the real [`IdempotencyBackend`] with an injected clock.

use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use rocky_core::config::IdempotencyConfig;
use rocky_core::idempotency::{IdempotencyBackend, IdempotencyCheck};
use rocky_core::state::StateStore;

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 21, 8, 0, 0).unwrap()
}

fn cfg(in_flight_ttl_hours: u32) -> IdempotencyConfig {
    IdempotencyConfig {
        in_flight_ttl_hours,
        ..IdempotencyConfig::default()
    }
}

fn open() -> (Arc<StateStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
    (Arc::new(store), dir)
}

/// Eight runs claim one key at once: exactly one proceeds, the rest skip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_have_exactly_one_winner() {
    let (store, _dir) = open();
    let mut tasks = Vec::new();
    for i in 0..8 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            IdempotencyBackend::Local
                .check_and_claim_at(Some(&store), "k", &format!("run-{i}"), &cfg(24), t0())
                .await
                .unwrap()
        }));
    }
    let mut proceeded = 0;
    for task in tasks {
        match task.await.unwrap() {
            IdempotencyCheck::Proceed => proceeded += 1,
            IdempotencyCheck::SkipInFlight { .. } => {}
            other => panic!("unexpected verdict {other:?}"),
        }
    }
    assert_eq!(proceeded, 1);
}

/// A run that crashes leaves its claim. Before the claim's expiry a new run
/// skips; after it, the new run adopts and names the crashed run.
#[tokio::test]
async fn a_crashed_claim_is_adopted_only_after_it_expires() {
    let (store, _dir) = open();
    let backend = IdempotencyBackend::Local;
    let first = backend
        .check_and_claim_at(Some(&store), "k", "run-a", &cfg(24), t0())
        .await
        .unwrap();
    assert!(matches!(first, IdempotencyCheck::Proceed));

    let early = backend
        .check_and_claim_at(
            Some(&store),
            "k",
            "run-b",
            &cfg(24),
            t0() + Duration::hours(23),
        )
        .await
        .unwrap();
    assert!(
        matches!(early, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-a"),
        "{early:?}"
    );

    let late = backend
        .check_and_claim_at(
            Some(&store),
            "k",
            "run-b",
            &cfg(24),
            t0() + Duration::hours(25),
        )
        .await
        .unwrap();
    assert!(
        matches!(late, IdempotencyCheck::AdoptStale { ref prior_run_id } if prior_run_id == "run-a"),
        "{late:?}"
    );
}

/// The claim is judged by the expiry it was stamped with. A run claims for
/// 30 h; the operator then lowers `in_flight_ttl_hours` to 6. Ten hours in,
/// the sweep keeps the live claim and a second run still skips.
#[tokio::test]
async fn the_sweep_keeps_a_claim_whose_own_expiry_is_ahead_after_the_config_ttl_drops() {
    let (store, _dir) = open();
    let backend = IdempotencyBackend::Local;
    backend
        .check_and_claim_at(Some(&store), "k", "run-a", &cfg(30), t0())
        .await
        .unwrap();

    let ten_hours = t0() + Duration::hours(10);
    let swept = backend
        .sweep_expired_at(Some(&store), ten_hours)
        .await
        .unwrap();
    assert_eq!(swept, 0, "a live 30 h claim must survive the sweep");

    let second = backend
        .check_and_claim_at(Some(&store), "k", "run-b", &cfg(6), ten_hours)
        .await
        .unwrap();
    assert!(
        matches!(second, IdempotencyCheck::SkipInFlight { .. }),
        "{second:?}"
    );

    let swept = backend
        .sweep_expired_at(Some(&store), t0() + Duration::hours(31))
        .await
        .unwrap();
    assert_eq!(swept, 1, "past its own expiry the claim goes");
}

/// A `Succeeded` entry past its own `expires_at` is dead: a later run adopts
/// it, and one inside retention still skips.
#[tokio::test]
async fn a_succeeded_entry_past_its_expiry_is_adopted() {
    let (store, _dir) = open();
    let b = IdempotencyBackend::Local;
    let config = cfg(24);
    b.check_and_claim_at(Some(&store), "k", "run-a", &config, t0())
        .await
        .unwrap();
    b.finalize_at(
        Some(&store),
        "k",
        "run-a",
        rocky_core::idempotency::FinalOutcome::Succeeded,
        &config,
        t0(),
    )
    .await
    .unwrap();
    let within = b
        .check_and_claim_at(
            Some(&store),
            "k",
            "run-b",
            &config,
            t0() + Duration::days(29),
        )
        .await
        .unwrap();
    assert!(
        matches!(within, IdempotencyCheck::SkipCompleted { .. }),
        "{within:?}"
    );
    let past = b
        .check_and_claim_at(
            Some(&store),
            "k",
            "run-b",
            &config,
            t0() + Duration::days(31),
        )
        .await
        .unwrap();
    assert!(
        matches!(past, IdempotencyCheck::AdoptStale { ref prior_run_id } if prior_run_id == "run-a"),
        "{past:?}"
    );
}
