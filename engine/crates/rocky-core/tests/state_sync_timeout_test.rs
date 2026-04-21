//! Integration tests for [`rocky_core::state_sync::upload_state`] timeout +
//! retry + `on_upload_failure` behaviour.
//!
//! Front-ends a fake S3 endpoint with `wiremock` and routes the
//! `AmazonS3Builder::from_env()` code path through it via `AWS_ENDPOINT` +
//! `AWS_ALLOW_HTTP`. Exercises four shapes:
//!
//! * **Hung endpoint, fail mode** — mock holds PUT for 1h;
//!   `on_upload_failure = Fail`. `StateSyncError::Timeout` must return
//!   within `transfer_timeout_seconds` + grace.
//! * **Hung endpoint, skip mode** — same hang, default `on_upload_failure = Skip`.
//!   The timeout still fires within budget, but the policy converts the
//!   error to `Ok(())` — this matches the de-facto behaviour of existing
//!   `rocky run` callers that `warn + continue` on upload failure.
//! * **Prompt endpoint** — mock replies `200 OK` immediately; upload
//!   completes well under 1 s (no retries, no timeout).
//! * **Retry succeeds after transient failure** — mock returns one 500
//!   then 200. The retry loop must observe the 500, back off, and succeed
//!   on the second attempt without tripping the outer timeout.
//!
//! Because `AmazonS3Builder::from_env()` reads process-wide env vars, the
//! tests share a `tokio::sync::Mutex` to serialize env mutation —
//! cargo otherwise runs `#[tokio::test]`s on separate threads.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rocky_core::config::{RetryConfig, StateBackend, StateConfig, StateUploadFailureMode};
use rocky_core::state_sync::{StateSyncError, upload_state};
use tempfile::TempDir;
use tokio::sync::{Mutex, MutexGuard};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serializes tests that mutate `AWS_*` env vars. `AmazonS3Builder::from_env()`
/// reads the live process-wide value at build time; two tests running in
/// parallel would race on `AWS_ENDPOINT`. Using `tokio::sync::Mutex` (not
/// `std::sync::Mutex`) so the guard can safely be held across `.await` points.
async fn env_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

/// Installs AWS env vars pointing the S3 SDK at `endpoint`. Safe to call from
/// any number of tests — subsequent calls update `AWS_ENDPOINT` in place,
/// which is fine because `AmazonS3Builder::from_env()` reads the live value
/// on every build.
///
/// `AWS_ENDPOINT` is the env var `object_store::aws::AmazonS3Builder::from_env`
/// actually reads (not the more widely-known `AWS_ENDPOINT_URL`, which is an
/// AWS-SDK-native variable). `AWS_ALLOW_HTTP=true` is the same crate's escape
/// hatch for talking to a plain-HTTP mock; `rocky_core::object_store` honours
/// it in `default_client_options()`.
fn install_aws_env(endpoint: &str) {
    static GUARD: OnceLock<()> = OnceLock::new();
    GUARD.get_or_init(|| {
        // SAFETY: these env vars are set exactly once before any builder
        // reads them; subsequent `std::env::set_var` calls only refresh
        // `AWS_ENDPOINT`, which is always done on the same single test
        // thread before the builder runs.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
            std::env::set_var("AWS_ALLOW_HTTP", "true");
        }
    });
    // SAFETY: single-threaded within each test; the endpoint URL differs per
    // mock server so the latest write wins before the S3 builder reads it.
    unsafe {
        std::env::set_var("AWS_ENDPOINT", endpoint);
    }
}

fn write_stub_state(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("state.redb");
    std::fs::write(&path, b"stub state bytes for upload test").unwrap();
    path
}

/// Retry policy with zero retries — use for timeout tests so a single
/// attempt fills the transfer budget without the retry loop consuming it
/// in backoff sleeps.
fn retry_disabled() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        ..RetryConfig::default()
    }
}

fn s3_config(
    bucket: &str,
    timeout_seconds: u64,
    on_upload_failure: StateUploadFailureMode,
    retry: RetryConfig,
) -> StateConfig {
    StateConfig {
        backend: StateBackend::S3,
        s3_bucket: Some(bucket.into()),
        s3_prefix: Some("rocky/state/".into()),
        transfer_timeout_seconds: timeout_seconds,
        on_upload_failure,
        retry,
        ..Default::default()
    }
}

/// Hung S3 endpoint with `on_upload_failure = Fail` — `upload_state` must
/// return `StateSyncError::Timeout` within budget + grace.
#[tokio::test]
async fn upload_state_times_out_on_hung_endpoint_fail_mode() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    // Any PUT (S3 `PutObject`) against the bucket hangs for 1h.
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3600)))
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    let state_path = write_stub_state(tmp.path());
    let config = s3_config(
        "rocky-state-test-bucket",
        2,
        StateUploadFailureMode::Fail,
        retry_disabled(),
    );

    let start = Instant::now();
    let result = upload_state(&config, &state_path).await;
    let elapsed = start.elapsed();

    match result {
        Err(StateSyncError::Timeout(d)) => {
            assert_eq!(
                d,
                Duration::from_secs(2),
                "timeout error should carry the configured budget"
            );
        }
        other => panic!("expected StateSyncError::Timeout, got {other:?}"),
    }

    assert!(
        elapsed < Duration::from_secs(4),
        "upload_state did not honour the 2 s timeout budget (elapsed {elapsed:?})"
    );
}

/// Hung S3 endpoint with default `on_upload_failure = Skip` — the timeout
/// still honours the configured budget, but the skip policy converts the
/// terminal error to `Ok(())` so the caller's run continues in degraded
/// mode. Matches the pre-R1 behaviour of `rocky run` callers that already
/// `warn + continue`.
#[tokio::test]
async fn upload_state_hung_endpoint_skip_mode_converts_to_ok() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3600)))
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    let state_path = write_stub_state(tmp.path());
    let config = s3_config(
        "rocky-state-skip-mode-bucket",
        2,
        StateUploadFailureMode::Skip,
        retry_disabled(),
    );

    let start = Instant::now();
    let result = upload_state(&config, &state_path).await;
    let elapsed = start.elapsed();

    result.expect("skip-mode should swallow terminal upload error");
    assert!(
        elapsed < Duration::from_secs(4),
        "upload_state did not honour the 2 s timeout budget (elapsed {elapsed:?})"
    );
}

/// Prompt S3 endpoint — the same config must succeed well inside the budget.
/// Guards against a regression where the timeout or retry change accidentally
/// starts to fire on healthy requests.
#[tokio::test]
async fn upload_state_succeeds_when_endpoint_is_prompt() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    // Any PUT replies 200 immediately. S3's `object_store` backend parses the
    // response `ETag` header out of the PutObject reply; without it the SDK
    // returns `MissingEtag`. A synthetic quoted hex string is sufficient.
    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\""),
        )
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    let state_path = write_stub_state(tmp.path());
    let config = s3_config(
        "rocky-state-happy-bucket",
        2,
        StateUploadFailureMode::Fail,
        retry_disabled(),
    );

    let start = Instant::now();
    let result = upload_state(&config, &state_path).await;
    let elapsed = start.elapsed();

    result.expect("prompt endpoint upload should succeed");
    assert!(
        elapsed < Duration::from_secs(1),
        "prompt upload took {elapsed:?} (expected <1s)"
    );
}

/// First PUT returns 500, second returns 200 — the retry loop must observe
/// the transient error, apply backoff, and succeed on the second attempt.
/// `max_retries = 1` is enough; `initial_backoff_ms = 50` keeps the test
/// snappy. The outer 3 s transfer budget comfortably covers one retry
/// with 50 ms backoff plus both HTTP round-trips.
#[tokio::test]
async fn upload_state_retries_transient_then_succeeds() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    // First PUT: 500 (transient). Second PUT: 200. Wiremock matches in
    // reverse registration order, so register the 200 first with
    // `up_to_n_times(1)` won't help here — use explicit priority via a
    // mount order + expect. Simpler: single mock that responds 500 once
    // then 200 forever isn't built-in, so register two mocks with
    // `respond_with` priorities via `mount_as_scoped` on up_to_n_times.

    // Register the 500 response first with `up_to_n_times(1)`. Wiremock
    // matches mounted mocks in registration order, so the first PUT
    // matches the 500 and consumes its single-use budget; the second PUT
    // falls through to the 200 default.
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\""),
        )
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    let state_path = write_stub_state(tmp.path());
    let config = s3_config(
        "rocky-state-retry-bucket",
        3,
        StateUploadFailureMode::Fail,
        RetryConfig {
            max_retries: 1,
            initial_backoff_ms: 50,
            max_backoff_ms: 100,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        },
    );

    let start = Instant::now();
    let result = upload_state(&config, &state_path).await;
    let elapsed = start.elapsed();

    result.expect("retry after transient 500 should succeed");
    assert!(
        elapsed < Duration::from_secs(3),
        "retry+upload took {elapsed:?} (expected <3s)"
    );
}
