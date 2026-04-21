//! Integration tests for [`rocky_core::state_sync::upload_state`] timeout
//! behaviour.
//!
//! Front-ends a fake S3 endpoint with `wiremock` and routes the
//! `AmazonS3Builder::from_env()` code path through it via `AWS_ENDPOINT` +
//! `AWS_ALLOW_HTTP`. Exercises two shapes:
//!
//! * **Hung endpoint** — the mock holds PUT requests for 1h. We configure
//!   `transfer_timeout_seconds = 2` and assert `StateSyncError::Timeout`
//!   returns within a 4 s wall-clock budget (2 s timeout + 2 s grace).
//! * **Prompt endpoint** — the mock replies `200 OK` immediately. Same
//!   2 s budget; upload must complete well under 1 s.
//!
//! Because `AmazonS3Builder::from_env()` reads process-wide env vars, the
//! two tests share a `tokio::sync::Mutex` to serialize env mutation —
//! cargo otherwise runs `#[tokio::test]`s on separate threads.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rocky_core::config::{StateBackend, StateConfig};
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

fn s3_config(bucket: &str, timeout_seconds: u64) -> StateConfig {
    StateConfig {
        backend: StateBackend::S3,
        s3_bucket: Some(bucket.into()),
        s3_prefix: Some("rocky/state/".into()),
        transfer_timeout_seconds: timeout_seconds,
        ..Default::default()
    }
}

/// Hung S3 endpoint — `upload_state` must return `StateSyncError::Timeout`
/// within `transfer_timeout_seconds` + a small grace window, not hang forever.
#[tokio::test]
async fn upload_state_times_out_on_hung_endpoint() {
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
    let config = s3_config("rocky-state-test-bucket", 2);

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

/// Prompt S3 endpoint — the same config must succeed well inside the budget.
/// Guards against a regression where the timeout change accidentally starts
/// to fire on healthy requests.
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
    let config = s3_config("rocky-state-happy-bucket", 2);

    let start = Instant::now();
    let result = upload_state(&config, &state_path).await;
    let elapsed = start.elapsed();

    result.expect("prompt endpoint upload should succeed");
    assert!(
        elapsed < Duration::from_secs(1),
        "prompt upload took {elapsed:?} (expected <1s)"
    );
}
