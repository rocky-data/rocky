//! Integration tests for [`rocky_core::state_sync::probe_state_backend`].
//!
//! Reuses the same `AWS_ENDPOINT` + `AWS_ALLOW_HTTP` wiremock pattern as
//! `state_sync_timeout_test.rs` so the S3 SDK talks to a mock. Exercises
//! two shapes:
//!
//! * **Happy path** — mock answers PUT / GET / DELETE successfully; the
//!   probe completes well under 1 s.
//! * **Failing probe** — mock returns 500 on PUT; probe propagates the
//!   error instead of pretending the backend is healthy.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rocky_core::config::{StateBackend, StateConfig};
use rocky_core::state_sync::probe_state_backend;
use tempfile::TempDir;
use tokio::sync::{Mutex, MutexGuard};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn env_guard() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

fn install_aws_env(endpoint: &str) {
    static GUARD: OnceLock<()> = OnceLock::new();
    GUARD.get_or_init(|| {
        // SAFETY: set exactly once before any object_store builder reads them.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
            std::env::set_var("AWS_ALLOW_HTTP", "true");
        }
    });
    // SAFETY: serialized via `env_guard()`; latest write wins before the
    // next S3 builder reads the endpoint.
    unsafe {
        std::env::set_var("AWS_ENDPOINT", endpoint);
    }
}

fn s3_config(bucket: &str) -> StateConfig {
    StateConfig {
        backend: StateBackend::S3,
        s3_bucket: Some(bucket.into()),
        s3_prefix: Some("rocky/state/".into()),
        transfer_timeout_seconds: 5,
        ..Default::default()
    }
}

/// Workaround: `AmazonS3Builder::from_env()` reads endpoint at build time
/// but doesn't gate on `tempfile`; we still allocate a scratch dir for
/// parity with the other integration test file.
fn _scratch(_dir: &Path) {}

/// Happy-path probe: PUT→GET→DELETE all succeed. Probe returns Ok quickly.
#[tokio::test]
async fn probe_state_backend_s3_roundtrip_succeeds() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    Mock::given(method("PUT"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\""),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(&b"rocky doctor probe"[..])
                .insert_header("ETag", "\"d41d8cd98f00b204e9800998ecf8427e\"")
                .insert_header("Content-Length", "18"),
        )
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    _scratch(tmp.path());
    let config = s3_config("rocky-state-probe-bucket");

    let start = Instant::now();
    let result = probe_state_backend(&config).await;
    let elapsed = start.elapsed();

    result.expect("probe should succeed with a healthy mock backend");
    assert!(
        elapsed < Duration::from_secs(2),
        "probe took {elapsed:?} (expected <2s)"
    );
}

/// Failing probe: PUT returns 500. Probe propagates the error so `rocky
/// doctor` can surface a Critical status instead of pretending the backend
/// is healthy.
#[tokio::test]
async fn probe_state_backend_s3_propagates_upload_error() {
    let _env = env_guard().await;
    let server = MockServer::start().await;
    install_aws_env(&server.uri());

    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let tmp = TempDir::new().unwrap();
    _scratch(tmp.path());
    let config = s3_config("rocky-state-probe-fail-bucket");

    let err = probe_state_backend(&config)
        .await
        .expect_err("500 on PUT should propagate as probe error");

    // Error shape comes through the ObjectStore backend variant since the
    // put is from aws-smithy; we only need to assert it's *not* silently
    // Ok. (`assert!(matches!(err, StateSyncError::ObjectStore(_)))` would
    // overfit to the current mapping.)
    let msg = err.to_string();
    assert!(
        !msg.is_empty(),
        "error message should carry diagnostics: {err:?}"
    );
}
