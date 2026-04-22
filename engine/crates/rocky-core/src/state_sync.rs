//! Remote state persistence.
//!
//! Pulls / pushes the redb state file across runs so ephemeral environments
//! (EKS pods, CI jobs) can resume watermarks and anomaly history. Four
//! backends: local (no-op), S3, GCS, Valkey, or Tiered (Valkey + S3).
//!
//! S3 and GCS use the shared [`ObjectStoreProvider`][crate::object_store::ObjectStoreProvider]
//! so credential resolution follows the standard AWS SDK / GCP ADC chains.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use thiserror::Error;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::circuit_breaker::TransitionOutcome;
use crate::config::{RetryConfig, StateBackend, StateConfig, StateUploadFailureMode};
use crate::object_store::{ObjectStoreError, ObjectStoreProvider};
use crate::retry::compute_backoff;
use crate::retry_budget::RetryBudget;

#[derive(Debug, Error)]
pub enum StateSyncError {
    #[error("S3 download failed: {0}")]
    S3Download(String),

    #[error("S3 upload failed: {0}")]
    S3Upload(String),

    #[error("GCS download failed: {0}")]
    GcsDownload(String),

    #[error("GCS upload failed: {0}")]
    GcsUpload(String),

    #[error("Valkey error: {0}")]
    Valkey(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("state backend '{0}' requires {1} to be configured")]
    MissingConfig(String, String),

    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    #[error("state transfer timed out after {0:?}")]
    Timeout(Duration),

    #[error(
        "state backend circuit breaker tripped after {consecutive_failures} consecutive transient failures"
    )]
    CircuitOpen { consecutive_failures: u32 },

    #[error("state retry budget exhausted (limit {limit}); aborting remaining retries")]
    RetryBudgetExhausted { limit: u32 },
}

/// State file name within the configured prefix.
const STATE_FILE: &str = "state.redb";
const DEFAULT_S3_PREFIX: &str = "rocky/state/";
const DEFAULT_GCS_PREFIX: &str = "rocky/state/";
const DEFAULT_VALKEY_PREFIX: &str = "rocky:state:";

/// Build an `ObjectStoreProvider` rooted at `<scheme>://<bucket>/<prefix>`.
fn cloud_provider(
    scheme: &str,
    bucket: &str,
    prefix: &str,
) -> Result<ObjectStoreProvider, StateSyncError> {
    let trimmed_prefix = prefix.trim_end_matches('/');
    let uri = if trimmed_prefix.is_empty() {
        format!("{scheme}://{bucket}")
    } else {
        format!("{scheme}://{bucket}/{trimmed_prefix}")
    };
    Ok(ObjectStoreProvider::from_uri(&uri)?)
}

/// Resolve the per-transfer wall-clock budget from `StateConfig`.
fn transfer_timeout(config: &StateConfig) -> Duration {
    Duration::from_secs(config.transfer_timeout_seconds)
}

/// Downloads state from remote storage to a local file before a run.
pub async fn download_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => {
            debug!("State backend: local (no sync needed)");
            Ok(())
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            download_from_object_store("s3", bucket, prefix, local_path, transfer_timeout(config))
                .await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            download_from_object_store("gs", bucket, prefix, local_path, transfer_timeout(config))
                .await
        }
        StateBackend::Valkey => download_from_valkey(config, local_path).await,
        StateBackend::Tiered => {
            // Try Valkey first (fast), fall back to S3 (durable)
            info!("State backend: tiered (Valkey → S3 fallback)");
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };

            match Box::pin(download_state(&valkey_config, local_path)).await {
                Ok(()) if local_path.exists() => {
                    debug!("State restored from Valkey");
                    Ok(())
                }
                _ => {
                    debug!("Valkey miss or error, trying S3");
                    Box::pin(download_state(&s3_config, local_path)).await
                }
            }
        }
    }
}

/// Uploads state from a local file to remote storage after a run.
///
/// Applies `config.retry` to transient failures and `config.on_upload_failure`
/// to the final result — by default (`Skip`) a post-retry failure is logged
/// and reported back as `Ok` so the run continues in degraded mode, matching
/// the de-facto behaviour of existing callers that `warn + continue` on upload
/// errors. Set `on_upload_failure = "fail"` for strict environments that must
/// treat state durability as a hard requirement.
pub async fn upload_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    if !local_path.exists() {
        debug!("No local state file to upload");
        return Ok(());
    }
    let result = dispatch_upload(config, local_path).await;
    apply_upload_failure_policy(config, result)
}

/// Internal upload dispatch — runs the raw upload (with retry) without
/// applying the `on_upload_failure` policy. Tiered recursion uses this
/// directly so the skip/fail decision is evaluated exactly once at the
/// outermost `upload_state` call, not per-leg.
async fn dispatch_upload(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => {
            debug!("State backend: local (no sync needed)");
            Ok(())
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            upload_to_object_store(
                "s3",
                bucket,
                prefix,
                local_path,
                transfer_timeout(config),
                &config.retry,
            )
            .await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            upload_to_object_store(
                "gs",
                bucket,
                prefix,
                local_path,
                transfer_timeout(config),
                &config.retry,
            )
            .await
        }
        StateBackend::Valkey => upload_to_valkey(config, local_path).await,
        StateBackend::Tiered => {
            // Write to both Valkey (fast) and S3 (durable)
            info!("State backend: tiered (uploading to Valkey + S3)");
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };

            // Valkey first (fast, best-effort). Recurse into the *inner*
            // dispatch so `on_upload_failure` is not applied here — the
            // outer `upload_state` owns that decision for the tiered leg
            // as a whole.
            if let Err(e) = Box::pin(dispatch_upload(&valkey_config, local_path)).await {
                warn!(error = %e, "Valkey upload failed (non-fatal, S3 is durable)");
            }

            // S3 second (durable, required)
            Box::pin(dispatch_upload(&s3_config, local_path)).await
        }
    }
}

/// Apply the `on_upload_failure` policy to a terminal upload result. `Skip`
/// converts Err → Ok with a structured warn; `Fail` propagates Err unchanged.
fn apply_upload_failure_policy(
    config: &StateConfig,
    result: Result<(), StateSyncError>,
) -> Result<(), StateSyncError> {
    match result {
        Ok(()) => Ok(()),
        Err(e) => match config.on_upload_failure {
            StateUploadFailureMode::Skip => {
                warn!(
                    error = %e,
                    outcome = "skipped_after_failure",
                    "state upload failed after retries; continuing in degraded mode \
                     (next run's discover will re-derive state)"
                );
                Ok(())
            }
            StateUploadFailureMode::Fail => Err(e),
        },
    }
}

/// Download `STATE_FILE` from an object store rooted at `<scheme>://<bucket>/<prefix>`.
///
/// If the object doesn't exist, logs a message and returns Ok — rocky will
/// start with a fresh state file.
async fn download_from_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
    timeout: Duration,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let span = info_span!(
        "state.download",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{STATE_FILE}"),
            local = %local_path.display(),
            "downloading state from object store"
        );

        with_transfer_timeout(timeout, async {
            match provider.exists(STATE_FILE).await {
                Ok(true) => {
                    provider.download_file(STATE_FILE, local_path).await?;
                    info!(
                        size = local_path.metadata().map(|m| m.len()).unwrap_or(0),
                        outcome = "ok",
                        "state restored from object store"
                    );
                    Ok(())
                }
                Ok(false) => {
                    info!(outcome = "absent", "No existing state in object store — starting fresh");
                    Ok(())
                }
                Err(e) => {
                    warn!(error = %e, outcome = "error_then_fresh", "state existence check failed (non-fatal, starting fresh)");
                    Ok(())
                }
            }
        })
        .await
    }
    .instrument(span)
    .await
}

/// Upload `STATE_FILE` to an object store rooted at `<scheme>://<bucket>/<prefix>`.
///
/// Wraps the put in a retry loop driven by `retry` (shared-shape
/// [`RetryConfig`]) and a call-local circuit breaker + retry budget. The
/// outer `with_transfer_timeout` still caps total wall-clock, so retries
/// share — not extend — the configured transfer budget.
async fn upload_to_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
    timeout: Duration,
    retry: &RetryConfig,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let size_bytes = local_path.metadata().map(|m| m.len()).unwrap_or(0);
    let span = info_span!(
        "state.upload",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
        size_bytes,
    );
    async {
        info!(
            uri = format!("{scheme}://{bucket}/{prefix}{STATE_FILE}"),
            "uploading state to object store"
        );
        let retries = with_transfer_timeout(timeout, async {
            retry_transient(retry, "state.upload.object_store", || async {
                provider
                    .upload_file(local_path, STATE_FILE)
                    .await
                    .map_err(StateSyncError::from)
            })
            .await
        })
        .await?;
        info!(
            bytes = size_bytes,
            retries,
            outcome = "ok",
            "state upload complete"
        );
        Ok(())
    }
    .instrument(span)
    .await
}

/// Wrap a state-transfer future with the configured timeout budget. On elapse
/// the returned error is `StateSyncError::Timeout` — distinct from the
/// per-request timeout the client raises, so callers can tell the two apart.
///
/// Emits a structured `tracing::warn!` on elapse so operators can diagnose
/// hung transfers from log output alone. Span fields (`backend`, `bucket`,
/// `size_bytes`) propagate via the enclosing `state.{upload,download}` span.
async fn with_transfer_timeout<F, T>(timeout: Duration, fut: F) -> Result<T, StateSyncError>
where
    F: std::future::Future<Output = Result<T, StateSyncError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                duration_ms = timeout.as_millis() as u64,
                outcome = "timeout",
                "state transfer exceeded timeout budget"
            );
            Err(StateSyncError::Timeout(timeout))
        }
    }
}

/// Download state from Valkey/Redis.
///
/// The `redis` crate's sync client blocks the current thread — a dead Valkey
/// peer would otherwise stall the tokio runtime indefinitely and no outer
/// `tokio::time::timeout` could rescue it. We offload the blocking work to a
/// dedicated thread via `spawn_blocking` and gate it with the same
/// `transfer_timeout_seconds` budget the object-store paths use.
async fn download_from_valkey(
    config: &StateConfig,
    local_path: &Path,
) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    let key = format!("{prefix}{STATE_FILE}");
    let local_path_owned = local_path.to_path_buf();
    let timeout = transfer_timeout(config);

    let span = info_span!("state.download", backend = "valkey");
    async move {
        info!(key = %key, "downloading state from Valkey");
        with_transfer_timeout(timeout, async move {
            let key_for_task = key.clone();
            let local_for_task = local_path_owned.clone();
            let join = tokio::task::spawn_blocking(move || -> Result<(), StateSyncError> {
                let client =
                    redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                let mut conn = client
                    .get_connection()
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                let data: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key_for_task)
                    .query(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                match data {
                    Some(bytes) => {
                        std::fs::write(&local_for_task, bytes)?;
                        let size = std::fs::metadata(&local_for_task)
                            .map(|m| m.len())
                            .unwrap_or(0);
                        info!(size, outcome = "ok", "state restored from Valkey");
                    }
                    None => {
                        info!(
                            outcome = "absent",
                            "No existing state in Valkey — starting fresh"
                        );
                    }
                }
                Ok(())
            })
            .await;
            match join {
                Ok(inner) => inner,
                Err(e) => Err(StateSyncError::Valkey(format!(
                    "valkey worker task failed: {e}"
                ))),
            }
        })
        .await
    }
    .instrument(span)
    .await
}

/// Upload state to Valkey/Redis.
///
/// Mirrors [`download_from_valkey`]: offloads the blocking redis SET via
/// `spawn_blocking` and wraps the handle with `with_transfer_timeout` so a
/// hung Valkey peer cannot stall the run past the configured budget.
async fn upload_to_valkey(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    let key = format!("{prefix}{STATE_FILE}");
    let data = std::fs::read(local_path)?;
    let size_bytes = data.len() as u64;
    let timeout = transfer_timeout(config);
    let retry = &config.retry;

    let span = info_span!("state.upload", backend = "valkey", size_bytes);
    async move {
        info!(key = %key, size = size_bytes, "uploading state to Valkey");
        let retries = with_transfer_timeout(timeout, async {
            retry_transient(retry, "state.upload.valkey", || {
                let url = url.clone();
                let key = key.clone();
                let data = data.clone();
                async move {
                    let join =
                        tokio::task::spawn_blocking(move || -> Result<(), StateSyncError> {
                            let client = redis::Client::open(url)
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                            let mut conn = client
                                .get_connection()
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

                            redis::cmd("SET")
                                .arg(&key)
                                .arg(data)
                                .query::<()>(&mut conn)
                                .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                            Ok(())
                        })
                        .await;
                    match join {
                        Ok(inner) => inner,
                        Err(e) => Err(StateSyncError::Valkey(format!(
                            "valkey worker task failed: {e}"
                        ))),
                    }
                }
            })
            .await
        })
        .await?;
        info!(
            bytes = size_bytes,
            retries,
            outcome = "ok",
            "state upload complete"
        );
        Ok(())
    }
    .instrument(span)
    .await
}

/// Round-trip RW probe against the configured state backend.
///
/// Writes a short-lived marker to a **distinct key** (never the real
/// `state.redb`), reads it back, and deletes it. Used by `rocky doctor`
/// to verify a state backend is actually reachable and writable — not
/// merely configured. Honours `transfer_timeout_seconds` as an outer
/// wall-clock cap; no retries — probes should produce a single-pass
/// pass/fail signal, not resilient writes.
///
/// For `tiered` both legs (Valkey + S3) must pass — either one failing
/// fails the probe. For `local` this is a no-op returning `Ok`.
pub async fn probe_state_backend(config: &StateConfig) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => Ok(()),
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or(DEFAULT_S3_PREFIX);
            probe_object_store("s3", bucket, prefix, transfer_timeout(config)).await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            probe_object_store("gs", bucket, prefix, transfer_timeout(config)).await
        }
        StateBackend::Valkey => probe_valkey(config).await,
        StateBackend::Tiered => {
            let valkey_config = StateConfig {
                backend: StateBackend::Valkey,
                ..config.clone()
            };
            let s3_config = StateConfig {
                backend: StateBackend::S3,
                ..config.clone()
            };
            Box::pin(probe_state_backend(&valkey_config)).await?;
            Box::pin(probe_state_backend(&s3_config)).await
        }
    }
}

/// Build a per-call probe key under the configured prefix. Uses PID +
/// nanosecond epoch so concurrent doctor invocations don't collide.
fn probe_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("doctor-probe-{}-{}.marker", std::process::id(), nanos)
}

async fn probe_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    timeout: Duration,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    let key = probe_key();
    let data = Bytes::from_static(b"rocky doctor probe");
    let span = info_span!(
        "state.probe",
        backend = %provider.scheme(),
        bucket = %provider.bucket(),
    );
    async move {
        with_transfer_timeout(timeout, async {
            provider.put(&key, data.clone()).await?;
            let got = provider.get(&key).await?;
            if got != data {
                // Reuse the backend-specific Upload variant so the message
                // carries the scheme; the probe-vs-real-upload distinction
                // is captured by the `state.probe` span name above.
                let err = format!("probe content mismatch (wrote {} bytes, read {} bytes)", data.len(), got.len());
                return match scheme {
                    "s3" => Err(StateSyncError::S3Upload(err)),
                    "gs" => Err(StateSyncError::GcsUpload(err)),
                    _ => Err(StateSyncError::S3Upload(err)),
                };
            }
            // Best-effort cleanup — a stale probe object is a small cost
            // (< 20 bytes, lifecycle rules clean up eventually); surfacing
            // a delete failure on an otherwise successful probe would
            // mask the real signal (RW works).
            if let Err(e) = provider.delete(&key).await {
                warn!(error = %e, key = %key, outcome = "probe_cleanup_failed", "state probe cleanup failed (object will remain until lifecycle rule cleans it up)");
            }
            info!(outcome = "ok", "state backend probe succeeded");
            Ok(())
        })
        .await
    }
    .instrument(span)
    .await
}

async fn probe_valkey(config: &StateConfig) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?
        .to_string();
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX)
        .to_string();
    let key = format!("{prefix}{}", probe_key());
    let timeout = transfer_timeout(config);

    let span = info_span!("state.probe", backend = "valkey");
    async move {
        with_transfer_timeout(timeout, async move {
            let join = tokio::task::spawn_blocking(move || -> Result<(), StateSyncError> {
                let client =
                    redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                let mut conn = client
                    .get_connection()
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                redis::cmd("SET")
                    .arg(&key)
                    .arg("rocky doctor probe")
                    .query::<()>(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                let val: Option<String> = redis::cmd("GET")
                    .arg(&key)
                    .query(&mut conn)
                    .map_err(|e| StateSyncError::Valkey(e.to_string()))?;
                if val.as_deref() != Some("rocky doctor probe") {
                    return Err(StateSyncError::Valkey(format!(
                        "probe value mismatch (got {val:?})"
                    )));
                }
                // Best-effort cleanup — same rationale as the object-store path.
                let _ = redis::cmd("DEL").arg(&key).query::<()>(&mut conn);
                Ok(())
            })
            .await;
            match join {
                Ok(inner) => inner,
                Err(e) => Err(StateSyncError::Valkey(format!(
                    "valkey worker task failed: {e}"
                ))),
            }
        })
        .await?;
        info!(outcome = "ok", "state backend probe succeeded");
        Ok(())
    }
    .instrument(span)
    .await
}

/// Drive `op` through the shared retry + circuit-breaker + budget policy.
///
/// Returns the number of retries consumed by the successful attempt (0 when
/// the first try wins) so the caller can stamp `retries` on the terminal
/// span event. On permanent failure returns the underlying
/// [`StateSyncError`] — or [`StateSyncError::CircuitOpen`] /
/// [`StateSyncError::RetryBudgetExhausted`] when the abort happens inside
/// this helper rather than at the transport layer.
///
/// The circuit breaker and retry budget are built per-call from `cfg`. This
/// keeps state-sync's lifecycle simple (each upload/download is independent)
/// and mirrors the `[adapter.databricks.retry]` shape end-to-end for
/// operational parity. Cross-call breaker state could be wired in later via
/// a state-sync context struct, but no caller needs that today.
async fn retry_transient<F, Fut>(
    cfg: &RetryConfig,
    op_name: &str,
    mut op: F,
) -> Result<u32, StateSyncError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), StateSyncError>>,
{
    let breaker = cfg.build_circuit_breaker();
    let budget = RetryBudget::from_config(cfg.max_retries_per_run);

    if let Err(e) = breaker.check() {
        return Err(StateSyncError::CircuitOpen {
            consecutive_failures: e.consecutive_failures,
        });
    }

    for attempt in 0..=cfg.max_retries {
        match op().await {
            Ok(()) => {
                if breaker.record_success() == TransitionOutcome::Recovered {
                    info!(
                        op = op_name,
                        outcome = "recovered",
                        "state backend circuit breaker recovered"
                    );
                }
                return Ok(attempt);
            }
            Err(err) if is_transient(&err) => {
                if breaker.record_failure(&err.to_string()) == TransitionOutcome::Tripped {
                    warn!(
                        op = op_name,
                        outcome = "circuit_open",
                        error = %err,
                        "state backend circuit breaker tripped"
                    );
                }
                if attempt < cfg.max_retries {
                    if !budget.try_consume() {
                        let limit = budget.total().unwrap_or(0);
                        warn!(
                            op = op_name,
                            attempt = attempt + 1,
                            budget_limit = limit,
                            error = %err,
                            outcome = "budget_exhausted",
                            "state retry budget exhausted; aborting further retries"
                        );
                        return Err(StateSyncError::RetryBudgetExhausted { limit });
                    }
                    let backoff_ms = compute_backoff(cfg, attempt);
                    warn!(
                        op = op_name,
                        attempt = attempt + 1,
                        max_retries = cfg.max_retries,
                        backoff_ms,
                        error = %err,
                        outcome = "retry",
                        "state transient error, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                warn!(
                    op = op_name,
                    attempts = attempt + 1,
                    error = %err,
                    outcome = "transient_exhausted",
                    "state transient retries exhausted"
                );
                return Err(err);
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("retry loop always returns within the for body")
}

/// Classify a state-sync error for retry decisions.
///
/// Network/SDK-level errors and timeouts are treated as transient — a
/// fresh attempt might clear a single flake. Config errors and local disk
/// I/O are permanent; retrying them wastes budget. The breaker/budget
/// sentinels are already terminal by construction and must not re-enter
/// the retry loop.
fn is_transient(err: &StateSyncError) -> bool {
    match err {
        StateSyncError::S3Download(_)
        | StateSyncError::S3Upload(_)
        | StateSyncError::GcsDownload(_)
        | StateSyncError::GcsUpload(_)
        | StateSyncError::Valkey(_)
        | StateSyncError::Timeout(_) => true,
        StateSyncError::ObjectStore(ObjectStoreError::Backend(_)) => true,
        StateSyncError::ObjectStore(
            ObjectStoreError::InvalidUri(..)
            | ObjectStoreError::UnsupportedScheme(_)
            | ObjectStoreError::Io(_),
        )
        | StateSyncError::Io(_)
        | StateSyncError::MissingConfig(..)
        | StateSyncError::CircuitOpen { .. }
        | StateSyncError::RetryBudgetExhausted { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_download_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(download_state(&config, &path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_upload_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(upload_state(&config, &path).await.is_ok());
    }

    #[tokio::test]
    async fn test_s3_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb")).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[tokio::test]
    async fn test_gcs_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::Gcs,
            gcs_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb")).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[tokio::test]
    async fn test_valkey_missing_url() {
        let config = StateConfig {
            backend: StateBackend::Valkey,
            valkey_url: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb")).await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[test]
    fn test_state_backend_display_includes_gcs() {
        assert_eq!(StateBackend::Gcs.to_string(), "gcs");
    }

    #[test]
    fn test_default_state_transfer_timeout() {
        let config = StateConfig::default();
        assert_eq!(config.transfer_timeout_seconds, 300);
    }

    #[tokio::test]
    async fn test_probe_state_backend_local_is_noop() {
        let config = StateConfig::default();
        assert!(matches!(config.backend, StateBackend::Local));
        probe_state_backend(&config)
            .await
            .expect("Local backend probe should be a no-op");
    }

    #[tokio::test]
    async fn test_probe_state_backend_s3_missing_bucket_fails_fast() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let err = probe_state_backend(&config)
            .await
            .expect_err("missing bucket should error");
        assert!(matches!(err, StateSyncError::MissingConfig(..)));
    }

    #[tokio::test]
    async fn test_probe_state_backend_valkey_missing_url_fails_fast() {
        let config = StateConfig {
            backend: StateBackend::Valkey,
            valkey_url: None,
            ..Default::default()
        };
        let err = probe_state_backend(&config)
            .await
            .expect_err("missing URL should error");
        assert!(matches!(err, StateSyncError::MissingConfig(..)));
    }

    #[test]
    fn test_probe_key_is_unique_across_calls() {
        let a = probe_key();
        let b = probe_key();
        assert_ne!(a, b, "probe_key should produce unique values per call");
        assert!(a.starts_with("doctor-probe-"));
        assert!(a.ends_with(".marker"));
    }

    #[test]
    fn test_default_on_upload_failure_is_skip() {
        let config = StateConfig::default();
        assert_eq!(config.on_upload_failure, StateUploadFailureMode::Skip);
    }

    #[test]
    fn test_is_transient_network_and_timeout() {
        assert!(is_transient(&StateSyncError::S3Upload("boom".into())));
        assert!(is_transient(&StateSyncError::S3Download("boom".into())));
        assert!(is_transient(&StateSyncError::GcsUpload("boom".into())));
        assert!(is_transient(&StateSyncError::GcsDownload("boom".into())));
        assert!(is_transient(&StateSyncError::Valkey("boom".into())));
        assert!(is_transient(&StateSyncError::Timeout(Duration::from_secs(
            1
        ))));
    }

    #[test]
    fn test_is_transient_permanent_errors_not_retried() {
        assert!(!is_transient(&StateSyncError::MissingConfig(
            "s3".into(),
            "bucket".into()
        )));
        assert!(!is_transient(&StateSyncError::CircuitOpen {
            consecutive_failures: 5
        }));
        assert!(!is_transient(&StateSyncError::RetryBudgetExhausted {
            limit: 3
        }));
    }

    #[tokio::test]
    async fn test_retry_transient_succeeds_after_two_transient_failures() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(StateSyncError::S3Upload(format!("flake {n}")))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 2, "should have taken 2 retries");
    }

    #[tokio::test]
    async fn test_retry_transient_gives_up_after_max_retries() {
        let cfg = RetryConfig {
            max_retries: 2,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let result = retry_transient(&cfg, "test", || async {
            Err(StateSyncError::S3Upload("always fails".into()))
        })
        .await;
        assert!(matches!(result, Err(StateSyncError::S3Upload(_))));
    }

    #[tokio::test]
    async fn test_retry_transient_does_not_retry_permanent_errors() {
        let cfg = RetryConfig {
            max_retries: 3,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(StateSyncError::MissingConfig("s3".into(), "bucket".into())) }
        })
        .await;
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "permanent errors should not be retried"
        );
    }

    #[tokio::test]
    async fn test_retry_budget_exhaustion_aborts_early() {
        let cfg = RetryConfig {
            max_retries: 5,
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            backoff_multiplier: 2.0,
            jitter: false,
            max_retries_per_run: Some(1),
            ..RetryConfig::default()
        };
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&cfg, "test", || {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err(StateSyncError::S3Upload("always flakes".into())) }
        })
        .await;
        // With budget=1: first attempt fails → consume retry slot → one
        // retry attempt → fails → budget exhausted → return
        // RetryBudgetExhausted. Total 2 calls to op() before we error.
        assert!(matches!(
            result,
            Err(StateSyncError::RetryBudgetExhausted { limit: 1 })
        ));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "budget should abort after first retry",
        );
    }
}
