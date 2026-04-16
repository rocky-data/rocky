//! Remote state persistence.
//!
//! Pulls / pushes the redb state file across runs so ephemeral environments
//! (EKS pods, CI jobs) can resume watermarks and anomaly history. Four
//! backends: local (no-op), S3, GCS, Valkey, or Tiered (Valkey + S3).
//!
//! S3 and GCS use the shared [`ObjectStoreProvider`][crate::object_store::ObjectStoreProvider]
//! so credential resolution follows the standard AWS SDK / GCP ADC chains.

use std::path::Path;

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::{StateBackend, StateConfig};
use crate::object_store::{ObjectStoreError, ObjectStoreProvider};

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
            download_from_object_store("s3", bucket, prefix, local_path).await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            download_from_object_store("gs", bucket, prefix, local_path).await
        }
        StateBackend::Valkey => download_from_valkey(config, local_path),
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
pub async fn upload_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    if !local_path.exists() {
        debug!("No local state file to upload");
        return Ok(());
    }

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
            upload_to_object_store("s3", bucket, prefix, local_path).await
        }
        StateBackend::Gcs => {
            let bucket = config.gcs_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("gcs".into(), "state.gcs_bucket".into())
            })?;
            let prefix = config.gcs_prefix.as_deref().unwrap_or(DEFAULT_GCS_PREFIX);
            upload_to_object_store("gs", bucket, prefix, local_path).await
        }
        StateBackend::Valkey => upload_to_valkey(config, local_path),
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

            // Valkey first (fast, best-effort)
            if let Err(e) = Box::pin(upload_state(&valkey_config, local_path)).await {
                warn!(error = %e, "Valkey upload failed (non-fatal, S3 is durable)");
            }

            // S3 second (durable, required)
            Box::pin(upload_state(&s3_config, local_path)).await
        }
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
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    info!(
        uri = format!("{scheme}://{bucket}/{prefix}{STATE_FILE}"),
        local = %local_path.display(),
        "downloading state from object store"
    );

    match provider.exists(STATE_FILE).await {
        Ok(true) => {
            provider.download_file(STATE_FILE, local_path).await?;
            info!(
                size = local_path.metadata().map(|m| m.len()).unwrap_or(0),
                "state restored from object store"
            );
            Ok(())
        }
        Ok(false) => {
            info!("No existing state in object store — starting fresh");
            Ok(())
        }
        Err(e) => {
            warn!(error = %e, "state existence check failed (non-fatal, starting fresh)");
            Ok(())
        }
    }
}

/// Upload `STATE_FILE` to an object store rooted at `<scheme>://<bucket>/<prefix>`.
async fn upload_to_object_store(
    scheme: &str,
    bucket: &str,
    prefix: &str,
    local_path: &Path,
) -> Result<(), StateSyncError> {
    let provider = cloud_provider(scheme, bucket, prefix)?;
    info!(
        uri = format!("{scheme}://{bucket}/{prefix}{STATE_FILE}"),
        "uploading state to object store"
    );
    provider.upload_file(local_path, STATE_FILE).await?;
    Ok(())
}

/// Download state from Valkey/Redis.
fn download_from_valkey(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?;
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX);
    let key = format!("{prefix}{STATE_FILE}");

    info!(key = key, "downloading state from Valkey");

    let client = redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
    let mut conn = client
        .get_connection()
        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

    let data: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&key)
        .query(&mut conn)
        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

    match data {
        Some(bytes) => {
            std::fs::write(local_path, bytes)?;
            info!(
                size = local_path.metadata()?.len(),
                "state restored from Valkey"
            );
        }
        None => {
            info!("No existing state in Valkey — starting fresh");
        }
    }
    Ok(())
}

/// Upload state to Valkey/Redis.
fn upload_to_valkey(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    let url = config
        .valkey_url
        .as_deref()
        .ok_or_else(|| StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into()))?;
    let prefix = config
        .valkey_prefix
        .as_deref()
        .unwrap_or(DEFAULT_VALKEY_PREFIX);
    let key = format!("{prefix}{STATE_FILE}");

    let data = std::fs::read(local_path)?;
    info!(key = key, size = data.len(), "uploading state to Valkey");

    let client = redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
    let mut conn = client
        .get_connection()
        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

    redis::cmd("SET")
        .arg(&key)
        .arg(data)
        .query::<()>(&mut conn)
        .map_err(|e| StateSyncError::Valkey(e.to_string()))?;

    Ok(())
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
}
