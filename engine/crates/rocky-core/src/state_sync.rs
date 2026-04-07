use std::path::Path;
use std::process::Command;

use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::{StateBackend, StateConfig};

#[derive(Debug, Error)]
pub enum StateSyncError {
    #[error("S3 download failed: {0}")]
    S3Download(String),

    #[error("S3 upload failed: {0}")]
    S3Upload(String),

    #[error("Valkey error: {0}")]
    Valkey(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("state backend '{0}' requires {1} to be configured")]
    MissingConfig(String, String),
}

/// Downloads state from remote storage to a local file before a run.
///
/// - `local`: no-op (state already on disk)
/// - `s3`: `aws s3 cp s3://{bucket}/{prefix}state.redb {local_path}`
/// - `valkey`: GET key → write to local file
pub fn download_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
    match config.backend {
        StateBackend::Local => {
            debug!("State backend: local (no sync needed)");
            Ok(())
        }
        StateBackend::S3 => {
            let bucket = config.s3_bucket.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("s3".into(), "state.s3_bucket".into())
            })?;
            let prefix = config.s3_prefix.as_deref().unwrap_or("rocky/state/");
            let s3_uri = format!("s3://{bucket}/{prefix}state.redb");

            info!(s3_uri = s3_uri, local = %local_path.display(), "downloading state from S3");

            let output = Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &s3_uri,
                    &local_path.display().to_string(),
                    "--quiet",
                ])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("NoSuchKey")
                    || stderr.contains("404")
                    || stderr.contains("does not exist")
                {
                    info!("No existing state in S3 — starting fresh");
                    return Ok(());
                }
                warn!(stderr = %stderr, "S3 download failed (non-fatal, starting fresh)");
            }
            Ok(())
        }
        StateBackend::Valkey => {
            let url = config.valkey_url.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into())
            })?;
            let prefix = config.valkey_prefix.as_deref().unwrap_or("rocky:state:");
            let key = format!("{prefix}state.redb");

            info!(key = key, "downloading state from Valkey");

            let client =
                redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
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

            match download_state(&valkey_config, local_path) {
                Ok(()) if local_path.exists() => {
                    debug!("State restored from Valkey");
                    Ok(())
                }
                _ => {
                    debug!("Valkey miss or error, trying S3");
                    download_state(&s3_config, local_path)
                }
            }
        }
    }
}

/// Uploads state from a local file to remote storage after a run.
pub fn upload_state(config: &StateConfig, local_path: &Path) -> Result<(), StateSyncError> {
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
            let prefix = config.s3_prefix.as_deref().unwrap_or("rocky/state/");
            let s3_uri = format!("s3://{bucket}/{prefix}state.redb");

            info!(s3_uri = s3_uri, "uploading state to S3");

            let output = Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &local_path.display().to_string(),
                    &s3_uri,
                    "--quiet",
                ])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(StateSyncError::S3Upload(stderr.to_string()));
            }
            Ok(())
        }
        StateBackend::Valkey => {
            let url = config.valkey_url.as_deref().ok_or_else(|| {
                StateSyncError::MissingConfig("valkey".into(), "state.valkey_url".into())
            })?;
            let prefix = config.valkey_prefix.as_deref().unwrap_or("rocky:state:");
            let key = format!("{prefix}state.redb");

            let data = std::fs::read(local_path)?;
            info!(key = key, size = data.len(), "uploading state to Valkey");

            let client =
                redis::Client::open(url).map_err(|e| StateSyncError::Valkey(e.to_string()))?;
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
            if let Err(e) = upload_state(&valkey_config, local_path) {
                warn!(error = %e, "Valkey upload failed (non-fatal, S3 is durable)");
            }

            // S3 second (durable, required)
            upload_state(&s3_config, local_path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_local_download_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(download_state(&config, &path).is_ok());
    }

    #[test]
    fn test_local_upload_noop() {
        let config = StateConfig::default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        assert!(upload_state(&config, &path).is_ok());
    }

    #[test]
    fn test_s3_missing_bucket() {
        let config = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"));
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }

    #[test]
    fn test_valkey_missing_url() {
        let config = StateConfig {
            backend: StateBackend::Valkey,
            valkey_url: None,
            ..Default::default()
        };
        let dir = TempDir::new().unwrap();
        let result = download_state(&config, &dir.path().join("state.redb"));
        assert!(matches!(result, Err(StateSyncError::MissingConfig(..))));
    }
}
