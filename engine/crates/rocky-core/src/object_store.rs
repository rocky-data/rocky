//! Unified cloud storage access via the Apache `object_store` crate.
//!
//! Wraps `object_store::ObjectStore` so Rocky code can read and write to S3,
//! GCS, or local filesystem behind one interface. Credentials come from the
//! standard provider chains (`AWS_*` env vars, `GOOGLE_APPLICATION_CREDENTIALS`,
//! etc.) — Rocky does not re-implement credential resolution.
//!
//! # Example
//!
//! ```no_run
//! use rocky_core::object_store::ObjectStoreProvider;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = ObjectStoreProvider::from_uri("s3://my-bucket/prefix")?;
//! let bytes = provider.get("state.redb").await?;
//! # Ok(()) }
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use object_store::{ClientOptions, ObjectStore, PutMode, PutOptions, path::Path as ObjectPath};
use thiserror::Error;
use tracing::debug;

/// Per-request HTTP timeout for the underlying cloud client. Without this the
/// default is unbounded, so a stuck TCP connection can hang forever (see Gold
/// run f107d533, 2026-04-18, where S3 state upload hung for ~7h before the
/// run was externally canceled).
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Connect-phase timeout. Shorter than the request timeout so unreachable
/// endpoints fail fast rather than chewing through the retry budget.
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn default_client_options() -> ClientOptions {
    let mut opts = ClientOptions::new()
        .with_timeout(CLIENT_REQUEST_TIMEOUT)
        .with_connect_timeout(CLIENT_CONNECT_TIMEOUT);
    // Respect the standard `object_store` env vars for allowing plain HTTP.
    // Production never sets these; integration tests (which front-end the S3
    // SDK with a local HTTP mock server) do.
    let allow_http = std::env::var("AWS_ALLOW_HTTP")
        .or_else(|_| std::env::var("GOOGLE_STORAGE_ALLOW_HTTP"))
        .or_else(|_| std::env::var("AZURE_ALLOW_HTTP"))
        .ok()
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    if allow_http {
        opts = opts.with_allow_http(true);
    }
    opts
}

/// Errors returned by [`ObjectStoreProvider`] operations.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("invalid cloud URI '{0}': {1}")]
    InvalidUri(String, String),

    #[error("unsupported scheme '{0}'; supported: s3, gs, az, file")]
    UnsupportedScheme(String),

    #[error("object store error: {0}")]
    Backend(#[from] object_store::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for object store operations.
pub type ObjectStoreResult<T> = Result<T, ObjectStoreError>;

/// Outcome of [`ObjectStoreProvider::put_if_not_exists`].
///
/// The backend's atomic "create-if-absent" primitive returns `Created` on a
/// successful write and `AlreadyExists` when another writer got there first
/// (S3 412 `PreconditionFailed` / GCS 412 `preconditionFailed` / local
/// filesystem `EEXIST`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutIfNotExistsOutcome {
    /// The object was written. Caller has exclusive claim on the key.
    Created,
    /// The object already existed; no bytes were written. Caller should
    /// read the existing object to resolve the conflict.
    AlreadyExists,
}

/// Wraps an [`ObjectStore`] with a root path, providing a simple async API for
/// reading and writing objects.
///
/// Created via [`from_uri`](Self::from_uri) which auto-detects the scheme
/// (s3/gs/az/file) and constructs the appropriate backend.
#[derive(Clone)]
pub struct ObjectStoreProvider {
    store: Arc<dyn ObjectStore>,
    /// URI scheme for logging / error messages.
    scheme: String,
    /// Bucket or container (without scheme).
    bucket: String,
    /// Optional path prefix within the bucket.
    prefix: String,
}

impl std::fmt::Debug for ObjectStoreProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreProvider")
            .field("scheme", &self.scheme)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl ObjectStoreProvider {
    /// Construct a provider from a cloud URI.
    ///
    /// Supported schemes:
    /// - `s3://bucket/prefix` → AWS S3
    /// - `gs://bucket/prefix` → Google Cloud Storage
    /// - `az://container/prefix` or `abfs://` → Azure Blob Storage
    /// - `file:///path` → local filesystem (mainly for testing)
    ///
    /// Credentials are resolved from the environment (AWS SDK credential
    /// chain, `GOOGLE_APPLICATION_CREDENTIALS`, etc.).
    pub fn from_uri(uri: &str) -> ObjectStoreResult<Self> {
        let parsed = url::Url::parse(uri)
            .map_err(|e| ObjectStoreError::InvalidUri(uri.into(), e.to_string()))?;
        let scheme = parsed.scheme().to_string();
        let bucket = parsed
            .host_str()
            .ok_or_else(|| ObjectStoreError::InvalidUri(uri.into(), "missing host/bucket".into()))?
            .to_string();
        let prefix = parsed
            .path()
            .trim_start_matches('/')
            .trim_end_matches('/')
            .to_string();

        let store: Arc<dyn ObjectStore> = match scheme.as_str() {
            "s3" | "s3a" => {
                let builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(&bucket)
                    .with_client_options(default_client_options());
                Arc::new(builder.build()?)
            }
            "gs" | "gcs" => {
                let builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&bucket)
                    .with_client_options(default_client_options());
                Arc::new(builder.build()?)
            }
            "az" | "abfs" | "abfss" => {
                let builder = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(&bucket)
                    .with_client_options(default_client_options());
                Arc::new(builder.build()?)
            }
            "file" => {
                let root = parsed.path();
                Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root)?)
            }
            other => return Err(ObjectStoreError::UnsupportedScheme(other.into())),
        };

        Ok(Self {
            store,
            scheme,
            bucket,
            prefix,
        })
    }

    /// Construct a provider from an in-memory backend (for tests).
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            scheme: "memory".into(),
            bucket: "memory".into(),
            prefix: String::new(),
        }
    }

    /// The URI scheme (`"s3"`, `"gs"`, `"file"`, etc.).
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// The bucket or container name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Build an absolute path within the provider's prefix.
    fn absolute_path(&self, relative: &str) -> ObjectPath {
        let relative = relative.trim_start_matches('/');
        if self.prefix.is_empty() {
            ObjectPath::from(relative)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, relative))
        }
    }

    /// Read an object from the store, returning its full contents as bytes.
    pub async fn get(&self, relative_path: &str) -> ObjectStoreResult<Bytes> {
        let path = self.absolute_path(relative_path);
        debug!(path = %path, "object_store get");
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    /// Write bytes to an object in the store, replacing any existing content.
    pub async fn put(&self, relative_path: &str, data: Bytes) -> ObjectStoreResult<()> {
        let path = self.absolute_path(relative_path);
        debug!(path = %path, size = data.len(), "object_store put");
        self.store.put(&path, data.into()).await?;
        Ok(())
    }

    /// Write bytes only if the object does not yet exist.
    ///
    /// Uses [`PutMode::Create`], which maps to:
    /// - S3: `If-None-Match: "*"` on `PutObject` (available since Nov 2024).
    /// - GCS: `x-goog-if-generation-match: 0` precondition.
    /// - Local filesystem: `O_CREAT | O_EXCL` open.
    /// - Memory: atomic check inside the in-memory map.
    ///
    /// Returns [`PutIfNotExistsOutcome::Created`] on a successful claim or
    /// [`PutIfNotExistsOutcome::AlreadyExists`] when the backend refuses the
    /// write because the object is already present. Other errors propagate
    /// through [`ObjectStoreError::Backend`].
    ///
    /// This is the atomic primitive backing `rocky run --idempotency-key` on
    /// `s3`-only and `gcs`-only state backends (FR-004 Phase 2 / Phase 3).
    pub async fn put_if_not_exists(
        &self,
        relative_path: &str,
        data: Bytes,
    ) -> ObjectStoreResult<PutIfNotExistsOutcome> {
        let path = self.absolute_path(relative_path);
        debug!(
            path = %path,
            size = data.len(),
            "object_store put_if_not_exists"
        );
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&path, data.into(), opts).await {
            Ok(_) => Ok(PutIfNotExistsOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Ok(PutIfNotExistsOutcome::AlreadyExists)
            }
            Err(e) => Err(ObjectStoreError::Backend(e)),
        }
    }

    /// Upload a local file to the object store.
    pub async fn upload_file(
        &self,
        local_path: &Path,
        relative_path: &str,
    ) -> ObjectStoreResult<()> {
        let data = tokio::fs::read(local_path).await?;
        self.put(relative_path, Bytes::from(data)).await
    }

    /// Download an object from the store to a local file.
    pub async fn download_file(
        &self,
        relative_path: &str,
        local_path: &Path,
    ) -> ObjectStoreResult<()> {
        let bytes = self.get(relative_path).await?;
        tokio::fs::write(local_path, &bytes).await?;
        Ok(())
    }

    /// List objects under a relative prefix, returning their paths relative to
    /// this provider's root.
    pub async fn list(&self, relative_prefix: &str) -> ObjectStoreResult<Vec<String>> {
        let prefix = self.absolute_path(relative_prefix);
        let mut stream = self.store.list(Some(&prefix));
        let mut paths = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            // Strip the provider's prefix so callers get relative paths.
            let full = meta.location.to_string();
            let stripped = if self.prefix.is_empty() {
                full.clone()
            } else {
                full.strip_prefix(&format!("{}/", self.prefix))
                    .unwrap_or(&full)
                    .to_string()
            };
            paths.push(stripped);
        }
        Ok(paths)
    }

    /// Delete an object.
    pub async fn delete(&self, relative_path: &str) -> ObjectStoreResult<()> {
        let path = self.absolute_path(relative_path);
        self.store.delete(&path).await?;
        Ok(())
    }

    /// Returns `true` if the object exists.
    pub async fn exists(&self, relative_path: &str) -> ObjectStoreResult<bool> {
        let path = self.absolute_path(relative_path);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_put_get() {
        let provider = ObjectStoreProvider::in_memory();
        provider
            .put("key1", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = provider.get("key1").await.unwrap();
        assert_eq!(got.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn test_in_memory_list() {
        let provider = ObjectStoreProvider::in_memory();
        provider
            .put("a/1.txt", Bytes::from_static(b"1"))
            .await
            .unwrap();
        provider
            .put("a/2.txt", Bytes::from_static(b"2"))
            .await
            .unwrap();
        provider
            .put("b/3.txt", Bytes::from_static(b"3"))
            .await
            .unwrap();

        let mut paths = provider.list("a").await.unwrap();
        paths.sort();
        assert_eq!(paths, vec!["a/1.txt".to_string(), "a/2.txt".to_string()]);
    }

    #[tokio::test]
    async fn test_in_memory_exists_delete() {
        let provider = ObjectStoreProvider::in_memory();
        assert!(!provider.exists("k").await.unwrap());
        provider.put("k", Bytes::from_static(b"v")).await.unwrap();
        assert!(provider.exists("k").await.unwrap());
        provider.delete("k").await.unwrap();
        assert!(!provider.exists("k").await.unwrap());
    }

    #[tokio::test]
    async fn test_put_if_not_exists_first_call_creates() {
        let provider = ObjectStoreProvider::in_memory();
        let outcome = provider
            .put_if_not_exists("claim-key", Bytes::from_static(b"first"))
            .await
            .unwrap();
        assert_eq!(outcome, PutIfNotExistsOutcome::Created);
        let stored = provider.get("claim-key").await.unwrap();
        assert_eq!(stored.as_ref(), b"first");
    }

    #[tokio::test]
    async fn test_put_if_not_exists_second_call_reports_existing() {
        let provider = ObjectStoreProvider::in_memory();
        provider
            .put_if_not_exists("claim-key", Bytes::from_static(b"first"))
            .await
            .unwrap();
        let outcome = provider
            .put_if_not_exists("claim-key", Bytes::from_static(b"second"))
            .await
            .unwrap();
        assert_eq!(outcome, PutIfNotExistsOutcome::AlreadyExists);
        // Original value preserved — second call was a no-op.
        let stored = provider.get("claim-key").await.unwrap();
        assert_eq!(stored.as_ref(), b"first");
    }

    #[tokio::test]
    async fn test_in_memory_upload_download() {
        let provider = ObjectStoreProvider::in_memory();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"file-data").unwrap();

        provider.upload_file(tmp.path(), "copy").await.unwrap();

        let out = tempfile::NamedTempFile::new().unwrap();
        provider.download_file("copy", out.path()).await.unwrap();
        let back = std::fs::read(out.path()).unwrap();
        assert_eq!(back, b"file-data");
    }

    #[test]
    fn test_parse_s3_uri() {
        // We can't fully construct without AWS creds, but we can test the parse path.
        // Provide stub env creds so the AmazonS3Builder doesn't fail.
        // SAFETY: single-threaded test, env vars are only set within this test.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        let provider = ObjectStoreProvider::from_uri("s3://my-bucket/some/prefix").unwrap();
        assert_eq!(provider.scheme(), "s3");
        assert_eq!(provider.bucket(), "my-bucket");
        assert_eq!(provider.prefix, "some/prefix");
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let err = ObjectStoreProvider::from_uri("ftp://bucket/path").unwrap_err();
        assert!(matches!(err, ObjectStoreError::UnsupportedScheme(_)));
    }

    #[test]
    fn test_parse_invalid_uri() {
        let err = ObjectStoreProvider::from_uri("not a uri").unwrap_err();
        assert!(matches!(err, ObjectStoreError::InvalidUri(_, _)));
    }
}
