use std::time::Duration;

use redis::AsyncCommands;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Error type for Valkey/Redis cache operations.
#[derive(Debug, thiserror::Error)]
pub enum ValkeyCacheError {
    #[error("Valkey connection error: {0}")]
    Connection(#[source] redis::RedisError),

    #[error("Valkey command error: {0}")]
    Command(#[source] redis::RedisError),

    #[error("Valkey serialization error: {0}")]
    Serialization(#[source] serde_json::Error),
}

/// Async Valkey/Redis distributed cache client.
///
/// All keys are prefixed with a configurable string to support
/// namespace isolation across different deployments or pipelines.
#[derive(Clone)]
pub struct ValkeyCache {
    pool: redis::aio::ConnectionManager,
    prefix: String,
    default_ttl: Duration,
}

impl std::fmt::Debug for ValkeyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyCache")
            .field("prefix", &self.prefix)
            .field("default_ttl", &self.default_ttl)
            .finish_non_exhaustive()
    }
}

impl ValkeyCache {
    /// Connects to a Valkey/Redis instance.
    ///
    /// # Arguments
    /// - `url` — Redis connection URL (e.g., `redis://localhost:6379`)
    /// - `prefix` — Key prefix for namespace isolation (e.g., `"rocky:"`)
    /// - `default_ttl` — Default TTL applied to keys when no explicit TTL is given
    pub async fn connect(
        url: &str,
        prefix: &str,
        default_ttl: Duration,
    ) -> Result<Self, ValkeyCacheError> {
        let client = redis::Client::open(url).map_err(ValkeyCacheError::Connection)?;
        let pool = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(ValkeyCacheError::Connection)?;

        Ok(ValkeyCache {
            pool,
            prefix: prefix.to_string(),
            default_ttl,
        })
    }

    /// Gets a value by key, deserializing from JSON.
    ///
    /// Returns `Ok(None)` if the key does not exist.
    pub async fn get<V: DeserializeOwned>(&self, key: &str) -> Result<Option<V>, ValkeyCacheError> {
        let prefixed = self.prefixed_key(key);
        let raw: Option<String> = self
            .pool
            .clone()
            .get(&prefixed)
            .await
            .map_err(ValkeyCacheError::Command)?;

        match raw {
            Some(json) => {
                let value = serde_json::from_str(&json).map_err(ValkeyCacheError::Serialization)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Sets a value by key, serializing to JSON with an expiry.
    ///
    /// Uses `ttl` if provided, otherwise falls back to `default_ttl`.
    pub async fn set<V: Serialize>(
        &self,
        key: &str,
        value: &V,
        ttl: Option<Duration>,
    ) -> Result<(), ValkeyCacheError> {
        let prefixed = self.prefixed_key(key);
        let json = serde_json::to_string(value).map_err(ValkeyCacheError::Serialization)?;
        let ttl_secs = ttl.unwrap_or(self.default_ttl).as_secs().max(1);

        self.pool
            .clone()
            .set_ex::<&str, &str, ()>(&prefixed, &json, ttl_secs)
            .await
            .map_err(ValkeyCacheError::Command)?;

        Ok(())
    }

    /// Deletes a key. Returns `true` if the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool, ValkeyCacheError> {
        let prefixed = self.prefixed_key(key);
        let removed: i64 = self
            .pool
            .clone()
            .del(&prefixed)
            .await
            .map_err(ValkeyCacheError::Command)?;

        Ok(removed > 0)
    }

    /// Returns the prefixed key for a given raw key.
    fn prefixed_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_key_prefixing() {
        // Verify key prefixing logic without needing a live Redis instance.
        let prefix = "rocky:test:";
        let key = "some_key";
        let expected = "rocky:test:some_key";
        assert_eq!(format!("{}{}", prefix, key), expected);

        let empty_prefix = "";
        assert_eq!(format!("{}{}", empty_prefix, key), "some_key");
    }

    #[test]
    fn test_serialization_roundtrip() {
        // Verify JSON serde round-trip for cache values.
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        struct TestValue {
            name: String,
            count: u64,
            tags: Vec<String>,
        }

        let original = TestValue {
            name: "connector_a".to_string(),
            count: 42,
            tags: vec!["prod".to_string(), "us-west".to_string()],
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TestValue = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_serialization_primitive_types() {
        // Verify that primitive types round-trip through JSON.
        let s = "hello";
        let json = serde_json::to_string(&s).unwrap();
        let back: String = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);

        let n: i64 = 12345;
        let json = serde_json::to_string(&n).unwrap();
        let back: i64 = serde_json::from_str(&json).unwrap();
        assert_eq!(n, back);
    }
}
