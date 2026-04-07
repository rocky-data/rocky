use std::future::Future;
use std::hash::Hash;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::memory::MemoryCache;

/// Three-tier cache: memory → Valkey → source.
///
/// When the `valkey` feature is enabled and a `ValkeyCache` is provided,
/// lookups check memory first, then Valkey, then the source function.
/// Without the `valkey` feature, it falls back to memory → source only.
pub struct TieredCache<K, V> {
    memory: Mutex<MemoryCache<K, V>>,
    #[cfg(feature = "valkey")]
    valkey: Option<crate::valkey::ValkeyCache>,
}

impl<K: Eq + Hash + Clone + Send, V: Clone + Send> TieredCache<K, V> {
    /// Creates a new `TieredCache` with only the in-memory tier.
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        TieredCache {
            memory: Mutex::new(MemoryCache::new(ttl, max_entries)),
            #[cfg(feature = "valkey")]
            valkey: None,
        }
    }

    /// Gets a value from the cache, or fetches it from the source function.
    ///
    /// Lookup order: memory → source.
    /// When the `valkey` feature is enabled, use `get_or_fetch_with_valkey` instead
    /// to include the Valkey tier.
    pub async fn get_or_fetch<F, Fut, E>(&self, key: K, fetch: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        // Check memory
        {
            let mut mem = self.memory.lock().await;
            if let Some(value) = mem.get(&key) {
                tracing::debug!("tiered cache: memory hit");
                return Ok(value);
            }
        }

        // Fetch from source
        let value = fetch().await?;

        // Store in memory
        {
            let mut mem = self.memory.lock().await;
            mem.insert(key, value.clone());
        }

        Ok(value)
    }

    /// Invalidates a key from all cache tiers.
    pub async fn invalidate(&self, key: &K) {
        let mut mem = self.memory.lock().await;
        mem.remove(key);
    }

    /// Clears all cache tiers.
    pub async fn clear(&self) {
        let mut mem = self.memory.lock().await;
        mem.clear();
    }
}

// --- Valkey-enabled extension methods ---

#[cfg(feature = "valkey")]
impl<K: Eq + Hash + Clone + Send, V: Clone + Send> TieredCache<K, V> {
    /// Creates a new `TieredCache` with both memory and Valkey tiers.
    pub fn with_valkey(
        ttl: Duration,
        max_entries: usize,
        valkey: crate::valkey::ValkeyCache,
    ) -> Self {
        TieredCache {
            memory: Mutex::new(MemoryCache::new(ttl, max_entries)),
            valkey: Some(valkey),
        }
    }

    /// Gets a value, checking memory → Valkey → source.
    ///
    /// Requires `V: Serialize + DeserializeOwned` for Valkey JSON serialization.
    /// Valkey errors are logged and treated as cache misses (graceful degradation).
    pub async fn get_or_fetch_with_valkey<F, Fut, E>(
        &self,
        key: K,
        valkey_key: &str,
        fetch: F,
    ) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        V: serde::Serialize + serde::de::DeserializeOwned,
    {
        // 1. Check memory
        {
            let mut mem = self.memory.lock().await;
            if let Some(value) = mem.get(&key) {
                tracing::debug!("tiered cache: memory hit");
                return Ok(value);
            }
        }

        // 2. Check Valkey (if configured)
        if let Some(ref valkey) = self.valkey {
            match valkey.get::<V>(valkey_key).await {
                Ok(Some(value)) => {
                    tracing::debug!("tiered cache: valkey hit");
                    // Populate memory
                    let mut mem = self.memory.lock().await;
                    mem.insert(key, value.clone());
                    return Ok(value);
                }
                Ok(None) => {
                    tracing::debug!("tiered cache: valkey miss");
                }
                Err(e) => {
                    tracing::warn!(
                        "tiered cache: valkey get failed, falling through to source: {e}"
                    );
                }
            }
        }

        // 3. Fetch from source
        let value = fetch().await?;

        // 4. Populate Valkey
        if let Some(ref valkey) = self.valkey {
            if let Err(e) = valkey.set(valkey_key, &value, None).await {
                tracing::warn!("tiered cache: valkey set failed: {e}");
            }
        }

        // 5. Populate memory
        {
            let mut mem = self.memory.lock().await;
            mem.insert(key, value.clone());
        }

        Ok(value)
    }

    /// Invalidates a key from all cache tiers, including Valkey.
    ///
    /// Valkey errors are logged but do not propagate.
    pub async fn invalidate_with_valkey(&self, key: &K, valkey_key: &str) {
        {
            let mut mem = self.memory.lock().await;
            mem.remove(key);
        }

        if let Some(ref valkey) = self.valkey {
            if let Err(e) = valkey.delete(valkey_key).await {
                tracing::warn!("tiered cache: valkey delete failed during invalidate: {e}");
            }
        }
    }

    /// Clears memory cache and optionally logs that Valkey should be cleared
    /// through its own management tools (FLUSHDB is too destructive for
    /// a shared Valkey instance).
    pub async fn clear_with_valkey(&self) {
        {
            let mut mem = self.memory.lock().await;
            mem.clear();
        }
        if self.valkey.is_some() {
            tracing::debug!(
                "tiered cache: memory tier cleared; valkey entries will expire via TTL"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test]
    async fn test_get_or_fetch_caches() {
        let cache: TieredCache<String, String> = TieredCache::new(Duration::from_secs(60), 100);

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();

        // First call — fetches
        let val = cache
            .get_or_fetch("key".into(), || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>("value".to_string())
                }
            })
            .await
            .unwrap();
        assert_eq!(val, "value");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);

        // Second call — cached
        let cc2 = call_count.clone();
        let val2 = cache
            .get_or_fetch("key".into(), || {
                let cc = cc2.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>("new_value".to_string())
                }
            })
            .await
            .unwrap();
        assert_eq!(val2, "value"); // still cached
        assert_eq!(call_count.load(Ordering::SeqCst), 1); // not called again
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache: TieredCache<String, String> = TieredCache::new(Duration::from_secs(60), 100);

        let call_count = Arc::new(AtomicU32::new(0));

        let cc = call_count.clone();
        cache
            .get_or_fetch("key".into(), || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>("v1".to_string())
                }
            })
            .await
            .unwrap();

        cache.invalidate(&"key".into()).await;

        let cc2 = call_count.clone();
        let val = cache
            .get_or_fetch("key".into(), || {
                let cc = cc2.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>("v2".to_string())
                }
            })
            .await
            .unwrap();

        assert_eq!(val, "v2");
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_clear() {
        let cache: TieredCache<String, i32> = TieredCache::new(Duration::from_secs(60), 100);

        cache
            .get_or_fetch("a".into(), || async { Ok::<_, String>(1) })
            .await
            .unwrap();
        cache
            .get_or_fetch("b".into(), || async { Ok::<_, String>(2) })
            .await
            .unwrap();

        cache.clear().await;

        // Both should need re-fetch
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        cache
            .get_or_fetch("a".into(), || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(10)
                }
            })
            .await
            .unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_fetch_error_not_cached() {
        let cache: TieredCache<String, String> = TieredCache::new(Duration::from_secs(60), 100);

        // First call fails
        let result: Result<String, String> = cache
            .get_or_fetch("key".into(), || async { Err("network error".to_string()) })
            .await;
        assert!(result.is_err());

        // Second call should still fetch (not cached)
        let val = cache
            .get_or_fetch("key".into(), || async {
                Ok::<_, String>("recovered".to_string())
            })
            .await
            .unwrap();
        assert_eq!(val, "recovered");
    }
}
