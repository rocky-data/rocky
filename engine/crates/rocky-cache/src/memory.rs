use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// In-process LRU cache with per-entry TTL.
pub struct MemoryCache<K, V> {
    entries: HashMap<K, CacheEntry<V>>,
    ttl: Duration,
    max_entries: usize,
}

struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
    last_accessed: Instant,
}

impl<K: Eq + Hash + Clone, V: Clone> MemoryCache<K, V> {
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        MemoryCache {
            entries: HashMap::new(),
            ttl,
            max_entries,
        }
    }

    /// Gets a value if it exists and hasn't expired.
    pub fn get(&mut self, key: &K) -> Option<V> {
        let now = Instant::now();
        if let Some(entry) = self.entries.get_mut(key) {
            if now.duration_since(entry.inserted_at) < self.ttl {
                entry.last_accessed = now;
                return Some(entry.value.clone());
            }
            // Expired — remove
            self.entries.remove(key);
        }
        None
    }

    /// Inserts a value, evicting the least-recently-accessed entry if at capacity.
    pub fn insert(&mut self, key: K, value: V) {
        // Evict expired entries first
        self.evict_expired();

        // If still at capacity, evict LRU
        if self.entries.len() >= self.max_entries {
            self.evict_lru();
        }

        let now = Instant::now();
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted_at: now,
                last_accessed: now,
            },
        );
    }

    /// Removes a specific key.
    pub fn remove(&mut self, key: &K) -> bool {
        self.entries.remove(key).is_some()
    }

    /// Clears the entire cache.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns the number of entries (including potentially expired ones).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        let ttl = self.ttl;
        self.entries
            .retain(|_, entry| now.duration_since(entry.inserted_at) < ttl);
    }

    fn evict_lru(&mut self) {
        if let Some(lru_key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(key, _)| key.clone())
        {
            self.entries.remove(&lru_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut cache = MemoryCache::new(Duration::from_secs(60), 100);
        cache.insert("key1", "value1");
        assert_eq!(cache.get(&"key1"), Some("value1"));
    }

    #[test]
    fn test_miss() {
        let mut cache: MemoryCache<&str, &str> = MemoryCache::new(Duration::from_secs(60), 100);
        assert_eq!(cache.get(&"nonexistent"), None);
    }

    #[test]
    fn test_overwrite() {
        let mut cache = MemoryCache::new(Duration::from_secs(60), 100);
        cache.insert("key", "v1");
        cache.insert("key", "v2");
        assert_eq!(cache.get(&"key"), Some("v2"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut cache = MemoryCache::new(Duration::from_secs(60), 100);
        cache.insert("key", "value");
        assert!(cache.remove(&"key"));
        assert_eq!(cache.get(&"key"), None);
        assert!(!cache.remove(&"key"));
    }

    #[test]
    fn test_clear() {
        let mut cache = MemoryCache::new(Duration::from_secs(60), 100);
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_max_entries_evicts_lru() {
        let mut cache = MemoryCache::new(Duration::from_secs(60), 2);
        cache.insert("a", 1);
        cache.insert("b", 2);

        // Access "a" to make it more recently used
        cache.get(&"a");

        // Insert "c" — should evict "b" (LRU)
        cache.insert("c", 3);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&"a"), Some(1));
        assert_eq!(cache.get(&"b"), None); // evicted
        assert_eq!(cache.get(&"c"), Some(3));
    }

    #[test]
    fn test_ttl_expiry() {
        let mut cache = MemoryCache::new(Duration::from_millis(0), 100);
        cache.insert("key", "value");
        // TTL is 0ms — already expired
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(cache.get(&"key"), None);
    }
}
