//! Compile cache for incremental compilation.
//!
//! Hashes source files (`.sql`, `.rocky`, `.toml` sidecars) and stores
//! the fingerprints. On recompile, unchanged files can be skipped.
//!
//! The cache is stored alongside the redb state store as a simple
//! JSON file (`rocky_compile_cache.json`) — no extra dependencies.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// A hash of a source file's contents.
type FileHash = u64;

/// Cache entry for a single model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Hash of the model's SQL/Rocky source file.
    pub source_hash: FileHash,
    /// Hash of the model's sidecar TOML (if present).
    pub sidecar_hash: Option<FileHash>,
    /// Hash of the model's contract file (if present).
    pub contract_hash: Option<FileHash>,
    /// Timestamp of last successful compilation.
    pub compiled_at: u64,
}

/// Persistent compile cache.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompileCache {
    /// Version — bump when the cache format changes.
    pub version: u32,
    /// Per-model cache entries, keyed by model name.
    pub entries: HashMap<String, CacheEntry>,
    /// Hash of the _defaults.toml file (invalidates all models in the dir if changed).
    pub defaults_hash: Option<FileHash>,
}

impl CompileCache {
    /// Current cache format version.
    const VERSION: u32 = 1;

    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            version: Self::VERSION,
            entries: HashMap::new(),
            defaults_hash: None,
        }
    }

    /// Load cache from disk, returning a fresh cache if missing or invalid.
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                if let Ok(cache) = serde_json::from_str::<CompileCache>(&content) {
                    if cache.version == Self::VERSION {
                        return cache;
                    }
                }
                // Version mismatch or corrupt — start fresh
                Self::new()
            }
            Err(_) => Self::new(),
        }
    }

    /// Save cache to disk.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let content = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, content)
    }

    /// Hash a file's contents using a fast, non-cryptographic hash (FNV-1a).
    pub fn hash_file(path: &Path) -> std::io::Result<FileHash> {
        let content = std::fs::read(path)?;
        Ok(Self::hash_bytes(&content))
    }

    /// Hash a byte slice using FNV-1a (fast, non-cryptographic).
    fn hash_bytes(data: &[u8]) -> FileHash {
        // FNV-1a 64-bit
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in data {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Check if a model needs recompilation.
    ///
    /// Returns `true` if the model should be recompiled (cache miss or
    /// content changed). Returns `false` if the cached entry matches.
    pub fn needs_recompile(
        &self,
        model_name: &str,
        source_path: &Path,
        sidecar_path: Option<&Path>,
        contract_path: Option<&Path>,
    ) -> bool {
        let entry = match self.entries.get(model_name) {
            Some(e) => e,
            None => return true, // cache miss
        };

        // Check source file
        match Self::hash_file(source_path) {
            Ok(h) if h == entry.source_hash => {}
            _ => return true,
        }

        // Check sidecar
        match (sidecar_path, entry.sidecar_hash) {
            (Some(path), Some(cached)) => match Self::hash_file(path) {
                Ok(h) if h == cached => {}
                _ => return true,
            },
            (None, None) => {}
            _ => return true, // sidecar added or removed
        }

        // Check contract
        match (contract_path, entry.contract_hash) {
            (Some(path), Some(cached)) => match Self::hash_file(path) {
                Ok(h) if h == cached => {}
                _ => return true,
            },
            (None, None) => {}
            _ => return true, // contract added or removed
        }

        false // all hashes match
    }

    /// Update the cache entry for a model after successful compilation.
    pub fn update(
        &mut self,
        model_name: &str,
        source_path: &Path,
        sidecar_path: Option<&Path>,
        contract_path: Option<&Path>,
    ) {
        let source_hash = Self::hash_file(source_path).unwrap_or(0);
        let sidecar_hash = sidecar_path.and_then(|p| Self::hash_file(p).ok());
        let contract_hash = contract_path.and_then(|p| Self::hash_file(p).ok());

        self.entries.insert(
            model_name.to_string(),
            CacheEntry {
                source_hash,
                sidecar_hash,
                contract_hash,
                compiled_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            },
        );
    }

    /// Remove stale entries for models that no longer exist.
    pub fn prune(&mut self, current_models: &[&str]) {
        let current: std::collections::HashSet<&str> = current_models.iter().copied().collect();
        self.entries.retain(|k, _| current.contains(k.as_str()));
    }

    /// Returns the default cache file path next to the state directory.
    pub fn default_path(project_dir: &Path) -> PathBuf {
        project_dir.join(".rocky").join("compile_cache.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        let h1 = CompileCache::hash_bytes(b"hello world");
        let h2 = CompileCache::hash_bytes(b"hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_different_content() {
        let h1 = CompileCache::hash_bytes(b"hello");
        let h2 = CompileCache::hash_bytes(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.json");

        let mut cache = CompileCache::new();
        cache.entries.insert(
            "my_model".into(),
            CacheEntry {
                source_hash: 12345,
                sidecar_hash: Some(67890),
                contract_hash: None,
                compiled_at: 1000,
            },
        );
        cache.save(&path).unwrap();

        let loaded = CompileCache::load(&path);
        assert_eq!(loaded.entries.len(), 1);
        assert_eq!(loaded.entries["my_model"].source_hash, 12345);
    }

    #[test]
    fn test_needs_recompile_cache_miss() {
        let cache = CompileCache::new();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("model.sql");
        std::fs::write(&src, "SELECT 1").unwrap();
        assert!(cache.needs_recompile("model", &src, None, None));
    }

    #[test]
    fn test_needs_recompile_cache_hit() {
        let mut cache = CompileCache::new();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("model.sql");
        std::fs::write(&src, "SELECT 1").unwrap();

        cache.update("model", &src, None, None);
        assert!(!cache.needs_recompile("model", &src, None, None));
    }

    #[test]
    fn test_needs_recompile_content_changed() {
        let mut cache = CompileCache::new();
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("model.sql");
        std::fs::write(&src, "SELECT 1").unwrap();

        cache.update("model", &src, None, None);

        // Change the file
        std::fs::write(&src, "SELECT 2").unwrap();
        assert!(cache.needs_recompile("model", &src, None, None));
    }

    #[test]
    fn test_prune_removes_stale() {
        let mut cache = CompileCache::new();
        cache.entries.insert(
            "active".into(),
            CacheEntry {
                source_hash: 1,
                sidecar_hash: None,
                contract_hash: None,
                compiled_at: 0,
            },
        );
        cache.entries.insert(
            "deleted".into(),
            CacheEntry {
                source_hash: 2,
                sidecar_hash: None,
                contract_hash: None,
                compiled_at: 0,
            },
        );

        cache.prune(&["active"]);
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key("active"));
    }
}
