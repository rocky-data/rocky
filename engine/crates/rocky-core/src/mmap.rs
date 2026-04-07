//! Memory-mapped file I/O for efficient project loading.
//!
//! At 50k+ models, project loading reads 50k+ files (SQL, TOML, contracts).
//! Memory mapping lets the OS handle I/O scheduling and page faults,
//! reducing syscall overhead on Linux containers.
//!
//! # Safety
//!
//! `memmap2::Mmap` is `unsafe` because another process could modify the
//! file while it's mapped. We accept this risk for read-only project
//! loading because:
//! 1. Project files are not modified during compilation.
//! 2. The worst case is a garbled read that fails parsing (not UB).
//! 3. The performance benefit at scale (50k+ files) justifies the trade-off.

use std::fs::File;
use std::io;
use std::path::Path;

/// Minimum file size (in bytes) to use mmap. Below this threshold,
/// a regular `fs::read_to_string` is faster due to mmap setup overhead.
const MMAP_THRESHOLD: u64 = 4096; // 4 KB

/// Read a file's contents as a string, using mmap for large files.
///
/// Falls back to `std::fs::read_to_string` for small files (< 4 KB)
/// where the mmap syscall overhead exceeds the I/O benefit.
pub fn read_file_smart(path: &Path) -> io::Result<String> {
    let metadata = std::fs::metadata(path)?;
    let size = metadata.len();

    if size < MMAP_THRESHOLD {
        // Small file — regular read is faster
        return std::fs::read_to_string(path);
    }

    // Large file — use mmap
    let file = File::open(path)?;
    // SAFETY: We only read project files that are not being modified
    // concurrently during compilation. Worst case: garbled read → parse error.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };

    std::str::from_utf8(&mmap)
        .map(|s| s.to_string())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read multiple files in bulk, using mmap for large files.
///
/// Returns results in the same order as the input paths.
/// Errors are per-file — one failed read doesn't abort the batch.
pub fn read_files_bulk(paths: &[&Path]) -> Vec<io::Result<String>> {
    paths.iter().map(|p| read_file_smart(p)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small.sql");
        std::fs::write(&path, "SELECT 1").unwrap();

        let content = read_file_smart(&path).unwrap();
        assert_eq!(content, "SELECT 1");
    }

    #[test]
    fn test_read_large_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.sql");
        let large_content = "SELECT 1\n".repeat(1000); // ~9KB, above threshold
        std::fs::write(&path, &large_content).unwrap();

        let content = read_file_smart(&path).unwrap();
        assert_eq!(content, large_content);
    }

    #[test]
    fn test_read_missing_file() {
        let result = read_file_smart(Path::new("/nonexistent/file.sql"));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_files_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.sql");
        let p2 = dir.path().join("b.sql");
        std::fs::write(&p1, "SELECT a").unwrap();
        std::fs::write(&p2, "SELECT b").unwrap();

        let results = read_files_bulk(&[p1.as_path(), p2.as_path()]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap(), "SELECT a");
        assert_eq!(results[1].as_ref().unwrap(), "SELECT b");
    }
}
