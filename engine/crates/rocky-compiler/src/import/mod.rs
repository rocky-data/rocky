//! Import from external project formats (dbt, etc.).

pub mod dbt;
pub mod dbt_macros;
pub mod dbt_manifest;
pub mod dbt_profiles;
pub mod dbt_project;
pub mod dbt_sources;
pub mod dbt_tests;
pub mod emit;
pub mod report;

/// Maximum directory-recursion depth for any importer tree walk.
///
/// `rocky import-dbt` ingests an untrusted third-party project, so the walkers
/// must not be able to recurse unboundedly. Combined with the symlink skip in
/// [`is_traversable_subdir`] this bounds a maliciously-deep or symlink-cyclic
/// tree. A real dbt project nests only a handful of levels; this cap is far
/// above any legitimate layout.
pub(crate) const MAX_IMPORT_RECURSION_DEPTH: usize = 64;

/// True when `entry` is a directory the importer should descend into.
///
/// Uses [`std::fs::DirEntry::file_type`], which (unlike `Path::is_dir`) does
/// **not** follow symlinks, then explicitly rejects symlinks. This prevents a
/// directory symlink cycle (e.g. `models/loop -> ..`) from driving an importer
/// walk into unbounded recursion / stack overflow. On any I/O error reading
/// the file type the entry is treated as non-traversable.
pub(crate) fn is_traversable_subdir(entry: &std::fs::DirEntry) -> bool {
    match entry.file_type() {
        Ok(ft) => ft.is_dir() && !ft.is_symlink(),
        Err(_) => false,
    }
}
