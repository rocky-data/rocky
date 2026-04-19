//! Shared helpers for building case-insensitive column lookups.
//!
//! Used by drift detection, contract validation, and column-match checks
//! to avoid duplicating the lowercase-key map/set construction.

use std::borrow::{Borrow, Cow};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::ir::ColumnInfo;

// ---------------------------------------------------------------------------
// Case-insensitive key (§P1.9)
// ---------------------------------------------------------------------------
//
// `build_column_map` used to allocate a fresh lowercased `String` per column
// for each map entry *and* each lookup — on a 100-table × 50-column run that
// churns ~10 000 Strings through the allocator. `CiKey<'a>` wraps a
// `Cow<'a, str>` so map entries borrow directly from the source column names
// (no allocation). `CiStr` is the unsized borrow target that lets
// `HashMap::get(CiStr::new(name))` work without allocating either.

/// Borrow target for case-insensitive string lookup. Transparent wrapper
/// over `str` with ASCII-case-insensitive `Hash` / `Eq`.
#[repr(transparent)]
#[derive(Debug)]
pub struct CiStr(str);

impl CiStr {
    /// View a `&str` as a `&CiStr` without allocation.
    pub fn new(s: &str) -> &CiStr {
        // SAFETY: `CiStr` is `#[repr(transparent)]` over `str`, so the layout
        // is identical. The cast is equivalent to `Path::new(&str) -> &Path`
        // in std.
        unsafe { &*(s as *const str as *const CiStr) }
    }
}

impl Hash for CiStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash each byte lowercased so `"Foo"` and `"foo"` collide.
        // Finalised with a length prefix equivalent to str's own hash impl
        // so no cross-collision with non-CiStr strings in the same hasher.
        for b in self.0.as_bytes() {
            b.to_ascii_lowercase().hash(state);
        }
    }
}

impl PartialEq for CiStr {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for CiStr {}

/// Owned / borrowed case-insensitive key for storing in a `HashMap`.
#[derive(Debug, Clone)]
pub struct CiKey<'a>(Cow<'a, str>);

impl<'a> CiKey<'a> {
    /// Wrap a `&str` without allocating.
    pub fn borrowed(s: &'a str) -> Self {
        Self(Cow::Borrowed(s))
    }

    /// Wrap an owned `String`.
    pub fn owned(s: String) -> Self {
        Self(Cow::Owned(s))
    }

    /// The underlying string in its original case.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> Borrow<CiStr> for CiKey<'a> {
    fn borrow(&self) -> &CiStr {
        CiStr::new(&self.0)
    }
}

impl<'a> Hash for CiKey<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        CiStr::new(&self.0).hash(state)
    }
}

impl<'a> PartialEq for CiKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        CiStr::new(&self.0) == CiStr::new(&other.0)
    }
}

impl<'a> Eq for CiKey<'a> {}

/// Build a case-insensitive `name → &ColumnInfo` map. Keys borrow from the
/// column names; no per-column `String` allocation.
pub fn build_column_map(columns: &[ColumnInfo]) -> HashMap<CiKey<'_>, &ColumnInfo> {
    columns
        .iter()
        .map(|c| (CiKey::borrowed(&c.name), c))
        .collect()
}

/// Build a case-insensitive set of column names, excluding any in `exclude`.
///
/// `exclude` is a pre-built lowercase `HashSet` — callers that invoke this
/// for both source and target columns (e.g. `check_column_match`) should
/// hoist the set construction so the lowercase pass happens once per
/// exclude list, not once per call (§P4.1). Use
/// [`build_exclude_set`] to construct one from a `&[String]`.
pub fn build_column_name_set(columns: &[ColumnInfo], exclude: &HashSet<String>) -> HashSet<String> {
    columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .filter(|n| !exclude.contains(n))
        .collect()
}

/// Build the lowercase-set representation of an exclude list.
///
/// Helper so callers that pass the same exclude list into multiple
/// [`build_column_name_set`] invocations pay the lowercase pass once.
pub fn build_exclude_set(exclude: &[String]) -> HashSet<String> {
    exclude.iter().map(|s| s.to_lowercase()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: "VARCHAR".to_string(),
            nullable: true,
        }
    }

    #[test]
    fn ci_str_hashes_regardless_of_case() {
        use std::collections::hash_map::DefaultHasher;
        let mut a = DefaultHasher::new();
        let mut b = DefaultHasher::new();
        CiStr::new("Foo").hash(&mut a);
        CiStr::new("FOO").hash(&mut b);
        assert_eq!(a.finish(), b.finish());
    }

    #[test]
    fn ci_str_eq_is_case_insensitive() {
        assert_eq!(CiStr::new("Foo"), CiStr::new("foo"));
        assert_eq!(CiStr::new("FOO"), CiStr::new("foO"));
        assert_ne!(CiStr::new("Foo"), CiStr::new("Bar"));
    }

    #[test]
    fn ci_key_preserves_original_casing() {
        let k = CiKey::borrowed("Foo");
        assert_eq!(k.as_str(), "Foo");
    }

    #[test]
    fn column_map_lookup_ignores_case_without_allocation() {
        let cols = vec![col("CustomerID"), col("created_at"), col("Status")];
        let map = build_column_map(&cols);
        // Lookups with any casing resolve to the same entry.
        assert!(map.contains_key(CiStr::new("customerid")));
        assert!(map.contains_key(CiStr::new("CUSTOMERID")));
        assert!(map.contains_key(CiStr::new("created_at")));
        assert!(map.contains_key(CiStr::new("CREATED_AT")));
        assert!(!map.contains_key(CiStr::new("missing")));
    }

    #[test]
    fn column_map_retains_borrowed_column_info() {
        let cols = vec![col("a"), col("B")];
        let map = build_column_map(&cols);
        let found = map.get(CiStr::new("A")).unwrap();
        // The returned reference must point back at the original slice.
        assert!(std::ptr::eq(*found, &cols[0]));
    }
}
