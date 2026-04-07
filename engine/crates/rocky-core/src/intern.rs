//! Lightweight string interning for repeated identifiers.
//!
//! Table names, column names, and schema names appear thousands of times
//! in a 50k-model DAG. Interning them reduces memory by storing each
//! unique string once and using cheap integer handles elsewhere.
//!
//! Usage:
//! ```
//! use rocky_core::intern::Interner;
//!
//! let interner = Interner::new();
//! let key1 = interner.intern("my_catalog.my_schema.my_table");
//! let key2 = interner.intern("my_catalog.my_schema.my_table");
//! assert_eq!(key1, key2); // same handle
//! assert_eq!(interner.resolve(key1), "my_catalog.my_schema.my_table");
//! ```

use std::num::NonZeroUsize;

use lasso::{RodeoReader, Spur, ThreadedRodeo};

/// A thread-safe string interner backed by `lasso::ThreadedRodeo`.
///
/// Suitable for concurrent use during parallel compilation phases.
#[derive(Debug)]
pub struct Interner {
    inner: ThreadedRodeo,
}

/// An interned string handle. Cheap to copy, compare, and hash.
pub type InternKey = Spur;

impl Interner {
    /// Create a new empty interner.
    pub fn new() -> Self {
        Self {
            inner: ThreadedRodeo::new(),
        }
    }

    /// Create an interner with pre-allocated capacity for `n` strings.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            inner: ThreadedRodeo::with_capacity(lasso::Capacity::new(
                n,
                NonZeroUsize::new(n * 32).unwrap_or(NonZeroUsize::new(1).unwrap()),
            )),
        }
    }

    /// Intern a string, returning a key. If the string was already interned,
    /// returns the existing key.
    pub fn intern(&self, s: &str) -> InternKey {
        self.inner.get_or_intern(s)
    }

    /// Resolve an interned key back to its string.
    pub fn resolve(&self, key: InternKey) -> &str {
        self.inner.resolve(&key)
    }

    /// Check if a string is already interned.
    pub fn contains(&self, s: &str) -> bool {
        self.inner.contains(s)
    }

    /// Returns the number of unique strings interned.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if no strings have been interned.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Convert to a read-only interner (cheaper lookups, no more inserts).
    pub fn into_reader(self) -> ReadOnlyInterner {
        ReadOnlyInterner {
            inner: self.inner.into_reader(),
        }
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}

/// A read-only interner — cheaper lookups, no insertion.
///
/// Converted from `Interner` after the interning phase is complete
/// (e.g., after project loading).
#[derive(Debug)]
pub struct ReadOnlyInterner {
    inner: RodeoReader,
}

impl ReadOnlyInterner {
    /// Resolve an interned key back to its string.
    pub fn resolve(&self, key: InternKey) -> &str {
        self.inner.resolve(&key)
    }

    /// Returns the number of unique strings interned.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if no strings have been interned.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_dedup() {
        let interner = Interner::new();
        let k1 = interner.intern("catalog.schema.table");
        let k2 = interner.intern("catalog.schema.table");
        assert_eq!(k1, k2);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn test_resolve() {
        let interner = Interner::new();
        let k = interner.intern("hello_world");
        assert_eq!(interner.resolve(k), "hello_world");
    }

    #[test]
    fn test_multiple_strings() {
        let interner = Interner::new();
        let k1 = interner.intern("a");
        let k2 = interner.intern("b");
        let k3 = interner.intern("a");
        assert_eq!(k1, k3);
        assert_ne!(k1, k2);
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_into_reader() {
        let interner = Interner::new();
        let k1 = interner.intern("test");
        let reader = interner.into_reader();
        assert_eq!(reader.resolve(k1), "test");
        assert_eq!(reader.len(), 1);
    }

    #[test]
    fn test_with_capacity() {
        let interner = Interner::with_capacity(1000);
        assert_eq!(interner.len(), 0);
    }
}
