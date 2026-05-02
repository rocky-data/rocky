//! Column-level lineage primitives.
//!
//! `QualifiedColumn` and `LineageEdge` describe a single column-level edge
//! across model boundaries. The cross-DAG graph that strings these edges
//! together (`SemanticGraph`, `ModelSchema`, builder) lives in
//! `rocky-compiler::semantic` — only the leaf primitives live here so that
//! `rocky-core::ir` can reference them without inducing a circular dependency
//! (rocky-compiler depends on rocky-core, one-way). `rocky-compiler::semantic`
//! re-exports `QualifiedColumn` and `LineageEdge` so existing call sites
//! continue to compile unchanged.
//!
//! `TransformKind` (the per-edge transform classifier) lives in `rocky-sql`,
//! which `rocky-core` already depends on.

use std::sync::Arc;

use rocky_sql::lineage::TransformKind;
use serde::{Deserialize, Serialize};

/// A column fully qualified by its model (or source) name.
///
/// Uses `Arc<str>` for cheap cloning — cloning a `QualifiedColumn` is an
/// atomic reference-count increment instead of two heap allocations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedColumn {
    /// Model name or source name.
    pub model: Arc<str>,
    /// Column name.
    pub column: Arc<str>,
}

/// An edge in the semantic graph connecting columns across models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Upstream column (source).
    pub source: QualifiedColumn,
    /// Downstream column (target).
    pub target: QualifiedColumn,
    /// How the column is transformed.
    pub transform: TransformKind,
}
