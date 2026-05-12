//! Column-masking strategy enum.
//!
//! Lives in `rocky-ir` because [`MaskStrategy`] is referenced by
//! [`crate::ir::ColumnMask`]. The richer `MaskingPolicy` /
//! `MaskingPolicyResolver` types stay in `rocky-core::traits` — only the
//! pure value enum lives here so that adding a new strategy variant does
//! not require updating consumers that only need to read the IR.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How a column is masked at apply time.
///
/// Serialized in lowercase to match the TOML spelling (`"hash"`, `"redact"`,
/// `"partial"`, `"none"`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum MaskStrategy {
    /// SHA-256 hex digest of the column value. Deterministic, one-way.
    Hash,
    /// Replace the column value with the literal string `'***'`.
    Redact,
    /// Keep the first and last two characters; replace the middle with `***`.
    /// Short values (<5 chars) are fully replaced with `'***'`.
    Partial,
    /// Explicit identity — no masking applied. Useful as a per-env override
    /// to "unmask" a column that defaults to masked at the workspace level.
    None,
}

impl MaskStrategy {
    /// Wire name used in `rocky.toml` and JSON schemas.
    pub fn as_str(self) -> &'static str {
        match self {
            MaskStrategy::Hash => "hash",
            MaskStrategy::Redact => "redact",
            MaskStrategy::Partial => "partial",
            MaskStrategy::None => "none",
        }
    }
}

impl std::fmt::Display for MaskStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
