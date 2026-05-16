//! Typed IR for `rocky compact` plans (Phase C — "SQL as `.o` files").
//!
//! `CompactPlanIr` captures the structured inputs that determine the
//! OPTIMIZE/VACUUM SQL emitted by `rocky compact`. It is the data-only
//! shape consumed by the `rocky_core::sql_gen::compact_from_ir` helper,
//! which regenerates the same SQL strings deterministically at apply time.
//!
//! ## Why this lives in `rocky-ir`
//!
//! Per `engine/CLAUDE.md` `rocky-ir` is the data-only leaf crate every
//! consumer can depend on without pulling in runtime traits (state store,
//! tokio, adapter wiring). The `compact` IR follows the same convention
//! as the rest of the IR vocabulary so the compiler, the CLI, and the
//! eventual `apply` regeneration path all share one shape.
//!
//! ## Status (Cluster 3 C — C-2)
//!
//! C-2 is **additive**: this type is introduced alongside the existing
//! `CompactOutput.statements[].sql` emission path. Persistence is not
//! flipped yet (that is a later PR). The regeneration helper plus an
//! equivalence test prove the IR can produce byte-identical SQL today.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Typed IR for a single-table compaction plan.
///
/// Captures the structured inputs the OPTIMIZE/VACUUM SQL is a function of:
/// the target table, optional file-size target, optional retention window,
/// and the partition / Z-ORDER columns. Today's emission path ignores the
/// partition / Z-ORDER vectors (they are empty in every CLI construction
/// site); they are modeled now so future enrichment does not break the
/// persisted shape.
///
/// Catalog-scoped compact plans (`rocky compact --catalog <name>`) are
/// represented as a `Vec<CompactPlanIr>` (or a `BTreeMap<Fqn,
/// CompactPlanIr>`) at a higher level — this struct is per-table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompactPlanIr {
    /// Fully-qualified target table identifier (e.g. `catalog.schema.orders`).
    /// Each dot-separated segment must validate as a SQL identifier before
    /// the SQL generator interpolates it.
    pub target_table: String,

    /// Target file size in megabytes for the OPTIMIZE step. `None` falls
    /// back to the engine default (256 MB), matching today's CLI default
    /// when `--target-size` is omitted.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target_size_mb: Option<u64>,

    /// VACUUM retention window in hours. `None` skips the VACUUM step
    /// entirely; today's CLI always populates `Some(168)`.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vacuum_retention_hours: Option<u64>,

    /// Partition columns the table is partitioned by. Empty today;
    /// populated only when future enrichment surfaces partition metadata.
    /// Emission ignores this vec when empty (no `PARTITIONED BY` clause).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub partition_columns: Vec<String>,

    /// Z-ORDER columns for Delta Lake OPTIMIZE. Empty today; populated
    /// only when future enrichment surfaces clustering metadata.
    /// Emission ignores this vec when empty (no `ZORDER BY` clause).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub zorder_columns: Vec<String>,
}

impl CompactPlanIr {
    /// Construct a minimal IR for the canonical today's-emission inputs
    /// (single table, default 168h VACUUM retention, no zorder / partition).
    /// Mirrors the CLI's `run_compact` / `run_compact_catalog` defaults.
    pub fn for_table(target_table: impl Into<String>, target_size_mb: u64) -> Self {
        Self {
            target_table: target_table.into(),
            target_size_mb: Some(target_size_mb),
            vacuum_retention_hours: Some(168),
            partition_columns: Vec::new(),
            zorder_columns: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_table_populates_defaults() {
        let ir = CompactPlanIr::for_table("catalog.schema.orders", 256);
        assert_eq!(ir.target_table, "catalog.schema.orders");
        assert_eq!(ir.target_size_mb, Some(256));
        assert_eq!(ir.vacuum_retention_hours, Some(168));
        assert!(ir.partition_columns.is_empty());
        assert!(ir.zorder_columns.is_empty());
    }

    #[test]
    fn skips_empty_optional_fields_on_serialize() {
        let ir = CompactPlanIr::for_table("catalog.schema.orders", 256);
        let json = serde_json::to_string(&ir).unwrap();
        // Empty vecs and None options should not surface in the canonical
        // JSON form, matching the `rocky-ir` canonical-JSON convention.
        assert!(!json.contains("partition_columns"));
        assert!(!json.contains("zorder_columns"));
        assert!(json.contains("target_table"));
        assert!(json.contains("target_size_mb"));
        assert!(json.contains("vacuum_retention_hours"));
    }
}
