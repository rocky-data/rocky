//! Typed IR for `rocky archive` plans (Phase C — "SQL as `.o` files").
//!
//! `ArchivePlanIr` captures the structured inputs that determine the
//! DELETE/VACUUM SQL emitted by `rocky archive`. It is the data-only
//! shape consumed by the `rocky_core::sql_gen::archive_from_ir` helper,
//! which regenerates the same SQL strings deterministically at apply
//! time.
//!
//! ## Why this lives in `rocky-ir`
//!
//! Same reasoning as [`crate::CompactPlanIr`]: `rocky-ir` is the
//! data-only leaf crate every consumer can depend on without pulling
//! runtime traits (state store, tokio, adapter wiring) in. The archive
//! IR follows the same convention as the rest of the IR vocabulary so
//! the compiler, the CLI, and the eventual `apply` regeneration path
//! all share one shape.
//!
//! ## Status (Cluster 3 C — C-3)
//!
//! C-3 is the mechanical mirror of C-2 against the archive command and
//! is **additive**: this type is introduced alongside the existing
//! `ArchiveOutput.statements[].sql` emission path. Persistence is not
//! flipped yet (that is a later PR — C-5). The regeneration helper
//! plus an equivalence test prove the IR can produce byte-identical
//! SQL today.
//!
//! ## Cutoff handling (option `a` per the C-3 dispatch)
//!
//! Today's `generate_archive_sql` emits a `DATEADD(DAY, -N,
//! CURRENT_TIMESTAMP())` filter — *runtime* date math, not a
//! plan-time cutoff literal. To preserve byte-equivalence with the
//! current emission path, [`ArchivePlanIr`] mirrors that shape: it
//! carries `older_than: String` (preserved for audit) and
//! `older_than_days: u64` (the parsed integer the DATEADD literal is
//! built from). Plan-time cutoff resolution (a `cutoff_resolved_at:
//! DateTime<Utc>` field with literal-timestamp emission) is deferred
//! to the v2 persisted-plan format in C-5 where the semantic switch
//! lands alongside the persistence flip.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Typed IR for a single-table archive plan.
///
/// Captures the structured inputs the DELETE/VACUUM SQL is a function
/// of: the target table (optional — `None` reproduces today's
/// wildcard `*` emission path), the parsed retention window, the
/// watermark column the DELETE filters on, and the VACUUM retention
/// window. Today's CLI always populates `partition_column =
/// "_fivetran_synced"` and `vacuum_retention_hours = Some(0)`; the
/// fields are modeled now so future enrichment (per-model watermark
/// column overrides, retention-window tuning) does not break the
/// persisted shape.
///
/// Catalog-scoped archive plans (`rocky archive --catalog <name>`)
/// are represented as a `Vec<ArchivePlanIr>` (or a `BTreeMap<Fqn,
/// ArchivePlanIr>`) at a higher level — this struct is per-table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ArchivePlanIr {
    /// Fully-qualified target table identifier (e.g.
    /// `catalog.schema.events`). Each dot-separated segment must
    /// validate as a SQL identifier before the SQL generator
    /// interpolates it. `None` reproduces today's degenerate
    /// `DELETE FROM *` emission path that the inline CLI reaches when
    /// the user supplies neither `--model` nor `--catalog`; modeling
    /// the case keeps the regeneration helper byte-equivalent.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target_table: Option<String>,

    /// Raw `--older-than` argument as the operator typed it
    /// (`"90d"`, `"6m"`, `"1y"`). Preserved for audit / display; the
    /// SQL generator uses [`Self::older_than_days`] for the literal.
    pub older_than: String,

    /// Parsed `--older-than` value in days. Drives the DATEADD
    /// literal in the DELETE filter
    /// (`DATEADD(DAY, -N, CURRENT_TIMESTAMP())`).
    pub older_than_days: u64,

    /// Watermark column the DELETE filters on. Today's emission path
    /// hardcodes `_fivetran_synced`; the field exists so future
    /// per-model overrides do not break the persisted shape.
    pub partition_column: String,

    /// VACUUM retention window in hours. `None` skips the VACUUM
    /// step entirely; today's CLI always populates `Some(0)` (immediate
    /// reclaim once DELETE is done).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub vacuum_retention_hours: Option<u64>,
}

impl ArchivePlanIr {
    /// Construct a minimal IR for the canonical today's-emission
    /// inputs (named table, `_fivetran_synced` watermark, immediate
    /// VACUUM). Mirrors the CLI's `run_archive` / `run_archive_catalog`
    /// defaults.
    pub fn for_table(target_table: impl Into<String>, older_than_days: u64) -> Self {
        Self {
            target_table: Some(target_table.into()),
            older_than: format!("{older_than_days}d"),
            older_than_days,
            partition_column: "_fivetran_synced".to_string(),
            vacuum_retention_hours: Some(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_table_populates_defaults() {
        let ir = ArchivePlanIr::for_table("catalog.schema.events", 90);
        assert_eq!(ir.target_table.as_deref(), Some("catalog.schema.events"));
        assert_eq!(ir.older_than, "90d");
        assert_eq!(ir.older_than_days, 90);
        assert_eq!(ir.partition_column, "_fivetran_synced");
        assert_eq!(ir.vacuum_retention_hours, Some(0));
    }

    #[test]
    fn skips_empty_optional_fields_on_serialize() {
        // `target_table = None` reproduces today's wildcard emission;
        // it must be absent from the canonical JSON form to keep the
        // serialised plan byte-stable.
        let ir = ArchivePlanIr {
            target_table: None,
            older_than: "30d".to_string(),
            older_than_days: 30,
            partition_column: "_fivetran_synced".to_string(),
            vacuum_retention_hours: None,
        };
        let json = serde_json::to_string(&ir).unwrap();
        assert!(!json.contains("target_table"));
        assert!(!json.contains("vacuum_retention_hours"));
        assert!(json.contains("older_than"));
        assert!(json.contains("older_than_days"));
        assert!(json.contains("partition_column"));
    }
}
