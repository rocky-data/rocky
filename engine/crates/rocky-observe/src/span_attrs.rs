//! Canonical span-attribute names for cross-adapter OTel observability.
//!
//! This module is the single source of truth for span-attribute keys
//! that Rocky's warehouse and source adapters emit. The goal is that an
//! OTel collector receiving spans from a Databricks run, a Snowflake
//! run, and a BigQuery run sees the **same attribute keys** for the
//! same semantic — `rocky.warehouse.query_id` everywhere, not
//! `databricks.statement_id` vs `snowflake.statement_handle` vs
//! `bigquery.job_id` — so dashboards, alerts, and trace pivots compose
//! without per-adapter casing.
//!
//! # Convention
//!
//! All keys follow the `rocky.<resource>.<field>` shape already
//! established by [`crate::events`] (`rocky.event.*`) and
//! [`rocky_core::dag_executor`] (`rocky.model.*`). The `<resource>`
//! tier groups attributes that pivot together:
//!
//! - `rocky.adapter.*` — which warehouse/source crate is emitting
//! - `rocky.warehouse.*` — warehouse-side IDs, byte counts, references
//! - `rocky.statement.*` — per-statement metadata (kind, retry)
//! - `rocky.retry.*` — retry-loop bookkeeping
//!
//! # Usage at the span site
//!
//! `tracing`'s `info_span!` macro requires **literal token field
//! names** at parse time — you cannot write
//! `info_span!(span_attrs::WAREHOUSE_QUERY_ID = ...)`. The convention
//! is therefore:
//!
//! 1. Spell the literal name in the macro body, matching the constant
//!    here verbatim (the constants are the spec; the macro literals
//!    are the implementation that conforms to it).
//! 2. For values not known at span creation (warehouse-side query IDs
//!    only returned after submission), declare the field as
//!    `tracing::field::Empty` in the macro and call
//!    `span.record(span_attrs::WAREHOUSE_QUERY_ID, value)` once the
//!    value lands — `Span::record` accepts the `&'static str`
//!    constant directly, so it stays grep-clean.
//!
//! # Aliasing policy
//!
//! Where an adapter previously emitted a bespoke span attribute (e.g.
//! `adapter = "databricks"`), the migration is to emit **both** the
//! bespoke name and the canonical name for one release, so existing
//! dashboards see no breakage during the transition. Follow-up PRs
//! drop the bespoke aliases.
//!
//! As of this introduction, only `adapter` and `statement.kind` had
//! existing span-attribute call sites; `query_id` / `bytes_scanned` /
//! `table_ref` were inline log fields only, so there's no alias work
//! for those — they're new attributes recorded against the existing
//! `statement.execute` and `materialize.table` spans.

// ---------------------------------------------------------------------------
// Adapter identity
// ---------------------------------------------------------------------------

/// Name of the Rocky adapter emitting the span. One of the values in
/// [`WAREHOUSE_NAMES`] for warehouse adapters, or a source-adapter
/// identifier (`airbyte`, `fivetran`) for source adapters.
///
/// **Canonical replacement** for the bespoke `adapter` attribute
/// emitted on `statement.execute` spans pre-unification.
pub const ADAPTER_NAME: &str = "rocky.adapter.name";

// ---------------------------------------------------------------------------
// Warehouse / source identity + telemetry
// ---------------------------------------------------------------------------

/// Logical warehouse name (Snowflake virtual warehouse, Databricks
/// SQL warehouse ID, BigQuery has no concept and may omit). Lets
/// dashboards group cost / latency by physical compute pool when a
/// single Rocky deployment hits multiple warehouses of the same kind.
pub const WAREHOUSE_NAME: &str = "rocky.warehouse.name";

/// Warehouse-side query identifier. Backed by
/// `StatementResponse.statement_id` (Databricks),
/// `StatementResponse.statement_handle` (Snowflake), or
/// `JobReference.job_id` (BigQuery). Critical for cross-referencing a
/// Rocky trace against the warehouse's own query history UI.
pub const WAREHOUSE_QUERY_ID: &str = "rocky.warehouse.query_id";

/// Byte count the warehouse reports as scanned/processed for this
/// statement. Adapter-specific semantics (see [`crate`] docs and
/// `rocky_cli::output::TraceModelEntry::bytes_scanned`):
///
/// - BigQuery: `totalBytesBilled` (10 MB floor applied)
/// - Databricks: manifest `total_byte_count`
/// - Snowflake: `None` (deferred)
pub const WAREHOUSE_BYTES_SCANNED: &str = "rocky.warehouse.bytes_scanned";

/// Fully-qualified table reference touched by the statement, in
/// `catalog.schema.table` form. Lets dashboards pivot statements by
/// target without re-parsing SQL.
pub const WAREHOUSE_TABLE_REF: &str = "rocky.warehouse.table_ref";

// ---------------------------------------------------------------------------
// Statement-level
// ---------------------------------------------------------------------------

/// Statement classification (`ddl`, `dml`, `query`, `merge`, ...).
/// Adapters classify via `rocky_sql` where possible; BigQuery's
/// `jobs.query` endpoint routes every kind through the same path and
/// today emits the literal `"query"` until upstream classification
/// lands.
///
/// **Canonical replacement** for the bespoke `statement.kind`
/// attribute emitted pre-unification.
pub const STATEMENT_KIND: &str = "rocky.statement.kind";

// ---------------------------------------------------------------------------
// Retry loop
// ---------------------------------------------------------------------------

/// 1-based attempt counter on a retried statement. `1` on the first
/// successful submission; `N` on the Nth retry. Recorded on the
/// active `statement.execute` span so retries surface as a single
/// span with a final attempt count, not N nested spans.
pub const RETRY_ATTEMPT: &str = "rocky.retry.attempt";

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

/// Recognised warehouse-adapter values for [`ADAPTER_NAME`] /
/// [`WAREHOUSE_NAME`]. Source-side adapters (`airbyte`, `fivetran`)
/// share the same [`ADAPTER_NAME`] key but live in their own
/// taxonomy.
pub const WAREHOUSE_NAMES: &[&str] = &[
    "databricks",
    "snowflake",
    "bigquery",
    "duckdb",
    "iceberg",
    "trino",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Every canonical key must use the `rocky.<resource>.<field>` shape
    /// — three dot-segments, lower-snake. Guards against typos that
    /// would silently fragment dashboards back into per-adapter chaos.
    #[test]
    fn all_keys_match_rocky_prefix_convention() {
        const KEYS: &[&str] = &[
            ADAPTER_NAME,
            WAREHOUSE_NAME,
            WAREHOUSE_QUERY_ID,
            WAREHOUSE_BYTES_SCANNED,
            WAREHOUSE_TABLE_REF,
            STATEMENT_KIND,
            RETRY_ATTEMPT,
        ];
        for key in KEYS {
            assert!(key.starts_with("rocky."), "{key} must start with 'rocky.'");
            let segments: Vec<&str> = key.split('.').collect();
            assert_eq!(
                segments.len(),
                3,
                "{key} must be three dot-segments (rocky.<resource>.<field>)"
            );
            for seg in &segments {
                assert!(
                    seg.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                    "{key} segment {seg} must be lower-snake"
                );
                assert!(!seg.is_empty(), "{key} has an empty segment");
            }
        }
    }

    /// Catch regressions if someone duplicates a key under a different
    /// constant name.
    #[test]
    fn all_keys_are_unique() {
        const KEYS: &[&str] = &[
            ADAPTER_NAME,
            WAREHOUSE_NAME,
            WAREHOUSE_QUERY_ID,
            WAREHOUSE_BYTES_SCANNED,
            WAREHOUSE_TABLE_REF,
            STATEMENT_KIND,
            RETRY_ATTEMPT,
        ];
        let mut seen = std::collections::HashSet::new();
        for k in KEYS {
            assert!(seen.insert(*k), "duplicate canonical key: {k}");
        }
    }

    /// `WAREHOUSE_NAMES` is the documented enumeration; ensure no
    /// duplicates and no empty entries slip in.
    #[test]
    fn warehouse_names_well_formed() {
        let mut seen = std::collections::HashSet::new();
        for name in WAREHOUSE_NAMES {
            assert!(!name.is_empty());
            assert!(seen.insert(*name), "duplicate warehouse name: {name}");
            assert!(
                name.chars().all(|c| c.is_ascii_lowercase()),
                "{name} must be lower-snake"
            );
        }
    }
}
