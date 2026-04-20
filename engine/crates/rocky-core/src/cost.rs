//! Per-query cost and cardinality estimation.
//!
//! Provides a foundation for estimating the cost of SQL operations using
//! simple heuristics. Each estimation function takes table statistics or
//! upstream cost estimates and produces a [`CostEstimate`] with row count,
//! byte size, dollar cost, and a confidence level.
//!
//! This module is warehouse-aware: [`WarehouseCostModel`] holds per-row and
//! per-byte pricing constants that differ across Databricks, Snowflake,
//! BigQuery, and DuckDB. Use [`WarehouseType::cost_model`] to get the
//! default parameters for a given warehouse.
//!
//! # Design
//!
//! The estimators use textbook heuristics (not a full query optimizer):
//! - **Table scan**: cost proportional to rows and bytes.
//! - **Join**: cardinality from standard selectivity rules (inner = min,
//!   left/right = preserved side, cross = product).
//! - **Filter**: rows reduced by a caller-supplied selectivity factor.
//! - **Aggregate**: output rows bounded by group-by cardinality or a
//!   heuristic fraction of input rows.
//!
//! These estimates are directionally useful for comparing plan alternatives,
//! surfacing expensive operations, and feeding into `rocky optimize`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Confidence level for a cost estimate.
///
/// Higher confidence means the estimate is based on more reliable statistics
/// (e.g., exact row counts vs. guessed selectivity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    /// Based on heuristic defaults or missing statistics.
    Low,
    /// Based on partial statistics (e.g., known row count, guessed selectivity).
    Medium,
    /// Based on exact statistics from the warehouse catalog.
    High,
}

impl Confidence {
    /// Returns the lower of two confidence levels.
    ///
    /// Useful when combining estimates: the combined confidence cannot exceed
    /// the weakest input.
    #[must_use]
    pub fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Low, _) | (_, Self::Low) => Self::Low,
            (Self::Medium, _) | (_, Self::Medium) => Self::Medium,
            _ => Self::High,
        }
    }
}

/// Estimated cost of a query operation.
///
/// All fields are best-effort estimates. Use [`confidence`](CostEstimate::confidence)
/// to gauge reliability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Estimated number of output rows.
    pub estimated_rows: u64,
    /// Estimated total bytes of output data.
    pub estimated_bytes: u64,
    /// Estimated compute cost in USD (warehouse-specific).
    pub estimated_compute_cost_usd: f64,
    /// How much to trust these numbers.
    pub confidence: Confidence,
}

// ---------------------------------------------------------------------------
// Warehouse cost model
// ---------------------------------------------------------------------------

/// Supported warehouse types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarehouseType {
    Databricks,
    Snowflake,
    BigQuery,
    DuckDb,
}

impl WarehouseType {
    /// Returns the default [`WarehouseCostModel`] for this warehouse type.
    #[must_use]
    pub fn cost_model(self) -> WarehouseCostModel {
        match self {
            Self::Databricks => WarehouseCostModel::databricks(),
            Self::Snowflake => WarehouseCostModel::snowflake(),
            Self::BigQuery => WarehouseCostModel::bigquery(),
            Self::DuckDb => WarehouseCostModel::duckdb(),
        }
    }

    /// Parse a Rocky adapter `type` string into a [`WarehouseType`].
    ///
    /// Returns `None` for adapters that don't represent a billed warehouse
    /// (e.g. `fivetran`, `airbyte` — source adapters that move data but
    /// don't charge compute to us).
    #[must_use]
    pub fn from_adapter_type(s: &str) -> Option<Self> {
        match s {
            "databricks" => Some(Self::Databricks),
            "snowflake" => Some(Self::Snowflake),
            "bigquery" => Some(Self::BigQuery),
            "duckdb" => Some(Self::DuckDb),
            _ => None,
        }
    }
}

/// Per-operation pricing constants for a warehouse.
///
/// All costs are in USD. The model deliberately uses simple linear
/// multipliers rather than tiered pricing so that estimates stay fast
/// and deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseCostModel {
    /// Cost per row scanned from storage (USD).
    pub per_row_scan_cost: f64,
    /// Cost per row of compute processing (joins, aggregates, etc.) (USD).
    pub per_row_compute_cost: f64,
    /// Cost per byte of I/O (USD).
    pub per_byte_io_cost: f64,
}

impl WarehouseCostModel {
    /// Databricks SQL Serverless defaults.
    ///
    /// Assumes ~$0.40/DBU, medium warehouse (~1 000 rows/ms throughput).
    #[must_use]
    pub fn databricks() -> Self {
        Self {
            per_row_scan_cost: 1.0e-9,    // $1 per billion rows scanned
            per_row_compute_cost: 5.0e-9, // $5 per billion rows processed
            per_byte_io_cost: 5.0e-12,    // $5 per TB of I/O
        }
    }

    /// Snowflake defaults (X-Small warehouse, on-demand).
    #[must_use]
    pub fn snowflake() -> Self {
        Self {
            per_row_scan_cost: 1.2e-9,
            per_row_compute_cost: 6.0e-9,
            per_byte_io_cost: 6.0e-12,
        }
    }

    /// BigQuery on-demand defaults ($6.25 per TB scanned).
    #[must_use]
    pub fn bigquery() -> Self {
        Self {
            per_row_scan_cost: 0.8e-9,
            per_row_compute_cost: 4.0e-9,
            per_byte_io_cost: 6.25e-12, // $6.25 / 1e12
        }
    }

    /// DuckDB (local, effectively free — costs are symbolic for comparison).
    #[must_use]
    pub fn duckdb() -> Self {
        Self {
            per_row_scan_cost: 0.0,
            per_row_compute_cost: 0.0,
            per_byte_io_cost: 0.0,
        }
    }

    /// Compute the dollar cost of scanning `rows` rows of `bytes` bytes.
    fn scan_cost(&self, rows: u64, bytes: u64) -> f64 {
        (rows as f64 * self.per_row_scan_cost) + (bytes as f64 * self.per_byte_io_cost)
    }
}

// ---------------------------------------------------------------------------
// Join type
// ---------------------------------------------------------------------------

/// SQL join type, used for cardinality estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

// ---------------------------------------------------------------------------
// Estimation functions
// ---------------------------------------------------------------------------

/// Estimates the cost of a full table scan.
///
/// # Arguments
///
/// * `row_count` — Number of rows in the table.
/// * `avg_row_bytes` — Average size of a single row in bytes.
/// * `warehouse_type` — Target warehouse (determines pricing).
///
/// # Returns
///
/// A [`CostEstimate`] with [`Confidence::High`] since the inputs are
/// assumed to come from catalog statistics.
#[must_use]
pub fn estimate_table_scan_cost(
    row_count: u64,
    avg_row_bytes: u64,
    warehouse_type: WarehouseType,
) -> CostEstimate {
    let model = warehouse_type.cost_model();
    let total_bytes = row_count.saturating_mul(avg_row_bytes);
    let cost = model.scan_cost(row_count, total_bytes);

    CostEstimate {
        estimated_rows: row_count,
        estimated_bytes: total_bytes,
        estimated_compute_cost_usd: cost,
        confidence: Confidence::High,
    }
}

/// Estimates the cost of a join between two inputs.
///
/// Cardinality heuristics (standard textbook rules):
/// - **Inner**: `min(left_rows, right_rows)` — assumes a selective equi-join.
/// - **Left**: `left_rows` — every left row appears at least once.
/// - **Right**: `right_rows` — every right row appears at least once.
/// - **Full**: `max(left_rows, right_rows)` — union of both sides.
/// - **Cross**: `left_rows * right_rows` — Cartesian product.
///
/// The estimated byte size scales linearly with the output row count,
/// using the average row size from the larger input.
#[must_use]
pub fn estimate_join_cost(
    left: &CostEstimate,
    right: &CostEstimate,
    join_type: JoinType,
) -> CostEstimate {
    let output_rows = match join_type {
        JoinType::Inner => left.estimated_rows.min(right.estimated_rows),
        JoinType::Left => left.estimated_rows,
        JoinType::Right => right.estimated_rows,
        JoinType::Full => left.estimated_rows.max(right.estimated_rows),
        JoinType::Cross => left.estimated_rows.saturating_mul(right.estimated_rows),
    };

    // Average bytes per row from the larger side (heuristic: the wider table
    // dominates the output row width in most practical joins).
    let avg_row_bytes = if left.estimated_rows > 0 && right.estimated_rows > 0 {
        let left_avg = left.estimated_bytes / left.estimated_rows.max(1);
        let right_avg = right.estimated_bytes / right.estimated_rows.max(1);
        left_avg.max(right_avg)
    } else {
        0
    };

    let output_bytes = output_rows.saturating_mul(avg_row_bytes);

    // Join cost = cost of reading both sides + cost of processing output rows.
    let read_cost = left.estimated_compute_cost_usd + right.estimated_compute_cost_usd;

    // Use the Databricks model as a baseline for the processing cost when
    // the input estimates already carry warehouse-specific pricing. In
    // practice, the caller should use consistent warehouse types; we pick
    // the more conservative (higher) per-row compute cost.
    let per_row = WarehouseCostModel::databricks().per_row_compute_cost;
    let process_cost = output_rows as f64 * per_row;
    let total_cost = read_cost + process_cost;

    let confidence = left.confidence.min(right.confidence);
    // Cross joins and full joins are harder to estimate, so downgrade.
    let confidence = match join_type {
        JoinType::Cross | JoinType::Full => confidence.min(Confidence::Low),
        _ => confidence.min(Confidence::Medium),
    };

    CostEstimate {
        estimated_rows: output_rows,
        estimated_bytes: output_bytes,
        estimated_compute_cost_usd: total_cost,
        confidence,
    }
}

/// Estimates the cost of applying a filter (WHERE clause) to an input.
///
/// # Arguments
///
/// * `input` — The upstream cost estimate before filtering.
/// * `selectivity` — Fraction of rows that pass the filter (0.0 = none, 1.0 = all).
///
/// # Panics
///
/// Does not panic. Selectivity is clamped to `[0.0, 1.0]`.
#[must_use]
pub fn estimate_filter_cost(input: &CostEstimate, selectivity: f64) -> CostEstimate {
    let selectivity = selectivity.clamp(0.0, 1.0);
    let output_rows = (input.estimated_rows as f64 * selectivity).round() as u64;
    let output_bytes = (input.estimated_bytes as f64 * selectivity).round() as u64;

    // Filtering still requires scanning the full input, so the read cost is
    // unchanged. Add a small compute overhead for evaluating the predicate.
    let per_row = WarehouseCostModel::databricks().per_row_compute_cost;
    let filter_cost = input.estimated_rows as f64 * per_row;
    let total_cost = input.estimated_compute_cost_usd + filter_cost;

    // Selectivity is often a guess, so downgrade confidence unless it's
    // exactly 0.0 or 1.0 (trivially precise).
    let confidence = if selectivity == 0.0 || selectivity == 1.0 {
        input.confidence
    } else {
        input.confidence.min(Confidence::Medium)
    };

    CostEstimate {
        estimated_rows: output_rows,
        estimated_bytes: output_bytes,
        estimated_compute_cost_usd: total_cost,
        confidence,
    }
}

/// Estimates the cost of an aggregate (GROUP BY) operation.
///
/// # Arguments
///
/// * `input` — The upstream cost estimate.
/// * `group_by_cardinality` — If known, the number of distinct groups.
///   When `None`, a heuristic of `sqrt(input_rows)` is used.
///
/// Aggregate output bytes are estimated as `output_rows * 64` (a rough
/// average for aggregated rows with a few numeric columns).
#[must_use]
pub fn estimate_aggregate_cost(
    input: &CostEstimate,
    group_by_cardinality: Option<u64>,
) -> CostEstimate {
    let output_rows = match group_by_cardinality {
        Some(card) => card.min(input.estimated_rows),
        None => {
            // Heuristic: sqrt(N) distinct groups when cardinality is unknown.
            (input.estimated_rows as f64).sqrt().ceil() as u64
        }
    };

    // Aggregated rows tend to be narrower (grouped keys + a few aggregates).
    const AGG_ROW_BYTES: u64 = 64;
    let output_bytes = output_rows.saturating_mul(AGG_ROW_BYTES);

    // Cost = scanning all input rows + hashing/aggregating.
    let per_row = WarehouseCostModel::databricks().per_row_compute_cost;
    let agg_cost = input.estimated_rows as f64 * per_row * 2.0; // 2x for hash + aggregate
    let total_cost = input.estimated_compute_cost_usd + agg_cost;

    let confidence = if group_by_cardinality.is_some() {
        input.confidence.min(Confidence::Medium)
    } else {
        input.confidence.min(Confidence::Low)
    };

    CostEstimate {
        estimated_rows: output_rows,
        estimated_bytes: output_bytes,
        estimated_compute_cost_usd: total_cost,
        confidence,
    }
}

// ---------------------------------------------------------------------------
// DAG-aware cost propagation
// ---------------------------------------------------------------------------

use std::collections::HashMap;

use crate::dag::DagNode;

/// Per-model table statistics used as seed data for cost propagation.
///
/// For source/leaf models with known catalog statistics (row count, average
/// row size), create a `TableStats` and pass it into [`propagate_costs`].
/// Transformation models that don't have catalog stats are inferred from
/// their upstream dependencies.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Number of rows in the table.
    pub row_count: u64,
    /// Average size of a single row in bytes.
    pub avg_row_bytes: u64,
}

/// Default filter selectivity applied when propagating cost through a
/// transformation model that has no explicit statistics.
///
/// 0.8 means we assume the transformation retains 80% of upstream rows
/// (a conservative heuristic — most staging transforms are light filters).
const DEFAULT_TRANSFORM_SELECTIVITY: f64 = 0.8;

/// Propagates cost estimates through a DAG using topological ordering.
///
/// For each node in the DAG:
/// - If `base_stats` contains an entry, the estimate starts from a table scan
///   using those statistics.
/// - Otherwise, the estimate is derived from the node's upstream dependencies
///   by summing their estimates (to model a union/join) and applying a default
///   filter selectivity to approximate transformation overhead.
///
/// Returns a map from model name to [`CostEstimate`]. Models involved in
/// cycles (which `topological_sort` would reject) are excluded.
///
/// # Errors
///
/// Returns `Err` if the DAG contains cycles or references unknown nodes.
pub fn propagate_costs(
    nodes: &[DagNode],
    base_stats: &HashMap<String, TableStats>,
    warehouse_type: WarehouseType,
) -> Result<HashMap<String, CostEstimate>, crate::dag::DagError> {
    let sorted = crate::dag::topological_sort(nodes)?;
    let mut estimates: HashMap<String, CostEstimate> = HashMap::with_capacity(nodes.len());

    let deps_map: HashMap<&str, &[String]> = nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();

    for name in &sorted {
        if let Some(stats) = base_stats.get(name.as_str()) {
            // Leaf/source node with known statistics.
            let est =
                estimate_table_scan_cost(stats.row_count, stats.avg_row_bytes, warehouse_type);
            estimates.insert(name.clone(), est);
        } else {
            // Transformation node: derive from upstream.
            let deps = deps_map.get(name.as_str()).copied().unwrap_or(&[]);
            let upstream: Vec<&CostEstimate> = deps
                .iter()
                .filter_map(|d| estimates.get(d.as_str()))
                .collect();

            let combined = if upstream.is_empty() {
                // No upstream estimates available — use a minimal placeholder.
                CostEstimate {
                    estimated_rows: 0,
                    estimated_bytes: 0,
                    estimated_compute_cost_usd: 0.0,
                    confidence: Confidence::Low,
                }
            } else if upstream.len() == 1 {
                // Single upstream: apply filter selectivity.
                estimate_filter_cost(upstream[0], DEFAULT_TRANSFORM_SELECTIVITY)
            } else {
                // Multiple upstreams: sum rows/bytes/cost, then apply selectivity.
                let total_rows: u64 = upstream.iter().map(|e| e.estimated_rows).sum();
                let total_bytes: u64 = upstream.iter().map(|e| e.estimated_bytes).sum();
                let total_cost: f64 = upstream.iter().map(|e| e.estimated_compute_cost_usd).sum();
                let min_confidence = upstream
                    .iter()
                    .map(|e| e.confidence)
                    .fold(Confidence::High, Confidence::min);

                let merged = CostEstimate {
                    estimated_rows: total_rows,
                    estimated_bytes: total_bytes,
                    estimated_compute_cost_usd: total_cost,
                    confidence: min_confidence,
                };
                estimate_filter_cost(&merged, DEFAULT_TRANSFORM_SELECTIVITY)
            };

            estimates.insert(name.clone(), combined);
        }
    }

    Ok(estimates)
}

/// Counts how many downstream (dependent) models each node has.
///
/// Returns a map from model name to the number of models that directly
/// depend on it. Nodes with no downstream dependents are included with
/// a count of 0.
#[must_use]
pub fn downstream_counts(nodes: &[DagNode]) -> HashMap<String, usize> {
    let mut counts: HashMap<String, usize> = nodes.iter().map(|n| (n.name.clone(), 0)).collect();

    for node in nodes {
        for dep in &node.depends_on {
            if let Some(count) = counts.get_mut(dep.as_str()) {
                *count += 1;
            }
        }
    }

    counts
}

/// Aggregates per-model cost estimates into a project total.
///
/// Sums the estimated compute cost across all models. Confidence is the
/// minimum across all inputs.
#[must_use]
pub fn aggregate_project_cost(estimates: &HashMap<String, CostEstimate>) -> CostEstimate {
    let total_rows: u64 = estimates.values().map(|e| e.estimated_rows).sum();
    let total_bytes: u64 = estimates.values().map(|e| e.estimated_bytes).sum();
    let total_cost: f64 = estimates
        .values()
        .map(|e| e.estimated_compute_cost_usd)
        .sum();
    let confidence = estimates
        .values()
        .map(|e| e.confidence)
        .fold(Confidence::High, Confidence::min);

    CostEstimate {
        estimated_rows: total_rows,
        estimated_bytes: total_bytes,
        estimated_compute_cost_usd: total_cost,
        confidence,
    }
}

// ---------------------------------------------------------------------------
// Observed cost (post-execution, per-adapter)
// ---------------------------------------------------------------------------

/// BigQuery on-demand pricing in USD per TB scanned. Hard-coded default
/// (`$6.25/TB`) — matches the public price list; we expose this as a
/// constant rather than a config knob until someone asks for per-region
/// overrides.
pub const BIGQUERY_USD_PER_TB_SCANNED: f64 = 6.25;

/// DBU throughput per hour for a named warehouse size.
///
/// Numbers mirror Databricks SQL Warehouse sizing. Snowflake users can
/// treat these as credits/hr with their own `cost_per_dbu` interpretation:
/// the scaling curve differs (Snowflake doubles exactly, Databricks
/// doesn't) but the order of magnitude holds for a wave-1 approximation.
/// Unknown sizes fall back to `Medium` rather than failing — the trust
/// outcome degrades gracefully to "directional cost estimate" instead of
/// "no cost at all".
#[must_use]
pub fn warehouse_size_to_dbu_per_hour(size: &str) -> f64 {
    match size.to_ascii_lowercase().as_str() {
        "2x-small" | "2xsmall" => 4.0,
        "x-small" | "xsmall" => 6.0,
        "small" => 12.0,
        "medium" => 24.0,
        "large" => 40.0,
        "x-large" | "xlarge" => 80.0,
        "2x-large" | "2xlarge" => 144.0,
        "3x-large" | "3xlarge" => 272.0,
        "4x-large" | "4xlarge" => 528.0,
        _ => 24.0,
    }
}

/// Observed dollar cost of a single completed execution, computed from
/// the measurements the adapter returned.
///
/// Unlike [`CostEstimate`] (which powers `rocky estimate` / `rocky
/// optimize` before execution), this is a post-hoc number intended for
/// `rocky run` cost attribution: given actual `duration_ms` and
/// `bytes_scanned`, pick the right billing formula per warehouse.
///
/// - **Databricks / Snowflake**: duration-based. `cost = duration_hours *
///   dbu_per_hour * cost_per_dbu`. DBU/credit throughput comes from
///   [`warehouse_size_to_dbu_per_hour`]; the per-DBU rate comes from
///   `[cost].compute_cost_per_dbu` in `rocky.toml`.
/// - **BigQuery**: bytes-based. `cost = bytes_scanned / 1e12 *
///   BIGQUERY_USD_PER_TB_SCANNED`. Returns `None` when `bytes_scanned`
///   wasn't reported.
/// - **DuckDB**: always `0.0` (local execution).
#[must_use]
pub fn compute_observed_cost_usd(
    warehouse_type: WarehouseType,
    bytes_scanned: Option<u64>,
    duration_ms: u64,
    dbu_per_hour: f64,
    cost_per_dbu: f64,
) -> Option<f64> {
    match warehouse_type {
        WarehouseType::Databricks | WarehouseType::Snowflake => {
            let hours = duration_ms as f64 / 3_600_000.0;
            Some(hours * dbu_per_hour * cost_per_dbu)
        }
        WarehouseType::BigQuery => {
            bytes_scanned.map(|b| (b as f64 / 1.0e12) * BIGQUERY_USD_PER_TB_SCANNED)
        }
        WarehouseType::DuckDb => Some(0.0),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Confidence -----------------------------------------------------------

    #[test]
    fn confidence_min_picks_lowest() {
        assert_eq!(Confidence::High.min(Confidence::High), Confidence::High);
        assert_eq!(Confidence::High.min(Confidence::Medium), Confidence::Medium);
        assert_eq!(Confidence::High.min(Confidence::Low), Confidence::Low);
        assert_eq!(Confidence::Medium.min(Confidence::Low), Confidence::Low);
        assert_eq!(Confidence::Low.min(Confidence::Low), Confidence::Low);
    }

    // -- WarehouseType --------------------------------------------------------

    #[test]
    fn warehouse_type_returns_distinct_models() {
        let db = WarehouseType::Databricks.cost_model();
        let sf = WarehouseType::Snowflake.cost_model();
        let bq = WarehouseType::BigQuery.cost_model();
        let dk = WarehouseType::DuckDb.cost_model();

        // DuckDB is free.
        assert_eq!(dk.per_row_scan_cost, 0.0);
        assert_eq!(dk.per_row_compute_cost, 0.0);
        assert_eq!(dk.per_byte_io_cost, 0.0);

        // Cloud warehouses have positive costs.
        assert!(db.per_row_scan_cost > 0.0);
        assert!(sf.per_row_scan_cost > 0.0);
        assert!(bq.per_row_scan_cost > 0.0);

        // Snowflake is slightly more expensive than Databricks per row.
        assert!(sf.per_row_scan_cost > db.per_row_scan_cost);
    }

    // -- Table scan -----------------------------------------------------------

    #[test]
    fn table_scan_basic() {
        let est = estimate_table_scan_cost(1_000_000, 256, WarehouseType::Databricks);
        assert_eq!(est.estimated_rows, 1_000_000);
        assert_eq!(est.estimated_bytes, 1_000_000 * 256);
        assert!(est.estimated_compute_cost_usd > 0.0);
        assert_eq!(est.confidence, Confidence::High);
    }

    #[test]
    fn table_scan_empty_table() {
        let est = estimate_table_scan_cost(0, 256, WarehouseType::Databricks);
        assert_eq!(est.estimated_rows, 0);
        assert_eq!(est.estimated_bytes, 0);
        assert_eq!(est.estimated_compute_cost_usd, 0.0);
        assert_eq!(est.confidence, Confidence::High);
    }

    #[test]
    fn table_scan_duckdb_is_free() {
        let est = estimate_table_scan_cost(1_000_000, 256, WarehouseType::DuckDb);
        assert_eq!(est.estimated_rows, 1_000_000);
        assert_eq!(est.estimated_compute_cost_usd, 0.0);
    }

    #[test]
    fn table_scan_cost_scales_with_size() {
        let small = estimate_table_scan_cost(1_000, 100, WarehouseType::Snowflake);
        let large = estimate_table_scan_cost(1_000_000, 100, WarehouseType::Snowflake);
        assert!(large.estimated_compute_cost_usd > small.estimated_compute_cost_usd);
        // Should scale roughly linearly (within 5% of 1000x).
        let ratio = large.estimated_compute_cost_usd / small.estimated_compute_cost_usd;
        assert!((ratio - 1000.0).abs() < 50.0);
    }

    // -- Join cost ------------------------------------------------------------

    #[test]
    fn join_inner_cardinality() {
        let left = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(5_000, 64, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Inner);

        // Inner join heuristic: min(10_000, 5_000) = 5_000
        assert_eq!(joined.estimated_rows, 5_000);
        assert!(joined.estimated_compute_cost_usd > 0.0);
        assert_eq!(joined.confidence, Confidence::Medium);
    }

    #[test]
    fn join_left_preserves_left_rows() {
        let left = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(5_000, 64, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Left);
        assert_eq!(joined.estimated_rows, 10_000);
    }

    #[test]
    fn join_right_preserves_right_rows() {
        let left = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(5_000, 64, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Right);
        assert_eq!(joined.estimated_rows, 5_000);
    }

    #[test]
    fn join_full_uses_max() {
        let left = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(5_000, 64, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Full);
        assert_eq!(joined.estimated_rows, 10_000);
        assert_eq!(joined.confidence, Confidence::Low);
    }

    #[test]
    fn join_cross_is_product() {
        let left = estimate_table_scan_cost(100, 64, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(200, 64, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Cross);
        assert_eq!(joined.estimated_rows, 20_000);
        assert_eq!(joined.confidence, Confidence::Low);
    }

    #[test]
    fn join_cost_includes_both_inputs() {
        let left = estimate_table_scan_cost(1_000, 100, WarehouseType::Databricks);
        let right = estimate_table_scan_cost(2_000, 100, WarehouseType::Databricks);
        let joined = estimate_join_cost(&left, &right, JoinType::Inner);

        // Total cost must exceed sum of input costs (join adds processing).
        let input_sum = left.estimated_compute_cost_usd + right.estimated_compute_cost_usd;
        assert!(joined.estimated_compute_cost_usd > input_sum);
    }

    // -- Filter cost ----------------------------------------------------------

    #[test]
    fn filter_reduces_rows() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let filtered = estimate_filter_cost(&input, 0.1);
        assert_eq!(filtered.estimated_rows, 1_000);
        assert_eq!(filtered.estimated_bytes, (10_000 * 128) / 10);
        assert_eq!(filtered.confidence, Confidence::Medium);
    }

    #[test]
    fn filter_selectivity_zero() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let filtered = estimate_filter_cost(&input, 0.0);
        assert_eq!(filtered.estimated_rows, 0);
        assert_eq!(filtered.estimated_bytes, 0);
        // Trivial selectivity preserves confidence.
        assert_eq!(filtered.confidence, Confidence::High);
    }

    #[test]
    fn filter_selectivity_one() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let filtered = estimate_filter_cost(&input, 1.0);
        assert_eq!(filtered.estimated_rows, 10_000);
        assert_eq!(filtered.confidence, Confidence::High);
    }

    #[test]
    fn filter_clamps_selectivity() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);

        let over = estimate_filter_cost(&input, 1.5);
        assert_eq!(over.estimated_rows, 10_000);

        let under = estimate_filter_cost(&input, -0.5);
        assert_eq!(under.estimated_rows, 0);
    }

    #[test]
    fn filter_cost_not_less_than_input() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let filtered = estimate_filter_cost(&input, 0.5);
        // Filtering must scan the full input, so cost >= input cost.
        assert!(filtered.estimated_compute_cost_usd >= input.estimated_compute_cost_usd);
    }

    // -- Aggregate cost -------------------------------------------------------

    #[test]
    fn aggregate_with_known_cardinality() {
        let input = estimate_table_scan_cost(100_000, 256, WarehouseType::Databricks);
        let agg = estimate_aggregate_cost(&input, Some(500));
        assert_eq!(agg.estimated_rows, 500);
        assert_eq!(agg.estimated_bytes, 500 * 64); // AGG_ROW_BYTES
        assert_eq!(agg.confidence, Confidence::Medium);
    }

    #[test]
    fn aggregate_cardinality_capped_at_input() {
        let input = estimate_table_scan_cost(100, 256, WarehouseType::Databricks);
        let agg = estimate_aggregate_cost(&input, Some(1_000));
        // Cannot produce more groups than input rows.
        assert_eq!(agg.estimated_rows, 100);
    }

    #[test]
    fn aggregate_unknown_cardinality_uses_sqrt() {
        let input = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let agg = estimate_aggregate_cost(&input, None);
        // sqrt(10_000) = 100
        assert_eq!(agg.estimated_rows, 100);
        assert_eq!(agg.confidence, Confidence::Low);
    }

    #[test]
    fn aggregate_cost_exceeds_input() {
        let input = estimate_table_scan_cost(50_000, 200, WarehouseType::Databricks);
        let agg = estimate_aggregate_cost(&input, Some(1_000));
        // Aggregation adds compute cost on top of input scan.
        assert!(agg.estimated_compute_cost_usd > input.estimated_compute_cost_usd);
    }

    // -- Composition (chained operations) -------------------------------------

    #[test]
    fn scan_filter_aggregate_pipeline() {
        // Simulate: SELECT region, COUNT(*) FROM orders WHERE amount > 100 GROUP BY region
        let scan = estimate_table_scan_cost(1_000_000, 200, WarehouseType::Databricks);
        let filtered = estimate_filter_cost(&scan, 0.3); // 30% pass filter
        let agg = estimate_aggregate_cost(&filtered, Some(50)); // 50 regions

        assert_eq!(scan.estimated_rows, 1_000_000);
        assert_eq!(filtered.estimated_rows, 300_000);
        assert_eq!(agg.estimated_rows, 50);
        // Each stage adds cost.
        assert!(filtered.estimated_compute_cost_usd > scan.estimated_compute_cost_usd);
        assert!(agg.estimated_compute_cost_usd > filtered.estimated_compute_cost_usd);
    }

    #[test]
    fn scan_join_filter_pipeline() {
        // Simulate: SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id WHERE c.active
        let orders = estimate_table_scan_cost(500_000, 256, WarehouseType::Databricks);
        let customers = estimate_table_scan_cost(10_000, 128, WarehouseType::Databricks);
        let joined = estimate_join_cost(&orders, &customers, JoinType::Inner);
        let filtered = estimate_filter_cost(&joined, 0.8);

        // Inner join: min(500k, 10k) = 10k
        assert_eq!(joined.estimated_rows, 10_000);
        // 80% pass filter
        assert_eq!(filtered.estimated_rows, 8_000);
        // Cost accumulates through the pipeline.
        assert!(
            filtered.estimated_compute_cost_usd
                > orders.estimated_compute_cost_usd + customers.estimated_compute_cost_usd
        );
    }

    // -- Serialization --------------------------------------------------------

    #[test]
    fn cost_estimate_roundtrips_json() {
        let est = estimate_table_scan_cost(42, 100, WarehouseType::Databricks);
        let json = serde_json::to_string(&est).unwrap();
        let back: CostEstimate = serde_json::from_str(&json).unwrap();
        assert_eq!(back.estimated_rows, 42);
        assert_eq!(back.confidence, Confidence::High);
    }

    #[test]
    fn warehouse_cost_model_roundtrips_json() {
        let model = WarehouseCostModel::databricks();
        let json = serde_json::to_string(&model).unwrap();
        let back: WarehouseCostModel = serde_json::from_str(&json).unwrap();
        assert!((back.per_row_scan_cost - model.per_row_scan_cost).abs() < f64::EPSILON);
    }

    #[test]
    fn confidence_serializes_as_snake_case() {
        let json = serde_json::to_string(&Confidence::High).unwrap();
        assert_eq!(json, "\"high\"");
        let json = serde_json::to_string(&Confidence::Medium).unwrap();
        assert_eq!(json, "\"medium\"");
        let json = serde_json::to_string(&Confidence::Low).unwrap();
        assert_eq!(json, "\"low\"");
    }

    // -- Observed cost --------------------------------------------------------

    #[test]
    fn from_adapter_type_known_warehouses() {
        assert_eq!(
            WarehouseType::from_adapter_type("databricks"),
            Some(WarehouseType::Databricks)
        );
        assert_eq!(
            WarehouseType::from_adapter_type("snowflake"),
            Some(WarehouseType::Snowflake)
        );
        assert_eq!(
            WarehouseType::from_adapter_type("bigquery"),
            Some(WarehouseType::BigQuery)
        );
        assert_eq!(
            WarehouseType::from_adapter_type("duckdb"),
            Some(WarehouseType::DuckDb)
        );
    }

    #[test]
    fn from_adapter_type_ignores_sources() {
        // Source adapters don't represent a billed warehouse.
        assert_eq!(WarehouseType::from_adapter_type("fivetran"), None);
        assert_eq!(WarehouseType::from_adapter_type("airbyte"), None);
        assert_eq!(WarehouseType::from_adapter_type(""), None);
    }

    #[test]
    fn warehouse_size_mapping_is_monotone() {
        let small = warehouse_size_to_dbu_per_hour("small");
        let medium = warehouse_size_to_dbu_per_hour("medium");
        let large = warehouse_size_to_dbu_per_hour("large");
        assert!(small < medium);
        assert!(medium < large);
    }

    #[test]
    fn warehouse_size_is_case_insensitive_and_alias_tolerant() {
        assert_eq!(warehouse_size_to_dbu_per_hour("Medium"), 24.0);
        assert_eq!(warehouse_size_to_dbu_per_hour("MEDIUM"), 24.0);
        assert_eq!(warehouse_size_to_dbu_per_hour("x-small"), 6.0);
        assert_eq!(warehouse_size_to_dbu_per_hour("xsmall"), 6.0);
    }

    #[test]
    fn warehouse_size_unknown_falls_back_to_medium() {
        assert_eq!(warehouse_size_to_dbu_per_hour("nonsense"), 24.0);
        assert_eq!(warehouse_size_to_dbu_per_hour(""), 24.0);
    }

    #[test]
    fn observed_cost_duckdb_is_free() {
        let cost =
            compute_observed_cost_usd(WarehouseType::DuckDb, Some(1_000_000), 5_000, 24.0, 0.40);
        assert_eq!(cost, Some(0.0));
    }

    #[test]
    fn observed_cost_databricks_uses_duration() {
        // 1 hour on a Medium (24 DBU/hr) at $0.40/DBU = $9.60.
        let cost =
            compute_observed_cost_usd(WarehouseType::Databricks, None, 3_600_000, 24.0, 0.40)
                .unwrap();
        assert!((cost - 9.60).abs() < 1.0e-9);
    }

    #[test]
    fn observed_cost_databricks_ignores_bytes() {
        // Whether bytes_scanned is set or not, Databricks cost depends only
        // on duration.
        let with_bytes = compute_observed_cost_usd(
            WarehouseType::Databricks,
            Some(10_000_000),
            1_800_000, // 30 min
            24.0,
            0.40,
        );
        let without_bytes =
            compute_observed_cost_usd(WarehouseType::Databricks, None, 1_800_000, 24.0, 0.40);
        assert_eq!(with_bytes, without_bytes);
    }

    #[test]
    fn observed_cost_bigquery_uses_bytes() {
        // 1 TB scanned at $6.25/TB = $6.25.
        let cost = compute_observed_cost_usd(
            WarehouseType::BigQuery,
            Some(1_000_000_000_000),
            60_000,
            24.0,
            0.40,
        )
        .unwrap();
        assert!((cost - 6.25).abs() < 1.0e-9);
    }

    #[test]
    fn observed_cost_bigquery_none_without_bytes() {
        let cost = compute_observed_cost_usd(WarehouseType::BigQuery, None, 60_000, 24.0, 0.40);
        assert_eq!(cost, None);
    }

    #[test]
    fn observed_cost_snowflake_scales_linearly_with_duration() {
        let thirty_min =
            compute_observed_cost_usd(WarehouseType::Snowflake, None, 1_800_000, 4.0, 2.00)
                .unwrap();
        let one_hour =
            compute_observed_cost_usd(WarehouseType::Snowflake, None, 3_600_000, 4.0, 2.00)
                .unwrap();
        assert!((one_hour - 2.0 * thirty_min).abs() < 1.0e-9);
    }

    // -- DAG propagation -------------------------------------------------------

    fn dag_node(name: &str, deps: &[&str]) -> DagNode {
        DagNode {
            name: name.to_string(),
            depends_on: deps.iter().map(std::string::ToString::to_string).collect(),
        }
    }

    #[test]
    fn propagate_linear_chain() {
        // source -> staging -> mart
        let nodes = vec![
            dag_node("source", &[]),
            dag_node("staging", &["source"]),
            dag_node("mart", &["staging"]),
        ];

        let mut base = HashMap::new();
        base.insert(
            "source".to_string(),
            TableStats {
                row_count: 100_000,
                avg_row_bytes: 200,
            },
        );

        let estimates = propagate_costs(&nodes, &base, WarehouseType::Databricks).unwrap();

        assert_eq!(estimates.len(), 3);
        assert_eq!(estimates["source"].estimated_rows, 100_000);
        // staging = source * 0.8 selectivity
        assert_eq!(estimates["staging"].estimated_rows, 80_000);
        // mart = staging * 0.8 = 64_000
        assert_eq!(estimates["mart"].estimated_rows, 64_000);
        // Cost should increase through the pipeline (each stage adds compute).
        assert!(
            estimates["staging"].estimated_compute_cost_usd
                > estimates["source"].estimated_compute_cost_usd
        );
    }

    #[test]
    fn propagate_diamond_dag() {
        // source -> left, source -> right, left + right -> output
        let nodes = vec![
            dag_node("source", &[]),
            dag_node("left", &["source"]),
            dag_node("right", &["source"]),
            dag_node("output", &["left", "right"]),
        ];

        let mut base = HashMap::new();
        base.insert(
            "source".to_string(),
            TableStats {
                row_count: 10_000,
                avg_row_bytes: 128,
            },
        );

        let estimates = propagate_costs(&nodes, &base, WarehouseType::Databricks).unwrap();

        assert_eq!(estimates.len(), 4);
        // left and right both derive from source with 0.8 selectivity.
        assert_eq!(estimates["left"].estimated_rows, 8_000);
        assert_eq!(estimates["right"].estimated_rows, 8_000);
        // output merges left+right (16_000 total) then applies 0.8 selectivity.
        assert_eq!(estimates["output"].estimated_rows, 12_800);
    }

    #[test]
    fn propagate_with_multiple_sources() {
        let nodes = vec![
            dag_node("orders", &[]),
            dag_node("customers", &[]),
            dag_node("joined", &["orders", "customers"]),
        ];

        let mut base = HashMap::new();
        base.insert(
            "orders".to_string(),
            TableStats {
                row_count: 500_000,
                avg_row_bytes: 256,
            },
        );
        base.insert(
            "customers".to_string(),
            TableStats {
                row_count: 10_000,
                avg_row_bytes: 128,
            },
        );

        let estimates = propagate_costs(&nodes, &base, WarehouseType::BigQuery).unwrap();

        assert_eq!(estimates.len(), 3);
        assert_eq!(estimates["orders"].estimated_rows, 500_000);
        assert_eq!(estimates["customers"].estimated_rows, 10_000);
        // joined = (500_000 + 10_000) * 0.8 = 408_000
        assert_eq!(estimates["joined"].estimated_rows, 408_000);
    }

    #[test]
    fn propagate_duckdb_is_free() {
        let nodes = vec![dag_node("source", &[]), dag_node("transform", &["source"])];

        let mut base = HashMap::new();
        base.insert(
            "source".to_string(),
            TableStats {
                row_count: 1_000_000,
                avg_row_bytes: 100,
            },
        );

        let estimates = propagate_costs(&nodes, &base, WarehouseType::DuckDb).unwrap();

        assert_eq!(estimates["source"].estimated_compute_cost_usd, 0.0);
        // DuckDB per-row costs are zero, but filter still uses Databricks
        // model for compute overhead — this is acceptable because DuckDB
        // is local and the cost is symbolic.
    }

    #[test]
    fn propagate_empty_dag() {
        let estimates = propagate_costs(&[], &HashMap::new(), WarehouseType::Databricks).unwrap();
        assert!(estimates.is_empty());
    }

    #[test]
    fn propagate_no_base_stats() {
        // All nodes are transformations with no catalog stats.
        let nodes = vec![dag_node("a", &[]), dag_node("b", &["a"])];

        let estimates =
            propagate_costs(&nodes, &HashMap::new(), WarehouseType::Databricks).unwrap();

        assert_eq!(estimates.len(), 2);
        // "a" has no upstream and no base stats → zero-row placeholder.
        assert_eq!(estimates["a"].estimated_rows, 0);
        assert_eq!(estimates["a"].confidence, Confidence::Low);
    }

    // -- Downstream counts ----------------------------------------------------

    #[test]
    fn downstream_counts_diamond() {
        let nodes = vec![
            dag_node("source", &[]),
            dag_node("left", &["source"]),
            dag_node("right", &["source"]),
            dag_node("output", &["left", "right"]),
        ];

        let counts = downstream_counts(&nodes);

        assert_eq!(counts["source"], 2); // left + right depend on source
        assert_eq!(counts["left"], 1); // only output depends on left
        assert_eq!(counts["right"], 1); // only output depends on right
        assert_eq!(counts["output"], 0); // leaf node
    }

    #[test]
    fn downstream_counts_independent_nodes() {
        let nodes = vec![dag_node("a", &[]), dag_node("b", &[]), dag_node("c", &[])];

        let counts = downstream_counts(&nodes);

        assert_eq!(counts["a"], 0);
        assert_eq!(counts["b"], 0);
        assert_eq!(counts["c"], 0);
    }

    #[test]
    fn downstream_counts_fan_out() {
        let nodes = vec![
            dag_node("hub", &[]),
            dag_node("a", &["hub"]),
            dag_node("b", &["hub"]),
            dag_node("c", &["hub"]),
            dag_node("d", &["hub"]),
        ];

        let counts = downstream_counts(&nodes);

        assert_eq!(counts["hub"], 4);
    }

    // -- Aggregate project cost -----------------------------------------------

    #[test]
    fn aggregate_project_cost_basic() {
        let nodes = vec![dag_node("source", &[]), dag_node("staging", &["source"])];

        let mut base = HashMap::new();
        base.insert(
            "source".to_string(),
            TableStats {
                row_count: 100_000,
                avg_row_bytes: 200,
            },
        );

        let estimates = propagate_costs(&nodes, &base, WarehouseType::Databricks).unwrap();
        let total = aggregate_project_cost(&estimates);

        assert!(total.estimated_rows > 0);
        assert!(total.estimated_compute_cost_usd > 0.0);
        // Total rows = source (100k) + staging (80k) = 180k
        assert_eq!(total.estimated_rows, 180_000);
    }
}
