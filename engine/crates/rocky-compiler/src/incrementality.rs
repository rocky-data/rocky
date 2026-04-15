//! Inferred incrementality detection.
//!
//! Analyzes a compiled model's column schema, SQL, and current strategy to
//! determine whether the model is a candidate for incremental materialization.
//! When a model is currently using `full_refresh` and contains columns that
//! look monotonic (timestamps, dates, auto-incrementing IDs), Rocky can
//! suggest switching to an incremental strategy with a recommended watermark
//! column.
//!
//! The detection is heuristic-based:
//! - **Name patterns**: columns ending in `_at`, `_date`, `_timestamp`,
//!   `_synced`, or named `id` / `*_id`.
//! - **Type signals**: temporal types (`Date`, `Timestamp`, `TimestampNtz`)
//!   and integer types (`Int32`, `Int64`) for ID columns.
//! - **SQL context**: the candidate column appearing in `ORDER BY` or in
//!   `WHERE` with comparison operators (`>`, `>=`, `<`, `<=`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{RockyType, TypedColumn};
use rocky_core::models::StrategyConfig;

/// Confidence level for an incrementality recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    /// Multiple strong signals (name + type + SQL context).
    High,
    /// Two signals (e.g., name + type, or name + SQL context).
    Medium,
    /// Single signal (e.g., name pattern only).
    Low,
}

/// A hint that a model could benefit from incremental materialization.
///
/// Returned by [`infer_incrementality`] when a `full_refresh` model has
/// columns that look monotonic. Surfaced in `rocky compile --output json`
/// as part of each model's detail.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IncrementalityHint {
    /// Whether the model is a candidate for incremental materialization.
    pub is_candidate: bool,
    /// The column recommended as the watermark / timestamp column.
    pub recommended_column: String,
    /// How confident the detector is in this recommendation.
    pub confidence: Confidence,
    /// Human-readable reasons why this column was chosen.
    pub signals: Vec<String>,
}

/// Well-known column name suffixes that indicate a monotonic timestamp.
const TIMESTAMP_SUFFIXES: &[&str] = &["_at", "_date", "_timestamp", "_synced"];

/// Well-known exact column names that are strong timestamp candidates.
const TIMESTAMP_EXACT_NAMES: &[&str] = &[
    "created_at",
    "updated_at",
    "inserted_at",
    "modified_at",
    "loaded_at",
    "_fivetran_synced",
    "event_time",
    "event_timestamp",
    "ingested_at",
    "synced_at",
];

/// Score awarded for each signal match during candidate evaluation.
struct CandidateScore {
    score: u8,
    signals: Vec<String>,
}

/// Analyze a compiled model and infer whether it could be incrementalized.
///
/// Returns `None` when:
/// - The model already uses an incremental-family strategy (`Incremental`,
///   `Merge`, `TimeInterval`, `Microbatch`, `DeleteInsert`).
/// - No candidate columns are found.
///
/// When multiple candidate columns score equally, the function prefers
/// well-known names (`_fivetran_synced`, `updated_at`, `created_at`) over
/// generic suffix matches.
pub fn infer_incrementality(
    model_name: &str,
    columns: &[TypedColumn],
    sql: &str,
    current_strategy: &StrategyConfig,
) -> Option<IncrementalityHint> {
    // Only suggest for full_refresh models.
    if !matches!(current_strategy, StrategyConfig::FullRefresh) {
        return None;
    }

    let sql_lower = sql.to_lowercase();

    let mut candidates: Vec<(String, CandidateScore)> = Vec::new();

    for col in columns {
        let col_lower = col.name.to_lowercase();
        let mut score = 0u8;
        let mut signals = Vec::new();

        // Signal 1: Name pattern match
        let name_match = check_name_pattern(&col_lower);
        if let Some(reason) = name_match {
            score += 2;
            signals.push(reason);
        }

        // Signal 2: Type match
        let type_match = check_type_signal(&col.data_type, &col_lower);
        if let Some(reason) = type_match {
            score += 2;
            signals.push(reason);
        }

        // Signal 3: SQL context (ORDER BY or WHERE with comparison)
        let sql_match = check_sql_context(&col_lower, &sql_lower);
        if let Some(reason) = sql_match {
            score += 1;
            signals.push(reason);
        }

        // Only consider columns with at least one signal
        if score > 0 {
            candidates.push((col.name.clone(), CandidateScore { score, signals }));
        }
    }

    if candidates.is_empty() {
        tracing::debug!(model = model_name, "no incrementality candidates found");
        return None;
    }

    // Sort by score descending, then by preference for well-known names.
    candidates.sort_by(|a, b| {
        b.1.score
            .cmp(&a.1.score)
            .then_with(|| well_known_priority(&a.0).cmp(&well_known_priority(&b.0)))
    });

    let (recommended_column, best) = candidates.into_iter().next()?;
    let confidence = match best.score {
        5 => Confidence::High,   // name + type + SQL context
        4 => Confidence::High,   // name + type
        3 => Confidence::Medium, // name + SQL context, or type + SQL context
        2 => Confidence::Medium, // name only or type only (each worth 2)
        _ => Confidence::Low,    // single weak signal
    };

    tracing::info!(
        model = model_name,
        column = %recommended_column,
        ?confidence,
        signals = ?best.signals,
        "incrementality candidate detected"
    );

    Some(IncrementalityHint {
        is_candidate: true,
        recommended_column,
        confidence,
        signals: best.signals,
    })
}

/// Check if the column name matches known monotonic patterns.
fn check_name_pattern(col_lower: &str) -> Option<String> {
    // Exact well-known names (strongest signal).
    for &name in TIMESTAMP_EXACT_NAMES {
        if col_lower == name {
            return Some(format!(
                "column name '{col_lower}' is a well-known monotonic column"
            ));
        }
    }

    // Suffix patterns.
    for &suffix in TIMESTAMP_SUFFIXES {
        if col_lower.ends_with(suffix) {
            return Some(format!(
                "column name '{col_lower}' ends with '{suffix}' (timestamp pattern)"
            ));
        }
    }

    // ID columns — only match `id` exactly or `*_id` suffix.
    if col_lower == "id" || col_lower.ends_with("_id") {
        return Some(format!(
            "column name '{col_lower}' matches auto-increment ID pattern"
        ));
    }

    None
}

/// Check if the column type is consistent with a monotonic watermark.
fn check_type_signal(data_type: &RockyType, col_lower: &str) -> Option<String> {
    if data_type.is_temporal() {
        return Some(format!(
            "column type is temporal ({data_type:?}), suitable as a watermark"
        ));
    }

    // Integer types are only a signal for ID-like columns.
    if data_type.is_integer() && (col_lower == "id" || col_lower.ends_with("_id")) {
        return Some(format!(
            "column type is integer ({data_type:?}), suitable as an auto-incrementing ID watermark"
        ));
    }

    None
}

/// Check if the column appears in SQL patterns that suggest monotonic usage.
fn check_sql_context(col_lower: &str, sql_lower: &str) -> Option<String> {
    // Check ORDER BY
    if sql_lower.contains(&format!("order by {col_lower}"))
        || sql_lower.contains(&format!("order by\n{col_lower}"))
        || sql_lower.contains(&format!("order by {col_lower} asc"))
        || sql_lower.contains(&format!("order by {col_lower} desc"))
    {
        return Some(format!(
            "column '{col_lower}' appears in ORDER BY (suggests monotonic ordering)"
        ));
    }

    // Check WHERE with comparison operators
    let comparison_patterns = [
        format!("{col_lower} >"),
        format!("{col_lower} >="),
        format!("{col_lower} <"),
        format!("{col_lower} <="),
        format!("> {col_lower}"),
        format!(">= {col_lower}"),
        format!("< {col_lower}"),
        format!("<= {col_lower}"),
    ];

    for pattern in &comparison_patterns {
        if sql_lower.contains(pattern) {
            return Some(format!(
                "column '{col_lower}' used with comparison operator in SQL (suggests range filtering)"
            ));
        }
    }

    None
}

/// Priority for well-known column names (lower = higher priority).
///
/// Used as a tie-breaker when multiple candidates have the same score.
fn well_known_priority(col_name: &str) -> u8 {
    let lower = col_name.to_lowercase();
    match lower.as_str() {
        "_fivetran_synced" => 0,
        "updated_at" => 1,
        "modified_at" => 2,
        "created_at" => 3,
        "inserted_at" => 4,
        "loaded_at" | "ingested_at" | "synced_at" => 5,
        "event_time" | "event_timestamp" => 6,
        _ if lower.ends_with("_at") || lower.ends_with("_timestamp") => 7,
        _ if lower.ends_with("_date") || lower.ends_with("_synced") => 8,
        _ => 10,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_column(name: &str, data_type: RockyType) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type,
            nullable: true,
        }
    }

    #[test]
    fn test_full_refresh_with_timestamp_column() {
        let columns = vec![
            make_column("id", RockyType::Int64),
            make_column("name", RockyType::String),
            make_column("created_at", RockyType::Timestamp),
        ];
        let sql = "SELECT id, name, created_at FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert!(hint.is_candidate);
        assert_eq!(hint.recommended_column, "created_at");
        assert!(matches!(hint.confidence, Confidence::High));
    }

    #[test]
    fn test_already_incremental_returns_none() {
        let columns = vec![make_column("updated_at", RockyType::Timestamp)];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::Incremental {
            timestamp_column: "updated_at".to_string(),
        };

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_none());
    }

    #[test]
    fn test_already_merge_returns_none() {
        let columns = vec![make_column("updated_at", RockyType::Timestamp)];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::Merge {
            unique_key: vec!["id".to_string()],
            update_columns: None,
        };

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_none());
    }

    #[test]
    fn test_no_candidates_returns_none() {
        let columns = vec![
            make_column("name", RockyType::String),
            make_column("amount", RockyType::Float64),
        ];
        let sql = "SELECT name, amount FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_none());
    }

    #[test]
    fn test_fivetran_synced_highest_priority() {
        let columns = vec![
            make_column("created_at", RockyType::Timestamp),
            make_column("_fivetran_synced", RockyType::Timestamp),
        ];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        // Both have the same score (name + type = 4), but _fivetran_synced
        // wins on tie-break priority.
        assert_eq!(hint.recommended_column, "_fivetran_synced");
    }

    #[test]
    fn test_sql_context_boosts_confidence() {
        let columns = vec![
            make_column("event_time", RockyType::Timestamp),
            make_column("name", RockyType::String),
        ];
        let sql = "SELECT * FROM events WHERE event_time > '2024-01-01' ORDER BY event_time";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert_eq!(hint.recommended_column, "event_time");
        assert!(matches!(hint.confidence, Confidence::High));
        // Should have at least 3 signals: name, type, and SQL context
        assert!(hint.signals.len() >= 3);
    }

    #[test]
    fn test_id_column_with_integer_type() {
        let columns = vec![
            make_column("id", RockyType::Int64),
            make_column("value", RockyType::String),
        ];
        let sql = "SELECT id, value FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert_eq!(hint.recommended_column, "id");
        assert!(matches!(hint.confidence, Confidence::High));
    }

    #[test]
    fn test_id_column_with_string_type_low_confidence() {
        let columns = vec![
            make_column("user_id", RockyType::String),
            make_column("name", RockyType::String),
        ];
        let sql = "SELECT user_id, name FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert_eq!(hint.recommended_column, "user_id");
        // String ID — name match only, no type signal
        assert!(matches!(hint.confidence, Confidence::Medium));
    }

    #[test]
    fn test_prefers_updated_at_over_created_at() {
        let columns = vec![
            make_column("created_at", RockyType::Timestamp),
            make_column("updated_at", RockyType::Timestamp),
        ];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        // updated_at should win over created_at on tie-break priority.
        assert_eq!(hint.recommended_column, "updated_at");
    }

    #[test]
    fn test_date_type_column() {
        let columns = vec![
            make_column("order_date", RockyType::Date),
            make_column("total", RockyType::Float64),
        ];
        let sql = "SELECT order_date, total FROM orders";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_some());
        let hint = hint.unwrap();
        assert_eq!(hint.recommended_column, "order_date");
    }

    #[test]
    fn test_empty_columns_returns_none() {
        let columns: Vec<TypedColumn> = vec![];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::FullRefresh;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_none());
    }

    #[test]
    fn test_ephemeral_strategy_returns_none() {
        let columns = vec![make_column("updated_at", RockyType::Timestamp)];
        let sql = "SELECT * FROM source_table";
        let strategy = StrategyConfig::Ephemeral;

        let hint = infer_incrementality("test_model", &columns, sql, &strategy);
        assert!(hint.is_none());
    }

    #[test]
    fn test_confidence_levels() {
        // Name only (score=2) → Medium
        let cols = vec![make_column("created_at", RockyType::Unknown)];
        let hint =
            infer_incrementality("m", &cols, "SELECT *", &StrategyConfig::FullRefresh).unwrap();
        assert!(matches!(hint.confidence, Confidence::Medium));

        // Name + type (score=4) → High
        let cols = vec![make_column("created_at", RockyType::Timestamp)];
        let hint =
            infer_incrementality("m", &cols, "SELECT *", &StrategyConfig::FullRefresh).unwrap();
        assert!(matches!(hint.confidence, Confidence::High));

        // Name + type + SQL (score=5) → High
        let cols = vec![make_column("created_at", RockyType::Timestamp)];
        let hint = infer_incrementality(
            "m",
            &cols,
            "SELECT * ORDER BY created_at",
            &StrategyConfig::FullRefresh,
        )
        .unwrap();
        assert!(matches!(hint.confidence, Confidence::High));
    }

    #[test]
    fn test_check_name_pattern() {
        assert!(check_name_pattern("created_at").is_some());
        assert!(check_name_pattern("order_date").is_some());
        assert!(check_name_pattern("event_timestamp").is_some());
        assert!(check_name_pattern("_fivetran_synced").is_some());
        assert!(check_name_pattern("id").is_some());
        assert!(check_name_pattern("user_id").is_some());
        assert!(check_name_pattern("name").is_none());
        assert!(check_name_pattern("amount").is_none());
    }

    #[test]
    fn test_check_type_signal() {
        assert!(check_type_signal(&RockyType::Timestamp, "created_at").is_some());
        assert!(check_type_signal(&RockyType::Date, "order_date").is_some());
        assert!(check_type_signal(&RockyType::TimestampNtz, "updated_at").is_some());
        assert!(check_type_signal(&RockyType::Int64, "id").is_some());
        assert!(check_type_signal(&RockyType::Int64, "user_id").is_some());
        // Integer type on a non-ID column is not a signal.
        assert!(check_type_signal(&RockyType::Int64, "count").is_none());
        assert!(check_type_signal(&RockyType::String, "name").is_none());
    }

    #[test]
    fn test_check_sql_context() {
        assert!(check_sql_context("created_at", "select * order by created_at").is_some());
        assert!(check_sql_context("ts", "where ts > '2024-01-01'").is_some());
        assert!(check_sql_context("ts", "where ts >= '2024-01-01'").is_some());
        assert!(check_sql_context("amount", "select amount from t").is_none());
    }
}
