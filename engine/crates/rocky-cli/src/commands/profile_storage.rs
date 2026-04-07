//! `rocky profile-storage` — storage profiling and encoding recommendations.

use anyhow::Result;

use crate::output::{EncodingRecommendationOutput, ProfileStorageOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Column encoding recommendation. This is the internal/local shape used
/// while building the recommendation; the public CLI output type is
/// `EncodingRecommendationOutput` in `crate::output`.
#[derive(Debug, Clone)]
struct EncodingRecommendation {
    column: String,
    data_type: String,
    estimated_cardinality: String,
    recommended_encoding: String,
    reasoning: String,
}

/// Execute `rocky profile-storage`.
pub fn run_profile_storage(model: &str, output_json: bool) -> Result<()> {
    // Generate SQL that would collect storage profile data
    let profile_sql = generate_storage_profile_sql(model);
    let recommendations = example_encoding_recommendations(model);

    if output_json {
        let typed_recs: Vec<EncodingRecommendationOutput> = recommendations
            .iter()
            .map(|r| EncodingRecommendationOutput {
                column: r.column.clone(),
                data_type: r.data_type.clone(),
                estimated_cardinality: r.estimated_cardinality.clone(),
                recommended_encoding: r.recommended_encoding.clone(),
                reasoning: r.reasoning.clone(),
            })
            .collect();
        let output = ProfileStorageOutput {
            version: VERSION.to_string(),
            command: "profile-storage".to_string(),
            model: model.to_string(),
            profile_sql: profile_sql.clone(),
            recommendations: typed_recs,
        };
        print_json(&output)?;
    } else {
        println!("Storage profile for: {model}");
        println!();
        println!("SQL to collect storage metrics:");
        println!("{profile_sql}");
        println!();

        if !recommendations.is_empty() {
            println!(
                "{:<25} {:<15} {:<15} {:<20} REASONING",
                "COLUMN", "TYPE", "CARDINALITY", "ENCODING"
            );
            println!("{}", "-".repeat(95));

            for rec in &recommendations {
                println!(
                    "{:<25} {:<15} {:<15} {:<20} {}",
                    rec.column,
                    rec.data_type,
                    rec.estimated_cardinality,
                    rec.recommended_encoding,
                    rec.reasoning,
                );
            }
        }
    }

    Ok(())
}

/// Generates SQL to collect storage profile metrics (file sizes, partitions).
fn generate_storage_profile_sql(model: &str) -> String {
    format!(
        "DESCRIBE DETAIL {model};\n\
         DESCRIBE HISTORY {model} LIMIT 10;\n\
         SELECT\n\
           COUNT(*) AS num_files,\n\
           SUM(size) AS total_bytes,\n\
           AVG(size) AS avg_file_bytes,\n\
           MIN(size) AS min_file_bytes,\n\
           MAX(size) AS max_file_bytes\n\
         FROM (DESCRIBE DETAIL {model})"
    )
}

/// Recommend column encodings based on known type/cardinality patterns.
fn recommend_encoding(column: &str, data_type: &str, cardinality: &str) -> EncodingRecommendation {
    let (encoding, reasoning) = match (data_type, cardinality) {
        ("STRING", "low") | ("VARCHAR", "low") => (
            "dictionary",
            "low cardinality string benefits from dictionary encoding",
        ),
        ("STRING", "high") | ("VARCHAR", "high") => {
            ("plain", "high cardinality string should use plain encoding")
        }
        ("TIMESTAMP", _) => (
            "delta + run-length",
            "timestamps with sequential values compress well with delta encoding",
        ),
        ("INT" | "INTEGER" | "BIGINT", "low") => (
            "run-length",
            "low cardinality integers benefit from run-length encoding",
        ),
        ("BOOLEAN", _) => (
            "bit-packed",
            "boolean values are best stored with bit-packing",
        ),
        _ => (
            "plain",
            "default encoding for this type/cardinality combination",
        ),
    };

    EncodingRecommendation {
        column: column.to_string(),
        data_type: data_type.to_string(),
        estimated_cardinality: cardinality.to_string(),
        recommended_encoding: encoding.to_string(),
        reasoning: reasoning.to_string(),
    }
}

/// Example recommendations based on common patterns (actual data would come from profile).
fn example_encoding_recommendations(model: &str) -> Vec<EncodingRecommendation> {
    // In a real implementation, these would be derived from actual profiling data.
    // For now, provide the recommendation engine that callers can feed data into.
    let _ = model;
    vec![
        recommend_encoding("status", "STRING", "low"),
        recommend_encoding("created_at", "TIMESTAMP", "sequential"),
        recommend_encoding("id", "BIGINT", "high"),
        recommend_encoding("is_active", "BOOLEAN", "binary"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_profile_sql() {
        let sql = generate_storage_profile_sql("catalog.schema.orders");
        assert!(sql.contains("DESCRIBE DETAIL catalog.schema.orders"));
        assert!(sql.contains("DESCRIBE HISTORY"));
        assert!(sql.contains("num_files"));
    }

    #[test]
    fn test_encoding_low_cardinality_string() {
        let rec = recommend_encoding("status", "STRING", "low");
        assert_eq!(rec.recommended_encoding, "dictionary");
    }

    #[test]
    fn test_encoding_timestamp() {
        let rec = recommend_encoding("created_at", "TIMESTAMP", "sequential");
        assert!(rec.recommended_encoding.contains("delta"));
    }

    #[test]
    fn test_encoding_boolean() {
        let rec = recommend_encoding("is_active", "BOOLEAN", "binary");
        assert_eq!(rec.recommended_encoding, "bit-packed");
    }
}
