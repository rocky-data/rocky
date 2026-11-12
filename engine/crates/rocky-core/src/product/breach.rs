//! The open volume breach of a product's output model.
//!
//! An `error` volume breach (`[product.output.volume] severity = "error"`)
//! fails the model in the run that wrote the rows. The rows stay in the table.
//! This marker keeps the model failed in later runs, until the breached rows
//! are rebuilt or someone clears the marker. See `rocky run`'s volume check.
//!
//! One marker per `(product, model)`, in the replicated
//! `product_volume_breaches` state table (schema v36).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The separator in a marker key. A product name is `[A-Za-z0-9_]` and a
/// model name is a validated identifier, so neither holds a `|`.
const KEY_SEPARATOR: char = '|';

/// The state key of the marker for `model` of `product`: `<product>|<model>`.
pub fn breach_key(product: &str, model: &str) -> String {
    format!("{product}{KEY_SEPARATOR}{model}")
}

/// The prefix every marker key of `product` starts with: `<product>|`.
pub fn breach_key_prefix(product: &str) -> String {
    format!("{product}{KEY_SEPARATOR}")
}

/// An open volume breach: the output model is held failed until it is
/// rebuilt.
///
/// Every field but `product` and `model` has a serde default, so a record
/// written by a later engine that drops or adds a field still reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeBreachMarker {
    /// The product that declares the range.
    pub product: String,
    /// The product's output model.
    pub model: String,
    /// When the marker was first opened.
    #[serde(default = "epoch")]
    pub opened_at: DateTime<Utc>,
    /// The run that opened the marker, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// For a `time_interval` model, the partition keys still breached.
    /// Empty means the whole output (every other strategy).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<String>,
    /// The row count of the newest breach, when it was counted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// The declared `min_rows` at the newest breach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
    /// The declared `max_rows` at the newest breach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// The newest breach, in words.
    #[serde(default)]
    pub reason: String,
}

fn epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

impl VolumeBreachMarker {
    /// True when the marker covers the whole output, not named partitions.
    pub fn is_whole_output(&self) -> bool {
        self.partitions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_joins_product_and_model() {
        assert_eq!(breach_key("p", "m"), "p|m");
        assert!(breach_key("p", "m").starts_with(&breach_key_prefix("p")));
        assert!(!breach_key("pp", "m").starts_with(&breach_key_prefix("p")));
    }

    #[test]
    fn a_sparse_record_reads_with_defaults() {
        let marker: VolumeBreachMarker =
            serde_json::from_str(r#"{"product":"p","model":"m"}"#).unwrap();
        assert!(marker.is_whole_output());
        assert_eq!(marker.rows, None);
        assert_eq!(marker.reason, "");
    }
}
