//! Poison pill detection for models that consistently fail.
//!
//! When a model fails N consecutive times across runs, it is marked as
//! "poisoned" in the state store. Poisoned models are skipped on subsequent
//! runs (with a warning) unless `--include-poisoned` is passed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Default number of consecutive failures before a model is poisoned.
pub const DEFAULT_POISON_THRESHOLD: u32 = 3;

/// Tracks consecutive failure history for a single model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoisonRecord {
    /// Model name (fully qualified).
    pub model: String,
    /// Number of consecutive failures.
    pub consecutive_failures: u32,
    /// Whether the model is currently poisoned.
    pub poisoned: bool,
    /// Last error message.
    pub last_error: String,
    /// When the model was first marked as poisoned (None if not poisoned).
    pub poisoned_at: Option<DateTime<Utc>>,
    /// When the last failure occurred.
    pub last_failure_at: DateTime<Utc>,
}

impl PoisonRecord {
    /// Record a failure for this model.
    pub fn record_failure(&mut self, error: &str, threshold: u32) {
        self.consecutive_failures += 1;
        self.last_error = error.to_string();
        self.last_failure_at = Utc::now();
        if self.consecutive_failures >= threshold && !self.poisoned {
            self.poisoned = true;
            self.poisoned_at = Some(Utc::now());
        }
    }

    /// Record a success — resets failure count and clears poison status.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.poisoned = false;
        self.poisoned_at = None;
        self.last_error.clear();
    }

    /// Create a new record for a model's first failure.
    pub fn first_failure(model: &str, error: &str, threshold: u32) -> Self {
        let mut record = Self {
            model: model.to_string(),
            consecutive_failures: 0,
            poisoned: false,
            last_error: String::new(),
            poisoned_at: None,
            last_failure_at: Utc::now(),
        };
        record.record_failure(error, threshold);
        record
    }
}

/// Manages poison state for all models.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoisonRegistry {
    pub records: std::collections::HashMap<String, PoisonRecord>,
    pub threshold: u32,
}

impl PoisonRegistry {
    pub fn new(threshold: u32) -> Self {
        Self {
            records: std::collections::HashMap::new(),
            threshold,
        }
    }

    /// Record a failure for a model.
    pub fn record_failure(&mut self, model: &str, error: &str) {
        self.records
            .entry(model.to_string())
            .and_modify(|r| r.record_failure(error, self.threshold))
            .or_insert_with(|| PoisonRecord::first_failure(model, error, self.threshold));
    }

    /// Record a success for a model.
    pub fn record_success(&mut self, model: &str) {
        if let Some(record) = self.records.get_mut(model) {
            record.record_success();
        }
    }

    /// Check if a model is poisoned.
    pub fn is_poisoned(&self, model: &str) -> bool {
        self.records.get(model).is_some_and(|r| r.poisoned)
    }

    /// Get all poisoned models.
    pub fn poisoned_models(&self) -> Vec<&PoisonRecord> {
        self.records.values().filter(|r| r.poisoned).collect()
    }

    /// Clear poison status for a specific model (manual override).
    pub fn clear_poison(&mut self, model: &str) {
        if let Some(record) = self.records.get_mut(model) {
            record.poisoned = false;
            record.poisoned_at = None;
            record.consecutive_failures = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poison_after_threshold() {
        let mut registry = PoisonRegistry::new(3);
        let model = "my_catalog.my_schema.my_model";

        registry.record_failure(model, "timeout");
        assert!(!registry.is_poisoned(model));
        registry.record_failure(model, "timeout");
        assert!(!registry.is_poisoned(model));
        registry.record_failure(model, "timeout");
        assert!(registry.is_poisoned(model));
    }

    #[test]
    fn test_success_clears_poison() {
        let mut registry = PoisonRegistry::new(2);
        let model = "m";
        registry.record_failure(model, "err");
        registry.record_failure(model, "err");
        assert!(registry.is_poisoned(model));

        registry.record_success(model);
        assert!(!registry.is_poisoned(model));
    }

    #[test]
    fn test_success_resets_counter() {
        let mut registry = PoisonRegistry::new(3);
        let model = "m";
        registry.record_failure(model, "err");
        registry.record_failure(model, "err");
        registry.record_success(model);
        registry.record_failure(model, "err");
        assert!(!registry.is_poisoned(model));
    }
}
