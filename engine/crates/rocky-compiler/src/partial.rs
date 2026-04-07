//! Partial compilation results for graceful degradation.
//!
//! When the compiler encounters errors in some models, it continues
//! compiling the rest and returns a `PartialCompileResult` containing
//! both the successfully compiled models and the errors. This enables
//! the LSP to provide completions and hover for working models even
//! when others have errors.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A compilation result that can be partially successful.
///
/// Instead of fail-fast, the compiler records per-model errors and
/// continues processing independent models. This enables the LSP to
/// provide features for valid models while showing diagnostics for
/// broken ones.
#[derive(Debug, Clone, Default)]
pub struct PartialCompileResult {
    /// Models that compiled successfully, keyed by model name.
    pub successful: HashMap<String, CompiledModel>,
    /// Models that failed compilation, keyed by model name.
    pub errors: HashMap<String, CompileError>,
    /// Total number of models attempted.
    pub total_models: usize,
}

/// A successfully compiled model's metadata (for LSP features).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledModel {
    /// Model name.
    pub name: String,
    /// Output columns with types (for hover/completions).
    pub columns: Vec<ColumnMeta>,
    /// Dependencies (for go-to-definition).
    pub depends_on: Vec<String>,
    /// File path (for navigation).
    pub file_path: String,
}

/// Column metadata from a compiled model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// A compilation error with location information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileError {
    /// Model name.
    pub model: String,
    /// Error code (e.g., "E001").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// File path.
    pub file_path: String,
    /// Line number (1-based).
    pub line: Option<usize>,
    /// Column number (1-based).
    pub column: Option<usize>,
    /// Suggested fix (if available).
    pub suggestion: Option<String>,
}

impl PartialCompileResult {
    /// Create a new empty result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful compilation.
    pub fn add_success(&mut self, model: CompiledModel) {
        self.successful.insert(model.name.clone(), model);
    }

    /// Record a compilation error.
    pub fn add_error(&mut self, error: CompileError) {
        self.errors.insert(error.model.clone(), error);
    }

    /// Returns true if any models compiled successfully.
    pub fn has_successes(&self) -> bool {
        !self.successful.is_empty()
    }

    /// Returns true if there were any errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Returns the success rate as a percentage.
    pub fn success_rate(&self) -> f64 {
        if self.total_models == 0 {
            return 100.0;
        }
        (self.successful.len() as f64 / self.total_models as f64) * 100.0
    }

    /// Get a compiled model by name (for LSP lookups).
    pub fn get_model(&self, name: &str) -> Option<&CompiledModel> {
        self.successful.get(name)
    }

    /// Get all column names across all successful models (for completions).
    pub fn all_columns(&self) -> Vec<&str> {
        let mut columns: Vec<&str> = self
            .successful
            .values()
            .flat_map(|m| m.columns.iter().map(|c| c.name.as_str()))
            .collect();
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    /// Get all model names (successful ones — for ref completions).
    pub fn all_model_names(&self) -> Vec<&str> {
        self.successful.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_result_mixed() {
        let mut result = PartialCompileResult::new();
        result.total_models = 3;

        result.add_success(CompiledModel {
            name: "model_a".into(),
            columns: vec![ColumnMeta {
                name: "id".into(),
                data_type: "INT".into(),
                nullable: false,
            }],
            depends_on: vec![],
            file_path: "models/a.sql".into(),
        });

        result.add_success(CompiledModel {
            name: "model_b".into(),
            columns: vec![ColumnMeta {
                name: "name".into(),
                data_type: "STRING".into(),
                nullable: true,
            }],
            depends_on: vec!["model_a".into()],
            file_path: "models/b.sql".into(),
        });

        result.add_error(CompileError {
            model: "model_c".into(),
            code: "E001".into(),
            message: "unknown column 'foo'".into(),
            file_path: "models/c.sql".into(),
            line: Some(5),
            column: Some(10),
            suggestion: Some("did you mean 'id'?".into()),
        });

        assert!(result.has_successes());
        assert!(result.has_errors());
        assert_eq!(result.successful.len(), 2);
        assert_eq!(result.errors.len(), 1);
        assert!((result.success_rate() - 66.67).abs() < 1.0);
    }

    #[test]
    fn test_all_columns() {
        let mut result = PartialCompileResult::new();
        result.add_success(CompiledModel {
            name: "a".into(),
            columns: vec![
                ColumnMeta {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "name".into(),
                    data_type: "STRING".into(),
                    nullable: true,
                },
            ],
            depends_on: vec![],
            file_path: "a.sql".into(),
        });
        result.add_success(CompiledModel {
            name: "b".into(),
            columns: vec![
                ColumnMeta {
                    name: "id".into(),
                    data_type: "INT".into(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "amount".into(),
                    data_type: "FLOAT".into(),
                    nullable: false,
                },
            ],
            depends_on: vec![],
            file_path: "b.sql".into(),
        });

        let cols = result.all_columns();
        assert_eq!(cols, vec!["amount", "id", "name"]); // sorted, deduped
    }

    #[test]
    fn test_empty_result() {
        let result = PartialCompileResult::new();
        assert!(!result.has_successes());
        assert!(!result.has_errors());
        assert_eq!(result.success_rate(), 100.0);
    }
}
