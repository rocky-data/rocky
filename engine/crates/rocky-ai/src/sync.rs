//! Schema change detection and intent-guided model sync.
//!
//! Compares column schemas between compilations to detect changes,
//! then uses LLM + intent to propose updates to downstream models.

use rocky_compiler::compile::CompileResult;
use serde::{Deserialize, Serialize};

use crate::client::{AiError, LlmClient};

/// A detected schema change in a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    pub model: String,
    pub change_type: SchemaChangeType,
    pub details: String,
}

/// The type of schema change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaChangeType {
    ColumnAdded {
        name: String,
        data_type: String,
    },
    ColumnRemoved {
        name: String,
    },
    ColumnRenamed {
        old: String,
        new: String,
    },
    ColumnTypeChanged {
        name: String,
        old_type: String,
        new_type: String,
    },
}

/// A proposed update to a model based on intent and upstream changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncProposal {
    pub model: String,
    pub intent: String,
    pub current_source: String,
    pub proposed_source: String,
    pub upstream_changes: Vec<SchemaChange>,
    pub diff: String,
}

/// Detect schema changes between two compilation results.
pub fn detect_schema_changes(
    previous: &CompileResult,
    current: &CompileResult,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    for (model_name, current_cols) in &current.type_check.typed_models {
        let prev_cols = match previous.type_check.typed_models.get(model_name) {
            Some(c) => c,
            None => {
                // Entirely new model
                changes.push(SchemaChange {
                    model: model_name.clone(),
                    change_type: SchemaChangeType::ColumnAdded {
                        name: "*".to_string(),
                        data_type: "new model".to_string(),
                    },
                    details: format!(
                        "New model '{model_name}' with {} columns",
                        current_cols.len()
                    ),
                });
                continue;
            }
        };

        // Detect added columns
        for col in current_cols {
            if !prev_cols.iter().any(|p| p.name == col.name) {
                // Check if this might be a rename (same type, old column missing)
                let possible_rename = prev_cols.iter().find(|p| {
                    p.data_type == col.data_type
                        && !current_cols.iter().any(|c| c.name == p.name)
                        && strsim::jaro_winkler(&p.name, &col.name) > 0.6
                });

                if let Some(old_col) = possible_rename {
                    changes.push(SchemaChange {
                        model: model_name.clone(),
                        change_type: SchemaChangeType::ColumnRenamed {
                            old: old_col.name.clone(),
                            new: col.name.clone(),
                        },
                        details: format!(
                            "Column '{}' likely renamed to '{}' in '{}'",
                            old_col.name, col.name, model_name
                        ),
                    });
                } else {
                    changes.push(SchemaChange {
                        model: model_name.clone(),
                        change_type: SchemaChangeType::ColumnAdded {
                            name: col.name.clone(),
                            data_type: format!("{:?}", col.data_type),
                        },
                        details: format!(
                            "Column '{}' ({:?}) added to '{}'",
                            col.name, col.data_type, model_name
                        ),
                    });
                }
            }
        }

        // Detect removed columns (skip those already handled as renames)
        let renamed_old: Vec<String> = changes
            .iter()
            .filter_map(|c| match &c.change_type {
                SchemaChangeType::ColumnRenamed { old, .. } if c.model == *model_name => {
                    Some(old.clone())
                }
                _ => None,
            })
            .collect();

        for col in prev_cols {
            if !current_cols.iter().any(|c| c.name == col.name)
                && !renamed_old.iter().any(|r| r == &col.name)
            {
                changes.push(SchemaChange {
                    model: model_name.clone(),
                    change_type: SchemaChangeType::ColumnRemoved {
                        name: col.name.clone(),
                    },
                    details: format!("Column '{}' removed from '{}'", col.name, model_name),
                });
            }
        }

        // Detect type changes
        for col in current_cols {
            if let Some(prev_col) = prev_cols.iter().find(|p| p.name == col.name) {
                if prev_col.data_type != col.data_type
                    && col.data_type != rocky_compiler::types::RockyType::Unknown
                    && prev_col.data_type != rocky_compiler::types::RockyType::Unknown
                {
                    changes.push(SchemaChange {
                        model: model_name.clone(),
                        change_type: SchemaChangeType::ColumnTypeChanged {
                            name: col.name.clone(),
                            old_type: format!("{:?}", prev_col.data_type),
                            new_type: format!("{:?}", col.data_type),
                        },
                        details: format!(
                            "Column '{}' type changed from {:?} to {:?} in '{}'",
                            col.name, prev_col.data_type, col.data_type, model_name
                        ),
                    });
                }
            }
        }
    }

    // Detect removed models
    for model_name in previous.type_check.typed_models.keys() {
        if !current.type_check.typed_models.contains_key(model_name) {
            changes.push(SchemaChange {
                model: model_name.clone(),
                change_type: SchemaChangeType::ColumnRemoved {
                    name: "*".to_string(),
                },
                details: format!("Model '{model_name}' was removed"),
            });
        }
    }

    changes
}

/// Generate a sync proposal for a model using its intent and upstream changes.
pub async fn sync_model(
    model: &rocky_core::models::Model,
    upstream_changes: &[SchemaChange],
    client: &LlmClient,
    compile_result: &CompileResult,
) -> Result<SyncProposal, AiError> {
    let intent = model
        .config
        .intent
        .as_deref()
        .unwrap_or("No intent specified");

    // Build upstream schema context
    let mut upstream_context = String::new();
    if let Some(schema) = compile_result
        .semantic_graph
        .model_schema(&model.config.name)
    {
        for up_name in &schema.upstream {
            if let Some(cols) = compile_result.type_check.typed_models.get(up_name) {
                upstream_context.push_str(&format!("\nModel '{up_name}' columns:\n"));
                for col in cols {
                    upstream_context.push_str(&format!("  - {}: {:?}\n", col.name, col.data_type));
                }
            }
        }
    }

    // Build change description
    let changes_desc: Vec<String> = upstream_changes.iter().map(|c| c.details.clone()).collect();

    let system = format!(
        "You are a SQL transformation expert. You update Rocky/SQL models when upstream schemas change.\n\
         \n\
         Current upstream schemas:{upstream_context}\n\
         \n\
         Schema changes:\n{}\n\
         \n\
         Rules:\n\
         - Preserve the model's intent\n\
         - Only change what's necessary to accommodate the schema changes\n\
         - Return ONLY the updated source code, no explanations",
        changes_desc.join("\n")
    );

    let user = format!(
        "Model intent: {intent}\n\nCurrent source:\n```\n{}\n```\n\nUpdate this model to accommodate the schema changes.",
        model.sql
    );

    let response = client.generate(&system, &user, None).await?;
    let proposed = crate::generate::extract_code(&response.content);

    // Generate a simple diff
    let diff = generate_diff(&model.sql, &proposed);

    Ok(SyncProposal {
        model: model.config.name.clone(),
        intent: intent.to_string(),
        current_source: model.sql.clone(),
        proposed_source: proposed,
        upstream_changes: upstream_changes.to_vec(),
        diff,
    })
}

/// Generate a simple unified diff between two strings.
fn generate_diff(old: &str, new: &str) -> String {
    let mut diff = String::new();
    diff.push_str("--- current\n+++ proposed\n");

    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    // Simple line-by-line diff
    let max_len = old_lines.len().max(new_lines.len());
    for i in 0..max_len {
        let old_line = old_lines.get(i).copied().unwrap_or("");
        let new_line = new_lines.get(i).copied().unwrap_or("");

        if old_line == new_line {
            diff.push_str(&format!(" {old_line}\n"));
        } else {
            if i < old_lines.len() {
                diff.push_str(&format!("-{old_line}\n"));
            }
            if i < new_lines.len() {
                diff.push_str(&format!("+{new_line}\n"));
            }
        }
    }

    diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_compiler::types::{RockyType, TypedColumn};

    fn make_cols(names_types: &[(&str, RockyType)]) -> Vec<TypedColumn> {
        names_types
            .iter()
            .map(|(name, ty)| TypedColumn {
                name: name.to_string(),
                data_type: ty.clone(),
                nullable: true,
            })
            .collect()
    }

    #[test]
    fn test_detect_column_added() {
        let mut prev = empty_result();
        prev.type_check
            .typed_models
            .insert("orders".into(), make_cols(&[("id", RockyType::Int64)]));

        let mut curr = empty_result();
        curr.type_check.typed_models.insert(
            "orders".into(),
            make_cols(&[("id", RockyType::Int64), ("total", RockyType::Float64)]),
        );

        let changes = detect_schema_changes(&prev, &curr);
        assert!(changes
            .iter()
            .any(|c| matches!(&c.change_type, SchemaChangeType::ColumnAdded { name, .. } if name == "total")));
    }

    #[test]
    fn test_detect_column_removed() {
        let mut prev = empty_result();
        prev.type_check.typed_models.insert(
            "orders".into(),
            make_cols(&[("id", RockyType::Int64), ("old_col", RockyType::String)]),
        );

        let mut curr = empty_result();
        curr.type_check
            .typed_models
            .insert("orders".into(), make_cols(&[("id", RockyType::Int64)]));

        let changes = detect_schema_changes(&prev, &curr);
        assert!(changes
            .iter()
            .any(|c| matches!(&c.change_type, SchemaChangeType::ColumnRemoved { name } if name == "old_col")));
    }

    #[test]
    fn test_detect_type_changed() {
        let mut prev = empty_result();
        prev.type_check
            .typed_models
            .insert("orders".into(), make_cols(&[("amount", RockyType::Int64)]));

        let mut curr = empty_result();
        curr.type_check.typed_models.insert(
            "orders".into(),
            make_cols(&[("amount", RockyType::Float64)]),
        );

        let changes = detect_schema_changes(&prev, &curr);
        assert!(changes
            .iter()
            .any(|c| matches!(&c.change_type, SchemaChangeType::ColumnTypeChanged { name, .. } if name == "amount")));
    }

    #[test]
    fn test_no_changes() {
        let mut prev = empty_result();
        prev.type_check
            .typed_models
            .insert("orders".into(), make_cols(&[("id", RockyType::Int64)]));

        let curr = CompileResult {
            type_check: rocky_compiler::typecheck::TypeCheckResult {
                typed_models: prev.type_check.typed_models.clone(),
                diagnostics: vec![],
                reference_map: rocky_compiler::typecheck::ReferenceMap::default(),
                model_typecheck_ms: std::collections::HashMap::new(),
            },
            ..empty_result()
        };

        let changes = detect_schema_changes(&prev, &curr);
        assert!(changes.is_empty());
    }

    fn empty_result() -> CompileResult {
        use indexmap::IndexMap;
        use rocky_compiler::semantic::SemanticGraph;
        CompileResult {
            project: rocky_compiler::project::Project {
                models: vec![],
                dag_nodes: vec![],
                execution_order: vec![],
                layers: vec![],
                lineage_cache: std::collections::HashMap::new(),
                resolve_diagnostics: vec![],
                unified_dag: None,
            },
            semantic_graph: SemanticGraph::new(IndexMap::new(), vec![]),
            type_check: rocky_compiler::typecheck::TypeCheckResult {
                typed_models: IndexMap::new(),
                diagnostics: vec![],
                reference_map: rocky_compiler::typecheck::ReferenceMap::default(),
                model_typecheck_ms: std::collections::HashMap::new(),
            },
            contract_diagnostics: vec![],
            diagnostics: vec![],
            has_errors: false,
            timings: rocky_compiler::compile::PhaseTimings::default(),
            model_timings: std::collections::HashMap::new(),
        }
    }
}
