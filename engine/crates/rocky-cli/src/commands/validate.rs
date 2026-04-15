use std::path::Path;

use anyhow::Result;

use crate::output::{
    ValidateAdapterStatus, ValidateMessage, ValidateModelsStatus, ValidateOutput,
    ValidatePipelineStatus, print_json,
};

/// Execute `rocky validate` — check config without APIs.
///
/// Returns a structured `ValidateOutput` that can be rendered as text
/// (default) or JSON (`--output json`).
pub fn validate(config_path: &Path, json: bool) -> Result<()> {
    let output = validate_inner(config_path)?;

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    Ok(())
}

fn validate_inner(config_path: &Path) -> Result<ValidateOutput> {
    let mut out = ValidateOutput::new();

    // Check file exists. Return the typed ConfigError so the CLI
    // error reporter can upgrade it to a rich miette diagnostic with
    // `rocky init` / `rocky playground` hints.
    if !config_path.exists() {
        return Err(rocky_core::config::ConfigError::FileNotFound {
            path: config_path.to_path_buf(),
        }
        .into());
    }

    // Parse config (with env var substitution)
    let cfg = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            out.push(ValidateMessage {
                severity: "ok".into(),
                code: "V001".into(),
                message: "Config syntax valid (v2 format)".into(),
                file: Some(config_path.display().to_string()),
                field: None,
            });
            cfg
        }
        Err(e) => {
            out.push(ValidateMessage {
                severity: "error".into(),
                code: "V001".into(),
                message: format!("Failed to parse config: {e}"),
                file: Some(config_path.display().to_string()),
                field: None,
            });
            return Ok(out);
        }
    };

    // Validate adapters
    if cfg.adapters.is_empty() {
        out.push(ValidateMessage {
            severity: "warn".into(),
            code: "V010".into(),
            message: "No adapters defined".into(),
            file: None,
            field: Some("adapter".into()),
        });
    } else {
        for (name, adapter) in &cfg.adapters {
            let (ok, msgs) = validate_adapter(name, adapter);
            out.adapters.push(ValidateAdapterStatus {
                name: name.clone(),
                adapter_type: adapter.adapter_type.clone(),
                ok,
            });
            for msg in msgs {
                out.push(msg);
            }
        }
    }

    // Validate pipelines
    if cfg.pipelines.is_empty() {
        out.push(ValidateMessage {
            severity: "warn".into(),
            code: "V020".into(),
            message: "No pipelines defined".into(),
            file: None,
            field: Some("pipeline".into()),
        });
    } else {
        for (name, pc) in &cfg.pipelines {
            let pipeline_type = pc.pipeline_type_str().to_string();
            let (ok, msgs, strategy, catalog_template, schema_template) = match pc {
                rocky_core::config::PipelineConfig::Replication(pipeline) => {
                    let (ok, msgs) = validate_replication_pipeline(name, pipeline, &cfg);
                    (
                        ok,
                        msgs,
                        pipeline.strategy.clone(),
                        pipeline.target.catalog_template.clone(),
                        pipeline.target.schema_template.clone(),
                    )
                }
                rocky_core::config::PipelineConfig::Transformation(pipeline) => {
                    let (ok, msgs) =
                        validate_transformation_pipeline(name, pipeline, &cfg, config_path);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Quality(pipeline) => {
                    let (ok, msgs) = validate_quality_pipeline(name, pipeline, &cfg);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Snapshot(pipeline) => {
                    let (ok, msgs) = validate_snapshot_pipeline(name, pipeline, &cfg);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Load(_pipeline) => {
                    // Load pipeline validation not yet implemented
                    (true, vec![], String::new(), String::new(), String::new())
                }
            };
            out.pipelines.push(ValidatePipelineStatus {
                name: name.clone(),
                pipeline_type,
                strategy,
                catalog_template,
                schema_template,
                ok,
            });
            for msg in msgs {
                out.push(msg);
            }
        }

        // Validate pipeline dependency graph (depends_on)
        validate_pipeline_dag(&cfg, &mut out);
    }

    // Validate models directory if it exists
    let models_dir = config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("models");
    let loaded_models = if models_dir.exists() {
        match rocky_core::models::load_models_from_dir(&models_dir) {
            Ok(models) => {
                let count = models.len();
                if count > 0 {
                    out.push(ValidateMessage {
                        severity: "ok".into(),
                        code: "V030".into(),
                        message: format!("{count} transformation models loaded"),
                        file: None,
                        field: None,
                    });

                    // Validate DAG
                    let dag_nodes: Vec<_> = models
                        .iter()
                        .map(rocky_core::models::Model::to_dag_node)
                        .collect();
                    match rocky_core::dag::topological_sort(&dag_nodes) {
                        Ok(order) => {
                            out.models = ValidateModelsStatus {
                                found: true,
                                count,
                                dag_valid: true,
                            };
                            out.push(ValidateMessage {
                                severity: "ok".into(),
                                code: "V031".into(),
                                message: format!("DAG valid ({} nodes, no cycles)", order.len()),
                                file: None,
                                field: None,
                            });
                        }
                        Err(e) => {
                            out.models = ValidateModelsStatus {
                                found: true,
                                count,
                                dag_valid: false,
                            };
                            out.push(ValidateMessage {
                                severity: "error".into(),
                                code: "V031".into(),
                                message: format!("DAG error: {e}"),
                                file: None,
                                field: None,
                            });
                        }
                    }
                } else {
                    out.models = ValidateModelsStatus {
                        found: true,
                        count: 0,
                        dag_valid: true,
                    };
                }
                models
            }
            Err(e) => {
                out.models = ValidateModelsStatus {
                    found: true,
                    count: 0,
                    dag_valid: false,
                };
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!("Model loading error: {e}"),
                    file: None,
                    field: None,
                });
                vec![]
            }
        }
    } else {
        vec![]
    };

    // --- Lint rules ---
    lint_config(&cfg, &loaded_models, &mut out);

    Ok(out)
}

fn validate_adapter(
    name: &str,
    adapter: &rocky_core::config::AdapterConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    match adapter.adapter_type.as_str() {
        "databricks" => {
            if adapter.host.is_none() || adapter.host.as_deref() == Some("") {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V011".into(),
                    message: format!("adapter.{name}: host not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.host")),
                });
            }
            if adapter.http_path.is_none() || adapter.http_path.as_deref() == Some("") {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V012".into(),
                    message: format!("adapter.{name}: http_path not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.http_path")),
                });
            }
            if adapter.token.is_none() && adapter.client_id.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V013".into(),
                    message: format!(
                        "adapter.{name}: no auth configured (token or client_id/secret)"
                    ),
                    file: None,
                    field: Some(format!("adapter.{name}")),
                });
            }
            if ok {
                msgs.push(ValidateMessage {
                    severity: "ok".into(),
                    code: "V010".into(),
                    message: format!("adapter.{name}: databricks (auth configured)"),
                    file: None,
                    field: None,
                });
            }
        }
        "fivetran" => {
            if adapter.api_key.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V014".into(),
                    message: format!("adapter.{name}: api_key not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.api_key")),
                });
            }
            if adapter.api_secret.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V015".into(),
                    message: format!("adapter.{name}: api_secret not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.api_secret")),
                });
            }
            if adapter.destination_id.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V016".into(),
                    message: format!("adapter.{name}: destination_id not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.destination_id")),
                });
            }
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: fivetran"),
                file: None,
                field: None,
            });
        }
        "manual" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: manual"),
                file: None,
                field: None,
            });
        }
        "duckdb" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: duckdb (local)"),
                file: None,
                field: None,
            });
        }
        "snowflake" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: snowflake"),
                file: None,
                field: None,
            });
        }
        other => {
            ok = false;
            msgs.push(ValidateMessage {
                severity: "warn".into(),
                code: "V017".into(),
                message: format!("adapter.{name}: unknown type '{other}'"),
                file: None,
                field: Some(format!("adapter.{name}.type")),
            });
        }
    }

    (ok, msgs)
}

fn validate_replication_pipeline(
    name: &str,
    pipeline: &rocky_core::config::ReplicationPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Validate schema pattern
    match pipeline.schema_pattern() {
        Ok(_) => msgs.push(ValidateMessage {
            severity: "ok".into(),
            code: "V021".into(),
            message: format!("pipeline.{name}: schema pattern parseable"),
            file: None,
            field: None,
        }),
        Err(e) => {
            ok = false;
            msgs.push(ValidateMessage {
                severity: "error".into(),
                code: "V021".into(),
                message: format!("pipeline.{name}: schema pattern error: {e}"),
                file: None,
                field: Some(format!("pipeline.{name}.source.schema_pattern")),
            });
        }
    }

    // Check adapter references
    if !cfg.adapters.contains_key(&pipeline.source.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V022".into(),
            message: format!(
                "pipeline.{name}: source adapter '{}' not found in [adapter]",
                pipeline.source.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.source.adapter")),
        });
    }
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }
    if let Some(ref disc) = pipeline.source.discovery {
        if !cfg.adapters.contains_key(&disc.adapter) {
            ok = false;
            msgs.push(ValidateMessage {
                severity: "error".into(),
                code: "V024".into(),
                message: format!(
                    "pipeline.{name}: discovery adapter '{}' not found in [adapter]",
                    disc.adapter
                ),
                file: None,
                field: Some(format!("pipeline.{name}.source.discovery.adapter")),
            });
        }
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: replication / {} -> {} / {}",
            pipeline.strategy, pipeline.target.catalog_template, pipeline.target.schema_template,
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_transformation_pipeline(
    name: &str,
    pipeline: &rocky_core::config::TransformationPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
    config_path: &Path,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    // Check that the models base directory exists
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    // Extract the directory portion of the glob (e.g., "models" from "models/**")
    let models_base = pipeline
        .models
        .split("**")
        .next()
        .unwrap_or(&pipeline.models)
        .trim_end_matches('/');
    let models_path = config_dir.join(models_base);
    if !models_path.exists() {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V025".into(),
            message: format!(
                "pipeline.{name}: models directory '{}' does not exist",
                models_base
            ),
            file: None,
            field: Some(format!("pipeline.{name}.models")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: transformation / models='{}'",
            pipeline.models
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_quality_pipeline(
    name: &str,
    pipeline: &rocky_core::config::QualityPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    if pipeline.tables.is_empty() {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V026".into(),
            message: format!("pipeline.{name}: no tables defined — nothing to check"),
            file: None,
            field: Some(format!("pipeline.{name}.tables")),
        });
    }

    if !pipeline.checks.enabled {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V027".into(),
            message: format!(
                "pipeline.{name}: checks.enabled is false — quality pipeline has no effect"
            ),
            file: None,
            field: Some(format!("pipeline.{name}.checks.enabled")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: quality / {} table ref(s)",
            pipeline.tables.len()
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_snapshot_pipeline(
    name: &str,
    pipeline: &rocky_core::config::SnapshotPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check source adapter exists
    if !cfg.adapters.contains_key(&pipeline.source.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V022".into(),
            message: format!(
                "pipeline.{name}: source adapter '{}' not found in [adapter]",
                pipeline.source.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.source.adapter")),
        });
    }

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    if pipeline.unique_key.is_empty() {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V028".into(),
            message: format!("pipeline.{name}: unique_key must not be empty"),
            file: None,
            field: Some(format!("pipeline.{name}.unique_key")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: snapshot / {}.{}.{} -> {}.{}.{}",
            pipeline.source.catalog,
            pipeline.source.schema,
            pipeline.source.table,
            pipeline.target.catalog,
            pipeline.target.schema,
            pipeline.target.table,
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

/// Validates the pipeline dependency graph (depends_on references).
///
/// Checks that all referenced pipeline names exist and that the graph is acyclic.
fn validate_pipeline_dag(cfg: &rocky_core::config::RockyConfig, out: &mut ValidateOutput) {
    let pipeline_names: std::collections::HashSet<&str> = cfg
        .pipelines
        .keys()
        .map(std::string::String::as_str)
        .collect();

    let mut has_deps = false;

    // Check all depends_on references exist
    for (name, pc) in &cfg.pipelines {
        for dep in pc.depends_on() {
            has_deps = true;
            if !pipeline_names.contains(dep.as_str()) {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V029".into(),
                    message: format!(
                        "pipeline.{name}: depends_on references unknown pipeline '{dep}'"
                    ),
                    file: None,
                    field: Some(format!("pipeline.{name}.depends_on")),
                });
                out.valid = false;
            }
        }
    }

    // Check for cycles using the existing DAG module
    if has_deps {
        let dag_nodes: Vec<rocky_core::dag::DagNode> = cfg
            .pipelines
            .iter()
            .map(|(name, pc)| rocky_core::dag::DagNode {
                name: name.clone(),
                depends_on: pc.depends_on().to_vec(),
            })
            .collect();

        match rocky_core::dag::topological_sort(&dag_nodes) {
            Ok(order) => {
                out.push(ValidateMessage {
                    severity: "ok".into(),
                    code: "V030".into(),
                    message: format!(
                        "pipeline dependency graph is acyclic (execution order: {})",
                        order.join(" -> ")
                    ),
                    file: None,
                    field: None,
                });
            }
            Err(rocky_core::dag::DagError::CyclicDependency { nodes }) => {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!(
                        "pipeline dependency cycle detected involving: {}",
                        nodes.join(", ")
                    ),
                    file: None,
                    field: Some("pipeline.*.depends_on".into()),
                });
                out.valid = false;
            }
            Err(e) => {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!("pipeline dependency graph error: {e}"),
                    file: None,
                    field: Some("pipeline.*.depends_on".into()),
                });
                out.valid = false;
            }
        }
    }
}

/// Lint rules: warn about redundant fields, no-op defaults, and opportunities
/// to simplify the configuration.
fn lint_config(
    cfg: &rocky_core::config::RockyConfig,
    models: &[rocky_core::models::Model],
    out: &mut ValidateOutput,
) {
    use std::collections::HashMap;

    // L004: [state] backend="local" is the default
    if cfg.state.backend == rocky_core::config::StateBackend::Local {
        out.push(ValidateMessage {
            severity: "lint".into(),
            code: "L004".into(),
            message: "[state] backend='local' is the default — you can omit it".into(),
            file: None,
            field: Some("state.backend".into()),
        });
    }

    // Per-pipeline lint rules (dispatch by pipeline type)
    for (name, pc) in &cfg.pipelines {
        if let Some(pipeline) = pc.as_replication() {
            // L006: auto_create_catalogs/schemas = false is the default
            if !pipeline.target.governance.auto_create_catalogs {
                out.push(ValidateMessage {
                    severity: "lint".into(),
                    code: "L006".into(),
                    message: format!("pipeline.{name}: auto_create_catalogs=false is the default — you can omit it"),
                    file: None,
                    field: Some(format!("pipeline.{name}.target.governance.auto_create_catalogs")),
                });
            }
            if !pipeline.target.governance.auto_create_schemas {
                out.push(ValidateMessage {
                    severity: "lint".into(),
                    code: "L006".into(),
                    message: format!("pipeline.{name}: auto_create_schemas=false is the default — you can omit it"),
                    file: None,
                    field: Some(format!("pipeline.{name}.target.governance.auto_create_schemas")),
                });
            }
        }
    }

    // L007: single-adapter project repeats adapter name
    if cfg.adapters.len() == 1 {
        let adapter_name = cfg.adapters.keys().next().unwrap();
        let mut ref_count = 0usize;
        for pc in cfg.pipelines.values() {
            // Count adapter references across all pipeline types
            if pc.target_adapter() == adapter_name {
                ref_count += 1;
            }
            if let Some(pipeline) = pc.as_replication() {
                if pipeline.source.adapter == *adapter_name {
                    ref_count += 1;
                }
                if let Some(ref disc) = pipeline.source.discovery {
                    if disc.adapter == *adapter_name {
                        ref_count += 1;
                    }
                }
            }
        }
        if ref_count > 1 && adapter_name != "default" {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L007".into(),
                message: format!(
                    "single-adapter project repeats adapter='{adapter_name}' {ref_count} times — consider unnamed [adapter]"
                ),
                file: None,
                field: None,
            });
        }
    }

    // Model-level lint rules
    if models.is_empty() {
        return;
    }

    // L001: name matches filename stem
    for model in models {
        let file_stem = std::path::Path::new(&model.file_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if model.config.name == file_stem {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L001".into(),
                message: format!(
                    "model '{}' declares name='{}' which matches its filename — you can omit it",
                    model.config.name, model.config.name
                ),
                file: Some(model.file_path.clone()),
                field: Some("name".into()),
            });
        }

        // L002: target.table matches name
        if model.config.target.table == model.config.name {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L002".into(),
                message: format!(
                    "model '{}' declares target.table='{}' which matches name — you can omit it",
                    model.config.name, model.config.target.table
                ),
                file: Some(model.file_path.clone()),
                field: Some("target.table".into()),
            });
        }

        // L008: auto-discovered contract (informational)
        if model.contract_path.is_some() {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L008".into(),
                message: format!(
                    "model '{}' has auto-discovered contract from sibling .contract.toml",
                    model.config.name
                ),
                file: model
                    .contract_path
                    .as_ref()
                    .map(|p| p.display().to_string()),
                field: None,
            });
        }
    }

    // L003: models share identical target.catalog and target.schema
    let mut catalog_schema_counts: HashMap<(String, String), Vec<String>> = HashMap::new();
    for model in models {
        let key = (
            model.config.target.catalog.clone(),
            model.config.target.schema.clone(),
        );
        catalog_schema_counts
            .entry(key)
            .or_default()
            .push(model.config.name.clone());
    }
    for ((catalog, schema), model_names) in &catalog_schema_counts {
        if model_names.len() >= 3 {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L003".into(),
                message: format!(
                    "models {} all declare target.catalog='{}' and target.schema='{}' — consider models/_defaults.toml",
                    model_names.join(", "), catalog, schema
                ),
                file: None,
                field: None,
            });
        }
    }
}

/// Render the structured output as human-readable text (matching today's
/// format for backward compatibility).
fn render_text(output: &ValidateOutput) {
    for msg in &output.messages {
        let prefix = match msg.severity.as_str() {
            "ok" => "  ok ",
            "warn" => "  !! ",
            "error" => "  !! ",
            "lint" => " note",
            _ => "     ",
        };
        println!("{prefix} {}", msg.message);
    }
    println!();
    println!("Validation complete.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn validate_toml(toml_str: &str) -> ValidateOutput {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(toml_str.as_bytes()).unwrap();
        validate_inner(f.path()).unwrap()
    }

    #[test]
    fn test_valid_duckdb_config() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"

[state]
backend = "local"
"#,
        );
        assert!(out.valid);
        assert_eq!(out.adapters.len(), 1);
        assert!(out.adapters[0].ok);
        assert_eq!(out.pipelines.len(), 1);
        assert!(out.pipelines[0].ok);
    }

    #[test]
    fn test_empty_config() {
        let out = validate_toml("");
        // Should warn about no adapters and no pipelines
        assert!(out.valid); // warnings don't set valid=false
        let warns: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "warn")
            .collect();
        assert!(
            warns.len() >= 2,
            "expected warnings for no adapters/pipelines"
        );
    }

    #[test]
    fn test_missing_adapter_ref() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "nonexistent"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(!out.valid);
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "error" && m.code == "V022")
            .collect();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("nonexistent"));
    }

    #[test]
    fn test_missing_databricks_auth() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "databricks"
host = "foo.databricks.com"
http_path = "/sql/1.0/warehouses/abc"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "db"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "db"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        // Should have a warning about missing auth
        let auth_warns: Vec<_> = out.messages.iter().filter(|m| m.code == "V013").collect();
        assert_eq!(auth_warns.len(), 1);
        assert!(!out.adapters[0].ok);
    }

    #[test]
    fn test_invalid_schema_pattern() {
        // An empty-name component ("") is rejected by SchemaPattern::parse_components
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = [""]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let schema_errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V021" && m.severity == "error")
            .collect();
        assert_eq!(schema_errors.len(), 1);
    }

    #[test]
    fn test_dag_cycle_detected() {
        // Create a temp dir with models that have a cycle
        let dir = tempfile::TempDir::new().unwrap();

        // Write rocky.toml
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        )
        .unwrap();

        // Create models directory with a cycle: a -> b -> a
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();

        std::fs::write(
            models_dir.join("a.toml"),
            r#"
name = "a"
depends_on = ["b"]
[target]
catalog = "c"
schema = "s"
table = "a"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("a.sql"), "SELECT 1").unwrap();

        std::fs::write(
            models_dir.join("b.toml"),
            r#"
name = "b"
depends_on = ["a"]
[target]
catalog = "c"
schema = "s"
table = "b"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("b.sql"), "SELECT 1").unwrap();

        let out = validate_inner(&config_path).unwrap();
        assert!(!out.valid);
        let dag_errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V031" && m.severity == "error")
            .collect();
        assert_eq!(dag_errors.len(), 1);
        assert!(dag_errors[0].message.contains("circular"));
    }

    #[test]
    fn test_unknown_adapter_type() {
        let out = validate_toml(
            r#"
[adapter.mystery]
type = "postgres"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "mystery"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "mystery"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let unknown: Vec<_> = out.messages.iter().filter(|m| m.code == "V017").collect();
        assert_eq!(unknown.len(), 1);
        assert!(unknown[0].message.contains("postgres"));
    }

    #[test]
    fn test_lint_fires_on_typical_poc() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"

[pipeline.poc.target.governance]
auto_create_catalogs = false
auto_create_schemas = false

[state]
backend = "local"
"#,
        );
        assert!(out.valid);
        let lint_codes: Vec<&str> = out
            .messages
            .iter()
            .filter(|m| m.severity == "lint")
            .map(|m| m.code.as_str())
            .collect();
        // Should fire: L004 (state backend), L006 x2 (governance), L007 (adapter)
        // Note: L005 (pipeline type) was removed — the `type` field is now the
        // enum discriminator and not a lintable config value.
        assert!(
            lint_codes.contains(&"L004"),
            "expected L004 (state backend)"
        );
        assert!(
            lint_codes.contains(&"L006"),
            "expected L006 (governance defaults)"
        );
        assert!(
            lint_codes.contains(&"L007"),
            "expected L007 (adapter repetition)"
        );
    }
}
