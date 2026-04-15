use std::path::Path;

use anyhow::{Context, Result};

use crate::output::*;

/// Execute `rocky list pipelines`.
pub fn list_pipelines(config_path: &Path, json: bool) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;

    let entries: Vec<ListPipelineEntry> = cfg
        .pipelines
        .iter()
        .map(|(name, pc)| {
            let source_adapter = match pc {
                rocky_core::config::PipelineConfig::Replication(r) => {
                    Some(r.source.adapter.clone())
                }
                _ => None,
            };
            ListPipelineEntry {
                name: name.clone(),
                pipeline_type: pc.pipeline_type_str().to_string(),
                target_adapter: pc.target_adapter().to_string(),
                source_adapter,
                depends_on: pc.depends_on().to_vec(),
                concurrency: pc.execution().concurrency.to_string(),
            }
        })
        .collect();

    if json {
        print_json(&ListPipelinesOutput::new(entries))?;
    } else {
        if entries.is_empty() {
            println!("No pipelines defined.");
        } else {
            println!(
                "{:<25} {:<16} {:<20} {:<20} DEPENDS ON",
                "NAME", "TYPE", "TARGET", "SOURCE"
            );
            for e in &entries {
                println!(
                    "{:<25} {:<16} {:<20} {:<20} {}",
                    e.name,
                    e.pipeline_type,
                    e.target_adapter,
                    e.source_adapter.as_deref().unwrap_or("-"),
                    if e.depends_on.is_empty() {
                        "-".to_string()
                    } else {
                        e.depends_on.join(", ")
                    },
                );
            }
        }
    }
    Ok(())
}

/// Execute `rocky list adapters`.
pub fn list_adapters(config_path: &Path, json: bool) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;

    let entries: Vec<ListAdapterEntry> = cfg
        .adapters
        .iter()
        .map(|(name, ac)| ListAdapterEntry {
            name: name.clone(),
            adapter_type: ac.adapter_type.clone(),
            host: ac.host.clone(),
        })
        .collect();

    if json {
        print_json(&ListAdaptersOutput::new(entries))?;
    } else {
        if entries.is_empty() {
            println!("No adapters defined.");
        } else {
            println!("{:<25} {:<16} HOST", "NAME", "TYPE");
            for e in &entries {
                println!(
                    "{:<25} {:<16} {}",
                    e.name,
                    e.adapter_type,
                    e.host.as_deref().unwrap_or("-"),
                );
            }
        }
    }
    Ok(())
}

/// Execute `rocky list models`.
///
/// Loads models from the given directory and all immediate subdirectories
/// (handles the common `models/{layer}/*.sql` layout). Each subdirectory
/// is scanned independently so per-directory `_defaults.toml` files work.
pub fn list_models(models_dir: &Path, json: bool) -> Result<()> {
    let models = load_all_models(models_dir)?;

    let entries: Vec<ListModelEntry> = models
        .iter()
        .map(|m| {
            let target = format!(
                "{}.{}.{}",
                m.config.target.catalog, m.config.target.schema, m.config.target.table
            );
            let strategy = match &m.config.strategy {
                rocky_core::models::StrategyConfig::FullRefresh => "full_refresh",
                rocky_core::models::StrategyConfig::Incremental { .. } => "incremental",
                rocky_core::models::StrategyConfig::Merge { .. } => "merge",
                rocky_core::models::StrategyConfig::TimeInterval { .. } => "time_interval",
                rocky_core::models::StrategyConfig::DeleteInsert { .. } => "delete_insert",
                rocky_core::models::StrategyConfig::Ephemeral => "ephemeral",
                rocky_core::models::StrategyConfig::Microbatch { .. } => "microbatch",
            }
            .to_string();
            ListModelEntry {
                name: m.config.name.clone(),
                target,
                strategy,
                depends_on: m.config.depends_on.clone(),
                has_contract: m.contract_path.is_some(),
            }
        })
        .collect();

    if json {
        print_json(&ListModelsOutput::new(entries))?;
    } else {
        if entries.is_empty() {
            println!("No models found in {}", models_dir.display());
        } else {
            println!(
                "{:<30} {:<40} {:<16} {:<12} DEPENDS ON",
                "NAME", "TARGET", "STRATEGY", "CONTRACT"
            );
            for e in &entries {
                println!(
                    "{:<30} {:<40} {:<16} {:<12} {}",
                    e.name,
                    e.target,
                    e.strategy,
                    if e.has_contract { "yes" } else { "-" },
                    if e.depends_on.is_empty() {
                        "-".to_string()
                    } else {
                        e.depends_on.join(", ")
                    },
                );
            }
        }
    }
    Ok(())
}

/// Execute `rocky list sources`.
pub fn list_sources(config_path: &Path, json: bool) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;

    let entries: Vec<ListSourceEntry> = cfg
        .pipelines
        .iter()
        .filter_map(|(name, pc)| {
            let repl = pc.as_replication()?;
            let pattern = &repl.source.schema_pattern;

            Some(ListSourceEntry {
                pipeline: name.clone(),
                adapter: repl.source.adapter.clone(),
                catalog: repl.source.catalog.clone(),
                schema_prefix: Some(pattern.prefix.clone()),
                discovery_adapter: repl.source.discovery.as_ref().map(|d| d.adapter.clone()),
                components: pattern.components.clone(),
            })
        })
        .collect();

    if json {
        print_json(&ListSourcesOutput::new(entries))?;
    } else {
        if entries.is_empty() {
            println!("No replication sources defined.");
        } else {
            println!(
                "{:<25} {:<16} {:<20} {:<12} COMPONENTS",
                "PIPELINE", "ADAPTER", "CATALOG", "DISCOVERY"
            );
            for e in &entries {
                println!(
                    "{:<25} {:<16} {:<20} {:<12} [{}]",
                    e.pipeline,
                    e.adapter,
                    e.catalog.as_deref().unwrap_or("-"),
                    e.discovery_adapter.as_deref().unwrap_or("-"),
                    e.components.join(", "),
                );
            }
        }
    }
    Ok(())
}

/// Load all models from a directory (with subdirectory scan).
fn load_all_models(models_dir: &Path) -> Result<Vec<rocky_core::models::Model>> {
    let mut all = rocky_core::models::load_models_from_dir(models_dir).context(format!(
        "failed to load models from {}",
        models_dir.display()
    ))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(sub) = rocky_core::models::load_models_from_dir(&entry.path()) {
                    all.extend(sub);
                }
            }
        }
    }
    all.sort_unstable_by(|a, b| a.config.name.cmp(&b.config.name));
    Ok(all)
}

/// Execute `rocky list deps <model>`.
pub fn list_deps(model_name: &str, models_dir: &Path, json: bool) -> Result<()> {
    let models = load_all_models(models_dir)?;

    let model = models
        .iter()
        .find(|m| m.config.name == model_name)
        .ok_or_else(|| {
            anyhow::anyhow!("model '{model_name}' not found in {}", models_dir.display())
        })?;

    let deps = &model.config.depends_on;

    if json {
        print_json(&serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "command": "list_deps",
            "model": model_name,
            "depends_on": deps,
        }))?;
    } else {
        if deps.is_empty() {
            println!("{model_name}: no dependencies declared");
        } else {
            println!("{model_name} depends on:");
            for dep in deps {
                println!("  -> {dep}");
            }
        }
    }
    Ok(())
}

/// Execute `rocky list consumers <model>`.
pub fn list_consumers(model_name: &str, models_dir: &Path, json: bool) -> Result<()> {
    let models = load_all_models(models_dir)?;

    // Verify the target model exists
    if !models.iter().any(|m| m.config.name == model_name) {
        anyhow::bail!("model '{model_name}' not found in {}", models_dir.display());
    }

    let consumers: Vec<String> = models
        .iter()
        .filter(|m| m.config.depends_on.iter().any(|d| d == model_name))
        .map(|m| m.config.name.clone())
        .collect();

    if json {
        print_json(&serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "command": "list_consumers",
            "model": model_name,
            "consumers": consumers,
        }))?;
    } else {
        if consumers.is_empty() {
            println!("{model_name}: no consumers found");
        } else {
            println!("Models that depend on {model_name}:");
            for c in &consumers {
                println!("  <- {c}");
            }
        }
    }
    Ok(())
}
