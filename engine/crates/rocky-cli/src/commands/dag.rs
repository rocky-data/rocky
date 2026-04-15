//! `rocky dag` — emit the full unified DAG as enriched JSON.
//!
//! Projects the engine's internal [`UnifiedDag`] into an orchestrator-friendly
//! shape: every pipeline stage becomes a node with its target table
//! coordinates, materialization strategy, freshness SLA, partition shape,
//! and direct upstream dependencies.
//!
//! Consumers (dagster-rocky) can build a complete, connected Dagster asset
//! graph from a single `rocky dag --output json` call.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::models::{Model, StrategyConfig, TimeGrain};
use rocky_core::seeds::SeedFile;
use rocky_core::unified_dag::{self, NodeKind, UnifiedDag};

use crate::output::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky dag`.
pub fn run_dag(
    config_path: &Path,
    models_dir: &Path,
    seeds_dir: Option<&Path>,
    contracts_dir: Option<&Path>,
    include_column_lineage: bool,
    json: bool,
) -> Result<()> {
    let cfg = rocky_core::config::load_rocky_config(config_path)?;

    // Load models from the models directory (including subdirectories).
    let models = load_all_models(models_dir)?;

    // Load seeds if the directory exists.
    let seeds = match seeds_dir {
        Some(dir) if dir.exists() => {
            rocky_core::seeds::discover_seeds(dir).unwrap_or_default()
        }
        _ => {
            // Try default "seeds" relative to config dir.
            let default_seeds = config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("seeds");
            if default_seeds.is_dir() {
                rocky_core::seeds::discover_seeds(&default_seeds).unwrap_or_default()
            } else {
                vec![]
            }
        }
    };

    // Build the unified DAG.
    let dag = unified_dag::build_unified_dag(&cfg, &models, &seeds)
        .context("failed to build unified DAG")?;

    // Compute execution phases (topological layers).
    let phases = unified_dag::execution_phases(&dag)
        .context("failed to compute execution phases")?;

    // Build the enriched output.
    let output = build_dag_output(
        &dag, &phases, &models, &seeds, include_column_lineage, models_dir, contracts_dir,
    )?;

    if json {
        print_json(&output)?;
    } else {
        print_dag_table(&output);
    }
    Ok(())
}

/// Build the full `DagOutput` from the unified DAG plus model/seed metadata.
fn build_dag_output(
    dag: &UnifiedDag,
    phases: &[Vec<&rocky_core::unified_dag::UnifiedNode>],
    models: &[Model],
    seeds: &[SeedFile],
    include_column_lineage: bool,
    models_dir: &Path,
    contracts_dir: Option<&Path>,
) -> Result<DagOutput> {
    // Build lookup maps.
    let model_map: HashMap<&str, &Model> = models
        .iter()
        .map(|m| (m.config.name.as_str(), m))
        .collect();

    let seed_map: HashMap<&str, &SeedFile> = seeds
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();

    // Project nodes.
    let nodes: Vec<DagNodeOutput> = dag
        .nodes
        .iter()
        .map(|node| {
            let depends_on: Vec<String> = dag
                .incoming_edges(&node.id)
                .iter()
                .map(|e| e.from.0.clone())
                .collect();

            let (target, strategy, freshness, partition_shape) = match node.kind {
                NodeKind::Transformation => {
                    if let Some(model) = model_map.get(node.label.as_str()) {
                        (
                            Some(model.config.target.clone()),
                            Some(model.config.strategy.clone()),
                            model.config.freshness.clone(),
                            extract_partition_shape(&model.config.strategy),
                        )
                    } else {
                        (None, None, None, None)
                    }
                }
                NodeKind::Seed => {
                    if let Some(seed) = seed_map.get(node.label.as_str()) {
                        let seed_target = seed.config.target.as_ref();
                        (
                            Some(rocky_core::models::TargetConfig {
                                catalog: seed_target
                                    .and_then(|t| t.catalog.clone())
                                    .unwrap_or_default(),
                                schema: seed_target
                                    .map(|t| t.schema.clone())
                                    .unwrap_or_default(),
                                table: seed_target
                                    .and_then(|t| t.table.clone())
                                    .unwrap_or_else(|| seed.name.clone()),
                            }),
                            None,
                            None,
                            None,
                        )
                    } else {
                        (None, None, None, None)
                    }
                }
                _ => (None, None, None, None),
            };

            DagNodeOutput {
                id: node.id.0.clone(),
                kind: node.kind.to_string(),
                label: node.label.clone(),
                pipeline: node.pipeline.clone(),
                target,
                strategy,
                freshness,
                partition_shape,
                depends_on,
            }
        })
        .collect();

    // Project edges.
    let edges: Vec<DagEdgeOutput> = dag
        .edges
        .iter()
        .map(|e| DagEdgeOutput {
            from: e.from.0.clone(),
            to: e.to.0.clone(),
            edge_type: e.edge_type.to_string(),
        })
        .collect();

    // Execution layers (list of node ID lists).
    let execution_layers: Vec<Vec<String>> = phases
        .iter()
        .map(|layer| layer.iter().map(|n| n.id.0.clone()).collect())
        .collect();

    // Summary.
    let mut counts_by_kind: HashMap<String, usize> = HashMap::new();
    for node in &dag.nodes {
        *counts_by_kind.entry(node.kind.to_string()).or_default() += 1;
    }

    let summary = DagSummaryOutput {
        total_nodes: dag.nodes.len(),
        total_edges: dag.edges.len(),
        execution_layers: phases.len(),
        counts_by_kind,
    };

    // Column lineage (optional).
    let column_lineage = if include_column_lineage {
        build_column_lineage_from_models(models_dir, contracts_dir)?
    } else {
        vec![]
    };

    Ok(DagOutput {
        version: VERSION.to_string(),
        command: "dag".to_string(),
        nodes,
        edges,
        execution_layers,
        summary,
        column_lineage,
    })
}

/// Map a `TimeGrain` to a human-friendly granularity string.
fn grain_to_string(grain: &TimeGrain) -> String {
    match grain {
        TimeGrain::Hour => "hourly",
        TimeGrain::Day => "daily",
        TimeGrain::Month => "monthly",
        TimeGrain::Year => "yearly",
    }
    .to_string()
}

/// Extract partition shape from a model's strategy config.
fn extract_partition_shape(strategy: &StrategyConfig) -> Option<PartitionShapeOutput> {
    match strategy {
        StrategyConfig::TimeInterval {
            granularity,
            first_partition,
            ..
        } => Some(PartitionShapeOutput {
            granularity: grain_to_string(granularity),
            first_partition: first_partition.clone(),
        }),
        StrategyConfig::Microbatch { granularity, .. } => Some(PartitionShapeOutput {
            granularity: grain_to_string(granularity),
            first_partition: None,
        }),
        _ => None,
    }
}

/// Build column-level lineage edges by compiling models and extracting
/// the semantic graph.
fn build_column_lineage_from_models(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
) -> Result<Vec<LineageEdgeRecord>> {
    let compile_config = rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(|p| p.to_path_buf()),
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
    };

    let result = match rocky_compiler::compile::compile(&compile_config) {
        Ok(r) => r,
        Err(_) => return Ok(vec![]),
    };

    let graph = &result.semantic_graph;
    let edges: Vec<LineageEdgeRecord> = graph
        .edges
        .iter()
        .map(|e| LineageEdgeRecord {
            source: LineageQualifiedColumn {
                model: e.source.model.to_string(),
                column: e.source.column.to_string(),
            },
            target: LineageQualifiedColumn {
                model: e.target.model.to_string(),
                column: e.target.column.to_string(),
            },
            transform: format!("{}", e.transform),
        })
        .collect();

    Ok(edges)
}

/// Load models from a directory and its immediate subdirectories.
///
/// Duplicated from `list.rs` — both commands need the same recursive
/// model loading. A shared helper would be ideal but this keeps the
/// diff contained for now.
fn load_all_models(models_dir: &Path) -> Result<Vec<Model>> {
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

/// Print a human-readable table view of the DAG.
fn print_dag_table(output: &DagOutput) {
    println!(
        "Unified DAG: {} nodes, {} edges, {} execution layers\n",
        output.summary.total_nodes, output.summary.total_edges, output.summary.execution_layers,
    );

    for (i, layer) in output.execution_layers.iter().enumerate() {
        println!("Layer {}:", i);
        for node_id in layer {
            if let Some(node) = output.nodes.iter().find(|n| &n.id == node_id) {
                let deps = if node.depends_on.is_empty() {
                    String::from("-")
                } else {
                    node.depends_on.join(", ")
                };
                println!(
                    "  {:<16} {:<30} depends_on: {}",
                    node.kind, node.label, deps
                );
            }
        }
    }

    if !output.column_lineage.is_empty() {
        println!("\nColumn lineage: {} edges", output.column_lineage.len());
    }
}
