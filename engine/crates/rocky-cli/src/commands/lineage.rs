//! `rocky lineage` — column-level lineage explorer.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_compiler::compile::{self, CompilerConfig};

use crate::output::{
    ColumnLineageOutput, LineageColumnDef, LineageEdgeRecord, LineageOutput,
    LineageQualifiedColumn, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Convert a borrowed `LineageEdge` into a serializable record.
fn to_edge_record(edge: &rocky_compiler::semantic::LineageEdge) -> LineageEdgeRecord {
    LineageEdgeRecord {
        source: LineageQualifiedColumn {
            model: edge.source.model.to_string(),
            column: edge.source.column.to_string(),
        },
        target: LineageQualifiedColumn {
            model: edge.target.model.to_string(),
            column: edge.target.column.to_string(),
        },
        transform: edge.transform.to_string(),
    }
}

/// Execute `rocky lineage`.
///
/// `cache_ttl_override`: optional CLI `--cache-ttl <seconds>` value
/// (Arc 7 wave 2 wave-2 PR 4). Replaces `[cache.schemas] ttl_seconds`
/// for this invocation only.
#[allow(clippy::too_many_arguments)]
pub fn run_lineage(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    target: &str,
    column: Option<&str>,
    format: Option<&str>,
    downstream: bool,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    // Wave-2 of Arc 7 wave 2: load cached warehouse schemas so lineage
    // edges inherit real types instead of `RockyType::Unknown` on the
    // leaves. Degrades to empty on cold cache / missing config.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => HashMap::new(),
    };

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    let result = compile::compile(&config)?;

    // Parse target: "model" or "model.column"
    let (model_name, col_name) = if let Some(col) = column {
        (target, Some(col))
    } else if target.contains('.') {
        let parts: Vec<&str> = target.splitn(2, '.').collect();
        (parts[0], Some(parts[1]))
    } else {
        (target, None)
    };

    let schema = result
        .semantic_graph
        .model_schema(model_name)
        .context(format!("model '{model_name}' not found"))?;

    let direction = if downstream { "downstream" } else { "upstream" };

    // `--format dot` is lineage-specific and only produces DOT, so it wins
    // over the global `--output json` (which defaults to `json`). Without
    // this, `rocky lineage <m> --format dot` silently emits JSON.
    let emit_dot = matches!(format, Some("dot"));

    if output_json && !emit_dot {
        if let Some(col) = col_name {
            let trace_edges = if downstream {
                result
                    .semantic_graph
                    .trace_column_downstream(model_name, col)
            } else {
                result.semantic_graph.trace_column(model_name, col)
            };
            let trace: Vec<LineageEdgeRecord> =
                trace_edges.iter().map(|e| to_edge_record(e)).collect();
            let output = ColumnLineageOutput {
                version: VERSION.to_string(),
                command: "lineage".to_string(),
                model: model_name.to_string(),
                column: col.to_string(),
                direction: direction.to_string(),
                trace,
            };
            print_json(&output)?;
        } else {
            let edges: Vec<LineageEdgeRecord> = result
                .semantic_graph
                .edges
                .iter()
                .filter(|e| &*e.target.model == model_name || &*e.source.model == model_name)
                .map(to_edge_record)
                .collect();
            let columns: Vec<LineageColumnDef> = schema
                .columns
                .iter()
                .map(|c| LineageColumnDef {
                    name: c.name.clone(),
                })
                .collect();
            let output = LineageOutput {
                version: VERSION.to_string(),
                command: "lineage".to_string(),
                model: model_name.to_string(),
                columns,
                upstream: schema.upstream.clone(),
                downstream: schema.downstream.clone(),
                edges,
            };
            print_json(&output)?;
        }
    } else if emit_dot {
        // Graphviz DOT output
        println!("digraph lineage {{");
        println!("  rankdir=LR;");

        // Add edges
        let edges: Vec<_> = if let Some(col) = col_name {
            if downstream {
                result
                    .semantic_graph
                    .trace_column_downstream(model_name, col)
            } else {
                result.semantic_graph.trace_column(model_name, col)
            }
        } else if downstream {
            result
                .semantic_graph
                .edges
                .iter()
                .filter(|e| &*e.source.model == model_name)
                .collect()
        } else {
            result
                .semantic_graph
                .edges
                .iter()
                .filter(|e| &*e.target.model == model_name)
                .collect()
        };

        for edge in &edges {
            println!(
                "  \"{}.{}\" -> \"{}.{}\";",
                edge.source.model, edge.source.column, edge.target.model, edge.target.column
            );
        }
        println!("}}");
    } else {
        // Human-readable
        println!("Model: {model_name}");
        println!("Upstream: {}", schema.upstream.join(", "));
        println!("Downstream: {}", schema.downstream.join(", "));
        println!();

        if let Some(col) = col_name {
            if downstream {
                println!("Column consumers: {model_name}.{col}");
                let trace = result
                    .semantic_graph
                    .trace_column_downstream(model_name, col);
                for (i, edge) in trace.iter().enumerate() {
                    let indent = "  ".repeat(i + 1);
                    println!(
                        "{indent}-> {}.{} ({})",
                        edge.target.model, edge.target.column, edge.transform
                    );
                }
            } else {
                println!("Column trace: {model_name}.{col}");
                let trace = result.semantic_graph.trace_column(model_name, col);
                for (i, edge) in trace.iter().enumerate() {
                    let indent = "  ".repeat(i + 1);
                    println!(
                        "{indent}<- {}.{} ({})",
                        edge.source.model, edge.source.column, edge.transform
                    );
                }
            }
        } else {
            println!("Columns:");
            for col_def in &schema.columns {
                let incoming: Vec<_> = result
                    .semantic_graph
                    .edges
                    .iter()
                    .filter(|e| &*e.target.model == model_name && *e.target.column == *col_def.name)
                    .collect();

                if let Some(edge) = incoming.first() {
                    println!(
                        "  {} <- {}.{} ({})",
                        col_def.name, edge.source.model, edge.source.column, edge.transform
                    );
                } else {
                    println!("  {} (no lineage)", col_def.name);
                }
            }
        }
    }

    Ok(())
}
