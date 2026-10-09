//! `rocky lineage` — column-level lineage explorer.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_compiler::compile::{self, CompilerConfig};

use crate::output::{
    ColumnLineageOutput, LineageColumnDef, LineageConsumerRecord, LineageEdgeRecord,
    LineageNodeDef, LineageOutput, LineageQualifiedColumn, RowSelectionEdgeRecord, print_json,
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

/// Row-selection edges relevant to a column trace.
///
/// Upstream: for the focal column and every column the value trace passes
/// through, the edges that select that column's model rows. Downstream: the
/// edges sourced from the focal column or any column derived from it. Sorted
/// and deduplicated so the output is deterministic.
pub(crate) fn row_selection_for_trace(
    graph: &rocky_compiler::semantic::SemanticGraph,
    model_name: &str,
    column: &str,
    downstream: bool,
) -> Vec<RowSelectionEdgeRecord> {
    let mut points: Vec<(String, String)> = vec![(model_name.to_string(), column.to_string())];
    if downstream {
        for edge in graph.trace_column_downstream(model_name, column) {
            points.push((
                edge.target.model.to_string(),
                edge.target.column.to_string(),
            ));
        }
    } else {
        for edge in graph.trace_column(model_name, column) {
            points.push((
                edge.source.model.to_string(),
                edge.source.column.to_string(),
            ));
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for (model, col) in &points {
        let edges = if downstream {
            graph.row_selection_consumers(model, col)
        } else {
            graph.row_selection_into(model, col)
        };
        for edge in edges {
            seen.insert((
                edge.target_model.to_string(),
                edge.target_column.as_deref().map(str::to_string),
                edge.kind.to_string(),
                edge.source.model.to_string(),
                edge.source.column.to_string(),
            ));
        }
    }
    seen.into_iter()
        .map(
            |(target_model, target_column, kind, model, column)| RowSelectionEdgeRecord {
                source: LineageQualifiedColumn { model, column },
                target_model,
                target_column,
                kind,
            },
        )
        .collect()
}

/// Print the labelled row-selection section of the human column view.
fn print_row_selection(records: &[RowSelectionEdgeRecord]) {
    if records.is_empty() {
        return;
    }
    println!();
    println!("Row selection (decides which rows/groups exist):");
    for r in records {
        let scope = r.target_column.as_deref().map_or_else(
            || r.target_model.clone(),
            |c| format!("{}.{c}", r.target_model),
        );
        println!(
            "  [{}] {}.{} -> {scope}",
            r.kind, r.source.model, r.source.column
        );
    }
}

/// Execute `rocky lineage`.
///
/// `cache_ttl_override`: optional CLI `--cache-ttl <seconds>` value
/// that replaces `[cache.schemas] ttl_seconds` for this invocation
/// only.
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
    // Load cached warehouse schemas so lineage edges inherit real types
    // instead of `RockyType::Unknown` on the leaves. Degrades to empty on a
    // cold cache or a project with no `rocky.toml`; a `rocky.toml` that is
    // present and does not load REFUSES (#1625) rather than quietly producing
    // the same lineage a config-less project would.
    let source_schemas = crate::source_schemas::load_project_source_schemas(
        config_path,
        state_path,
        cache_ttl_override,
    )?;

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
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

    // `--format dot` is lineage-specific and only produces DOT, so it wins
    // over the global `--output json` (which defaults to `json`). Without
    // this, `rocky lineage <m> --format dot` silently emits JSON.
    let emit_dot = matches!(format, Some("dot"));

    if output_json && !emit_dot {
        if let Some(col) = col_name {
            let output = column_lineage_output(&result, model_name, col, downstream)?;
            print_json(&output)?;
        } else {
            let output = lineage_output(&result, model_name)?;
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
        let consumers = consumers_downstream_of(&result, model_name);
        if !consumers.is_empty() {
            println!("Consumers:");
            for c in &consumers {
                let mut line = format!("  {} ({})", c.name, c.kind);
                if let Some(owner) = &c.owner {
                    line.push_str(&format!(", owner: {owner}"));
                }
                if let Some(url) = &c.url {
                    line.push_str(&format!(", {url}"));
                }
                if !c.direct {
                    line.push_str(" [via downstream models]");
                }
                println!("{line}");
            }
        }
        println!();

        if let Some(col) = col_name {
            if downstream {
                println!("Column consumers: {model_name}.{col}");
                println!("Value derivation:");
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
                println!("Value derivation:");
                let trace = result.semantic_graph.trace_column(model_name, col);
                for (i, edge) in trace.iter().enumerate() {
                    let indent = "  ".repeat(i + 1);
                    println!(
                        "{indent}<- {}.{} ({})",
                        edge.source.model, edge.source.column, edge.transform
                    );
                }
            }
            print_row_selection(&row_selection_for_trace(
                &result.semantic_graph,
                model_name,
                col,
                downstream,
            ));
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

/// Side-effect-free core producing the model-level [`LineageOutput`] (the
/// no-`--column` case). Mirrors the JSON-branch assembly in [`run_lineage`]
/// without printing.
///
/// `result` is a completed compile; `model_name` is the focal model. Errors
/// when the model is absent from the semantic graph.
// Reusable typed-output core for the in-process MCP server (`rocky-mcp`),
// alongside `run_lineage`'s JSON branch.
pub fn lineage_output(result: &compile::CompileResult, model_name: &str) -> Result<LineageOutput> {
    let schema = result
        .semantic_graph
        .model_schema(model_name)
        .context(format!("model '{model_name}' not found"))?;

    let edges: Vec<LineageEdgeRecord> = result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| &*e.target.model == model_name || &*e.source.model == model_name)
        .map(to_edge_record)
        .collect();
    // Look up typed columns for this model from the typecheck pass so each
    // `LineageColumnDef` can carry its inferred `data_type`. The typed schema
    // may be absent (e.g. on a model that failed typecheck) or report
    // `RockyType::Unknown` for columns it couldn't resolve — both map to `None`.
    let typed_cols = result.type_check.typed_models.get(model_name);
    let columns: Vec<LineageColumnDef> = schema
        .columns
        .iter()
        .map(|c| {
            let data_type = typed_cols
                .and_then(|cols| cols.iter().find(|t| t.name == c.name))
                .filter(|t| !matches!(t.data_type, rocky_ir::types::RockyType::Unknown))
                .map(|t| t.data_type.to_string());
            LineageColumnDef {
                name: c.name.clone(),
                data_type,
            }
        })
        .collect();
    // Per-node metadata for the focal model + every distinct endpoint of
    // `edges`. Project models contribute a `target_schema` from their declared
    // target config; nodes not present in `project.models` are treated as
    // external sources and carry their qualified reference string as
    // `source_id` so the VS Code lineage subgraph drill-in can cluster them
    // without parsing the qualified name.
    let mut node_ids: Vec<String> = Vec::new();
    let mut seen_nodes: std::collections::HashSet<String> = std::collections::HashSet::new();
    if seen_nodes.insert(model_name.to_string()) {
        node_ids.push(model_name.to_string());
    }
    for edge in result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| &*e.target.model == model_name || &*e.source.model == model_name)
    {
        for endpoint in [&edge.source.model, &edge.target.model] {
            let name = endpoint.to_string();
            if seen_nodes.insert(name.clone()) {
                node_ids.push(name);
            }
        }
    }
    let nodes: Vec<LineageNodeDef> = node_ids
        .into_iter()
        .map(|id| {
            if let Some(model) = result.project.model(&id) {
                LineageNodeDef {
                    model: id,
                    // A target prints resolved, as `rocky run`'s
                    // `asset_key` does (#1919).
                    target_schema: Some(model.config.target.schema.clone()),
                    source_id: None,
                }
            } else {
                LineageNodeDef {
                    model: id.clone(),
                    target_schema: None,
                    source_id: Some(id),
                }
            }
        })
        .collect();

    Ok(LineageOutput {
        version: VERSION.to_string(),
        command: "lineage".to_string(),
        model: model_name.to_string(),
        columns,
        upstream: schema.upstream.clone(),
        downstream: schema.downstream.clone(),
        edges,
        nodes,
        consumers: consumers_downstream_of(result, model_name),
    })
}

/// Downstream consumers of `model_name`: those that read it, or read any model
/// downstream of it. Sorted by name.
///
/// A consumer is not a model, so it never appears in `downstream`. Walks the
/// semantic graph's model-level `downstream` edges to a fixed point.
pub(crate) fn consumers_downstream_of(
    result: &compile::CompileResult,
    model_name: &str,
) -> Vec<LineageConsumerRecord> {
    if result.consumers.is_empty() {
        return Vec::new();
    }
    let mut reachable: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut queue = vec![model_name.to_string()];
    while let Some(name) = queue.pop() {
        let Some(schema) = result.semantic_graph.model_schema(&name) else {
            continue;
        };
        for next in &schema.downstream {
            if reachable.insert(next.clone()) {
                queue.push(next.clone());
            }
        }
    }
    let mut records: Vec<LineageConsumerRecord> = result
        .consumers
        .iter()
        .filter_map(|c| {
            let direct = c.depends_on.iter().any(|m| m == model_name);
            (direct || c.depends_on.iter().any(|m| reachable.contains(m))).then(|| {
                LineageConsumerRecord {
                    name: c.name.clone(),
                    kind: c.kind.as_str().to_string(),
                    owner: c.owner.clone(),
                    url: c.url.clone(),
                    description: c.description.clone(),
                    direct,
                }
            })
        })
        .collect();
    records.sort_by(|a, b| a.name.cmp(&b.name));
    records
}

/// Side-effect-free core producing the column-level [`ColumnLineageOutput`]
/// (the `--column` / `model.column` case). Mirrors the JSON-branch assembly in
/// [`run_lineage`] without printing.
///
/// `downstream` selects the trace direction: `true` traces consumers, `false`
/// traces sources.
// Reusable typed-output core for the in-process MCP server (`rocky-mcp`),
// alongside `run_lineage`'s JSON branch.
pub fn column_lineage_output(
    result: &compile::CompileResult,
    model_name: &str,
    column: &str,
    downstream: bool,
) -> Result<ColumnLineageOutput> {
    // Assert the model exists so the column core matches `run_lineage`'s
    // model-not-found behaviour (the wrapper resolves `schema` before
    // dispatch).
    result
        .semantic_graph
        .model_schema(model_name)
        .context(format!("model '{model_name}' not found"))?;

    let direction = if downstream { "downstream" } else { "upstream" };
    let trace_edges = if downstream {
        result
            .semantic_graph
            .trace_column_downstream(model_name, column)
    } else {
        result.semantic_graph.trace_column(model_name, column)
    };
    let trace: Vec<LineageEdgeRecord> = trace_edges.iter().map(|e| to_edge_record(e)).collect();

    // Author-time downstream-impact preview: every column that transitively
    // consumes `(model_name, column)`. Always computed (independent of
    // `direction`) so the default upstream trace still surfaces the blast
    // radius. Deduped + sorted via a BTreeSet keyed on (model, column) — the
    // graph walk's order is unspecified and `LineageQualifiedColumn` is not
    // `Eq`, so `.dedup()` wouldn't apply. Mirrors `lineage_diff.rs`.
    // Inspection only: this never gates a build/skip/reuse decision.
    let mut seen: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
    for edge in result
        .semantic_graph
        .trace_column_downstream(model_name, column)
    {
        seen.insert((
            edge.target.model.to_string(),
            edge.target.column.to_string(),
        ));
    }
    let downstream_consumers: Vec<LineageQualifiedColumn> = seen
        .into_iter()
        .map(|(model, column)| LineageQualifiedColumn { model, column })
        .collect();

    Ok(ColumnLineageOutput {
        version: VERSION.to_string(),
        command: "lineage".to_string(),
        model: model_name.to_string(),
        column: column.to_string(),
        direction: direction.to_string(),
        trace,
        downstream_consumers,
        row_selection: row_selection_for_trace(
            &result.semantic_graph,
            model_name,
            column,
            downstream,
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;

    /// Write a `.sql` model plus its `.toml` sidecar into `dir`. Mirrors
    /// the helper in `ci_diff.rs`'s tests so the compile path is faithful.
    fn write_model(dir: &Path, name: &str, sql: &str) {
        fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    fn compile_chain(models_dir: &Path) -> compile::CompileResult {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            ..Default::default()
        };
        compile::compile(&config).expect("chain compiles")
    }

    /// A model's lineage lists the consumers that read it directly or through
    /// a downstream model, and not the ones that read only upstream models.
    #[test]
    fn lineage_lists_downstream_consumers() {
        let root = TempDir::new().unwrap();
        let models_dir = root.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "a", "SELECT id FROM source.raw.users");
        write_model(&models_dir, "b", "SELECT id FROM a");
        write_model(&models_dir, "c", "SELECT id FROM b");
        let consumers_dir = root.path().join("consumers");
        std::fs::create_dir_all(&consumers_dir).unwrap();
        for (file, body) in [
            (
                "direct.toml",
                "kind = \"dashboard\"\nowner = \"x\"\ndepends_on = [\"b\"]\n",
            ),
            ("via.toml", "kind = \"ml\"\ndepends_on = [\"c\"]\n"),
            ("upstream_only.toml", "depends_on = [\"a\"]\n"),
        ] {
            std::fs::write(consumers_dir.join(file), body).unwrap();
        }

        let result = compile_chain(&models_dir);
        let out = lineage_output(&result, "b").unwrap();
        let got: Vec<(&str, bool)> = out
            .consumers
            .iter()
            .map(|c| (c.name.as_str(), c.direct))
            .collect();
        assert_eq!(got, [("direct", true), ("via", false)]);
        assert_eq!(out.consumers[0].kind, "dashboard");
        assert_eq!(out.consumers[0].owner.as_deref(), Some("x"));
        // Consumers are not models: `downstream` is unchanged.
        assert_eq!(out.downstream, vec!["c".to_string()]);
        // `via` reads `c` itself, so it is direct there and the only entry.
        let leaf = lineage_output(&result, "c").unwrap();
        assert_eq!(leaf.consumers.len(), 1);
        assert!(leaf.consumers[0].direct);
        // Every consumer is downstream of `a`.
        assert_eq!(lineage_output(&result, "a").unwrap().consumers.len(), 3);
        // With no consumers declared, the field is left out of the JSON.
        let none = tempfile::TempDir::new().unwrap();
        let none_models = none.path().join("models");
        std::fs::create_dir_all(&none_models).unwrap();
        write_model(&none_models, "a", "SELECT id FROM source.raw.users");
        let json =
            serde_json::to_string(&lineage_output(&compile_chain(&none_models), "a").unwrap())
                .unwrap();
        assert!(!json.contains("consumers"), "{json}");
    }

    /// The default (upstream) column output must still carry the
    /// downstream-impact preview: every column that transitively consumes
    /// the target. Chain: a -> b -> c, so `a.id` reaches `b.id` and `c.id`.
    #[test]
    fn upstream_output_carries_downstream_consumers() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        write_model(models_dir, "a", "SELECT id, name FROM source.raw.users");
        write_model(models_dir, "b", "SELECT id, name FROM a");
        write_model(models_dir, "c", "SELECT id FROM b");

        let result = compile_chain(models_dir);
        // `downstream = false` is the default upstream trace.
        let out = column_lineage_output(&result, "a", "id", false).unwrap();

        assert_eq!(out.direction, "upstream");
        let consumers: Vec<(&str, &str)> = out
            .downstream_consumers
            .iter()
            .map(|c| (c.model.as_str(), c.column.as_str()))
            .collect();
        assert!(
            consumers.contains(&("b", "id")),
            "expected b.id in {consumers:?}"
        );
        assert!(
            consumers.contains(&("c", "id")),
            "expected c.id in {consumers:?}"
        );

        // Deterministic: BTreeSet ordering, no duplicates.
        let mut sorted = consumers.clone();
        sorted.sort();
        assert_eq!(consumers, sorted, "consumers must be sorted");
        assert_eq!(
            consumers.len(),
            consumers
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            "consumers must be deduplicated"
        );
    }

    /// #2307: `--column` traces through CTEs to the source column. The import
    /// CTE `orders` passes model `orders` through a `SELECT *`; the chain
    /// `a -> b` renames `orders.id` twice before the output reads it.
    #[test]
    fn column_trace_goes_through_ctes_to_the_source_column() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        write_model(models_dir, "orders", "SELECT id FROM source.raw.orders");
        write_model(
            models_dir,
            "imp",
            "WITH orders AS (SELECT * FROM orders) SELECT id FROM orders",
        );
        write_model(
            models_dir,
            "chain",
            "WITH a AS (SELECT id AS k FROM orders), b AS (SELECT k AS j FROM a) SELECT j FROM b",
        );

        let result = compile_chain(models_dir);
        for (model, column) in [("imp", "id"), ("chain", "j")] {
            let out = column_lineage_output(&result, model, column, false).unwrap();
            let sources: Vec<(&str, &str)> = out
                .trace
                .iter()
                .map(|e| (e.source.model.as_str(), e.source.column.as_str()))
                .collect();
            assert!(
                sources.contains(&("orders", "id")),
                "{model}.{column}: expected orders.id in {sources:?}"
            );
        }
    }

    /// A leaf column (consumed by nobody) yields an empty consumer set,
    /// which serde then omits via `skip_serializing_if`.
    #[test]
    fn leaf_column_has_no_downstream_consumers() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        write_model(models_dir, "a", "SELECT id FROM source.raw.users");
        write_model(models_dir, "b", "SELECT id FROM a");

        let result = compile_chain(models_dir);
        // `b.id` is the leaf — nothing downstream consumes it.
        let out = column_lineage_output(&result, "b", "id", false).unwrap();
        assert!(
            out.downstream_consumers.is_empty(),
            "leaf column should have no consumers, got {:?}",
            out.downstream_consumers
        );

        let json = serde_json::to_string(&out).unwrap();
        assert!(
            !json.contains("downstream_consumers"),
            "empty consumer set must be omitted from JSON"
        );
    }

    /// G6: `--column` labels row-selection edges separately from the value
    /// trace. `fct.total` derives its value from `stg.amount`, but which rows
    /// and groups exist is decided by the join key, filter and group key.
    #[test]
    fn column_output_labels_row_selection_edges() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        write_model(
            models_dir,
            "stg",
            "SELECT order_id, customer_id, amount, status FROM source.raw.orders",
        );
        write_model(
            models_dir,
            "cust",
            "SELECT customer_id, region FROM source.raw.customers",
        );
        write_model(
            models_dir,
            "fct",
            "SELECT c.region, SUM(s.amount) AS total FROM stg s \
             JOIN cust c ON s.customer_id = c.customer_id \
             WHERE s.status = 'paid' GROUP BY c.region",
        );

        let result = compile_chain(models_dir);
        let out = column_lineage_output(&result, "fct", "total", false).unwrap();

        // Value trace is unchanged: only `stg.amount` feeds `total`.
        let value_sources: Vec<(&str, &str)> = out
            .trace
            .iter()
            .filter(|e| e.target.model == "fct")
            .map(|e| (e.source.model.as_str(), e.source.column.as_str()))
            .collect();
        assert_eq!(value_sources, vec![("stg", "amount")]);

        let rs: Vec<(&str, &str, &str)> = out
            .row_selection
            .iter()
            .map(|r| {
                (
                    r.kind.as_str(),
                    r.source.model.as_str(),
                    r.source.column.as_str(),
                )
            })
            .collect();
        for expected in [
            ("join_key", "stg", "customer_id"),
            ("join_key", "cust", "customer_id"),
            ("filter", "stg", "status"),
            ("group_by", "cust", "region"),
        ] {
            assert!(rs.contains(&expected), "missing {expected:?} in {rs:?}");
        }

        // Downstream from `stg.status`: no value consumer, one filter consumer.
        let down = column_lineage_output(&result, "stg", "status", true).unwrap();
        assert!(down.trace.is_empty());
        assert_eq!(down.row_selection.len(), 1, "{:?}", down.row_selection);
        assert_eq!(down.row_selection[0].target_model, "fct");
        assert_eq!(down.row_selection[0].kind, "filter");
    }

    // ------------------------------------------------------------------
    // #1680: the caller-coupled half of #1667's conversion.
    //
    // The shared-loader test in `source_schemas.rs` stays green if someone
    // puts `.ok()` back HERE, so it does not defend this command. These two
    // drive `run_lineage` itself.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1680: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// A present-but-unloadable `rocky.toml` refuses `rocky lineage`, and the
    /// error NAMES the file. Restoring `.ok()` at the config leg makes this
    /// fail.
    #[test]
    fn lineage_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1680).unwrap();

        let err = run_lineage(
            &cfg,
            &tmp.path().join("state.redb"),
            &models_dir,
            "m",
            None,
            None,
            false,
            true,
            None,
        )
        .expect_err("a present but unloadable rocky.toml must refuse `rocky lineage`");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );
    }

    /// Absent is not invalid: a standalone `models/` directory with no
    /// `rocky.toml` still traces lineage. This is the honest-failure guard —
    /// the new refusal must not fire on a project that never had a config.
    #[test]
    fn lineage_still_runs_without_any_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        assert!(!cfg.exists());

        run_lineage(
            &cfg,
            &tmp.path().join("state.redb"),
            &models_dir,
            "m",
            None,
            None,
            false,
            true,
            None,
        )
        .expect("a missing rocky.toml must not refuse `rocky lineage`");
    }
}
