//! Cross-DAG semantic graph: column-level lineage across model boundaries.
//!
//! Chains single-model lineage from `rocky_sql::lineage` into a full-project
//! semantic graph tracking which source columns flow through which models to
//! which final outputs.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use rocky_core::ir::ColumnInfo;
use rocky_sql::lineage::{self, TransformKind};
use serde::{Deserialize, Serialize};

use crate::project::Project;

/// A column fully qualified by its model (or source) name.
///
/// Uses `Arc<str>` for cheap cloning — cloning a `QualifiedColumn` is an
/// atomic reference-count increment instead of two heap allocations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedColumn {
    /// Model name or source name.
    pub model: Arc<str>,
    /// Column name.
    pub column: Arc<str>,
}

/// An edge in the semantic graph connecting columns across models.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Upstream column (source).
    pub source: QualifiedColumn,
    /// Downstream column (target).
    pub target: QualifiedColumn,
    /// How the column is transformed.
    pub transform: TransformKind,
}

/// Definition of a column in a model's output schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
}

/// Inferred schema for a single model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    /// Output columns of this model.
    pub columns: Vec<ColumnDef>,
    /// Whether this model uses SELECT * (schema depends on upstream).
    pub has_star: bool,
    /// Models this model depends on.
    pub upstream: Vec<String>,
    /// Models that depend on this model.
    pub downstream: Vec<String>,
    /// Natural language intent (from model config).
    pub intent: Option<String>,
}

/// The full semantic graph for a project.
///
/// `edges` is the canonical store; the two index maps below are derived from
/// it and exist purely to avoid the O(N·E) scan-all-edges pattern that bends
/// the type-checker's curve at 10k+ models. Always construct via
/// [`SemanticGraph::new`] (or call [`SemanticGraph::rebuild_indices`] manually
/// if you mutate `edges` after construction or load from a deserialized form).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticGraph {
    /// Per-model schemas, in topological order.
    pub models: IndexMap<String, ModelSchema>,
    /// All column lineage edges across the project.
    pub edges: Vec<LineageEdge>,
    /// `target.model` → indices into `edges`. Built from `edges`; not serialized.
    #[serde(skip, default)]
    edges_by_target_model: HashMap<String, Vec<usize>>,
    /// `(target.model, target.column)` → index into `edges`. Built from `edges`; not serialized.
    #[serde(skip, default)]
    edge_by_target_column: HashMap<(String, String), usize>,
}

impl SemanticGraph {
    /// Construct a graph and build the edge indices in one shot.
    pub fn new(models: IndexMap<String, ModelSchema>, edges: Vec<LineageEdge>) -> Self {
        let mut graph = SemanticGraph {
            models,
            edges,
            edges_by_target_model: HashMap::new(),
            edge_by_target_column: HashMap::new(),
        };
        graph.rebuild_indices();
        graph
    }

    /// Rebuild the derived edge indices from `self.edges`.
    ///
    /// Call this after any mutation to `edges` or after loading from a
    /// deserialized form (the index maps are `#[serde(skip)]`).
    pub fn rebuild_indices(&mut self) {
        let mut by_model: HashMap<String, Vec<usize>> = HashMap::with_capacity(self.models.len());
        let mut by_column: HashMap<(String, String), usize> =
            HashMap::with_capacity(self.edges.len());
        for (idx, edge) in self.edges.iter().enumerate() {
            by_model
                .entry(edge.target.model.to_string())
                .or_default()
                .push(idx);
            // First-writer-wins: matches the semantics of the prior `iter().find(...)`
            // call site at typecheck.rs:172. Multiple edges may target the same
            // (model, column) when SELECT * expansion overlaps with explicit projections;
            // the first edge inserted is the producing one.
            by_column
                .entry((
                    edge.target.model.to_string(),
                    edge.target.column.to_string(),
                ))
                .or_insert(idx);
        }
        self.edges_by_target_model = by_model;
        self.edge_by_target_column = by_column;
    }

    /// Get the schema for a model.
    pub fn model_schema(&self, name: &str) -> Option<&ModelSchema> {
        self.models.get(name)
    }

    /// Iterate edges whose `target.model` equals `model`. O(fan-in) lookup.
    pub fn edges_targeting<'a>(&'a self, model: &str) -> impl Iterator<Item = &'a LineageEdge> {
        self.edges_by_target_model
            .get(model)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .map(move |&idx| &self.edges[idx])
    }

    /// Find the producing edge for `(model, column)`. O(1) lookup.
    pub fn producing_edge(&self, model: &str, column: &str) -> Option<&LineageEdge> {
        // Allocation-free lookup is awkward without nightly raw_entry; the cost
        // of one String alloc per call is negligible vs the prior full edge scan.
        let key = (model.to_string(), column.to_string());
        self.edge_by_target_column
            .get(&key)
            .map(|&idx| &self.edges[idx])
    }

    /// Trace a column backward to its ultimate sources.
    pub fn trace_column(&self, model: &str, column: &str) -> Vec<&LineageEdge> {
        let mut result = Vec::new();
        let mut stack = vec![QualifiedColumn {
            model: Arc::from(model),
            column: Arc::from(column),
        }];
        let mut visited = std::collections::HashSet::new();

        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            for edge in self.edges_targeting(&current.model) {
                if edge.target.column == current.column {
                    result.push(edge);
                    stack.push(edge.source.clone()); // cheap: atomic ref-count inc
                }
            }
        }

        result
    }
}

/// Build a semantic graph from a resolved project.
///
/// Iterates models in topological order, extracts per-model lineage,
/// and chains edges across model boundaries.
///
/// `source_schemas` provides column info for external sources (from warehouse
/// `DESCRIBE TABLE` or cache). Models that reference external tables without
/// schema info get `has_star = true` treatment.
pub fn build_semantic_graph(
    project: &Project,
    source_schemas: &HashMap<String, Vec<ColumnInfo>>,
) -> Result<SemanticGraph, String> {
    let mut models: IndexMap<String, ModelSchema> = IndexMap::new();
    let mut edges: Vec<LineageEdge> = Vec::new();

    // Build upstream/downstream maps from DAG using references to avoid
    // cloning every model name into both maps.
    let mut downstream_map: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut upstream_map: HashMap<&str, &[String]> = HashMap::new();

    for node in &project.dag_nodes {
        upstream_map.insert(node.name.as_str(), node.depends_on.as_slice());
        for dep in &node.depends_on {
            downstream_map
                .entry(dep.as_str())
                .or_default()
                .push(node.name.as_str());
        }
    }

    // Process models in topological order
    for model_name in &project.execution_order {
        let model = match project.model(model_name) {
            Some(m) => m,
            None => continue,
        };

        // Create one Arc<str> for this model name — every edge targeting this
        // model reuses this arc via cheap atomic ref-count increments.
        let model_name_arc: Arc<str> = Arc::from(model_name.as_str());

        let lineage_result = match project.lineage_cache.get(model_name) {
            Some(cached) => cached.clone(),
            None => lineage::extract_lineage(&model.sql)
                .map_err(|e| format!("lineage extraction failed for '{}': {}", model_name, e))?,
        };

        // Build table alias → resolved name map for this model's SQL
        let alias_to_table: HashMap<String, String> = lineage_result
            .source_tables
            .iter()
            .filter_map(|t| t.alias.as_ref().map(|a| (a.clone(), t.name.clone())))
            .collect();

        // Collect output columns. We track names in a parallel HashSet so the
        // SELECT * dedup pass below is O(1) per column instead of O(C²) per
        // model — at 20k models with wide schemas the Vec::contains pattern
        // dominated star-expanded models.
        let mut output_columns: Vec<ColumnDef> = Vec::new();
        let mut output_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        for col_lineage in &lineage_result.columns {
            output_names.insert(col_lineage.target_column.clone());
            output_columns.push(ColumnDef {
                name: col_lineage.target_column.clone(),
            });

            // Resolve source table name to a model or external source
            let source_table = col_lineage.source_table.as_ref().and_then(|t| {
                // If it's an alias, resolve to the real table name
                alias_to_table.get(t).cloned().or(Some(t.clone()))
            });

            if let Some(ref source_name) = source_table {
                edges.push(LineageEdge {
                    source: QualifiedColumn {
                        model: Arc::from(source_name.as_str()),
                        column: Arc::from(col_lineage.source_column.as_str()),
                    },
                    target: QualifiedColumn {
                        model: model_name_arc.clone(), // cheap: atomic inc
                        column: Arc::from(col_lineage.target_column.as_str()),
                    },
                    transform: col_lineage.transform.clone(),
                });
            }
        }

        // If SELECT *, expand upstream model columns into output
        if lineage_result.has_star {
            for table_ref in &lineage_result.source_tables {
                let table_name = &table_ref.name;
                let table_name_arc: Arc<str> = Arc::from(table_name.as_str());
                // Check if it's a model
                if let Some(upstream_schema) = models.get(table_name.as_str()) {
                    for col in &upstream_schema.columns {
                        if output_names.insert(col.name.clone()) {
                            let col_arc: Arc<str> = Arc::from(col.name.as_str());
                            output_columns.push(ColumnDef {
                                name: col.name.clone(),
                            });
                            edges.push(LineageEdge {
                                source: QualifiedColumn {
                                    model: table_name_arc.clone(), // cheap: atomic inc
                                    column: col_arc.clone(),       // cheap: atomic inc
                                },
                                target: QualifiedColumn {
                                    model: model_name_arc.clone(), // cheap: atomic inc
                                    column: col_arc,
                                },
                                transform: TransformKind::Direct,
                            });
                        }
                    }
                } else if let Some(source_cols) = source_schemas.get(table_name.as_str()) {
                    // External source with known schema
                    for col in source_cols {
                        if output_names.insert(col.name.clone()) {
                            let col_arc: Arc<str> = Arc::from(col.name.as_str());
                            output_columns.push(ColumnDef {
                                name: col.name.clone(),
                            });
                            edges.push(LineageEdge {
                                source: QualifiedColumn {
                                    model: table_name_arc.clone(), // cheap: atomic inc
                                    column: col_arc.clone(),       // cheap: atomic inc
                                },
                                target: QualifiedColumn {
                                    model: model_name_arc.clone(), // cheap: atomic inc
                                    column: col_arc,
                                },
                                transform: TransformKind::Direct,
                            });
                        }
                    }
                }
            }
        }

        models.insert(
            model_name.clone(),
            ModelSchema {
                columns: output_columns,
                has_star: lineage_result.has_star,
                upstream: upstream_map
                    .get(model_name.as_str())
                    .map(|deps| deps.iter().map(std::string::ToString::to_string).collect())
                    .unwrap_or_default(),
                downstream: downstream_map
                    .get(model_name.as_str())
                    .map(|v| v.iter().map(std::string::ToString::to_string).collect())
                    .unwrap_or_default(),
                intent: model.config.intent.clone(),
            },
        );
    }

    Ok(SemanticGraph::new(models, edges))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, sql: &str) -> Model {
        Model {
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "silver".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    #[test]
    fn test_linear_chain_lineage() {
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT id, name FROM a"),
            make_model("c", "SELECT id FROM b"),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        assert_eq!(graph.models.len(), 3);

        // c.id traces back through b to a
        let c_edges: Vec<_> = graph
            .edges
            .iter()
            .filter(|e| &*e.target.model == "c")
            .collect();
        assert!(!c_edges.is_empty());
        assert_eq!(&*c_edges[0].source.model, "b");
        assert_eq!(&*c_edges[0].source.column, "id");
    }

    #[test]
    fn test_diamond_lineage() {
        let models = vec![
            make_model("a", "SELECT id, value FROM source.raw.data"),
            make_model("b", "SELECT id, value AS b_value FROM a"),
            make_model("c", "SELECT id, value AS c_value FROM a"),
            make_model(
                "d",
                "SELECT b.id, b.b_value, c.c_value FROM b JOIN c ON b.id = c.id",
            ),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        // d should have edges from both b and c
        let d_edges: Vec<_> = graph
            .edges
            .iter()
            .filter(|e| &*e.target.model == "d")
            .collect();
        let d_sources: Vec<&str> = d_edges.iter().map(|e| &*e.source.model).collect();
        assert!(d_sources.contains(&"b"));
        assert!(d_sources.contains(&"c"));
    }

    #[test]
    fn test_star_expansion_from_model() {
        let models = vec![
            make_model("a", "SELECT id, name, email FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        // b should inherit a's columns
        let b_schema = graph.model_schema("b").unwrap();
        assert!(b_schema.has_star);
        let b_col_names: Vec<&str> = b_schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(b_col_names.contains(&"id"));
        assert!(b_col_names.contains(&"name"));
        assert!(b_col_names.contains(&"email"));
    }

    #[test]
    fn test_star_expansion_from_source_schema() {
        let models = vec![make_model(
            "a",
            "SELECT * FROM catalog.schema.external_table",
        )];

        let mut source_schemas = HashMap::new();
        source_schemas.insert(
            "catalog.schema.external_table".to_string(),
            vec![
                ColumnInfo {
                    name: "col1".to_string(),
                    data_type: "STRING".to_string(),
                    nullable: true,
                },
                ColumnInfo {
                    name: "col2".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                },
            ],
        );

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &source_schemas).unwrap();

        let a_schema = graph.model_schema("a").unwrap();
        assert_eq!(a_schema.columns.len(), 2);
        assert_eq!(a_schema.columns[0].name, "col1");
        assert_eq!(a_schema.columns[1].name, "col2");
    }

    #[test]
    fn test_trace_column() {
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT id, name FROM a"),
            make_model("c", "SELECT id FROM b"),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        // Trace c.id backward
        let trace = graph.trace_column("c", "id");
        assert!(!trace.is_empty());
        // Should find edges from b→c and a→b
        let source_models: Vec<&str> = trace.iter().map(|e| &*e.source.model).collect();
        assert!(source_models.contains(&"b"));
        assert!(source_models.contains(&"a"));
    }

    #[test]
    fn test_upstream_downstream() {
        let models = vec![
            make_model("a", "SELECT 1 AS id"),
            make_model("b", "SELECT id FROM a"),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let a_schema = graph.model_schema("a").unwrap();
        assert_eq!(a_schema.downstream, vec!["b"]);
        assert!(a_schema.upstream.is_empty());

        let b_schema = graph.model_schema("b").unwrap();
        assert_eq!(b_schema.upstream, vec!["a"]);
        assert!(b_schema.downstream.is_empty());
    }

    #[test]
    fn test_transform_kind_tracked() {
        let models = vec![
            make_model("a", "SELECT id, amount FROM source.raw.orders"),
            make_model(
                "b",
                "SELECT id, CAST(amount AS BIGINT) AS amount_int FROM a",
            ),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let cast_edge = graph
            .edges
            .iter()
            .find(|e| &*e.target.model == "b" && &*e.target.column == "amount_int");
        assert!(cast_edge.is_some());
        assert_eq!(cast_edge.unwrap().transform, TransformKind::Cast);
    }
}
