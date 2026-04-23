//! Blast-radius linter for `SELECT *` (P002).
//!
//! Emits a warning when a model uses `SELECT *` AND at least one downstream
//! model references specific columns of its output. The lint is
//! *blast-radius-aware*: a leaf model with `SELECT *` is intentionally not
//! flagged — the user inspected the result themselves. The diagnostic only
//! fires when an upstream schema change at the star's source would silently
//! propagate into a downstream that names columns of this model.
//!
//! Detection runs against the semantic graph rather than re-parsing SQL:
//! `ModelSchema::has_star` is set during graph construction by
//! `rocky_sql::lineage`, and `column_consumers(model, column)` enumerates
//! the exact downstream edges that make the radius concrete. No new
//! parser or type-inference machinery is required for this wave; the next
//! wave (Arc 7 type inference over raw `.sql` models) will sharpen which
//! upstream is treated as "typed."

use indexmap::IndexMap;
use std::collections::HashMap;

use crate::diagnostic::{Diagnostic, P002, SourceSpan};
use crate::semantic::SemanticGraph;

/// Cap on listed columns per downstream entry in the diagnostic message,
/// to keep miette output legible on wide schemas.
const COLS_PREVIEW_LIMIT: usize = 3;

/// Inspect the semantic graph for `SELECT *` blast-radius hazards and
/// return one warning per offending model.
///
/// `file_paths` maps model name → source file path so the resulting
/// diagnostics carry a [`SourceSpan`] pointing at the model file. Models
/// without a file path entry get a span-less diagnostic — the LSP and
/// CLI both render that gracefully.
pub fn detect_select_star_blast_radius(
    graph: &SemanticGraph,
    file_paths: &HashMap<String, String>,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    for (name, schema) in &graph.models {
        if !schema.has_star {
            continue;
        }

        let mut by_downstream: IndexMap<&str, Vec<&str>> = IndexMap::new();
        for col in &schema.columns {
            for edge in graph.column_consumers(name, &col.name) {
                let down: &str = &edge.target.model;
                by_downstream
                    .entry(down)
                    .or_default()
                    .push(col.name.as_str());
            }
        }

        if by_downstream.is_empty() {
            continue;
        }

        let summary = summarize_downstreams(&by_downstream);
        let downstream_count = by_downstream.len();
        let plural = if downstream_count == 1 {
            "consumer"
        } else {
            "consumers"
        };
        let message = format!(
            "SELECT * silently propagates upstream schema changes to {downstream_count} downstream {plural}: {summary}",
        );

        let mut diag = Diagnostic::warning(P002, name, message).with_suggestion(
            "replace SELECT * with an explicit column list to make schema dependencies visible",
        );

        if let Some(file) = file_paths.get(name) {
            diag = diag.with_span(SourceSpan {
                file: file.clone(),
                line: 1,
                col: 1,
            });
        }

        diags.push(diag);
    }

    diags
}

fn summarize_downstreams(by_downstream: &IndexMap<&str, Vec<&str>>) -> String {
    by_downstream
        .iter()
        .map(|(d, cols)| {
            let total = cols.len();
            let preview = cols
                .iter()
                .take(COLS_PREVIEW_LIMIT)
                .copied()
                .collect::<Vec<_>>()
                .join(", ");
            if total > COLS_PREVIEW_LIMIT {
                let extra = total - COLS_PREVIEW_LIMIT;
                format!("`{d}` ({preview}, +{extra} more)")
            } else {
                format!("`{d}` ({preview})")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::Severity;
    use crate::project::Project;
    use crate::semantic::build_semantic_graph;
    use rocky_core::ir::ColumnInfo;
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
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    fn file_paths_for(models: &[Model]) -> HashMap<String, String> {
        models
            .iter()
            .map(|m| (m.config.name.clone(), m.file_path.clone()))
            .collect()
    }

    #[test]
    fn flags_select_star_with_explicit_downstream_consumer() {
        // a is a typed upstream, b stars from a, c references specific
        // columns from b → b's blast radius is real.
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
            make_model("c", "SELECT id FROM b"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert_eq!(diags.len(), 1);
        let d = &diags[0];
        assert_eq!(&*d.code, "P002");
        assert_eq!(d.severity, Severity::Warning);
        assert_eq!(d.model, "b");
        assert!(d.message.contains("`c`"), "message: {}", d.message);
        assert!(d.message.contains("id"), "message: {}", d.message);
        assert_eq!(
            d.span.as_ref().map(|s| s.file.as_str()),
            Some("models/b.sql"),
        );
    }

    #[test]
    fn no_warning_when_select_star_has_no_downstream() {
        // Leaf model with SELECT * — user already inspected the result.
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert!(diags.is_empty(), "leaf SELECT * should not warn: {diags:?}",);
    }

    #[test]
    fn no_warning_when_no_select_star() {
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT id, name FROM a"),
            make_model("c", "SELECT id FROM b"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert!(diags.is_empty(), "no star → no warning: {diags:?}");
    }

    #[test]
    fn flags_select_star_from_typed_external_source() {
        // External source with known schema → star expansion is typed →
        // downstream consumer makes blast radius real.
        let models = vec![
            make_model("a", "SELECT * FROM catalog.schema.users"),
            make_model("b", "SELECT id FROM a"),
        ];
        let file_paths = file_paths_for(&models);
        let mut sources = HashMap::new();
        sources.insert(
            "catalog.schema.users".to_string(),
            vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "STRING".to_string(),
                    nullable: true,
                },
            ],
        );
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &sources).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert_eq!(diags.len(), 1, "diags: {diags:?}");
        assert_eq!(diags[0].model, "a");
        assert!(diags[0].message.contains("`b`"));
    }

    #[test]
    fn no_warning_when_upstream_is_untyped_external_source() {
        // Star expansion against an unknown source produces no columns,
        // so no consumer edges exist — no blast radius to flag.
        let models = vec![
            make_model("a", "SELECT * FROM catalog.schema.unknown"),
            make_model("b", "SELECT id FROM a"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert!(
            diags.is_empty(),
            "untyped upstream → no blast radius: {diags:?}",
        );
    }

    #[test]
    fn aggregates_columns_per_downstream() {
        // Two downstreams reference different columns of b — diagnostic
        // should list both with their referenced columns.
        let models = vec![
            make_model("a", "SELECT id, name, email FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
            make_model("c", "SELECT id, name FROM b"),
            make_model("d", "SELECT email FROM b"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert_eq!(diags.len(), 1);
        let msg = diags[0].message.to_string();
        assert!(msg.contains("2 downstream consumers"), "msg: {msg}");
        assert!(msg.contains("`c`"), "msg: {msg}");
        assert!(msg.contains("`d`"), "msg: {msg}");
    }

    #[test]
    fn truncates_long_column_lists_in_message() {
        let models = vec![
            make_model("a", "SELECT c1, c2, c3, c4, c5, c6 FROM source.raw.wide"),
            make_model("b", "SELECT * FROM a"),
            make_model("c", "SELECT c1, c2, c3, c4, c5 FROM b"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert_eq!(diags.len(), 1);
        let msg = diags[0].message.to_string();
        assert!(msg.contains("+2 more"), "msg should truncate: {msg}");
    }

    #[test]
    fn diagnostic_has_actionable_suggestion() {
        let models = vec![
            make_model("a", "SELECT id FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
            make_model("c", "SELECT id FROM b"),
        ];
        let file_paths = file_paths_for(&models);
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

        let diags = detect_select_star_blast_radius(&graph, &file_paths);
        assert_eq!(diags.len(), 1);
        assert!(
            diags[0]
                .suggestion
                .as_deref()
                .is_some_and(|s| s.contains("explicit column list")),
            "suggestion: {:?}",
            diags[0].suggestion,
        );
    }
}
