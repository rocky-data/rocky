//! Automatic dependency resolution from SQL table references.
//!
//! Extracts table references from model SQL and classifies them as:
//! - **Model refs** — bare names matching another model in the project
//! - **Source refs** — two-part qualified names (`schema.table`)
//! - **Raw refs** — three-part fully qualified names (`catalog.schema.table`)
//!
//! Model refs become DAG edges. Source and raw refs are external dependencies.

use std::collections::{HashMap, HashSet};

use rocky_core::models::Model;
use rocky_ir::dag::DagNode;
use rocky_sql::lineage;
use thiserror::Error;

use crate::diagnostic::Diagnostic;

/// Resolved output: DAG nodes, per-model lineage cache, and diagnostics.
pub type ResolveOutput = (
    Vec<DagNode>,
    HashMap<String, lineage::LineageResult>,
    Vec<Diagnostic>,
);

/// How a table reference in SQL maps to the project's dependency graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableRefKind {
    /// References another model in the project (bare name like `orders`).
    ModelRef(String),
    /// Two-part qualified reference (`schema.table`) — external source.
    SourceRef { schema: String, table: String },
    /// Three-part fully qualified reference (`catalog.schema.table`) — external.
    RawRef(String),
}

/// Errors during dependency resolution.
/// `#[non_exhaustive]` because this enum is public and adding a variant —
/// as #1224 just did — would otherwise break an external exhaustive match.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ResolveError {
    #[error("failed to extract lineage from model '{model}': {reason}")]
    LineageExtraction { model: String, reason: String },

    /// Every model whose SQL could not be parsed, not just the first.
    ///
    /// Resolution still fails as a whole — a project Rocky cannot parse is a
    /// project it cannot reason about, and that contract is unchanged. What
    /// changed is that it stops reporting one model per compile. A single
    /// unsupported construct can account for most of a project's parse
    /// failures (#1224), and fixing them one recompile at a time hides the
    /// scale: the user sees "1 model failed" repeatedly instead of "112 models
    /// failed, all on the same syntax".
    #[error(
        "{} model(s) failed to parse:\n{}",
        failures.len(),
        failures
            .iter()
            .map(|(model, reason)| format!("  - {model}: {reason}"))
            .collect::<Vec<_>>()
            .join("\n")
    )]
    LineageExtractionMany { failures: Vec<(String, String)> },
}

/// Classify a table reference name based on its structure and known models.
///
/// Rules:
/// - Bare name matching a known model → `ModelRef`
/// - Two-part `schema.table` → `SourceRef`
/// - Three-part `catalog.schema.table` → `RawRef`
/// - Bare name NOT matching a model → `RawRef` (unknown external table)
pub fn classify_table_ref(name: &str, model_names: &HashSet<String>) -> TableRefKind {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => {
            let bare_name = parts[0];
            if model_names.contains(bare_name) {
                TableRefKind::ModelRef(bare_name.to_string())
            } else {
                // Unknown bare name — treat as external
                TableRefKind::RawRef(name.to_string())
            }
        }
        2 => TableRefKind::SourceRef {
            schema: parts[0].to_string(),
            table: parts[1].to_string(),
        },
        _ => {
            // 3+ parts — fully qualified external reference
            TableRefKind::RawRef(name.to_string())
        }
    }
}

/// Resolve dependencies for all models by parsing their SQL.
///
/// Returns `DagNode` entries with `depends_on` auto-populated from SQL table refs
/// that match other model names, along with a cache of `LineageResult` per model
/// (keyed by model name) so downstream phases can reuse the parsed lineage
/// without re-parsing SQL.
///
/// If a model already has explicit `depends_on` in its config, those are
/// preserved and merged with auto-resolved dependencies.
pub fn resolve_dependencies(models: &[Model]) -> Result<ResolveOutput, ResolveError> {
    let model_names: HashSet<String> = models.iter().map(|m| m.config.name.clone()).collect();
    let mut dag_nodes = Vec::with_capacity(models.len());
    let mut lineage_cache = HashMap::with_capacity(models.len());
    let mut diagnostics = Vec::new();

    // Collect EVERY parse failure rather than propagating the first (#1224).
    //
    // Done in the SAME pass as the real work, not a pre-pass: a pre-pass costs
    // a second `extract_lineage` for every model on the success path — 2N
    // parses on a healthy project, which is the common case and the one that
    // must stay fast. Here a model that parses is parsed once and its result
    // used; only a model that FAILS is recorded, and after the first failure
    // the remaining work is skipped since the result is already an error.
    let mut parse_failures: Vec<(String, String)> = Vec::new();

    for model in models {
        let lineage_result = match lineage::extract_lineage(&model.sql) {
            Ok(result) => result,
            Err(reason) => {
                parse_failures.push((model.config.name.clone(), reason));
                continue;
            }
        };
        if !parse_failures.is_empty() {
            // Already failing: keep parsing to complete the report, but skip
            // the dependency/diagnostic work whose output is about to be
            // discarded.
            continue;
        }

        let auto_deps =
            extract_deps_from_lineage(&lineage_result, &model.config.name, &model_names);

        // D011: warn when explicit depends_on is non-empty but misses auto-derived deps
        if !model.config.depends_on.is_empty() {
            let explicit: HashSet<&str> = model
                .config
                .depends_on
                .iter()
                .map(std::string::String::as_str)
                .collect();
            let missing: Vec<&String> = auto_deps
                .iter()
                .filter(|d| !explicit.contains(d.as_str()))
                .collect();
            if !missing.is_empty() {
                let missing_str = missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                diagnostics.push(
                    Diagnostic::warning(
                        "D011",
                        &model.config.name,
                        format!(
                            "depends_on declares [{}] but SQL body also references [{}]. \
                             The auto-derived dependencies will be merged, but consider \
                             updating depends_on or removing it to let auto-derivation \
                             handle everything.",
                            model.config.depends_on.join(", "),
                            missing_str,
                        ),
                    )
                    .with_suggestion(format!(
                        "Add '{}' to depends_on, or remove the depends_on field entirely",
                        missing_str,
                    )),
                );
            }
        }

        // Merge: explicit depends_on + auto-resolved, deduplicated via HashSet
        let mut all_deps: Vec<String> = model.config.depends_on.clone();
        let mut seen: HashSet<String> = all_deps.iter().cloned().collect();
        for dep in auto_deps {
            if seen.insert(dep.clone()) {
                all_deps.push(dep);
            }
        }

        lineage_cache.insert(model.config.name.clone(), lineage_result);

        dag_nodes.push(DagNode {
            name: model.config.name.clone(),
            depends_on: all_deps,
        });
    }

    if !parse_failures.is_empty() {
        // Deterministic order: the same project must produce the same message
        // twice, and a set that changes order between runs is unreadable in CI.
        parse_failures.sort();
        return Err(ResolveError::LineageExtractionMany {
            failures: parse_failures,
        });
    }

    Ok((dag_nodes, lineage_cache, diagnostics))
}

/// Extract model dependencies from a pre-computed `LineageResult`.
///
/// Returns the names of referenced models (bare names that match known model
/// names), excluding self-references and duplicates.
fn extract_deps_from_lineage(
    lineage_result: &lineage::LineageResult,
    model_name: &str,
    model_names: &HashSet<String>,
) -> Vec<String> {
    let mut deps = Vec::new();
    let mut seen = HashSet::new();

    for table_ref in &lineage_result.source_tables {
        if let TableRefKind::ModelRef(name) = classify_table_ref(&table_ref.name, model_names) {
            // Don't add self-references
            if name != model_name && seen.insert(name.clone()) {
                deps.push(name);
            }
        }
    }

    deps
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

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
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    /// #1224: every unparseable model is reported, not just the first.
    ///
    /// Resolution still fails as a whole — that contract is unchanged. But a
    /// single unsupported construct can account for most of a real project's
    /// parse failures, and reporting one per compile hides the scale: the user
    /// fixes one, recompiles, and meets the next.
    #[test]
    fn every_unparseable_model_is_reported_not_just_the_first() {
        // Deliberately malformed SQL, not a merely-unsupported construct.
        //
        // This fixture used to be `SELECT * EXCEPT (...)`, which the parser
        // rejected — until #1224 enabled `supports_select_wildcard_except` and
        // it started parsing, silently emptying this test. A test that proves
        // "every failure is reported" must not rest on a syntax gap someone
        // will eventually close; unbalanced parens cannot become valid.
        let models = vec![
            make_model("a", "SELECT ((( FROM raw.t"),
            make_model("ok", "SELECT 1 AS id"),
            make_model("b", "SELECT )))"),
        ];

        let err = resolve_dependencies(&models).expect_err("unparseable SQL must fail resolution");
        let ResolveError::LineageExtractionMany { failures } = &err else {
            panic!("expected the multi-failure variant, got {err:?}");
        };

        let names: Vec<&str> = failures.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["a", "b"],
            "both failures reported, sorted for a deterministic message; the \
             parseable model contributes nothing"
        );

        let rendered = err.to_string();
        assert!(
            rendered.contains("2 model(s) failed to parse"),
            "{rendered}"
        );
        assert!(
            rendered.contains("- a:") && rendered.contains("- b:"),
            "{rendered}"
        );
    }

    fn make_model_with_deps(name: &str, sql: &str, deps: Vec<&str>) -> Model {
        let mut m = make_model(name, sql);
        m.config.depends_on = deps.into_iter().map(String::from).collect();
        m
    }

    #[test]
    fn test_classify_bare_name_model() {
        let models: HashSet<String> = ["orders", "customers"]
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            classify_table_ref("orders", &models),
            TableRefKind::ModelRef("orders".to_string())
        );
    }

    #[test]
    fn test_classify_bare_name_unknown() {
        let models: HashSet<String> = ["orders"].iter().map(ToString::to_string).collect();
        assert_eq!(
            classify_table_ref("unknown_table", &models),
            TableRefKind::RawRef("unknown_table".to_string())
        );
    }

    #[test]
    fn test_classify_two_part() {
        let models: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_table_ref("staging.orders", &models),
            TableRefKind::SourceRef {
                schema: "staging".to_string(),
                table: "orders".to_string(),
            }
        );
    }

    #[test]
    fn test_classify_three_part() {
        let models: HashSet<String> = HashSet::new();
        assert_eq!(
            classify_table_ref("catalog.schema.table", &models),
            TableRefKind::RawRef("catalog.schema.table".to_string())
        );
    }

    #[test]
    fn test_resolve_simple_dependency() {
        let models = vec![
            make_model("orders", "SELECT * FROM raw_orders"),
            make_model("raw_orders", "SELECT * FROM source.fivetran.orders"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();

        // orders depends on raw_orders
        let orders_node = dag_nodes.iter().find(|n| n.name == "orders").unwrap();
        assert_eq!(orders_node.depends_on, vec!["raw_orders"]);

        // raw_orders has no model dependencies (source is external)
        let raw_node = dag_nodes.iter().find(|n| n.name == "raw_orders").unwrap();
        assert!(raw_node.depends_on.is_empty());
    }

    #[test]
    fn test_resolve_join_dependencies() {
        let models = vec![
            make_model(
                "customer_orders",
                "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
            ),
            make_model("orders", "SELECT * FROM catalog.raw.orders"),
            make_model("customers", "SELECT * FROM catalog.raw.customers"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let co_node = dag_nodes
            .iter()
            .find(|n| n.name == "customer_orders")
            .unwrap();

        assert!(co_node.depends_on.contains(&"orders".to_string()));
        assert!(co_node.depends_on.contains(&"customers".to_string()));
        assert_eq!(co_node.depends_on.len(), 2);
    }

    #[test]
    fn test_resolve_external_refs_not_dependencies() {
        let models = vec![make_model(
            "summary",
            "SELECT * FROM warehouse.staging.orders",
        )];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert!(node.depends_on.is_empty());
    }

    #[test]
    fn test_resolve_merges_explicit_and_auto() {
        let models = vec![
            make_model_with_deps("customer_orders", "SELECT * FROM orders", vec!["extra_dep"]),
            make_model("orders", "SELECT 1"),
            make_model("extra_dep", "SELECT 1"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let co_node = dag_nodes
            .iter()
            .find(|n| n.name == "customer_orders")
            .unwrap();

        // Both explicit (extra_dep) and auto-resolved (orders) present
        assert!(co_node.depends_on.contains(&"extra_dep".to_string()));
        assert!(co_node.depends_on.contains(&"orders".to_string()));
    }

    #[test]
    fn test_resolve_no_self_reference() {
        // A model referencing itself should not create a self-dependency
        let models = vec![make_model(
            "orders",
            "SELECT * FROM orders WHERE status = 'active'",
        )];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "orders").unwrap();
        assert!(node.depends_on.is_empty());
    }

    #[test]
    fn test_resolve_deduplicates() {
        // orders referenced twice in SQL (FROM + JOIN) should appear once
        let models = vec![
            make_model(
                "summary",
                "SELECT a.id, b.name FROM orders a JOIN orders b ON a.id = b.id",
            ),
            make_model("orders", "SELECT 1 AS id, 'test' AS name"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let node = dag_nodes.iter().find(|n| n.name == "summary").unwrap();
        assert_eq!(node.depends_on, vec!["orders"]);
    }
}
