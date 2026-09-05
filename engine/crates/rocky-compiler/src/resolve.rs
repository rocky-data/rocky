//! Automatic dependency resolution from SQL table references.
//!
//! Extracts table references from model SQL and classifies them as:
//! - **Model refs** — bare names matching another model in the project
//! - **Source refs** — two-part qualified names (`schema.table`)
//! - **Raw refs** — three-part fully qualified names (`catalog.schema.table`)
//!
//! Model refs become DAG edges. Source and raw refs are external dependencies.
//!
//! A bare name matches by MODEL NAME, and that is only sometimes the same
//! question as "does this read return that model's output" (#1354). Nothing
//! rewrites a bare reference on the default path (`--defer` and shadow routing
//! are the only rewrites), so on a warehouse run `FROM customers` resolves
//! through connection state to a PHYSICAL table called `customers` — which is
//! not the output of a model that writes `prod.customers_v2`. On the local
//! path it is: `rocky_engine::executor::execute_locally` (`rocky test`,
//! `rocky ci`) materializes every model as `CREATE OR REPLACE TABLE
//! <model name>`, ignoring the configured target, so there the bare read does
//! reach the model.
//!
//! The edge is therefore kept — dropping it breaks the local path and can
//! reorder `semantic.rs`, changing `SELECT *` expansion — and the ambiguity is
//! reported instead, as D012. `rocky_core::physical_edges::bare_name_binds` is
//! the shared spelling of "does a bare read of this name reach this model's
//! target", used here and by the content-reuse read resolver.

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
///
/// The bare-name match is by model NAME, not by the model's target table. On a
/// warehouse run that can name a different object than the model writes —
/// [`resolve_dependencies`] reports it as D012 rather than dropping the edge.
/// See the module docs and #1354 for why the edge is kept.
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
    // Models a bare read of their own NAME does not reach on a warehouse run:
    // the target table is spelled differently, so the search path resolves the
    // name to some other physical object (#1354). Mapped to the target they do
    // write, for the D012 message. `bare_name_binds` is the shared spelling of
    // that question — the content-reuse read resolver asks it too.
    let renamed_targets: HashMap<&str, String> = models
        .iter()
        .filter(|m| {
            !rocky_core::physical_edges::bare_name_binds(&m.config.name, &m.config.target.table)
        })
        .map(|m| {
            let t = &m.config.target;
            let spelled = if t.catalog.is_empty() {
                format!("{}.{}", t.schema, t.table)
            } else {
                format!("{}.{}.{}", t.catalog, t.schema, t.table)
            };
            (m.config.name.as_str(), spelled)
        })
        .collect();
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

        let (auto_deps, renamed_target_reads) = extract_deps_from_lineage(
            &lineage_result,
            &model.config.name,
            &model_names,
            &renamed_targets,
        );

        // D012: this edge exists because the names match, and on a warehouse
        // run it may name a different object than the model writes. The edge is
        // KEPT — `rocky_engine::executor::execute_locally` (`rocky test`,
        // `rocky ci`) materializes each model under its own NAME, so there the
        // read really does reach it, and dropping the edge would break that
        // path and reorder `semantic.rs` (changing `SELECT *` expansion).
        // Reporting is what this layer can honestly do; #1354 holds the
        // decision about which execution semantics the graph should encode.
        for bare in &renamed_target_reads {
            let target = &renamed_targets[bare.as_str()];
            diagnostics.push(
                Diagnostic::warning(
                    "D012",
                    &model.config.name,
                    format!(
                        "bare read of '{bare}' matches model '{bare}' by name, so Rocky \
                         derives a dependency on it — but that model's configured target is \
                         '{target}'. A bare name carries no schema, so on a warehouse run it \
                         resolves through the connection's search path to a physical table \
                         called '{bare}', not to '{target}', and the edge may be false. It \
                         holds where the bare name IS the object: `rocky test` and `rocky ci` \
                         materialize each model under its own name, and `--defer` rewrites a \
                         selected model's read of an unbuilt upstream to that upstream's defer \
                         target."
                    ),
                )
                .with_suggestion(format!(
                    "Give model '{bare}' a target table called '{bare}' so the name and \
                     the object agree, or reference the object you mean explicitly in the SQL"
                )),
            );
        }

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
/// Returns two lists, both deduplicated and in SQL order:
/// 1. the names of referenced models (bare names that match known model
///    names), excluding self-references — the DAG edges;
/// 2. the subset of those whose model writes a differently-named target, so
///    the edge rests on a name match a warehouse run does not honour (#1354).
///    Only a `ModelRef` can appear here, so a dotted name never can. This
///    compares TABLE COMPONENTS only: a model named `customers` targeting
///    `prod.customers` does not qualify even though a bare read resolves
///    through the session's current schema, which may not be `prod`. Rocky
///    cannot observe that schema, so the check is "is the name even the right
///    table", not "is the binding sound".
///    Self-references are excluded from both: a model reading its own name
///    gets no edge, so there is nothing to qualify.
fn extract_deps_from_lineage(
    lineage_result: &lineage::LineageResult,
    model_name: &str,
    model_names: &HashSet<String>,
    renamed_targets: &HashMap<&str, String>,
) -> (Vec<String>, Vec<String>) {
    let mut deps = Vec::new();
    let mut seen = HashSet::new();
    let mut renamed_target_reads = Vec::new();

    for table_ref in &lineage_result.source_tables {
        if let TableRefKind::ModelRef(name) = classify_table_ref(&table_ref.name, model_names) {
            // Don't add self-references
            if name != model_name && seen.insert(name.clone()) {
                if renamed_targets.contains_key(name.as_str()) {
                    renamed_target_reads.push(name.clone());
                }
                deps.push(name);
            }
        }
    }

    (deps, renamed_target_reads)
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

    /// A bare name matching a model NAME is a model reference. #1354 asked
    /// whether it should instead match the model's TARGET; the answer is not
    /// this layer's to give (`rocky test` materializes by model name, so both
    /// answers are right on some path), so the rule is unchanged and the
    /// ambiguous case is reported as D012 instead.
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

    fn make_model_with_target(name: &str, sql: &str, schema: &str, table: &str) -> Model {
        let mut m = make_model(name, sql);
        m.config.target.schema = schema.to_string();
        m.config.target.table = table.to_string();
        m
    }

    /// #1354: a bare read that matches a model by NAME while that model writes
    /// a differently-named table. The edge is KEPT and the ambiguity reported.
    ///
    /// Keeping it is not indecision. `rocky_engine::executor::execute_locally`
    /// (`rocky test`, `rocky ci`) materializes every model as
    /// `CREATE OR REPLACE TABLE <model name>`, ignoring the configured target,
    /// so on that path this read really does return the model's output and the
    /// edge orders it correctly. On a warehouse run it does not. One graph, two
    /// execution semantics — D012 says so rather than picking silently.
    #[test]
    fn a_bare_read_of_a_renamed_target_models_name_keeps_its_edge_and_warns() {
        let models = vec![
            make_model_with_target("customers", "SELECT 1 AS id", "prod", "customers_v2"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert_eq!(
            rollup.depends_on,
            vec!["customers"],
            "the edge is kept: dropping it breaks `rocky test`, which materializes by name"
        );

        let d012: Vec<&Diagnostic> = diags.iter().filter(|d| &*d.code == "D012").collect();
        assert_eq!(d012.len(), 1, "{diags:?}");
        assert_eq!(d012[0].model, "rollup");
        assert!(
            d012[0].message.contains("customers")
                && d012[0].message.contains("warehouse.prod.customers_v2"),
            "the warning must name the read and the target it may not reach: {}",
            d012[0].message
        );
        // A wrapped message literal that loses its `\` continuations reads as
        // one line of run-together spaces; the text is user-facing, so pin it.
        assert!(
            !d012[0].message.contains("  ")
                && !d012[0].suggestion.as_deref().unwrap().contains("  "),
            "{:?} / {:?}",
            d012[0].message,
            d012[0].suggestion
        );
    }

    /// The harm #1354 reports, pinned as it stands today: the name edge makes
    /// the TRUE physical-read edge look like a cycle-closer, so the run-time
    /// derivation skips it and the pair executes in the wrong order.
    ///
    /// ```text
    ///   rollup --(name match)--------► customers
    ///   customers --(reads warehouse.silver.rollup)--► rollup   SKIPPED (cycle)
    /// ```
    ///
    /// This test asserts the DEFECT, not a fix. Invert it when #1354 is
    /// resolved — whichever way the owner resolves it, this assertion moves.
    #[test]
    fn the_name_edge_still_suppresses_the_true_physical_read_edge() {
        let models = vec![
            make_model_with_target(
                "customers",
                "SELECT y FROM warehouse.silver.rollup",
                "prod",
                "customers_v2",
            ),
            make_model("rollup", "SELECT x FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, _diags) = resolve_dependencies(&models).unwrap();
        let existing: Vec<(String, String)> = dag_nodes
            .iter()
            .flat_map(|n| {
                n.depends_on
                    .iter()
                    .map(|d| (n.name.clone(), d.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            existing,
            vec![("rollup".to_string(), "customers".to_string())]
        );

        let edge_models: Vec<rocky_core::physical_edges::PhysicalEdgeModel<'_>> = models
            .iter()
            .map(rocky_core::physical_edges::PhysicalEdgeModel::from_model)
            .collect();
        let derived = rocky_core::physical_edges::derive_physical_edges(&edge_models, &existing);
        assert!(
            derived.edges.is_empty(),
            "the true edge is still suppressed today: {derived:?}"
        );
        assert_eq!(
            derived.skipped_cycle_edges,
            vec![("customers".to_string(), "rollup".to_string())],
            "and the suppression is at least reported as a scheduling warning"
        );
    }

    /// The common case — a model whose target table is its own name — must
    /// keep its edge and must NOT warn. This is the noise guard: D012 has to
    /// stay rare or it means nothing.
    #[test]
    fn the_common_case_binds_without_a_warning() {
        let models = vec![
            make_model("customers", "SELECT 1 AS id"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["customers"]);
        assert!(
            diags.iter().all(|d| &*d.code != "D012"),
            "the common case must not warn: {diags:?}"
        );
    }

    /// The comparison folds case on both sides. A project that spells its
    /// targets in upper case (`[target] table = "CUSTOMERS"` for model
    /// `customers` — the ordinary Snowflake shape) is the SAME name, not a
    /// renamed target, and must not warn. Without the fold, D012 would fire on
    /// every model in such a project.
    #[test]
    fn a_target_table_spelled_in_another_case_does_not_warn() {
        let models = vec![
            make_model_with_target("customers", "SELECT 1 AS id", "silver", "CUSTOMERS"),
            make_model("rollup", "SELECT id FROM customers"),
        ];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        let rollup = dag_nodes.iter().find(|n| n.name == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["customers"]);
        assert!(diags.iter().all(|d| &*d.code != "D012"), "{diags:?}");
    }

    /// A model reading its OWN name gets no edge (self-references are
    /// excluded), so there is no edge to qualify and nothing to warn about.
    #[test]
    fn a_self_read_of_a_renamed_target_name_does_not_warn() {
        let models = vec![make_model_with_target(
            "customers",
            "SELECT id FROM customers WHERE status = 'active'",
            "prod",
            "customers_v2",
        )];

        let (dag_nodes, _lineage_cache, diags) = resolve_dependencies(&models).unwrap();
        assert!(dag_nodes[0].depends_on.is_empty());
        assert!(
            diags.iter().all(|d| &*d.code != "D012"),
            "a self-read has no edge to qualify: {diags:?}"
        );
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
