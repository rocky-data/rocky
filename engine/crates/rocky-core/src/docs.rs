//! Documentation site generator — builds a single-page HTML model catalog.
//!
//! Converts Rocky's internal [`Model`] and [`RockyConfig`] types into
//! documentation-specific structs ([`DocModel`], [`DocColumn`], [`DocIndex`])
//! and renders them as a self-contained HTML page with no external
//! dependencies.
//!
//! This module provides the **foundation** layer only: types and HTML
//! generation. The CLI command and file writing live elsewhere.

use serde::{Deserialize, Serialize};

use crate::config::RockyConfig;
use crate::ir::ColumnInfo;
use crate::models::{Model, StrategyConfig};
use crate::tests::TestDecl;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A column in a documented model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocColumn {
    /// Column name.
    pub name: String,
    /// SQL data type (e.g., `STRING`, `BIGINT`, `TIMESTAMP`).
    pub data_type: String,
    /// Whether the column accepts NULLs.
    pub nullable: bool,
}

/// A single model entry in the documentation catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocModel {
    /// Model name (stem of the `.sql` / `.rocky` file).
    pub name: String,
    /// Human-readable description (from the `intent` field in the sidecar).
    pub description: Option<String>,
    /// Fully-qualified target (`catalog.schema.table`).
    pub target: String,
    /// Materialization strategy label.
    pub strategy: String,
    /// Names of upstream models this model depends on.
    pub depends_on: Vec<String>,
    /// Column metadata (populated when schema info is available).
    pub columns: Vec<DocColumn>,
    /// Declarative tests declared on this model.
    pub tests: Vec<TestDecl>,
}

/// Top-level documentation index — the input to [`generate_index_html`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocIndex {
    /// All documented models.
    pub models: Vec<DocModel>,
    /// Number of pipelines in the config.
    pub pipeline_count: usize,
    /// Number of adapters in the config.
    pub adapter_count: usize,
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

/// Returns a human-readable label for a [`StrategyConfig`].
fn strategy_label(strategy: &StrategyConfig) -> String {
    match strategy {
        StrategyConfig::FullRefresh => "full_refresh".into(),
        StrategyConfig::Incremental { .. } => "incremental".into(),
        StrategyConfig::Merge { .. } => "merge".into(),
        StrategyConfig::TimeInterval { .. } => "time_interval".into(),
        StrategyConfig::Ephemeral => "ephemeral".into(),
        StrategyConfig::DeleteInsert { .. } => "delete_insert".into(),
        StrategyConfig::Microbatch { .. } => "microbatch".into(),
    }
}

/// Builds a [`DocIndex`] from a slice of compiled models and the project
/// configuration.
///
/// Column metadata is not available from the model files alone — pass
/// `column_map` to attach schema information from a prior `DESCRIBE TABLE`
/// or compile step. If `None`, every model will have an empty `columns` vec.
pub fn build_doc_index(
    models: &[Model],
    config: &RockyConfig,
    column_map: Option<&std::collections::HashMap<String, Vec<ColumnInfo>>>,
) -> DocIndex {
    let mut doc_models: Vec<DocModel> = models
        .iter()
        .map(|m| {
            let target = format!(
                "{}.{}.{}",
                m.config.target.catalog, m.config.target.schema, m.config.target.table
            );
            let columns = column_map
                .and_then(|cm| cm.get(&m.config.name))
                .map(|cols| {
                    cols.iter()
                        .map(|c| DocColumn {
                            name: c.name.clone(),
                            data_type: c.data_type.clone(),
                            nullable: c.nullable,
                        })
                        .collect()
                })
                .unwrap_or_default();

            DocModel {
                name: m.config.name.clone(),
                description: m.config.intent.clone(),
                target,
                strategy: strategy_label(&m.config.strategy),
                depends_on: m.config.depends_on.clone(),
                columns,
                tests: m.config.tests.clone(),
            }
        })
        .collect();

    doc_models.sort_by(|a, b| a.name.cmp(&b.name));

    DocIndex {
        models: doc_models,
        pipeline_count: config.pipelines.len(),
        adapter_count: config.adapters.len(),
    }
}

// ---------------------------------------------------------------------------
// HTML generation
// ---------------------------------------------------------------------------

/// Escapes a string for safe inclusion in HTML content.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Generates a self-contained single-page HTML catalog from a [`DocIndex`].
///
/// The page uses:
/// - A summary banner with model, pipeline, and adapter counts
/// - A filterable table of all models (name, target, strategy, deps, tests)
/// - Expandable `<details>` per model showing columns, dependencies, and tests
/// - Inline CSS, dark theme, monospace — no external dependencies
pub fn generate_index_html(index: &DocIndex) -> String {
    let mut html = String::with_capacity(16 * 1024);

    // --- Head ---
    html.push_str(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rocky — Model Catalog</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; padding: 0; }
  a { color: #58a6ff; text-decoration: none; }
  a:hover { text-decoration: underline; }

  .header { background: #161b22; border-bottom: 1px solid #30363d; padding: 16px 24px; display: flex; align-items: center; gap: 16px; }
  .header h1 { font-size: 18px; font-weight: 600; color: #58a6ff; }
  .header .stats { font-size: 13px; color: #8b949e; margin-left: auto; }
  .stat-badge { display: inline-block; background: #21262d; border: 1px solid #30363d; border-radius: 12px; padding: 2px 10px; margin-left: 8px; font-variant-numeric: tabular-nums; }

  .toolbar { background: #161b22; border-bottom: 1px solid #30363d; padding: 8px 24px; }
  #search { background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; padding: 6px 12px; border-radius: 6px; font-size: 13px; width: 320px; }
  #search:focus { outline: none; border-color: #58a6ff; box-shadow: 0 0 0 2px rgba(88,166,255,0.15); }

  .content { max-width: 1200px; margin: 24px auto; padding: 0 24px; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace; }
  thead th { text-align: left; padding: 8px 12px; border-bottom: 2px solid #30363d; color: #8b949e; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }
  tbody td { padding: 8px 12px; border-bottom: 1px solid #21262d; vertical-align: top; }
  tbody tr:hover { background: #161b22; }
  tbody tr.hidden { display: none; }

  .strategy { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .strategy-full_refresh { background: #1f3a2e; color: #3fb950; }
  .strategy-incremental { background: #2a2016; color: #d29922; }
  .strategy-merge { background: #241830; color: #bc8cff; }
  .strategy-time_interval { background: #1a2332; color: #58a6ff; }
  .strategy-ephemeral { background: #21262d; color: #8b949e; }
  .strategy-delete_insert { background: #2d1a1a; color: #f85149; }
  .strategy-microbatch { background: #1a2332; color: #79c0ff; }

  .count { color: #8b949e; font-variant-numeric: tabular-nums; }

  details { margin: 4px 0; }
  details summary { cursor: pointer; color: #58a6ff; font-size: 13px; }
  details summary:hover { text-decoration: underline; }
  .detail-panel { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px 16px; margin: 8px 0 4px 0; }
  .detail-panel h4 { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin: 12px 0 6px 0; }
  .detail-panel h4:first-child { margin-top: 0; }
  .description { color: #c9d1d9; font-style: italic; margin-bottom: 8px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }

  .col-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .col-table th { text-align: left; padding: 4px 8px; border-bottom: 1px solid #30363d; color: #8b949e; font-weight: 600; }
  .col-table td { padding: 4px 8px; border-bottom: 1px solid #21262d; }
  .col-type { color: #d29922; }
  .col-nullable { color: #8b949e; font-size: 11px; }

  .dep-list, .test-list { list-style: none; padding: 0; }
  .dep-list li, .test-list li { padding: 2px 0; font-size: 12px; }
  .dep-list li::before { content: "\2192 "; color: #8b949e; }
  .test-badge { display: inline-block; padding: 1px 6px; border-radius: 8px; font-size: 10px; font-weight: 500; margin-right: 4px; }
  .test-error { background: #2d1a1a; color: #f85149; }
  .test-warning { background: #2a2016; color: #d29922; }

  .empty { color: #484f58; font-style: italic; }

  footer { text-align: center; padding: 24px; color: #484f58; font-size: 11px; border-top: 1px solid #21262d; margin-top: 48px; }
</style>
</head>
<body>
"#,
    );

    // --- Header ---
    html.push_str(&format!(
        r#"<div class="header">
  <h1>Rocky Model Catalog</h1>
  <div class="stats">
    Models<span class="stat-badge">{}</span>
    Pipelines<span class="stat-badge">{}</span>
    Adapters<span class="stat-badge">{}</span>
  </div>
</div>
"#,
        index.models.len(),
        index.pipeline_count,
        index.adapter_count,
    ));

    // --- Search toolbar ---
    html.push_str(
        r#"<div class="toolbar">
  <input type="text" id="search" placeholder="Filter models..." autocomplete="off">
</div>
"#,
    );

    // --- Model table ---
    html.push_str(
        r#"<div class="content">
<table id="model-table">
<thead>
  <tr>
    <th>Model</th>
    <th>Target</th>
    <th>Strategy</th>
    <th>Deps</th>
    <th>Tests</th>
  </tr>
</thead>
<tbody>
"#,
    );

    for model in &index.models {
        let name_escaped = html_escape(&model.name);
        let target_escaped = html_escape(&model.target);
        let strategy_escaped = html_escape(&model.strategy);
        let dep_count = model.depends_on.len();
        let test_count = model.tests.len();

        html.push_str(&format!(
            r#"<tr data-name="{name_lower}">
  <td>
    <details>
      <summary>{name}</summary>
      <div class="detail-panel">
"#,
            name_lower = html_escape(&model.name.to_lowercase()),
            name = name_escaped,
        ));

        // Description
        if let Some(desc) = &model.description {
            html.push_str(&format!(
                "        <div class=\"description\">{}</div>\n",
                html_escape(desc)
            ));
        }

        // Columns section
        html.push_str("        <h4>Columns</h4>\n");
        if model.columns.is_empty() {
            html.push_str("        <p class=\"empty\">No column metadata available</p>\n");
        } else {
            html.push_str(
                r#"        <table class="col-table">
          <thead><tr><th>Name</th><th>Type</th><th>Nullable</th></tr></thead>
          <tbody>
"#,
            );
            for col in &model.columns {
                html.push_str(&format!(
                    "            <tr><td>{}</td><td class=\"col-type\">{}</td><td class=\"col-nullable\">{}</td></tr>\n",
                    html_escape(&col.name),
                    html_escape(&col.data_type),
                    if col.nullable { "yes" } else { "no" },
                ));
            }
            html.push_str("          </tbody>\n        </table>\n");
        }

        // Dependencies section
        html.push_str("        <h4>Dependencies</h4>\n");
        if model.depends_on.is_empty() {
            html.push_str("        <p class=\"empty\">No dependencies</p>\n");
        } else {
            html.push_str("        <ul class=\"dep-list\">\n");
            for dep in &model.depends_on {
                html.push_str(&format!("          <li>{}</li>\n", html_escape(dep)));
            }
            html.push_str("        </ul>\n");
        }

        // Tests section
        html.push_str("        <h4>Tests</h4>\n");
        if model.tests.is_empty() {
            html.push_str("        <p class=\"empty\">No tests declared</p>\n");
        } else {
            html.push_str("        <ul class=\"test-list\">\n");
            for test in &model.tests {
                let test_type_label = test_type_label(&test.test_type);
                let severity_class = match test.severity {
                    crate::tests::TestSeverity::Error => "test-error",
                    crate::tests::TestSeverity::Warning => "test-warning",
                };
                let col_suffix = test
                    .column
                    .as_ref()
                    .map(|c| format!(" on <code>{}</code>", html_escape(c)))
                    .unwrap_or_default();
                html.push_str(&format!(
                    "          <li><span class=\"test-badge {severity_class}\">{severity}</span>{test_type}{col_suffix}</li>\n",
                    severity = html_escape(&format!("{:?}", test.severity).to_lowercase()),
                    test_type = html_escape(&test_type_label),
                ));
            }
            html.push_str("        </ul>\n");
        }

        html.push_str("      </div>\n    </details>\n  </td>\n");

        // Remaining columns
        html.push_str(&format!(
            "  <td><code>{target}</code></td>\n",
            target = target_escaped
        ));
        html.push_str(&format!(
            "  <td><span class=\"strategy strategy-{strat}\">{strat}</span></td>\n",
            strat = strategy_escaped
        ));
        html.push_str(&format!("  <td class=\"count\">{dep_count}</td>\n"));
        html.push_str(&format!("  <td class=\"count\">{test_count}</td>\n"));
        html.push_str("</tr>\n");
    }

    html.push_str("</tbody>\n</table>\n</div>\n");

    // --- Search script ---
    html.push_str(
        r#"<footer>Generated by Rocky</footer>
<script>
document.getElementById('search').addEventListener('input', function(e) {
  var q = e.target.value.toLowerCase();
  var rows = document.querySelectorAll('#model-table tbody tr');
  for (var i = 0; i < rows.length; i++) {
    var name = rows[i].getAttribute('data-name') || '';
    if (!q || name.indexOf(q) !== -1) {
      rows[i].classList.remove('hidden');
    } else {
      rows[i].classList.add('hidden');
    }
  }
});
</script>
</body>
</html>"#,
    );

    html
}

/// Returns a human-readable label for a test type.
fn test_type_label(test_type: &crate::tests::TestType) -> String {
    match test_type {
        crate::tests::TestType::NotNull => "not_null".into(),
        crate::tests::TestType::Unique => "unique".into(),
        crate::tests::TestType::AcceptedValues { .. } => "accepted_values".into(),
        crate::tests::TestType::Relationships { .. } => "relationships".into(),
        crate::tests::TestType::Expression { .. } => "expression".into(),
        crate::tests::TestType::RowCountRange { .. } => "row_count_range".into(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{TestDecl, TestSeverity, TestType};

    fn sample_index() -> DocIndex {
        DocIndex {
            models: vec![
                DocModel {
                    name: "stg_orders".into(),
                    description: Some("Staged orders from Shopify".into()),
                    target: "acme.staging.stg_orders".into(),
                    strategy: "incremental".into(),
                    depends_on: vec!["src_orders".into()],
                    columns: vec![
                        DocColumn {
                            name: "order_id".into(),
                            data_type: "BIGINT".into(),
                            nullable: false,
                        },
                        DocColumn {
                            name: "customer_id".into(),
                            data_type: "BIGINT".into(),
                            nullable: true,
                        },
                        DocColumn {
                            name: "status".into(),
                            data_type: "STRING".into(),
                            nullable: false,
                        },
                    ],
                    tests: vec![
                        TestDecl {
                            test_type: TestType::NotNull,
                            column: Some("order_id".into()),
                            severity: TestSeverity::Error,
                        },
                        TestDecl {
                            test_type: TestType::Unique,
                            column: Some("order_id".into()),
                            severity: TestSeverity::Error,
                        },
                        TestDecl {
                            test_type: TestType::AcceptedValues {
                                values: vec![
                                    "pending".into(),
                                    "shipped".into(),
                                    "delivered".into(),
                                ],
                            },
                            column: Some("status".into()),
                            severity: TestSeverity::Warning,
                        },
                    ],
                },
                DocModel {
                    name: "fct_revenue".into(),
                    description: None,
                    target: "acme.marts.fct_revenue".into(),
                    strategy: "full_refresh".into(),
                    depends_on: vec!["stg_orders".into(), "stg_payments".into()],
                    columns: vec![],
                    tests: vec![TestDecl {
                        test_type: TestType::RowCountRange {
                            min: Some(1),
                            max: None,
                        },
                        column: None,
                        severity: TestSeverity::Error,
                    }],
                },
            ],
            pipeline_count: 2,
            adapter_count: 1,
        }
    }

    #[test]
    fn html_contains_doctype_and_title() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("<title>Rocky"));
        assert!(html.contains("Model Catalog"));
    }

    #[test]
    fn html_contains_model_names() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("stg_orders"));
        assert!(html.contains("fct_revenue"));
    }

    #[test]
    fn html_contains_target_refs() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("acme.staging.stg_orders"));
        assert!(html.contains("acme.marts.fct_revenue"));
    }

    #[test]
    fn html_contains_strategy_badges() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("strategy-incremental"));
        assert!(html.contains("strategy-full_refresh"));
    }

    #[test]
    fn html_contains_column_names() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("order_id"));
        assert!(html.contains("customer_id"));
        assert!(html.contains("BIGINT"));
        assert!(html.contains("STRING"));
    }

    #[test]
    fn html_contains_dependency_list() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("src_orders"));
        assert!(html.contains("stg_payments"));
    }

    #[test]
    fn html_contains_test_badges() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("not_null"));
        assert!(html.contains("unique"));
        assert!(html.contains("accepted_values"));
        assert!(html.contains("row_count_range"));
        assert!(html.contains("test-error"));
        assert!(html.contains("test-warning"));
    }

    #[test]
    fn html_contains_stat_counts() {
        let html = generate_index_html(&sample_index());
        // Model count badge
        assert!(html.contains(">2<"));
        // Pipeline count
        assert!(html.contains("Pipelines"));
        // Adapter count
        assert!(html.contains("Adapters"));
    }

    #[test]
    fn html_contains_search_script() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("getElementById('search')"));
        assert!(html.contains("data-name"));
    }

    #[test]
    fn html_contains_description() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("Staged orders from Shopify"));
    }

    #[test]
    fn html_escapes_special_characters() {
        let index = DocIndex {
            models: vec![DocModel {
                name: "model_with_<angle>".into(),
                description: Some("Uses & ampersands and \"quotes\"".into()),
                target: "cat.sch.tbl".into(),
                strategy: "full_refresh".into(),
                depends_on: vec![],
                columns: vec![],
                tests: vec![],
            }],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let html = generate_index_html(&index);
        assert!(html.contains("&lt;angle&gt;"));
        assert!(html.contains("&amp; ampersands"));
        assert!(html.contains("&quot;quotes&quot;"));
        assert!(!html.contains("<angle>"));
    }

    #[test]
    fn html_empty_index_renders() {
        let index = DocIndex {
            models: vec![],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let html = generate_index_html(&index);
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains(">0<")); // zero model count
    }

    #[test]
    fn html_no_columns_shows_empty_message() {
        let html = generate_index_html(&sample_index());
        assert!(html.contains("No column metadata available"));
    }

    #[test]
    fn html_no_deps_shows_empty_message() {
        let index = DocIndex {
            models: vec![DocModel {
                name: "standalone".into(),
                description: None,
                target: "cat.sch.tbl".into(),
                strategy: "full_refresh".into(),
                depends_on: vec![],
                columns: vec![],
                tests: vec![],
            }],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let html = generate_index_html(&index);
        assert!(html.contains("No dependencies"));
    }

    #[test]
    fn html_no_tests_shows_empty_message() {
        let index = DocIndex {
            models: vec![DocModel {
                name: "untested".into(),
                description: None,
                target: "cat.sch.tbl".into(),
                strategy: "full_refresh".into(),
                depends_on: vec![],
                columns: vec![],
                tests: vec![],
            }],
            pipeline_count: 0,
            adapter_count: 0,
        };
        let html = generate_index_html(&index);
        assert!(html.contains("No tests declared"));
    }

    #[test]
    fn strategy_label_covers_all_variants() {
        assert_eq!(strategy_label(&StrategyConfig::FullRefresh), "full_refresh");
        assert_eq!(
            strategy_label(&StrategyConfig::Incremental {
                timestamp_column: "ts".into()
            }),
            "incremental"
        );
        assert_eq!(
            strategy_label(&StrategyConfig::Merge {
                unique_key: vec!["id".into()],
                update_columns: None,
            }),
            "merge"
        );
        assert_eq!(
            strategy_label(&StrategyConfig::TimeInterval {
                time_column: "dt".into(),
                granularity: crate::models::TimeGrain::Day,
                lookback: 0,
                batch_size: std::num::NonZeroU32::new(1).unwrap(),
                first_partition: None,
            }),
            "time_interval"
        );
        assert_eq!(strategy_label(&StrategyConfig::Ephemeral), "ephemeral");
        assert_eq!(
            strategy_label(&StrategyConfig::DeleteInsert {
                partition_by: vec!["dt".into()],
            }),
            "delete_insert"
        );
        assert_eq!(
            strategy_label(&StrategyConfig::Microbatch {
                timestamp_column: "ts".into(),
                granularity: crate::models::TimeGrain::Hour,
            }),
            "microbatch"
        );
    }

    #[test]
    fn models_are_sorted_alphabetically() {
        use crate::config::RockyConfig;
        use indexmap::IndexMap;

        let config = RockyConfig {
            state: Default::default(),
            adapters: IndexMap::new(),
            pipelines: IndexMap::new(),
            hooks: Default::default(),
            cost: Default::default(),
        };

        let models = vec![
            Model {
                config: crate::models::ModelConfig {
                    name: "z_model".into(),
                    depends_on: vec![],
                    strategy: StrategyConfig::FullRefresh,
                    target: crate::models::TargetConfig {
                        catalog: "c".into(),
                        schema: "s".into(),
                        table: "t".into(),
                    },
                    sources: vec![],
                    intent: None,
                    freshness: None,
                    tests: vec![],
                    format: None,
                    format_options: None,
                },
                sql: String::new(),
                file_path: "z.sql".into(),
                contract_path: None,
            },
            Model {
                config: crate::models::ModelConfig {
                    name: "a_model".into(),
                    depends_on: vec![],
                    strategy: StrategyConfig::FullRefresh,
                    target: crate::models::TargetConfig {
                        catalog: "c".into(),
                        schema: "s".into(),
                        table: "t".into(),
                    },
                    sources: vec![],
                    intent: None,
                    freshness: None,
                    tests: vec![],
                    format: None,
                    format_options: None,
                },
                sql: String::new(),
                file_path: "a.sql".into(),
                contract_path: None,
            },
        ];

        let index = build_doc_index(&models, &config, None);
        assert_eq!(index.models[0].name, "a_model");
        assert_eq!(index.models[1].name, "z_model");
    }

    #[test]
    fn build_doc_index_attaches_columns_from_map() {
        use crate::config::RockyConfig;
        use crate::ir::ColumnInfo;
        use indexmap::IndexMap;
        use std::collections::HashMap;

        let config = RockyConfig {
            state: Default::default(),
            adapters: IndexMap::new(),
            pipelines: IndexMap::new(),
            hooks: Default::default(),
            cost: Default::default(),
        };

        let models = vec![Model {
            config: crate::models::ModelConfig {
                name: "my_model".into(),
                depends_on: vec![],
                strategy: StrategyConfig::FullRefresh,
                target: crate::models::TargetConfig {
                    catalog: "c".into(),
                    schema: "s".into(),
                    table: "t".into(),
                },
                sources: vec![],
                intent: Some("Test model".into()),
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
            },
            sql: String::new(),
            file_path: "my_model.sql".into(),
            contract_path: None,
        }];

        let mut col_map = HashMap::new();
        col_map.insert(
            "my_model".into(),
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: "STRING".into(),
                    nullable: true,
                },
            ],
        );

        let index = build_doc_index(&models, &config, Some(&col_map));
        assert_eq!(index.models.len(), 1);
        assert_eq!(index.models[0].columns.len(), 2);
        assert_eq!(index.models[0].columns[0].name, "id");
        assert_eq!(index.models[0].columns[1].data_type, "STRING");
        assert!(!index.models[0].columns[0].nullable);
        assert!(index.models[0].columns[1].nullable);
        assert_eq!(index.models[0].description.as_deref(), Some("Test model"));
    }

    #[test]
    fn build_doc_index_counts_pipelines_and_adapters() {
        use crate::config::RockyConfig;
        use indexmap::IndexMap;

        let config = RockyConfig {
            state: Default::default(),
            adapters: IndexMap::new(),
            pipelines: IndexMap::new(),
            hooks: Default::default(),
            cost: Default::default(),
        };

        let index = build_doc_index(&[], &config, None);
        assert_eq!(index.pipeline_count, 0);
        assert_eq!(index.adapter_count, 0);
        assert!(index.models.is_empty());
    }
}
