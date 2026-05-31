//! Consumer-side checking of imported producer-project snapshots.
//!
//! When a project declares one or more `[imports.<name>]` blocks (see
//! [`rocky_core::config::ImportEntry`]), `rocky compile` runs the checks in
//! this module after assembling its own diagnostics. The checks are:
//!
//! - **E033 — pin mismatch.** If the import sets a concrete recipe-hash
//!   `pin` and the vendored snapshot's hash differs, the consumer is
//!   compiling against a snapshot it has not pinned to.
//! - **E030 — dropped column referenced.** If the import declares a
//!   `baseline`, we diff it against the current `snapshot` with the existing
//!   breaking-change classifier ([`rocky_core::breaking_change::diff_project_ir`])
//!   and collect the columns the producer dropped. Any dropped column that a
//!   consumer model still reads fails compilation.
//!
//! ## Linking consumer models to producer outputs
//!
//! A consumer model declares the producer table it reads via a `[[sources]]`
//! sidecar entry, whose full name (`catalog.schema.table`) matches the
//! producer's `TargetRef::full_name()` — the key the classifier reports a
//! dropped column under. For the single-source case we attribute every
//! column the model's SQL references (via
//! [`rocky_sql::lineage::extract_lineage`]) to that one producer target, so
//! E030 fires only for columns the consumer actually uses.
//!
//! ## Filtered vs unfiltered
//!
//! Per-column filtering needs an enumerable column list. When a consumer
//! model uses `SELECT *` (`has_star`) or joins multiple sources (so an
//! unqualified column can't be attributed to one producer), we fall back to
//! the conservative *unfiltered* behaviour: every dropped producer column is
//! flagged against that consumer model. That over-reports rather than
//! letting a breaking change slip through silently.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rocky_compiler::diagnostic::{self, Diagnostic};
use rocky_core::breaking_change::{BreakingChange, diff_project_ir};
use rocky_core::config::RockyConfig;
use rocky_core::imports::{PinStatus, load_snapshot, verify_pin};
use rocky_core::models::Model;

/// How a consumer model references a producer target.
enum Reference {
    /// Specific columns the model reads (lowercased) — per-column filtering.
    Columns(BTreeSet<String>),
    /// `SELECT *` or a multi-source join: any dropped column is flagged.
    Unfiltered,
}

/// Compute import-contract diagnostics for the consumer project.
///
/// Returns E033 (pin mismatch) and E030 (dropped column still referenced)
/// diagnostics. An empty `[imports]` map yields an empty vec (no work). A
/// missing or unreadable snapshot file yields a warning diagnostic and skips
/// that import rather than aborting the whole compile.
pub fn imports_diagnostics(
    config: &RockyConfig,
    config_dir: &Path,
    consumer_models: &[Model],
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for (import_name, entry) in &config.imports {
        let dir = config_dir.join(&entry.path);

        let current = match load_snapshot(&dir, &entry.snapshot) {
            Ok(ir) => ir,
            Err(e) => {
                diagnostics.push(Diagnostic::warning(
                    diagnostic::W012,
                    import_name,
                    format!("import '{import_name}': could not load snapshot: {e}"),
                ));
                continue;
            }
        };

        // E033 — recipe-hash pin verification.
        if let PinStatus::Mismatch { expected, actual } = verify_pin(&current, entry.pin.as_deref())
        {
            diagnostics.push(Diagnostic::error(
                diagnostic::E033,
                import_name,
                format!(
                    "import '{import_name}': snapshot recipe hash {actual} does not match \
                     configured pin {expected}"
                ),
            ));
        }

        // E030 — dropped column still referenced. Needs a baseline to diff
        // against; without one there is no "before" to detect a drop from.
        let Some(baseline_file) = entry.baseline.as_deref() else {
            continue;
        };
        let baseline = match load_snapshot(&dir, baseline_file) {
            Ok(ir) => ir,
            Err(e) => {
                diagnostics.push(Diagnostic::warning(
                    diagnostic::W012,
                    import_name,
                    format!("import '{import_name}': could not load baseline snapshot: {e}"),
                ));
                continue;
            }
        };

        // Columns the producer dropped between baseline and current, keyed
        // by producer target full name.
        let mut dropped: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for finding in diff_project_ir(&baseline, &current) {
            if let BreakingChange::ColumnDropped { model, column, .. } = finding.change {
                dropped.entry(model).or_default().push(column);
            }
        }
        if dropped.is_empty() {
            continue;
        }

        // For each consumer model, build its reference set against the
        // single producer target it declares via `[[sources]]`.
        for model in consumer_models {
            // Single-source linkage only. A model with no sources can't
            // reference an imported producer; multi-source models are
            // handled by the unfiltered fallback below.
            let Some(source) = single_source(model) else {
                // Multi-source: attribute every referenced source as
                // unfiltered so we never silently drop a breaking change.
                for source in &model.config.sources {
                    flag_dropped(
                        &mut diagnostics,
                        import_name,
                        &model.config.name,
                        &source_full_name(source),
                        &dropped,
                        &Reference::Unfiltered,
                    );
                }
                continue;
            };

            let producer_target = source_full_name(&source);
            if !dropped.contains_key(&producer_target) {
                continue;
            }

            let reference = match rocky_sql::lineage::extract_lineage(&model.sql) {
                Ok(lineage) if !lineage.has_star => Reference::Columns(
                    lineage
                        .columns
                        .iter()
                        .map(|c| c.source_column.to_lowercase())
                        .collect(),
                ),
                // `SELECT *` or unparsable SQL: be conservative.
                _ => Reference::Unfiltered,
            };

            flag_dropped(
                &mut diagnostics,
                import_name,
                &model.config.name,
                &producer_target,
                &dropped,
                &reference,
            );
        }
    }

    diagnostics
}

/// Returns the model's sole `[[sources]]` full name when it declares exactly
/// one source, else `None`.
fn single_source(model: &Model) -> Option<rocky_core::models::SourceConfig> {
    match model.config.sources.as_slice() {
        [single] => Some(single.clone()),
        _ => None,
    }
}

/// Push an E030 for every dropped column on `producer_target` that
/// `reference` indicates the consumer model uses.
fn flag_dropped(
    diagnostics: &mut Vec<Diagnostic>,
    import_name: &str,
    consumer_model: &str,
    producer_target: &str,
    dropped: &BTreeMap<String, Vec<String>>,
    reference: &Reference,
) {
    let Some(columns) = dropped.get(producer_target) else {
        return;
    };
    for column in columns {
        let used = match reference {
            Reference::Unfiltered => true,
            Reference::Columns(set) => set.contains(&column.to_lowercase()),
        };
        if !used {
            continue;
        }
        diagnostics.push(Diagnostic::error(
            diagnostic::E030,
            consumer_model,
            format!(
                "model '{consumer_model}' references column '{column}' which the imported \
                 producer '{import_name}' ({producer_target}) no longer outputs"
            ),
        ));
    }
}

/// Full three-part name (`catalog.schema.table`) of a consumer
/// `[[sources]]` entry — the key the producer reports a dropped column
/// under (it equals the producer's `TargetRef::full_name()`).
fn source_full_name(source: &rocky_core::models::SourceConfig) -> String {
    format!("{}.{}.{}", source.catalog, source.schema, source.table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::config::ImportEntry;
    use rocky_core::models::{ModelConfig, SourceConfig, StrategyConfig, TargetConfig};
    use rocky_ir::{
        GovernanceConfig, MaterializationStrategy, ModelIr, ProjectIr, RockyType, TargetRef,
        TypedColumn,
    };

    /// Build a producer snapshot whose one model outputs the given columns.
    fn producer_snapshot(columns: &[&str]) -> ProjectIr {
        let mut model = ModelIr::transformation(
            TargetRef {
                catalog: "shop".to_string(),
                schema: "core".to_string(),
                table: "orders".to_string(),
            },
            MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT * FROM raw.orders".to_string(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model.typed_columns = columns
            .iter()
            .map(|c| TypedColumn {
                name: (*c).to_string(),
                data_type: RockyType::Int64,
                nullable: false,
            })
            .collect();
        ProjectIr {
            models: vec![model],
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    /// Build a consumer model that reads `shop.core.orders` via the given SQL.
    fn consumer_model(sql: &str) -> Model {
        Model {
            config: ModelConfig {
                name: "shipments".to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "ship".to_string(),
                    schema: "core".to_string(),
                    table: "shipments".to_string(),
                },
                sources: vec![SourceConfig {
                    catalog: "shop".to_string(),
                    schema: "core".to_string(),
                    table: "orders".to_string(),
                }],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
                budget: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: "models/shipments.sql".to_string(),
            contract_path: None,
        }
    }

    /// Write baseline + current snapshots and a config pointing at them.
    fn fixture(
        dir: &std::path::Path,
        baseline: &ProjectIr,
        current: &ProjectIr,
        pin: Option<&str>,
    ) -> RockyConfig {
        std::fs::write(
            dir.join("baseline.json"),
            serde_json::to_string_pretty(baseline).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.join("current.json"),
            serde_json::to_string_pretty(current).unwrap(),
        )
        .unwrap();
        let mut config: RockyConfig = toml::from_str("").unwrap();
        config.imports.insert(
            "orders".to_string(),
            ImportEntry {
                path: ".".to_string(),
                snapshot: "current.json".to_string(),
                baseline: Some("baseline.json".to_string()),
                pin: pin.map(str::to_string),
            },
        );
        config
    }

    #[test]
    fn e030_fires_when_consumer_references_dropped_column() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot(&["id", "customer_id", "shipped_at"]);
        let current = producer_snapshot(&["id", "customer_id"]); // dropped shipped_at
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id, customer_id, shipped_at FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        let e030: Vec<_> = diags.iter().filter(|d| &*d.code == "E030").collect();
        assert_eq!(e030.len(), 1, "expected one E030, got: {diags:?}");
        assert!(e030[0].message.contains("shipped_at"));
        assert_eq!(e030[0].model, "shipments");
    }

    #[test]
    fn e030_silent_when_consumer_does_not_reference_dropped_column() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot(&["id", "customer_id", "shipped_at"]);
        let current = producer_snapshot(&["id", "customer_id"]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        // Consumer reads only id + customer_id — the dropped shipped_at is irrelevant.
        let model = consumer_model("SELECT id, customer_id FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(
            !diags.iter().any(|d| &*d.code == "E030"),
            "expected no E030, got: {diags:?}"
        );
    }

    #[test]
    fn select_star_falls_back_to_unfiltered() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot(&["id", "shipped_at"]);
        let current = producer_snapshot(&["id"]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        // SELECT * can't enumerate columns — any dropped column is flagged.
        let model = consumer_model("SELECT * FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(
            diags.iter().any(|d| &*d.code == "E030"),
            "SELECT * should conservatively flag the drop, got: {diags:?}"
        );
    }

    #[test]
    fn no_drop_means_no_e030() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = producer_snapshot(&["id", "customer_id", "shipped_at"]);
        let config = fixture(dir.path(), &snapshot, &snapshot, Some("*"));
        let model = consumer_model("SELECT id, shipped_at FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(diags.iter().all(|d| &*d.code != "E030"));
    }

    #[test]
    fn e033_fires_on_pin_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = producer_snapshot(&["id"]);
        let config = fixture(dir.path(), &snapshot, &snapshot, Some("deadbeef"));
        let model = consumer_model("SELECT id FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(
            diags.iter().any(|d| &*d.code == "E033"),
            "expected E033 on pin mismatch, got: {diags:?}"
        );
    }

    #[test]
    fn missing_snapshot_yields_warning_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let mut config: RockyConfig = toml::from_str("").unwrap();
        config.imports.insert(
            "orders".to_string(),
            ImportEntry {
                path: ".".to_string(),
                snapshot: "nope.json".to_string(),
                baseline: None,
                pin: None,
            },
        );
        let model = consumer_model("SELECT id FROM shop.core.orders");
        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(diags.iter().any(|d| &*d.code == "W012"));
        assert!(
            diags
                .iter()
                .all(|d| d.severity != diagnostic::Severity::Error)
        );
    }
}
