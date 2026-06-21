//! Consumer-side checking of imported producer-project snapshots.
//!
//! When a project declares one or more `[imports.<name>]` blocks (see
//! [`rocky_core::config::ImportEntry`]), `rocky compile` runs the checks in
//! this module after assembling its own diagnostics. The checks are:
//!
//! - **E033 — pin mismatch.** If the import sets a concrete recipe-hash
//!   `pin` and the vendored snapshot's hash differs, the consumer is
//!   compiling against a snapshot it has not pinned to.
//! - **The cross-team-contract column codes.** If the import declares a
//!   `baseline`, we diff it against the current `snapshot` with the existing
//!   breaking-change classifier ([`rocky_core::breaking_change::diff_project_ir`])
//!   and re-emit the producer's column-level changes as consumer-side
//!   diagnostics, filtered by what the consumer actually reads:
//!
//!   | Code | Producer change | Severity | Fires when the consumer… |
//!   |------|-----------------|----------|--------------------------|
//!   | E030 | column dropped | error | references the column |
//!   | E031 | column type narrowed | error | references the column |
//!   | E032 | nullable → NOT NULL | error | references the column |
//!   | W030 | column added | info | reads the producer via `SELECT *` |
//!   | W031 | column type widened | warning | references the column |
//!
//! ## Linking consumer models to producer outputs
//!
//! A consumer model declares the producer table it reads via a `[[sources]]`
//! sidecar entry, whose full name (`catalog.schema.table`) matches the
//! producer's `TargetRef::full_name()` — the key the classifier reports a
//! change under. For the single-source case we attribute every column the
//! model's SQL references (via [`rocky_sql::lineage::extract_lineage`]) to
//! that one producer target, so the E03x codes fire only for columns the
//! consumer actually uses.
//!
//! ## Filtered vs unfiltered
//!
//! Per-column filtering needs an enumerable column list. When a consumer
//! model uses `SELECT *` (`has_star`) or joins multiple sources (so an
//! unqualified column can't be attributed to one producer), we fall back to
//! the conservative *unfiltered* behaviour: every relevant producer change is
//! flagged against that consumer model. That over-reports rather than letting
//! a breaking change slip through silently. W030 (added column) is the
//! exception — a brand-new column is referenced by no consumer, so it fires
//! *only* in the unfiltered (`SELECT *`) case, where it shifts positional
//! projection.

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
    /// `SELECT *` or a multi-source join: any change is flagged.
    Unfiltered,
}

/// A column-level change the producer made between baseline and current that
/// has a consumer-side code. Carries only what the diagnostic message needs.
enum ColumnChange {
    /// Producer dropped the column — E030.
    Dropped,
    /// Producer narrowed the column's type — E031.
    Narrowed { old_type: String, new_type: String },
    /// Producer tightened the column from nullable to NOT NULL — E032.
    Tightened,
    /// Producer widened the column's type — W031.
    Widened { old_type: String, new_type: String },
    /// Producer added a new column — W030 (only relevant to `SELECT *`
    /// consumers; never matched by the column-reference filter).
    Added,
}

impl ColumnChange {
    /// Build the consumer-side diagnostic this change re-emits as, naming the
    /// column and the producer it came from. Severity tracks the code class:
    /// E0xx are errors (the change can break the consumer's reads), W0xx are
    /// non-fatal (widening keeps existing reads working; an added column only
    /// shifts `SELECT *` projection).
    fn diagnostic(
        &self,
        import_name: &str,
        consumer_model: &str,
        producer_target: &str,
        column: &str,
    ) -> Diagnostic {
        match self {
            ColumnChange::Dropped => Diagnostic::error(
                diagnostic::E030,
                consumer_model,
                format!(
                    "model '{consumer_model}' references column '{column}' which the imported \
                     producer '{import_name}' ({producer_target}) no longer outputs"
                ),
            ),
            ColumnChange::Narrowed { old_type, new_type } => Diagnostic::error(
                diagnostic::E031,
                consumer_model,
                format!(
                    "model '{consumer_model}' references column '{column}' whose type the imported \
                     producer '{import_name}' ({producer_target}) narrowed from {old_type} to \
                     {new_type}; existing values may no longer fit"
                ),
            ),
            ColumnChange::Tightened => Diagnostic::error(
                diagnostic::E032,
                consumer_model,
                format!(
                    "model '{consumer_model}' references column '{column}' which the imported \
                     producer '{import_name}' ({producer_target}) tightened from nullable to NOT \
                     NULL"
                ),
            ),
            ColumnChange::Widened { old_type, new_type } => Diagnostic::warning(
                diagnostic::W031,
                consumer_model,
                format!(
                    "model '{consumer_model}' references column '{column}' whose type the imported \
                     producer '{import_name}' ({producer_target}) widened from {old_type} to \
                     {new_type}; the consumer's declared output type may now be too small"
                ),
            ),
            ColumnChange::Added => Diagnostic::info(
                diagnostic::W030,
                consumer_model,
                format!(
                    "model '{consumer_model}' reads imported producer '{import_name}' \
                     ({producer_target}) via SELECT *, which added column '{column}'; positional \
                     projection may shift"
                ),
            ),
        }
    }

    /// True for codes that gate on the consumer *referencing the column by
    /// name*. `Added` is the lone exception: no consumer names a brand-new
    /// column, so it gates on `SELECT *` instead (see [`flag_changes`]).
    fn filters_by_reference(&self) -> bool {
        !matches!(self, ColumnChange::Added)
    }
}

/// Compute import-contract diagnostics for the consumer project.
///
/// Returns E033 (pin mismatch) plus the cross-team-contract column codes for
/// producer changes the consumer reads: E030 (dropped), E031 (type narrowed),
/// E032 (nullable -> NOT NULL), W031 (type widened), and W030 (column added,
/// for `SELECT *` consumers only). An empty `[imports]` map yields an empty
/// vec (no work). A missing or unreadable snapshot file yields a warning
/// diagnostic and skips that import rather than aborting the whole compile.
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

        // Producer column-level changes that carry a consumer-side code,
        // keyed by producer target full name. Only column changes map to an
        // import diagnostic; model add/remove, column reorder, and
        // materialization changes have no consumer contract code.
        let mut changes: BTreeMap<String, Vec<(String, ColumnChange)>> = BTreeMap::new();
        for finding in diff_project_ir(&baseline, &current) {
            let (model, column, change) = match finding.change {
                BreakingChange::ColumnDropped { model, column, .. } => {
                    (model, column, ColumnChange::Dropped)
                }
                BreakingChange::ColumnTypeChanged {
                    model,
                    column,
                    old_type,
                    new_type,
                    narrowing,
                } => {
                    let change = if narrowing {
                        ColumnChange::Narrowed { old_type, new_type }
                    } else {
                        ColumnChange::Widened { old_type, new_type }
                    };
                    (model, column, change)
                }
                // Only a nullable -> NOT NULL tightening can break a consumer
                // (it may rely on the column holding NULLs); the reverse
                // loosens the contract and is safe.
                BreakingChange::ColumnNullabilityChanged {
                    model,
                    column,
                    old_nullable: true,
                    new_nullable: false,
                } => (model, column, ColumnChange::Tightened),
                BreakingChange::ColumnAdded { model, column, .. } => {
                    (model, column, ColumnChange::Added)
                }
                _ => continue,
            };
            changes.entry(model).or_default().push((column, change));
        }
        if changes.is_empty() {
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
                    flag_changes(
                        &mut diagnostics,
                        import_name,
                        &model.config.name,
                        &source_full_name(source),
                        &changes,
                        &Reference::Unfiltered,
                    );
                }
                continue;
            };

            let producer_target = source_full_name(&source);
            if !changes.contains_key(&producer_target) {
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

            flag_changes(
                &mut diagnostics,
                import_name,
                &model.config.name,
                &producer_target,
                &changes,
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

/// Emit a consumer-side diagnostic for every producer column change on
/// `producer_target` relevant to this consumer model. Reference-filtered
/// changes (drop / narrow / tighten / widen) fire only for columns the
/// consumer reads; an added column (W030) fires only for a `SELECT *`
/// consumer, where it shifts positional projection.
fn flag_changes(
    diagnostics: &mut Vec<Diagnostic>,
    import_name: &str,
    consumer_model: &str,
    producer_target: &str,
    changes: &BTreeMap<String, Vec<(String, ColumnChange)>>,
    reference: &Reference,
) {
    let Some(columns) = changes.get(producer_target) else {
        return;
    };
    for (column, change) in columns {
        let relevant = if change.filters_by_reference() {
            match reference {
                Reference::Unfiltered => true,
                Reference::Columns(set) => set.contains(&column.to_lowercase()),
            }
        } else {
            matches!(reference, Reference::Unfiltered)
        };
        if !relevant {
            continue;
        }
        diagnostics.push(change.diagnostic(import_name, consumer_model, producer_target, column));
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
                tags: Default::default(),
                retention: None,
                budget: None,
                skip: None,
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

    /// Producer snapshot whose one model (`shop.core.orders`) outputs the
    /// given `(name, type, nullable)` columns — for the type/nullability
    /// cases the all-`Int64`/non-null `producer_snapshot` can't express.
    fn producer_snapshot_typed(cols: &[(&str, RockyType, bool)]) -> ProjectIr {
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
        model.typed_columns = cols
            .iter()
            .map(|(name, ty, nullable)| TypedColumn {
                name: (*name).to_string(),
                data_type: ty.clone(),
                nullable: *nullable,
            })
            .collect();
        ProjectIr {
            models: vec![model],
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    #[test]
    fn e031_fires_when_consumer_references_narrowed_column() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int64, false),
        ]);
        // Int64 -> Int32 is a narrowing per is_type_narrowing.
        let current = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int32, false),
        ]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id, amount FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        let e031: Vec<_> = diags.iter().filter(|d| &*d.code == "E031").collect();
        assert_eq!(e031.len(), 1, "expected one E031, got: {diags:?}");
        assert!(e031[0].message.contains("amount"));
        assert_eq!(e031[0].severity, diagnostic::Severity::Error);
    }

    #[test]
    fn e031_silent_when_narrowed_column_unreferenced() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int64, false),
        ]);
        let current = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int32, false),
        ]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(
            !diags.iter().any(|d| &*d.code == "E031"),
            "narrowed column not read — expected no E031, got: {diags:?}"
        );
    }

    #[test]
    fn e032_fires_when_nullable_tightened_to_not_null() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("email", RockyType::String, true),
        ]);
        let current = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("email", RockyType::String, false),
        ]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id, email FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        let e032: Vec<_> = diags.iter().filter(|d| &*d.code == "E032").collect();
        assert_eq!(e032.len(), 1, "expected one E032, got: {diags:?}");
        assert!(e032[0].message.contains("email"));
    }

    #[test]
    fn e032_silent_when_not_null_loosened_to_nullable() {
        let dir = tempfile::tempdir().unwrap();
        // NOT NULL -> nullable loosens the contract; safe, no E032.
        let baseline = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("email", RockyType::String, false),
        ]);
        let current = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("email", RockyType::String, true),
        ]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id, email FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        assert!(
            !diags.iter().any(|d| &*d.code == "E032"),
            "loosening nullability is safe — expected no E032, got: {diags:?}"
        );
    }

    #[test]
    fn w031_warns_on_widened_referenced_column() {
        let dir = tempfile::tempdir().unwrap();
        // Int32 -> Int64 widens: existing reads still work, but warn.
        let baseline = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int32, false),
        ]);
        let current = producer_snapshot_typed(&[
            ("id", RockyType::Int64, false),
            ("amount", RockyType::Int64, false),
        ]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));
        let model = consumer_model("SELECT id, amount FROM shop.core.orders");

        let diags = imports_diagnostics(&config, dir.path(), &[model]);
        let w031: Vec<_> = diags.iter().filter(|d| &*d.code == "W031").collect();
        assert_eq!(w031.len(), 1, "expected one W031, got: {diags:?}");
        assert_eq!(w031[0].severity, diagnostic::Severity::Warning);
    }

    #[test]
    fn w030_added_column_flagged_only_under_select_star() {
        let dir = tempfile::tempdir().unwrap();
        let baseline = producer_snapshot(&["id", "customer_id"]);
        let current = producer_snapshot(&["id", "customer_id", "promo_code"]);
        let config = fixture(dir.path(), &baseline, &current, Some("*"));

        // SELECT * consumer: positional projection shifts -> W030 (info).
        let star = consumer_model("SELECT * FROM shop.core.orders");
        let star_diags = imports_diagnostics(&config, dir.path(), &[star]);
        let w030: Vec<_> = star_diags.iter().filter(|d| &*d.code == "W030").collect();
        assert_eq!(
            w030.len(),
            1,
            "SELECT * should get W030, got: {star_diags:?}"
        );
        assert_eq!(w030[0].severity, diagnostic::Severity::Info);

        // Explicit-column consumer never names the new column -> no W030.
        let explicit = consumer_model("SELECT id, customer_id FROM shop.core.orders");
        let explicit_diags = imports_diagnostics(&config, dir.path(), &[explicit]);
        assert!(
            !explicit_diags.iter().any(|d| &*d.code == "W030"),
            "explicit columns should not get W030, got: {explicit_diags:?}"
        );
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
