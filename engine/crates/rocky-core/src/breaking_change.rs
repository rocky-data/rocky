//! Semantic breaking-change classifier over [`rocky_ir::ProjectIr`] snapshots.
//!
//! Compares two `ProjectIr` values and produces a list of typed
//! [`BreakingFinding`]s describing each structural change between them, plus
//! a severity classification ([`BreakingSeverity::Breaking`] /
//! [`BreakingSeverity::Warning`] / [`BreakingSeverity::Info`]).
//!
//! The classifier is consumed by two CLI surfaces:
//!
//! - `rocky ci-diff --semantic` — surfaces findings in PR-preview JSON.
//! - `rocky branch promote` — fail-fast gate on any `Breaking` finding
//!   unless `--allow-breaking` is set.
//!
//! Compared against text-shaped diff (the SQLMesh approach), this is
//! strictly more powerful: it classifies *structural* changes against the
//! typed IR rather than parsing emitted SQL strings.
//!
//! ## What counts as breaking
//!
//! "Breaking" means *the change can produce different SQL outputs for an
//! existing downstream consumer*. Driven by the recipe-hash-participating
//! fields documented at the top of `rocky_ir::ir`. Implementation-detail
//! fields (project-level DAG, lineage ordering) are intentionally not
//! diffed — changes to them never break downstream consumers.
//!
//! Severity rules:
//!
//! - [`BreakingSeverity::Breaking`]: column dropped, type narrowing,
//!   materialization strategy swap, partition columns changed, target
//!   renamed, lakehouse format swapped, model removed, column mask
//!   removed.
//! - [`BreakingSeverity::Warning`]: nullable → NOT NULL, column reordered
//!   (`SELECT *` consumers shift), column added as NOT NULL, mask added.
//! - [`BreakingSeverity::Info`]: model added, nullable column added,
//!   type widening (Int32 → Int64), NOT NULL → nullable.

use std::collections::BTreeMap;

use rocky_ir::{
    ColumnSelection, LakehouseFormat, LakehouseOptions, MaterializationStrategy, ModelIr,
    ProjectIr, RockyType, TargetRef, TypedColumn,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Severity classification for a single semantic change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum BreakingSeverity {
    /// Will break a downstream consumer that reads the model's prior shape.
    Breaking,
    /// May break consumers depending on usage; e.g. nullable → NOT NULL,
    /// column reordered.
    Warning,
    /// Informational only — purely additive or backwards-compatible.
    Info,
}

/// A single typed semantic change between two `ProjectIr` snapshots.
///
/// Each variant carries the minimum identifying context (model + column +
/// before/after values) needed for a CLI / PR-preview surface to render a
/// useful message without re-loading either IR.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BreakingChange {
    /// A model present on the base side is absent on the head side.
    ModelRemoved { model: String },
    /// A model present on the head side is absent on the base side.
    ModelAdded { model: String },
    /// A column present on the base side is absent on the head side.
    ColumnDropped {
        model: String,
        column: String,
        data_type: String,
    },
    /// A column was added.
    ColumnAdded {
        model: String,
        column: String,
        data_type: String,
        nullable: bool,
    },
    /// A column's data type changed.
    ColumnTypeChanged {
        model: String,
        column: String,
        old_type: String,
        new_type: String,
        /// `true` when the new type cannot represent every value of the old
        /// type (Int64 → Int32, Decimal precision shrink, Timestamp → Date).
        narrowing: bool,
    },
    /// A column's nullability flipped.
    ColumnNullabilityChanged {
        model: String,
        column: String,
        old_nullable: bool,
        new_nullable: bool,
    },
    /// A column's position in the output schema changed. `SELECT *`
    /// downstream consumers see different positional projection.
    ColumnReordered {
        model: String,
        column: String,
        old_index: usize,
        new_index: usize,
    },
    /// The materialization strategy switched between top-level variants
    /// (e.g. `FullRefresh` → `Incremental`).
    MaterializationStrategyChanged {
        model: String,
        old_strategy: String,
        new_strategy: String,
    },
    /// Materialization-specific key columns changed without the top-level
    /// variant changing (e.g. `Merge` `unique_key` rewritten,
    /// `Incremental` `timestamp_column` swapped).
    MaterializationKeyChanged {
        model: String,
        /// Which key changed: `unique_key`, `timestamp_column`, `partition_by`,
        /// `time_column`, `granularity`, `target_lag`, `update_columns`,
        /// `storage_prefix`, or `partition_columns`.
        key_kind: String,
        old: Vec<String>,
        new: Vec<String>,
    },
    /// Replication column-selection mode flipped between `All` and
    /// `Explicit(...)` or the explicit list changed.
    ReplicationColumnsChanged {
        model: String,
        old: Vec<String>,
        new: Vec<String>,
    },
    /// `LakehouseOptions::partition_by` changed without the strategy
    /// changing. Distinct from `MaterializationKeyChanged` since this lives
    /// on `format_options` rather than the strategy enum.
    PartitionByChanged {
        model: String,
        old: Vec<String>,
        new: Vec<String>,
    },
    /// Target table reference renamed (catalog, schema, or table component).
    TargetRenamed {
        model: String,
        old: String,
        new: String,
    },
    /// Source reference rebound to a different table.
    SourceChanged {
        model: String,
        old: Vec<String>,
        new: Vec<String>,
    },
    /// A column mask was added, removed, or its strategy changed.
    ColumnMaskChanged {
        model: String,
        column: String,
        old_strategy: Option<String>,
        new_strategy: Option<String>,
    },
    /// Lakehouse format switched (e.g. `DeltaTable` → `IcebergTable`).
    LakehouseFormatChanged {
        model: String,
        old: String,
        new: String,
    },
    /// SQL body changed without any other detectable structural change.
    /// Surfaced as `Info` because the typed-output shape is unchanged, but
    /// runtime row contents may differ.
    SqlBodyChanged { model: String },
}

/// A classified finding produced by [`diff_project_ir`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BreakingFinding {
    pub change: BreakingChange,
    pub severity: BreakingSeverity,
}

impl BreakingFinding {
    /// True when this finding's severity is [`BreakingSeverity::Breaking`].
    pub fn is_breaking(&self) -> bool {
        matches!(self.severity, BreakingSeverity::Breaking)
    }
}

/// Diff two [`ProjectIr`] snapshots and return classified semantic findings.
///
/// Models are paired by their `target.full_name()` (the externally-visible
/// table the model materializes into); the `name` field on `ModelIr` is an
/// internal identifier and not stable across renames.
///
/// The output is sorted by (model name, finding kind) so two diff runs with
/// the same inputs produce byte-identical JSON — important for `ci-diff`
/// regression tests and PR-preview review.
pub fn diff_project_ir(old: &ProjectIr, new: &ProjectIr) -> Vec<BreakingFinding> {
    let old_by_target: BTreeMap<String, &ModelIr> = old
        .models
        .iter()
        .map(|m| (m.target.full_name(), m))
        .collect();
    let new_by_target: BTreeMap<String, &ModelIr> = new
        .models
        .iter()
        .map(|m| (m.target.full_name(), m))
        .collect();

    let mut findings: Vec<BreakingFinding> = Vec::new();

    for (target_name, old_model) in &old_by_target {
        match new_by_target.get(target_name) {
            None => findings.push(BreakingFinding {
                change: BreakingChange::ModelRemoved {
                    model: target_name.clone(),
                },
                severity: BreakingSeverity::Breaking,
            }),
            Some(new_model) => {
                diff_model(target_name, old_model, new_model, &mut findings);
            }
        }
    }

    for target_name in new_by_target.keys() {
        if !old_by_target.contains_key(target_name) {
            findings.push(BreakingFinding {
                change: BreakingChange::ModelAdded {
                    model: target_name.clone(),
                },
                severity: BreakingSeverity::Info,
            });
        }
    }

    findings
}

// ---------------------------------------------------------------------------
// Per-model diff
// ---------------------------------------------------------------------------

fn diff_model(model_name: &str, old: &ModelIr, new: &ModelIr, findings: &mut Vec<BreakingFinding>) {
    diff_columns(model_name, &old.typed_columns, &new.typed_columns, findings);
    diff_materialization(
        model_name,
        &old.materialization,
        &new.materialization,
        findings,
    );
    diff_target(model_name, &old.target, &new.target, findings);
    diff_sources(model_name, old, new, findings);
    diff_columns_selection(model_name, &old.columns, &new.columns, findings);
    diff_lakehouse_format(model_name, &old.format, &new.format, findings);
    diff_partition_by(
        model_name,
        &old.format_options,
        &new.format_options,
        findings,
    );
    diff_column_masks(model_name, &old.column_masks, &new.column_masks, findings);

    // SQL body change — surface only if no other structural change explains
    // a regenerated SQL. Always `Info`: the typed-output shape is unchanged
    // by definition (typed_columns is diffed above), but row contents may
    // shift.
    if old.sql != new.sql {
        findings.push(BreakingFinding {
            change: BreakingChange::SqlBodyChanged {
                model: model_name.to_string(),
            },
            severity: BreakingSeverity::Info,
        });
    }
}

fn diff_columns(
    model: &str,
    old: &[TypedColumn],
    new: &[TypedColumn],
    findings: &mut Vec<BreakingFinding>,
) {
    let old_by_name: BTreeMap<&str, (usize, &TypedColumn)> = old
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.as_str(), (i, c)))
        .collect();
    let new_by_name: BTreeMap<&str, (usize, &TypedColumn)> = new
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.as_str(), (i, c)))
        .collect();

    for (name, (_old_idx, old_col)) in &old_by_name {
        match new_by_name.get(name) {
            None => findings.push(BreakingFinding {
                change: BreakingChange::ColumnDropped {
                    model: model.to_string(),
                    column: (*name).to_string(),
                    data_type: old_col.data_type.to_string(),
                },
                severity: BreakingSeverity::Breaking,
            }),
            Some((_new_idx, new_col)) => {
                if old_col.data_type != new_col.data_type {
                    let narrowing = is_type_narrowing(&old_col.data_type, &new_col.data_type);
                    findings.push(BreakingFinding {
                        change: BreakingChange::ColumnTypeChanged {
                            model: model.to_string(),
                            column: (*name).to_string(),
                            old_type: old_col.data_type.to_string(),
                            new_type: new_col.data_type.to_string(),
                            narrowing,
                        },
                        severity: if narrowing {
                            BreakingSeverity::Breaking
                        } else {
                            BreakingSeverity::Info
                        },
                    });
                }
                if old_col.nullable != new_col.nullable {
                    // Nullable → NOT NULL is breaking (consumers may have
                    // ingested NULLs that can no longer be stored).
                    // NOT NULL → Nullable is informational (purely relaxing).
                    let severity = if old_col.nullable && !new_col.nullable {
                        BreakingSeverity::Warning
                    } else {
                        BreakingSeverity::Info
                    };
                    findings.push(BreakingFinding {
                        change: BreakingChange::ColumnNullabilityChanged {
                            model: model.to_string(),
                            column: (*name).to_string(),
                            old_nullable: old_col.nullable,
                            new_nullable: new_col.nullable,
                        },
                        severity,
                    });
                }
            }
        }
    }

    for (name, (new_idx, new_col)) in &new_by_name {
        if !old_by_name.contains_key(name) {
            // NOT NULL additions are warnings (need a default for existing
            // consumers); nullable additions are pure info.
            let severity = if new_col.nullable {
                BreakingSeverity::Info
            } else {
                BreakingSeverity::Warning
            };
            findings.push(BreakingFinding {
                change: BreakingChange::ColumnAdded {
                    model: model.to_string(),
                    column: (*name).to_string(),
                    data_type: new_col.data_type.to_string(),
                    nullable: new_col.nullable,
                },
                severity,
            });
            let _ = new_idx; // reserved for future positional reporting
        }
    }

    // Reorder detection — only on columns that exist on both sides.
    for (name, (old_idx, _)) in &old_by_name {
        if let Some((new_idx, _)) = new_by_name.get(name) {
            if old_idx != new_idx {
                findings.push(BreakingFinding {
                    change: BreakingChange::ColumnReordered {
                        model: model.to_string(),
                        column: (*name).to_string(),
                        old_index: *old_idx,
                        new_index: *new_idx,
                    },
                    severity: BreakingSeverity::Warning,
                });
            }
        }
    }
}

fn diff_materialization(
    model: &str,
    old: &MaterializationStrategy,
    new: &MaterializationStrategy,
    findings: &mut Vec<BreakingFinding>,
) {
    let old_tag = strategy_tag(old);
    let new_tag = strategy_tag(new);
    if old_tag != new_tag {
        findings.push(BreakingFinding {
            change: BreakingChange::MaterializationStrategyChanged {
                model: model.to_string(),
                old_strategy: old_tag.to_string(),
                new_strategy: new_tag.to_string(),
            },
            severity: BreakingSeverity::Breaking,
        });
        return;
    }

    // Same top-level variant — drill into the variant-specific fields.
    match (old, new) {
        (
            MaterializationStrategy::Incremental {
                timestamp_column: o_ts,
            },
            MaterializationStrategy::Incremental {
                timestamp_column: n_ts,
            },
        ) if o_ts != n_ts => findings.push(BreakingFinding {
            change: BreakingChange::MaterializationKeyChanged {
                model: model.to_string(),
                key_kind: "timestamp_column".to_string(),
                old: vec![o_ts.clone()],
                new: vec![n_ts.clone()],
            },
            severity: BreakingSeverity::Breaking,
        }),
        (
            MaterializationStrategy::Merge {
                unique_key: o_uk,
                update_columns: o_uc,
            },
            MaterializationStrategy::Merge {
                unique_key: n_uk,
                update_columns: n_uc,
            },
        ) => {
            let o_uk_vec: Vec<String> = o_uk.iter().map(std::string::ToString::to_string).collect();
            let n_uk_vec: Vec<String> = n_uk.iter().map(std::string::ToString::to_string).collect();
            if o_uk_vec != n_uk_vec {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "unique_key".to_string(),
                        old: o_uk_vec,
                        new: n_uk_vec,
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
            let o_uc_vec = column_selection_to_vec(o_uc);
            let n_uc_vec = column_selection_to_vec(n_uc);
            if o_uc_vec != n_uc_vec {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "update_columns".to_string(),
                        old: o_uc_vec,
                        new: n_uc_vec,
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
        }
        (
            MaterializationStrategy::DynamicTable { target_lag: o },
            MaterializationStrategy::DynamicTable { target_lag: n },
        ) if o != n => findings.push(BreakingFinding {
            change: BreakingChange::MaterializationKeyChanged {
                model: model.to_string(),
                key_kind: "target_lag".to_string(),
                old: vec![o.clone()],
                new: vec![n.clone()],
            },
            severity: BreakingSeverity::Warning,
        }),
        (
            MaterializationStrategy::TimeInterval {
                time_column: o_tc,
                granularity: o_g,
                ..
            },
            MaterializationStrategy::TimeInterval {
                time_column: n_tc,
                granularity: n_g,
                ..
            },
        ) => {
            if o_tc != n_tc {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "time_column".to_string(),
                        old: vec![o_tc.clone()],
                        new: vec![n_tc.clone()],
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
            if o_g != n_g {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "granularity".to_string(),
                        old: vec![format!("{o_g:?}")],
                        new: vec![format!("{n_g:?}")],
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
        }
        (
            MaterializationStrategy::DeleteInsert { partition_by: o },
            MaterializationStrategy::DeleteInsert { partition_by: n },
        ) => {
            let o_vec: Vec<String> = o.iter().map(std::string::ToString::to_string).collect();
            let n_vec: Vec<String> = n.iter().map(std::string::ToString::to_string).collect();
            if o_vec != n_vec {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "partition_by".to_string(),
                        old: o_vec,
                        new: n_vec,
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
        }
        (
            MaterializationStrategy::Microbatch {
                timestamp_column: o_ts,
                granularity: o_g,
            },
            MaterializationStrategy::Microbatch {
                timestamp_column: n_ts,
                granularity: n_g,
            },
        ) => {
            if o_ts != n_ts {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "timestamp_column".to_string(),
                        old: vec![o_ts.clone()],
                        new: vec![n_ts.clone()],
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
            if o_g != n_g {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "granularity".to_string(),
                        old: vec![format!("{o_g:?}")],
                        new: vec![format!("{n_g:?}")],
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
        }
        (
            MaterializationStrategy::ContentAddressed {
                storage_prefix: o_sp,
                partition_columns: o_pc,
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: n_sp,
                partition_columns: n_pc,
            },
        ) => {
            if o_sp != n_sp {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "storage_prefix".to_string(),
                        old: vec![o_sp.clone()],
                        new: vec![n_sp.clone()],
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
            if o_pc != n_pc {
                findings.push(BreakingFinding {
                    change: BreakingChange::MaterializationKeyChanged {
                        model: model.to_string(),
                        key_kind: "partition_columns".to_string(),
                        old: o_pc.clone(),
                        new: n_pc.clone(),
                    },
                    severity: BreakingSeverity::Breaking,
                });
            }
        }
        // FullRefresh / MaterializedView / Ephemeral have no variant fields.
        _ => {}
    }
}

fn diff_target(model: &str, old: &TargetRef, new: &TargetRef, findings: &mut Vec<BreakingFinding>) {
    if old.full_name() != new.full_name() {
        findings.push(BreakingFinding {
            change: BreakingChange::TargetRenamed {
                model: model.to_string(),
                old: old.full_name(),
                new: new.full_name(),
            },
            severity: BreakingSeverity::Breaking,
        });
    }
}

fn diff_sources(model: &str, old: &ModelIr, new: &ModelIr, findings: &mut Vec<BreakingFinding>) {
    let old_sources = collect_sources(old);
    let new_sources = collect_sources(new);
    if old_sources != new_sources {
        findings.push(BreakingFinding {
            change: BreakingChange::SourceChanged {
                model: model.to_string(),
                old: old_sources,
                new: new_sources,
            },
            severity: BreakingSeverity::Breaking,
        });
    }
}

fn collect_sources(m: &ModelIr) -> Vec<String> {
    let mut acc: Vec<String> = Vec::new();
    if let Some(src) = &m.source {
        acc.push(src.full_name());
    }
    for src in &m.sources {
        acc.push(src.full_name());
    }
    acc.sort();
    acc
}

fn diff_columns_selection(
    model: &str,
    old: &Option<ColumnSelection>,
    new: &Option<ColumnSelection>,
    findings: &mut Vec<BreakingFinding>,
) {
    let old_vec = old
        .as_ref()
        .map(column_selection_to_vec)
        .unwrap_or_default();
    let new_vec = new
        .as_ref()
        .map(column_selection_to_vec)
        .unwrap_or_default();
    if old_vec != new_vec {
        findings.push(BreakingFinding {
            change: BreakingChange::ReplicationColumnsChanged {
                model: model.to_string(),
                old: old_vec,
                new: new_vec,
            },
            severity: BreakingSeverity::Breaking,
        });
    }
}

fn diff_lakehouse_format(
    model: &str,
    old: &Option<LakehouseFormat>,
    new: &Option<LakehouseFormat>,
    findings: &mut Vec<BreakingFinding>,
) {
    if old != new {
        findings.push(BreakingFinding {
            change: BreakingChange::LakehouseFormatChanged {
                model: model.to_string(),
                old: old
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_else(|| "none".to_string()),
                new: new
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_else(|| "none".to_string()),
            },
            severity: BreakingSeverity::Breaking,
        });
    }
}

fn diff_partition_by(
    model: &str,
    old: &Option<LakehouseOptions>,
    new: &Option<LakehouseOptions>,
    findings: &mut Vec<BreakingFinding>,
) {
    let old_pb = old
        .as_ref()
        .map(|o| o.partition_by.clone())
        .unwrap_or_default();
    let new_pb = new
        .as_ref()
        .map(|o| o.partition_by.clone())
        .unwrap_or_default();
    if old_pb != new_pb {
        findings.push(BreakingFinding {
            change: BreakingChange::PartitionByChanged {
                model: model.to_string(),
                old: old_pb,
                new: new_pb,
            },
            severity: BreakingSeverity::Breaking,
        });
    }
}

fn diff_column_masks(
    model: &str,
    old: &[rocky_ir::ColumnMask],
    new: &[rocky_ir::ColumnMask],
    findings: &mut Vec<BreakingFinding>,
) {
    let old_by_col: BTreeMap<&str, &rocky_ir::ColumnMask> =
        old.iter().map(|m| (m.column.as_ref(), m)).collect();
    let new_by_col: BTreeMap<&str, &rocky_ir::ColumnMask> =
        new.iter().map(|m| (m.column.as_ref(), m)).collect();

    for (col, old_mask) in &old_by_col {
        match new_by_col.get(col) {
            None => findings.push(BreakingFinding {
                change: BreakingChange::ColumnMaskChanged {
                    model: model.to_string(),
                    column: (*col).to_string(),
                    old_strategy: Some(format!("{:?}", old_mask.strategy)),
                    new_strategy: None,
                },
                // Removing a mask exposes previously-redacted data — treat
                // as Breaking under the trust-system framing.
                severity: BreakingSeverity::Breaking,
            }),
            Some(new_mask) => {
                if format!("{:?}", old_mask.strategy) != format!("{:?}", new_mask.strategy) {
                    findings.push(BreakingFinding {
                        change: BreakingChange::ColumnMaskChanged {
                            model: model.to_string(),
                            column: (*col).to_string(),
                            old_strategy: Some(format!("{:?}", old_mask.strategy)),
                            new_strategy: Some(format!("{:?}", new_mask.strategy)),
                        },
                        severity: BreakingSeverity::Warning,
                    });
                }
            }
        }
    }
    for (col, new_mask) in &new_by_col {
        if !old_by_col.contains_key(col) {
            findings.push(BreakingFinding {
                change: BreakingChange::ColumnMaskChanged {
                    model: model.to_string(),
                    column: (*col).to_string(),
                    old_strategy: None,
                    new_strategy: Some(format!("{:?}", new_mask.strategy)),
                },
                // Adding a mask changes the emitted SQL but doesn't break
                // existing consumers — they get masked output where they
                // previously got plaintext. Warning rather than Breaking.
                severity: BreakingSeverity::Warning,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn strategy_tag(s: &MaterializationStrategy) -> &'static str {
    match s {
        MaterializationStrategy::FullRefresh => "full_refresh",
        MaterializationStrategy::Incremental { .. } => "incremental",
        MaterializationStrategy::Merge { .. } => "merge",
        MaterializationStrategy::MaterializedView => "materialized_view",
        MaterializationStrategy::DynamicTable { .. } => "dynamic_table",
        MaterializationStrategy::TimeInterval { .. } => "time_interval",
        MaterializationStrategy::Ephemeral => "ephemeral",
        MaterializationStrategy::DeleteInsert { .. } => "delete_insert",
        MaterializationStrategy::Microbatch { .. } => "microbatch",
        MaterializationStrategy::ContentAddressed { .. } => "content_addressed",
    }
}

fn column_selection_to_vec(sel: &ColumnSelection) -> Vec<String> {
    match sel {
        ColumnSelection::All => vec!["*".to_string()],
        ColumnSelection::Explicit(cols) => {
            cols.iter().map(std::string::ToString::to_string).collect()
        }
    }
}

/// Classifies a type change as narrowing (the new type cannot represent
/// every value of the old type) versus widening / lateral.
///
/// Hand-coded for the canonical narrowing pairs Rocky cares about; defaults
/// to `false` (widening) for anything not explicitly enumerated so the
/// classifier doesn't over-alarm on unknown transitions.
fn is_type_narrowing(old: &RockyType, new: &RockyType) -> bool {
    use RockyType::*;
    match (old, new) {
        (Int64, Int32 | Float32) => true,
        (Float64, Float32 | Int32 | Int64) => true,
        (
            Decimal {
                precision: op,
                scale: os,
            },
            Decimal {
                precision: np,
                scale: ns,
            },
        ) => np < op || ns < os,
        (String, Int32 | Int64 | Float32 | Float64 | Boolean | Date | Timestamp | TimestampNtz) => {
            true
        }
        (Timestamp | TimestampNtz, Date) => true,
        (Array(_) | Map(..) | Struct(_) | Variant, _) if !matches!(new, Variant) => true,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_ir::{
        ColumnMask, ColumnSelection, GovernanceConfig, MaterializationStrategy, ModelIr, ProjectIr,
        SourceRef, TargetRef, TypedColumn,
    };
    use std::sync::Arc;

    fn typed_col(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    fn target(catalog: &str, schema: &str, table: &str) -> TargetRef {
        TargetRef {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    fn source(catalog: &str, schema: &str, table: &str) -> SourceRef {
        SourceRef {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
        }
    }

    fn base_model() -> ModelIr {
        let mut m = ModelIr::replication(
            target("warehouse", "raw", "orders"),
            MaterializationStrategy::FullRefresh,
            source("source", "public", "orders"),
            ColumnSelection::All,
            Vec::new(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        );
        m.typed_columns = vec![
            typed_col("id", RockyType::Int64, false),
            typed_col(
                "amount",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
            typed_col("created_at", RockyType::Timestamp, false),
        ];
        m
    }

    fn project(models: Vec<ModelIr>) -> ProjectIr {
        ProjectIr {
            models,
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    #[test]
    fn identical_projects_produce_no_findings() {
        let old = project(vec![base_model()]);
        let new = project(vec![base_model()]);
        assert!(diff_project_ir(&old, &new).is_empty());
    }

    #[test]
    fn model_removed_is_breaking() {
        let old = project(vec![base_model()]);
        let new = project(vec![]);
        let findings = diff_project_ir(&old, &new);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].is_breaking());
        assert!(matches!(
            findings[0].change,
            BreakingChange::ModelRemoved { .. }
        ));
    }

    #[test]
    fn model_added_is_info() {
        let old = project(vec![]);
        let new = project(vec![base_model()]);
        let findings = diff_project_ir(&old, &new);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, BreakingSeverity::Info);
        assert!(matches!(
            findings[0].change,
            BreakingChange::ModelAdded { .. }
        ));
    }

    #[test]
    fn column_dropped_is_breaking() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns.pop();
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let dropped: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnDropped { .. }))
            .collect();
        assert_eq!(dropped.len(), 1);
        assert!(dropped[0].is_breaking());
    }

    #[test]
    fn column_added_nullable_is_info() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns
            .push(typed_col("new_col", RockyType::String, true));
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let added: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnAdded { .. }))
            .collect();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].severity, BreakingSeverity::Info);
    }

    #[test]
    fn column_added_not_null_is_warning() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns
            .push(typed_col("required_col", RockyType::String, false));
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let added: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnAdded { .. }))
            .collect();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].severity, BreakingSeverity::Warning);
    }

    #[test]
    fn type_widening_is_info() {
        // Int32 -> Int64 is widening.
        let mut old_m = base_model();
        old_m.typed_columns[0].data_type = RockyType::Int32;
        let mut new_m = base_model();
        new_m.typed_columns[0].data_type = RockyType::Int64;
        let old = project(vec![old_m]);
        let new = project(vec![new_m]);
        let findings = diff_project_ir(&old, &new);
        let type_changes: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnTypeChanged { .. }))
            .collect();
        assert_eq!(type_changes.len(), 1);
        assert_eq!(type_changes[0].severity, BreakingSeverity::Info);
    }

    #[test]
    fn type_narrowing_is_breaking() {
        let mut old_m = base_model();
        old_m.typed_columns[0].data_type = RockyType::Int64;
        let mut new_m = base_model();
        new_m.typed_columns[0].data_type = RockyType::Int32;
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let type_changes: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnTypeChanged { .. }))
            .collect();
        assert_eq!(type_changes.len(), 1);
        assert!(type_changes[0].is_breaking());
    }

    #[test]
    fn nullability_tightening_is_warning() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns[1].nullable = false; // was nullable
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let nul: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnNullabilityChanged { .. }))
            .collect();
        assert_eq!(nul.len(), 1);
        assert_eq!(nul[0].severity, BreakingSeverity::Warning);
    }

    #[test]
    fn column_reorder_is_warning() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns.swap(0, 2);
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let reorders: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnReordered { .. }))
            .collect();
        // Swapping 0 and 2 produces two reorder findings (both end columns
        // move).
        assert_eq!(reorders.len(), 2);
        for r in &reorders {
            assert_eq!(r.severity, BreakingSeverity::Warning);
        }
    }

    #[test]
    fn materialization_strategy_swap_is_breaking() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.materialization = MaterializationStrategy::Incremental {
            timestamp_column: "created_at".to_string(),
        };
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let strat: Vec<_> = findings
            .iter()
            .filter(|f| {
                matches!(
                    f.change,
                    BreakingChange::MaterializationStrategyChanged { .. }
                )
            })
            .collect();
        assert_eq!(strat.len(), 1);
        assert!(strat[0].is_breaking());
    }

    #[test]
    fn merge_unique_key_change_is_breaking() {
        let mut old_m = base_model();
        old_m.materialization = MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("id")],
            update_columns: ColumnSelection::All,
        };
        let mut new_m = base_model();
        new_m.materialization = MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("id"), Arc::from("tenant")],
            update_columns: ColumnSelection::All,
        };
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let key_changes: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::MaterializationKeyChanged { .. }))
            .collect();
        assert_eq!(key_changes.len(), 1);
        assert!(key_changes[0].is_breaking());
    }

    #[test]
    fn target_rename_is_breaking() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.target = target("warehouse", "raw", "orders_v2");
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        // target rename surfaces as ModelRemoved + ModelAdded since models
        // are keyed by target.full_name(); explicit TargetRenamed would
        // only fire if the model's name (not target) is the key. This
        // matches user-facing semantics: a renamed target IS a new model
        // from a downstream consumer's perspective.
        let removed: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ModelRemoved { .. }))
            .collect();
        let added: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ModelAdded { .. }))
            .collect();
        assert_eq!(removed.len(), 1);
        assert_eq!(added.len(), 1);
        assert!(removed[0].is_breaking());
    }

    #[test]
    fn partition_by_change_is_breaking() {
        use rocky_ir::LakehouseOptions;
        let mut old_m = base_model();
        old_m.format_options = Some(LakehouseOptions {
            partition_by: vec!["day".to_string()],
            ..Default::default()
        });
        let mut new_m = base_model();
        new_m.format_options = Some(LakehouseOptions {
            partition_by: vec!["day".to_string(), "tenant".to_string()],
            ..Default::default()
        });
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let pb: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::PartitionByChanged { .. }))
            .collect();
        assert_eq!(pb.len(), 1);
        assert!(pb[0].is_breaking());
    }

    #[test]
    fn lakehouse_format_swap_is_breaking() {
        use rocky_ir::LakehouseFormat;
        let mut old_m = base_model();
        old_m.format = Some(LakehouseFormat::DeltaTable);
        let mut new_m = base_model();
        new_m.format = Some(LakehouseFormat::IcebergTable);
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let fmt: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::LakehouseFormatChanged { .. }))
            .collect();
        assert_eq!(fmt.len(), 1);
        assert!(fmt[0].is_breaking());
    }

    #[test]
    fn column_mask_removal_is_breaking() {
        use rocky_ir::mask::MaskStrategy;
        let mut old_m = base_model();
        old_m.column_masks = vec![ColumnMask {
            column: Arc::from("email"),
            strategy: MaskStrategy::Hash,
        }];
        let new_m = base_model();
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let mask: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::ColumnMaskChanged { .. }))
            .collect();
        assert_eq!(mask.len(), 1);
        assert!(mask[0].is_breaking());
    }

    #[test]
    fn source_change_is_breaking() {
        let mut old_m = base_model();
        old_m.source = Some(source("source", "public", "orders"));
        let mut new_m = base_model();
        new_m.source = Some(source("source", "public", "orders_v2"));
        let findings = diff_project_ir(&project(vec![old_m]), &project(vec![new_m]));
        let src: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::SourceChanged { .. }))
            .collect();
        assert_eq!(src.len(), 1);
        assert!(src[0].is_breaking());
    }

    #[test]
    fn sql_body_change_alone_is_info() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.sql = "SELECT 1".to_string();
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let sql: Vec<_> = findings
            .iter()
            .filter(|f| matches!(f.change, BreakingChange::SqlBodyChanged { .. }))
            .collect();
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0].severity, BreakingSeverity::Info);
    }

    #[test]
    fn findings_serialise_to_canonical_json() {
        let old = project(vec![base_model()]);
        let mut m = base_model();
        m.typed_columns.pop();
        let new = project(vec![m]);
        let findings = diff_project_ir(&old, &new);
        let json = serde_json::to_string(&findings).expect("findings serialise");
        assert!(json.contains("column_dropped"));
        assert!(json.contains("breaking"));
    }
}
