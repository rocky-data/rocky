//! Type checking across the DAG.
//!
//! Propagates inferred types through the semantic graph AND walks SQL AST
//! expressions to infer types from CAST, aggregations, arithmetic, literals,
//! CASE/WHEN, and comparisons. Detects type mismatches, join key
//! incompatibilities, and provides diagnostics with suggestions.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use indexmap::IndexMap;
use rayon::prelude::*;
use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::parser::Parser;

use crate::compile::default_type_mapper;
use crate::diagnostic::{
    Diagnostic, E001, E020, E021, E022, E023, E024, E025, E026, E035, I001, I002, SourceSpan, W001,
    W002, W003, W004, W005, W006,
};
use crate::semantic::{ModelSchema, SemanticGraph};
use crate::types::{RockyType, TypedColumn};
use rocky_core::column_map::{CiKey, CiStr};
use rocky_ir::dag::{self, DagNode};

/// A reference location: which file and where in the source.
#[derive(Debug, Clone)]
pub struct RefLocation {
    pub file: PathBuf,
    pub line: usize,
    pub col: usize,
    pub end_col: usize,
}

/// Tracks where models and columns are referenced across the project.
#[derive(Debug, Default)]
pub struct ReferenceMap {
    /// model_name → locations where it appears in FROM/JOIN clauses.
    pub model_refs: HashMap<String, Vec<RefLocation>>,
    /// (model_name, column_name) → locations where the column is referenced.
    pub column_refs: HashMap<(String, String), Vec<RefLocation>>,
    /// model_name → location of its definition (the file itself).
    pub model_defs: HashMap<String, RefLocation>,
}

/// Result of type checking a project.
#[derive(Debug)]
pub struct TypeCheckResult {
    /// Per-model typed column schemas.
    pub typed_models: IndexMap<String, Vec<TypedColumn>>,
    /// Diagnostics from type checking.
    pub diagnostics: Vec<Diagnostic>,
    /// Reference map for Find References / Rename.
    pub reference_map: ReferenceMap,
    /// Wall-clock typecheck duration per model, in milliseconds.
    /// Empty entries are treated as `0` by callers; absent keys mean
    /// the model wasn't typechecked (e.g., source schemas injected
    /// directly via `source_schemas`).
    pub model_typecheck_ms: HashMap<String, u64>,
}

/// Scope for resolving column types during expression inference.
///
/// §P3.8: keys are `CiKey` (case-insensitive, owned; see
/// `rocky_core::column_map`) so `lookup` / `lookup_qualified` can `.get()`
/// via `CiStr::new(name)` without allocating a lowercased `String` per
/// call. `qualified` is nested `HashMap<CiKey, HashMap<CiKey, V>>` so the
/// table + column keys look up independently (a flat `(String, String)`
/// key would need `format!("{table}.{col}")` at the lookup site and
/// re-introduce the allocation).
struct TypeScope {
    /// column_name → (type, nullable) for all columns in scope.
    columns: HashMap<CiKey<'static>, (RockyType, bool)>,
    /// table → { column → (type, nullable) } for qualified references.
    qualified: HashMap<CiKey<'static>, HashMap<CiKey<'static>, (RockyType, bool)>>,
}

impl TypeScope {
    fn new() -> Self {
        Self {
            columns: HashMap::new(),
            qualified: HashMap::new(),
        }
    }

    fn lookup(&self, name: &str) -> (RockyType, bool) {
        self.columns
            .get(CiStr::new(name))
            .cloned()
            .unwrap_or((RockyType::Unknown, true))
    }

    fn lookup_qualified(&self, table: &str, col: &str) -> (RockyType, bool) {
        self.qualified
            .get(CiStr::new(table))
            .and_then(|m| m.get(CiStr::new(col)).cloned())
            .unwrap_or((RockyType::Unknown, true))
    }
}

/// Type check a project given its semantic graph and known source schemas.
pub fn typecheck_project(
    graph: &SemanticGraph,
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    _type_map: Option<&dyn Fn(&str) -> RockyType>,
) -> TypeCheckResult {
    typecheck_project_with_models(graph, source_schemas, _type_map, &[], None)
}

/// Type check with access to model SQL and file paths for reference tracking.
///
/// `join_keys_acc`, when provided, accumulates the time spent inside
/// `check_join_keys` so callers can attribute it as a sub-phase of typecheck.
pub fn typecheck_project_with_models(
    graph: &SemanticGraph,
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    _type_map: Option<&dyn Fn(&str) -> RockyType>,
    models: &[rocky_core::models::Model],
    join_keys_acc: Option<&Arc<AtomicU64>>,
) -> TypeCheckResult {
    let mut typed_models: IndexMap<String, Vec<TypedColumn>> = IndexMap::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let mut reference_map = ReferenceMap::default();
    let mut model_typecheck_ms: HashMap<String, u64> = HashMap::with_capacity(graph.models.len());

    // Build model name set for classifying table refs
    let mut model_names: HashSet<String> = HashSet::with_capacity(graph.models.len());
    model_names.extend(graph.models.keys().cloned());

    // Build model lookup by name
    let model_by_name: HashMap<&str, &rocky_core::models::Model> =
        models.iter().map(|m| (m.config.name.as_str(), m)).collect();

    // Register model definition locations
    for m in models {
        reference_map.model_defs.insert(
            m.config.name.clone(),
            RefLocation {
                file: PathBuf::from(&m.file_path),
                line: 1,
                col: 0,
                end_col: 0,
            },
        );
    }

    // Inject source schemas (already typed)
    for (source_name, cols) in source_schemas {
        typed_models.insert(source_name.clone(), cols.clone());
    }

    // Compute execution layers from the graph itself so we don't need a Project
    // here (keeps `typecheck_project` callable in test/AI contexts that don't
    // build a Project). Each layer can be type-checked in parallel because
    // models within a layer have no inter-dependencies by definition.
    let layers = derive_execution_layers(graph);

    // Pre-build column index for each typed model: name → position.
    // This is shared (read-only) across all workers within a layer, then
    // updated incrementally after each layer completes — avoiding O(M)
    // HashMap reconstructions when there are M total models.
    let mut col_index: HashMap<String, HashMap<String, usize>> =
        HashMap::with_capacity(typed_models.len());
    for (name, cols) in typed_models.iter() {
        let idx: HashMap<String, usize> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        col_index.insert(name.clone(), idx);
    }

    for layer in &layers {
        // Snapshot of typed_models that all par_iter workers can borrow.
        // It contains everything written by *prior* layers — no model in this
        // layer reads anything from another model in the same layer.
        let typed_snapshot = &typed_models;
        let col_index_snapshot = &col_index;

        let mut layer_outputs: Vec<ModelTypecheckOutput> = layer
            .par_iter()
            .with_min_len(32) // Don't split into rayon tasks if fewer than 32 items
            .filter_map(|model_name| {
                let model_schema = graph.model_schema(model_name)?;
                Some(compute_model_typecheck(
                    model_name,
                    model_schema,
                    graph,
                    typed_snapshot,
                    col_index_snapshot,
                    &model_by_name,
                    &model_names,
                    join_keys_acc,
                ))
            })
            .collect();

        // Deterministic merge order: sort by model name so diagnostics output
        // is stable regardless of how rayon scheduled the work.
        layer_outputs.sort_by(|a, b| a.model_name.cmp(&b.model_name));

        for out in layer_outputs {
            model_typecheck_ms.insert(out.model_name.clone(), out.typecheck_ms);
            // Update the shared column index with the newly typed model.
            let idx: HashMap<String, usize> = out
                .typed_cols
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), i))
                .collect();
            col_index.insert(out.model_name.clone(), idx);
            typed_models.insert(out.model_name, out.typed_cols);
            diagnostics.extend(out.diagnostics);
            merge_reference_map(&mut reference_map, out.ref_map);
        }
    }

    TypeCheckResult {
        typed_models,
        diagnostics,
        reference_map,
        model_typecheck_ms,
    }
}

/// §P3.1 — Incremental typecheck. Reuses `previous.typed_models` entries for
/// models that are **not** in `affected`, and re-typechecks only the models
/// that are.
///
/// The caller is responsible for computing a correct `affected` set — any
/// model whose typecheck inputs may have shifted (own SQL changed, upstream
/// changed, config changed, new to the project). Members of `affected` that
/// don't exist in `graph.models` are ignored.
///
/// The `reference_map` is always rebuilt in full (SQL scanning is cheap),
/// and diagnostics / timings are stitched so the result is observationally
/// identical to a full `typecheck_project_with_models` in shape.
pub fn typecheck_project_incremental(
    graph: &SemanticGraph,
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    models: &[rocky_core::models::Model],
    affected: &HashSet<String>,
    previous: &TypeCheckResult,
    join_keys_acc: Option<&Arc<AtomicU64>>,
) -> TypeCheckResult {
    let mut typed_models: IndexMap<String, Vec<TypedColumn>> = IndexMap::new();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let mut model_typecheck_ms: HashMap<String, u64> = HashMap::with_capacity(graph.models.len());

    let mut model_names: HashSet<String> = HashSet::with_capacity(graph.models.len());
    model_names.extend(graph.models.keys().cloned());

    let model_by_name: HashMap<&str, &rocky_core::models::Model> =
        models.iter().map(|m| (m.config.name.as_str(), m)).collect();

    // Files that belong to affected models — any reference_map entry
    // whose RefLocation.file is in this set came FROM an affected file and
    // is therefore stale.
    let affected_files: HashSet<PathBuf> = models
        .iter()
        .filter(|m| affected.contains(&m.config.name))
        .map(|m| PathBuf::from(&m.file_path))
        .collect();

    // Seed reference_map from `previous`, dropping stale entries (anything
    // recorded FROM an affected file — those refs were rebuilt below).
    // Avoids the SQL-reparse cost of running `collect_references` over the
    // non-affected models, which is the dominant overhead on wide DAGs.
    let mut reference_map = ReferenceMap {
        model_refs: previous
            .reference_map
            .model_refs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .filter(|loc| !affected_files.contains(&loc.file))
                        .cloned()
                        .collect(),
                )
            })
            .filter(|(_, v): &(String, Vec<RefLocation>)| !v.is_empty())
            .collect(),
        column_refs: previous
            .reference_map
            .column_refs
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .filter(|loc| !affected_files.contains(&loc.file))
                        .cloned()
                        .collect(),
                )
            })
            .filter(|(_, v): &((String, String), Vec<RefLocation>)| !v.is_empty())
            .collect(),
        model_defs: HashMap::new(),
    };

    // Register fresh model definition locations for every model in the
    // current graph (overwrites stale defs from removed models).
    for m in models {
        reference_map.model_defs.insert(
            m.config.name.clone(),
            RefLocation {
                file: PathBuf::from(&m.file_path),
                line: 1,
                col: 0,
                end_col: 0,
            },
        );
    }

    // Inject source schemas.
    for (source_name, cols) in source_schemas {
        typed_models.insert(source_name.clone(), cols.clone());
    }

    // Seed typed_models with non-affected entries from the previous result.
    // Source schemas overlap is fine — we inserted them first, and re-inserting
    // the same entry from previous.typed_models is idempotent.
    for (name, cols) in &previous.typed_models {
        if !affected.contains(name) && graph.models.contains_key(name) {
            typed_models.insert(name.clone(), cols.clone());
            // Carry over timing so model_typecheck_ms stays complete.
            if let Some(&ms) = previous.model_typecheck_ms.get(name) {
                model_typecheck_ms.insert(name.clone(), ms);
            }
        }
    }

    let layers = derive_execution_layers(graph);

    let mut col_index: HashMap<String, HashMap<String, usize>> =
        HashMap::with_capacity(typed_models.len());
    for (name, cols) in typed_models.iter() {
        let idx: HashMap<String, usize> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        col_index.insert(name.clone(), idx);
    }

    for layer in &layers {
        let typed_snapshot = &typed_models;
        let col_index_snapshot = &col_index;

        let mut layer_outputs: Vec<ModelTypecheckOutput> = layer
            .par_iter()
            .with_min_len(32)
            .filter_map(|model_name| {
                if !affected.contains(model_name.as_str()) {
                    return None;
                }
                let model_schema = graph.model_schema(model_name)?;
                Some(compute_model_typecheck(
                    model_name,
                    model_schema,
                    graph,
                    typed_snapshot,
                    col_index_snapshot,
                    &model_by_name,
                    &model_names,
                    join_keys_acc,
                ))
            })
            .collect();

        layer_outputs.sort_by(|a, b| a.model_name.cmp(&b.model_name));

        for out in layer_outputs {
            model_typecheck_ms.insert(out.model_name.clone(), out.typecheck_ms);
            let idx: HashMap<String, usize> = out
                .typed_cols
                .iter()
                .enumerate()
                .map(|(i, c)| (c.name.clone(), i))
                .collect();
            col_index.insert(out.model_name.clone(), idx);
            typed_models.insert(out.model_name, out.typed_cols);
            diagnostics.extend(out.diagnostics);
            merge_reference_map(&mut reference_map, out.ref_map);
        }
    }

    // Carry over non-affected models' diagnostics so the full diagnostic set
    // reflects the whole project. The affected models produced fresh
    // diagnostics above; here we stitch in the untouched remainder.
    for d in &previous.diagnostics {
        if !affected.contains(&d.model) && graph.models.contains_key(&d.model) {
            diagnostics.push(d.clone());
        }
    }

    TypeCheckResult {
        typed_models,
        diagnostics,
        reference_map,
        model_typecheck_ms,
    }
}

/// Pure per-model output from a single typecheck pass.
struct ModelTypecheckOutput {
    model_name: String,
    typed_cols: Vec<TypedColumn>,
    diagnostics: Vec<Diagnostic>,
    ref_map: ReferenceMap,
    /// Wall-clock duration of `compute_model_typecheck` for this model,
    /// in milliseconds. Surfaced via `TypeCheckResult.model_typecheck_ms`
    /// so callers (CLI / Dagster) can attach per-model compile time to
    /// materialization metadata.
    typecheck_ms: u64,
}

/// Type-check a single model. Pure: no shared mutation, safe to call from a
/// rayon worker. All inputs are read-only borrows; outputs are owned.
#[allow(clippy::too_many_arguments)]
fn compute_model_typecheck(
    model_name: &str,
    model_schema: &ModelSchema,
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    col_index: &HashMap<String, HashMap<String, usize>>,
    model_by_name: &HashMap<&str, &rocky_core::models::Model>,
    model_names: &HashSet<String>,
    join_keys_acc: Option<&Arc<AtomicU64>>,
) -> ModelTypecheckOutput {
    let model_start = Instant::now();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    // Extract references from this model's SQL if available.
    let ref_map = if let Some(model) = model_by_name.get(model_name) {
        collect_references(&model.sql, &model.file_path, model_names)
    } else {
        ReferenceMap::default()
    };

    // Step 1: Lineage-based type propagation
    let mut typed_cols: Vec<TypedColumn> = Vec::with_capacity(model_schema.columns.len());

    for col_def in &model_schema.columns {
        let producing_edge = graph.producing_edge(model_name, &col_def.name);

        let (data_type, nullable) = if let Some(edge) = producing_edge {
            let upstream_type = typed_models
                .get(&*edge.source.model)
                .and_then(|cols| {
                    col_index
                        .get(&*edge.source.model)
                        .and_then(|idx| idx.get(&*edge.source.column))
                        .map(|&i| &cols[i])
                })
                .map(|c| (c.data_type.clone(), c.nullable))
                .unwrap_or((RockyType::Unknown, true));

            match &edge.transform {
                rocky_sql::lineage::TransformKind::Direct => upstream_type,
                // Infallible cast: type is refined in Step 2; nullability is the
                // input's (a non-null input stays non-null).
                rocky_sql::lineage::TransformKind::Cast => (RockyType::Unknown, upstream_type.1),
                // Fallible cast (`TRY_CAST` / `SAFE_CAST`): returns NULL on a
                // failed conversion, so the output is nullable regardless of the
                // input's nullability (#1148). The target type is still refined
                // in Step 2 — only the nullable bit differs from `Cast`.
                rocky_sql::lineage::TransformKind::TryCast => (RockyType::Unknown, true),
                rocky_sql::lineage::TransformKind::Aggregation(func) => {
                    let func_upper = func.to_uppercase();
                    infer_aggregation_type(&func_upper, &upstream_type.0)
                }
                rocky_sql::lineage::TransformKind::Expression => (RockyType::Unknown, true),
            }
        } else {
            (RockyType::Unknown, true)
        };

        typed_cols.push(TypedColumn {
            name: col_def.name.clone(),
            data_type,
            nullable,
        });
    }

    // Lineage resolves aliases to source models, losing which occurrence an
    // outer join null-extends. Infer in the SQL relation scope before using
    // the resulting nullable bit for contracts and downstream models.
    let needs_inference = typed_cols.iter().any(|col| {
        graph
            .producing_edge(model_name, &col.name)
            .is_some_and(|edge| {
                edge.transform.is_cast()
                    || (!col.nullable
                        && edge.transform == rocky_sql::lineage::TransformKind::Direct)
            })
    });
    let inferred_cols = model_by_name
        .get(model_name)
        .filter(|_| needs_inference)
        .and_then(|model| {
            infer_select_types_with_lookup(&model.sql, &|name| {
                typed_models.get(name).map(Vec::as_slice)
            })
            .ok()
        });
    if let Some(inferred) = &inferred_cols {
        let mut nullable_by_name = HashMap::new();
        for (index, col) in inferred.columns.iter().enumerate() {
            // Match semantic-graph star expansion, which keeps the first
            // occurrence when multiple relations expose the same name.
            nullable_by_name
                .entry(col.name.as_str())
                .or_insert((col.nullable, inferred.reference_outputs.contains(&index)));
        }
        for col in &mut typed_cols {
            let Some(edge) = graph.producing_edge(model_name, &col.name) else {
                continue;
            };
            let Some(&(nullable, reference_output)) = nullable_by_name.get(col.name.as_str())
            else {
                continue;
            };
            if edge.transform == rocky_sql::lineage::TransformKind::Direct
                || (edge.transform == rocky_sql::lineage::TransformKind::Cast && reference_output)
            {
                // Lineage also labels CAST(SUM(...)) as Cast. Preserve existing
                // computed-expression behavior; only reference casts carry this
                // join-side correction. Cast target refinement still runs below.
                col.nullable |= nullable;
            }
        }
    }
    let enhanced_diags = enhanced_inference(
        model_name,
        graph,
        typed_models,
        col_index,
        inferred_cols
            .as_ref()
            .map(|inferred| inferred.columns.as_slice()),
        &mut typed_cols,
    );
    diagnostics.extend(enhanced_diags);

    // Step 3: SELECT * warning
    let schema_incomplete = model_schema.has_star
        && model_schema
            .upstream
            .iter()
            .any(|up| typed_models.get(up).is_none_or(std::vec::Vec::is_empty));

    if model_schema.has_star {
        if schema_incomplete {
            diagnostics.push(Diagnostic::warning(
                W002,
                model_name,
                "SELECT * prevents full type checking — upstream schema unknown",
            ));
        } else {
            diagnostics.push(Diagnostic::info(
                I001,
                model_name,
                "SELECT * used — consider explicit column list for stability",
            ));
        }
    }

    // Step 4: Join key compatibility (timed if accumulator provided).
    let jk_start = join_keys_acc.map(|_| Instant::now());
    let join_diags = check_join_keys(model_name, typed_models, graph);
    if let (Some(start), Some(acc)) = (jk_start, join_keys_acc) {
        acc.fetch_add(start.elapsed().as_micros() as u64 / 1000, Ordering::Relaxed);
    }
    diagnostics.extend(join_diags);

    // Step 5: strategy validation against the typed output schema. For models
    // declaring `[strategy] type = "time_interval"` we confirm the partition
    // column is real, has the right type, isn't nullable, etc. — see
    // `check_time_interval_strategy` for the full list of E020-E026 + W003.
    // For `type = "merge"` we confirm every `unique_key` column is real — see
    // `check_merge_strategy` (W006).
    //
    // W006 is an *absence* check, so it only runs when `typed_cols` is provably
    // the model's whole output. That question is answered by
    // `ModelSchema::schema_is_complete`, which the semantic-graph builder
    // derives from the lineage extractor — not reconstructed here. Deriving it
    // locally is what produced this check's false positives twice over: neither
    // "is any upstream unknown?" nor "is there a star?" is a completeness
    // signal, because a star-free projection can still fail to enumerate (a
    // parenthesised or computed item lineage cannot name) and an emptiness
    // check misses the partial case.
    if let Some(model) = model_by_name.get(model_name) {
        diagnostics.extend(check_time_interval_strategy(model, &typed_cols));
        diagnostics.extend(check_merge_strategy(
            model,
            &typed_cols,
            model_schema.schema_is_complete(),
        ));
    }

    // Step 6: Enrich diagnostics with the model's file path as a SourceSpan
    // when they don't already have one. This gives miette a file to render.
    if let Some(model) = model_by_name.get(model_name) {
        let file_path = &model.file_path;
        for diag in &mut diagnostics {
            if diag.span.is_none() {
                diag.span = Some(SourceSpan {
                    file: file_path.clone(),
                    line: 1,
                    col: 1,
                });
            }
        }
    }

    let typecheck_ms = model_start.elapsed().as_millis() as u64;

    ModelTypecheckOutput {
        model_name: model_name.to_string(),
        typed_cols,
        diagnostics,
        ref_map,
        typecheck_ms,
    }
}

/// Validate a model's `time_interval` strategy against its typed output schema.
///
/// Returns diagnostics for any of the following violations (no diagnostic if
/// the model isn't using `time_interval`):
///
/// | Code  | Severity | Meaning |
/// |-------|----------|---------|
/// | E020  | Error    | `time_column` not in the model's output schema |
/// | E021  | Error    | `time_column` is not a date/timestamp type |
/// | E022  | Error    | `time_column` is nullable |
/// | E023  | Error    | `time_column` failed SQL identifier validation |
/// | E024  | Error    | Neither `@start_date` nor `@end_date` referenced in SQL |
/// | W003  | Warning  | Only one of `@start_date` / `@end_date` referenced |
/// | E025  | Error    | `granularity = "hour"` requires TIMESTAMP, not DATE |
/// | E026  | Error    | `first_partition` is not a valid canonical key for grain |
fn check_time_interval_strategy(
    model: &rocky_core::models::Model,
    typed_cols: &[crate::types::TypedColumn],
) -> Vec<Diagnostic> {
    use rocky_core::models::StrategyConfig;
    use rocky_ir::TimeGrain;

    let mut diagnostics = Vec::new();
    let model_name = model.config.name.as_str();

    let (time_column, granularity, first_partition) = match &model.config.strategy {
        StrategyConfig::TimeInterval {
            time_column,
            granularity,
            first_partition,
            ..
        } => (time_column, *granularity, first_partition.as_deref()),
        // Not a time_interval model — nothing to check.
        _ => return diagnostics,
    };

    // E023: identifier validation. Run before the schema lookup so a malformed
    // column name produces a clear error rather than a "missing column" hit.
    if let Err(e) = rocky_sql::validation::validate_identifier(time_column) {
        diagnostics.push(
            Diagnostic::error(
                E023,
                model_name,
                format!("time_column '{time_column}' is not a valid SQL identifier: {e}"),
            )
            .with_suggestion(
                "Use a column name matching [a-zA-Z0-9_]+ (no quotes, dots, or spaces)",
            ),
        );
    }

    // E020: time_column must exist in the typed output schema.
    let column = typed_cols.iter().find(|c| c.name == *time_column);
    let Some(column) = column else {
        diagnostics.push(
            Diagnostic::error(
                E020,
                model_name,
                format!(
                    "time_column '{time_column}' is not in the output schema of model '{model_name}'"
                ),
            )
            .with_suggestion(format!(
                "Available columns: {}",
                typed_cols
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        );
        // Without the column we can't run E021/E022/E025; bail on column-shape
        // checks but still validate first_partition and the SQL placeholders.
        diagnostics.extend(check_time_interval_placeholders(model_name, &model.sql));
        if let Some(fp) = first_partition {
            diagnostics.extend(check_first_partition(model_name, granularity, fp));
        }
        return diagnostics;
    };

    // For E021/E022/E025 we need to know the column's actual type. If the
    // compiler couldn't infer it (e.g., the model reads from a raw warehouse
    // table whose schema isn't declared in source_schemas), the type will be
    // Unknown — in which case we skip the type-shape checks rather than
    // emit a misleading "not temporal" or "is nullable" error. The runtime
    // will catch any actual type mismatch when SQL execution fails.
    let type_known = !matches!(column.data_type, crate::types::RockyType::Unknown);

    // E021: time_column must be a date/timestamp type.
    if type_known && !column.data_type.is_temporal() {
        diagnostics.push(
            Diagnostic::error(
                E021,
                model_name,
                format!(
                    "time_column '{time_column}' has type {:?}, but time_interval requires a date or timestamp column",
                    column.data_type
                ),
            )
            .with_suggestion(
                "Change the column to DATE/TIMESTAMP, or pick a different time_column",
            ),
        );
    }

    // E022: time_column must not be nullable. Partition keys can't be NULL.
    // Skip when the type is Unknown — the nullable bit defaults to true in
    // that case, which would falsely fire on every model with an inferred
    // upstream.
    if type_known && column.nullable {
        diagnostics.push(
            Diagnostic::error(
                E022,
                model_name,
                format!(
                    "time_column '{time_column}' is nullable, but partition keys cannot be NULL"
                ),
            )
            .with_suggestion("Wrap the column in COALESCE(...) or filter out NULLs upstream"),
        );
    }

    // E025: granularity = "hour" requires a TIMESTAMP column. A DATE column
    // has no time-of-day and can't carry hourly partitions. Same Unknown
    // handling as E021/E022.
    if type_known
        && granularity == TimeGrain::Hour
        && matches!(column.data_type, crate::types::RockyType::Date)
    {
        diagnostics.push(
            Diagnostic::error(
                E025,
                model_name,
                format!(
                    "granularity 'hour' requires a TIMESTAMP column, but '{time_column}' is DATE"
                ),
            )
            .with_suggestion("Use granularity = 'day' for DATE columns, or convert to TIMESTAMP"),
        );
    }

    // E024 + W003: validate placeholder usage in the SQL body.
    diagnostics.extend(check_time_interval_placeholders(model_name, &model.sql));

    // E026: first_partition format must match the granularity.
    if let Some(fp) = first_partition {
        diagnostics.extend(check_first_partition(model_name, granularity, fp));
    }

    diagnostics
}

/// Validate a model's `merge` strategy against its typed output schema.
///
/// Returns diagnostics for any of the following violations (no diagnostic if
/// the model isn't using `merge`):
///
/// | Code  | Severity | Meaning |
/// |-------|----------|---------|
/// | W006  | Warning  | `unique_key` column not in the model's output schema |
///
/// This is the merge-key counterpart to [`check_time_interval_strategy`]'s
/// E020: a `unique_key` naming a column the model doesn't produce is a typo
/// that would otherwise only surface when the warehouse rejects the generated
/// `MERGE ... ON` clause.
///
/// # Why this warns rather than errors
///
/// The check can only be as trustworthy as the compiler's enumeration of a
/// model's output columns, and that enumeration is recovered from lineage
/// extraction rather than a full semantic analysis of the SQL. Three distinct
/// false-positive classes were found in this check before it ever shipped, each
/// one a different way the enumeration falls short. Under an error severity
/// every such gap fails a valid build; under a warning it costs a line of
/// output. Given a soft input signal, the soft severity is the honest one.
///
/// # `schema_complete`
///
/// Supplied by [`crate::semantic::ModelSchema::schema_is_complete`] — the check
/// runs only when the model's output columns are *provably* the whole set. It
/// deliberately does not re-derive that locally: this is an absence check, and
/// "column X is not here" is only meaningful against a complete list. A star
/// may expand from a raw source with no known schema; an unnamed non-identifier
/// projection item (`SELECT (order_id)`) contributes no entry at all. In both
/// cases `typed_cols` is a partial view and every key would look missing.
///
/// # Case sensitivity — matched case-insensitively, with one known gap
///
/// `unique_key` entries are constrained to `^[a-zA-Z0-9_]+$` by
/// [`rocky_sql::validation::validate_identifier`], so a key is always a plain
/// identifier. What each dialect then does with it differs, and the survey
/// matters because it bounds what this check can honestly claim:
///
/// | Adapter    | `merge_into` renders the key as | Case behaviour |
/// |------------|---------------------------------|----------------|
/// | Databricks | `t.{key} = s.{key}` — bare      | resolves case-insensitively |
/// | DuckDB     | `t.{key} = s.{key}` — bare      | resolves case-insensitively |
/// | `BigQuery` | `target.{key} = source.{key}` — bare | resolves case-insensitively |
/// | Snowflake  | `t.{q} = s.{q}` — **double-quoted** via `quote_identifier` | case-**sensitive** |
/// | Trino      | n/a — returns `not_supported`; `merge` is rejected at validate time | n/a |
///
/// Snowflake quotes deliberately: it folds unquoted identifiers to uppercase,
/// so a lowercase stored column only resolves under explicit quoting. The
/// consequence for this check is a real, accepted limitation: `unique_key =
/// ["ORDER_ID"]` against a projected `order_id` passes here and then fails at
/// run time on Snowflake alone.
///
/// Matching case-insensitively anyway is the correct trade:
///
/// - Exact matching would emit on every case-only difference, which is valid on
///   four of the five adapters — trading a Snowflake-only false negative for a
///   false positive on everything else.
/// - Deciding per dialect is not possible here. The target adapter is not
///   reachable at typecheck time: neither
///   [`crate::compile::CompilerConfig`] nor `typecheck_project_with_models`
///   carries one, and callers such as the LSP and `rocky-ai` legitimately have
///   no adapter configured at all. Plumbing one through to serve a single
///   warning is not a trade worth making.
/// - The failure this check exists to prevent — a typo'd key — is caught
///   regardless; only the case-variant subset escapes, and only on Snowflake.
///
/// The Snowflake gap is documented rather than closed. Closing it belongs with
/// a dialect-aware validation pass that has the adapter in hand, not here.
///
/// # Boundary
///
/// Case-insensitive matching cannot disambiguate a model that projects two
/// columns differing only in case (`Order_ID` and `order_id`). `unique_key =
/// ["ORDER_ID"]` matches both and the check stays quiet. That is acceptable:
/// this is an existence check, and the column does exist. Such a model is
/// already ambiguous on every case-insensitive warehouse — the ambiguity is the
/// projection's, and diagnosing it belongs to a duplicate-column check.
fn check_merge_strategy(
    model: &rocky_core::models::Model,
    typed_cols: &[crate::types::TypedColumn],
    schema_complete: bool,
) -> Vec<Diagnostic> {
    use rocky_core::models::StrategyConfig;

    let model_name = model.config.name.as_str();

    let StrategyConfig::Merge { unique_key, .. } = &model.config.strategy else {
        // Not a merge model — nothing to check.
        return Vec::new();
    };

    // Can't enumerate the model's real output columns, so an absence check
    // would be guesswork. Stay quiet rather than emit a false positive.
    if !schema_complete {
        return Vec::new();
    }

    // W006: every unique_key entry must exist in the typed output schema.
    //
    // Matched case-insensitively — see this function's doc comment for the
    // per-adapter survey behind that choice and the gap it accepts.
    unique_key
        .iter()
        .filter(|key| !typed_cols.iter().any(|c| c.name.eq_ignore_ascii_case(key)))
        .map(|key| {
            Diagnostic::warning(
                W006,
                model_name,
                format!("unique_key '{key}' is not in the output schema of model '{model_name}'"),
            )
            .with_suggestion(format!(
                "Available columns: {}",
                typed_cols
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
        .collect()
}

/// E024 / W003 — both `@start_date` and `@end_date` placeholders should appear
/// in the model's SQL body. Neither = error; only one = warning.
fn check_time_interval_placeholders(model_name: &str, sql: &str) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let has_start = contains_placeholder(sql, "@start_date");
    let has_end = contains_placeholder(sql, "@end_date");
    match (has_start, has_end) {
        (true, true) => {}
        (false, false) => {
            diags.push(
                Diagnostic::error(
                    E024,
                    model_name,
                    "time_interval model must reference both `@start_date` and `@end_date` in its SQL",
                )
                .with_suggestion(
                    "Add `WHERE <ts_col> >= @start_date AND <ts_col> < @end_date` to the model SQL",
                ),
            );
        }
        (true, false) => {
            diags.push(
                Diagnostic::warning(
                    W003,
                    model_name,
                    "time_interval model references `@start_date` but not `@end_date` — partition window is unbounded above",
                )
                .with_suggestion("Add `AND <ts_col> < @end_date` to bound the upper end of the window"),
            );
        }
        (false, true) => {
            diags.push(
                Diagnostic::warning(
                    W003,
                    model_name,
                    "time_interval model references `@end_date` but not `@start_date` — partition window is unbounded below",
                )
                .with_suggestion("Add `<ts_col> >= @start_date AND` to bound the lower end of the window"),
            );
        }
    }
    diags
}

/// Match a placeholder as a whole token (word boundary on the right side).
/// `@start_date` matches `@start_date AND` but not `@start_date_extra`.
fn contains_placeholder(sql: &str, placeholder: &str) -> bool {
    let bytes = sql.as_bytes();
    let needle = placeholder.as_bytes();
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            // Trailing character must not be an identifier character.
            let trailing = bytes.get(i + needle.len()).copied();
            let is_word_char =
                matches!(trailing, Some(c) if c.is_ascii_alphanumeric() || c == b'_');
            if !is_word_char {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// E026 — `first_partition`, if present, must parse to a canonical key for
/// the model's granularity (e.g. `"2024-01-01"` for `Day`, `"2024"` for `Year`).
fn check_first_partition(
    model_name: &str,
    grain: rocky_ir::TimeGrain,
    first_partition: &str,
) -> Vec<Diagnostic> {
    match rocky_core::incremental::validate_partition_key(grain, first_partition) {
        Ok(()) => Vec::new(),
        Err(e) => vec![
            Diagnostic::error(
                E026,
                model_name,
                format!("first_partition '{first_partition}' is not a valid {grain:?} key: {e}"),
            )
            .with_suggestion(format!(
                "Use the canonical format `{}` for granularity {grain:?}",
                grain.format_str()
            )),
        ],
    }
}

/// W004 — emit one warning per `(model, column, tag)` triple where the
/// classification tag has no matching `[mask]` / `[mask.<env>]` strategy
/// and isn't listed in `[classifications.allow_unmasked]`.
///
/// A tag `T` is considered resolved if it appears either as a top-level
/// `[mask]` default ([`rocky_core::config::MaskEntry::Strategy`]) OR as a
/// key inside any `[mask.<env>]` override table
/// ([`rocky_core::config::MaskEntry::EnvOverride`]). This is a compile-time
/// completeness check — it doesn't gate on `--env`, so a tag defined only
/// under `[mask.prod]` is still considered resolved in dev.
///
/// Iterates models in the project order the caller passed in, then each
/// model's `classification` map in `BTreeMap` order; the resulting
/// diagnostic sequence is therefore deterministic.
pub fn check_classification_tags(
    models: &[rocky_core::models::Model],
    mask: &std::collections::BTreeMap<String, rocky_core::config::MaskEntry>,
    allow_unmasked: &[String],
) -> Vec<Diagnostic> {
    use rocky_core::config::MaskEntry;

    // Pre-compute the set of resolved tags once: any top-level Strategy key
    // plus every key present in any EnvOverride map. Per-call cost is tiny
    // (configs typically carry single-digit tags) but this keeps the inner
    // loop a pure hash lookup.
    let mut resolved: HashSet<&str> = HashSet::new();
    for (name, entry) in mask {
        match entry {
            MaskEntry::Strategy(_) => {
                resolved.insert(name.as_str());
            }
            MaskEntry::EnvOverride(inner) => {
                for k in inner.keys() {
                    resolved.insert(k.as_str());
                }
            }
        }
    }

    let allow: HashSet<&str> = allow_unmasked.iter().map(String::as_str).collect();

    let mut diagnostics = Vec::new();
    for model in models {
        for (column, tag) in &model.config.classification {
            if resolved.contains(tag.as_str()) || allow.contains(tag.as_str()) {
                continue;
            }
            let message = format!(
                "classification tag '{tag}' on column '{column}' has no matching `[mask]` strategy"
            );
            let suggestion = format!(
                "add `[mask.{tag}]` to rocky.toml or list `{tag}` in `[classifications.allow_unmasked]`"
            );
            diagnostics.push(
                Diagnostic::warning(W004, &model.config.name, message).with_suggestion(suggestion),
            );
        }
    }
    diagnostics
}

/// W005 — emit one warning per model that has at least one temporal
/// output column (DATE / TIMESTAMP / TIMESTAMP_NTZ) but no `freshness`
/// declaration in scope.
///
/// "In scope" means either:
/// - the model's sidecar `[freshness]` is set, or
/// - the project-level `[freshness]` carries an `expected_lag_seconds`.
///
/// The latter is supplied via `project_freshness_default` — a `true`
/// value suppresses W005 globally because every model inherits the
/// project-level TTL.
///
/// This is a soft hint, not a structural error. Pure-metadata checks
/// like "model has a date column you might want to assert freshness on"
/// belong in the warning channel.
///
/// The diagnostic's first suggested column (used by the LSP arm to
/// pre-fill an AI prompt) is the first temporal column in `typed`'s
/// natural order. The compiler does not need to pick the "best" one —
/// the AI arm asks the LLM to choose against the full model context.
pub fn check_freshness_coverage(
    models: &[rocky_core::models::Model],
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    project_freshness_default: bool,
) -> Vec<Diagnostic> {
    if project_freshness_default {
        // Project-level default covers every model — no W005s.
        return Vec::new();
    }

    let mut diagnostics = Vec::new();
    for model in models {
        if model.config.freshness.is_some() {
            continue;
        }
        let Some(cols) = typed_models.get(&model.config.name) else {
            continue;
        };
        let temporal_columns: Vec<&str> = cols
            .iter()
            .filter(|c| c.data_type.is_temporal())
            .map(|c| c.name.as_str())
            .collect();
        if temporal_columns.is_empty() {
            continue;
        }

        // Stable, single message — the LSP AI arm parses out the column
        // list from the message text, so keep the shape pinned.
        let columns_joined = temporal_columns.join(", ");
        let message = format!(
            "model '{}' has temporal column(s) ({}) but no `freshness` block declared",
            model.config.name, columns_joined,
        );
        let suggestion = format!(
            "add a `[freshness]` block to the model sidecar, e.g. \
             `[freshness] expected_lag_seconds = 3600, time_column = \"{}\"`",
            temporal_columns[0],
        );
        diagnostics.push(
            Diagnostic::warning(W005, &model.config.name, message).with_suggestion(suggestion),
        );
    }
    diagnostics
}

/// E035 — reject managed-Iceberg `format_options` that the Databricks
/// warehouse rejects at execution, so `rocky compile` surfaces a clear
/// diagnostic before any warehouse call.
///
/// Delegates the constraint logic to
/// [`rocky_ir::lakehouse::validate_managed_iceberg_options`] — the same
/// function the DDL generator guards with — so the compile-time and run-time
/// checks can never drift. Emits one error diagnostic per violation; each
/// message names the offending option(s). Models without a lakehouse `format`
/// (or without `format_options`) are skipped.
pub fn check_lakehouse_format_options(models: &[rocky_core::models::Model]) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for model in models {
        let Some(format) = &model.config.format else {
            continue;
        };
        let Some(options) = &model.config.format_options else {
            continue;
        };
        for violation in rocky_ir::lakehouse::validate_managed_iceberg_options(format, options) {
            diagnostics.push(
                Diagnostic::error(E035, &model.config.name, violation.message)
                    .with_suggestion(violation.suggestion),
            );
        }
    }
    diagnostics
}

/// Derive parallel execution layers from the semantic graph.
///
/// Falls back to a single layer containing every model in topological order
/// if dependency resolution fails (e.g., upstream references to external
/// sources that aren't models in the graph). The fallback yields the same
/// behavior as the prior serial loop — correct, just not parallelized.
fn derive_execution_layers(graph: &SemanticGraph) -> Vec<Vec<String>> {
    let model_set: HashSet<&str> = graph
        .models
        .keys()
        .map(std::string::String::as_str)
        .collect();
    let nodes: Vec<DagNode> = graph
        .models
        .iter()
        .map(|(name, schema)| DagNode {
            name: name.clone(),
            depends_on: schema
                .upstream
                .iter()
                .filter(|u| model_set.contains(u.as_str()))
                .cloned()
                .collect(),
        })
        .collect();

    dag::execution_layers(&nodes).unwrap_or_else(|_| vec![graph.models.keys().cloned().collect()])
}

/// Merge a per-model `ReferenceMap` into the project-wide one.
fn merge_reference_map(into: &mut ReferenceMap, other: ReferenceMap) {
    for (k, v) in other.model_refs {
        into.model_refs.entry(k).or_default().extend(v);
    }
    for (k, v) in other.column_refs {
        into.column_refs.entry(k).or_default().extend(v);
    }
    for (k, v) in other.model_defs {
        into.model_defs.insert(k, v);
    }
}

/// Scan a model's SQL and record model/column reference locations.
///
/// Returns a per-model `ReferenceMap` so callers can merge multiple in
/// parallel without contention. Empty if SQL parsing fails.
fn collect_references(sql: &str, file_path: &str, model_names: &HashSet<String>) -> ReferenceMap {
    let mut ref_map = ReferenceMap::default();
    let dialect = rocky_sql::dialect::DatabricksDialect;
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return ref_map,
    };

    for stmt in &stmts {
        if let Statement::Query(query) = stmt {
            collect_refs_from_query(query, file_path, model_names, &mut ref_map);
        }
    }
    ref_map
}

fn collect_refs_from_query(
    query: &ast::Query,
    file_path: &str,
    model_names: &HashSet<String>,
    ref_map: &mut ReferenceMap,
) {
    // Handle CTEs
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_refs_from_query(&cte.query, file_path, model_names, ref_map);
        }
    }

    if let SetExpr::Select(select) = query.body.as_ref() {
        // Collect table references from FROM/JOIN
        for table in &select.from {
            collect_refs_from_table_factor(&table.relation, file_path, model_names, ref_map);
            for join in &table.joins {
                collect_refs_from_table_factor(&join.relation, file_path, model_names, ref_map);
            }
        }

        // Collect column references from SELECT items
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    collect_refs_from_expr(expr, file_path, ref_map);
                }
                _ => {}
            }
        }

        // Collect from WHERE
        if let Some(ref selection) = select.selection {
            collect_refs_from_expr(selection, file_path, ref_map);
        }

        // Collect from GROUP BY
        if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                collect_refs_from_expr(expr, file_path, ref_map);
            }
        }

        // Collect from HAVING
        if let Some(ref having) = select.having {
            collect_refs_from_expr(having, file_path, ref_map);
        }

        // Collect from ORDER BY
        if let Some(ref order_by) = query.order_by
            && let ast::OrderByKind::Expressions(ref exprs) = order_by.kind
        {
            for order in exprs {
                collect_refs_from_expr(&order.expr, file_path, ref_map);
            }
        }
    }
}

fn collect_refs_from_table_factor(
    factor: &TableFactor,
    file_path: &str,
    model_names: &HashSet<String>,
    ref_map: &mut ReferenceMap,
) {
    if let TableFactor::Table { name, .. } = factor {
        let table_name = name.to_string();
        // Only track model refs (bare names matching known models)
        if model_names.contains(&table_name) {
            // Use the span from sqlparser if available, otherwise approximate
            let (line, col) = if let Some(first_part) = name.0.first() {
                if let Some(ident) = first_part.as_ident() {
                    (
                        ident.span.start.line as usize,
                        ident.span.start.column as usize,
                    )
                } else {
                    (1, 0)
                }
            } else {
                (1, 0)
            };

            ref_map
                .model_refs
                .entry(table_name.clone())
                .or_default()
                .push(RefLocation {
                    file: PathBuf::from(file_path),
                    line,
                    col,
                    end_col: col + table_name.len(),
                });
        }
    }
}

fn collect_refs_from_expr(expr: &Expr, file_path: &str, ref_map: &mut ReferenceMap) {
    match expr {
        Expr::Identifier(ident) => {
            let loc = RefLocation {
                file: PathBuf::from(file_path),
                line: ident.span.start.line as usize,
                col: ident.span.start.column as usize,
                end_col: ident.span.start.column as usize + ident.value.len(),
            };
            // Store as unqualified column reference (model="", column=name)
            ref_map
                .column_refs
                .entry((String::new(), ident.value.clone()))
                .or_default()
                .push(loc);
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = &parts[parts.len() - 2].value;
            let col_ident = &parts[parts.len() - 1];
            let loc = RefLocation {
                file: PathBuf::from(file_path),
                line: col_ident.span.start.line as usize,
                col: col_ident.span.start.column as usize,
                end_col: col_ident.span.start.column as usize + col_ident.value.len(),
            };
            ref_map
                .column_refs
                .entry((table.clone(), col_ident.value.clone()))
                .or_default()
                .push(loc);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_refs_from_expr(left, file_path, ref_map);
            collect_refs_from_expr(right, file_path, ref_map);
        }
        Expr::UnaryOp { expr: inner, .. } => {
            collect_refs_from_expr(inner, file_path, ref_map);
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner) => {
            collect_refs_from_expr(inner, file_path, ref_map);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            collect_refs_from_expr(inner, file_path, ref_map);
            collect_refs_from_expr(low, file_path, ref_map);
            collect_refs_from_expr(high, file_path, ref_map);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_refs_from_expr(op, file_path, ref_map);
            }
            for case_when in conditions {
                collect_refs_from_expr(&case_when.condition, file_path, ref_map);
                collect_refs_from_expr(&case_when.result, file_path, ref_map);
            }
            if let Some(el) = else_result {
                collect_refs_from_expr(el, file_path, ref_map);
            }
        }
        Expr::Function(f) => {
            if let ast::FunctionArguments::List(arg_list) = &f.args {
                for arg in &arg_list.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                        collect_refs_from_expr(e, file_path, ref_map);
                    }
                }
            }
        }
        Expr::Nested(inner) => {
            collect_refs_from_expr(inner, file_path, ref_map);
        }
        Expr::Cast { expr: inner, .. } => {
            collect_refs_from_expr(inner, file_path, ref_map);
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            collect_refs_from_expr(inner, file_path, ref_map);
            for e in list {
                collect_refs_from_expr(e, file_path, ref_map);
            }
        }
        _ => {}
    }
}

/// Infer the result type of an aggregate function.
fn infer_aggregation_type(func: &str, input_type: &RockyType) -> (RockyType, bool) {
    match func {
        "COUNT" => (RockyType::Int64, false), // COUNT is never null
        "SUM" => {
            // SUM preserves numeric type, nullable (empty group → NULL)
            let ty = if input_type.is_integer() {
                RockyType::Int64
            } else {
                input_type.clone()
            };
            (ty, true)
        }
        "AVG" => {
            // `AVG` of an exact-numeric input is dialect-dependent and this layer
            // has no adapter context: DuckDB returns DOUBLE, Databricks returns
            // DECIMAL(p + 4, s + 4), BigQuery preserves NUMERIC/BIGNUMERIC. Only
            // `Decimal` degrades to `Unknown` (#1238) — every other input keeps
            // `Float64`, so an argument Rocky simply failed to resolve does not
            // silently drop out of contract validation.
            let ty = if matches!(input_type, RockyType::Decimal { .. }) {
                RockyType::Unknown
            } else {
                RockyType::Float64
            };
            (ty, true)
        }
        "MIN" | "MAX" => (input_type.clone(), true),
        "COUNT_DISTINCT" => (RockyType::Int64, false),
        _ => (RockyType::Unknown, true),
    }
}

/// Refine explicit casts by parsing their target types from the model SQL.
///
/// Only columns that lineage left `Unknown` **and** that are produced by a cast
/// edge — infallible [`Cast`] or fallible [`TryCast`] — whose input type is
/// known are refined, substituting the parsed cast target (so `CAST(id AS
/// STRING) AS id` resolves to `String`, not the source's pre-cast type — see
/// #1145). This refines only the *type*; the nullable bit was already set in
/// Step 1 (a `TryCast` output is nullable regardless of input — #1148) and is
/// left untouched here.
///
/// [`Cast`]: rocky_sql::lineage::TransformKind::Cast
/// [`TryCast`]: rocky_sql::lineage::TransformKind::TryCast
///
/// Two deliberate restrictions, both erring toward `Unknown` — the safe
/// "cannot type-check" state, where a contract on the column is skipped rather
/// than validated against a fabricated type:
///
/// - **No alias-based fallback.** A remaining `Unknown` column is not resolved
///   by matching its output name against an upstream column. That lookup was
///   unsound: an expression aliased back to a source name (e.g.
///   `amount_cents / 100 AS amount`) would inherit the source type even though
///   the expression can change it. Such columns are left `Unknown`.
/// - **Unknown input stays Unknown.** The cast target is knowable from the SQL
///   alone, but the target is only applied when the cast's *input* column
///   resolves to a known type. This avoids fabricating a type for a cast over a
///   missing or unresolved column, whose input reads as `Unknown` for the same
///   reason a broken reference would.
fn enhanced_inference(
    model_name: &str,
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    col_index: &HashMap<String, HashMap<String, usize>>,
    inferred_cols: Option<&[TypedColumn]>,
    typed_cols: &mut [TypedColumn],
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for col in typed_cols.iter_mut() {
        if col.data_type != RockyType::Unknown {
            continue; // Already has a type from lineage
        }

        let Some(edge) = graph.producing_edge(model_name, &col.name) else {
            continue;
        };
        if !edge.transform.is_cast() {
            continue;
        }

        let input_is_known = typed_models
            .get(&*edge.source.model)
            .and_then(|columns| {
                col_index
                    .get(&*edge.source.model)
                    .and_then(|index| index.get(&*edge.source.column))
                    .map(|&index| &columns[index])
            })
            .is_some_and(|input| input.data_type != RockyType::Unknown);
        if !input_is_known {
            continue;
        }

        if let Some(inferred) = inferred_cols
            .and_then(|columns| columns.iter().find(|inferred| inferred.name == col.name))
            .filter(|inferred| inferred.data_type != RockyType::Unknown)
        {
            col.data_type = inferred.data_type.clone();
        }
    }

    // Check for columns that remain Unknown and could indicate issues
    let unknown_count = typed_cols
        .iter()
        .filter(|c| c.data_type == RockyType::Unknown)
        .count();
    if unknown_count > 0 && unknown_count < typed_cols.len() {
        // Only warn if some columns are typed but others aren't (partial inference)
        diagnostics.push(Diagnostic::info(
            I002,
            model_name,
            format!(
                "{unknown_count} column(s) have unknown types — provide source schemas for full type checking"
            ),
        ));
    }

    diagnostics
}

/// Infer the type of an sqlparser expression given a type scope.
///
/// This is the core expression-level type inference function.
/// Used by both the enhanced inference pass and for ad-hoc type checking.
fn infer_expr_type(expr: &Expr, scope: &TypeScope) -> (RockyType, bool) {
    match expr {
        // Column reference
        Expr::Identifier(ident) => scope.lookup(&ident.value),

        // Qualified column: table.column
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let table = &parts[parts.len() - 2].value;
            let col = &parts[parts.len() - 1].value;
            scope.lookup_qualified(table, col)
        }

        // Literals
        Expr::Value(val) => match &val.value {
            ast::Value::Number(_, _) => (RockyType::Int64, false),
            ast::Value::SingleQuotedString(_) | ast::Value::DoubleQuotedString(_) => {
                (RockyType::String, false)
            }
            ast::Value::Boolean(_) => (RockyType::Boolean, false),
            ast::Value::Null => (RockyType::Unknown, true),
            _ => (RockyType::Unknown, false),
        },

        // CAST(expr AS type)
        Expr::Cast {
            expr,
            data_type,
            kind,
            ..
        } => {
            let nullable = match kind {
                ast::CastKind::Cast | ast::CastKind::DoubleColon => infer_expr_type(expr, scope).1,
                ast::CastKind::TryCast | ast::CastKind::SafeCast => true,
            };
            (sql_type_to_rocky(data_type), nullable)
        }

        // Binary operations
        Expr::BinaryOp { left, op, right } => infer_binary_op_type(left, op, right, scope),

        // Unary operations
        Expr::UnaryOp { op, expr } => {
            let (inner_type, nullable) = infer_expr_type(expr, scope);
            match op {
                ast::UnaryOperator::Not => (RockyType::Boolean, nullable),
                ast::UnaryOperator::Minus | ast::UnaryOperator::Plus => (inner_type, nullable),
                _ => (RockyType::Unknown, nullable),
            }
        }

        // Function calls (including window functions with OVER clause)
        Expr::Function(func) => infer_function_type(func, scope),

        // CASE WHEN ... THEN ... ELSE ... END
        Expr::Case {
            conditions,
            else_result,
            ..
        } => infer_case_type(conditions, else_result, scope),

        // IS NULL / IS NOT NULL → Boolean, non-nullable
        Expr::IsNull(_) | Expr::IsNotNull(_) => (RockyType::Boolean, false),

        // IN list / subquery → Boolean
        Expr::InList { .. } | Expr::InSubquery { .. } => (RockyType::Boolean, true),

        // EXISTS → Boolean
        Expr::Exists { .. } => (RockyType::Boolean, false),

        // BETWEEN → Boolean
        Expr::Between { .. } => (RockyType::Boolean, true),

        // Subquery → Unknown (would need recursive analysis)
        Expr::Subquery(_) => (RockyType::Unknown, true),

        // Nested expression
        Expr::Nested(inner) => infer_expr_type(inner, scope),

        _ => (RockyType::Unknown, true),
    }
}

/// Infer the result type of a binary operator expression.
///
/// Handles comparison, boolean logic, arithmetic (with numeric promotion),
/// and string concatenation operators.
fn infer_binary_op_type(
    left: &Expr,
    op: &ast::BinaryOperator,
    right: &Expr,
    scope: &TypeScope,
) -> (RockyType, bool) {
    let (left_type, left_null) = infer_expr_type(left, scope);
    let (right_type, right_null) = infer_expr_type(right, scope);
    let nullable = left_null || right_null;

    match op {
        // Comparison → Boolean
        ast::BinaryOperator::Eq
        | ast::BinaryOperator::NotEq
        | ast::BinaryOperator::Lt
        | ast::BinaryOperator::LtEq
        | ast::BinaryOperator::Gt
        | ast::BinaryOperator::GtEq => (RockyType::Boolean, nullable),

        // Boolean logic
        ast::BinaryOperator::And | ast::BinaryOperator::Or => (RockyType::Boolean, nullable),

        // Arithmetic → numeric promotion
        ast::BinaryOperator::Plus
        | ast::BinaryOperator::Minus
        | ast::BinaryOperator::Multiply
        | ast::BinaryOperator::Divide
        | ast::BinaryOperator::Modulo => {
            let result_type = crate::types::common_supertype(&left_type, &right_type)
                .unwrap_or(RockyType::Unknown);
            (result_type, nullable)
        }

        // String concatenation
        ast::BinaryOperator::StringConcat => (RockyType::String, nullable),

        _ => (RockyType::Unknown, nullable),
    }
}

/// Infer the result type of a SQL function call.
///
/// Covers aggregate functions (COUNT, SUM, AVG, MIN, MAX, COALESCE),
/// ranking and distribution window functions, value and offset window
/// functions, string functions, numeric functions, date/time functions,
/// and conditional functions (IF, NULLIF, GREATEST, LEAST).
fn infer_function_type(func: &ast::Function, scope: &TypeScope) -> (RockyType, bool) {
    let name = func.name.to_string().to_uppercase();

    // Validate OVER clause columns if present
    if let Some(ast::WindowType::WindowSpec(ref spec)) = func.over {
        validate_window_columns(spec, scope);
    }

    match name.as_str() {
        // Aggregate functions (work with or without OVER)
        "COUNT" => (RockyType::Int64, false),
        "SUM" => {
            let arg_type = first_arg_type(func, scope);
            let result = if arg_type.is_integer() {
                RockyType::Int64
            } else {
                arg_type
            };
            (result, true)
        }
        "AVG" => {
            let arg_type = first_arg_type(func, scope);
            infer_aggregation_type("AVG", &arg_type)
        }
        "MIN" | "MAX" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true)
        }
        "COALESCE" => {
            let arg_types = all_arg_types(func, scope);
            let result_type = arg_types
                .iter()
                .map(|(t, _)| t)
                .try_fold(RockyType::Unknown, |acc, t| {
                    crate::types::common_supertype(&acc, t)
                })
                .unwrap_or(RockyType::Unknown);
            let all_nullable = arg_types.iter().all(|(_, n)| *n);
            (result_type, all_nullable)
        }

        // Ranking window functions → always Int64
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => (RockyType::Int64, false),

        // Distribution window functions → always Float64
        "PERCENT_RANK" | "CUME_DIST" => (RockyType::Float64, false),

        // Value window functions → same type as first arg
        "FIRST_VALUE" | "LAST_VALUE" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true)
        }
        "NTH_VALUE" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true) // nullable — row may not exist
        }

        // Offset window functions → same type as first arg, nullable
        "LAG" | "LEAD" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true)
        }

        // String functions
        "CONCAT" | "CONCAT_WS" | "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
        | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REVERSE" | "INITCAP" => {
            (RockyType::String, true)
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" => (RockyType::Int64, true),
        "POSITION" | "STRPOS" | "INSTR" => (RockyType::Int64, true),

        // Numeric functions
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE" | "TRUNC" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true)
        }
        "SIGN" => (RockyType::Int32, true),
        "POWER" | "POW" | "SQRT" | "LOG" | "LOG2" | "LOG10" | "LN" | "EXP" | "SIN" | "COS"
        | "TAN" => (RockyType::Float64, true),

        // Date/time functions
        "NOW" | "CURRENT_TIMESTAMP" => (RockyType::Timestamp, false),
        "CURRENT_DATE" | "TODAY" => (RockyType::Date, false),
        "DATE" | "TO_DATE" => (RockyType::Date, true),
        "TIMESTAMP" | "TO_TIMESTAMP" => (RockyType::Timestamp, true),
        "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "DAYOFWEEK" | "DAYOFYEAR"
        | "WEEKOFYEAR" | "QUARTER" => (RockyType::Int32, true),
        "DATE_TRUNC" | "DATE_ADD" | "DATE_SUB" | "DATEADD" | "DATESUB" => {
            (RockyType::Timestamp, true)
        }
        "DATEDIFF" | "TIMESTAMPDIFF" | "MONTHS_BETWEEN" => (RockyType::Int64, true),

        // Conditional
        "IF" | "IFF" => {
            let arg_types = all_arg_types(func, scope);
            if arg_types.len() >= 2 {
                (arg_types[1].0.clone(), true)
            } else {
                (RockyType::Unknown, true)
            }
        }
        "NULLIF" => {
            let arg_type = first_arg_type(func, scope);
            (arg_type, true) // always nullable
        }
        "GREATEST" | "LEAST" => {
            let arg_types = all_arg_types(func, scope);
            let result_type = arg_types
                .iter()
                .map(|(t, _)| t)
                .try_fold(RockyType::Unknown, |acc, t| {
                    crate::types::common_supertype(&acc, t)
                })
                .unwrap_or(RockyType::Unknown);
            (result_type, true)
        }

        "CAST" => (RockyType::Unknown, true), // handled by Expr::Cast above
        _ => (RockyType::Unknown, true),
    }
}

/// Infer the result type of a CASE WHEN expression.
///
/// Folds the common supertype across all THEN branches and the optional
/// ELSE branch. The result is nullable when any branch is nullable or
/// when no ELSE clause is present.
fn infer_case_type(
    conditions: &[ast::CaseWhen],
    else_result: &Option<Box<Expr>>,
    scope: &TypeScope,
) -> (RockyType, bool) {
    let mut result_type = RockyType::Unknown;
    let mut nullable = else_result.is_none(); // No ELSE → nullable

    for case_when in conditions {
        let (t, n) = infer_expr_type(&case_when.result, scope);
        result_type =
            crate::types::common_supertype(&result_type, &t).unwrap_or(RockyType::Unknown);
        nullable = nullable || n;
    }

    if let Some(else_expr) = else_result {
        let (t, n) = infer_expr_type(else_expr, scope);
        result_type =
            crate::types::common_supertype(&result_type, &t).unwrap_or(RockyType::Unknown);
        nullable = nullable || n;
    }

    (result_type, nullable)
}

/// Convert an sqlparser DataType to RockyType.
fn sql_type_to_rocky(dt: &ast::DataType) -> RockyType {
    match dt {
        ast::DataType::Boolean => RockyType::Boolean,
        ast::DataType::TinyInt(_)
        | ast::DataType::SmallInt(_)
        | ast::DataType::Int(_)
        | ast::DataType::Integer(_)
        | ast::DataType::MediumInt(_) => RockyType::Int32,
        ast::DataType::BigInt(_) => RockyType::Int64,
        ast::DataType::Float(_) | ast::DataType::Real => RockyType::Float32,
        ast::DataType::Double(_) | ast::DataType::DoublePrecision => RockyType::Float64,
        ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => match info {
            ast::ExactNumberInfo::PrecisionAndScale(p, s) => RockyType::Decimal {
                precision: *p as u8,
                scale: *s as u8,
            },
            ast::ExactNumberInfo::Precision(p) => RockyType::Decimal {
                precision: *p as u8,
                scale: 0,
            },
            ast::ExactNumberInfo::None => RockyType::Decimal {
                precision: 38,
                scale: 0,
            },
        },
        ast::DataType::Varchar(_)
        | ast::DataType::Char(_)
        | ast::DataType::Text
        | ast::DataType::String(_) => RockyType::String,
        ast::DataType::Binary(_) | ast::DataType::Varbinary(_) | ast::DataType::Blob(_) => {
            RockyType::Binary
        }
        ast::DataType::Date => RockyType::Date,
        ast::DataType::Timestamp(_, _) => RockyType::Timestamp,
        _ => {
            // Try the string-based mapper as fallback
            let type_str = format!("{dt}");
            default_type_mapper(&type_str)
        }
    }
}

/// Get the type of the first argument to a function.
fn first_arg_type(func: &ast::Function, scope: &TypeScope) -> RockyType {
    match &func.args {
        ast::FunctionArguments::List(arg_list) => {
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) =
                arg_list.args.first()
            {
                infer_expr_type(expr, scope).0
            } else {
                RockyType::Unknown
            }
        }
        _ => RockyType::Unknown,
    }
}

/// Get all argument types for a function.
fn all_arg_types(func: &ast::Function, scope: &TypeScope) -> Vec<(RockyType, bool)> {
    match &func.args {
        ast::FunctionArguments::List(arg_list) => arg_list
            .args
            .iter()
            .filter_map(|arg| {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    Some(infer_expr_type(expr, scope))
                } else {
                    None
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Validate that PARTITION BY and ORDER BY columns in a window spec exist in scope.
///
/// Returns resolved column types for the partition/order expressions.
/// Does not emit diagnostics directly — this is a best-effort validation
/// that ensures window columns resolve to known types.
fn validate_window_columns(spec: &ast::WindowSpec, scope: &TypeScope) -> Vec<(String, RockyType)> {
    let mut resolved = Vec::new();

    for expr in &spec.partition_by {
        let (ty, _) = infer_expr_type(expr, scope);
        let name = match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| &p.value)
                .cloned()
                .collect::<Vec<_>>()
                .join("."),
            _ => format!("{expr}"),
        };
        resolved.push((name, ty));
    }

    for order_expr in &spec.order_by {
        let (ty, _) = infer_expr_type(&order_expr.expr, scope);
        let name = match &order_expr.expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| &p.value)
                .cloned()
                .collect::<Vec<_>>()
                .join("."),
            _ => format!("{}", order_expr.expr),
        };
        resolved.push((name, ty));
    }

    resolved
}

/// Parse SQL and infer types for SELECT items using expression-level inference.
///
/// This is called by the compile pipeline to type-check model SQL directly.
pub fn infer_select_types(
    sql: &str,
    scope: &HashMap<String, Vec<TypedColumn>>,
    _model_name: &str,
) -> Result<Vec<TypedColumn>, String> {
    infer_select_types_with_lookup(sql, &|name| {
        scope
            .get(name)
            .or_else(|| scope.get(name.rsplit('.').next().unwrap_or(name)))
            .map(Vec::as_slice)
    })
    .map(|inferred| inferred.columns)
}

#[derive(Default)]
struct SelectInference {
    columns: Vec<TypedColumn>,
    // Projection indexes keep metadata aligned with duplicate wildcard names.
    reference_outputs: HashSet<usize>,
}

impl SelectInference {
    fn push_expression(&mut self, name: String, expr: &Expr, scope: &TypeScope) {
        if is_column_reference(expr) {
            self.reference_outputs.insert(self.columns.len());
        }
        let (data_type, nullable) = infer_expr_type(expr, scope);
        self.columns.push(TypedColumn {
            name,
            data_type,
            nullable,
        });
    }
}

fn is_column_reference(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Nested(expr)
        | Expr::Cast {
            expr,
            kind: ast::CastKind::Cast | ast::CastKind::DoubleColon,
            ..
        } => is_column_reference(expr),
        _ => false,
    }
}

fn infer_select_types_with_lookup<'a>(
    sql: &str,
    lookup: &dyn Fn(&str) -> Option<&'a [TypedColumn]>,
) -> Result<SelectInference, String> {
    let dialect = rocky_sql::dialect::DatabricksDialect;
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let Statement::Query(query) = stmts.first().ok_or("empty SQL")? else {
        return Err("only SELECT statements supported".to_string());
    };
    infer_query_types(query, lookup)
}

fn infer_query_types<'a>(
    query: &ast::Query,
    lookup: &dyn Fn(&str) -> Option<&'a [TypedColumn]>,
) -> Result<SelectInference, String> {
    let mut ctes: HashMap<String, Vec<TypedColumn>> = HashMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let mut columns = infer_query_types(&cte.query, &|name| {
                ctes.get(name).map(Vec::as_slice).or_else(|| lookup(name))
            })
            .unwrap_or_default()
            .columns;
            rename_relation_columns(&mut columns, &cte.alias);
            ctes.insert(cte.alias.name.value.clone(), columns);
        }
    }
    let lookup = |name: &str| ctes.get(name).map(Vec::as_slice).or_else(|| lookup(name));
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        SetExpr::Query(query) => return infer_query_types(query, &lookup),
        _ => return Err("unsupported query form".to_string()),
    };
    let mut from_scope = JoinScope::default();
    for from in &select.from {
        let joined = infer_join_relations(from, &lookup);
        from_scope.relations.extend(joined.relations);
        from_scope.columns.extend(joined.columns);
    }
    let mut type_scope = TypeScope::new();
    for col in &from_scope.columns {
        type_scope
            .columns
            .entry(CiKey::owned(col.name.clone()))
            .and_modify(|ty| *ty = (RockyType::Unknown, true))
            .or_insert_with(|| (col.data_type.clone(), col.nullable));
    }
    for relation in &from_scope.relations {
        for col in &relation.columns {
            type_scope
                .qualified
                .entry(CiKey::owned(relation.qualifier.clone()))
                .or_default()
                .insert(
                    CiKey::owned(col.name.clone()),
                    (col.data_type.clone(), col.nullable),
                );
        }
    }

    let mut inferred = SelectInference::default();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                inferred.push_expression(expr_name(expr), expr, &type_scope);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                inferred.push_expression(alias.value.clone(), expr, &type_scope);
            }
            // Spark SQL `SELECT expr AS (a, b, c)` — multi-alias binding.
            // Emit one typed column per alias with the same inferred type
            // (we don't model struct destructuring, so each alias shares the
            // expression's inferred type — same conservative shape as
            // `ExprWithAlias` above).
            SelectItem::ExprWithAliases { expr, aliases } => {
                for alias in aliases {
                    inferred.push_expression(alias.value.clone(), expr, &type_scope);
                }
            }
            SelectItem::Wildcard(_) => {
                inferred.columns.extend(from_scope.columns.iter().cloned());
            }
            SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) => {
                let Some(name) = name.0.last().and_then(ast::ObjectNamePart::as_ident) else {
                    continue;
                };
                for relation in &from_scope.relations {
                    if relation.qualifier.eq_ignore_ascii_case(&name.value) {
                        inferred.columns.extend(relation.columns.iter().cloned());
                    }
                }
            }
            SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::Expr(_), _) => {}
        }
    }

    Ok(inferred)
}

struct RelationColumns {
    qualifier: String,
    columns: Vec<TypedColumn>,
}

#[derive(Default)]
struct JoinScope {
    // Qualified references retain each side's columns. USING/NATURAL keys
    // merge only in the unqualified output used by SELECT * and bare names.
    relations: Vec<RelationColumns>,
    columns: Vec<TypedColumn>,
}

fn infer_join_relations<'a>(
    from: &ast::TableWithJoins,
    lookup: &dyn Fn(&str) -> Option<&'a [TypedColumn]>,
) -> JoinScope {
    use ast::JoinOperator;

    let mut left = infer_relation_columns(&from.relation, lookup);
    for join in &from.joins {
        let mut right = infer_relation_columns(&join.relation, lookup);
        let merged = merged_join_columns(&left.columns, &right.columns, &join.join_operator);
        if matches!(
            join.join_operator,
            JoinOperator::Right(_) | JoinOperator::RightOuter(_) | JoinOperator::FullOuter(_)
        ) {
            null_extend_scope(&mut left);
        }
        if matches!(
            join.join_operator,
            JoinOperator::Left(_)
                | JoinOperator::LeftOuter(_)
                | JoinOperator::FullOuter(_)
                | JoinOperator::OuterApply
        ) {
            null_extend_scope(&mut right);
        }
        for col in &mut left.columns {
            if let Some(key) = merged.get(CiStr::new(&col.name)) {
                *col = key.clone();
            }
        }
        left.columns.extend(
            right
                .columns
                .into_iter()
                .filter(|col| !merged.contains_key(CiStr::new(&col.name))),
        );
        left.relations.extend(right.relations);
    }
    left
}

fn merged_join_columns(
    left: &[TypedColumn],
    right: &[TypedColumn],
    operator: &ast::JoinOperator,
) -> HashMap<CiKey<'static>, TypedColumn> {
    use ast::{JoinConstraint, JoinOperator};

    let constraint = match operator {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => c,
        _ => return HashMap::new(),
    };
    if !matches!(
        constraint,
        JoinConstraint::Using(_) | JoinConstraint::Natural
    ) {
        return HashMap::new();
    }
    let right: HashMap<_, _> = right
        .iter()
        .map(|col| (CiKey::owned(col.name.clone()), col))
        .collect();
    let using: HashSet<_> = match constraint {
        JoinConstraint::Using(names) => names
            .iter()
            .filter_map(|name| name.0.last().and_then(ast::ObjectNamePart::as_ident))
            .map(|name| CiKey::owned(name.value.clone()))
            .collect(),
        _ => right.keys().cloned().collect(),
    };
    left.iter()
        .filter_map(|left| {
            if !using.contains(CiStr::new(&left.name)) {
                return None;
            }
            let right = right.get(CiStr::new(&left.name))?;
            let nullable = match operator {
                JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => left.nullable,
                JoinOperator::Right(_) | JoinOperator::RightOuter(_) => right.nullable,
                // FULL JOIN can emit unmatched rows from either input. The
                // coalesced key is non-null only when BOTH input keys are non-null.
                JoinOperator::FullOuter(_) => left.nullable || right.nullable,
                // An inner join emits only matched rows, and the merged key's
                // equality never matches a NULL, so the key is non-null as soon
                // as either input key is.
                JoinOperator::Join(_) | JoinOperator::Inner(_) => left.nullable && right.nullable,
                // The guard above admits only the operators named here. Treat a
                // variant that ever reaches this arm as nullable, the
                // conservative answer.
                _ => true,
            };
            Some((
                CiKey::owned(left.name.clone()),
                TypedColumn {
                    name: left.name.clone(),
                    data_type: if left.data_type == right.data_type {
                        left.data_type.clone()
                    } else {
                        RockyType::Unknown
                    },
                    nullable,
                },
            ))
        })
        .collect()
}

fn null_extend_scope(scope: &mut JoinScope) {
    for col in &mut scope.columns {
        col.nullable = true;
    }
    for relation in &mut scope.relations {
        for col in &mut relation.columns {
            col.nullable = true;
        }
    }
}

fn infer_relation_columns<'a>(
    factor: &TableFactor,
    lookup: &dyn Fn(&str) -> Option<&'a [TypedColumn]>,
) -> JoinScope {
    let (qualifier, mut columns, alias) = match factor {
        TableFactor::Table { name, alias, .. } => {
            let name = name.to_string();
            let short = name.rsplit('.').next().unwrap_or(&name);
            let columns = lookup(&name).unwrap_or_default().to_vec();
            (short.to_string(), columns, alias)
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => (
            String::new(),
            infer_query_types(subquery, lookup)
                .unwrap_or_default()
                .columns,
            alias,
        ),
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let scope = infer_join_relations(table_with_joins, lookup);
            if alias.is_none() {
                return scope;
            }
            (String::new(), scope.columns, alias)
        }
        _ => return JoinScope::default(),
    };
    let qualifier = if let Some(alias) = alias {
        rename_relation_columns(&mut columns, alias);
        alias.name.value.clone()
    } else {
        qualifier
    };
    JoinScope {
        relations: vec![RelationColumns {
            qualifier,
            columns: columns.clone(),
        }],
        columns,
    }
}

fn rename_relation_columns(columns: &mut [TypedColumn], alias: &ast::TableAlias) {
    for (col, alias) in columns.iter_mut().zip(&alias.columns) {
        col.name.clone_from(&alias.name.value);
    }
}

/// Extract a reasonable name from an expression (for unnamed SELECT items).
fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|p| p.value.clone()).unwrap_or_default()
        }
        Expr::Function(f) => f.name.to_string().to_lowercase(),
        _ => "?column?".to_string(),
    }
}

/// Check join key type compatibility for a model.
///
/// Returns owned diagnostics so this is safe to call from a parallel worker.
fn check_join_keys(
    model_name: &str,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    graph: &SemanticGraph,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    let model_schema = match graph.model_schema(model_name) {
        Some(s) => s,
        None => return diagnostics,
    };

    if model_schema.upstream.len() < 2 {
        return diagnostics;
    }

    let mut columns_by_name: HashMap<String, Vec<(&str, &RockyType)>> = HashMap::new();

    for upstream_name in &model_schema.upstream {
        if let Some(upstream_cols) = typed_models.get(upstream_name.as_str()) {
            for col in upstream_cols {
                columns_by_name
                    .entry(col.name.clone())
                    .or_default()
                    .push((upstream_name.as_str(), &col.data_type));
            }
        }
    }

    for (col_name, sources) in &columns_by_name {
        if sources.len() < 2 {
            continue;
        }

        // Fast path: compare all against the first source's type.
        // When all types match (the overwhelmingly common case), this is O(N)
        // instead of O(N²). Only fall back to pairwise for actual mismatches.
        let &(first_model, first_type) = &sources[0];
        if *first_type == RockyType::Unknown {
            // If the reference type is Unknown, check remaining pairs only
            // among sources that have known types.
            let known: Vec<_> = sources
                .iter()
                .filter(|(_, t)| **t != RockyType::Unknown)
                .collect();
            if known.len() >= 2 {
                // Fall back to pairwise for the known subset (typically tiny)
                for i in 0..known.len() {
                    for j in (i + 1)..known.len() {
                        let (model_a, type_a) = known[i];
                        let (model_b, type_b) = known[j];
                        if type_a != type_b {
                            emit_join_key_diagnostic(
                                &mut diagnostics,
                                model_name,
                                col_name,
                                model_a,
                                type_a,
                                model_b,
                                type_b,
                            );
                        }
                    }
                }
            }
            continue;
        }

        let mut all_same = true;
        for &(other_model, other_type) in &sources[1..] {
            if *other_type == RockyType::Unknown {
                continue;
            }
            if *first_type != *other_type {
                all_same = false;
                emit_join_key_diagnostic(
                    &mut diagnostics,
                    model_name,
                    col_name,
                    first_model,
                    first_type,
                    other_model,
                    other_type,
                );
            }
        }

        // If not all the same, there may be additional mismatches among the
        // non-first sources that the first-vs-rest pass didn't catch (e.g.,
        // sources[1] and sources[2] differ from each other but both differ
        // from sources[0] in different ways). Do pairwise on the rest.
        if !all_same && sources.len() > 2 {
            for i in 1..sources.len() {
                for j in (i + 1)..sources.len() {
                    let (model_a, type_a) = sources[i];
                    let (model_b, type_b) = sources[j];
                    if *type_a == RockyType::Unknown || *type_b == RockyType::Unknown {
                        continue;
                    }
                    if type_a != type_b {
                        emit_join_key_diagnostic(
                            &mut diagnostics,
                            model_name,
                            col_name,
                            model_a,
                            type_a,
                            model_b,
                            type_b,
                        );
                    }
                }
            }
        }
    }

    diagnostics
}

/// Emit a W001 (implicit coercion) or E001 (incompatible) diagnostic for a
/// join key column whose types differ between two upstream models.
fn emit_join_key_diagnostic(
    diagnostics: &mut Vec<Diagnostic>,
    model_name: &str,
    col_name: &str,
    model_a: &str,
    type_a: &RockyType,
    model_b: &str,
    type_b: &RockyType,
) {
    if crate::types::common_supertype(type_a, type_b).is_some() {
        diagnostics.push(
            Diagnostic::warning(
                W001,
                model_name,
                format!(
                    "implicit type coercion on column '{col_name}': \
                     {model_a} has {type_a:?}, {model_b} has {type_b:?}"
                ),
            )
            .with_suggestion("add explicit CAST to match types"),
        );
    } else {
        diagnostics.push(
            Diagnostic::error(
                E001,
                model_name,
                format!(
                    "join key type mismatch on column '{col_name}': \
                     {model_a} has {type_a:?}, {model_b} has {type_b:?}"
                ),
            )
            .with_suggestion(format!(
                "add explicit CAST to convert '{col_name}' to a common type"
            )),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{CompilerContract, ContractColumn, ContractRules, validate_contract};
    use crate::project::Project;
    use crate::semantic::build_semantic_graph;
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

    fn source_schema(cols: &[(&str, RockyType, bool)]) -> Vec<TypedColumn> {
        cols.iter()
            .map(|(name, ty, nullable)| TypedColumn {
                name: name.to_string(),
                data_type: ty.clone(),
                nullable: *nullable,
            })
            .collect()
    }

    #[test]
    fn outer_join_nullability_preserves_relation_identity() {
        let sources = HashMap::from([(
            "raw.users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        )]);
        for (join, expected) in [
            ("LEFT JOIN", [false, true]),
            ("LEFT OUTER JOIN", [false, true]),
            ("RIGHT JOIN", [true, false]),
            ("RIGHT OUTER JOIN", [true, false]),
            ("FULL OUTER JOIN", [true, true]),
            ("INNER JOIN", [false, false]),
        ] {
            let sql = format!(
                "SELECT a.id AS left_id, b.id AS right_id \
                 FROM raw.users a {join} raw.users b ON a.id = b.id"
            );
            let project = Project::from_models(vec![make_model("joined", &sql)]).unwrap();
            let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
            let result =
                typecheck_project_with_models(&graph, &sources, None, &project.models, None);
            let columns = &result.typed_models["joined"];
            for (col, nullable) in columns.iter().zip(expected) {
                assert_eq!(col.data_type, RockyType::Int64, "{sql}");
                assert_eq!(col.nullable, nullable, "{}: {sql}", col.name);
            }
            assert_eq!(columns.len(), 2);
        }
    }

    #[test]
    fn outer_join_scope_handles_chains_nesting_and_stars() {
        let sources = HashMap::from([
            (
                "a".to_string(),
                source_schema(&[("a_id", RockyType::Int64, false)]),
            ),
            (
                "b".to_string(),
                source_schema(&[("b_id", RockyType::Int64, false)]),
            ),
            (
                "c".to_string(),
                source_schema(&[("c_id", RockyType::Int64, false)]),
            ),
        ]);
        for (sql, expected) in [
            (
                "SELECT * FROM a LEFT JOIN b ON true INNER JOIN c ON true",
                vec![false, true, false],
            ),
            (
                "SELECT * FROM a INNER JOIN b ON true RIGHT JOIN c ON true",
                vec![true, true, false],
            ),
            (
                "SELECT * FROM a LEFT JOIN (b INNER JOIN c ON true) ON true",
                vec![false, true, true],
            ),
            (
                "SELECT * FROM (a LEFT JOIN b ON true) RIGHT JOIN c ON true",
                vec![true, true, false],
            ),
            (
                "SELECT * FROM a, b RIGHT JOIN c ON true",
                vec![false, true, false],
            ),
            ("SELECT r.* FROM a l LEFT JOIN b r ON true", vec![true]),
            ("SELECT l.* FROM a l LEFT JOIN b r ON true", vec![false]),
            (
                "SELECT r.* FROM a l LEFT JOIN (SELECT b_id FROM b) r ON true",
                vec![true],
            ),
            (
                "WITH r AS (SELECT b_id FROM b) SELECT r.* FROM a LEFT JOIN r ON true",
                vec![true],
            ),
            (
                "SELECT r.* FROM a LEFT JOIN (b INNER JOIN c ON true) r ON true",
                vec![true, true],
            ),
        ] {
            let columns = infer_select_types(sql, &sources, "joined").unwrap();
            assert_eq!(columns.len(), expected.len(), "{sql}");
            for (col, nullable) in columns.iter().zip(expected) {
                assert_eq!(col.data_type, RockyType::Int64, "{sql}");
                assert_eq!(col.nullable, nullable, "{}: {sql}", col.name);
            }
        }
    }

    #[test]
    fn outer_join_casts_and_null_replacing_expressions() {
        let sources = HashMap::from([(
            "users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        )]);
        let sql = "SELECT CAST(a.id AS BIGINT) AS kept, CAST(b.id AS BIGINT) AS extended, \
                   TRY_CAST(a.id AS BIGINT) AS fallible, COALESCE(b.id, 0) AS fallback, \
                   COUNT(b.id) AS count_id FROM users a LEFT JOIN users b ON a.id = b.id";
        let columns = infer_select_types(sql, &sources, "joined").unwrap();
        assert_eq!(columns.len(), 5);
        for (col, nullable) in columns.iter().zip([false, true, true, false, false]) {
            assert_eq!(col.data_type, RockyType::Int64, "{}", col.name);
            assert_eq!(col.nullable, nullable, "{}", col.name);
        }
        let project = Project::from_models(vec![make_model("joined", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let columns = &result.typed_models["joined"];
        for (col, nullable) in columns[..3].iter().zip([false, true, true]) {
            assert_eq!(col.data_type, RockyType::Int64, "{}", col.name);
            assert_eq!(col.nullable, nullable, "{}", col.name);
        }
        assert_eq!(columns[3].data_type, RockyType::Unknown);
        assert!(
            !columns[4].nullable,
            "COUNT stays non-null even over null-extended input"
        );
        let columns = infer_select_types(
            "SELECT CAST(SUM(id) AS BIGINT) AS total FROM users GROUP BY id",
            &sources,
            "totals",
        )
        .unwrap();
        assert!(
            columns[0].nullable,
            "expression inference remains conservative for casted aggregates"
        );
    }

    #[test]
    fn outer_join_star_nullability_matches_selected_alias() {
        let sources = HashMap::from([(
            "users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        )]);
        let external = HashMap::from([(
            "users".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }],
        )]);
        for (projection, nullable) in [
            ("*", false),
            ("l.*", false),
            ("r.*", true),
            ("\"r\".*", true),
        ] {
            let sql = format!("SELECT {projection} FROM users l LEFT JOIN users r ON l.id = r.id");
            let project = Project::from_models(vec![make_model("joined", &sql)]).unwrap();
            let graph = build_semantic_graph(&project, &external).unwrap();
            let result =
                typecheck_project_with_models(&graph, &sources, None, &project.models, None);
            let columns = &result.typed_models["joined"];
            assert_eq!(
                columns.len(),
                1,
                "semantic graph keeps the first duplicate name"
            );
            assert_eq!(columns[0].data_type, RockyType::Int64, "{sql}");
            assert_eq!(columns[0].nullable, nullable, "{sql}");
        }
    }

    #[test]
    fn outer_join_using_and_natural_keys_keep_merged_nullability() {
        for right_nullable in [false, true] {
            let sources = HashMap::from([
                (
                    "a".to_string(),
                    source_schema(&[("id", RockyType::Int64, false)]),
                ),
                (
                    "b".to_string(),
                    source_schema(&[("id", RockyType::Int64, right_nullable)]),
                ),
            ]);
            let external: HashMap<_, _> = sources
                .iter()
                .map(|(name, columns)| {
                    (
                        name.clone(),
                        columns
                            .iter()
                            .map(|col| rocky_ir::ColumnInfo {
                                name: col.name.clone(),
                                data_type: "BIGINT".to_string(),
                                nullable: col.nullable,
                            })
                            .collect(),
                    )
                })
                .collect();
            for (join, merged_nullable, left_nullable) in [
                ("LEFT JOIN", false, false),
                ("RIGHT JOIN", right_nullable, true),
                ("FULL OUTER JOIN", right_nullable, true),
            ] {
                for clause in [
                    format!("{join} b r USING (id)"),
                    format!("NATURAL {join} b r"),
                ] {
                    for projection in ["id", "*"] {
                        let sql = format!("SELECT {projection} FROM a l {clause}");
                        let columns = infer_select_types(&sql, &sources, "joined").unwrap();
                        assert_eq!(columns.len(), 1, "{sql}");
                        assert_eq!(columns[0].data_type, RockyType::Int64, "{sql}");
                        assert_eq!(columns[0].nullable, merged_nullable, "{sql}");
                        if projection == "*" {
                            let project =
                                Project::from_models(vec![make_model("joined", &sql)]).unwrap();
                            let graph = build_semantic_graph(&project, &external).unwrap();
                            let result = typecheck_project_with_models(
                                &graph,
                                &sources,
                                None,
                                &project.models,
                                None,
                            );
                            let columns = &result.typed_models["joined"];
                            assert_eq!(columns[0].nullable, merged_nullable, "{sql}");
                            let contract = CompilerContract {
                                columns: vec![ContractColumn {
                                    name: "id".to_string(),
                                    type_name: Some("Int64".to_string()),
                                    nullable: Some(false),
                                    description: None,
                                }],
                                rules: ContractRules::default(),
                            };
                            assert_eq!(
                                validate_contract("joined", columns, &contract)
                                    .iter()
                                    .any(|d| &*d.code == "E012"),
                                merged_nullable,
                                "{sql}"
                            );
                        }
                    }
                    let sql = format!("SELECT l.id AS left_id, r.id AS right_id FROM a l {clause}");
                    let columns = infer_select_types(&sql, &sources, "joined").unwrap();
                    assert_eq!(columns[0].nullable, left_nullable, "{sql}");
                    assert_eq!(
                        columns[1].nullable,
                        right_nullable || join != "RIGHT JOIN",
                        "{sql}"
                    );
                }
            }
        }
    }

    /// An inner join emits only matched rows, and its merged key never matches a
    /// NULL, so the key stays non-null as soon as either input key is. Sharing
    /// the FULL rule here widened it and rejected valid contracts with E012.
    #[test]
    fn inner_join_using_and_natural_keys_keep_the_merged_key_non_null() {
        for (left_nullable, right_nullable, merged_nullable) in [
            (false, false, false),
            (false, true, false),
            (true, false, false),
            (true, true, true),
        ] {
            let sources = HashMap::from([
                (
                    "a".to_string(),
                    source_schema(&[("id", RockyType::Int64, left_nullable)]),
                ),
                (
                    "b".to_string(),
                    source_schema(&[("id", RockyType::Int64, right_nullable)]),
                ),
            ]);
            let external: HashMap<_, _> = sources
                .iter()
                .map(|(name, columns)| {
                    (
                        name.clone(),
                        columns
                            .iter()
                            .map(|col| rocky_ir::ColumnInfo {
                                name: col.name.clone(),
                                data_type: "BIGINT".to_string(),
                                nullable: col.nullable,
                            })
                            .collect(),
                    )
                })
                .collect();
            // The whole-project path starts from the star's lineage edge, which
            // resolves to the first relation, and the join overlay only widens.
            // A nullable left key therefore stays nullable there even though the
            // merged key itself cannot be null.
            let project_nullable = merged_nullable || left_nullable;
            for join in ["JOIN", "INNER JOIN"] {
                for clause in [
                    format!("{join} b r USING (id)"),
                    format!("NATURAL {join} b r"),
                ] {
                    for projection in ["id", "*"] {
                        let sql = format!("SELECT {projection} FROM a l {clause}");
                        let columns = infer_select_types(&sql, &sources, "joined").unwrap();
                        assert_eq!(
                            columns.len(),
                            1,
                            "{sql} ({left_nullable}, {right_nullable})"
                        );
                        assert_eq!(
                            columns[0].data_type,
                            RockyType::Int64,
                            "{sql} ({left_nullable}, {right_nullable})"
                        );
                        assert_eq!(
                            columns[0].nullable, merged_nullable,
                            "{sql} ({left_nullable}, {right_nullable})"
                        );
                    }

                    let sql = format!("SELECT * FROM a l {clause}");
                    let project = Project::from_models(vec![make_model("joined", &sql)]).unwrap();
                    let graph = build_semantic_graph(&project, &external).unwrap();
                    let result = typecheck_project_with_models(
                        &graph,
                        &sources,
                        None,
                        &project.models,
                        None,
                    );
                    let columns = &result.typed_models["joined"];
                    assert_eq!(
                        columns[0].nullable, project_nullable,
                        "{sql} ({left_nullable}, {right_nullable})"
                    );
                    let contract = CompilerContract {
                        columns: vec![ContractColumn {
                            name: "id".to_string(),
                            type_name: Some("Int64".to_string()),
                            nullable: Some(false),
                            description: None,
                        }],
                        rules: ContractRules::default(),
                    };
                    assert_eq!(
                        validate_contract("joined", columns, &contract)
                            .iter()
                            .any(|d| &*d.code == "E012"),
                        project_nullable,
                        "{sql} ({left_nullable}, {right_nullable})"
                    );

                    let sql = format!("SELECT l.id AS left_id, r.id AS right_id FROM a l {clause}");
                    let columns = infer_select_types(&sql, &sources, "joined").unwrap();
                    assert_eq!(
                        columns[0].nullable, left_nullable,
                        "{sql} ({left_nullable}, {right_nullable})"
                    );
                    assert_eq!(
                        columns[1].nullable, right_nullable,
                        "{sql} ({left_nullable}, {right_nullable})"
                    );
                }
            }
        }
    }

    #[test]
    fn outer_join_scope_respects_column_aliases_and_missing_sources() {
        let sources = HashMap::from([(
            "users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        )]);
        for sql in [
            "SELECT l.kept, r.extended FROM users l(kept) LEFT JOIN users r(extended) ON true",
            "WITH c(renamed) AS (SELECT id FROM users) SELECT l.renamed, r.renamed FROM c l LEFT JOIN c r ON true",
        ] {
            let columns = infer_select_types(sql, &sources, "joined").unwrap();
            assert_eq!(columns.len(), 2, "{sql}");
            for (col, nullable) in columns.iter().zip([false, true]) {
                assert_eq!(col.data_type, RockyType::Int64, "{sql}");
                assert_eq!(col.nullable, nullable, "{sql}");
            }
        }
        let columns = infer_select_types_with_lookup("SELECT u.id FROM raw.users u", &|name| {
            sources.get(name).map(Vec::as_slice)
        })
        .unwrap()
        .columns;
        assert_eq!(columns[0].data_type, RockyType::Unknown);
        assert!(
            columns[0].nullable,
            "raw.users must not resolve to the unrelated users model"
        );
    }

    #[test]
    fn outer_join_incremental_nullability_matches_full_typecheck() {
        let sources = HashMap::from([(
            "users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        )]);
        let models = |join| {
            vec![
                make_model(
                    "joined",
                    &format!("SELECT b.id AS id FROM users a {join} users b ON a.id = b.id"),
                ),
                make_model("downstream", "SELECT id FROM joined"),
            ]
        };
        let initial = Project::from_models(models("INNER JOIN")).unwrap();
        let graph = build_semantic_graph(&initial, &HashMap::new()).unwrap();
        let previous = typecheck_project_with_models(&graph, &sources, None, &initial.models, None);
        assert!(!previous.typed_models["downstream"][0].nullable);

        let updated = Project::from_models(models("LEFT JOIN")).unwrap();
        let graph = build_semantic_graph(&updated, &HashMap::new()).unwrap();
        let affected = HashSet::from(["joined".to_string(), "downstream".to_string()]);
        let incremental = typecheck_project_incremental(
            &graph,
            &sources,
            &updated.models,
            &affected,
            &previous,
            None,
        );
        let full = typecheck_project_with_models(&graph, &sources, None, &updated.models, None);
        assert_eq!(incremental.typed_models, full.typed_models);
        for name in ["joined", "downstream"] {
            assert_eq!(full.typed_models[name][0].data_type, RockyType::Int64);
            assert!(full.typed_models[name][0].nullable, "{name}");
        }
    }

    fn make_time_interval_select_star_model(name: &str, time_column: &str, sql: &str) -> Model {
        let mut model = make_model(name, sql);
        model.config.strategy = StrategyConfig::TimeInterval {
            time_column: time_column.to_string(),
            granularity: rocky_ir::TimeGrain::Day,
            lookback: 0,
            batch_size: std::num::NonZeroU32::new(1).unwrap(),
            first_partition: None,
        };
        model
    }

    #[test]
    fn test_e020_not_fired_for_select_star_over_derived_table() {
        // Regression: `SELECT * FROM (<subquery>) AS alias` used to compute an
        // empty output schema, so E020 wrongly fired for a time_column the
        // inner query clearly projects. The derived table now expands.
        let models = vec![make_time_interval_select_star_model(
            "ti",
            "ts",
            "SELECT * FROM (SELECT ts, id FROM raw.base) AS x \
             WHERE ts >= @start_date AND ts < @end_date",
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        let ti_cols = result.typed_models.get("ti").unwrap();
        assert!(
            ti_cols.iter().any(|c| c.name == "ts"),
            "expected ts in output schema, got: {ti_cols:?}"
        );
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "E020"),
            "E020 must not fire when the inner query projects ts: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_e020_not_fired_for_select_star_over_derived_select_star() {
        // The dbt-importer microbatch wrapper shape:
        // `SELECT * FROM (SELECT * FROM <model>) AS x`. The inner `SELECT *`
        // can't be enumerated at the SQL layer, but the upstream model's
        // schema is known here, so the derived star resolves transitively and
        // E020 must not fire for a time_column the upstream clearly projects.
        let mut ti = make_time_interval_select_star_model(
            "ti",
            "ts",
            "SELECT * FROM (SELECT * FROM up) AS _rocky_microbatch \
             WHERE ts >= @start_date AND ts < @end_date",
        );
        // The import-dbt path emits the upstream dependency explicitly (the
        // ref is buried in the derived subquery), which fixes execution order.
        ti.config.depends_on = vec!["up".to_string()];
        let models = vec![make_model("up", "SELECT id, ts FROM source.raw.base"), ti];
        let project = Project::from_models(models).unwrap();
        let mut external = HashMap::new();
        external.insert(
            "source.raw.base".to_string(),
            vec![
                rocky_ir::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                },
                rocky_ir::ColumnInfo {
                    name: "ts".to_string(),
                    data_type: "DATE".to_string(),
                    nullable: false,
                },
            ],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        let ti_cols = result.typed_models.get("ti").unwrap();
        assert!(
            ti_cols.iter().any(|c| c.name == "ts"),
            "expected ts in output schema via transitive star, got: {ti_cols:?}"
        );
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "E020"),
            "E020 must not fire when the upstream projects ts: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_e020_fires_for_select_star_over_derived_select_star_missing_column() {
        // The transitive resolution must not weaken E020: when the upstream
        // genuinely lacks the time_column, it still errors.
        let mut ti = make_time_interval_select_star_model(
            "ti",
            "ts",
            "SELECT * FROM (SELECT * FROM up) AS _rocky_microbatch \
             WHERE ts >= @start_date AND ts < @end_date",
        );
        ti.config.depends_on = vec!["up".to_string()];
        let models = vec![make_model("up", "SELECT id FROM source.raw.base"), ti];
        let project = Project::from_models(models).unwrap();
        let mut external = HashMap::new();
        external.insert(
            "source.raw.base".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            result.diagnostics.iter().any(|d| &*d.code == "E020"),
            "E020 must still fire when the upstream omits the time_column: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_e020_still_fires_when_inner_query_omits_time_column() {
        // The fix must not weaken E020: a genuinely-absent time_column still
        // errors even with a SELECT * over a derived table.
        let models = vec![make_time_interval_select_star_model(
            "ti",
            "ts",
            "SELECT * FROM (SELECT id FROM raw.base) AS x \
             WHERE id >= @start_date AND id < @end_date",
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            result.diagnostics.iter().any(|d| &*d.code == "E020"),
            "E020 must still fire for a missing time_column: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_basic_type_propagation() {
        let models = vec![
            make_model("a", "SELECT id, name FROM source.raw.users"),
            make_model("b", "SELECT id, name FROM a"),
        ];

        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.users".to_string(),
            vec![
                rocky_ir::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                },
                rocky_ir::ColumnInfo {
                    name: "name".to_string(),
                    data_type: "STRING".to_string(),
                    nullable: true,
                },
            ],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.users".to_string(),
            source_schema(&[
                ("id", RockyType::Int64, false),
                ("name", RockyType::String, true),
            ]),
        );

        let result = typecheck_project(&graph, &sources, None);

        let b_cols = result.typed_models.get("b").unwrap();
        assert_eq!(b_cols.len(), 2);
        assert_eq!(b_cols[0].data_type, RockyType::Int64);
        assert_eq!(b_cols[1].data_type, RockyType::String);
    }

    #[test]
    fn test_avg_type_is_conservative_for_dialect_dependent_decimal() {
        let models = vec![make_model(
            "averages",
            "SELECT AVG(amount) AS avg_amount, AVG(quantity) AS avg_quantity \
             FROM source.raw.orders",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.orders".to_string(),
            vec![
                rocky_ir::ColumnInfo {
                    name: "amount".to_string(),
                    data_type: "DECIMAL(10,2)".to_string(),
                    nullable: false,
                },
                rocky_ir::ColumnInfo {
                    name: "quantity".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                },
            ],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.orders".to_string(),
            source_schema(&[
                (
                    "amount",
                    RockyType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    false,
                ),
                ("quantity", RockyType::Int64, false),
            ]),
        );

        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let columns = &result.typed_models["averages"];
        let avg_amount = columns.iter().find(|col| col.name == "avg_amount").unwrap();
        let avg_quantity = columns
            .iter()
            .find(|col| col.name == "avg_quantity")
            .unwrap();

        assert_eq!(avg_amount.data_type, RockyType::Unknown);
        assert_eq!(avg_quantity.data_type, RockyType::Float64);
        assert!(avg_amount.nullable);
        assert!(avg_quantity.nullable);

        let contract = CompilerContract {
            columns: vec![
                ContractColumn {
                    name: "avg_amount".to_string(),
                    type_name: Some("Decimal".to_string()),
                    nullable: Some(true),
                    description: None,
                },
                ContractColumn {
                    name: "avg_quantity".to_string(),
                    type_name: Some("Decimal".to_string()),
                    nullable: Some(true),
                    description: None,
                },
            ],
            rules: ContractRules::default(),
        };
        let diagnostics = validate_contract("averages", columns, &contract);
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| &*diagnostic.code == "E011"
                    && diagnostic.message.contains("avg_quantity")),
            "known Float64 AVG should reject a Decimal contract: {diagnostics:?}"
        );
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.message.contains("avg_amount")),
            "dialect-dependent Decimal AVG should not produce a false mismatch: {diagnostics:?}"
        );
    }

    /// The `Decimal` carve-out must not widen into "any input Rocky failed to
    /// resolve". `validate_contract` skips type validation when the inferred
    /// type is `Unknown`, so an over-broad withhold would turn E011 into
    /// silence for every `AVG` in a project compiled without source schemas —
    /// a fail-open at a gate, not conservative inference.
    #[test]
    fn test_avg_over_unresolved_input_still_validates_contracts() {
        let models = vec![make_model(
            "averages",
            "SELECT AVG(amount) AS avg_amount FROM source.raw.orders",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert("source.raw.orders".to_string(), vec![]);
        let graph = build_semantic_graph(&project, &external).unwrap();

        // No source schema for the referenced table — the `AVG` argument does
        // not resolve, exactly as with a cold schema cache or a fresh clone.
        let sources = HashMap::new();
        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let columns = &result.typed_models["averages"];
        let avg_amount = columns.iter().find(|col| col.name == "avg_amount").unwrap();
        assert_eq!(avg_amount.data_type, RockyType::Float64);

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "avg_amount".to_string(),
                type_name: Some("Boolean".to_string()),
                nullable: Some(true),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let diagnostics = validate_contract("averages", columns, &contract);
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| &*diagnostic.code == "E011"
                    && diagnostic.message.contains("avg_amount")),
            "an unresolved AVG argument must not silence the contract check: {diagnostics:?}"
        );
    }

    #[test]
    fn test_cast_alias_uses_cast_type_not_source_type() {
        let models = vec![make_model(
            "cast_id",
            "SELECT CAST(id AS STRING) AS id FROM source.raw.users",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.users".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.users".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        );

        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let cast_id = &result.typed_models["cast_id"][0];
        assert_eq!(cast_id.data_type, RockyType::String);
        assert!(!cast_id.nullable);

        let wrong_contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some("Int64".to_string()),
                nullable: Some(false),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        assert!(
            validate_contract("cast_id", std::slice::from_ref(cast_id), &wrong_contract)
                .iter()
                .any(|diagnostic| &*diagnostic.code == "E011")
        );
    }

    #[test]
    fn test_cast_over_unknown_input_stays_unknown() {
        // The cast target (`STRING`) is knowable from the SQL alone, but when the
        // cast's input column resolves to `Unknown` — e.g. a source type Rocky
        // can't map, or a missing/unresolved reference — the output is left
        // `Unknown` rather than asserting the parsed target. This is the
        // conservative branch of the cast-alias fix (#1145): a contract on the
        // column is skipped instead of validated against a fabricated type.
        let models = vec![make_model(
            "cast_unknown",
            "SELECT CAST(id AS STRING) AS id FROM source.raw.users",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.users".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".to_string(),
                data_type: "SOME_UNMAPPED_TYPE".to_string(),
                nullable: true,
            }],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        // The typed source column resolves to Unknown, so `input_is_known` is
        // false and the cast output must remain Unknown.
        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.users".to_string(),
            source_schema(&[("id", RockyType::Unknown, true)]),
        );

        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let cast_id = &result.typed_models["cast_unknown"][0];
        assert_eq!(cast_id.data_type, RockyType::Unknown);
    }

    #[test]
    fn test_try_cast_over_non_null_is_nullable() {
        // A fallible cast (`TRY_CAST` / `SAFE_CAST`) returns NULL on a failed
        // conversion, so its output is nullable even when the input column is
        // non-null. The target type is still refined from the SQL (Int64), only
        // the nullable bit differs from a plain `CAST`. A `nullable = false`
        // contract on such a column must fail with the nullability diagnostic
        // E012 (#1148).
        for cast in ["TRY_CAST", "SAFE_CAST"] {
            let models = vec![make_model(
                "casted",
                &format!("SELECT {cast}(id AS BIGINT) AS id FROM source.raw.users"),
            )];
            let project = Project::from_models(models).unwrap();

            let mut external = HashMap::new();
            external.insert(
                "source.raw.users".to_string(),
                vec![rocky_ir::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                }],
            );
            let graph = build_semantic_graph(&project, &external).unwrap();

            let mut sources = HashMap::new();
            sources.insert(
                "source.raw.users".to_string(),
                source_schema(&[("id", RockyType::Int64, false)]),
            );

            let result =
                typecheck_project_with_models(&graph, &sources, None, &project.models, None);
            let casted = &result.typed_models["casted"][0];
            assert_eq!(
                casted.data_type,
                RockyType::Int64,
                "{cast} target type should still resolve"
            );
            assert!(
                casted.nullable,
                "{cast} output must be nullable even over a non-null input"
            );

            // Contract type matches (Int64) so only the nullability mismatch
            // surfaces — E012, not E011.
            let wrong_contract = CompilerContract {
                columns: vec![ContractColumn {
                    name: "id".to_string(),
                    type_name: Some("Int64".to_string()),
                    nullable: Some(false),
                    description: None,
                }],
                rules: ContractRules::default(),
            };
            let diags = validate_contract("casted", std::slice::from_ref(casted), &wrong_contract);
            assert!(
                diags.iter().any(|diagnostic| &*diagnostic.code == "E012"),
                "{cast}: expected E012 nullability diagnostic, got {diags:?}"
            );
        }
    }

    #[test]
    fn test_infallible_cast_over_fallible_cast_is_nullable() {
        // Fallibility is sticky through an outer infallible cast:
        // `CAST(TRY_CAST(id AS INT) AS BIGINT)` still returns NULL when the
        // inner conversion fails, so the output must be nullable even over a
        // non-null input. The target type is the outer cast's (BIGINT → Int64).
        // Regression guard for the nested-cast hole (#1148).
        let models = vec![make_model(
            "casted",
            "SELECT CAST(TRY_CAST(id AS INT) AS BIGINT) AS id FROM source.raw.users",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.users".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".to_string(),
                data_type: "STRING".to_string(),
                nullable: false,
            }],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.users".to_string(),
            source_schema(&[("id", RockyType::String, false)]),
        );

        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let casted = &result.typed_models["casted"][0];
        assert_eq!(
            casted.data_type,
            RockyType::Int64,
            "outer cast target (BIGINT) should resolve"
        );
        assert!(
            casted.nullable,
            "an infallible cast over a fallible one must stay nullable"
        );

        let wrong_contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some("Int64".to_string()),
                nullable: Some(false),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let diags = validate_contract("casted", std::slice::from_ref(casted), &wrong_contract);
        assert!(
            diags.iter().any(|diagnostic| &*diagnostic.code == "E012"),
            "expected E012 nullability diagnostic, got {diags:?}"
        );
    }

    #[test]
    fn test_select_star_warning() {
        let models = vec![
            make_model("a", "SELECT id FROM source.raw.users"),
            make_model("b", "SELECT * FROM a"),
        ];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project(&graph, &HashMap::new(), None);

        let star_diags: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| d.model == "b" && (&*d.code == "I001" || &*d.code == "W002"))
            .collect();
        assert!(!star_diags.is_empty());
    }

    #[test]
    fn test_clean_model_no_errors() {
        let models = vec![make_model("a", "SELECT 1 AS id, 'hello' AS name")];

        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project(&graph, &HashMap::new(), None);

        let errors: Vec<_> = result.diagnostics.iter().filter(|d| d.is_error()).collect();
        assert!(errors.is_empty());
    }

    #[test]
    fn test_join_type_mismatch_detected() {
        let models = vec![
            make_model("a", "SELECT id, value FROM source.raw.t1"),
            make_model("b", "SELECT id, label FROM source.raw.t2"),
            make_model(
                "c",
                "SELECT a.id, a.value, b.label FROM a JOIN b ON a.id = b.id",
            ),
        ];

        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.t1".to_string(),
            vec![
                rocky_ir::ColumnInfo {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                },
                rocky_ir::ColumnInfo {
                    name: "value".into(),
                    data_type: "STRING".into(),
                    nullable: true,
                },
            ],
        );
        external.insert(
            "source.raw.t2".to_string(),
            vec![
                rocky_ir::ColumnInfo {
                    name: "id".into(),
                    data_type: "STRING".into(),
                    nullable: false,
                },
                rocky_ir::ColumnInfo {
                    name: "label".into(),
                    data_type: "STRING".into(),
                    nullable: true,
                },
            ],
        );

        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.t1".to_string(),
            source_schema(&[
                ("id", RockyType::Int64, false),
                ("value", RockyType::String, true),
            ]),
        );
        sources.insert(
            "source.raw.t2".to_string(),
            source_schema(&[
                ("id", RockyType::String, false),
                ("label", RockyType::String, true),
            ]),
        );

        let result = typecheck_project(&graph, &sources, None);
        let type_errors: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "E001")
            .collect();
        assert!(
            !type_errors.is_empty(),
            "should detect join key type mismatch"
        );
    }

    #[test]
    fn test_join_compatible_types_warning() {
        let models = vec![
            make_model("a", "SELECT id FROM source.raw.t1"),
            make_model("b", "SELECT id FROM source.raw.t2"),
            make_model("c", "SELECT a.id FROM a JOIN b ON a.id = b.id"),
        ];

        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.t1".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".into(),
                data_type: "INT".into(),
                nullable: false,
            }],
        );
        external.insert(
            "source.raw.t2".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
        );

        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.t1".to_string(),
            source_schema(&[("id", RockyType::Int32, false)]),
        );
        sources.insert(
            "source.raw.t2".to_string(),
            source_schema(&[("id", RockyType::Int64, false)]),
        );

        let result = typecheck_project(&graph, &sources, None);
        let warnings: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W001")
            .collect();
        assert!(
            !warnings.is_empty(),
            "should warn about implicit type coercion"
        );
    }

    // --- New expression-level inference tests ---

    #[test]
    fn test_infer_expr_literal_int() {
        let scope = TypeScope::new();
        let expr = parse_expr("42");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_expr_literal_string() {
        let scope = TypeScope::new();
        let expr = parse_expr("'hello'");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::String);
    }

    #[test]
    fn test_infer_expr_literal_bool() {
        let scope = TypeScope::new();
        let expr = parse_expr("TRUE");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Boolean);
    }

    #[test]
    fn test_infer_expr_cast() {
        let scope = TypeScope::new();
        let expr = parse_expr("CAST(x AS BIGINT)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_expr_cast_decimal() {
        let scope = TypeScope::new();
        let expr = parse_expr("CAST(x AS DECIMAL(10,2))");
        assert_eq!(
            infer_expr_type(&expr, &scope).0,
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_infer_expr_comparison_is_boolean() {
        let scope = TypeScope::new();
        let expr = parse_expr("a > b");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Boolean);
    }

    #[test]
    fn test_infer_expr_arithmetic() {
        let mut scope = TypeScope::new();
        scope
            .columns
            .insert(CiKey::owned("x".to_string()), (RockyType::Int64, false));
        scope
            .columns
            .insert(CiKey::owned("y".to_string()), (RockyType::Int64, false));
        let expr = parse_expr("x + y");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_expr_count() {
        let scope = TypeScope::new();
        let expr = parse_expr("COUNT(*)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(!nullable, "COUNT should be non-nullable");
    }

    #[test]
    fn test_infer_expr_sum() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("amount".to_string()),
            (RockyType::Int64, false),
        );
        let expr = parse_expr("SUM(amount)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(nullable, "SUM should be nullable (empty group)");
    }

    #[test]
    fn test_infer_expr_avg() {
        let mut scope = TypeScope::new();
        let expr = parse_expr("AVG(x)");

        // An argument Rocky cannot resolve keeps `Float64`. Degrading it to
        // `Unknown` would silently disable the column's E011 contract check
        // (`contracts.rs` skips type validation for `Unknown`), which is a
        // fail-open at a gate rather than conservative inference.
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);

        scope
            .columns
            .insert(CiKey::owned("x".to_string()), (RockyType::Int64, false));
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);

        scope
            .columns
            .insert(CiKey::owned("x".to_string()), (RockyType::Float32, false));
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);

        // Only an exact-numeric input is dialect-dependent enough to withhold.
        scope.columns.insert(
            CiKey::owned("x".to_string()),
            (
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            ),
        );
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Unknown);
    }

    #[test]
    fn test_infer_expr_is_null_boolean() {
        let scope = TypeScope::new();
        let expr = parse_expr("x IS NULL");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Boolean);
        assert!(!nullable, "IS NULL should be non-nullable");
    }

    #[test]
    fn test_infer_expr_case_when() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("status".to_string()),
            (RockyType::String, false),
        );
        let expr = parse_expr("CASE WHEN status = 'a' THEN 1 ELSE 0 END");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_expr_coalesce_non_nullable() {
        let mut scope = TypeScope::new();
        scope
            .columns
            .insert(CiKey::owned("a".to_string()), (RockyType::Int64, true));
        let expr = parse_expr("COALESCE(a, 0)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(
            !nullable,
            "COALESCE with literal fallback should be non-nullable"
        );
    }

    #[test]
    fn test_infer_select_types_full() {
        let sql =
            "SELECT id, CAST(amount AS DECIMAL(10,2)) AS amount_dec, COUNT(*) AS cnt FROM orders";
        let mut scope = HashMap::new();
        scope.insert(
            "orders".to_string(),
            vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "amount".into(),
                    data_type: RockyType::Float64,
                    nullable: true,
                },
            ],
        );

        let result = infer_select_types(sql, &scope, "test").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "id");
        assert_eq!(result[0].data_type, RockyType::Int64);
        assert_eq!(result[1].name, "amount_dec");
        assert_eq!(
            result[1].data_type,
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(result[2].name, "cnt");
        assert_eq!(result[2].data_type, RockyType::Int64);
    }

    // --- CTE tests ---

    #[test]
    fn test_cte_simple() {
        let sql = "WITH cte AS (SELECT 1 AS x, 'hello' AS y) SELECT x, y FROM cte";
        let result = infer_select_types(sql, &HashMap::new(), "test").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "x");
        assert_eq!(result[0].data_type, RockyType::Int64);
        assert_eq!(result[1].name, "y");
        assert_eq!(result[1].data_type, RockyType::String);
    }

    #[test]
    fn test_cte_chain() {
        let sql = "WITH a AS (SELECT 1 AS id), b AS (SELECT id FROM a) SELECT id FROM b";
        let result = infer_select_types(sql, &HashMap::new(), "test").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "id");
        assert_eq!(result[0].data_type, RockyType::Int64);
    }

    #[test]
    fn test_cte_with_upstream_scope() {
        let sql = "WITH filtered AS (SELECT id, amount FROM orders WHERE amount > 0) SELECT id, amount FROM filtered";
        let mut scope = HashMap::new();
        scope.insert(
            "orders".to_string(),
            vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "amount".into(),
                    data_type: RockyType::Float64,
                    nullable: true,
                },
            ],
        );
        let result = infer_select_types(sql, &scope, "test").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].data_type, RockyType::Int64);
        assert_eq!(result[1].data_type, RockyType::Float64);
    }

    #[test]
    fn test_cte_with_aggregation() {
        let sql = "WITH totals AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) SELECT customer_id, total FROM totals";
        let mut scope = HashMap::new();
        scope.insert(
            "orders".to_string(),
            vec![
                TypedColumn {
                    name: "customer_id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "amount".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
            ],
        );
        let result = infer_select_types(sql, &scope, "test").unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "customer_id");
        assert_eq!(result[1].name, "total");
        // SUM of Int64 → Int64
        assert_eq!(result[1].data_type, RockyType::Int64);
    }

    // --- Window function tests ---

    #[test]
    fn test_infer_window_row_number() {
        let scope = TypeScope::new();
        let expr = parse_expr("ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(!nullable, "ROW_NUMBER is never null");
    }

    #[test]
    fn test_infer_window_rank() {
        let scope = TypeScope::new();
        let expr = parse_expr("RANK() OVER (ORDER BY amount DESC)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_window_percent_rank() {
        let scope = TypeScope::new();
        let expr = parse_expr("PERCENT_RANK() OVER (ORDER BY amount)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);
    }

    #[test]
    fn test_infer_window_cume_dist() {
        let scope = TypeScope::new();
        let expr = parse_expr("CUME_DIST() OVER (ORDER BY amount)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);
    }

    #[test]
    fn test_infer_window_lag() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("amount".to_string()),
            (RockyType::Float64, false),
        );
        let expr = parse_expr("LAG(amount, 1) OVER (ORDER BY order_date)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Float64);
        assert!(nullable, "LAG can produce nulls");
    }

    #[test]
    fn test_infer_window_lead() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("status".to_string()),
            (RockyType::String, false),
        );
        let expr = parse_expr("LEAD(status) OVER (ORDER BY id)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::String);
        assert!(nullable);
    }

    #[test]
    fn test_infer_window_first_value() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("amount".to_string()),
            (RockyType::Int64, false),
        );
        let expr = parse_expr("FIRST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY ts)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_window_sum_over() {
        let mut scope = TypeScope::new();
        scope.columns.insert(
            CiKey::owned("amount".to_string()),
            (RockyType::Int64, false),
        );
        let expr = parse_expr("SUM(amount) OVER (PARTITION BY customer_id)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(nullable, "windowed SUM is nullable");
    }

    #[test]
    fn test_infer_window_count_over() {
        let scope = TypeScope::new();
        let expr = parse_expr("COUNT(*) OVER (PARTITION BY customer_id)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(!nullable, "COUNT is never null");
    }

    #[test]
    fn test_infer_window_avg_over() {
        let mut scope = TypeScope::new();
        let expr = parse_expr("AVG(amount) OVER (ORDER BY id)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Float64);

        // The windowed form routes through the same rule as the aggregate form.
        scope.columns.insert(
            CiKey::owned("amount".to_string()),
            (
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            ),
        );
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Unknown);
    }

    #[test]
    fn test_infer_window_ntile() {
        let scope = TypeScope::new();
        let expr = parse_expr("NTILE(4) OVER (ORDER BY amount)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_window_nth_value() {
        let mut scope = TypeScope::new();
        scope
            .columns
            .insert(CiKey::owned("name".to_string()), (RockyType::String, false));
        let expr = parse_expr("NTH_VALUE(name, 2) OVER (ORDER BY id)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::String);
        assert!(nullable, "NTH_VALUE is nullable");
    }

    // --- Subquery tests ---

    #[test]
    fn test_infer_exists_is_boolean() {
        let scope = TypeScope::new();
        let expr = parse_expr("EXISTS (SELECT 1 FROM orders)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Boolean);
    }

    #[test]
    fn test_infer_in_subquery_is_boolean() {
        let scope = TypeScope::new();
        let expr = parse_expr("id IN (SELECT order_id FROM orders)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Boolean);
    }

    // --- Additional function tests ---

    #[test]
    fn test_infer_length() {
        let scope = TypeScope::new();
        let expr = parse_expr("LENGTH('hello')");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_year() {
        let scope = TypeScope::new();
        let expr = parse_expr("YEAR(order_date)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int32);
    }

    #[test]
    fn test_infer_datediff() {
        let scope = TypeScope::new();
        let expr = parse_expr("DATEDIFF(DAY, start_date, end_date)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    #[test]
    fn test_infer_nullif() {
        let mut scope = TypeScope::new();
        scope
            .columns
            .insert(CiKey::owned("x".to_string()), (RockyType::Int64, false));
        let expr = parse_expr("NULLIF(x, 0)");
        let (ty, nullable) = infer_expr_type(&expr, &scope);
        assert_eq!(ty, RockyType::Int64);
        assert!(nullable, "NULLIF always nullable");
    }

    /// Helper to parse a single SQL expression for testing.
    fn parse_expr(expr_str: &str) -> Expr {
        let sql = format!("SELECT {expr_str}");
        let dialect = rocky_sql::dialect::DatabricksDialect;
        let stmts = Parser::parse_sql(&dialect, &sql).unwrap();
        if let Statement::Query(q) = &stmts[0]
            && let SetExpr::Select(s) = q.body.as_ref()
        {
            if let SelectItem::UnnamedExpr(e) = &s.projection[0] {
                return e.clone();
            }
            if let SelectItem::ExprWithAlias { expr, .. } = &s.projection[0] {
                return expr.clone();
            }
        }
        panic!("failed to parse expression: {expr_str}");
    }

    // ----- time_interval validation tests (Phase 1.5) -----

    use rocky_ir::TimeGrain;
    use std::num::NonZeroU32;

    /// Build a `Model` whose strategy is `time_interval` with the given fields,
    /// and whose SQL contains both `@start_date` and `@end_date` (so the
    /// placeholder check passes by default — individual tests override `sql`
    /// when they want to exercise E024/W003).
    fn make_time_interval_model(
        name: &str,
        time_column: &str,
        granularity: TimeGrain,
        first_partition: Option<&str>,
    ) -> Model {
        Model {
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::TimeInterval {
                    time_column: time_column.to_string(),
                    granularity,
                    lookback: 0,
                    batch_size: NonZeroU32::new(1).unwrap(),
                    first_partition: first_partition.map(String::from),
                },
                target: TargetConfig {
                    catalog: "warehouse".into(),
                    schema: "marts".into(),
                    table: name.into(),
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
            sql: format!(
                "SELECT {time_column} FROM upstream WHERE {time_column} >= @start_date AND {time_column} < @end_date"
            ),
            file_path: format!("models/{name}.sql"),
            contract_path: None,
        }
    }

    fn typed_col(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.into(),
            data_type: ty,
            nullable,
        }
    }

    #[test]
    fn test_time_interval_clean_model_no_diags() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(
            diags.is_empty(),
            "expected no diagnostics for clean time_interval model, got: {diags:?}"
        );
    }

    #[test]
    fn test_time_interval_non_time_interval_strategy_no_diags() {
        // A FullRefresh model should produce zero diagnostics from the
        // time_interval pass.
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.config.strategy = StrategyConfig::FullRefresh;
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_e020_missing_time_column() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        let cols = vec![typed_col("other_col", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E020"));
    }

    #[test]
    fn test_e021_wrong_column_type() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        let cols = vec![typed_col("order_date", RockyType::String, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E021"));
    }

    #[test]
    fn test_e021_e022_e025_skipped_for_unknown_type() {
        // The compiler couldn't infer the type (e.g., source schema not
        // declared). E021/E022/E025 should NOT fire — the runtime will
        // catch any actual type mismatch when SQL execution fails.
        let model = make_time_interval_model("m", "order_date", TimeGrain::Hour, None);
        // Unknown + nullable=true would normally fire E021 (not temporal),
        // E022 (nullable), AND E025 (hour grain on non-TIMESTAMP).
        let cols = vec![typed_col("order_date", RockyType::Unknown, true)];
        let diags = check_time_interval_strategy(&model, &cols);
        let codes: Vec<&str> = diags.iter().map(|d| &*d.code).collect();
        assert!(
            !codes.contains(&"E021"),
            "E021 should be skipped when type is Unknown, got: {codes:?}"
        );
        assert!(
            !codes.contains(&"E022"),
            "E022 should be skipped when type is Unknown, got: {codes:?}"
        );
        assert!(
            !codes.contains(&"E025"),
            "E025 should be skipped when type is Unknown, got: {codes:?}"
        );
    }

    #[test]
    fn test_e022_nullable_column() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        let cols = vec![typed_col("order_date", RockyType::Date, true)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E022"));
    }

    #[test]
    fn test_e023_invalid_identifier() {
        let model = make_time_interval_model("m", "order date", TimeGrain::Day, None);
        let cols = vec![typed_col("order date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E023"));
    }

    #[test]
    fn test_e024_no_placeholders() {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E024"));
    }

    #[test]
    fn test_w003_only_start_date() {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE order_date >= @start_date".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        let w003: Vec<_> = diags.iter().filter(|d| &*d.code == "W003").collect();
        assert_eq!(w003.len(), 1, "expected one W003, got: {diags:?}");
    }

    #[test]
    fn test_w003_only_end_date() {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE order_date < @end_date".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "W003"));
    }

    #[test]
    fn test_e025_hour_grain_on_date_column() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Hour, None);
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E025"));
    }

    #[test]
    fn test_e025_hour_grain_on_timestamp_ok() {
        let model = make_time_interval_model("m", "event_at", TimeGrain::Hour, None);
        let cols = vec![typed_col("event_at", RockyType::Timestamp, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(
            !diags.iter().any(|d| &*d.code == "E025"),
            "TIMESTAMP column should accept hour granularity"
        );
    }

    #[test]
    fn test_e026_bad_first_partition_format() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, Some("2024-13-01"));
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E026"));
    }

    #[test]
    fn test_e026_first_partition_grain_mismatch() {
        // first_partition "2024-01-01" passed to a Year-grain model should
        // fail because Year keys are "YYYY" not "YYYY-MM-DD".
        let model = make_time_interval_model("m", "year_col", TimeGrain::Year, Some("2024-01-01"));
        let cols = vec![typed_col("year_col", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E026"));
    }

    #[test]
    fn test_first_partition_valid_passes() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, Some("2024-01-01"));
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(!diags.iter().any(|d| &*d.code == "E026"));
    }

    #[test]
    fn test_placeholder_word_boundary() {
        // `@start_date_extra` should NOT match `@start_date`. Test the
        // word-boundary logic in contains_placeholder.
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE x = @start_date_extra".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        // Should fire E024 (neither placeholder present, since @start_date_extra
        // doesn't count) — not W003.
        assert!(diags.iter().any(|d| &*d.code == "E024"));
    }

    // ----- W006: merge unique_key existence -----

    /// Build a `merge`-strategy model with the given `unique_key`. Mirrors
    /// `make_time_interval_model` but swaps the strategy so the merge checks
    /// have something to bite on.
    fn make_merge_model(name: &str, unique_key: &[&str]) -> Model {
        let mut m = make_time_interval_model(name, "order_date", TimeGrain::Day, None);
        m.config.strategy = StrategyConfig::Merge {
            unique_key: unique_key.iter().map(|k| (*k).to_string()).collect(),
            update_columns: None,
        };
        m
    }

    #[test]
    fn test_merge_clean_model_no_diags() {
        let model = make_merge_model("m", &["customer_id"]);
        let cols = vec![
            typed_col("customer_id", RockyType::String, false),
            typed_col("order_date", RockyType::Date, false),
        ];
        let diags = check_merge_strategy(&model, &cols, true);
        assert!(
            diags.is_empty(),
            "expected no diagnostics for clean merge model, got: {diags:?}"
        );
    }

    #[test]
    fn test_merge_non_merge_strategy_no_diags() {
        // A FullRefresh model should produce zero diagnostics from the merge
        // pass, even though it has no matching columns at all.
        let mut model = make_merge_model("m", &["customer_id"]);
        model.config.strategy = StrategyConfig::FullRefresh;
        let diags = check_merge_strategy(&model, &[], true);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_w006_missing_unique_key() {
        // `custommer_id` is a typo — the real column is `customer_id`.
        let model = make_merge_model("m", &["custommer_id"]);
        let cols = vec![typed_col("customer_id", RockyType::String, false)];
        let diags = check_merge_strategy(&model, &cols, true);
        assert!(diags.iter().any(|d| &*d.code == "W006"));
    }

    #[test]
    fn test_w006_reports_every_missing_key() {
        // A composite key with two typos should report both, not just the
        // first — one diagnostic per missing key.
        let model = make_merge_model("m", &["tenant_id", "custommer_id", "ordr_id"]);
        let cols = vec![
            typed_col("tenant_id", RockyType::String, false),
            typed_col("customer_id", RockyType::String, false),
            typed_col("order_id", RockyType::String, false),
        ];
        let diags = check_merge_strategy(&model, &cols, true);
        let w006: Vec<_> = diags.iter().filter(|d| &*d.code == "W006").collect();
        assert_eq!(w006.len(), 2, "expected both bad keys reported: {diags:?}");
        assert!(w006.iter().any(|d| d.message.contains("custommer_id")));
        assert!(w006.iter().any(|d| d.message.contains("ordr_id")));
        // The valid key must not be reported.
        assert!(!w006.iter().any(|d| d.message.contains("tenant_id")));
    }

    #[test]
    fn test_w006_skipped_when_schema_unknown() {
        // `SELECT *` over an unknown upstream: typed_cols isn't the model's
        // real output, so we must stay quiet rather than flag every key.
        let model = make_merge_model("m", &["customer_id"]);
        let diags = check_merge_strategy(&model, &[], false);
        assert!(
            diags.is_empty(),
            "must not fire when the output schema is incomplete, got: {diags:?}"
        );
    }

    #[test]
    fn test_w006_suggestion_lists_available_columns() {
        let model = make_merge_model("m", &["nope"]);
        let cols = vec![
            typed_col("customer_id", RockyType::String, false),
            typed_col("order_date", RockyType::Date, false),
        ];
        let diags = check_merge_strategy(&model, &cols, true);
        let d = diags
            .iter()
            .find(|d| &*d.code == "W006")
            .expect("expected W006");
        let suggestion = d.suggestion.as_deref().expect("expected a suggestion");
        assert!(suggestion.contains("customer_id"));
        assert!(suggestion.contains("order_date"));
    }

    #[test]
    fn test_w006_is_a_warning_not_an_error() {
        // Severity in this codebase follows the code prefix, so a `W` code
        // emitted at `Error` severity would be a contradiction — and would
        // silently reintroduce the build-breaking behaviour this check was
        // downgraded away from.
        let model = make_merge_model("m", &["nope"]);
        let cols = vec![typed_col("customer_id", RockyType::String, false)];
        let diags = check_merge_strategy(&model, &cols, true);
        let d = diags
            .iter()
            .find(|d| &*d.code == "W006")
            .expect("expected W006");
        assert_eq!(d.severity, crate::diagnostic::Severity::Warning);
    }

    // ----- W006 end-to-end: the completeness flag as the caller computes it.
    // The unit tests above hand the flag to `check_merge_strategy`, so they can
    // only prove the check behaves correctly *given* a correct flag — the two
    // false-positive classes this check shipped with were both a caller
    // computing it wrong, which such a test can never catch. Everything below
    // goes through `typecheck_project_with_models`, which derives it. -----

    /// A `merge` model with the given SQL and `unique_key`, suitable for
    /// building a real `Project` (unlike `make_merge_model`, which is shaped
    /// for direct `check_merge_strategy` calls).
    fn make_merge_project_model(name: &str, sql: &str, unique_key: &[&str]) -> Model {
        let mut m = make_model(name, sql);
        m.config.strategy = StrategyConfig::Merge {
            unique_key: unique_key.iter().map(|k| (*k).to_string()).collect(),
            update_columns: None,
        };
        m
    }

    #[test]
    fn test_w006_not_fired_for_select_star_over_non_model_source() {
        // Regression: `SELECT *` over a raw (non-model) source produces an
        // empty typed output schema, because `ModelSchema::upstream` lists
        // only *models*. The original guard asked "does any upstream have an
        // unknown schema?" — for a root model `upstream` is empty, so that was
        // vacuously false and the check ran against zero columns, flagging
        // every unique_key as missing on a perfectly valid model.
        let models = vec![make_merge_project_model(
            "ingest_orders",
            "SELECT * FROM poc.raw.orders",
            &["order_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire for `SELECT *` over a non-model source: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_not_fired_for_select_star_joining_non_model_source() {
        // The under-inclusive sibling of the case above: here `upstream` is
        // `["up"]` and `up`'s schema *is* known, so an "any upstream unknown?"
        // guard is false — yet the star also pulls columns from
        // `poc.raw.orders`, which is not a model, so the enumerated output is
        // still not the model's real output set. `order_id` lives in the raw
        // side and must not be reported missing.
        let models = vec![
            make_model("up", "SELECT customer_id FROM poc.raw.customers"),
            make_merge_project_model(
                "joined",
                "SELECT * FROM up JOIN poc.raw.orders o ON up.customer_id = o.customer_id",
                &["order_id"],
            ),
        ];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire when a star also expands a non-model source: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_still_fires_end_to_end_for_explicit_columns() {
        // The true positive must survive the fix: an explicit column list is
        // fully enumerable, so a typo'd `unique_key` is provably wrong.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT order_id, amount FROM poc.raw.orders",
            &["ordr_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        let w006: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W006")
            .collect();
        assert_eq!(
            w006.len(),
            1,
            "expected exactly one W006 for the typo'd key: {:?}",
            result.diagnostics
        );
        assert!(w006[0].message.contains("ordr_id"));
    }

    #[test]
    fn test_w006_not_fired_end_to_end_for_correct_explicit_key() {
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT order_id, amount FROM poc.raw.orders",
            &["order_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire when the key is a real column: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_not_fired_for_parenthesised_projection() {
        // Regression: `(order_id)` parses as `Expr::Nested`, which the lineage
        // extractor does not resolve — so the item yields no output column, and
        // being unnamed it gets no alias fallback either. The projection has no
        // star, so a `!has_star` completeness test called this schema fully
        // known while `typed_cols` was in fact *empty*, and the check flagged a
        // model whose generated MERGE runs perfectly well.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT (order_id) FROM poc.raw.orders",
            &["order_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire when a projection item could not be enumerated: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_not_fired_for_partially_parenthesised_projection() {
        // The partial sibling of the case above, and the reason a
        // `typed_cols.is_empty()` guard is not a sufficient fix: `amount`
        // resolves, `(order_id)` does not, so the enumerated schema is
        // non-empty *and* incomplete. Only a completeness signal from the
        // extractor itself distinguishes this from a genuinely one-column
        // model with a typo'd key.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT (order_id), amount FROM poc.raw.orders",
            &["order_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "a non-empty but incomplete schema must still suppress W006: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_not_fired_for_unnamed_computed_projection() {
        // The general form of the same defect: an unnamed item the extractor
        // *can* trace still has no predictable output name — `UPPER(order_id)`
        // is recorded as lineage from `order_id`, which is the right answer for
        // impact analysis and the wrong one for "what columns does this model
        // output?". The schema is therefore not complete and the check must
        // stay quiet. Guards against a fix that only special-cases `Nested`.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT UPPER(order_id), amount FROM poc.raw.orders",
            &["order_key"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire when an unnamed expression has no predictable name: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_still_fires_when_expressions_are_aliased() {
        // The flip side: aliasing restores completeness, because `expr AS name`
        // always yields a correctly named output column. A model that computes
        // things is still fully checkable as long as it names its outputs — so
        // the completeness gate costs coverage only on genuinely unnameable
        // projections, not on every model that uses an expression.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT UPPER(order_id) AS order_key, amount FROM poc.raw.orders",
            &["ordr_key"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        let w006: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W006")
            .collect();
        assert_eq!(
            w006.len(),
            1,
            "aliased expressions keep the schema complete: {:?}",
            result.diagnostics
        );
        assert!(w006[0].message.contains("ordr_key"));
    }

    #[test]
    fn test_w006_qualified_identifier_projection_stays_complete() {
        // `o.order_id` is a `CompoundIdentifier` — still a bare identifier with
        // a predictable output name (`order_id`), so it must not be counted as
        // unresolved. Without this the completeness gate would silently switch
        // the check off for every model that qualifies its columns, which is
        // most joined models.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT o.order_id, o.amount FROM poc.raw.orders o",
            &["ordr_id"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "qualified identifiers must not suppress W006: {:?}",
            result.diagnostics
        );
    }

    // ----- W006: identifier case -----

    #[test]
    fn test_w006_not_fired_for_case_mismatched_unique_key() {
        // `ORDER_ID` vs the projected `order_id`. Rocky interpolates the key
        // into `MERGE ... ON t.ORDER_ID = s.ORDER_ID` as a bare unquoted
        // identifier, which every supported warehouse resolves
        // case-insensitively, so this model is valid and must compile.
        let models = vec![make_merge_project_model(
            "ingest_orders",
            "SELECT order_id, amount FROM poc.raw.orders",
            &["ORDER_ID"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "W006"),
            "W006 must not fire on a case-mismatched but valid key: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn test_w006_case_insensitive_match_both_directions() {
        // Mixed case on either side must match: the model may project
        // `Order_ID` just as easily as the key may be written `order_id`.
        let model = make_merge_model("m", &["order_id"]);
        let cols = vec![typed_col("Order_ID", RockyType::String, false)];
        assert!(
            check_merge_strategy(&model, &cols, true).is_empty(),
            "lowercase key must match a mixed-case column"
        );

        let model = make_merge_model("m", &["Order_ID"]);
        let cols = vec![typed_col("order_id", RockyType::String, false)];
        assert!(
            check_merge_strategy(&model, &cols, true).is_empty(),
            "mixed-case key must match a lowercase column"
        );
    }

    #[test]
    fn test_w006_case_insensitivity_does_not_swallow_near_miss_typos() {
        // The true positive must survive: `order_idd` differs from `order_id`
        // by more than case, so case-insensitive matching must still reject
        // it. Guards against a fix that over-corrects into fuzzy matching.
        let models = vec![make_merge_project_model(
            "orders",
            "SELECT order_id, amount FROM poc.raw.orders",
            &["order_idd"],
        )];
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        let w006: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W006")
            .collect();
        assert_eq!(
            w006.len(),
            1,
            "expected W006 for a genuine typo: {:?}",
            result.diagnostics
        );
        assert!(w006[0].message.contains("order_idd"));
    }

    #[test]
    fn test_w006_case_insensitive_only_ignores_case_not_separators() {
        // `orderid` (no underscore) is a different identifier, not a case
        // variant, and must still error.
        let model = make_merge_model("m", &["ORDERID"]);
        let cols = vec![typed_col("order_id", RockyType::String, false)];
        let diags = check_merge_strategy(&model, &cols, true);
        assert!(
            diags.iter().any(|d| &*d.code == "W006"),
            "separator differences are not case differences: {diags:?}"
        );
    }

    // ----- W004: classification-tag completeness -----

    /// Build a bare model carrying a single `[classification]` entry.
    /// Mirrors `make_model` but lets each test customise the column → tag
    /// map without rebuilding the whole `ModelConfig` literal.
    fn make_classified_model(
        name: &str,
        classification: &[(&str, &str)],
    ) -> rocky_core::models::Model {
        let mut m = make_model(name, "SELECT 1 AS id");
        m.config.classification = classification
            .iter()
            .map(|(col, tag)| ((*col).to_string(), (*tag).to_string()))
            .collect();
        m
    }

    /// Build a `[mask]` table seeded with default strategies for `tags`,
    /// plus optional `[mask.<env>]` override tables. Returns the shape
    /// `CompilerConfig.mask` expects.
    fn mask_table(
        defaults: &[&str],
        env_overrides: &[(&str, &[&str])],
    ) -> std::collections::BTreeMap<String, rocky_core::config::MaskEntry> {
        use rocky_core::config::MaskEntry;
        use rocky_ir::MaskStrategy;

        let mut out = std::collections::BTreeMap::new();
        for tag in defaults {
            out.insert((*tag).to_string(), MaskEntry::Strategy(MaskStrategy::Hash));
        }
        for (env, tags) in env_overrides {
            let inner: std::collections::BTreeMap<String, MaskStrategy> = tags
                .iter()
                .map(|t| ((*t).to_string(), MaskStrategy::Redact))
                .collect();
            out.insert((*env).to_string(), MaskEntry::EnvOverride(inner));
        }
        out
    }

    #[test]
    fn w004_resolved_tag_emits_no_diagnostic() {
        let models = vec![make_classified_model("users", &[("email", "pii")])];
        let mask = mask_table(&["pii"], &[]);
        let diags = check_classification_tags(&models, &mask, &[]);
        assert!(
            diags.is_empty(),
            "a tag resolved via [mask] should emit no W004, got: {diags:?}"
        );
    }

    #[test]
    fn w004_unresolved_tag_emits_diagnostic_with_helpful_text() {
        let models = vec![make_classified_model(
            "users",
            &[("audit_note", "audit_only")],
        )];
        // No mask entry for `audit_only`, and no allow-list entry.
        let mask = mask_table(&["pii"], &[]);
        let diags = check_classification_tags(&models, &mask, &[]);

        assert_eq!(diags.len(), 1, "expected exactly one W004, got: {diags:?}");
        let d = &diags[0];
        assert_eq!(&*d.code, "W004");
        assert_eq!(d.severity, crate::diagnostic::Severity::Warning);
        assert_eq!(d.model, "users");
        // The message must name the model/column/tag so it's actionable.
        let msg = d.message.as_ref();
        assert!(msg.contains("audit_only"), "message missing tag: {msg}");
        assert!(msg.contains("audit_note"), "message missing column: {msg}");
        // And the remedy must point at both escape hatches.
        let help = d.suggestion.as_deref().unwrap_or("");
        assert!(
            help.contains("[mask.audit_only]"),
            "suggestion missing mask hint: {help}"
        );
        assert!(
            help.contains("allow_unmasked"),
            "suggestion missing allow_unmasked hint: {help}"
        );
    }

    #[test]
    fn w004_allow_unmasked_suppresses_diagnostic() {
        let models = vec![make_classified_model(
            "users",
            &[("audit_note", "audit_only")],
        )];
        let mask = mask_table(&["pii"], &[]);
        let allow = vec!["audit_only".to_string()];
        let diags = check_classification_tags(&models, &mask, &allow);
        assert!(
            diags.is_empty(),
            "allow_unmasked should suppress W004 for listed tags, got: {diags:?}"
        );
    }

    #[test]
    fn w004_tag_resolved_only_via_env_override_is_resolved() {
        // `pii_high` only appears under `[mask.prod]` — the compile-time
        // check must treat it as resolved regardless of the active env.
        let models = vec![make_classified_model("users", &[("ssn", "pii_high")])];
        let mask = mask_table(&[], &[("prod", &["pii_high"])]);
        let diags = check_classification_tags(&models, &mask, &[]);
        assert!(
            diags.is_empty(),
            "tag present only in [mask.prod] should still resolve, got: {diags:?}"
        );
    }

    #[test]
    fn w004_emits_one_diagnostic_per_model_column_tag() {
        // Two models, each with a distinct unresolved tag. Expect two
        // W004s, each attributed to its owning model.
        let models = vec![
            make_classified_model("users", &[("nickname", "internal_only")]),
            make_classified_model("accounts", &[("audit_note", "audit_only")]),
        ];
        let mask = mask_table(&["pii"], &[]);
        let diags = check_classification_tags(&models, &mask, &[]);

        assert_eq!(
            diags.len(),
            2,
            "expected one W004 per (model, column, tag): {diags:?}"
        );
        let mut models_seen: Vec<&str> = diags.iter().map(|d| d.model.as_str()).collect();
        models_seen.sort();
        assert_eq!(models_seen, vec!["accounts", "users"]);
        for d in &diags {
            assert_eq!(&*d.code, "W004");
        }
    }

    // ----- W005: freshness-coverage soft-warn -----

    /// Build a `typed_models` map with a single entry. `check_freshness_coverage`
    /// keys columns by model name, mirroring `TypeCheckResult::typed_models`.
    fn typed_models_for(
        name: &str,
        cols: &[(&str, RockyType)],
    ) -> IndexMap<String, Vec<TypedColumn>> {
        let mut out = IndexMap::new();
        out.insert(
            name.to_string(),
            cols.iter()
                .map(|(c, ty)| typed_col(c, ty.clone(), false))
                .collect(),
        );
        out
    }

    #[test]
    fn w005_temporal_column_without_freshness_emits_diagnostic() {
        let models = vec![make_model("events", "SELECT 1")];
        let typed = typed_models_for(
            "events",
            &[("id", RockyType::Int64), ("event_ts", RockyType::Timestamp)],
        );

        let diags = check_freshness_coverage(&models, &typed, false);

        assert_eq!(diags.len(), 1, "expected exactly one W005, got: {diags:?}");
        let d = &diags[0];
        assert_eq!(&*d.code, "W005");
        assert_eq!(d.severity, crate::diagnostic::Severity::Warning);
        assert_eq!(d.model, "events");
        let msg = d.message.as_ref();
        assert!(msg.contains("event_ts"), "message missing column: {msg}");
        // The suggestion must name the public-facing field + a candidate column.
        let help = d.suggestion.as_deref().unwrap_or("");
        assert!(
            help.contains("expected_lag_seconds"),
            "suggestion missing TTL hint: {help}"
        );
        assert!(
            help.contains("event_ts"),
            "suggestion missing candidate time_column: {help}"
        );
    }

    #[test]
    fn w005_no_temporal_column_emits_nothing() {
        let models = vec![make_model("dims", "SELECT 1")];
        let typed = typed_models_for(
            "dims",
            &[("id", RockyType::Int64), ("label", RockyType::String)],
        );

        let diags = check_freshness_coverage(&models, &typed, false);
        assert!(
            diags.is_empty(),
            "a model with no temporal column should emit no W005, got: {diags:?}"
        );
    }

    #[test]
    fn w005_model_freshness_block_suppresses_diagnostic() {
        let mut model = make_model("events", "SELECT 1");
        model.config.freshness = Some(rocky_core::models::ModelFreshnessConfig {
            max_lag_seconds: 3600,
            time_column: Some("event_ts".to_string()),
            severity: None,
        });
        let models = vec![model];
        let typed = typed_models_for("events", &[("event_ts", RockyType::Timestamp)]);

        let diags = check_freshness_coverage(&models, &typed, false);
        assert!(
            diags.is_empty(),
            "a per-model [freshness] block should suppress W005, got: {diags:?}"
        );
    }

    #[test]
    fn w005_project_default_suppresses_diagnostic_globally() {
        let models = vec![make_model("events", "SELECT 1")];
        let typed = typed_models_for("events", &[("event_ts", RockyType::Timestamp)]);

        // `project_freshness_default = true` => every model inherits the
        // project-level TTL, so W005 stays silent.
        let diags = check_freshness_coverage(&models, &typed, true);
        assert!(
            diags.is_empty(),
            "a project-level [freshness] default should suppress all W005s, got: {diags:?}"
        );
    }

    // ----- E035: managed-Iceberg format_options -----

    fn make_iceberg_model(name: &str, options: rocky_ir::LakehouseOptions) -> Model {
        let mut m = make_model(name, "SELECT 1 AS id");
        m.config.format = Some(rocky_ir::LakehouseFormat::IcebergTable);
        m.config.format_options = Some(options);
        m
    }

    #[test]
    fn e035_partition_and_cluster_together_emits_error_naming_options() {
        let models = vec![make_iceberg_model(
            "fct_events",
            rocky_ir::LakehouseOptions {
                partition_by: vec!["event_date".into()],
                cluster_by: vec!["user_id".into()],
                ..Default::default()
            },
        )];
        let diags = check_lakehouse_format_options(&models);
        assert_eq!(diags.len(), 1, "expected one E035, got: {diags:?}");
        let d = &diags[0];
        assert_eq!(&*d.code, "E035");
        assert!(d.is_error());
        assert_eq!(d.model, "fct_events");
        let msg = d.message.as_ref();
        assert!(msg.contains("partition_by"), "names partition_by: {msg}");
        assert!(msg.contains("cluster_by"), "names cluster_by: {msg}");
        assert!(d.suggestion.is_some());
    }

    #[test]
    fn e035_write_format_property_emits_error_naming_key() {
        let models = vec![make_iceberg_model(
            "fct_events",
            rocky_ir::LakehouseOptions {
                table_properties: vec![("write.format.default".into(), "parquet".into())],
                ..Default::default()
            },
        )];
        let diags = check_lakehouse_format_options(&models);
        assert_eq!(diags.len(), 1);
        assert_eq!(&*diags[0].code, "E035");
        assert!(
            diags[0].message.contains("write.format.default"),
            "names the offending key: {}",
            diags[0].message
        );
    }

    #[test]
    fn e035_valid_iceberg_options_emit_nothing() {
        let models = vec![
            make_iceberg_model(
                "partitioned",
                rocky_ir::LakehouseOptions {
                    partition_by: vec!["event_date".into()],
                    table_properties: vec![("delta.enableChangeDataFeed".into(), "true".into())],
                    ..Default::default()
                },
            ),
            make_iceberg_model(
                "clustered",
                rocky_ir::LakehouseOptions {
                    cluster_by: vec!["user_id".into()],
                    ..Default::default()
                },
            ),
        ];
        let diags = check_lakehouse_format_options(&models);
        assert!(
            diags.is_empty(),
            "valid combos emit nothing, got: {diags:?}"
        );
    }

    #[test]
    fn e035_non_iceberg_and_formatless_models_skipped() {
        // Delta with partition+cluster+write.format is fine; a model with no
        // lakehouse format is skipped entirely.
        let mut delta = make_model("delta_m", "SELECT 1 AS id");
        delta.config.format = Some(rocky_ir::LakehouseFormat::DeltaTable);
        delta.config.format_options = Some(rocky_ir::LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..Default::default()
        });
        let plain = make_model("plain_m", "SELECT 1 AS id");
        let diags = check_lakehouse_format_options(&[delta, plain]);
        assert!(diags.is_empty(), "got: {diags:?}");
    }
}
