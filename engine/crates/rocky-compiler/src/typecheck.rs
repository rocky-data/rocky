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
    Diagnostic, E001, E020, E021, E022, E023, E024, E025, E026, E035, E037, E046, I001, I002,
    SourceSpan, W001, W002, W004, W005, W006, W046, W056,
};
use crate::operand_check::{OperandDialect, OperandTarget, TargetDialects};
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
pub(crate) struct TypeScope {
    /// column_name → (type, nullable) for all columns in scope.
    columns: HashMap<CiKey<'static>, (RockyType, bool)>,
    /// table → { column → (type, nullable) } for qualified references.
    qualified: HashMap<CiKey<'static>, HashMap<CiKey<'static>, (RockyType, bool)>>,
    /// Bare column names whose type is a guess: a CTE or derived-table
    /// column built from an expression that is not exact (see
    /// [`has_exact_type`]). A column read from a table or a model is exact.
    inexact: HashSet<CiKey<'static>>,
    /// table → columns whose type is a guess, for qualified references.
    qualified_inexact: HashMap<CiKey<'static>, HashSet<CiKey<'static>>>,
    /// The warehouses the SQL runs on. A `CAST` target whose width differs
    /// between warehouses is typed from them (#2333).
    target: OperandTarget,
}

impl TypeScope {
    /// A scope with no known warehouse.
    #[cfg(test)]
    fn new() -> Self {
        Self::with_target(OperandTarget::Unconfigured)
    }

    fn with_target(target: OperandTarget) -> Self {
        Self {
            columns: HashMap::new(),
            qualified: HashMap::new(),
            inexact: HashSet::new(),
            qualified_inexact: HashMap::new(),
            target,
        }
    }

    /// Whether the bare column `name` has a type read from the SQL, not a
    /// guess.
    fn is_exact(&self, name: &str) -> bool {
        !self.inexact.contains(CiStr::new(name))
    }

    /// Whether the qualified column `table.col` has a type read from the
    /// SQL, not a guess.
    fn is_exact_qualified(&self, table: &str, col: &str) -> bool {
        !self
            .qualified_inexact
            .get(CiStr::new(table))
            .is_some_and(|cols| cols.contains(CiStr::new(col)))
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
    typecheck_project_for_targets(
        graph,
        source_schemas,
        models,
        join_keys_acc,
        &TargetDialects::default(),
    )
}

/// [`typecheck_project_with_models`], knowing the warehouse each model runs
/// on. A `CAST` to a type whose width differs between warehouses (`INT`,
/// `FLOAT`, `TIMESTAMP`, …) is typed for that warehouse, and stays
/// [`RockyType::Unknown`] for a model with no known target (#2333).
pub fn typecheck_project_for_targets(
    graph: &SemanticGraph,
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    models: &[rocky_core::models::Model],
    join_keys_acc: Option<&Arc<AtomicU64>>,
    targets: &TargetDialects,
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
                    targets.for_model(model_name),
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
///
/// `previous` must have been computed with the same `targets`: a model whose
/// target changed is not in `affected` by that alone.
pub fn typecheck_project_incremental(
    graph: &SemanticGraph,
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    models: &[rocky_core::models::Model],
    affected: &HashSet<String>,
    previous: &TypeCheckResult,
    join_keys_acc: Option<&Arc<AtomicU64>>,
    targets: &TargetDialects,
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
                    targets.for_model(model_name),
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
    target: &OperandTarget,
) -> ModelTypecheckOutput {
    let model_start = Instant::now();
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    // User-defined functions: installed for this model's inference only.
    let udf_scope = crate::udf::TypecheckScope::enter(
        graph.functions(),
        model_name,
        model_by_name.get(model_name).map(|m| m.sql.as_str()),
    );

    // Extract references from this model's SQL if available.
    let ref_map = if let Some(model) = model_by_name.get(model_name) {
        collect_references(
            &model.sql,
            &model.file_path.display().to_string(),
            model_names,
        )
    } else {
        ReferenceMap::default()
    };

    // A qualified read of an upstream model's own target table
    // (`FROM analytics.main.customer_ltv`) types from that model.
    let relation_key = |name: &str| -> String {
        qualified_upstream_model(name, &model_schema.upstream, model_by_name)
            .map_or_else(|| name.to_string(), str::to_string)
    };

    // Step 1: Lineage-based type propagation
    let mut typed_cols: Vec<TypedColumn> = Vec::with_capacity(model_schema.columns.len());

    for col_def in &model_schema.columns {
        let producing_edge = graph.producing_edge(model_name, &col_def.name);

        let (data_type, nullable) = if let Some(edge) = producing_edge {
            let source_model = relation_key(&edge.source.model);
            let upstream_type = typed_models
                .get(&source_model)
                .and_then(|cols| {
                    col_index
                        .get(&source_model)
                        .and_then(|idx| idx.get(&*edge.source.column))
                        .map(|&i| &cols[i])
                })
                .map(|c| (c.data_type.clone(), c.nullable))
                .unwrap_or((RockyType::Unknown, true));

            match &edge.transform {
                rocky_sql::lineage::TransformKind::Direct => upstream_type,
                // Plain cast: type is refined in Step 2. Nullability starts as
                // the input's; the expression-inference pass below widens it
                // when the cast can fail (#2299).
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
    //
    // A nested expression (`CAST(MAX(x) AS BIGINT)`) and a column with no
    // traceable source (`COUNT(*)`) also need it: the edge kind alone cannot
    // type them (#2295).
    // A set operation takes its type from every branch, but lineage reads only
    // the first; a model whose columns are all nullable would otherwise skip
    // inference and keep the first branch's type (#2303 red team). A keyword
    // match is enough: a false positive only runs inference once more.
    let mentions_set_operation = model_by_name
        .get(model_name)
        .is_some_and(|m| sql_mentions_set_operation(&m.sql));
    let needs_inference = udf_scope.is_active()
        || mentions_set_operation
        || typed_cols
            .iter()
            .any(|col| match graph.producing_edge(model_name, &col.name) {
                Some(edge) => {
                    edge.transform.is_cast()
                        || edge.transform == rocky_sql::lineage::TransformKind::Expression
                        || !col.nullable
                }
                None => true,
            });
    let inference = model_by_name
        .get(model_name)
        .filter(|_| needs_inference)
        .map(|model| {
            infer_select_types_with_lookup(
                &model.sql,
                &|name| typed_models.get(&relation_key(name)).map(Vec::as_slice),
                target,
            )
            .ok()
        });
    // Inference was needed and could not answer (a query form it does not
    // read, mismatched set-operation branches). Lineage then holds only a
    // guess, so claim nothing it cannot back: every column is nullable, and a
    // non-`Direct` edge is `Unknown` (#2303).
    if matches!(inference, Some(None)) {
        for col in &mut typed_cols {
            col.nullable = true;
            let direct = graph
                .producing_edge(model_name, &col.name)
                .is_some_and(|edge| edge.transform == rocky_sql::lineage::TransformKind::Direct);
            // A `Direct` column of a model with a set operation reads only
            // the first branch's type, so it is `Unknown` too (#2304).
            if !direct || mentions_set_operation {
                col.data_type = RockyType::Unknown;
            }
        }
    }
    let inferred_cols = inference.flatten();
    if let Some(inferred) = &inferred_cols {
        let mut inferred_by_name = HashMap::new();
        for (index, col) in inferred.columns.iter().enumerate() {
            // Match semantic-graph star expansion, which keeps the first
            // occurrence when multiple relations expose the same name.
            inferred_by_name.entry(col.name.as_str()).or_insert((
                col,
                inferred.exact_type_outputs.contains(&index),
                inferred.count_outputs.contains(&index),
                inferred.cast_outputs.contains(&index),
            ));
        }
        for col in &mut typed_cols {
            let Some(&(inferred_col, exact_type, count, cast)) =
                inferred_by_name.get(col.name.as_str())
            else {
                continue;
            };
            // A cast's output type is its target type, whatever the input
            // is (or whether it has a traceable column at all). Only the
            // type is taken here: nullability keeps following the input
            // rules below, so an unresolved input stays nullable. A bare
            // `DECIMAL` target has no digits, so inference returns
            // `Unknown` for it and nothing is fabricated (#1721).
            if cast && col.data_type == RockyType::Unknown {
                col.data_type = inferred_col.data_type.clone();
            }
            let Some(edge) = graph.producing_edge(model_name, &col.name) else {
                // No traceable source column. Only `COUNT(...)` is typed here:
                // it is a non-null BIGINT whatever its argument (#2295). Other
                // source-less projections (literals, multi-column arithmetic)
                // stay Unknown.
                if count {
                    col.data_type = inferred_col.data_type.clone();
                    col.nullable = inferred_col.nullable;
                }
                continue;
            };
            match &edge.transform {
                // A bare column or a cast over one: inference in the SQL
                // relation scope carries the outer-join side. Only ever widen
                // nullability. Cast target refinement still runs below.
                rocky_sql::lineage::TransformKind::Direct
                | rocky_sql::lineage::TransformKind::Cast
                | rocky_sql::lineage::TransformKind::TryCast => {
                    col.nullable |= inferred_col.nullable;
                    // Lineage reads only the first branch of a set operation,
                    // also one inside a CTE or derived table that this
                    // `Direct` column reads through (#2304).
                    if (inferred.set_operation || mentions_set_operation)
                        && edge.transform == rocky_sql::lineage::TransformKind::Direct
                    {
                        col.data_type =
                            set_operation_column_type(&col.data_type, inferred_col, exact_type);
                    }
                }
                // A nested expression. Step 1 left it `(Unknown, true)`. Take
                // inference's answer only when the type comes straight from
                // the SQL (a cast target, `COUNT`, `SUM`/`MIN`/`MAX`/`AVG` over
                // one, or a `CASE`/`COALESCE` whose branches agree) and the
                // traced input column is known. (A projection that is itself a
                // cast was typed above, whatever its input.)
                rocky_sql::lineage::TransformKind::Expression => {
                    if exact_type
                        && inferred_col.data_type != RockyType::Unknown
                        && edge_input_is_known(edge, typed_models, col_index, &relation_key)
                    {
                        col.data_type = inferred_col.data_type.clone();
                        col.nullable = inferred_col.nullable;
                    }
                }
                // An aggregate over a bare column: Step 1 already typed it
                // from the column's type.
                // For a set operation another branch may be nullable.
                rocky_sql::lineage::TransformKind::Aggregation(_) => {
                    if inferred.set_operation {
                        col.nullable |= inferred_col.nullable;
                        col.data_type =
                            set_operation_column_type(&col.data_type, inferred_col, exact_type);
                    } else if col.data_type == RockyType::Unknown
                        && exact_type
                        && inferred_col.data_type != RockyType::Unknown
                        && edge_input_is_known(edge, typed_models, col_index, &relation_key)
                    {
                        // A function over one column that Step 1 cannot type
                        // from its name alone (`COALESCE(id, 0)`): take
                        // inference's exact answer, as for `Expression`.
                        col.data_type = inferred_col.data_type.clone();
                        col.nullable = inferred_col.nullable;
                    }
                }
            }
        }
    }
    if udf_scope.is_active()
        && let Some(model) = model_by_name.get(model_name)
    {
        crate::udf::apply_direct_call_types(&model.sql, graph.functions(), &mut typed_cols);
    }
    diagnostics.extend(udf_scope.finish());
    let enhanced_diags = enhanced_inference(
        model_name,
        graph,
        typed_models,
        col_index,
        &relation_key,
        inferred_cols
            .as_ref()
            .map(|inferred| inferred.columns.as_slice()),
        &mut typed_cols,
    );
    diagnostics.extend(enhanced_diags);

    // A missing type is normally conservative Unknown: unsupported functions,
    // incomplete source schemas, and expressions outside our inference subset
    // are all valid reasons not to know. Refuse only a reference that binds
    // unambiguously to a complete in-project upstream model and is absent from
    // that model's proven output schema.
    diagnostics.extend(check_known_missing_upstream_refs(
        model_name,
        model_schema,
        graph,
        model_by_name,
    ));

    // Step 2c: GROUP BY validity (E044) and ambiguous bare names (E029).
    // Names resolve only against upstream models this model depends on and
    // known source schemas; anything else is unknown and stays silent.
    if let Some(model) = model_by_name.get(model_name) {
        let relation_columns = |name: &str| -> Option<Vec<String>> {
            let columns = if name.contains('.') {
                typed_models.get(&relation_key(name)).or_else(|| {
                    typed_models
                        .iter()
                        .find(|(key, _)| key.contains('.') && key.eq_ignore_ascii_case(name))
                        .map(|(_, columns)| columns)
                })
            } else if model_schema.upstream.iter().any(|up| up == name) {
                typed_models.get(name)
            } else {
                None
            }?;
            Some(columns.iter().map(|column| column.name.clone()).collect())
        };
        diagnostics.extend(crate::group_by::check_group_by(
            model_name,
            &model.sql,
            &relation_columns,
        ));
        // E029: a bare column name two joined relations both provably have.
        diagnostics.extend(crate::ambiguous::check_ambiguous_columns(
            model_name,
            &model.sql,
            &relation_columns,
        ));
    }

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
    // `check_time_interval_strategy` for the full list of E020-E026.
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
        diagnostics.extend(check_incremental_strategy(
            model,
            &typed_cols,
            model_schema.schema_is_complete(),
        ));
        let mut time_interval_diagnostics = check_time_interval_strategy(model, &typed_cols);
        // E022 reads the column's nullable bit, which is only a fact when every
        // column it traces back to has a known type. A cast takes its target
        // type whatever its input is, so `CAST(d AS DATE)` over a source with
        // no schema is typed `Date` but still has the nullable bit of an
        // unknown input. That guess must not refuse the model.
        if let rocky_core::models::StrategyConfig::TimeInterval { time_column, .. } =
            &model.config.strategy
            && !nullability_is_proven(
                graph,
                typed_models,
                col_index,
                &relation_key,
                model_name,
                time_column,
            )
        {
            time_interval_diagnostics.retain(|d| &*d.code != E022);
        }
        diagnostics.extend(time_interval_diagnostics);
        diagnostics.extend(check_merge_strategy(
            model,
            &typed_cols,
            model_schema.schema_is_complete(),
        ));
        diagnostics.extend(crate::snapshot::check_snapshot_strategy(
            model,
            &typed_cols,
            model_schema.schema_is_complete(),
            model_schema.has_star,
        ));
        // After the checks above (which judge the SELECT's own columns):
        // a snapshot's table also holds its metadata columns.
        crate::snapshot::append_snapshot_metadata_columns(model, &mut typed_cols);
    }

    // Step 6: Enrich diagnostics with the model's file path as a SourceSpan
    // when they don't already have one. This gives miette a file to render.
    if let Some(model) = model_by_name.get(model_name) {
        // `SourceSpan.file` is what miette renders, so a lossy string is the
        // right type at this seam — unlike the affected-set comparison in
        // `compile.rs`, nothing here is matched against a real path (#1730).
        let file_path = model.file_path.display().to_string();
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

/// E039: a reference, in any clause, to a column that a complete upstream
/// model does not output.
///
/// An upstream counts only when absence is provable: the reader depends on
/// it, its lineage schema is complete ([`ModelSchema::schema_is_complete`]),
/// its output names are fixed by its own SQL ([`has_provably_fixed_output_names`]),
/// and no two of them collide case-insensitively. A bare read binds to the
/// model only when the model writes a table of that name (or is ephemeral). A
/// qualified read of the model's target is not checked. Scope resolution, aliases,
/// CTEs, subqueries and struct-field reads follow
/// [`crate::source_refs::check_upstream_model_column_refs`]: anything it
/// cannot bind stays `Unknown`.
fn check_known_missing_upstream_refs(
    model_name: &str,
    model_schema: &ModelSchema,
    graph: &SemanticGraph,
    model_by_name: &HashMap<&str, &rocky_core::models::Model>,
) -> Vec<Diagnostic> {
    use rocky_core::physical_edges::fold_identifier;
    let Some(model) = model_by_name.get(model_name) else {
        return Vec::new();
    };
    let mut snapshot_meta: Vec<Vec<String>> = Vec::new();
    let mut candidates: Vec<(&rocky_core::models::Model, &ModelSchema)> = Vec::new();
    for upstream in &model_schema.upstream {
        let Some(upstream_model) = model_by_name.get(upstream.as_str()) else {
            continue;
        };
        let Some(upstream_schema) = graph
            .model_schema(upstream)
            .filter(|schema| schema.schema_is_complete())
        else {
            continue;
        };
        if !has_provably_fixed_output_names(&upstream_model.sql) {
            continue;
        }
        let mut output_names = HashSet::with_capacity(upstream_schema.columns.len());
        if upstream_schema
            .columns
            .iter()
            .any(|column| !output_names.insert(CiKey::owned(column.name.clone())))
        {
            // Warehouses can rename duplicate projected names while
            // materializing the model (DuckDB turns the second `id` into
            // `id_1`), so the graph cannot prove absence from that relation.
            continue;
        }
        // A snapshot model's table holds its SELECT's columns plus the SCD2
        // metadata columns (`valid_from`, `is_current`, ...), which the
        // semantic graph does not list. Readers may read them.
        snapshot_meta.push(
            upstream_model
                .config
                .strategy
                .snapshot_lowered()
                .map(|lowered| {
                    lowered
                        .spec
                        .meta_columns
                        .reserved()
                        .into_iter()
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        );
        candidates.push((upstream_model, upstream_schema));
    }
    let upstreams: Vec<crate::source_refs::UpstreamModelColumns<'_>> = candidates
        .iter()
        .zip(&snapshot_meta)
        .map(|((upstream_model, upstream_schema), meta)| {
            let ephemeral = matches!(
                upstream_model.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            );
            let name = upstream_model.config.name.as_str();
            // A bare `FROM x` reaches the table `x`: this model only when it
            // writes that table, or is ephemeral (inlined by name).
            let bare_binding = ephemeral
                || fold_identifier(&upstream_model.config.target.table) == fold_identifier(name);
            crate::source_refs::UpstreamModelColumns {
                name,
                bare_binding,
                columns: upstream_schema
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .chain(meta.iter().map(String::as_str))
                    .collect(),
            }
        })
        .collect();
    crate::source_refs::check_upstream_model_column_refs(model, &upstreams)
}

/// The upstream model whose `[target]` table a qualified read names, for a
/// reader that depends on it: `catalog.schema.table`, or `schema.table` when
/// exactly one upstream writes that schema and table.
///
/// Only models in `upstream` (the reader's DAG edges) are candidates, so a
/// physical name never binds to a model the reader does not read (#1631). An
/// ephemeral model writes no table and never matches. A bare name returns
/// `None`: it is already a model key.
pub(crate) fn qualified_upstream_model<'m>(
    name: &str,
    upstream: &[String],
    model_by_name: &HashMap<&str, &'m rocky_core::models::Model>,
) -> Option<&'m str> {
    use rocky_core::physical_edges::fold_identifier;
    let parts: Vec<String> = name.split('.').map(fold_identifier).collect();
    if parts.len() < 2 || parts.len() > 3 || parts.iter().any(String::is_empty) {
        return None;
    }
    let mut matches = upstream.iter().filter_map(|up| {
        let model = model_by_name.get(up.as_str())?;
        if matches!(
            model.config.strategy,
            rocky_core::models::StrategyConfig::Ephemeral
        ) {
            return None;
        }
        let target = &model.config.target;
        let table_matches = fold_identifier(&target.schema) == parts[parts.len() - 2]
            && fold_identifier(&target.table) == parts[parts.len() - 1];
        let catalog_matches = parts.len() == 2 || fold_identifier(&target.catalog) == parts[0];
        (table_matches && catalog_matches).then_some(model.config.name.as_str())
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

pub(crate) fn is_warehouse_pseudo_column(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_metadata")
        || name.eq_ignore_ascii_case("_partitiontime")
        || name.eq_ignore_ascii_case("_partitiondate")
        || name
            .get(.."metadata$".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("metadata$"))
}

/// Whether a model's output column names are fixed by its own SQL: one
/// `SELECT` (CTEs allowed) whose every projection item is a column reference
/// or carries an alias.
///
/// An alias over a function that can expand into several output columns
/// (`unnest` of a struct in DuckDB, `explode` of a map in Spark, `COLUMNS`,
/// `UNPACK`, `json_tuple`, `stack`, `inline`) does not fix the names, and
/// neither does a set operation, a star, or a string-quoted alias.
fn has_provably_fixed_output_names(sql: &str) -> bool {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return false;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.exclude.is_none()
        && select.value_table_mode.is_none()
        && select.lateral_views.is_empty()
        && select.flavor == ast::SelectFlavor::Standard
        && select.projection.iter().all(|item| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => true,
            SelectItem::ExprWithAlias { expr, alias } => {
                alias.quote_style != Some('\'') && !calls_expanding_function(expr)
            }
            _ => false,
        })
}

/// Whether `expr` calls a function that can return several columns.
fn calls_expanding_function(expr: &Expr) -> bool {
    use std::ops::ControlFlow;
    ast::visit_expressions(expr, |e| match e {
        Expr::Function(function)
            if function
                .name
                .0
                .last()
                .and_then(ast::ObjectNamePart::as_ident)
                .is_some_and(|ident| {
                    matches!(
                        ident.value.to_ascii_lowercase().as_str(),
                        "unnest"
                            | "explode"
                            | "explode_outer"
                            | "posexplode"
                            | "posexplode_outer"
                            | "inline"
                            | "inline_outer"
                            | "json_tuple"
                            | "stack"
                            | "columns"
                            | "unpack"
                            | "flatten"
                    )
                }) =>
        {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break()
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
/// | E024  | Error    | `@start_date` or `@end_date` does not filter the emitted rows |
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
    // upstream. Skip also when the model's own WHERE compares the column:
    // a comparison is never TRUE on NULL, so no emitted row has a NULL key,
    // whatever the upstream column allows.
    if type_known && column.nullable && !where_rejects_null_column(&model.sql, time_column) {
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

    // E024: validate placeholder usage in the SQL body.
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

/// E037 / E046 / W046 — validate a transformation `incremental` model (#1990).
///
/// An `incremental` model loads only rows past the target's own
/// `MAX(<watermark>)`. That needs a watermark column and a place to apply it:
///
/// - no `timestamp_column` (alias `watermark`) → **E037**: the only SQL left
///   is an unfiltered INSERT that appends every row again on each run;
/// - an `@incremental_filter` placeholder in the SQL → valid;
/// - no placeholder → the runtime filters the model's *output* column, which
///   is only equivalent when lineage proves that column is a direct
///   passthrough (`TransformKind::Direct`) of one input column. Anything else
///   (an expression, an aggregate, a `SELECT *`, SQL lineage cannot read) →
///   **E046**, naming where to put the placeholder;
/// - a watermark absent from a provably complete output schema → **E046**:
///   the target would have no such column to take `MAX` of;
/// - `lookback` without `unique_key` → **W046**: the re-read window is
///   appended again on every run;
/// - no `lookback` (with or without `unique_key`) → **W056**: the strict `>`
///   watermark skips a late row whose timestamp equals the target's `MAX`;
///   `unique_key` only merges rows the filter reads, so it does not help.
///
/// A placeholder in a model of any other strategy is **E046** too: nothing
/// would resolve it, and the warehouse would reject the SQL.
///
/// Loaded `microbatch` models are normalized to `time_interval` before this
/// check, so they receive the partition-window validation instead.
fn check_incremental_strategy(
    model: &rocky_core::models::Model,
    typed_cols: &[TypedColumn],
    schema_complete: bool,
) -> Vec<Diagnostic> {
    use rocky_core::incremental_filter::{PLACEHOLDER, has_placeholder};
    use rocky_core::models::StrategyConfig;

    let model_name = model.config.name.as_str();
    let StrategyConfig::Incremental {
        timestamp_column,
        unique_key,
        lookback,
        filter_column,
        ..
    } = &model.config.strategy
    else {
        if has_placeholder(&model.sql) {
            return vec![
                Diagnostic::error(
                    E046,
                    model_name,
                    format!(
                        "model '{model_name}' uses `{PLACEHOLDER}`, but its strategy is not \
                         `incremental`: nothing resolves the placeholder, so the warehouse \
                         would reject the SQL"
                    ),
                )
                .with_suggestion(
                    "Set `[strategy] type = \"incremental\"` with `timestamp_column`, or remove \
                     the placeholder",
                ),
            ];
        }
        return Vec::new();
    };

    let Some(watermark) = timestamp_column.as_deref().filter(|w| !w.is_empty()) else {
        return vec![
            Diagnostic::error(
                E037,
                model_name,
                format!(
                    "model '{model_name}' uses `type = \"incremental\"` with no watermark \
                     column: without one Rocky can only emit an unfiltered INSERT, which \
                     appends every row again on each run"
                ),
            )
            .with_suggestion(format!(
                "Declare the watermark in [strategy] — `timestamp_column = \"updated_at\"` — and \
                 put `{PLACEHOLDER}` where the filter belongs (`WHERE {PLACEHOLDER}`); or use \
                 `type = \"merge\"`, `\"delete_insert\"`, `\"time_interval\"` or \
                 `\"full_refresh\"`"
            )),
        ];
    };

    if rocky_sql::validation::validate_identifier(watermark).is_err() {
        return vec![
            Diagnostic::error(
                E046,
                model_name,
                format!(
                    "model '{model_name}': incremental watermark '{watermark}' is not a plain \
                 column name"
                ),
            )
            .with_suggestion("Name an output column of the model: letters, digits and `_`"),
        ];
    }

    let mut diagnostics = Vec::new();

    if let Some(filter) = filter_column
        && let Err(reason) = rocky_core::incremental_filter::validate_filter_column(filter)
    {
        diagnostics.push(
            Diagnostic::error(
                E046,
                model_name,
                format!("model '{model_name}': incremental {reason}"),
            )
            .with_suggestion(
                "Set `filter_column` to a column or `<alias>.<column>` of the model's input",
            ),
        );
    }

    if schema_complete
        && !typed_cols.is_empty()
        && !typed_cols
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(watermark))
    {
        diagnostics.push(
            Diagnostic::error(
                E046,
                model_name,
                format!(
                    "model '{model_name}': incremental watermark '{watermark}' is not an output \
                     column, so the target has no '{watermark}' to take MAX() of"
                ),
            )
            .with_suggestion(format!(
                "Select the watermark in the model output. Output columns: {}",
                typed_cols
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        );
    }

    if !has_placeholder(&model.sql) && !watermark_is_direct_passthrough(&model.sql, watermark) {
        diagnostics.push(
            Diagnostic::error(
                E046,
                model_name,
                format!(
                    "model '{model_name}' declares incremental watermark '{watermark}' but its \
                     SQL has no `{PLACEHOLDER}`, and Rocky cannot prove '{watermark}' passes \
                     straight through from an input column, so filtering the model output on \
                     it may not select the new input rows"
                ),
            )
            .with_suggestion(format!(
                "Put `{PLACEHOLDER}` in the WHERE clause that reads the source \
                 (`WHERE {PLACEHOLDER}`), and set `filter_column = \"<alias>.<column>\"` in \
                 [strategy] when it compares a qualified or renamed input column"
            )),
        );
    }

    if lookback.is_some_and(|lb| lb.amount > 0) && unique_key.is_empty() {
        diagnostics.push(
            Diagnostic::warning(
                W046,
                model_name,
                format!(
                    "model '{model_name}' sets an incremental `lookback` without `unique_key`: \
                     each run appends the re-read window again, duplicating those rows"
                ),
            )
            .with_suggestion("Add `unique_key` so the window is merged, or remove `lookback`"),
        );
    }

    if !lookback.is_some_and(|lb| lb.amount > 0) {
        let key_note = if unique_key.is_empty() {
            ""
        } else {
            "; `unique_key` alone does not help: it merges the rows the filter reads, \
             but the filter never reads that row again"
        };
        diagnostics.push(
            Diagnostic::warning(
                W056,
                model_name,
                format!(
                    "model '{model_name}' is an incremental model with no `lookback`: the \
                     filter is a strict `>` against the target's `MAX({watermark})`, so a row \
                     that arrives late with a '{watermark}' equal to that maximum is never \
                     loaded{key_note}"
                ),
            )
            .with_suggestion(
                "Set `lookback` (for example `\"1 hour\"`) so each run re-reads that window, \
                 together with `unique_key` so the re-read rows are merged instead of \
                 appended again. `unique_key` without `lookback` does not load the late row. \
                 Or confirm the source never delivers rows at an already-loaded timestamp",
            ),
        );
    }

    diagnostics
}

/// Whether `watermark` is an output column copied unchanged from one column
/// of a physical input table — the only shape for which filtering the model's
/// output equals filtering its input. Any doubt answers `false`:
/// unparseable SQL, a `SELECT *`, an expression, two output columns of that
/// name, a column read from a CTE or a derived table (whose own body may
/// aggregate), or a top-level `LIMIT` / `OFFSET` / `FETCH` (which picks rows
/// before the filter would).
fn watermark_is_direct_passthrough(sql: &str, watermark: &str) -> bool {
    use rocky_sql::lineage::{TableBinding, TransformKind};

    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return false;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    if query.with.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return false;
    }
    let Ok(lineage) = rocky_sql::lineage::extract_lineage(sql) else {
        return false;
    };
    let mut edges = lineage
        .columns
        .iter()
        .filter(|c| c.target_column.eq_ignore_ascii_case(watermark));
    let (Some(edge), None) = (edges.next(), edges.next()) else {
        return false;
    };
    if !matches!(edge.transform, TransformKind::Direct) {
        return false;
    }
    let physical = |t: &&rocky_sql::lineage::TableReference| {
        t.binding == TableBinding::Physical && t.name != "(subquery)"
    };
    match edge.source_table.as_deref() {
        Some(qualifier) => lineage.source_tables.iter().any(|t| {
            let named = t
                .alias
                .as_deref()
                .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                || t.name.eq_ignore_ascii_case(qualifier)
                || t.name
                    .rsplit('.')
                    .next()
                    .is_some_and(|last| last.eq_ignore_ascii_case(qualifier));
            named && physical(&t)
        }),
        // An unqualified column is unambiguous only with one relation.
        None => matches!(lineage.source_tables.as_slice(), [only] if physical(&only)),
    }
}

/// E024 — both `@start_date` and `@end_date` must bound the rows the model
/// emits.
///
/// A placeholder counts only where it filters rows: a `WHERE`, `HAVING`,
/// `QUALIFY` or `PREWHERE` clause, or the `ON` clause of an inner or semi
/// join. A placeholder in a comment, inside a longer string literal, or only
/// in the projection does not count (#2233): such a model copies every source
/// row on each partition run. The quoted form `'@start_date'` counts, because
/// the runtime substitutes it like the bare form.
///
/// The filter must reach every row the model emits: each `UNION` branch, and
/// only CTEs and subqueries the output reads. See
/// [`emitted_rows_window_bound`].
///
/// When the SQL does not parse, the check cannot see where the placeholders
/// are, so it fails closed with E024.
fn check_time_interval_placeholders(model_name: &str, sql: &str) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return vec![
            Diagnostic::error(
                E024,
                model_name,
                "time_interval model SQL does not parse, so Rocky cannot verify that \
                 `@start_date` and `@end_date` bound the rows it emits",
            )
            .with_suggestion(
                "Write the model as one SELECT with `WHERE <ts_col> >= @start_date AND \
                 <ts_col> < @end_date`",
            ),
        ];
    };
    let bound = emitted_rows_window_bound(&statements);
    let mut diags = Vec::new();
    match (bound.start, bound.end) {
        (true, true) => {}
        (false, false) => {
            diags.push(
                Diagnostic::error(
                    E024,
                    model_name,
                    "time_interval model must filter every row it emits on both \
                     `@start_date` and `@end_date` (in a WHERE, HAVING, QUALIFY or inner \
                     JOIN ON clause of each UNION branch, or of a CTE or subquery the output \
                     reads; a comment, a string literal, an unused CTE or the SELECT list \
                     does not count)",
                )
                .with_suggestion(
                    "Add `WHERE <ts_col> >= @start_date AND <ts_col> < @end_date` to the model SQL",
                ),
            );
        }
        (true, false) => {
            diags.push(
                Diagnostic::error(
                    E024,
                    model_name,
                    "time_interval model filters on `@start_date` but not `@end_date` — partition window is unbounded above",
                )
                .with_suggestion("Add `AND <ts_col> < @end_date` to bound the upper end of the window"),
            );
        }
        (false, true) => {
            diags.push(
                Diagnostic::error(
                    E024,
                    model_name,
                    "time_interval model filters on `@end_date` but not `@start_date` — partition window is unbounded below",
                )
                .with_suggestion("Add `<ts_col> >= @start_date AND` to bound the lower end of the window"),
            );
        }
    }
    diags
}

/// Which window placeholders bound a row set. `start` is set when every row
/// in the set passed a row filter on `@start_date`; `end` likewise for
/// `@end_date`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WindowBound {
    start: bool,
    end: bool,
}

impl WindowBound {
    /// Rows that one of two bounded inputs restricts (an inner join, an
    /// `INTERSECT`, a filter on top of a source).
    fn or(self, other: Self) -> Self {
        Self {
            start: self.start || other.start,
            end: self.end || other.end,
        }
    }

    /// Rows that both inputs contribute unfiltered (each `UNION` branch, each
    /// side of a `FULL OUTER JOIN`).
    fn and(self, other: Self) -> Self {
        Self {
            start: self.start && other.start,
            end: self.end && other.end,
        }
    }
}

/// The CTEs visible at a point in the query, innermost last: lowercased name
/// and the bound of the rows it holds.
type CteScope = Vec<(String, WindowBound)>;

/// The window bound of every row the model emits (#2233 follow-up).
///
/// A placeholder bounds the output only when each row source that reaches
/// the final `SELECT` passes it:
///
/// - each `UNION` branch must be bounded; `INTERSECT` needs one side,
///   `EXCEPT` the left side;
/// - a CTE counts only where the output reads it, so a filter in an unused
///   CTE bounds nothing;
/// - a `SELECT` is bounded by its own row filters (`WHERE`, `HAVING`,
///   `QUALIFY`, `PREWHERE`) or by its `FROM` tree;
/// - an inner or semi join is bounded when either side or its `ON` is; an
///   outer or anti join only by the side it keeps every row of;
/// - a scalar subquery in the `SELECT` list is not a row source, so a filter
///   inside it bounds nothing;
/// - a base table, a table function (including a date spine such as
///   `GENERATE_SERIES(@start_date, @end_date)`), `UNNEST` and `VALUES` are
///   unbounded. A spine's end is inclusive in most warehouses, so its rows
///   spill into the next partition; filter it with a `WHERE` instead.
///
/// Anything but a single query statement is unbounded (fail closed).
fn emitted_rows_window_bound(statements: &[Statement]) -> WindowBound {
    match statements {
        [Statement::Query(query)] => query_window_bound(query, &mut CteScope::new()),
        _ => WindowBound::default(),
    }
}

fn query_window_bound(query: &ast::Query, scope: &mut CteScope) -> WindowBound {
    let outer = scope.len();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            // A CTE sees the CTEs declared before it. A recursive CTE's
            // reference to itself resolves to whatever was in scope before
            // it (usually a base table), so its recursive branch is
            // unbounded unless filtered.
            let bound = query_window_bound(&cte.query, scope);
            scope.push((cte.alias.name.value.to_lowercase(), bound));
        }
    }
    let bound = set_expr_window_bound(&query.body, scope);
    scope.truncate(outer);
    bound
}

fn set_expr_window_bound(body: &SetExpr, scope: &mut CteScope) -> WindowBound {
    match body {
        SetExpr::Select(select) => select_window_bound(select, scope),
        SetExpr::Query(query) => query_window_bound(query, scope),
        SetExpr::SetOperation {
            op, left, right, ..
        } => {
            let left = set_expr_window_bound(left, scope);
            let right = set_expr_window_bound(right, scope);
            match op {
                ast::SetOperator::Union => left.and(right),
                ast::SetOperator::Intersect => left.or(right),
                ast::SetOperator::Except | ast::SetOperator::Minus => left,
            }
        }
        // VALUES, TABLE and DML bodies carry no row filter.
        _ => WindowBound::default(),
    }
}

fn select_window_bound(select: &ast::Select, scope: &mut CteScope) -> WindowBound {
    let mut bound = WindowBound::default();
    for filter in [
        &select.selection,
        &select.having,
        &select.qualify,
        &select.prewhere,
    ]
    .into_iter()
    .flatten()
    {
        bound = bound.or(filter_window_bound(filter, scope));
    }
    // Comma-separated `FROM` items are a cross join: one bounded item bounds
    // the product.
    for table in &select.from {
        bound = bound.or(table_with_joins_window_bound(table, scope));
    }
    bound
}

fn table_with_joins_window_bound(table: &ast::TableWithJoins, scope: &mut CteScope) -> WindowBound {
    use ast::JoinOperator as J;
    let mut acc = table_factor_window_bound(&table.relation, scope);
    for join in &table.joins {
        let right = table_factor_window_bound(&join.relation, scope);
        acc = match &join.join_operator {
            // Every output row matches a row on both sides and the `ON`.
            J::Join(c)
            | J::Inner(c)
            | J::StraightJoin(c)
            | J::CrossJoin(c)
            | J::Semi(c)
            | J::LeftSemi(c)
            | J::RightSemi(c) => acc.or(right).or(join_on_window_bound(c, scope)),
            J::CrossApply => acc.or(right),
            // These keep every row of the left side.
            J::Left(_)
            | J::LeftOuter(_)
            | J::Anti(_)
            | J::LeftAnti(_)
            | J::OuterApply
            | J::ArrayJoin
            | J::LeftArrayJoin
            | J::InnerArrayJoin => acc,
            // These keep every row of the right side.
            J::Right(_) | J::RightOuter(_) | J::RightAnti(_) => right,
            // FULL OUTER keeps both sides; any other join must be bounded
            // on both sides (fail closed).
            _ => acc.and(right),
        };
    }
    acc
}

fn join_on_window_bound(constraint: &ast::JoinConstraint, scope: &mut CteScope) -> WindowBound {
    match constraint {
        ast::JoinConstraint::On(on) => filter_window_bound(on, scope),
        _ => WindowBound::default(),
    }
}

fn table_factor_window_bound(relation: &TableFactor, scope: &mut CteScope) -> WindowBound {
    match relation {
        // A one-part name may be a CTE in scope; anything else is a base
        // table. `args` marks a table function, which is never a CTE.
        TableFactor::Table {
            name, args: None, ..
        } => match name.0.as_slice() {
            [ast::ObjectNamePart::Identifier(ident)] => {
                let key = ident.value.to_lowercase();
                scope
                    .iter()
                    .rev()
                    .find(|(name, _)| *name == key)
                    .map(|(_, bound)| *bound)
                    .unwrap_or_default()
            }
            _ => WindowBound::default(),
        },
        TableFactor::Derived { subquery, .. } => query_window_bound(subquery, scope),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_window_bound(table_with_joins, scope),
        TableFactor::Pivot { table, .. } | TableFactor::Unpivot { table, .. } => {
            table_factor_window_bound(table, scope)
        }
        // Table functions (date spines included), UNNEST, JSON_TABLE, ...:
        // the window does not filter their rows.
        _ => WindowBound::default(),
    }
}

/// The window placeholders a row filter bounds its rows by (#2233 follow-up).
///
/// A placeholder counts only in a top-level `AND` conjunct of this filter
/// that bounds a column on the right side of the window:
///
/// - `<col> >= @start_date` (or `>`, or `=`), or `@start_date <= <col>`;
/// - `<col> < @end_date` (or `<=`, or `=`), or `@end_date > <col>`;
/// - `<col> BETWEEN @start_date AND @end_date`;
/// - `<col> IN (<subquery>)`, when the subquery's rows are bounded, so a
///   semi-join can read a bounded CTE.
///
/// The column side may wrap the column in a cast or a function
/// (`DATE(ts)`); the placeholder side may cast it or shift it by a constant
/// (`@start_date - INTERVAL 1 DAY`). The bare placeholder and the
/// whole-literal quoted form `'@start_date'` count alike, because the runtime
/// substitutes both.
///
/// An `OR` bounds its rows only where every branch does, by this same rule:
/// `(a >= @start_date AND a < @end_date) OR (b >= @start_date AND b < @end_date)`
/// is bounded.
///
/// Anything else bounds nothing, because it can let rows outside the window
/// through: an `OR` with an unbounded branch or a conjunct under `NOT`
/// (`ts >= @start_date OR 1=1`, `@start_date IS NULL OR ...`), a comparison facing the wrong way
/// (`ts <= @start_date`), `NOT IN`, and `EXISTS`. A subquery is never
/// searched for placeholders: its filter bounds its own rows, not this one's.
fn filter_window_bound(filter: &Expr, scope: &mut CteScope) -> WindowBound {
    let mut conjuncts = Vec::new();
    top_level_conjuncts(filter, &mut conjuncts);
    conjuncts
        .into_iter()
        .fold(WindowBound::default(), |bound, conjunct| {
            bound.or(conjunct_window_bound(conjunct, scope))
        })
}

/// Whether the model emits no row whose `column` is NULL, because its WHERE
/// requires a comparison on that column to be TRUE.
///
/// Holds only for the narrow shape where that is certain: one SELECT (no set
/// operation), whose projection carries `column` as a bare column reference
/// of the same name (so the WHERE reads the same value the output carries),
/// and whose WHERE has a top-level `AND` conjunct `column <op> <expr>` or
/// `<expr> <op> column` with `<op>` one of `=`, `<>`, `<`, `<=`, `>`, `>=`.
/// Under SQL three-valued logic such a comparison is NULL, never TRUE, when
/// `column` is NULL, so the row is filtered out. Anything else — a parse
/// failure, a set operation, an aliased or computed projection, a comparison
/// under `OR`, a `GROUP BY` with `ROLLUP`, `CUBE` or `GROUPING SETS` (their
/// subtotal rows carry a NULL key) — answers `false`.
fn where_rejects_null_column(sql: &str, column: &str) -> bool {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return false;
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return false;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    // ROLLUP, CUBE and GROUPING SETS add subtotal rows whose grouping
    // columns are NULL, after the WHERE has run.
    let adds_subtotal_rows = match &select.group_by {
        ast::GroupByExpr::All(modifiers) => !modifiers.is_empty(),
        ast::GroupByExpr::Expressions(exprs, modifiers) => {
            !modifiers.is_empty()
                || exprs.iter().any(|expr| {
                    matches!(
                        expr,
                        Expr::Rollup(_) | Expr::Cube(_) | Expr::GroupingSets(_)
                    )
                })
        }
    };
    if adds_subtotal_rows {
        return false;
    }
    // A column reference as its lowercased name parts, when its last part is
    // `column`. The WHERE side must spell the same reference as the
    // projection, so `SELECT p.d ... WHERE o.d > x` does not count.
    let column_ref = |expr: &Expr| -> Option<Vec<String>> {
        let parts: Vec<String> = match expr {
            Expr::Identifier(ident) => vec![ident.value.to_lowercase()],
            Expr::CompoundIdentifier(idents) => {
                idents.iter().map(|i| i.value.to_lowercase()).collect()
            }
            _ => return None,
        };
        parts
            .last()
            .is_some_and(|last| last.eq_ignore_ascii_case(column))
            .then_some(parts)
    };
    let projected: Vec<Vec<String>> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) => column_ref(expr),
            SelectItem::ExprWithAlias { expr, alias } => alias
                .value
                .eq_ignore_ascii_case(column)
                .then(|| column_ref(expr))
                .flatten(),
            SelectItem::ExprWithAliases { .. }
            | SelectItem::QualifiedWildcard(..)
            | SelectItem::Wildcard(..) => None,
        })
        .collect();
    if projected.is_empty() {
        return false;
    }
    let Some(selection) = &select.selection else {
        return false;
    };
    let is_projected = |expr: &Expr| column_ref(expr).is_some_and(|r| projected.contains(&r));
    let mut conjuncts = Vec::new();
    top_level_conjuncts(selection, &mut conjuncts);
    conjuncts.into_iter().any(|conjunct| match conjunct {
        Expr::BinaryOp { left, op, right } => {
            matches!(
                op,
                ast::BinaryOperator::Eq
                    | ast::BinaryOperator::NotEq
                    | ast::BinaryOperator::Lt
                    | ast::BinaryOperator::LtEq
                    | ast::BinaryOperator::Gt
                    | ast::BinaryOperator::GtEq
            ) && (is_projected(left) || is_projected(right))
        }
        _ => false,
    })
}

/// Split `expr` on top-level `AND`, looking through parentheses.
fn top_level_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            top_level_conjuncts(left, out);
            top_level_conjuncts(right, out);
        }
        Expr::Nested(inner) => top_level_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// One edge of the partition window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WindowEdge {
    Start,
    End,
}

fn conjunct_window_bound(conjunct: &Expr, scope: &mut CteScope) -> WindowBound {
    let mut bound = WindowBound::default();
    match conjunct {
        // Every row satisfies at least one branch, so the branches together
        // bound the rows only where each branch does.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Or,
            right,
        } => bound = filter_window_bound(left, scope).and(filter_window_bound(right, scope)),
        Expr::BinaryOp { left, op, right } => {
            // Normalise to `<column> <op> <placeholder>`.
            let (op, edge) = if is_column_term(left) {
                match placeholder_term(right) {
                    Some(edge) => (op.clone(), edge),
                    None => return bound,
                }
            } else if is_column_term(right) {
                match (placeholder_term(left), flip_comparison(op)) {
                    (Some(edge), Some(op)) => (op, edge),
                    _ => return bound,
                }
            } else {
                return bound;
            };
            use ast::BinaryOperator as B;
            match (edge, op) {
                (WindowEdge::Start, B::GtEq | B::Gt | B::Eq) => bound.start = true,
                (WindowEdge::End, B::Lt | B::LtEq | B::Eq) => bound.end = true,
                _ => {}
            }
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } if is_column_term(expr) => {
            bound.start = placeholder_term(low) == Some(WindowEdge::Start);
            bound.end = placeholder_term(high) == Some(WindowEdge::End);
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } if is_column_term(expr) => bound = query_window_bound(subquery, scope),
        _ => {}
    }
    bound
}

/// The comparison with its operands swapped: `a < b` is `b > a`. `None` for
/// an operator that is not an ordering comparison.
fn flip_comparison(op: &ast::BinaryOperator) -> Option<ast::BinaryOperator> {
    use ast::BinaryOperator as B;
    Some(match op {
        B::Lt => B::Gt,
        B::LtEq => B::GtEq,
        B::Gt => B::Lt,
        B::GtEq => B::LtEq,
        B::Eq => B::Eq,
        _ => return None,
    })
}

/// Which window placeholder `expr` is, allowing a cast, a function of it
/// alone (`DATE(@start_date)`, `DATEADD(day, -1, @start_date)`), and a shift
/// by a constant (`@start_date - INTERVAL 1 DAY`).
fn placeholder_term(expr: &Expr) -> Option<WindowEdge> {
    match expr {
        Expr::Value(v) => placeholder_token(&v.value),
        Expr::TypedString(ts) => placeholder_token(&ts.value.value),
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => placeholder_term(inner),
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Plus | ast::BinaryOperator::Minus,
            right,
        } if is_constant(right) => placeholder_term(left),
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Plus,
            right,
        } if is_constant(left) => placeholder_term(right),
        Expr::Function(function) => {
            let args = plain_function_args(function)?;
            let mut edge = None;
            for arg in args {
                match placeholder_term(arg) {
                    Some(found) if edge.is_none() => edge = Some(found),
                    Some(_) => return None,
                    None if is_constant(arg) || is_date_part(arg) => {}
                    None => return None,
                }
            }
            edge
        }
        _ => None,
    }
}

fn placeholder_token(value: &ast::Value) -> Option<WindowEdge> {
    let token = match value {
        ast::Value::Placeholder(p) => p.as_str(),
        ast::Value::SingleQuotedString(s) => s.as_str(),
        _ => return None,
    };
    match token {
        "@start_date" => Some(WindowEdge::Start),
        "@end_date" => Some(WindowEdge::End),
        _ => None,
    }
}

/// A column, possibly cast, wrapped in a function of it alone
/// (`DATE_TRUNC('day', ts)`), or shifted by a constant. Never a subquery and
/// never a placeholder.
fn is_column_term(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => is_column_term(inner),
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Plus | ast::BinaryOperator::Minus,
            right,
        } => {
            (is_column_term(left) && is_constant(right))
                || (is_constant(left) && is_column_term(right))
        }
        Expr::Function(function) => {
            let Some(args) = plain_function_args(function) else {
                return false;
            };
            let mut columns = 0;
            for arg in args {
                if is_column_term(arg) {
                    columns += 1;
                } else if !(is_constant(arg) || is_date_part(arg)) {
                    return false;
                }
            }
            columns > 0
        }
        _ => false,
    }
}

/// A literal that is not a window placeholder, possibly cast, negated or
/// written as an interval.
fn is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Value(v) => {
            placeholder_token(&v.value).is_none() && !matches!(v.value, ast::Value::Placeholder(_))
        }
        Expr::TypedString(ts) => placeholder_token(&ts.value.value).is_none(),
        Expr::Interval(interval) => is_constant(&interval.value),
        Expr::Nested(inner) | Expr::Cast { expr: inner, .. } => is_constant(inner),
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus | ast::UnaryOperator::Plus,
            expr,
        } => is_constant(expr),
        _ => false,
    }
}

/// A bare date-part keyword in a function argument (`DATEADD(day, ...)`),
/// which parses as an identifier.
fn is_date_part(expr: &Expr) -> bool {
    const PARTS: &[&str] = &[
        "year", "quarter", "month", "week", "day", "hour", "minute", "second",
    ];
    matches!(expr, Expr::Identifier(ident)
        if PARTS.iter().any(|p| ident.value.eq_ignore_ascii_case(p)))
}

/// The positional argument expressions of a plain scalar call, or `None` for
/// a call this check does not look through: a window, `FILTER`, `WITHIN
/// GROUP`, a subquery argument, or a wildcard or named argument.
fn plain_function_args(function: &ast::Function) -> Option<Vec<&Expr>> {
    if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() {
        return None;
    }
    if !matches!(function.parameters, ast::FunctionArguments::None) {
        return None;
    }
    let ast::FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if !list.clauses.is_empty() {
        return None;
    }
    list.args
        .iter()
        .map(|arg| match arg {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
            _ => None,
        })
        .collect()
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
        // `COUNT(DISTINCT x)` reaches here as "COUNT": lineage keys on the
        // function name, never on its DISTINCT modifier.
        _ => (RockyType::Unknown, true),
    }
}

/// Whether the source column a lineage edge reads has a known type.
fn edge_input_is_known(
    edge: &crate::semantic::LineageEdge,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    col_index: &HashMap<String, HashMap<String, usize>>,
    relation_key: &dyn Fn(&str) -> String,
) -> bool {
    let source = relation_key(&edge.source.model);
    typed_models
        .get(&source)
        .and_then(|columns| {
            col_index
                .get(&source)
                .and_then(|index| index.get(&*edge.source.column))
                .map(|&index| &columns[index])
        })
        .is_some_and(|input| input.data_type != RockyType::Unknown)
}

/// Whether the nullable bit of `(model, column)` rests on known types: every
/// lineage edge on the way back to its sources reads a column whose type is
/// known. A column with no edges (a literal, a cast of a literal) has nothing
/// to prove its nullable bit, so it is not proven. That holds for a column of
/// an upstream model the trace reaches, not only the model's own column.
///
/// A column typed by a cast over an unknown input has a type but only a
/// guessed nullable bit (nullable, the safe answer for a contract or a
/// `NOT NULL` check, but not evidence). A check that would refuse a model
/// because the column "is nullable" must ask this first.
fn nullability_is_proven(
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    col_index: &HashMap<String, HashMap<String, usize>>,
    relation_key: &dyn Fn(&str) -> String,
    model: &str,
    column: &str,
) -> bool {
    let edges = graph.trace_column(model, column);
    if edges.is_empty() {
        return false;
    }
    // A column of any project model that the trace reaches but that has no
    // edge of its own (`SELECT CAST('2024-01-01' AS DATE) AS d`) has typed
    // output and a guessed nullable bit, in an upstream model just as in this
    // one. An external source column has no model in the graph; its type is
    // checked by `edge_input_is_known`.
    let reaches_edgeless_column = edges.iter().any(|edge| {
        graph.model_schema(&edge.source.model).is_some()
            && graph
                .producing_edge(&edge.source.model, &edge.source.column)
                .is_none()
    });
    !reaches_edgeless_column
        && edges
            .into_iter()
            .all(|edge| edge_input_is_known(edge, typed_models, col_index, relation_key))
}

/// Refine explicit casts by parsing their target types from the model SQL.
///
/// A projection that is itself a cast takes its target type in the inference
/// merge in `compute_model_typecheck`, whatever its input is: a cast's output type
/// is its target (so `CAST(id AS STRING) AS id` resolves to `String`, not the
/// source's pre-cast type — see #1145 — and still does when `id` has no known
/// type). This pass is the fallback for a cast edge that merge left `Unknown`
/// and whose input type is known. It refines only the *type*; the nullable
/// bit was already set in Step 1 (a `TryCast` output is nullable regardless
/// of input — #1148) and is left untouched here.
///
/// [`Cast`]: rocky_sql::lineage::TransformKind::Cast
/// [`TryCast`]: rocky_sql::lineage::TransformKind::TryCast
///
/// One deliberate restriction, erring toward `Unknown` — the safe "cannot
/// type-check" state, where a contract on the column is skipped rather than
/// validated against a fabricated type:
///
/// - **No alias-based fallback.** A remaining `Unknown` column is not resolved
///   by matching its output name against an upstream column. That lookup was
///   unsound: an expression aliased back to a source name (e.g.
///   `amount_cents / 100 AS amount`) would inherit the source type even though
///   the expression can change it. Such columns are left `Unknown`.
///
/// A cast whose input is unknown is typed by the merge, not left `Unknown`:
/// the target is stated in the SQL. Its nullability is not guessed non-null —
/// an unknown input keeps the column nullable, since a non-null guess would
/// pass a NULL under a `nullable = false` contract. A bare `DECIMAL` target
/// names no digits and stays `Unknown` (#1721).
fn enhanced_inference(
    model_name: &str,
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    col_index: &HashMap<String, HashMap<String, usize>>,
    relation_key: &dyn Fn(&str) -> String,
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

        if !edge_input_is_known(edge, typed_models, col_index, relation_key) {
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
pub(crate) fn infer_expr_type(expr: &Expr, scope: &TypeScope) -> (RockyType, bool) {
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
            let target = cast_target_type(data_type, &scope.target);
            let nullable = match kind {
                ast::CastKind::Cast | ast::CastKind::DoubleColon => {
                    let (source, source_nullable) = infer_expr_type(expr, scope);
                    // A plain cast can still yield NULL: Spark/Databricks with
                    // ANSI off, and some dialects' `::`, return NULL on a value
                    // that does not convert (#2299). Say nullable unless the
                    // cast provably cannot fail.
                    source_nullable
                        || (cast_can_fail(&source, data_type, &target)
                            && !integer_literal_fits(expr, data_type, &target))
                }
                ast::CastKind::TryCast | ast::CastKind::SafeCast => true,
            };
            (target, nullable)
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

        // IN list → Boolean. Per SQL 3VL the result is NULL only when the
        // operand is NULL, or a list item is NULL and nothing matched; so it is
        // nullable iff the operand or any item is. `NOT IN` is the same.
        Expr::InList { expr, list, .. } => {
            let nullable =
                infer_expr_type(expr, scope).1 || list.iter().any(|e| infer_expr_type(e, scope).1);
            (RockyType::Boolean, nullable)
        }

        // IN subquery → Boolean; the subquery's nullability is not modelled.
        Expr::InSubquery { .. } => (RockyType::Boolean, true),

        // EXISTS → Boolean
        Expr::Exists { .. } => (RockyType::Boolean, false),

        // BETWEEN → Boolean
        Expr::Between { .. } => (RockyType::Boolean, true),

        // Subquery → Unknown (would need recursive analysis)
        Expr::Subquery(_) => (RockyType::Unknown, true),

        // Nested expression
        Expr::Nested(inner) => infer_expr_type(inner, scope),

        // `SUBSTRING(x FROM a FOR b)`, `SUBSTR(x, a)` and `CEIL`/`FLOOR` parse
        // as their own nodes, not function calls. Same rule as the entries in
        // `NULL_PRESERVING_SCALARS` (#2298); a date `CEIL(x TO DAY)` stays
        // Unknown.
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            // Postgres regex forms (`FROM 'pattern'`, `SIMILAR`) return NULL
            // on no match, so the start and length must be integer typed.
            let nullable = infer_expr_type(expr, scope).1
                || [substring_from, substring_for]
                    .into_iter()
                    .flatten()
                    .any(|e| !is_non_null_of(e, scope, RockyType::is_integer));
            (RockyType::String, nullable)
        }
        Expr::Ceil { expr, field } | Expr::Floor { expr, field } => {
            if matches!(
                field,
                ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime)
            ) {
                let (ty, nullable) = infer_expr_type(expr, scope);
                let exact = ty.is_integer() || ty.is_float();
                (ty, nullable || !exact)
            } else {
                (RockyType::Unknown, true)
            }
        }

        // `TRIM(...)` parses as its own node, not a function call. NULL only
        // for a NULL operand or trim character (#2298).
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            let nullable = infer_expr_type(expr, scope).1
                || trim_what
                    .as_deref()
                    .is_some_and(|e| infer_expr_type(e, scope).1)
                || trim_characters
                    .as_deref()
                    .is_some_and(|es| es.iter().any(|e| infer_expr_type(e, scope).1));
            (RockyType::String, nullable)
        }

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

        // Date arithmetic is dialect-dependent: `DATE - DATE` is a BIGINT in
        // DuckDB, an INTERVAL in Databricks, and `DATE - 5` is a DATE in
        // DuckDB. `common_supertype` would call `DATE - DATE` a DATE.
        // An `Unknown` operand may be a date too, so the result is unknown
        // (`common_supertype` would take the other side's type).
        ast::BinaryOperator::Plus | ast::BinaryOperator::Minus
            if left_type.is_temporal()
                || right_type.is_temporal()
                || left_type == RockyType::Unknown
                || right_type == RockyType::Unknown =>
        {
            (RockyType::Unknown, nullable)
        }
        // Arithmetic → numeric promotion
        ast::BinaryOperator::Plus | ast::BinaryOperator::Minus | ast::BinaryOperator::Multiply => {
            let result_type = crate::types::common_supertype(&left_type, &right_type)
                .unwrap_or(RockyType::Unknown);
            (result_type, nullable)
        }
        // Division and modulo by zero return NULL in several dialects (DuckDB,
        // Spark with ANSI mode off), so the result is nullable even over
        // non-null operands (#2295).
        ast::BinaryOperator::Divide | ast::BinaryOperator::Modulo => {
            let result_type = crate::types::common_supertype(&left_type, &right_type)
                .unwrap_or(RockyType::Unknown);
            (result_type, true)
        }
        ast::BinaryOperator::DuckIntegerDivide | ast::BinaryOperator::MyIntegerDivide => {
            (RockyType::Unknown, true)
        }

        // String concatenation
        ast::BinaryOperator::StringConcat => (RockyType::String, nullable),

        _ => (RockyType::Unknown, nullable),
    }
}

/// Scalar functions whose result is NULL only when an argument is NULL, so a
/// call over non-null arguments is non-null (#2298).
///
/// Each entry is non-null over non-null arguments in every dialect Rocky
/// targets (DuckDB, Databricks/Spark, Snowflake, BigQuery, Trino), or the
/// function is left out and stays nullable. Dialect evidence:
///
/// - `UPPER`, `LOWER`, `REVERSE`, `TRIM`, `LTRIM`, `RTRIM`: standard string
///   functions; NULL in, NULL out, and no dialect here maps an empty result to
///   NULL (Oracle does, and is not a target).
/// - `LENGTH`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `OCTET_LENGTH`: return a
///   count; NULL only for a NULL argument.
/// - `SUBSTRING`, `SUBSTR`: an out-of-range start gives an empty string, not
///   NULL (ANSI, DuckDB, Spark, Snowflake, Trino, BigQuery). Only when the
///   start and length are inferred integers: Postgres regex forms
///   (`SUBSTRING(x FROM 'pat')`, `SUBSTRING(x, 'pat')`, `SIMILAR`) return NULL
///   on no match.
/// - `LPAD`, `RPAD`: NULL only for a NULL argument. A bad pad (empty string in
///   BigQuery or Trino) raises an error; it does not return NULL.
/// - `CONCAT`: Postgres, DuckDB and Snowflake-style `CONCAT` skip NULL
///   arguments, Spark and BigQuery return NULL for any NULL argument. Either
///   way, all-non-null arguments give a non-null result, so no dialect switch
///   is needed. `CONCAT_WS` is left out: with a NULL separator it returns NULL
///   and the dialects disagree on the rest.
/// - `ABS`, `SIGN`, `CEIL`, `CEILING`, `FLOOR`, `ROUND`: NULL only for a NULL
///   argument over inferred integer and float types. Over `DECIMAL`, Spark with
///   ANSI mode off returns NULL when the rounded value overflows the precision.
///   Over a string, Spark with ANSI off casts a non-numeric value to NULL.
///   Unknown proves nothing. All three stay nullable (see `args_all_non_null`).
///
/// Dialects checked: ANSI, DuckDB, Spark/Databricks, Snowflake, Trino, BigQuery
/// and Postgres (regex SUBSTRING). SQL Server and ClickHouse were not checked
/// against these rules; the type guards (integer or float arguments only) are
/// what keep the claim narrow there.
///
/// Not listed, so nullable: `LEFT`/`RIGHT` (a negative length is NULL in some
/// dialects), `REPLACE`, `INITCAP`, `TRUNC`/`TRUNCATE` (date forms), `POSITION`
/// family, `POWER`/`SQRT`/`LOG`/`LN` (domain errors return NULL in DuckDB and
/// Spark), `NULLIF`, `/` and `%`.
const NULL_PRESERVING_SCALARS: &[&str] = &[
    "UPPER",
    "LOWER",
    "REVERSE",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "LENGTH",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "OCTET_LENGTH",
    "SUBSTRING",
    "SUBSTR",
    "LPAD",
    "RPAD",
    "CONCAT",
    "ABS",
    "SIGN",
    "CEIL",
    "CEILING",
    "FLOOR",
    "ROUND",
];

/// True when `expr` is non-null and its inferred type satisfies `accept`.
/// `Unknown` is never accepted by the integer and float predicates.
fn is_non_null_of(expr: &ast::Expr, scope: &TypeScope, accept: fn(&RockyType) -> bool) -> bool {
    let (ty, nullable) = infer_expr_type(expr, scope);
    !nullable && accept(&ty)
}

/// True when `name` is a null-preserving scalar and the call is a plain
/// scalar call whose every argument is a non-null expression.
///
/// Anything unusual (no arguments, a named or wildcard argument, an `OVER`,
/// `FILTER` or `WITHIN GROUP` clause, a non-integer/float argument to a numeric
/// function, a non-integer substring or pad length) returns false, which keeps the result nullable.
fn args_all_non_null(name: &str, func: &ast::Function, scope: &TypeScope) -> bool {
    if !NULL_PRESERVING_SCALARS.contains(&name)
        || func.over.is_some()
        || func.filter.is_some()
        || !func.within_group.is_empty()
        || func.null_treatment.is_some()
    {
        return false;
    }
    let ast::FunctionArguments::List(arg_list) = &func.args else {
        return false;
    };
    if arg_list.args.is_empty()
        || arg_list.duplicate_treatment.is_some()
        || !arg_list.clauses.is_empty()
    {
        return false;
    }
    let numeric = matches!(
        name,
        "ABS" | "SIGN" | "CEIL" | "CEILING" | "FLOOR" | "ROUND"
    );
    arg_list.args.iter().enumerate().all(|(i, arg)| {
        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg else {
            return false;
        };
        // Numeric functions: every argument must be an inferred integer or
        // float. Spark with ANSI off casts a non-numeric string to NULL, a
        // decimal can overflow to NULL, and Unknown proves nothing.
        // SUBSTRING/SUBSTR start and length, and LPAD/RPAD length, must be
        // integers: Postgres `SUBSTRING(x, 'pattern')` returns NULL on no match.
        if numeric {
            is_non_null_of(expr, scope, |t| t.is_integer() || t.is_float())
        } else if matches!(name, "SUBSTRING" | "SUBSTR") && i > 0
            || matches!(name, "LPAD" | "RPAD") && i == 1
        {
            is_non_null_of(expr, scope, RockyType::is_integer)
        } else {
            !infer_expr_type(expr, scope).1
        }
    })
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

        // String functions. The entries in `NULL_PRESERVING_SCALARS` are
        // non-null when every argument is non-null (#2298); the rest stay
        // nullable.
        "CONCAT" | "CONCAT_WS" | "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
        | "SUBSTRING" | "SUBSTR" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REVERSE" | "INITCAP" => {
            (RockyType::String, !args_all_non_null(&name, func, scope))
        }
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" => {
            (RockyType::Int64, !args_all_non_null(&name, func, scope))
        }
        "POSITION" | "STRPOS" | "INSTR" => (RockyType::Int64, true),

        // Numeric functions
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNCATE" | "TRUNC" => {
            let arg_type = first_arg_type(func, scope);
            let nullable = !args_all_non_null(&name, func, scope);
            (arg_type, nullable)
        }
        "SIGN" => (RockyType::Int32, !args_all_non_null(&name, func, scope)),
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
        // A project UDF (`functions/`) types to its declared return type.
        _ => crate::udf::infer_active_call(func, &|expr| infer_expr_type(expr, scope))
            .unwrap_or((RockyType::Unknown, true)),
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

/// Whether `sql` contains a `UNION`, `INTERSECT` or `EXCEPT` keyword as a
/// whole word, in any case. Over-approximates (a keyword inside a string or
/// an identifier matches too); the caller only uses it to run inference.
fn sql_mentions_set_operation(sql: &str) -> bool {
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|w| {
            w.eq_ignore_ascii_case("union")
                || w.eq_ignore_ascii_case("intersect")
                || w.eq_ignore_ascii_case("except")
        })
}

/// Whether a plain `CAST(x AS target)` can return NULL (or fail) for a
/// non-null `x` of type `source`.
///
/// Only the conversions listed as safe return `false`; everything else,
/// including an `Unknown` source, is treated as fallible. Inference may wrongly
/// say nullable, never wrongly non-null (#2299).
fn cast_can_fail(source: &RockyType, target_sql: &ast::DataType, target: &RockyType) -> bool {
    use RockyType as T;
    // A VARIANT can hold a JSON null, which casts to SQL NULL (Databricks,
    // Snowflake), and an Unknown source may be one. Both stay fallible.
    if matches!(source, T::Variant | T::Unknown) {
        return true;
    }
    // Any other type renders as text.
    if *target == T::String {
        return false;
    }
    // `sql_type_to_rocky` folds TINYINT/SMALLINT into Int32, so the width of
    // the target is lost; a cast to either can overflow whatever the source.
    if matches!(
        target_sql,
        ast::DataType::TinyInt(_) | ast::DataType::SmallInt(_)
    ) {
        return *source != T::Boolean;
    }
    match (source, target) {
        (T::Unknown, _) | (_, T::Unknown) => true,
        (
            T::Decimal {
                precision: sp,
                scale: ss,
            },
            T::Decimal {
                precision: tp,
                scale: ts,
            },
        ) => {
            // Safe only when no integer digit and no fractional digit is lost.
            let source_int_digits = i16::from(*sp) - i16::from(*ss);
            let target_int_digits = i16::from(*tp) - i16::from(*ts);
            target_int_digits < source_int_digits || ts < ss
        }
        (a, b) if a == b => false,
        // An integer fits a DECIMAL with as many integer digits as the
        // integer's widest value: 10 for a 32-bit one, 19 for a 64-bit one.
        // Snowflake's `INTEGER` / `BIGINT` are `NUMBER(38,0)` (#2333).
        (
            T::Int32,
            T::Decimal {
                precision: tp,
                scale: ts,
            },
        ) => i16::from(*tp) - i16::from(*ts) < 10,
        (
            T::Int64,
            T::Decimal {
                precision: tp,
                scale: ts,
            },
        ) => i16::from(*tp) - i16::from(*ts) < 19,
        (T::Boolean, T::Int32 | T::Int64 | T::Float32 | T::Float64) => false,
        (T::Int32, T::Int64 | T::Float32 | T::Float64) => false,
        (T::Int64, T::Float32 | T::Float64) => false,
        (T::Float32, T::Float64) => false,
        (T::Decimal { .. }, T::Float32 | T::Float64) => false,
        (T::Date, T::Timestamp | T::TimestampNtz) => false,
        (T::Timestamp | T::TimestampNtz, T::Date | T::Timestamp | T::TimestampNtz) => false,
        _ => true,
    }
}

/// Whether `expr` is an integer literal that fits the cast target, so the cast
/// cannot fail (`CAST(0 AS DECIMAL(18,2))`, `CAST(1 AS SMALLINT)`).
fn integer_literal_fits(expr: &Expr, target_sql: &ast::DataType, target: &RockyType) -> bool {
    match expr {
        Expr::Nested(inner) => integer_literal_fits(inner, target_sql, target),
        Expr::Value(val) => {
            let ast::Value::Number(text, _) = &val.value else {
                return false;
            };
            let Ok(value) = text.parse::<i128>() else {
                return false;
            };
            let in_range = |min: i128, max: i128| (min..=max).contains(&value);
            match target_sql {
                ast::DataType::TinyInt(_) => in_range(i8::MIN.into(), i8::MAX.into()),
                ast::DataType::SmallInt(_) => in_range(i16::MIN.into(), i16::MAX.into()),
                ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::MediumInt(_) => {
                    in_range(i32::MIN.into(), i32::MAX.into())
                }
                ast::DataType::BigInt(_) => in_range(i64::MIN.into(), i64::MAX.into()),
                _ => match target {
                    RockyType::Decimal { precision, scale } => {
                        let integer_digits = if value == 0 {
                            0
                        } else {
                            value.unsigned_abs().to_string().len()
                        };
                        integer_digits + usize::from(*scale) <= usize::from(*precision)
                    }
                    _ => false,
                },
            }
        }
        _ => false,
    }
}

/// The `(precision, scale)` a `DECIMAL(p)` / `DECIMAL(p, s)` target states,
/// or `None` for a bare `DECIMAL` or digits outside `1 <= p <= 38`,
/// `0 <= s <= p`. `DECIMAL(p)` is `DECIMAL(p, 0)` by the SQL standard.
fn decimal_digits(info: &ast::ExactNumberInfo) -> Option<(u8, u8)> {
    let (precision, scale) = match info {
        ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p, *s),
        ast::ExactNumberInfo::Precision(p) => (*p, 0),
        ast::ExactNumberInfo::None => return None,
    };
    if !(1..=38).contains(&precision) || !(0..=i128::from(precision)).contains(&i128::from(scale)) {
        return None;
    }
    Some((u8::try_from(precision).ok()?, u8::try_from(scale).ok()?))
}

/// The type of a `CAST` to `dt` on the warehouses of `target` (#2333).
///
/// - A name that means the same on every warehouse
///   ([`data_type_is_warehouse_independent`]) has that type.
/// - Any other name is typed per warehouse ([`dialect_cast_type`]). It has a
///   type only when every target warehouse is known and they all agree.
/// - With no known target it stays [`RockyType::Unknown`], the same answer a
///   cast over an unknown input gives (#2334): a wrong concrete type would
///   fail a correct contract with `E011`.
fn cast_target_type(dt: &ast::DataType, target: &OperandTarget) -> RockyType {
    if data_type_is_warehouse_independent(dt) {
        return sql_type_to_rocky(dt);
    }
    let mut types = target
        .known_dialects()
        .iter()
        .map(|dialect| dialect_cast_type(dt, *dialect));
    let Some(first) = types.next() else {
        return RockyType::Unknown;
    };
    if types.all(|ty| ty == first) {
        first
    } else {
        RockyType::Unknown
    }
}

/// The type a `CAST` to `dt` produces on `dialect`, for the names whose
/// meaning differs between warehouses. `Unknown` where the warehouse has no
/// such type or Rocky does not know its width.
///
/// - Integers: Snowflake's `TINYINT` … `BIGINT` are all `NUMBER(38,0)`, which
///   Rocky reads as `Decimal(38,0)` (`rocky-snowflake/src/loader.rs`).
///   BigQuery's are all `INT64`. PostgreSQL and Redshift have no `TINYINT`.
///   Elsewhere `BIGINT` is 64-bit and the narrower names are `Int32`, the
///   width Rocky gives a `TINYINT` / `SMALLINT` column too.
/// - `REAL` is 32-bit except on Snowflake, where every float is 64-bit.
///   BigQuery has only `FLOAT64`.
/// - `FLOAT` is 32-bit on DuckDB and Databricks, 64-bit on Snowflake,
///   PostgreSQL, Redshift and SQL Server. `FLOAT(p)` is 32-bit for
///   `p <= 24` and 64-bit for `25 <= p <= 53` on PostgreSQL and SQL Server.
///   BigQuery and Trino have no `FLOAT`.
/// - A bare `TIMESTAMP` on Snowflake is `TIMESTAMP_NTZ`, `_LTZ` or `_TZ` by
///   the session's `TIMESTAMP_TYPE_MAPPING`, so it is not known. On SQL
///   Server `TIMESTAMP` is a row version, not a time.
fn dialect_cast_type(dt: &ast::DataType, dialect: OperandDialect) -> RockyType {
    use OperandDialect as D;
    let integer = |wide: bool| match dialect {
        D::Snowflake => RockyType::Decimal {
            precision: 38,
            scale: 0,
        },
        D::BigQuery => RockyType::Int64,
        D::DuckDb | D::Databricks | D::Trino | D::SqlServer | D::Postgres | D::Redshift => {
            if wide {
                RockyType::Int64
            } else {
                RockyType::Int32
            }
        }
    };
    match dt {
        ast::DataType::TinyInt(_) => match dialect {
            D::Postgres | D::Redshift => RockyType::Unknown,
            D::DuckDb | D::Snowflake | D::Databricks | D::BigQuery | D::Trino | D::SqlServer => {
                integer(false)
            }
        },
        ast::DataType::SmallInt(_) | ast::DataType::Int(_) | ast::DataType::Integer(_) => {
            integer(false)
        }
        ast::DataType::BigInt(_) => integer(true),
        ast::DataType::Real => match dialect {
            D::Snowflake => RockyType::Float64,
            D::BigQuery => RockyType::Unknown,
            D::DuckDb | D::Databricks | D::Trino | D::SqlServer | D::Postgres | D::Redshift => {
                RockyType::Float32
            }
        },
        ast::DataType::Float(ast::ExactNumberInfo::None) => match dialect {
            D::DuckDb | D::Databricks => RockyType::Float32,
            D::Snowflake | D::Postgres | D::Redshift | D::SqlServer => RockyType::Float64,
            D::BigQuery | D::Trino => RockyType::Unknown,
        },
        ast::DataType::Float(ast::ExactNumberInfo::Precision(p)) => match dialect {
            D::Postgres | D::SqlServer => match p {
                1..=24 => RockyType::Float32,
                25..=53 => RockyType::Float64,
                _ => RockyType::Unknown,
            },
            D::DuckDb | D::Snowflake | D::Databricks | D::BigQuery | D::Trino | D::Redshift => {
                RockyType::Unknown
            }
        },
        ast::DataType::Timestamp(_, _) => match dialect {
            D::Snowflake | D::SqlServer => RockyType::Unknown,
            D::DuckDb | D::Databricks | D::BigQuery | D::Trino | D::Postgres | D::Redshift => {
                RockyType::Timestamp
            }
        },
        // `MEDIUMINT` is MySQL's; no warehouse here has it.
        ast::DataType::MediumInt(_)
        | ast::DataType::Float(ast::ExactNumberInfo::PrecisionAndScale(_, _)) => RockyType::Unknown,
        _ => sql_type_to_rocky(dt),
    }
}

/// Convert an sqlparser DataType to RockyType.
///
/// This is the reading with no warehouse in view. A `CAST` target goes
/// through [`cast_target_type`] instead, which only uses this for names that
/// mean the same everywhere or once the warehouse is known.
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
        ast::DataType::Double(_) | ast::DataType::DoublePrecision | ast::DataType::Float64 => {
            RockyType::Float64
        }
        ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => match info {
            // Out-of-range digits (`NUMERIC(300, 0)`) are not a type Rocky
            // can name; narrowing them with `as u8` wrapped to a wrong one.
            ast::ExactNumberInfo::PrecisionAndScale(_, _) | ast::ExactNumberInfo::Precision(_) => {
                match decimal_digits(info) {
                    Some((precision, scale)) => RockyType::Decimal { precision, scale },
                    None => RockyType::Unknown,
                }
            }
            // A bare `DECIMAL` / `NUMERIC` names no digits, so Rocky does
            // not know the type — the same answer `warehouse_type_to_rocky`
            // gives for the bare string since #1646. Guessing (38,0) here
            // was the third normaliser reading one column differently from
            // the other two, and it is what made a CAST able to manufacture
            // a passing contract check (#1721).
            //
            // `Precision(p)` above is NOT a guess: SQL defines DECIMAL(p) as
            // DECIMAL(p, 0), so the scale is stated by the standard rather
            // than invented here.
            ast::ExactNumberInfo::None => RockyType::Unknown,
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
    infer_select_types_with_lookup(
        sql,
        &|name| {
            scope
                .get(name)
                .or_else(|| scope.get(name.rsplit('.').next().unwrap_or(name)))
                .map(Vec::as_slice)
        },
        &OperandTarget::Unconfigured,
    )
    .map(|inferred| inferred.columns)
}

#[derive(Default)]
pub(crate) struct SelectInference {
    pub(crate) columns: Vec<TypedColumn>,
    // Projection indexes keep metadata aligned with duplicate wildcard names.
    /// Outputs whose inferred type comes straight from the SQL — see
    /// [`has_exact_type`].
    exact_type_outputs: HashSet<usize>,
    /// Outputs whose type is a guess for a query that reads this one as a
    /// relation (a CTE or derived table): an expression that is not exact,
    /// or a `*` column that was not exact where it came from. An outer
    /// expression over such a column is not exact either (#2320).
    relation_inexact: HashSet<usize>,
    /// Outputs that are a `COUNT(...)` call.
    count_outputs: HashSet<usize>,
    /// Outputs whose projection is itself a cast (`CAST`, `TRY_CAST`,
    /// `SAFE_CAST`, `::`), looking through parentheses. Such an output has
    /// the cast's target type whatever its input.
    cast_outputs: HashSet<usize>,
    /// The query is a `UNION` / `INTERSECT` / `EXCEPT`: its columns combine
    /// every branch, while lineage reads only the first (#2303).
    set_operation: bool,
}

impl SelectInference {
    fn push_expression(&mut self, name: String, expr: &Expr, scope: &TypeScope) {
        let (data_type, nullable) = infer_expr_type(expr, scope);
        let function = function_name(expr);
        // `SUM` / `AVG` of a DECIMAL widens the precision by a
        // dialect-dependent amount (DuckDB DECIMAL(38, s), Databricks
        // DECIMAL(p + 10, s)), so the argument's DECIMAL is not the result.
        let widened_decimal = matches!(function.as_deref(), Some("SUM" | "AVG"))
            && matches!(data_type, RockyType::Decimal { .. });
        if has_exact_type(expr, scope) && !widened_decimal {
            self.exact_type_outputs.insert(self.columns.len());
        } else {
            self.relation_inexact.insert(self.columns.len());
        }
        if function.as_deref() == Some("COUNT") {
            self.count_outputs.insert(self.columns.len());
        }
        // The cast's type was read for the model's warehouses
        // ([`cast_target_type`]): a name whose meaning differs between them
        // is `Unknown` unless they are known, so the merge takes nothing
        // from it.
        if is_cast_expr(expr) {
            self.cast_outputs.insert(self.columns.len());
        }

        self.columns.push(TypedColumn {
            name,
            data_type,
            nullable,
        });
    }
}

/// Whether the cast target `dt` means the same thing on every warehouse
/// Rocky targets, so it has a type when the warehouse is not known.
///
/// The list: BOOLEAN; DOUBLE / DOUBLE PRECISION / FLOAT64 (every warehouse
/// that accepts one of these spellings means a 64-bit float; BigQuery has no
/// `DOUBLE` and rejects it); DECIMAL / NUMERIC with `1 <= p <= 38` and
/// `0 <= s <= p`; VARCHAR, CHAR, TEXT, STRING; BINARY, VARBINARY, BLOB; DATE.
///
/// Snowflake's `FLOAT` is 64-bit, its `INTEGER` and `BIGINT` are
/// `NUMBER(38,0)`, PostgreSQL's `FLOAT` is `DOUBLE PRECISION`, and Snowflake's
/// bare `TIMESTAMP` follows a session parameter. Those names, and any name
/// not listed here, are typed only for a known warehouse
/// ([`dialect_cast_type`]); a bare `DECIMAL` never is.
fn data_type_is_warehouse_independent(dt: &ast::DataType) -> bool {
    match dt {
        ast::DataType::Decimal(info) | ast::DataType::Numeric(info) => {
            decimal_digits(info).is_some()
        }
        _ => matches!(
            dt,
            ast::DataType::Boolean
                | ast::DataType::Double(_)
                | ast::DataType::DoublePrecision
                | ast::DataType::Float64
                | ast::DataType::Varchar(_)
                | ast::DataType::Char(_)
                | ast::DataType::Text
                | ast::DataType::String(_)
                | ast::DataType::Binary(_)
                | ast::DataType::Varbinary(_)
                | ast::DataType::Blob(_)
                | ast::DataType::Date
        ),
    }
}

/// Whether `expr` is a cast, looking through parentheses.
fn is_cast_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => is_cast_expr(inner),
        Expr::Cast { .. } => true,
        _ => false,
    }
}

/// The upper-cased name of the function `expr` calls, looking through
/// parentheses.
fn function_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Nested(inner) => function_name(inner),
        Expr::Function(func) => Some(func.name.to_string().to_uppercase()),
        _ => None,
    }
}

/// Whether [`infer_expr_type`] reads this expression's type from the SQL
/// itself rather than from a guess: a column (of a CTE or derived table,
/// only when the expression that built it is exact — #2320), a cast (its
/// target), `COUNT`, `SUM` / `MIN` / `MAX` / `AVG` over one of these, or a
/// `CASE` / `COALESCE` whose branches agree (see [`branches_agree`]).
///
/// Anything else — a scalar function whose result width is dialect-dependent
/// (`LENGTH`), a numeric literal, `COALESCE` over a literal that changes the
/// type — is not exact, so a nested expression built from it stays `Unknown`
/// (#2295).
fn has_exact_type(expr: &Expr, scope: &TypeScope) -> bool {
    match expr {
        // A column of a CTE or derived table is exact only when the
        // expression that built it is (#2320).
        Expr::Identifier(ident) => scope.is_exact(&ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            scope.is_exact_qualified(&parts[parts.len() - 2].value, &parts[parts.len() - 1].value)
        }
        Expr::CompoundIdentifier(_) | Expr::Cast { .. } => true,
        Expr::Nested(inner) => has_exact_type(inner, scope),
        Expr::Function(func) => match func.name.to_string().to_uppercase().as_str() {
            "COUNT" => true,
            "SUM" | "MIN" | "MAX" | "AVG" => match &func.args {
                ast::FunctionArguments::List(list) => matches!(
                    list.args.first(),
                    Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)))
                        if has_exact_type(arg, scope)
                ),
                _ => false,
            },
            // `NULLIF(x, y)` returns `x` or NULL. PostgreSQL, Redshift and
            // BigQuery may promote it to the common type of `x` and `y`
            // (`NULLIF(int_col, 0.5)` is numeric), so it has the type of `x`
            // only when `y` cannot widen it: the same rule as a `COALESCE`
            // branch (an integer literal, `NULL`, or a value of `x`'s type).
            "NULLIF" if func.over.is_none() && func.filter.is_none() => match &func.args {
                ast::FunctionArguments::List(list)
                    if list.args.len() == 2
                        && list.duplicate_treatment.is_none()
                        && list.clauses.is_empty() =>
                {
                    let mut args = Vec::with_capacity(2);
                    for arg in &list.args {
                        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg else {
                            return false;
                        };
                        args.push(arg);
                    }
                    has_exact_type(args[0], scope)
                        && branches_agree(&args, &infer_expr_type(expr, scope).0, scope)
                }
                _ => false,
            },
            "COALESCE" if func.over.is_none() && func.filter.is_none() => match &func.args {
                ast::FunctionArguments::List(list)
                    if list.duplicate_treatment.is_none() && list.clauses.is_empty() =>
                {
                    let mut branches = Vec::with_capacity(list.args.len());
                    for arg in &list.args {
                        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg else {
                            return false;
                        };
                        branches.push(arg);
                    }
                    branches_agree(&branches, &infer_expr_type(expr, scope).0, scope)
                }
                _ => false,
            },
            _ => false,
        },
        // Simple `CASE x WHEN ...` and searched `CASE WHEN ...`: the type
        // comes from the results only.
        Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            let branches: Vec<&Expr> = conditions
                .iter()
                .map(|when| &when.result)
                .chain(else_result.as_deref())
                .collect();
            branches_agree(&branches, &infer_expr_type(expr, scope).0, scope)
        }
        _ => false,
    }
}

/// Whether the branches of a `CASE` or `COALESCE` fix its type `result`:
/// every branch is exact (see [`has_exact_type`]), a text or boolean literal,
/// an integer literal, or `NULL`; at least one branch is neither `NULL` nor a
/// number; and every branch that is neither infers to `result` exactly.
///
/// An integer literal takes its type from the context in DuckDB
/// (`COALESCE(int_col, 0)` is `INTEGER`), so Rocky's `BIGINT` for it is a
/// guess. It is accepted only because the other branches must already infer
/// to `result`: if the literal had widened the type, they would not.
fn branches_agree(branches: &[&Expr], result: &RockyType, scope: &TypeScope) -> bool {
    if *result == RockyType::Unknown {
        return false;
    }
    let mut typed_branch = false;
    for branch in branches {
        let branch = strip_nested(branch);
        if let Expr::Value(value) = branch {
            match &value.value {
                ast::Value::Null => continue,
                ast::Value::Number(text, _) if text.bytes().all(|b| b.is_ascii_digit()) => {
                    continue;
                }
                // A double-quoted value is an identifier in DuckDB and
                // PostgreSQL, so it is not a text literal here.
                ast::Value::SingleQuotedString(_) | ast::Value::Boolean(_) => {}
                _ => return false,
            }
        } else if !has_exact_type(branch, scope) {
            return false;
        }
        if infer_expr_type(branch, scope).0 != *result {
            return false;
        }
        typed_branch = true;
    }
    typed_branch
}

fn strip_nested(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => strip_nested(inner),
        other => other,
    }
}

/// The columns of one relation a query reads: a table, a model, a CTE.
#[derive(Clone, Copy)]
pub(crate) struct RelationRef<'a> {
    pub(crate) columns: &'a [TypedColumn],
    /// Positions in `columns` whose type is a guess (a CTE column built
    /// from an expression that is not exact). `None`: every column is exact,
    /// as for a table or a model.
    pub(crate) inexact: Option<&'a HashSet<usize>>,
}

impl<'a> From<&'a [TypedColumn]> for RelationRef<'a> {
    fn from(columns: &'a [TypedColumn]) -> Self {
        Self {
            columns,
            inexact: None,
        }
    }
}

/// What expression inference reads besides the SQL: the relations in scope
/// and the warehouses the SQL runs on.
#[derive(Clone, Copy)]
pub(crate) struct InferEnv<'e, 'a> {
    pub(crate) lookup: &'e dyn Fn(&str) -> Option<RelationRef<'a>>,
    pub(crate) target: &'e OperandTarget,
}

fn infer_select_types_with_lookup<'a>(
    sql: &str,
    lookup: &dyn Fn(&str) -> Option<&'a [TypedColumn]>,
    target: &OperandTarget,
) -> Result<SelectInference, String> {
    let dialect = rocky_sql::dialect::DatabricksDialect;
    let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let Statement::Query(query) = stmts.first().ok_or("empty SQL")? else {
        return Err("only SELECT statements supported".to_string());
    };
    let relation = |name: &str| lookup(name).map(RelationRef::from);
    infer_query_types(
        query,
        InferEnv {
            lookup: &relation,
            target,
        },
    )
}

pub(crate) fn infer_query_types(
    query: &ast::Query,
    env: InferEnv<'_, '_>,
) -> Result<SelectInference, String> {
    // Each CTE keeps which of its columns are exact, so an outer expression
    // over one is exact only when the CTE's expression is (#2320).
    let mut ctes: HashMap<String, (Vec<TypedColumn>, HashSet<usize>)> = HashMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let inferred = {
                let lookup = |name: &str| cte_relation(&ctes, name).or_else(|| (env.lookup)(name));
                infer_query_types(
                    &cte.query,
                    InferEnv {
                        lookup: &lookup,
                        target: env.target,
                    },
                )
                .unwrap_or_default()
            };
            let mut columns = inferred.columns;
            rename_relation_columns(&mut columns, &cte.alias);
            ctes.insert(
                cte.alias.name.value.clone(),
                (columns, inferred.relation_inexact),
            );
        }
    }
    let lookup = |name: &str| cte_relation(&ctes, name).or_else(|| (env.lookup)(name));
    infer_set_expr_types(
        query.body.as_ref(),
        InferEnv {
            lookup: &lookup,
            target: env.target,
        },
    )
}

fn cte_relation<'c>(
    ctes: &'c HashMap<String, (Vec<TypedColumn>, HashSet<usize>)>,
    name: &str,
) -> Option<RelationRef<'c>> {
    ctes.get(name).map(|(columns, inexact)| RelationRef {
        columns,
        inexact: Some(inexact),
    })
}

/// Infer the output columns of one query body: a `SELECT`, a parenthesised
/// query, or a set operation over bodies (#2303).
fn infer_set_expr_types(body: &SetExpr, env: InferEnv<'_, '_>) -> Result<SelectInference, String> {
    match body {
        SetExpr::Select(select) => infer_select_types_in_scope(select, env),
        SetExpr::Query(query) => infer_query_types(query, env),
        SetExpr::SetOperation { left, right, .. } => {
            let left = infer_set_expr_types(left, env)?;
            let right = infer_set_expr_types(right, env)?;
            combine_set_operation(left, &right)
        }
        _ => Err("unsupported query form".to_string()),
    }
}

/// Combine the two branches of `UNION` / `INTERSECT` / `EXCEPT` column by
/// position (#2303). Names come from the left branch.
///
/// - **Nullable** when either branch is nullable. `INTERSECT` and `EXCEPT`
///   could be tighter, but "any branch" is always sound.
/// - **Type** is the common supertype, `Unknown` when a branch is `Unknown`
///   or the types have none. (`common_supertype` treats `Unknown` as
///   compatible with anything; here that would claim a type we cannot back.)
/// - A type is *exact* only when both branches are exact and agree, since a
///   widened result (`INT` with `BIGINT`) or a literal branch is a guess.
///
/// A different column count is an `Err`: the position pairing is not defined.
fn combine_set_operation(
    mut left: SelectInference,
    right: &SelectInference,
) -> Result<SelectInference, String> {
    if left.columns.len() != right.columns.len() {
        return Err("set operation branches differ in column count".to_string());
    }
    let mut exact_type_outputs = HashSet::new();
    let mut relation_inexact = HashSet::new();
    let mut count_outputs = HashSet::new();
    for (index, (l, r)) in left.columns.iter_mut().zip(&right.columns).enumerate() {
        let both_exact =
            left.exact_type_outputs.contains(&index) && right.exact_type_outputs.contains(&index);
        let same_type = l.data_type == r.data_type;
        let data_type = if l.data_type == RockyType::Unknown || r.data_type == RockyType::Unknown {
            RockyType::Unknown
        } else {
            crate::types::common_supertype(&l.data_type, &r.data_type).unwrap_or(RockyType::Unknown)
        };
        if both_exact && same_type && data_type != RockyType::Unknown {
            exact_type_outputs.insert(index);
        }
        if left.relation_inexact.contains(&index)
            || right.relation_inexact.contains(&index)
            || !same_type
        {
            relation_inexact.insert(index);
        }
        if left.count_outputs.contains(&index) && right.count_outputs.contains(&index) {
            count_outputs.insert(index);
        }
        l.data_type = data_type;
        l.nullable |= r.nullable;
    }
    Ok(SelectInference {
        columns: left.columns,
        exact_type_outputs,
        relation_inexact,
        count_outputs,
        // A combined column's type is a supertype across branches, not a
        // single cast target.
        cast_outputs: HashSet::new(),
        set_operation: true,
    })
}

/// The type of a set-operation column whose lineage edge saw only the first
/// branch. Keep it when the combined type agrees; take the combined type when
/// it is exact; otherwise a branch changed it and the answer is `Unknown`.
fn set_operation_column_type(
    from_lineage: &RockyType,
    combined: &TypedColumn,
    exact_type: bool,
) -> RockyType {
    if *from_lineage == combined.data_type {
        from_lineage.clone()
    } else if exact_type {
        combined.data_type.clone()
    } else {
        RockyType::Unknown
    }
}

fn infer_select_types_in_scope(
    select: &ast::Select,
    env: InferEnv<'_, '_>,
) -> Result<SelectInference, String> {
    let (from_scope, type_scope) = select_type_scope(select, env);

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
                for col in &from_scope.columns {
                    if from_scope.inexact.contains(CiStr::new(&col.name)) {
                        inferred.relation_inexact.insert(inferred.columns.len());
                    }
                    inferred.columns.push(col.clone());
                }
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
                        for col in &relation.columns {
                            if relation.inexact.contains(CiStr::new(&col.name)) {
                                inferred.relation_inexact.insert(inferred.columns.len());
                            }
                            inferred.columns.push(col.clone());
                        }
                    }
                }
            }
            SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::Expr(_), _) => {}
        }
    }

    Ok(inferred)
}

/// Build the relation scope of one `SELECT`: its `FROM` / `JOIN` relations
/// and the [`TypeScope`] that resolves bare and qualified column names
/// against them. A bare name exposed by more than one relation is ambiguous
/// and resolves to `Unknown`.
pub(crate) fn select_type_scope(
    select: &ast::Select,
    env: InferEnv<'_, '_>,
) -> (JoinScope, TypeScope) {
    let mut from_scope = JoinScope::default();
    for from in &select.from {
        let joined = infer_join_relations(from, env);
        from_scope.relations.extend(joined.relations);
        from_scope.columns.extend(joined.columns);
        from_scope.inexact.extend(joined.inexact);
    }
    let mut type_scope = TypeScope::with_target(env.target.clone());
    type_scope.inexact.clone_from(&from_scope.inexact);
    for relation in &from_scope.relations {
        if !relation.inexact.is_empty() {
            type_scope
                .qualified_inexact
                .entry(CiKey::owned(relation.qualifier.clone()))
                .or_default()
                .extend(relation.inexact.iter().cloned());
        }
    }
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
    (from_scope, type_scope)
}

pub(crate) struct RelationColumns {
    qualifier: String,
    columns: Vec<TypedColumn>,
    /// Names in `columns` whose type is a guess (see [`RelationRef`]).
    inexact: HashSet<CiKey<'static>>,
}

#[derive(Default)]
pub(crate) struct JoinScope {
    // Qualified references retain each side's columns. USING/NATURAL keys
    // merge only in the unqualified output used by SELECT * and bare names.
    relations: Vec<RelationColumns>,
    columns: Vec<TypedColumn>,
    /// Names in `columns` whose type is a guess. A name that is a guess in
    /// any relation counts, which also covers a merged USING key.
    inexact: HashSet<CiKey<'static>>,
}

fn infer_join_relations(from: &ast::TableWithJoins, env: InferEnv<'_, '_>) -> JoinScope {
    use ast::JoinOperator;

    let mut left = infer_relation_columns(&from.relation, env);
    for join in &from.joins {
        let mut right = infer_relation_columns(&join.relation, env);
        left.inexact.extend(std::mem::take(&mut right.inexact));
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

fn infer_relation_columns(factor: &TableFactor, env: InferEnv<'_, '_>) -> JoinScope {
    // `inexact` holds positions, so it survives the alias renaming below.
    let (qualifier, mut columns, inexact, alias) = match factor {
        TableFactor::Table { name, alias, .. } => {
            let name = name.to_string();
            let short = name.rsplit('.').next().unwrap_or(&name);
            let relation = (env.lookup)(&name);
            let columns = relation.map(|r| r.columns).unwrap_or_default().to_vec();
            let inexact = relation
                .and_then(|r| r.inexact)
                .cloned()
                .unwrap_or_default();
            (short.to_string(), columns, inexact, alias)
        }
        // A derived table keeps which columns are exact, as a CTE does
        // (#2320).
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let inferred = infer_query_types(subquery, env).unwrap_or_default();
            (
                String::new(),
                inferred.columns,
                inferred.relation_inexact,
                alias,
            )
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let scope = infer_join_relations(table_with_joins, env);
            if alias.is_none() {
                return scope;
            }
            let inexact = scope
                .columns
                .iter()
                .enumerate()
                .filter(|(_, col)| scope.inexact.contains(CiStr::new(&col.name)))
                .map(|(index, _)| index)
                .collect();
            (String::new(), scope.columns, inexact, alias)
        }
        _ => return JoinScope::default(),
    };
    let qualifier = if let Some(alias) = alias {
        rename_relation_columns(&mut columns, alias);
        alias.name.value.clone()
    } else {
        qualifier
    };
    let inexact: HashSet<CiKey<'static>> = inexact
        .iter()
        .filter_map(|&index| columns.get(index))
        .map(|col| CiKey::owned(col.name.clone()))
        .collect();
    JoinScope {
        relations: vec![RelationColumns {
            qualifier,
            columns: columns.clone(),
            inexact: inexact.clone(),
        }],
        columns,
        inexact,
    }
}

pub(crate) fn rename_relation_columns(columns: &mut [TypedColumn], alias: &ast::TableAlias) {
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
    use crate::diagnostic::E039;
    use crate::project::Project;
    use crate::semantic::build_semantic_graph;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};

    /// The decimal arms of the THIRD normaliser (#1721).
    ///
    /// Rocky has three type normalisers and this one had drifted: a bare
    /// `DECIMAL` / `NUMERIC` became `Decimal(38,0)` here while
    /// `warehouse_type_to_rocky` had returned `Unknown` for the same bare
    /// string since #1646. One typecheck pass therefore read one column two
    /// ways depending on which side it arrived from, and a `CAST(x AS
    /// NUMERIC)` could manufacture a passing contract check.
    ///
    /// No test covered this arm before, which is why the drift survived two
    /// issues about it. Stated as a table so a future edit has to disagree
    /// with the standard explicitly.
    #[test]
    fn bare_decimal_is_unknown_while_stated_digits_stay_concrete() {
        use sqlparser::ast::{DataType, ExactNumberInfo};

        // A bare name states no digits, so it is not a type Rocky knows.
        assert_eq!(
            sql_type_to_rocky(&DataType::Numeric(ExactNumberInfo::None)),
            RockyType::Unknown,
            "bare NUMERIC must not become a made-up decimal"
        );
        assert_eq!(
            sql_type_to_rocky(&DataType::Decimal(ExactNumberInfo::None)),
            RockyType::Unknown,
            "bare DECIMAL must not become a made-up decimal"
        );

        // DECIMAL(p) is DECIMAL(p, 0) by the SQL standard — stated, not
        // guessed, so it stays concrete.
        assert_eq!(
            sql_type_to_rocky(&DataType::Decimal(ExactNumberInfo::Precision(10))),
            RockyType::Decimal {
                precision: 10,
                scale: 0
            }
        );
        assert_eq!(
            sql_type_to_rocky(&DataType::Numeric(ExactNumberInfo::PrecisionAndScale(
                38, 9
            ))),
            RockyType::Decimal {
                precision: 38,
                scale: 9
            }
        );
    }

    fn make_model(name: &str, sql: &str) -> Model {
        Model {
            drop_existing_kind: None,
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
            file_path: format!("models/{name}.sql").into(),
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
        let columns = infer_select_types_on(sql, &sources, &duckdb());
        assert_eq!(columns.len(), 5);
        for (col, nullable) in columns.iter().zip([false, true, true, false, false]) {
            assert_eq!(col.data_type, RockyType::Int64, "{}", col.name);
            assert_eq!(col.nullable, nullable, "{}", col.name);
        }
        let project = Project::from_models(vec![make_model("joined", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_for_targets(
            &graph,
            &sources,
            &project.models,
            None,
            &TargetDialects::uniform(duckdb()),
        );
        let columns = &result.typed_models["joined"];
        for (col, nullable) in columns[..3].iter().zip([false, true, true]) {
            assert_eq!(col.data_type, RockyType::Int64, "{}", col.name);
            assert_eq!(col.nullable, nullable, "{}", col.name);
        }
        // `COALESCE(b.id, 0)`: the BIGINT branch fixes the type, and the
        // non-null literal makes it non-null even over the null-extended side.
        assert_eq!(columns[3].data_type, RockyType::Int64);
        assert!(!columns[3].nullable);
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

    /// Typecheck one model over a source `t` (`x INT NOT NULL`, `n STRING NOT
    /// NULL`, `y INT NOT NULL`) and return its `(name, type, nullable)` rows.
    /// A model that runs on DuckDB, so a cast to `INT` / `BIGINT` /
    /// `TIMESTAMP` has a type (#2333).
    fn duckdb() -> OperandTarget {
        Some(OperandDialect::DuckDb).into()
    }

    /// [`infer_select_types`] for SQL that runs on `target`.
    fn infer_select_types_on(
        sql: &str,
        sources: &HashMap<String, Vec<TypedColumn>>,
        target: &OperandTarget,
    ) -> Vec<TypedColumn> {
        infer_select_types_with_lookup(
            sql,
            &|name| {
                sources
                    .get(name)
                    .or_else(|| sources.get(name.rsplit('.').next().unwrap_or(name)))
                    .map(Vec::as_slice)
            },
            target,
        )
        .unwrap()
        .columns
    }

    /// Typecheck model `m` over `source` (`x INT`, `n STRING`, `y INT`, all
    /// NOT NULL) on DuckDB.
    fn typecheck_over_t(source: &str, sql: &str) -> Vec<(String, RockyType, bool)> {
        typecheck_over_t_on(duckdb(), source, sql)
    }

    fn typecheck_over_t_on(
        target: OperandTarget,
        source: &str,
        sql: &str,
    ) -> Vec<(String, RockyType, bool)> {
        let sources = HashMap::from([(
            source.to_string(),
            source_schema(&[
                ("x", RockyType::Int32, false),
                ("n", RockyType::String, false),
                ("y", RockyType::Int32, false),
            ]),
        )]);
        let project = Project::from_models(vec![make_model("m", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_for_targets(
            &graph,
            &sources,
            &project.models,
            None,
            &TargetDialects::uniform(target),
        );
        result.typed_models["m"]
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect()
    }

    /// Golden table for #2299, through the full typecheck (lineage edge plus
    /// expression inference). A plain cast that can fail returns NULL on some
    /// warehouses (Spark with ANSI off), so it is nullable over a NOT NULL
    /// input. A cast that cannot fail keeps the input's nullability.
    #[test]
    fn fallible_plain_cast_is_nullable_golden() {
        // `x` and `y` are INT NOT NULL, `n` is STRING NOT NULL.
        for (expr, nullable) in [
            // String to numeric, boolean, temporal, decimal: can fail.
            ("CAST(n AS INT)", true),
            ("CAST(n AS BIGINT)", true),
            ("CAST(n AS DOUBLE)", true),
            ("CAST(n AS DECIMAL(10,2))", true),
            ("CAST(n AS DATE)", true),
            ("CAST(n AS TIMESTAMP)", true),
            ("CAST(n AS BOOLEAN)", true),
            ("n::INT", true),
            // Nested: the fallible cast sits under another call.
            ("ABS(CAST(n AS INT))", true),
            // Numeric narrowing: can fail.
            ("CAST(CAST(x AS BIGINT) AS INT)", true),
            ("CAST(x AS SMALLINT)", true),
            ("CAST(x AS TINYINT)", true),
            ("CAST(x AS DECIMAL(10,2))", true),
            // Fallible by construction, as before.
            ("TRY_CAST(x AS BIGINT)", true),
            ("SAFE_CAST(x AS BIGINT)", true),
            // Cannot fail: keeps the input's NOT NULL.
            ("CAST(x AS INT)", false),
            ("CAST(x AS BIGINT)", false),
            ("CAST(x AS DOUBLE)", false),
            ("CAST(x AS FLOAT)", false),
            ("CAST(x AS STRING)", false),
            ("CAST(n AS STRING)", false),
            ("CAST(n AS VARCHAR(10))", false),
            ("x::BIGINT", false),
        ] {
            let rows = typecheck_over_t("t", &format!("SELECT {expr} AS c FROM t"));
            assert_eq!(rows.len(), 1, "{expr}: {rows:?}");
            assert_eq!(rows[0].2, nullable, "{expr}: {rows:?}");
        }
    }

    /// #2299: `CAST(s AS INT)` over a NOT NULL string fails a `nullable =
    /// false` contract (E012).
    #[test]
    fn fallible_cast_fails_not_null_contract() {
        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "c".to_string(),
                type_name: Some("Int32".to_string()),
                nullable: Some(false),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[("n", RockyType::String, false)]),
        )]);
        let project =
            Project::from_models(vec![make_model("m", "SELECT CAST(n AS INT) AS c FROM t")])
                .unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        assert!(
            validate_contract("m", &result.typed_models["m"], &contract)
                .iter()
                .any(|d| &*d.code == "E012")
        );
    }

    /// Typecheck one model over sources `t` and `u`, both `(x INT NOT NULL, n
    /// STRING NOT NULL, z INT NULL)`.
    fn typecheck_over_t_and_u(sql: &str) -> Vec<(String, RockyType, bool)> {
        let schema = || {
            source_schema(&[
                ("x", RockyType::Int32, false),
                ("n", RockyType::String, false),
                ("z", RockyType::Int32, true),
            ])
        };
        let sources = HashMap::from([("t".to_string(), schema()), ("u".to_string(), schema())]);
        let project = Project::from_models(vec![make_model("m", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_for_targets(
            &graph,
            &sources,
            &project.models,
            None,
            &TargetDialects::uniform(duckdb()),
        );
        result.typed_models["m"]
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect()
    }

    /// #2307: a CTE named like a source shadows it. The column comes from the
    /// CTE body (`u.n`, a STRING), never from the same-named source `t` (INT).
    #[test]
    fn a_cte_named_like_a_source_does_not_take_the_sources_types() {
        for sql in [
            "WITH t AS (SELECT n AS x FROM u) SELECT x FROM t",
            // Aliased, qualified and nested forms of the same read.
            "WITH t AS (SELECT n AS x FROM u) SELECT a.x FROM t AS a",
            "WITH t AS (SELECT n AS x FROM u) SELECT t.x AS x FROM t",
            "WITH t AS (SELECT n AS x FROM u), w AS (SELECT x FROM t) SELECT x FROM t",
            // The inner `t` is the source; the outer `t` is the CTE.
            "WITH t AS (SELECT n AS x FROM t) SELECT x FROM t",
        ] {
            let rows = typecheck_over_t_and_u(sql);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert_ne!(rows[0].1, RockyType::Int32, "{sql}: {rows:?}");
        }
    }

    type Cols<'a> = &'a [(&'a str, RockyType, bool)];

    /// Typecheck one model `m` over the given `(name, columns)` sources.
    fn typecheck_over(sql: &str, tables: &[(&str, Cols)]) -> Vec<(String, RockyType, bool)> {
        let sources: HashMap<_, _> = tables
            .iter()
            .map(|(name, cols)| (name.to_string(), source_schema(cols)))
            .collect();
        let project = Project::from_models(vec![make_model("m", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        result.typed_models["m"]
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect()
    }

    /// #2307: the physical origin a CTE read resolves to is not re-resolved
    /// through the OUTER query's aliases. `customers AS orders` must not
    /// capture `orders.n` read through the CTE.
    #[test]
    fn an_outer_alias_does_not_capture_a_cte_origin() {
        let orders: &[(&str, RockyType, bool)] = &[
            ("id", RockyType::Int32, false),
            ("n", RockyType::String, false),
        ];
        let customers: &[(&str, RockyType, bool)] = &[
            ("id", RockyType::Int32, false),
            ("n", RockyType::Int32, true),
        ];
        let rows = typecheck_over(
            "WITH c AS (SELECT n AS x FROM orders) \
             SELECT c.x FROM c JOIN customers AS orders ON c.x = orders.id",
            &[("orders", orders), ("customers", customers)],
        );
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].1, RockyType::String, "{rows:?}");
        assert!(!rows[0].2, "{rows:?}");
    }

    /// #2307: row-selection edges through a CTE are not captured either.
    #[test]
    fn an_outer_alias_does_not_capture_a_cte_row_selection_origin() {
        let sql = "WITH c AS (SELECT n AS x FROM orders) \
                   SELECT c.x FROM c JOIN customers AS orders ON c.x = orders.id WHERE c.x > 0";
        let project = Project::from_models(vec![make_model("m", sql)]).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let from_customers_n = graph
            .row_selection_edges
            .iter()
            .any(|e| &*e.source.model == "customers" && &*e.source.column == "n");
        assert!(!from_customers_n, "{:?}", graph.row_selection_edges);
        assert!(
            graph
                .row_selection_edges
                .iter()
                .any(|e| &*e.source.model == "orders" && &*e.source.column == "n"),
            "{:?}",
            graph.row_selection_edges
        );
    }

    /// #2307: a CTE body column from the null-supplying side of an outer join
    /// is nullable in the model, though the source column is NOT NULL.
    #[test]
    fn a_cte_body_outer_join_column_stays_nullable() {
        let o: &[(&str, RockyType, bool)] = &[("id", RockyType::Int32, false)];
        let p: &[(&str, RockyType, bool)] = &[
            ("oid", RockyType::Int32, false),
            ("y", RockyType::Int32, false),
        ];
        for sql in [
            "WITH a AS (SELECT o.id, p.y FROM o LEFT JOIN p ON o.id = p.oid) SELECT y FROM a",
            "WITH a AS (SELECT o.id, p.y FROM o LEFT JOIN p ON o.id = p.oid) SELECT a.y FROM a",
            "WITH a AS (SELECT o.id, p.y AS yy FROM o LEFT JOIN p ON o.id = p.oid), \
             b AS (SELECT yy FROM a) SELECT yy FROM b",
        ] {
            let rows = typecheck_over(sql, &[("o", o), ("p", p)]);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert!(rows[0].2, "{sql}: {rows:?}");
        }
        // The preserved side stays NOT NULL.
        let rows = typecheck_over(
            "WITH a AS (SELECT o.id, p.y FROM o LEFT JOIN p ON o.id = p.oid) SELECT id FROM a",
            &[("o", o), ("p", p)],
        );
        assert!(!rows[0].2, "{rows:?}");
    }

    /// #2307: a recursive CTE's column is nullable when its recursive branch
    /// can supply NULL, whatever the anchor says.
    #[test]
    fn a_recursive_cte_column_is_nullable_when_a_branch_can_be_null() {
        let o: &[(&str, RockyType, bool)] = &[
            ("id", RockyType::Int32, false),
            ("z", RockyType::Int32, true),
        ];
        let rows = typecheck_over(
            "WITH RECURSIVE r AS (SELECT id, id AS v FROM o \
             UNION ALL SELECT r.id, o.z FROM r JOIN o ON r.id = o.id) SELECT v FROM r",
            &[("o", o)],
        );
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(rows[0].2, "{rows:?}");
    }

    /// #2307: a CTE with another name leaves the source read alone.
    #[test]
    fn a_non_shadowing_cte_keeps_source_types() {
        let rows = typecheck_over_t_and_u("WITH c AS (SELECT n FROM u) SELECT x FROM t");
        assert_eq!(rows[0].1, RockyType::Int32, "{rows:?}");
        let rows = typecheck_over_t_and_u("SELECT x FROM t");
        assert_eq!(rows[0].1, RockyType::Int32, "{rows:?}");
    }

    /// #2307: a column read through a CTE takes the type of the CTE body's
    /// column, traced to the physical table at the end of the chain.
    #[test]
    fn a_cte_column_takes_the_type_of_its_body_column() {
        for (sql, want, nullable) in [
            // Shadowed name: the body reads `u.n` (STRING), not source `t`.
            (
                "WITH t AS (SELECT n AS x FROM u) SELECT x FROM t",
                RockyType::String,
                false,
            ),
            // Import CTE: `SELECT *` over the same-named source passes through.
            (
                "WITH t AS (SELECT * FROM t) SELECT x FROM t",
                RockyType::Int32,
                false,
            ),
            (
                "WITH t AS (SELECT * FROM t) SELECT z FROM t",
                RockyType::Int32,
                true,
            ),
            // A chain of CTEs traces to the physical table at its end.
            (
                "WITH a AS (SELECT n AS x FROM u), b AS (SELECT x AS y FROM a) SELECT y FROM b",
                RockyType::String,
                false,
            ),
            (
                "WITH a AS (SELECT * FROM t), b AS (SELECT * FROM a) SELECT z FROM b",
                RockyType::Int32,
                true,
            ),
        ] {
            let rows = typecheck_over_t_and_u(sql);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert_eq!(rows[0].1, want, "{sql}: {rows:?}");
            assert_eq!(rows[0].2, nullable, "{sql}: {rows:?}");
        }
    }

    /// #2307: a transform inside the CTE body reaches the outer column's
    /// type; the outer column never copies the pre-transform source type.
    #[test]
    fn a_transform_in_a_cte_body_types_the_outer_column() {
        for (sql, want) in [
            (
                "WITH c AS (SELECT CAST(n AS INT) AS x FROM u) SELECT x FROM c",
                RockyType::Int32,
            ),
            (
                "WITH c AS (SELECT TRY_CAST(n AS INT) AS x FROM u) SELECT x FROM c",
                RockyType::Int32,
            ),
            (
                "WITH c AS (SELECT SUM(z) AS s FROM u) SELECT s FROM c",
                RockyType::Int64,
            ),
        ] {
            let rows = typecheck_over_t_and_u(sql);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert_eq!(rows[0].1, want, "{sql}: {rows:?}");
            // A cast can fail and a SUM over no rows is NULL.
            assert!(rows[0].2, "{sql}: {rows:?}");
        }
    }

    /// #2307: `SELECT *` over an import CTE (`WITH orders AS (SELECT * FROM
    /// orders)`) expands the upstream model's columns with their types, so a
    /// `time_column` the upstream projects does not raise E020.
    #[test]
    fn a_star_over_an_import_cte_keeps_the_upstream_columns_and_types() {
        let ti = make_time_interval_select_star_model(
            "ti",
            "ts",
            "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders \
             WHERE ts >= @start_date AND ts < @end_date",
        );
        let models = vec![
            make_model("orders", "SELECT id, ts FROM source.raw.base"),
            ti,
        ];
        let project = Project::from_models(models).unwrap();
        let external = HashMap::from([(
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
        )]);
        let graph = build_semantic_graph(&project, &external).unwrap();
        let typed_sources = HashMap::from([(
            "source.raw.base".to_string(),
            source_schema(&[
                ("id", RockyType::Int64, false),
                ("ts", RockyType::Date, false),
            ]),
        )]);
        let result =
            typecheck_project_with_models(&graph, &typed_sources, None, &project.models, None);
        let ti_cols = &result.typed_models["ti"];
        let ts = ti_cols.iter().find(|c| c.name == "ts");
        assert_eq!(
            ts.map(|c| &c.data_type),
            Some(&RockyType::Date),
            "{ti_cols:?}"
        );
        assert!(ti_cols.iter().any(|c| c.name == "id"), "{ti_cols:?}");
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "E020"),
            "{:?}",
            result.diagnostics
        );
    }

    /// #2303: a set operation is typed by combining its branches by
    /// position. A column is non-null only when it is non-null in every branch.
    #[test]
    fn set_operation_nullability_golden() {
        for (sql, nullable) in [
            // A fallible cast in the first, the second or both branches.
            (
                "SELECT CAST(n AS INT) AS c FROM t UNION ALL SELECT CAST(n AS INT) FROM u",
                true,
            ),
            (
                "SELECT CAST(x AS INT) AS c FROM t UNION ALL SELECT CAST(n AS INT) FROM u",
                true,
            ),
            (
                "SELECT CAST(n AS INT) AS c FROM t UNION ALL SELECT CAST(x AS INT) FROM u",
                true,
            ),
            // Two non-null direct columns stay non-null.
            ("SELECT x AS c FROM t UNION SELECT x FROM u", false),
            ("SELECT x AS c FROM t UNION ALL SELECT x FROM u", false),
            // One nullable branch makes the column nullable, on either side.
            ("SELECT x AS c FROM t UNION ALL SELECT z FROM u", true),
            ("SELECT z AS c FROM t UNION ALL SELECT x FROM u", true),
            // INTERSECT and EXCEPT.
            ("SELECT x AS c FROM t INTERSECT SELECT x FROM u", false),
            (
                "SELECT x AS c FROM t INTERSECT SELECT CAST(n AS INT) FROM u",
                true,
            ),
            ("SELECT x AS c FROM t EXCEPT SELECT x FROM u", false),
            (
                "SELECT CAST(n AS INT) AS c FROM t EXCEPT SELECT x FROM u",
                true,
            ),
            // Nested: parenthesised and chained set operations.
            (
                "SELECT x AS c FROM t UNION ALL (SELECT x FROM u UNION ALL SELECT CAST(n AS INT) FROM u)",
                true,
            ),
            (
                "(SELECT x AS c FROM t UNION SELECT x FROM u) EXCEPT SELECT x FROM u",
                false,
            ),
            (
                "SELECT x AS c FROM t UNION ALL SELECT x FROM u UNION ALL SELECT z FROM u",
                true,
            ),
            // A COUNT in one branch and a nullable SUM in the other.
            (
                "SELECT COUNT(x) AS c FROM t UNION ALL SELECT SUM(x) FROM u",
                true,
            ),
            // A set operation inside a CTE.
            (
                "WITH w AS (SELECT CAST(n AS INT) AS c FROM t UNION ALL SELECT x FROM u) \
                 SELECT c FROM w",
                true,
            ),
        ] {
            let rows = typecheck_over_t_and_u(sql);
            assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
            assert_eq!(rows[0].0, "c", "{sql}: {rows:?}");
            assert_eq!(rows[0].2, nullable, "{sql}: {rows:?}");
        }
    }

    /// #2303: the output name comes from the first branch, and the type is
    /// the common supertype, `Unknown` when the branches have none.
    #[test]
    fn set_operation_names_and_types() {
        let rows = typecheck_over_t_and_u(
            "SELECT x AS first_name, n AS second_name FROM t \
             UNION ALL SELECT x AS other, n AS another FROM u",
        );
        assert_eq!(
            rows,
            vec![
                ("first_name".to_string(), RockyType::Int32, false),
                ("second_name".to_string(), RockyType::String, false),
            ]
        );
        // INT with STRING has no common supertype.
        let rows = typecheck_over_t_and_u("SELECT x AS c FROM t UNION ALL SELECT n FROM u");
        assert_eq!(rows[0].1, RockyType::Unknown, "{rows:?}");
        // A fallible cast to INT in one branch, a plain INT column in the other.
        let rows =
            typecheck_over_t_and_u("SELECT CAST(n AS INT) AS c FROM t UNION ALL SELECT x FROM u");
        assert_eq!(rows[0].1, RockyType::Int32, "{rows:?}");
    }

    /// #2304: lineage reads a CTE column as the same-named column of a source
    /// or model, so a CTE that shadows one (`WITH t AS (...) SELECT x FROM t`)
    /// shows the outer `Direct` column the table's type. With a set operation
    /// inside the CTE that type can be wrong; without one it must not change.
    #[test]
    fn set_operation_in_cte_does_not_keep_the_shadowed_type() {
        // Not shadowing: lineage has no source column, so the type is Unknown.
        for sql in [
            "WITH w AS (SELECT x AS c FROM t UNION ALL SELECT n FROM u) SELECT c FROM w",
            "SELECT c FROM (SELECT x AS c FROM t UNION ALL SELECT n FROM u) AS w",
        ] {
            let rows = typecheck_over_t_and_u(sql);
            assert_eq!(rows[0].1, RockyType::Unknown, "{sql}: {rows:?}");
        }
        // Shadowing source `t`: INT with STRING has no common type.
        let rows = typecheck_over_t_and_u(
            "WITH t AS (SELECT x FROM t UNION ALL SELECT n FROM u) SELECT x FROM t",
        );
        assert_eq!(rows[0].1, RockyType::Unknown, "{rows:?}");
        // Branches that agree keep their type.
        let rows = typecheck_over_t_and_u(
            "WITH t AS (SELECT x FROM u UNION ALL SELECT x FROM u) SELECT x FROM t",
        );
        assert_eq!(rows, vec![("x".to_string(), RockyType::Int32, false)]);
        // No set operation: type and nullability do not change.
        let rows = typecheck_over_t_and_u("WITH t AS (SELECT x FROM u) SELECT x FROM t");
        assert_eq!(rows, vec![("x".to_string(), RockyType::Int32, false)]);
    }

    /// #2304: when inference fails, a `Direct` column of a model with a set
    /// operation does not keep the first branch's type.
    #[test]
    fn uninferable_set_operation_types_direct_column_unknown() {
        let rows = typecheck_over_t_and_u("SELECT x AS c FROM t UNION ALL VALUES ('a')");
        assert_eq!(rows[0].1, RockyType::Unknown, "{rows:?}");
        assert!(rows[0].2, "{rows:?}");
        // Without a set operation a plain query keeps its type.
        let rows = typecheck_over_t_and_u("SELECT x AS c FROM t");
        assert_eq!(rows[0].1, RockyType::Int32, "{rows:?}");
    }

    /// #2303: branches that cannot be paired by position give no answer. The
    /// safety net then makes every column nullable and a non-Direct column
    /// Unknown, so no NOT NULL claim survives.
    #[test]
    fn uninferable_query_claims_no_not_null() {
        // Different column counts.
        let rows = typecheck_over_t_and_u(
            "SELECT CAST(n AS INT) AS c, x AS d FROM t UNION ALL SELECT x FROM u",
        );
        for (name, ty, nullable) in &rows {
            assert!(*nullable, "{name}: {rows:?}");
            if name == "c" {
                assert_eq!(*ty, RockyType::Unknown, "{rows:?}");
            }
        }
        // A `VALUES` branch is a query form inference does not read.
        let rows = typecheck_over_t_and_u("SELECT CAST(n AS INT) AS c FROM t UNION ALL VALUES (1)");
        assert!(rows[0].2, "{rows:?}");
    }

    /// #2303: a fallible cast inside a UNION fails a `nullable = false`
    /// contract (E012).
    #[test]
    fn fallible_cast_in_union_fails_not_null_contract() {
        let contract = |nullable| CompilerContract {
            columns: vec![ContractColumn {
                name: "c".to_string(),
                type_name: Some("Int32".to_string()),
                nullable: Some(nullable),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let typed = |sql: &str| {
            let sources = HashMap::from([(
                "t".to_string(),
                source_schema(&[
                    ("n", RockyType::String, false),
                    ("x", RockyType::Int32, false),
                ]),
            )]);
            let project = Project::from_models(vec![make_model("m", sql)]).unwrap();
            let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
            typecheck_project_with_models(&graph, &sources, None, &project.models, None)
                .typed_models["m"]
                .clone()
        };
        let fallible = typed("SELECT x AS c FROM t UNION ALL SELECT CAST(n AS INT) FROM t");
        assert!(
            validate_contract("m", &fallible, &contract(false))
                .iter()
                .any(|d| &*d.code == "E012")
        );
        let safe = typed("SELECT x AS c FROM t UNION ALL SELECT x FROM t");
        assert!(
            !validate_contract("m", &safe, &contract(false))
                .iter()
                .any(|d| &*d.code == "E012")
        );
    }

    /// #2303 red team: a set operation whose columns are all nullable still
    /// runs inference, so it does not keep the first branch's concrete type.
    #[test]
    fn all_nullable_union_does_not_keep_the_first_branch_type() {
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[
                ("z", RockyType::Int32, true),
                ("n", RockyType::String, true),
            ]),
        )]);
        let project = Project::from_models(vec![make_model(
            "m",
            "SELECT z AS c FROM t UNION ALL SELECT n FROM t",
        )])
        .unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let typed = typecheck_project_with_models(&graph, &sources, None, &project.models, None)
            .typed_models["m"]
            .clone();
        let c = typed.iter().find(|col| col.name == "c").unwrap();
        assert_ne!(c.data_type, RockyType::Int32, "{c:?}");
        assert!(c.nullable);
        assert!(super::sql_mentions_set_operation(
            "select a from t Union select b from u"
        ));
        assert!(!super::sql_mentions_set_operation(
            "select unionized from t"
        ));
    }

    /// #2299 at the expression level: decimal narrowing, an unknown input and
    /// the safe conversions.
    #[test]
    fn cast_fallibility_by_source_type() {
        let d = |precision, scale| RockyType::Decimal { precision, scale };
        let mut scope = TypeScope::with_target(duckdb());
        for (name, ty) in [
            ("d102", d(10, 2)),
            ("i64", RockyType::Int64),
            ("f64", RockyType::Float64),
            ("dt", RockyType::Date),
            ("ts", RockyType::Timestamp),
            ("b", RockyType::Boolean),
            ("u", RockyType::Unknown),
        ] {
            scope
                .columns
                .insert(CiKey::owned(name.to_string()), (ty, false));
        }
        for (expr, nullable) in [
            ("CAST(d102 AS DECIMAL(12,2))", false),
            ("CAST(d102 AS DECIMAL(10,2))", false),
            ("CAST(d102 AS DECIMAL(5,2))", true),
            ("CAST(d102 AS DECIMAL(12,0))", true),
            ("CAST(d102 AS DOUBLE)", false),
            ("CAST(d102 AS BIGINT)", true),
            ("CAST(i64 AS DOUBLE)", false),
            ("CAST(i64 AS INT)", true),
            // 19 integer digits hold every 64-bit integer (#2333).
            ("CAST(i64 AS DECIMAL(20,0))", false),
            ("CAST(i64 AS DECIMAL(19,0))", false),
            ("CAST(i64 AS DECIMAL(20,2))", true),
            ("CAST(f64 AS FLOAT)", true),
            ("CAST(f64 AS BIGINT)", true),
            ("CAST(dt AS TIMESTAMP)", false),
            ("CAST(ts AS DATE)", false),
            ("CAST(dt AS INT)", true),
            ("CAST(b AS INT)", false),
            ("CAST(u AS INT)", true),
            ("CAST(u AS STRING)", true),
            ("CAST(0 AS DECIMAL(18,2))", false),
            ("CAST(1000 AS DECIMAL(5,2))", true),
            ("CAST(100 AS DECIMAL(5,2))", false),
            ("CAST(99 AS DECIMAL(5,2))", false),
            ("CAST(300 AS TINYINT)", true),
            ("CAST(100 AS TINYINT)", false),
            ("CAST(3000000000 AS INT)", true),
            ("CAST(7 AS INT)", false),
        ] {
            let (_, got) = infer_expr_type(&parse_expr(expr), &scope);
            assert_eq!(got, nullable, "{expr}");
        }
    }

    /// Golden table for #2295. Lineage used to overwrite a nested call's edge
    /// kind with the outer one, so the column's type and nullability came
    /// from the bare input column. Each row gives the old answer in a comment.
    /// The invariant: inference may wrongly say nullable, never non-null.
    #[test]
    fn nested_expression_types_and_nullability_golden() {
        for source in ["t", "cat.sch.t"] {
            let sql = format!(
                "SELECT CAST(NULLIF(x, 0) AS INT) AS a, CAST(MAX(x) AS BIGINT) AS b, \
                 MAX(LENGTH(n)) AS c, SUM(CAST(y AS DOUBLE)) AS d, COUNT(*) AS e, \
                 COUNT(x) AS f, COUNT(DISTINCT x) AS g, MAX(CAST(y AS BIGINT)) AS h, \
                 SUM(CAST(y AS DECIMAL(10,2))) AS i FROM {source}"
            );
            let expected = vec![
                // old: (Int32, false) — unsound, NULL when x = 0
                ("a".to_string(), RockyType::Int32, true),
                // old: (Int64, false) — unsound, NULL on an empty group
                ("b".to_string(), RockyType::Int64, true),
                // old: (String, true) — n's type, not an integer. LENGTH's
                // width is dialect-dependent, so Unknown over a guess.
                ("c".to_string(), RockyType::Unknown, true),
                // old: (Int64, true) — y's integer type
                ("d".to_string(), RockyType::Float64, true),
                // old: (Unknown, true) — no lineage edge
                ("e".to_string(), RockyType::Int64, false),
                // unchanged
                ("f".to_string(), RockyType::Int64, false),
                // unchanged — reaches inference as "COUNT"
                ("g".to_string(), RockyType::Int64, false),
                // old: (Int32, true) — y's type, not the cast target
                ("h".to_string(), RockyType::Int64, true),
                // old: (Int64, true) — y's integer type. SUM widens a
                // DECIMAL by a dialect-dependent amount: Unknown.
                ("i".to_string(), RockyType::Unknown, true),
            ];
            assert_eq!(typecheck_over_t(source, &sql), expected, "FROM {source}");
        }
    }

    /// `NULLIF(x, y)` is `x` or NULL: it has the type of `x`, and is nullable,
    /// when `y` cannot widen it. Over an expression whose type is a guess
    /// (`LENGTH`), or against a value that can promote it (`0.5`, a column of
    /// another type), it stays Unknown.
    #[test]
    fn nullif_takes_the_type_of_its_first_argument() {
        let sql = "SELECT NULLIF(x, 0) AS a, NULLIF(LENGTH(n), 0) AS b, \
                   NULLIF(x, 0.5) AS c, NULLIF(x, n) AS d, NULLIF(x, x) AS e FROM t";
        let expected = vec![
            ("a".to_string(), RockyType::Int32, true),
            ("b".to_string(), RockyType::Unknown, true),
            ("c".to_string(), RockyType::Unknown, true),
            ("d".to_string(), RockyType::Unknown, true),
            ("e".to_string(), RockyType::Int32, true),
        ];
        assert_eq!(typecheck_over_t("t", sql), expected);
    }

    /// #2318: an outer MAX / SUM over a CTE column built from an expression
    /// must not take the type of the one column that expression reads.
    #[test]
    fn an_outer_aggregate_over_a_cte_expression_is_not_typed_from_its_input() {
        let sql = "WITH c AS (SELECT CASE WHEN x > 0 THEN 'hi' ELSE 'lo' END AS label, \
                   x * 1.5 AS amt FROM t) \
                   SELECT MAX(label) AS a, SUM(amt) AS b, CAST(amt AS BIGINT) AS d FROM c";
        // `x` is INT: no result may be an integer from `x`. `MAX` over the
        // all-text CASE is text, and NULL over zero rows. `SUM(amt)` reads
        // `x * 1.5`, whose type Rocky guesses, so it is Unknown (#2320).
        // `CAST(amt AS BIGINT)` is the cast's target on DuckDB.
        assert_eq!(
            typecheck_over_t("t", sql),
            vec![
                ("a".to_string(), RockyType::String, true),
                ("b".to_string(), RockyType::Unknown, true),
                ("d".to_string(), RockyType::Int64, false),
            ]
        );
        // Scenarios A and B: a wrapper over a cast or an aggregate column.
        let typed = typecheck_over_t(
            "t",
            "WITH c AS (SELECT CAST(x AS VARCHAR) AS s, COUNT(n) AS k, MAX(x) AS mx FROM t) \
             SELECT MAX(s) AS a, MAX(k) AS b, SUM(k) AS e, CAST(mx AS BIGINT) AS f FROM c",
        );
        let by_name: HashMap<_, _> = typed
            .iter()
            .map(|(n, t, nl)| (n.as_str(), (t.clone(), *nl)))
            .collect();
        // Each outer aggregate is NULL over zero rows.
        assert_eq!(by_name["a"], (RockyType::String, true));
        assert_eq!(by_name["b"], (RockyType::Int64, true));
        // `SUM` of a BIGINT is wider than BIGINT on some warehouses (DuckDB
        // HUGEINT); only that it is not text, and nullable, is pinned here.
        assert!(!matches!(by_name["e"].0, RockyType::String), "{by_name:?}");
        assert!(by_name["e"].1, "{by_name:?}");
        // MAX(x) is NULL over zero rows, so the cast of it is nullable even
        // though `x` is NOT NULL.
        assert_eq!(by_name["f"], (RockyType::Int64, true));
    }

    /// #2320: a CTE column is exact only when the expression that built it
    /// is. `LENGTH`'s width and `x * 1.5`'s type are guesses, so an outer
    /// aggregate over them is Unknown, as the same expression written inline
    /// is (#2295). A CTE column that is a cast or a bare column stays exact,
    /// also through `SELECT *` and a qualified read.
    #[test]
    fn an_outer_expression_over_a_cte_column_is_exact_only_when_the_column_is() {
        type Expected<'a> = Vec<(&'a str, RockyType, bool)>;
        let cases: Vec<(&str, Expected)> = vec![
            (
                "WITH c AS (SELECT LENGTH(n) AS l FROM t) \
                 SELECT MAX(l) AS m, MAX(c.l) AS q FROM c",
                vec![
                    ("m", RockyType::Unknown, true),
                    ("q", RockyType::Unknown, true),
                ],
            ),
            // The inline form, for comparison.
            (
                "SELECT MAX(LENGTH(n)) AS m FROM t",
                vec![("m", RockyType::Unknown, true)],
            ),
            (
                "WITH c AS (SELECT x * 1.5 AS amt FROM t) SELECT SUM(amt) AS s FROM c",
                vec![("s", RockyType::Unknown, true)],
            ),
            // Controls: a cast and a bare column keep their types.
            (
                "WITH c AS (SELECT CAST(x AS DOUBLE) AS d, x FROM t) \
                 SELECT MAX(d) AS m, MAX(c.d) AS q, MAX(x) AS mx FROM c",
                vec![
                    ("m", RockyType::Float64, true),
                    ("q", RockyType::Float64, true),
                    ("mx", RockyType::Int32, true),
                ],
            ),
            // `SELECT *` carries each column's exactness.
            (
                "WITH c AS (SELECT LENGTH(n) AS l, CAST(x AS DOUBLE) AS d FROM t), \
                 w AS (SELECT * FROM c) SELECT MAX(l) AS m, MAX(d) AS md FROM w",
                vec![
                    ("m", RockyType::Unknown, true),
                    ("md", RockyType::Float64, true),
                ],
            ),
        ];
        for (sql, expected) in cases {
            let expected: Vec<(String, RockyType, bool)> = expected
                .into_iter()
                .map(|(n, t, nl)| (n.to_string(), t, nl))
                .collect();
            assert_eq!(typecheck_over_t("t", sql), expected, "{sql}");
        }
    }

    /// #2320 one layer down: the exact flag an outer query reads for a CTE or
    /// derived-table column. A derived table keeps it the way a CTE does.
    #[test]
    fn a_cte_or_derived_column_built_from_a_guess_is_not_exact() {
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[
                ("x", RockyType::Int32, false),
                ("n", RockyType::String, false),
            ]),
        )]);
        for (sql, exact) in [
            (
                "WITH c AS (SELECT LENGTH(n) AS l FROM t) SELECT MAX(l) FROM c",
                false,
            ),
            ("SELECT MAX(l) FROM (SELECT LENGTH(n) AS l FROM t) s", false),
            (
                "SELECT MAX(s.l) FROM (SELECT LENGTH(n) AS l FROM t) s",
                false,
            ),
            (
                "SELECT MAX(l) FROM (SELECT * FROM (SELECT LENGTH(n) AS l FROM t) a) s",
                false,
            ),
            (
                "SELECT MAX(d) FROM (SELECT CAST(x AS DOUBLE) AS d FROM t) s",
                true,
            ),
            ("WITH c AS (SELECT x FROM t) SELECT MAX(x) FROM c", true),
            // A USING key is a guess when either side's is.
            (
                "SELECT MAX(l) FROM (SELECT LENGTH(n) AS l FROM t) a \
                 JOIN (SELECT LENGTH(n) AS l FROM t) b USING (l)",
                false,
            ),
            // A set operation is exact only when both branches are.
            (
                "WITH c AS (SELECT CAST(x AS DOUBLE) AS d FROM t \
                 UNION ALL SELECT CAST(n AS DOUBLE) FROM t) SELECT MAX(d) FROM c",
                true,
            ),
            (
                "WITH c AS (SELECT CAST(x AS DOUBLE) AS d FROM t \
                 UNION ALL SELECT LENGTH(n) FROM t) SELECT MAX(d) FROM c",
                false,
            ),
        ] {
            let inferred = infer_select_types_with_lookup(
                sql,
                &|name| sources.get(name).map(Vec::as_slice),
                &duckdb(),
            )
            .unwrap();
            assert_eq!(inferred.exact_type_outputs.contains(&0), exact, "{sql}");
        }
    }

    /// Golden table for #2298: expression -> (type, nullable) through direct
    /// inference. Source `t` has `x INT NOT NULL`, `n STRING NOT NULL`,
    /// `y INT NOT NULL`, plus nullable `nx INT`, `nn STRING`.
    #[test]
    fn null_preserving_scalars_golden() {
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[
                ("x", RockyType::Int32, false),
                ("n", RockyType::String, false),
                ("y", RockyType::Int32, false),
                ("nx", RockyType::Int32, true),
                ("nn", RockyType::String, true),
                ("f", RockyType::Float64, false),
                (
                    "d",
                    RockyType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    false,
                ),
            ]),
        )]);
        let s = RockyType::String;
        let i64t = RockyType::Int64;
        let i32t = RockyType::Int32;
        let cases: Vec<(&str, RockyType, bool)> = vec![
            // null-preserving over non-null input: non-null
            ("UPPER(n)", s.clone(), false),
            ("LOWER(n)", s.clone(), false),
            ("TRIM(n)", s.clone(), false),
            ("LTRIM(n)", s.clone(), false),
            ("RTRIM(n)", s.clone(), false),
            ("REVERSE(n)", s.clone(), false),
            ("LENGTH(n)", i64t.clone(), false),
            ("CHAR_LENGTH(n)", i64t.clone(), false),
            ("SUBSTRING(n, 1, 2)", s.clone(), false),
            ("SUBSTR(n, 2)", s.clone(), false),
            ("LPAD(n, 5, '0')", s.clone(), false),
            ("RPAD(n, 5, '0')", s.clone(), false),
            ("CONCAT(n, 'a')", s.clone(), false),
            ("ABS(x)", i32t.clone(), false),
            ("ROUND(x)", i32t.clone(), false),
            ("FLOOR(x)", i32t.clone(), false),
            ("CEIL(x)", i32t.clone(), false),
            ("CEILING(x)", i32t.clone(), false),
            ("SIGN(x)", i32t.clone(), false),
            ("UPPER(TRIM(n))", s.clone(), false),
            // a nullable argument stays nullable
            ("UPPER(nn)", s.clone(), true),
            ("LENGTH(nn)", i64t.clone(), true),
            ("ABS(nx)", i32t.clone(), true),
            ("CONCAT(n, nn)", s.clone(), true),
            ("SUBSTRING(nn, 1, 2)", s.clone(), true),
            ("LPAD(n, 5, nn)", s.clone(), true),
            ("UPPER(NULL)", s.clone(), true),
            ("UPPER(TRIM(nn))", s.clone(), true),
            // decimal numerics stay nullable (Spark overflow gives NULL)
            (
                "ROUND(d, 1)",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
            (
                "FLOOR(d)",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
            (
                "CEIL(d)",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
            // regex SUBSTRING returns NULL on no match: nullable
            ("SUBSTRING(n FROM 'a+')", s.clone(), true),
            ("SUBSTRING(n, 'a+')", s.clone(), true),
            ("SUBSTRING(n FROM 'a' FOR '#')", s.clone(), true),
            // a string start is nullable; an integer FROM/FOR form is not
            ("SUBSTRING(n, n)", s.clone(), true),
            ("SUBSTR(n, '1')", s.clone(), true),
            ("SUBSTRING(n FROM 1 FOR 2)", s.clone(), false),
            ("LPAD(n, '5', '0')", s.clone(), true),
            // ANSI-off Spark: a non-numeric string casts to NULL
            ("ABS(n)", s.clone(), true),
            ("FLOOR(n)", s.clone(), true),
            ("ROUND(n, 1)", s.clone(), true),
            // Unknown never qualifies
            ("ABS(UNKNOWN_FN(x))", RockyType::Unknown, true),
            ("ABS((SELECT 1))", RockyType::Unknown, true),
            // float input stays non-null
            ("ABS(f)", RockyType::Float64, false),
            // not in the table: stay nullable
            ("NULLIF(x, 0)", i32t.clone(), true),
            ("x / y", i32t.clone(), true),
            ("x % y", i32t.clone(), true),
            ("LEFT(n, 2)", s.clone(), true),
            ("REPLACE(n, 'a', 'b')", s.clone(), true),
            ("CONCAT_WS(',', n, n)", s.clone(), true),
            ("SQRT(x)", RockyType::Float64, true),
            // a malformed call stays nullable
            ("UPPER()", s.clone(), true),
        ];
        let mut failures = Vec::new();
        for (expr, ty, nullable) in cases {
            let sql = format!("SELECT {expr} AS c FROM t");
            let columns = infer_select_types(&sql, &sources, "m").unwrap();
            let got = (columns[0].data_type.clone(), columns[0].nullable);
            if got != (ty.clone(), nullable) {
                failures.push(format!("{expr}: got {got:?}, want {:?}", (ty, nullable)));
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// #2298: `CAST(UPPER(name) AS STRING)` over a NOT NULL column is non-null
    /// through the full typecheck, and satisfies a `nullable = false` contract
    /// (no E012). The nullable and unlisted-function variants still fail it.
    #[test]
    fn cast_over_null_preserving_scalar_satisfies_not_null_contract() {
        let contract = |type_name: &str| CompilerContract {
            columns: vec![ContractColumn {
                name: "c".to_string(),
                type_name: Some(type_name.to_string()),
                nullable: Some(false),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[
                ("n", RockyType::String, false),
                ("nn", RockyType::String, true),
                ("x", RockyType::Int32, false),
            ]),
        )]);
        for (expr, type_name, expect_e012) in [
            ("CAST(UPPER(n) AS STRING)", "String", false),
            ("CAST(LENGTH(n) AS BIGINT)", "Int64", false),
            ("CAST(ABS(x) AS INT)", "Int32", false),
            ("CAST(UPPER(nn) AS STRING)", "String", true),
            ("CAST(LEFT(n, 2) AS STRING)", "String", true),
            ("CAST(NULLIF(n, 'a') AS STRING)", "String", true),
            ("CAST(x / x AS INT)", "Int32", true),
        ] {
            let sql = format!("SELECT {expr} AS c FROM t");
            let project = Project::from_models(vec![make_model("m", &sql)]).unwrap();
            let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
            let result = typecheck_project_for_targets(
                &graph,
                &sources,
                &project.models,
                None,
                &TargetDialects::uniform(duckdb()),
            );
            let columns = &result.typed_models["m"];
            let has_e012 = validate_contract("m", columns, &contract(type_name))
                .iter()
                .any(|d| &*d.code == "E012");
            assert_eq!(has_e012, expect_e012, "{expr}: {columns:?}");
        }
    }

    /// Controls: a bare column, a cast over one and a direct aggregate keep
    /// their answers.
    #[test]
    fn direct_edges_keep_their_types_after_nested_fix() {
        let rows = typecheck_over_t(
            "t",
            "SELECT x, CAST(x AS BIGINT) AS cx, TRY_CAST(n AS INT) AS tn FROM t",
        );
        assert_eq!(
            rows,
            vec![
                ("x".to_string(), RockyType::Int32, false),
                ("cx".to_string(), RockyType::Int64, false),
                ("tn".to_string(), RockyType::Int32, true),
            ]
        );
        let rows = typecheck_over_t("t", "SELECT MAX(x) AS mx, SUM(y) AS sy FROM t");
        assert_eq!(
            rows,
            vec![
                ("mx".to_string(), RockyType::Int32, true),
                ("sy".to_string(), RockyType::Int64, true),
            ]
        );
    }

    /// A model whose only column is `COUNT(*)` has no lineage edge at all, so
    /// expression inference must run for edge-less columns too.
    #[test]
    fn count_star_alone_is_non_null_bigint() {
        assert_eq!(
            typecheck_over_t("t", "SELECT COUNT(*) AS c FROM t"),
            vec![("c".to_string(), RockyType::Int64, false)]
        );
        // Other edge-less projections stay Unknown: a literal's width is not
        // known from the SQL alone.
        assert_eq!(
            typecheck_over_t("t", "SELECT 1 AS one FROM t"),
            vec![("one".to_string(), RockyType::Unknown, true)]
        );
    }

    /// When expression inference cannot run (a set operation), the edge kind
    /// is the only answer. A cast over an aggregate must then be nullable, not
    /// the input column's NOT NULL.
    #[test]
    fn nested_cast_is_nullable_without_expression_inference() {
        let rows = typecheck_over_t(
            "t",
            "SELECT CAST(MAX(x) AS BIGINT) AS b FROM t \
             UNION ALL SELECT CAST(MAX(x) AS BIGINT) AS b FROM t",
        );
        assert_eq!(rows.len(), 1);
        // old: (Unknown, false)
        assert!(rows[0].2, "CAST(MAX(x)) can be NULL: {rows:?}");
    }

    #[test]
    fn division_and_modulo_are_nullable() {
        let scope = TypeScope::new();
        for sql in ["4 / 2", "4 % 2"] {
            let expr = parse_expr(sql);
            let (_, nullable) = infer_expr_type(&expr, &scope);
            assert!(nullable, "{sql}: division by zero can return NULL");
        }
        // The integer-division operators do not parse in the Databricks
        // dialect; build them directly.
        for op in [
            ast::BinaryOperator::DuckIntegerDivide,
            ast::BinaryOperator::MyIntegerDivide,
        ] {
            let expr = Expr::BinaryOp {
                left: Box::new(parse_expr("4")),
                op: op.clone(),
                right: Box::new(parse_expr("2")),
            };
            let (_, nullable) = infer_expr_type(&expr, &scope);
            assert!(nullable, "{op}: division by zero can return NULL");
        }
        let (_, nullable) = infer_expr_type(&parse_expr("4 * 2"), &scope);
        assert!(
            !nullable,
            "multiplication of non-null literals stays non-null"
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
        let columns = infer_select_types_with_lookup(
            "SELECT u.id FROM raw.users u",
            &|name| sources.get(name).map(Vec::as_slice),
            &OperandTarget::Unconfigured,
        )
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
            &TargetDialects::default(),
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
        // This assertion used to be "no diagnostic mentions avg_amount at
        // all", which was satisfied by the #1240 fail-open: the column was not
        // checked and nobody was told. The claim being protected is narrower —
        // no *false mismatch* — so it is now scoped to E011, and paired with a
        // positive check that the withheld type is reported at info severity.
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| &*diagnostic.code != "E011"
                    || !diagnostic.message.contains("avg_amount")),
            "dialect-dependent Decimal AVG should not produce a false mismatch: {diagnostics:?}"
        );
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| &*diagnostic.code == "I003"
                    && diagnostic.message.contains("avg_amount")),
            "the unchecked Decimal AVG contract type must be reported: {diagnostics:?}"
        );
    }

    /// The #1240 case end to end: `AVG` over a `DECIMAL` input infers
    /// `Unknown`, so a contract that declares `Boolean` for it cannot be
    /// checked. That must not fail the build — and must not be silent either.
    #[test]
    fn test_avg_over_decimal_reports_an_unchecked_contract_type() {
        let models = vec![make_model(
            "averages",
            "SELECT AVG(amount) AS avg_amount FROM source.raw.orders",
        )];
        let project = Project::from_models(models).unwrap();

        let mut external = HashMap::new();
        external.insert(
            "source.raw.orders".to_string(),
            vec![rocky_ir::ColumnInfo {
                name: "amount".to_string(),
                data_type: "DECIMAL(10,2)".to_string(),
                nullable: false,
            }],
        );
        let graph = build_semantic_graph(&project, &external).unwrap();

        let mut sources = HashMap::new();
        sources.insert(
            "source.raw.orders".to_string(),
            source_schema(&[(
                "amount",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                false,
            )]),
        );

        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        let columns = &result.typed_models["averages"];
        let avg_amount = columns.iter().find(|col| col.name == "avg_amount").unwrap();
        assert_eq!(
            avg_amount.data_type,
            RockyType::Unknown,
            "the Decimal AVG carve-out must still withhold a type"
        );

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

        let i003 = diagnostics
            .iter()
            .find(|diagnostic| &*diagnostic.code == "I003")
            .unwrap_or_else(|| panic!("expected I003, got {diagnostics:?}"));
        assert!(
            i003.message.contains("avg_amount") && i003.message.contains("Boolean"),
            "I003 must name the column and the declared type: {i003:?}"
        );
        assert!(
            !diagnostics
                .iter()
                .any(crate::diagnostic::Diagnostic::is_error),
            "an unresolved type must not fail the build: {diagnostics:?}"
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

    /// Type the first output column of `SELECT <projection> FROM
    /// source.raw.users`. `id_type` is the type the source schema gives `id`
    /// (`None` = no schema for the table at all).
    fn first_column_over(projection: &str, id_type: Option<RockyType>) -> TypedColumn {
        let models = vec![make_model(
            "casted",
            &format!("SELECT {projection} FROM source.raw.users"),
        )];
        let project = Project::from_models(models).unwrap();
        let mut external = HashMap::new();
        let mut sources = HashMap::new();
        if let Some(id_type) = id_type {
            external.insert(
                "source.raw.users".to_string(),
                vec![rocky_ir::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "SOME_UNMAPPED_TYPE".to_string(),
                    nullable: true,
                }],
            );
            sources.insert(
                "source.raw.users".to_string(),
                source_schema(&[("id", id_type, true)]),
            );
        }
        let graph = build_semantic_graph(&project, &external).unwrap();
        let result = typecheck_project_with_models(&graph, &sources, None, &project.models, None);
        result.typed_models["casted"][0].clone()
    }

    #[test]
    fn test_cast_over_unknown_input_takes_the_target_type() {
        // A cast's output type is its target, whatever the input is. The
        // input column resolves to `Unknown` (a source type Rocky cannot map)
        // and the target is still `STRING`. Nullability stays what Rocky can
        // back: the input is unknown, so the column is nullable (the old
        // "leave it Unknown" rule of #1145 existed to avoid guessing that).
        let col = first_column_over("CAST(id AS STRING) AS id", Some(RockyType::Unknown));
        assert_eq!(col.data_type, RockyType::String);
        assert!(col.nullable, "an unresolved input must stay nullable");
    }

    #[test]
    fn test_cast_with_no_source_schema_takes_the_target_type() {
        let decimal = RockyType::Decimal {
            precision: 12,
            scale: 2,
        };
        for projection in [
            "CAST(id AS DECIMAL(12, 2)) AS id",
            "id::DECIMAL(12, 2) AS id",
            "TRY_CAST(id AS DECIMAL(12, 2)) AS id",
            "SAFE_CAST(id AS DECIMAL(12, 2)) AS id",
            "CAST(id + 1 AS DECIMAL(12, 2)) AS id",
            "CAST('1.50' AS DECIMAL(12, 2)) AS id",
            "(CAST(id AS DECIMAL(12, 2))) AS id",
        ] {
            let col = first_column_over(projection, None);
            assert_eq!(col.data_type, decimal, "{projection}");
            assert!(col.nullable, "{projection}: nothing proves non-null");
        }
    }

    #[test]
    fn test_cast_to_a_bare_decimal_stays_unknown_without_a_schema() {
        // A bare `DECIMAL` names no digits, so the target is not a type
        // (#1721). The cast must not manufacture one.
        for projection in [
            "CAST(id AS DECIMAL) AS id",
            "id::NUMERIC AS id",
            "TRY_CAST(id AS DECIMAL) AS id",
        ] {
            let col = first_column_over(projection, None);
            assert_eq!(col.data_type, RockyType::Unknown, "{projection}");
        }
    }

    #[test]
    fn test_cast_to_a_warehouse_dependent_type_stays_unknown_over_unknown_input() {
        // FLOAT / REAL / INT / INTEGER / SMALLINT / TIMESTAMP mean different
        // widths on Snowflake, PostgreSQL and Databricks. Without a known
        // input the cast must not pick one.
        for projection in [
            "CAST(id AS FLOAT) AS id",
            "CAST(id AS REAL) AS id",
            "CAST(id AS INTEGER) AS id",
            "id::INT AS id",
            "CAST(id AS SMALLINT) AS id",
            "CAST(id AS TIMESTAMP) AS id",
            "TRY_CAST(id AS FLOAT) AS id",
            // Snowflake's BIGINT is NUMBER(38,0), which Rocky reads as
            // Decimal(38,0), not Int64.
            "CAST(id AS BIGINT) AS id",
            "TRY_CAST(id AS BIGINT) AS id",
            "CAST(id AS INT64) AS id",
            // Digits outside 1 <= p <= 38, 0 <= s <= p name no type.
            "CAST(id AS NUMERIC(300, 0)) AS id",
            "CAST(id AS DECIMAL(39, 0)) AS id",
            "CAST(id AS DECIMAL(0)) AS id",
            "CAST(id AS DECIMAL(10, 11)) AS id",
        ] {
            let col = first_column_over(projection, None);
            assert_eq!(col.data_type, RockyType::Unknown, "{projection}");
        }
        for (projection, expected) in [
            ("CAST(id AS DOUBLE) AS id", RockyType::Float64),
            ("CAST(id AS DATE) AS id", RockyType::Date),
            (
                "CAST(id AS DECIMAL(38, 0)) AS id",
                RockyType::Decimal {
                    precision: 38,
                    scale: 0,
                },
            ),
            (
                "CAST(id AS NUMERIC(10)) AS id",
                RockyType::Decimal {
                    precision: 10,
                    scale: 0,
                },
            ),
        ] {
            let col = first_column_over(projection, None);
            assert_eq!(col.data_type, expected, "{projection}");
        }
    }

    #[test]
    fn test_try_cast_over_unknown_input_is_typed_and_nullable() {
        let col = first_column_over("TRY_CAST(id AS DECIMAL(18, 2)) AS id", None);
        assert_eq!(
            col.data_type,
            RockyType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        assert!(col.nullable);
    }

    #[test]
    fn test_out_of_range_decimal_digits_never_wrap_into_a_type() {
        // `NUMERIC(300, 0)` used to wrap through `as u8` to Decimal(44, 0).
        for projection in [
            "CAST(id AS NUMERIC(300, 0)) AS id",
            "CAST(id AS DECIMAL(256, 2)) AS id",
            "CAST(id AS DECIMAL(39)) AS id",
            "CAST(id AS DECIMAL(10, 11)) AS id",
        ] {
            let col = first_column_over(projection, Some(RockyType::Int32));
            assert_eq!(col.data_type, RockyType::Unknown, "{projection}");
        }
    }

    #[test]
    fn test_a_function_over_a_cast_is_not_typed_as_the_cast() {
        // Only a projection that IS a cast takes the target. `LENGTH(CAST(..))`
        // is an integer of a width the dialect picks, not the cast's target.
        let col = first_column_over("LENGTH(CAST(id AS STRING)) AS id", None);
        assert_ne!(col.data_type, RockyType::String);
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

            let result = typecheck_project_for_targets(
                &graph,
                &sources,
                &project.models,
                None,
                &TargetDialects::uniform(duckdb()),
            );
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

        let result = typecheck_project_for_targets(
            &graph,
            &sources,
            &project.models,
            None,
            &TargetDialects::uniform(duckdb()),
        );
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

    fn compile_typechecks(models: Vec<Model>) -> TypeCheckResult {
        let project = Project::from_models(models).unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None)
    }

    fn e039_diagnostics(result: &TypeCheckResult) -> Vec<&Diagnostic> {
        result
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code.as_ref() == E039)
            .collect()
    }

    #[test]
    fn known_missing_direct_projection_refuses() {
        let result = compile_typechecks(vec![
            make_model(
                "stg_orders",
                "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
            ),
            make_model("fct_revenue", "SELECT order_id, amount FROM stg_orders"),
        ]);

        let diagnostics = e039_diagnostics(&result);
        assert_eq!(
            diagnostics.len(),
            1,
            "diagnostics: {:?}",
            result.diagnostics
        );
        let diagnostic = diagnostics[0];
        assert_eq!(diagnostic.model, "fct_revenue");
        assert!(diagnostic.message.contains("'amount'"));
        assert!(diagnostic.message.contains("'stg_orders'"));
        assert_eq!(diagnostic.span.as_ref().map(|span| span.line), Some(1));
        assert_eq!(diagnostic.span.as_ref().map(|span| span.col), Some(1));
    }

    /// A snapshot model's table also holds the SCD2 metadata columns, so a
    /// reader projecting them must not get E039; a truly absent column still
    /// does.
    #[test]
    fn snapshot_metadata_columns_are_readable_downstream() {
        let mut snap = make_model(
            "snap",
            "SELECT order_id, amount, updated_at FROM raw.orders",
        );
        snap.config.strategy = toml::from_str(
            "type = \"snapshot\"\nunique_key = \"order_id\"\nstrategy = \"timestamp\"\n\
             updated_at = \"updated_at\"\nsnapshot_meta_column_names = { scd_id = \"version_id\" }",
        )
        .unwrap();
        let result = compile_typechecks(vec![
            snap.clone(),
            make_model(
                "reader",
                "SELECT order_id, valid_from, valid_to, is_current, version_id FROM snap",
            ),
        ]);
        assert!(
            e039_diagnostics(&result).is_empty(),
            "{:?}",
            result.diagnostics
        );

        let result = compile_typechecks(vec![
            snap,
            make_model("reader", "SELECT order_id, snapshot_id FROM snap"),
        ]);
        assert_eq!(
            e039_diagnostics(&result).len(),
            1,
            "{:?}",
            result.diagnostics
        );
    }

    #[test]
    fn known_missing_qualified_projection_refuses_but_valid_alias_passes() {
        for (projection, expected_errors) in [
            ("s.amount", 1),
            ("s.order_amount", 0),
            ("stg_orders.amount", 0),
        ] {
            let sql = format!("SELECT {projection} FROM stg_orders AS s");
            let result = compile_typechecks(vec![
                make_model(
                    "stg_orders",
                    "SELECT amount AS order_amount FROM raw.orders",
                ),
                make_model("consumer", &sql),
            ]);
            assert_eq!(
                e039_diagnostics(&result).len(),
                expected_errors,
                "projection {projection}: {:?}",
                result.diagnostics
            );
        }
    }

    #[test]
    fn known_missing_check_preserves_alias_and_nested_scopes() {
        for sql in [
            "SELECT order_id AS id2, id2 FROM stg_orders",
            "SELECT u FROM stg_orders AS u",
            "SELECT missing FROM stg_orders AS u(order_id, order_amount)",
            "WITH stg_orders AS (SELECT 1 AS missing) SELECT missing FROM stg_orders",
            "SELECT scoped.missing FROM (SELECT 1 AS missing) AS scoped",
            "SELECT sha256(order_amount) AS digest FROM stg_orders",
            "SELECT item FROM stg_orders LATERAL VIEW explode(array(1)) t AS item",
            "SELECT missing FROM stg_orders()",
            "SELECT s.foo FROM stg_orders AS s",
            "SELECT rowid FROM stg_orders",
            "SELECT s.ROWID FROM stg_orders AS s",
            "SELECT _metadata FROM stg_orders",
            "SELECT _PARTITIONTIME FROM stg_orders",
            "SELECT _partitiondate FROM stg_orders",
            "SELECT id_1 FROM stg_orders",
        ] {
            let upstream_sql = match sql {
                "SELECT s.foo FROM stg_orders AS s" => "SELECT payload AS s FROM raw.orders",
                "SELECT id_1 FROM stg_orders" => "SELECT 1 AS id, 2 AS id",
                _ => "SELECT order_id, amount AS order_amount FROM raw.orders",
            };
            let result = compile_typechecks(vec![
                make_model("stg_orders", upstream_sql),
                make_model("consumer", sql),
            ]);
            assert!(
                e039_diagnostics(&result).is_empty(),
                "valid or deferred scope must remain accepted for {sql}: {:?}",
                result.diagnostics
            );
        }
    }

    #[test]
    fn known_warehouse_pseudo_column_names_remain_conservative() {
        for name in [
            "rowid",
            "ROWID",
            "_metadata",
            "_PARTITIONTIME",
            "_partitiondate",
            "METADATA$FILENAME",
            "metadata$file_row_number",
        ] {
            assert!(is_warehouse_pseudo_column(name), "{name}");
        }
        assert!(!is_warehouse_pseudo_column("ordinary_column"));
    }

    #[test]
    fn known_missing_check_requires_exact_single_model_binding() {
        let mut qualified_physical =
            make_model("physical_reader", "SELECT missing FROM raw.upstream");
        qualified_physical.config.depends_on = vec!["upstream".to_string()];

        for consumer in [
            qualified_physical,
            make_model(
                "mixed_reader",
                "SELECT missing FROM upstream JOIN raw.extra ON upstream.id = raw.extra.id",
            ),
            make_model("source_reader", "SELECT missing FROM raw.orders"),
        ] {
            let result = compile_typechecks(vec![
                make_model("upstream", "SELECT id FROM raw.orders"),
                consumer,
            ]);
            assert!(
                e039_diagnostics(&result).is_empty(),
                "non-exact or mixed binding must stay conservative: {:?}",
                result.diagnostics
            );
        }
    }

    #[test]
    fn known_missing_check_requires_complete_plain_upstream_projection() {
        for upstream_sql in [
            "SELECT * FROM raw.orders",
            "SELECT (order_id), amount FROM raw.orders",
        ] {
            let result = compile_typechecks(vec![
                make_model("upstream", upstream_sql),
                make_model("consumer", "SELECT missing FROM upstream"),
            ]);
            assert!(
                e039_diagnostics(&result).is_empty(),
                "incomplete upstream must suppress absence for {upstream_sql}: {:?}",
                result.diagnostics
            );
        }

        assert!(!has_provably_fixed_output_names(
            "SELECT id FROM raw.left UNION BY NAME SELECT id FROM raw.right"
        ));
    }

    #[test]
    fn known_missing_check_skips_output_expanding_upstream_functions() {
        let result = compile_typechecks(vec![
            make_model("upstream", "SELECT unnest(array(1, 2)) AS x"),
            make_model("consumer", "SELECT a FROM upstream"),
        ]);
        assert!(
            e039_diagnostics(&result).is_empty(),
            "an aliased function can expand to different physical output names: {:?}",
            result.diagnostics
        );
    }

    #[test]
    fn parser_062_star_modifier_compatibility_gate_is_explicit() {
        let dialect = rocky_sql::dialect::DatabricksDialect;
        for sql in [
            "SELECT * EXCLUDE (secret) FROM raw.orders",
            "SELECT * REPLACE (1 AS amount) FROM raw.orders",
            "SELECT * RENAME (amount AS order_amount) FROM raw.orders",
        ] {
            assert!(
                Parser::parse_sql(&dialect, sql).is_err(),
                "sqlparser 0.62 baseline unexpectedly accepts {sql}; add it to the incomplete-upstream controls before upgrading"
            );
        }
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
        let scope = TypeScope::with_target(duckdb());
        let expr = parse_expr("CAST(x AS BIGINT)");
        assert_eq!(infer_expr_type(&expr, &scope).0, RockyType::Int64);
    }

    /// #2333: a cast to a name whose width differs between warehouses takes
    /// the width of the model's warehouse, and is `Unknown` when that is not
    /// known. The input `x` is a known `BIGINT NOT NULL`.
    #[test]
    fn a_cast_target_is_typed_for_the_models_warehouse() {
        use OperandDialect as D;
        let f32 = RockyType::Float32;
        let f64 = RockyType::Float64;
        let i32 = RockyType::Int32;
        let i64 = RockyType::Int64;
        let n38 = RockyType::Decimal {
            precision: 38,
            scale: 0,
        };
        let ts = RockyType::Timestamp;
        let unknown = RockyType::Unknown;
        // (target, [DuckDB, Snowflake, Databricks, BigQuery, Trino, SQL Server,
        //  PostgreSQL, Redshift], no known warehouse)
        let rows: Vec<(&str, [&RockyType; 8], &RockyType)> = vec![
            (
                "FLOAT",
                [&f32, &f64, &f32, &unknown, &unknown, &f64, &f64, &f64],
                &unknown,
            ),
            (
                "REAL",
                [&f32, &f64, &f32, &unknown, &f32, &f32, &f32, &f32],
                &unknown,
            ),
            (
                "FLOAT(24)",
                [
                    &unknown, &unknown, &unknown, &unknown, &unknown, &f32, &f32, &unknown,
                ],
                &unknown,
            ),
            (
                "FLOAT(53)",
                [
                    &unknown, &unknown, &unknown, &unknown, &unknown, &f64, &f64, &unknown,
                ],
                &unknown,
            ),
            (
                "INT",
                [&i32, &n38, &i32, &i64, &i32, &i32, &i32, &i32],
                &unknown,
            ),
            (
                "INTEGER",
                [&i32, &n38, &i32, &i64, &i32, &i32, &i32, &i32],
                &unknown,
            ),
            (
                "SMALLINT",
                [&i32, &n38, &i32, &i64, &i32, &i32, &i32, &i32],
                &unknown,
            ),
            (
                "TINYINT",
                [&i32, &n38, &i32, &i64, &i32, &i32, &unknown, &unknown],
                &unknown,
            ),
            (
                "BIGINT",
                [&i64, &n38, &i64, &i64, &i64, &i64, &i64, &i64],
                &unknown,
            ),
            (
                "TIMESTAMP",
                [&ts, &unknown, &ts, &ts, &ts, &unknown, &ts, &ts],
                &unknown,
            ),
            // Names that mean the same everywhere keep their type with no
            // known warehouse.
            ("DOUBLE", [&f64; 8], &f64),
            ("DATE", [&RockyType::Date; 8], &RockyType::Date),
        ];
        let dialects = [
            D::DuckDb,
            D::Snowflake,
            D::Databricks,
            D::BigQuery,
            D::Trino,
            D::SqlServer,
            D::Postgres,
            D::Redshift,
        ];
        let typed = |target: OperandTarget, cast: &str| {
            let mut scope = TypeScope::with_target(target);
            scope
                .columns
                .insert(CiKey::owned("x".to_string()), (RockyType::Int64, false));
            infer_expr_type(&parse_expr(&format!("CAST(x AS {cast})")), &scope).0
        };
        for (cast, per_dialect, none) in &rows {
            for (dialect, expected) in dialects.iter().zip(per_dialect) {
                assert_eq!(
                    &typed(Some(*dialect).into(), cast),
                    *expected,
                    "CAST(x AS {cast}) on {}",
                    dialect.name()
                );
            }
            assert_eq!(
                &typed(OperandTarget::Unconfigured, cast),
                *none,
                "CAST(x AS {cast}) with no target"
            );
            // A warehouse Rocky has no width table for leaves it unknown too.
            let unruled = OperandTarget::Targets {
                dialects: vec![D::DuckDb],
                unruled: vec!["clickhouse".to_string()],
            };
            assert_eq!(&typed(unruled, cast), *none, "CAST(x AS {cast}) + unruled");
        }
        // A model on two warehouses has a type only where they agree.
        let both = |a, b| OperandTarget::Targets {
            dialects: vec![a, b],
            unruled: Vec::new(),
        };
        assert_eq!(typed(both(D::DuckDb, D::Databricks), "INT"), i32);
        assert_eq!(typed(both(D::DuckDb, D::Snowflake), "INT"), unknown);
        assert_eq!(typed(both(D::Postgres, D::Snowflake), "FLOAT"), f64);
    }

    /// #2333: each model is typed for its own warehouse; a model with no
    /// entry takes the default.
    #[test]
    fn each_model_casts_for_its_own_warehouse() {
        let sources = HashMap::from([(
            "t".to_string(),
            source_schema(&[("x", RockyType::Int64, false)]),
        )]);
        let sql = "SELECT CAST(x AS INT) AS i FROM t";
        let project =
            Project::from_models(vec![make_model("on_duck", sql), make_model("on_sf", sql)])
                .unwrap();
        let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();
        let mut targets = TargetDialects::uniform(duckdb());
        targets.set("on_sf", Some(OperandDialect::Snowflake).into());
        let result =
            typecheck_project_for_targets(&graph, &sources, &project.models, None, &targets);
        assert_eq!(
            result.typed_models["on_duck"][0].data_type,
            RockyType::Int32
        );
        assert_eq!(
            result.typed_models["on_sf"][0].data_type,
            RockyType::Decimal {
                precision: 38,
                scale: 0
            }
        );
    }

    /// #2333: on Snowflake `CAST(int_col AS BIGINT)` is `NUMBER(38,0)`. The
    /// integer fits, so the cast cannot fail and a NOT NULL input stays NOT
    /// NULL; a narrower DECIMAL can overflow.
    #[test]
    fn an_integer_cast_to_a_wide_decimal_keeps_the_inputs_nullability() {
        let mut scope = TypeScope::with_target(Some(OperandDialect::Snowflake).into());
        for (name, ty) in [("i32", RockyType::Int32), ("i64", RockyType::Int64)] {
            scope
                .columns
                .insert(CiKey::owned(name.to_string()), (ty, false));
        }
        let n38 = RockyType::Decimal {
            precision: 38,
            scale: 0,
        };
        for (expr, ty, nullable) in [
            ("CAST(i64 AS BIGINT)", &n38, false),
            ("CAST(i32 AS INT)", &n38, false),
            (
                "CAST(i32 AS DECIMAL(10,0))",
                &RockyType::Decimal {
                    precision: 10,
                    scale: 0,
                },
                false,
            ),
            (
                "CAST(i32 AS DECIMAL(9,0))",
                &RockyType::Decimal {
                    precision: 9,
                    scale: 0,
                },
                true,
            ),
            (
                "CAST(i64 AS DECIMAL(18,0))",
                &RockyType::Decimal {
                    precision: 18,
                    scale: 0,
                },
                true,
            ),
        ] {
            assert_eq!(
                infer_expr_type(&parse_expr(expr), &scope),
                (ty.clone(), nullable),
                "{expr}"
            );
        }
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
    fn test_infer_expr_in_list_nullability() {
        let mut scope = TypeScope::new();
        scope
            .columns
            .insert(CiKey::owned("a".to_string()), (RockyType::Int64, false));
        scope
            .columns
            .insert(CiKey::owned("n".to_string()), (RockyType::Int64, true));

        for sql in ["a IN (1, 2)", "a NOT IN (1, 2)"] {
            let (ty, nullable) = infer_expr_type(&parse_expr(sql), &scope);
            assert_eq!(ty, RockyType::Boolean, "{sql}");
            assert!(
                !nullable,
                "{sql}: non-null operand and items is non-nullable"
            );
        }
        for sql in [
            "a NOT IN (1, NULL)",
            "a IN (1, NULL)",
            "n IN (1, 2)",
            "a NOT IN (1, n)",
        ] {
            let (ty, nullable) = infer_expr_type(&parse_expr(sql), &scope);
            assert_eq!(ty, RockyType::Boolean, "{sql}");
            assert!(nullable, "{sql}: a NULL operand or item makes IN nullable");
        }
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
    /// when they want to exercise E024).
    fn make_time_interval_model(
        name: &str,
        time_column: &str,
        granularity: TimeGrain,
        first_partition: Option<&str>,
    ) -> Model {
        Model {
            drop_existing_kind: None,
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
            file_path: format!("models/{name}.sql").into(),
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
        // The WHERE filters `ts`, not the emitted `order_date`, so nothing
        // proves the key non-NULL.
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT CAST(ts AS DATE) AS order_date FROM upstream \
                     WHERE ts >= @start_date AND ts < @end_date"
            .to_string();
        let cols = vec![typed_col("order_date", RockyType::Date, true)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E022"));
    }

    /// A nullable upstream column is no refusal when the model's own WHERE
    /// compares it: the comparison is never TRUE on NULL, so no emitted row
    /// has a NULL key. The standard `time_column >= @start_date` shape, on a
    /// column typed from a seed (every seed column reads as nullable).
    #[test]
    fn test_e022_skipped_when_where_compares_the_time_column() {
        let model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        let cols = vec![typed_col("order_date", RockyType::Date, true)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(
            !diags.iter().any(|d| &*d.code == "E022"),
            "unexpected E022: {diags:?}"
        );

        // Qualified, aliased to its own name, the comparison on the right.
        let mut qualified = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        qualified.sql = "SELECT f.order_date AS order_date, f.category FROM fct AS f \
                         WHERE @start_date <= f.order_date AND f.order_date < @end_date"
            .to_string();
        let diags = check_time_interval_strategy(&qualified, &cols);
        assert!(
            !diags.iter().any(|d| &*d.code == "E022"),
            "unexpected E022: {diags:?}"
        );
    }

    /// The null-rejection must be on the emitted column and certain: a
    /// comparison under OR, on another table's same-named column, or in a
    /// UNION keeps E022.
    #[test]
    fn test_e022_kept_when_the_where_does_not_reject_null_keys() {
        let cols = vec![typed_col("order_date", RockyType::Date, true)];
        for sql in [
            // OR: a row with a NULL key passes through the other branch.
            "SELECT order_date FROM upstream WHERE (order_date >= @start_date OR flag) \
             AND ts < @end_date AND ts >= @start_date",
            // The filter reads `o.order_date`; the output carries `p.order_date`.
            "SELECT p.order_date FROM o JOIN p ON o.id = p.id \
             WHERE o.order_date >= @start_date AND o.order_date < @end_date",
            // A set operation: the other branch is not filtered on the key.
            "SELECT order_date FROM a WHERE order_date >= @start_date AND order_date < @end_date \
             UNION ALL SELECT order_date FROM b WHERE ts >= @start_date AND ts < @end_date",
            // ROLLUP adds a grand-total row with a NULL key after the WHERE.
            "SELECT order_date, SUM(x) AS s FROM t \
             WHERE order_date >= @start_date AND order_date < @end_date GROUP BY ROLLUP(order_date)",
            "SELECT order_date, SUM(x) AS s FROM t \
             WHERE order_date >= @start_date AND order_date < @end_date \
             GROUP BY GROUPING SETS ((order_date), ())",
            // The comparison is on another column, not the key.
            "SELECT order_date FROM t WHERE ts >= @start_date AND ts < @end_date AND amount > 0",
            // The comparison is on an expression over the key.
            "SELECT order_date FROM t WHERE COALESCE(order_date, ts) >= @start_date \
             AND ts < @end_date",
            // A computed projection named like the key.
            "SELECT CAST(order_date AS DATE) AS order_date FROM t \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // A wildcard projection.
            "SELECT * FROM t WHERE order_date >= @start_date AND order_date < @end_date",
        ] {
            let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
            model.sql = sql.to_string();
            let diags = check_time_interval_strategy(&model, &cols);
            assert!(
                diags.iter().any(|d| &*d.code == "E022"),
                "expected E022 for {sql}: {diags:?}"
            );
        }
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
    fn test_e024_only_start_date() {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE order_date >= @start_date".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        let e024: Vec<_> = diags.iter().filter(|d| &*d.code == "E024").collect();
        assert_eq!(e024.len(), 1, "expected one E024, got: {diags:?}");
        assert!(e024[0].is_error());
    }

    #[test]
    fn test_e024_only_end_date() {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE order_date < @end_date".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        assert!(diags.iter().any(|d| &*d.code == "E024" && d.is_error()));
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
        // `@start_date_extra` should NOT match `@start_date`.
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = "SELECT order_date FROM upstream WHERE x = @start_date_extra".into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        let diags = check_time_interval_strategy(&model, &cols);
        // Should fire E024 (neither placeholder present, since @start_date_extra
        // doesn't count).
        assert!(diags.iter().any(|d| &*d.code == "E024"));
    }

    /// E024 verdict for one SQL body: `true` when E024 fires as an error.
    fn e024_fires(sql: &str) -> bool {
        let mut model = make_time_interval_model("m", "order_date", TimeGrain::Day, None);
        model.sql = sql.into();
        let cols = vec![typed_col("order_date", RockyType::Date, false)];
        check_time_interval_strategy(&model, &cols)
            .iter()
            .any(|d| &*d.code == "E024" && d.is_error())
    }

    /// #2233: placeholders that bound nothing must not satisfy E024. Each of
    /// these bodies copies every source row on every partition run.
    #[test]
    fn test_e024_placeholders_that_bound_nothing() {
        for sql in [
            // The issue's repro: only in a block comment.
            "SELECT order_date FROM upstream /* @start_date @end_date */",
            // Only in a line comment.
            "SELECT order_date FROM upstream -- @start_date @end_date\n",
            // Inside a longer string literal in the WHERE.
            "SELECT order_date FROM upstream WHERE note <> 'x @start_date @end_date'",
            // Only in the SELECT list.
            "SELECT order_date, @start_date AS s, @end_date AS e FROM upstream",
            // A LEFT JOIN ON keeps every left row whatever the ON says.
            "SELECT u.order_date FROM upstream u LEFT JOIN cal c \
             ON u.order_date >= @start_date AND u.order_date < @end_date",
            // One bound in a filter, the other only in a comment.
            "SELECT order_date FROM upstream WHERE order_date >= @start_date /* @end_date */",
        ] {
            assert!(e024_fires(sql), "E024 must fire for: {sql}");
        }
    }

    /// The filter positions that do bound the rows keep passing E024.
    #[test]
    fn test_e024_placeholders_in_row_filters_pass() {
        for sql in [
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // The quoted form, which the runtime also substitutes.
            "SELECT order_date FROM upstream \
             WHERE order_date >= '@start_date' AND order_date < '@end_date'",
            // In a CTE.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM w",
            // In a derived table.
            "SELECT * FROM (SELECT order_date FROM upstream \
             WHERE order_date BETWEEN @start_date AND @end_date) AS x",
            // In an inner JOIN ON.
            "SELECT u.order_date FROM upstream u JOIN cal c \
             ON u.order_date >= @start_date AND u.order_date < @end_date",
            // In HAVING.
            "SELECT order_date FROM upstream GROUP BY order_date \
             HAVING order_date >= @start_date AND order_date < @end_date",
            // A comment beside a real filter is harmless.
            "SELECT order_date FROM upstream /* @start_date */ \
             WHERE order_date >= @start_date AND order_date < @end_date",
        ] {
            assert!(!e024_fires(sql), "E024 must not fire for: {sql}");
        }
    }

    /// SQL that does not parse cannot be checked, so E024 fails closed.
    #[test]
    fn test_e024_unparseable_sql_fails_closed() {
        assert!(e024_fires(
            "SELECT order_date FROM upstream WHERE order_date >= @start_date \
             AND order_date < @end_date AND ((("
        ));
    }

    /// A filter must reach every row the model emits, not just some (#2233
    /// follow-up). Each of these bodies holds a correct filter somewhere,
    /// yet copies unbounded rows on every partition run.
    #[test]
    fn test_e024_filter_that_misses_some_rows() {
        for sql in [
            // One UNION ALL branch is unfiltered.
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date \
             UNION ALL SELECT order_date FROM other",
            // Same with the unfiltered branch first, and plain UNION.
            "SELECT order_date FROM other UNION SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // Each branch bounds one end only.
            "SELECT order_date FROM upstream WHERE order_date >= @start_date \
             UNION ALL SELECT order_date FROM upstream WHERE order_date < @end_date",
            // EXCEPT keeps the left rows; a filter on the right bounds nothing.
            "SELECT order_date FROM upstream EXCEPT SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // The only filter sits in a CTE that is never read.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM upstream",
            // The filtered CTE is read only by another unused CTE.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date), \
             v AS (SELECT * FROM w) SELECT order_date FROM upstream",
            // The only filter is in a scalar subquery in the SELECT list.
            "SELECT order_date, (SELECT COUNT(*) FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) AS n FROM other",
            // A scalar subquery with no FROM on the outer query.
            "SELECT (SELECT MAX(order_date) FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) AS order_date",
            // A bounded CTE on the dropped side of a LEFT JOIN.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT o.order_date FROM other o LEFT JOIN w ON o.order_date = w.order_date",
            // A FULL OUTER JOIN keeps the unbounded side's rows.
            "SELECT o.order_date FROM other o FULL OUTER JOIN (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) w \
             ON o.order_date = w.order_date",
            // NOT IN a bounded set keeps rows outside the window.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM other WHERE order_date NOT IN (SELECT order_date FROM w)",
            // A bare date spine: its end is inclusive in most warehouses, so
            // it emits a row in the next partition.
            "SELECT d AS order_date FROM GENERATE_SERIES(@start_date, @end_date, INTERVAL 1 DAY) AS s(d)",
            // A shadowing inner CTE hides the bounded outer one.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT * FROM (WITH w AS (SELECT order_date FROM other) SELECT order_date FROM w) x",
            // VALUES rows are not filtered by the window.
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date \
             UNION ALL VALUES (DATE '2020-01-01')",
        ] {
            assert!(e024_fires(sql), "E024 must fire for: {sql}");
        }
    }

    /// #2233 follow-up (P2): a placeholder must sit in a top-level `AND`
    /// conjunct that bounds a column, in the filter's own scope. Each of these
    /// mentions both placeholders in a filter, yet copies unbounded rows.
    #[test]
    fn test_e024_placeholder_that_does_not_bound_the_filter() {
        for sql in [
            // An OR branch lets every row through.
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date OR 1 = 1",
            "SELECT order_date FROM upstream \
             WHERE (order_date >= @start_date OR 1 = 1) AND order_date < @end_date",
            // A null test on the placeholder short-circuits the bound.
            "SELECT order_date FROM upstream \
             WHERE @start_date IS NULL OR (order_date >= @start_date AND order_date < @end_date)",
            // NOT inverts the window: every row outside it.
            "SELECT order_date FROM upstream \
             WHERE NOT (order_date >= @start_date AND order_date < @end_date)",
            // A bounded set the outer rows are NOT IN.
            "SELECT order_date FROM upstream WHERE order_date NOT IN \
             (SELECT d FROM cal WHERE d >= @start_date AND d < @end_date)",
            // EXISTS over a bounded subquery restricts nothing per row.
            "SELECT order_date FROM upstream WHERE EXISTS \
             (SELECT 1 FROM cal WHERE d >= @start_date AND d < @end_date)",
            // A scalar subquery compared to a column: the placeholders bound
            // the subquery's rows, not this filter's.
            "SELECT order_date FROM upstream WHERE order_date >= \
             (SELECT MIN(d) FROM cal WHERE d >= @start_date AND d < @end_date)",
            // Bounds facing the wrong way.
            "SELECT order_date FROM upstream \
             WHERE order_date <= @start_date AND order_date > @end_date",
            "SELECT order_date FROM upstream \
             WHERE @start_date >= order_date AND @end_date < order_date",
            // NOT BETWEEN keeps the rows outside the window.
            "SELECT order_date FROM upstream \
             WHERE order_date NOT BETWEEN @start_date AND @end_date",
            // One OR branch bounds only one end.
            "SELECT order_date FROM upstream \
             WHERE (order_date >= @start_date AND order_date < @end_date) \
             OR ship_date >= @start_date",
            // IN on a constant, not a column, restricts no row.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM other WHERE 1 IN (SELECT 1 FROM w)",
            // A comparison between the placeholders bounds no column.
            "SELECT order_date FROM upstream WHERE @start_date < @end_date",
            // A CASE that only sometimes uses the window.
            "SELECT order_date FROM upstream WHERE order_date >= \
             CASE WHEN flag THEN @start_date ELSE DATE '1900-01-01' END \
             AND order_date < @end_date",
        ] {
            assert!(e024_fires(sql), "E024 must fire for: {sql}");
        }
    }

    /// The forms a real window filter takes keep passing.
    #[test]
    fn test_e024_column_bounds_in_common_forms_pass() {
        for sql in [
            // Placeholder on the left.
            "SELECT order_date FROM upstream \
             WHERE @start_date <= order_date AND @end_date > order_date",
            // Parenthesised conjuncts and extra unrelated conjuncts.
            "SELECT order_date FROM upstream \
             WHERE (order_date >= @start_date) AND (status = 'ok' OR status = 'late') \
             AND (order_date < @end_date)",
            // A cast or function on either side.
            "SELECT order_date FROM upstream \
             WHERE CAST(ts AS DATE) >= CAST(@start_date AS DATE) AND DATE(ts) < DATE(@end_date)",
            "SELECT order_date FROM upstream \
             WHERE DATE_TRUNC('day', ts) >= @start_date AND ts < TIMESTAMP '@end_date'",
            // A lookback shift by a constant.
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date - INTERVAL 1 DAY AND order_date < @end_date",
            // Every OR branch bounds both ends.
            "SELECT order_date FROM upstream \
             WHERE (order_date >= @start_date AND order_date < @end_date) \
             OR (ship_date >= @start_date AND ship_date < @end_date)",
            // A qualified column.
            "SELECT u.order_date FROM upstream u \
             WHERE u.order_date >= @start_date AND u.order_date < @end_date",
            // An EXISTS beside a real window filter is harmless.
            "SELECT order_date FROM upstream WHERE EXISTS (SELECT 1 FROM cal) \
             AND order_date >= @start_date AND order_date < @end_date",
        ] {
            assert!(!e024_fires(sql), "E024 must not fire for: {sql}");
        }
    }

    /// Shapes where the filter does reach every emitted row keep passing.
    #[test]
    fn test_e024_filter_reaching_every_row_passes() {
        for sql in [
            // Every UNION ALL branch is filtered.
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date \
             UNION ALL SELECT order_date FROM other \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // A filter on top of the union bounds both branches.
            "SELECT order_date FROM (SELECT order_date FROM upstream \
             UNION ALL SELECT order_date FROM other) u \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // INTERSECT needs one filtered side.
            "SELECT order_date FROM upstream INTERSECT SELECT order_date FROM other \
             WHERE order_date >= @start_date AND order_date < @end_date",
            // A filtered CTE read through a chain of CTEs.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date), \
             v AS (SELECT order_date FROM w) SELECT order_date FROM v",
            // CTE names match case-insensitively.
            "WITH W AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM w",
            // A bounded CTE inner-joined to a dimension.
            "WITH w AS (SELECT order_date, k FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT w.order_date FROM w JOIN dim d ON w.k = d.k",
            // A bounded CTE on the kept side of a LEFT JOIN.
            "WITH w AS (SELECT order_date, k FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT w.order_date FROM w LEFT JOIN dim d ON w.k = d.k",
            // A bounded CTE on the kept side of a RIGHT JOIN.
            "WITH w AS (SELECT order_date, k FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT w.order_date FROM dim d RIGHT JOIN w ON w.k = d.k",
            // A semi-join through IN on a bounded CTE.
            "WITH w AS (SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date) \
             SELECT order_date FROM other WHERE order_date IN (SELECT order_date FROM w)",
            // A date spine filtered by a WHERE.
            "SELECT d AS order_date FROM GENERATE_SERIES(@start_date, @end_date, INTERVAL 1 DAY) AS s(d) \
             WHERE d >= @start_date AND d < @end_date",
            // A SELECT-list scalar subquery beside a real filter is harmless.
            "SELECT order_date, (SELECT 1) AS one FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date",
        ] {
            assert!(!e024_fires(sql), "E024 must not fire for: {sql}");
        }
    }

    /// Only a single query statement is checked; anything else fails closed.
    #[test]
    fn test_e024_non_query_statement_fails_closed() {
        assert!(e024_fires(
            "SELECT order_date FROM upstream \
             WHERE order_date >= @start_date AND order_date < @end_date; \
             SELECT order_date FROM other"
        ));
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
            declared_in_sidecar: true,
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

    /// Compile `models` with `sources` and return the typed columns and the
    /// diagnostics.
    fn compile_typed(
        models: &[(&str, &str)],
        sources: HashMap<String, Vec<TypedColumn>>,
    ) -> crate::compile::CompileResult {
        compile_typed_on(models, sources, OperandTarget::Unconfigured)
    }

    /// [`compile_typed`] for models that run on `target`.
    fn compile_typed_on(
        models: &[(&str, &str)],
        sources: HashMap<String, Vec<TypedColumn>>,
        target: OperandTarget,
    ) -> crate::compile::CompileResult {
        let models: Vec<Model> = models.iter().map(|(n, s)| make_model(n, s)).collect();
        let config = crate::compile::CompilerConfig {
            source_schemas: sources,
            target_dialects: TargetDialects::uniform(target),
            ..Default::default()
        };
        crate::compile::compile_preloaded_models(models, &config).expect("compile")
    }

    fn column<'a>(
        result: &'a crate::compile::CompileResult,
        model: &str,
        name: &str,
    ) -> &'a TypedColumn {
        result.type_check.typed_models[model]
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("{model}.{name} missing"))
    }

    fn product_sources() -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([(
            "raw.products".to_string(),
            source_schema(&[
                ("product_id", RockyType::Int64, false),
                ("name", RockyType::String, true),
                ("qty", RockyType::Int32, true),
                (
                    "price",
                    RockyType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    true,
                ),
            ]),
        )])
    }

    #[test]
    fn case_over_text_literals_is_text() {
        let result = compile_typed(
            &[(
                "stg_products",
                "SELECT product_id, name, price, \
                 CASE WHEN price < 20 THEN 'budget' WHEN price < 60 THEN 'standard' \
                 ELSE 'premium' END AS price_band FROM raw.products",
            )],
            product_sources(),
        );
        let band = column(&result, "stg_products", "price_band");
        assert_eq!(band.data_type, RockyType::String);
        assert!(!band.nullable, "every branch is a non-null literal");
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "I002"),
            "{:?}",
            result.diagnostics
        );
    }

    #[test]
    fn case_and_coalesce_type_only_when_branches_agree() {
        let result = compile_typed(
            &[(
                "m",
                "SELECT product_id, \
                 CASE WHEN qty > 1 THEN 'x' END AS no_else, \
                 COALESCE(product_id, 0) AS id_or_zero, \
                 COALESCE(name, 'unknown') AS name_or_default, \
                 COALESCE(qty, 0) AS qty_or_zero, \
                 COALESCE(price, 0) AS price_or_zero, \
                 COALESCE(product_id, 1.5) AS id_or_fraction, \
                 CASE WHEN qty > 1 THEN 1 ELSE 2 END AS literals_only, \
                 CASE WHEN qty > 1 THEN qty ELSE product_id END AS widened, \
                 CASE WHEN qty > 1 THEN name ELSE NULL END AS name_or_null \
                 FROM raw.products",
            )],
            product_sources(),
        );
        let ty = |name: &str| column(&result, "m", name).data_type.clone();
        let no_else = column(&result, "m", "no_else");
        assert_eq!(no_else.data_type, RockyType::String);
        assert!(no_else.nullable, "no ELSE yields NULL");
        assert_eq!(ty("id_or_zero"), RockyType::Int64);
        assert!(!column(&result, "m", "id_or_zero").nullable);
        assert_eq!(ty("name_or_default"), RockyType::String);
        assert_eq!(ty("name_or_null"), RockyType::String);
        // DuckDB types `COALESCE(INTEGER, 0)` INTEGER and
        // `COALESCE(DECIMAL(10,2), 0)` DECIMAL(10,2); Rocky would widen
        // both, so it claims no type.
        assert_eq!(ty("qty_or_zero"), RockyType::Unknown);
        assert_eq!(ty("price_or_zero"), RockyType::Unknown);
        // `1.5` is not an integer literal; numeric-literal-only and widening
        // branches have no exact type.
        assert_eq!(ty("id_or_fraction"), RockyType::Unknown);
        assert_eq!(ty("literals_only"), RockyType::Unknown);
        assert_eq!(ty("widened"), RockyType::Unknown);
    }

    #[test]
    fn a_cast_takes_its_target_with_or_without_a_known_input() {
        let sql = "SELECT o.id, \
                   CAST(o.qty * o.price AS DECIMAL(12, 2)) AS amount, \
                   CAST(o.id AS BIGINT) AS id_big, \
                   CAST(o.id AS DECIMAL) AS bare_decimal \
                   FROM raw.orders AS o";
        let known = HashMap::from([(
            "raw.orders".to_string(),
            source_schema(&[
                ("id", RockyType::Int32, false),
                ("qty", RockyType::Int32, false),
                (
                    "price",
                    RockyType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    false,
                ),
            ]),
        )]);
        let result = compile_typed_on(&[("m", sql)], known.clone(), duckdb());
        assert_eq!(
            column(&result, "m", "amount").data_type,
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        assert_eq!(column(&result, "m", "id_big").data_type, RockyType::Int64);
        // A bare DECIMAL names no precision: still Unknown.
        assert_eq!(
            column(&result, "m", "bare_decimal").data_type,
            RockyType::Unknown
        );
        // With no known warehouse, BIGINT has no single width (#2333): a
        // known input does not change that.
        let result = compile_typed(&[("m", sql)], known);
        assert_eq!(column(&result, "m", "id_big").data_type, RockyType::Unknown);
        assert_eq!(
            column(&result, "m", "amount").data_type,
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );

        // No schema: the target is still the type, whatever the input is. The
        // nullable bit stays true, because an unknown input proves nothing.
        let result = compile_typed(&[("m", sql)], HashMap::new());
        assert_eq!(
            column(&result, "m", "amount").data_type,
            RockyType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        // BIGINT is Decimal(38,0) on Snowflake: no target without an input.
        assert_eq!(column(&result, "m", "id_big").data_type, RockyType::Unknown);
        assert_eq!(
            column(&result, "m", "bare_decimal").data_type,
            RockyType::Unknown
        );
        assert!(column(&result, "m", "amount").nullable);
    }

    /// Why `E022` is withheld for a cast over an unknown input: the cast gives
    /// the column its type, but the nullable bit is only a guess about an
    /// input Rocky knows nothing of. Refusing the model on that guess is the
    /// false positive the old "leave it Unknown" rule existed to avoid.
    fn time_interval_compile(
        sql: &str,
        sources: HashMap<String, Vec<TypedColumn>>,
    ) -> crate::compile::CompileResult {
        let mut model = make_model("m", sql);
        model.config.strategy = StrategyConfig::TimeInterval {
            time_column: "order_date".to_string(),
            granularity: TimeGrain::Day,
            lookback: 0,
            batch_size: NonZeroU32::new(1).unwrap(),
            first_partition: None,
        };
        let config = crate::compile::CompilerConfig {
            source_schemas: sources,
            ..Default::default()
        };
        crate::compile::compile_preloaded_models(vec![model], &config).expect("compile")
    }

    fn has_code(result: &crate::compile::CompileResult, code: &str) -> bool {
        result.diagnostics.iter().any(|d| &*d.code == code)
    }

    #[test]
    fn a_typed_cast_over_an_unknown_input_does_not_raise_e022() {
        let sql = "SELECT CAST(order_date AS DATE) AS order_date FROM raw.orders \
                   WHERE order_date >= @start_date AND order_date < @end_date";
        let result = time_interval_compile(sql, HashMap::new());
        let col = column(&result, "m", "order_date");
        assert_eq!(col.data_type, RockyType::Date);
        assert!(col.nullable);
        assert!(!has_code(&result, "E022"), "{:?}", result.diagnostics);
    }

    #[test]
    fn a_cast_of_a_literal_time_column_does_not_raise_e022() {
        // The cast has no source column, so lineage has no edge to prove its
        // nullable bit (a cast over an unknown input is nullable by default).
        let sql = "SELECT CAST('2024-01-01' AS DATE) AS order_date FROM raw.orders \
                   WHERE order_date >= @start_date AND order_date < @end_date";
        let result = time_interval_compile(sql, HashMap::new());
        assert_eq!(
            column(&result, "m", "order_date").data_type,
            RockyType::Date
        );
        assert!(!has_code(&result, "E022"), "{:?}", result.diagnostics);
    }

    /// The trace reaches an upstream project model's column that has no edge
    /// of its own, so the nullable bit is a guess in `up` as well.
    #[test]
    fn a_time_column_cast_over_an_edgeless_upstream_column_does_not_raise_e022() {
        let up = make_model(
            "up",
            "SELECT CAST('2024-01-01' AS DATE) AS d FROM raw.orders",
        );
        let mut down = make_model(
            "down",
            "SELECT CAST(d AS DATE) AS d FROM up WHERE d >= @start_date AND d < @end_date",
        );
        down.config.strategy = StrategyConfig::TimeInterval {
            time_column: "d".to_string(),
            granularity: TimeGrain::Day,
            lookback: 0,
            batch_size: NonZeroU32::new(1).unwrap(),
            first_partition: None,
        };
        let config = crate::compile::CompilerConfig::default();
        let result =
            crate::compile::compile_preloaded_models(vec![up, down], &config).expect("compile");
        assert_eq!(column(&result, "down", "d").data_type, RockyType::Date);
        assert!(!has_code(&result, "E022"), "{:?}", result.diagnostics);
    }

    #[test]
    fn a_nullable_known_input_still_raises_e022() {
        let sql = "SELECT CAST(order_date AS DATE) AS order_date FROM raw.orders \
                   WHERE order_date >= @start_date AND order_date < @end_date";
        let sources = HashMap::from([(
            "raw.orders".to_string(),
            source_schema(&[("order_date", RockyType::Date, true)]),
        )]);
        let result = time_interval_compile(sql, sources);
        assert!(has_code(&result, "E022"), "{:?}", result.diagnostics);
    }

    #[test]
    fn date_arithmetic_is_unknown() {
        let sources = HashMap::from([(
            "raw.orders".to_string(),
            source_schema(&[
                ("first_order", RockyType::Date, false),
                ("last_order", RockyType::Date, false),
                ("n", RockyType::Int64, false),
            ]),
        )]);
        let columns = infer_select_types(
            "SELECT last_order - first_order AS span, last_order - 5 AS earlier, \
             first_order + n AS later, missing + 1 AS unknown_plus, n + 1 AS next_n \
             FROM raw.orders",
            &sources,
            "m",
        )
        .unwrap();
        let ty = |name: &str| {
            columns
                .iter()
                .find(|c| c.name == name)
                .map(|c| c.data_type.clone())
                .unwrap()
        };
        for name in ["span", "earlier", "later", "unknown_plus"] {
            assert_eq!(ty(name), RockyType::Unknown, "{name}");
        }
        assert_eq!(ty("next_n"), RockyType::Int64);
    }

    #[test]
    fn a_qualified_read_of_an_upstream_target_types_from_that_model() {
        let sources = HashMap::from([(
            "raw.orders".to_string(),
            source_schema(&[
                ("customer_id", RockyType::Int64, false),
                ("amount", RockyType::Float64, true),
            ]),
        )]);
        let ltv = make_model(
            "customer_ltv",
            "SELECT customer_id, SUM(amount) AS lifetime_value, COUNT(*) AS order_count \
             FROM raw.orders GROUP BY customer_id",
        );
        // Ephemeral, reads its upstream by the physical name it writes, and
        // declares the dependency.
        let mut active = make_model(
            "int_active",
            "SELECT customer_id, lifetime_value, order_count \
             FROM warehouse.silver.customer_ltv WHERE order_count > 0",
        );
        active.config.depends_on = vec!["customer_ltv".to_string()];
        active.config.strategy = StrategyConfig::Ephemeral;
        let top = make_model(
            "top",
            "WITH ranked AS (SELECT customer_id, lifetime_value AS ltv, order_count \
             FROM int_active) SELECT customer_id, ltv, order_count FROM ranked",
        );
        // Same read, but no dependency on the model: the name may be any
        // physical table, so nothing binds.
        let stray = make_model(
            "stray",
            "SELECT customer_id FROM warehouse.silver.customer_ltv",
        );
        let models = vec![ltv, active, top, stray];
        let config = crate::compile::CompilerConfig {
            source_schemas: sources,
            ..Default::default()
        };
        let result = crate::compile::compile_preloaded_models(models, &config).expect("compile");
        for model in ["int_active", "top"] {
            let ty = |name: &str| column(&result, model, name).data_type.clone();
            assert_eq!(ty("customer_id"), RockyType::Int64, "{model}");
            assert_eq!(ty("order_count"), RockyType::Int64, "{model}");
        }
        assert_eq!(column(&result, "top", "ltv").data_type, RockyType::Float64);
        assert_eq!(
            column(&result, "stray", "customer_id").data_type,
            RockyType::Unknown
        );
    }

    /// An upstream whose output names are fixed by aliases over expressions.
    const ORDER_LINES: &str = "SELECT o.order_id, o.customer_id, o.status, o.order_date, \
                               CAST(o.quantity * o.price AS DECIMAL(12, 2)) AS amount \
                               FROM raw.orders AS o";

    #[test]
    fn known_missing_upstream_column_is_refused_in_every_clause() {
        for sql in [
            "SELECT order_id FROM order_lines WHERE stats = 'completed'",
            "SELECT l.order_id FROM order_lines AS l WHERE l.stats = 'completed'",
            "SELECT a.order_id FROM order_lines AS a JOIN order_lines AS b ON a.order_id = b.ordr_id",
            "SELECT customer_id, COUNT(*) AS n FROM order_lines GROUP BY customer_id, stats",
            "SELECT customer_id FROM order_lines GROUP BY customer_id HAVING MAX(amout) > 0",
            "SELECT CASE WHEN stats = 'x' THEN 1 END AS flag FROM order_lines",
            "SELECT UPPER(stats) AS s FROM order_lines",
            "SELECT order_id FROM order_lines WHERE stats IN ('a', 'b')",
            "SELECT order_id FROM order_lines WHERE order_id IN (SELECT order_id FROM order_lines WHERE stats = 'x')",
            "WITH c AS (SELECT order_id, stats FROM order_lines) SELECT order_id FROM c",
        ] {
            let mut consumer = make_model("consumer", sql);
            consumer.config.depends_on = vec!["order_lines".to_string()];
            let result = compile_typechecks(vec![make_model("order_lines", ORDER_LINES), consumer]);
            let found = e039_diagnostics(&result);
            assert_eq!(found.len(), 1, "`{sql}`: {:?}", result.diagnostics);
            assert!(found[0].message.contains("'order_lines'"), "{:?}", found[0]);
            assert!(found[0].is_error());
        }
    }

    #[test]
    fn known_missing_column_of_a_case_aliased_upstream_is_refused() {
        // The upstream projects an aliased CASE (what a `.rocky` derive
        // lowers to) and drops `category`; the reader still reads it.
        let result = compile_typechecks(vec![
            make_model(
                "products",
                "SELECT product_id, name, price, \
                 CASE WHEN price < 20 THEN 'budget' ELSE 'premium' END AS price_band \
                 FROM raw.products",
            ),
            make_model("order_lines", ORDER_LINES),
            make_model(
                "lines",
                "SELECT o.order_id, p.category FROM order_lines AS o \
                 JOIN products AS p ON o.order_id = p.product_id",
            ),
        ]);
        let found = e039_diagnostics(&result);
        assert_eq!(found.len(), 1, "{:?}", result.diagnostics);
        assert!(found[0].message.contains("'category'"));
        assert!(found[0].message.contains("'products'"));
    }

    #[test]
    fn known_missing_upstream_controls_stay_clean() {
        for sql in [
            // Every column exists.
            "SELECT order_id, status, amount FROM order_lines WHERE status = 'completed' \
             AND order_id >= '1'",
            // A SELECT alias reused later in the projection (DuckDB lateral alias).
            "SELECT customer_id, MIN(order_date) AS first_order, MAX(order_date) AS last_order, \
             last_order - first_order AS span_days FROM order_lines GROUP BY customer_id",
            // ORDER BY an output alias.
            "SELECT order_id AS id FROM order_lines ORDER BY id",
            // A correlated subquery reading the outer relation.
            "SELECT c.customer_id, (SELECT COUNT(*) FROM order_lines AS so \
             WHERE so.customer_id = c.customer_id) AS n FROM raw.customers AS c",
            // A join with a relation Rocky cannot enumerate: unqualified names
            // may come from it.
            "SELECT tier FROM order_lines JOIN raw.customers USING (customer_id)",
            // A CTE that shadows the model's name.
            "WITH order_lines AS (SELECT 1 AS stats) SELECT stats FROM order_lines",
            // A derived table with its own columns.
            "SELECT d.stats FROM (SELECT status AS stats FROM order_lines) AS d",
            // A lambda parameter is not a column.
            "SELECT list_transform([1], x -> x + 1) AS l FROM order_lines",
            // A qualified read of the target is not checked (`rocky run
            // --defer` keeps such a read local).
            "SELECT order_id FROM warehouse.silver.order_lines WHERE stats = 'x'",
            "SELECT order_id FROM silver.order_lines WHERE stats = 'x'",
        ] {
            let mut consumer = make_model("consumer", sql);
            consumer.config.depends_on = vec!["order_lines".to_string()];
            let result = compile_typechecks(vec![make_model("order_lines", ORDER_LINES), consumer]);
            assert!(
                e039_diagnostics(&result).is_empty(),
                "`{sql}`: {:?}",
                result.diagnostics
            );
        }
    }

    #[test]
    fn a_bare_read_binds_only_to_the_model_that_writes_that_table() {
        // `order_lines` writes `silver.order_lines_v2`: a bare `order_lines`
        // reaches some other table, so nothing is provable about it.
        let mut renamed = make_model("order_lines", ORDER_LINES);
        renamed.config.target.table = "order_lines_v2".to_string();
        let mut reader = make_model("consumer", "SELECT stats FROM order_lines");
        reader.config.depends_on = vec!["order_lines".to_string()];
        let result = compile_typechecks(vec![renamed, reader]);
        assert!(
            e039_diagnostics(&result).is_empty(),
            "{:?}",
            result.diagnostics
        );
        // The same reader with the default target is refused.
        let mut reader = make_model("consumer", "SELECT stats FROM order_lines");
        reader.config.depends_on = vec!["order_lines".to_string()];
        let result = compile_typechecks(vec![make_model("order_lines", ORDER_LINES), reader]);
        assert_eq!(
            e039_diagnostics(&result).len(),
            1,
            "{:?}",
            result.diagnostics
        );
    }

    #[test]
    fn an_upstream_with_an_expanding_or_unaliased_expression_proves_nothing() {
        for upstream in [
            "SELECT order_id, unnest(items) AS item FROM raw.orders",
            "SELECT order_id, amount * 2 FROM raw.orders",
            "SELECT order_id FROM raw.a UNION ALL SELECT order_id FROM raw.b",
            "SELECT order_id, 'x' AS 'quoted' FROM raw.orders",
        ] {
            let result = compile_typechecks(vec![
                make_model("up", upstream),
                make_model("consumer", "SELECT order_id FROM up WHERE missing = 1"),
            ]);
            assert!(
                e039_diagnostics(&result).is_empty(),
                "`{upstream}`: {:?}",
                result.diagnostics
            );
        }
    }
}
