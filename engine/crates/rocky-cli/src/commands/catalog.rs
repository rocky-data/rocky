//! `rocky catalog` — emit a project-wide column-level lineage snapshot.
//!
//! Walks the SemanticGraph, builds a [`CatalogOutput`], and writes it
//! to `./.rocky/catalog/` by default. Stdout is a one-screen summary;
//! the structured payload is the file(s).
//!
//! Two artefact families:
//!
//! - `catalog.json` — single-file snapshot with assets + edges nested
//!   together; the "front door" for shell scripts and PR bots.
//! - `edges.parquet` + `assets.parquet` — flat analytics-friendly
//!   files, one row per column-lineage edge / one row per asset
//!   column. Designed to JOIN against warehouse queries.
//!
//! Format selection lives behind `--format json|parquet|both`
//! (default `both`).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::semantic::{LineageEdge, SemanticGraph};
use rocky_core::state::{RunStatus, StateStore};

use crate::output::{
    AssetKind, CatalogAsset, CatalogColumn, CatalogEdge, CatalogOutput, CatalogStats,
    EdgeConfidence, config_fingerprint,
};
use crate::registry::resolve_pipeline;
use crate::scope::resolve_managed_tables_in_catalog;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Which on-disk artefact families to emit.
///
/// `Both` is the default: write `catalog.json` plus `edges.parquet` +
/// `assets.parquet`. The variants are intentionally orthogonal to the
/// CLI's `--output` meta-format, which only controls what stdout
/// summarises (table vs JSON mirror of the persisted payload).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogFormat {
    Json,
    Parquet,
    Both,
}

impl CatalogFormat {
    fn writes_json(self) -> bool {
        matches!(self, Self::Json | Self::Both)
    }

    fn writes_parquet(self) -> bool {
        matches!(self, Self::Parquet | Self::Both)
    }
}

/// Execute `rocky catalog`.
///
/// Async because `--catalog <name>` resolves managed tables via
/// [`resolve_managed_tables_in_catalog`], which is itself async (it
/// shares the call shape with `rocky compact --catalog` and
/// `rocky archive --catalog`).
#[allow(clippy::too_many_arguments)]
pub async fn run_catalog(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    out_dir: &Path,
    format: CatalogFormat,
    catalog_filter: Option<&str>,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let started = Instant::now();
    let mut output =
        compute_catalog_output(config_path, state_path, models_dir, cache_ttl_override)?;

    // `--catalog <name>` filters the catalog snapshot down to assets
    // whose target FQN sits in the named warehouse catalog. Reuses the
    // same scope helper compact + archive use, so the resolution logic
    // (managed-table set, catalog-name normalisation, "no matches"
    // diagnostic) is identical across the three commands.
    if let Some(name) = catalog_filter {
        let scope = resolve_managed_tables_in_catalog(config_path, name).await?;
        filter_to_catalog_scope(&mut output, &scope.tables);
    }

    std::fs::create_dir_all(out_dir).with_context(|| format!("creating {}", out_dir.display()))?;

    // Track every file we write so the human summary can list them.
    let mut written: Vec<PathBuf> = Vec::new();

    if format.writes_json() {
        let json_path = out_dir.join("catalog.json");
        let pretty = serde_json::to_string_pretty(&output).context("serializing catalog")?;
        std::fs::write(&json_path, pretty + "\n")
            .with_context(|| format!("writing {}", json_path.display()))?;
        written.push(json_path);
    }

    if format.writes_parquet() {
        let edges_path = out_dir.join("edges.parquet");
        write_edges_parquet(&edges_path, &output)
            .with_context(|| format!("writing {}", edges_path.display()))?;
        written.push(edges_path);

        let assets_path = out_dir.join("assets.parquet");
        write_assets_parquet(&assets_path, &output)
            .with_context(|| format!("writing {}", assets_path.display()))?;
        written.push(assets_path);
    }

    if output_json {
        // Mirror the persisted artifact on stdout so machine consumers
        // can pipe `rocky catalog --output json` without re-reading the
        // file. Note the stdout JSON is independent of `--format`; it
        // is always the same `CatalogOutput` shape.
        crate::output::print_json(&output)?;
    } else {
        let duration_ms = started.elapsed().as_millis();
        println!("rocky catalog");
        println!("  project:          {}", output.project_name);
        println!("  assets:           {}", output.stats.asset_count);
        println!("  columns:          {}", output.stats.column_count);
        println!("  edges:            {}", output.stats.edge_count);
        if output.stats.assets_with_star > 0 {
            println!(
                "  partial lineage:  {} asset(s) (SELECT *)",
                output.stats.assets_with_star
            );
        }
        if output.stats.orphan_columns > 0 {
            println!("  orphan columns:   {}", output.stats.orphan_columns);
        }
        for path in &written {
            println!("  wrote:            {}", path.display());
        }
        println!("  duration:         {duration_ms}ms");
    }

    Ok(())
}

/// Build a [`CatalogOutput`] without persisting or rendering it.
///
/// Extracted from [`run_catalog`] so unit tests can assert on the
/// structured value directly. Compiles the project, walks the
/// SemanticGraph for assets + edges, then enriches with state-store
/// run history (`last_run_id` / `last_materialized_at`) when the
/// state store is available.
pub(crate) fn compute_catalog_output(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    cache_ttl_override: Option<u64>,
) -> Result<CatalogOutput> {
    let started = Instant::now();

    // 1. Load cached source schemas (mirrors lineage.rs's bootstrap so
    //    leaf models inherit real types when the cache is warm).
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).ok();
    let source_schemas = match &rocky_cfg {
        Some(cfg) => {
            let schema_cfg = cfg
                .cache
                .schemas
                .clone()
                .with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        None => HashMap::new(),
    };

    // 2. Compile the project.
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: HashMap::new(),
        ..Default::default()
    };
    let result = compile::compile(&config).context("compile failed")?;

    // 3. Resolve project name from the config (first pipeline if multi).
    let project_name = match &rocky_cfg {
        Some(cfg) => resolve_pipeline(cfg, None)
            .map(|(name, _)| name.to_string())
            .unwrap_or_else(|_| "unknown".to_string()),
        None => "unknown".to_string(),
    };

    // 4. Index typed columns for type/nullability enrichment.
    let typed_models = &result.type_check.typed_models;

    // 5. Index target FQNs from project models.
    let mut target_fqns: HashMap<String, String> = HashMap::new();
    for m in &result.project.models {
        let t = &m.config.target;
        target_fqns.insert(
            m.config.name.clone(),
            format!("{}.{}.{}", t.catalog, t.schema, t.table),
        );
    }

    // 6. Index intent from project models. ModelSchema also exposes
    //    `intent`, but project models are the canonical source.
    let mut intents: HashMap<String, String> = HashMap::new();
    for m in &result.project.models {
        if let Some(intent) = &m.config.intent {
            intents.insert(m.config.name.clone(), intent.clone());
        }
    }

    // 7. Build assets from the SemanticGraph models (insertion-ordered
    //    via IndexMap). Source-only nodes (referenced but not declared
    //    as a project model) get kind = Source.
    let mut model_names: HashSet<String> = HashSet::new();
    for m in &result.project.models {
        model_names.insert(m.config.name.clone());
    }

    let mut assets: Vec<CatalogAsset> = Vec::new();
    let mut total_columns: usize = 0;
    let mut assets_with_star: usize = 0;
    let mut orphan_columns: usize = 0;

    // Track all (model, column) pairs that have a producing edge for
    // orphan detection.
    let mut produced: HashSet<(String, String)> = HashSet::new();
    for edge in &result.semantic_graph.edges {
        produced.insert((
            edge.target.model.to_string(),
            edge.target.column.to_string(),
        ));
    }

    for (name, schema) in &result.semantic_graph.models {
        let kind = if model_names.contains(name) {
            AssetKind::Model
        } else {
            AssetKind::Source
        };

        let typed = typed_models.get(name);
        let columns: Vec<CatalogColumn> = schema
            .columns
            .iter()
            .map(|col| {
                let typed_col = typed.and_then(|cols| cols.iter().find(|c| c.name == col.name));
                let (data_type, nullable) = match typed_col {
                    Some(tc) => {
                        let dt = match tc.data_type {
                            rocky_compiler::types::RockyType::Unknown => None,
                            ref t => Some(t.to_string()),
                        };
                        (dt, Some(tc.nullable))
                    }
                    None => (None, None),
                };
                CatalogColumn {
                    name: col.name.clone(),
                    data_type,
                    nullable,
                }
            })
            .collect();

        total_columns += columns.len();
        if schema.has_star {
            assets_with_star += 1;
        }
        for col in &schema.columns {
            if !produced.contains(&(name.clone(), col.name.clone()))
                && matches!(kind, AssetKind::Model)
            {
                orphan_columns += 1;
            }
        }

        let fqn = target_fqns
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.clone());
        let intent = intents.get(name).cloned().or_else(|| schema.intent.clone());

        assets.push(CatalogAsset {
            fqn,
            model_name: name.clone(),
            kind,
            columns,
            upstream_models: schema.upstream.clone(),
            downstream_models: schema.downstream.clone(),
            intent,
            // Filled in below by `enrich_with_state_store` once the
            // full asset list is built and we have a single read-only
            // state-store handle.
            last_materialized_at: None,
            last_run_id: None,
        });
    }

    // 8. Build edges. Sort by `(source_model, source_column,
    //    target_model, target_column)` for byte-stable output across
    //    runs — guards against any non-deterministic ordering creeping
    //    in upstream.
    let mut edges: Vec<CatalogEdge> = result
        .semantic_graph
        .edges
        .iter()
        .map(edge_to_catalog_edge)
        .collect();
    edges.sort_by(|a, b| {
        a.source_model
            .cmp(&b.source_model)
            .then_with(|| a.source_column.cmp(&b.source_column))
            .then_with(|| a.target_model.cmp(&b.target_model))
            .then_with(|| a.target_column.cmp(&b.target_column))
    });
    regrade_star_edges(&mut edges, &result.semantic_graph);

    // 9. Sort assets by (fqn, then by name as a tiebreaker for
    //    same-fqn collisions) and sort columns within each asset by
    //    name. Both sorts are stable and keep `assets.parquet` /
    //    `catalog.json` byte-stable across compiles.
    for asset in &mut assets {
        asset.columns.sort_by(|a, b| a.name.cmp(&b.name));
    }
    assets.sort_by(|a, b| {
        a.fqn
            .cmp(&b.fqn)
            .then_with(|| a.model_name.cmp(&b.model_name))
    });

    // 10. Enrich with state-store run history (best effort). Sets
    //     `CatalogOutput.last_run_id` to the most recent successful run
    //     and `CatalogAsset.last_run_id` / `last_materialized_at` to the
    //     most recent run that produced each model. Falls through
    //     silently when the state store is missing or unreadable: the
    //     command must work pre-first-run.
    let last_run_id = enrich_with_state_store(state_path, &mut assets);

    let stats = CatalogStats {
        asset_count: assets.len(),
        column_count: total_columns,
        edge_count: edges.len(),
        assets_with_star,
        orphan_columns,
        duration_ms: started.elapsed().as_millis() as u64,
    };

    let config_hash = config_fingerprint(config_path);

    Ok(CatalogOutput {
        version: VERSION.to_string(),
        command: "catalog".to_string(),
        generated_at: Utc::now(),
        project_name,
        config_hash,
        last_run_id,
        assets,
        edges,
        stats,
    })
}

/// Number of recent runs to consider when populating `last_run_id` /
/// `last_materialized_at`. 50 is enough to cover real workloads
/// (newest-first) while keeping the redb scan cheap.
const STATE_RUN_LIMIT: usize = 50;

/// Best-effort state-store enrichment.
///
/// Opens the state store read-only so the command never blocks an
/// in-flight `rocky run`. Returns the project-level `last_run_id` (the
/// most recent run with `RunStatus::Success`), and mutates each asset
/// in-place to set `last_run_id` + `last_materialized_at` from the
/// newest-per-model successful [`ModelExecution`].
///
/// Silent on failure: a missing or unreadable state store leaves all
/// state-derived fields as `None`. `rocky catalog` is meant to run
/// before the first `rocky run` has ever populated state, and a hard
/// error there would be hostile.
fn enrich_with_state_store(state_path: &Path, assets: &mut [CatalogAsset]) -> Option<String> {
    let store = StateStore::open_read_only(state_path).ok()?;
    let runs = store.list_runs(STATE_RUN_LIMIT).ok()?;

    // Project-level: first successful run, newest-first.
    let project_last_run_id = runs
        .iter()
        .find(|r| r.status == RunStatus::Success)
        .map(|r| r.run_id.clone());

    // Per-model: walk runs newest-first and record the first successful
    // (ModelExecution) hit per model. ModelExecution.status is a
    // `String`; production writers use lowercase "success" — see
    // `RunOutput` materialization handling.
    let mut by_model: HashMap<String, (String, chrono::DateTime<chrono::Utc>)> = HashMap::new();
    for run in &runs {
        for me in &run.models_executed {
            if me.status != "success" {
                continue;
            }
            by_model
                .entry(me.model_name.clone())
                .or_insert_with(|| (run.run_id.clone(), me.finished_at));
        }
    }

    for asset in assets.iter_mut() {
        if let Some((run_id, finished_at)) = by_model.get(&asset.model_name) {
            asset.last_run_id = Some(run_id.clone());
            asset.last_materialized_at = Some(*finished_at);
        }
    }

    project_last_run_id
}

/// Filter a [`CatalogOutput`] in place to only include assets whose
/// FQN appears in `tables` (matched case-insensitively against the
/// scope helper's lowercased entries).
///
/// Edges are dropped when *either* endpoint is removed, so the
/// remaining edge set never points at a missing asset. `stats` is
/// recomputed from the post-filter assets so consumers see counts that
/// match the filtered file. Source-kind assets typically fall out
/// here — `resolve_managed_tables_in_catalog` only returns
/// project-managed targets, which is the right semantics for "this
/// catalog's view of the lineage."
pub(crate) fn filter_to_catalog_scope(output: &mut CatalogOutput, tables: &[String]) {
    let allow: HashSet<String> = tables.iter().map(|s| s.to_lowercase()).collect();

    output
        .assets
        .retain(|a| allow.contains(&a.fqn.to_lowercase()));

    let surviving_models: HashSet<String> =
        output.assets.iter().map(|a| a.model_name.clone()).collect();
    output.edges.retain(|e| {
        surviving_models.contains(&e.source_model) && surviving_models.contains(&e.target_model)
    });

    let total_columns: usize = output.assets.iter().map(|a| a.columns.len()).sum();
    let assets_with_star = output
        .edges
        .iter()
        .filter(|e| matches!(e.confidence, EdgeConfidence::Medium))
        .map(|e| e.target_model.as_str())
        .collect::<HashSet<_>>()
        .len();
    output.stats.asset_count = output.assets.len();
    output.stats.column_count = total_columns;
    output.stats.edge_count = output.edges.len();
    output.stats.assets_with_star = assets_with_star;
    // `orphan_columns` is a structural diagnostic — recomputing it
    // honestly after the filter requires re-walking the edges; the
    // count stays valid (no rows are *added* by filtering, just
    // removed). Leave the pre-filter value to flag global lineage
    // gaps; the JSON consumer can recompute against `assets` if it
    // needs a scope-local count.
}

/// Convert a SemanticGraph [`LineageEdge`] into a [`CatalogEdge`].
///
/// Confidence grading: `Medium` when the producing target model uses
/// `SELECT *` (lineage was inferred via star expansion); `High`
/// otherwise. `Low` is reserved for v2 once the extractor begins
/// surfacing partially-recovered constructs.
fn edge_to_catalog_edge(edge: &LineageEdge) -> CatalogEdge {
    CatalogEdge {
        source_model: edge.source.model.to_string(),
        source_column: edge.source.column.to_string(),
        target_model: edge.target.model.to_string(),
        target_column: edge.target.column.to_string(),
        transform: edge.transform.to_string(),
        // PR-1 keeps confidence coarse: every edge from the extractor
        // is `High` because we cannot distinguish star-derived edges
        // from explicit ones at the edge level alone (the `has_star`
        // flag lives on the target model). Re-grade in the assembler
        // once we cross-reference targets.
        confidence: EdgeConfidence::High,
    }
}

/// Re-grade edges whose target model uses `SELECT *` to `Medium`.
///
/// Kept separate from [`edge_to_catalog_edge`] so the visit happens
/// once, after the asset list is known.
fn regrade_star_edges(edges: &mut [CatalogEdge], graph: &SemanticGraph) {
    for edge in edges.iter_mut() {
        if let Some(target_schema) = graph.models.get(&edge.target_model)
            && target_schema.has_star
        {
            edge.confidence = EdgeConfidence::Medium;
        }
    }
}

/// Default catalog output directory: `./.rocky/catalog/`.
pub fn default_out_dir() -> PathBuf {
    PathBuf::from(".rocky").join("catalog")
}

// ---------------------------------------------------------------------------
// Parquet writers
// ---------------------------------------------------------------------------
//
// Two flat schemas, one row per edge / one row per (asset, column).
// Both files share `generated_at` and `config_hash` so consumers can
// JOIN edges to assets without round-tripping through the JSON. All
// timestamps are encoded as `TIMESTAMP(MICROS, UTC)`; the timezone tag
// is part of the schema so DuckDB / BigQuery / Spark all treat the
// column as zoned rather than naive.

const TIMESTAMP_TZ: &str = "UTC";

/// Schema for `edges.parquet`.
///
/// Columns mirror the design memo §3.2 verbatim:
/// `source_model`, `source_column`, `target_model`, `target_column`,
/// `transform`, `confidence`, `generated_at`, `config_hash`,
/// `last_run_id` (nullable).
fn edges_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source_model", DataType::Utf8, false),
        Field::new("source_column", DataType::Utf8, false),
        Field::new("target_model", DataType::Utf8, false),
        Field::new("target_column", DataType::Utf8, false),
        Field::new("transform", DataType::Utf8, false),
        Field::new("confidence", DataType::Utf8, false),
        Field::new(
            "generated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            false,
        ),
        Field::new("config_hash", DataType::Utf8, false),
        Field::new("last_run_id", DataType::Utf8, true),
    ]))
}

/// Schema for `assets.parquet`.
///
/// Columns mirror the design memo §3.2 verbatim:
/// `asset_fqn`, `model_name`, `asset_kind`, `column_name`,
/// `data_type` (nullable), `is_nullable` (nullable), `has_star`,
/// `intent` (nullable), `last_materialized_at` (nullable),
/// `generated_at`, `config_hash`.
fn assets_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("asset_fqn", DataType::Utf8, false),
        Field::new("model_name", DataType::Utf8, false),
        Field::new("asset_kind", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, true),
        Field::new("is_nullable", DataType::Boolean, true),
        Field::new("has_star", DataType::Boolean, false),
        Field::new("intent", DataType::Utf8, true),
        Field::new(
            "last_materialized_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            true,
        ),
        Field::new(
            "generated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            false,
        ),
        Field::new("config_hash", DataType::Utf8, false),
    ]))
}

/// Write `edges.parquet` (one row per [`CatalogEdge`]).
///
/// The output is sorted by
/// `(source_model, source_column, target_model, target_column)` —
/// preserves the same order the JSON artifact uses so consumers
/// JOINing the two see identical row orderings.
pub fn write_edges_parquet(path: &Path, output: &CatalogOutput) -> Result<()> {
    let schema = edges_arrow_schema();
    let n = output.edges.len();
    let generated_at_micros = output.generated_at.timestamp_micros();

    let mut source_model: Vec<&str> = Vec::with_capacity(n);
    let mut source_column: Vec<&str> = Vec::with_capacity(n);
    let mut target_model: Vec<&str> = Vec::with_capacity(n);
    let mut target_column: Vec<&str> = Vec::with_capacity(n);
    let mut transform: Vec<&str> = Vec::with_capacity(n);
    let mut confidence: Vec<&str> = Vec::with_capacity(n);

    for edge in &output.edges {
        source_model.push(&edge.source_model);
        source_column.push(&edge.source_column);
        target_model.push(&edge.target_model);
        target_column.push(&edge.target_column);
        transform.push(&edge.transform);
        confidence.push(confidence_label(&edge.confidence));
    }

    let last_run_id_arr = StringArray::from(vec![output.last_run_id.as_deref(); n]);
    let generated_at_arr =
        TimestampMicrosecondArray::from(vec![generated_at_micros; n]).with_timezone(TIMESTAMP_TZ);
    let config_hash_arr = StringArray::from(vec![output.config_hash.as_str(); n]);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(source_model)),
        Arc::new(StringArray::from(source_column)),
        Arc::new(StringArray::from(target_model)),
        Arc::new(StringArray::from(target_column)),
        Arc::new(StringArray::from(transform)),
        Arc::new(StringArray::from(confidence)),
        Arc::new(generated_at_arr),
        Arc::new(config_hash_arr),
        Arc::new(last_run_id_arr),
    ];

    let batch =
        RecordBatch::try_new(schema.clone(), columns).context("building edges record batch")?;

    write_record_batch(path, schema, &batch)
}

/// Write `assets.parquet` (one row per asset column).
///
/// Per the design memo, the table is "one row = one column on one
/// asset" — assets without columns produce zero rows. The output is
/// sorted by `(asset_fqn, column_name)` because the upstream
/// `compute_catalog_output` sorts assets by `fqn` and columns by
/// `name`, and we iterate in that order.
pub fn write_assets_parquet(path: &Path, output: &CatalogOutput) -> Result<()> {
    let schema = assets_arrow_schema();
    let generated_at_micros = output.generated_at.timestamp_micros();

    // Pre-size optimistically: most assets have a handful of columns.
    let approx_rows: usize = output.assets.iter().map(|a| a.columns.len()).sum();

    let mut asset_fqn: Vec<&str> = Vec::with_capacity(approx_rows);
    let mut model_name: Vec<&str> = Vec::with_capacity(approx_rows);
    let mut asset_kind: Vec<&str> = Vec::with_capacity(approx_rows);
    let mut column_name: Vec<&str> = Vec::with_capacity(approx_rows);
    let mut data_type: Vec<Option<&str>> = Vec::with_capacity(approx_rows);
    let mut is_nullable: Vec<Option<bool>> = Vec::with_capacity(approx_rows);
    let mut has_star: Vec<bool> = Vec::with_capacity(approx_rows);
    let mut intent: Vec<Option<&str>> = Vec::with_capacity(approx_rows);
    let mut last_materialized_micros: Vec<Option<i64>> = Vec::with_capacity(approx_rows);

    // Track `has_star` per asset by membership in `upstream_models`
    // emitted by the regrade pass — but the catalog asset doesn't
    // carry the bit directly. Recover it from the assets that own
    // medium-confidence outbound edges. Cheaper: we treat any asset
    // whose `model_name` matches a Medium target as star-derived.
    let star_targets: std::collections::HashSet<&str> = output
        .edges
        .iter()
        .filter(|e| matches!(e.confidence, EdgeConfidence::Medium))
        .map(|e| e.target_model.as_str())
        .collect();

    for asset in &output.assets {
        let kind_label = asset_kind_label(&asset.kind);
        let fqn = asset.fqn.as_str();
        let m_name = asset.model_name.as_str();
        let intent_str = asset.intent.as_deref();
        let asset_has_star = star_targets.contains(m_name);
        let last_mat = asset.last_materialized_at.map(|t| t.timestamp_micros());

        for col in &asset.columns {
            asset_fqn.push(fqn);
            model_name.push(m_name);
            asset_kind.push(kind_label);
            column_name.push(col.name.as_str());
            data_type.push(col.data_type.as_deref());
            is_nullable.push(col.nullable);
            has_star.push(asset_has_star);
            intent.push(intent_str);
            last_materialized_micros.push(last_mat);
        }
    }

    let n = asset_fqn.len();

    let last_materialized_arr =
        TimestampMicrosecondArray::from(last_materialized_micros).with_timezone(TIMESTAMP_TZ);
    let generated_at_arr =
        TimestampMicrosecondArray::from(vec![generated_at_micros; n]).with_timezone(TIMESTAMP_TZ);
    let config_hash_arr = StringArray::from(vec![output.config_hash.as_str(); n]);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(asset_fqn)),
        Arc::new(StringArray::from(model_name)),
        Arc::new(StringArray::from(asset_kind)),
        Arc::new(StringArray::from(column_name)),
        Arc::new(StringArray::from(data_type)),
        Arc::new(BooleanArray::from(is_nullable)),
        Arc::new(BooleanArray::from(has_star)),
        Arc::new(StringArray::from(intent)),
        Arc::new(last_materialized_arr),
        Arc::new(generated_at_arr),
        Arc::new(config_hash_arr),
    ];

    let batch =
        RecordBatch::try_new(schema.clone(), columns).context("building assets record batch")?;

    write_record_batch(path, schema, &batch)
}

fn write_record_batch(path: &Path, schema: Arc<Schema>, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(props)).context("opening parquet writer")?;
    writer.write(batch).context("writing record batch")?;
    writer.close().context("closing parquet writer")?;
    Ok(())
}

fn confidence_label(c: &EdgeConfidence) -> &'static str {
    match c {
        EdgeConfidence::High => "high",
        EdgeConfidence::Medium => "medium",
        EdgeConfidence::Low => "low",
    }
}

fn asset_kind_label(k: &AssetKind) -> &'static str {
    match k {
        AssetKind::Source => "source",
        AssetKind::Model => "model",
        AssetKind::View => "view",
        AssetKind::MaterializedView => "materialized_view",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke-test the catalog assembler against the playground POC,
    /// which ships in the repo and needs no warehouse credentials.
    /// Mirrors the pattern in `compact.rs`'s e2e test.
    #[cfg(feature = "duckdb")]
    #[test]
    fn compute_catalog_output_against_playground_pocs() -> Result<()> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        // crates/rocky-cli → engine → workspace root → examples/...
        let poc_dir = std::path::Path::new(manifest_dir)
            .join("..")
            .join("..")
            .join("..")
            .join("examples")
            .join("playground")
            .join("pocs")
            .join("00-foundations")
            .join("00-playground-default");

        if !poc_dir.is_dir() {
            eprintln!(
                "skipping compute_catalog_output_against_playground_pocs: \
                 {} not found (running outside the monorepo?)",
                poc_dir.display()
            );
            return Ok(());
        }

        let config_path = poc_dir.join("rocky.toml");
        let models_dir = poc_dir.join("models");
        let state_path = poc_dir.join(".rocky-state.redb");

        let output = compute_catalog_output(&config_path, &state_path, &models_dir, None)?;

        assert_eq!(output.command, "catalog");
        assert_eq!(output.project_name, "playground");

        // Every shipped model should surface as an asset.
        let names: Vec<&str> = output
            .assets
            .iter()
            .map(|a| a.model_name.as_str())
            .collect();
        assert!(
            names.contains(&"raw_orders"),
            "missing raw_orders in {names:?}"
        );
        assert!(
            names.contains(&"customer_orders"),
            "missing customer_orders in {names:?}"
        );
        assert!(
            names.contains(&"revenue_summary"),
            "missing revenue_summary in {names:?}"
        );

        // Stats consistency.
        assert_eq!(output.stats.asset_count, output.assets.len());
        assert_eq!(output.stats.edge_count, output.edges.len());
        let column_total: usize = output.assets.iter().map(|a| a.columns.len()).sum();
        assert_eq!(output.stats.column_count, column_total);

        // Edges should be deterministically sorted by
        // (source_model, source_column, target_model, target_column).
        let keys: Vec<_> = output
            .edges
            .iter()
            .map(|e| {
                (
                    e.source_model.as_str(),
                    e.source_column.as_str(),
                    e.target_model.as_str(),
                    e.target_column.as_str(),
                )
            })
            .collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys, "edges must be sorted before emit");

        // Assets should be deterministically sorted by (fqn, model_name)
        // and each asset's columns sorted by name. The artefact is meant
        // to be diffable in git — non-deterministic ordering would break
        // that contract for both catalog.json and assets.parquet.
        let asset_keys: Vec<_> = output.assets.iter().map(|a| a.fqn.as_str()).collect();
        let mut sorted_asset_keys = asset_keys.clone();
        sorted_asset_keys.sort();
        assert_eq!(
            asset_keys, sorted_asset_keys,
            "assets must be sorted by fqn"
        );
        for asset in &output.assets {
            let col_names: Vec<&str> = asset.columns.iter().map(|c| c.name.as_str()).collect();
            let mut sorted_col_names = col_names.clone();
            sorted_col_names.sort();
            assert_eq!(
                col_names, sorted_col_names,
                "columns must be sorted by name on asset {}",
                asset.fqn
            );
        }

        // Two compiles must produce identical structural output (assets,
        // edges, counts) — the catalog is meant to be diffable in git.
        let again = compute_catalog_output(&config_path, &state_path, &models_dir, None)?;
        assert_eq!(output.assets.len(), again.assets.len());
        assert_eq!(output.edges.len(), again.edges.len());
        assert_eq!(output.stats.column_count, again.stats.column_count);

        Ok(())
    }

    /// Schema-conformance test for the two Parquet artefacts.
    ///
    /// Runs the catalog assembler against the playground POC, writes
    /// both files, then re-opens them with the Arrow Parquet reader
    /// and asserts (a) the column names match the design memo §3.2
    /// schema verbatim, (b) the timestamp columns carry the `UTC`
    /// timezone tag, and (c) the row counts line up with the in-memory
    /// catalog (one row per edge / one row per asset column).
    #[cfg(feature = "duckdb")]
    #[test]
    fn parquet_files_match_design_memo_schema() -> Result<()> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let poc_dir = std::path::Path::new(manifest_dir)
            .join("..")
            .join("..")
            .join("..")
            .join("examples")
            .join("playground")
            .join("pocs")
            .join("00-foundations")
            .join("00-playground-default");

        if !poc_dir.is_dir() {
            eprintln!(
                "skipping parquet_files_match_design_memo_schema: \
                 {} not found (running outside the monorepo?)",
                poc_dir.display()
            );
            return Ok(());
        }

        let config_path = poc_dir.join("rocky.toml");
        let models_dir = poc_dir.join("models");
        let state_path = poc_dir.join(".rocky-state.redb");

        let output = compute_catalog_output(&config_path, &state_path, &models_dir, None)?;

        let tmp = tempfile::tempdir()?;
        let edges_path = tmp.path().join("edges.parquet");
        let assets_path = tmp.path().join("assets.parquet");
        write_edges_parquet(&edges_path, &output)?;
        write_assets_parquet(&assets_path, &output)?;

        // edges.parquet — schema + row count.
        let file = std::fs::File::open(&edges_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut total_rows = 0usize;
        let mut observed_schema: Option<arrow::datatypes::SchemaRef> = None;
        for batch in reader {
            let batch = batch?;
            total_rows += batch.num_rows();
            observed_schema.get_or_insert_with(|| batch.schema());
        }
        assert_eq!(
            total_rows,
            output.edges.len(),
            "edges.parquet row count diverged from CatalogOutput.edges"
        );
        let schema = observed_schema.expect("edges.parquet had no batches");
        let edge_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            edge_cols,
            vec![
                "source_model",
                "source_column",
                "target_model",
                "target_column",
                "transform",
                "confidence",
                "generated_at",
                "config_hash",
                "last_run_id",
            ],
            "edges.parquet column names diverged from design memo §3.2"
        );
        let generated_at_field = schema.field_with_name("generated_at")?;
        assert_eq!(
            generated_at_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            "edges.parquet generated_at must be TIMESTAMP(MICROS, UTC)"
        );
        assert!(!schema.field_with_name("source_model")?.is_nullable());
        assert!(schema.field_with_name("last_run_id")?.is_nullable());

        // assets.parquet — schema + row count.
        let file = std::fs::File::open(&assets_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut total_rows = 0usize;
        let mut observed_schema: Option<arrow::datatypes::SchemaRef> = None;
        for batch in reader {
            let batch = batch?;
            total_rows += batch.num_rows();
            observed_schema.get_or_insert_with(|| batch.schema());
        }
        let expected_rows: usize = output.assets.iter().map(|a| a.columns.len()).sum();
        assert_eq!(
            total_rows, expected_rows,
            "assets.parquet row count diverged from sum(asset.columns.len())"
        );
        let schema = observed_schema.expect("assets.parquet had no batches");
        let asset_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            asset_cols,
            vec![
                "asset_fqn",
                "model_name",
                "asset_kind",
                "column_name",
                "data_type",
                "is_nullable",
                "has_star",
                "intent",
                "last_materialized_at",
                "generated_at",
                "config_hash",
            ],
            "assets.parquet column names diverged from design memo §3.2"
        );
        let last_mat_field = schema.field_with_name("last_materialized_at")?;
        assert_eq!(
            last_mat_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into())),
            "assets.parquet last_materialized_at must be TIMESTAMP(MICROS, UTC)"
        );
        assert!(last_mat_field.is_nullable());
        assert!(!schema.field_with_name("has_star")?.is_nullable());
        assert!(schema.field_with_name("data_type")?.is_nullable());
        assert!(schema.field_with_name("is_nullable")?.is_nullable());
        assert!(schema.field_with_name("intent")?.is_nullable());

        Ok(())
    }

    /// `default_out_dir` always resolves to a relative path under the
    /// project root so consumers can `git add .rocky/catalog/`.
    #[test]
    fn default_out_dir_is_relative_under_dot_rocky() {
        let dir = default_out_dir();
        assert!(dir.is_relative());
        let s = dir.to_string_lossy();
        assert!(s.contains(".rocky"), "got {s}");
        assert!(s.contains("catalog"), "got {s}");
    }

    /// Edges whose target model has `has_star = true` get downgraded
    /// to `Medium` confidence. Mirrors the partial-lineage signal that
    /// the catalog surfaces via `stats.assets_with_star`.
    #[test]
    fn regrade_star_edges_marks_star_targets_as_medium() {
        use indexmap::IndexMap;
        use rocky_compiler::semantic::ModelSchema;

        let mut models: IndexMap<String, ModelSchema> = IndexMap::new();
        models.insert(
            "downstream_star".to_string(),
            ModelSchema {
                columns: vec![],
                has_star: true,
                upstream: vec!["upstream".into()],
                downstream: vec![],
                intent: None,
            },
        );
        models.insert(
            "downstream_explicit".to_string(),
            ModelSchema {
                columns: vec![],
                has_star: false,
                upstream: vec!["upstream".into()],
                downstream: vec![],
                intent: None,
            },
        );
        let graph = SemanticGraph::new(models, Vec::new());

        let mut edges = vec![
            CatalogEdge {
                source_model: "upstream".into(),
                source_column: "id".into(),
                target_model: "downstream_star".into(),
                target_column: "id".into(),
                transform: "direct".into(),
                confidence: EdgeConfidence::High,
            },
            CatalogEdge {
                source_model: "upstream".into(),
                source_column: "id".into(),
                target_model: "downstream_explicit".into(),
                target_column: "id".into(),
                transform: "direct".into(),
                confidence: EdgeConfidence::High,
            },
        ];

        regrade_star_edges(&mut edges, &graph);

        assert!(matches!(edges[0].confidence, EdgeConfidence::Medium));
        assert!(matches!(edges[1].confidence, EdgeConfidence::High));
    }

    /// State-store enrichment populates `last_run_id` /
    /// `last_materialized_at` from the most recent successful
    /// [`ModelExecution`] per model, and falls through silently when
    /// the state file does not exist.
    #[test]
    fn enrich_with_state_store_populates_last_run_and_falls_back_silently() {
        use chrono::{Duration as ChronoDuration, TimeZone, Utc};
        use rocky_core::state::{ModelExecution, RunStatus, RunTrigger};

        // 1. Empty state path (no file) → all assets stay None and the
        //    project-level run id is None.
        let tmp = tempfile::tempdir().unwrap();
        let missing_state = tmp.path().join("nonexistent.redb");

        let mut empty_assets = vec![sample_asset("raw_orders")];
        let project_run = enrich_with_state_store(&missing_state, &mut empty_assets);
        assert_eq!(project_run, None);
        assert_eq!(empty_assets[0].last_run_id, None);
        assert_eq!(empty_assets[0].last_materialized_at, None);

        // 2. Populated state store: two runs, the older one materialised
        //    `raw_orders`, the newer one materialised `customer_orders`.
        //    Each model's `last_run_id` should point at the run that
        //    actually built it; the project's `last_run_id` is the
        //    newest successful run.
        let state_path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();

            let older_started = Utc.with_ymd_and_hms(2026, 5, 1, 10, 0, 0).unwrap();
            let older_finished = older_started + ChronoDuration::seconds(30);
            let newer_started = Utc.with_ymd_and_hms(2026, 5, 2, 10, 0, 0).unwrap();
            let newer_finished = newer_started + ChronoDuration::seconds(30);

            let older = sample_run_record(
                "run-001",
                older_started,
                older_finished,
                vec![sample_model_execution(
                    "raw_orders",
                    older_started,
                    older_finished,
                    "success",
                )],
            );
            let newer = sample_run_record(
                "run-002",
                newer_started,
                newer_finished,
                vec![sample_model_execution(
                    "customer_orders",
                    newer_started,
                    newer_finished,
                    "success",
                )],
            );
            store.record_run(&older).unwrap();
            store.record_run(&newer).unwrap();
            // Drop the writer before the read-only handle opens.
            drop(store);
        }

        let mut assets = vec![
            sample_asset("raw_orders"),
            sample_asset("customer_orders"),
            // Model with no run history → enrichment leaves it None.
            sample_asset("revenue_summary"),
        ];
        let project_run = enrich_with_state_store(&state_path, &mut assets);

        assert_eq!(project_run.as_deref(), Some("run-002"));
        assert_eq!(assets[0].last_run_id.as_deref(), Some("run-001"));
        assert!(assets[0].last_materialized_at.is_some());
        assert_eq!(assets[1].last_run_id.as_deref(), Some("run-002"));
        assert!(assets[1].last_materialized_at.is_some());
        assert_eq!(assets[2].last_run_id, None);
        assert_eq!(assets[2].last_materialized_at, None);

        // 3. Failed model executions never enrich. Add a third run that
        //    "executed" `revenue_summary` with status "failed"; the
        //    asset should remain None.
        let store = StateStore::open(&state_path).unwrap();
        let failed_started = Utc.with_ymd_and_hms(2026, 5, 3, 10, 0, 0).unwrap();
        let failed_finished = failed_started + ChronoDuration::seconds(5);
        store
            .record_run(&sample_run_record(
                "run-003-failed",
                failed_started,
                failed_finished,
                vec![sample_model_execution(
                    "revenue_summary",
                    failed_started,
                    failed_finished,
                    "failed",
                )],
            ))
            .unwrap();
        drop(store);

        let mut assets = vec![sample_asset("revenue_summary")];
        let _ = enrich_with_state_store(&state_path, &mut assets);
        assert_eq!(assets[0].last_run_id, None);
        assert_eq!(assets[0].last_materialized_at, None);

        fn sample_asset(name: &str) -> CatalogAsset {
            CatalogAsset {
                fqn: format!("warehouse.public.{name}"),
                model_name: name.to_string(),
                kind: AssetKind::Model,
                columns: vec![],
                upstream_models: vec![],
                downstream_models: vec![],
                intent: None,
                last_materialized_at: None,
                last_run_id: None,
            }
        }

        fn sample_run_record(
            id: &str,
            started: chrono::DateTime<chrono::Utc>,
            finished: chrono::DateTime<chrono::Utc>,
            models: Vec<ModelExecution>,
        ) -> rocky_core::state::RunRecord {
            rocky_core::state::RunRecord {
                run_id: id.to_string(),
                started_at: started,
                finished_at: finished,
                status: RunStatus::Success,
                models_executed: models,
                trigger: RunTrigger::Manual,
                config_hash: "test".to_string(),
                triggering_identity: None,
                session_source: Default::default(),
                git_commit: None,
                git_branch: None,
                idempotency_key: None,
                target_catalog: None,
                hostname: "test-host".to_string(),
                rocky_version: "test-version".to_string(),
            }
        }

        fn sample_model_execution(
            name: &str,
            started: chrono::DateTime<chrono::Utc>,
            finished: chrono::DateTime<chrono::Utc>,
            status: &str,
        ) -> ModelExecution {
            ModelExecution {
                model_name: name.to_string(),
                started_at: started,
                finished_at: finished,
                duration_ms: (finished - started).num_milliseconds().max(0) as u64,
                rows_affected: Some(42),
                status: status.to_string(),
                sql_hash: "abc123".to_string(),
                bytes_scanned: None,
                bytes_written: None,
            }
        }
    }

    /// `--catalog foo` filters the catalog snapshot to assets whose
    /// FQN sits in the named warehouse catalog. Edges referencing
    /// dropped assets are pruned so the JSON / Parquet artifacts never
    /// have dangling references.
    #[test]
    fn filter_to_catalog_scope_drops_other_catalogs_and_dangling_edges() {
        let mut output = sample_output_with_two_catalogs();

        // Pre-condition: 3 assets across two catalogs, 2 edges that
        // span catalogs (one stays, one straddles).
        assert_eq!(output.assets.len(), 3);
        assert_eq!(output.edges.len(), 2);

        // Scope down to "warehouse_a" only. The scope helper returns
        // lowercase FQNs, mirroring `resolve_managed_tables_in_catalog`.
        let scope = vec![
            "warehouse_a.public.raw_orders".to_string(),
            "warehouse_a.public.customer_orders".to_string(),
        ];
        filter_to_catalog_scope(&mut output, &scope);

        let asset_names: Vec<&str> = output
            .assets
            .iter()
            .map(|a| a.model_name.as_str())
            .collect();
        // Filter preserves input order (which here is the deterministic
        // `(fqn, model_name)` sort produced upstream); the test fixture
        // puts `raw_orders` before `customer_orders`.
        assert_eq!(asset_names, vec!["raw_orders", "customer_orders"]);

        // Edge touching `revenue_summary` (which lives in warehouse_b)
        // is pruned; the in-catalog edge survives.
        assert_eq!(output.edges.len(), 1);
        assert_eq!(output.edges[0].source_model, "raw_orders");
        assert_eq!(output.edges[0].target_model, "customer_orders");

        // Stats are recomputed from the post-filter set.
        assert_eq!(output.stats.asset_count, 2);
        assert_eq!(output.stats.edge_count, 1);

        fn sample_output_with_two_catalogs() -> CatalogOutput {
            CatalogOutput {
                version: "test".to_string(),
                command: "catalog".to_string(),
                generated_at: Utc::now(),
                project_name: "test".to_string(),
                config_hash: "test".to_string(),
                last_run_id: None,
                assets: vec![
                    CatalogAsset {
                        fqn: "warehouse_a.public.raw_orders".to_string(),
                        model_name: "raw_orders".to_string(),
                        kind: AssetKind::Model,
                        columns: vec![],
                        upstream_models: vec![],
                        downstream_models: vec!["customer_orders".to_string()],
                        intent: None,
                        last_materialized_at: None,
                        last_run_id: None,
                    },
                    CatalogAsset {
                        fqn: "warehouse_a.public.customer_orders".to_string(),
                        model_name: "customer_orders".to_string(),
                        kind: AssetKind::Model,
                        columns: vec![],
                        upstream_models: vec!["raw_orders".to_string()],
                        downstream_models: vec!["revenue_summary".to_string()],
                        intent: None,
                        last_materialized_at: None,
                        last_run_id: None,
                    },
                    CatalogAsset {
                        fqn: "warehouse_b.public.revenue_summary".to_string(),
                        model_name: "revenue_summary".to_string(),
                        kind: AssetKind::Model,
                        columns: vec![],
                        upstream_models: vec!["customer_orders".to_string()],
                        downstream_models: vec![],
                        intent: None,
                        last_materialized_at: None,
                        last_run_id: None,
                    },
                ],
                edges: vec![
                    CatalogEdge {
                        source_model: "raw_orders".to_string(),
                        source_column: "id".to_string(),
                        target_model: "customer_orders".to_string(),
                        target_column: "id".to_string(),
                        transform: "direct".to_string(),
                        confidence: EdgeConfidence::High,
                    },
                    CatalogEdge {
                        source_model: "customer_orders".to_string(),
                        source_column: "id".to_string(),
                        target_model: "revenue_summary".to_string(),
                        target_column: "id".to_string(),
                        transform: "direct".to_string(),
                        confidence: EdgeConfidence::High,
                    },
                ],
                stats: CatalogStats {
                    asset_count: 3,
                    column_count: 0,
                    edge_count: 2,
                    assets_with_star: 0,
                    orphan_columns: 0,
                    duration_ms: 0,
                },
            }
        }
    }
}
