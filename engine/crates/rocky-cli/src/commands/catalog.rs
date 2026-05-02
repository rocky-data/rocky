//! `rocky catalog` — emit a project-wide column-level lineage snapshot.
//!
//! PR-1 of the catalog rollout: walks the SemanticGraph, builds a
//! [`CatalogOutput`], and writes it to `./.rocky/catalog/catalog.json`
//! by default. Stdout is a one-screen summary; the structured payload
//! is the file. State-store and Parquet emit are deferred to
//! follow-up PRs.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::semantic::{LineageEdge, SemanticGraph};

use crate::output::{
    AssetKind, CatalogAsset, CatalogColumn, CatalogEdge, CatalogOutput, CatalogStats,
    EdgeConfidence, config_fingerprint,
};
use crate::registry::resolve_pipeline;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky catalog`.
pub fn run_catalog(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    out_dir: &Path,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let started = Instant::now();
    let output = compute_catalog_output(config_path, state_path, models_dir, cache_ttl_override)?;

    // Persist the JSON artifact regardless of stdout format. The file
    // is the contract; stdout is the human-readable summary.
    let json_path = out_dir.join("catalog.json");
    std::fs::create_dir_all(out_dir).with_context(|| format!("creating {}", out_dir.display()))?;
    let pretty = serde_json::to_string_pretty(&output).context("serializing catalog")?;
    std::fs::write(&json_path, pretty + "\n")
        .with_context(|| format!("writing {}", json_path.display()))?;

    if output_json {
        // Mirror the persisted artifact on stdout so machine consumers
        // can pipe `rocky catalog --output json` without re-reading the file.
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
        println!("  json:             {}", json_path.display());
        println!("  duration:         {duration_ms}ms");
    }

    Ok(())
}

/// Build a [`CatalogOutput`] without persisting or rendering it.
///
/// Extracted from [`run_catalog`] so unit tests can assert on the
/// structured value directly. Pure compute over the SemanticGraph; no
/// state-store touch in PR-1.
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
            // PR-1: state-store enrichment deferred. See the catalog
            // command memo for the PR-3 plan.
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
        last_run_id: None,
        assets,
        edges,
        stats,
    })
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

        // Two compiles must produce identical structural output (assets,
        // edges, counts) — the catalog is meant to be diffable in git.
        let again = compute_catalog_output(&config_path, &state_path, &models_dir, None)?;
        assert_eq!(output.assets.len(), again.assets.len());
        assert_eq!(output.edges.len(), again.edges.len());
        assert_eq!(output.stats.column_count, again.stats.column_count);

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
}
