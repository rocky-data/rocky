//! `rocky seed` — load CSV seed files into the warehouse.
//!
//! Discovers `.csv` files in a `seeds/` directory, infers column types,
//! generates CREATE TABLE + INSERT INTO VALUES SQL, and executes against
//! the configured warehouse adapter.

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::seeds::{
    ColumnDef, SeedFile, discover_seeds, generate_create_table_sql, generate_insert_sql,
    infer_schema_from_csv_with_overrides,
};

use crate::output::{SeedOutput, SeedTableOutput, print_json};
use crate::registry::{AdapterRegistry, resolve_pipeline};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky seed`: discover, infer, create, and load seed tables.
pub async fn run_seed(
    config_path: &Path,
    seeds_dir: &Path,
    pipeline: Option<&str>,
    filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Load config and build adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

    // Resolve pipeline to get the target adapter.
    let (_pipeline_name, pipeline_cfg) =
        resolve_pipeline(&rocky_cfg, pipeline).context("failed to resolve pipeline for seed")?;
    let adapter_name = pipeline_cfg.target_adapter();
    let adapter = registry.warehouse_adapter(adapter_name)?;
    let dialect = adapter.dialect();

    // Discover seeds.
    let seeds = discover_seeds(seeds_dir).context(format!(
        "failed to discover seeds in {}",
        seeds_dir.display()
    ))?;

    if seeds.is_empty() {
        if json {
            let output = SeedOutput {
                version: VERSION.into(),
                command: "seed".into(),
                seeds_dir: seeds_dir.display().to_string(),
                tables_loaded: 0,
                tables_failed: 0,
                tables: vec![],
                duration_ms: start.elapsed().as_millis() as u64,
            };
            print_json(&output)?;
        } else {
            println!("No seed files found in {}", seeds_dir.display());
        }
        return Ok(());
    }

    // Apply optional name filter.
    let seeds: Vec<SeedFile> = match filter {
        Some(name) => seeds.into_iter().filter(|s| s.name == name).collect(),
        None => seeds,
    };

    // Default target coordinates: use the pipeline's replication target catalog
    // template as the catalog (stripped of placeholders), and "seeds" as the
    // schema. For non-replication pipelines or templates with placeholders,
    // fall back to "main".
    let default_catalog = pipeline_cfg
        .as_replication()
        .map(|r| &r.target.catalog_template)
        .filter(|t| !t.contains('{'))
        .cloned()
        .unwrap_or_else(|| "main".to_string());
    let default_schema = "seeds".to_string();

    let mut table_results: Vec<SeedTableOutput> = Vec::new();
    let mut tables_loaded = 0usize;
    let mut tables_failed = 0usize;

    for seed in &seeds {
        let seed_start = Instant::now();
        let catalog = seed
            .config
            .target
            .as_ref()
            .and_then(|t| t.catalog.as_deref())
            .unwrap_or(&default_catalog)
            .to_string();
        let target_schema = seed
            .config
            .target
            .as_ref()
            .map(|t| t.schema.as_str())
            .unwrap_or(&default_schema)
            .to_string();
        let table_name = seed
            .config
            .target
            .as_ref()
            .and_then(|t| t.table.as_deref())
            .unwrap_or(&seed.name)
            .to_string();

        info!(
            seed = %seed.name,
            file = %seed.file_path.display(),
            target = format!("{catalog}.{target_schema}.{table_name}"),
            "loading seed"
        );

        match load_single_seed(seed, dialect, adapter.as_ref(), &catalog, &target_schema).await {
            Ok((row_count, schema)) => {
                let duration = seed_start.elapsed().as_millis() as u64;
                info!(
                    seed = %seed.name,
                    rows = row_count,
                    duration_ms = duration,
                    "seed loaded"
                );
                table_results.push(SeedTableOutput {
                    name: seed.name.clone(),
                    target: format!("{catalog}.{target_schema}.{table_name}"),
                    rows: row_count,
                    columns: schema.len(),
                    duration_ms: duration,
                    error: None,
                });
                tables_loaded += 1;
            }
            Err(e) => {
                let duration = seed_start.elapsed().as_millis() as u64;
                let err_msg = format!("{e:#}");
                tracing::error!(seed = %seed.name, error = %err_msg, "seed failed");
                table_results.push(SeedTableOutput {
                    name: seed.name.clone(),
                    target: format!("{catalog}.{target_schema}.{table_name}"),
                    rows: 0,
                    columns: 0,
                    duration_ms: duration,
                    error: Some(err_msg),
                });
                tables_failed += 1;
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    if json {
        let output = SeedOutput {
            version: VERSION.into(),
            command: "seed".into(),
            seeds_dir: seeds_dir.display().to_string(),
            tables_loaded,
            tables_failed,
            tables: table_results,
            duration_ms,
        };
        print_json(&output)?;
    } else {
        println!(
            "\nSeed complete: {} loaded, {} failed ({} ms)",
            tables_loaded, tables_failed, duration_ms
        );
        for t in &table_results {
            let status = if t.error.is_some() { "FAIL" } else { "OK" };
            println!(
                "  [{status}] {} -> {} ({} rows, {} cols, {} ms)",
                t.name, t.target, t.rows, t.columns, t.duration_ms
            );
            if let Some(ref err) = t.error {
                println!("       {err}");
            }
        }
    }

    if tables_failed > 0 {
        anyhow::bail!("{tables_failed} seed(s) failed");
    }

    Ok(())
}

/// Load a single seed file: infer schema, create table, insert rows.
///
/// Returns (row_count, schema) on success.
async fn load_single_seed(
    seed: &SeedFile,
    dialect: &dyn rocky_core::traits::SqlDialect,
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
    catalog: &str,
    target_schema: &str,
) -> Result<(usize, Vec<ColumnDef>)> {
    // 1. Infer schema (with sidecar overrides).
    let schema = infer_schema_from_csv_with_overrides(&seed.file_path, &seed.config.column_types)
        .context(format!("schema inference failed for {}", seed.name))?;

    // 2. Ensure schema exists.
    if let Some(Ok(create_schema_sql)) = dialect.create_schema_sql(catalog, target_schema) {
        adapter
            .execute_statement(&create_schema_sql)
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to create schema {catalog}.{target_schema}: {e}")
            })?;
    }

    // 3. Generate and execute CREATE TABLE (with DROP).
    let ddl_stmts = generate_create_table_sql(seed, &schema, dialect, catalog, target_schema)
        .context(format!("DDL generation failed for {}", seed.name))?;
    for stmt in &ddl_stmts {
        adapter
            .execute_statement(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("DDL execution failed for {}: {e}", seed.name))?;
    }

    // 4. Generate and execute INSERT statements.
    let insert_stmts = generate_insert_sql(seed, dialect, catalog, target_schema, &schema, None)
        .context(format!("INSERT generation failed for {}", seed.name))?;

    let mut total_rows = 0usize;
    for stmt in &insert_stmts {
        // Count rows by counting value tuples (lines starting with '(').
        let batch_rows =
            stmt.matches("\n(").count() + if stmt.contains("VALUES\n(") { 1 } else { 0 };
        adapter
            .execute_statement(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("INSERT failed for {}: {e}", seed.name))?;
        total_rows += batch_rows;
    }

    Ok((total_rows, schema))
}
