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
    // 0. Run pre-hooks BEFORE any warehouse write (including the destructive
    //    DROP TABLE). A failing pre-hook aborts the seed with nothing written —
    //    the canonical guard is "abort if the target already holds rows".
    for (i, stmt) in seed.config.pre_hook.iter().enumerate() {
        adapter.execute_statement(stmt).await.map_err(|e| {
            anyhow::anyhow!(
                "pre_hook[{i}] failed for {} (no data written): {e}",
                seed.name
            )
        })?;
    }

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
        // Count the value tuples in this batch. `generate_insert_sql` joins
        // one `(..)` tuple per row with `,\n`, so adjacent tuples are
        // separated by exactly `),\n(`. The row count is (separators + 1) —
        // every non-empty statement holds at least one tuple. (The previous
        // heuristic double-counted the first row via a spurious `VALUES\n(`
        // term, reporting N+1.)
        let batch_rows = stmt.matches("),\n(").count() + 1;
        adapter
            .execute_statement(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("INSERT failed for {}: {e}", seed.name))?;
        total_rows += batch_rows;
    }

    // 5. Run post-hooks after a successful load, in order.
    for (i, stmt) in seed.config.post_hook.iter().enumerate() {
        adapter
            .execute_statement(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("post_hook[{i}] failed for {}: {e}", seed.name))?;
    }

    Ok((total_rows, schema))
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use rocky_core::seeds::{SeedConfig, SeedFormat, SeedTarget};
    use rocky_core::traits::WarehouseAdapter;
    use rocky_duckdb::DuckDbConnector;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    /// Build a `SeedFile` backed by a temp CSV with the given hooks.
    fn seed_with_hooks(
        dir: &Path,
        csv: &str,
        pre_hook: Vec<String>,
        post_hook: Vec<String>,
    ) -> SeedFile {
        let path = dir.join("widgets.csv");
        std::fs::write(&path, csv).unwrap();
        SeedFile {
            name: "widgets".into(),
            file_path: path,
            format: SeedFormat::Csv,
            config: SeedConfig {
                target: Some(SeedTarget {
                    catalog: None,
                    schema: "seeds".into(),
                    table: Some("widgets".into()),
                }),
                pre_hook,
                post_hook,
                ..Default::default()
            },
        }
    }

    /// Coerce a DuckDB scalar cell to i64 — the driver may serialize counts as
    /// either a JSON number or a string.
    fn cell_as_i64(value: &serde_json::Value) -> i64 {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or_else(|| panic!("expected integer cell, got {value:?}"))
    }

    fn row_count(conn: &Arc<Mutex<DuckDbConnector>>, table: &str) -> i64 {
        let guard = conn.lock().unwrap();
        let result = guard
            .execute_sql(&format!("SELECT COUNT(*) FROM {table}"))
            .unwrap();
        cell_as_i64(&result.rows[0][0])
    }

    /// (a) A pre_hook that errors aborts the load before any write — the
    /// pre-existing target row survives because the DROP never fired.
    #[tokio::test]
    async fn pre_hook_error_aborts_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let conn = adapter.shared_connector();

        // Pre-populate the destructive target with a sentinel row.
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS seeds")
            .await
            .unwrap();
        adapter
            .execute_statement("CREATE TABLE seeds.widgets (id BIGINT, name VARCHAR)")
            .await
            .unwrap();
        adapter
            .execute_statement("INSERT INTO seeds.widgets VALUES (99, 'sentinel')")
            .await
            .unwrap();

        // A guard that fails: abort when the target already holds rows.
        let seed = seed_with_hooks(
            dir.path(),
            "id,name\n1,Alice\n2,Bob\n",
            vec![
                "SELECT CASE WHEN (SELECT COUNT(*) FROM seeds.widgets) > 0 \
                 THEN error('target not empty') END"
                    .into(),
            ],
            vec![],
        );

        let dialect = adapter.dialect();
        let result = load_single_seed(&seed, dialect, &adapter, "", "seeds").await;

        assert!(result.is_err(), "pre_hook failure must abort the seed");
        // The original sentinel row is intact — the DROP never ran.
        assert_eq!(row_count(&conn, "seeds.widgets"), 1);
        let guard = conn.lock().unwrap();
        let names = guard.execute_sql("SELECT name FROM seeds.widgets").unwrap();
        assert_eq!(names.rows[0][0].as_str(), Some("sentinel"));
    }

    /// (b) A clean pre_hook proceeds and the seed loads its rows.
    #[tokio::test]
    async fn clean_pre_hook_proceeds_and_loads() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let conn = adapter.shared_connector();

        let seed = seed_with_hooks(
            dir.path(),
            "id,name\n1,Alice\n2,Bob\n3,Carol\n",
            // Trivially-true guard: target is empty, so no error.
            vec!["SELECT 1".into()],
            vec![],
        );

        let dialect = adapter.dialect();
        load_single_seed(&seed, dialect, &adapter, "", "seeds")
            .await
            .expect("clean pre_hook should allow the load");

        // The DB is the source of truth: all 3 CSV rows landed.
        assert_eq!(row_count(&conn, "seeds.widgets"), 3);
    }

    /// The reported row count equals the number of CSV *data* rows (header
    /// excluded), for both a 2-row and a 5-row file. Regression guard for the
    /// off-by-one heuristic that reported N+1 (B9).
    #[tokio::test]
    async fn reported_row_count_matches_data_rows() {
        for (csv, expected) in [
            ("id,name\n1,Alice\n2,Bob\n", 2usize),
            ("id,name\n1,A\n2,B\n3,C\n4,D\n5,E\n", 5usize),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
            let seed = seed_with_hooks(dir.path(), csv, vec![], vec![]);
            let dialect = adapter.dialect();
            let (rows, _schema) = load_single_seed(&seed, dialect, &adapter, "", "seeds")
                .await
                .expect("seed should load");
            assert_eq!(rows, expected, "row count mismatch for {expected}-row CSV");
            // Cross-check against the database itself.
            let conn = adapter.shared_connector();
            assert_eq!(row_count(&conn, "seeds.widgets"), expected as i64);
        }
    }

    /// (c) A post_hook runs after a successful load.
    #[tokio::test]
    async fn post_hook_runs_after_successful_load() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        let conn = adapter.shared_connector();

        let seed = seed_with_hooks(
            dir.path(),
            "id,name\n1,Alice\n2,Bob\n",
            vec![],
            // Side-effecting post_hook: materialize a marker table that can
            // only exist if the post_hook fired after the load.
            vec![
                "CREATE TABLE seeds.post_marker AS \
                 SELECT COUNT(*) AS loaded FROM seeds.widgets"
                    .into(),
            ],
        );

        let dialect = adapter.dialect();
        load_single_seed(&seed, dialect, &adapter, "", "seeds")
            .await
            .expect("seed with post_hook should succeed");

        assert_eq!(row_count(&conn, "seeds.widgets"), 2);
        // The marker exists and recorded the loaded row count — proving the
        // post_hook fired after the load completed.
        let guard = conn.lock().unwrap();
        let marker = guard
            .execute_sql("SELECT loaded FROM seeds.post_marker")
            .unwrap();
        assert_eq!(cell_as_i64(&marker.rows[0][0]), 2);
    }
}
