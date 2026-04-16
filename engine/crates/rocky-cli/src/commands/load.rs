//! `rocky load` — load files from a local directory into the warehouse.
//!
//! Discovers files in a source directory, uses the configured adapter's
//! [`LoaderAdapter`] implementation (or falls back to the generic
//! [`CsvBatchReader`] + batched INSERT path), and reports per-file results.
//!
//! [`LoaderAdapter`]: rocky_adapter_sdk::LoaderAdapter
//! [`CsvBatchReader`]: rocky_core::arrow_loader::CsvBatchReader

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tracing::info;

use rocky_adapter_sdk::{FileFormat, LoadOptions, LoadSource, LoaderAdapter, TableRef};
use rocky_core::state::{LoadedFileRecord, StateStore};

use crate::output::{LoadFileOutput, LoadOutput, print_json};
use crate::registry::{AdapterRegistry, resolve_pipeline};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky load`: discover files, load them into the target table(s).
///
/// CLI args override values from the load pipeline config. When `--source-dir`
/// is omitted, the pipeline's `source_dir` from `rocky.toml` is used.
pub async fn run_load(
    config_path: &Path,
    cli_source_dir: Option<&Path>,
    format: Option<&str>,
    target_table: Option<&str>,
    pipeline: Option<&str>,
    truncate: bool,
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
    let (pipeline_name, pipeline_cfg) =
        resolve_pipeline(&rocky_cfg, pipeline).context("failed to resolve pipeline for load")?;
    let load_cfg = pipeline_cfg.as_load();

    // Resolve source directory: CLI overrides config.
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    let source_dir_buf;
    let source_dir: &Path = if let Some(dir) = cli_source_dir {
        dir
    } else if let Some(lc) = load_cfg {
        source_dir_buf = config_dir.join(&lc.source_dir);
        &source_dir_buf
    } else {
        bail!(
            "no --source-dir provided and the pipeline is not type = \"load\"; \
             either pass --source-dir or configure a load pipeline in rocky.toml"
        );
    };

    if !source_dir.is_dir() {
        bail!("source directory does not exist: {}", source_dir.display());
    }

    // Resolve format: CLI > config > auto-detect.
    let requested_format = match format {
        Some(f) => Some(parse_format(f)?),
        None => load_cfg
            .as_ref()
            .and_then(|lc| lc.format)
            .map(config_format_to_sdk),
    };

    // Resolve target catalog/schema from config, falling back to defaults.
    let target_catalog = load_cfg
        .as_ref()
        .map(|lc| lc.target.catalog.as_str())
        .unwrap_or("");
    let target_schema = load_cfg
        .as_ref()
        .map(|lc| lc.target.schema.as_str())
        .unwrap_or("main");
    let config_target_table = load_cfg.as_ref().and_then(|lc| lc.target.table.as_deref());

    // Resolve load options from config, with CLI overrides.
    let base_options = load_cfg
        .as_ref()
        .map(|lc| config_options_to_sdk(&lc.options))
        .unwrap_or_default();

    // Open the state store for tracking loaded files.
    let state_path = config_dir.join(".rocky_state");
    let state_store = StateStore::open(&state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    let adapter_name = pipeline_cfg.target_adapter();

    // Build a DuckDB loader adapter if the adapter type is DuckDB.
    let adapter_cfg = registry
        .adapter_config(adapter_name)
        .context(format!("no adapter config for '{adapter_name}'"))?;

    let loader: Box<dyn LoaderAdapter> = build_loader(adapter_name, adapter_cfg, &registry)?;

    // Discover files in the source directory.
    let files = discover_files(source_dir, requested_format)?;

    let format_label = format.unwrap_or("auto");

    if files.is_empty() {
        if json {
            let output = LoadOutput {
                version: VERSION.into(),
                command: "load".into(),
                source_dir: source_dir.display().to_string(),
                format: format_label.to_string(),
                files_loaded: 0,
                files_failed: 0,
                total_rows: 0,
                total_bytes: 0,
                files: vec![],
                duration_ms: start.elapsed().as_millis() as u64,
            };
            print_json(&output)?;
        } else {
            println!("No loadable files found in {}", source_dir.display());
        }
        return Ok(());
    }

    let mut file_results: Vec<LoadFileOutput> = Vec::new();
    let mut files_loaded = 0usize;
    let mut files_failed = 0usize;
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;

    for file_path in &files {
        let file_start = Instant::now();
        let file_name = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        // Derive the target table name: CLI > config > file stem.
        let table_name = target_table.or(config_target_table).unwrap_or(file_name);

        let target = TableRef {
            catalog: target_catalog.to_string(),
            schema: target_schema.to_string(),
            table: table_name.to_string(),
        };

        let target_display = if target_catalog.is_empty() {
            format!("{target_schema}.{table_name}")
        } else {
            format!("{target_catalog}.{target_schema}.{table_name}")
        };

        let options = LoadOptions {
            format: requested_format.or(base_options.format),
            create_table: base_options.create_table,
            truncate_first: truncate || base_options.truncate_first,
            batch_size: base_options.batch_size,
            csv_delimiter: base_options.csv_delimiter,
            csv_has_header: base_options.csv_has_header,
        };

        info!(
            file = %file_path.display(),
            target = %target_display,
            "loading file"
        );

        let load_source = LoadSource::LocalFile(file_path.clone());
        match loader.load(&load_source, &target, &options).await {
            Ok(result) => {
                let duration = file_start.elapsed().as_millis() as u64;
                info!(
                    file = %file_path.display(),
                    rows = result.rows_loaded,
                    bytes = result.bytes_read,
                    duration_ms = duration,
                    "file loaded"
                );
                total_rows += result.rows_loaded;
                total_bytes += result.bytes_read;

                // Record the loaded file in the state store for incremental tracking.
                let record = LoadedFileRecord {
                    loaded_at: chrono::Utc::now(),
                    rows_loaded: result.rows_loaded,
                    bytes_read: result.bytes_read,
                    duration_ms: duration,
                    file_hash: None,
                };
                if let Err(e) = state_store.record_loaded_file(
                    pipeline_name,
                    &file_path.display().to_string(),
                    &record,
                ) {
                    tracing::warn!(
                        error = %e,
                        file = %file_path.display(),
                        "failed to record loaded file in state store"
                    );
                }

                file_results.push(LoadFileOutput {
                    file: file_path.display().to_string(),
                    target: target_display,
                    rows_loaded: result.rows_loaded,
                    bytes_read: result.bytes_read,
                    duration_ms: duration,
                    error: None,
                });
                files_loaded += 1;
            }
            Err(e) => {
                let duration = file_start.elapsed().as_millis() as u64;
                let err_msg = format!("{e}");
                tracing::error!(
                    file = %file_path.display(),
                    error = %err_msg,
                    "file load failed"
                );
                file_results.push(LoadFileOutput {
                    file: file_path.display().to_string(),
                    target: target_display,
                    rows_loaded: 0,
                    bytes_read: 0,
                    duration_ms: duration,
                    error: Some(err_msg),
                });
                files_failed += 1;
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    if json {
        let output = LoadOutput {
            version: VERSION.into(),
            command: "load".into(),
            source_dir: source_dir.display().to_string(),
            format: format_label.to_string(),
            files_loaded,
            files_failed,
            total_rows,
            total_bytes,
            files: file_results,
            duration_ms,
        };
        print_json(&output)?;
    } else {
        println!(
            "\nLoad complete: {} loaded, {} failed ({} ms)",
            files_loaded, files_failed, duration_ms
        );
        println!("  Total: {} rows, {} bytes", total_rows, total_bytes);
        for f in &file_results {
            let status = if f.error.is_some() { "FAIL" } else { "OK" };
            println!(
                "  [{status}] {} -> {} ({} rows, {} bytes, {} ms)",
                f.file, f.target, f.rows_loaded, f.bytes_read, f.duration_ms
            );
            if let Some(ref err) = f.error {
                println!("       {err}");
            }
        }
    }

    if files_failed > 0 {
        bail!("{files_failed} file(s) failed to load");
    }

    Ok(())
}

/// Convert a config `LoadFileFormat` to the adapter SDK `FileFormat`.
fn config_format_to_sdk(fmt: rocky_core::config::LoadFileFormat) -> FileFormat {
    match fmt {
        rocky_core::config::LoadFileFormat::Csv => FileFormat::Csv,
        rocky_core::config::LoadFileFormat::Parquet => FileFormat::Parquet,
        rocky_core::config::LoadFileFormat::JsonLines => FileFormat::JsonLines,
    }
}

/// Convert config `LoadOptionsConfig` to the adapter SDK `LoadOptions`.
fn config_options_to_sdk(opts: &rocky_core::config::LoadOptionsConfig) -> LoadOptions {
    LoadOptions {
        batch_size: opts.batch_size,
        create_table: opts.create_table,
        truncate_first: opts.truncate_first,
        format: None,
        csv_delimiter: opts.csv_delimiter.chars().next().unwrap_or(','),
        csv_has_header: opts.csv_has_header,
    }
}

/// Parse a format string into a [`FileFormat`].
fn parse_format(format: &str) -> Result<FileFormat> {
    match format.to_lowercase().as_str() {
        "csv" => Ok(FileFormat::Csv),
        "parquet" => Ok(FileFormat::Parquet),
        "jsonl" | "jsonlines" | "ndjson" => Ok(FileFormat::JsonLines),
        other => bail!("unsupported format '{other}'; supported: csv, parquet, jsonl"),
    }
}

/// Build a `LoaderAdapter` for the given warehouse adapter, dispatching on
/// the adapter type string from config.
///
/// Each warehouse adapter has a dedicated `LoaderAdapter` impl with its own
/// bulk-load semantics (DuckDB COPY, Databricks COPY INTO, Snowflake stages,
/// BigQuery INSERT fallback).
fn build_loader(
    adapter_name: &str,
    adapter_cfg: &rocky_core::config::AdapterConfig,
    registry: &AdapterRegistry,
) -> Result<Box<dyn LoaderAdapter>> {
    match adapter_cfg.adapter_type.as_str() {
        #[cfg(feature = "duckdb")]
        "duckdb" => {
            use rocky_duckdb::loader::DuckDbLoaderAdapter;

            let db_adapter = if let Some(p) = adapter_cfg.path.as_deref() {
                DuckDbLoaderAdapter::new(
                    rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(std::path::Path::new(p))
                        .context(format!("failed to open DuckDB at '{p}'"))?
                        .shared_connector(),
                )
            } else {
                DuckDbLoaderAdapter::in_memory().context("failed to create in-memory DuckDB")?
            };
            Ok(Box::new(db_adapter))
        }
        #[cfg(not(feature = "duckdb"))]
        "duckdb" => {
            bail!("DuckDB support not compiled in (enable 'duckdb' feature)");
        }
        "bigquery" => registry.bigquery_loader(adapter_name),
        "databricks" => registry.databricks_loader(adapter_name),
        "snowflake" => registry.snowflake_loader(adapter_name),
        other => {
            bail!(
                "adapter type '{other}' does not support the load command yet; \
                 supported: duckdb, bigquery, databricks, snowflake"
            );
        }
    }
}

/// Discover loadable files in a directory.
///
/// If a format filter is provided, only files matching that extension are
/// returned. Otherwise, all files with recognized extensions are included.
fn discover_files(
    dir: &Path,
    format_filter: Option<FileFormat>,
) -> Result<Vec<std::path::PathBuf>> {
    let entries = std::fs::read_dir(dir)
        .context(format!("failed to read source directory {}", dir.display()))?;

    let mut files: Vec<std::path::PathBuf> = entries
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .filter(|p| {
            let ext = p.extension().and_then(|e| e.to_str()).unwrap_or_default();
            match format_filter {
                Some(fmt) => ext.eq_ignore_ascii_case(fmt.extension()),
                None => FileFormat::from_extension(ext).is_some(),
            }
        })
        .collect();

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_format_csv() {
        assert_eq!(parse_format("csv").unwrap(), FileFormat::Csv);
        assert_eq!(parse_format("CSV").unwrap(), FileFormat::Csv);
    }

    #[test]
    fn test_parse_format_parquet() {
        assert_eq!(parse_format("parquet").unwrap(), FileFormat::Parquet);
    }

    #[test]
    fn test_parse_format_jsonl() {
        assert_eq!(parse_format("jsonl").unwrap(), FileFormat::JsonLines);
        assert_eq!(parse_format("jsonlines").unwrap(), FileFormat::JsonLines);
        assert_eq!(parse_format("ndjson").unwrap(), FileFormat::JsonLines);
    }

    #[test]
    fn test_parse_format_unknown() {
        assert!(parse_format("xlsx").is_err());
    }

    #[test]
    fn test_discover_files_csv() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        let txt = dir.path().join("readme.txt");
        std::fs::File::create(&csv)
            .unwrap()
            .write_all(b"id,name\n1,Alice\n")
            .unwrap();
        std::fs::File::create(&txt)
            .unwrap()
            .write_all(b"not a data file")
            .unwrap();

        let files = discover_files(dir.path(), None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], csv);
    }

    #[test]
    fn test_discover_files_with_format_filter() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data.csv");
        let jsonl = dir.path().join("events.jsonl");
        std::fs::File::create(&csv)
            .unwrap()
            .write_all(b"id\n1\n")
            .unwrap();
        std::fs::File::create(&jsonl)
            .unwrap()
            .write_all(b"{\"id\":1}\n")
            .unwrap();

        let files = discover_files(dir.path(), Some(FileFormat::Csv)).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].to_str().unwrap().ends_with(".csv"));
    }

    #[test]
    fn test_discover_files_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = discover_files(dir.path(), None).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_discover_files_nonexistent_dir() {
        let result = discover_files(Path::new("/nonexistent/path"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_format_to_sdk() {
        assert_eq!(
            config_format_to_sdk(rocky_core::config::LoadFileFormat::Csv),
            FileFormat::Csv
        );
        assert_eq!(
            config_format_to_sdk(rocky_core::config::LoadFileFormat::Parquet),
            FileFormat::Parquet
        );
        assert_eq!(
            config_format_to_sdk(rocky_core::config::LoadFileFormat::JsonLines),
            FileFormat::JsonLines
        );
    }

    #[test]
    fn test_config_options_to_sdk_defaults() {
        let config_opts = rocky_core::config::LoadOptionsConfig::default();
        let sdk_opts = config_options_to_sdk(&config_opts);
        assert_eq!(sdk_opts.batch_size, 10_000);
        assert!(sdk_opts.create_table);
        assert!(!sdk_opts.truncate_first);
        assert_eq!(sdk_opts.csv_delimiter, ',');
        assert!(sdk_opts.csv_has_header);
    }

    #[test]
    fn test_config_options_to_sdk_custom() {
        let config_opts = rocky_core::config::LoadOptionsConfig {
            batch_size: 5000,
            create_table: false,
            truncate_first: true,
            csv_delimiter: "\t".to_string(),
            csv_has_header: false,
        };
        let sdk_opts = config_options_to_sdk(&config_opts);
        assert_eq!(sdk_opts.batch_size, 5000);
        assert!(!sdk_opts.create_table);
        assert!(sdk_opts.truncate_first);
        assert_eq!(sdk_opts.csv_delimiter, '\t');
        assert!(!sdk_opts.csv_has_header);
    }

    /// Config-driven round-trip: parse load pipeline config, construct loader,
    /// load a file, verify data and state tracking.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn test_config_driven_load_roundtrip() {
        use rocky_duckdb::loader::DuckDbLoaderAdapter;

        // Create a temp directory with a CSV file and a rocky.toml.
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir(&data_dir).unwrap();

        let csv_path = data_dir.join("orders.csv");
        std::fs::write(
            &csv_path,
            "id,product,qty\n1,Widget,10\n2,Gadget,5\n3,Doohickey,3\n",
        )
        .unwrap();

        let toml_content = r#"
[adapter.default]
type = "duckdb"

[pipeline.ingest]
type = "load"
source_dir = "data/"
format = "csv"

[pipeline.ingest.target]
adapter = "default"
catalog = ""
schema = "main"

[pipeline.ingest.options]
batch_size = 1000
create_table = true
"#;
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, toml_content).unwrap();

        // Parse the config.
        let rocky_cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let pipeline = rocky_cfg.pipelines.get("ingest").unwrap();
        let load_cfg = pipeline.as_load().unwrap();

        // Verify config values parsed correctly.
        assert_eq!(load_cfg.source_dir, "data/");
        assert_eq!(
            load_cfg.format,
            Some(rocky_core::config::LoadFileFormat::Csv)
        );
        assert_eq!(load_cfg.target.schema, "main");
        assert_eq!(load_cfg.options.batch_size, 1000);

        // Build the loader adapter from the config.
        let loader = DuckDbLoaderAdapter::in_memory().unwrap();

        // Convert config options to SDK options.
        let options = config_options_to_sdk(&load_cfg.options);
        let target = rocky_adapter_sdk::TableRef {
            catalog: load_cfg.target.catalog.clone(),
            schema: load_cfg.target.schema.clone(),
            table: "orders".into(),
        };

        // Load the file.
        let source = rocky_adapter_sdk::LoadSource::LocalFile(csv_path.clone());
        let result = loader.load(&source, &target, &options).await.unwrap();

        assert_eq!(result.rows_loaded, 3);
        assert!(result.bytes_read > 0);

        // Verify data in the table.
        let conn = loader.shared_connector();
        let guard = conn.lock().unwrap();
        let qr = guard
            .execute_sql("SELECT product FROM main.orders ORDER BY id")
            .unwrap();
        assert_eq!(qr.rows.len(), 3);
        assert_eq!(qr.rows[0][0], "Widget");
        assert_eq!(qr.rows[1][0], "Gadget");
        assert_eq!(qr.rows[2][0], "Doohickey");

        // Verify state tracking round-trip.
        let state_path = dir.path().join(".rocky_state");
        let state_store = StateStore::open(&state_path).unwrap();
        let record = LoadedFileRecord {
            loaded_at: chrono::Utc::now(),
            rows_loaded: result.rows_loaded,
            bytes_read: result.bytes_read,
            duration_ms: result.duration_ms,
            file_hash: None,
        };
        state_store
            .record_loaded_file("ingest", &csv_path.display().to_string(), &record)
            .unwrap();

        // Confirm the state was recorded.
        let retrieved = state_store
            .get_loaded_file("ingest", &csv_path.display().to_string())
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.rows_loaded, 3);

        // Confirm listing works.
        let files = state_store.list_loaded_files("ingest").unwrap();
        assert_eq!(files.len(), 1);
    }
}
