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

use rocky_adapter_sdk::{FileFormat, LoadOptions, LoaderAdapter, TableRef};

use crate::output::{LoadFileOutput, LoadOutput, print_json};
use crate::registry::{AdapterRegistry, resolve_pipeline};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky load`: discover files, load them into the target table(s).
pub async fn run_load(
    config_path: &Path,
    source_dir: &Path,
    format: Option<&str>,
    target_table: Option<&str>,
    pipeline: Option<&str>,
    truncate: bool,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Validate source directory exists.
    if !source_dir.is_dir() {
        bail!("source directory does not exist: {}", source_dir.display());
    }

    // Parse the requested format (if any).
    let requested_format = match format {
        Some(f) => Some(parse_format(f)?),
        None => None,
    };

    // Load config and build adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

    // Resolve pipeline to get the target adapter.
    let (_pipeline_name, pipeline_cfg) =
        resolve_pipeline(&rocky_cfg, pipeline).context("failed to resolve pipeline for load")?;
    let adapter_name = pipeline_cfg.target_adapter();

    // Build a DuckDB loader adapter if the adapter type is DuckDB.
    let adapter_cfg = registry
        .adapter_config(adapter_name)
        .context(format!("no adapter config for '{adapter_name}'"))?;

    let loader: Box<dyn LoaderAdapter> = match adapter_cfg.adapter_type.as_str() {
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
            Box::new(db_adapter)
        }
        #[cfg(not(feature = "duckdb"))]
        "duckdb" => {
            bail!("DuckDB support not compiled in (enable 'duckdb' feature)");
        }
        other => {
            bail!(
                "adapter type '{other}' does not support the load command yet; \
                 only 'duckdb' is supported"
            );
        }
    };

    // Discover files in the source directory.
    let files = discover_files(source_dir, requested_format)?;

    if files.is_empty() {
        if json {
            let output = LoadOutput {
                version: VERSION.into(),
                command: "load".into(),
                source_dir: source_dir.display().to_string(),
                format: format.unwrap_or("auto").to_string(),
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

        // Derive the target table name: use the explicit target or the file stem.
        let table_name = target_table.unwrap_or(file_name);

        let target = TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: table_name.to_string(),
        };

        let options = LoadOptions {
            format: requested_format,
            create_table: true,
            truncate_first: truncate,
            ..Default::default()
        };

        info!(
            file = %file_path.display(),
            target = %table_name,
            "loading file"
        );

        match loader.load_file(file_path, &target, &options).await {
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
                file_results.push(LoadFileOutput {
                    file: file_path.display().to_string(),
                    target: format!("main.{table_name}"),
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
                    target: format!("main.{table_name}"),
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
            format: format.unwrap_or("auto").to_string(),
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

/// Parse a format string into a [`FileFormat`].
fn parse_format(format: &str) -> Result<FileFormat> {
    match format.to_lowercase().as_str() {
        "csv" => Ok(FileFormat::Csv),
        "parquet" => Ok(FileFormat::Parquet),
        "jsonl" | "jsonlines" | "ndjson" => Ok(FileFormat::JsonLines),
        other => bail!("unsupported format '{other}'; supported: csv, parquet, jsonl"),
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
}
