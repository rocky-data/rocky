//! `rocky load` — load files from a local directory into the warehouse.
//!
//! Discovers files in a source directory, uses the configured adapter's
//! [`LoaderAdapter`] implementation (or falls back to the generic
//! [`CsvBatchReader`] + batched INSERT path), and reports per-file results.
//!
//! [`LoaderAdapter`]: rocky_adapter_sdk::LoaderAdapter
//! [`CsvBatchReader`]: rocky_core::arrow_loader::CsvBatchReader

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tracing::info;

use rocky_adapter_sdk::{FileFormat, LoadOptions, LoadSource, LoaderAdapter, TableRef};
use rocky_core::contracts::{ContractConfig, ContractResult, validate_contract_typed};
use rocky_core::state::{LoadedFileRecord, StateStore};
use rocky_core::traits::WarehouseAdapter;

use crate::output::{LoadFileOutput, LoadOutput, print_json};
use crate::registry::{AdapterRegistry, resolve_pipeline};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Suffix appended to a target table name to form its per-load staging table.
const STAGING_SUFFIX: &str = "__rocky_stg";

/// Execute `rocky load`: discover files, load them into the target table(s).
///
/// CLI args override values from the load pipeline config. When `--source-dir`
/// is omitted, the pipeline's `source_dir` from `rocky.toml` is used.
///
/// WP-01 PR-B (2b, RD-003): executes from the caller's fingerprinted `loaded`
/// snapshot (never a self-load — #1120) against the caller's canonical
/// `state_path` (never the legacy `<config_dir>/.rocky_state`), and owns a
/// [`RemoteStateSession`][rocky_core::state_sync::RemoteStateSession] around
/// its state mutation: acquire + unconditional `require_synced` before the
/// store open, capture the per-file results, ALWAYS finalize, then propagate.
/// `config_path` remains for directory resolution only (`source_dir`, the
/// legacy state file's location).
#[allow(clippy::too_many_arguments)]
pub async fn run_load(
    config_path: &Path,
    loaded: &rocky_core::config::LoadedConfig,
    state_path: &Path,
    cli_source_dir: Option<&Path>,
    format: Option<&str>,
    target_table: Option<&str>,
    pipeline: Option<&str>,
    truncate: bool,
    durability: rocky_core::state_sync::FinalizeDurability,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // The caller's threaded snapshot (formerly a self-load here — #1120).
    let rocky_cfg = &loaded.config;
    let registry = AdapterRegistry::from_config(rocky_cfg)?;

    // Resolve pipeline to get the target adapter.
    let (pipeline_name, pipeline_cfg) =
        resolve_pipeline(rocky_cfg, pipeline).context("failed to resolve pipeline for load")?;
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

    let adapter_name = pipeline_cfg.target_adapter();

    let adapter_cfg = registry
        .adapter_config(adapter_name)
        .context(format!("no adapter config for '{adapter_name}'"))?;

    // Optional data contract that gates the load (staging-promote semantics).
    let contract: Option<&ContractConfig> = load_cfg.as_ref().and_then(|lc| lc.contract.as_ref());

    // Build the loader. When a contract is present we route through the
    // warehouse adapter's *shared* connector so the staging table the loader
    // writes is visible to `describe_table`/promote SQL (critical on DuckDB,
    // where a fresh in-memory loader would land staging in a separate DB).
    let loader: Box<dyn LoaderAdapter> = build_loader(adapter_name, adapter_cfg, &registry)?;

    // The warehouse adapter is only needed for the staging-promote gate.
    let warehouse: Option<Arc<dyn WarehouseAdapter>> = if contract.is_some() {
        Some(
            registry
                .warehouse_adapter(adapter_name)
                .context("failed to resolve warehouse adapter for contract-gated load")?,
        )
    } else {
        None
    };

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
        // Pre-session early return: an empty load performs NO state mutation,
        // so it does no remote I/O at all (the ADR's lazy contract).
        return Ok(());
    }

    // -----------------------------------------------------------------------
    // Remote-state session (WP-01 PR-B 2b, RD-003) — acquired only once there
    // is at least one file to load, BEFORE the store open below.
    // -----------------------------------------------------------------------
    let mut session =
        rocky_core::state_sync::RemoteStateSession::new(&rocky_cfg.state, state_path, durability);
    if let Err(e) = session.acquire().await {
        // Unreachable on a fresh session (`Err` = double-acquire misuse);
        // consume defensively so no exit path can leak the session.
        session.abandon("load acquire misuse").await;
        return Err(e.into());
    }
    // UNCONDITIONAL fail-closed guard (unlike the run arms' governed-only
    // bail): a load is a mutating single-purpose command, and proceeding
    // blind on shared state it could not read is wrong for it regardless of
    // governance. A genuine fresh start (`FreshStart`) proceeds; only
    // `Indeterminate` refuses.
    //
    // DELIBERATE exception to the general ungoverned degraded-continue
    // contract (documented in ADR-STATE-SESSION §3). Honest scope
    // (re-review corrected): NOTHING in the engine consults `loaded_files`
    // to skip a load today — the loop below loads every discovered file
    // without reading the ledger and records only after success — so a
    // missing remote record does not by itself cause duplicate ingestion.
    // The rationale is ledger integrity, not dedup: `loaded_files` is the
    // replicated cross-pod record of what each pod ingested (the audit
    // trail and the foundation any future incremental-skip consumer reads),
    // and proceeding degraded would silently orphan it — the Indeterminate
    // authority suppresses the terminal upload, so this run's records would
    // be stranded locally, invisible to every other pod. Refusing beats
    // silently losing replicated ledger rows for a command whose only job
    // is the mutation being recorded. Pinned by
    // `download_failure_fails_closed_load` (tests/remote_state_bypass.rs).
    if let Err(e) = session.require_synced() {
        session.abandon("load download failure (fail-closed)").await;
        return Err(anyhow::Error::new(e).context(
            "failed to download remote state before the load; a remote-backend load \
             requires the state backend reachable to read the authoritative loaded-files \
             ledger (fail-closed)",
        ));
    }

    // Open the CANONICAL state store for tracking loaded files — the same
    // path + `on_schema_mismatch` policy as `rocky run` (load previously
    // hard-`open`ed a legacy `<config_dir>/.rocky_state`).
    let state_store =
        match StateStore::open_with_policy(state_path, rocky_cfg.state.on_schema_mismatch) {
            Ok(store) => store,
            Err(e) => {
                session.abandon("load state store open failed").await;
                return Err(anyhow::Error::new(e).context(format!(
                    "failed to open state store at {}",
                    state_path.display()
                )));
            }
        };
    // Forward-incompat recreate ⇒ never push the downgraded ledger back over
    // newer shared state (parity with the run path's suppression).
    if state_store.was_recreated_for_forward_incompat() {
        session.set_suppress_upload("forward-incompat recreate");
    }

    // Legacy-state migration: a LOGICAL IMPORT, never a rename (ADR §3). The
    // legacy `<config_dir>/.rocky_state` and the canonical state file map to
    // the SAME remote object (`remote_state_key` keys on the parent directory
    // name), so a rename would be erased by the next remote download —
    // LOADED_FILES is a replicated table that downloads replace wholesale —
    // and a shared legacy DB can hold OTHER pipelines' rows (keys are
    // `pipeline|file_path`), which must not be moved out from under them.
    // So: import ONLY the selected pipeline's rows into the canonical store,
    // idempotently (existing canonical keys win), and leave the legacy file
    // in place. A legacy file that fails to open is warned about and skipped
    // — never aborts the load.
    let legacy_state_path = config_dir.join(".rocky_state");
    let same_as_canonical = match (legacy_state_path.canonicalize(), state_path.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => legacy_state_path == state_path,
    };
    if legacy_state_path.exists() && !same_as_canonical {
        match StateStore::open_read_only(&legacy_state_path) {
            Ok(legacy_store) => match legacy_store.list_loaded_files(pipeline_name) {
                Ok(rows) => {
                    let mut imported = 0usize;
                    for (file_path, record) in rows {
                        let already = state_store
                            .get_loaded_file(pipeline_name, &file_path)
                            .ok()
                            .flatten()
                            .is_some();
                        if already {
                            continue;
                        }
                        if let Err(e) =
                            state_store.record_loaded_file(pipeline_name, &file_path, &record)
                        {
                            tracing::warn!(
                                error = %e,
                                file = %file_path,
                                "failed to import a legacy loaded-file record; its ledger \
                                 entry remains only in the legacy state file"
                            );
                        } else {
                            imported += 1;
                        }
                    }
                    if imported > 0 {
                        tracing::warn!(
                            legacy_state = %legacy_state_path.display(),
                            pipeline = pipeline_name,
                            imported,
                            "imported legacy loaded-file records into the canonical state \
                             store; the legacy file is left in place (other pipelines may \
                             still reference it) and is no longer read by this pipeline"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        legacy_state = %legacy_state_path.display(),
                        "failed to read legacy loaded-file records; skipping the one-time \
                         import (their ledger entries remain only in the legacy state file)"
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    legacy_state = %legacy_state_path.display(),
                    "failed to open the legacy load state file; skipping the one-time \
                     import (its ledger entries remain only in the legacy state file)"
                );
            }
        }
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

        // Two paths: a contract-gated staging-promote, or the direct load.
        let outcome = match (contract, warehouse.as_deref()) {
            (Some(contract), Some(warehouse)) => {
                load_with_contract_gate(
                    loader.as_ref(),
                    warehouse,
                    &load_source,
                    &target,
                    &options,
                    contract,
                )
                .await
            }
            _ => loader
                .load(&load_source, &target, &options)
                .await
                .map(|r| (r.rows_loaded, r.bytes_read, None))
                .map_err(|e| anyhow::anyhow!("{e}")),
        };

        match outcome {
            Ok((rows_loaded, bytes_read, contract_result)) => {
                let duration = file_start.elapsed().as_millis() as u64;
                info!(
                    file = %file_path.display(),
                    rows = rows_loaded,
                    bytes = bytes_read,
                    duration_ms = duration,
                    "file loaded"
                );
                total_rows += rows_loaded;
                total_bytes += bytes_read;

                // Record the loaded file only after a successful load (and, for
                // contract-gated loads, a successful promote).
                let record = LoadedFileRecord {
                    loaded_at: chrono::Utc::now(),
                    rows_loaded,
                    bytes_read,
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
                    rows_loaded,
                    bytes_read,
                    duration_ms: duration,
                    error: None,
                    contract: contract_result,
                });
                files_loaded += 1;
            }
            Err(e) => {
                let duration = file_start.elapsed().as_millis() as u64;
                let err_msg = format!("{e:#}");
                tracing::error!(
                    file = %file_path.display(),
                    error = %err_msg,
                    "file load failed"
                );
                // Surface the contract result on a gate failure when available,
                // so JSON consumers see the specific violations.
                let contract_result = e.downcast_ref::<ContractGateError>().map(|g| g.0.clone());
                file_results.push(LoadFileOutput {
                    file: file_path.display().to_string(),
                    target: target_display,
                    rows_loaded: 0,
                    bytes_read: 0,
                    duration_ms: duration,
                    error: Some(err_msg),
                    contract: contract_result,
                });
                files_failed += 1;
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    // Per-file results captured → ALWAYS finalize → then propagate. `run_load`
    // records successes per-file and reports errors only after all files (the
    // `files_failed` bail below), so `abandon` on a partial failure would drop
    // file A's just-recorded ledger entry from the shared remote — the
    // successful files' records must ride the terminal upload regardless of a
    // later file's failure. The store is dropped first (advisory-lock release
    // + flush), matching the gc/policy/restore seam ordering.
    drop(state_store);
    session.finalize().await.context(
        "refusing to exit 0: the loaded-file state could not be persisted to the \
         remote [state] backend",
    )?;

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
            // Share the registry's DuckDB connector so a loaded (staging)
            // table is visible to the same warehouse adapter's `describe_table`
            // and promote SQL. This matters for in-memory DuckDB, where a
            // freshly-opened loader would otherwise use a separate database.
            registry.duckdb_loader(adapter_name)
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

/// Error carrying the [`ContractResult`] of a failed contract gate so the
/// caller can surface the specific violations in JSON output.
#[derive(Debug)]
struct ContractGateError(ContractResult);

impl std::fmt::Display for ContractGateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msgs: Vec<String> = self
            .0
            .violations
            .iter()
            .map(|v| format!("{} [{}]: {}", v.column, v.rule, v.message))
            .collect();
        write!(
            f,
            "contract validation failed ({} violation(s)): {}",
            self.0.violations.len(),
            msgs.join("; ")
        )
    }
}

impl std::error::Error for ContractGateError {}

/// Load one file into a staging table, validate it against `contract`, and
/// promote to `target` only if validation passes (true staging-promote gate).
///
/// On a contract failure the staging table is dropped and the function returns
/// a [`ContractGateError`] — the target is never created or modified, so
/// non-conforming data never lands. The staging table is also dropped on the
/// happy path after a successful promote.
///
/// Returns `(rows_loaded, bytes_read, Some(contract_result))` on success.
async fn load_with_contract_gate(
    loader: &dyn LoaderAdapter,
    warehouse: &dyn WarehouseAdapter,
    source: &LoadSource,
    target: &TableRef,
    options: &LoadOptions,
    contract: &ContractConfig,
) -> Result<(u64, u64, Option<ContractResult>)> {
    // Validate every identifier before it touches SQL. The staging name is
    // `<table>__rocky_stg`; the suffix keeps it a valid SQL identifier.
    let staging_name = format!("{}{STAGING_SUFFIX}", target.table);
    rocky_sql::validation::validate_identifier(&target.table)
        .with_context(|| format!("invalid target table name '{}'", target.table))?;
    rocky_sql::validation::validate_identifier(&target.schema)
        .with_context(|| format!("invalid target schema '{}'", target.schema))?;
    // The catalog is intentionally NOT validated with the strict
    // `validate_identifier` here. It's the project/catalog component, which on
    // BigQuery is a GCP project ID containing hyphens (`my-project-123`) that
    // the strict rule rejects. It's validated dialect-appropriately by
    // `dialect.format_table_ref` below — BigQuery via `validate_gcp_project_id`
    // (blocks injection chars, permits hyphens), every other dialect via the
    // strict `validate_identifier` — and that runs before any SQL touches the
    // warehouse (see the `format_table_ref` calls preceding the first
    // `execute_statement`). This matches the non-gate `rocky load` path.
    // staging_name is derived from the already-validated table name + a static
    // suffix, so it's guaranteed valid; validate anyway as a belt-and-braces.
    rocky_sql::validation::validate_identifier(&staging_name)
        .with_context(|| format!("invalid staging table name '{staging_name}'"))?;

    let staging = TableRef {
        catalog: target.catalog.clone(),
        schema: target.schema.clone(),
        table: staging_name.clone(),
    };
    let staging_ir = to_ir_table_ref(&staging);
    let target_ir = to_ir_table_ref(target);

    let dialect = warehouse.dialect();
    let staging_ref = dialect
        .format_table_ref(&staging.catalog, &staging.schema, &staging.table)
        .map_err(|e| anyhow::anyhow!("failed to format staging table ref: {e}"))?;
    let target_ref = dialect
        .format_table_ref(&target.catalog, &target.schema, &target.table)
        .map_err(|e| anyhow::anyhow!("failed to format target table ref: {e}"))?;

    // Start from a clean staging table so a prior aborted run can't leak rows.
    let drop_staging_sql = dialect.drop_table_sql(&staging_ref);
    warehouse
        .execute_statement(&drop_staging_sql)
        .await
        .map_err(|e| anyhow::anyhow!("failed to drop pre-existing staging table: {e}"))?;

    // Load the file into staging (always create it fresh).
    let staging_options = LoadOptions {
        create_table: true,
        truncate_first: false,
        ..options.clone()
    };
    let load_result = match loader.load(source, &staging, &staging_options).await {
        Ok(r) => r,
        Err(e) => {
            // Best-effort cleanup; don't mask the original load error.
            let _ = warehouse.execute_statement(&drop_staging_sql).await;
            return Err(anyhow::anyhow!("staging load failed: {e}"));
        }
    };

    // Validate the landed staging schema against the contract.
    let landed = match warehouse.describe_table(&staging_ir).await {
        Ok(cols) => cols,
        Err(e) => {
            let _ = warehouse.execute_statement(&drop_staging_sql).await;
            return Err(anyhow::anyhow!(
                "failed to describe staging table for contract validation: {e}"
            ));
        }
    };
    let result = validate_contract_typed(contract, &landed);

    for warning in &result.warnings {
        tracing::warn!(table = %target, "{warning}");
    }

    if !result.passed {
        // Gate closed: drop staging, leave the target untouched.
        if let Err(e) = warehouse.execute_statement(&drop_staging_sql).await {
            tracing::warn!(error = %e, "failed to drop staging table after contract failure");
        }
        return Err(ContractGateError(result).into());
    }

    // Gate open: promote staging into the target, mirroring the loader's
    // create/truncate semantics but sourced from the staging table.
    let select_from_staging = format!("SELECT * FROM {staging_ref}");
    // Existence probe. A positively-transient failure (lock, timeout, 5xx)
    // propagates (fail closed) rather than assume absence and promote over a
    // target that may be live (the conflation behind #1206). Every other
    // failure, including a genuine "not found", is treated as absent so the
    // load can create the target; a genuine not-found is never classified
    // transient, so first-run load stays intact.
    let target_exists = match warehouse.describe_table(&target_ir).await {
        Ok(cols) => !cols.is_empty(),
        Err(e) if warehouse.classify_failure(&e).is_retryable() => {
            return Err(anyhow::Error::from(e).context(format!(
                "target existence probe for load target '{target_ref}' failed with a \
                 retryable error; refusing to assume it is absent"
            )));
        }
        Err(_) => false,
    };

    // Second line of defense: even if the probe treated a live target as
    // absent, the create path uses the *non-replacing* `create_table_as_new`,
    // so promotion fails ("already exists") instead of erasing it with
    // `CREATE OR REPLACE`. Mirrors the bootstrap fix in `run.rs` / `sql_gen.rs`.
    let promote_stmts: Vec<String> = if !target_exists && options.create_table {
        vec![dialect.create_table_as_new(&target_ref, &select_from_staging)]
    } else {
        let mut stmts = Vec::new();
        if options.truncate_first {
            // `DELETE FROM <t> WHERE TRUE` (via the dialect) instead of a bare
            // `DELETE FROM <t>`: BigQuery rejects an unqualified DELETE, and the
            // `WHERE TRUE` form is portable across DuckDB/Snowflake/Databricks/BQ.
            stmts.push(dialect.delete_where(&target_ref, "TRUE"));
        }
        stmts.push(dialect.insert_into(&target_ref, &select_from_staging));
        stmts
    };

    for stmt in &promote_stmts {
        if let Err(e) = warehouse.execute_statement(stmt).await {
            // Promote failed mid-flight; clean up staging and surface the error.
            let _ = warehouse.execute_statement(&drop_staging_sql).await;
            return Err(anyhow::anyhow!("failed to promote staging to target: {e}"));
        }
    }

    // Promote done — drop staging.
    if let Err(e) = warehouse.execute_statement(&drop_staging_sql).await {
        tracing::warn!(error = %e, "failed to drop staging table after promote");
    }

    Ok((
        load_result.rows_loaded,
        load_result.bytes_read,
        Some(result),
    ))
}

/// Convert an adapter-SDK [`TableRef`] into the rocky-ir `TableRef` that the
/// `WarehouseAdapter` trait consumes.
fn to_ir_table_ref(t: &TableRef) -> rocky_ir::TableRef {
    rocky_ir::TableRef {
        catalog: t.catalog.clone(),
        schema: t.schema.clone(),
        table: t.table.clone(),
    }
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

    // ------------------------------------------------------------------
    // Contract gate (staging-promote) — DuckDB-backed, no credentials
    // ------------------------------------------------------------------

    /// Open a file-backed DuckDB and return whether `main.<table>` exists and
    /// its row count. Uses a fresh adapter so we observe committed on-disk
    /// state after `run_load` has dropped its registry (and thus its
    /// connection).
    #[cfg(feature = "duckdb")]
    fn table_row_count(db_path: &Path, table: &str) -> Option<usize> {
        use rocky_duckdb::DuckDbConnector;
        let conn = DuckDbConnector::open(db_path).unwrap();
        let exists = conn
            .execute_sql(&format!(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name = '{table}'"
            ))
            .map(|r| !r.rows.is_empty())
            .unwrap_or(false);
        if !exists {
            return None;
        }
        let r = conn
            .execute_sql(&format!("SELECT COUNT(*) FROM main.{table}"))
            .unwrap();
        Some(
            r.rows[0][0]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
        )
    }

    #[cfg(feature = "duckdb")]
    fn write_contract_config(dir: &Path, db_path: &Path) -> std::path::PathBuf {
        let toml_content = format!(
            r#"
[adapter.default]
type = "duckdb"
path = "{}"

[pipeline.ingest]
type = "load"
source_dir = "data/"
format = "csv"

[pipeline.ingest.target]
adapter = "default"
catalog = ""
schema = "main"
table = "orders"

[pipeline.ingest.options]
create_table = true

[pipeline.ingest.contract]
required_columns = [
    {{ name = "id", type = "BIGINT" }},
    {{ name = "product", type = "VARCHAR" }},
]
"#,
            db_path.display()
        );
        let config_path = dir.join("rocky.toml");
        std::fs::write(&config_path, toml_content).unwrap();
        config_path
    }

    /// A passing load whose contract also declares `protected_columns`
    /// succeeds and surfaces the unenforceable-clause warning on the gate
    /// result (decision #4: warn, don't silently no-op). Exercises the path
    /// from `validate_contract_typed` warnings into `load_with_contract_gate`.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn test_contract_gate_pass_surfaces_unenforceable_warning() {
        use rocky_core::contracts::{ContractConfig, validate_contract_typed};
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::DuckDbConnector;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::loader::DuckDbLoaderAdapter;
        use std::sync::{Arc, Mutex};

        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("orders.csv");
        std::fs::write(&csv_path, "id,product\n1,Widget\n").unwrap();

        let shared = Arc::new(Mutex::new(DuckDbConnector::in_memory().unwrap()));
        let loader = DuckDbLoaderAdapter::new(Arc::clone(&shared));
        let wh = DuckDbWarehouseAdapter::from_shared(Arc::clone(&shared));

        let contract = ContractConfig {
            required_columns: vec![rocky_core::contracts::RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: true,
            }],
            protected_columns: vec!["id".into()],
            ..Default::default()
        };

        let target = TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "orders".into(),
        };
        let source = LoadSource::LocalFile(csv_path);
        let options = LoadOptions {
            format: Some(FileFormat::Csv),
            create_table: true,
            ..LoadOptions::default()
        };

        let (rows, _bytes, result) =
            load_with_contract_gate(&loader, &wh, &source, &target, &options, &contract)
                .await
                .expect("passing contract should promote");
        assert_eq!(rows, 1);
        let result = result.expect("gate result should be present");
        assert!(result.passed);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("protected_columns")),
            "unenforceable protected_columns clause must surface as a warning: {:?}",
            result.warnings
        );

        // Sanity: the same warning is what validate_contract_typed emits.
        let landed = wh
            .describe_table(&rocky_ir::TableRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "orders".into(),
            })
            .await
            .unwrap();
        assert!(
            !validate_contract_typed(&contract, &landed)
                .warnings
                .is_empty()
        );
    }

    /// Regression: a transient target `DESCRIBE` failure during contract-gated
    /// promotion must not turn an append load into a destructive replace. The
    /// probe collapses to `target_exists = false`, so the promote must use a
    /// non-replacing CREATE that fails closed on the live target and leaves its
    /// rows untouched. Sibling of the replication-path regression in `run.rs`.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn target_describe_failure_does_not_replace_existing_load_target() {
        use async_trait::async_trait;
        use rocky_core::contracts::{ContractConfig, RequiredColumn};
        use rocky_core::traits::{
            AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter,
        };
        use rocky_duckdb::DuckDbConnector;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::loader::DuckDbLoaderAdapter;
        use std::sync::{Arc, Mutex};

        /// Delegates everything to the real DuckDB adapter but fails `DESCRIBE`
        /// for the target table, simulating a transient metadata-probe failure.
        struct FailTargetDescribe<'a> {
            inner: &'a DuckDbWarehouseAdapter,
            target_table: String,
        }

        #[async_trait]
        impl WarehouseAdapter for FailTargetDescribe<'_> {
            fn dialect(&self) -> &dyn SqlDialect {
                self.inner.dialect()
            }

            async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
                self.inner.execute_statement(sql).await
            }

            async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
                self.inner.execute_query(sql).await
            }

            async fn describe_table(
                &self,
                table: &rocky_ir::TableRef,
            ) -> AdapterResult<Vec<rocky_ir::ColumnInfo>> {
                if table.table == self.target_table {
                    return Err(AdapterError::msg("injected target DESCRIBE failure"));
                }
                self.inner.describe_table(table).await
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("orders.csv");
        std::fs::write(&csv_path, "id,product\n1,Widget\n").unwrap();

        // Loader + warehouse share one in-memory DuckDB so the staging table the
        // loader writes is visible to the promote path.
        let shared = Arc::new(Mutex::new(DuckDbConnector::in_memory().unwrap()));
        let loader = DuckDbLoaderAdapter::new(Arc::clone(&shared));
        let wh = DuckDbWarehouseAdapter::from_shared(Arc::clone(&shared));

        // A live target with history that a destructive replace would erase.
        wh.execute_statement("CREATE TABLE main.orders (id INTEGER)")
            .await
            .unwrap();
        wh.execute_statement("INSERT INTO main.orders VALUES (99)")
            .await
            .unwrap();

        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let target = TableRef {
            catalog: String::new(),
            schema: "main".into(),
            table: "orders".into(),
        };
        let source = LoadSource::LocalFile(csv_path);
        // create_table = true, truncate_first = false → the append case a
        // misprobe would otherwise convert into a full replace.
        let options = LoadOptions {
            format: Some(FileFormat::Csv),
            ..LoadOptions::default()
        };

        let failing = FailTargetDescribe {
            inner: &wh,
            target_table: "orders".into(),
        };
        let error =
            load_with_contract_gate(&loader, &failing, &source, &target, &options, &contract)
                .await
                .expect_err("a failed target probe must not replace an existing table");
        assert!(
            format!("{error:#}").contains("already exists"),
            "promote should fail closed on the existing target: {error:#}"
        );

        // The pre-existing row survived — no destructive replace.
        let rows = wh
            .execute_query("SELECT id FROM main.orders ORDER BY id")
            .await
            .unwrap();
        assert_eq!(rows.rows, vec![vec![serde_json::json!("99")]]);
    }

    /// Drive `run_load` the way the CLI entries do: a fingerprinted config
    /// snapshot + a canonical state path beside the config, `ConfigDefault`
    /// durability (these projects use the Local backend, so the session is a
    /// zero-I/O no-op).
    #[cfg(feature = "duckdb")]
    async fn drive_load(config_path: &Path, json: bool) -> Result<()> {
        let loaded = rocky_core::config::load_rocky_config_fingerprinted(config_path)?;
        let state_path = config_path
            .parent()
            .unwrap_or(Path::new("."))
            .join(".rocky-state.redb");
        run_load(
            config_path,
            &loaded,
            &state_path,
            None,
            None,
            None,
            None,
            false,
            rocky_core::state_sync::FinalizeDurability::ConfigDefault,
            json,
        )
        .await
    }

    /// (a) A CSV satisfying the contract loads and lands rows in the target.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn test_contract_gate_pass_lands_rows() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir(&data_dir).unwrap();
        std::fs::write(
            data_dir.join("orders.csv"),
            "id,product,qty\n1,Widget,10\n2,Gadget,5\n3,Doohickey,3\n",
        )
        .unwrap();

        let db_path = dir.path().join("wh.duckdb");
        let config_path = write_contract_config(dir.path(), &db_path);

        drive_load(&config_path, false)
            .await
            .expect("contract-satisfying load should succeed");

        // Target has the rows; staging table is gone.
        assert_eq!(table_row_count(&db_path, "orders"), Some(3));
        assert_eq!(
            table_row_count(&db_path, "orders__rocky_stg"),
            None,
            "staging table must be dropped after a successful promote"
        );
    }

    /// (b) A CSV violating the contract (missing required column AND a
    /// wrong-type column) fails the command, and the target is never created.
    /// The staging table must also be gone.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn test_contract_gate_fail_leaves_target_absent() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir(&data_dir).unwrap();
        // Missing required column `product`; `id` lands as VARCHAR (not BIGINT).
        std::fs::write(data_dir.join("orders.csv"), "id,qty\nabc,10\ndef,5\n").unwrap();

        let db_path = dir.path().join("wh.duckdb");
        let config_path = write_contract_config(dir.path(), &db_path);

        let err = drive_load(&config_path, false)
            .await
            .expect_err("contract-violating load must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to load"),
            "unexpected error message: {msg}"
        );

        // Target was never created; staging is gone (no leaked tables).
        assert_eq!(
            table_row_count(&db_path, "orders"),
            None,
            "non-conforming data must never land in the target"
        );
        assert_eq!(
            table_row_count(&db_path, "orders__rocky_stg"),
            None,
            "staging table must be dropped after a failed gate"
        );
    }

    /// On a re-run that satisfies the contract, an existing target with a
    /// truncate-first option is replaced rather than duplicated.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn test_contract_gate_json_output_reports_violations() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        std::fs::create_dir(&data_dir).unwrap();
        std::fs::write(data_dir.join("orders.csv"), "id,qty\n1,10\n").unwrap();

        let db_path = dir.path().join("wh.duckdb");
        let config_path = write_contract_config(dir.path(), &db_path);

        // JSON mode still fails (non-zero) but should not panic.
        let err = drive_load(&config_path, true)
            .await
            .expect_err("contract-violating load must fail in json mode too");
        assert!(format!("{err:#}").contains("failed to load"));
        assert_eq!(table_row_count(&db_path, "orders"), None);
    }
}
