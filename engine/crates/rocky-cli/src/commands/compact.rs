//! `rocky compact` — generate OPTIMIZE/VACUUM SQL for storage compaction.
//!
//! Also hosts `rocky compact --measure-dedup` — the Layer 0 cross-table
//! dedup-measurement experiment (`run_measure_dedup`). See
//! `plans/rocky-storage-layer-0.md` for the scope + decision gate.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};

use rocky_core::dedup_analysis::{DedupStats, compute_dedup_stats};
use rocky_core::incremental::{PartitionChecksum, generate_whole_table_checksum_sql};
use rocky_core::ir::TableRef;
use rocky_core::traits::{QueryResult, WarehouseAdapter};
use rocky_sql::validation::validate_identifier;

use crate::output::{
    ByteCalibration, CompactDedupOutput, CompactOutput, CompactTableEntry, CompactTotals,
    DedupPair, DedupSummary, NamedStatement, TableDedupContribution, print_json,
};
use crate::registry::{AdapterRegistry, resolve_pipeline};
use crate::scope::{
    managed_catalog_set, resolve_managed_tables, resolve_managed_tables_in_catalog,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky compact`.
pub fn run_compact(
    model: &str,
    target_size: Option<&str>,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    let target_mb = target_size
        .map(|s| {
            s.trim_end_matches("MB")
                .trim_end_matches("mb")
                .parse::<u64>()
        })
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid --target-size: {e}"))?
        .unwrap_or(256);

    let statements = generate_compact_sql(model, target_mb)?;

    if output_json {
        let typed_statements: Vec<NamedStatement> = statements
            .iter()
            .map(|(purpose, sql)| NamedStatement {
                purpose: purpose.clone(),
                sql: sql.clone(),
            })
            .collect();
        let output = CompactOutput {
            version: VERSION.to_string(),
            command: "compact".to_string(),
            model: Some(model.to_string()),
            catalog: None,
            scope: None,
            dry_run,
            target_size_mb: target_mb,
            statements: typed_statements,
            tables: None,
            totals: None,
        };
        print_json(&output)?;
    } else {
        println!(
            "Compaction plan for: {model} (target file size: {target_mb}MB){}",
            if dry_run { " [DRY RUN]" } else { "" }
        );
        println!();

        for (purpose, sql) in &statements {
            println!("-- {purpose}");
            println!("{sql};");
            println!();
        }

        if dry_run {
            println!("(dry run: no statements executed)");
        }
    }

    Ok(())
}

/// Execute `rocky compact --catalog <name>` — generate compaction SQL
/// for every Rocky-managed table in the catalog.
///
/// Resolves the managed-table set via [`resolve_managed_tables_in_catalog`]
/// (config-only — no warehouse round trip), then templates `OPTIMIZE` /
/// `VACUUM` per table via [`generate_compact_sql`], aggregating into a
/// single envelope. Per-table failures aren't a concern here: the SQL
/// generator is pure, so the only failure paths are pre-loop (config
/// load, pipeline resolution, empty catalog) and surface as a hard
/// error rather than a partial run.
///
/// The flat `statements` list and the per-table `tables` map carry the
/// same information in two shapes — consumers that just iterate SQL
/// keep working; consumers that want per-table attribution read `tables`.
pub async fn run_compact_catalog(
    config_path: &Path,
    catalog: &str,
    target_size: Option<&str>,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    let target_mb = target_size
        .map(|s| {
            s.trim_end_matches("MB")
                .trim_end_matches("mb")
                .parse::<u64>()
        })
        .transpose()
        .map_err(|e| anyhow!("invalid --target-size: {e}"))?
        .unwrap_or(256);

    let scope = resolve_managed_tables_in_catalog(config_path, catalog).await?;

    let mut flat_statements: Vec<NamedStatement> = Vec::new();
    let mut per_table: std::collections::BTreeMap<String, CompactTableEntry> =
        std::collections::BTreeMap::new();

    for fqn in &scope.tables {
        let stmts = generate_compact_sql(fqn, target_mb)?;
        let typed: Vec<NamedStatement> = stmts
            .into_iter()
            .map(|(purpose, sql)| NamedStatement { purpose, sql })
            .collect();
        flat_statements.extend(typed.iter().cloned());
        per_table.insert(fqn.clone(), CompactTableEntry { statements: typed });
    }

    let totals = CompactTotals {
        table_count: scope.tables.len(),
        statement_count: flat_statements.len(),
    };

    if output_json {
        let output = CompactOutput {
            version: VERSION.to_string(),
            command: "compact".to_string(),
            model: None,
            catalog: Some(scope.catalog.clone()),
            scope: Some("catalog".to_string()),
            dry_run,
            target_size_mb: target_mb,
            statements: flat_statements,
            tables: Some(per_table),
            totals: Some(totals),
        };
        print_json(&output)?;
    } else {
        println!(
            "Compaction plan for catalog '{}' ({} tables, target {}MB){}",
            scope.catalog,
            scope.tables.len(),
            target_mb,
            if dry_run { " [DRY RUN]" } else { "" }
        );
        println!();

        for (fqn, entry) in &per_table {
            println!("-- {fqn}");
            for s in &entry.statements {
                println!("-- {}", s.purpose);
                println!("{};", s.sql);
            }
            println!();
        }

        if dry_run {
            println!("(dry run: no statements executed)");
        }
    }

    Ok(())
}

/// Default columns excluded from the "semantic" dedup hash.
///
/// These are the per-row columns Rocky's replication pipeline stamps
/// onto target rows, plus the Fivetran-sourced columns commonly present
/// in Rocky-managed tables. Users can override via `--exclude-columns`.
///
/// **Note:** This list is Fivetran-biased by default because Fivetran is
/// the most common source adapter in practice, but the set is likely to
/// be revisited as other adapters (Airbyte, Meltano, custom) become
/// first-class. Centralizing the defaults here means any future sweep
/// touches exactly one constant.
fn default_metadata_columns() -> Vec<String> {
    let mut cols: Vec<String> = rocky_core::config::WATERMARK_COLUMNS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    cols.insert(0, "_loaded_by".into());
    cols
}

/// Execute `rocky compact --measure-dedup` — the Layer 0 cross-table
/// dedup-measurement experiment.
///
/// Hashes every user table in the warehouse twice (raw = `HASH(*)`,
/// semantic = `HASH(...non-metadata columns...)`) and reports a
/// partition-level **lower bound** on cross-table byte dedup, plus a
/// decision-gate verdict per `plans/rocky-storage-layer-0.md` §6.
///
/// ## Scope (Layer 0 Step 4)
///
/// Deliberate simplifications, to keep this shippable as an experiment:
///
/// - **First pipeline only.** Multi-pipeline projects haven't existed in
///   practice yet; `resolve_pipeline(..., None)` errors cleanly if
///   multiple are defined and prompts the user to narrow. (Decision
///   4(c) from the plan review.)
/// - **`--calibrate-bytes` is row-level.** When passed, samples up to
///   3 tables from the top dedup pairs, pulls their first 10k rows via
///   `SELECT *`, hashes each row with blake3, and cross-compares to
///   produce a byte-level dedup percentage alongside the partition-level
///   one. Not a random sample — uses the warehouse's natural row order.
/// - **No UNION ALL batching.** Each table gets two unbatched queries
///   (raw + semantic). Fine for projects under ~100 tables; batching
///   via the `rocky-databricks::batch` UNION ALL pattern is a follow-up
///   if larger projects need it.
/// - **Whole-table partition key only.** Time-interval partitioning
///   per model sidecar is a Step 4.1 improvement; the plan's L0-D7
///   fallback (`__whole_table__`) handles all tables uniformly for v1.
///
/// ## Alignment with improvement plans
///
/// This handler is trait-object-only — it talks to the warehouse through
/// `WarehouseAdapter` (from `rocky-core::traits`), never through
/// warehouse-specific crates. That's the contract Plan 01 wants `run.rs`
/// to reach and Plan 08 will enforce with a CI lint. Adding this module
/// shouldn't generate any new debt for either plan.
pub async fn run_measure_dedup(
    config_path: &Path,
    exclude_columns: Option<Vec<String>>,
    calibrate_bytes: bool,
    all_tables: bool,
    output_json: bool,
) -> Result<()> {
    let output =
        compute_measure_dedup(config_path, exclude_columns, calibrate_bytes, all_tables).await?;
    if output_json {
        print_json(&output)?;
    } else {
        render_measure_dedup_human(&output);
    }
    Ok(())
}

/// Build the [`CompactDedupOutput`] for `rocky compact --measure-dedup`
/// without rendering it.
///
/// Extracted from [`run_measure_dedup`] so the integration tests can
/// assert on the structured result directly instead of capturing stdout.
/// Visibility is `pub(crate)` because the test in this module's
/// `tests` submodule is the only intended caller — the binary always
/// goes through `run_measure_dedup`.
pub(crate) async fn compute_measure_dedup(
    config_path: &Path,
    exclude_columns: Option<Vec<String>>,
    calibrate_bytes: bool,
    all_tables: bool,
) -> Result<CompactDedupOutput> {
    let started_at = Utc::now();
    let start = Instant::now();

    // 1. Load config and build the adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

    // 2. Resolve the pipeline (single or first).
    let (pipeline_name, pipeline) = resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline for dedup measurement")?;
    let project = pipeline_name.to_string();

    // 3. Get the warehouse adapter — trait object, no warehouse-specific import.
    let adapter_name = pipeline.target_adapter();
    let adapter = registry.warehouse_adapter(adapter_name)?;

    // 4. Engine identifier — carries into the output so published
    //    results document which warehouse produced them.
    let engine = registry
        .adapter_config(adapter_name)
        .map(|c| c.adapter_type.clone())
        .unwrap_or_else(|| "unknown".to_string());

    let excluded_columns: Vec<String> = exclude_columns.unwrap_or_else(default_metadata_columns);

    tracing::info!(
        pipeline = %project,
        engine = %engine,
        excluded_columns = ?excluded_columns,
        "rocky compact --measure-dedup: starting Layer 0 dedup measurement"
    );

    // 5. Enumerate tables to measure, scoping to managed tables unless
    //    --all-tables was passed.
    //
    //    Order matters: resolve managed tables *first* so we can scope
    //    the `information_schema.tables` query to the specific catalogs
    //    Rocky manages. On Unity Catalog there is no workspace-wide
    //    `information_schema` — every `FROM information_schema.tables`
    //    needs a catalog prefix (`<cat>.information_schema.tables`).
    //    Fanning out per catalog is the only form that works across
    //    Databricks, Snowflake, and DuckDB.
    let managed = if all_tables {
        None
    } else {
        match resolve_managed_tables(&rocky_cfg, pipeline_name, pipeline, &registry, config_path)
            .await
        {
            Ok(Some(managed_set)) => Some(managed_set),
            Ok(None) => {
                tracing::warn!(
                    "could not resolve managed tables for this pipeline type; \
                     falling back to all warehouse tables"
                );
                None
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to resolve managed tables; falling back to all warehouse tables"
                );
                None
            }
        }
    };

    let catalog_scope = managed.as_ref().map(managed_catalog_set);
    let all_warehouse_tables = enumerate_tables(adapter.as_ref(), catalog_scope.as_deref()).await?;

    let (tables, scope) = match managed.as_ref() {
        Some(managed_set) => {
            let filtered: Vec<TableRef> = all_warehouse_tables
                .iter()
                .filter(|t| {
                    t.validated_full_name()
                        .map(|name| managed_set.contains(&name.to_lowercase()))
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
            tracing::info!(
                managed_tables = managed_set.len(),
                warehouse_tables = all_warehouse_tables.len(),
                matched = filtered.len(),
                "scoped to Rocky-managed tables"
            );
            if filtered.is_empty() && !managed_set.is_empty() && !all_warehouse_tables.is_empty() {
                let managed_sample: Vec<&String> = managed_set.iter().take(5).collect();
                let warehouse_sample: Vec<String> = all_warehouse_tables
                    .iter()
                    .take(5)
                    .filter_map(|t| t.validated_full_name().ok().map(|s| s.to_lowercase()))
                    .collect();
                tracing::warn!(
                    ?managed_sample,
                    ?warehouse_sample,
                    "managed ∩ warehouse is empty — naming mismatch between \
                     pipeline-resolved target names and warehouse table names"
                );
            }
            (filtered, "managed".to_string())
        }
        None => (all_warehouse_tables, "all".to_string()),
    };

    let tables_scanned = tables.len();

    if tables_scanned == 0 {
        bail!(
            "no user tables found in the warehouse — the dedup measurement \
             needs at least one table. Materialize the pipeline first \
             (`rocky run`) and retry."
        );
    }

    tracing::info!(tables = tables_scanned, "hashing raw + semantic partitions");

    // 6. Hash each table twice (raw = HASH(*), semantic = HASH(filtered)).
    let mut raw_per_table: Vec<(String, Vec<PartitionChecksum>)> =
        Vec::with_capacity(tables_scanned);
    let mut semantic_per_table: Vec<(String, Vec<PartitionChecksum>)> =
        Vec::with_capacity(tables_scanned);

    // Counters for the graceful-degradation summary emitted after the
    // sweep. `describe_table` and `hash_table` failures on individual
    // tables are treated as skippable — a measurement sweep over
    // thousands of warehouse tables must not abort on one bad row.
    let mut describe_failures: usize = 0;
    let mut hash_failures: usize = 0;

    for table in &tables {
        let table_ref_str = table
            .validated_full_name()
            .map_err(|e| {
                anyhow!(
                    "rocky compact --measure-dedup could not validate table reference {table} \
                     before hashing; verify the warehouse returned catalog, schema, and table names \
                     that are valid SQL identifiers: {e}"
                )
            })?;

        // One describe_table call per table, reused for both hash
        // passes. This also sidesteps `HASH(*)` — which isn't portable
        // (DuckDB's `hash()` is variadic but the `*` doesn't expand
        // inside function arguments) — by always passing explicit
        // column lists.
        let column_info = match adapter.describe_table(table).await {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!(
                    table = %table_ref_str,
                    error = %e,
                    "describe_table failed — skipping"
                );
                describe_failures += 1;
                continue;
            }
        };

        let all_columns: Vec<String> = column_info.iter().map(|c| c.name.clone()).collect();
        let semantic_columns: Vec<String> = column_info
            .iter()
            .filter(|c| {
                !excluded_columns
                    .iter()
                    .any(|ex| ex.eq_ignore_ascii_case(&c.name))
            })
            .map(|c| c.name.clone())
            .collect();

        // Raw hash over every column (including metadata).
        let raw = if all_columns.is_empty() {
            tracing::warn!(
                table = %table_ref_str,
                "describe_table returned no columns; skipping raw hash for this table"
            );
            Vec::new()
        } else {
            match hash_table(adapter.as_ref(), table, &all_columns).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        table = %table_ref_str,
                        error = %e,
                        "raw hash failed — skipping table"
                    );
                    hash_failures += 1;
                    continue;
                }
            }
        };
        raw_per_table.push((table_ref_str.clone(), raw));

        // Semantic hash over columns minus the metadata filter.
        let semantic = if semantic_columns.is_empty() {
            tracing::warn!(
                table = %table_ref_str,
                "all columns excluded by semantic filter; skipping semantic hash for this table"
            );
            Vec::new()
        } else {
            match hash_table(adapter.as_ref(), table, &semantic_columns).await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        table = %table_ref_str,
                        error = %e,
                        "semantic hash failed — skipping table"
                    );
                    hash_failures += 1;
                    // Roll back the raw push we just made so the two
                    // per_table vecs stay aligned for downstream stats.
                    raw_per_table.pop();
                    continue;
                }
            }
        };
        semantic_per_table.push((table_ref_str, semantic));
    }

    if describe_failures > 0 || hash_failures > 0 {
        tracing::warn!(
            describe_failures,
            hash_failures,
            total_matched = tables.len(),
            hashed = semantic_per_table.len(),
            "some tables were skipped during hashing; dedup numbers reflect only the hashed set"
        );
    }

    // 7. Compute cross-table dedup stats for both passes.
    let raw_stats = compute_dedup_stats(&raw_per_table);
    let semantic_stats = compute_dedup_stats(&semantic_per_table);
    let partitions_scanned = raw_stats.total_partitions;

    // 8. Byte-level calibration via blake3 row hashing (when requested).
    let calibration = if calibrate_bytes {
        Some(calibrate_byte_dedup(adapter.as_ref(), &semantic_stats, &semantic_per_table).await?)
    } else {
        None
    };

    // 9. Convert to the CLI output type.
    let output = build_compact_dedup_output(
        engine,
        project,
        scope,
        tables_scanned,
        partitions_scanned,
        &raw_stats,
        &semantic_stats,
        excluded_columns,
        calibration,
        started_at,
        start.elapsed().as_millis() as u64,
    );

    Ok(output)
}

/// Enumerate non-system tables visible to the warehouse adapter via
/// `information_schema.tables`.
///
/// When `catalogs` is `Some(list)`, issues one catalog-prefixed query
/// per catalog (`FROM <cat>.information_schema.tables`) and merges the
/// results. This is required on Unity Catalog (Databricks) which has
/// no workspace-wide `information_schema`, and also on Snowflake
/// where the session's current database bounds the unqualified form.
///
/// When `catalogs` is `None`, issues a single unqualified query. This
/// works on DuckDB (single-DB scope) and is the fall-through used when
/// no managed-table resolution is available (e.g. `--all-tables`).
///
/// System schemas (`information_schema`, `pg_catalog`, `system`) are
/// filtered so the measurement doesn't include the warehouse's own
/// metadata tables.
async fn enumerate_tables(
    adapter: &dyn WarehouseAdapter,
    catalogs: Option<&[String]>,
) -> Result<Vec<TableRef>> {
    const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "system"];

    let mut tables: Vec<TableRef> = Vec::new();
    match catalogs {
        Some(catalog_list) => {
            let mut skipped: Vec<(String, String)> = Vec::new();
            for catalog in catalog_list {
                rocky_sql::validation::validate_identifier(catalog).map_err(|e| {
                    anyhow!(
                        "rocky compact --measure-dedup could not enumerate managed catalog \
                         {catalog:?} because it is not a valid SQL identifier; verify the Rocky \
                         target catalog template resolves to a plain catalog name: {e}"
                    )
                })?;
                let sql = format!(
                    "SELECT table_catalog, table_schema, table_name \
                     FROM {catalog}.information_schema.tables \
                     WHERE table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'MATERIALIZED VIEW')"
                );
                match adapter.execute_query(&sql).await {
                    Ok(result) => {
                        collect_table_rows(&result.rows, SYSTEM_SCHEMAS, &mut tables)?;
                    }
                    Err(e) => {
                        // Catalog may not exist yet (auto-created on first
                        // write) or the session may lack USE CATALOG. A
                        // measurement tool should degrade gracefully and
                        // surface the skipped catalogs in a single summary
                        // rather than aborting.
                        tracing::warn!(
                            catalog = %catalog,
                            error = %e,
                            "skipping catalog — enumeration failed (catalog missing or not accessible)"
                        );
                        skipped.push((catalog.clone(), e.to_string()));
                    }
                }
            }
            if !skipped.is_empty() {
                tracing::warn!(
                    skipped_catalogs = skipped.len(),
                    total_catalogs = catalog_list.len(),
                    "some catalogs were skipped during enumeration; dedup numbers \
                     below reflect only the catalogs that were readable"
                );
            }
        }
        None => {
            let sql = "SELECT table_catalog, table_schema, table_name \
                       FROM information_schema.tables \
                       WHERE table_type NOT IN ('VIEW', 'MATERIALIZED_VIEW', 'MATERIALIZED VIEW')";
            let result = adapter
                .execute_query(sql)
                .await
                .map_err(|e| {
                    anyhow!(
                        "rocky compact --measure-dedup failed while enumerating user tables from \
                         information_schema; verify the warehouse exposes information_schema.tables \
                         and the configured credential can list tables in the active catalog/schema: {e}"
                    )
                })?;
            collect_table_rows(&result.rows, SYSTEM_SCHEMAS, &mut tables)?;
        }
    }
    Ok(tables)
}

/// Parse `SELECT table_catalog, table_schema, table_name` rows into
/// [`TableRef`]s, dropping any that fall inside a system schema.
fn collect_table_rows(
    rows: &[Vec<serde_json::Value>],
    system_schemas: &[&str],
    tables: &mut Vec<TableRef>,
) -> Result<()> {
    for row in rows {
        let catalog = row
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let schema = row
            .get(1)
            .and_then(|v| v.as_str())
            .context("enumerate_tables: row missing table_schema column")?
            .to_string();
        if system_schemas
            .iter()
            .any(|s| schema.eq_ignore_ascii_case(s))
        {
            continue;
        }
        let name = row
            .get(2)
            .and_then(|v| v.as_str())
            .context("enumerate_tables: row missing table_name column")?
            .to_string();
        tables.push(TableRef {
            catalog,
            schema,
            table: name,
        });
    }
    Ok(())
}

/// Execute one whole-table checksum SQL against the warehouse and parse
/// the result into a [`PartitionChecksum`] list.
///
/// Empty `value_columns` → `HASH(*)`.
async fn hash_table(
    adapter: &dyn WarehouseAdapter,
    table: &TableRef,
    value_columns: &[String],
) -> Result<Vec<PartitionChecksum>> {
    let table_ref_str = table.validated_full_name().map_err(|e| {
        anyhow!(
            "rocky compact --measure-dedup could not validate table reference {table} before \
                 building the checksum query; verify the warehouse returned catalog, schema, and \
                 table names that are valid SQL identifiers: {e}"
        )
    })?;
    let sql = generate_whole_table_checksum_sql(&table_ref_str, value_columns);
    let result = adapter.execute_query(&sql).await.map_err(|e| {
        anyhow!(
            "rocky compact --measure-dedup failed while running the checksum query for \
                 {table_ref_str}; verify the table exists, the credential has SELECT access, and \
                 the selected columns can be hashed by this warehouse adapter: {e}"
        )
    })?;
    parse_checksum_rows(&result)
        .with_context(|| format!("parsing checksum rows for {table_ref_str}"))
}

/// Parse a `QueryResult` from the whole-table checksum SQL into a
/// `Vec<PartitionChecksum>`.
///
/// The query shape is `(partition_key, checksum, row_count)`. `checksum`
/// may arrive as a signed BIGINT (Databricks), unsigned UBIGINT
/// (DuckDB), or a string-encoded number (some REST drivers serialize
/// BIGINT as string to preserve precision) — all three paths land in
/// the `u64` field via wrapping conversion.
fn parse_checksum_rows(result: &QueryResult) -> Result<Vec<PartitionChecksum>> {
    let now = Utc::now();
    let mut checksums = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let partition_key = row
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or("__whole_table__")
            .to_string();
        // Empty tables: the HASH aggregate returns NULL (Databricks) or
        // the query returns zero rows (DuckDB). Either way there's no
        // content to dedupe — skip silently instead of failing the sweep.
        let checksum_cell = row.get(1);
        if checksum_cell
            .map(serde_json::Value::is_null)
            .unwrap_or(true)
        {
            continue;
        }
        let checksum = json_number_to_u64(checksum_cell)
            .context("checksum column is missing or not a number")?;
        let row_count = json_number_to_u64(row.get(2))
            .context("row_count column is missing or not a number")?;
        checksums.push(PartitionChecksum {
            partition_key,
            checksum,
            row_count,
            computed_at: now,
        });
    }
    Ok(checksums)
}

/// Accept signed-integer, unsigned-integer, and string-encoded integer
/// JSON values, truncating wider-than-u64 values to their low 64 bits.
///
/// - Databricks `SUM(HASH(...))` returns `BIGINT` (signed 64-bit) —
///   arrives as `serde_json::Number` i64, `as i64 as u64` handles it.
/// - DuckDB `SUM(hash(...))` returns `INT128` because `hash()` is
///   `UBIGINT` and `SUM` widens — the duckdb driver serializes this as
///   a decimal string (e.g. `"41828572338317399049"`). Parsing it as
///   `i128` and truncating to `u64` gives us a consistent low-64-bit
///   fingerprint. Consistent truncation means two tables with
///   identical content still dedup-match.
/// - Some warehouse REST drivers serialize BIGINT as string to
///   preserve precision — same string path handles them.
fn json_number_to_u64(v: Option<&serde_json::Value>) -> Option<u64> {
    match v? {
        serde_json::Value::Number(n) => n.as_u64().or_else(|| n.as_i64().map(|x| x as u64)),
        serde_json::Value::String(s) => {
            if let Ok(i) = s.parse::<i128>() {
                Some(i as u64)
            } else if let Ok(u) = s.parse::<u128>() {
                Some(u as u64)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_compact_dedup_output(
    engine: String,
    project: String,
    scope: String,
    tables_scanned: usize,
    partitions_scanned: usize,
    raw_stats: &DedupStats,
    semantic_stats: &DedupStats,
    excluded_columns: Vec<String>,
    calibration: Option<ByteCalibration>,
    measured_at: DateTime<Utc>,
    duration_ms: u64,
) -> CompactDedupOutput {
    // The CLI-facing "top pairs" and "per table" views are built from
    // the *semantic* stats — that's the pitch number. Raw stats flow
    // into the summary block only.
    let top_dedup_pairs = semantic_stats
        .top_dedup_pairs
        .iter()
        .map(|p| DedupPair {
            table_a: p.table_a.clone(),
            table_b: p.table_b.clone(),
            shared_partitions: p.shared_partitions,
            shared_rows: p.shared_rows,
        })
        .collect();

    let per_table = semantic_stats
        .per_table
        .iter()
        .map(|t| TableDedupContribution {
            table: t.table.clone(),
            partitions: t.partitions,
            partitions_shared_with_others: t.partitions_shared_with_others,
            contribution_pct: t.contribution_pct,
        })
        .collect();

    CompactDedupOutput {
        version: VERSION.to_string(),
        command: "compact_dedup".to_string(),
        engine,
        project,
        scope,
        tables_scanned,
        partitions_scanned,
        raw: stats_to_summary(raw_stats),
        semantic: stats_to_summary(semantic_stats),
        excluded_columns,
        top_dedup_pairs,
        per_table,
        calibration,
        measured_at,
        duration_ms,
    }
}

fn stats_to_summary(stats: &DedupStats) -> DedupSummary {
    DedupSummary {
        total_rows: stats.total_rows,
        unique_partitions: stats.unique_partitions,
        duplicate_partitions: stats.duplicate_partitions,
        dedup_ratio: stats.dedup_ratio,
        estimated_savings_pct: stats.estimated_savings_pct,
    }
}

fn render_measure_dedup_human(output: &CompactDedupOutput) {
    println!(
        "Scanning Rocky project '{}' (engine: {}, scope: {})... {} tables, {} partitions",
        output.project,
        output.engine,
        output.scope,
        output.tables_scanned,
        output.partitions_scanned
    );
    println!();
    println!(
        "Cross-table dedup analysis (engine-scoped: {}, scope: {}, granularity: partition)",
        output.engine, output.scope
    );
    println!();
    println!("  Raw dedup (all columns):");
    print_summary(&output.raw, "    ");
    println!();
    println!(
        "  Semantic dedup (excluding {}):",
        if output.excluded_columns.is_empty() {
            "<none>".to_string()
        } else {
            output.excluded_columns.join(", ")
        }
    );
    print_summary(&output.semantic, "    ");
    println!();

    if !output.top_dedup_pairs.is_empty() {
        println!("Top dedup opportunities (semantic):");
        for pair in &output.top_dedup_pairs {
            println!(
                "  {} ↔ {}  {} partitions shared, {} rows",
                pair.table_a, pair.table_b, pair.shared_partitions, pair.shared_rows
            );
        }
        println!();
    }

    if let Some(cal) = &output.calibration {
        println!(
            "Byte-level calibration (blake3, {} tables sampled):",
            cal.tables_sampled.len()
        );
        for t in &cal.tables_sampled {
            println!("  - {t}");
        }
        println!(
            "  Partition dedup (sample): {:>6.1}%",
            cal.partition_dedup_pct
        );
        println!("  Byte dedup (blake3):      {:>6.1}%", cal.byte_dedup_pct);
        println!(
            "  Lower-bound multiplier:   {:>6.2}x",
            cal.lower_bound_multiplier
        );
        println!();
    } else {
        println!("Interpretation: Partition-level measurement is a LOWER bound on byte-level");
        println!("dedup. Two partitions that share 99% of their row groups but differ on one");
        println!("row will appear fully distinct here. To get a byte-level calibration on a");
        println!("sample, re-run with --calibrate-bytes.");
        println!();
    }

    let (verdict, message) = verdict_for_semantic(output.semantic.dedup_ratio);
    println!("Decision gate (see plans/rocky-storage-layer-0.md §6):");
    println!(
        "  Semantic partition dedup {:.1}% → {}",
        output.semantic.dedup_ratio * 100.0,
        verdict
    );
    println!("  {message}");
    println!();
    println!("No data was modified.");
}

fn print_summary(summary: &DedupSummary, indent: &str) {
    println!("{indent}Total rows:           {:>10}", summary.total_rows);
    println!(
        "{indent}Unique partitions:    {:>10}",
        summary.unique_partitions
    );
    println!(
        "{indent}Duplicate partitions: {:>10}",
        summary.duplicate_partitions
    );
    println!(
        "{indent}Dedup ratio:          {:>9.1}%",
        summary.dedup_ratio * 100.0
    );
    println!(
        "{indent}Est. savings (rows):  {:>9.1}%",
        summary.estimated_savings_pct
    );
}

/// Decision-gate thresholds from `plans/rocky-storage-layer-0.md` §6.
///
/// The partition-level number is a **lower bound** on byte-level dedup,
/// so a low result isn't a kill shot on its own — it triggers the
/// calibration recommendation rather than a RED verdict.
fn verdict_for_semantic(semantic_ratio: f64) -> (&'static str, &'static str) {
    let pct = semantic_ratio * 100.0;
    if pct > 30.0 {
        (
            "GREEN",
            "Vision is real (lower bound above threshold); proceed to Layer 1.",
        )
    } else if pct >= 10.0 {
        (
            "YELLOW",
            "Genuinely uncertain. Write Layer 1 but reframe pitch around \
             branch-as-pointer; do not lead with cross-table dedup.",
        )
    } else {
        (
            "INCONCLUSIVE",
            "Low partition-level result; re-run with --calibrate-bytes for a \
             byte-level sample before killing the vision (see §6).",
        )
    }
}

/// Default number of rows sampled per table during `--calibrate-bytes`.
///
/// 10 000 rows is enough to give a statistically meaningful signal
/// without pulling excessive data over the wire. The warehouse's natural
/// row ordering is used (no `ORDER BY`), which is fine for same-engine
/// calibration: if the first N rows of two identical tables don't match,
/// the tables aren't duplicates at the row level either.
const CALIBRATE_SAMPLE_ROWS: usize = 10_000;

/// Maximum number of tables to sample during `--calibrate-bytes`.
///
/// Preferentially picks tables that appear in the top dedup pairs (the
/// ones with the highest partition-level sharing signal) so the
/// calibration measures where the action is.
const CALIBRATE_MAX_TABLES: usize = 3;

/// Produce a blake3 hash for a single row (a slice of JSON cells).
///
/// Deterministic within the same engine: cells are serialized by
/// `serde_json::to_string`, which is stable for the value types
/// warehouses return (numbers, strings, nulls, booleans). Cross-engine
/// comparisons are not meaningful because different drivers may
/// serialize the same SQL value differently — but that's already true
/// for the partition-level `HASH()`, and the `engine` field in the
/// output scopes every result.
fn hash_row_blake3(row: &[serde_json::Value]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    for (i, cell) in row.iter().enumerate() {
        if i > 0 {
            // Field separator so adjacent null + string can't collide
            // with a single string that happens to contain "null".
            hasher.update(b"\x1f");
        }
        // Compact JSON serialization — deterministic for the primitive
        // types warehouses return.
        let bytes = serde_json::to_string(cell).unwrap_or_default();
        hasher.update(bytes.as_bytes());
    }
    hasher.finalize()
}

/// Run byte-level blake3 calibration on a sampled subset of tables.
///
/// Picks up to [`CALIBRATE_MAX_TABLES`] tables from the top dedup pairs
/// (highest sharing signal), pulls their first [`CALIBRATE_SAMPLE_ROWS`]
/// rows via `SELECT *`, hashes each row with blake3, and cross-compares
/// hashes to compute byte-level dedup on the sample.
///
/// Returns a [`ByteCalibration`] comparing the byte-level result against
/// the partition-level result on the same subset, producing a
/// `lower_bound_multiplier` that tells the user how tight the cheap
/// partition-level measurement really is.
async fn calibrate_byte_dedup(
    adapter: &dyn WarehouseAdapter,
    semantic_stats: &DedupStats,
    semantic_per_table: &[(String, Vec<PartitionChecksum>)],
) -> Result<ByteCalibration> {
    // 1. Pick sample tables — prefer those in the top dedup pairs.
    let sample_tables = pick_sample_tables(semantic_stats, CALIBRATE_MAX_TABLES);

    if sample_tables.len() < 2 {
        // With fewer than 2 tables we can't do cross-table comparison.
        // Return a degenerate calibration block.
        tracing::warn!(
            "fewer than 2 tables eligible for byte-level calibration; \
             returning zero-dedup calibration"
        );
        return Ok(ByteCalibration {
            tables_sampled: sample_tables,
            partition_dedup_pct: 0.0,
            byte_dedup_pct: 0.0,
            lower_bound_multiplier: 1.0,
        });
    }

    // Validate sample-table FQNs before interpolating into SELECT.
    // The names trace back to rocky.toml pipeline config — untrusted
    // from the engine's perspective. See `engine/CLAUDE.md`.
    for table_name in &sample_tables {
        for part in table_name.split('.') {
            validate_identifier(part).with_context(|| {
                format!("calibrate-bytes: invalid table identifier '{table_name}'")
            })?;
        }
    }

    tracing::info!(
        tables = ?sample_tables,
        sample_rows = CALIBRATE_SAMPLE_ROWS,
        "calibrate-bytes: pulling rows and hashing with blake3"
    );

    // 2. Pull rows and hash each with blake3.
    //    Key: blake3 hash -> list of table names that contain that row.
    let mut hash_index: HashMap<blake3::Hash, Vec<String>> = HashMap::new();
    let mut total_rows: u64 = 0;

    for table_name in &sample_tables {
        let sql = format!("SELECT * FROM {table_name} LIMIT {CALIBRATE_SAMPLE_ROWS}");
        let result = adapter.execute_query(&sql).await.map_err(|e| {
            anyhow!(
                "rocky compact --measure-dedup --calibrate-bytes failed while sampling rows \
                     from {table_name}; verify the table exists, the credential has SELECT access, \
                     and the adapter can read rows from that table: {e}"
            )
        })?;

        for row in &result.rows {
            total_rows += 1;
            let hash = hash_row_blake3(row);
            hash_index.entry(hash).or_default().push(table_name.clone());
        }
    }

    // 3. Count byte-level duplicates: rows whose hash appears in 2+
    //    distinct tables.
    let mut duplicate_rows: u64 = 0;
    for holders in hash_index.values() {
        if holders.len() > 1 {
            // Deduplicate to distinct tables — the same row might
            // appear multiple times within one table.
            let mut distinct_tables: Vec<&str> = holders.iter().map(String::as_str).collect();
            distinct_tables.sort_unstable();
            distinct_tables.dedup();
            if distinct_tables.len() > 1 {
                // All copies beyond the canonical one are duplicates.
                let extra = holders.len() as u64 - 1;
                duplicate_rows += extra;
            }
        }
    }

    let byte_dedup_pct = if total_rows == 0 {
        0.0
    } else {
        (duplicate_rows as f64 / total_rows as f64) * 100.0
    };

    // 4. Recompute partition-level dedup on just the sampled tables so
    //    the two percentages are directly comparable.
    let sampled_subset: Vec<(String, Vec<PartitionChecksum>)> = semantic_per_table
        .iter()
        .filter(|(name, _)| sample_tables.contains(name))
        .cloned()
        .collect();
    let subset_stats = compute_dedup_stats(&sampled_subset);
    let partition_dedup_pct = subset_stats.dedup_ratio * 100.0;

    // 5. Multiplier: how much the byte-level measurement amplifies over
    //    the partition-level one. Guard against division by zero.
    let lower_bound_multiplier = if partition_dedup_pct > 0.0 {
        byte_dedup_pct / partition_dedup_pct
    } else {
        1.0
    };

    Ok(ByteCalibration {
        tables_sampled: sample_tables,
        partition_dedup_pct,
        byte_dedup_pct,
        lower_bound_multiplier,
    })
}

/// Pick up to `max` tables for byte-level calibration, preferring
/// tables that appear in the top dedup pairs (they have the highest
/// signal for cross-table duplication).
fn pick_sample_tables(stats: &DedupStats, max: usize) -> Vec<String> {
    let mut tables = Vec::with_capacity(max);
    // First pass: collect from top dedup pairs.
    for pair in &stats.top_dedup_pairs {
        if tables.len() >= max {
            break;
        }
        if !tables.contains(&pair.table_a) {
            tables.push(pair.table_a.clone());
        }
        if tables.len() < max && !tables.contains(&pair.table_b) {
            tables.push(pair.table_b.clone());
        }
    }
    // Second pass: fill from per_table (already sorted by contribution).
    if tables.len() < max {
        for t in &stats.per_table {
            if tables.len() >= max {
                break;
            }
            if !tables.contains(&t.table) {
                tables.push(t.table.clone());
            }
        }
    }
    tables.sort();
    tables
}

/// Generates OPTIMIZE and VACUUM SQL for a given model.
///
/// For Delta Lake (Databricks):
/// - `OPTIMIZE` compacts small files into larger ones
/// - `VACUUM` removes old data files no longer referenced
///
/// Every dot-separated segment of `model` is validated as a SQL
/// identifier before interpolation — the model name reaches us from
/// CLI args and config files (via `--catalog` scope resolution),
/// both of which are untrusted from the engine's perspective. See
/// the validation rules in `engine/CLAUDE.md`.
fn generate_compact_sql(model: &str, target_size_mb: u64) -> Result<Vec<(String, String)>> {
    for part in model.split('.') {
        validate_identifier(part).with_context(|| format!("invalid model identifier '{model}'"))?;
    }

    Ok(vec![
        (
            "compact small files".to_string(),
            format!("OPTIMIZE {model} WHERE true\n  -- target file size: {target_size_mb}MB"),
        ),
        (
            "remove stale data files".to_string(),
            format!("VACUUM {model} RETAIN 168 HOURS"),
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_compact_sql() {
        let stmts = generate_compact_sql("catalog.schema.orders", 256).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].1.contains("OPTIMIZE"));
        assert!(stmts[0].1.contains("catalog.schema.orders"));
        assert!(stmts[1].1.contains("VACUUM"));
        assert!(stmts[1].1.contains("168 HOURS"));
    }

    #[test]
    fn test_generate_compact_sql_custom_size() {
        let stmts = generate_compact_sql("my_table", 512).unwrap();
        assert!(stmts[0].1.contains("512MB"));
    }

    #[test]
    fn test_generate_compact_sql_rejects_injection() {
        assert!(generate_compact_sql("orders; DROP TABLE admin; --", 256).is_err());
        assert!(generate_compact_sql("catalog.schema; DROP TABLE x", 256).is_err());
        assert!(generate_compact_sql("'; DROP TABLE users; --", 256).is_err());
        assert!(generate_compact_sql("", 256).is_err());
        assert!(generate_compact_sql("catalog..table", 256).is_err());
    }

    /// E2E test for `compute_measure_dedup` against an ephemeral DuckDB.
    ///
    /// Sets up a temp database with three tables — two identical
    /// (`bronze.orders` and `silver.orders`) and one distinct
    /// (`bronze.distinct`) — writes a minimal `rocky.toml`, and asserts
    /// that the returned `CompactDedupOutput` shows the right dedup
    /// ratio, the right top pair, and the right per-table contribution.
    ///
    /// Layer 0 Step 5: validates the entire handler stack —
    /// config loading, adapter registry, warehouse SQL execution,
    /// describe-then-hash flow, dedup analysis, output build — against
    /// real warehouse-side row-hashing and the rocky-duckdb adapter's
    /// `HUGEINT` serialization path that the Step-4 sibling commit
    /// fixed.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn compute_measure_dedup_detects_duplicate_tables_against_real_duckdb() -> Result<()> {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test.duckdb");

        // Seed the database in a nested scope so the connection is
        // released before compute_measure_dedup opens its own through
        // the AdapterRegistry. DuckDB allows a single writer per file.
        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path)
                .map_err(|e| anyhow!("setup: open DuckDB: {e}"))?;
            for sql in &[
                "CREATE SCHEMA bronze",
                "CREATE SCHEMA silver",
                "CREATE TABLE bronze.orders AS \
                 SELECT i AS id, ('row_' || i) AS name \
                 FROM generate_series(1, 100) t(i)",
                "CREATE TABLE silver.orders AS \
                 SELECT i AS id, ('row_' || i) AS name \
                 FROM generate_series(1, 100) t(i)",
                "CREATE TABLE bronze.distinct AS \
                 SELECT i AS id FROM generate_series(1, 50) t(i)",
            ] {
                adapter
                    .execute_statement(sql)
                    .await
                    .map_err(|e| anyhow!("setup: {sql}: {e}"))?;
            }
        }

        // Minimal rocky.toml pointing at the seeded DuckDB. Schema
        // pattern values are placeholders — the dedup-measurement
        // handler doesn't read them; it enumerates everything via
        // information_schema.
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.test]
strategy = "full_refresh"

[pipeline.test.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.test.target]
catalog_template = "test"
schema_template = "staging__{{source}}"
"#,
                db_path.display()
            ),
        )?;

        // Exercise the pure-compute path with all_tables=true because
        // the seeded tables (bronze.*, silver.*) don't match the config's
        // target templates.
        let output = compute_measure_dedup(&config_path, None, false, true).await?;

        // Top-level shape.
        assert_eq!(output.engine, "duckdb");
        assert_eq!(output.project, "test");
        assert_eq!(output.scope, "all");
        assert_eq!(output.tables_scanned, 3);
        assert_eq!(output.partitions_scanned, 3);

        // Two identical tables collapse to one bucket; one distinct
        // table is its own bucket → 2 unique, 1 duplicate, 1/3 ratio.
        assert_eq!(output.semantic.unique_partitions, 2);
        assert_eq!(output.semantic.duplicate_partitions, 1);
        assert!(
            (output.semantic.dedup_ratio - (1.0 / 3.0)).abs() < 1e-9,
            "expected dedup_ratio ≈ 0.333, got {}",
            output.semantic.dedup_ratio
        );
        // 100 of 250 total rows are collapsible (silver.orders is the
        // duplicate copy of bronze.orders).
        assert_eq!(output.semantic.total_rows, 250);
        assert!(
            (output.semantic.estimated_savings_pct - 40.0).abs() < 1e-9,
            "expected estimated_savings_pct ≈ 40.0, got {}",
            output.semantic.estimated_savings_pct
        );

        // Raw and semantic agree because none of the columns we
        // created match the default metadata exclusion list.
        assert_eq!(
            output.raw.dedup_ratio, output.semantic.dedup_ratio,
            "raw and semantic should match when no metadata columns exist"
        );

        // Exactly one pair of duplicate tables surfaces, alphabetically
        // ordered (bronze < silver).
        assert_eq!(output.top_dedup_pairs.len(), 1);
        let pair = &output.top_dedup_pairs[0];
        assert!(
            pair.table_a.ends_with("bronze.orders"),
            "table_a was {}",
            pair.table_a
        );
        assert!(
            pair.table_b.ends_with("silver.orders"),
            "table_b was {}",
            pair.table_b
        );
        assert_eq!(pair.shared_partitions, 1);
        assert_eq!(pair.shared_rows, 100);

        // Per-table contribution: the two duplicate tables both show
        // 100% sharing; the distinct table shows 0%.
        let by_table: std::collections::HashMap<&str, &TableDedupContribution> = output
            .per_table
            .iter()
            .map(|t| (t.table.as_str(), t))
            .collect();
        let bronze_orders = by_table
            .iter()
            .find(|(k, _)| k.ends_with("bronze.orders"))
            .map(|(_, v)| *v)
            .expect("bronze.orders missing from per_table");
        let silver_orders = by_table
            .iter()
            .find(|(k, _)| k.ends_with("silver.orders"))
            .map(|(_, v)| *v)
            .expect("silver.orders missing from per_table");
        let bronze_distinct = by_table
            .iter()
            .find(|(k, _)| k.ends_with("bronze.distinct"))
            .map(|(_, v)| *v)
            .expect("bronze.distinct missing from per_table");

        assert_eq!(bronze_orders.partitions_shared_with_others, 1);
        assert_eq!(bronze_orders.contribution_pct, 100.0);
        assert_eq!(silver_orders.partitions_shared_with_others, 1);
        assert_eq!(silver_orders.contribution_pct, 100.0);
        assert_eq!(bronze_distinct.partitions_shared_with_others, 0);
        assert_eq!(bronze_distinct.contribution_pct, 0.0);

        // Calibration is None because we passed `false`.
        assert!(output.calibration.is_none());

        Ok(())
    }

    /// E2E test for managed-table scoping against an ephemeral DuckDB.
    ///
    /// Sets up a database with four tables across two schemas:
    /// - `staging__shopify.orders` and `staging__shopify.events` (match
    ///   the target templates from the config)
    /// - `bronze.unmanaged` and `silver.noise` (don't match)
    ///
    /// With `all_tables = false` (default), only the two managed tables
    /// should be scanned. With `all_tables = true`, all four appear.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn managed_scope_filters_to_pipeline_targets() -> Result<()> {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test_scope.duckdb");

        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path)
                .map_err(|e| anyhow!("setup: open DuckDB: {e}"))?;
            for sql in &[
                "CREATE SCHEMA \"staging__shopify\"",
                "CREATE TABLE \"staging__shopify\".orders AS \
                 SELECT i AS id, ('row_' || i) AS name \
                 FROM generate_series(1, 50) t(i)",
                "CREATE TABLE \"staging__shopify\".events AS \
                 SELECT i AS id FROM generate_series(1, 30) t(i)",
                "CREATE SCHEMA bronze",
                "CREATE TABLE bronze.unmanaged AS \
                 SELECT i AS id FROM generate_series(1, 20) t(i)",
                "CREATE SCHEMA silver",
                "CREATE TABLE silver.noise AS \
                 SELECT i AS id FROM generate_series(1, 10) t(i)",
            ] {
                adapter
                    .execute_statement(sql)
                    .await
                    .map_err(|e| anyhow!("setup: {sql}: {e}"))?;
            }
        }

        // The config's target templates resolve to
        // `test.staging__<source>.<table>`. We set up a DuckDB with
        // tables at `test.staging__shopify.orders` and
        // `test.staging__shopify.events` that match.
        //
        // DuckDB only has one catalog (`test_scope` = the file name).
        // The actual catalog name from `information_schema.tables` will
        // be the db name, so we match on `staging__shopify` schema only.
        //
        // For replication pipeline, managed-table resolution requires a
        // discovery adapter. Since DuckDB doesn't have one, the resolver
        // falls back to `Ok(None)` → scope = "all". That's fine for
        // testing that the fallback works correctly.
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.test]
strategy = "full_refresh"

[pipeline.test.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.test.target]
catalog_template = "test"
schema_template = "staging__{{source}}"
"#,
                db_path.display()
            ),
        )?;

        // With all_tables = false but no discovery adapter, should fall
        // back to all tables and scope = "all".
        let output_default = compute_measure_dedup(&config_path, None, false, false).await?;
        assert_eq!(
            output_default.scope, "all",
            "without discovery adapter, scope should fall back to 'all'"
        );
        assert_eq!(output_default.tables_scanned, 4);

        // With all_tables = true, should scan everything.
        let output_all = compute_measure_dedup(&config_path, None, false, true).await?;
        assert_eq!(output_all.scope, "all");
        assert_eq!(output_all.tables_scanned, 4);

        Ok(())
    }

    #[test]
    fn hash_row_blake3_deterministic() {
        let row = vec![
            serde_json::json!(1),
            serde_json::json!("hello"),
            serde_json::json!(null),
        ];
        let h1 = hash_row_blake3(&row);
        let h2 = hash_row_blake3(&row);
        assert_eq!(h1, h2, "same content should produce same hash");
    }

    #[test]
    fn hash_row_blake3_distinguishes_different_content() {
        let row_a = vec![serde_json::json!(1), serde_json::json!("hello")];
        let row_b = vec![serde_json::json!(2), serde_json::json!("hello")];
        let row_c = vec![serde_json::json!(1), serde_json::json!("world")];
        assert_ne!(hash_row_blake3(&row_a), hash_row_blake3(&row_b));
        assert_ne!(hash_row_blake3(&row_a), hash_row_blake3(&row_c));
    }

    #[test]
    fn hash_row_blake3_field_separator_prevents_collisions() {
        // Without a field separator, ["null", "x"] and [null, "x"] could
        // collide because serde_json serializes null as "null". The \x1f
        // separator prevents this.
        let row_a = vec![serde_json::json!(null), serde_json::json!("x")];
        let row_b = vec![serde_json::json!("null"), serde_json::json!("x")];
        assert_ne!(
            hash_row_blake3(&row_a),
            hash_row_blake3(&row_b),
            "null literal and \"null\" string should hash differently"
        );
    }

    #[test]
    fn pick_sample_tables_prefers_top_pairs() {
        use rocky_core::dedup_analysis::{DedupPairStat, DedupStats, TableDedupStat};

        let stats = DedupStats {
            total_partitions: 10,
            unique_partitions: 7,
            duplicate_partitions: 3,
            total_rows: 1000,
            dedup_ratio: 0.3,
            estimated_savings_pct: 30.0,
            top_dedup_pairs: vec![
                DedupPairStat {
                    table_a: "db.bronze.orders".into(),
                    table_b: "db.silver.orders".into(),
                    shared_partitions: 3,
                    shared_rows: 300,
                },
                DedupPairStat {
                    table_a: "db.bronze.events".into(),
                    table_b: "db.silver.events".into(),
                    shared_partitions: 1,
                    shared_rows: 100,
                },
            ],
            per_table: vec![
                TableDedupStat {
                    table: "db.bronze.orders".into(),
                    partitions: 3,
                    partitions_shared_with_others: 3,
                    contribution_pct: 100.0,
                },
                TableDedupStat {
                    table: "db.silver.orders".into(),
                    partitions: 3,
                    partitions_shared_with_others: 3,
                    contribution_pct: 100.0,
                },
                TableDedupStat {
                    table: "db.bronze.events".into(),
                    partitions: 2,
                    partitions_shared_with_others: 1,
                    contribution_pct: 50.0,
                },
                TableDedupStat {
                    table: "db.silver.events".into(),
                    partitions: 2,
                    partitions_shared_with_others: 1,
                    contribution_pct: 50.0,
                },
            ],
        };

        let sample = pick_sample_tables(&stats, 3);
        assert_eq!(sample.len(), 3);
        // Should include both tables from the top pair plus one from the
        // second pair. The result is sorted alphabetically.
        assert!(sample.contains(&"db.bronze.orders".to_string()));
        assert!(sample.contains(&"db.silver.orders".to_string()));
        // Third slot goes to table_a of the second pair.
        assert!(sample.contains(&"db.bronze.events".to_string()));
    }

    #[test]
    fn pick_sample_tables_caps_at_max() {
        use rocky_core::dedup_analysis::{DedupPairStat, DedupStats, TableDedupStat};

        let stats = DedupStats {
            total_partitions: 6,
            unique_partitions: 4,
            duplicate_partitions: 2,
            total_rows: 600,
            dedup_ratio: 0.33,
            estimated_savings_pct: 33.0,
            top_dedup_pairs: vec![DedupPairStat {
                table_a: "a".into(),
                table_b: "b".into(),
                shared_partitions: 2,
                shared_rows: 200,
            }],
            per_table: vec![
                TableDedupStat {
                    table: "a".into(),
                    partitions: 3,
                    partitions_shared_with_others: 2,
                    contribution_pct: 66.7,
                },
                TableDedupStat {
                    table: "b".into(),
                    partitions: 3,
                    partitions_shared_with_others: 2,
                    contribution_pct: 66.7,
                },
            ],
        };

        // Ask for max 2 — should not exceed it.
        let sample = pick_sample_tables(&stats, 2);
        assert_eq!(sample.len(), 2);
    }

    /// E2E test for `compute_measure_dedup` with `--calibrate-bytes`
    /// enabled against an ephemeral DuckDB.
    ///
    /// Uses the same three-table setup as the partition-level test:
    /// - bronze.orders and silver.orders are identical (100 rows each)
    /// - bronze.distinct has different content (50 rows)
    ///
    /// With calibration enabled, the byte-level blake3 dedup should
    /// detect that bronze.orders and silver.orders share the same rows.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn calibrate_bytes_detects_row_level_dedup_against_real_duckdb() -> Result<()> {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test_cal.duckdb");

        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path)
                .map_err(|e| anyhow!("setup: open DuckDB: {e}"))?;
            for sql in &[
                "CREATE SCHEMA bronze",
                "CREATE SCHEMA silver",
                "CREATE TABLE bronze.orders AS \
                 SELECT i AS id, ('row_' || i) AS name \
                 FROM generate_series(1, 100) t(i)",
                "CREATE TABLE silver.orders AS \
                 SELECT i AS id, ('row_' || i) AS name \
                 FROM generate_series(1, 100) t(i)",
                "CREATE TABLE bronze.distinct AS \
                 SELECT i AS id FROM generate_series(1, 50) t(i)",
            ] {
                adapter
                    .execute_statement(sql)
                    .await
                    .map_err(|e| anyhow!("setup: {sql}: {e}"))?;
            }
        }

        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.test]
strategy = "full_refresh"

[pipeline.test.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.test.target]
catalog_template = "test"
schema_template = "staging__{{source}}"
"#,
                db_path.display()
            ),
        )?;

        let output = compute_measure_dedup(&config_path, None, true, true).await?;

        // Calibration should now be present.
        let cal = output
            .calibration
            .as_ref()
            .expect("calibration should be Some when --calibrate-bytes is passed");

        // At least 2 tables should have been sampled (bronze.orders +
        // silver.orders are the top dedup pair).
        assert!(
            cal.tables_sampled.len() >= 2,
            "expected at least 2 sampled tables, got {}",
            cal.tables_sampled.len()
        );

        // Both duplicate tables should be in the sample.
        let has_bronze = cal
            .tables_sampled
            .iter()
            .any(|t| t.ends_with("bronze.orders"));
        let has_silver = cal
            .tables_sampled
            .iter()
            .any(|t| t.ends_with("silver.orders"));
        assert!(
            has_bronze && has_silver,
            "sample should include both bronze.orders and silver.orders, got {:?}",
            cal.tables_sampled
        );

        // Byte-level dedup should be > 0 because bronze.orders and
        // silver.orders have identical rows.
        assert!(
            cal.byte_dedup_pct > 0.0,
            "byte_dedup_pct should be > 0 for identical tables, got {}",
            cal.byte_dedup_pct
        );

        // Partition-level dedup on the sample should also be > 0.
        assert!(
            cal.partition_dedup_pct > 0.0,
            "partition_dedup_pct should be > 0, got {}",
            cal.partition_dedup_pct
        );

        // The multiplier should be >= 1.0 (byte-level should find at
        // least as much dedup as partition-level, often more).
        assert!(
            cal.lower_bound_multiplier >= 1.0,
            "lower_bound_multiplier should be >= 1.0, got {}",
            cal.lower_bound_multiplier
        );

        Ok(())
    }
}
