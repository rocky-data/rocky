//! `rocky compact` — generate OPTIMIZE/VACUUM SQL for storage compaction.
//!
//! Also hosts `rocky compact --measure-dedup` — the Layer 0 cross-table
//! dedup-measurement experiment (`run_measure_dedup`). See
//! `plans/rocky-storage-layer-0.md` for the scope + decision gate.

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};

use rocky_core::dedup_analysis::{DedupStats, compute_dedup_stats};
use rocky_core::incremental::{PartitionChecksum, generate_whole_table_checksum_sql};
use rocky_core::ir::TableRef;
use rocky_core::traits::{QueryResult, WarehouseAdapter};

use crate::output::{
    ByteCalibration, CompactDedupOutput, CompactOutput, DedupPair, DedupSummary, NamedStatement,
    TableDedupContribution, print_json,
};
use crate::registry::{AdapterRegistry, resolve_pipeline};

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

    let statements = generate_compact_sql(model, target_mb);

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
            model: model.to_string(),
            dry_run,
            target_size_mb: target_mb,
            statements: typed_statements,
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
/// - **Partition-level only.** `--calibrate-bytes` logs a warning and
///   proceeds without the byte-level calibration. The plan's §6 says
///   calibration is recommended but not mandatory for an initial run.
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
    output_json: bool,
) -> Result<()> {
    let output = compute_measure_dedup(config_path, exclude_columns, calibrate_bytes).await?;
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
) -> Result<CompactDedupOutput> {
    let started_at = Utc::now();
    let start = Instant::now();

    if calibrate_bytes {
        tracing::warn!(
            "`--calibrate-bytes` is not yet implemented (Layer 0 Step 4 \
             ships partition-level only; byte-level calibration lands in a \
             follow-up per plans/rocky-storage-layer-0.md §4 Step 4 point 9). \
             Proceeding without calibration."
        );
    }

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

    // 5. Enumerate tables to measure.
    let tables = enumerate_tables(adapter.as_ref()).await?;
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

    for table in &tables {
        let table_ref_str = table
            .validated_full_name()
            .map_err(|e| anyhow!("invalid table reference: {e}"))?;

        // One describe_table call per table, reused for both hash
        // passes. This also sidesteps `HASH(*)` — which isn't portable
        // (DuckDB's `hash()` is variadic but the `*` doesn't expand
        // inside function arguments) — by always passing explicit
        // column lists.
        let column_info = adapter
            .describe_table(table)
            .await
            .map_err(|e| anyhow!("failed to describe {table_ref_str}: {e}"))?;

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
            hash_table(adapter.as_ref(), table, &all_columns).await?
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
            hash_table(adapter.as_ref(), table, &semantic_columns).await?
        };
        semantic_per_table.push((table_ref_str, semantic));
    }

    // 7. Compute cross-table dedup stats for both passes.
    let raw_stats = compute_dedup_stats(&raw_per_table);
    let semantic_stats = compute_dedup_stats(&semantic_per_table);
    let partitions_scanned = raw_stats.total_partitions;

    // 8. Convert to the CLI output type.
    let output = build_compact_dedup_output(
        engine,
        project,
        tables_scanned,
        partitions_scanned,
        &raw_stats,
        &semantic_stats,
        excluded_columns,
        None, // calibration deferred
        started_at,
        start.elapsed().as_millis() as u64,
    );

    Ok(output)
}

/// Enumerate non-system tables visible to the warehouse adapter via
/// `information_schema.tables`.
///
/// Uses the **unqualified** form (no catalog prefix), which works on
/// DuckDB (single-DB scope) and on Databricks/Snowflake when the
/// session already has a default catalog/database bound — which is the
/// case for every Rocky config that runs through `AdapterRegistry`.
///
/// System schemas (`information_schema`, `pg_catalog`, `system`) are
/// filtered so the measurement doesn't include the warehouse's own
/// metadata tables.
async fn enumerate_tables(adapter: &dyn WarehouseAdapter) -> Result<Vec<TableRef>> {
    const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "system"];

    let sql = "SELECT table_catalog, table_schema, table_name \
               FROM information_schema.tables \
               WHERE table_type IN ('BASE TABLE', 'TABLE')";

    let result = adapter
        .execute_query(sql)
        .await
        .map_err(|e| anyhow!("failed to enumerate tables via information_schema: {e}"))?;

    let mut tables = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
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
        if SYSTEM_SCHEMAS
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
    Ok(tables)
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
    let table_ref_str = table
        .validated_full_name()
        .map_err(|e| anyhow!("invalid table reference: {e}"))?;
    let sql = generate_whole_table_checksum_sql(&table_ref_str, value_columns);
    let result = adapter
        .execute_query(&sql)
        .await
        .map_err(|e| anyhow!("checksum query failed for {table_ref_str}: {e}"))?;
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
        let checksum =
            json_number_to_u64(row.get(1)).context("checksum column is missing or not a number")?;
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
        "Scanning Rocky project '{}' (engine: {})... {} tables, {} partitions",
        output.project, output.engine, output.tables_scanned, output.partitions_scanned
    );
    println!();
    println!(
        "Cross-table dedup analysis (engine-scoped: {}, granularity: partition)",
        output.engine
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

    println!("Interpretation: Partition-level measurement is a LOWER bound on byte-level");
    println!("dedup. Two partitions that share 99% of their row groups but differ on one");
    println!("row will appear fully distinct here. To get a byte-level calibration on a");
    println!("sample, re-run with --calibrate-bytes (not yet implemented).");
    println!();

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

/// Generates OPTIMIZE and VACUUM SQL for a given model.
///
/// For Delta Lake (Databricks):
/// - `OPTIMIZE` compacts small files into larger ones
/// - `VACUUM` removes old data files no longer referenced
fn generate_compact_sql(model: &str, target_size_mb: u64) -> Vec<(String, String)> {
    let mut stmts = Vec::new();

    // OPTIMIZE with file size target
    stmts.push((
        "compact small files".to_string(),
        format!("OPTIMIZE {model} WHERE true\n  -- target file size: {target_size_mb}MB"),
    ));

    // VACUUM to remove old unreferenced files (default 7 days retention)
    stmts.push((
        "remove stale data files".to_string(),
        format!("VACUUM {model} RETAIN 168 HOURS"),
    ));

    stmts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_compact_sql() {
        let stmts = generate_compact_sql("catalog.schema.orders", 256);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].1.contains("OPTIMIZE"));
        assert!(stmts[0].1.contains("catalog.schema.orders"));
        assert!(stmts[1].1.contains("VACUUM"));
        assert!(stmts[1].1.contains("168 HOURS"));
    }

    #[test]
    fn test_generate_compact_sql_custom_size() {
        let stmts = generate_compact_sql("my_table", 512);
        assert!(stmts[0].1.contains("512MB"));
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

        // Exercise the pure-compute path.
        let output = compute_measure_dedup(&config_path, None, false).await?;

        // Top-level shape.
        assert_eq!(output.engine, "duckdb");
        assert_eq!(output.project, "test");
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

        // Calibration is None — Layer 0 Step 4 deferred byte-level
        // calibration to a follow-up.
        assert!(output.calibration.is_none());

        Ok(())
    }
}
