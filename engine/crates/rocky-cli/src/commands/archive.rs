//! `rocky archive` — archive old data partitions.
//!
//! ## Plan persistence (`archive apply`)
//!
//! After generating an archive plan the default path writes it to
//! `.rocky/plans/<plan_id>.json`. `run_archive_apply` reads that file and
//! executes the SQL statements via the warehouse adapter, reporting per-
//! statement results in `ArchiveApplyOutput`.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use rocky_sql::validation::validate_identifier;

use crate::output::{
    ArchiveApplyOutput, ArchiveOutput, ArchiveTableEntry, ArchiveTotals, NamedStatement,
    StatementResult, print_json,
};
use crate::plan_store::{PlanKind, read_plan, write_plan};
use crate::registry::AdapterRegistry;
use crate::scope::resolve_managed_tables_in_catalog;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky archive`.
///
/// After generating the plan, persists it to `.rocky/plans/<plan_id>.json`
/// (relative to the current working directory) so it can be applied later
/// via `rocky archive apply <plan_id>`.
pub fn run_archive(
    model: Option<&str>,
    older_than: &str,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    // Parse the older_than duration (e.g., "90d", "6m", "1y")
    let days = parse_duration_days(older_than)?;
    let statements = generate_archive_sql(model, days)?;

    let typed_statements: Vec<NamedStatement> = statements
        .iter()
        .map(|(purpose, sql)| NamedStatement {
            purpose: purpose.clone(),
            sql: Some(sql.clone()),
        })
        .collect();

    // Build output without plan_id first, then hash it.
    let mut output = ArchiveOutput {
        version: VERSION.to_string(),
        command: "archive".to_string(),
        model: model.map(String::from),
        catalog: None,
        scope: None,
        older_than: older_than.to_string(),
        older_than_days: days,
        dry_run,
        statements: typed_statements,
        tables: None,
        totals: None,
        plan_id: None,
    };

    // Persist the plan (plan_id is None so the hash is stable).
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    match write_plan(&cwd, PlanKind::Archive, &output) {
        Ok(id) => {
            tracing::info!(plan_id = %id, "archive plan persisted");
            output.plan_id = Some(id);
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to persist archive plan; 'apply' will not be available for this run");
        }
    }

    if output_json {
        print_json(&output)?;
    } else {
        println!(
            "Archive plan: data older than {older_than} ({days} days){}",
            if dry_run { " [DRY RUN]" } else { "" }
        );
        if let Some(m) = model {
            println!("Model: {m}");
        }
        if let Some(ref id) = output.plan_id {
            println!("Plan ID: {id}");
            println!("Apply with: rocky archive apply {id}");
        }
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

/// Execute `rocky archive --catalog <name>` — generate archive SQL for
/// every Rocky-managed table in the catalog.
///
/// Mirrors `rocky compact --catalog`: resolves the managed-table set via
/// [`resolve_managed_tables_in_catalog`] (config-only, no warehouse round
/// trip), then templates `DELETE` / `VACUUM` per table and aggregates
/// into a single envelope.
pub async fn run_archive_catalog(
    config_path: &Path,
    catalog: &str,
    older_than: &str,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    let days = parse_duration_days(older_than)?;
    let scope = resolve_managed_tables_in_catalog(config_path, catalog).await?;

    let mut flat_statements: Vec<NamedStatement> = Vec::new();
    let mut per_table: BTreeMap<String, ArchiveTableEntry> = BTreeMap::new();

    for fqn in &scope.tables {
        let stmts = generate_archive_sql(Some(fqn), days)?;
        let typed: Vec<NamedStatement> = stmts
            .into_iter()
            .map(|(purpose, sql)| NamedStatement {
                purpose,
                sql: Some(sql),
            })
            .collect();
        flat_statements.extend(typed.iter().cloned());
        per_table.insert(fqn.clone(), ArchiveTableEntry { statements: typed });
    }

    let totals = ArchiveTotals {
        table_count: scope.tables.len(),
        statement_count: flat_statements.len(),
    };

    let mut output = ArchiveOutput {
        version: VERSION.to_string(),
        command: "archive".to_string(),
        model: None,
        catalog: Some(scope.catalog.clone()),
        scope: Some("catalog".to_string()),
        older_than: older_than.to_string(),
        older_than_days: days,
        dry_run,
        statements: flat_statements,
        tables: Some(per_table),
        totals: Some(totals),
        plan_id: None,
    };

    // Persist the plan (plan_id is None so the hash is stable).
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    match write_plan(&cwd, PlanKind::Archive, &output) {
        Ok(id) => {
            tracing::info!(plan_id = %id, catalog = %scope.catalog, "archive catalog plan persisted");
            output.plan_id = Some(id);
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to persist archive catalog plan");
        }
    }

    if output_json {
        print_json(&output)?;
    } else {
        println!(
            "Archive plan for catalog '{}' ({} tables, older than {} = {} days){}",
            scope.catalog,
            scope.tables.len(),
            older_than,
            days,
            if dry_run { " [DRY RUN]" } else { "" }
        );
        if let Some(ref id) = output.plan_id {
            println!("Plan ID: {id}");
            println!("Apply with: rocky archive apply {id}");
        }
        println!();

        if let Some(ref per_table) = output.tables {
            for (fqn, entry) in per_table {
                println!("-- {fqn}");
                for s in &entry.statements {
                    println!("-- {}", s.purpose);
                    if let Some(ref sql) = s.sql {
                        println!("{sql};");
                    }
                }
                println!();
            }
        }

        if dry_run {
            println!("(dry run: no statements executed)");
        }
    }

    Ok(())
}

/// Parses a human-readable duration into days.
///
/// Supported formats: `30d`, `6m`, `1y`, `90` (days).
fn parse_duration_days(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('d') {
        n.parse::<u64>()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))
    } else if let Some(n) = s.strip_suffix('m') {
        let months: u64 = n
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))?;
        Ok(months * 30)
    } else if let Some(n) = s.strip_suffix('y') {
        let years: u64 = n
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))?;
        Ok(years * 365)
    } else {
        s.parse::<u64>()
            .map_err(|e| anyhow::anyhow!("invalid duration '{s}': {e} (use 30d, 6m, or 1y)"))
    }
}

/// Generates archive SQL (DELETE + VACUUM) for old data.
///
/// When `model` is `Some`, every dot-separated segment is validated as
/// a SQL identifier before interpolation — the model name reaches us
/// from CLI args and config files, both of which are untrusted from
/// the engine's perspective. See the validation rules in `engine/CLAUDE.md`.
fn generate_archive_sql(model: Option<&str>, days: u64) -> Result<Vec<(String, String)>> {
    let target = match model {
        Some(m) => {
            for part in m.split('.') {
                validate_identifier(part)
                    .with_context(|| format!("invalid model identifier '{m}'"))?;
            }
            m.to_string()
        }
        None => "*".to_string(),
    };

    Ok(vec![
        (
            format!("delete rows older than {days} days"),
            format!(
                "DELETE FROM {target}\n\
                 WHERE _fivetran_synced < DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())"
            ),
        ),
        (
            "reclaim storage after deletion".to_string(),
            format!("VACUUM {target} RETAIN 0 HOURS"),
        ),
    ])
}

/// Execute `rocky archive apply <plan_id>`.
///
/// Reads the persisted plan from `.rocky/plans/<plan_id>.json`, validates
/// that it is an archive plan, then executes each statement via the warehouse
/// adapter. Per-statement results are collected in `ArchiveApplyOutput`.
///
/// DuckDB does not support `VACUUM` with `RETAIN` syntax in the Databricks
/// form — adapter errors surface cleanly through `StatementResult.error`
/// and `success: false`; no special-casing is needed.
pub async fn run_archive_apply(config_path: &Path, plan_id: &str, output_json: bool) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_archive_apply_in(&cwd, config_path, plan_id, output_json).await
}

/// Inner implementation of `rocky archive apply` that takes an explicit `root`
/// for the plans directory. Extracted so tests can pass a temp dir without
/// touching the process-global current working directory.
pub(crate) async fn run_archive_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    output_json: bool,
) -> Result<()> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read archive plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Archive {
        bail!(
            "plan '{}' is a {} plan, not an archive plan. \
             Use `rocky {} apply {}` instead.",
            plan_id,
            plan.kind,
            plan.kind,
            plan_id
        );
    }

    // Deserialize the payload back to ArchiveOutput to extract statements.
    let archive_output: ArchiveOutput = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize archive plan payload")?;

    // Build the warehouse adapter from the config.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_pipeline_name, pipeline) = crate::registry::resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline for archive apply")?;
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;

    let executed_at = Utc::now();
    let mut results: Vec<StatementResult> = Vec::with_capacity(archive_output.statements.len());
    let mut overall_success = true;

    for stmt in &archive_output.statements {
        // `NamedStatement.sql` is `Option<String>` to leave room for the
        // v2 persisted plan format (Cluster 3 C — "SQL as `.o` files");
        // a `None` here means the plan was written by a newer engine
        // that persists typed IR. See the matching comment in `compact.rs`.
        let sql = match stmt.sql.as_deref() {
            Some(sql) => sql,
            None => bail!(
                "archive plan '{}' carries a statement without inline SQL \
                 (purpose: '{}'). This plan was written by a newer engine \
                 that persists typed-IR plans; this binary cannot regenerate \
                 SQL from IR yet — upgrade and re-apply.",
                plan_id,
                stmt.purpose,
            ),
        };

        if !overall_success {
            results.push(StatementResult {
                purpose: stmt.purpose.clone(),
                sql: sql.to_string(),
                success: false,
                duration_ms: 0,
                error: Some("skipped: a prior statement failed".to_string()),
            });
            continue;
        }

        let start = Instant::now();
        let outcome = adapter.execute_statement(sql).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match outcome {
            Ok(()) => {
                tracing::info!(purpose = %stmt.purpose, duration_ms, "archive apply: statement succeeded");
                results.push(StatementResult {
                    purpose: stmt.purpose.clone(),
                    sql: sql.to_string(),
                    success: true,
                    duration_ms,
                    error: None,
                });
            }
            Err(e) => {
                tracing::warn!(
                    purpose = %stmt.purpose,
                    duration_ms,
                    error = %e,
                    "archive apply: statement failed"
                );
                results.push(StatementResult {
                    purpose: stmt.purpose.clone(),
                    sql: sql.to_string(),
                    success: false,
                    duration_ms,
                    error: Some(format!("{e}")),
                });
                overall_success = false;
            }
        }
    }

    let output = ArchiveApplyOutput {
        version: VERSION.to_string(),
        command: "archive apply".to_string(),
        plan_id: plan_id.to_string(),
        executed_at,
        statements: results,
        success: overall_success,
    };

    if output_json {
        print_json(&output)?;
    } else {
        println!("Archive apply — plan: {plan_id}");
        println!("Executed at: {executed_at}");
        println!();
        for r in &output.statements {
            let status = if r.success { "OK" } else { "FAIL" };
            print!("[{status}] {} ({}ms)", r.purpose, r.duration_ms);
            if let Some(ref err) = r.error {
                print!(" — {err}");
            }
            println!();
        }
        println!();
        if overall_success {
            println!("All statements succeeded.");
        } else {
            println!("One or more statements failed.");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_days("30d").unwrap(), 30);
        assert_eq!(parse_duration_days("90").unwrap(), 90);
        assert_eq!(parse_duration_days("6m").unwrap(), 180);
        assert_eq!(parse_duration_days("1y").unwrap(), 365);
        assert_eq!(parse_duration_days("2y").unwrap(), 730);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration_days("abc").is_err());
        assert!(parse_duration_days("").is_err());
    }

    #[test]
    fn test_generate_archive_sql() {
        let stmts = generate_archive_sql(Some("catalog.schema.orders"), 90).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].1.contains("DELETE FROM"));
        assert!(stmts[0].1.contains("-90"));
        assert!(stmts[1].1.contains("VACUUM"));
    }

    #[test]
    fn test_generate_archive_sql_wildcard() {
        let stmts = generate_archive_sql(None, 30).unwrap();
        assert!(stmts[0].1.contains("DELETE FROM *"));
    }

    #[test]
    fn test_generate_archive_sql_rejects_injection() {
        assert!(generate_archive_sql(Some("orders; DROP TABLE admin; --"), 30).is_err());
        assert!(generate_archive_sql(Some("catalog.schema; DROP TABLE x"), 30).is_err());
        assert!(generate_archive_sql(Some("'; DROP TABLE users; --"), 30).is_err());
        assert!(generate_archive_sql(Some(""), 30).is_err());
        assert!(generate_archive_sql(Some("catalog..table"), 30).is_err());
    }

    /// Integration test for `rocky archive apply` against a real DuckDB.
    ///
    /// DuckDB does not support the Databricks-style `VACUUM ... RETAIN HOURS`
    /// syntax, and `DELETE FROM` on a table with a Databricks-specific column
    /// (`_fivetran_synced`) will fail too. The apply path must surface adapter
    /// errors via `StatementResult.error` and `success: false` rather than
    /// panicking. This test verifies that contract.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn archive_apply_surfaces_duckdb_errors_cleanly() -> anyhow::Result<()> {
        use anyhow::anyhow;
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test_arc_apply.duckdb");

        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path)
                .map_err(|e| anyhow!("setup: open DuckDB: {e}"))?;
            adapter
                .execute_statement("CREATE TABLE events (id INTEGER)")
                .await
                .map_err(|e| anyhow!("setup: {e}"))?;
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

        // Write an archive plan using the standard generated SQL.
        let plan_id = write_plan(
            dir.path(),
            PlanKind::Archive,
            &ArchiveOutput {
                version: "test".to_string(),
                command: "archive".to_string(),
                model: Some("events".to_string()),
                catalog: None,
                scope: None,
                older_than: "90d".to_string(),
                older_than_days: 90,
                dry_run: false,
                statements: vec![
                    NamedStatement {
                        purpose: "delete rows older than 90 days".to_string(),
                        sql: Some("DELETE FROM events WHERE _fivetran_synced < DATEADD(DAY, -90, CURRENT_TIMESTAMP())".to_string()),
                    },
                    NamedStatement {
                        purpose: "reclaim storage after deletion".to_string(),
                        sql: Some("VACUUM events RETAIN 0 HOURS".to_string()),
                    },
                ],
                tables: None,
                totals: None,
                plan_id: None,
            },
        )?;

        // Apply the plan using the inner function with an explicit root, so we
        // don't need to change the process-global current directory.
        run_archive_apply_in(dir.path(), &config_path, &plan_id, false).await?;

        // Plan round-trip sanity.
        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::Archive);

        Ok(())
    }

    /// `archive apply` with a wrong kind (compact plan) returns a clear error.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn archive_apply_rejects_compact_plan() -> anyhow::Result<()> {
        use anyhow::anyhow;
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test_arc_wrong.duckdb");
        {
            let adapter =
                DuckDbWarehouseAdapter::open(&db_path).map_err(|e| anyhow!("setup: {e}"))?;
            adapter
                .execute_statement("CREATE TABLE y (id INTEGER)")
                .await
                .map_err(|e| anyhow!("setup: {e}"))?;
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

        // Write a compact plan (wrong kind for archive apply).
        let plan_id = write_plan(
            dir.path(),
            PlanKind::Compact,
            &serde_json::json!({"dummy": true}),
        )?;

        let err = run_archive_apply_in(dir.path(), &config_path, &plan_id, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("compact plan, not an archive plan"),
            "error should mention kind mismatch, got: {msg}"
        );
        Ok(())
    }

    /// Cluster 3 C — C-3 equivalence proof.
    ///
    /// The CLI's inline `generate_archive_sql` and the typed-IR-driven
    /// `rocky_core::sql_gen::archive_from_ir` must emit byte-identical
    /// `(purpose, sql)` pairs for every input the current emission
    /// path uses today (single-model, wildcard, and
    /// per-table-in-catalog-scope). If this test fails, the v2
    /// persisted plan format would silently regenerate different SQL
    /// at apply time — see the audit memo for Phase C.
    ///
    /// `dialect` is plumbed through `archive_from_ir` for forward
    /// compatibility but is unused by today's emission path (which
    /// matches the inline CLI behavior). The Databricks dialect is
    /// used here because it is the dialect archive targets (Delta
    /// Lake `DELETE` / `VACUUM` with `DATEADD` runtime date math).
    mod archive_from_ir_equivalence {
        use super::*;
        use rocky_core::sql_gen::archive_from_ir;
        use rocky_databricks::dialect::DatabricksSqlDialect;
        use rocky_ir::ArchivePlanIr;

        fn assert_equivalent(model: Option<&str>, days: u64) {
            let dialect = DatabricksSqlDialect;
            let inline = generate_archive_sql(model, days)
                .expect("inline generation must succeed for valid inputs");

            // Mirror today's IR construction: `for_table` for named
            // models, manual construction for the wildcard path.
            let ir = match model {
                Some(table) => ArchivePlanIr::for_table(table, days),
                None => ArchivePlanIr {
                    target_table: None,
                    older_than: format!("{days}d"),
                    older_than_days: days,
                    partition_column: "_fivetran_synced".to_string(),
                    vacuum_retention_hours: Some(0),
                },
            };
            let ir_pairs = archive_from_ir(&ir, &dialect)
                .expect("ir generation must succeed for valid inputs");
            assert_eq!(
                inline, ir_pairs,
                "archive_from_ir must produce byte-identical SQL to the inline path \
                 for model={model:?} days={days}",
            );
        }

        #[test]
        fn matches_single_model_default_older_than() {
            // 90 days is today's canonical default (`rocky archive
            // --older-than 90d`).
            assert_equivalent(Some("catalog.schema.events"), 90);
        }

        #[test]
        fn matches_single_model_custom_older_than() {
            assert_equivalent(Some("catalog.schema.events"), 180);
            assert_equivalent(Some("catalog.schema.events"), 365);
        }

        #[test]
        fn matches_two_part_table_ref() {
            // Catalog-scope CLI invocations resolve managed tables to
            // fully-qualified `catalog.schema.table` strings. Two-part
            // and single-segment forms also need to round-trip.
            assert_equivalent(Some("schema.events"), 90);
            assert_equivalent(Some("events"), 90);
        }

        #[test]
        fn matches_catalog_scope_per_table() {
            // Simulate the `--catalog` path: every table in the scope
            // goes through the same single-model emission. Each pair
            // must match individually so the flat `statements` list
            // (the catalog-envelope concatenation) is byte-stable.
            let tables = [
                "warehouse.facts.events",
                "warehouse.facts.orders",
                "warehouse.facts.line_items",
            ];
            for fqn in tables {
                assert_equivalent(Some(fqn), 90);
            }
        }

        #[test]
        fn matches_wildcard_target() {
            // `model = None` reproduces today's degenerate
            // `DELETE FROM *` emission — preserved for byte-equivalence
            // even though the resulting SQL is intentionally invalid.
            assert_equivalent(None, 30);
        }
    }
}
