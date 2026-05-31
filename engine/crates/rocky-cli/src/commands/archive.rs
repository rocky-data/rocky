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
use rocky_ir::ArchivePlanIr;

use crate::output::{
    ArchiveApplyOutput, ArchiveOutput, ArchiveTableEntry, ArchiveTotals, NamedStatement,
    StatementResult, print_json,
};
use crate::plan_store::{PlanKind, read_plan, write_plan_v2};
use crate::registry::AdapterRegistry;
use crate::scope::resolve_managed_tables_in_catalog;

/// Build the per-table `ArchivePlanIr` that mirrors the CLI's inline
/// `generate_archive_sql` inputs for a `model` / `days` pair. Matches
/// the construction used in the C-3 equivalence test so v2 plan
/// regeneration replays byte-identical SQL.
fn build_archive_ir(model: Option<&str>, days: u64) -> ArchivePlanIr {
    match model {
        Some(table) => ArchivePlanIr::for_table(table, days),
        None => ArchivePlanIr {
            target_table: None,
            older_than: format!("{days}d"),
            older_than_days: days,
            partition_column: "_fivetran_synced".to_string(),
            vacuum_retention_hours: Some(0),
        },
    }
}

/// Persist an archive plan as the typed-IR v2 envelope. After C-7 this
/// is the only loadable shape for `Archive` plans — `rocky apply`
/// regenerates SQL at execution time via `sql_gen::archive_from_ir`.
/// Mirrors [`super::compact::persist_compact_plan`]; the only
/// difference is the envelope shape (carries `older_than` /
/// `older_than_days` in addition to the per-table IR list).
fn persist_archive_plan(
    root: &Path,
    output: &ArchiveOutput,
    ir_payloads: &[ArchivePlanIr],
) -> Result<String> {
    let v2_payload = serde_json::json!({
        "model": output.model,
        "catalog": output.catalog,
        "scope": output.scope,
        "older_than": output.older_than,
        "older_than_days": output.older_than_days,
        "dry_run": output.dry_run,
        "plans": ir_payloads,
    });
    write_plan_v2(root, PlanKind::Archive, &v2_payload)
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky archive`.
///
/// After generating the plan, persists it to `.rocky/plans/<plan_id>.json`
/// (relative to the current working directory) so it can be applied later
/// via `rocky archive apply <plan_id>`. The persisted shape is the
/// typed-IR v2 envelope (`ArchivePlanIr`); the stdout JSON shape always
/// carries SQL for human + CI consumers.
pub fn run_archive(
    config_path: &Path,
    model: Option<&str>,
    older_than: &str,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    // Parse the older_than duration (e.g., "90d", "6m", "1y")
    let days = parse_duration_days(older_than)?;
    // Resolve the configured target dialect (config-only, no credentials) so
    // the gated generator fails fast off Databricks at plan time instead of
    // printing DELETE/VACUUM SQL the warehouse rejects at apply time.
    let dialect = crate::commands::plan::resolve_configured_dialect(config_path)?;
    let statements = generate_archive_sql(model, days, dialect.as_ref())?;

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
    let ir_payloads = vec![build_archive_ir(model, days)];
    match persist_archive_plan(&cwd, &output, &ir_payloads) {
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

    // Resolve the configured target dialect once for the whole catalog so the
    // gated generator fails fast off Databricks before templating any SQL.
    let dialect = crate::commands::plan::resolve_configured_dialect(config_path)?;

    let mut flat_statements: Vec<NamedStatement> = Vec::new();
    let mut per_table: BTreeMap<String, ArchiveTableEntry> = BTreeMap::new();
    let mut ir_payloads: Vec<ArchivePlanIr> = Vec::with_capacity(scope.tables.len());

    for fqn in &scope.tables {
        let stmts = generate_archive_sql(Some(fqn), days, dialect.as_ref())?;
        let typed: Vec<NamedStatement> = stmts
            .into_iter()
            .map(|(purpose, sql)| NamedStatement {
                purpose,
                sql: Some(sql),
            })
            .collect();
        flat_statements.extend(typed.iter().cloned());
        per_table.insert(fqn.clone(), ArchiveTableEntry { statements: typed });
        ir_payloads.push(build_archive_ir(Some(fqn), days));
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
    match persist_archive_plan(&cwd, &output, &ir_payloads) {
        Ok(id) => {
            tracing::info!(
                plan_id = %id,
                catalog = %scope.catalog,
                "archive catalog plan persisted"
            );
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

/// Generates archive SQL (DELETE + VACUUM) for old data via the gated
/// `sql_gen::archive_from_ir` chokepoint.
///
/// `dialect` is the configured target dialect: `archive_from_ir` refuses
/// to emit the Delta-flavoured DELETE/VACUUM bundle (`DATEADD` +
/// `RETAIN N HOURS`) for any dialect that doesn't advertise
/// `supports_delta_maintenance()`, so this returns a clear
/// known-limitation error off Databricks rather than templating SQL the
/// warehouse rejects.
///
/// When `model` is `Some`, every dot-separated segment is validated as
/// a SQL identifier (inside `archive_from_ir`) — the model name reaches
/// us from CLI args and config files, both of which are untrusted from
/// the engine's perspective. See the validation rules in `engine/CLAUDE.md`.
fn generate_archive_sql(
    model: Option<&str>,
    days: u64,
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> Result<Vec<(String, String)>> {
    let ir = build_archive_ir(model, days);
    rocky_core::sql_gen::archive_from_ir(&ir, dialect).with_context(|| {
        format!(
            "failed to generate archive SQL for '{}'",
            model.unwrap_or("*")
        )
    })
}

/// Regenerate `Vec<NamedStatement>` from a v2 (typed-IR) persisted
/// archive plan payload.
///
/// Mirrors `regenerate_v2_compact_statements` in `compact.rs`. The v2
/// envelope shape is `{ model?, catalog?, scope?, older_than,
/// older_than_days, dry_run, plans: [ArchivePlanIr, ...] }`. Cutoff
/// math stays at runtime (`DATEADD(DAY, -N, CURRENT_TIMESTAMP())`) so
/// regeneration is byte-equivalent to the v1 inline path; literal-
/// timestamp cutoffs (`cutoff_resolved_at`) are a separate semantic
/// improvement deferred to a follow-up PR.
fn regenerate_v2_archive_statements(
    plan_id: &str,
    payload: &serde_json::Value,
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> Result<Vec<NamedStatement>> {
    let plans_value = payload
        .get("plans")
        .ok_or_else(|| anyhow::anyhow!(
            "archive plan '{}' (v2) is missing the `plans` field; expected an envelope of \
             {{ model?, catalog?, scope?, older_than, older_than_days, dry_run, plans: [ArchivePlanIr, ...] }}",
            plan_id,
        ))?;
    let ir_plans: Vec<ArchivePlanIr> = serde_json::from_value(plans_value.clone())
        .with_context(|| format!(
            "archive plan '{plan_id}' (v2) `plans` field failed to deserialize as Vec<ArchivePlanIr>"
        ))?;

    let mut statements = Vec::new();
    for ir in &ir_plans {
        let pairs = rocky_core::sql_gen::archive_from_ir(ir, dialect).with_context(|| {
            format!(
                "archive plan '{plan_id}' (v2): failed to regenerate SQL for target {target}",
                target = ir.target_table.as_deref().unwrap_or("<wildcard>"),
            )
        })?;
        for (purpose, sql) in pairs {
            statements.push(NamedStatement {
                purpose,
                sql: Some(sql),
            });
        }
    }
    Ok(statements)
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

    // Resolve the configured target dialect (config-only) so SQL
    // regeneration is gated on the real warehouse: applying an archive plan
    // against a non-Databricks adapter fails fast with the limitation error.
    let dialect = crate::commands::plan::resolve_configured_dialect(config_path)?;

    // Phase C — "SQL as `.o` files" — v2 plans carry an `ArchivePlanIr`
    // envelope and SQL is regenerated here via `sql_gen::archive_from_ir`.
    // The v1 inline-SQL envelope was retired in C-7; v1-shaped archive
    // payloads on disk (written by Rocky < engine-v1.35.0) are rejected
    // with a migration error pointing at the recipe.
    let statements: Vec<NamedStatement> = match plan.format_version {
        1 => bail!(
            "archive plan '{}' is in format v1, which is no longer supported \
             (this plan was written by Rocky < engine-v1.35.0). \
             Re-run `rocky archive` to write a fresh v2 plan. \
             See https://docs.rocky-data.com/concepts/plan-store-v1-to-v2 \
             for the migration guide.",
            plan_id,
        ),
        2 => regenerate_v2_archive_statements(plan_id, &plan.payload, dialect.as_ref())?,
        other => bail!(
            "archive plan '{}' has unknown format_version {}. \
             This binary supports v2 (typed ArchivePlanIr) persisted plans; \
             the file is from a newer engine — upgrade and re-apply.",
            plan_id,
            other
        ),
    };

    // Build the warehouse adapter from the config.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_pipeline_name, pipeline) = crate::registry::resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline for archive apply")?;
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;

    let executed_at = Utc::now();
    let mut results: Vec<StatementResult> = Vec::with_capacity(statements.len());
    let mut overall_success = true;

    for stmt in &statements {
        // After the v2 dispatch above, `sql` is always populated:
        // `archive_from_ir` returns `Some(sql)` for every statement.
        // A `None` here means a malformed plan file — surface a clear
        // error rather than executing an empty statement.
        let sql = match stmt.sql.as_deref() {
            Some(sql) => sql,
            None => bail!(
                "archive plan '{}' carries a statement without inline SQL \
                 (purpose: '{}'). This is a malformed plan file — re-run \
                 `rocky archive` to write a fresh plan.",
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
        let dialect = rocky_databricks::dialect::DatabricksSqlDialect;
        let stmts = generate_archive_sql(Some("catalog.schema.orders"), 90, &dialect).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].1.contains("DELETE FROM"));
        assert!(stmts[0].1.contains("-90"));
        assert!(stmts[1].1.contains("VACUUM"));
    }

    #[test]
    fn test_generate_archive_sql_wildcard() {
        let dialect = rocky_databricks::dialect::DatabricksSqlDialect;
        let stmts = generate_archive_sql(None, 30, &dialect).unwrap();
        assert!(stmts[0].1.contains("DELETE FROM *"));
    }

    #[test]
    fn test_generate_archive_sql_rejects_injection() {
        let d = &rocky_databricks::dialect::DatabricksSqlDialect;
        assert!(generate_archive_sql(Some("orders; DROP TABLE admin; --"), 30, d).is_err());
        assert!(generate_archive_sql(Some("catalog.schema; DROP TABLE x"), 30, d).is_err());
        assert!(generate_archive_sql(Some("'; DROP TABLE users; --"), 30, d).is_err());
        assert!(generate_archive_sql(Some(""), 30, d).is_err());
        assert!(generate_archive_sql(Some("catalog..table"), 30, d).is_err());
    }

    /// `generate_archive_sql` routes through the gated `archive_from_ir`,
    /// so a non-Databricks dialect must yield the known-limitation error
    /// rather than templating the Delta DELETE/VACUUM bundle.
    #[test]
    fn test_generate_archive_sql_refuses_non_databricks() {
        let dialect = rocky_snowflake::dialect::SnowflakeSqlDialect;
        let err = generate_archive_sql(Some("db.schema.orders"), 90, &dialect)
            .expect_err("Snowflake must be refused for archive");
        // The limitation lives in the error chain (the helper adds a context
        // frame), so assert on the full alternate-formatted chain.
        let msg = format!("{err:#}");
        assert!(
            msg.contains("VACUUM") && msg.contains("snowflake"),
            "expected a VACUUM/snowflake limitation message, got: {msg}"
        );
    }

    /// Integration test for `rocky archive apply` against a real DuckDB.
    ///
    /// DuckDB does not support the Databricks-style `VACUUM ... RETAIN HOURS`
    /// C-7 — `archive apply` rejects v1-shaped archive plans on disk
    /// with a clear migration error pointing at the recipe. Before C-7
    /// this test exercised the round-trip apply path against a real
    /// DuckDB; after C-7 the v1 inline-SQL envelope is no longer a
    /// loadable shape for archive plans, so the apply path errors out
    /// before reaching the adapter. The DuckDB feature gate stays
    /// because we still spin up a minimal database for the rocky.toml
    /// adapter to point at.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn archive_apply_rejects_v1_plan_with_migration_error() -> anyhow::Result<()> {
        use crate::plan_store::write_plan;
        use anyhow::anyhow;
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("test_arc_v1_reject.duckdb");

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

        // Fabricate a v1-shaped archive plan on disk (`write_plan` tags
        // `format_version = 1` with an `ArchiveOutput` payload — the
        // legacy inline-SQL envelope written by Rocky < engine-v1.35.0).
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
                statements: vec![NamedStatement {
                    purpose: "delete rows older than 90 days".to_string(),
                    sql: Some(
                        "DELETE FROM events WHERE _fivetran_synced < DATEADD(DAY, -90, CURRENT_TIMESTAMP())"
                            .to_string(),
                    ),
                }],
                tables: None,
                totals: None,
                plan_id: None,
            },
        )?;

        // Sanity check: the file is on disk and parses as v1.
        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::Archive);
        assert_eq!(plan.format_version, 1);

        // Apply must reject with the migration error.
        let err = run_archive_apply_in(dir.path(), &config_path, &plan_id, false)
            .await
            .expect_err("v1 archive plan must be rejected by apply");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("format v1") || msg.contains("v1 plan"),
            "error should call out v1 format, got: {msg}"
        );
        assert!(
            msg.contains("plan-store-v1-to-v2"),
            "error should point at the migration guide, got: {msg}"
        );
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
        let plan_id = crate::plan_store::write_plan(
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
            let inline = generate_archive_sql(model, days, &dialect)
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

    /// Cluster 3 C — C-5 v2-payload regeneration tests.
    ///
    /// The reader-side dispatch in `run_archive_apply_in` calls
    /// `regenerate_v2_archive_statements` on a `format_version = 2`
    /// payload. These tests pin that helper against:
    ///
    /// - the inline `generate_archive_sql` baseline (byte-equivalence),
    /// - the envelope-shape contract (missing `plans` field → clear error),
    /// - the v1↔v2 invariant that both paths produce the same
    ///   `(purpose, sql)` pairs for the same input.
    mod v2_regeneration {
        use super::*;
        use rocky_databricks::dialect::DatabricksSqlDialect;

        fn build_v2_payload(model: Option<&str>, days: u64) -> serde_json::Value {
            let ir = build_archive_ir(model, days);
            serde_json::json!({
                "model": model,
                "older_than": format!("{days}d"),
                "older_than_days": days,
                "dry_run": false,
                "plans": [ir],
            })
        }

        #[test]
        fn v2_regenerates_named_model_archive_statements() {
            let payload = build_v2_payload(Some("catalog.schema.events"), 90);
            let dialect = DatabricksSqlDialect;
            let stmts = regenerate_v2_archive_statements("test-plan", &payload, &dialect).unwrap();
            // Inline path baseline — must be byte-identical.
            let baseline =
                generate_archive_sql(Some("catalog.schema.events"), 90, &dialect).unwrap();
            assert_eq!(stmts.len(), baseline.len());
            for (s, (purpose, sql)) in stmts.iter().zip(baseline.iter()) {
                assert_eq!(s.purpose, *purpose);
                assert_eq!(s.sql.as_deref(), Some(sql.as_str()));
            }
        }

        #[test]
        fn v2_regenerates_catalog_scope_archive_statements_in_order() {
            // Multi-entry `plans` list models the `--catalog` path.
            // Flat concatenation must match the inline catalog loop.
            let tables = ["warehouse.facts.events", "warehouse.facts.orders"];
            let irs: Vec<_> = tables
                .iter()
                .map(|t| build_archive_ir(Some(t), 90))
                .collect();
            let payload = serde_json::json!({
                "catalog": "warehouse",
                "scope": "catalog",
                "older_than": "90d",
                "older_than_days": 90,
                "dry_run": false,
                "plans": irs,
            });
            let dialect = DatabricksSqlDialect;
            let stmts = regenerate_v2_archive_statements("test-plan", &payload, &dialect).unwrap();
            let mut expected: Vec<(String, String)> = Vec::new();
            for t in tables {
                expected.extend(generate_archive_sql(Some(t), 90, &dialect).unwrap());
            }
            assert_eq!(stmts.len(), expected.len());
            for (s, (purpose, sql)) in stmts.iter().zip(expected.iter()) {
                assert_eq!(s.purpose, *purpose);
                assert_eq!(s.sql.as_deref(), Some(sql.as_str()));
            }
        }

        #[test]
        fn v2_missing_plans_field_returns_clear_error() {
            let payload = serde_json::json!({
                "model": "x",
                "older_than": "90d",
                "older_than_days": 90,
                "dry_run": false,
                // no `plans` field
            });
            let dialect = DatabricksSqlDialect;
            let err =
                regenerate_v2_archive_statements("test-plan", &payload, &dialect).unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("plans") && msg.contains("v2"),
                "error should mention the missing `plans` field, got: {msg}"
            );
        }
    }
}
