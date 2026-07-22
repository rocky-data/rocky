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
#[cfg(test)]
fn regenerate_v2_archive_statements(
    plan_id: &str,
    payload: &serde_json::Value,
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> Result<Vec<NamedStatement>> {
    let ir_plans = extract_archive_ir_plans(plan_id, payload)?;
    archive_statements_from_ir(plan_id, &ir_plans, dialect)
}

/// Deserialize the per-table `ArchivePlanIr` list from a v2 archive plan
/// payload's `plans` field. Split out from [`regenerate_v2_archive_statements`]
/// so the policy gate can build its touched-target set from the SAME typed IR
/// that regenerates the SQL, deserialized once.
fn extract_archive_ir_plans(
    plan_id: &str,
    payload: &serde_json::Value,
) -> Result<Vec<ArchivePlanIr>> {
    let plans_value = payload
        .get("plans")
        .ok_or_else(|| anyhow::anyhow!(
            "archive plan '{}' (v2) is missing the `plans` field; expected an envelope of \
             {{ model?, catalog?, scope?, older_than, older_than_days, dry_run, plans: [ArchivePlanIr, ...] }}",
            plan_id,
        ))?;
    serde_json::from_value(plans_value.clone()).with_context(|| {
        format!("archive plan '{plan_id}' (v2) `plans` field failed to deserialize as Vec<ArchivePlanIr>")
    })
}

/// Regenerate the flat `(purpose, sql)` statement list from an already-parsed
/// `ArchivePlanIr` list. `archive_from_ir` refuses any dialect that doesn't
/// advertise Delta maintenance, so a non-Databricks target fails fast here.
fn archive_statements_from_ir(
    plan_id: &str,
    ir_plans: &[ArchivePlanIr],
    dialect: &dyn rocky_core::traits::SqlDialect,
) -> Result<Vec<NamedStatement>> {
    let mut statements = Vec::new();
    for ir in ir_plans {
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
pub async fn run_archive_apply(
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_archive_apply_in(
        &cwd,
        config_path,
        plan_id,
        state_path,
        runtime_principal,
        output_json,
    )
    .await
}

/// Read + validate the archive plan, gate it under the agent-policy plane, and
/// regenerate the DELETE/VACUUM statements — everything up to (but not
/// including) warehouse execution.
///
/// Mirrors [`super::compact::run_compact_apply_in`]'s preparation, plus the
/// **targetless-archive guard**: an archive plan may carry `target_table = None`
/// (the `DELETE FROM *` wildcard the inline CLI reaches when given neither
/// `--model` nor `--catalog`). Such a plan has no concrete target to key a
/// policy decision on, so treating it as an empty touched set would vacuously
/// pass the gate. When a `[policy]` block is configured we therefore REFUSE a
/// targetless archive outright — a wildcard delete cannot be safely gated. With
/// no `[policy]` block there is nothing to enforce, so behaviour is unchanged
/// (the wildcard flows through to the warehouse, which rejects `DELETE FROM *`).
async fn prepare_archive_apply(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
) -> Result<(rocky_core::config::RockyConfig, Vec<NamedStatement>)> {
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

    // Phase C — "SQL as `.o` files" — v2 plans carry an `ArchivePlanIr`
    // envelope. The v1 inline-SQL envelope was retired in C-7; a v1-shaped
    // payload cannot be applied, so reject it before the gate rather than
    // deserializing a shape that no longer exists.
    match plan.format_version {
        1 => bail!(
            "archive plan '{}' is in format v1, which is no longer supported \
             (this plan was written by Rocky < engine-v1.35.0). \
             Re-run `rocky archive` to write a fresh v2 plan. \
             See https://docs.rocky-data.com/concepts/plan-store-v1-to-v2 \
             for the migration guide.",
            plan_id,
        ),
        2 => {}
        other => bail!(
            "archive plan '{}' has unknown format_version {}. \
             This binary supports v2 (typed ArchivePlanIr) persisted plans; \
             the file is from a newer engine — upgrade and re-apply.",
            plan_id,
            other
        ),
    }

    // THE single fingerprinted config snapshot for this apply (#1120): the
    // policy gate, the touched-target resolution, the dialect, and the adapter
    // the caller builds all read THIS instance.
    let loaded =
        rocky_core::config::load_rocky_config_fingerprinted(config_path).with_context(|| {
            format!(
                "refusing to apply archive plan '{plan_id}': failed to load config from {}",
                config_path.display()
            )
        })?;

    // Deserialize the typed IR once — it drives BOTH the policy touched set and
    // the SQL regeneration below.
    let ir_plans = extract_archive_ir_plans(plan_id, &plan.payload)?;

    // Targetless-archive guard: a `target_table = None` IR regenerates
    // `DELETE FROM *`. It has no concrete target to gate, so an enforced apply
    // must refuse it rather than let it vacuously pass an empty touched set.
    if loaded.config.policy.is_some() && ir_plans.iter().any(|ir| ir.target_table.is_none()) {
        bail!(
            "policy refuses archive plan '{plan_id}': it targets no concrete table \
             (a `DELETE FROM *` wildcard) and so cannot be gated under [policy]. \
             Re-scope the archive to an explicit `--model` or `--catalog` and re-plan."
        );
    }

    // Gate under `PolicyCapability::Apply`, keyed on the touched targets
    // (physical FQN → logical model name best-effort). The targetless guard
    // above means every IR here carries a concrete `target_table`.
    let touched = crate::commands::apply::resolve_touched_apply_targets(
        &loaded.config,
        config_path,
        ir_plans.iter().filter_map(|ir| ir.target_table.clone()),
    );
    crate::commands::apply::gate_maintenance_apply(
        root,
        &plan,
        plan_id,
        &loaded.config,
        config_path,
        state_path,
        runtime_principal,
        &touched,
    )
    .await?;

    // Resolve the dialect from the SAME snapshot (no reload) so SQL
    // regeneration is gated on the real warehouse: a non-Databricks adapter
    // fails fast with the known-limitation error.
    let dialect = crate::commands::plan::resolve_dialect_from_config(&loaded.config)?;
    let statements = archive_statements_from_ir(plan_id, &ir_plans, dialect.as_ref())?;

    Ok((loaded.config, statements))
}

/// Inner implementation of `rocky archive apply` that takes an explicit `root`
/// for the plans directory. Extracted so tests can pass a temp dir without
/// touching the process-global current working directory.
pub(crate) async fn run_archive_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
    output_json: bool,
) -> Result<()> {
    let (rocky_cfg, statements) =
        prepare_archive_apply(root, config_path, plan_id, state_path, runtime_principal).await?;

    // Build the warehouse adapter from the SAME snapshot the gate cleared, then
    // execute. Only reached once the policy gate has passed.
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_pipeline_name, pipeline) = crate::registry::resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline for archive apply")?;
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;

    execute_archive_apply(adapter.as_ref(), plan_id, &statements, output_json).await
}

/// Execute the regenerated archive statements against `adapter` and emit the
/// `ArchiveApplyOutput` envelope. Separated from the gate + adapter
/// construction so tests can drive the executed-statement path with an
/// injected recording adapter (see [`run_archive_apply_in_with`]).
async fn execute_archive_apply(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
    plan_id: &str,
    statements: &[NamedStatement],
    output_json: bool,
) -> Result<()> {
    let executed_at = Utc::now();
    let mut results: Vec<StatementResult> = Vec::with_capacity(statements.len());
    let mut overall_success = true;

    for stmt in statements {
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

/// [`run_archive_apply_in`] with an injected warehouse adapter — runs the SAME
/// policy gate (incl. the targetless-archive guard) + SQL regeneration, then
/// executes against `adapter` instead of building the real one. Tests use this
/// to prove a policy DENY refuses BEFORE any `execute_statement` reaches the
/// adapter (zero recorded statements), while an allowed apply reaches it.
#[cfg(test)]
pub(crate) async fn run_archive_apply_in_with(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
    output_json: bool,
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
) -> Result<()> {
    let (_rocky_cfg, statements) =
        prepare_archive_apply(root, config_path, plan_id, state_path, runtime_principal).await?;
    execute_archive_apply(adapter, plan_id, &statements, output_json).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Policy-gate parity coverage for `archive apply` (PR-1). Archive is the
    /// destructive kind (DELETE + VACUUM), so the deny/no-execution assertions
    /// carry the most weight. Covers BOTH routes (injectable sink + generic
    /// `apply <archive-plan>`), the downgraded-stamp case, and the
    /// targetless-archive (`DELETE FROM *`) refusal decision.
    mod policy_gate {
        use super::*;
        use crate::plan_store::write_plan_v2;
        use rocky_core::config::PolicyPrincipal;
        use rocky_core::traits::{AdapterResult, QueryResult, SqlDialect, WarehouseAdapter};
        use std::sync::Mutex;

        /// A warehouse adapter that records every statement it is asked to
        /// execute (and always succeeds). `dialect` / `execute_query` are never
        /// reached on the archive-apply path, so they panic if called.
        struct CountingAdapter {
            executed: Mutex<Vec<String>>,
        }
        impl CountingAdapter {
            fn new() -> Self {
                Self {
                    executed: Mutex::new(Vec::new()),
                }
            }
            fn count(&self) -> usize {
                self.executed.lock().unwrap().len()
            }
        }
        #[async_trait::async_trait]
        impl WarehouseAdapter for CountingAdapter {
            fn dialect(&self) -> &dyn SqlDialect {
                unimplemented!("archive apply execute path never calls dialect()")
            }
            async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
                self.executed.lock().unwrap().push(sql.to_string());
                Ok(())
            }
            async fn execute_query(&self, _sql: &str) -> AdapterResult<QueryResult> {
                unimplemented!("archive apply execute path never calls execute_query()")
            }
            async fn describe_table(
                &self,
                _table: &rocky_ir::TableRef,
            ) -> AdapterResult<Vec<rocky_ir::ColumnInfo>> {
                unimplemented!("archive apply execute path never calls describe_table()")
            }
        }

        /// A Databricks-typed config (so the Delta DELETE/VACUUM regenerates)
        /// with the given `[policy]` rules spliced in. Passing an empty rules
        /// string still emits a `[policy]` block (needed for the targetless
        /// guard, which fires whenever any policy is configured).
        fn write_config(dir: &Path, policy_rules: &str) -> std::path::PathBuf {
            let toml = format!(
                r#"
[adapter]
type = "databricks"
host = "https://example.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "pat-xxx"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "require_review"
{policy_rules}
"#
            );
            let path = dir.join("rocky.toml");
            std::fs::write(&path, toml).unwrap();
            path
        }

        /// A Databricks-typed config with NO `[policy]` block (nothing to
        /// enforce) — used to prove the targetless guard is policy-conditional.
        fn write_config_no_policy(dir: &Path) -> std::path::PathBuf {
            let toml = r#"
[adapter]
type = "databricks"
host = "https://example.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "pat-xxx"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true
"#;
            let path = dir.join("rocky.toml");
            std::fs::write(&path, toml).unwrap();
            path
        }

        const DENY_AGENT_APPLY: &str = r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "deny"
"#;

        /// Write a v2 archive plan for a single concrete target, returning its
        /// plan id. `write_plan_v2` stamps the kind default (`human`) — the
        /// benign stored stamp the enforcement path deliberately ignores.
        fn write_archive_plan(root: &Path, target: &str) -> String {
            let payload = serde_json::json!({
                "model": target,
                "catalog": null,
                "scope": null,
                "older_than": "90d",
                "older_than_days": 90,
                "dry_run": false,
                "plans": [ArchivePlanIr::for_table(target, 90)],
            });
            write_plan_v2(root, PlanKind::Archive, &payload).unwrap()
        }

        /// Write a v2 archive plan whose single IR is TARGETLESS
        /// (`target_table = None` → `DELETE FROM *`).
        fn write_targetless_archive_plan(root: &Path) -> String {
            let payload = serde_json::json!({
                "model": null,
                "catalog": null,
                "scope": null,
                "older_than": "30d",
                "older_than_days": 30,
                "dry_run": false,
                "plans": [build_archive_ir(None, 30)],
            });
            write_plan_v2(root, PlanKind::Archive, &payload).unwrap()
        }

        fn fresh_state(dir: &Path) -> std::path::PathBuf {
            let state_path = dir.join("state.redb");
            drop(rocky_core::state::StateStore::open(&state_path).unwrap());
            state_path
        }

        /// Direct-alias sink route: an agent applying a destructive archive plan
        /// under `deny agent apply` is REFUSED, and ZERO DELETE/VACUUM
        /// statements reach the adapter.
        #[tokio::test]
        async fn archive_apply_sink_denies_agent_and_executes_nothing() {
            let dir = tempfile::tempdir().unwrap();
            let config = write_config(dir.path(), DENY_AGENT_APPLY);
            let state = fresh_state(dir.path());
            let plan_id = write_archive_plan(dir.path(), "events");
            let adapter = CountingAdapter::new();

            let err = run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Agent,
                true,
                &adapter,
            )
            .await
            .expect_err("agent archive apply must be denied by policy");
            assert!(
                err.to_string().contains("policy DENIES"),
                "expected a policy-deny error, got: {err}"
            );
            assert_eq!(
                adapter.count(),
                0,
                "a denied archive apply must not reach execute_statement (no DELETE/VACUUM)"
            );
        }

        /// A human applying the SAME destructive plan under the SAME policy is
        /// allowed, and the DELETE/VACUUM statements DO reach the adapter.
        #[tokio::test]
        async fn archive_apply_sink_allows_human_and_executes() {
            let dir = tempfile::tempdir().unwrap();
            let config = write_config(dir.path(), DENY_AGENT_APPLY);
            let state = fresh_state(dir.path());
            let plan_id = write_archive_plan(dir.path(), "events");
            let adapter = CountingAdapter::new();

            run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Human,
                true,
                &adapter,
            )
            .await
            .expect("a human archive apply must be allowed");
            assert!(
                adapter.count() > 0,
                "an allowed archive apply must execute its statements"
            );
        }

        /// Enforcement uses the runtime principal + plan kind, never the stored
        /// stamp: a plan stamped `human` is STILL denied when an agent applies
        /// it.
        #[tokio::test]
        async fn archive_apply_ignores_downgraded_stored_stamp() {
            let dir = tempfile::tempdir().unwrap();
            let config = write_config(dir.path(), DENY_AGENT_APPLY);
            let state = fresh_state(dir.path());
            let plan_id = write_archive_plan(dir.path(), "events");
            let adapter = CountingAdapter::new();

            let err = run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Agent,
                true,
                &adapter,
            )
            .await
            .expect_err("a human-stamped plan applied by an agent must still be denied");
            assert!(err.to_string().contains("policy DENIES"), "got: {err}");
            assert_eq!(adapter.count(), 0);
        }

        /// Generic route (`rocky apply <archive-plan>` → `run_apply_in`): the
        /// documented-bug regression. The gate refuses BEFORE the real adapter
        /// is built, so no credentials are needed to observe the deny.
        #[tokio::test]
        async fn archive_apply_generic_route_denies_agent() {
            let dir = tempfile::tempdir().unwrap();
            let config = write_config(dir.path(), DENY_AGENT_APPLY);
            let state = fresh_state(dir.path());
            let plan_id = write_archive_plan(dir.path(), "events");

            let err = crate::commands::apply::run_apply_in(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Agent,
                true,
            )
            .await
            .expect_err("agent archive apply via the generic route must be denied");
            assert!(err.to_string().contains("policy DENIES"), "got: {err}");
        }

        /// Targetless-archive decision: a `DELETE FROM *` wildcard plan cannot
        /// be gated, so with a `[policy]` block configured it is REFUSED
        /// outright (never vacuously allowed), and nothing executes — even for a
        /// human, since the plan itself is un-gateable.
        #[tokio::test]
        async fn archive_apply_refuses_targetless_under_policy() {
            let dir = tempfile::tempdir().unwrap();
            // Any policy at all (even with no rules) activates the guard.
            let config = write_config(dir.path(), "");
            let state = fresh_state(dir.path());
            let plan_id = write_targetless_archive_plan(dir.path());
            let adapter = CountingAdapter::new();

            let err = run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Human,
                true,
                &adapter,
            )
            .await
            .expect_err("a targetless archive must be refused under policy");
            assert!(
                err.to_string().contains("no concrete table"),
                "expected the wildcard-refusal error, got: {err}"
            );
            assert_eq!(
                adapter.count(),
                0,
                "a refused targetless archive executes nothing"
            );
        }

        /// With NO `[policy]` block the targetless guard does NOT fire (there is
        /// nothing to enforce): the wildcard flows through to the adapter,
        /// preserving today's behaviour. Proves the guard is policy-conditional.
        #[tokio::test]
        async fn archive_apply_targetless_proceeds_without_policy() {
            let dir = tempfile::tempdir().unwrap();
            let config = write_config_no_policy(dir.path());
            let state = fresh_state(dir.path());
            let plan_id = write_targetless_archive_plan(dir.path());
            let adapter = CountingAdapter::new();

            run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Human,
                true,
                &adapter,
            )
            .await
            .expect("without policy the targetless archive is not gate-refused");
            assert!(
                adapter.count() > 0,
                "the un-gated wildcard still regenerates + reaches the adapter"
            );
        }

        /// End-to-end: a `require_review` verdict on a destructive archive apply
        /// is REFUSED without a marker and PROCEEDS once the real `rocky review
        /// --approve` path (`compute_review`) has written the sign-off — proving
        /// the verdict is clearable. Regression for the review-allowlist omission
        /// that stranded archive plans in RequireReview forever. Drives the
        /// actual review-compute function, not a hand-written marker file.
        #[tokio::test]
        async fn archive_apply_require_review_cleared_by_real_review_approve() {
            let dir = tempfile::tempdir().unwrap();
            // `default_agent_effect = require_review`, no explicit rule → an
            // agent apply resolves to RequireReview (a human stays Allow).
            let config = write_config(dir.path(), "");
            let state = fresh_state(dir.path());
            let plan_id = write_archive_plan(dir.path(), "events");

            // Without the marker: refused, no DELETE/VACUUM executed.
            let adapter = CountingAdapter::new();
            let err = run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Agent,
                true,
                &adapter,
            )
            .await
            .expect_err("agent archive apply must require review without a marker");
            assert!(
                err.to_string().contains("requires human review"),
                "got: {err}"
            );
            assert_eq!(adapter.count(), 0);

            // Drive the REAL `rocky review --approve` path (fails on a head that
            // omits Archive from the reviewable allowlist).
            crate::commands::review::compute_review(dir.path(), &config, &plan_id, "HEAD", true)
                .await
                .expect("`rocky review --approve` must accept an archive plan");

            // With the marker the destructive apply proceeds and executes.
            let adapter2 = CountingAdapter::new();
            run_archive_apply_in_with(
                dir.path(),
                &config,
                &plan_id,
                &state,
                PolicyPrincipal::Agent,
                true,
                &adapter2,
            )
            .await
            .expect("agent archive apply must proceed once the review marker is written");
            assert!(
                adapter2.count() > 0,
                "a reviewed archive apply must execute its statements"
            );
        }
    }

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
        let err = run_archive_apply_in(
            dir.path(),
            &config_path,
            &plan_id,
            &dir.path().join("state.redb"),
            rocky_core::config::PolicyPrincipal::Human,
            false,
        )
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

        let err = run_archive_apply_in(
            dir.path(),
            &config_path,
            &plan_id,
            &dir.path().join("state.redb"),
            rocky_core::config::PolicyPrincipal::Human,
            false,
        )
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
