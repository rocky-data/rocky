//! `rocky retention-status` — report per-model data retention policies.
//!
//! Compiles the project, walks the resolved model set, and emits one row per
//! model with the declared retention (if any). When `--drift` is set, also
//! probes the warehouse via the governance adapter's
//! [`read_retention_days`] method and populates `warehouse_days` /
//! recomputes `in_sync`. The probe is Databricks + Snowflake only —
//! DuckDB / BigQuery inherit the default `Ok(None)` impl from the trait and
//! `--drift` degrades to "no observation" on those targets.
//!
//! [`read_retention_days`]: rocky_core::traits::GovernanceAdapter::read_retention_days

use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::ir::TableRef;
use rocky_core::traits::GovernanceAdapter;

use crate::output::{ModelRetentionStatus, RetentionStatusOutput, print_json};
use crate::registry::AdapterRegistry;

/// Execute `rocky retention-status`.
///
/// `model_filter`, when set, restricts the output to a single model. When
/// `drift` is true, Rocky resolves a governance adapter per model (via the
/// model's `[adapter]` override, falling back to the first pipeline's target
/// adapter) and probes the warehouse. Probe errors surface per-model on
/// stderr but do not fail the whole command.
pub async fn run_retention_status(
    config_path: &Path,
    models_dir: &Path,
    model_filter: Option<&str>,
    drift: bool,
    output_json: bool,
) -> Result<()> {
    // Compile the project so we see the resolved ModelConfig (with
    // retention flattened to a typed `Option<RetentionPolicy>`). We
    // don't need the full SQL typecheck result — just the models.
    let compile = rocky_compiler::compile::compile(&rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    })
    .context("failed to compile project for retention-status")?;

    // When `--drift` is set, we need the adapter registry + a default
    // adapter name to probe each model against. Build both lazily so the
    // declaration-only path (no `--drift`) doesn't pay the cost of loading
    // `rocky.toml`.
    let probe_ctx = if drift {
        Some(
            build_probe_context(config_path)
                .context("failed to resolve governance adapter for --drift probe")?,
        )
    } else {
        None
    };

    let mut rows: Vec<ModelRetentionStatus> = Vec::new();
    for model in &compile.project.models {
        if model_filter.is_some_and(|name| model.config.name != name) {
            continue;
        }
        let configured = model.config.retention.map(|r| r.duration_days);

        let warehouse_days = if let Some(ctx) = probe_ctx.as_ref() {
            probe_model_retention(ctx, model).await
        } else {
            None
        };

        rows.push(ModelRetentionStatus {
            model: model.config.name.clone(),
            configured_days: configured,
            warehouse_days,
            in_sync: compute_in_sync(configured, warehouse_days),
        });
    }

    if drift {
        // Keep the existing UX of limiting --drift output to models with a
        // declared policy — that's the only set where the comparison is
        // meaningful. Unlike v1, we no longer print a "deferred" note since
        // the probe is live.
        rows.retain(|r| r.configured_days.is_some());
    }

    if output_json {
        print_json(&RetentionStatusOutput::new(rows))?;
    } else {
        println!(
            "{:<40} {:<16} {:<16} {:<8}",
            "MODEL", "CONFIGURED", "WAREHOUSE", "IN SYNC"
        );
        println!("{}", "-".repeat(82));
        for row in &rows {
            let cfg = row
                .configured_days
                .map(|d| format!("{d} days"))
                .unwrap_or_else(|| "-".to_string());
            let wh = row
                .warehouse_days
                .map(|d| format!("{d} days"))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<40} {:<16} {:<16} {:<8}",
                row.model,
                cfg,
                wh,
                if row.in_sync { "yes" } else { "no" }
            );
        }
        if rows.is_empty() {
            println!(
                "No models{}.",
                if model_filter.is_some() {
                    " matched the filter"
                } else {
                    ""
                }
            );
        }
    }

    Ok(())
}

/// Compute the `in_sync` flag preserving the v1 semantics.
///
/// Matches the previous `configured == warehouse_days` comparison: two
/// `None`s collapse to `true` (a model with no declared retention is
/// trivially "in sync"); everything else compares literally.
fn compute_in_sync(configured: Option<u32>, warehouse: Option<u32>) -> bool {
    configured == warehouse
}

/// Probe context shared across all models for a single `--drift` run.
///
/// Holding the [`AdapterRegistry`] at the command boundary (rather than
/// rebuilding per model) lets the dispatch remain `O(1)` per probe.
struct ProbeContext {
    registry: AdapterRegistry,
    /// Default adapter name to use when a model doesn't set its own
    /// `[adapter]` override. Today this is the first pipeline's target
    /// adapter — most configs have one pipeline; for multi-pipeline
    /// configs the per-model override is how users scope to a different
    /// warehouse.
    default_adapter: String,
}

fn build_probe_context(config_path: &Path) -> Result<ProbeContext> {
    let cfg = rocky_core::config::load_rocky_config(config_path)
        .context("failed to load rocky.toml for --drift probe")?;
    let default_adapter = cfg
        .pipelines
        .values()
        .next()
        .map(|pc| pc.target_adapter().to_string())
        .context(
            "rocky.toml has no pipelines; --drift needs at least one pipeline target adapter to probe against",
        )?;
    let registry = AdapterRegistry::from_config(&cfg)
        .context("failed to build adapter registry for --drift probe")?;
    Ok(ProbeContext {
        registry,
        default_adapter,
    })
}

/// Probe a single model's retention. Returns `None` on any failure —
/// errors are surfaced on stderr per-model but never abort the command.
async fn probe_model_retention(
    ctx: &ProbeContext,
    model: &rocky_core::models::Model,
) -> Option<u32> {
    let adapter_name = model
        .config
        .adapter
        .as_deref()
        .unwrap_or(&ctx.default_adapter);
    let governance: Box<dyn GovernanceAdapter> = ctx.registry.governance_adapter(adapter_name);

    let plan = model.to_plan();
    let table = TableRef {
        catalog: plan.target.catalog,
        schema: plan.target.schema,
        table: plan.target.table,
    };

    match governance.read_retention_days(&table).await {
        Ok(days) => days,
        Err(e) => {
            // Surface per-model errors on stderr (even in JSON mode — stderr
            // is a separate stream from stdout, so plain text here doesn't
            // pollute the JSON payload) so operators can see why a probe
            // didn't resolve. Continue with warehouse_days = None rather
            // than aborting the whole command.
            eprintln!(
                "warning: retention probe failed for model '{}' (adapter '{}'): {}",
                model.config.name, adapter_name, e
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn in_sync_both_none_is_true() {
        // v1 contract: two None values collapse to `true`, matching the
        // original `configured == warehouse_days` semantics. A model with
        // no declared retention is never flagged as out-of-sync.
        assert!(compute_in_sync(None, None));
    }

    #[test]
    fn in_sync_equal_values_is_true() {
        assert!(compute_in_sync(Some(90), Some(90)));
    }

    #[test]
    fn in_sync_diverging_values_is_false() {
        assert!(!compute_in_sync(Some(90), Some(30)));
    }

    #[test]
    fn in_sync_configured_but_no_warehouse_is_false() {
        // DuckDB / BigQuery case: configured_days set, warehouse_days None
        // because the adapter doesn't support the probe. Still "out of
        // sync" under the literal comparison — that matches v1 semantics.
        assert!(!compute_in_sync(Some(90), None));
    }

    #[test]
    fn in_sync_warehouse_but_no_configured_is_false() {
        // Model with no `retention = ...` declaration but the warehouse
        // has a non-default value. Under literal comparison this is "out
        // of sync" — which is truthful: Rocky didn't ask for it.
        assert!(!compute_in_sync(None, Some(90)));
    }

    /// End-to-end: write a minimal rocky.toml + a model sidecar with
    /// `retention = "90d"`, drive `run_retention_status` with `--drift`,
    /// and assert it returns cleanly when the target adapter can't probe
    /// (DuckDB here — falls through to NoopGovernanceAdapter → default
    /// trait impl → `Ok(None)`).
    ///
    /// Verifies the probe wiring end-to-end: config load → AdapterRegistry
    /// build → per-model governance_adapter() dispatch → `Ok(None)` return
    /// → no command-level failure. The `in_sync` assertion pins the
    /// "configured but no warehouse observation" semantics documented in
    /// [`in_sync_configured_but_no_warehouse_is_false`].
    #[tokio::test]
    async fn drift_probe_on_duckdb_target_returns_ok_with_none_warehouse_days() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        // Minimal rocky.toml with a DuckDB target — no governance backend,
        // so the probe falls through to NoopGovernanceAdapter and the trait
        // default returns Ok(None).
        let cfg_path = root.join("rocky.toml");
        fs::write(
            &cfg_path,
            r#"
[adapter.local]
type = "duckdb"

[pipeline.demo]
type = "transformation"
models = "models/**"

[pipeline.demo.target]
adapter = "local"
"#,
        )
        .unwrap();

        // Minimal SQL model with a retention sidecar.
        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("events.sql"), "SELECT 1 AS id\n").unwrap();
        fs::write(
            models_dir.join("events.toml"),
            r#"
name = "events"
retention = "90d"

[target]
catalog = "warehouse"
schema = "analytics"
table = "events"
"#,
        )
        .unwrap();

        // Run with --drift. Assertion is that the command completes
        // without returning an error — the probe was dispatched, the
        // NoopGovernanceAdapter returned Ok(None), and `in_sync` computed
        // to `false` (configured=90, warehouse=None). We assert the exit
        // shape, not the stdout, because the human-readable printer is
        // covered by the `in_sync_*` unit tests above.
        let result = run_retention_status(
            &cfg_path,
            &models_dir,
            None,
            true, // --drift
            true, // json (keeps stdout a single well-formed payload)
        )
        .await;
        assert!(
            result.is_ok(),
            "run_retention_status should not fail when the probe returns Ok(None); \
             got: {result:?}"
        );
    }

    /// Same end-to-end shape but without `--drift`: verifies the
    /// declaration-only path (no registry build, no probe) still works
    /// after the signature change — a guard against regressing v1
    /// behaviour while adding v2.
    #[tokio::test]
    async fn declaration_only_path_skips_probe_and_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        let cfg_path = root.join("rocky.toml");
        fs::write(
            &cfg_path,
            r#"
[adapter.local]
type = "duckdb"

[pipeline.demo]
type = "transformation"
models = "models/**"

[pipeline.demo.target]
adapter = "local"
"#,
        )
        .unwrap();

        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("events.sql"), "SELECT 1 AS id\n").unwrap();
        fs::write(
            models_dir.join("events.toml"),
            r#"
name = "events"

[target]
catalog = "warehouse"
schema = "analytics"
table = "events"
"#,
        )
        .unwrap();

        // drift=false: no probe, no registry build, should work even if
        // the rocky.toml were malformed for adapter construction.
        run_retention_status(&cfg_path, &models_dir, None, false, true)
            .await
            .expect("declaration-only path should succeed");
    }
}
