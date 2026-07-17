use std::path::Path;

use anyhow::Result;

use crate::output::{
    ValidateAdapterStatus, ValidateMessage, ValidateModelsStatus, ValidateOutput,
    ValidatePipelineStatus, print_json,
};

/// Execute `rocky validate` — check config without APIs.
///
/// Returns a structured `ValidateOutput` that can be rendered as text
/// (default) or JSON (`--output json`).
pub fn validate(config_path: &Path, json: bool) -> Result<()> {
    let output = validate_inner(config_path)?;

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    if !output.valid {
        std::process::exit(1);
    }

    Ok(())
}

fn validate_inner(config_path: &Path) -> Result<ValidateOutput> {
    let mut out = ValidateOutput::new();

    // Parse config (with env var substitution) — use the lenient parser
    // so every `kind`-field issue surfaces as its own V032 / V033
    // diagnostic instead of the V001 catch-all bailing on the first one.
    // parse_rocky_config returns ConfigError::FileNotFound for missing
    // files, which the CLI error reporter upgrades to a rich miette
    // diagnostic with `rocky init` / `rocky playground` hints.
    let cfg = match rocky_core::config::parse_rocky_config(config_path) {
        Ok(cfg) => {
            out.push(ValidateMessage {
                severity: "ok".into(),
                code: "V001".into(),
                message: "Config syntax valid (v2 format)".into(),
                file: Some(config_path.display().to_string()),
                field: None,
            });
            cfg
        }
        Err(e) => {
            out.push(ValidateMessage {
                severity: "error".into(),
                code: "V001".into(),
                message: format!("Failed to parse config: {e}"),
                file: Some(config_path.display().to_string()),
                field: None,
            });
            return Ok(out);
        }
    };

    // Emit a structured diagnostic for every adapter-kind / pipeline-role
    // issue. V032 covers `[adapter.*]` `kind` invariants; V033 covers
    // `source.adapter` / `source.discovery.adapter` role mismatches.
    for err in rocky_core::config::validate_adapter_kinds(&cfg) {
        out.push(kind_diagnostic(&err, config_path));
    }

    // Validate adapters
    if cfg.adapters.is_empty() {
        out.push(ValidateMessage {
            severity: "warn".into(),
            code: "V010".into(),
            message: "No adapters defined".into(),
            file: None,
            field: Some("adapter".into()),
        });
    } else {
        for (name, adapter) in &cfg.adapters {
            let (ok, msgs) = validate_adapter(name, adapter);
            out.adapters.push(ValidateAdapterStatus {
                name: name.clone(),
                adapter_type: adapter.adapter_type.clone(),
                ok,
            });
            for msg in msgs {
                out.push(msg);
            }
        }
    }

    // Validate pipelines
    if cfg.pipelines.is_empty() {
        out.push(ValidateMessage {
            severity: "warn".into(),
            code: "V020".into(),
            message: "No pipelines defined".into(),
            file: None,
            field: Some("pipeline".into()),
        });
    } else {
        for (name, pc) in &cfg.pipelines {
            let pipeline_type = pc.pipeline_type_str().to_string();
            let (ok, msgs, strategy, catalog_template, schema_template) = match pc {
                rocky_core::config::PipelineConfig::Replication(pipeline) => {
                    let (ok, msgs) = validate_replication_pipeline(name, pipeline, &cfg);
                    (
                        ok,
                        msgs,
                        pipeline.strategy.clone(),
                        pipeline.target.catalog_template.clone(),
                        pipeline.target.schema_template.clone(),
                    )
                }
                rocky_core::config::PipelineConfig::Transformation(pipeline) => {
                    let (ok, msgs) =
                        validate_transformation_pipeline(name, pipeline, &cfg, config_path);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Quality(pipeline) => {
                    let (ok, msgs) = validate_quality_pipeline(name, pipeline, &cfg);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Snapshot(pipeline) => {
                    let (ok, msgs) = validate_snapshot_pipeline(name, pipeline, &cfg);
                    (ok, msgs, String::new(), String::new(), String::new())
                }
                rocky_core::config::PipelineConfig::Load(_pipeline) => {
                    // Load pipeline validation not yet implemented
                    (true, vec![], String::new(), String::new(), String::new())
                }
            };
            out.pipelines.push(ValidatePipelineStatus {
                name: name.clone(),
                pipeline_type,
                strategy,
                catalog_template,
                schema_template,
                ok,
            });
            for msg in msgs {
                out.push(msg);
            }

            // Warn on check/strategy config a pipeline type won't act on, so
            // silently-dead config is caught at validate time rather than after
            // a clean run that never executed the guard.
            for msg in inert_config_messages(name, pc) {
                out.push(msg);
            }

            // Validate the optional `[schedule]` block (native demand
            // reconciliation) — invalid cron/tz/catchup/freshness, unknown
            // after targets, inert blocks, and the run-record persistence gap.
            for msg in schedule_messages(name, pc, &cfg) {
                out.push(msg);
            }
        }

        // Validate pipeline dependency graph (depends_on)
        validate_pipeline_dag(&cfg, &mut out);

        // Validate the schedule `after` graph is acyclic (mutual `after` hangs).
        validate_schedule_dag(&cfg, &mut out);
    }

    // Validate models directory if it exists
    let models_dir = config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("models");
    let loaded_models = if models_dir.exists() {
        match crate::models_loader::load_project_models(&models_dir) {
            Ok(models) => {
                let count = models.len();
                if count > 0 {
                    out.push(ValidateMessage {
                        severity: "ok".into(),
                        code: "V030".into(),
                        message: format!("{count} transformation models loaded"),
                        file: None,
                        field: None,
                    });

                    // Validate DAG
                    let dag_nodes: Vec<_> = models
                        .iter()
                        .map(rocky_core::models::Model::to_dag_node)
                        .collect();
                    match rocky_ir::dag::topological_sort(&dag_nodes) {
                        Ok(order) => {
                            out.models = ValidateModelsStatus {
                                found: true,
                                count,
                                dag_valid: true,
                            };
                            out.push(ValidateMessage {
                                severity: "ok".into(),
                                code: "V031".into(),
                                message: format!("DAG valid ({} nodes, no cycles)", order.len()),
                                file: None,
                                field: None,
                            });
                        }
                        Err(e) => {
                            out.models = ValidateModelsStatus {
                                found: true,
                                count,
                                dag_valid: false,
                            };
                            out.push(ValidateMessage {
                                severity: "error".into(),
                                code: "V031".into(),
                                message: format!("DAG error: {e}"),
                                file: None,
                                field: None,
                            });
                        }
                    }
                } else {
                    out.models = ValidateModelsStatus {
                        found: true,
                        count: 0,
                        dag_valid: true,
                    };
                }
                models
            }
            Err(e) => {
                out.models = ValidateModelsStatus {
                    found: true,
                    count: 0,
                    dag_valid: false,
                };
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!("Model loading error: {e}"),
                    file: None,
                    field: None,
                });
                vec![]
            }
        }
    } else {
        vec![]
    };

    // --- Lint rules ---
    lint_config(&cfg, &loaded_models, &mut out);

    Ok(out)
}

/// Converts a `kind`-validation error into a `ValidateMessage` with a
/// structured V-code and a `field` path that points an IDE at the
/// offending key in `rocky.toml`.
fn kind_diagnostic(err: &rocky_core::config::ConfigError, config_path: &Path) -> ValidateMessage {
    use rocky_core::config::ConfigError;

    let (code, field) = match err {
        ConfigError::AdapterMissingDiscoveryKind { name, .. }
        | ConfigError::AdapterKindUnsupported { name, .. } => {
            ("V032", Some(format!("adapter.{name}.kind")))
        }
        ConfigError::PipelineSourceAdapterNotData { pipeline, .. } => {
            ("V033", Some(format!("pipeline.{pipeline}.source.adapter")))
        }
        ConfigError::PipelineDiscoveryAdapterNotDiscovery { pipeline, .. } => (
            "V033",
            Some(format!("pipeline.{pipeline}.source.discovery.adapter")),
        ),
        _ => ("V001", None),
    };

    ValidateMessage {
        severity: "error".into(),
        code: code.into(),
        message: err.to_string(),
        file: Some(config_path.display().to_string()),
        field,
    }
}

fn validate_adapter(
    name: &str,
    adapter: &rocky_core::config::AdapterConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    match adapter.adapter_type.as_str() {
        "databricks" => {
            if adapter.host.is_none() || adapter.host.as_deref() == Some("") {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V011".into(),
                    message: format!("adapter.{name}: host not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.host")),
                });
            }
            if adapter.http_path.is_none() || adapter.http_path.as_deref() == Some("") {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V012".into(),
                    message: format!("adapter.{name}: http_path not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.http_path")),
                });
            }
            if adapter.token.is_none() && adapter.client_id.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V013".into(),
                    message: format!(
                        "adapter.{name}: no auth configured (token or client_id/secret)"
                    ),
                    file: None,
                    field: Some(format!("adapter.{name}")),
                });
            }
            if ok {
                msgs.push(ValidateMessage {
                    severity: "ok".into(),
                    code: "V010".into(),
                    message: format!("adapter.{name}: databricks (auth configured)"),
                    file: None,
                    field: None,
                });
            }
        }
        "fivetran" => {
            if adapter.api_key.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V014".into(),
                    message: format!("adapter.{name}: api_key not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.api_key")),
                });
            }
            if adapter.api_secret.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V015".into(),
                    message: format!("adapter.{name}: api_secret not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.api_secret")),
                });
            }
            if adapter.destination_id.is_none() {
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "warn".into(),
                    code: "V016".into(),
                    message: format!("adapter.{name}: destination_id not set"),
                    file: None,
                    field: Some(format!("adapter.{name}.destination_id")),
                });
            }
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: fivetran"),
                file: None,
                field: None,
            });
        }
        "manual" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: manual"),
                file: None,
                field: None,
            });
        }
        "duckdb" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: duckdb (local)"),
                file: None,
                field: None,
            });
        }
        "snowflake" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: snowflake"),
                file: None,
                field: None,
            });
        }
        "bigquery" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: bigquery"),
                file: None,
                field: None,
            });
        }
        "trino" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: trino"),
                file: None,
                field: None,
            });
        }
        "airbyte" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: airbyte"),
                file: None,
                field: None,
            });
        }
        "iceberg" => {
            msgs.push(ValidateMessage {
                severity: "ok".into(),
                code: "V010".into(),
                message: format!("adapter.{name}: iceberg"),
                file: None,
                field: None,
            });
        }
        other => {
            // Catchall: types the registry knows how to construct but
            // for which `validate_adapter` has no per-type credential
            // checks (V011-V016) emit a generic V010 ok rather than a
            // cosmetic V017. Genuinely unrecognised types still warn.
            // Driving this off `AdapterRegistry::is_known` keeps V017
            // drift-proof against future adapter additions — adding a
            // new dispatch arm in `registry.rs` is enough.
            if crate::registry::AdapterRegistry::is_known(other) {
                msgs.push(ValidateMessage {
                    severity: "ok".into(),
                    code: "V010".into(),
                    message: format!("adapter.{name}: {other}"),
                    file: None,
                    field: None,
                });
            } else {
                // Unknown adapter types are a hard error, not a cosmetic
                // warning: `rocky run` rejects them outright (registry.rs
                // `from_config` bails), so `rocky validate` must agree and
                // set `valid = false`. Reuse the same supported-types list
                // and `did_you_mean` suggestion the runtime error uses.
                use crate::error_reporter::{self, KNOWN_ADAPTER_TYPES};
                let mut message = format!(
                    "adapter.{name}: unknown type '{other}'. Supported: {}",
                    KNOWN_ADAPTER_TYPES.join(", "),
                );
                if let Some(suggestion) = error_reporter::did_you_mean(other, KNOWN_ADAPTER_TYPES) {
                    message.push_str(&format!(". Did you mean '{suggestion}'?"));
                }
                ok = false;
                msgs.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V017".into(),
                    message,
                    file: None,
                    field: Some(format!("adapter.{name}.type")),
                });
            }
        }
    }

    (ok, msgs)
}

/// Warnings for check/strategy config a pipeline's runner will not act on.
/// Driven by [`PipelineConfig::executed_check_kinds`] (the shared source of
/// truth) so the lint can never claim a check runs that the runner skips.
fn inert_config_messages(
    name: &str,
    pc: &rocky_core::config::PipelineConfig,
) -> Vec<ValidateMessage> {
    let mut msgs = Vec::new();

    // Configured checks the runner for this pipeline type never executes.
    let executed = pc.executed_check_kinds();
    for kind in pc.checks().configured_explicit_kinds() {
        if !executed.contains(&kind) {
            let field = check_kind_field(kind);
            msgs.push(ValidateMessage {
                severity: "warn".into(),
                code: "V034".into(),
                message: format!(
                    "pipeline.{name}: checks.{field} is configured but a {} pipeline does not execute it — the check never runs (no-op). Move it to a pipeline type that runs it, or remove it.",
                    pc.pipeline_type_str()
                ),
                file: None,
                field: Some(format!("pipeline.{name}.checks.{field}")),
            });
        }
    }

    // A replication strategy typo parses cleanly and silently falls back to
    // full_refresh at run time — surface it at both the pipeline level and in
    // each `[[table_overrides]]` (the override flows through the same fallback).
    if let Some(repl) = pc.as_replication() {
        let recognized =
            |s: &str| rocky_core::config::RECOGNIZED_REPLICATION_STRATEGIES.contains(&s);
        let unrecognized = |strategy: &str, field: String| ValidateMessage {
            severity: "warn".into(),
            code: "V035".into(),
            message: format!(
                "pipeline.{name}: strategy \"{strategy}\" is not a recognized replication strategy and will silently fall back to full_refresh at run time. Recognized: {}.",
                rocky_core::config::RECOGNIZED_REPLICATION_STRATEGIES.join(", ")
            ),
            file: None,
            field: Some(field),
        };
        if !recognized(&repl.strategy) {
            msgs.push(unrecognized(
                &repl.strategy,
                format!("pipeline.{name}.strategy"),
            ));
        }
        for (i, ov) in repl.table_overrides.iter().enumerate() {
            if let Some(s) = ov.strategy.as_deref()
                && !recognized(s)
            {
                msgs.push(unrecognized(
                    s,
                    format!("pipeline.{name}.table_overrides[{i}].strategy"),
                ));
            }
        }
    }

    msgs
}

/// Whether a pipeline type persists a `RunRecord` on a successful run — the
/// ground truth `after`/`freshness` demands read. Replication and transformation
/// do; quality, snapshot, and load do not (yet), so a demand that reads one of
/// their records would wait forever. Surfaced as an experimental limitation.
fn schedule_persists_run_records(pc: &rocky_core::config::PipelineConfig) -> bool {
    use rocky_core::config::PipelineConfig;
    matches!(
        pc,
        PipelineConfig::Replication(_) | PipelineConfig::Transformation(_)
    )
}

/// Validation for a pipeline's optional `[schedule]` block (native demand
/// reconciliation). Core validity is driven by the reconciler's own
/// [`resolve_schedule`](rocky_core::schedule::resolve_schedule) so `rocky
/// validate` and `rocky tick` never disagree on what is invalid; the remaining
/// checks catch unknown `after` targets, inert blocks, and the run-record
/// persistence gap.
fn schedule_messages(
    name: &str,
    pc: &rocky_core::config::PipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> Vec<ValidateMessage> {
    use rocky_core::schedule::{ScheduleConfigError, resolve_schedule};

    let Some(sched) = pc.schedule() else {
        return Vec::new();
    };
    let mut msgs = Vec::new();
    let field = format!("pipeline.{name}.schedule");

    // Core validity — the same resolution the reconciler runs. Reports the first
    // blocking error found.
    if let Err(e) = resolve_schedule(
        name,
        sched,
        &cfg.schedule.timezone,
        &[],
        cfg.freshness.expected_lag_seconds,
    ) {
        let (code, message) = match e {
            ScheduleConfigError::BadCron(expr) => (
                "V036",
                format!(
                    "pipeline.{name}: schedule.cron \"{expr}\" is not a valid cron expression."
                ),
            ),
            ScheduleConfigError::BadTimezone(tz) => (
                "V036",
                format!(
                    "pipeline.{name}: schedule.timezone \"{tz}\" is not a known IANA timezone name."
                ),
            ),
            ScheduleConfigError::BadCatchup(v) if v == "all" => (
                "V037",
                format!(
                    "pipeline.{name}: schedule.catchup = \"all\" is not supported — Rocky runs are watermark-driven, not windowed, so replaying every missed occurrence is pure cost. Use \"latest\" (default: one run at the most recent missed occurrence) or \"skip\"."
                ),
            ),
            ScheduleConfigError::BadCatchup(v) => (
                "V037",
                format!(
                    "pipeline.{name}: schedule.catchup \"{v}\" is invalid — expected \"latest\" or \"skip\"."
                ),
            ),
            ScheduleConfigError::FreshnessNoBudget => (
                "V038",
                format!(
                    "pipeline.{name}: schedule.freshness = true but no freshness budget resolves — set [freshness].expected_lag_seconds or a per-model max_lag_seconds."
                ),
            ),
        };
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: code.into(),
            message,
            file: None,
            field: Some(field.clone()),
        });
    }

    // Unknown `after` targets.
    for up in &sched.after {
        if !cfg.pipelines.contains_key(up) {
            msgs.push(ValidateMessage {
                severity: "error".into(),
                code: "V039".into(),
                message: format!(
                    "pipeline.{name}: schedule.after references unknown pipeline \"{up}\"."
                ),
                file: None,
                field: Some(format!("{field}.after")),
            });
        }
    }

    // An enabled schedule with no demand source is inert (never scheduled).
    if sched.enabled && sched.cron.is_none() && sched.after.is_empty() && !sched.freshness {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V040".into(),
            message: format!(
                "pipeline.{name}: [schedule] declares no cron, after, or freshness — the block is inert and the pipeline is never scheduled. Add a demand source or remove the block."
            ),
            file: None,
            field: Some(field.clone()),
        });
    }

    // Run-record persistence gap (experimental limitation): an `after` on, or
    // `freshness = true` for, a pipeline type that persists no run record cannot
    // observe the success it waits on, so the demand never fires.
    for up in &sched.after {
        if let Some(up_pc) = cfg.pipelines.get(up)
            && !schedule_persists_run_records(up_pc)
        {
            msgs.push(ValidateMessage {
                severity: "warn".into(),
                code: "V041".into(),
                message: format!(
                    "pipeline.{name}: schedule.after references \"{up}\", a {} pipeline that does not persist a run record — this `after` demand cannot observe its success and will not fire (experimental limitation).",
                    up_pc.pipeline_type_str()
                ),
                file: None,
                field: Some(format!("{field}.after")),
            });
        }
    }
    if sched.freshness && !schedule_persists_run_records(pc) {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V041".into(),
            message: format!(
                "pipeline.{name}: schedule.freshness = true on a {} pipeline that does not persist a run record — its run-staleness cannot be observed, so the demand will not fire (experimental limitation).",
                pc.pipeline_type_str()
            ),
            file: None,
            field: Some(format!("{field}.freshness")),
        });
    }

    msgs
}

/// Validates the schedule `after` graph is acyclic — a mutual `after` would make
/// the reconciler refuse to run rather than pick an ambiguous order. Only edges
/// to existing pipelines are considered (unknown targets are the V039 error).
fn validate_schedule_dag(cfg: &rocky_core::config::RockyConfig, out: &mut ValidateOutput) {
    let names: std::collections::HashSet<&str> = cfg.pipelines.keys().map(String::as_str).collect();
    let mut has_after = false;
    let dag_nodes: Vec<rocky_ir::dag::DagNode> = cfg
        .pipelines
        .iter()
        .map(|(name, pc)| {
            let after: Vec<String> = pc
                .schedule()
                .map(|s| {
                    s.after
                        .iter()
                        .filter(|u| names.contains(u.as_str()))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            if !after.is_empty() {
                has_after = true;
            }
            rocky_ir::dag::DagNode {
                name: name.clone(),
                depends_on: after,
            }
        })
        .collect();
    if !has_after {
        return;
    }
    if let Err(e) = rocky_ir::dag::topological_sort(&dag_nodes) {
        out.push(ValidateMessage {
            severity: "error".into(),
            code: "V042".into(),
            message: format!(
                "schedule.after graph has a cycle — a pipeline cannot run after itself, transitively ({e})."
            ),
            file: None,
            field: None,
        });
    }
}

/// The `[checks]` sub-field name for a [`CheckKind`], used to point the
/// inert-config warning at the offending key.
fn check_kind_field(kind: rocky_core::checks::CheckKind) -> &'static str {
    use rocky_core::checks::CheckKind;
    match kind {
        CheckKind::RowCount => "row_count",
        CheckKind::ColumnMatch => "column_match",
        CheckKind::Freshness => "freshness",
        CheckKind::NullRate => "null_rate",
        CheckKind::Custom => "custom",
        CheckKind::CrossSourceOverlap => "cross_source_overlap",
        CheckKind::Assertions => "assertions",
        CheckKind::Anomaly => "anomaly_threshold_pct",
    }
}

fn validate_replication_pipeline(
    name: &str,
    pipeline: &rocky_core::config::ReplicationPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Validate schema pattern
    match pipeline.schema_pattern() {
        Ok(_) => msgs.push(ValidateMessage {
            severity: "ok".into(),
            code: "V021".into(),
            message: format!("pipeline.{name}: schema pattern parseable"),
            file: None,
            field: None,
        }),
        Err(e) => {
            ok = false;
            msgs.push(ValidateMessage {
                severity: "error".into(),
                code: "V021".into(),
                message: format!("pipeline.{name}: schema pattern error: {e}"),
                file: None,
                field: Some(format!("pipeline.{name}.source.schema_pattern")),
            });
        }
    }

    // Check adapter references
    if !cfg.adapters.contains_key(&pipeline.source.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V022".into(),
            message: format!(
                "pipeline.{name}: source adapter '{}' not found in [adapter]",
                pipeline.source.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.source.adapter")),
        });
    }
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }
    if let Some(ref disc) = pipeline.source.discovery
        && !cfg.adapters.contains_key(&disc.adapter)
    {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V024".into(),
            message: format!(
                "pipeline.{name}: discovery adapter '{}' not found in [adapter]",
                disc.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.source.discovery.adapter")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: replication / {} -> {} / {}",
            pipeline.strategy, pipeline.target.catalog_template, pipeline.target.schema_template,
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_transformation_pipeline(
    name: &str,
    pipeline: &rocky_core::config::TransformationPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
    config_path: &Path,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    // Check that the models base directory exists
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    // Extract the directory portion of the glob (e.g., "models" from "models/**")
    let models_base = pipeline
        .models
        .split("**")
        .next()
        .unwrap_or(&pipeline.models)
        .trim_end_matches('/');
    let models_path = config_dir.join(models_base);
    if !models_path.exists() {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V025".into(),
            message: format!(
                "pipeline.{name}: models directory '{}' does not exist",
                models_base
            ),
            file: None,
            field: Some(format!("pipeline.{name}.models")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: transformation / models='{}'",
            pipeline.models
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_quality_pipeline(
    name: &str,
    pipeline: &rocky_core::config::QualityPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    if pipeline.tables.is_empty() {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V026".into(),
            message: format!("pipeline.{name}: no tables defined — nothing to check"),
            file: None,
            field: Some(format!("pipeline.{name}.tables")),
        });
    }

    if !pipeline.checks.enabled {
        msgs.push(ValidateMessage {
            severity: "warn".into(),
            code: "V027".into(),
            message: format!(
                "pipeline.{name}: checks.enabled is false — quality pipeline has no effect"
            ),
            file: None,
            field: Some(format!("pipeline.{name}.checks.enabled")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: quality / {} table ref(s)",
            pipeline.tables.len()
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

fn validate_snapshot_pipeline(
    name: &str,
    pipeline: &rocky_core::config::SnapshotPipelineConfig,
    cfg: &rocky_core::config::RockyConfig,
) -> (bool, Vec<ValidateMessage>) {
    let mut msgs = Vec::new();
    let mut ok = true;

    // Check source adapter exists
    if !cfg.adapters.contains_key(&pipeline.source.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V022".into(),
            message: format!(
                "pipeline.{name}: source adapter '{}' not found in [adapter]",
                pipeline.source.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.source.adapter")),
        });
    }

    // Check target adapter exists
    if !cfg.adapters.contains_key(&pipeline.target.adapter) {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V023".into(),
            message: format!(
                "pipeline.{name}: target adapter '{}' not found in [adapter]",
                pipeline.target.adapter
            ),
            file: None,
            field: Some(format!("pipeline.{name}.target.adapter")),
        });
    }

    if pipeline.unique_key.is_empty() {
        ok = false;
        msgs.push(ValidateMessage {
            severity: "error".into(),
            code: "V028".into(),
            message: format!("pipeline.{name}: unique_key must not be empty"),
            file: None,
            field: Some(format!("pipeline.{name}.unique_key")),
        });
    }

    msgs.push(ValidateMessage {
        severity: "ok".into(),
        code: "V020".into(),
        message: format!(
            "pipeline.{name}: snapshot / {}.{}.{} -> {}.{}.{}",
            pipeline.source.catalog,
            pipeline.source.schema,
            pipeline.source.table,
            pipeline.target.catalog,
            pipeline.target.schema,
            pipeline.target.table,
        ),
        file: None,
        field: None,
    });

    (ok, msgs)
}

/// Validates the pipeline dependency graph (depends_on references).
///
/// Checks that all referenced pipeline names exist and that the graph is acyclic.
fn validate_pipeline_dag(cfg: &rocky_core::config::RockyConfig, out: &mut ValidateOutput) {
    let pipeline_names: std::collections::HashSet<&str> = cfg
        .pipelines
        .keys()
        .map(std::string::String::as_str)
        .collect();

    let mut has_deps = false;

    // Check all depends_on references exist
    for (name, pc) in &cfg.pipelines {
        for dep in pc.depends_on() {
            has_deps = true;
            if !pipeline_names.contains(dep.as_str()) {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V029".into(),
                    message: format!(
                        "pipeline.{name}: depends_on references unknown pipeline '{dep}'"
                    ),
                    file: None,
                    field: Some(format!("pipeline.{name}.depends_on")),
                });
                out.valid = false;
            }
        }
    }

    // Check for cycles using the existing DAG module
    if has_deps {
        let dag_nodes: Vec<rocky_ir::dag::DagNode> = cfg
            .pipelines
            .iter()
            .map(|(name, pc)| rocky_ir::dag::DagNode {
                name: name.clone(),
                depends_on: pc.depends_on().to_vec(),
            })
            .collect();

        match rocky_ir::dag::topological_sort(&dag_nodes) {
            Ok(order) => {
                out.push(ValidateMessage {
                    severity: "ok".into(),
                    code: "V030".into(),
                    message: format!(
                        "pipeline dependency graph is acyclic (execution order: {})",
                        order.join(" -> ")
                    ),
                    file: None,
                    field: None,
                });
            }
            Err(rocky_ir::dag::DagError::CyclicDependency { nodes }) => {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!(
                        "pipeline dependency cycle detected involving: {}",
                        nodes.join(", ")
                    ),
                    file: None,
                    field: Some("pipeline.*.depends_on".into()),
                });
                out.valid = false;
            }
            Err(e) => {
                out.push(ValidateMessage {
                    severity: "error".into(),
                    code: "V030".into(),
                    message: format!("pipeline dependency graph error: {e}"),
                    file: None,
                    field: Some("pipeline.*.depends_on".into()),
                });
                out.valid = false;
            }
        }
    }
}

/// Lint rules: warn about redundant fields, no-op defaults, and opportunities
/// to simplify the configuration.
fn lint_config(
    cfg: &rocky_core::config::RockyConfig,
    models: &[rocky_core::models::Model],
    out: &mut ValidateOutput,
) {
    use std::collections::HashMap;

    // L004: [state] backend="local" is the default
    if cfg.state.backend == rocky_core::config::StateBackend::Local {
        out.push(ValidateMessage {
            severity: "lint".into(),
            code: "L004".into(),
            message: "[state] backend='local' is the default — you can omit it".into(),
            file: None,
            field: Some("state.backend".into()),
        });
    }

    // Per-pipeline lint rules (dispatch by pipeline type)
    for (name, pc) in &cfg.pipelines {
        if let Some(pipeline) = pc.as_replication() {
            // L006: auto_create_catalogs/schemas = false is the default
            if !pipeline.target.governance.auto_create_catalogs {
                out.push(ValidateMessage {
                    severity: "lint".into(),
                    code: "L006".into(),
                    message: format!("pipeline.{name}: auto_create_catalogs=false is the default — you can omit it"),
                    file: None,
                    field: Some(format!("pipeline.{name}.target.governance.auto_create_catalogs")),
                });
            }
            if !pipeline.target.governance.auto_create_schemas {
                out.push(ValidateMessage {
                    severity: "lint".into(),
                    code: "L006".into(),
                    message: format!("pipeline.{name}: auto_create_schemas=false is the default — you can omit it"),
                    file: None,
                    field: Some(format!("pipeline.{name}.target.governance.auto_create_schemas")),
                });
            }
        }
    }

    // L007: single-adapter project repeats adapter name
    if cfg.adapters.len() == 1 {
        let adapter_name = cfg
            .adapters
            .keys()
            .next()
            .expect("len == 1 guarantees one key");
        let mut ref_count = 0usize;
        for pc in cfg.pipelines.values() {
            // Count adapter references across all pipeline types
            if pc.target_adapter() == adapter_name {
                ref_count += 1;
            }
            if let Some(pipeline) = pc.as_replication() {
                if pipeline.source.adapter == *adapter_name {
                    ref_count += 1;
                }
                if let Some(ref disc) = pipeline.source.discovery
                    && disc.adapter == *adapter_name
                {
                    ref_count += 1;
                }
            }
        }
        if ref_count > 1 && adapter_name != "default" {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L007".into(),
                message: format!(
                    "single-adapter project repeats adapter='{adapter_name}' {ref_count} times — consider unnamed [adapter]"
                ),
                file: None,
                field: None,
            });
        }
    }

    // Model-level lint rules
    if models.is_empty() {
        return;
    }

    // L001: name matches filename stem.
    //
    // Both the post-substitution `name` and the raw author-declared
    // `name_declared` must match — otherwise an env-resolved value that
    // happens to coincide with the filename stem (e.g.
    // `name = "${ROCKY_MODEL_NAME:-fct_orders}"` in `fct_orders.toml`)
    // would noisily fire the "you can omit it" lint on an intentional
    // template.
    for model in models {
        let file_stem = std::path::Path::new(&model.file_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if model.config.name == file_stem && model.config.name_declared == file_stem {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L001".into(),
                message: format!(
                    "model '{}' declares name='{}' which matches its filename — you can omit it",
                    model.config.name, model.config.name
                ),
                file: Some(model.file_path.clone()),
                field: Some("name".into()),
            });
        }

        // L002: target.table matches name.
        //
        // Same raw-vs-resolved reasoning as L001 — only fire when the
        // author's declared template also matches, so
        // `table = "${ROCKY_TABLE_OVERRIDE:-customer_facts}"` with
        // `name = "customer_facts"` no longer trips the lint.
        if model.config.target.table == model.config.name
            && model.config.target_table_declared == model.config.name_declared
        {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L002".into(),
                message: format!(
                    "model '{}' declares target.table='{}' which matches name — you can omit it",
                    model.config.name, model.config.target.table
                ),
                file: Some(model.file_path.clone()),
                field: Some("target.table".into()),
            });
        }

        // L008: auto-discovered contract (informational)
        if model.contract_path.is_some() {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L008".into(),
                message: format!(
                    "model '{}' has auto-discovered contract from sibling .contract.toml",
                    model.config.name
                ),
                file: model
                    .contract_path
                    .as_ref()
                    .map(|p| p.display().to_string()),
                field: None,
            });
        }
    }

    // L003: models share identical target.catalog and target.schema
    let mut catalog_schema_counts: HashMap<(String, String), Vec<String>> = HashMap::new();
    for model in models {
        let key = (
            model.config.target.catalog.clone(),
            model.config.target.schema.clone(),
        );
        catalog_schema_counts
            .entry(key)
            .or_default()
            .push(model.config.name.clone());
    }
    for ((catalog, schema), model_names) in &catalog_schema_counts {
        if model_names.len() >= 3 {
            out.push(ValidateMessage {
                severity: "lint".into(),
                code: "L003".into(),
                message: format!(
                    "models {} all declare target.catalog='{}' and target.schema='{}' — consider models/_defaults.toml",
                    model_names.join(", "), catalog, schema
                ),
                file: None,
                field: None,
            });
        }
    }
}

/// Line prefix for a message severity in human-readable validate output.
///
/// Errors render with a distinct ` ERR ` prefix (matching `rocky doctor`)
/// so a failed validation is visually distinguishable from a warning —
/// both previously shared the `  !! ` prefix, hiding the difference
/// between a hard failure and an advisory.
fn severity_prefix(severity: &str) -> &'static str {
    match severity {
        "ok" => "  ok ",
        "warn" => "  !! ",
        "error" => " ERR ",
        "lint" => " note",
        _ => "     ",
    }
}

/// Render the structured output as human-readable text (matching today's
/// format for backward compatibility).
fn render_text(output: &ValidateOutput) {
    for msg in &output.messages {
        println!("{} {}", severity_prefix(&msg.severity), msg.message);
    }
    println!();
    println!("Validation complete.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn validate_toml(toml_str: &str) -> ValidateOutput {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(toml_str.as_bytes()).unwrap();
        validate_inner(f.path()).unwrap()
    }

    #[test]
    fn schedule_catchup_all_is_rejected_v037() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"
catchup = "all"
"#,
        );
        let v037: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V037" && m.severity == "error")
            .collect();
        assert_eq!(v037.len(), 1, "expected one V037: {:?}", out.messages);
        assert!(v037[0].message.contains("watermark-driven"));
        assert!(!out.valid);
    }

    #[test]
    fn schedule_unknown_after_target_is_v039() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["ghost"]
"#,
        );
        assert!(
            out.messages
                .iter()
                .any(|m| m.code == "V039" && m.severity == "error"),
            "{:?}",
            out.messages
        );
        assert!(!out.valid);
    }

    #[test]
    fn schedule_after_cycle_is_v042() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.a]
type = "transformation"
[pipeline.a.target]
adapter = "db"
[pipeline.a.schedule]
after = ["b"]
[pipeline.b]
type = "transformation"
[pipeline.b.target]
adapter = "db"
[pipeline.b.schedule]
after = ["a"]
"#,
        );
        assert!(
            out.messages
                .iter()
                .any(|m| m.code == "V042" && m.severity == "error"),
            "{:?}",
            out.messages
        );
        assert!(!out.valid);
    }

    #[test]
    fn schedule_freshness_without_budget_is_v038() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
freshness = true
"#,
        );
        assert!(
            out.messages
                .iter()
                .any(|m| m.code == "V038" && m.severity == "error"),
            "{:?}",
            out.messages
        );
        assert!(!out.valid);
    }

    #[test]
    fn schedule_inert_block_is_v040_warning() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
enabled = true
"#,
        );
        assert!(
            out.messages
                .iter()
                .any(|m| m.code == "V040" && m.severity == "warn"),
            "{:?}",
            out.messages
        );
        assert!(out.valid, "inert schedule is a warning, not an error");
    }

    #[test]
    fn schedule_after_non_persisting_type_is_v041_warning() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.dq]
type = "quality"
[pipeline.dq.target]
adapter = "db"
[pipeline.dq.checks]
enabled = true
[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["dq"]
"#,
        );
        let v041: Vec<_> = out.messages.iter().filter(|m| m.code == "V041").collect();
        assert_eq!(v041.len(), 1, "expected one V041: {:?}", out.messages);
        assert_eq!(v041[0].severity, "warn");
        assert!(out.valid, "the persistence gap is a warning, not an error");
    }

    #[test]
    fn schedule_valid_config_has_no_schedule_errors() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "duckdb"
[pipeline.raw]
type = "transformation"
[pipeline.raw.target]
adapter = "db"
[pipeline.raw.schedule]
cron = "0 3 * * *"
timezone = "Europe/Lisbon"
[pipeline.staging]
type = "transformation"
[pipeline.staging.target]
adapter = "db"
[pipeline.staging.schedule]
after = ["raw"]
"#,
        );
        let sched_errs: Vec<_> = out
            .messages
            .iter()
            .filter(|m| {
                ["V036", "V037", "V038", "V039", "V040", "V041", "V042"].contains(&m.code.as_str())
                    && m.severity == "error"
            })
            .collect();
        assert!(
            sched_errs.is_empty(),
            "valid schedule should have no errors: {sched_errs:?}"
        );
    }

    #[test]
    fn test_missing_discovery_kind_emits_v032() {
        let out = validate_toml(
            r#"
[adapter.fivetran_main]
type = "fivetran"
destination_id = "d"
api_key = "k"
api_secret = "s"
"#,
        );
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "error" && m.code == "V032")
            .collect();
        assert_eq!(
            errors.len(),
            1,
            "expected one V032 diagnostic: {:?}",
            out.messages
        );
        assert!(errors[0].message.contains("discovery-only"));
        assert_eq!(
            errors[0].field.as_deref(),
            Some("adapter.fivetran_main.kind")
        );
    }

    #[test]
    fn test_adapter_kind_mismatch_emits_v032() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "databricks"
kind = "discovery"
host = "h"
http_path = "p"
token = "t"
"#,
        );
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "error" && m.code == "V032")
            .collect();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("only supports data"));
    }

    #[test]
    fn test_pipeline_discovery_to_data_only_emits_v033() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "databricks"
host = "h"
http_path = "p"
token = "t"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "db"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.source.discovery]
adapter = "db"

[pipeline.poc.target]
adapter = "db"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "error" && m.code == "V033")
            .collect();
        assert_eq!(
            errors.len(),
            1,
            "expected one V033 diagnostic: {:?}",
            out.messages
        );
        assert_eq!(
            errors[0].field.as_deref(),
            Some("pipeline.poc.source.discovery.adapter")
        );
    }

    #[test]
    fn test_multiple_kind_issues_all_surface() {
        // Two unrelated kind issues in the same file — both should
        // surface as separate diagnostics instead of bailing on the
        // first one at parse time.
        let out = validate_toml(
            r#"
[adapter.fivetran_main]
type = "fivetran"
destination_id = "d"
api_key = "k"
api_secret = "s"

[adapter.db]
type = "databricks"
kind = "discovery"
host = "h"
http_path = "p"
token = "t"
"#,
        );
        let v032: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V032" && m.severity == "error")
            .collect();
        assert_eq!(
            v032.len(),
            2,
            "both V032 issues should surface: {:?}",
            out.messages
        );
    }

    #[test]
    fn test_valid_duckdb_config() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"

[state]
backend = "local"
"#,
        );
        assert!(out.valid);
        assert_eq!(out.adapters.len(), 1);
        assert!(out.adapters[0].ok);
        assert_eq!(out.pipelines.len(), 1);
        assert!(out.pipelines[0].ok);
    }

    #[test]
    fn inert_check_on_quality_pipeline_emits_v034() {
        // A quality pipeline runs only row_count / custom / assertions, so a
        // configured `freshness` check never executes — V034 must surface it.
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.q]
type = "quality"

[pipeline.q.target]
adapter = "local"

[[pipeline.q.tables]]
catalog = "poc"
schema  = "s"
table   = "t"

[pipeline.q.checks]
enabled = true
freshness = { threshold_seconds = 3600 }
"#,
        );
        assert!(out.valid, "inert config is a warning, not an error");
        let v034: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V034" && m.severity == "warn")
            .collect();
        assert_eq!(v034.len(), 1, "expected one V034: {:?}", out.messages);
        assert_eq!(
            v034[0].field.as_deref(),
            Some("pipeline.q.checks.freshness")
        );
    }

    #[test]
    fn configured_checks_on_replication_do_not_warn() {
        // Replication executes custom + null_rate (and the rest), so none of
        // these is inert — guards the runner/lint coupling: if a future change
        // stops the replication runner executing one of these, this test and
        // `executed_check_kinds` must move together.
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"

[pipeline.poc.checks]
enabled = true
null_rate = { columns = ["a"], threshold = 0.1 }

[[pipeline.poc.checks.custom]]
name = "has_rows"
sql = "SELECT COUNT(*) FROM {table}"
threshold = 1
"#,
        );
        let inert: Vec<_> = out.messages.iter().filter(|m| m.code == "V034").collect();
        assert!(
            inert.is_empty(),
            "replication runs custom + null_rate — expected no V034: {inert:?}"
        );
    }

    #[test]
    fn unrecognized_replication_strategy_emits_v035() {
        // `time_interval` is not a replication strategy — it parses clean and
        // silently becomes full_refresh at run time. The lint must catch it.
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "time_interval"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(out.valid, "a strategy typo is a warning, not an error");
        let v035: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V035" && m.severity == "warn")
            .collect();
        assert_eq!(v035.len(), 1, "expected one V035: {:?}", out.messages);
        assert_eq!(v035[0].field.as_deref(), Some("pipeline.poc.strategy"));
        assert!(v035[0].message.contains("full_refresh"));
    }

    #[test]
    fn test_empty_config() {
        let out = validate_toml("");
        // Should warn about no adapters and no pipelines
        assert!(out.valid); // warnings don't set valid=false
        let warns: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "warn")
            .collect();
        assert!(
            warns.len() >= 2,
            "expected warnings for no adapters/pipelines"
        );
    }

    #[test]
    fn test_missing_adapter_ref() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "nonexistent"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        assert!(!out.valid);
        let errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.severity == "error" && m.code == "V022")
            .collect();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].message.contains("nonexistent"));
    }

    #[test]
    fn test_missing_databricks_auth() {
        let out = validate_toml(
            r#"
[adapter.db]
type = "databricks"
host = "foo.databricks.com"
http_path = "/sql/1.0/warehouses/abc"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "db"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "db"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        // Should have a warning about missing auth
        let auth_warns: Vec<_> = out.messages.iter().filter(|m| m.code == "V013").collect();
        assert_eq!(auth_warns.len(), 1);
        assert!(!out.adapters[0].ok);
    }

    #[test]
    fn test_invalid_schema_pattern() {
        // An empty-name component ("") is rejected by SchemaPattern::parse_components
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = [""]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let schema_errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V021" && m.severity == "error")
            .collect();
        assert_eq!(schema_errors.len(), 1);
    }

    #[test]
    fn test_dag_cycle_detected() {
        // Create a temp dir with models that have a cycle
        let dir = tempfile::TempDir::new().unwrap();

        // Write rocky.toml
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"
"#,
        )
        .unwrap();

        // Create models directory with a cycle: a -> b -> a
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();

        std::fs::write(
            models_dir.join("a.toml"),
            r#"
name = "a"
depends_on = ["b"]
[target]
catalog = "c"
schema = "s"
table = "a"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("a.sql"), "SELECT 1").unwrap();

        std::fs::write(
            models_dir.join("b.toml"),
            r#"
name = "b"
depends_on = ["a"]
[target]
catalog = "c"
schema = "s"
table = "b"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("b.sql"), "SELECT 1").unwrap();

        let out = validate_inner(&config_path).unwrap();
        assert!(!out.valid);
        let dag_errors: Vec<_> = out
            .messages
            .iter()
            .filter(|m| m.code == "V031" && m.severity == "error")
            .collect();
        assert_eq!(dag_errors.len(), 1);
        assert!(dag_errors[0].message.contains("circular"));
    }

    #[test]
    fn test_known_adapter_types_do_not_warn() {
        // Every adapter type recognised by `AdapterRegistry` must also
        // be recognised by `validate_adapter`, otherwise a perfectly
        // valid `rocky.toml` emits a cosmetic V017 warning. Driving the
        // loop directly off `AdapterRegistry::known_types()` keeps this
        // test drift-proof — adding a new adapter to the registry
        // automatically adds it to this regression set.
        for adapter_type in crate::registry::AdapterRegistry::known_types() {
            let toml = format!(
                r#"
[adapter.x]
type = "{adapter_type}"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "x"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "x"
catalog_template = "poc"
schema_template = "demo"
"#
            );
            let out = validate_toml(&toml);
            let v017: Vec<_> = out.messages.iter().filter(|m| m.code == "V017").collect();
            assert!(
                v017.is_empty(),
                "type '{adapter_type}' unexpectedly emitted V017: {v017:?}"
            );
        }
    }

    #[test]
    fn test_unknown_adapter_type() {
        let out = validate_toml(
            r#"
[adapter.mystery]
type = "postgres"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "mystery"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "mystery"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let unknown: Vec<_> = out.messages.iter().filter(|m| m.code == "V017").collect();
        assert_eq!(unknown.len(), 1);
        assert!(unknown[0].message.contains("postgres"));
        // An unknown adapter type is a hard error: `rocky run` rejects it,
        // so `rocky validate` must report `valid = false` (non-zero exit),
        // not a cosmetic warning.
        assert_eq!(unknown[0].severity, "error");
        assert!(!out.valid, "unknown adapter type must invalidate config");
        // No close match for "postgres" — message lists supported types
        // but offers no suggestion.
        assert!(unknown[0].message.contains("Supported:"));
        assert!(!unknown[0].message.contains("Did you mean"));
    }

    #[test]
    fn test_unknown_adapter_type_suggests_near_miss() {
        let out = validate_toml(
            r#"
[adapter.wh]
type = "databrick"

[pipeline.poc]
type = "replication"

[pipeline.poc.source]
adapter = "wh"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "wh"
catalog_template = "poc"
schema_template = "demo"
"#,
        );
        let v017: Vec<_> = out.messages.iter().filter(|m| m.code == "V017").collect();
        assert_eq!(v017.len(), 1);
        assert_eq!(v017[0].severity, "error");
        assert!(!out.valid);
        assert!(
            v017[0].message.contains("Did you mean 'databricks'?"),
            "expected a near-miss suggestion, got: {}",
            v017[0].message
        );
    }

    /// L002 must NOT fire when `target.table` is declared as a
    /// `${VAR:-default}` template that happens to collapse to the
    /// model's `name` after env substitution. Compares raw declared
    /// templates against each other, not env-resolved literals.
    #[test]
    fn test_l002_skips_env_template_default() {
        use rocky_core::models::{Model, ModelConfig, SourceConfig, StrategyConfig, TargetConfig};
        let model = Model {
            config: ModelConfig {
                name: "customer_facts".into(),
                depends_on: Vec::new(),
                strategy: StrategyConfig::FullRefresh,
                target: TargetConfig {
                    catalog: "analytics".into(),
                    schema: "marts".into(),
                    // Post-substitution literal (env unset, default applied).
                    table: "customer_facts".into(),
                },
                sources: Vec::<SourceConfig>::new(),
                adapter: None,
                intent: None,
                freshness: None,
                tests: Vec::new(),
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: "customer_facts".into(),
                target_table_declared: "${ROCKY_TABLE_OVERRIDE:-customer_facts}".into(),
            },
            sql: "SELECT 1".into(),
            file_path: "/tmp/customer_facts.sql".into(),
            contract_path: None,
        };

        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline.t]
type = "transformation"
models = "models/**"
target.adapter = "default"
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(toml_str.as_bytes()).unwrap();
        let cfg = rocky_core::config::load_rocky_config(f.path()).unwrap();

        let mut out = ValidateOutput::default();
        lint_config(&cfg, std::slice::from_ref(&model), &mut out);

        let l002: Vec<_> = out.messages.iter().filter(|m| m.code == "L002").collect();
        assert!(
            l002.is_empty(),
            "L002 must not fire on env-template default that resolves to name: {l002:?}"
        );
    }

    /// Counter-check: when the author actually wrote a literal
    /// `target.table = "<name>"` that matches `name`, L002 still fires.
    /// This locks in the original lint signal so the raw-vs-resolved fix
    /// doesn't silently disable the lint for the literal case.
    #[test]
    fn test_l002_still_fires_on_literal_redundant_table() {
        use rocky_core::models::{Model, ModelConfig, SourceConfig, StrategyConfig, TargetConfig};
        let model = Model {
            config: ModelConfig {
                name: "customer_facts".into(),
                depends_on: Vec::new(),
                strategy: StrategyConfig::FullRefresh,
                target: TargetConfig {
                    catalog: "analytics".into(),
                    schema: "marts".into(),
                    table: "customer_facts".into(),
                },
                sources: Vec::<SourceConfig>::new(),
                adapter: None,
                intent: None,
                freshness: None,
                tests: Vec::new(),
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                // Declared literally — same value as `name_declared`.
                skip: None,
                name_declared: "customer_facts".into(),
                target_table_declared: "customer_facts".into(),
            },
            sql: "SELECT 1".into(),
            file_path: "/tmp/customer_facts.sql".into(),
            contract_path: None,
        };

        let toml_str = r#"
[adapter]
type = "duckdb"

[pipeline.t]
type = "transformation"
models = "models/**"
target.adapter = "default"
"#;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(toml_str.as_bytes()).unwrap();
        let cfg = rocky_core::config::load_rocky_config(f.path()).unwrap();

        let mut out = ValidateOutput::default();
        lint_config(&cfg, std::slice::from_ref(&model), &mut out);

        let l002: Vec<_> = out.messages.iter().filter(|m| m.code == "L002").collect();
        assert_eq!(
            l002.len(),
            1,
            "L002 must still fire on literal redundant target.table: {:?}",
            out.messages
        );
    }

    #[test]
    fn test_lint_fires_on_typical_poc() {
        let out = validate_toml(
            r#"
[adapter.local]
type = "duckdb"

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "local"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "local"
catalog_template = "poc"
schema_template = "demo"

[pipeline.poc.target.governance]
auto_create_catalogs = false
auto_create_schemas = false

[state]
backend = "local"
"#,
        );
        assert!(out.valid);
        let lint_codes: Vec<&str> = out
            .messages
            .iter()
            .filter(|m| m.severity == "lint")
            .map(|m| m.code.as_str())
            .collect();
        // Should fire: L004 (state backend), L006 x2 (governance), L007 (adapter)
        // Note: L005 (pipeline type) was removed — the `type` field is now the
        // enum discriminator and not a lintable config value.
        assert!(
            lint_codes.contains(&"L004"),
            "expected L004 (state backend)"
        );
        assert!(
            lint_codes.contains(&"L006"),
            "expected L006 (governance defaults)"
        );
        assert!(
            lint_codes.contains(&"L007"),
            "expected L007 (adapter repetition)"
        );
    }

    #[test]
    fn test_error_and_warning_render_with_distinct_prefixes() {
        // Errors must be visually distinguishable from warnings in the
        // human-readable output — they previously shared `  !! `.
        assert_eq!(severity_prefix("error"), " ERR ");
        assert_eq!(severity_prefix("warn"), "  !! ");
        assert_ne!(severity_prefix("error"), severity_prefix("warn"));
        assert_eq!(severity_prefix("ok"), "  ok ");
        assert_eq!(severity_prefix("lint"), " note");
    }
}
