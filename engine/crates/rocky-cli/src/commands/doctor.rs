use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use schemars::JsonSchema;
use serde::Serialize;

/// Health check status.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

/// A single health check result.
#[derive(Debug, Serialize, JsonSchema)]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthStatus,
    pub message: String,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub details: Vec<(String, String)>,
}

/// Doctor output structure.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DoctorOutput {
    pub command: String,
    pub overall: String,
    pub checks: Vec<HealthCheck>,
    pub suggestions: Vec<String>,
}

/// Exit code returned when `rocky doctor` finds at least one Critical
/// health check.
///
/// Deliberately distinct from `2`: that code is reserved for `rocky run`
/// partial-success (one or more tables materialized, one or more failed),
/// which Dagster keys on via `allow_partial=True`. A doctor failure and a
/// partial run are different conditions, so they must not share an exit
/// code. See the exit-code convention in `rocky/src/main.rs`.
const DOCTOR_CRITICAL_EXIT_CODE: i32 = 3;

/// Execute `rocky doctor` — aggregate health checks, render, and exit.
///
/// Health-check collection is delegated to [`collect_health_checks`] so it
/// can be unit-tested without the `process::exit` in this wrapper.
pub async fn doctor(
    config_path: &Path,
    state_path: &Path,
    output_json: bool,
    check_filter: Option<&str>,
    verbose: bool,
) -> Result<()> {
    let (checks, suggestions) =
        collect_health_checks(config_path, state_path, check_filter, verbose).await;

    // Determine overall status
    let has_critical = checks
        .iter()
        .any(|c| matches!(c.status, HealthStatus::Critical));
    let has_warning = checks
        .iter()
        .any(|c| matches!(c.status, HealthStatus::Warning));
    let overall = if has_critical {
        "critical"
    } else if has_warning {
        "warning"
    } else {
        "healthy"
    };

    let doctor_output = DoctorOutput {
        command: "doctor".into(),
        overall: overall.into(),
        checks,
        suggestions,
    };

    if output_json {
        println!("{}", serde_json::to_string_pretty(&doctor_output)?);
    } else {
        // Human-readable output
        println!("\nRocky Doctor\n");
        for check in &doctor_output.checks {
            let icon = match check.status {
                HealthStatus::Healthy => "  ok ",
                HealthStatus::Warning => "  !! ",
                HealthStatus::Critical => " ERR ",
            };
            println!(
                "{} {} — {} ({}ms)",
                icon, check.name, check.message, check.duration_ms
            );
            if !check.details.is_empty() {
                for (label, value) in &check.details {
                    println!("        {label}: {value}");
                }
            }
        }
        println!("\nOverall: {}\n", doctor_output.overall);
        if !doctor_output.suggestions.is_empty() {
            println!("Suggestions:");
            for s in &doctor_output.suggestions {
                println!("  - {s}");
            }
            println!();
        }
    }

    if has_critical {
        std::process::exit(DOCTOR_CRITICAL_EXIT_CODE);
    }

    Ok(())
}

/// Run every requested health check and return the results plus
/// remediation suggestions.
///
/// Split out of [`doctor`] so tests can assert check outcomes (e.g. a
/// broken `rocky.toml` under `--check adapters` produces a Critical)
/// without the `process::exit` the wrapper performs.
async fn collect_health_checks(
    config_path: &Path,
    state_path: &Path,
    check_filter: Option<&str>,
    verbose: bool,
) -> (Vec<HealthCheck>, Vec<String>) {
    let mut checks: Vec<HealthCheck> = Vec::new();
    let mut suggestions: Vec<String> = Vec::new();

    // 1. Config validation
    if should_run("config", check_filter) {
        let start = Instant::now();
        let details = if verbose {
            vec![("path".into(), config_path.display().to_string())]
        } else {
            Vec::new()
        };
        match rocky_core::config::load_rocky_config(config_path) {
            Ok(_) => {
                checks.push(HealthCheck {
                    name: "config".into(),
                    status: HealthStatus::Healthy,
                    message: "Config syntax valid".into(),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
            }
            Err(e) => {
                checks.push(HealthCheck {
                    name: "config".into(),
                    status: HealthStatus::Critical,
                    message: format!("Config invalid: {e}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
                suggestions.push(format!("Fix config file at {}", config_path.display()));
            }
        }
    }

    // 2. State store
    if should_run("state", check_filter) {
        let start = Instant::now();
        let details = if verbose {
            let mut details = vec![("path".into(), state_path.display().to_string())];
            if state_path.exists() {
                details.push((
                    "size_bytes".into(),
                    std::fs::metadata(state_path)
                        .map(|metadata| metadata.len().to_string())
                        .unwrap_or_else(|_| "n/a".into()),
                ));
            }
            details
        } else {
            Vec::new()
        };
        match rocky_core::state::StateStore::open_read_only(state_path) {
            Ok(store) => {
                // Try reading watermarks to verify the DB is healthy
                match store.list_watermarks() {
                    Ok(wms) => {
                        checks.push(HealthCheck {
                            name: "state".into(),
                            status: HealthStatus::Healthy,
                            message: format!("State store healthy ({} watermarks)", wms.len()),
                            duration_ms: start.elapsed().as_millis() as u64,
                            details,
                        });
                    }
                    Err(e) => {
                        checks.push(HealthCheck {
                            name: "state".into(),
                            status: HealthStatus::Warning,
                            message: format!("State store read error: {e}"),
                            duration_ms: start.elapsed().as_millis() as u64,
                            details,
                        });
                        suggestions.push(
                            "State store may be corrupted — try deleting and re-running".into(),
                        );
                    }
                }
            }
            Err(e) => {
                checks.push(HealthCheck {
                    name: "state".into(),
                    status: HealthStatus::Warning,
                    message: format!("Cannot open state store: {e}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
            }
        }
    }

    // 2b. State-schema compatibility. A deploy-time gate for rolling upgrades
    //     that cross a redb schema version: return Critical when the on-disk
    //     state is *newer* than this binary supports (forward-incompatible), so
    //     an orchestrator can put `state_schema` in its strict-startup checks
    //     and fail an incompatible pod fast and visibly at boot — instead of
    //     after a per-run, hour-long retry spiral. Uses `peek_schema_version`
    //     so it reports the on-disk version precisely when it is newer (the
    //     normal open path hard-fails there). A missing/unversioned store, or
    //     one older than this binary (which auto-migrates forward), is healthy.
    if should_run("state_schema", check_filter) {
        let start = Instant::now();
        let supported = rocky_core::state::current_schema_version();
        match rocky_core::state::StateStore::peek_schema_version(state_path) {
            Ok(on_disk) => {
                let details = if verbose {
                    vec![
                        ("supported".into(), format!("v{supported}")),
                        (
                            "on_disk".into(),
                            on_disk
                                .map(|v| format!("v{v}"))
                                .unwrap_or_else(|| "none".into()),
                        ),
                    ]
                } else {
                    Vec::new()
                };
                let (status, message) = classify_state_schema(on_disk, supported);
                if matches!(status, HealthStatus::Critical) {
                    suggestions.push(
                        "state_schema: an incompatible pod met newer on-disk state — complete the \
                         rollout or roll this pod forward to the newer engine"
                            .into(),
                    );
                }
                checks.push(HealthCheck {
                    name: "state_schema".into(),
                    status,
                    message,
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
            }
            Err(e) => {
                checks.push(HealthCheck {
                    name: "state_schema".into(),
                    status: HealthStatus::Warning,
                    message: format!("Cannot read on-disk state schema version: {e}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details: Vec::new(),
                });
            }
        }
    }

    // 3. Adapter configuration checks (without actual connectivity)
    if should_run("adapters", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Err(e) => checks.push(config_load_failed("adapters", &e, start, &mut suggestions)),
            Ok(cfg) => {
                let mut adapter_ok = true;
                let mut details = Vec::new();
                for (name, adapter) in &cfg.adapters {
                    if verbose {
                        details.push((format!("{name}.type"), adapter.adapter_type.clone()));
                        details.push((
                            format!("{name}.credential"),
                            credential_kind(adapter).into(),
                        ));
                    }
                    match adapter.adapter_type.as_str() {
                        "databricks" => {
                            if adapter.host.is_none() || adapter.host.as_deref() == Some("") {
                                suggestions.push(format!("adapters.{name}: host not configured"));
                                adapter_ok = false;
                            }
                            if adapter.token.is_none() && adapter.client_id.is_none() {
                                suggestions.push(format!(
                                    "adapters.{name}: no auth configured \
                                 (set DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID/SECRET)"
                                ));
                                adapter_ok = false;
                            }
                        }
                        "fivetran" if adapter.api_key.is_none() => {
                            suggestions.push(format!("adapters.{name}: FIVETRAN_API_KEY not set"));
                            adapter_ok = false;
                        }
                        _ => {}
                    }
                }
                checks.push(HealthCheck {
                    name: "adapters".into(),
                    status: if adapter_ok {
                        HealthStatus::Healthy
                    } else {
                        HealthStatus::Warning
                    },
                    message: if adapter_ok {
                        format!("{} adapter(s) configured", cfg.adapters.len())
                    } else {
                        "Some adapters have missing configuration".into()
                    },
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
            }
        }
    }

    // 4. Pipeline validation
    if should_run("pipelines", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Err(e) => checks.push(config_load_failed("pipelines", &e, start, &mut suggestions)),
            Ok(cfg) => {
                let mut issues = Vec::new();
                let mut details = Vec::new();
                for (name, pipeline) in &cfg.pipelines {
                    if verbose {
                        details.push((
                            format!("pipeline.{name}"),
                            pipeline.pipeline_type_str().into(),
                        ));
                    }
                    // Check schema pattern is parseable (replication pipelines only)
                    if let Some(repl) = pipeline.as_replication()
                        && let Err(e) = repl.schema_pattern()
                    {
                        issues.push(format!("pipeline '{name}': invalid schema pattern: {e}"));
                    }
                }
                let pipeline_count = cfg.pipelines.len();
                checks.push(HealthCheck {
                    name: "pipelines".into(),
                    status: if issues.is_empty() {
                        HealthStatus::Healthy
                    } else {
                        HealthStatus::Warning
                    },
                    message: if issues.is_empty() {
                        format!("{pipeline_count} pipeline(s) valid")
                    } else {
                        format!("{} issue(s) found", issues.len())
                    },
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
                suggestions.extend(issues);
            }
        }
    }

    // 5. State sync configuration
    if should_run("state_sync", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Err(e) => checks.push(config_load_failed(
                "state_sync",
                &e,
                start,
                &mut suggestions,
            )),
            Ok(cfg) => {
                let backend = &cfg.state.backend;
                let details = if verbose {
                    vec![("backend".into(), backend.to_string())]
                } else {
                    Vec::new()
                };
                // A `local` state backend is the healthy default for a single-node
                // project (it mirrors the `state_rw` check, which already treats
                // local as Healthy — no remote probe needed). `Warning` is
                // reserved for a misconfigured remote backend, surfaced by the
                // `state_rw` RW probe below.
                checks.push(HealthCheck {
                    name: "state_sync".into(),
                    status: HealthStatus::Healthy,
                    message: format!("State backend: {backend}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details,
                });
            }
        }
    }

    // 6. State backend RW probe — actually write/read/delete a test
    //    object against the configured backend. Complements `state_sync`
    //    (which only inspects the backend type) by surfacing permission
    //    and reachability problems that would otherwise only manifest at
    //    end-of-run upload time.
    if should_run("state_rw", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Err(e) => checks.push(config_load_failed("state_rw", &e, start, &mut suggestions)),
            Ok(cfg) => {
                let backend = &cfg.state.backend;
                let details = if verbose {
                    vec![("backend".into(), backend.to_string())]
                } else {
                    Vec::new()
                };
                if *backend == rocky_core::config::StateBackend::Local {
                    checks.push(HealthCheck {
                        name: "state_rw".into(),
                        status: HealthStatus::Healthy,
                        message: "Local backend — no remote probe needed".into(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        details,
                    });
                } else {
                    match rocky_core::state_sync::probe_state_backend(&cfg.state).await {
                        Ok(()) => {
                            checks.push(HealthCheck {
                                name: "state_rw".into(),
                                status: HealthStatus::Healthy,
                                message: format!("State backend RW probe succeeded ({backend})"),
                                duration_ms: start.elapsed().as_millis() as u64,
                                details,
                            });
                        }
                        Err(e) => {
                            checks.push(HealthCheck {
                                name: "state_rw".into(),
                                status: HealthStatus::Critical,
                                message: format!("State backend RW probe failed: {e}"),
                                duration_ms: start.elapsed().as_millis() as u64,
                                details,
                            });
                            suggestions.push(format!(
                                "state_rw: verify the '{backend}' backend has read+write access \
                             to the configured bucket / prefix (tried put/get/delete of a \
                             short-lived marker object)"
                            ));
                        }
                    }
                }
            }
        }
    }

    // 7. Auth — construct adapters and ping each warehouse
    if should_run("auth", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Ok(cfg) => {
                match crate::registry::AdapterRegistry::from_config(&cfg) {
                    Ok(registry) => {
                        let names = registry.warehouse_adapter_names();
                        if names.is_empty() {
                            checks.push(HealthCheck {
                                name: "auth".into(),
                                status: HealthStatus::Warning,
                                message: "No warehouse adapters registered".into(),
                                duration_ms: start.elapsed().as_millis() as u64,
                                details: Vec::new(),
                            });
                        } else {
                            let mut all_ok = true;
                            for name in &names {
                                let adapter = registry.warehouse_adapter(name).unwrap();
                                let ping_start = Instant::now();
                                match adapter.ping().await {
                                    Ok(()) => {
                                        checks.push(HealthCheck {
                                            name: format!("auth/{name}"),
                                            status: HealthStatus::Healthy,
                                            message: format!("Authenticated to {name}"),
                                            duration_ms: ping_start.elapsed().as_millis() as u64,
                                            details: Vec::new(),
                                        });
                                    }
                                    Err(e) => {
                                        all_ok = false;
                                        // Frame common auth failures (403/401)
                                        // into an actionable message; stash the
                                        // raw error in verbose `details`.
                                        let framed =
                                            crate::output::frame_warehouse_adapter_error(&e, name);
                                        let message = match &framed {
                                            Some(m) => format!("Ping failed: {m}"),
                                            None => format!("Ping failed: {e}"),
                                        };
                                        let details = if verbose || framed.is_some() {
                                            vec![("raw".into(), format!("{e}"))]
                                        } else {
                                            Vec::new()
                                        };
                                        checks.push(HealthCheck {
                                            name: format!("auth/{name}"),
                                            status: HealthStatus::Critical,
                                            message,
                                            duration_ms: ping_start.elapsed().as_millis() as u64,
                                            details,
                                        });
                                        suggestions.push(format!(
                                            "Adapter '{name}': verify credentials and network access"
                                        ));
                                    }
                                }
                            }
                            if all_ok {
                                let details = if verbose {
                                    vec![("warehouse_count".into(), names.len().to_string())]
                                } else {
                                    Vec::new()
                                };
                                checks.push(HealthCheck {
                                    name: "auth".into(),
                                    status: HealthStatus::Healthy,
                                    message: format!(
                                        "All {} warehouse adapter(s) authenticated",
                                        names.len()
                                    ),
                                    duration_ms: start.elapsed().as_millis() as u64,
                                    details,
                                });
                            }
                        }

                        // Discovery adapters (e.g., Fivetran)
                        let disc_names = registry.discovery_adapter_names();
                        for name in &disc_names {
                            let adapter = registry.discovery_adapter(name).unwrap();
                            let ping_start = Instant::now();
                            match adapter.ping().await {
                                Ok(()) => {
                                    checks.push(HealthCheck {
                                        name: format!("auth/{name}"),
                                        status: HealthStatus::Healthy,
                                        message: format!("Discovery adapter {name} reachable"),
                                        duration_ms: ping_start.elapsed().as_millis() as u64,
                                        details: Vec::new(),
                                    });
                                }
                                Err(e) => {
                                    checks.push(HealthCheck {
                                        name: format!("auth/{name}"),
                                        status: HealthStatus::Critical,
                                        message: format!("Discovery ping failed: {e}"),
                                        duration_ms: ping_start.elapsed().as_millis() as u64,
                                        details: Vec::new(),
                                    });
                                    suggestions.push(format!(
                                        "Discovery adapter '{name}': verify API credentials"
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        checks.push(HealthCheck {
                            name: "auth".into(),
                            status: HealthStatus::Critical,
                            message: format!("Adapter construction failed: {e}"),
                            duration_ms: start.elapsed().as_millis() as u64,
                            details: Vec::new(),
                        });
                        suggestions.push(
                            "Fix adapter configuration — see `rocky doctor --check adapters` for details".into(),
                        );
                    }
                }
            }
            Err(e) => {
                checks.push(HealthCheck {
                    name: "auth".into(),
                    status: HealthStatus::Critical,
                    message: format!("Cannot load config: {e}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                    details: Vec::new(),
                });
            }
        }
    }

    // N. Scheduler health (native orchestration). Silent when no pipeline
    //    declares a `[schedule]` — the check simply isn't emitted.
    // The check records its own remediation hints as it decides status, so a
    // config-load failure no longer draws a misleading "a reconciler is wedged".
    if should_run("scheduler", check_filter)
        && let Some(check) = scheduler_check(config_path, state_path, verbose, &mut suggestions)
    {
        checks.push(check);
    }

    (checks, suggestions)
}

/// Health of native orchestration: is a reconciler wedged, and is the timer
/// alive? Returns `None` (silent) when no pipeline declares a `[schedule]`.
///
/// Two signals, both offline (filesystem + state only — never the warehouse):
/// - **Critical** when `.rocky/tick.lock` is held but its heartbeat is stale
///   past [`LOCK_TAKEOVER_AFTER`] — a live-but-wedged reconciler no restart-free
///   takeover can dislodge.
/// - **Warning** when the most recent `last_evaluated_at` across all scheduled
///   pipelines is older than 2× the shortest cron interval — the timer looks
///   dead (nothing is driving ticks). Keyed off cron cadence; a project with
///   only `after`/`freshness` schedules has no fixed interval, so this signal is
///   skipped for it (the wedge Critical still applies).
///
/// Works for both `rocky serve --scheduler` and cron-driven `rocky tick`, since
/// both write the same `tick.lock` heartbeat and `schedule_state`.
fn scheduler_check(
    config_path: &Path,
    state_path: &Path,
    verbose: bool,
    suggestions: &mut Vec<String>,
) -> Option<HealthCheck> {
    use rocky_core::schedule::{LOCK_TAKEOVER_AFTER, cron_interval_estimate, probe_tick_lock};

    let start = Instant::now();
    // A config we cannot READ is not the same as a project with no schedules.
    // Swallowing the error here emitted no check at all, so `rocky doctor
    // --check scheduler` on a broken `rocky.toml` reported `overall: healthy`
    // with exit 0 while the resident scheduler skipped every iteration. Fail
    // loudly instead — the same treatment the adapters/pipelines/state checks
    // already give an unreadable config.
    let config = match rocky_core::config::load_rocky_config(config_path) {
        Ok(c) => c,
        Err(e) => return Some(config_load_failed("scheduler", &e, start, suggestions)),
    };
    let scheduled: Vec<(&String, &rocky_core::config::ScheduleConfig)> = config
        .pipelines
        .iter()
        .filter_map(|(name, p)| p.schedule().map(|s| (name, s)))
        .collect();
    if scheduled.is_empty() {
        return None; // Nothing scheduled — stay silent.
    }

    let now = chrono::Utc::now();
    let rocky_dir = config_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join(".rocky");

    let mut status = HealthStatus::Healthy;
    let mut messages: Vec<String> = Vec::new();
    let mut details: Vec<(String, String)> = Vec::new();

    // 1. Wedged reconciler: lock held with a stale heartbeat.
    match probe_tick_lock(&rocky_dir) {
        Ok(Some(probe)) if probe.held => match probe.heartbeat_age {
            Some(age) if age > LOCK_TAKEOVER_AFTER => {
                status = HealthStatus::Critical;
                suggestions.push("a reconciler is wedged — restart the `rocky serve --scheduler` (or the process running `rocky tick`) that holds `.rocky/tick.lock`".into());
                messages.push(format!(
                    "a reconciler holds .rocky/tick.lock but its heartbeat is {}s stale — it is wedged; restart the holding process",
                    age.as_secs()
                ));
            }
            _ => details.push(("tick_lock".into(), "held by a live reconciler".into())),
        },
        Ok(Some(_)) => details.push(("tick_lock".into(), "free".into())),
        Ok(None) => details.push(("tick_lock".into(), "never taken".into())),
        // A probe we could not perform is not evidence of health: recording the
        // error while leaving the status Healthy reported a green check whose
        // own message said it could not tell.
        Err(e) => {
            status = HealthStatus::Warning;
            messages.push(format!("could not probe the tick lock: {e}"));
        }
    }

    // 2. Dead timer: the newest last_evaluated_at across scheduled pipelines is
    //    older than 2× the shortest cron interval.
    let shortest_interval = scheduled
        .iter()
        .filter_map(|(_, s)| {
            let cron = s.cron.as_deref()?;
            let tz = s.timezone.as_deref().unwrap_or("UTC");
            cron_interval_estimate(cron, tz, now)
        })
        .min();

    // An ABSENT state file is normal — no tick has run yet — and stays healthy.
    // A file that EXISTS but cannot be opened (corrupt, unreadable, forward-
    // incompatible) is a real fault, and discarding that error left the check
    // reporting "reconciler healthy" over a broken store.
    let store = match rocky_core::state::StateStore::open_read_only(state_path) {
        Ok(store) => Some(store),
        Err(e) => {
            if state_path.exists() {
                if !matches!(status, HealthStatus::Critical) {
                    status = HealthStatus::Warning;
                }
                messages.push(format!("could not read scheduler state: {e}"));
            }
            None
        }
    };
    let most_recent_eval = store.as_ref().and_then(|store| {
        scheduled
            .iter()
            .filter_map(|(name, _)| {
                store
                    .get_schedule_state(name)
                    .ok()
                    .flatten()?
                    .last_evaluated_at
                    .as_deref()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            })
            .max()
    });

    match (shortest_interval, most_recent_eval) {
        (Some(interval), Some(last)) => {
            details.push(("last_evaluated".into(), last.to_rfc3339()));
            let age = now.signed_duration_since(last).to_std().unwrap_or_default();
            if age > interval * 2 {
                if !matches!(status, HealthStatus::Critical) {
                    status = HealthStatus::Warning;
                }
                suggestions.push(
                    "no recent scheduler tick — confirm `rocky serve --scheduler` or the cron driving `rocky tick` is running".into(),
                );
                messages.push(format!(
                    "no tick has evaluated any schedule in {}s (>2× the shortest interval, {}s) — the scheduler timer may be dead",
                    age.as_secs(),
                    interval.as_secs()
                ));
            }
        }
        (_, None) => details.push((
            "last_evaluated".into(),
            "never (no tick has run yet)".into(),
        )),
        (None, _) => {} // Only after/freshness schedules — no fixed interval.
    }

    let message = if messages.is_empty() {
        format!(
            "{} scheduled pipeline(s); reconciler healthy",
            scheduled.len()
        )
    } else {
        messages.join("; ")
    };
    if !verbose {
        details.clear();
    }

    Some(HealthCheck {
        name: "scheduler".into(),
        status,
        message,
        duration_ms: start.elapsed().as_millis() as u64,
        details,
    })
}

fn should_run(name: &str, filter: Option<&str>) -> bool {
    filter.is_none_or(|f| f == name)
}

/// Classify on-disk-vs-supported state-schema compatibility for the
/// `state_schema` check.
///
/// `Critical` **iff** the on-disk schema is *newer* than this binary supports
/// (forward-incompatible — the deploy-safety signal). An on-disk schema that is
/// older (auto-migrates forward), exactly equal, or absent is `Healthy`.
fn classify_state_schema(on_disk: Option<u32>, supported: u32) -> (HealthStatus, String) {
    match on_disk {
        Some(found) if found > supported => (
            HealthStatus::Critical,
            format!(
                "on-disk state schema v{found} is newer than this binary supports \
                 (v{supported}); this pod is forward-incompatible. Upgrade rocky, or \
                 set [state] on_schema_mismatch = recreate to run a full refresh."
            ),
        ),
        Some(found) if found < supported => (
            HealthStatus::Healthy,
            format!("on-disk state schema v{found} will auto-migrate forward to v{supported}"),
        ),
        Some(found) => (
            HealthStatus::Healthy,
            format!("state schema v{found} matches this binary"),
        ),
        None => (
            HealthStatus::Healthy,
            format!("no on-disk state schema (binary supports v{supported})"),
        ),
    }
}

/// Build a Critical `HealthCheck` for a check whose config could not be
/// loaded, and record a remediation suggestion.
///
/// Checks that need `rocky.toml` (`adapters`, `pipelines`, `state_sync`,
/// `state_rw`) previously swallowed a load error and emitted no check at
/// all, so `rocky doctor --check adapters` on a broken config reported
/// "healthy" with exit 0. Surfacing the failure as Critical makes those
/// scoped checks fail loudly and exit non-zero.
fn config_load_failed(
    check_name: &str,
    err: &impl std::fmt::Display,
    start: Instant,
    suggestions: &mut Vec<String>,
) -> HealthCheck {
    suggestions.push(format!(
        "{check_name}: could not load config — run `rocky doctor --check config` to diagnose"
    ));
    HealthCheck {
        name: check_name.into(),
        status: HealthStatus::Critical,
        message: format!("could not load config: {err} — run `rocky doctor --check config`"),
        duration_ms: start.elapsed().as_millis() as u64,
        details: Vec::new(),
    }
}

/// Best-effort label for the credential shape an adapter is configured to use.
///
/// Surfaced in `rocky doctor --verbose` to help diagnose adapters that look
/// healthy on a structural check but are wired to the wrong auth path. Order
/// matches the auth precedence each adapter actually applies at connect time.
fn credential_kind(adapter: &rocky_core::config::AdapterConfig) -> &'static str {
    if adapter.token.is_some() {
        "token"
    } else if adapter.oauth_token.is_some() {
        "oauth_token"
    } else if adapter.private_key_path.is_some() {
        "key_pair"
    } else if adapter.client_id.is_some() {
        "oauth_client"
    } else if adapter.api_key.is_some() {
        "api_key"
    } else if adapter.password.is_some() {
        "password"
    } else {
        "implicit"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // should_run filter logic
    // -----------------------------------------------------------------------

    #[test]
    fn should_run_no_filter_always_matches() {
        assert!(should_run("auth", None));
        assert!(should_run("config", None));
        assert!(should_run("state", None));
    }

    #[test]
    fn should_run_exact_match() {
        assert!(should_run("auth", Some("auth")));
    }

    #[test]
    fn should_run_mismatch() {
        assert!(!should_run("auth", Some("config")));
        assert!(!should_run("config", Some("auth")));
    }

    // -----------------------------------------------------------------------
    // A broken rocky.toml under a config-dependent check must surface as
    // Critical, not silently report healthy (EF2).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn broken_config_under_adapters_check_is_critical() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        // Syntactically invalid TOML (unterminated string).
        let mut f = std::fs::File::create(&config_path).unwrap();
        write!(f, "[adapter.x]\ntype = \"duckdb\nbroken").unwrap();
        drop(f);
        let state_path = dir.path().join("state.redb");

        let (checks, _suggestions) =
            collect_health_checks(&config_path, &state_path, Some("adapters"), false).await;

        // The check ran and reported Critical (rather than emitting nothing
        // and letting the overall status default to healthy).
        let adapters = checks
            .iter()
            .find(|c| c.name == "adapters")
            .expect("adapters check must be emitted even when config fails to load");
        assert!(
            matches!(adapters.status, HealthStatus::Critical),
            "broken config must yield a Critical adapters check, got {:?}",
            adapters.status
        );
        assert!(
            checks
                .iter()
                .any(|c| matches!(c.status, HealthStatus::Critical)),
            "doctor must report at least one Critical so it exits non-zero"
        );
    }

    // -----------------------------------------------------------------------
    // scheduler check — silent unless scheduled; emitted (healthy) otherwise
    // -----------------------------------------------------------------------

    /// A config that cannot be parsed is NOT "no schedules": swallowing the load
    /// error emitted no check at all, so `rocky doctor --check scheduler` on a
    /// broken `rocky.toml` reported healthy with exit 0 while a resident
    /// scheduler was skipping every iteration.
    #[tokio::test]
    async fn scheduler_check_fails_loudly_on_unreadable_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(&config_path, "this is not valid toml : : :").unwrap();
        let state_path = dir.path().join("state.redb");

        let (checks, _) =
            collect_health_checks(&config_path, &state_path, Some("scheduler"), false).await;

        let check = checks
            .iter()
            .find(|c| c.name == "scheduler")
            .expect("an unreadable config must still emit a scheduler check");
        assert!(
            !matches!(check.status, HealthStatus::Healthy),
            "a config we cannot read must not report healthy, got {:?}",
            check.status,
        );
    }

    #[tokio::test]
    async fn scheduler_check_is_silent_without_a_schedule() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter.db]\ntype = \"duckdb\"\n[pipeline.raw]\ntype = \"transformation\"\n[pipeline.raw.target]\nadapter = \"db\"\n",
        )
        .unwrap();
        let state_path = dir.path().join("state.redb");

        let (checks, _) =
            collect_health_checks(&config_path, &state_path, Some("scheduler"), false).await;
        assert!(
            !checks.iter().any(|c| c.name == "scheduler"),
            "no [schedule] anywhere ⇒ the scheduler check stays silent",
        );
    }

    #[tokio::test]
    async fn scheduler_check_healthy_for_a_scheduled_pipeline_with_no_reconciler() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter.db]\ntype = \"duckdb\"\n[pipeline.raw]\ntype = \"transformation\"\n[pipeline.raw.target]\nadapter = \"db\"\n[pipeline.raw.schedule]\ncron = \"0 3 * * *\"\n",
        )
        .unwrap();
        let state_path = dir.path().join("state.redb");

        let (checks, _) =
            collect_health_checks(&config_path, &state_path, Some("scheduler"), false).await;
        let scheduler = checks
            .iter()
            .find(|c| c.name == "scheduler")
            .expect("a scheduled pipeline ⇒ the scheduler check is emitted");
        // No tick lock has ever been taken and no tick has run — nothing is
        // wedged and there is no stale timer to warn about yet.
        assert!(
            matches!(scheduler.status, HealthStatus::Healthy),
            "no reconciler running is not itself unhealthy, got {:?}",
            scheduler.status
        );
    }

    // -----------------------------------------------------------------------
    // state_schema check — forward-incompat is Critical, everything else OK
    // -----------------------------------------------------------------------

    #[test]
    fn classify_state_schema_forward_incompat_is_critical() {
        // On-disk NEWER than supported → Critical, with both versions named.
        let (status, msg) = classify_state_schema(Some(9), 7);
        assert!(matches!(status, HealthStatus::Critical));
        assert!(msg.contains("v9") && msg.contains("v7"), "got: {msg}");
    }

    #[test]
    fn classify_state_schema_older_equal_or_absent_is_healthy() {
        // Older auto-migrates forward; equal matches; absent is fine.
        assert!(matches!(
            classify_state_schema(Some(6), 9).0,
            HealthStatus::Healthy
        ));
        assert!(matches!(
            classify_state_schema(Some(9), 9).0,
            HealthStatus::Healthy
        ));
        assert!(matches!(
            classify_state_schema(None, 9).0,
            HealthStatus::Healthy
        ));
    }

    #[tokio::test]
    async fn state_schema_check_healthy_when_no_state_file() {
        // A fresh pod (no state file yet) is healthy under `state_schema` — the
        // check must not fail a pod merely because state hasn't been written.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        let state_path = dir.path().join("state.redb"); // does not exist
        let (checks, _suggestions) =
            collect_health_checks(&config_path, &state_path, Some("state_schema"), false).await;
        let check = checks
            .iter()
            .find(|c| c.name == "state_schema")
            .expect("state_schema check must be emitted");
        assert!(
            matches!(check.status, HealthStatus::Healthy),
            "missing state file must be healthy, got {:?}",
            check.status
        );
    }

    #[tokio::test]
    async fn state_schema_check_healthy_on_current_version_store() {
        // A freshly-created store stamped at the current version is healthy.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("rocky.toml");
        let state_path = dir.path().join("state.redb");
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());
        let (checks, _suggestions) =
            collect_health_checks(&config_path, &state_path, Some("state_schema"), true).await;
        let check = checks
            .iter()
            .find(|c| c.name == "state_schema")
            .expect("state_schema check must be emitted");
        assert!(matches!(check.status, HealthStatus::Healthy));
    }

    #[test]
    fn doctor_critical_exit_code_is_not_run_partial_success() {
        // Doctor-critical and run-partial-success are different conditions
        // and must not share exit code 2.
        assert_eq!(DOCTOR_CRITICAL_EXIT_CODE, 3);
        assert_ne!(DOCTOR_CRITICAL_EXIT_CODE, 2);
    }

    // -----------------------------------------------------------------------
    // DuckDB adapter ping succeeds (real adapter, exercises WarehouseAdapter::ping)
    // -----------------------------------------------------------------------

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_warehouse_ping_healthy() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let adapter = DuckDbWarehouseAdapter::in_memory().expect("in-memory DuckDB");
        // Default ping runs `SELECT 1`.
        adapter.ping().await.expect("DuckDB ping should succeed");
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn duckdb_discovery_ping_healthy() {
        use rocky_core::traits::DiscoveryAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
        use rocky_duckdb::discovery::DuckDbDiscoveryAdapter;

        let wh = DuckDbWarehouseAdapter::in_memory().expect("in-memory DuckDB");
        let discovery = DuckDbDiscoveryAdapter::new(wh.shared_connector());
        // Default ping runs `discover("")` — should succeed on empty DB.
        discovery
            .ping()
            .await
            .expect("DuckDB discovery ping should succeed");
    }

    // -----------------------------------------------------------------------
    // Mock adapter whose ping fails -> produces Critical health check
    // -----------------------------------------------------------------------

    /// Minimal warehouse adapter whose `ping` always fails.
    struct FailingWarehouseAdapter;

    #[async_trait::async_trait]
    impl rocky_core::traits::WarehouseAdapter for FailingWarehouseAdapter {
        fn dialect(&self) -> &dyn rocky_core::traits::SqlDialect {
            unimplemented!("not needed for ping test")
        }

        async fn execute_statement(&self, _sql: &str) -> rocky_core::traits::AdapterResult<()> {
            Err(rocky_core::traits::AdapterError::msg("connection refused"))
        }

        async fn execute_query(
            &self,
            _sql: &str,
        ) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
            Err(rocky_core::traits::AdapterError::msg("connection refused"))
        }

        async fn describe_table(
            &self,
            _table: &rocky_ir::TableRef,
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ColumnInfo>> {
            Err(rocky_core::traits::AdapterError::msg("connection refused"))
        }
    }

    /// Minimal discovery adapter whose `ping` always fails.
    struct FailingDiscoveryAdapter;

    #[async_trait::async_trait]
    impl rocky_core::traits::DiscoveryAdapter for FailingDiscoveryAdapter {
        async fn discover(
            &self,
            _schema_prefix: &str,
        ) -> rocky_core::traits::AdapterResult<rocky_core::source::DiscoveryResult> {
            Err(rocky_core::traits::AdapterError::msg("unauthorized"))
        }
    }

    #[tokio::test]
    async fn mock_warehouse_ping_failure_produces_critical() {
        use rocky_core::traits::WarehouseAdapter;

        let adapter = FailingWarehouseAdapter;
        let err = adapter.ping().await.unwrap_err();

        // Simulate what the auth check does: map the error to a Critical HealthCheck.
        let check = HealthCheck {
            name: "auth/failing".into(),
            status: HealthStatus::Critical,
            message: format!("Ping failed: {err}"),
            duration_ms: 0,
            details: Vec::new(),
        };

        assert!(matches!(check.status, HealthStatus::Critical));
        assert!(check.message.contains("connection refused"));
    }

    #[tokio::test]
    async fn mock_discovery_ping_failure_produces_critical() {
        use rocky_core::traits::DiscoveryAdapter;

        let adapter = FailingDiscoveryAdapter;
        let err = adapter.ping().await.unwrap_err();

        let check = HealthCheck {
            name: "auth/failing_discovery".into(),
            status: HealthStatus::Critical,
            message: format!("Discovery ping failed: {err}"),
            duration_ms: 0,
            details: Vec::new(),
        };

        assert!(matches!(check.status, HealthStatus::Critical));
        assert!(check.message.contains("unauthorized"));
    }

    // -----------------------------------------------------------------------
    // credential_kind — verbose adapter detail heuristic
    // -----------------------------------------------------------------------

    fn empty_adapter(adapter_type: &str) -> rocky_core::config::AdapterConfig {
        rocky_core::config::AdapterConfig {
            adapter_type: adapter_type.into(),
            kind: None,
            host: None,
            http_path: None,
            token: None,
            client_id: None,
            client_secret: None,
            timeout_secs: None,
            destination_id: None,
            api_key: None,
            api_secret: None,
            account: None,
            warehouse: None,
            username: None,
            password: None,
            oauth_token: None,
            private_key_path: None,
            pat: None,
            role: None,
            database: None,
            project_id: None,
            location: None,
            path: None,
            retry: rocky_core::config::RetryConfig::default(),
            cache: None,
            ratelimit: None,
            stampede: None,
            circuit_breaker: None,
            extra: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn credential_kind_databricks_pat() {
        let mut a = empty_adapter("databricks");
        a.token = Some(rocky_core::redacted::RedactedString::new("dapi_x".into()));
        assert_eq!(credential_kind(&a), "token");
    }

    #[test]
    fn credential_kind_databricks_oauth_m2m() {
        let mut a = empty_adapter("databricks");
        a.client_id = Some("client_123".into());
        a.client_secret = Some(rocky_core::redacted::RedactedString::new("secret".into()));
        assert_eq!(credential_kind(&a), "oauth_client");
    }

    #[test]
    fn credential_kind_snowflake_oauth() {
        let mut a = empty_adapter("snowflake");
        a.oauth_token = Some(rocky_core::redacted::RedactedString::new("oauth_x".into()));
        assert_eq!(credential_kind(&a), "oauth_token");
    }

    #[test]
    fn credential_kind_snowflake_keypair() {
        let mut a = empty_adapter("snowflake");
        a.private_key_path = Some("/etc/rocky/snowflake.pem".into());
        assert_eq!(credential_kind(&a), "key_pair");
    }

    #[test]
    fn credential_kind_snowflake_password() {
        let mut a = empty_adapter("snowflake");
        a.password = Some(rocky_core::redacted::RedactedString::new("pw".into()));
        assert_eq!(credential_kind(&a), "password");
    }

    #[test]
    fn credential_kind_fivetran_api_key() {
        let mut a = empty_adapter("fivetran");
        a.api_key = Some(rocky_core::redacted::RedactedString::new("key".into()));
        assert_eq!(credential_kind(&a), "api_key");
    }

    /// BigQuery and DuckDB don't carry credentials in [adapter.*] — auth is
    /// resolved from the environment (ADC, in-memory). "implicit" (rather
    /// than "none") signals "not misconfigured, just env-based" and avoids
    /// alarming a user whose adapter is actually fine.
    #[test]
    fn credential_kind_bigquery_implicit() {
        let mut a = empty_adapter("bigquery");
        a.project_id = Some("my-gcp-project".into());
        a.location = Some("US".into());
        assert_eq!(credential_kind(&a), "implicit");
    }

    /// Token wins when more than one credential field is set — matches the
    /// auth precedence the Databricks adapter applies at connect time.
    #[test]
    fn credential_kind_token_precedence() {
        let mut a = empty_adapter("databricks");
        a.token = Some(rocky_core::redacted::RedactedString::new("t".into()));
        a.client_id = Some("c".into());
        a.password = Some(rocky_core::redacted::RedactedString::new("p".into()));
        assert_eq!(credential_kind(&a), "token");
    }

    // -----------------------------------------------------------------------
    // HealthCheck JSON contract — empty `details` must not appear in payloads
    // (existing dagster fixtures depend on this; the field only materializes
    // under `--verbose`).
    // -----------------------------------------------------------------------

    #[test]
    fn health_check_json_omits_empty_details() {
        let check = HealthCheck {
            name: "config".into(),
            status: HealthStatus::Healthy,
            message: "ok".into(),
            duration_ms: 0,
            details: Vec::new(),
        };
        let json = serde_json::to_string(&check).unwrap();
        assert!(!json.contains("details"), "empty details leaked: {json}");
    }

    #[test]
    fn health_check_json_emits_populated_details() {
        let check = HealthCheck {
            name: "config".into(),
            status: HealthStatus::Healthy,
            message: "ok".into(),
            duration_ms: 0,
            details: vec![("path".into(), "/etc/rocky.toml".into())],
        };
        let json = serde_json::to_string(&check).unwrap();
        assert!(
            json.contains("\"details\":[[\"path\",\"/etc/rocky.toml\"]]"),
            "{json}"
        );
    }
}
