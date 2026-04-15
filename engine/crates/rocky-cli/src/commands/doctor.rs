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
}

/// Doctor output structure.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DoctorOutput {
    pub command: String,
    pub overall: String,
    pub checks: Vec<HealthCheck>,
    pub suggestions: Vec<String>,
}

/// Execute `rocky doctor` — aggregate health checks.
pub async fn doctor(
    config_path: &Path,
    state_path: &Path,
    output_json: bool,
    check_filter: Option<&str>,
) -> Result<()> {
    let mut checks: Vec<HealthCheck> = Vec::new();
    let mut suggestions: Vec<String> = Vec::new();

    // 1. Config validation
    if should_run("config", check_filter) {
        let start = Instant::now();
        match rocky_core::config::load_rocky_config(config_path) {
            Ok(_) => {
                checks.push(HealthCheck {
                    name: "config".into(),
                    status: HealthStatus::Healthy,
                    message: "Config syntax valid".into(),
                    duration_ms: start.elapsed().as_millis() as u64,
                });
            }
            Err(e) => {
                checks.push(HealthCheck {
                    name: "config".into(),
                    status: HealthStatus::Critical,
                    message: format!("Config invalid: {e}"),
                    duration_ms: start.elapsed().as_millis() as u64,
                });
                suggestions.push(format!("Fix config file at {}", config_path.display()));
            }
        }
    }

    // 2. State store
    if should_run("state", check_filter) {
        let start = Instant::now();
        match rocky_core::state::StateStore::open(state_path) {
            Ok(store) => {
                // Try reading watermarks to verify the DB is healthy
                match store.list_watermarks() {
                    Ok(wms) => {
                        checks.push(HealthCheck {
                            name: "state".into(),
                            status: HealthStatus::Healthy,
                            message: format!("State store healthy ({} watermarks)", wms.len()),
                            duration_ms: start.elapsed().as_millis() as u64,
                        });
                    }
                    Err(e) => {
                        checks.push(HealthCheck {
                            name: "state".into(),
                            status: HealthStatus::Warning,
                            message: format!("State store read error: {e}"),
                            duration_ms: start.elapsed().as_millis() as u64,
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
                });
            }
        }
    }

    // 3. Adapter configuration checks (without actual connectivity)
    if should_run("adapters", check_filter) {
        let start = Instant::now();
        if let Ok(cfg) = rocky_core::config::load_rocky_config(config_path) {
            let mut adapter_ok = true;
            for (name, adapter) in &cfg.adapters {
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
                    "fivetran" => {
                        if adapter.api_key.is_none() {
                            suggestions.push(format!("adapters.{name}: FIVETRAN_API_KEY not set"));
                            adapter_ok = false;
                        }
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
            });
        }
    }

    // 4. Pipeline validation
    if should_run("pipelines", check_filter) {
        let start = Instant::now();
        if let Ok(cfg) = rocky_core::config::load_rocky_config(config_path) {
            let pipeline_count = cfg.pipelines.len();
            let mut issues = Vec::new();
            for (name, pipeline) in &cfg.pipelines {
                // Check schema pattern is parseable (replication pipelines only)
                if let Some(repl) = pipeline.as_replication() {
                    if let Err(e) = repl.schema_pattern() {
                        issues.push(format!("pipeline '{name}': invalid schema pattern: {e}"));
                    }
                }
            }
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
            });
            suggestions.extend(issues);
        }
    }

    // 5. State sync configuration
    if should_run("state_sync", check_filter) {
        let start = Instant::now();
        if let Ok(cfg) = rocky_core::config::load_rocky_config(config_path) {
            let backend = &cfg.state.backend;
            let has_remote = *backend != rocky_core::config::StateBackend::Local;
            checks.push(HealthCheck {
                name: "state_sync".into(),
                status: if has_remote {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Warning
                },
                message: format!("State backend: {backend}"),
                duration_ms: start.elapsed().as_millis() as u64,
            });
            if !has_remote {
                suggestions
                    .push("Consider using 'tiered' state backend for distributed execution".into());
            }
        }
    }

    // 6. Auth — construct adapters and ping each warehouse
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
                                        });
                                    }
                                    Err(e) => {
                                        all_ok = false;
                                        checks.push(HealthCheck {
                                            name: format!("auth/{name}"),
                                            status: HealthStatus::Critical,
                                            message: format!("Ping failed: {e}"),
                                            duration_ms: ping_start.elapsed().as_millis() as u64,
                                        });
                                        suggestions.push(format!(
                                            "Adapter '{name}': verify credentials and network access"
                                        ));
                                    }
                                }
                            }
                            if all_ok {
                                checks.push(HealthCheck {
                                    name: "auth".into(),
                                    status: HealthStatus::Healthy,
                                    message: format!(
                                        "All {} warehouse adapter(s) authenticated",
                                        names.len()
                                    ),
                                    duration_ms: start.elapsed().as_millis() as u64,
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
                                    });
                                }
                                Err(e) => {
                                    checks.push(HealthCheck {
                                        name: format!("auth/{name}"),
                                        status: HealthStatus::Critical,
                                        message: format!("Discovery ping failed: {e}"),
                                        duration_ms: ping_start.elapsed().as_millis() as u64,
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
                });
            }
        }
    }

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
        std::process::exit(2);
    }

    Ok(())
}

fn should_run(name: &str, filter: Option<&str>) -> bool {
    filter.is_none_or(|f| f == name)
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
            _table: &rocky_core::ir::TableRef,
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_core::ir::ColumnInfo>> {
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
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_core::source::DiscoveredConnector>>
        {
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
        };

        assert!(matches!(check.status, HealthStatus::Critical));
        assert!(check.message.contains("unauthorized"));
    }
}
