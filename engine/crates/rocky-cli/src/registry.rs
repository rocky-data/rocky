//! Adapter registry: constructs and stores adapter instances from config.
//!
//! The registry parses `RockyConfig.adapters` and creates the appropriate
//! trait-object implementations, stored by name for pipeline resolution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tracing::warn;

use crate::error_reporter;

use rocky_core::config::{AdapterConfig, RockyConfig};
use rocky_core::traits::{DiscoveryAdapter, WarehouseAdapter};

use rocky_databricks::adapter::{DatabricksBatchCheckAdapter, DatabricksWarehouseAdapter};
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};

#[cfg(feature = "duckdb")]
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
#[cfg(feature = "duckdb")]
use rocky_duckdb::discovery::DuckDbDiscoveryAdapter;

use rocky_airbyte::adapter::AirbyteDiscoveryAdapter;
use rocky_airbyte::client::AirbyteClient;

use rocky_fivetran::adapter::FivetranDiscoveryAdapter;
use rocky_fivetran::client::FivetranClient;

use rocky_iceberg::adapter::IcebergDiscoveryAdapter;
use rocky_iceberg::client::IcebergCatalogClient;

use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::connector::SnowflakeConnector;

use rocky_bigquery::connector::BigQueryAdapter;

/// Holds constructed adapter instances, keyed by name from the config.
pub struct AdapterRegistry {
    warehouse: HashMap<String, Arc<dyn WarehouseAdapter>>,
    discovery: HashMap<String, Arc<dyn DiscoveryAdapter>>,
    connectors: HashMap<String, Arc<DatabricksConnector>>,
    adapter_configs: HashMap<String, AdapterConfig>,
}

impl AdapterRegistry {
    /// Build the registry from a `RockyConfig`.
    pub fn from_config(config: &RockyConfig) -> Result<Self> {
        let mut warehouse = HashMap::new();
        let mut discovery = HashMap::new();
        let mut connectors = HashMap::new();
        let mut adapter_configs = HashMap::new();

        for (name, adapter_cfg) in &config.adapters {
            adapter_configs.insert(name.clone(), adapter_cfg.clone());

            match adapter_cfg.adapter_type.as_str() {
                "databricks" => {
                    let host = adapter_cfg
                        .host
                        .as_deref()
                        .context(format!("adapters.{name}: host required for databricks"))?;
                    let http_path = adapter_cfg.http_path.as_deref().context(format!(
                        "adapters.{name}: http_path required for databricks"
                    ))?;

                    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(http_path)
                        .context(format!(
                            "adapters.{name}: failed to extract warehouse_id from http_path"
                        ))?;

                    let auth = Auth::from_config(AuthConfig {
                        host: host.to_string(),
                        token: adapter_cfg.token.as_ref().map(|s| s.expose().to_string()),
                        client_id: adapter_cfg.client_id.clone(),
                        client_secret: adapter_cfg
                            .client_secret
                            .as_ref()
                            .map(|s| s.expose().to_string()),
                    })
                    .context(format!("adapters.{name}: auth configuration error"))?;

                    let connector_config = ConnectorConfig {
                        host: host.to_string(),
                        warehouse_id,
                        timeout: Duration::from_secs(adapter_cfg.timeout_secs.unwrap_or(120)),
                        retry: adapter_cfg.retry.clone(),
                    };

                    let connector = Arc::new(DatabricksConnector::new(connector_config, auth));
                    let adapter =
                        Arc::new(DatabricksWarehouseAdapter::new(DatabricksConnector::new(
                            ConnectorConfig {
                                host: host.to_string(),
                                warehouse_id: ConnectorConfig::warehouse_id_from_http_path(
                                    http_path,
                                )
                                .context("failed to extract warehouse_id from http_path")?,
                                timeout: Duration::from_secs(
                                    adapter_cfg.timeout_secs.unwrap_or(120),
                                ),
                                retry: adapter_cfg.retry.clone(),
                            },
                            Auth::from_config(AuthConfig {
                                host: host.to_string(),
                                token: adapter_cfg.token.as_ref().map(|s| s.expose().to_string()),
                                client_id: adapter_cfg.client_id.clone(),
                                client_secret: adapter_cfg
                                    .client_secret
                                    .as_ref()
                                    .map(|s| s.expose().to_string()),
                            })
                            .context("failed to resolve Databricks auth from adapter config")?,
                        )));

                    connectors.insert(name.clone(), connector);
                    warehouse.insert(name.clone(), adapter as Arc<dyn WarehouseAdapter>);
                }
                #[cfg(feature = "duckdb")]
                "duckdb" => {
                    // Use a persistent file when `path` is set, otherwise in-memory.
                    // Discovery + warehouse share the same connector via `Arc<Mutex<>>`.
                    let warehouse_adapter = if let Some(p) = adapter_cfg.path.as_deref() {
                        DuckDbWarehouseAdapter::open(std::path::Path::new(p))
                            .context(format!("adapters.{name}: failed to open DuckDB at '{p}'"))?
                    } else {
                        DuckDbWarehouseAdapter::in_memory().context(format!(
                            "adapters.{name}: failed to create in-memory DuckDB"
                        ))?
                    };

                    let shared = warehouse_adapter.shared_connector();
                    let warehouse_arc = Arc::new(warehouse_adapter);
                    warehouse.insert(name.clone(), warehouse_arc as Arc<dyn WarehouseAdapter>);

                    // Always register a discovery adapter for DuckDB so the same
                    // local database can act as a source for `rocky discover`.
                    let discovery_adapter = Arc::new(DuckDbDiscoveryAdapter::new(shared));
                    discovery.insert(name.clone(), discovery_adapter as Arc<dyn DiscoveryAdapter>);
                }
                #[cfg(not(feature = "duckdb"))]
                "duckdb" => {
                    bail!(
                        "adapters.{name}: DuckDB support not compiled in (enable 'duckdb' feature)"
                    );
                }
                "fivetran" => {
                    let api_key = adapter_cfg
                        .api_key
                        .as_ref()
                        .map(rocky_core::redacted::RedactedString::expose)
                        .context(format!("adapters.{name}: api_key required for fivetran"))?;
                    let api_secret = adapter_cfg
                        .api_secret
                        .as_ref()
                        .map(rocky_core::redacted::RedactedString::expose)
                        .context(format!("adapters.{name}: api_secret required for fivetran"))?;
                    let destination_id = adapter_cfg.destination_id.as_deref().context(format!(
                        "adapters.{name}: destination_id required for fivetran"
                    ))?;

                    let client = FivetranClient::with_retry(
                        api_key.to_string(),
                        api_secret.to_string(),
                        adapter_cfg.retry.clone(),
                    );

                    let adapter = Arc::new(FivetranDiscoveryAdapter::new(
                        client,
                        destination_id.to_string(),
                    ));
                    discovery.insert(name.clone(), adapter as Arc<dyn DiscoveryAdapter>);
                }
                "airbyte" => {
                    let api_url = adapter_cfg.host.as_deref().context(format!(
                        "adapters.{name}: host (API URL) required for airbyte"
                    ))?;
                    let auth_token = adapter_cfg.token.as_ref().map(|s| s.expose().to_string());

                    let client =
                        AirbyteClient::with_retry(api_url, auth_token, adapter_cfg.retry.clone());

                    let adapter = Arc::new(AirbyteDiscoveryAdapter::new(client));
                    discovery.insert(name.clone(), adapter as Arc<dyn DiscoveryAdapter>);
                }
                "iceberg" => {
                    let catalog_url = adapter_cfg.host.as_deref().context(format!(
                        "adapters.{name}: host (REST catalog URL) required for iceberg"
                    ))?;
                    let auth_token = adapter_cfg.token.as_ref().map(|s| s.expose().to_string());

                    let client = IcebergCatalogClient::with_retry(
                        catalog_url,
                        auth_token,
                        adapter_cfg.retry.clone(),
                    );

                    let adapter = Arc::new(IcebergDiscoveryAdapter::new(client));
                    discovery.insert(name.clone(), adapter as Arc<dyn DiscoveryAdapter>);
                }
                "manual" => {
                    // Manual discovery doesn't need an adapter instance;
                    // it's handled inline from pipeline source config.
                }
                "snowflake" => {
                    let account = adapter_cfg
                        .account
                        .as_deref()
                        .context(format!("adapters.{name}: account required for snowflake"))?;
                    let sf_warehouse = adapter_cfg
                        .warehouse
                        .as_deref()
                        .context(format!("adapters.{name}: warehouse required for snowflake"))?;

                    let sf_auth = rocky_snowflake::auth::Auth::from_config(
                        rocky_snowflake::auth::AuthConfig {
                            account: account.to_string(),
                            username: adapter_cfg.username.clone(),
                            password: adapter_cfg
                                .password
                                .as_ref()
                                .map(|s| s.expose().to_string()),
                            oauth_token: adapter_cfg
                                .oauth_token
                                .as_ref()
                                .map(|s| s.expose().to_string()),
                            private_key_path: adapter_cfg.private_key_path.clone(),
                        },
                    )
                    .context(format!("adapters.{name}: auth configuration error"))?;

                    let sf_connector = SnowflakeConnector::new(
                        rocky_snowflake::connector::ConnectorConfig {
                            account: account.to_string(),
                            warehouse: sf_warehouse.to_string(),
                            database: adapter_cfg.database.clone(),
                            schema: None,
                            role: adapter_cfg.role.clone(),
                            timeout: Duration::from_secs(adapter_cfg.timeout_secs.unwrap_or(120)),
                            retry: adapter_cfg.retry.clone(),
                        },
                        sf_auth,
                    );

                    let adapter = SnowflakeWarehouseAdapter::new(sf_connector);
                    warehouse.insert(name.clone(), Arc::new(adapter));
                }
                "bigquery" => {
                    let project_id = adapter_cfg
                        .project_id
                        .as_deref()
                        .context(format!("adapters.{name}: project_id required for bigquery"))?;
                    let location = adapter_cfg.location.as_deref().unwrap_or("US");

                    let bq_auth = rocky_bigquery::auth::BigQueryAuth::from_env()
                        .context(format!("adapters.{name}: auth configuration error"))?;

                    let adapter = BigQueryAdapter::new(project_id, location, bq_auth)
                        .with_timeout(adapter_cfg.timeout_secs.unwrap_or(300));
                    warehouse.insert(name.clone(), Arc::new(adapter));
                }
                other => {
                    let mut msg = format!(
                        "adapters.{name}: unsupported adapter type '{other}'. \
                         Supported: databricks, duckdb, snowflake, bigquery, \
                         fivetran, airbyte, iceberg, manual"
                    );
                    if let Some(suggestion) =
                        error_reporter::did_you_mean(other, error_reporter::KNOWN_ADAPTER_TYPES)
                    {
                        msg.push_str(&format!(". Did you mean '{suggestion}'?"));
                    }
                    bail!(msg);
                }
            }
        }

        // Log a warning for any experimental adapters that were registered.
        for (name, adapter) in &warehouse {
            if adapter.is_experimental() {
                warn!(
                    adapter = %name,
                    "adapter '{name}' is experimental — some features may be incomplete or behave differently from production-ready adapters"
                );
            }
        }

        Ok(Self {
            warehouse,
            discovery,
            connectors,
            adapter_configs,
        })
    }

    /// Get a warehouse adapter by name (generic trait object).
    pub fn warehouse_adapter(&self, name: &str) -> Result<Arc<dyn WarehouseAdapter>> {
        self.warehouse
            .get(name)
            .cloned()
            .context(format!("no warehouse adapter named '{name}'"))
    }

    /// Get a discovery adapter by name.
    pub fn discovery_adapter(&self, name: &str) -> Result<Arc<dyn DiscoveryAdapter>> {
        self.discovery
            .get(name)
            .cloned()
            .context(format!("no discovery adapter named '{name}'"))
    }

    /// Get the raw Databricks connector by adapter name (for governance/batch operations).
    pub fn databricks_connector(&self, name: &str) -> Result<Arc<DatabricksConnector>> {
        self.connectors
            .get(name)
            .cloned()
            .context(format!("no databricks connector named '{name}'"))
    }

    /// Get the adapter config by name.
    pub fn adapter_config(&self, name: &str) -> Option<&AdapterConfig> {
        self.adapter_configs.get(name)
    }

    /// Returns the names of all registered warehouse adapters.
    pub fn warehouse_adapter_names(&self) -> Vec<String> {
        self.warehouse.keys().cloned().collect()
    }

    /// Returns the names of all registered discovery adapters.
    pub fn discovery_adapter_names(&self) -> Vec<String> {
        self.discovery.keys().cloned().collect()
    }

    /// Create a batch check adapter for a Databricks warehouse adapter.
    pub fn batch_check_adapter(&self, name: &str) -> Result<DatabricksBatchCheckAdapter> {
        let connector = self.databricks_connector(name)?;
        Ok(DatabricksBatchCheckAdapter::new(connector))
    }
}

/// Resolve which pipeline to use from a `RockyConfig`.
///
/// If `pipeline_name` is provided, looks it up. If not, uses the only pipeline
/// (errors if there are multiple).
pub fn resolve_pipeline<'a>(
    config: &'a RockyConfig,
    pipeline_name: Option<&'a str>,
) -> Result<(&'a str, &'a rocky_core::config::PipelineConfig)> {
    match pipeline_name {
        Some(name) => {
            let pipeline = config.pipelines.get(name);
            match pipeline {
                Some(p) => Ok((name, p)),
                None => {
                    let known: Vec<&str> = config.pipelines.keys().map(String::as_str).collect();
                    let mut msg = format!("pipeline '{name}' not found in config");
                    if let Some(suggestion) = error_reporter::did_you_mean(name, &known) {
                        msg.push_str(&format!(". Did you mean '{suggestion}'?"));
                    }
                    bail!(msg)
                }
            }
        }
        None => {
            if config.pipelines.len() == 1 {
                let (name, pipeline) = config.pipelines.iter().next().unwrap();
                Ok((name.as_str(), pipeline))
            } else if config.pipelines.is_empty() {
                bail!("no pipelines defined in config")
            } else {
                let names: Vec<&str> = config
                    .pipelines
                    .keys()
                    .map(std::string::String::as_str)
                    .collect();
                bail!(
                    "multiple pipelines defined ({}). Use --pipeline <name> to select one.",
                    names.join(", ")
                )
            }
        }
    }
}

/// Resolve a pipeline and verify it is a replication pipeline.
///
/// Commands that only operate on replication pipelines (discover, plan, run)
/// should use this instead of [`resolve_pipeline`] to get a clear error when
/// the user points at a non-replication pipeline.
pub fn resolve_replication_pipeline<'a>(
    config: &'a RockyConfig,
    pipeline_name: Option<&'a str>,
) -> Result<(&'a str, &'a rocky_core::config::ReplicationPipelineConfig)> {
    let (name, pipeline) = resolve_pipeline(config, pipeline_name)?;
    let repl = pipeline.as_replication().with_context(|| {
        format!(
            "pipeline '{name}' is type '{}', but this command only supports replication pipelines",
            pipeline.pipeline_type_str()
        )
    })?;
    Ok((name, repl))
}
