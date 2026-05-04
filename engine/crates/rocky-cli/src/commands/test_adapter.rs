//! `rocky test-adapter` — run conformance tests against an adapter.
//!
//! This command validates that an adapter implementation correctly handles
//! the operations Rocky requires. It can test both compiled-in adapters
//! and process-based adapters.

use anyhow::{Context, Result};
use rocky_adapter_sdk::conformance::{self, ConformanceResult};
use rocky_adapter_sdk::manifest::{AdapterCapabilities, AdapterManifest};
use rocky_adapter_sdk::process::ProcessAdapter;
use rocky_adapter_sdk::{
    AdapterResult, ColumnSelection, MetadataColumn, SqlDialect, WarehouseAdapter,
};

use crate::output::{TestAdapterOutput, TestAdapterTestResult};

/// Run the conformance test suite against a process adapter.
///
/// The `adapter_command` is the path to the adapter binary. Rocky spawns it,
/// sends `initialize`, reads the manifest, and runs all applicable tests.
pub async fn run_test_adapter(
    adapter_command: &str,
    config_json: Option<&str>,
    json_output: bool,
) -> Result<()> {
    let config: serde_json::Value = match config_json {
        Some(s) => serde_json::from_str(s).context("failed to parse adapter config JSON")?,
        None => serde_json::json!({}),
    };

    // Attempt to spawn and initialize the process adapter.
    let adapter_result = ProcessAdapter::spawn(adapter_command, &[], &config).await;

    let result = match adapter_result {
        Ok(adapter) => {
            let manifest = adapter.manifest().clone();

            // Run conformance suite.
            let result = conformance::run_conformance(&manifest, adapter.dialect());

            // Clean up.
            let _ = adapter.close().await;

            result
        }
        Err(e) => {
            // If we can't connect, report a single connection failure.
            tracing::error!("failed to initialize adapter: {e}");

            // Build a minimal manifest for reporting.
            let manifest = AdapterManifest {
                name: adapter_command.to_string(),
                version: "unknown".into(),
                sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
                dialect: "unknown".into(),
                capabilities: AdapterCapabilities::warehouse_only(),
                auth_methods: vec![],
                config_schema: serde_json::Value::Null,
            };

            let dialect = FallbackDialect::new("unknown");
            let mut result = conformance::run_conformance(&manifest, &dialect);
            // Override the first test (connect) as failed.
            if let Some(connect_test) = result.results.first_mut() {
                connect_test.status = conformance::TestStatus::Failed;
                connect_test.message = Some(format!("failed to initialize: {e}"));
                result.tests_passed -= 1;
                result.tests_failed += 1;
                result.tests_run = result.tests_passed + result.tests_failed;
            }

            result
        }
    };

    output_result(&result, json_output)?;

    if result.tests_failed > 0 {
        anyhow::bail!("{} conformance test(s) failed", result.tests_failed);
    }

    Ok(())
}

/// Run the conformance test suite against a built-in adapter by name.
///
/// This validates a compiled-in adapter without spawning a process.
pub async fn run_test_adapter_builtin(
    adapter_name: &str,
    _config_path: Option<&str>,
    json_output: bool,
) -> Result<()> {
    // For built-in adapters, we construct a manifest from the adapter name
    // and run the conformance suite. Actual execution requires a live warehouse,
    // so this mode validates the test plan and capability matching.
    let capabilities = match adapter_name {
        "databricks" => AdapterCapabilities::full(),
        "snowflake" => AdapterCapabilities {
            warehouse: true,
            discovery: false,
            governance: true,
            batch_checks: true,
            create_catalog: false,
            create_schema: true,
            merge: true,
            tablesample: true,
            file_load: false,
        },
        "duckdb" => AdapterCapabilities {
            warehouse: true,
            discovery: false,
            governance: false,
            batch_checks: false,
            create_catalog: false,
            create_schema: true,
            merge: false,
            tablesample: true,
            file_load: true,
        },
        _ => {
            anyhow::bail!(
                "unknown built-in adapter '{adapter_name}'. Known adapters: databricks, snowflake, duckdb"
            );
        }
    };

    let manifest = AdapterManifest {
        name: adapter_name.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
        dialect: adapter_name.into(),
        capabilities,
        auth_methods: vec![],
        config_schema: serde_json::Value::Null,
    };

    let dialect = FallbackDialect::new(adapter_name);
    let result = conformance::run_conformance(&manifest, &dialect);
    output_result(&result, json_output)?;

    Ok(())
}

struct FallbackDialect {
    name: String,
}

impl FallbackDialect {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl SqlDialect for FallbackDialect {
    fn name(&self) -> &str {
        &self.name
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        Ok(format!("{catalog}.{schema}.{table}"))
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[String],
        _update_cols: Option<&[String]>,
    ) -> AdapterResult<String> {
        Ok(format!(
            "MERGE INTO {target} USING ({source_sql}) ON {}",
            keys.join(", ")
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
        Some(Ok(format!("CREATE CATALOG IF NOT EXISTS {name}")))
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some(Ok(format!(
            "CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
        )))
    }

    fn row_hash_expr(&self, columns: &[String]) -> String {
        format!("MD5(CONCAT({}))", columns.join(", "))
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE ({percent} PERCENT)"))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let mut sql = match columns {
            ColumnSelection::All => "SELECT *".to_string(),
            ColumnSelection::Explicit(cols) => format!("SELECT {}", cols.join(", ")),
        };
        for column in metadata {
            sql.push_str(&format!(
                ", CAST({} AS {}) AS {}",
                column.value, column.data_type, column.name
            ));
        }
        Ok(sql)
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        Ok(format!(
            "WHERE {timestamp_col} > (SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01') FROM {target_ref})"
        ))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Ok(vec![
            format!("DELETE FROM {target} WHERE {partition_filter}"),
            format!("INSERT INTO {target}\n{select_sql}"),
        ])
    }
}

fn output_result(result: &ConformanceResult, json_output: bool) -> Result<()> {
    if json_output {
        let typed = TestAdapterOutput {
            adapter: result.adapter.clone(),
            sdk_version: result.sdk_version.clone(),
            tests_run: result.tests_run,
            tests_passed: result.tests_passed,
            tests_failed: result.tests_failed,
            tests_skipped: result.tests_skipped,
            results: result
                .results
                .iter()
                .map(|t| TestAdapterTestResult {
                    name: t.name.clone(),
                    category: format!("{:?}", t.category).to_lowercase(),
                    status: format!("{:?}", t.status).to_lowercase(),
                    message: t.message.clone(),
                    duration_ms: t.duration_ms,
                })
                .collect(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&typed).context("failed to serialize result")?
        );
    } else {
        print!("{}", result.report());
    }
    Ok(())
}
