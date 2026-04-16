use std::path::Path;

use anyhow::Result;

/// Execute `rocky init` — scaffold a new project.
///
/// Templates:
/// - `duckdb` (default) — minimal DuckDB-backed local project, runnable immediately
/// - `databricks-fivetran` — two-adapter setup (Databricks + Fivetran) with governance
/// - `snowflake` — Snowflake adapter with key-pair auth
pub fn init(path: &str, template: Option<&str>) -> Result<()> {
    let dir = Path::new(path);

    if dir != Path::new(".") {
        std::fs::create_dir_all(dir)?;
    }

    let config_path = dir.join("rocky.toml");
    if config_path.exists() {
        anyhow::bail!("rocky.toml already exists in {}", dir.display());
    }

    let template_name = template.unwrap_or("duckdb");

    match template_name {
        "duckdb" => init_duckdb(dir)?,
        "databricks-fivetran" => init_databricks_fivetran(dir)?,
        "snowflake" => init_snowflake(dir)?,
        _ => anyhow::bail!(
            "unknown template '{template_name}'. Available: duckdb, databricks-fivetran, snowflake"
        ),
    }

    Ok(())
}

fn init_duckdb(dir: &Path) -> Result<()> {
    // rocky.toml — minimal, runnable
    std::fs::write(
        dir.join("rocky.toml"),
        r#"# Rocky project — DuckDB local quickstart
# Seed: duckdb playground.duckdb < seeds/seed.sql
# Run:  rocky run --filter source=orders

[adapter]
type = "duckdb"
path = "playground.duckdb"

[pipeline]
source.schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }
target = { catalog_template = "main", schema_template = "staging__{source}" }
"#,
    )?;

    // models/ with _defaults.toml
    let models_dir = dir.join("models");
    std::fs::create_dir_all(&models_dir)?;

    std::fs::write(
        models_dir.join("_defaults.toml"),
        r#"[target]
catalog = "main"
schema = "silver"
"#,
    )?;

    // Sample model
    std::fs::write(
        models_dir.join("stg_orders.sql"),
        r#"SELECT
    order_id,
    customer_id,
    amount,
    status,
    _updated_at
FROM main.staging__orders.orders
"#,
    )?;

    std::fs::write(
        models_dir.join("stg_orders.toml"),
        r#"depends_on = []
"#,
    )?;

    // seeds/
    let seeds_dir = dir.join("seeds");
    std::fs::create_dir_all(&seeds_dir)?;

    std::fs::write(
        seeds_dir.join("seed.sql"),
        r#"-- Load sample data into DuckDB
CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE TABLE IF NOT EXISTS raw__orders.orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    status VARCHAR,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO raw__orders.orders VALUES
    (1, 100, 29.99, 'completed', CURRENT_TIMESTAMP),
    (2, 101, 49.99, 'pending',   CURRENT_TIMESTAMP),
    (3, 100, 15.00, 'completed', CURRENT_TIMESTAMP);
"#,
    )?;

    println!("Initialized Rocky project in {}", dir.display());
    println!();
    println!("  rocky.toml           — pipeline config (DuckDB)");
    println!("  models/_defaults.toml — shared model defaults");
    println!("  models/stg_orders.*  — sample transformation model");
    println!("  seeds/seed.sql       — sample data");
    println!();
    println!("Quick start:");
    println!("  duckdb playground.duckdb < seeds/seed.sql");
    println!("  rocky validate");
    println!("  rocky compile --models models/");
    Ok(())
}

fn init_databricks_fivetran(dir: &Path) -> Result<()> {
    std::fs::write(
        dir.join("rocky.toml"),
        r#"# Rocky pipeline configuration — Databricks + Fivetran
# Docs: https://github.com/rocky-data/rocky
#
# This template uses a flat single-tenant schema pattern: source
# schemas are named `src__<source>` (e.g. `src__orders`) and land in
# a single `warehouse` catalog under `stage__<source>` schemas.
#
# For a multi-tenant SaaS shape where each source carries a tenant
# plus a variable-length region hierarchy (e.g.
# `src__<tenant>__<region...>__<source>` → per-tenant catalogs), use
# components = ["tenant", "regions...", "source"] in schema_pattern
# and catalog_template = "{tenant}_warehouse" + schema_template =
# "stage__{regions}__{source}" below. See
# https://rocky-data.github.io/rocky/concepts/schema-patterns/ for
# the full pattern reference.

[adapter.databricks_prod]
type = "databricks"
host = "${DATABRICKS_HOST}"
http_path = "${DATABRICKS_HTTP_PATH}"
# PAT (takes precedence if set)
token = "${DATABRICKS_TOKEN:-}"
# OAuth M2M (used if token is empty)
# client_id = "${DATABRICKS_CLIENT_ID}"
# client_secret = "${DATABRICKS_CLIENT_SECRET}"

[adapter.fivetran_main]
type = "fivetran"
# Fivetran is metadata-only: Rocky calls the Fivetran REST API to
# enumerate connectors. It does not move data — that's the warehouse
# adapter above.
kind = "discovery"
destination_id = "${FIVETRAN_DESTINATION_ID}"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"

[pipeline.raw_replication]
strategy = "incremental"
timestamp_column = "_fivetran_synced"

[pipeline.raw_replication.source]
adapter = "databricks_prod"
catalog = "${FIVETRAN_SOURCE_CATALOG:-}"

[pipeline.raw_replication.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]

[pipeline.raw_replication.source.discovery]
adapter = "fivetran_main"

[pipeline.raw_replication.target]
adapter = "databricks_prod"
catalog_template = "warehouse"
schema_template = "stage__{source}"

[pipeline.raw_replication.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

# [pipeline.raw_replication.target.governance.tags]
# managed_by = "rocky"

[pipeline.raw_replication.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }

[pipeline.raw_replication.execution]
concurrency = 8
"#,
    )?;

    let models_dir = dir.join("models");
    std::fs::create_dir_all(&models_dir)?;

    println!("Initialized Rocky project in {}", dir.display());
    println!();
    println!("  rocky.toml  — Databricks + Fivetran pipeline");
    println!("  models/     — transformation models");
    println!();
    println!("Next steps:");
    println!("  1. Set environment variables (DATABRICKS_HOST, etc.)");
    println!("  2. rocky validate");
    println!("  3. rocky discover");
    println!("  4. rocky plan --filter source=orders");
    Ok(())
}

fn init_snowflake(dir: &Path) -> Result<()> {
    std::fs::write(
        dir.join("rocky.toml"),
        r#"# Rocky pipeline configuration — Snowflake
# Docs: https://github.com/rocky-data/rocky

[adapter.snowflake_prod]
type = "snowflake"
# account = "${SNOWFLAKE_ACCOUNT}"
# username = "${SNOWFLAKE_USER}"
# private_key_path = "${SNOWFLAKE_KEY_PATH}"
# warehouse = "${SNOWFLAKE_WAREHOUSE}"

[pipeline.raw_replication]
strategy = "incremental"
timestamp_column = "_loaded_at"

[pipeline.raw_replication.source]
adapter = "snowflake_prod"

[pipeline.raw_replication.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.raw_replication.target]
adapter = "snowflake_prod"
catalog_template = "analytics"
schema_template = "staging__{source}"

[pipeline.raw_replication.execution]
concurrency = 4
"#,
    )?;

    let models_dir = dir.join("models");
    std::fs::create_dir_all(&models_dir)?;

    println!("Initialized Rocky project in {}", dir.display());
    println!();
    println!("  rocky.toml  — Snowflake pipeline");
    println!("  models/     — transformation models");
    println!();
    println!("Next steps:");
    println!("  1. Set environment variables (SNOWFLAKE_ACCOUNT, etc.)");
    println!("  2. rocky validate");
    println!("  3. rocky discover");
    Ok(())
}
