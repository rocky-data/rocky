//! `rocky playground` — zero-friction onboarding with DuckDB.
//!
//! Creates a self-contained sample project with models, data, and a
//! DuckDB pipeline config. No external credentials needed.
//!
//! Supports multiple templates via `--template`:
//! - `quickstart` (default): 3 models, basic pipeline
//! - `ecommerce`: orders/customers/products, incremental + merge
//! - `showcase`: every Rocky feature in one project

use std::path::Path;

use anyhow::{Context, Result};

/// Available playground templates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Template {
    /// Minimal: 3 models, basic full refresh pipeline
    Quickstart,
    /// E-commerce: 4 sources, staging, intermediate, marts with incremental + merge
    Ecommerce,
    /// Every Rocky feature demonstrated in one project
    Showcase,
}

impl Template {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "quickstart" | "quick" | "qs" => Ok(Template::Quickstart),
            "ecommerce" | "ecom" | "shop" => Ok(Template::Ecommerce),
            "showcase" | "all" | "full" => Ok(Template::Showcase),
            other => anyhow::bail!(
                "unknown template '{other}'. Available: quickstart, ecommerce, showcase"
            ),
        }
    }
}

/// Execute `rocky playground`.
pub fn run_playground(target_dir: &str) -> Result<()> {
    run_playground_with_template(target_dir, "quickstart")
}

/// Execute `rocky playground` with a specific template.
pub fn run_playground_with_template(target_dir: &str, template_name: &str) -> Result<()> {
    let template = Template::from_str(template_name)?;
    let dir = Path::new(target_dir);

    if dir.exists() {
        anyhow::bail!("directory '{}' already exists", dir.display());
    }

    std::fs::create_dir_all(dir.join("models")).context("failed to create models directory")?;
    std::fs::create_dir_all(dir.join("contracts"))
        .context("failed to create contracts directory")?;
    std::fs::create_dir_all(dir.join("data")).context("failed to create data directory")?;

    match template {
        Template::Quickstart => write_quickstart(dir)?,
        Template::Ecommerce => write_ecommerce(dir)?,
        Template::Showcase => write_showcase(dir)?,
    }

    let template_label = match template {
        Template::Quickstart => "Quickstart (3 models)",
        Template::Ecommerce => "E-Commerce (11 models, incremental + merge)",
        Template::Showcase => "Showcase (every Rocky feature)",
    };

    println!("  Rocky Playground");
    println!();
    println!("  Created sample project at ./{target_dir}/");
    println!("  Template: {template_label}");
    println!("  Using DuckDB (local, no warehouse needed)");
    println!();
    println!("  Try:");
    println!("    cd {target_dir}");
    println!("    rocky compile                           # type-check the models");
    println!("    rocky test                              # run models on DuckDB");
    println!("    rocky bench compile                     # benchmark compile speed");
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// Quickstart template (existing, enhanced)
// ---------------------------------------------------------------------------

fn write_quickstart(dir: &Path) -> Result<()> {
    // rocky.toml
    std::fs::write(
        dir.join("rocky.toml"),
        include_str!("playground_data/rocky.toml"),
    )?;

    // Models
    std::fs::write(
        dir.join("models/raw_orders.sql"),
        include_str!("playground_data/raw_orders.sql"),
    )?;
    std::fs::write(
        dir.join("models/raw_orders.toml"),
        include_str!("playground_data/raw_orders.toml"),
    )?;
    std::fs::write(
        dir.join("models/customer_orders.rocky"),
        include_str!("playground_data/customer_orders.rocky"),
    )?;
    std::fs::write(
        dir.join("models/customer_orders.toml"),
        include_str!("playground_data/customer_orders.toml"),
    )?;
    std::fs::write(
        dir.join("models/revenue_summary.sql"),
        include_str!("playground_data/revenue_summary.sql"),
    )?;
    std::fs::write(
        dir.join("models/revenue_summary.toml"),
        include_str!("playground_data/revenue_summary.toml"),
    )?;

    // Contract
    std::fs::write(
        dir.join("contracts/revenue_summary.contract.toml"),
        include_str!("playground_data/revenue_summary.contract.toml"),
    )?;

    // Seed data
    std::fs::write(dir.join("data/seed.sql"), QUICKSTART_SEED)?;

    Ok(())
}

const QUICKSTART_SEED: &str = r#"-- Seed data for quickstart playground.
--
-- `rocky test` auto-loads this file into an in-memory DuckDB before
-- executing models, so the playground is runnable end-to-end with no
-- manual setup.
--
-- For the `rocky discover/plan/run` flow you need to seed the persistent
-- file referenced by `path` in rocky.toml first:
--   duckdb playground.duckdb < data/seed.sql

CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 50) AS customer_id,
    1 + (i % 20) AS product_id,
    ROUND(CAST(5.0 + random() * 495.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN random() < 0.05 THEN 'cancelled'
         WHEN random() < 0.10 THEN 'pending'
         ELSE 'completed' END AS status,
    CAST(TIMESTAMP '2025-06-01' + INTERVAL (i * 3600) SECOND AS DATE) AS order_date,
    TIMESTAMP '2026-01-01' + INTERVAL (i * 60) SECOND AS _updated_at
FROM generate_series(1, 500) AS t(i);
"#;

// ---------------------------------------------------------------------------
// E-commerce template
// ---------------------------------------------------------------------------

fn write_ecommerce(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir.join("models/sources"))?;
    std::fs::create_dir_all(dir.join("models/staging"))?;
    std::fs::create_dir_all(dir.join("models/intermediate"))?;
    std::fs::create_dir_all(dir.join("models/marts"))?;

    // Pipeline config
    std::fs::write(dir.join("rocky.toml"), ECOMMERCE_PIPELINE)?;

    // --- Source models ---
    std::fs::write(
        dir.join("models/sources/raw_orders.sql"),
        ECOM_RAW_ORDERS_SQL,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_orders.toml"),
        ECOM_RAW_ORDERS_TOML,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_customers.sql"),
        ECOM_RAW_CUSTOMERS_SQL,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_customers.toml"),
        ECOM_RAW_CUSTOMERS_TOML,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_products.sql"),
        ECOM_RAW_PRODUCTS_SQL,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_products.toml"),
        ECOM_RAW_PRODUCTS_TOML,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_order_items.sql"),
        ECOM_RAW_ORDER_ITEMS_SQL,
    )?;
    std::fs::write(
        dir.join("models/sources/raw_order_items.toml"),
        ECOM_RAW_ORDER_ITEMS_TOML,
    )?;

    // --- Staging models ---
    std::fs::write(
        dir.join("models/staging/stg_orders.sql"),
        ECOM_STG_ORDERS_SQL,
    )?;
    std::fs::write(
        dir.join("models/staging/stg_orders.toml"),
        ECOM_STG_ORDERS_TOML,
    )?;
    std::fs::write(
        dir.join("models/staging/stg_customers.sql"),
        ECOM_STG_CUSTOMERS_SQL,
    )?;
    std::fs::write(
        dir.join("models/staging/stg_customers.toml"),
        ECOM_STG_CUSTOMERS_TOML,
    )?;

    // --- Intermediate ---
    std::fs::write(
        dir.join("models/intermediate/int_order_totals.sql"),
        ECOM_INT_ORDER_TOTALS_SQL,
    )?;
    std::fs::write(
        dir.join("models/intermediate/int_order_totals.toml"),
        ECOM_INT_ORDER_TOTALS_TOML,
    )?;

    // --- Marts ---
    std::fs::write(dir.join("models/marts/fct_orders.sql"), ECOM_FCT_ORDERS_SQL)?;
    std::fs::write(
        dir.join("models/marts/fct_orders.toml"),
        ECOM_FCT_ORDERS_TOML,
    )?;
    std::fs::write(
        dir.join("models/marts/dim_customers.sql"),
        ECOM_DIM_CUSTOMERS_SQL,
    )?;
    std::fs::write(
        dir.join("models/marts/dim_customers.toml"),
        ECOM_DIM_CUSTOMERS_TOML,
    )?;
    std::fs::write(
        dir.join("models/marts/fct_daily_revenue.sql"),
        ECOM_FCT_DAILY_REVENUE_SQL,
    )?;
    std::fs::write(
        dir.join("models/marts/fct_daily_revenue.toml"),
        ECOM_FCT_DAILY_REVENUE_TOML,
    )?;

    // Contract
    std::fs::write(
        dir.join("contracts/fct_orders.contract.toml"),
        ECOM_FCT_ORDERS_CONTRACT,
    )?;

    // Seed data
    std::fs::write(dir.join("data/seed.sql"), ECOMMERCE_SEED)?;

    Ok(())
}

const ECOMMERCE_PIPELINE: &str = r#"# Rocky E-Commerce Playground — DuckDB
# Demonstrates: sources, staging, intermediate, marts, incremental, contracts

[adapter.local]
type = "duckdb"

[pipeline.ecommerce]
type = "replication"
strategy = "incremental"
timestamp_column = "_fivetran_synced"

[pipeline.ecommerce.source]
adapter = "local"

[pipeline.ecommerce.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ecommerce.target]
adapter = "local"
catalog_template = "warehouse"
schema_template = "ecommerce__{source}"

[pipeline.ecommerce.target.governance]
auto_create_catalogs = false
auto_create_schemas = false

[pipeline.ecommerce.checks]
row_count = true
column_match = true

[pipeline.ecommerce.execution]
concurrency = 4

[state]
backend = "local"
"#;

// --- Source models ---
const ECOM_RAW_ORDERS_SQL: &str = "SELECT order_id, customer_id, order_date, status, total_amount, _fivetran_synced\nFROM source.raw_orders\n";
const ECOM_RAW_ORDERS_TOML: &str = "name = \"raw_orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"sources\"\ntable = \"raw_orders\"\n";

const ECOM_RAW_CUSTOMERS_SQL: &str = "SELECT customer_id, name, email, tier, signup_date, _fivetran_synced\nFROM source.raw_customers\n";
const ECOM_RAW_CUSTOMERS_TOML: &str = "name = \"raw_customers\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"sources\"\ntable = \"raw_customers\"\n";

const ECOM_RAW_PRODUCTS_SQL: &str =
    "SELECT product_id, name, category, price, _fivetran_synced\nFROM source.raw_products\n";
const ECOM_RAW_PRODUCTS_TOML: &str = "name = \"raw_products\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"sources\"\ntable = \"raw_products\"\n";

const ECOM_RAW_ORDER_ITEMS_SQL: &str = "SELECT order_id, product_id, quantity, unit_price, _fivetran_synced\nFROM source.raw_order_items\n";
const ECOM_RAW_ORDER_ITEMS_TOML: &str = "name = \"raw_order_items\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"sources\"\ntable = \"raw_order_items\"\n";

// --- Staging models ---
const ECOM_STG_ORDERS_SQL: &str = r#"SELECT
    order_id,
    customer_id,
    order_date,
    LOWER(status) AS status,
    total_amount,
    _fivetran_synced
FROM raw_orders
WHERE status != 'cancelled'
"#;
const ECOM_STG_ORDERS_TOML: &str = "name = \"stg_orders\"\ndepends_on = [\"raw_orders\"]\n\n[strategy]\ntype = \"incremental\"\ntimestamp_column = \"_fivetran_synced\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"staging\"\ntable = \"stg_orders\"\n";

const ECOM_STG_CUSTOMERS_SQL: &str = r#"SELECT
    customer_id,
    TRIM(name) AS name,
    LOWER(email) AS email,
    COALESCE(tier, 'bronze') AS tier,
    signup_date,
    _fivetran_synced
FROM raw_customers
"#;
const ECOM_STG_CUSTOMERS_TOML: &str = "name = \"stg_customers\"\ndepends_on = [\"raw_customers\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"staging\"\ntable = \"stg_customers\"\n";

// --- Intermediate models ---
const ECOM_INT_ORDER_TOTALS_SQL: &str = r#"SELECT
    o.customer_id,
    COUNT(*) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM stg_orders o
GROUP BY o.customer_id
"#;
const ECOM_INT_ORDER_TOTALS_TOML: &str = "name = \"int_order_totals\"\ndepends_on = [\"stg_orders\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"intermediate\"\ntable = \"int_order_totals\"\n";

// --- Mart models ---
const ECOM_FCT_ORDERS_SQL: &str = r#"SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.tier AS customer_tier,
    o.order_date,
    o.status,
    o.total_amount,
    o._fivetran_synced
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
"#;
const ECOM_FCT_ORDERS_TOML: &str = "name = \"fct_orders\"\ndepends_on = [\"stg_orders\", \"stg_customers\"]\n\n[strategy]\ntype = \"incremental\"\ntimestamp_column = \"_fivetran_synced\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"marts\"\ntable = \"fct_orders\"\n";

const ECOM_DIM_CUSTOMERS_SQL: &str = r#"SELECT
    c.customer_id,
    c.name,
    c.email,
    c.tier,
    c.signup_date,
    COALESCE(t.total_orders, 0) AS lifetime_orders,
    COALESCE(t.total_revenue, 0) AS lifetime_revenue,
    t.avg_order_value,
    t.first_order_date,
    t.last_order_date,
    CASE
        WHEN t.total_revenue > 10000 THEN 'whale'
        WHEN t.total_revenue > 1000 THEN 'regular'
        ELSE 'new'
    END AS customer_segment
FROM stg_customers c
LEFT JOIN int_order_totals t ON c.customer_id = t.customer_id
"#;
const ECOM_DIM_CUSTOMERS_TOML: &str = "name = \"dim_customers\"\ndepends_on = [\"stg_customers\", \"int_order_totals\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"marts\"\ntable = \"dim_customers\"\n";

const ECOM_FCT_DAILY_REVENUE_SQL: &str = r#"SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM stg_orders
GROUP BY order_date
ORDER BY order_date
"#;
const ECOM_FCT_DAILY_REVENUE_TOML: &str = "name = \"fct_daily_revenue\"\ndepends_on = [\"stg_orders\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"marts\"\ntable = \"fct_daily_revenue\"\n";

const ECOM_FCT_ORDERS_CONTRACT: &str = r#"[[columns]]
name = "order_id"
type = "Int64"
nullable = false

[[columns]]
name = "customer_id"
type = "Int64"
nullable = false

[[columns]]
name = "total_amount"
type = "Decimal"
nullable = false

[rules]
required = ["order_id", "customer_id", "total_amount", "order_date"]
"#;

const ECOMMERCE_SEED: &str = r#"-- Seed data for e-commerce playground
-- Run: duckdb playground.duckdb < data/seed.sql
SELECT SETSEED(0.42);

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE source.raw_customers AS
SELECT
    i AS customer_id,
    'Customer ' || i AS name,
    'customer' || i || '@shop.com' AS email,
    CASE WHEN random() < 0.1 THEN 'gold'
         WHEN random() < 0.3 THEN 'silver'
         ELSE 'bronze' END AS tier,
    CAST(TIMESTAMP '2024-01-01' + INTERVAL (random() * 365 * 86400) SECOND AS DATE) AS signup_date,
    TIMESTAMP '2026-03-01' + INTERVAL (random() * 30 * 86400) SECOND AS _fivetran_synced
FROM generate_series(1, 200) AS t(i);

CREATE TABLE source.raw_products AS
SELECT
    i AS product_id,
    'Product ' || i AS name,
    CASE (i % 5) WHEN 0 THEN 'electronics' WHEN 1 THEN 'clothing'
         WHEN 2 THEN 'food' WHEN 3 THEN 'books' ELSE 'home' END AS category,
    ROUND(CAST(5.0 + random() * 995.0 AS DECIMAL(10,2)), 2) AS price,
    TIMESTAMP '2026-03-01' + INTERVAL (random() * 30 * 86400) SECOND AS _fivetran_synced
FROM generate_series(1, 50) AS t(i);

CREATE TABLE source.raw_orders AS
SELECT
    i AS order_id,
    1 + CAST(floor(pow(random(), 2) * 200) AS INTEGER) AS customer_id,
    CAST(TIMESTAMP '2025-01-01' + INTERVAL (random() * 365 * 86400) SECOND AS DATE) AS order_date,
    CASE WHEN random() < 0.05 THEN 'cancelled'
         WHEN random() < 0.10 THEN 'pending'
         ELSE 'completed' END AS status,
    ROUND(CAST(10.0 + random() * 990.0 AS DECIMAL(10,2)), 2) AS total_amount,
    TIMESTAMP '2026-01-01' + INTERVAL (i * 86400 * 90 / 2000) SECOND AS _fivetran_synced
FROM generate_series(1, 2000) AS t(i);

CREATE TABLE source.raw_order_items AS
SELECT
    o.order_id,
    1 + CAST(floor(random() * 50) AS INTEGER) AS product_id,
    1 + CAST(floor(random() * 5) AS INTEGER) AS quantity,
    ROUND(CAST(5.0 + random() * 200.0 AS DECIMAL(10,2)), 2) AS unit_price,
    o._fivetran_synced
FROM source.raw_orders o,
     generate_series(1, 1 + CAST(floor(random() * 3) AS INTEGER)) AS items(n);
"#;

// ---------------------------------------------------------------------------
// Showcase template
// ---------------------------------------------------------------------------

fn write_showcase(dir: &Path) -> Result<()> {
    // Showcase includes everything from ecommerce plus extra features
    write_ecommerce(dir)?;

    // Add a Rocky DSL model
    std::fs::write(
        dir.join("models/staging/stg_products.rocky"),
        SHOWCASE_STG_PRODUCTS_ROCKY,
    )?;
    std::fs::write(
        dir.join("models/staging/stg_products.toml"),
        SHOWCASE_STG_PRODUCTS_TOML,
    )?;

    // Add a contract for dim_customers
    std::fs::write(
        dir.join("contracts/dim_customers.contract.toml"),
        SHOWCASE_DIM_CUSTOMERS_CONTRACT,
    )?;

    Ok(())
}

const SHOWCASE_STG_PRODUCTS_ROCKY: &str = r#"-- Rocky DSL staging model (compiles to SQL)
from raw_products
where price > 0
derive {
    price_tier: match price {
        > 500 => "premium",
        > 100 => "standard",
        _     => "budget"
    }
}
select {
    product_id,
    name,
    category,
    price,
    price_tier
}
"#;

const SHOWCASE_STG_PRODUCTS_TOML: &str = "name = \"stg_products\"\ndepends_on = [\"raw_products\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"staging\"\ntable = \"stg_products\"\n";

const SHOWCASE_DIM_CUSTOMERS_CONTRACT: &str = r#"[[columns]]
name = "customer_id"
type = "Int64"
nullable = false

[[columns]]
name = "name"
type = "String"
nullable = false

[[columns]]
name = "email"
type = "String"
nullable = false

[rules]
required = ["customer_id", "name", "email", "tier"]
"#;
