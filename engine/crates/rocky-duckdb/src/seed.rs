//! Deterministic test data generator for DuckDB.
//!
//! Generates realistic seed data at configurable scales using DuckDB's
//! built-in `generate_series()` and `SETSEED()` for reproducibility.
//! Used by the playground, integration tests, and benchmarks.

use crate::{DuckDbConnector, DuckDbError};

/// Scale factor for seed data generation.
#[derive(Debug, Clone, Copy)]
pub enum SeedScale {
    /// ~10k orders, ~1k customers, ~100 products
    Small,
    /// ~100k orders, ~10k customers, ~1k products
    Medium,
    /// ~1M orders, ~100k customers, ~10k products
    Large,
}

impl SeedScale {
    fn order_count(self) -> u64 {
        match self {
            SeedScale::Small => 10_000,
            SeedScale::Medium => 100_000,
            SeedScale::Large => 1_000_000,
        }
    }

    fn customer_count(self) -> u64 {
        match self {
            SeedScale::Small => 1_000,
            SeedScale::Medium => 10_000,
            SeedScale::Large => 100_000,
        }
    }

    fn product_count(self) -> u64 {
        match self {
            SeedScale::Small => 100,
            SeedScale::Medium => 1_000,
            SeedScale::Large => 10_000,
        }
    }
}

/// Stats returned after seeding a database.
#[derive(Debug, Clone)]
pub struct SeedStats {
    pub tables: Vec<(String, u64)>,
}

/// Seed a DuckDB database with deterministic test data.
///
/// Creates three source tables in the `source` schema:
/// - `source.raw_customers` — customer dimension
/// - `source.raw_products` — product dimension
/// - `source.raw_orders` — fact table with foreign keys + `_fivetran_synced`
///
/// Data is deterministic via `SETSEED(0.42)` — same scale always produces
/// identical data, making tests reproducible.
pub fn seed_database(conn: &DuckDbConnector, scale: SeedScale) -> Result<SeedStats, DuckDbError> {
    let customers = scale.customer_count();
    let products = scale.product_count();
    let orders = scale.order_count();

    // Set deterministic seed for all random() calls
    conn.execute_statement("SELECT SETSEED(0.42)")?;

    // Create source schema
    conn.execute_statement("CREATE SCHEMA IF NOT EXISTS source")?;

    // --- raw_customers ---
    conn.execute_statement(&format!(
        "CREATE OR REPLACE TABLE source.raw_customers AS
         SELECT
             i AS customer_id,
             'customer_' || i AS name,
             'customer' || i || '@example.com' AS email,
             TIMESTAMP '2024-01-01' + INTERVAL (i * 86400 / {customers}) SECOND AS signup_date,
             CASE WHEN random() < 0.1 THEN 'gold'
                  WHEN random() < 0.3 THEN 'silver'
                  ELSE 'bronze' END AS tier,
             TIMESTAMP '2026-03-01' + INTERVAL (random() * 86400 * 30) SECOND AS _fivetran_synced
         FROM generate_series(1, {customers}) AS t(i)"
    ))?;

    // Reset seed for next table
    conn.execute_statement("SELECT SETSEED(0.42)")?;

    // --- raw_products ---
    conn.execute_statement(&format!(
        "CREATE OR REPLACE TABLE source.raw_products AS
         SELECT
             i AS product_id,
             'product_' || i AS name,
             CASE (i % 5)
                 WHEN 0 THEN 'electronics'
                 WHEN 1 THEN 'clothing'
                 WHEN 2 THEN 'food'
                 WHEN 3 THEN 'books'
                 ELSE 'home' END AS category,
             ROUND(CAST(5.0 + random() * 995.0 AS DECIMAL(10,2)), 2) AS price,
             TIMESTAMP '2026-03-01' + INTERVAL (random() * 86400 * 30) SECOND AS _fivetran_synced
         FROM generate_series(1, {products}) AS t(i)"
    ))?;

    // Reset seed for next table
    conn.execute_statement("SELECT SETSEED(0.42)")?;

    // --- raw_orders ---
    // Uses Zipf-like distribution for customer_id (some customers order more)
    conn.execute_statement(&format!(
        "CREATE OR REPLACE TABLE source.raw_orders AS
         SELECT
             i AS order_id,
             CAST(1 + floor(pow(random(), 2) * {customers}) AS BIGINT) AS customer_id,
             CAST(1 + floor(random() * {products}) AS BIGINT) AS product_id,
             ROUND(CAST(1.0 + random() * 999.0 AS DECIMAL(10,2)), 2) AS amount,
             CASE WHEN random() < 0.05 THEN 'cancelled'
                  WHEN random() < 0.10 THEN 'pending'
                  WHEN random() < 0.15 THEN 'refunded'
                  ELSE 'completed' END AS status,
             CAST(TIMESTAMP '2025-01-01' + INTERVAL (random() * 86400 * 365) SECOND AS DATE) AS order_date,
             TIMESTAMP '2026-01-01' + INTERVAL (i * 86400 * 90 / {orders}) SECOND AS _fivetran_synced
         FROM generate_series(1, {orders}) AS t(i)"
    ))?;

    // Collect stats
    let mut tables = Vec::new();
    for table in &[
        "source.raw_customers",
        "source.raw_products",
        "source.raw_orders",
    ] {
        let result = conn.execute_sql(&format!("SELECT COUNT(*) FROM {table}"))?;
        let count: u64 = result.rows[0][0]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        tables.push((table.to_string(), count));
    }

    Ok(SeedStats { tables })
}

/// Seed a single flat table for simpler test scenarios.
///
/// Creates `{schema}.{table}` with the given columns and row count.
/// Column values are deterministic sequences.
pub fn seed_flat_table(
    conn: &DuckDbConnector,
    schema: &str,
    table: &str,
    columns: &[(&str, &str)],
    row_count: u64,
) -> Result<u64, DuckDbError> {
    conn.execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))?;

    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, dtype)| format!("{name} {dtype}"))
        .collect();

    conn.execute_statement(&format!(
        "CREATE OR REPLACE TABLE {schema}.{table} ({})",
        col_defs.join(", ")
    ))?;

    // Generate data with deterministic values based on column types
    conn.execute_statement("SELECT SETSEED(0.42)")?;

    let col_exprs: Vec<String> = columns
        .iter()
        .map(|(name, dtype)| {
            let dtype_upper = dtype.to_uppercase();
            let expr = if dtype_upper.contains("INT") {
                "i".to_string()
            } else if dtype_upper.contains("VARCHAR") || dtype_upper.contains("TEXT") || dtype_upper.contains("STRING") {
                format!("'{name}_' || i")
            } else if dtype_upper.contains("DECIMAL") || dtype_upper.contains("FLOAT") || dtype_upper.contains("DOUBLE") {
                "ROUND(CAST(random() * 1000.0 AS DECIMAL(10,2)), 2)".to_string()
            } else if dtype_upper.contains("TIMESTAMP") {
                format!("TIMESTAMP '2026-01-01' + INTERVAL (i * 86400 * 90 / {row_count}) SECOND")
            } else if dtype_upper.contains("DATE") {
                format!("CAST(TIMESTAMP '2025-01-01' + INTERVAL (i * 86400 * 365 / {row_count}) SECOND AS DATE)")
            } else if dtype_upper.contains("BOOL") {
                "CASE WHEN random() < 0.5 THEN true ELSE false END".to_string()
            } else {
                "i".to_string()
            };
            format!("{expr} AS {name}")
        })
        .collect();

    conn.execute_statement(&format!(
        "INSERT INTO {schema}.{table} SELECT {} FROM generate_series(1, {row_count}) AS t(i)",
        col_exprs.join(", ")
    ))?;

    Ok(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed_small() {
        let db = DuckDbConnector::in_memory().unwrap();
        let stats = seed_database(&db, SeedScale::Small).unwrap();

        assert_eq!(stats.tables.len(), 3);
        assert_eq!(stats.tables[0].0, "source.raw_customers");
        assert_eq!(stats.tables[0].1, 1_000);
        assert_eq!(stats.tables[1].0, "source.raw_products");
        assert_eq!(stats.tables[1].1, 100);
        assert_eq!(stats.tables[2].0, "source.raw_orders");
        assert_eq!(stats.tables[2].1, 10_000);
    }

    #[test]
    fn test_seed_deterministic() {
        // Two runs with same scale should produce identical data
        let db1 = DuckDbConnector::in_memory().unwrap();
        seed_database(&db1, SeedScale::Small).unwrap();
        let r1 = db1
            .execute_sql("SELECT SUM(amount) FROM source.raw_orders")
            .unwrap();

        let db2 = DuckDbConnector::in_memory().unwrap();
        seed_database(&db2, SeedScale::Small).unwrap();
        let r2 = db2
            .execute_sql("SELECT SUM(amount) FROM source.raw_orders")
            .unwrap();

        assert_eq!(r1.rows[0][0], r2.rows[0][0]);
    }

    #[test]
    fn test_seed_has_fivetran_synced() {
        let db = DuckDbConnector::in_memory().unwrap();
        seed_database(&db, SeedScale::Small).unwrap();

        let result = db
            .execute_sql(
                "SELECT COUNT(*) FROM source.raw_orders WHERE _fivetran_synced IS NOT NULL",
            )
            .unwrap();
        assert_eq!(result.rows[0][0], "10000");
    }

    #[test]
    fn test_seed_customer_tiers() {
        let db = DuckDbConnector::in_memory().unwrap();
        seed_database(&db, SeedScale::Small).unwrap();

        let result = db
            .execute_sql("SELECT DISTINCT tier FROM source.raw_customers ORDER BY tier")
            .unwrap();
        let tiers: Vec<&str> = result.rows.iter().filter_map(|r| r[0].as_str()).collect();
        assert!(tiers.contains(&"gold"));
        assert!(tiers.contains(&"silver"));
        assert!(tiers.contains(&"bronze"));
    }

    #[test]
    fn test_seed_flat_table() {
        let db = DuckDbConnector::in_memory().unwrap();
        let count = seed_flat_table(
            &db,
            "test",
            "my_table",
            &[
                ("id", "INTEGER"),
                ("name", "VARCHAR"),
                ("amount", "DECIMAL(10,2)"),
                ("ts", "TIMESTAMP"),
            ],
            500,
        )
        .unwrap();

        assert_eq!(count, 500);

        let result = db
            .execute_sql("SELECT COUNT(*) FROM test.my_table")
            .unwrap();
        assert_eq!(result.rows[0][0], "500");
    }
}
