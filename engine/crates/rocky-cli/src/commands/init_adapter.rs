//! `rocky init-adapter` — scaffold a new adapter crate.

use std::path::Path;

use anyhow::{Context, Result};

/// Execute `rocky init-adapter <name>`.
pub fn run_init_adapter(name: &str) -> Result<()> {
    let crate_name = format!("rocky-{name}");
    let crate_dir = Path::new("crates").join(&crate_name);

    if crate_dir.exists() {
        anyhow::bail!("directory '{}' already exists", crate_dir.display());
    }

    let src_dir = crate_dir.join("src");
    std::fs::create_dir_all(&src_dir).context("failed to create crate directory")?;

    // Cargo.toml
    std::fs::write(
        crate_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "{crate_name}"
version = "0.1.0"
description = "{name} warehouse adapter for Rocky"
edition.workspace = true
license.workspace = true
rust-version.workspace = true

[dependencies]
rocky-core = {{ path = "../rocky-core" }}
rocky-sql = {{ path = "../rocky-sql" }}
async-trait = {{ workspace = true }}
reqwest = {{ workspace = true }}
serde = {{ workspace = true }}
serde_json = {{ workspace = true }}
thiserror = {{ workspace = true }}
tracing = {{ workspace = true }}

[dev-dependencies]
tokio = {{ workspace = true }}
"#
        ),
    )?;

    // lib.rs
    std::fs::write(
        src_dir.join("lib.rs"),
        format!(
            r#"//! {name} warehouse adapter for Rocky.

pub mod adapter;
pub mod dialect;
pub mod types;
"#
        ),
    )?;

    // dialect.rs — SqlDialect skeleton
    let name_pascal = to_pascal_case(name);
    std::fs::write(
        src_dir.join("dialect.rs"),
        format!(
            r#"//! {name} SQL dialect implementation.
//!
//! TODO: Implement each method for {name}-specific SQL syntax.

use rocky_core::ir::{{ColumnSelection, MetadataColumn}};
use rocky_core::traits::{{AdapterError, AdapterResult, SqlDialect}};

/// {name} SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct {name_pascal}SqlDialect;

impl SqlDialect for {name_pascal}SqlDialect {{
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {{
        // TODO: Implement {name}-specific table reference formatting
        rocky_sql::validation::format_table_ref(catalog, schema, table).map_err(AdapterError::new)
    }}

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {{
        format!("CREATE OR REPLACE TABLE {{target}} AS\n{{select_sql}}")
    }}

    fn insert_into(&self, target: &str, select_sql: &str) -> String {{
        format!("INSERT INTO {{target}}\n{{select_sql}}")
    }}

    fn merge_into(
        &self,
        _target: &str,
        _source_sql: &str,
        _keys: &[String],
        _update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {{
        // TODO: Implement {name}-specific MERGE syntax
        Err(AdapterError::msg("MERGE not yet implemented for {name}"))
    }}

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {{
        let mut sql = String::from("SELECT ");
        match columns {{
            ColumnSelection::All => sql.push('*'),
            ColumnSelection::Explicit(cols) => sql.push_str(&cols.join(", ")),
        }}
        for mc in metadata {{
            sql.push_str(&format!(", CAST({{}} AS {{}}) AS {{}}", mc.value, mc.data_type, mc.name));
        }}
        Ok(sql)
    }}

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {{
        Ok(format!(
            "WHERE {{timestamp_col}} > (SELECT COALESCE(MAX({{timestamp_col}}), TIMESTAMP '1970-01-01') FROM {{target_ref}})"
        ))
    }}

    fn describe_table_sql(&self, table_ref: &str) -> String {{
        format!("DESCRIBE {{table_ref}}")
    }}

    fn drop_table_sql(&self, table_ref: &str) -> String {{
        format!("DROP TABLE IF EXISTS {{table_ref}}")
    }}

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {{
        // TODO: Implement if {name} supports catalog creation
        None
    }}

    fn create_schema_sql(&self, _catalog: &str, schema: &str) -> Option<AdapterResult<String>> {{
        Some(Ok(format!("CREATE SCHEMA IF NOT EXISTS {{schema}}")))
    }}

    fn tablesample_clause(&self, percent: u32) -> Option<String> {{
        Some(format!("TABLESAMPLE ({{percent}} PERCENT)"))
    }}

    fn insert_overwrite_partition(
        &self,
        _target: &str,
        _partition_filter: &str,
        _select_sql: &str,
    ) -> AdapterResult<Vec<String>> {{
        // TODO: Implement {name}-specific partition replacement for the
        // time_interval materialization strategy. Common patterns:
        //   - Delta Lake: vec![format!("INSERT INTO {{}} REPLACE WHERE {{}}", ...)]
        //   - Transactional: vec!["BEGIN", "DELETE...", "INSERT...", "COMMIT"]
        Err(AdapterError::msg(
            "insert_overwrite_partition not yet implemented for {name}",
        ))
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_format_table_ref() {{
        let d = {name_pascal}SqlDialect;
        assert!(d.format_table_ref("cat", "sch", "tbl").is_ok());
    }}

    #[test]
    fn test_create_table_as() {{
        let d = {name_pascal}SqlDialect;
        let sql = d.create_table_as("t", "SELECT 1");
        assert!(sql.contains("CREATE OR REPLACE TABLE"));
    }}
}}
"#
        ),
    )?;

    // adapter.rs — WarehouseAdapter skeleton
    std::fs::write(
        src_dir.join("adapter.rs"),
        format!(
            r#"//! {name} warehouse adapter implementing `WarehouseAdapter`.
//!
//! TODO: Implement the HTTP connector and wire it up here.

use crate::dialect::{name_pascal}SqlDialect;

/// {name} warehouse adapter.
pub struct {name_pascal}WarehouseAdapter {{
    dialect: {name_pascal}SqlDialect,
    // TODO: Add connector field
}}

impl {name_pascal}WarehouseAdapter {{
    pub fn new() -> Self {{
        Self {{
            dialect: {name_pascal}SqlDialect,
        }}
    }}
}}

// TODO: Implement WarehouseAdapter trait
// See crates/rocky-databricks/src/adapter.rs for reference
"#
        ),
    )?;

    // types.rs — TypeMapper skeleton
    std::fs::write(
        src_dir.join("types.rs"),
        format!(
            r#"//! {name} type mapper.

use rocky_core::traits::TypeMapper;

/// {name} type mapper.
#[derive(Debug, Clone, Default)]
pub struct {name_pascal}TypeMapper;

impl TypeMapper for {name_pascal}TypeMapper {{
    fn normalize_type(&self, warehouse_type: &str) -> String {{
        warehouse_type.trim().to_uppercase()
    }}

    fn types_compatible(&self, type_a: &str, type_b: &str) -> bool {{
        self.normalize_type(type_a) == self.normalize_type(type_b)
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_same_type_compatible() {{
        let mapper = {name_pascal}TypeMapper;
        assert!(mapper.types_compatible("VARCHAR", "VARCHAR"));
    }}
}}
"#
        ),
    )?;

    // Tests directory
    let tests_dir = crate_dir.join("tests");
    std::fs::create_dir_all(&tests_dir)?;
    std::fs::write(
        tests_dir.join("integration.rs"),
        format!(
            r#"//! Integration tests for {name} adapter.
//! These tests require live {name} credentials and are marked #[ignore].

#[test]
#[ignore]
fn test_live_connection() {{
    // TODO: Test against live {name} instance
}}
"#
        ),
    )?;

    println!("Created adapter scaffold at crates/{crate_name}/");
    println!();
    println!("  crates/{crate_name}/");
    println!("  ├── Cargo.toml");
    println!("  ├── src/");
    println!("  │   ├── lib.rs");
    println!("  │   ├── dialect.rs     ← SqlDialect trait (TODO: implement methods)");
    println!("  │   ├── adapter.rs     ← WarehouseAdapter trait (TODO: implement)");
    println!("  │   └── types.rs       ← TypeMapper trait");
    println!("  └── tests/");
    println!("      └── integration.rs ← Live tests (#[ignore])");
    println!();
    println!("Next steps:");
    println!("  1. Add \"{crate_name}\" to workspace members in Cargo.toml");
    println!("  2. Implement SqlDialect methods in dialect.rs");
    println!("  3. Add an HTTP connector in connector.rs");
    println!("  4. Implement WarehouseAdapter in adapter.rs");
    println!("  5. Add \"{name}\" case to registry.rs");

    Ok(())
}

fn to_pascal_case(s: &str) -> String {
    s.split(['_', '-'])
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pascal_case() {
        assert_eq!(to_pascal_case("big_query"), "BigQuery");
        assert_eq!(to_pascal_case("snowflake"), "Snowflake");
        assert_eq!(to_pascal_case("redshift"), "Redshift");
    }
}
