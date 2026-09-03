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
rocky-ir = {{ path = "../rocky-ir" }}
rocky-sql = {{ path = "../rocky-sql" }}
async-trait = {{ workspace = true }}
chrono = {{ workspace = true }}
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
        render_dialect_rs(name, &name_pascal),
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
    println!(
        "  2. Pick `literal_escape` in dialect.rs — the crate deliberately refuses to\n     \
         compile until you state how {name}'s lexer reads a quoted string"
    );
    println!("  3. Implement the remaining SqlDialect methods in dialect.rs");
    println!("  4. Add an HTTP connector in connector.rs");
    println!("  5. Implement WarehouseAdapter in adapter.rs");
    println!("  6. Add \"{name}\" case to registry.rs");

    Ok(())
}

/// Renders the scaffold's `src/dialect.rs`.
///
/// Split out from [`run_init_adapter`] so a test can assert on the text
/// without writing into the repository — `run_init_adapter` resolves its
/// output against a relative `crates/` path.
fn render_dialect_rs(name: &str, name_pascal: &str) -> String {
    format!(
        r#"//! {name} SQL dialect implementation.
//!
//! TODO: Implement each method for {name}-specific SQL syntax.

use rocky_ir::{{ColumnSelection, MetadataColumn}};
use rocky_core::traits::{{AdapterError, AdapterResult, LiteralEscape, SqlDialect}};

/// {name} SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct {name_pascal}SqlDialect;

impl SqlDialect for {name_pascal}SqlDialect {{
    /// How {name}'s lexer reads a single-quoted string literal.
    ///
    ///   - `LiteralEscape::Standard`  — no backslash escapes; a quote is
    ///     doubled (`''`), a backslash stands for itself, a line break stays
    ///     raw. Trino, DuckDB.
    ///   - `LiteralEscape::Backslash` — backslash escapes are read; a quote
    ///     is `\'`, a backslash `\\`, a line break `\n` / `\r`. Snowflake,
    ///     Databricks, BigQuery.
    ///
    /// **This does not compile until you choose.** The trait has no default
    /// and neither does the scaffold: the wrong rule corrupts values on some
    /// warehouses and lets a quote close the literal on others, and a guess
    /// that compiles is exactly how that ships unnoticed. Read {name}'s own
    /// lexer documentation, then prove it — encode a value holding a quote
    /// AND a backslash, `SELECT` it back, and assert it round-trips
    /// byte-identical. Replace the line below with your answer.
    fn literal_escape(&self) -> LiteralEscape {{
        compile_error!(
            "pick LiteralEscape::Standard or LiteralEscape::Backslash for {name} — \
             read its string-literal lexer documentation and prove the choice with a \
             round trip, then delete this line"
        )
    }}

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
        _keys: &[std::sync::Arc<str>],
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

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {{
        // Substitute the previous run's max source watermark as a literal —
        // the runner queries SELECT MAX(ts) FROM source post-execute and
        // records that value to the state store. None means "no prior
        // watermark" (first run / after delete_watermark); scan everything.
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!("WHERE {{timestamp_col}} > TIMESTAMP '{{literal}}'"))
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
    )
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

    /// Nothing in CI compiles the scaffold — the tests inside the template
    /// are text, not code — so a required trait method missing from the
    /// template would make `rocky init-adapter` emit a broken crate with
    /// nothing noticing. These three render tests are the whole guard.
    ///
    /// The list below is **hand-maintained**: this test cannot enumerate the
    /// trait's required methods, so it does not prove the scaffold is
    /// complete — only that it still emits the names named here. Add a name
    /// whenever a required method is added to `SqlDialect`.
    #[test]
    fn the_scaffolded_dialect_emits_the_methods_this_list_names() {
        let rendered = render_dialect_rs("redshift", "Redshift");

        for required in [
            "fn format_table_ref(",
            "fn create_table_as(",
            "fn insert_into(",
            "fn merge_into(",
            "fn select_clause(",
            "fn watermark_where(",
            "fn describe_table_sql(",
            "fn drop_table_sql(",
            "fn create_catalog_sql(",
            "fn create_schema_sql(",
            "fn tablesample_clause(",
            "fn insert_overwrite_partition(",
            "fn literal_escape(",
        ] {
            assert!(
                rendered.contains(required),
                "scaffolded dialect.rs is missing `{required}` — `rocky init-adapter` \
                 would emit a crate that does not compile"
            );
        }
    }

    /// The scaffold must not answer `literal_escape` for the author. A rule
    /// that compiles is a guess about somebody else's lexer, and a wrong one
    /// is silent: it corrupts values on some warehouses and lets a quote close
    /// the literal on others. So the emitted body is a `compile_error!` — the
    /// scaffolded crate refuses to build until a human states the fact.
    ///
    /// This is the test that would have to be deleted to reintroduce a
    /// default, which is the point of it.
    #[test]
    fn the_scaffolded_literal_escape_refuses_to_compile_until_it_is_chosen() {
        let rendered = render_dialect_rs("redshift", "Redshift");

        assert!(
            rendered.contains(
                "use rocky_core::traits::{AdapterError, AdapterResult, LiteralEscape, SqlDialect};"
            ),
            "scaffolded dialect.rs does not import LiteralEscape"
        );

        let (_, body) = rendered
            .split_once("fn literal_escape(&self) -> LiteralEscape {")
            .expect("literal_escape is rendered");
        let (body, _) = body.split_once("\n    }").expect("the method body closes");

        assert!(
            body.contains("compile_error!"),
            "scaffolded literal_escape must not compile until the author chooses: {body}"
        );
        assert!(
            body.contains("redshift"),
            "the refusal must name the adapter it is asking about: {body}"
        );

        // The refusal message names both variants on purpose, so look for a
        // variant used as a *value* — outside the message's quotes. That is
        // the silent default this method exists to remove.
        let code: String = body
            .split('"')
            .step_by(2)
            .collect::<Vec<_>>()
            .join(" <message> ");
        assert!(
            !code.contains("LiteralEscape::Standard") && !code.contains("LiteralEscape::Backslash"),
            "scaffolded literal_escape returns a variant instead of refusing: {code}"
        );
    }

    /// The refusal is only useful if it says how to answer it: both rules by
    /// name, and the evidence bar for picking one.
    #[test]
    fn the_scaffolded_literal_escape_documents_both_rules_and_the_evidence_bar() {
        let rendered = render_dialect_rs("redshift", "Redshift");
        let (doc, _) = rendered
            .split_once("fn literal_escape(")
            .expect("literal_escape is rendered");
        let (_, doc) = doc
            .rsplit_once("/// How redshift's lexer reads")
            .expect("literal_escape carries a doc comment naming the adapter");

        assert!(
            doc.contains("LiteralEscape::Standard") && doc.contains("LiteralEscape::Backslash"),
            "the doc must name both rules so the author picks one"
        );
        assert!(
            doc.contains("round-trips\n    /// byte-identical"),
            "the doc must ask for executed evidence, not a guess"
        );
    }
}
