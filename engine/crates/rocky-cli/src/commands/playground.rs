//! `rocky playground` — zero-friction onboarding with DuckDB.
//!
//! Creates a self-contained sample project with models, data, and a
//! DuckDB pipeline config. No external credentials needed.
//!
//! Templates (`--template`), each held to the same contract: a fresh scaffold
//! passes `rocky compile && rocky test && rocky run` first try, and `rocky run`
//! builds every model into the persistent DuckDB file. The seed is loaded at
//! scaffold time for every template. `engine/rocky/tests/playground_templates.rs`
//! runs that contract against every name in [`PLAYGROUND_TEMPLATES`].
//!
//! - `quickstart` (default): 3 models, basic pipeline
//!
//! The `ecommerce` and `showcase` templates were removed (#1983): their
//! scaffold was a replication pipeline over an in-memory database, so
//! `rocky run` built none of their models. Their names and aliases are refused
//! with a message that lists the remaining templates.

use std::path::Path;

use anyhow::{Context, Result};

/// Available playground templates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Template {
    /// Minimal: 3 models, basic full refresh pipeline
    Quickstart,
}

/// Every template name `rocky playground --template` accepts, canonical
/// spelling only (aliases excluded). The scaffold contract test iterates this.
pub const PLAYGROUND_TEMPLATES: &[&str] = &["quickstart"];

/// Template names (with their aliases) that earlier releases accepted and that
/// are now refused, with the reason shown to the user.
const REMOVED_TEMPLATES: &[(&[&str], &str)] = &[(
    &["ecommerce", "ecom", "shop", "showcase", "all", "full"],
    "its project built no models on `rocky run` (#1983)",
)];

impl Template {
    pub fn from_str(s: &str) -> Result<Self> {
        let name = s.to_lowercase();
        match name.as_str() {
            "quickstart" | "quick" | "qs" => Ok(Template::Quickstart),
            other => {
                let available = PLAYGROUND_TEMPLATES.join(", ");
                if let Some((_, reason)) = REMOVED_TEMPLATES
                    .iter()
                    .find(|(names, _)| names.contains(&other))
                {
                    anyhow::bail!(
                        "template '{other}' was removed: {reason}. Available: {available}"
                    )
                }
                anyhow::bail!("unknown template '{other}'. Available: {available}")
            }
        }
    }

    /// Label printed in the scaffold summary.
    fn label(self) -> &'static str {
        match self {
            Template::Quickstart => "Quickstart (3 models)",
        }
    }

    /// The seed script written to `data/seed.sql` and loaded into
    /// `playground.duckdb` at scaffold time.
    fn seed(self) -> &'static str {
        match self {
            Template::Quickstart => QUICKSTART_SEED,
        }
    }

    /// A model the template builds, named in the `rocky preview rows` hint.
    fn preview_model(self) -> &'static str {
        match self {
            Template::Quickstart => "customer_orders",
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
    }
    std::fs::write(dir.join("data/seed.sql"), template.seed())?;

    // Auto-seed the persistent DuckDB file so the
    // `rocky compile && rocky test && rocky run` golden path works first-try
    // with no manual `duckdb playground.duckdb < data/seed.sql` step and no
    // DuckDB-CLI prerequisite. Every template's `rocky.toml` points its
    // adapter at `playground.duckdb`; we load the seed into that exact file.
    seed_playground_db(dir, template.seed())?;

    let preview_model = template.preview_model();
    let preview_cmd = format!("rocky preview rows --model {preview_model}");

    println!("  Rocky Playground");
    println!();
    println!("  Created sample project at ./{target_dir}/");
    println!("  Template: {}", template.label());
    println!("  Using DuckDB (local, no warehouse needed)");
    println!();
    println!("  Try:");
    println!("    cd {target_dir}");
    println!("    rocky compile                           # type-check the models");
    println!("    rocky test                              # run models on an in-memory DuckDB");
    println!("    rocky run                               # materialize the model DAG");
    println!("    {preview_cmd}  # peek at materialized rows");
    println!();

    Ok(())
}

/// Load the template's seed into the persistent `playground.duckdb` file so
/// `rocky run` has source tables to read on the very first invocation.
///
/// The seed is applied to `<dir>/playground.duckdb` — the exact path the
/// scaffolded `rocky.toml` adapter points at. We open the file, execute the
/// seed script, then drop the connection before returning so the file isn't
/// locked when the user's `rocky run` opens it.
#[cfg(feature = "duckdb")]
fn seed_playground_db(dir: &Path, seed: &str) -> Result<()> {
    use rocky_duckdb::DuckDbConnector;

    let db_path = dir.join("playground.duckdb");
    let conn = DuckDbConnector::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    conn.execute_statement(seed)
        .context("failed to seed playground.duckdb")?;
    // `conn` drops here, releasing the file lock before the next-steps print.
    Ok(())
}

/// No-op when the `duckdb` feature is disabled: the scaffold still writes
/// `data/seed.sql`, but such builds cannot run DuckDB anyway.
#[cfg(not(feature = "duckdb"))]
fn seed_playground_db(_dir: &Path, _seed: &str) -> Result<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Quickstart template
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

    Ok(())
}

const QUICKSTART_SEED: &str = r#"-- Seed data for quickstart playground.
--
-- `rocky playground` auto-loads this file into the persistent
-- `playground.duckdb` during scaffold, so `rocky run` works first-try with
-- no manual setup. `rocky test` also auto-loads it into an in-memory DuckDB
-- before executing models.
--
-- Re-seed at any time (e.g. after editing this file) with:
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_listed_template_parses() {
        for name in PLAYGROUND_TEMPLATES {
            Template::from_str(name).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    /// #1983: the removed templates and their aliases fail with a message
    /// that says they were removed and lists what remains.
    #[test]
    fn removed_templates_and_aliases_are_refused_with_the_remaining_list() {
        for name in [
            "ecommerce",
            "ecom",
            "shop",
            "showcase",
            "all",
            "full",
            "Showcase",
        ] {
            let msg = Template::from_str(name).unwrap_err().to_string();
            assert!(msg.contains("was removed"), "{name}: {msg}");
            assert!(msg.contains("Available: quickstart"), "{name}: {msg}");
            assert!(!msg.contains("ecommerce, showcase"), "{name}: {msg}");
        }
    }

    #[test]
    fn unknown_template_lists_the_remaining_templates() {
        let msg = Template::from_str("nope").unwrap_err().to_string();
        assert_eq!(msg, "unknown template 'nope'. Available: quickstart");
    }

    #[test]
    fn a_removed_template_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("demo");
        run_playground_with_template(target.to_str().unwrap(), "ecommerce").unwrap_err();
        assert!(!target.exists());
    }
}
