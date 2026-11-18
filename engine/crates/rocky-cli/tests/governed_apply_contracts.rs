//! #2635: a governed `rocky apply` (an agent's apply of a `rocky plan` run
//! plan) must reach the same contract verdict as `rocky run` on the same
//! project.
//!
//! `rocky run` describes an external source that has no schema-cache entry
//! before it checks a contract (#2614). A run plan captures its reviewed
//! source snapshot from the schema cache only, and a governed apply replays
//! that snapshot, so neither sees the warehouse's types or NOT NULL.
//!
//! The project: one replication pipeline, plus a model
//! `SELECT customer_id FROM ext.customers`. The table
//! `ext.customers (customer_id BIGINT NOT NULL)` exists in the warehouse and
//! has no schema-cache entry.

#![cfg(feature = "duckdb")]

use std::path::{Path, PathBuf};

use rocky_cli::commands::plan::PlanRunOptions;
use rocky_cli::commands::plan_cost::CostEstimateMode;
use rocky_core::config::PrincipalRef;
use rocky_core::config::PolicyPrincipal;
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Write the project under `root` and seed its warehouse. `contract` is the
/// body of `contracts/fct.contract.toml`.
async fn project(root: &Path, contract: &str) -> (PathBuf, PathBuf) {
    let db = root.join("wh.duckdb");
    {
        let a = DuckDbWarehouseAdapter::open(&db).expect("open db");
        for sql in [
            "CREATE SCHEMA raw__orders",
            "CREATE TABLE raw__orders.orders (order_id INTEGER NOT NULL)",
            "INSERT INTO raw__orders.orders VALUES (1)",
            "CREATE SCHEMA ext",
            "CREATE TABLE ext.customers (customer_id BIGINT NOT NULL)",
            "INSERT INTO ext.customers VALUES (10)",
        ] {
            a.execute_statement(sql).await.expect(sql);
        }
    }
    let config = root.join("rocky.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{db}"

[pipeline.rep]
strategy = "full_refresh"

[pipeline.rep.source.discovery]
adapter = "default"

[pipeline.rep.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.rep.target]
catalog_template = "wh"
schema_template = "staging__{{source}}"

[pipeline.rep.target.governance]
auto_create_schemas = true

[pipeline.rep.checks]
row_count = false
column_match = false
"#,
            db = db.display()
        ),
    )
    .expect("config");
    let models = root.join("models");
    std::fs::create_dir_all(&models).expect("models dir");
    std::fs::write(models.join("fct.sql"), "SELECT customer_id FROM ext.customers\n")
        .expect("model sql");
    std::fs::write(
        models.join("fct.toml"),
        "name = \"fct\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
         [target]\ncatalog = \"wh\"\nschema = \"main\"\ntable = \"fct\"\n",
    )
    .expect("model toml");
    let contracts = root.join("contracts");
    std::fs::create_dir_all(&contracts).expect("contracts dir");
    std::fs::write(contracts.join("fct.contract.toml"), contract).expect("contract");
    (config, models)
}

/// Run `rocky plan --filter source=orders --models models` as an agent and
/// return the saved plan's id. The plan is written under the current
/// directory, which the caller has set to the project root.
async fn agent_plan(config: &Path, models: &Path, state: &Path) -> anyhow::Result<String> {
    let options = PlanRunOptions {
        models_dir: Some(models.to_path_buf()),
        principal: Some(PolicyPrincipal::Agent),
        ..Default::default()
    };
    rocky_cli::commands::plan::plan(
        config,
        Some("source=orders"),
        None,
        None,
        &options,
        false,
        "main",
        state,
        CostEstimateMode::Heuristic,
        true,
    )
    .await?;
    let plans = config.parent().expect("root").join(".rocky").join("plans");
    let mut ids: Vec<String> = std::fs::read_dir(&plans)?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_suffix(".json").map(str::to_string)
        })
        .collect();
    anyhow::ensure!(ids.len() == 1, "expected one saved plan, found {ids:?}");
    Ok(ids.remove(0))
}

/// Whether `main.fct` exists in the project's warehouse.
async fn fct_exists(root: &Path) -> bool {
    let a = DuckDbWarehouseAdapter::open(&root.join("wh.duckdb")).expect("reopen db");
    let rows = a
        .execute_query(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_name = 'fct'",
        )
        .await
        .expect("query");
    rows.rows[0][0].as_i64() == Some(1) || rows.rows[0][0].as_str() == Some("1")
}

/// A `nullable = false` contract on a column the warehouse reports NOT NULL.
/// `rocky run` passes it (#2614), so the plan must too, and the agent's apply
/// must write the model.
#[tokio::test]
async fn a_not_null_contract_run_passes_is_planned_and_applied() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?;
    std::env::set_current_dir(&root)?;
    let (config, models) = project(
        &root,
        "[[columns]]\nname = \"customer_id\"\nnullable = false\n",
    )
    .await;
    let state = root.join("state.redb");
    let plan_id = agent_plan(&config, &models, &state).await.map_err(|e| {
        anyhow::anyhow!("the plan must not refuse a contract `rocky run` passes: {e:#}")
    })?;
    rocky_cli::commands::apply::run_apply(
        &config,
        &plan_id,
        &state,
        PolicyPrincipal::Agent,
        &PrincipalRef::unnamed(),
        None,
        true,
    )
    .await?;
    assert!(fct_exists(&root).await, "the governed apply must write fct");
    Ok(())
}

/// A `type = "String"` contract on a BIGINT column. `rocky run` refuses it
/// with `E011` and writes nothing, so the agent's apply must not write it
/// either: at worst the plan refuses, at best the apply does.
#[tokio::test]
async fn a_wrong_type_contract_run_refuses_is_not_applied() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().canonicalize()?;
    std::env::set_current_dir(&root)?;
    let (config, models) = project(&root, "[[columns]]\nname = \"customer_id\"\ntype = \"String\"\n")
        .await;
    let state = root.join("state.redb");
    if let Ok(plan_id) = agent_plan(&config, &models, &state).await {
        // The apply may return Ok with the failure carried in its output, or
        // Err: either way the table must not be written.
        let _ = rocky_cli::commands::apply::run_apply(
            &config,
            &plan_id,
            &state,
            PolicyPrincipal::Agent,
            &PrincipalRef::unnamed(),
            None,
            true,
        )
        .await;
    }
    assert!(
        !fct_exists(&root).await,
        "a governed apply must not write a model whose contract type `rocky run` refuses (E011)"
    );
    Ok(())
}
