//! Live Databricks governance probes for view-aware Unity Catalog tag DDL.
//!
//! Verifies that [`DatabricksGovernanceAdapter::set_tags`] applies a model's
//! `[governance.tags]` to the right securable — `TagTarget::View` emits
//! `ALTER VIEW ... SET TAGS` and `TagTarget::Table` emits
//! `ALTER TABLE ... SET TAGS` — and that re-applying the same keys is
//! idempotent (an upsert, not an error).
//!
//! `#[ignore]`-gated. Sandbox config comes from `ROCKY_TEST_*` env vars,
//! falling back to the `DATABRICKS_*` vars the other live tests use:
//! `ROCKY_TEST_HOST` (or `DATABRICKS_HOST`), `ROCKY_TEST_HTTP_PATH` (or
//! `DATABRICKS_HTTP_PATH`), `ROCKY_TEST_TOKEN` (or `DATABRICKS_TOKEN`, or the
//! `ROCKY_TEST_CLIENT_ID` + `ROCKY_TEST_CLIENT_SECRET` OAuth pair), and
//! `ROCKY_TEST_CATALOG` (or `DATABRICKS_TEST_CATALOG` / `DATABRICKS_CATALOG_PREFIX`).
//!
//! No workspace identifiers are hardcoded — every schema/table/view is
//! `hc_`-prefixed and dropped CASCADE on exit.
//!
//! Run with:
//! `cargo test -p rocky-databricks --test governance_view_tags_live -- --ignored --nocapture`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::{GovernanceAdapter, TagTarget, WarehouseAdapter};
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::governance::DatabricksGovernanceAdapter;

/// Read a sandbox setting from a `ROCKY_TEST_*` var first, falling back to the
/// `DATABRICKS_*` vars the other live tests use. The operator-facing knobs are
/// `ROCKY_TEST_*`; the fallbacks keep this test runnable alongside the existing
/// live suite without re-exporting every var.
fn env_first(rocky_key: &str, fallbacks: &[&str]) -> Option<String> {
    if let Ok(v) = std::env::var(rocky_key) {
        return Some(v);
    }
    fallbacks.iter().find_map(|k| std::env::var(k).ok())
}

fn parts_from_env() -> Option<(DatabricksWarehouseAdapter, Arc<DatabricksConnector>, String)> {
    let host = env_first("ROCKY_TEST_HOST", &["DATABRICKS_HOST"])?;
    let http_path = env_first("ROCKY_TEST_HTTP_PATH", &["DATABRICKS_HTTP_PATH"])?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;

    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: env_first("ROCKY_TEST_TOKEN", &["DATABRICKS_TOKEN"]),
        client_id: env_first("ROCKY_TEST_CLIENT_ID", &["DATABRICKS_CLIENT_ID"]),
        client_secret: env_first("ROCKY_TEST_CLIENT_SECRET", &["DATABRICKS_CLIENT_SECRET"]),
    })
    .ok()?;

    let config = ConnectorConfig {
        host,
        warehouse_id,
        timeout: Duration::from_secs(120),
        retry: Default::default(),
    };
    // `DatabricksConnector::new` consumes its config + auth, so the adapter and
    // the governance-side connector each take an independent clone (both types
    // derive `Clone`).
    let connector = Arc::new(DatabricksConnector::new(config.clone(), auth.clone()));
    let adapter = DatabricksWarehouseAdapter::new(DatabricksConnector::new(config, auth));
    Some((adapter, connector, catalog_from_env()?))
}

fn catalog_from_env() -> Option<String> {
    env_first(
        "ROCKY_TEST_CATALOG",
        &["DATABRICKS_TEST_CATALOG", "DATABRICKS_CATALOG_PREFIX"],
    )
}

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

/// Count rows in `<catalog>.information_schema.<relation>_tags` matching a
/// schema/relation/tag-key/tag-value tuple. Used to confirm a tag actually
/// landed (and, on a second pass, that the value upserted rather than
/// duplicated). `relation` is `"table"` or `"view"`.
async fn tag_present(
    adapter: &DatabricksWarehouseAdapter,
    catalog: &str,
    relation_kind: &str,
    schema: &str,
    relation: &str,
    key: &str,
    value: &str,
) -> i64 {
    // Databricks exposes view tags through `table_tags` too (views live in the
    // `tables` relation), so both kinds query the same view. `relation_kind`
    // is threaded only to make the assertion message self-describing.
    let _ = relation_kind;
    let sql = format!(
        "SELECT COUNT(*) FROM {catalog}.information_schema.table_tags \
         WHERE schema_name = '{schema}' AND table_name = '{relation}' \
         AND tag_name = '{key}' AND tag_value = '{value}'"
    );
    match adapter.execute_query(&sql).await {
        Ok(result) => result
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0),
        Err(e) => {
            eprintln!("tag_present query failed: {e}");
            0
        }
    }
}

/// `ALTER VIEW ... SET TAGS` applies and is idempotent on a view.
#[tokio::test]
#[ignore]
async fn live_set_view_tags_applies_and_is_idempotent() {
    let Some((adapter, connector, catalog)) = parts_from_env() else {
        eprintln!("SKIP live_set_view_tags: Databricks env not set");
        return;
    };
    let schema = format!("hc_gov_view_tags_{}", suffix());
    let view = "hc_v_orders";
    let gov = DatabricksGovernanceAdapter::without_workspace(connector);

    // Setup: a base table + a view over it.
    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        .await
        .expect("create schema");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.hc_base AS \
             SELECT * FROM (VALUES (1), (2)) AS t(id)"
        ))
        .await
        .expect("create base table");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE VIEW {catalog}.{schema}.{view} AS \
             SELECT id FROM {catalog}.{schema}.hc_base"
        ))
        .await
        .expect("create view");

    let mut tags = BTreeMap::new();
    tags.insert("domain".to_string(), "finance".to_string());
    tags.insert("tier".to_string(), "gold".to_string());

    let target = TagTarget::View {
        catalog: catalog.clone(),
        schema: schema.clone(),
        view: view.to_string(),
    };

    // First apply: ALTER VIEW ... SET TAGS must succeed.
    gov.set_tags(&target, &tags)
        .await
        .expect("ALTER VIEW SET TAGS must succeed on a view");

    let after_first = tag_present(
        &adapter, &catalog, "view", &schema, view, "domain", "finance",
    )
    .await;
    assert_eq!(
        after_first, 1,
        "view tag domain=finance must be present after first apply"
    );

    // Second apply with one value changed: idempotent upsert — no error, and
    // the value is overwritten (still exactly one row for the key).
    tags.insert("tier".to_string(), "silver".to_string());
    gov.set_tags(&target, &tags)
        .await
        .expect("re-applying ALTER VIEW SET TAGS must be idempotent (upsert)");

    let tier_rows = tag_present(&adapter, &catalog, "view", &schema, view, "tier", "silver").await;
    assert_eq!(
        tier_rows, 1,
        "tier must upsert to exactly one row with the new value"
    );
    let stale_tier = tag_present(&adapter, &catalog, "view", &schema, view, "tier", "gold").await;
    assert_eq!(stale_tier, 0, "old tier value must not linger after upsert");

    // Cleanup.
    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
}

/// `ALTER TABLE ... SET TAGS` applies on a table (the non-view path
/// for the same governance entry point).
#[tokio::test]
#[ignore]
async fn live_set_table_tags_applies_on_table() {
    let Some((adapter, connector, catalog)) = parts_from_env() else {
        eprintln!("SKIP live_set_table_tags: Databricks env not set");
        return;
    };
    let schema = format!("hc_gov_table_tags_{}", suffix());
    let table = "hc_t_orders";
    let gov = DatabricksGovernanceAdapter::without_workspace(connector);

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        .await
        .expect("create schema");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS \
             SELECT * FROM (VALUES (1), (2)) AS t(id)"
        ))
        .await
        .expect("create table");

    let mut tags = BTreeMap::new();
    tags.insert("domain".to_string(), "finance".to_string());

    let target = TagTarget::Table {
        catalog: catalog.clone(),
        schema: schema.clone(),
        table: table.to_string(),
    };

    gov.set_tags(&target, &tags)
        .await
        .expect("ALTER TABLE SET TAGS must succeed on a table");

    let present = tag_present(
        &adapter, &catalog, "table", &schema, table, "domain", "finance",
    )
    .await;
    assert_eq!(
        present, 1,
        "table tag domain=finance must be present after apply"
    );

    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE"))
        .await;
}
