//! Standalone cleanup verification: confirm no `hc_dialsweep` /
//! `HC_DIALSWEEP` schemas survived the Snowflake dialect-sweep probes.
//! Throwaway audit helper; not a regression test.

use std::time::Duration;

use rocky_core::config::RetryConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};

#[tokio::test]
#[ignore]
async fn cleanup_check_no_leftover_schemas() {
    let account = std::env::var("SNOWFLAKE_ACCOUNT").expect("SNOWFLAKE_ACCOUNT");
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").expect("SNOWFLAKE_WAREHOUSE");
    let database = std::env::var("SNOWFLAKE_TEST_DATABASE")
        .or_else(|_| std::env::var("SNOWFLAKE_DATABASE"))
        .expect("SNOWFLAKE_TEST_DATABASE");
    let auth = Auth::from_config(AuthConfig {
        account: account.clone(),
        username: std::env::var("SNOWFLAKE_USERNAME").ok(),
        password: None,
        oauth_token: std::env::var("SNOWFLAKE_OAUTH_TOKEN").ok(),
        private_key_path: std::env::var("SNOWFLAKE_PRIVATE_KEY_PATH").ok(),
        pat: std::env::var("SNOWFLAKE_PAT").ok(),
    })
    .unwrap();
    let adapter = SnowflakeWarehouseAdapter::new(SnowflakeConnector::new(
        ConnectorConfig {
            account,
            warehouse,
            database: Some(database.clone()),
            schema: None,
            role: std::env::var("SNOWFLAKE_ROLE").ok(),
            timeout: Duration::from_secs(120),
            retry: RetryConfig::default(),
        },
        auth,
    ));

    // Snowflake folds the unquoted LIKE pattern to upper; match both cases
    // with ILIKE so any survivor (HC_DIALSWEEP_* or quoted hc_dialsweep_*)
    // surfaces.
    let q = adapter
        .execute_query(&format!(
            "SHOW SCHEMAS LIKE '%DIALSWEEP%' IN DATABASE \"{database}\""
        ))
        .await
        .expect("show schemas");
    println!("Snowflake leftover *DIALSWEEP* schemas: {:?}", q.rows);
    assert!(
        q.rows.is_empty(),
        "leftover Snowflake schemas not cleaned up: {:?}",
        q.rows
    );
    println!("Snowflake: CLEAN — no *dialsweep* schemas remain in {database}");
}
