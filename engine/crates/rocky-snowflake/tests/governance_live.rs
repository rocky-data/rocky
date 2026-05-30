//! Live Snowflake governance tests: the grant lifecycle (apply → show → revoke).
//!
//! `#[ignore]`-gated; needs a real Snowflake account (same `SNOWFLAKE_*` env
//! vars as the other live tests). The connected role must be able to create a
//! schema in `SNOWFLAKE_TEST_DATABASE` and grant privileges on it.
//!
//! **What this pins.** `SnowflakeGovernanceAdapter` generates `GRANT` /
//! `SHOW GRANTS` / `REVOKE` SQL that Snowflake actually accepts, and the
//! `SHOW GRANTS` parser round-trips an applied grant. A wiremock test can't
//! validate the SQL against Snowflake's privilege model — that `USAGE` is a
//! schema-valid privilege, that the role name must be double-quoted, or that
//! the `SHOW GRANTS` column layout matches what `get_grants` indexes into.
//!
//! Schemas use the `hc_gov_*` prefix per Hugo's Snowflake convention; the
//! per-run microsecond suffix isolates parallel runs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::config::RetryConfig;
use rocky_core::traits::GovernanceAdapter;
use rocky_ir::{Grant, GrantTarget, Permission};
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};
use rocky_snowflake::governance::SnowflakeGovernanceAdapter;

fn connector_from_env() -> Option<(SnowflakeConnector, String)> {
    let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok()?;
    let database = std::env::var("SNOWFLAKE_TEST_DATABASE")
        .or_else(|_| std::env::var("SNOWFLAKE_DATABASE"))
        .ok()?;

    let auth = Auth::from_config(AuthConfig {
        account: account.clone(),
        username: std::env::var("SNOWFLAKE_USERNAME").ok(),
        password: std::env::var("SNOWFLAKE_PASSWORD").ok(),
        oauth_token: std::env::var("SNOWFLAKE_OAUTH_TOKEN").ok(),
        private_key_path: std::env::var("SNOWFLAKE_PRIVATE_KEY_PATH").ok(),
        pat: std::env::var("SNOWFLAKE_PAT").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        account,
        warehouse,
        database: Some(database.clone()),
        schema: None,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        timeout: Duration::from_secs(180),
        retry: RetryConfig::default(),
    };
    Some((SnowflakeConnector::new(config, auth), database))
}

fn schema_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

/// Whether a grant list holds a USAGE-class grant to the PUBLIC role.
///
/// Snowflake collapses both `UseCatalog` and `UseSchema` to the `USAGE`
/// privilege, and the `SHOW GRANTS` parser maps `"USAGE"` back to
/// `Permission::UseCatalog`, so an applied `UseSchema` reads back as
/// `UseCatalog`. Accept either to stay robust to that documented collapse.
fn has_public_usage(grants: &[Grant]) -> bool {
    grants.iter().any(|g| {
        g.principal.eq_ignore_ascii_case("PUBLIC")
            && matches!(g.permission, Permission::UseCatalog | Permission::UseSchema)
    })
}

/// `apply_grants` → `get_grants` → `revoke_grants` round-trips against live
/// Snowflake.
///
/// Grants `USAGE` on a freshly created schema to the built-in PUBLIC role,
/// confirms `SHOW GRANTS` surfaces it, then revokes and confirms it is gone.
/// Filtering on the PUBLIC grantee keeps the assertion independent of the
/// owner-role grants `SHOW GRANTS` also returns.
#[tokio::test]
#[ignore]
async fn live_grant_apply_show_revoke_roundtrip() {
    let Some((connector, database)) = connector_from_env() else {
        eprintln!("Snowflake env vars not set; skipping");
        return;
    };
    let suffix = schema_suffix();
    let schema = format!("hc_gov_{suffix}");

    // Create the schema UNQUOTED, matching the dialect's `create_schema_sql`
    // (and `format_grant_target`/`format_show_grants_sql`) — the governance
    // adapter interpolates schema identifiers unquoted, relying on Snowflake's
    // uppercase folding. A quoted (case-sensitive lowercase) schema here would
    // not be found by the unquoted GRANT/SHOW GRANTS that follow.
    connector
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {database}.{schema}"))
        .await
        .expect("create schema");

    let gov = SnowflakeGovernanceAdapter::from_ref(&connector);
    let target = GrantTarget::Schema {
        catalog: database.clone(),
        schema: schema.clone(),
    };
    let grant = Grant {
        principal: "PUBLIC".to_string(),
        permission: Permission::UseSchema,
        target: target.clone(),
    };

    let applied = gov.apply_grants(std::slice::from_ref(&grant)).await;
    let after_apply = gov.get_grants(&target).await;
    let revoked = gov.revoke_grants(std::slice::from_ref(&grant)).await;
    let after_revoke = gov.get_grants(&target).await;

    // Drop before asserting so a failed assertion leaves no orphan schema.
    let _ = connector
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS {database}.{schema} CASCADE"
        ))
        .await;

    applied.expect("apply_grants should be accepted by Snowflake");
    revoked.expect("revoke_grants should be accepted by Snowflake");

    let after_apply = after_apply.expect("get_grants after apply");
    assert!(
        has_public_usage(&after_apply),
        "USAGE grant to PUBLIC must appear in SHOW GRANTS after apply; got {after_apply:?}"
    );

    let after_revoke = after_revoke.expect("get_grants after revoke");
    assert!(
        !has_public_usage(&after_revoke),
        "USAGE grant to PUBLIC must be gone after revoke; got {after_revoke:?}"
    );
}
