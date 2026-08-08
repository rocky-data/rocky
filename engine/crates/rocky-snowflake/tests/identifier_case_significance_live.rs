//! Live Snowflake test for `WarehouseAdapter::identifier_case_significance`
//! — the #1281 probe that reads `QUOTED_IDENTIFIERS_IGNORE_CASE` off the
//! session instead of assuming how the account treats identifier case.
//!
//! Rocky renders Snowflake targets DOUBLE-QUOTED (`dialect::format_table_ref`),
//! so this parameter is exactly what decides whether two of Rocky's targets
//! differing only by case are one object or two.
//!
//! **This test needs no warehouse compute.** `SHOW PARAMETERS` is metadata, so
//! it answers on an account whose trial has lapsed and whose DDL/compute is
//! refused with `000666 / 57014 "account is suspended"`. That is why this probe
//! could be live-verified when the rest of the Snowflake live suite could not.
//!
//! `#[ignore]`-gated; reads the same env vars as the other Snowflake live
//! tests:
//!   SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE,
//!   SNOWFLAKE_TEST_DATABASE (or SNOWFLAKE_DATABASE),
//!   SNOWFLAKE_PAT (or username + key-pair / OAuth), SNOWFLAKE_ROLE
//!
//! Run with:
//!   cargo test -p rocky-snowflake --test identifier_case_significance_live -- --ignored --nocapture

use std::time::Duration;

use rocky_core::config::RetryConfig;
use rocky_core::traits::{CaseSignificance, WarehouseAdapter};
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};

fn adapter_from_env() -> Option<SnowflakeWarehouseAdapter> {
    let account = std::env::var("SNOWFLAKE_ACCOUNT").ok()?;
    let warehouse = std::env::var("SNOWFLAKE_WAREHOUSE").ok()?;
    let database = std::env::var("SNOWFLAKE_TEST_DATABASE")
        .or_else(|_| std::env::var("SNOWFLAKE_DATABASE"))
        .ok()?;

    let auth = Auth::from_config(AuthConfig {
        account: account.clone(),
        username: std::env::var("SNOWFLAKE_USERNAME").ok(),
        // The sandbox `.env` mislabels its PAT as SNOWFLAKE_PASSWORD; it must
        // be supplied via SNOWFLAKE_PAT. Deliberately not read here so a
        // password-mode auth cannot be selected by mistake.
        password: None,
        oauth_token: std::env::var("SNOWFLAKE_OAUTH_TOKEN").ok(),
        private_key_path: std::env::var("SNOWFLAKE_PRIVATE_KEY_PATH").ok(),
        pat: std::env::var("SNOWFLAKE_PAT").ok(),
    })
    .ok()?;

    let config = ConnectorConfig {
        account,
        warehouse,
        database: Some(database),
        schema: None,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        timeout: Duration::from_secs(180),
        retry: RetryConfig::default(),
    };
    Some(SnowflakeWarehouseAdapter::new(SnowflakeConnector::new(
        config, auth,
    )))
}

/// #1281: the probe returns a DEFINITE answer against a real account.
///
/// The assertion is deliberately not "equals Significant". This account
/// reports `QUOTED_IDENTIFIERS_IGNORE_CASE = false`, but pinning that would
/// make the test assert the account's configuration rather than the probe's
/// behaviour — and it would start failing if the parameter were ever flipped,
/// which is the very thing the probe exists to notice. What must hold is that
/// a reachable account yields a definite answer rather than an error, and that
/// the answer is the one the parameter dictates.
#[tokio::test]
#[ignore = "requires live Snowflake credentials"]
async fn probe_reads_a_definite_answer_from_the_live_session() {
    let Some(adapter) = adapter_from_env() else {
        eprintln!("skipping: Snowflake env vars not set");
        return;
    };

    let significance = adapter
        .identifier_case_significance()
        .await
        .expect("a reachable account must yield a definite answer, not an error");

    // Cross-check against the raw parameter through a second, independent
    // read, so the test cannot pass by the probe inventing an answer.
    let raw = adapter
        .execute_query("SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE'")
        .await
        .expect("SHOW PARAMETERS is metadata and answers on a suspended account");
    let value_idx = raw
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case("value"))
        .expect("SHOW PARAMETERS has a `value` column");
    let raw_value = raw.rows[0][value_idx]
        .as_str()
        .expect("value renders as a string over the SQL API")
        .to_ascii_lowercase();

    let expected = if raw_value == "true" {
        CaseSignificance::Insignificant
    } else {
        CaseSignificance::Significant
    };
    assert_eq!(
        significance, expected,
        "probe disagreed with QUOTED_IDENTIFIERS_IGNORE_CASE={raw_value}"
    );

    eprintln!("live: QUOTED_IDENTIFIERS_IGNORE_CASE={raw_value} -> {significance:?}");
}
