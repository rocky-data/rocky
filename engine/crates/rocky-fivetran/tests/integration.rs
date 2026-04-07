//! Integration tests against real Fivetran API.
//!
//! Requires:
//! - FIVETRAN_API_KEY
//! - FIVETRAN_API_SECRET
//! - FIVETRAN_DESTINATION_ID
//! - FIVETRAN_SCHEMA_PREFIX (optional, defaults to "src__")
//!
//! Run with: cargo test -p rocky-fivetran --test integration -- --ignored

use rocky_fivetran::client::FivetranClient;
use rocky_fivetran::connector;
use rocky_fivetran::sync;

fn client_from_env() -> Option<FivetranClient> {
    let api_key = std::env::var("FIVETRAN_API_KEY").ok()?;
    let api_secret = std::env::var("FIVETRAN_API_SECRET").ok()?;
    Some(FivetranClient::new(api_key, api_secret))
}

fn schema_prefix() -> String {
    std::env::var("FIVETRAN_SCHEMA_PREFIX").unwrap_or_else(|_| "src__".to_string())
}

fn destination_id() -> Option<String> {
    std::env::var("FIVETRAN_DESTINATION_ID").ok()
}

#[tokio::test]
#[ignore]
async fn test_list_connectors() {
    let client = client_from_env().expect("Fivetran env vars not set");
    let dest_id = destination_id().expect("FIVETRAN_DESTINATION_ID not set");

    let connectors = connector::list_connectors(&client, &dest_id).await.unwrap();

    println!("Found {} connectors", connectors.len());
    assert!(!connectors.is_empty(), "expected at least one connector");

    for c in &connectors {
        println!(
            "  {} | {} | {} | setup={} sync={}",
            c.id, c.service, c.schema, c.status.setup_state, c.status.sync_state
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_discover_connectors_with_prefix() {
    let client = client_from_env().expect("Fivetran env vars not set");
    let dest_id = destination_id().expect("FIVETRAN_DESTINATION_ID not set");

    let prefix = schema_prefix();
    let connectors = connector::discover_connectors(&client, &dest_id, &prefix)
        .await
        .unwrap();

    println!(
        "Found {} connectors matching '{}' prefix",
        connectors.len(),
        prefix,
    );
    for c in &connectors {
        assert!(
            c.schema.starts_with(&prefix),
            "connector {} has schema '{}' which doesn't start with '{}'",
            c.id,
            c.schema,
            prefix,
        );
        assert!(c.is_connected());
    }
}

#[tokio::test]
#[ignore]
async fn test_get_single_connector() {
    let client = client_from_env().expect("Fivetran env vars not set");
    let dest_id = destination_id().expect("FIVETRAN_DESTINATION_ID not set");

    // Get the first connector
    let connectors = connector::list_connectors(&client, &dest_id).await.unwrap();
    let first = connectors.first().expect("no connectors found");

    let detail = connector::get_connector(&client, &first.id).await.unwrap();
    assert_eq!(detail.id, first.id);
    assert_eq!(detail.service, first.service);
}

#[tokio::test]
#[ignore]
async fn test_get_schema_config() {
    let client = client_from_env().expect("Fivetran env vars not set");
    let dest_id = destination_id().expect("FIVETRAN_DESTINATION_ID not set");

    let prefix = schema_prefix();
    let connectors = connector::discover_connectors(&client, &dest_id, &prefix)
        .await
        .unwrap();
    let first = connectors
        .first()
        .expect("no connectors found matching prefix");

    let schema_config = rocky_fivetran::schema::get_schema_config(&client, &first.id)
        .await
        .unwrap();

    let tables = schema_config.enabled_tables();
    println!(
        "Connector {} ({}) has {} enabled tables",
        first.id,
        first.schema,
        tables.len()
    );
    for t in &tables {
        println!(
            "  {}.{} ({} columns)",
            t.schema_name, t.table_name, t.column_count
        );
    }
    assert!(!tables.is_empty(), "expected at least one enabled table");
}

#[tokio::test]
#[ignore]
async fn test_sync_detection() {
    let client = client_from_env().expect("Fivetran env vars not set");
    let dest_id = destination_id().expect("FIVETRAN_DESTINATION_ID not set");

    let connectors = connector::list_connectors(&client, &dest_id).await.unwrap();

    // No cursor = first run, all synced connectors should be "new"
    let statuses = sync::detect_synced_connectors(&connectors, None);
    let synced_count = statuses.iter().filter(|s| s.has_synced).count();
    let new_count = statuses.iter().filter(|s| s.is_new_since).count();

    println!(
        "{} connectors, {} have synced, {} new since (no cursor)",
        connectors.len(),
        synced_count,
        new_count
    );
    assert_eq!(synced_count, new_count);
}
