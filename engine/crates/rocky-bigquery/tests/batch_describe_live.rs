//! Live BigQuery conformance test for the batched schema-describe path.
//!
//! `#[ignore]`-gated because it requires a real GCP project + BigQuery
//! credentials. Run locally with:
//!
//! ```bash
//! BIGQUERY_TEST_PROJECT=<your-project> \
//! BIGQUERY_TEST_LOCATION=EU \
//! GOOGLE_APPLICATION_CREDENTIALS=<path-to-sa-json> \
//! cargo test -p rocky-bigquery --test batch_describe_live -- --ignored --nocapture
//! ```
//!
//! What this test pins (closes the `feedback_sql_dialect_live_execute_tests`
//! gap for the per-dataset `INFORMATION_SCHEMA.COLUMNS` batch query):
//!
//! 1. `BigQueryBatchCheckAdapter::batch_describe_schema` returns one entry
//!    per table in the dataset in a single round trip (BigQuery scopes
//!    `INFORMATION_SCHEMA.COLUMNS` to a single dataset), keyed by lowercase
//!    table name.
//! 2. The batched column set matches the per-table
//!    `WarehouseAdapter::describe_table` path for the same table — same
//!    column names (compared case-insensitively, since the batch path
//!    lowercases names via `LOWER(column_name)` while the per-table path
//!    does not), same `data_type`, same nullability. Both paths read
//!    `INFORMATION_SCHEMA.COLUMNS`, so `data_type` strings *are* expected
//!    to match here.
//!
//! Without this test, the inline SQL-shape unit tests in `batch.rs` would
//! pass while a result-shape or region-qualification regression against the
//! real warehouse shipped silently — the class of bug stub tests have
//! repeatedly hidden on the BigQuery adapter.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::batch::BigQueryBatchCheckAdapter;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_core::traits::{BatchCheckAdapter, WarehouseAdapter};
use rocky_ir::TableRef;

/// Build a shared adapter from env, or `None` when creds aren't plumbed in
/// so the `#[ignore]`d test skips silently on a dev box.
fn adapter_from_env() -> Option<(std::sync::Arc<BigQueryAdapter>, String)> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    let auth = BigQueryAuth::from_env().ok()?;
    Some((
        std::sync::Arc::new(BigQueryAdapter::new(project.clone(), location, auth)),
        project,
    ))
}

fn dataset_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn exec(adapter: &BigQueryAdapter, sql: &str) {
    adapter
        .execute_statement(sql)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {sql}\nerror: {e:?}"));
}

#[tokio::test]
#[ignore = "requires live BigQuery project"]
async fn live_batch_describe_matches_per_table_describe() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("BIGQUERY_* env vars unset; skipping");
        return;
    };

    let dataset = format!("hc_batch_describe_{}", dataset_suffix());

    exec(
        &adapter,
        &format!("CREATE SCHEMA IF NOT EXISTS `{project}`.`{dataset}`"),
    )
    .await;

    // Two tables with mixed types + nullability so the batched query has to
    // group multiple tables and parse `is_nullable` (YES/NO) correctly.
    exec(
        &adapter,
        &format!(
            "CREATE TABLE `{project}`.`{dataset}`.`orders` (\
                id INT64 NOT NULL, \
                customer STRING, \
                amount FLOAT64\
            )"
        ),
    )
    .await;
    exec(
        &adapter,
        &format!(
            "CREATE TABLE `{project}`.`{dataset}`.`events` (\
                event_id INT64, \
                payload JSON, \
                created_at TIMESTAMP NOT NULL\
            )"
        ),
    )
    .await;

    // Run the unit under test, then compare against per-table describe.
    let batch = BigQueryBatchCheckAdapter::new(adapter.clone());
    let result = batch.batch_describe_schema(&project, &dataset).await;

    let cleanup = format!("DROP SCHEMA IF EXISTS `{project}`.`{dataset}` CASCADE");

    let batched = match result {
        Ok(map) => map,
        Err(e) => {
            exec(&adapter, &cleanup).await;
            panic!("batch_describe_schema failed: {e:?}");
        }
    };

    let mut mismatch: Option<String> = None;
    for table in ["orders", "events"] {
        let per_table = adapter
            .describe_table(&TableRef {
                catalog: project.clone(),
                schema: dataset.clone(),
                table: table.to_string(),
            })
            .await;

        let per_table = match per_table {
            Ok(c) => c,
            Err(e) => {
                mismatch = Some(format!("per-table describe of {table} failed: {e:?}"));
                break;
            }
        };

        let Some(batch_cols) = batched.get(table) else {
            mismatch = Some(format!(
                "batched result missing table {table:?}; keys present: {:?}",
                batched.keys().collect::<Vec<_>>()
            ));
            break;
        };

        // The batch path lowercases names; the per-table path does not — so
        // compare names case-insensitively.
        let batch_names: BTreeSet<String> =
            batch_cols.iter().map(|c| c.name.to_lowercase()).collect();
        let per_table_names: BTreeSet<String> =
            per_table.iter().map(|c| c.name.to_lowercase()).collect();
        if batch_names != per_table_names {
            mismatch = Some(format!(
                "{table}: column-name set mismatch\n  batch     = {batch_names:?}\n  per-table = {per_table_names:?}"
            ));
            break;
        }

        // Both paths read INFORMATION_SCHEMA.COLUMNS, so data_type + nullable
        // must agree column-for-column.
        for col in &per_table {
            let b = batch_cols
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&col.name))
                .expect("name sets already proven equal");
            if b.data_type != col.data_type {
                mismatch = Some(format!(
                    "{table}.{}: data_type mismatch — batch={:?} per-table={:?}",
                    col.name, b.data_type, col.data_type
                ));
                break;
            }
            if b.nullable != col.nullable {
                mismatch = Some(format!(
                    "{table}.{}: nullability mismatch — batch={} per-table={}",
                    col.name, b.nullable, col.nullable
                ));
                break;
            }
        }
        if mismatch.is_some() {
            break;
        }
        eprintln!(
            "OK: {table} — {} columns match per-table describe (names + data_type + nullability)",
            batch_cols.len()
        );
    }

    exec(&adapter, &cleanup).await;

    if let Some(msg) = mismatch {
        panic!("{msg}");
    }
}
