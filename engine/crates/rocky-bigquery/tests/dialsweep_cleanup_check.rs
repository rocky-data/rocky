//! Standalone cleanup verification: confirm no `hc_dialsweep` datasets
//! survived the BigQuery dialect-sweep probes. Throwaway audit helper.

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_core::traits::WarehouseAdapter;

#[tokio::test]
#[ignore]
async fn cleanup_check_no_leftover_datasets() {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT");
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    let auth = BigQueryAuth::from_env().expect("BigQueryAuth::from_env");
    let adapter = BigQueryAdapter::new(project.clone(), location.clone(), auth);

    let region = format!("region-{}", location.to_lowercase());
    let q = adapter
        .execute_query(&format!(
            "SELECT schema_name FROM `{project}`.`{region}`.INFORMATION_SCHEMA.SCHEMATA \
             WHERE STARTS_WITH(schema_name, 'hc_dialsweep')"
        ))
        .await
        .expect("query schemata");
    println!("BigQuery leftover hc_dialsweep datasets: {:?}", q.rows);
    assert!(
        q.rows.is_empty(),
        "leftover BigQuery datasets not cleaned up: {:?}",
        q.rows
    );
    println!("BigQuery: CLEAN — no hc_dialsweep datasets remain in {project}");
}
