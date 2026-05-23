//! Integration tests against a real BigQuery environment.
//!
//! These tests require:
//! - `BIGQUERY_TEST_PROJECT` (the GCP project ID under which to create
//!   the ephemeral test datasets)
//! - one of:
//!   - `GOOGLE_APPLICATION_CREDENTIALS` (path to a service-account JSON
//!     file with `bigquery.dataEditor` + `bigquery.jobUser` on the
//!     project), or
//!   - `BIGQUERY_TOKEN` (a pre-exchanged OAuth bearer token; useful for
//!     `gcloud auth print-access-token` flows in CI).
//!
//! Optional:
//! - `BIGQUERY_TEST_LOCATION` — the dataset location (defaults to
//!   `"EU"` to match the typical free-trial quota).
//!
//! Run with: `cargo test -p rocky-bigquery --test integration -- --ignored`

use std::time::{SystemTime, UNIX_EPOCH};

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::TableRef;

/// Build an adapter from env vars. Returns `None` when the required
/// vars aren't present, so the `#[ignore]`d test silently skips on a
/// developer machine without GCP creds plumbed in.
fn adapter_from_env() -> Option<BigQueryAdapter> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    // `BigQueryAuth::from_env()` honours `BIGQUERY_TOKEN` first, then
    // falls back to `GOOGLE_APPLICATION_CREDENTIALS`.
    let auth = BigQueryAuth::from_env().ok()?;
    Some(BigQueryAdapter::new(project, location, auth))
}

/// Verifies the BigQuery `clone_table_for_branch` override emits a
/// working `CREATE OR REPLACE TABLE ... COPY` statement: source dataset
/// + table created, clone produced in a sibling dataset, row contents
/// match. Both datasets are dropped (CASCADE-equivalent
/// `DROP SCHEMA ... CASCADE`) at the end regardless of test outcome.
///
/// Project is read from `BIGQUERY_TEST_PROJECT`. Datasets use the
/// `hc_phase5_` prefix to match Hugo's Databricks-side convention so
/// sandbox housekeeping uses one regex across warehouses.
#[tokio::test]
#[ignore]
async fn test_clone_table_for_branch_copy() {
    let adapter = adapter_from_env().expect("BigQuery env vars not set");
    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let src_dataset = format!("hc_phase5_src_{suffix}");
    let brn_dataset = format!("hc_phase5_brn_{suffix}");
    let table = "test_table";

    // Setup: create the two datasets + a 2-row source table. BigQuery
    // calls them "datasets" rather than "schemas", but the
    // `CREATE SCHEMA` DDL works either way (it's an alias).
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{src_dataset}`"
        ))
        .await
        .expect("create source dataset");
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{brn_dataset}`"
        ))
        .await
        .expect("create branch dataset");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{project}`.`{src_dataset}`.`{table}` AS \
             SELECT * FROM UNNEST([STRUCT(1 AS id, 'a' AS name), STRUCT(2, 'b')])"
        ))
        .await
        .expect("create source table");

    // Run the unit under test, capture the result so cleanup runs first.
    let source = TableRef {
        catalog: project.clone(),
        schema: src_dataset.clone(),
        table: table.to_string(),
    };
    let clone_result = adapter.clone_table_for_branch(&source, &brn_dataset).await;

    let row_count = if clone_result.is_ok() {
        let q = adapter
            .execute_query(&format!(
                "SELECT COUNT(*) AS n FROM `{project}`.`{brn_dataset}`.`{table}`"
            ))
            .await
            .ok();
        q.and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
    } else {
        None
    };

    // Unconditional cleanup (best-effort; ignored on failure).
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{src_dataset}` CASCADE"
        ))
        .await;
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{brn_dataset}` CASCADE"
        ))
        .await;

    clone_result.expect("clone_table_for_branch should succeed");
    let n = row_count.expect("clone target should be queryable");
    // BigQuery returns COUNT(*) results as JSON strings (`{"v": "2"}`)
    // rather than typed numbers, so handle both shapes defensively.
    let n_num: i64 = n
        .as_i64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(-1);
    assert_eq!(n_num, 2, "cloned table should have 2 rows, got {n:?}");
}

/// Verifies `BigQueryDiscoveryAdapter::discover` lists matching
/// datasets + their tables against the live sandbox. Creates two
/// datasets with the `hc_phase14_disc_<ts>_` prefix, seeds one table
/// in each, runs discover, and asserts both datasets and their tables
/// are returned. Datasets are dropped on test exit (best-effort).
#[tokio::test]
#[ignore]
async fn test_discover_lists_datasets_and_tables() {
    use rocky_bigquery::BigQueryDiscoveryAdapter;
    use rocky_core::traits::DiscoveryAdapter as _;
    use std::sync::Arc;

    let warehouse = adapter_from_env().expect("BigQuery env vars not set");
    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let prefix = format!("hc_phase14_disc_{suffix}_");
    let alpha = format!("{prefix}alpha");
    let beta = format!("{prefix}beta");

    // Setup. Two datasets, one table each.
    warehouse
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{alpha}`"))
        .await
        .expect("create alpha");
    warehouse
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{beta}`"))
        .await
        .expect("create beta");
    warehouse
        .execute_statement(&format!(
            "CREATE TABLE `{project}`.`{alpha}`.`orders` AS SELECT 1 AS id"
        ))
        .await
        .expect("seed alpha.orders");
    warehouse
        .execute_statement(&format!(
            "CREATE TABLE `{project}`.`{beta}`.`customers` AS SELECT 1 AS id"
        ))
        .await
        .expect("seed beta.customers");

    let discovery = BigQueryDiscoveryAdapter::new(Arc::new(warehouse));
    let result = discovery.discover(&prefix).await;

    // Re-construct the warehouse adapter for cleanup; we moved the
    // original into the discovery adapter via Arc.
    let cleanup_adapter = adapter_from_env().expect("BigQuery env vars not set");
    let _ = cleanup_adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{alpha}` CASCADE"
        ))
        .await;
    let _ = cleanup_adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{beta}` CASCADE"
        ))
        .await;

    let result = result.expect("discover should succeed");
    assert_eq!(
        result.connectors.len(),
        2,
        "expected 2 datasets, got {:?}",
        result
            .connectors
            .iter()
            .map(|c| &c.schema)
            .collect::<Vec<_>>()
    );
    assert!(result.failed.is_empty());

    let alpha_conn = result
        .connectors
        .iter()
        .find(|c| c.schema == alpha)
        .expect("alpha dataset present");
    assert_eq!(alpha_conn.source_type, "bigquery");
    assert_eq!(alpha_conn.tables.len(), 1);
    assert_eq!(alpha_conn.tables[0].name, "orders");

    let beta_conn = result
        .connectors
        .iter()
        .find(|c| c.schema == beta)
        .expect("beta dataset present");
    assert_eq!(beta_conn.tables.len(), 1);
    assert_eq!(beta_conn.tables[0].name, "customers");
}

/// Verifies `execute_statement_with_stats` enriches the response with
/// `statistics.query.totalBytesBilled` via a follow-up `jobs.get` call.
/// Without the enrichment, only the bare `totalBytesProcessed` from
/// the synchronous `jobs.query` response would surface — which doesn't
/// apply BigQuery's 10 MB minimum-bill floor.
///
/// Uses `INFORMATION_SCHEMA.SCHEMATA` so the query reads real data
/// (constant queries like `SELECT 1` are exempt from the bill floor).
/// If the enrichment regresses, `bytes_scanned` falls back to
/// `totalBytesProcessed` — which is small for a metadata view scan
/// and well below the 10 MB floor.
#[tokio::test]
#[ignore]
async fn test_execute_statement_with_stats_reports_billed_bytes() {
    use rocky_core::traits::WarehouseAdapter;

    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());

    let adapter = adapter_from_env().expect("BigQuery env vars not set");
    let region = format!("region-{}", location.to_lowercase());
    let sql = format!("SELECT COUNT(*) AS n FROM `{project}.{region}.INFORMATION_SCHEMA.SCHEMATA`");

    let stats = adapter
        .execute_statement_with_stats(&sql)
        .await
        .expect("execute_statement_with_stats");

    let bytes = stats
        .bytes_scanned
        .expect("bytes_scanned should be populated via jobs.get enrichment");
    const MIN_BILL: u64 = 10 * 1024 * 1024;
    assert!(
        bytes >= MIN_BILL,
        "bytes_scanned ({bytes}) below 10 MB minimum-bill floor — \
         likely fell back to totalBytesProcessed from jobs.query"
    );
}

/// Cross-checks `ExecutionStats.bytes_scanned` against an independently
/// fetched `statistics.query.totalBytesBilled` from a fresh `jobs.get`
/// REST call.
///
/// Rationale: `test_execute_statement_with_stats_reports_billed_bytes`
/// confirms the figure clears the 10 MB floor, but it can't catch a
/// regression where the adapter mis-parses `totalBytesBilled` to a value
/// that's still > 10 MB but wrong by a factor of (say) 8. This test
/// independently re-fetches the job via a vanilla `reqwest::get` against
/// the BigQuery `jobs.get` endpoint and asserts the numbers agree within
/// ±1% — exactly the cross-check operators run as `rocky run --output
/// json | jq '.materializations[].job_ids[]'` piped into `bq show -j`.
///
/// The independent fetch deliberately bypasses
/// `BigQueryAdapter::fetch_job_statistics`: if both sides went through
/// the same code path, the test would be circular.
///
/// `bytes_scanned` is captured against `ExecutionStats::job_id` from the
/// same call — this is the field surfaced into
/// `MaterializationOutput.job_ids` in `rocky run --output json`.
#[tokio::test]
#[ignore]
async fn test_bytes_scanned_matches_independent_jobs_get_total_bytes_billed() {
    use rocky_bigquery::auth::BigQueryAuth;
    use rocky_core::traits::WarehouseAdapter;
    use serde::Deserialize;

    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());

    let adapter = adapter_from_env().expect("BigQuery env vars not set");
    // `INFORMATION_SCHEMA.SCHEMATA` scans real metadata — keeps the
    // billed figure well above the 10 MB floor so the ±1% tolerance
    // isn't dominated by floor rounding.
    let region = format!("region-{}", location.to_lowercase());
    let sql = format!("SELECT COUNT(*) AS n FROM `{project}.{region}.INFORMATION_SCHEMA.SCHEMATA`");

    let stats = adapter
        .execute_statement_with_stats(&sql)
        .await
        .expect("execute_statement_with_stats");

    let rocky_bytes = stats
        .bytes_scanned
        .expect("bytes_scanned should be populated");
    let job_id = stats.job_id.expect("job_id should be populated");

    // Independent `jobs.get` REST call — fresh client + fresh auth so
    // we're not exercising the same path that produced `stats`.
    let auth = BigQueryAuth::from_env().expect("BigQueryAuth::from_env");
    let client = reqwest::Client::new();
    let token = auth.get_token(&client).await.expect("acquire bearer token");
    let url =
        format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/{job_id}");
    let resp = client
        .get(&url)
        .bearer_auth(&token)
        .query(&[("location", location.as_str())])
        .send()
        .await
        .expect("jobs.get send");
    assert!(
        resp.status().is_success(),
        "jobs.get returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // Minimal local response shape — keeps the deserializer tolerant of
    // fields we don't read (camelCase per BigQuery's REST API).
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct JobGet {
        statistics: Stats,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stats {
        query: Query,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Query {
        total_bytes_billed: String,
    }

    let job: JobGet = resp.json().await.expect("parse jobs.get JSON");
    let bq_bytes: u64 = job
        .statistics
        .query
        .total_bytes_billed
        .parse()
        .expect("totalBytesBilled parses as u64");

    // ±1% tolerance: in practice these should match exactly because
    // `totalBytesBilled` is an integer the adapter passes through
    // unchanged. The slack covers any future rounding pass added on
    // either side.
    let tolerance = (bq_bytes / 100).max(1);
    let diff = rocky_bytes.abs_diff(bq_bytes);
    assert!(
        diff <= tolerance,
        "bytes_scanned cross-check failed: rocky={rocky_bytes} bq_jobs_get={bq_bytes} \
         diff={diff} tolerance={tolerance} (±1%) job_id={job_id} location={location}",
    );

    // Receipt the test prints when run with `-- --nocapture`. Captured
    // by the operator and pasted into the PR description as the live
    // cross-check evidence.
    println!(
        "bq-crosscheck OK | job_id={job_id} location={location} \
         rocky.bytes_scanned={rocky_bytes} bq.totalBytesBilled={bq_bytes} \
         diff={diff} tolerance={tolerance}"
    );
}

/// Pipeline-level cost attribution cross-check (Phase 2.2 of the BQ
/// trial-window plan).
///
/// PR #617 cross-checked `bytes_scanned` against `totalBytesBilled` for a
/// *single* statement. This test extends that to the pipeline shape: when
/// `rocky run` materializes N models, the sum of per-model `cost_usd`
/// across all materializations should equal what BigQuery billed for the
/// pipeline's jobs, within ±1%. Confirms the per-model attribution
/// doesn't double-count or drop sub-jobs when aggregated.
///
/// The test stays inside `rocky-bigquery` so it can't dev-depend on
/// `rocky-cli` (cyclic) — and therefore can't pull in
/// `RunOutput::populate_cost_summary` directly. Instead it mirrors the
/// arithmetic that function performs: `compute_observed_cost_usd` per
/// statement (the exact call the run finalizer makes) summed across the
/// pipeline. The function lives in `rocky-core::cost`, which both this
/// test and the CLI's run finalizer share, so the two paths use the
/// same per-model formula.
///
/// Three distinct `INFORMATION_SCHEMA` views are queried (`SCHEMATA`,
/// `TABLES`, `COLUMNS`). In practice all three bill at BigQuery's 10 MB
/// minimum floor, so the sum is `3 × 10 MB`. That's still enough to
/// surface the regression this test is built for: dropping one of the
/// three jobs would put rocky's sum at `2 × 10 MB` against BQ's
/// `3 × 10 MB`, a 33% divergence that the ±1% assertion catches loudly.
/// Per-job and sum assertions both run so the two failure modes
/// (per-job mis-reporting vs missed an entire job) remain
/// distinguishable in test output.
///
/// **Scope boundary.** Because the test doesn't subprocess-drive
/// `rocky run`, it doesn't catch a regression where the adapter issues
/// N statements for one model but only surfaces M < N job IDs into
/// `MaterializationOutput.job_ids` — that's a `commands/run.rs`
/// statement-collection wiring concern, separable from the
/// bytes × cost arithmetic this test owns.
#[tokio::test]
#[ignore]
async fn test_pipeline_level_cost_attribution_matches_jobs_get() {
    use rocky_bigquery::auth::BigQueryAuth;
    use rocky_core::cost::{BIGQUERY_USD_PER_TB_SCANNED, WarehouseType, compute_observed_cost_usd};
    use rocky_core::traits::WarehouseAdapter;
    use serde::Deserialize;

    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());

    let adapter = adapter_from_env().expect("BigQuery env vars not set");

    // Three distinct INFORMATION_SCHEMA views. All three typically
    // bill at BigQuery's 10 MB minimum floor for metadata scans of
    // this size, but the queries themselves are distinct jobs with
    // distinct job_ids — so missing a job in collection still
    // surfaces as a ~33% sum divergence (one job dropped from
    // 3 × 10 MB), well outside the ±1% tolerance.
    let region = format!("region-{}", location.to_lowercase());
    let statements = [
        format!("SELECT COUNT(*) AS n FROM `{project}.{region}.INFORMATION_SCHEMA.SCHEMATA`"),
        format!("SELECT COUNT(*) AS n FROM `{project}.{region}.INFORMATION_SCHEMA.TABLES`"),
        format!("SELECT COUNT(*) AS n FROM `{project}.{region}.INFORMATION_SCHEMA.COLUMNS`"),
    ];
    assert!(
        statements.len() >= 3,
        "Phase 2.2 cross-check requires >= 3 statements to exercise the aggregation"
    );

    // 1) Drive each statement through the adapter and collect what the
    // run finalizer would feed into `compute_observed_cost_usd`.
    let mut rocky_jobs: Vec<(String, u64)> = Vec::with_capacity(statements.len());
    let mut rocky_total_cost_usd = 0.0_f64;
    for (idx, sql) in statements.iter().enumerate() {
        let stats = adapter
            .execute_statement_with_stats(sql)
            .await
            .unwrap_or_else(|e| panic!("execute_statement_with_stats #{idx} failed: {e}"));
        let bytes = stats
            .bytes_scanned
            .unwrap_or_else(|| panic!("bytes_scanned missing for statement #{idx}"));
        let job_id = stats
            .job_id
            .unwrap_or_else(|| panic!("job_id missing for statement #{idx}"));

        // Mirror the per-model `populate_cost_summary` step: bytes ×
        // per-TB rate, summed across materializations. `duration_ms`
        // and DBU args are ignored for BigQuery — the formula is
        // bytes-only — but the call signature is shared with the
        // duration-based adapters, so the zeros document that this
        // path doesn't read them.
        let cost = compute_observed_cost_usd(WarehouseType::BigQuery, Some(bytes), 0, 0.0, 0.0)
            .unwrap_or_else(|| {
                panic!("compute_observed_cost_usd returned None for BigQuery bytes")
            });

        rocky_total_cost_usd += cost;
        rocky_jobs.push((job_id, bytes));
    }

    // 2) Independent `jobs.get` per job_id — fresh client + fresh auth
    // so this side bypasses any caching or reuse in the adapter path.
    let auth = BigQueryAuth::from_env().expect("BigQueryAuth::from_env");
    let client = reqwest::Client::new();
    let token = auth.get_token(&client).await.expect("acquire bearer token");

    // Minimal local response shape — keeps the deserializer tolerant of
    // fields we don't read (camelCase per BigQuery's REST API).
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct JobGet {
        statistics: Stats,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stats {
        query: Query,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Query {
        total_bytes_billed: String,
    }

    let mut bq_total_bytes_billed: u64 = 0;
    let mut per_job_receipts: Vec<String> = Vec::with_capacity(rocky_jobs.len());
    for (job_id, rocky_bytes) in &rocky_jobs {
        let url =
            format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project}/jobs/{job_id}");
        let resp = client
            .get(&url)
            .bearer_auth(&token)
            .query(&[("location", location.as_str())])
            .send()
            .await
            .unwrap_or_else(|e| panic!("jobs.get send failed for {job_id}: {e}"));
        assert!(
            resp.status().is_success(),
            "jobs.get returned {} for {job_id}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );

        let job: JobGet = resp
            .json()
            .await
            .unwrap_or_else(|e| panic!("parse jobs.get JSON for {job_id}: {e}"));
        let bq_bytes: u64 = job
            .statistics
            .query
            .total_bytes_billed
            .parse()
            .unwrap_or_else(|e| panic!("totalBytesBilled parses as u64 for {job_id}: {e}"));

        // Per-job ±1% check first: surfaces "matched the wrong job"
        // vs "missed a job" as distinct failure modes when the totals
        // also diverge.
        let per_job_tolerance = (bq_bytes / 100).max(1);
        let per_job_diff = rocky_bytes.abs_diff(bq_bytes);
        assert!(
            per_job_diff <= per_job_tolerance,
            "per-job bytes mismatch for {job_id}: \
             rocky={rocky_bytes} bq.totalBytesBilled={bq_bytes} \
             diff={per_job_diff} tolerance={per_job_tolerance} (±1%)"
        );

        bq_total_bytes_billed = bq_total_bytes_billed.saturating_add(bq_bytes);
        per_job_receipts.push(format!(
            "  {job_id} rocky={rocky_bytes} bq={bq_bytes} diff={per_job_diff}"
        ));
    }

    // 3) Sum side: use the same `compute_observed_cost_usd` formula so
    // the comparison is apples-to-apples — both rocky_total_cost_usd
    // and bq_total_cost_usd run through the same per-TB constant.
    let bq_total_cost_usd = (bq_total_bytes_billed as f64 / 1.0e12) * BIGQUERY_USD_PER_TB_SCANNED;

    // 4) Pipeline-level ±1% assertion. Halt criterion in the plan
    // (Phase 2.2) is "> 5% divergence = real adapter bug, write a
    // memo." The hard assertion stays at ±1%; a separate
    // halt-criterion print fires when the divergence is in the
    // "real-bug" band so the operator catches it even if the panic
    // message scrolls past.
    let rel_diff = if bq_total_cost_usd == 0.0 {
        0.0
    } else {
        ((rocky_total_cost_usd - bq_total_cost_usd) / bq_total_cost_usd).abs()
    };
    if rel_diff > 0.05 {
        eprintln!(
            "HALT-CRITERION TRIPPED: pipeline cost divergence {:.2}% > 5%. \
             Per the BQ trial-window plan (Phase 2.2), this is a real adapter \
             bug — capture a memo and surface separately. \
             rocky=${rocky_total_cost_usd:.6} bq=${bq_total_cost_usd:.6} \
             rocky_bytes_sum={} bq_bytes_sum={bq_total_bytes_billed}",
            rel_diff * 100.0,
            rocky_jobs.iter().map(|(_, b)| *b).sum::<u64>(),
        );
    }
    assert!(
        rel_diff <= 0.01,
        "pipeline cost cross-check failed: \
         rocky=${rocky_total_cost_usd:.6} bq=${bq_total_cost_usd:.6} \
         relative_diff={:.4}% (±1% required) \
         rocky_bytes_sum={} bq_bytes_sum={bq_total_bytes_billed} \
         jobs={}",
        rel_diff * 100.0,
        rocky_jobs.iter().map(|(_, b)| *b).sum::<u64>(),
        rocky_jobs
            .iter()
            .map(|(j, _)| j.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );

    // Receipt block. Operator captures with `-- --nocapture` and pastes
    // into the PR description as the live cross-check evidence.
    println!(
        "bq-pipeline-crosscheck OK | location={location} jobs={}\n\
         per-job:\n{}\n\
         totals: rocky=${rocky_total_cost_usd:.6} bq=${bq_total_cost_usd:.6} \
         rel_diff={:.4}% (±1% required)",
        rocky_jobs.len(),
        per_job_receipts.join("\n"),
        rel_diff * 100.0,
    );
}
