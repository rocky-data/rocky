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

/// Verifies the Arrow Storage Read path on `WarehouseAdapter::fetch_arrow_batch`.
///
/// Mirrors the rocky-duckdb conformance test (PR #631): runs
/// `SELECT 1 AS n, 'foo' AS s`, asserts a 1-row / 2-column workspace
/// `arrow 58` `RecordBatch` with column types `Int64` (BigQuery's
/// canonical signed integer width) and `Utf8`. The two-hop flow under
/// test:
///   1. `jobs.query` runs the SELECT.
///   2. `jobs.get` resolves the anonymous destination table.
///   3. Storage Read API `CreateReadSession` + `ReadRows` over gRPC
///      streams Arrow IPC bytes back.
///
/// Caller IAM requires `bigquery.readSessions.create` on
/// `BIGQUERY_TEST_PROJECT` in addition to the usual `dataEditor` +
/// `jobUser` grants (the bundled `BigQuery Read Session User` role
/// supplies it).
///
/// Run with:
///   `cargo test -p rocky-bigquery --test integration -- --ignored \
///    fetch_arrow_batch_returns_workspace_arrow_batch`
#[tokio::test]
#[ignore]
async fn fetch_arrow_batch_returns_workspace_arrow_batch() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;

    let adapter = adapter_from_env().expect("BigQuery env vars not set");

    let batch = adapter
        .fetch_arrow_batch("SELECT 1 AS n, 'foo' AS s")
        .await
        .expect("fetch_arrow_batch should succeed on BigQuery sandbox");

    assert_eq!(
        batch.num_rows(),
        1,
        "expected 1 row, got {}",
        batch.num_rows()
    );
    assert_eq!(batch.num_columns(), 2, "expected 2 columns");

    // Schema check — `INT64` / `STRING` is the BigQuery canonical
    // surface for the literal types `1` and `'foo'`. Both map onto
    // workspace arrow's `Int64` / `Utf8`.
    let schema = batch.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["n", "s"]);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

    let n_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column 0 is Int64");
    assert_eq!(n_col.value(0), 1);

    let s_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column 1 is Utf8");
    assert_eq!(s_col.value(0), "foo");
}

/// BigQuery's 10 MB minimum-bill floor (Phase 2.3 of the BQ trial-window
/// plan).
///
/// BigQuery's on-demand pricing applies a 10 MB (10 × 1024 × 1024 =
/// 10_485_760 bytes) minimum per query — any query that reads less than
/// 10 MB of raw columnar data is still billed as if it scanned 10 MB.
/// Documented at <https://cloud.google.com/bigquery/pricing#on_demand_pricing>:
/// "Each query you run will have a 10 MB minimum data processed."
///
/// **Query choice is load-bearing.** This test creates a tiny ephemeral
/// table (`UNNEST([STRUCT(1 AS id, 'a' AS name), STRUCT(2, 'b')])` — the
/// same shape as `test_clone_table_for_branch_copy`) rather than
/// scanning an `INFORMATION_SCHEMA` view. Reason: against
/// `INFORMATION_SCHEMA` views, BigQuery reports *both*
/// `totalBytesProcessed` and `totalBytesBilled` as the 10 MB floor —
/// the metadata path has no underlying columnar storage to report a
/// smaller raw figure against. Querying a real (tiny) table makes
/// `totalBytesProcessed` reflect the actual columnar bytes read (a few
/// dozen bytes for a 2-row INT64+STRING table) while
/// `totalBytesBilled` is rounded up to the floor. The two figures
/// genuinely diverge, which is what lets the test discriminate
/// "Rocky reads billed" from "Rocky reads processed". Constant-only
/// queries like `SELECT 1` aren't useful here either — BQ exempts
/// them from the floor entirely (they bill 0).
///
/// Rocky's cost path reads `totalBytesBilled` (floor-applied), not
/// `totalBytesProcessed` (raw scan) — the enrichment lives in
/// `connector.rs::execute_statement_with_stats` (lines 386-401), which
/// calls `fetch_job_statistics` after the sync `jobs.query` response
/// so the figure surfaced into `ExecutionStats::bytes_scanned` is the
/// billed one. Cost calc downstream multiplies that by the per-TB
/// rate. So for any sub-10 MB query, Rocky's reported cost should
/// match the floor-based number, not the cheaper processed-bytes
/// number.
///
/// This test makes that explicit:
///
/// 1. Issue a sub-10 MB query against an ephemeral 2-row table.
/// 2. Independently fetch `jobs.get` and confirm
///    `totalBytesProcessed < 10 MB` and `totalBytesBilled == 10 MB`
///    exactly. This documents what the floor actually looks like at
///    the BQ REST surface — future maintainers seeing a tiny query
///    that reports a 10 MB cost can read the assertion and understand
///    why.
/// 3. Assert Rocky's `bytes_scanned` equals `totalBytesBilled` (the
///    floor), not `totalBytesProcessed` (the raw scan).
/// 4. Assert Rocky's computed cost matches the floor-based calc and
///    *exceeds* the cheaper processed-based calc. The redundant
///    inequality is the regression guard: if a future change swaps
///    the connector back to reading `totalBytesProcessed` (the bug
///    that PR #330 fixed), the equality stays true while the
///    inequality flips, and the test trips loudly.
///
/// PR #617's `test_bytes_scanned_matches_independent_jobs_get_total_bytes_billed`
/// cross-checks that Rocky reads `totalBytesBilled` rather than
/// `totalBytesProcessed`, but it only asserts the byte figure clears
/// the 10 MB floor — it can't catch a regression where the *floor
/// itself* moves (BQ changes the minimum, or a future config knob
/// makes it configurable). This test pins the floor value as an
/// explicit assertion so the documented behavior stays load-bearing.
///
/// Dataset is dropped CASCADE on test exit regardless of outcome,
/// mirroring `test_clone_table_for_branch_copy`'s cleanup pattern.
#[tokio::test]
#[ignore]
async fn test_sub_10mb_query_bills_at_minimum_floor() {
    use rocky_bigquery::auth::BigQueryAuth;
    use rocky_core::cost::{BIGQUERY_USD_PER_TB_SCANNED, WarehouseType, compute_observed_cost_usd};
    use rocky_core::traits::WarehouseAdapter;
    use serde::Deserialize;

    /// BigQuery's 10 MB minimum-bill floor in bytes (10 × 1024 × 1024).
    /// Documented at <https://cloud.google.com/bigquery/pricing#on_demand_pricing>:
    /// "Each query you run will have a 10 MB minimum data processed."
    const BQ_MINIMUM_BILL_FLOOR_BYTES: u64 = 10 * 1024 * 1024;

    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());

    let adapter = adapter_from_env().expect("BigQuery env vars not set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let dataset = format!("hc_phase5_floor_{suffix}");
    let table = "tiny";

    // Setup: a tiny 2-row table. BigQuery's `jobs.get` reports
    // `totalBytesProcessed` as the actual columnar bytes read (a few
    // dozen for INT64+STRING × 2 rows) while `totalBytesBilled` gets
    // rounded up to the 10 MB floor. Both figures coming from
    // `INFORMATION_SCHEMA` views happen to be the floor (the metadata
    // path has no smaller raw figure to report), so a real ephemeral
    // table is what makes the processed-vs-billed comparison
    // meaningful.
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS `{project}`.`{dataset}`"
        ))
        .await
        .expect("create dataset");
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE `{project}`.`{dataset}`.`{table}` AS \
             SELECT * FROM UNNEST([STRUCT(1 AS id, 'a' AS name), STRUCT(2, 'b')])"
        ))
        .await
        .expect("create tiny table");

    let sql = format!("SELECT id, name FROM `{project}`.`{dataset}`.`{table}`");

    // Run unit under test, capture result; cleanup runs before any
    // assertion-driven panic so a mid-test failure doesn't leak the
    // dataset.
    let probe_result = adapter.execute_statement_with_stats(&sql).await;

    // Independent `jobs.get` REST call so the floor assertion isn't
    // circular with the adapter path that produced `stats`. Same
    // shape as PR #617's cross-check, extended to include
    // `totalBytesProcessed` since this test compares processed vs
    // billed. Only attempted when the adapter call succeeded; on
    // failure the cleanup-then-assert pattern still rethrows the
    // underlying error.
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
        total_bytes_processed: String,
        total_bytes_billed: String,
    }

    let job_get: Option<(String, u64, u64, u64)> = if let Ok(ref stats) = probe_result {
        let rocky_bytes = stats.bytes_scanned.expect("bytes_scanned populated");
        let job_id = stats.job_id.clone().expect("job_id populated");
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
        let job: JobGet = resp.json().await.expect("parse jobs.get JSON");
        let bq_processed: u64 = job
            .statistics
            .query
            .total_bytes_processed
            .parse()
            .expect("totalBytesProcessed parses as u64");
        let bq_billed: u64 = job
            .statistics
            .query
            .total_bytes_billed
            .parse()
            .expect("totalBytesBilled parses as u64");
        Some((job_id, rocky_bytes, bq_processed, bq_billed))
    } else {
        None
    };

    // Unconditional cleanup. Best-effort — a leftover `hc_phase5_*`
    // dataset is easy to mop up via `bq rm -r -f -d` but tearing down
    // here keeps the sandbox clean on the happy path.
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{dataset}` CASCADE"
        ))
        .await;

    // Surface adapter failure (if any) after cleanup.
    let _stats = probe_result.expect("execute_statement_with_stats should succeed");
    let (job_id, rocky_bytes, bq_processed, bq_billed) =
        job_get.expect("job_get populated when probe succeeded");

    // Receipt printed before assertions so a `cargo test -- --ignored
    // --nocapture` run shows the raw values even when an assertion
    // later fails — easier to diagnose a real floor change vs a test
    // bug.
    let rocky_cost_usd =
        compute_observed_cost_usd(WarehouseType::BigQuery, Some(rocky_bytes), 0, 0.0, 0.0)
            .expect("compute_observed_cost_usd returned None for BigQuery bytes");
    let floor_cost_usd =
        (BQ_MINIMUM_BILL_FLOOR_BYTES as f64 / 1.0e12) * BIGQUERY_USD_PER_TB_SCANNED;
    let processed_cost_usd = (bq_processed as f64 / 1.0e12) * BIGQUERY_USD_PER_TB_SCANNED;
    println!(
        "bq-floor-check | job_id={job_id} location={location} \
         bq.totalBytesProcessed={bq_processed} (raw scan) \
         bq.totalBytesBilled={bq_billed} (floor-applied) \
         floor_bytes={BQ_MINIMUM_BILL_FLOOR_BYTES} \
         rocky.bytes_scanned={rocky_bytes} \
         rocky.cost_usd=${rocky_cost_usd:.9} \
         floor.cost_usd=${floor_cost_usd:.9} \
         processed.cost_usd=${processed_cost_usd:.9}"
    );

    // (1) Precondition: the raw-scan figure must be strictly under the
    // floor — otherwise the query is too large to exercise the floor
    // path. The 2-row UNNEST seed makes this trivially true; the
    // assertion guards future test edits that swap in a larger probe.
    assert!(
        bq_processed < BQ_MINIMUM_BILL_FLOOR_BYTES,
        "test precondition violated: totalBytesProcessed={bq_processed} is not under \
         the 10 MB floor ({BQ_MINIMUM_BILL_FLOOR_BYTES}). The query needs to scan less \
         than 10 MB of raw data to exercise the floor behavior."
    );

    // (2) The billed figure must equal the floor exactly. Pinning the
    // exact value makes a future "BigQuery moved the floor" change
    // surface here as a loud failure, with the doc-comment and the
    // receipt available as the diagnostic context.
    assert_eq!(
        bq_billed, BQ_MINIMUM_BILL_FLOOR_BYTES,
        "totalBytesBilled={bq_billed} does not match the documented 10 MB floor \
         ({BQ_MINIMUM_BILL_FLOOR_BYTES}). Either BigQuery has changed its minimum-bill \
         behavior or the query unexpectedly scans more than 10 MB. Inspect job_id={job_id} \
         in the GCP console."
    );

    // (3) Rocky must read the billed figure (floor-applied), not the
    // processed figure (raw scan). The connector enrichment path
    // (`connector.rs::execute_statement_with_stats` →
    // `fetch_job_statistics`) is what makes this true — this assertion
    // is the canary if that enrichment ever regresses.
    assert_eq!(
        rocky_bytes, bq_billed,
        "Rocky's bytes_scanned must equal BigQuery's totalBytesBilled (the floor-applied \
         figure surfaced via jobs.get), not totalBytesProcessed. \
         rocky.bytes_scanned={rocky_bytes} bq.totalBytesBilled={bq_billed} \
         bq.totalBytesProcessed={bq_processed}. \
         If they diverge, connector.rs::execute_statement_with_stats has likely lost the \
         jobs.get enrichment fixed in PR #330."
    );

    // (4) Rocky's computed cost must match the floor-based dollar calc
    // exactly. Uses the same `compute_observed_cost_usd` call the run
    // finalizer makes (mirrors the cost path in `populate_cost_summary`).
    let rel_diff = if floor_cost_usd == 0.0 {
        0.0
    } else {
        ((rocky_cost_usd - floor_cost_usd) / floor_cost_usd).abs()
    };
    assert!(
        rel_diff < 1e-9,
        "Rocky's computed cost does not match the floor-based formula. \
         rocky.cost_usd=${rocky_cost_usd:.9} floor.cost_usd=${floor_cost_usd:.9} \
         rel_diff={rel_diff:e}. \
         Expected `bytes_scanned * BIGQUERY_USD_PER_TB_SCANNED / 1e12` with \
         bytes_scanned = floor ({BQ_MINIMUM_BILL_FLOOR_BYTES}).",
    );

    // (5) Redundant cross-check: the cost must strictly exceed what
    // it would be if Rocky read `totalBytesProcessed` instead of
    // `totalBytesBilled`. If a future change reverts the PR #330 fix
    // (reading processed instead of billed), assertion (3) catches it
    // via the byte equality, but this one fails too — and the failure
    // message points directly at the cost-attribution implication
    // rather than the byte-level cause.
    assert!(
        rocky_cost_usd > processed_cost_usd,
        "Rocky's cost ({rocky_cost_usd:.9}) does not exceed the processed-based cost \
         ({processed_cost_usd:.9}). For a sub-10 MB query the floor-based cost MUST be \
         higher — if it isn't, Rocky is reading the raw scan figure rather than the \
         billed figure, defeating the floor enrichment."
    );
}

/// Verifies the wired `BigQueryGovernanceAdapter::set_tags` path applies
/// labels to a dataset and a table via `ALTER SCHEMA ... SET OPTIONS` /
/// `ALTER TABLE ... SET OPTIONS` and that BigQuery exposes the applied
/// labels via `INFORMATION_SCHEMA.SCHEMATA_OPTIONS` /
/// `INFORMATION_SCHEMA.TABLE_OPTIONS`.
///
/// Until the registry wiring for `BigQueryGovernanceAdapter` landed, this
/// adapter was instantiable in code but never reached production callers.
/// The test exercises the dataset/table targets (the two that execute
/// real SQL); `TagTarget::Catalog` stays a warn-and-return because BQ
/// projects do not support labels via SQL.
#[tokio::test]
#[ignore]
async fn test_governance_set_tags_applies_labels() {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use rocky_bigquery::governance::BigQueryGovernanceAdapter;
    use rocky_core::traits::{GovernanceAdapter, TagTarget};

    let warehouse = Arc::new(adapter_from_env().expect("BigQuery env vars not set"));
    let project =
        std::env::var("BIGQUERY_TEST_PROJECT").expect("BIGQUERY_TEST_PROJECT must be set");

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let dataset = format!("hc_phase6_gov_{suffix}");
    let table = "labels_target";

    // Setup. Arc auto-derefs through &self method calls.
    warehouse
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");
    warehouse
        .execute_statement(&format!(
            "CREATE TABLE `{project}`.`{dataset}`.`{table}` AS \
             SELECT * FROM UNNEST([STRUCT(1 AS id, 'a' AS name)])"
        ))
        .await
        .expect("create table");

    // Run the unit under test via the wired adapter (Arc-shaped).
    let gov = BigQueryGovernanceAdapter::new(Arc::clone(&warehouse));

    let mut labels = BTreeMap::new();
    labels.insert("env".to_string(), "test".to_string());
    labels.insert("owner".to_string(), "rocky".to_string());

    let schema_target = TagTarget::Schema {
        catalog: project.clone(),
        schema: dataset.clone(),
    };
    let schema_result = gov.set_tags(&schema_target, &labels).await;

    let table_target = TagTarget::Table {
        catalog: project.clone(),
        schema: dataset.clone(),
        table: table.to_string(),
    };
    let table_result = gov.set_tags(&table_target, &labels).await;

    // Read the labels back via INFORMATION_SCHEMA *_OPTIONS so the
    // assertion goes through BigQuery's own metadata surface rather
    // than trusting the SQL we emitted to match what BigQuery stored.
    let schema_labels_q = format!(
        "SELECT option_name, option_value FROM `{project}`.`region-EU`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS \
         WHERE schema_name = '{dataset}' AND option_name = 'labels'"
    );
    let schema_labels_result = warehouse.execute_query(&schema_labels_q).await.ok();

    let table_labels_q = format!(
        "SELECT option_name, option_value FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLE_OPTIONS \
         WHERE table_name = '{table}' AND option_name = 'labels'"
    );
    let table_labels_result = warehouse.execute_query(&table_labels_q).await.ok();

    // Cleanup before assertions.
    let _ = warehouse
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{dataset}` CASCADE"
        ))
        .await;

    schema_result.expect("set_tags(Schema) should succeed");
    table_result.expect("set_tags(Table) should succeed");

    let schema_rows = schema_labels_result.expect("schema labels queryable");
    assert!(
        !schema_rows.rows.is_empty(),
        "expected at least one labels row for the dataset"
    );
    let schema_label_value = schema_rows.rows[0]
        .last()
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        schema_label_value.contains("env") && schema_label_value.contains("owner"),
        "dataset labels should contain env + owner; got {schema_label_value}"
    );

    let table_rows = table_labels_result.expect("table labels queryable");
    assert!(
        !table_rows.rows.is_empty(),
        "expected at least one labels row for the table"
    );
    let table_label_value = table_rows.rows[0]
        .last()
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        table_label_value.contains("env") && table_label_value.contains("owner"),
        "table labels should contain env + owner; got {table_label_value}"
    );
}
