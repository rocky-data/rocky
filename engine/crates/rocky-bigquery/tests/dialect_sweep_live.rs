//! Live BigQuery dialect-sweep regression probes.
//!
//! Live execute-coverage regression tests for the dialect adapter-gating
//! fixes (D1/D3), two capability probes that the audit REFUTED
//! (insert_overwrite_partition, ALTER SET DATA TYPE widening), and a
//! positive MERGE probe (`merge_into` — `INSERT ROW` + per-column UPDATE
//! SET, the BQ-reachable `rocky run` merge path). They run the SQL Rocky
//! *actually emits* (via the real emitter functions) against the BigQuery
//! sandbox. Trial credits — tiny tables only.
//!
//! POST-FIX behaviour pinned:
//!   - D1 (lakehouse `USING <format>` DDL) and D3 (compact/archive
//!     `OPTIMIZE`/`VACUUM`/`DATEADD`) now fail fast *in the engine* — the
//!     generator refuses with a known-limitation error BEFORE any SQL is
//!     produced, so the probes assert the in-process `Err`. (This also
//!     supersedes the old "hyphenated GCP project fails validate_identifier"
//!     path, because the maintenance gate fires first.)
//!
//! `#[ignore]`-gated; reads from the SAME env vars as the other live tests:
//!   BIGQUERY_TEST_PROJECT, BIGQUERY_TEST_LOCATION (default "EU"),
//!   GOOGLE_APPLICATION_CREDENTIALS (or BIGQUERY_TOKEN)
//!
//! Run with:
//!   cargo test -p rocky-bigquery --test dialect_sweep_live -- --ignored --nocapture
//!
//! Datasets use the `hc_dialsweep_` prefix and are dropped CASCADE on exit.

use std::time::{SystemTime, UNIX_EPOCH};

use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryAdapter;
use rocky_bigquery::dialect::BigQueryDialect;
use rocky_core::lakehouse::{self, LakehouseError, LakehouseFormat, LakehouseOptions};
use rocky_core::sql_gen::{SqlGenError, archive_from_ir, compact_from_ir};
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::{ArchivePlanIr, ColumnSelection, CompactPlanIr};

fn adapter_from_env() -> Option<(BigQueryAdapter, String)> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    let location = std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string());
    let auth = BigQueryAuth::from_env().ok()?;
    Some((
        BigQueryAdapter::new(project.clone(), location, auth),
        project,
    ))
}

fn suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

async fn drop_dataset(adapter: &BigQueryAdapter, project: &str, dataset: &str) {
    let _ = adapter
        .execute_statement(&format!(
            "DROP SCHEMA IF EXISTS `{project}`.`{dataset}` CASCADE"
        ))
        .await;
}

/// Read an i64 COUNT/value out of BigQuery's JSON-string row encoding.
fn as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// D1 — lakehouse `USING DELTA|ICEBERG` DDL is Spark/Databricks-only.
/// POST-FIX: `generate_lakehouse_ddl` refuses on the BigQuery dialect with
/// `LakehouseError::DialectUnsupported` BEFORE producing any SQL.
#[tokio::test]
#[ignore]
async fn d1_lakehouse_using_format_ddl_refused_in_engine() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP d1: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_d1_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");

    let dialect = BigQueryDialect;
    let target = dialect
        .format_table_ref(&project, &dataset, "hc_probe")
        .expect("format target");
    let opts = LakehouseOptions::default();

    // CONTROL: plain CTAS (no USING) MUST succeed.
    let control_sql = format!("CREATE OR REPLACE TABLE {target} AS SELECT 1 AS id");
    let control = adapter.execute_statement(&control_sql).await;

    // POST-FIX: the engine refuses before producing SQL.
    let mut results = Vec::new();
    for fmt in [LakehouseFormat::DeltaTable, LakehouseFormat::IcebergTable] {
        let outcome =
            lakehouse::generate_lakehouse_ddl(&fmt, &target, "SELECT 1 AS id", &opts, &dialect);
        results.push((format!("{fmt}"), outcome));
    }
    drop_dataset(&adapter, &project, &dataset).await;

    control.expect("CONTROL plain CTAS must succeed — proves warehouse reached");
    println!("\n=== D1 BigQuery: lakehouse USING <format> DDL (engine refusal) ===");
    println!("CONTROL (plain CTAS, no USING): SUCCEEDED → warehouse reached, dataset/select valid");
    for (fmt, outcome) in results {
        match outcome {
            Ok(sql) => panic!("[{fmt}] expected engine refusal but got SQL: {sql:?}"),
            Err(LakehouseError::DialectUnsupported { format, dialect }) => {
                assert_eq!(dialect, "bigquery");
                println!(
                    "[{fmt}] VERDICT: engine refused before SQL — \
                     DialectUnsupported {{ format: {format:?}, dialect: {dialect:?} }}"
                );
            }
            Err(other) => panic!("[{fmt}] expected DialectUnsupported, got {other:?}"),
        }
    }
}

/// D3a — `OPTIMIZE` (compact) refused in the engine on BigQuery.
/// POST-FIX: `compact_from_ir` refuses with `UnsupportedMaintenance` BEFORE
/// producing any SQL (the maintenance gate fires before the
/// hyphenated-project validation path).
#[tokio::test]
#[ignore]
async fn d3a_compact_refused_in_engine() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP d3a: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_d3a_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");
    let bare_ref = format!("{project}.{dataset}.hc_probe");
    let quoted_ref = format!("`{project}`.`{dataset}`.`hc_probe`");
    let control = adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {quoted_ref} AS SELECT 1 AS id"
        ))
        .await;

    // POST-FIX: the gated generator refuses before producing any SQL.
    let ir = CompactPlanIr::for_table(&bare_ref, 256);
    let outcome = compact_from_ir(&ir, &BigQueryDialect);
    drop_dataset(&adapter, &project, &dataset).await;

    control.expect("CONTROL CTAS must succeed — proves warehouse reached + table exists");
    println!("\n=== D3a BigQuery: OPTIMIZE (compact) engine refusal ===");
    println!("CONTROL CTAS: SUCCEEDED → warehouse reached, table exists");
    match outcome {
        Ok(stmts) => panic!("expected engine refusal but compact_from_ir produced SQL: {stmts:?}"),
        Err(SqlGenError::UnsupportedMaintenance { operation, dialect }) => {
            assert_eq!(operation, "OPTIMIZE");
            assert_eq!(dialect, "bigquery");
            println!(
                "VERDICT: engine refused before SQL — UnsupportedMaintenance(OPTIMIZE, bigquery)"
            );
        }
        Err(other) => panic!("expected UnsupportedMaintenance, got {other:?}"),
    }
}

/// D3b/D3c — archive (`DELETE … DATEADD` + `VACUUM`) refused in the engine.
/// POST-FIX: `archive_from_ir` refuses with `UnsupportedMaintenance` BEFORE
/// producing any SQL — so the `DATEADD`-is-not-a-BQ-function and
/// hyphenated-project failures are both moot.
#[tokio::test]
#[ignore]
async fn d3_archive_refused_in_engine() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP d3 archive: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_d3c_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");
    let bare_ref = format!("{project}.{dataset}.hc_probe");
    let quoted_ref = format!("`{project}`.`{dataset}`.`hc_probe`");
    let control = adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {quoted_ref} AS \
             SELECT 1 AS id, CURRENT_TIMESTAMP() AS _fivetran_synced"
        ))
        .await;

    // POST-FIX: the gated generator refuses before producing any SQL.
    let ir = ArchivePlanIr::for_table(&bare_ref, 90);
    let outcome = archive_from_ir(&ir, &BigQueryDialect);
    drop_dataset(&adapter, &project, &dataset).await;

    control.expect("CONTROL CTAS must succeed — proves warehouse reached + table exists");
    println!("\n=== D3 BigQuery: archive bundle engine refusal ===");
    println!("CONTROL CTAS (with _fivetran_synced col): SUCCEEDED → warehouse reached");
    match outcome {
        Ok(stmts) => panic!("expected engine refusal but archive_from_ir produced SQL: {stmts:?}"),
        Err(SqlGenError::UnsupportedMaintenance { operation, dialect }) => {
            assert_eq!(operation, "VACUUM");
            assert_eq!(dialect, "bigquery");
            println!(
                "VERDICT: engine refused before SQL — UnsupportedMaintenance(VACUUM, bigquery)"
            );
        }
        Err(other) => panic!("expected UnsupportedMaintenance, got {other:?}"),
    }
}

/// EXECUTE-GAP #4 (REFUTED — kept as regression coverage) — BigQuery
/// `insert_overwrite_partition` multi-statement
/// `BEGIN TRANSACTION; DELETE; INSERT; COMMIT TRANSACTION` script.
/// SUCCESS + only the targeted partition replaced (others byte-identical) =
/// REFUTED (works). Execute the Vec element AS-IS (single script — do NOT
/// split on `;`).
#[tokio::test]
#[ignore]
async fn gap4_insert_overwrite_partition_replaces_only_target() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP gap4: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_g4_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");

    let dialect = BigQueryDialect;
    let target = dialect
        .format_table_ref(&project, &dataset, "hc_part")
        .expect("format target");

    // Seed two partitions: part=1 (rows 10,11) and part=2 (rows 20,21).
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {target} AS \
             SELECT * FROM UNNEST([\
               STRUCT(1 AS part, 10 AS val), STRUCT(1, 11), \
               STRUCT(2, 20), STRUCT(2, 21)])"
        ))
        .await
        .expect("seed two partitions");

    // Real emitter: overwrite partition part=1 with a single new row (99).
    let stmts = dialect
        .insert_overwrite_partition(&target, "part = 1", "SELECT 1 AS part, 99 AS val")
        .expect("insert_overwrite_partition");
    assert_eq!(stmts.len(), 1, "BQ emits ONE single-script statement");
    let script = stmts[0].clone();

    let exec = adapter.execute_statement(&script).await;

    // Read back regardless of exec outcome (capture before cleanup).
    let part1 = adapter
        .execute_query(&format!(
            "SELECT val FROM {target} WHERE part = 1 ORDER BY val"
        ))
        .await
        .ok();
    let part2 = adapter
        .execute_query(&format!(
            "SELECT val FROM {target} WHERE part = 2 ORDER BY val"
        ))
        .await
        .ok();

    drop_dataset(&adapter, &project, &dataset).await;

    println!("\n=== EXECUTE-GAP #4 BigQuery: insert_overwrite_partition ===");
    println!("SCRIPT SENT (single statement, executed as-is):\n{script}");
    match &exec {
        Ok(()) => println!("RAW RESPONSE: script SUCCEEDED"),
        Err(e) => println!("RAW ERROR: {e}"),
    }
    exec.expect("partition-overwrite script must execute — else CONFIRMED bug; see raw error");

    let p1 = part1.expect("part=1 queryable");
    let p2 = part2.expect("part=2 queryable");
    let p1_vals: Vec<i64> = p1.rows.iter().filter_map(|r| as_i64(&r[0])).collect();
    let p2_vals: Vec<i64> = p2.rows.iter().filter_map(|r| as_i64(&r[0])).collect();
    println!(
        "AFTER: part=1 vals={p1_vals:?} (expected [99]) | part=2 vals={p2_vals:?} (expected [20,21] untouched)"
    );

    assert_eq!(
        p1_vals,
        vec![99],
        "targeted partition part=1 should be fully replaced by [99]"
    );
    assert_eq!(
        p2_vals,
        vec![20, 21],
        "untargeted partition part=2 must be byte-identical (untouched)"
    );
    println!("VERDICT: REFUTED — partition-replace works; only part=1 replaced, part=2 intact");
}

/// EXECUTE-GAP #5 (REFUTED — kept as regression coverage) — BigQuery
/// `ALTER ... SET DATA TYPE` safe widening (INT64 → NUMERIC). SUCCESS =
/// REFUTED (works). SQL generated via the real
/// `BigQueryDialect::alter_column_type_sql`.
#[tokio::test]
#[ignore]
async fn gap5_alter_set_data_type_widening() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP gap5: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_g5_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");

    let dialect = BigQueryDialect;
    let target = dialect
        .format_table_ref(&project, &dataset, "hc_widen")
        .expect("format target");
    // amount starts as INT64.
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {target} (id INT64, amount INT64)"
        ))
        .await
        .expect("seed table with INT64 amount");

    // Real emitter → "ALTER TABLE <t> ALTER COLUMN amount SET DATA TYPE NUMERIC"
    let alter_sql = dialect
        .alter_column_type_sql(&target, "amount", "NUMERIC")
        .expect("alter_column_type_sql");

    let exec = adapter.execute_statement(&alter_sql).await;

    // Confirm the new type via INFORMATION_SCHEMA (capture before cleanup).
    let type_q = adapter
        .execute_query(&format!(
            "SELECT data_type FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = 'hc_widen' AND column_name = 'amount'"
        ))
        .await
        .ok();

    drop_dataset(&adapter, &project, &dataset).await;

    println!("\n=== EXECUTE-GAP #5 BigQuery: ALTER ... SET DATA TYPE (INT64→NUMERIC) ===");
    println!("SQL SENT:\n{alter_sql}");
    match &exec {
        Ok(()) => println!("RAW RESPONSE: ALTER SUCCEEDED"),
        Err(e) => println!("RAW ERROR: {e}"),
    }
    exec.expect("ALTER SET DATA TYPE widening must succeed — else CONFIRMED bug; see raw error");

    let new_type = type_q
        .and_then(|q| {
            q.rows
                .first()
                .and_then(|r| r.first().and_then(|v| v.as_str().map(str::to_string)))
        })
        .unwrap_or_default();
    println!("AFTER: amount data_type = {new_type:?} (expected NUMERIC)");
    assert_eq!(
        new_type.to_uppercase(),
        "NUMERIC",
        "column should be widened to NUMERIC"
    );
    println!("VERDICT: REFUTED — BigQuery accepts the emitted ALTER ... SET DATA TYPE widening");
}

/// MERGE — `BigQueryDialect::merge_into` execute-coverage against the live
/// sandbox. The `Merge` strategy dispatches **unconditionally** to
/// `merge_into` on every dialect (`rocky_core::sql_gen` — both the
/// replication path at ~:221 and the transformation path at ~:331); there is
/// no per-dialect maintenance gate like the one D3/D3a/D3b probes assert for
/// `OPTIMIZE`/`VACUUM`. So merge is a BigQuery-reachable `rocky run` path,
/// and the BQ-specific `WHEN NOT MATCHED THEN INSERT ROW` + per-column
/// `target.<c> = source.<c>` UPDATE SET it emits are exactly the kind of
/// non-standard syntax that string-content unit tests pass but a real
/// warehouse can reject.
///
/// **Production-faithful column selection.** The replication runner resolves
/// `ColumnSelection::All` into the explicit per-column form *before* SQL-gen
/// (`commands/run.rs::resolve_merge_update_columns`, called at ~:6328 with the
/// note "Snowflake's MERGE rejects `UPDATE SET *`"), so the SQL `rocky run`
/// actually emits is per-column `target.<c> = source.<c>`, not the whole-row
/// `target = source` form. This test passes `ColumnSelection::Explicit([id,
/// val])` — the keys-included shape the resolver produces — so it exercises
/// the same MERGE statement the production replication path generates, plus
/// the always-on `INSERT ROW` clause.
///
/// Seeds a 2-row target, merges a 2-row source (1 matched key, 1 new key),
/// then reads back and asserts:
///   - matched key (id=1) UPDATED to the new value,
///   - new key (id=3) INSERTED via `INSERT ROW`,
///   - untouched key (id=2) left intact.
/// Reads are captured BEFORE the CASCADE drop. Dataset uses the
/// `hc_dialsweep_merge_` prefix and is dropped on exit.
#[tokio::test]
#[ignore]
async fn merge_into_updates_matched_and_inserts_unmatched() {
    let Some((adapter, project)) = adapter_from_env() else {
        eprintln!("SKIP merge: BigQuery env not set");
        return;
    };
    let dataset = format!("hc_dialsweep_merge_{}", suffix());
    drop_dataset(&adapter, &project, &dataset).await;
    adapter
        .execute_statement(&format!("CREATE SCHEMA `{project}`.`{dataset}`"))
        .await
        .expect("create dataset");

    let dialect = BigQueryDialect;
    let target = dialect
        .format_table_ref(&project, &dataset, "hc_merge")
        .expect("format target");

    // Seed the target: id=1 ('old'), id=2 ('old'). The column order
    // (id, val) must match the source's STRUCT field order so BigQuery's
    // `INSERT ROW` (positional, whole-row insert) lands the new row in the
    // right columns.
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {target} AS \
             SELECT * FROM UNNEST([\
               STRUCT(1 AS id, 'old' AS val), STRUCT(2, 'old')])"
        ))
        .await
        .expect("seed target");

    // Real emitter: the source is the same UNNEST shape, aliased so the
    // merge's `ON target.id = source.id` and `target.val = source.val`
    // resolve. id=1 matches (→ UPDATE), id=3 is new (→ INSERT ROW).
    let source_sql = "SELECT id, val FROM UNNEST([\
                      STRUCT(1 AS id, 'new' AS val), STRUCT(3, 'new')])";
    let update_cols = ColumnSelection::Explicit(vec!["id".into(), "val".into()]);
    let merge_sql = dialect
        .merge_into(&target, source_sql, &["id".into()], &update_cols)
        .expect("merge_into");

    let exec = adapter.execute_statement(&merge_sql).await;

    // Read back all three keys regardless of exec outcome (before cleanup).
    let after = adapter
        .execute_query(&format!("SELECT id, val FROM {target} ORDER BY id"))
        .await
        .ok();

    drop_dataset(&adapter, &project, &dataset).await;

    println!("\n=== MERGE BigQuery: merge_into (matched UPDATE + unmatched INSERT ROW) ===");
    println!("SQL SENT:\n{merge_sql}");
    match &exec {
        Ok(()) => println!("RAW RESPONSE: MERGE SUCCEEDED"),
        Err(e) => println!("RAW ERROR: {e}"),
    }
    exec.expect(
        "MERGE must execute — else CONFIRMED bug in BigQueryDialect::merge_into \
         (INSERT ROW / per-column UPDATE SET rejected); see raw error",
    );

    let rows = after.expect("target queryable after merge");
    // (id, val) keyed by id. BigQuery returns INT64 as a JSON string, so
    // parse defensively via `as_i64`.
    let mut by_id: std::collections::BTreeMap<i64, String> = std::collections::BTreeMap::new();
    for r in &rows.rows {
        let id = as_i64(&r[0]).expect("id parses");
        let val = r[1].as_str().unwrap_or_default().to_string();
        by_id.insert(id, val);
    }
    println!("AFTER: {by_id:?} (expected {{1: \"new\", 2: \"old\", 3: \"new\"}})");

    assert_eq!(
        by_id.get(&1).map(String::as_str),
        Some("new"),
        "matched key id=1 must be UPDATED to 'new'"
    );
    assert_eq!(
        by_id.get(&2).map(String::as_str),
        Some("old"),
        "untouched key id=2 must stay 'old' (no spurious update)"
    );
    assert_eq!(
        by_id.get(&3).map(String::as_str),
        Some("new"),
        "unmatched key id=3 must be INSERTED via INSERT ROW"
    );
    assert_eq!(
        by_id.len(),
        3,
        "exactly 3 distinct ids after merge; got {by_id:?}"
    );
    // Physical-row guard: a fan-out / double-applied MERGE can emit two rows
    // sharing an id, which `by_id` silently dedupes — so check the raw count too.
    assert_eq!(
        rows.rows.len(),
        3,
        "exactly 3 physical rows after merge; a duplicated id would slip the distinct-id check; got {} rows",
        rows.rows.len()
    );
    println!("VERDICT: merge_into works — id=1 UPDATED, id=3 INSERTED (INSERT ROW), id=2 intact");
}
