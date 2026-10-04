//! DuckDB-backed regression tests for the checksum-bisection chunk
//! checksum (RV2-P0).
//!
//! The chunk checksum is `COUNT(*) + BIT_XOR(row_hash)`. The row hash
//! used to leave the primary key out. Three defects followed:
//!
//! 1. Two rows that change to one shared value tuple cancel under XOR
//!    (`h ^ h = 0`), so the chunk checksum did not change.
//! 2. Values that swap between keys leave the multiset of value tuples,
//!    and so the chunk checksum, unchanged.
//! 3. Rows with a NULL key were counted but never compared.
//!
//! The fix hashes the key with the values and compares the null-key
//! group by count and row-hash multiset. Each case below reported "no
//! change" before the fix. Test names start with `compare_bisection_`
//! so `cargo test -p rocky-core compare` runs them.

use rocky_core::compare::bisection::{
    BisectionConfig, BisectionDiffResult, BisectionTarget, LeafRowKind, bisection_diff,
};
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
use rocky_ir::TableRef;

/// Create `schema.t (id INTEGER, name VARCHAR)` and insert `rows`.
/// `None` as the id inserts a NULL key.
async fn seed(adapter: &DuckDbWarehouseAdapter, schema: &str, rows: &[(Option<i64>, &str)]) {
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {schema}.t (id INTEGER, name VARCHAR)"
        ))
        .await
        .unwrap();
    if rows.is_empty() {
        return;
    }
    let values = rows
        .iter()
        .map(|(id, name)| {
            let id = id.map_or_else(|| "NULL".to_string(), |v| v.to_string());
            format!("({id}, '{name}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    adapter
        .execute_statement(&format!("INSERT INTO {schema}.t VALUES {values}"))
        .await
        .unwrap();
}

/// Seed `n` rows `(i, 'row_<i>')` for `i in 0..n`, then apply `updates`.
async fn seed_range(adapter: &DuckDbWarehouseAdapter, schema: &str, n: u64, updates: &[&str]) {
    seed(adapter, schema, &[]).await;
    adapter
        .execute_statement(&format!(
            "INSERT INTO {schema}.t SELECT i, 'row_' || i FROM range(0, {n}) t(i)"
        ))
        .await
        .unwrap();
    for update in updates {
        adapter
            .execute_statement(&format!("UPDATE {schema}.t SET {update}"))
            .await
            .unwrap();
    }
}

fn table(schema: &str) -> TableRef {
    TableRef {
        catalog: String::new(),
        schema: schema.into(),
        table: "t".into(),
    }
}

async fn diff(
    adapter: &DuckDbWarehouseAdapter,
    pk_lo: i128,
    pk_hi: i128,
    min_chunk_rows: u64,
) -> BisectionDiffResult {
    let base = table("base");
    let branch = table("branch");
    let value_columns = vec!["name".to_string()];
    let target = BisectionTarget {
        base: &base,
        branch: &branch,
        pk_column: "id",
        value_columns: &value_columns,
        pk_lo,
        pk_hi,
    };
    let config = BisectionConfig {
        min_chunk_rows: Some(min_chunk_rows),
        ..Default::default()
    };
    bisection_diff(adapter, adapter, &target, &config)
        .await
        .expect("bisection_diff must succeed on DuckDB")
}

/// Defect 1: base `{(1,'a'),(2,'a')}`, branch `{(1,'b'),(2,'b')}`. Under
/// `BIT_XOR` over a value-only hash both sides are `(2 rows, 0)`. With
/// the key in the hash, `h(1,'a') ^ h(2,'a')` is not 0.
#[tokio::test]
async fn compare_bisection_detects_equal_value_rows_changed_together() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed(&adapter, "base", &[(Some(1), "a"), (Some(2), "a")]).await;
    seed(&adapter, "branch", &[(Some(1), "b"), (Some(2), "b")]).await;

    let result = diff(&adapter, 1, 3, 1000).await;

    assert_eq!(result.rows_changed, 2, "both rows changed: {result:?}");
    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.stats.leaves_materialized, 1);
}

/// Defect 1 at scale: inside a 10k-row table, two rows in the same chunk
/// change together to one shared value. The runner must recurse into the
/// chunk and surface both rows, not stop at the root.
#[tokio::test]
async fn compare_bisection_detects_pair_changed_together_inside_large_chunk() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_range(
        &adapter,
        "base",
        10_000,
        &["name = 'same_before' WHERE id IN (5000, 5001)"],
    )
    .await;
    seed_range(
        &adapter,
        "branch",
        10_000,
        &["name = 'same_after' WHERE id IN (5000, 5001)"],
    )
    .await;

    let result = diff(&adapter, 0, 10_000, 100).await;

    assert_eq!(result.rows_changed, 2, "{result:?}");
    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    let pks: Vec<&str> = result.samples.iter().map(|s| s.pk.as_str()).collect();
    assert_eq!(pks, vec!["5000", "5001"]);
}

/// Defect 2: base `{(1,'a'),(2,'b')}`, branch `{(1,'b'),(2,'a')}`. The
/// multiset of value tuples is the same, so a value-only hash misses it.
#[tokio::test]
async fn compare_bisection_detects_value_swap_between_keys() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed(&adapter, "base", &[(Some(1), "a"), (Some(2), "b")]).await;
    seed(&adapter, "branch", &[(Some(1), "b"), (Some(2), "a")]).await;

    let result = diff(&adapter, 1, 3, 1000).await;

    assert_eq!(result.rows_changed, 2, "{result:?}");
    let pks: Vec<&str> = result.samples.iter().map(|s| s.pk.as_str()).collect();
    assert_eq!(pks, vec!["1", "2"]);
}

/// Defect 3: the null-key rows hold different values, with the same
/// count on both sides. The keyed rows are identical.
#[tokio::test]
async fn compare_bisection_detects_null_key_rows_with_different_values() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed(
        &adapter,
        "base",
        &[(Some(1), "a"), (Some(2), "b"), (None, "x"), (None, "y")],
    )
    .await;
    seed(
        &adapter,
        "branch",
        &[(Some(1), "a"), (Some(2), "b"), (None, "x"), (None, "z")],
    )
    .await;

    let result = diff(&adapter, 1, 3, 1000).await;

    assert_eq!(result.stats.null_pk_rows_base, 2);
    assert_eq!(result.stats.null_pk_rows_branch, 2);
    assert_eq!(result.rows_added, 1, "{result:?}");
    assert_eq!(result.rows_removed, 1, "{result:?}");
    assert_eq!(result.rows_changed, 0);
    let kinds: Vec<(LeafRowKind, &str)> = result
        .samples
        .iter()
        .map(|s| (s.kind, s.pk.as_str()))
        .collect();
    assert_eq!(
        kinds,
        vec![(LeafRowKind::Added, "NULL"), (LeafRowKind::Removed, "NULL")]
    );
    // The keyed rows match, so no chunk was materialized.
    assert_eq!(result.stats.leaves_materialized, 0);
}

/// Defect 3: the branch holds one more null-key row than the base.
#[tokio::test]
async fn compare_bisection_detects_extra_null_key_row() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed(&adapter, "base", &[(Some(1), "a"), (None, "x")]).await;
    seed(&adapter, "branch", &[(Some(1), "a"), (None, "x"), (None, "x")]).await;

    let result = diff(&adapter, 1, 2, 1000).await;

    assert_eq!(result.rows_added, 1, "{result:?}");
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
}

/// Defect 3, duplicates: two identical null-key rows change to two other
/// identical rows. An XOR over the group would cancel to 0 on both
/// sides; the sorted hash multiset does not.
#[tokio::test]
async fn compare_bisection_detects_duplicate_null_key_rows_changed_together() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed(&adapter, "base", &[(Some(1), "a"), (None, "x"), (None, "x")]).await;
    seed(&adapter, "branch", &[(Some(1), "a"), (None, "y"), (None, "y")]).await;

    let result = diff(&adapter, 1, 2, 1000).await;

    assert_eq!(result.rows_added, 2, "{result:?}");
    assert_eq!(result.rows_removed, 2, "{result:?}");
}

/// Unchanged tables, including equal values at different keys and
/// duplicate null-key rows, report no diff and materialize no leaf.
#[tokio::test]
async fn compare_bisection_unchanged_tables_report_no_diff() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    let rows = [
        (Some(1), "a"),
        (Some(2), "a"),
        (Some(3), "b"),
        (None, "x"),
        (None, "x"),
    ];
    seed(&adapter, "base", &rows).await;
    seed(&adapter, "branch", &rows).await;

    let result = diff(&adapter, 1, 4, 1000).await;

    assert_eq!(result.rows_added, 0, "{result:?}");
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    assert!(result.samples.is_empty());
    assert_eq!(result.stats.leaves_materialized, 0);
    assert_eq!(result.stats.null_pk_rows_base, 2);
    assert_eq!(result.stats.null_pk_rows_branch, 2);
}

/// Unchanged large tables stop at the root: `K` chunks per side, no
/// recursion, no leaf. Insert order differs between the sides; the
/// XOR aggregate does not depend on row order.
#[tokio::test]
async fn compare_bisection_unchanged_large_tables_stop_at_root() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_range(&adapter, "base", 10_000, &[]).await;
    seed(&adapter, "branch", &[]).await;
    adapter
        .execute_statement(
            "INSERT INTO branch.t SELECT i, 'row_' || i FROM range(0, 10000) t(i) ORDER BY i DESC",
        )
        .await
        .unwrap();

    let result = diff(&adapter, 0, 10_000, 100).await;

    assert_eq!(result.rows_added + result.rows_removed + result.rows_changed, 0);
    assert_eq!(result.stats.depth_max, 0);
    assert_eq!(result.stats.leaves_materialized, 0);
    assert_eq!(
        result.stats.chunks_examined,
        u64::from(BisectionConfig::default().k) * 2
    );
}
