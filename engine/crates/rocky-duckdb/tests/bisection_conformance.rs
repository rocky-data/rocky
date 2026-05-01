//! Conformance suite for the checksum-bisection diff against an
//! in-process DuckDB.
//!
//! Pins three behaviors as verification gates:
//!
//! 1. **Determinism** (gate 1) — same data on both sides produces a
//!    byte-identical [`BisectionStats`] across runs.
//! 2. **False-negative rate** (gate 2) — a single planted change at row
//!    90,000 of a 100k-row table is found by every run.
//! 3. **Cost bound on no-op diff** (gate 3) — identical-data run examines
//!    only `K` chunks per side at the root and stops there.
//!
//! Plus two correctness cases the design doc names but doesn't gate
//! explicitly:
//!
//! - Empty branch surfaces every base row as `Removed`.
//! - Empty base surfaces every branch row as `Added`.

use rocky_core::compare::bisection::{
    BisectionConfig, BisectionTarget, LeafRowKind, bisection_diff,
};
use rocky_core::ir::TableRef;
use rocky_core::traits::{SplitStrategy, WarehouseAdapter};
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Seed a `(id INTEGER, name VARCHAR, value INTEGER)` table with N rows.
/// `id` runs `0..N`. `name` is `"row_<id>"`. `value` defaults to `id * 7`.
/// `overrides` lists per-row exceptions (`(id, value)`) that get UPDATEed
/// after the bulk INSERT — keeping the bulk path fast and the planted
/// changes explicit.
async fn seed_table(
    adapter: &DuckDbWarehouseAdapter,
    schema: &str,
    table: &str,
    n: u64,
    overrides: &[(u64, i64)],
) {
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {schema}.{table} \
             (id INTEGER, name VARCHAR, value INTEGER)"
        ))
        .await
        .unwrap();
    if n > 0 {
        // Bulk-insert via `range` is much faster than per-row INSERTs.
        adapter
            .execute_statement(&format!(
                "INSERT INTO {schema}.{table} \
                 SELECT i AS id, 'row_' || i AS name, i * 7 AS value \
                 FROM range(0, {n}) t(i)"
            ))
            .await
            .unwrap();
    }
    for (id, value) in overrides {
        adapter
            .execute_statement(&format!(
                "UPDATE {schema}.{table} SET value = {value} WHERE id = {id}"
            ))
            .await
            .unwrap();
    }
}

fn target<'a>(
    base: &'a TableRef,
    branch: &'a TableRef,
    pk_lo: i128,
    pk_hi: i128,
    value_columns: &'a [String],
) -> BisectionTarget<'a> {
    BisectionTarget {
        base,
        branch,
        pk_column: "id",
        value_columns,
        pk_lo,
        pk_hi,
    }
}

/// Verification gate 3 — no-op diff bottoms out at the root level
/// without recursion.
#[tokio::test]
async fn noop_diff_examines_root_chunks_only() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 10_000, &[]).await;
    seed_table(&adapter, "branch", "t", 10_000, &[]).await;

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(100),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 10_000, &value_columns),
        &config,
    )
    .await
    .expect("bisection_diff on identical data must succeed");

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    assert!(result.samples.is_empty());
    assert_eq!(result.stats.depth_max, 0, "no recursion on no-op diff");
    assert!(!result.stats.depth_capped);
    assert_eq!(result.stats.leaves_materialized, 0);
    // K root-level chunks examined per side, both sides — 2K total.
    assert_eq!(
        result.stats.chunks_examined,
        u64::from(config.k) * 2,
        "no-op diff should only checksum K root chunks per side"
    );
    assert_eq!(result.stats.split_strategy, SplitStrategy::IntRange);
}

/// Verification gate 2 — single planted change at row 90,000 of a 100k
/// table is found.
#[tokio::test]
async fn finds_planted_change_at_row_90000() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 100_000, &[]).await;
    seed_table(&adapter, "branch", "t", 100_000, &[(90_000, -1)]).await;

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(1000),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 100_000, &value_columns),
        &config,
    )
    .await
    .expect("bisection_diff against 100k-row fixture must succeed");

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 1, "exactly one row should differ");
    assert_eq!(result.samples.len(), 1);
    assert_eq!(result.samples[0].kind, LeafRowKind::Changed);
    assert_eq!(result.samples[0].pk, "90000");
    assert!(result.stats.depth_max >= 1, "single-row change requires recursion");
    assert!(!result.stats.depth_capped);
    assert_eq!(result.stats.leaves_materialized, 1, "exactly one leaf bottoms out");
    assert_eq!(result.stats.split_strategy, SplitStrategy::IntRange);
}

/// Verification gate 1 — same data on both sides produces a
/// byte-identical [`BisectionStats`] across runs. Re-runs the
/// `noop_diff_examines_root_chunks_only` setup twice and asserts
/// `BisectionStats` equality.
#[tokio::test]
async fn determinism_same_data_byte_identical_stats() {
    async fn run_once(
        seed: u64,
        config: &BisectionConfig,
    ) -> (
        u64,
        u64,
        u64,
        rocky_core::compare::bisection::BisectionStats,
    ) {
        let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
        seed_table(&adapter, "base", "t", seed, &[]).await;
        seed_table(&adapter, "branch", "t", seed, &[(90_000, -1)]).await;
        let base = TableRef {
            catalog: String::new(),
            schema: "base".into(),
            table: "t".into(),
        };
        let branch = TableRef {
            catalog: String::new(),
            schema: "branch".into(),
            table: "t".into(),
        };
        let value_columns = vec!["name".to_string(), "value".to_string()];
        let result = bisection_diff(
            &adapter,
            &adapter,
            &target(&base, &branch, 0, seed as i128, &value_columns),
            config,
        )
        .await
        .unwrap();
        (
            result.rows_added,
            result.rows_removed,
            result.rows_changed,
            result.stats,
        )
    }

    let config = BisectionConfig {
        min_chunk_rows: Some(1000),
        ..Default::default()
    };
    let a = run_once(100_000, &config).await;
    let b = run_once(100_000, &config).await;
    assert_eq!(a, b, "two runs on the same data must produce identical results");
}

/// Empty branch surfaces every base row as `Removed`. Bisection
/// recurses to the leaf threshold and materializes the dense range.
#[tokio::test]
async fn empty_branch_surfaces_base_as_removed() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 200, &[]).await;
    seed_table(&adapter, "branch", "t", 0, &[]).await;

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        // Every root chunk is below threshold → leaf-materialize each.
        min_chunk_rows: Some(500),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 200, &value_columns),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 200);
    assert_eq!(result.rows_changed, 0);
    assert_eq!(result.samples.len(), config.max_samples);
    for sample in &result.samples {
        assert_eq!(sample.kind, LeafRowKind::Removed);
    }
    // Every root chunk holds <500 rows, so every non-empty root chunk
    // bottoms out as a leaf. 200 rows over 32 chunks distribute across
    // all of them, so every chunk materializes.
    assert_eq!(
        result.stats.leaves_materialized,
        u64::from(config.k),
        "every root chunk should leaf-materialize because all are below threshold"
    );
}

/// Empty base surfaces every branch row as `Added`.
#[tokio::test]
async fn empty_base_surfaces_branch_as_added() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 0, &[]).await;
    seed_table(&adapter, "branch", "t", 200, &[]).await;

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(500),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 200, &value_columns),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(result.rows_added, 200);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    for sample in &result.samples {
        assert_eq!(sample.kind, LeafRowKind::Added);
    }
}

/// Regression: PK comparison at leaves must use numeric ordering, not
/// lexical. With base = `[0..200)` and branch = `[0..200) ∖ {100}`, a
/// merge-walk that string-compares would mis-classify rows whose
/// stringified PKs cross length boundaries (e.g., "99" > "100"
/// lexically). The numeric compare classifies row 100 as `Removed`
/// once and every other row as a match.
#[tokio::test]
async fn pk_comparison_handles_cross_length_numeric_ids() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 200, &[]).await;
    seed_table(&adapter, "branch", "t", 200, &[]).await;
    // Drop one row from branch — id=100 sits across the lexical
    // boundary between two-digit ("99") and three-digit ("100") IDs.
    adapter
        .execute_statement("DELETE FROM branch.t WHERE id = 100")
        .await
        .unwrap();

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        // Force the runner to materialize the chunk that crosses the
        // cardinality boundary at id=100.
        min_chunk_rows: Some(500),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 200, &value_columns),
        &config,
    )
    .await
    .unwrap();

    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 1, "exactly id=100 should be Removed");
    assert_eq!(result.rows_changed, 0);
    assert_eq!(result.samples.len(), 1);
    assert_eq!(result.samples[0].kind, LeafRowKind::Removed);
    assert_eq!(result.samples[0].pk, "100");
}

/// Null-PK rows are excluded from chunks but counted at the root and
/// surfaced on `BisectionStats`. A divergence in null-PK counts must
/// not silently disappear from the diff.
#[tokio::test]
async fn null_pk_rows_surfaced_on_stats() {
    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    seed_table(&adapter, "base", "t", 100, &[]).await;
    seed_table(&adapter, "branch", "t", 100, &[]).await;
    // Insert null-PK rows on each side, with branch carrying more.
    adapter
        .execute_statement("INSERT INTO base.t VALUES (NULL, 'null_a', 1), (NULL, 'null_b', 2)")
        .await
        .unwrap();
    adapter
        .execute_statement(
            "INSERT INTO branch.t VALUES \
             (NULL, 'null_a', 1), (NULL, 'null_b', 2), \
             (NULL, 'null_c', 3), (NULL, 'null_d', 4), (NULL, 'null_e', 5)",
        )
        .await
        .unwrap();

    let base = TableRef {
        catalog: String::new(),
        schema: "base".into(),
        table: "t".into(),
    };
    let branch = TableRef {
        catalog: String::new(),
        schema: "branch".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string(), "value".to_string()];
    let config = BisectionConfig {
        min_chunk_rows: Some(500),
        ..Default::default()
    };

    let result = bisection_diff(
        &adapter,
        &adapter,
        &target(&base, &branch, 0, 100, &value_columns),
        &config,
    )
    .await
    .unwrap();

    // Non-null rows match; null rows don't appear in the diff totals
    // but are reported on the stats.
    assert_eq!(result.rows_added, 0);
    assert_eq!(result.rows_removed, 0);
    assert_eq!(result.rows_changed, 0);
    assert_eq!(result.stats.null_pk_rows_base, 2);
    assert_eq!(result.stats.null_pk_rows_branch, 5);
}

/// Smoke test on the `checksum_chunks` adapter call shape: a 32-row
/// table chunked into K=4 ranges returns 4 entries with row_count=8 each.
#[tokio::test]
async fn checksum_chunks_smoke_uniform_ranges() {
    use rocky_core::traits::PkRange;

    let adapter = DuckDbWarehouseAdapter::in_memory().unwrap();
    adapter
        .execute_statement("CREATE SCHEMA IF NOT EXISTS smoke")
        .await
        .unwrap();
    adapter
        .execute_statement(
            "CREATE OR REPLACE TABLE smoke.t (id INTEGER, name VARCHAR)",
        )
        .await
        .unwrap();
    adapter
        .execute_statement(
            "INSERT INTO smoke.t SELECT i, 'r' || i FROM range(0, 32) t(i)",
        )
        .await
        .unwrap();

    let table = TableRef {
        catalog: String::new(),
        schema: "smoke".into(),
        table: "t".into(),
    };
    let value_columns = vec!["name".to_string()];
    let ranges = vec![
        PkRange::IntRange { lo: 0, hi: 8 },
        PkRange::IntRange { lo: 8, hi: 16 },
        PkRange::IntRange { lo: 16, hi: 24 },
        PkRange::IntRange { lo: 24, hi: 32 },
    ];

    let sums = adapter
        .checksum_chunks(&table, "id", &value_columns, &ranges)
        .await
        .expect("checksum_chunks should succeed on DuckDB");
    assert_eq!(sums.len(), 4);
    for chunk in &sums {
        assert_eq!(chunk.row_count, 8, "each chunk should hold 8 rows");
    }
    // Determinism check: two consecutive calls produce identical
    // checksum bytes (DuckDB's `hash` is deterministic; bit_xor over
    // HUGEINT preserves the bytes).
    let sums2 = adapter
        .checksum_chunks(&table, "id", &value_columns, &ranges)
        .await
        .unwrap();
    for (a, b) in sums.iter().zip(sums2.iter()) {
        assert_eq!(a.checksum, b.checksum);
    }
}
