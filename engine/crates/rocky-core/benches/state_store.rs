//! Benchmarks for the redb state-store write paths.
//!
//! Two head-to-head comparisons, both against a fresh temp redb so the
//! numbers reflect real `begin_write` / `commit` (fsync) cost:
//!
//! - `record_table_progress` — the shipped O(1) blind-insert path vs an inline
//!   reproduction of the old O(N²) read-modify-write of one growing
//!   `RunProgress` blob. Swept over N = 100 / 500 / 1000 completed tables; the
//!   old path's cost grows quadratically, the new one linearly.
//! - `schema_cache_write` — the per-entry `write_schema_cache_entry` loop (N
//!   commits = N fsyncs) vs the batched `batch_write_schema_cache_entries`
//!   (one commit = one fsync). Swept over N = 100 / 500 / 1000 entries.
//!
//! Run with: `cargo bench --bench state_store -p rocky-core`
//!
//! Credential-free — everything runs against a `tempfile::TempDir`.

use std::hint::black_box;
use std::time::Duration;

use chrono::Utc;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use redb::{Database, TableDefinition};
use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn};
use rocky_core::state::{RunProgress, StateStore, TableProgress, TableStatus};
use tempfile::TempDir;

const RUN_PROGRESS: TableDefinition<&str, &[u8]> = TableDefinition::new("run_progress");
const SIZES: [usize; 3] = [100, 500, 1000];

fn temp_store() -> (StateStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("state.redb");
    let store = StateStore::open(&path).unwrap();
    (store, dir)
}

fn sample_progress(index: usize) -> TableProgress {
    TableProgress {
        index,
        table_key: format!("cat.sch.tbl_{index}"),
        asset_key: vec!["fivetran".into(), "acme".into(), format!("tbl_{index}")],
        status: TableStatus::Success,
        error: None,
        duration_ms: 120,
        completed_at: Utc::now(),
    }
}

/// Reproduces the pre-v8 O(N²) write path: read the whole `RunProgress` blob,
/// deserialize every prior entry, push one, re-serialize the lot, commit.
/// Lives only in the bench so we can measure the improvement head-to-head.
fn naive_record_table_progress(db: &Database, run_id: &str, progress: &TableProgress) {
    let mut run_progress = {
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(RUN_PROGRESS).unwrap();
        match table.get(run_id).unwrap() {
            Some(value) => serde_json::from_slice(value.value()).unwrap(),
            None => RunProgress {
                run_id: run_id.to_string(),
                started_at: Utc::now(),
                total_tables: 0,
                tables: Vec::new(),
            },
        }
    };
    run_progress.tables.push(progress.clone());
    let bytes = serde_json::to_vec(&run_progress).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(RUN_PROGRESS).unwrap();
        table.insert(run_id, bytes.as_slice()).unwrap();
    }
    txn.commit().unwrap();
}

fn sample_entry(index: usize) -> SchemaCacheEntry {
    SchemaCacheEntry {
        columns: vec![
            StoredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            },
            StoredColumn {
                name: format!("col_{index}"),
                data_type: "VARCHAR".into(),
                nullable: true,
            },
        ],
        cached_at: Utc::now(),
    }
}

fn bench_record_table_progress(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_table_progress");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for n in SIZES {
        // New path: O(1) blind insert per table -> O(N) for the run.
        group.bench_with_input(BenchmarkId::new("blind_insert", n), &n, |b, &n| {
            b.iter_batched(
                temp_store,
                |(store, _dir)| {
                    store.init_run_progress("run-001", n).unwrap();
                    for i in 0..n {
                        store
                            .record_table_progress("run-001", &sample_progress(i))
                            .unwrap();
                    }
                    black_box(store.get_run_progress("run-001").unwrap());
                },
                criterion::BatchSize::PerIteration,
            );
        });

        // Old path: read-modify-write the whole blob each call -> O(N^2).
        group.bench_with_input(BenchmarkId::new("naive_rmw", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = TempDir::new().unwrap();
                    let path = dir.path().join("state.redb");
                    let db = Database::create(&path).unwrap();
                    {
                        let txn = db.begin_write().unwrap();
                        {
                            let _t = txn.open_table(RUN_PROGRESS).unwrap();
                        }
                        txn.commit().unwrap();
                    }
                    (db, dir)
                },
                |(db, _dir)| {
                    for i in 0..n {
                        naive_record_table_progress(&db, "run-001", &sample_progress(i));
                    }
                    black_box(&db);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn bench_schema_cache_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_cache_write");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for n in SIZES {
        // Old path: one commit (fsync) per entry.
        group.bench_with_input(BenchmarkId::new("per_entry_loop", n), &n, |b, &n| {
            b.iter_batched(
                temp_store,
                |(store, _dir)| {
                    for i in 0..n {
                        let key = format!("cat.sch.tbl_{i}");
                        store
                            .write_schema_cache_entry(&key, &sample_entry(i))
                            .unwrap();
                    }
                    black_box(&store);
                },
                criterion::BatchSize::PerIteration,
            );
        });

        // New path: one commit (fsync) for all N entries.
        group.bench_with_input(BenchmarkId::new("batch_write", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let (store, dir) = temp_store();
                    let keys: Vec<String> = (0..n).map(|i| format!("cat.sch.tbl_{i}")).collect();
                    let entries: Vec<SchemaCacheEntry> = (0..n).map(sample_entry).collect();
                    (store, dir, keys, entries)
                },
                |(store, _dir, keys, entries)| {
                    let batch: Vec<(&str, &SchemaCacheEntry)> = keys
                        .iter()
                        .zip(entries.iter())
                        .map(|(k, e)| (k.as_str(), e))
                        .collect();
                    store.batch_write_schema_cache_entries(&batch).unwrap();
                    black_box(&store);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_record_table_progress,
    bench_schema_cache_write
);
criterion_main!(benches);
