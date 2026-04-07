//! Cross-table partition dedup analysis — the pure computation behind
//! `rocky compact --measure-dedup` (Layer 0 storage experiment).
//!
//! This module is **intentionally I/O-free**: it takes a slice of
//! [`PartitionChecksum`] vectors (one per table), indexes them by
//! content fingerprint, and returns a [`DedupStats`] describing how
//! many partitions are duplicated across tables and which table pairs
//! share the most.
//!
//! The CLI (`rocky-cli::commands::compact::run_measure_dedup`) owns the
//! warehouse roundtrips that produce the `PartitionChecksum` input and
//! the conversion from the internal [`DedupStats`] to the serializable
//! `CompactDedupOutput`. Keeping the computation pure makes it trivially
//! unit-testable and lets the same logic be reused when / if Layer 1
//! ships a byte-level variant.
//!
//! ## Granularity caveat
//!
//! The `dedup_ratio` and `estimated_savings_pct` this module returns are
//! **partition-level lower bounds** on true byte-level dedup. Two
//! partitions that share 99% of their row groups but differ on a single
//! row will appear fully distinct here. See
//! `plans/rocky-storage-layer-0.md` §6 for what this implies for the
//! decision gate.

use std::collections::HashMap;

use crate::incremental::PartitionChecksum;

/// Cap on the number of table pairs returned in
/// [`DedupStats::top_dedup_pairs`]. Chosen to keep reports scannable;
/// the full breakdown lives in `per_table`.
pub const TOP_DEDUP_PAIRS_LIMIT: usize = 10;

/// Result of the cross-table partition dedup analysis.
///
/// Internal to `rocky-core`; the CLI converts it to the serializable
/// `CompactDedupOutput` at the boundary between crates.
#[derive(Debug, Clone, PartialEq)]
pub struct DedupStats {
    /// Total number of partition checksums passed in (sum of
    /// `per_table[i].1.len()`).
    pub total_partitions: usize,
    /// Number of *distinct* partition fingerprints observed. Equal to
    /// the number of buckets in the internal `(checksum, row_count)`
    /// index.
    pub unique_partitions: usize,
    /// `total_partitions - unique_partitions`. Partitions that would
    /// collapse away under perfect partition-level dedup.
    pub duplicate_partitions: usize,
    /// Sum of `row_count` across every input partition.
    pub total_rows: u64,
    /// `0.0..=1.0`. Partition-count ratio:
    /// `duplicate_partitions / total_partitions`.
    ///
    /// **Lower bound** on true byte-level dedup.
    pub dedup_ratio: f64,
    /// `0.0..=100.0`. Row-weighted savings estimate — for each bucket
    /// with N holders, counts `(N - 1) * row_count` as collapsible.
    /// More meaningful than `dedup_ratio` when partition sizes vary.
    pub estimated_savings_pct: f64,
    /// Top table pairs by shared-partition count, capped at
    /// [`TOP_DEDUP_PAIRS_LIMIT`]. Sorted descending by
    /// `shared_partitions`, tie-broken alphabetically by `table_a`.
    pub top_dedup_pairs: Vec<DedupPairStat>,
    /// Per-table breakdown, sorted descending by
    /// `partitions_shared_with_others`.
    pub per_table: Vec<TableDedupStat>,
}

/// A pair of tables that share one or more duplicate partitions.
#[derive(Debug, Clone, PartialEq)]
pub struct DedupPairStat {
    pub table_a: String,
    pub table_b: String,
    pub shared_partitions: usize,
    pub shared_rows: u64,
}

/// Per-table contribution to the overall duplicate set.
#[derive(Debug, Clone, PartialEq)]
pub struct TableDedupStat {
    pub table: String,
    pub partitions: usize,
    /// Number of this table's partitions whose content also appears in
    /// at least one *other* table.
    pub partitions_shared_with_others: usize,
    /// `0.0..=100.0`. `partitions_shared_with_others / partitions`
    /// as a percentage.
    pub contribution_pct: f64,
}

/// Compute cross-table partition dedup statistics from a set of
/// per-table partition checksums.
///
/// Pure function — no I/O, no warehouse calls. The input layout is
/// deliberately `&[(String, Vec<PartitionChecksum>)]` rather than a
/// `HashMap` so the caller controls ordering and the function can stay
/// trivially fuzzable.
///
/// The index key is `(checksum, row_count)`, not just `checksum`. Two
/// partitions collide only if their warehouse `SUM(HASH(...))` AND row
/// counts match, which at realistic project scales (~10⁴ partitions)
/// keeps false-positive collision probability astronomically low
/// (~10⁻¹⁴).
pub fn compute_dedup_stats(per_table: &[(String, Vec<PartitionChecksum>)]) -> DedupStats {
    // 1. Build the fingerprint index and gather totals.
    //    Key: (checksum, row_count). Value: every (table, partition_key)
    //    that produced that fingerprint.
    let mut index: HashMap<(u64, u64), Vec<(String, String)>> = HashMap::new();
    let mut total_partitions = 0usize;
    let mut total_rows: u64 = 0;

    for (table, parts) in per_table {
        for p in parts {
            total_partitions += 1;
            total_rows = total_rows.saturating_add(p.row_count);
            index
                .entry((p.checksum, p.row_count))
                .or_default()
                .push((table.clone(), p.partition_key.clone()));
        }
    }

    // 2. Unique / duplicate counts.
    //    A bucket of size N contributes 1 "unique" (the canonical
    //    content) and N-1 "duplicates". So:
    //      unique_partitions    = number of buckets = index.len()
    //      duplicate_partitions = total_partitions - unique_partitions
    let unique_partitions = index.len();
    let duplicate_partitions = total_partitions.saturating_sub(unique_partitions);

    let dedup_ratio = if total_partitions == 0 {
        0.0
    } else {
        duplicate_partitions as f64 / total_partitions as f64
    };

    // 3. Row-weighted savings — for each multi-holder bucket, count the
    //    rows that could be collapsed.
    let mut duplicate_rows: u64 = 0;
    for ((_checksum, row_count), holders) in &index {
        if holders.len() > 1 {
            let extra_copies = holders.len() as u64 - 1;
            duplicate_rows = duplicate_rows.saturating_add(extra_copies.saturating_mul(*row_count));
        }
    }
    let estimated_savings_pct = if total_rows == 0 {
        0.0
    } else {
        (duplicate_rows as f64 / total_rows as f64) * 100.0
    };

    // 4. Pair statistics. For every bucket whose holders span ≥2
    //    distinct tables, emit a count for each unordered pair. A
    //    bucket whose holders are all from a single table (same content
    //    appears twice in one table — rare but possible for unpartitioned
    //    or weirdly-keyed inputs) contributes zero pairs, correctly.
    let mut pair_counts: HashMap<(String, String), (usize, u64)> = HashMap::new();
    for ((_checksum, row_count), holders) in &index {
        if holders.len() < 2 {
            continue;
        }
        // Deduplicate the holder list down to distinct tables so the
        // pair emission below counts bucket-level sharing, not
        // entry-level.
        let mut tables: Vec<&String> = holders.iter().map(|(t, _)| t).collect();
        tables.sort();
        tables.dedup();
        for i in 0..tables.len() {
            for j in (i + 1)..tables.len() {
                let (a, b) = (tables[i], tables[j]);
                let key = if a <= b {
                    (a.clone(), b.clone())
                } else {
                    (b.clone(), a.clone())
                };
                let entry = pair_counts.entry(key).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = entry.1.saturating_add(*row_count);
            }
        }
    }
    let mut pair_vec: Vec<DedupPairStat> = pair_counts
        .into_iter()
        .map(|((a, b), (shared, rows))| DedupPairStat {
            table_a: a,
            table_b: b,
            shared_partitions: shared,
            shared_rows: rows,
        })
        .collect();
    pair_vec.sort_by(|x, y| {
        y.shared_partitions
            .cmp(&x.shared_partitions)
            .then_with(|| x.table_a.cmp(&y.table_a))
            .then_with(|| x.table_b.cmp(&y.table_b))
    });
    let top_dedup_pairs = pair_vec.into_iter().take(TOP_DEDUP_PAIRS_LIMIT).collect();

    // 5. Per-table contribution.
    let mut per_table_stats: Vec<TableDedupStat> = per_table
        .iter()
        .map(|(table, parts)| {
            let partitions = parts.len();
            let shared = parts
                .iter()
                .filter(|p| {
                    index
                        .get(&(p.checksum, p.row_count))
                        .is_some_and(|holders| holders.iter().any(|(t, _)| t != table))
                })
                .count();
            let contribution_pct = if partitions == 0 {
                0.0
            } else {
                (shared as f64 / partitions as f64) * 100.0
            };
            TableDedupStat {
                table: table.clone(),
                partitions,
                partitions_shared_with_others: shared,
                contribution_pct,
            }
        })
        .collect();
    per_table_stats.sort_by(|a, b| {
        b.partitions_shared_with_others
            .cmp(&a.partitions_shared_with_others)
            .then_with(|| a.table.cmp(&b.table))
    });

    DedupStats {
        total_partitions,
        unique_partitions,
        duplicate_partitions,
        total_rows,
        dedup_ratio,
        estimated_savings_pct,
        top_dedup_pairs,
        per_table: per_table_stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// Build a `PartitionChecksum` with an arbitrary `computed_at` so
    /// test cases stay readable. `computed_at` isn't used by the dedup
    /// analysis — only the partition_key/checksum/row_count triple
    /// matters.
    fn pc(key: &str, checksum: u64, row_count: u64) -> PartitionChecksum {
        PartitionChecksum {
            partition_key: key.into(),
            checksum,
            row_count,
            computed_at: Utc::now(),
        }
    }

    #[test]
    fn empty_input_is_all_zeros() {
        let stats = compute_dedup_stats(&[]);
        assert_eq!(stats.total_partitions, 0);
        assert_eq!(stats.unique_partitions, 0);
        assert_eq!(stats.duplicate_partitions, 0);
        assert_eq!(stats.total_rows, 0);
        assert_eq!(stats.dedup_ratio, 0.0);
        assert_eq!(stats.estimated_savings_pct, 0.0);
        assert!(stats.top_dedup_pairs.is_empty());
        assert!(stats.per_table.is_empty());
    }

    #[test]
    fn single_table_with_all_unique_partitions_has_no_duplicates() {
        let input = vec![(
            "db.schema.t".into(),
            vec![
                pc("2026-01", 100, 10),
                pc("2026-02", 200, 20),
                pc("2026-03", 300, 30),
            ],
        )];
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 3);
        assert_eq!(stats.unique_partitions, 3);
        assert_eq!(stats.duplicate_partitions, 0);
        assert_eq!(stats.total_rows, 60);
        assert_eq!(stats.dedup_ratio, 0.0);
        assert_eq!(stats.estimated_savings_pct, 0.0);
        assert!(stats.top_dedup_pairs.is_empty());
        assert_eq!(stats.per_table.len(), 1);
        assert_eq!(stats.per_table[0].partitions_shared_with_others, 0);
        assert_eq!(stats.per_table[0].contribution_pct, 0.0);
    }

    #[test]
    fn two_tables_sharing_every_partition_show_full_duplication() {
        // Bronze and silver have identical content on three partitions.
        let input = vec![
            (
                "bronze.orders".into(),
                vec![
                    pc("2026-01", 100, 10),
                    pc("2026-02", 200, 20),
                    pc("2026-03", 300, 30),
                ],
            ),
            (
                "silver.orders".into(),
                vec![
                    pc("2026-01", 100, 10),
                    pc("2026-02", 200, 20),
                    pc("2026-03", 300, 30),
                ],
            ),
        ];
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 6);
        assert_eq!(stats.unique_partitions, 3);
        assert_eq!(stats.duplicate_partitions, 3);
        assert_eq!(stats.total_rows, 120);
        assert_eq!(stats.dedup_ratio, 0.5);
        // Row-weighted: 3 * (10+20+30) = 180 collapsible rows out of
        // 240? No wait — total_rows is 120 (sum across both tables) and
        // duplicate_rows is the "extra copies" only. With bucket sizes
        // of 2 each, extra_copies = 1 per bucket, so duplicate_rows =
        // 10 + 20 + 30 = 60, which is 60/120 = 50%.
        assert_eq!(stats.estimated_savings_pct, 50.0);
        assert_eq!(stats.top_dedup_pairs.len(), 1);
        let pair = &stats.top_dedup_pairs[0];
        assert_eq!(pair.table_a, "bronze.orders");
        assert_eq!(pair.table_b, "silver.orders");
        assert_eq!(pair.shared_partitions, 3);
        assert_eq!(pair.shared_rows, 60);
        // Both tables have 100% contribution.
        for t in &stats.per_table {
            assert_eq!(t.partitions_shared_with_others, 3);
            assert_eq!(t.contribution_pct, 100.0);
        }
    }

    #[test]
    fn mixed_unique_and_shared_partitions_report_correctly() {
        let input = vec![
            (
                "bronze.orders".into(),
                vec![
                    pc("2026-01", 100, 10),
                    pc("2026-02", 200, 20),
                    pc("2026-03", 999, 30),
                ],
            ),
            (
                "silver.orders".into(),
                vec![
                    pc("2026-01", 100, 10),
                    pc("2026-02", 200, 20),
                    pc("2026-04", 888, 40),
                ],
            ),
        ];
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 6);
        // Four distinct fingerprints: (100,10), (200,20), (999,30), (888,40)
        assert_eq!(stats.unique_partitions, 4);
        assert_eq!(stats.duplicate_partitions, 2);
        assert!((stats.dedup_ratio - (2.0 / 6.0)).abs() < 1e-9);
        // One pair, two shared partitions.
        assert_eq!(stats.top_dedup_pairs.len(), 1);
        assert_eq!(stats.top_dedup_pairs[0].shared_partitions, 2);
        assert_eq!(stats.top_dedup_pairs[0].shared_rows, 30);
    }

    #[test]
    fn asymmetric_one_big_table_and_several_small_ones() {
        // One big table with 10 partitions, and four small tables that each
        // share one partition with the big one. Exercises the sort-and-truncate
        // path even though we have fewer than 10 pairs.
        let big: Vec<PartitionChecksum> = (0..10)
            .map(|i| pc(&format!("2026-{:02}", i + 1), 1000 + i, 100))
            .collect();
        let input = vec![
            ("archive.events".into(), big),
            ("bronze.events".into(), vec![pc("2026-01", 1000, 100)]),
            ("curated.events".into(), vec![pc("2026-02", 1001, 100)]),
            ("mart.events".into(), vec![pc("2026-03", 1002, 100)]),
            ("staging.events".into(), vec![pc("2026-04", 1003, 100)]),
        ];
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 14);
        assert_eq!(stats.unique_partitions, 10);
        assert_eq!(stats.duplicate_partitions, 4);
        // Four pairs, all (archive.events, <other>) — each sharing 1.
        assert_eq!(stats.top_dedup_pairs.len(), 4);
        for pair in &stats.top_dedup_pairs {
            assert_eq!(pair.table_a, "archive.events");
            assert_eq!(pair.shared_partitions, 1);
            assert_eq!(pair.shared_rows, 100);
        }
        // archive.events should have 4 shared partitions (the other four
        // tables only have 1 partition each, all shared — tied at 1 with
        // archive following alphabetically after, wait — let me re-check.
        // archive.events has 4 shared, tables are sorted by
        // partitions_shared_with_others desc, so archive is first.
        assert_eq!(stats.per_table[0].table, "archive.events");
        assert_eq!(stats.per_table[0].partitions_shared_with_others, 4);
    }

    #[test]
    fn top_n_truncation_returns_only_ten_pairs() {
        // Generate 12 tables, all sharing every partition pairwise via a
        // single identical-content partition. That creates C(12, 2) = 66
        // pairs, so the truncation path definitely fires.
        let mut input = Vec::new();
        for i in 0..12 {
            input.push((format!("t{i:02}"), vec![pc("k", 42, 1)]));
        }
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 12);
        assert_eq!(stats.unique_partitions, 1);
        assert_eq!(stats.duplicate_partitions, 11);
        assert_eq!(stats.top_dedup_pairs.len(), TOP_DEDUP_PAIRS_LIMIT);
        // All pairs share the same 1 partition, so the truncation is
        // deterministic via the alphabetical tie-break on (table_a,
        // table_b). The first returned pair should start with "t00".
        assert_eq!(stats.top_dedup_pairs[0].table_a, "t00");
    }

    #[test]
    fn intra_table_duplicates_are_not_counted_as_cross_table_sharing() {
        // Two partitions with identical content inside ONE table should
        // show up as a duplicate (they can still be deduplicated by a
        // content-addressed store) but NOT as a cross-table pair.
        let input = vec![(
            "bronze.orders".into(),
            vec![pc("2026-01", 100, 10), pc("2026-02", 100, 10)],
        )];
        let stats = compute_dedup_stats(&input);
        assert_eq!(stats.total_partitions, 2);
        assert_eq!(stats.unique_partitions, 1);
        assert_eq!(stats.duplicate_partitions, 1);
        assert!(stats.top_dedup_pairs.is_empty());
        // Per-table contribution is zero — the sharing is within one
        // table, not across tables.
        assert_eq!(stats.per_table[0].partitions_shared_with_others, 0);
    }
}
