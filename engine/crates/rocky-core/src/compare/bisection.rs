//! Checksum-bisection exhaustive row-level diff.
//!
//! Datafold-style chunk recursion: split the primary-key range into `K`
//! chunks, checksum each chunk on both sides via
//! [`crate::traits::WarehouseAdapter::checksum_chunks`], recurse into
//! mismatched chunks until a leaf threshold, then materialize-and-diff the
//! leaves row-by-row.
//!
//! Two properties make this a step-change over sampling:
//!
//! 1. **Bounded scan cost.** A no-op diff reads `K` chunk checksums per
//!    side. Uniform mismatch bottoms out at `O(K · log_K(N))` chunks
//!    examined. Compare to `O(N)` for full-table compare and `O(1000)` —
//!    with an unbounded false-negative tail — for sampling.
//! 2. **Exhaustive coverage.** Every row hashes into exactly one chunk; if
//!    any row differs between sides, the chunk it lives in is guaranteed
//!    to mismatch and the recursion is guaranteed to find it. No coverage
//!    hedge.
//!
//! **Current scope.** This implementation supports a single-column
//! integer / numeric primary key (`SplitStrategy::IntRange`). Composite
//! and UUID / hash-bucket primary keys return an error; wiring those up
//! is a follow-up. The DuckDB conformance test in
//! `rocky-duckdb/tests/bisection_conformance.rs` exercises a 100k-row
//! seeded table with a planted change at row 90,000 and snapshots the
//! [`BisectionStats`] for determinism.

use serde::{Deserialize, Serialize};

use crate::ir::TableRef;
use crate::traits::{
    AdapterError, AdapterResult, ChunkChecksum, PkRange, SplitStrategy, WarehouseAdapter,
};

/// Default chunk fanout. `K = 32` matches Datafold's data-diff: at higher
/// fanout the per-level checksum query has too many predicates and the
/// warehouse chokes; at lower fanout the recursion does too many
/// roundtrips.
pub const DEFAULT_K: u32 = 32;

/// Maximum recursion depth. `K^MAX_DEPTH = 32^8 ≈ 1e12`, which covers
/// any plausible production table. The cap prevents runaway on
/// adversarial PK skew.
pub const DEFAULT_MAX_DEPTH: u32 = 8;

/// Default cap on the number of representative changed rows surfaced
/// from leaves. Five matches the Phase 2 sampled-diff render budget.
pub const DEFAULT_MAX_SAMPLES: usize = 5;

/// Knobs for the bisection runner. Defaults: `K=32` (matches Datafold's
/// data-diff sweet spot), `MAX_DEPTH=8` (covers `K^MAX_DEPTH ≈ 1e12`
/// rows), `MIN_CHUNK_ROWS=1000` (break-even between an extra checksum
/// roundtrip and a small leaf materialization).
///
/// Most callers should construct via `BisectionConfig::default()` and
/// override only specific knobs. The [`min_chunk_rows`] field is what
/// adapters tune via
/// [`crate::traits::WarehouseAdapter::recommended_leaf_size`]; the
/// runner reads the adapter recommendation when no override is set.
#[derive(Debug, Clone)]
pub struct BisectionConfig {
    /// Chunk fanout per recursion level.
    pub k: u32,
    /// Recursion depth bound; on hit the runner reports
    /// [`BisectionStats::depth_capped`] = `true` and falls back to leaf
    /// materialization on the dense range.
    pub max_depth: u32,
    /// Below this row count, the runner stops splitting and materializes
    /// the chunk for row-by-row diff. `None` defers to
    /// [`crate::traits::WarehouseAdapter::recommended_leaf_size`].
    pub min_chunk_rows: Option<u64>,
    /// Number of representative changed rows to surface in
    /// [`BisectionDiffResult::samples`]. Surplus mismatches are counted
    /// but not materialized into the result.
    pub max_samples: usize,
}

impl Default for BisectionConfig {
    fn default() -> Self {
        Self {
            k: DEFAULT_K,
            max_depth: DEFAULT_MAX_DEPTH,
            min_chunk_rows: None,
            max_samples: DEFAULT_MAX_SAMPLES,
        }
    }
}

/// Inputs identifying which two tables to diff. The `pk_column` and
/// `value_columns` apply to both sides; if the schemas have drifted, the
/// caller is expected to detect that via the structural diff layer
/// before invoking the bisection runner.
#[derive(Debug, Clone)]
pub struct BisectionTarget<'a> {
    /// Base-side table (the "main branch" / "previous state").
    pub base: &'a TableRef,
    /// Branch-side table (the "this PR" / "new state").
    pub branch: &'a TableRef,
    /// Primary-key column name. Sub-step 1 supports a single integer /
    /// numeric column.
    pub pk_column: &'a str,
    /// Columns to hash for chunk checksums and to compare row-by-row at
    /// leaves. Typically every non-PK column the caller cares about; the
    /// PK column itself is excluded so two rows with the same PK but
    /// different content surface as `Changed`.
    pub value_columns: &'a [String],
    /// `[lo, hi)` bounds of the primary-key range to search. Callers
    /// derive these from `MIN(pk)` / `MAX(pk)` on either side, or from
    /// declared partition bounds. Sub-step 1 takes them as caller input
    /// rather than running the bound-discovery query itself.
    pub pk_lo: i128,
    pub pk_hi: i128,
}

/// Aggregate row-level result from [`bisection_diff`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectionDiffResult {
    /// Rows present on branch but not on base.
    pub rows_added: u64,
    /// Rows present on base but not on branch.
    pub rows_removed: u64,
    /// Rows present on both sides with different value-column content.
    pub rows_changed: u64,
    /// Up to [`BisectionConfig::max_samples`] representative changed
    /// rows surfaced from the leaves. Stable order: `(kind, pk)`.
    pub samples: Vec<LeafRowSample>,
    /// Trace of the recursion so callers (snapshot tests, debug logs,
    /// future PR-comment renderers) can pin the algorithm's behavior.
    pub stats: BisectionStats,
}

/// One representative changed row surfaced from a bisection leaf.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeafRowSample {
    /// Why the row was surfaced.
    pub kind: LeafRowKind,
    /// Primary-key value as a string — adapter-quoted for stability
    /// across snapshot tests.
    pub pk: String,
}

/// Why a row appears in [`BisectionDiffResult::samples`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LeafRowKind {
    /// Branch only — appears in `rows_added`.
    Added,
    /// Base only — appears in `rows_removed`.
    Removed,
    /// Same PK, different value-column content — appears in
    /// `rows_changed`.
    Changed,
}

/// Trace of how the bisection runner traversed the chunk lattice.
/// Determinism contract: same input on both sides produces a
/// byte-identical [`BisectionStats`] across runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BisectionStats {
    /// Total non-empty chunk-checksum entries returned by the adapter,
    /// summed across both sides and all recursion levels. Each level
    /// asks for `K` chunks per side (a fixed cost the adapter charges as
    /// one batched query); this counter measures the *result-set* shape,
    /// not the number of underlying SQL queries.
    pub chunks_examined: u64,
    /// Number of leaf chunks materialized for row-by-row diff.
    pub leaves_materialized: u64,
    /// Maximum recursion depth reached.
    pub depth_max: u32,
    /// `true` if the runner hit [`BisectionConfig::max_depth`] before
    /// any chunk fell below the leaf threshold. On hit the runner
    /// materialized the dense range as a leaf — so the diff is still
    /// exhaustive over that range; just slower.
    pub depth_capped: bool,
    /// How the runner split the primary-key space. Currently always
    /// reports `IntRange`; `Composite` / `HashBucket` / `FirstColumn`
    /// land in follow-up changes.
    pub split_strategy: SplitStrategy,
    /// Rows on the base side whose primary-key column is NULL. Excluded
    /// from chunk membership but counted at the root so a divergence in
    /// null-PK row counts surfaces instead of silently dropping rows.
    pub null_pk_rows_base: u64,
    /// Rows on the branch side whose primary-key column is NULL.
    pub null_pk_rows_branch: u64,
}

/// Run a checksum-bisection diff between two tables on the same logical
/// schema. The base and branch adapters MAY be the same warehouse
/// pointing at two schemas, or two different warehouses (e.g., shadow
/// mode) — the algorithm only touches them through
/// [`WarehouseAdapter::checksum_chunks`] and
/// [`WarehouseAdapter::execute_query`].
///
/// **Sub-step 1 contract:**
/// - `target.pk_column` must be a single integer / numeric column.
/// - `target.pk_lo .. target.pk_hi` must be a non-empty `[lo, hi)` range.
///   Callers typically derive this from `MIN(pk)`/`MAX(pk)+1` on the
///   side with the wider bound.
/// - Returns an error rather than degrading silently if the adapter's
///   `checksum_chunks` errors, or if the leaf step's `execute_query`
///   fails. Schema drift detection is the caller's responsibility.
pub async fn bisection_diff(
    base: &dyn WarehouseAdapter,
    branch: &dyn WarehouseAdapter,
    target: &BisectionTarget<'_>,
    config: &BisectionConfig,
) -> AdapterResult<BisectionDiffResult> {
    if target.pk_hi <= target.pk_lo {
        return Err(AdapterError::msg(format!(
            "bisection_diff requires pk_lo < pk_hi (got [{}, {}))",
            target.pk_lo, target.pk_hi
        )));
    }
    if config.k < 2 {
        return Err(AdapterError::msg(
            "bisection_diff requires k >= 2",
        ));
    }

    // The adapter recommendation defines the leaf threshold when the
    // caller didn't override.  Both adapters get a vote; we take the max
    // so we never split below either's break-even.
    let leaf_threshold = config.min_chunk_rows.unwrap_or_else(|| {
        std::cmp::max(base.recommended_leaf_size(), branch.recommended_leaf_size())
    });

    // Count null-PK rows once per side at the root. Null-PK rows
    // never land in any chunk (the chunking SQL filters them out) so a
    // null-only divergence would otherwise be silent. Surfaced on
    // BisectionStats; if base and branch counts diverge, the caller
    // should treat that as a row-count mismatch at the table level.
    let null_pk_rows_base = count_null_pk_rows(base, target.base, target.pk_column).await?;
    let null_pk_rows_branch =
        count_null_pk_rows(branch, target.branch, target.pk_column).await?;

    let mut state = TraversalState {
        config,
        leaf_threshold,
        chunks_examined: 0,
        leaves_materialized: 0,
        depth_max: 0,
        depth_capped: false,
        rows_added: 0,
        rows_removed: 0,
        rows_changed: 0,
        samples: Vec::new(),
    };

    // Iterative depth-first traversal.  A stack avoids `async fn` self-
    // recursion (which would force every implementor to either pull in
    // the `async_recursion` crate or hand-box every future) without
    // changing the algorithm — depth-first vs. breadth-first is a
    // traversal-order detail, not an algorithmic one, since each level's
    // checksums are evaluated independently.
    //
    // The root level is K equal chunks over the full primary-key window —
    // matches the design-doc pseudocode `chunks_root = K equal chunks
    // over full_pk_range(t)`. Without this split the root call would
    // reduce to a single full-table checksum and skip the cost-bound
    // floor (`K` chunks per side on a no-op diff).
    let root_chunks = split_int_range(
        &PkRange::IntRange {
            lo: target.pk_lo,
            hi: target.pk_hi,
        },
        config.k,
    )?;
    let mut stack: Vec<(Vec<PkRange>, u32)> = vec![(root_chunks, 0)];
    while let Some((chunks, depth)) = stack.pop() {
        diff_one_level(base, branch, target, &chunks, depth, &mut state, &mut stack).await?;
    }

    state.samples.sort_by(sample_sort_key);
    Ok(BisectionDiffResult {
        rows_added: state.rows_added,
        rows_removed: state.rows_removed,
        rows_changed: state.rows_changed,
        samples: state.samples,
        stats: BisectionStats {
            chunks_examined: state.chunks_examined,
            leaves_materialized: state.leaves_materialized,
            depth_max: state.depth_max,
            depth_capped: state.depth_capped,
            split_strategy: SplitStrategy::IntRange,
            null_pk_rows_base,
            null_pk_rows_branch,
        },
    })
}

/// Issue one `COUNT(*) WHERE pk IS NULL` per side at the root. Returns
/// 0 if the table has no null-PK rows.
async fn count_null_pk_rows(
    adapter: &dyn WarehouseAdapter,
    table: &TableRef,
    pk_column: &str,
) -> AdapterResult<u64> {
    rocky_sql::validation::validate_identifier(pk_column).map_err(AdapterError::new)?;
    let dialect = adapter.dialect();
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    let sql = format!(
        "SELECT COUNT(*) FROM {table_ref} WHERE \"{pk_column}\" IS NULL"
    );
    let result = adapter.execute_query(&sql).await?;
    let row = result
        .rows
        .first()
        .ok_or_else(|| AdapterError::msg("null-pk count query returned no rows"))?;
    let cell = row
        .first()
        .ok_or_else(|| AdapterError::msg("null-pk count query returned an empty row"))?;
    match cell {
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|e| {
            AdapterError::msg(format!("failed to parse null-pk count {s:?}: {e}"))
        }),
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| AdapterError::msg(format!("null-pk count not representable as u64: {n}"))),
        other => Err(AdapterError::msg(format!(
            "null-pk count returned unexpected JSON shape: {other:?}"
        ))),
    }
}

struct TraversalState<'a> {
    config: &'a BisectionConfig,
    leaf_threshold: u64,
    chunks_examined: u64,
    leaves_materialized: u64,
    depth_max: u32,
    depth_capped: bool,
    rows_added: u64,
    rows_removed: u64,
    rows_changed: u64,
    samples: Vec<LeafRowSample>,
}

/// Process one recursion level: checksum every chunk in `chunks` on both
/// sides, classify mismatches, and push deeper levels onto `stack` for
/// later processing. Leaves materialize-and-diff inline.
#[allow(clippy::too_many_arguments)]
async fn diff_one_level(
    base: &dyn WarehouseAdapter,
    branch: &dyn WarehouseAdapter,
    target: &BisectionTarget<'_>,
    chunks: &[PkRange],
    depth: u32,
    state: &mut TraversalState<'_>,
    stack: &mut Vec<(Vec<PkRange>, u32)>,
) -> AdapterResult<()> {
    state.depth_max = state.depth_max.max(depth);

    let base_sums = base
        .checksum_chunks(target.base, target.pk_column, target.value_columns, chunks)
        .await?;
    let branch_sums = branch
        .checksum_chunks(
            target.branch,
            target.pk_column,
            target.value_columns,
            chunks,
        )
        .await?;
    state.chunks_examined = state
        .chunks_examined
        .saturating_add((base_sums.len() + branch_sums.len()) as u64);

    let base_by_id = index_by_chunk_id(&base_sums);
    let branch_by_id = index_by_chunk_id(&branch_sums);

    for (idx, chunk) in chunks.iter().enumerate() {
        let chunk_id = u32::try_from(idx).map_err(|_| {
            AdapterError::msg("bisection level produced more than u32::MAX chunks")
        })?;
        let base_entry = base_by_id.get(&chunk_id).copied();
        let branch_entry = branch_by_id.get(&chunk_id).copied();

        let base_count = base_entry.map(|e| e.row_count).unwrap_or(0);
        let branch_count = branch_entry.map(|e| e.row_count).unwrap_or(0);
        let base_check = base_entry.map(|e| e.checksum).unwrap_or(0);
        let branch_check = branch_entry.map(|e| e.checksum).unwrap_or(0);

        if base_count == branch_count && base_check == branch_check {
            continue;
        }

        let max_rows = base_count.max(branch_count);
        let at_leaf = max_rows <= state.leaf_threshold;
        let at_depth_cap = depth + 1 >= state.config.max_depth;

        if at_leaf || at_depth_cap {
            if at_depth_cap && !at_leaf {
                state.depth_capped = true;
            }
            materialize_and_diff(base, branch, target, chunk, state).await?;
            continue;
        }

        let sub_chunks = split_int_range(chunk, state.config.k)?;
        if !sub_chunks.is_empty() {
            stack.push((sub_chunks, depth + 1));
        }
    }
    Ok(())
}

fn index_by_chunk_id(sums: &[ChunkChecksum]) -> std::collections::HashMap<u32, &ChunkChecksum> {
    sums.iter().map(|s| (s.chunk_id, s)).collect()
}

/// Split `[lo, hi)` into `K` equal-width sub-ranges. Last chunk absorbs
/// the integer-division remainder so the union covers `[lo, hi)`
/// exactly.
fn split_int_range(parent: &PkRange, k: u32) -> AdapterResult<Vec<PkRange>> {
    let (lo, hi) = match parent {
        PkRange::IntRange { lo, hi } => (*lo, *hi),
        PkRange::Composite { .. } | PkRange::HashBucket { .. } => {
            return Err(AdapterError::msg(
                "bisection_diff currently supports IntRange split \
                 strategies only; composite and hash-bucket strategies \
                 are not yet wired",
            ));
        }
    };
    if hi <= lo {
        return Ok(Vec::new());
    }
    let step = (hi - lo) / i128::from(k);
    if step == 0 {
        // Range is narrower than K — return a single tight chunk so the
        // recursion stops at the next leaf check.
        return Ok(vec![PkRange::IntRange { lo, hi }]);
    }
    let mut out = Vec::with_capacity(k as usize);
    for i in 0..k {
        let start = lo + step * i128::from(i);
        let end = if i + 1 == k {
            hi
        } else {
            lo + step * i128::from(i + 1)
        };
        out.push(PkRange::IntRange { lo: start, hi: end });
    }
    Ok(out)
}

/// Materialize the rows on each side under `chunk`'s predicate and walk
/// them in sorted order, classifying as added/removed/changed.
async fn materialize_and_diff(
    base: &dyn WarehouseAdapter,
    branch: &dyn WarehouseAdapter,
    target: &BisectionTarget<'_>,
    chunk: &PkRange,
    state: &mut TraversalState<'_>,
) -> AdapterResult<()> {
    state.leaves_materialized = state.leaves_materialized.saturating_add(1);

    let (lo, hi) = match chunk {
        PkRange::IntRange { lo, hi } => (*lo, *hi),
        _ => unreachable!("split_int_range only emits IntRange"),
    };

    let base_rows = fetch_chunk_rows(base, target.base, target, lo, hi).await?;
    let branch_rows = fetch_chunk_rows(branch, target.branch, target, lo, hi).await?;

    let mut bi = 0;
    let mut ri = 0;
    while bi < base_rows.len() && ri < branch_rows.len() {
        let b = &base_rows[bi];
        let r = &branch_rows[ri];
        // Numeric primary-key compare. String comparison would
        // mis-classify rows whose stringified PKs cross length
        // boundaries (e.g., "99" string-compares greater than "100"
        // because '9' > '1'), even though the SQL `ORDER BY` produces
        // numerically-sorted result sets.
        match b.pk_value.cmp(&r.pk_value) {
            std::cmp::Ordering::Less => {
                record_sample(state, LeafRowKind::Removed, &b.pk_str);
                state.rows_removed = state.rows_removed.saturating_add(1);
                bi += 1;
            }
            std::cmp::Ordering::Greater => {
                record_sample(state, LeafRowKind::Added, &r.pk_str);
                state.rows_added = state.rows_added.saturating_add(1);
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                if b.values != r.values {
                    record_sample(state, LeafRowKind::Changed, &b.pk_str);
                    state.rows_changed = state.rows_changed.saturating_add(1);
                }
                bi += 1;
                ri += 1;
            }
        }
    }
    while bi < base_rows.len() {
        record_sample(state, LeafRowKind::Removed, &base_rows[bi].pk_str);
        state.rows_removed = state.rows_removed.saturating_add(1);
        bi += 1;
    }
    while ri < branch_rows.len() {
        record_sample(state, LeafRowKind::Added, &branch_rows[ri].pk_str);
        state.rows_added = state.rows_added.saturating_add(1);
        ri += 1;
    }
    Ok(())
}

struct LeafRow {
    pk_value: i128,
    pk_str: String,
    values: Vec<serde_json::Value>,
}

async fn fetch_chunk_rows(
    adapter: &dyn WarehouseAdapter,
    table: &TableRef,
    target: &BisectionTarget<'_>,
    lo: i128,
    hi: i128,
) -> AdapterResult<Vec<LeafRow>> {
    rocky_sql::validation::validate_identifier(target.pk_column).map_err(AdapterError::new)?;
    for col in target.value_columns {
        rocky_sql::validation::validate_identifier(col).map_err(AdapterError::new)?;
    }
    let dialect = adapter.dialect();
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    let pk = target.pk_column;
    let cols_clause = if target.value_columns.is_empty() {
        format!("\"{pk}\"")
    } else {
        let mut clause = format!("\"{pk}\"");
        for c in target.value_columns {
            clause.push_str(", \"");
            clause.push_str(c);
            clause.push('"');
        }
        clause
    };

    let sql = format!(
        "SELECT {cols_clause} FROM {table_ref} \
         WHERE \"{pk}\" IS NOT NULL \
           AND \"{pk}\" >= {lo} \
           AND \"{pk}\" < {hi} \
         ORDER BY \"{pk}\""
    );

    let result = adapter.execute_query(&sql).await?;
    let mut out = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        if row.is_empty() {
            continue;
        }
        let pk_str = match &row[0] {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let pk_value: i128 = pk_str.parse().map_err(|e| {
            AdapterError::msg(format!(
                "primary-key column \"{pk}\" returned a value that didn't parse as i128: {pk_str:?} ({e})"
            ))
        })?;
        let values = row.into_iter().skip(1).collect();
        out.push(LeafRow {
            pk_value,
            pk_str,
            values,
        });
    }
    Ok(out)
}

fn record_sample(state: &mut TraversalState<'_>, kind: LeafRowKind, pk: &str) {
    if state.samples.len() >= state.config.max_samples {
        return;
    }
    state.samples.push(LeafRowSample {
        kind,
        pk: pk.to_string(),
    });
}

fn sample_sort_key(a: &LeafRowSample, b: &LeafRowSample) -> std::cmp::Ordering {
    let ka = match a.kind {
        LeafRowKind::Added => 0,
        LeafRowKind::Removed => 1,
        LeafRowKind::Changed => 2,
    };
    let kb = match b.kind {
        LeafRowKind::Added => 0,
        LeafRowKind::Removed => 1,
        LeafRowKind::Changed => 2,
    };
    ka.cmp(&kb).then_with(|| a.pk.cmp(&b.pk))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_int_range_evenly_partitions_window() {
        let parent = PkRange::IntRange { lo: 0, hi: 100 };
        let chunks = split_int_range(&parent, 4).unwrap();
        assert_eq!(chunks.len(), 4);
        match &chunks[0] {
            PkRange::IntRange { lo, hi } => {
                assert_eq!(*lo, 0);
                assert_eq!(*hi, 25);
            }
            _ => panic!("expected IntRange"),
        }
        match &chunks[3] {
            PkRange::IntRange { lo, hi } => {
                assert_eq!(*lo, 75);
                assert_eq!(*hi, 100);
            }
            _ => panic!("expected IntRange"),
        }
    }

    #[test]
    fn split_int_range_absorbs_remainder_in_last_chunk() {
        let parent = PkRange::IntRange { lo: 0, hi: 103 };
        let chunks = split_int_range(&parent, 4).unwrap();
        match &chunks[3] {
            PkRange::IntRange { lo, hi } => {
                assert_eq!(*lo, 75);
                assert_eq!(*hi, 103);
            }
            _ => panic!("expected IntRange"),
        }
    }

    #[test]
    fn split_int_range_narrower_than_k_returns_single_chunk() {
        let parent = PkRange::IntRange { lo: 5, hi: 7 };
        let chunks = split_int_range(&parent, 32).unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn split_int_range_rejects_composite() {
        let parent = PkRange::Composite {
            lo: vec![],
            hi: vec![],
        };
        assert!(split_int_range(&parent, 4).is_err());
    }
}
