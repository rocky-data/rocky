/* eslint-disable */

/**
 * JSON output for `rocky compact --measure-dedup` (Layer 0 storage experiment).
 *
 * Measures cross-table partition dedup across all Rocky-managed tables in a project. The `raw` and `semantic` numbers are both **partition-level lower bounds** on byte-level dedup — two partitions that share 99% of their row groups but differ on a single row will appear fully distinct here. The optional `calibration` block (populated by `--calibrate-bytes`) hashes actual row-group bytes on a sampled subset of tables to give a sharper, more expensive second number.
 *
 * The measurement is **engine-scoped**: warehouse-native `HASH()` is not portable across engines (Databricks ≠ Snowflake ≠ DuckDB), so the `engine` field must be carried with any published result.
 */
export interface CompactDedupOutput {
  /**
   * Byte-level calibration on a sampled subset of tables. Present only when `--calibrate-bytes` was passed. Produces a sharper estimate at the cost of pulling raw rows and hashing them locally.
   */
  calibration?: ByteCalibration | null;
  command: string;
  duration_ms: number;
  /**
   * Warehouse engine identifier (`"databricks"`, `"snowflake"`, `"duckdb"`). Measurements are not comparable across engines because `HASH()` is not portable.
   */
  engine: string;
  /**
   * Columns excluded from the `semantic` hash.
   */
  excluded_columns: string[];
  measured_at: string;
  partitions_scanned: number;
  /**
   * Per-table breakdown showing which tables contribute most to the duplicate set.
   */
  per_table: TableDedupContribution[];
  /**
   * Project name from `rocky.toml`.
   */
  project: string;
  /**
   * Dedup computed over all columns (what a byte-level chunk store would actually deduplicate).
   */
  raw: DedupSummary;
  /**
   * Whether the measurement was scoped to Rocky-managed tables (`"managed"`) or included all warehouse tables (`"all"`).
   */
  scope: string;
  /**
   * Dedup computed after excluding Rocky-owned metadata columns. The gap between `raw` and `semantic` is "dedup we'd unlock if we ignored Rocky's own per-row metadata."
   */
  semantic: DedupSummary;
  tables_scanned: number;
  /**
   * Top-N table pairs by shared partition count (from the `semantic` measurement).
   */
  top_dedup_pairs: DedupPair[];
  version: string;
  [k: string]: unknown;
}
/**
 * Byte-level calibration block populated by `--calibrate-bytes`.
 *
 * Samples a handful of tables (default: 3), pulls their rows via `SELECT *`, hashes each row group with blake3 in-process, and computes byte-level dedup on the sample. The `lower_bound_multiplier` is the ratio `byte_dedup_pct / partition_dedup_pct` — it tells you how much of a floor the cheap partition-level measurement actually is.
 */
export interface ByteCalibration {
  /**
   * Byte-level blake3 dedup on the same sample.
   */
  byte_dedup_pct: number;
  /**
   * `byte_dedup_pct / partition_dedup_pct`. A value >1.0 means the cheap partition-level measurement is genuinely underestimating.
   */
  lower_bound_multiplier: number;
  /**
   * Partition-level `SUM(HASH())` dedup recomputed on the sample (so it's directly comparable to `byte_dedup_pct`).
   */
  partition_dedup_pct: number;
  /**
   * Which tables were hashed at byte level.
   */
  tables_sampled: string[];
  [k: string]: unknown;
}
/**
 * Per-table contribution to the overall duplicate set.
 */
export interface TableDedupContribution {
  /**
   * 0.0..=100.0. Share of this table's partitions that are duplicates of some other table's partitions.
   */
  contribution_pct: number;
  partitions: number;
  partitions_shared_with_others: number;
  table: string;
  [k: string]: unknown;
}
/**
 * Summary of a single dedup measurement (raw or semantic).
 *
 * `dedup_ratio` is a **lower bound** on true byte-level dedup — see the docstring on [`CompactDedupOutput`].
 */
export interface DedupSummary {
  /**
   * 0.0..=1.0. Partition-level lower bound on byte-level dedup.
   */
  dedup_ratio: number;
  duplicate_partitions: number;
  /**
   * 0.0..=100.0. Estimated storage savings if duplicate partitions were collapsed to a single copy.
   */
  estimated_savings_pct: number;
  total_rows: number;
  unique_partitions: number;
  [k: string]: unknown;
}
/**
 * A pair of tables that share duplicate partitions.
 */
export interface DedupPair {
  shared_partitions: number;
  shared_rows: number;
  table_a: string;
  table_b: string;
  [k: string]: unknown;
}
