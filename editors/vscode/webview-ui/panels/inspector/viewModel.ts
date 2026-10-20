import type { CatalogEdge } from "../../../src/types/generated/catalog";
import type {
  CostHint,
  ModelFreshnessConfig,
} from "../../../src/types/generated/compile";
import type { DeclarativeTestResult } from "../../../src/types/generated/test";

/** Worst-case test outcome for a column (or none when untested). */
export type ColumnTestStatus = "pass" | "warn" | "fail" | "none";

/** Human label for a `contract_source` value. */
export function contractLabel(source: string | null): string {
  if (source === "auto") return "Auto (sidecar)";
  if (source === "explicit") return "Explicit";
  return "None";
}

/** Seconds → compact human duration ("90s", "5m", "2h", "3d"). */
export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

const DURATION_UNITS: Record<string, number> = { s: 1, h: 3600, d: 86400 };

/**
 * Seconds in a freshness duration ("12h", "7d"): a positive integer and one
 * unit of s, h or d, the engine's grammar. Null for anything else.
 */
export function durationSeconds(raw: string | null | undefined): number | null {
  if (!raw) return null;
  const match = /^([1-9][0-9]*)([shd])$/.exec(raw);
  if (!match) return null;
  return Number(match[1]) * DURATION_UNITS[match[2]];
}

/**
 * Seconds until a model first reports stale: the smaller of `warn_after` /
 * `error_after`, or the deprecated `max_lag_seconds` when the block uses it.
 * Mirrors the engine's `FreshnessThresholds::budget_seconds`.
 */
export function freshnessBudgetSeconds(
  freshness: ModelFreshnessConfig,
): number | null {
  if (freshness.max_lag_seconds != null) return freshness.max_lag_seconds;
  const levels = [
    durationSeconds(freshness.warn_after),
    durationSeconds(freshness.error_after),
  ].filter((s): s is number => s !== null);
  return levels.length > 0 ? Math.min(...levels) : null;
}

/**
 * Short freshness label ("≤ 2h (warning)", or "≤ 1h (warning) · ≤ 6h (error)"
 * for a block with both levels), or null when undeclared.
 */
export function freshnessLabel(
  freshness: ModelFreshnessConfig | null,
): string | null {
  if (!freshness) return null;
  if (freshness.max_lag_seconds != null) {
    const lag = formatDuration(freshness.max_lag_seconds);
    return `≤ ${lag}${freshness.severity ? ` (${freshness.severity})` : ""}`;
  }
  const parts: string[] = [];
  const warn = durationSeconds(freshness.warn_after);
  const error = durationSeconds(freshness.error_after);
  if (warn !== null) parts.push(`≤ ${formatDuration(warn)} (warning)`);
  if (error !== null) parts.push(`≤ ${formatDuration(error)} (error)`);
  return parts.length > 0 ? parts.join(" · ") : null;
}

/** Compact integer count ("1.2k", "3.4M"). */
export function formatCount(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}k`;
  return `${(n / 1_000_000).toFixed(1)}M`;
}

/** Format the heuristic compile-time cost estimate, or null when absent. */
export function costLabel(cost: CostHint | null): string | null {
  if (!cost) return null;
  return `~$${cost.estimated_cost_usd.toFixed(4)} · ${formatCount(cost.estimated_rows)} rows (${cost.confidence})`;
}

/** Map a declarative result's status + severity to a column badge status. */
export function toStatus(status: string, severity: string): ColumnTestStatus {
  if (status === "pass") return "pass";
  if (status === "fail" || status === "error") {
    return severity === "warning" ? "warn" : "fail";
  }
  return "none";
}

const RANK: Record<ColumnTestStatus, number> = {
  none: 0,
  pass: 1,
  warn: 2,
  fail: 3,
};

/** Worst test status per column name, from declarative results. */
export function testStatusByColumn(
  results: DeclarativeTestResult[],
): Map<string, ColumnTestStatus> {
  const map = new Map<string, ColumnTestStatus>();
  for (const r of results) {
    if (!r.column) continue;
    const next = toStatus(r.status, r.severity);
    const current = map.get(r.column) ?? "none";
    map.set(r.column, RANK[next] > RANK[current] ? next : current);
  }
  return map;
}

/** Sort rank for the Columns "Tests" sort — worst status first. */
export function statusRank(status: ColumnTestStatus | undefined): number {
  return status ? RANK[status] : 0;
}

/** Per-column upstream/downstream column-lineage edges for one model. */
export interface ColumnLineage {
  column: string;
  upstream: CatalogEdge[];
  downstream: CatalogEdge[];
}

/** Group a model's column-lineage edges by the model's own column. */
export function groupColumnLineage(
  model: string,
  columns: string[],
  edges: CatalogEdge[],
): ColumnLineage[] {
  return columns.map((column) => ({
    column,
    upstream: edges.filter(
      (e) => e.target_model === model && e.target_column === column,
    ),
    downstream: edges.filter(
      (e) => e.source_model === model && e.source_column === column,
    ),
  }));
}
