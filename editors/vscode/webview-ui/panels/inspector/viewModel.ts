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

/** Short freshness label ("≤ 2h (warning)"), or null when undeclared. */
export function freshnessLabel(
  freshness: ModelFreshnessConfig | null,
): string | null {
  if (!freshness) return null;
  const lag = formatDuration(freshness.max_lag_seconds);
  return `≤ ${lag}${freshness.severity ? ` (${freshness.severity})` : ""}`;
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
