import type { PlanCostData } from "../../../../src/webviews/lineage/contract";
import type { Badge, LineageOverlay } from "./types";

const YELLOW = "var(--vscode-charts-yellow)";
const BLUE = "var(--vscode-charts-blue)";
const STALE = "var(--vscode-disabledForeground)";

/** A plan older than this is labelled stale even if no model file changed. */
const STALE_AFTER_MS = 24 * 60 * 60 * 1000;

/** "just now", "5 min ago", "3 h ago", "2 d ago". */
export function relativeTime(iso: string, now: number): string {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "unknown time";
  const mins = Math.max(0, Math.round((now - t) / 60_000));
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins} min ago`;
  const hours = Math.round(mins / 60);
  if (hours < 48) return `${hours} h ago`;
  return `${Math.round(hours / 24)} d ago`;
}

/** Why a plan estimate is stale, or null when it can be shown as current. */
export function staleReason(plan: PlanCostData, now: number): string | null {
  if (plan.modelsChangedSince) return "models changed since this plan";
  const t = Date.parse(plan.recordedAt);
  if (Number.isNaN(t) || now - t > STALE_AFTER_MS) return "plan is over a day old";
  return null;
}

/** Heuristic compile-time badge (the fallback, and the non-rebuild nodes). */
function heuristicBadge(
  cost: { estimated_cost_usd: number; estimated_rows: number; confidence: string },
  note?: string,
): Badge {
  return {
    text: `~$${cost.estimated_cost_usd.toFixed(3)} (heuristic)`,
    color: YELLOW,
    title: `≈ ${cost.estimated_rows.toLocaleString()} rows · ${cost.confidence} confidence (heuristic compile-time estimate)${note ? ` · ${note}` : ""}`,
  };
}

/**
 * Build the cost overlay. A model the last explicit `rocky plan` priced shows
 * that figure labelled "from last plan" with its time (and "stale" when the
 * plan is old or a model changed since). Every other node keeps the heuristic
 * `cost_hint`, labelled as such. Plan figures cover the rebuild scope only.
 */
export function makeCostOverlay(
  plan: PlanCostData | null,
  now: number = Date.now(),
): LineageOverlay {
  const byModel = new Map((plan?.models ?? []).map((m) => [m.model, m]));
  const stale = plan ? staleReason(plan, now) : null;
  const filterNote = plan?.filter ? ` · filter ${plan.filter}` : "";
  const when = plan
    ? `${relativeTime(plan.recordedAt, now)}${filterNote}`
    : "";
  return {
    id: "cost",
    decorate(node) {
      const row = plan
        ? (byModel.get(node.id) ?? byModel.get(node.data.fqn))
        : undefined;
      if (plan && row && row.estimatedCostUsd != null) {
        const label = stale ? "from last plan, stale" : "from last plan";
        return [
          {
            text: `~$${row.estimatedCostUsd.toFixed(3)} (plan, ${row.source}${stale ? ", stale" : ""})`,
            color: stale ? STALE : BLUE,
            title: `${label} · ${when} (${plan.recordedAt}) · ${row.source} estimate${
              row.estimatedRows != null
                ? ` · ≈ ${row.estimatedRows.toLocaleString()} rows`
                : ""
            }${stale ? ` · stale: ${stale}` : ""}`,
          },
        ];
      }
      const hint = node.data.costHint;
      if (!hint) return undefined;
      return [
        heuristicBadge(
          hint,
          plan && row ? "the last plan could not price this model" : undefined,
        ),
      ];
    },
    legend() {
      return [
        {
          text: plan
            ? `est. cost from last plan · ${when}${stale ? " (stale)" : ""}`
            : "est. cost / run (heuristic)",
          color: plan ? (stale ? STALE : BLUE) : YELLOW,
        },
        ...(plan ? [{ text: "heuristic (not in last plan)", color: YELLOW }] : []),
      ];
    },
  };
}
