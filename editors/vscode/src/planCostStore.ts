import type { PlanOutput } from "./types/generated/plan";
import type { PlanCostData } from "./webviews/lineage/contract";

/** The slice of `vscode.Memento` the store needs; lets tests pass a plain map. */
export interface PlanCostMemento {
  get<T>(key: string): T | undefined;
  update(key: string, value: unknown): PromiseLike<void>;
}

type RecordedPlanCost = Omit<PlanCostData, "modelsChangedSince">;

const KEY = "rocky.lastPlanCost";

let memento: PlanCostMemento | undefined;

/** Bind the store to workspace state so the estimate survives a reload. */
export function initPlanCostStore(state: PlanCostMemento): void {
  memento = state;
}

/**
 * Keep the cost preview of a plan the user ran explicitly. A plan without a
 * `cost_preview` (replication-only, older engine) leaves the previous record
 * alone: it says nothing about cost, so it must not erase a real estimate.
 */
export function recordPlanCost(
  plan: PlanOutput,
  filter: string,
  now: Date = new Date(),
): void {
  const preview = plan.cost_preview;
  if (!preview || !memento) return;
  const data: RecordedPlanCost = {
    recordedAt: plan.created_at ?? now.toISOString(),
    filter,
    source: preview.source,
    models: preview.models.map((m) => ({
      model: m.model,
      estimatedCostUsd: m.estimated_cost_usd ?? null,
      estimatedRows: m.estimated_rows ?? null,
      source: m.source,
    })),
  };
  void memento.update(KEY, data);
}

/** The last recorded plan estimate, without the staleness flag. */
export function readPlanCost(): RecordedPlanCost | null {
  return memento?.get<RecordedPlanCost>(KEY) ?? null;
}

/** True when any model file was modified after the plan ran. */
export function changedSince(recordedAt: string, mtimesMs: number[]): boolean {
  const t = Date.parse(recordedAt);
  if (Number.isNaN(t)) return true;
  return mtimesMs.some((m) => m > t);
}
