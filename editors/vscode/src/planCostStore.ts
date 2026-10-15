import * as vscode from "vscode";
import { resolveProjectRoot } from "./config";
import type { PlanOutput } from "./types/generated/plan";
import type { PlanCostData } from "./webviews/lineage/contract";

/** The slice of `vscode.Memento` the store needs; lets tests pass a plain map. */
export interface PlanCostMemento {
  get<T>(key: string): T | undefined;
  update(key: string, value: unknown): PromiseLike<void>;
}

/** What is stored: the payload minus the flag computed at read time. */
export type RecordedPlanCost = Omit<PlanCostData, "modelsChangedSince"> & {
  /** Record layout version; a mismatch reads as "no plan". */
  version: number;
  /** Project root the plan ran in. Model names match bare, so this scopes them. */
  projectRoot: string;
};

const KEY = "rocky.lastPlanCost";
const VERSION = 1;
/** Most files the staleness scan looks at; reaching it reads as "changed". */
const SCAN_CAP = 20000;

let memento: PlanCostMemento | undefined;
const listeners = new Set<() => void>();

/** Bind the store to workspace state so the estimate survives a reload. */
export function initPlanCostStore(state: PlanCostMemento): void {
  memento = state;
}

/** Subscribe to new plan records (the Inspector refreshes its overlay on this). */
export function onPlanCostRecorded(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/**
 * Keep the cost preview of a plan the user ran explicitly in `projectRoot`. A
 * plan without a `cost_preview` (replication-only, older engine) leaves the
 * previous record alone: it says nothing about cost, so it must not erase a
 * real estimate.
 */
export function recordPlanCost(
  plan: PlanOutput,
  filter: string,
  projectRoot: string | undefined,
  now: Date = new Date(),
): void {
  const preview = plan.cost_preview;
  if (!preview || !memento) return;
  const data: RecordedPlanCost = {
    version: VERSION,
    projectRoot: projectRoot ?? "",
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
  for (const l of listeners) l();
}

/** The last recorded estimate for `projectRoot`, or null (none, other project, other layout). */
export function readPlanCost(
  projectRoot: string | undefined,
): RecordedPlanCost | null {
  const rec = memento?.get<RecordedPlanCost>(KEY);
  if (!rec || rec.version !== VERSION) return null;
  if (rec.projectRoot !== (projectRoot ?? "")) return null;
  return rec;
}

/** True when any file was modified after the plan ran. */
export function changedSince(recordedAt: string, mtimesMs: number[]): boolean {
  const t = Date.parse(recordedAt);
  if (Number.isNaN(t)) return true;
  return mtimesMs.some((m) => m > t);
}

/**
 * Plan-time cost for the cost overlay: the estimate recorded by the user's last
 * Plan command in this project. Never runs `rocky plan` (it persists a plan
 * file per call). The plan is stale when any `.rocky`, `.sql` or `.toml` file
 * under the project root changed after it: model bodies, sidecars,
 * `_defaults.toml` and `rocky.toml` (an adapter or `[cost]` change reprices).
 * The models directory is configurable per pipeline, so the scan covers the
 * whole root rather than assuming `models/`.
 */
export async function loadPlanCost(
  graphRoot?: string | null,
): Promise<PlanCostData | null> {
  // The root the Inspector's graph was built from, not whichever editor is
  // active now: a Plan run in another project must not price this graph.
  const root =
    graphRoot === undefined ? resolveProjectRoot() : (graphRoot ?? undefined);
  const recorded = readPlanCost(root);
  if (!recorded) return null;
  const { version: _version, projectRoot: _root, ...payload } = recorded;
  if (!root) return { ...payload, modelsChangedSince: true };
  try {
    // `undefined` exclude keeps the user's `files.exclude` (node_modules, .git).
    const files = await vscode.workspace.findFiles(
      new vscode.RelativePattern(root, "**/*.{rocky,sql,toml}"),
      undefined,
      SCAN_CAP,
    );
    // A capped scan may have missed a newer file: fail toward stale.
    if (files.length >= SCAN_CAP) {
      return { ...payload, modelsChangedSince: true };
    }
    const mtimes = await Promise.all(
      files.map(async (f) => {
        try {
          return (await vscode.workspace.fs.stat(f)).mtime;
        } catch {
          return 0;
        }
      }),
    );
    return {
      ...payload,
      modelsChangedSince: changedSince(recorded.recordedAt, mtimes),
    };
  } catch {
    // Cannot tell: label it stale rather than current.
    return { ...payload, modelsChangedSince: true };
  }
}
