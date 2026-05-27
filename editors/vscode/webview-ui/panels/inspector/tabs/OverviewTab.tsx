import { useEffect, useState } from "react";
import type { InspectorModelData } from "../../../../src/webviews/inspector/contract";
import type {
  BreakingData,
  DriftData,
  GovernanceData,
  ReplayData,
} from "../../../../src/webviews/lineage/contract";
import { getRpc } from "../../../runtime/rpcClient";
import { StatusCard } from "../components";
import {
  contractLabel,
  costLabel,
  formatCount,
  freshnessLabel,
} from "../viewModel";

/**
 * Overview = the model trust dashboard. Headline cost + blast-radius, then a
 * grid of the trust-plane signals dbt has no engine for — contract, freshness,
 * drift, governance, last run — fetched once the tab opens (project-wide, then
 * sliced to this model). Each card degrades to a muted state when its source
 * has no data yet (no runs, no base ref, no classified columns).
 */
export function OverviewTab({ data }: { data: InspectorModelData }) {
  const [drift, setDrift] = useState<DriftData | null>(null);
  const [gov, setGov] = useState<GovernanceData | null>(null);
  const [breaking, setBreaking] = useState<BreakingData | null>(null);
  const [replay, setReplay] = useState<ReplayData | null>(null);

  useEffect(() => {
    let active = true;
    const req = <T,>(method: string, set: (v: T) => void): void => {
      void getRpc()
        .request<T>(method)
        .then((v) => active && set(v))
        .catch(() => {});
    };
    req<DriftData>("drift", setDrift);
    req<GovernanceData>("governance", setGov);
    req<BreakingData>("breaking", setBreaking);
    req<ReplayData>("replay", setReplay);
    return () => {
      active = false;
    };
  }, [data.modelName]);

  const govModel = gov?.models.find((m) => m.model === data.modelName);
  const lastRun = replay?.models.find((m) => m.model === data.modelName);
  const breakingFinding = breaking?.findings.find(
    (f) => f.model === data.modelName,
  );
  const driftAction = drift?.actions.find(
    (a) =>
      a.table === data.modelName ||
      a.table === data.fqn ||
      a.table.endsWith(`.${data.modelName}`),
  );

  return (
    <div className="flex flex-col gap-4">
      {data.intent ? (
        <p className="text-sm text-vscode-fg">{data.intent}</p>
      ) : (
        <p className="text-sm italic text-vscode-desc">
          No description in the model sidecar.
        </p>
      )}

      {/* Headline signals — the differentiators dbt can't show. */}
      <div className="grid grid-cols-2 gap-3">
        <StatusCard
          hero
          label="Estimated cost"
          title="Compile-time cost estimate (directional, not a warehouse EXPLAIN)."
          value={costLabel(data.costHint) ?? "—"}
          tone={data.costHint ? "warn" : "muted"}
          sub="per run"
        />
        <StatusCard
          hero
          label="Blast radius"
          title="Models downstream of this one — what could break if you change it."
          value={`${data.downstreamModels.length} downstream`}
          tone={
            breakingFinding
              ? "risk"
              : data.downstreamModels.length > 0
                ? "warn"
                : "ok"
          }
          sub={
            breakingFinding
              ? `breaking · ${breakingFinding.severity}`
              : `${data.upstreamModels.length} upstream`
          }
        />
      </div>

      {/* Trust-plane status grid. */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <StatusCard
          label="Contract"
          title="Whether this model has a data contract (auto-inferred or explicit)."
          value={contractLabel(data.contractSource)}
          tone={data.contractSource ? "ok" : "muted"}
        />
        <StatusCard
          label="Freshness"
          title="Declared freshness SLA for this model."
          value={freshnessLabel(data.freshness) ?? "Not declared"}
          tone={data.freshness ? "ok" : "muted"}
        />
        <StatusCard
          label="Drift"
          title="Schema drift Rocky reconciled on the last run."
          value={driftAction ? driftAction.action : drift ? "None" : "…"}
          tone={
            driftAction
              ? driftAction.action.toUpperCase().includes("REFRESH")
                ? "risk"
                : "warn"
              : drift
                ? "ok"
                : "pending"
          }
          sub={driftAction?.reason}
        />
        <StatusCard
          label="Governance"
          title="Classified (PII/sensitive) columns, and any left unmasked."
          value={
            govModel
              ? `${govModel.classifiedColumns} classified`
              : gov
                ? "None"
                : "…"
          }
          tone={
            govModel?.unmaskedColumns
              ? "risk"
              : govModel?.classifiedColumns
                ? "ok"
                : gov
                  ? "muted"
                  : "pending"
          }
          sub={
            govModel?.unmaskedColumns
              ? `${govModel.unmaskedColumns} unmasked`
              : undefined
          }
        />
        <StatusCard
          label="Last run"
          title="The most recent run's status and row count for this model."
          value={lastRun ? lastRun.status : replay ? "Never run" : "…"}
          tone={
            lastRun
              ? lastRun.status.toLowerCase() === "success"
                ? "ok"
                : "warn"
              : replay
                ? "muted"
                : "pending"
          }
          sub={
            lastRun?.rows != null
              ? `${formatCount(lastRun.rows)} rows`
              : undefined
          }
        />
        <StatusCard
          label="Materialization"
          value={data.materialization ?? "—"}
          tone="muted"
        />
        <StatusCard
          label="Columns"
          value={String(data.columns.length)}
          tone="muted"
        />
        <StatusCard
          label="Last materialized"
          value={data.lastMaterializedAt ?? "Never"}
          tone={data.lastMaterializedAt ? "ok" : "muted"}
        />
      </div>
    </div>
  );
}
