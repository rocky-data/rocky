import { useEffect, useState } from "react";
import { ElPopover } from "@tailwindplus/elements/react";
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

const DOWNSTREAM_POPOVER_ID = "rocky-blast-radius-downstream";

/**
 * The blast-radius value, click-to-reveal: shows the downstream count and (when
 * non-zero) opens a popover listing exactly which models a change would hit.
 * el-popover renders in the browser top layer, so it never clips at the panel edge.
 */
function BlastRadiusValue({ downstream }: { downstream: string[] }) {
  const label = `${downstream.length} downstream`;
  if (downstream.length === 0) return <>{label}</>;
  // el-popover's `anchor` isn't in React's HTMLAttributes — pass it as a raw attribute.
  const anchorAttr = { anchor: "bottom start" };
  return (
    <>
      <button
        type="button"
        popoverTarget={DOWNSTREAM_POPOVER_ID}
        className="text-left underline decoration-dotted underline-offset-4 hover:decoration-solid"
      >
        {label}
      </button>
      <ElPopover
        id={DOWNSTREAM_POPOVER_ID}
        popover="auto"
        {...anchorAttr}
        className="max-h-64 w-56 overflow-auto rounded-md border border-vscode-border bg-vscode-widget-bg p-1 shadow-lg [--anchor-gap:4px]"
      >
        <p className="px-2 py-1 text-[11px] uppercase tracking-wide text-vscode-desc">
          Downstream models
        </p>
        <ul>
          {downstream.map((m) => (
            <li
              key={m}
              className="truncate rounded px-2 py-1 font-mono text-xs text-vscode-fg"
            >
              {m}
            </li>
          ))}
        </ul>
      </ElPopover>
    </>
  );
}

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
          value={<BlastRadiusValue downstream={data.downstreamModels} />}
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
