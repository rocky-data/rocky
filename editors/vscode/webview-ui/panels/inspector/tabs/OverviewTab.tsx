import type { InspectorModelData } from "../../../../src/webviews/inspector/contract";
import { contractLabel, costLabel, freshnessLabel } from "../viewModel";

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border border-vscode-border p-3">
      <div className="text-[11px] uppercase tracking-wide text-vscode-desc">
        {label}
      </div>
      <div className="mt-1 break-words text-sm text-vscode-fg">{value}</div>
    </div>
  );
}

export function OverviewTab({ data }: { data: InspectorModelData }) {
  return (
    <div className="flex flex-col gap-4">
      {data.intent ? (
        <p className="text-sm text-vscode-fg">{data.intent}</p>
      ) : (
        <p className="text-sm italic text-vscode-desc">
          No description in the model sidecar.
        </p>
      )}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
        <Field label="Kind" value={data.kind} />
        <Field label="Materialization" value={data.materialization ?? "—"} />
        <Field label="Contract" value={contractLabel(data.contractSource)} />
        <Field
          label="Freshness"
          value={freshnessLabel(data.freshness) ?? "Not declared"}
        />
        <Field label="Est. cost" value={costLabel(data.costHint) ?? "—"} />
        <Field
          label="Last materialized"
          value={data.lastMaterializedAt ?? "Never"}
        />
        <Field label="Columns" value={String(data.columns.length)} />
        <Field label="Upstream" value={String(data.upstreamModels.length)} />
        <Field label="Downstream" value={String(data.downstreamModels.length)} />
      </div>
    </div>
  );
}
