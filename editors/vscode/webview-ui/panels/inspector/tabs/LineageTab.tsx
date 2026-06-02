import type { InspectorModelData } from "../../../../src/webviews/inspector/contract";
import { groupColumnLineage } from "../viewModel";

const CONFIDENCE_COLOR: Record<string, string> = {
  High: "var(--vscode-charts-green)",
  Medium: "var(--vscode-charts-yellow)",
  Low: "var(--vscode-charts-red)",
};

function Hop({
  confidence,
  arrow,
  label,
  transform,
}: {
  confidence: string;
  arrow: string;
  label: string;
  transform: string;
}) {
  return (
    <div className="flex items-center gap-2">
      <span
        title={`${confidence} confidence`}
        style={{
          color:
            CONFIDENCE_COLOR[confidence] ?? "var(--vscode-descriptionForeground)",
        }}
      >
        ●
      </span>
      <span className="text-vscode-desc">{arrow}</span>
      <span className="font-mono text-vscode-fg">{label}</span>
      <span
        className="rounded-sm px-1 text-vscode-desc"
        style={{ background: "var(--vscode-editorWidget-background)" }}
      >
        {transform}
      </span>
    </div>
  );
}

export function LineageTab({ data }: { data: InspectorModelData }) {
  const lineage = groupColumnLineage(
    data.modelName,
    data.columns.map((c) => c.name),
    data.columnEdges,
  ).filter((col) => col.upstream.length > 0 || col.downstream.length > 0);

  if (lineage.length === 0) {
    return (
      <p className="text-vscode-desc">
        No column-level lineage tracked for this model.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {lineage.map((col) => (
        <div key={col.column} className="rounded-sm border border-vscode-border p-3">
          <div className="font-mono text-sm text-vscode-fg">{col.column}</div>
          <div className="mt-2 flex flex-col gap-1 text-xs">
            {col.upstream.map((edge, i) => (
              <Hop
                key={`u${i}`}
                confidence={edge.confidence}
                arrow="←"
                label={`${edge.source_model}.${edge.source_column}`}
                transform={edge.transform}
              />
            ))}
            {col.downstream.map((edge, i) => (
              <Hop
                key={`d${i}`}
                confidence={edge.confidence}
                arrow="→"
                label={`${edge.target_model}.${edge.target_column}`}
                transform={edge.transform}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
