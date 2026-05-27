import { Handle, Position, type NodeProps } from "@xyflow/react";
import { kindColor, kindGlyph, materializationColor } from "../colors";
import { useColorMode, useOverlays } from "../context";
import type { ModelFlowNode } from "../layout";
import { composeBadges } from "../overlays/compose";

/** A rounded card node: a kind glyph, the model name, a kind/materialization accent, and overlay badges. */
export function ModelNode({ id, data, selected }: NodeProps<ModelFlowNode>) {
  const mode = useColorMode();
  const overlays = useOverlays();
  const accent =
    mode === "kind"
      ? kindColor(data.kind)
      : materializationColor(data.materialization);
  const badges = composeBadges({ id, data }, overlays);

  return (
    <div
      className="relative flex items-center gap-2 rounded-md border px-2.5 py-2 shadow-sm"
      style={{
        width: 184,
        background: "var(--vscode-editorWidget-background)",
        borderColor: selected ? "var(--vscode-focusBorder)" : "var(--vscode-panel-border)",
        borderLeft: `3px solid ${accent}`,
      }}
      title={data.fqn}
    >
      <Handle
        type="target"
        position={Position.Left}
        style={{ background: "var(--vscode-panel-border)" }}
      />
      <span
        className="rounded px-1 py-0.5 text-[9px] font-bold leading-none"
        style={{ background: accent, color: "var(--vscode-editor-background)" }}
      >
        {kindGlyph(data.kind)}
      </span>
      <span className="truncate text-xs text-vscode-fg">{data.label}</span>
      <Handle
        type="source"
        position={Position.Right}
        style={{ background: "var(--vscode-panel-border)" }}
      />
      {badges.length > 0 && (
        <div className="absolute -bottom-5 left-0 flex max-w-full gap-1 overflow-hidden">
          {badges.map((badge, i) => (
            <span
              key={i}
              className="whitespace-nowrap rounded px-1 text-[9px] leading-tight"
              style={{
                background: badge.color ?? "var(--vscode-badge-background)",
                color: "var(--vscode-editor-background)",
              }}
              title={badge.title}
            >
              {badge.text}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
