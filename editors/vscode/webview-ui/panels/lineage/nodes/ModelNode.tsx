import { Handle, Position, type NodeProps } from "@xyflow/react";
import { kindColor, kindGlyph, materializationColor } from "../colors";
import { useColorMode } from "../context";
import type { ModelFlowNode } from "../layout";

/** A rounded card node: a kind glyph, the model name, and a kind/materialization accent. */
export function ModelNode({ data, selected }: NodeProps<ModelFlowNode>) {
  const mode = useColorMode();
  const accent =
    mode === "kind"
      ? kindColor(data.kind)
      : materializationColor(data.materialization);

  return (
    <div
      className="flex items-center gap-2 rounded-md border px-2.5 py-2 shadow-sm"
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
    </div>
  );
}
