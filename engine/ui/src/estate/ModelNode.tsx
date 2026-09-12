import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { ModelFlowNode } from "./layout";
import { NODE_HEIGHT, NODE_WIDTH } from "./layout";
import { nodeRoute } from "./nodeRoute";

/** Accent by resource kind, the VS Code Inspector's idiom in the SPA's palette. */
function kindClass(kind: string): string {
  switch (kind.toLowerCase()) {
    case "source":
      return "border-l-emerald-500";
    case "model":
    case "load":
      return "border-l-sky-500";
    case "view":
      return "border-l-violet-500";
    case "materializedview":
      return "border-l-amber-500";
    default:
      return "border-l-zinc-400";
  }
}

/** Short glyph shown on a node, by resource kind. */
export function kindGlyph(kind: string): string {
  switch (kind.toLowerCase()) {
    case "source":
      return "SRC";
    case "model":
      return "MDL";
    case "load":
      return "LD";
    case "view":
      return "VW";
    case "materializedview":
      return "MV";
    default:
      return "•";
  }
}

/** A rounded card: a kind glyph and the model name. Every value is text. */
export function ModelNode({ data, selected }: NodeProps<ModelFlowNode>) {
  const title = [data.target, data.strategy].filter((s) => s !== null).join(" · ");
  // Only a node the detail route can serve invites a click.
  const openable = nodeRoute(data).state === "servable";
  return (
    <div
      className={`flex items-center gap-2 rounded-md border border-l-4 bg-white px-2.5 py-2 text-xs shadow-xs dark:bg-zinc-900 ${kindClass(data.kind)} ${
        openable ? "cursor-pointer" : "cursor-default"
      } ${selected ? "border-sky-500" : "border-zinc-200 dark:border-zinc-700"}`}
      // Both, not just the width. `layout.ts` declares this size to React
      // Flow, and `position()` spaces rows by it. Measured with the card left
      // to size itself: the wrapper was 184×46 and the card inside it 184×34,
      // so the node carried a 12px invisible clickable band and its handles
      // sat 6px below the card's visual centre.
      style={{ width: NODE_WIDTH, height: NODE_HEIGHT }}
      title={title}
    >
      <Handle type="target" position={Position.Left} className="!bg-zinc-400" />
      <span className="rounded-sm bg-zinc-200 px-1 py-0.5 text-[9px] font-bold leading-none text-zinc-700 dark:bg-zinc-700 dark:text-zinc-100">
        {kindGlyph(data.kind)}
      </span>
      <span className="truncate text-zinc-900 dark:text-zinc-100">{data.label}</span>
      <Handle type="source" position={Position.Right} className="!bg-zinc-400" />
    </div>
  );
}
