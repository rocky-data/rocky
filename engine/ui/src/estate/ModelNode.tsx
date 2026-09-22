import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { ModelFlowNode } from "./layout";
import { NODE_HEIGHT, NODE_WIDTH } from "./layout";
import { NODE_KINDS, type NodeKind, nodeRoute } from "./nodeRoute";

interface Presentation {
  readonly glyph: string;
  readonly accent: string;
}

/**
 * One glyph and one accent per kind the engine can actually send — `NODE_KINDS`
 * from `nodeRoute.ts`, the one place that list is written down. A
 * `Record<NodeKind, Presentation>` fails to typecheck if a kind here is
 * missing or misspelled, so this table cannot drift from the route table the
 * way the old one (keyed on `model`, `view`, `materializedview` — none of
 * them a kind the engine emits) silently did (#1859).
 */
const PRESENTATION: Record<NodeKind, Presentation> = {
  source: { glyph: "SRC", accent: "border-l-emerald-500" },
  replication: { glyph: "REP", accent: "border-l-teal-500" },
  transformation: { glyph: "XFM", accent: "border-l-sky-500" },
  quality: { glyph: "DQ", accent: "border-l-fuchsia-500" },
  snapshot: { glyph: "SNP", accent: "border-l-violet-500" },
  load: { glyph: "LD", accent: "border-l-cyan-500" },
  seed: { glyph: "SD", accent: "border-l-lime-500" },
  test: { glyph: "TST", accent: "border-l-amber-500" },
};

const DEFAULT_PRESENTATION: Presentation = { glyph: "•", accent: "border-l-zinc-400" };

const KNOWN_KINDS: ReadonlySet<string> = new Set(NODE_KINDS);

/**
 * A kind's presentation, or the default dot and grey accent for anything
 * `NODE_KINDS` does not name. `data.kind` reaches this component as a bare
 * string — the engine's `NodeKind` enum crosses the wire as JSON — so an
 * unknown value (a stale stored DAG, a kind a newer engine adds) has to fall
 * to the default rather than fail to render.
 */
function presentationFor(kind: string): Presentation {
  return KNOWN_KINDS.has(kind) ? PRESENTATION[kind as NodeKind] : DEFAULT_PRESENTATION;
}

/** Accent by resource kind, the VS Code Inspector's idiom in the SPA's palette. */
export function kindClass(kind: string): string {
  return presentationFor(kind).accent;
}

/** Short glyph shown on a node, by resource kind. */
export function kindGlyph(kind: string): string {
  return presentationFor(kind).glyph;
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
      } ${
        // `border-{color}` and `dark:border-{color}` set the CSS `border-color`
        // shorthand, which — whichever of it and `kindClass`'s `border-l-*`
        // compiles later in Tailwind's stylesheet — resets `border-left-color`
        // too, so the unselected card's neutral border silently ate the kind
        // accent it was drawn next to (confirmed by inspecting the built CSS:
        // `dark:border-zinc-700` compiles after every `border-l-{kind}-500`
        // rule, so every card showed the same grey border-left in dark mode,
        // whatever its kind). The neutral border is now three directional
        // utilities, none of which touch the left side, so only `kindClass`
        // ever sets it. Selected keeps the shorthand on purpose: it is meant
        // to override every accent, transformation's included, to mark
        // selection — `layout.ts` only ever lets a transformation node
        // become `selected` (`selectable: openable`), and transformation's
        // own accent is already sky-500, so today the override and the kind
        // accent agree rather than one visibly winning over the other.
        selected
          ? "border-sky-500"
          : "border-t-zinc-200 border-r-zinc-200 border-b-zinc-200 dark:border-t-zinc-700 dark:border-r-zinc-700 dark:border-b-zinc-700"
      }`}
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
