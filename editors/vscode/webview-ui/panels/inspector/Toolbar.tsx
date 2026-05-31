import { useEffect, useRef, useState } from "react";
import type { ColorMode, OverlayKind } from "./context";

/** Delay before a keystroke propagates to the (graph-rebuilding) search filter. */
const SEARCH_DEBOUNCE_MS = 180;

const OVERLAY_LABELS: Record<OverlayKind, string> = {
  cost: "Cost",
  freshness: "Freshness",
  drift: "Drift",
  breaking: "Breaking",
  lastRun: "Last run",
  governance: "Governance",
};

function segment(active: boolean): string {
  return (
    "px-2 py-1 " +
    (active
      ? "bg-vscode-button-bg text-vscode-button-fg"
      : "text-vscode-desc hover:text-vscode-fg")
  );
}

export function Toolbar({
  colorMode,
  onColorMode,
  search,
  onSearch,
  activeOverlays,
  onToggleOverlay,
}: {
  colorMode: ColorMode;
  onColorMode: (mode: ColorMode) => void;
  search: string;
  onSearch: (query: string) => void;
  activeOverlays: Set<OverlayKind>;
  onToggleOverlay: (kind: OverlayKind) => void;
}) {
  // Local state keeps the input responsive on every keystroke; the
  // graph-rebuilding `onSearch` is debounced so each keypress doesn't re-derive
  // every node/edge in the canvas. Kept in sync when `search` is reset upstream.
  const [draft, setDraft] = useState(search);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => setDraft(search), [search]);
  useEffect(() => () => clearTimeout(timer.current), []);

  const onChange = (value: string): void => {
    setDraft(value);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => onSearch(value), SEARCH_DEBOUNCE_MS);
  };

  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-vscode-border px-3 py-2">
      <input
        value={draft}
        onChange={(e) => onChange(e.target.value)}
        placeholder="Filter models (substring or /regex/)…"
        className="w-56 rounded border border-vscode-border bg-transparent px-2 py-1 text-sm text-vscode-fg outline-none focus:border-vscode-focus"
      />
      <span className="flex-1" />
      <span className="text-xs text-vscode-desc">Overlays</span>
      <div className="flex gap-1 text-xs">
        {(Object.keys(OVERLAY_LABELS) as OverlayKind[]).map((kind) => (
          <button
            key={kind}
            type="button"
            onClick={() => onToggleOverlay(kind)}
            className={"rounded border border-vscode-border " + segment(activeOverlays.has(kind))}
          >
            {OVERLAY_LABELS[kind]}
          </button>
        ))}
      </div>
      <span className="text-xs text-vscode-desc">Color by</span>
      <div className="flex overflow-hidden rounded border border-vscode-border text-xs">
        <button
          type="button"
          onClick={() => onColorMode("kind")}
          className={segment(colorMode === "kind")}
        >
          Type
        </button>
        <button
          type="button"
          onClick={() => onColorMode("materialization")}
          className={segment(colorMode === "materialization")}
        >
          Materialization
        </button>
      </div>
    </div>
  );
}
