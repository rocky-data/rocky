import type { ColorMode } from "./context";

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
}: {
  colorMode: ColorMode;
  onColorMode: (mode: ColorMode) => void;
  search: string;
  onSearch: (query: string) => void;
}) {
  return (
    <div className="flex items-center gap-2 border-b border-vscode-border px-3 py-2">
      <input
        value={search}
        onChange={(e) => onSearch(e.target.value)}
        placeholder="Filter models (substring or /regex/)…"
        className="w-64 rounded border border-vscode-border bg-transparent px-2 py-1 text-sm text-vscode-fg outline-none focus:border-vscode-focus"
      />
      <span className="flex-1" />
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
