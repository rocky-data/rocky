import { useEffect, useRef } from "react";
import {
  ElCommandList,
  ElCommandPalette,
  ElDialog,
  ElDialogBackdrop,
  ElDialogPanel,
} from "@tailwindplus/elements/react";
import { kindColor, kindGlyph } from "./colors";

interface ModelSearchProps {
  /** Project models to search, in display order. */
  models: { id: string; label: string; fqn: string; kind: string }[];
  open: boolean;
  onClose: () => void;
  /** A model was picked — re-target the Inspector to it. */
  onSelect: (model: string) => void;
}

type DialogElement = HTMLElement & { show(): void; hide(): void };

/**
 * Cmd+K model search — a TailwindPlus Elements command palette inside a dialog.
 * The palette filters the model rows as you type (toggling each row's `hidden`)
 * and marks the active row with `aria-selected` for keyboard nav; picking one
 * re-targets the Inspector. Browse-then-filter: an empty query lists every
 * model (no `el-no-results`, per the `tailwind-plus-elements` skill). The input
 * is uncontrolled so React never re-renders the row list out from under the
 * palette's filtering. Each row reuses the canvas's `kindColor` / `kindGlyph`
 * so a model reads the same here as on the lineage graph. Themed to
 * `--vscode-*`; driven from React state via the el-dialog `show()`/`hide()`.
 */
export function ModelSearch({
  models,
  open,
  onClose,
  onSelect,
}: ModelSearchProps) {
  const ref = useRef<DialogElement | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Open/close the native modal from React state, and focus the search input
  // on open (React's autoFocus fires at mount, while the dialog is still hidden).
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    let cancelled = false;
    void customElements.whenDefined("el-dialog").then(() => {
      if (cancelled || !ref.current) return;
      if (open) {
        ref.current.show();
        inputRef.current?.focus();
      } else {
        ref.current.hide();
      }
    });
    return () => {
      cancelled = true;
    };
  }, [open]);

  // Sync state back when the user dismisses via Esc or click-outside.
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.addEventListener("close", onClose);
    el.addEventListener("cancel", onClose);
    return () => {
      el.removeEventListener("close", onClose);
      el.removeEventListener("cancel", onClose);
    };
  }, [onClose]);

  const pick = (model: string): void => {
    onSelect(model);
    onClose();
  };

  return (
    <ElDialog ref={ref}>
      <dialog className="m-0 mx-auto mt-[12vh] w-[min(40rem,92vw)] bg-transparent p-0 backdrop:bg-transparent">
        <ElDialogBackdrop className="fixed inset-0 bg-black/50 transition duration-150 data-[closed]:opacity-0" />
        <ElDialogPanel className="overflow-hidden rounded-lg border border-vscode-border bg-vscode-widget-bg shadow-2xl transition duration-150 data-[closed]:scale-95 data-[closed]:opacity-0">
          <ElCommandPalette>
            {/* Search input with a leading magnifying-glass, layered via a
                single-cell grid so the icon sits inside the input's padding. */}
            <div className="grid grid-cols-1 border-b border-vscode-border">
              <input
                ref={inputRef}
                placeholder="Search models…"
                className="col-start-1 row-start-1 h-11 w-full bg-transparent pr-4 pl-10 text-sm text-vscode-fg outline-none placeholder:text-vscode-desc"
              />
              <svg
                viewBox="0 0 20 20"
                fill="currentColor"
                aria-hidden="true"
                className="pointer-events-none col-start-1 row-start-1 ml-3.5 size-4 self-center text-vscode-desc"
              >
                <path
                  fillRule="evenodd"
                  clipRule="evenodd"
                  d="M9 3.5a5.5 5.5 0 1 0 0 11 5.5 5.5 0 0 0 0-11ZM2 9a7 7 0 1 1 12.452 4.391l3.328 3.329a.75.75 0 1 1-1.06 1.06l-3.329-3.328A7 7 0 0 1 2 9Z"
                />
              </svg>
            </div>
            <ElCommandList className="block max-h-80 scroll-py-2 overflow-auto p-2">
              {models.map((m) => (
                <button
                  key={m.id}
                  hidden
                  type="button"
                  onClick={() => pick(m.id)}
                  className="group flex w-full items-center gap-3 rounded-md px-2.5 py-2 text-left outline-none aria-selected:bg-[var(--vscode-list-activeSelectionBackground)]"
                >
                  <span
                    aria-hidden="true"
                    className="flex size-7 flex-none items-center justify-center rounded text-[10px] font-semibold tracking-wide text-[var(--vscode-editor-background)]"
                    style={{ backgroundColor: kindColor(m.kind) }}
                  >
                    {kindGlyph(m.kind)}
                  </span>
                  <span className="min-w-0 flex-auto">
                    <span className="block truncate font-mono text-sm text-vscode-fg group-aria-selected:text-[var(--vscode-list-activeSelectionForeground)]">
                      {m.label}
                    </span>
                    <span className="block truncate text-xs text-vscode-desc group-aria-selected:text-[var(--vscode-list-activeSelectionForeground)]">
                      {m.fqn}
                    </span>
                  </span>
                  <span className="flex-none text-[10px] tracking-wide text-vscode-desc uppercase group-aria-selected:text-[var(--vscode-list-activeSelectionForeground)]">
                    {m.kind}
                  </span>
                </button>
              ))}
            </ElCommandList>
          </ElCommandPalette>
          {/* Static keyboard-hint footer (outside the palette so it's never
              filtered). */}
          <div className="flex items-center justify-end gap-4 border-t border-vscode-border px-3 py-1.5 text-[10px] text-vscode-desc">
            <span>↑↓ navigate</span>
            <span>↵ open</span>
            <span>esc dismiss</span>
          </div>
        </ElDialogPanel>
      </dialog>
    </ElDialog>
  );
}
