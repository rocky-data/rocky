import { useEffect, useRef } from "react";
import {
  ElCommandList,
  ElCommandPalette,
  ElDialog,
  ElDialogBackdrop,
  ElDialogPanel,
} from "@tailwindplus/elements/react";

interface ModelSearchProps {
  /** Project models to search, in display order. */
  models: { id: string; label: string; fqn: string }[];
  open: boolean;
  onClose: () => void;
  /** A model was picked — re-target the Inspector to it. */
  onSelect: (model: string) => void;
}

type DialogElement = HTMLElement & { show(): void; hide(): void };

/**
 * Cmd+K model search — a TailwindPlus Elements command palette inside a dialog.
 * The palette filters the model buttons as you type and handles keyboard nav;
 * picking one re-targets the Inspector. Themed to `--vscode-*` like the rest of
 * the webview. Driven from React state via the el-dialog `show()`/`hide()`
 * methods (gated on the element being defined). See the `tailwind-plus-elements`
 * skill for the integration notes.
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
            <input
              ref={inputRef}
              placeholder="Search models…"
              className="w-full border-b border-vscode-border bg-transparent px-4 py-3 text-sm text-vscode-fg outline-none placeholder:text-vscode-desc"
            />
            <ElCommandList className="block max-h-80 overflow-auto p-2">
              {models.map((m) => (
                <button
                  key={m.id}
                  hidden
                  type="button"
                  onClick={() => pick(m.id)}
                  className="flex w-full items-baseline gap-2 rounded px-3 py-1.5 text-left text-sm text-vscode-fg outline-none hover:bg-[var(--vscode-list-hoverBackground)] focus:bg-[var(--vscode-list-activeSelectionBackground)] focus:text-[var(--vscode-list-activeSelectionForeground)]"
                >
                  <span className="font-mono">{m.label}</span>
                  <span className="truncate text-xs text-vscode-desc">{m.fqn}</span>
                </button>
              ))}
            </ElCommandList>
          </ElCommandPalette>
        </ElDialogPanel>
      </dialog>
    </ElDialog>
  );
}
