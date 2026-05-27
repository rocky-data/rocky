import { useEffect, useRef } from "react";

export interface MenuItem {
  label: string;
  onClick: () => void;
}

/**
 * A positioned context menu rendered as a DOM overlay — VS Code webviews have
 * no native context-menu API. Dismisses on click-away or Escape.
 */
export function ContextMenu({
  x,
  y,
  items,
  onClose,
}: {
  x: number;
  y: number;
  items: MenuItem[];
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const onPointerDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="fixed z-50 min-w-[168px] rounded border py-1 text-sm shadow-lg"
      style={{
        left: x,
        top: y,
        background: "var(--vscode-menu-background)",
        borderColor: "var(--vscode-menu-border, var(--vscode-panel-border))",
        color: "var(--vscode-menu-foreground)",
      }}
    >
      {items.map((item) => (
        <button
          key={item.label}
          type="button"
          onClick={() => {
            item.onClick();
            onClose();
          }}
          className="block w-full px-3 py-1 text-left hover:bg-[var(--vscode-menu-selectionBackground)] hover:text-[var(--vscode-menu-selectionForeground)]"
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
