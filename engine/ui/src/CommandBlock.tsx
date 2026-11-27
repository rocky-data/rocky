import { useState } from "react";
import { CheckIcon, ClipboardDocumentIcon } from "@heroicons/react/20/solid";

/**
 * A terminal command as text, with a copy button when the browser allows it.
 * The command stays on the page as text, so it can also be selected by hand.
 */
export function CommandBlock({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);
  const canCopy = typeof navigator !== "undefined" && navigator.clipboard !== undefined;
  return (
    <div className="flex items-start gap-2 rounded-md bg-surface-2 p-2">
      <code className="min-w-0 flex-1 py-1 pl-1 font-mono text-xs break-all whitespace-pre-wrap text-ink">
        {command}
      </code>
      {canCopy && (
        <button
          type="button"
          onClick={() => {
            navigator.clipboard.writeText(command).then(
              () => setCopied(true),
              () => setCopied(false),
            );
          }}
          className="inline-flex size-8 shrink-0 items-center justify-center rounded-md text-muted hover:bg-hover hover:text-ink focus-visible:outline-2 focus-visible:outline-accent"
        >
          {copied ? (
            <CheckIcon aria-hidden="true" className="size-4" />
          ) : (
            <ClipboardDocumentIcon aria-hidden="true" className="size-4" />
          )}
          <span className="sr-only">{copied ? "Copied" : `Copy: ${command}`}</span>
        </button>
      )}
    </div>
  );
}
