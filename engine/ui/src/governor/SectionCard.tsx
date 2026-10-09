import type { ReactNode } from "react";
import type { SectionAvailability } from "@rocky-types/brief";
import { TABLE } from "../components";
import { NOT_RECORDED } from "../format";

/**
 * One section of a governor digest, rendered by its `availability`, the
 * fail-closed rule made visible: `available` shows the rows, `no_data` says
 * the window held nothing, `unavailable` shows the engine's note and no
 * rows at all. A section never invents a value.
 */
export function SectionCard({
  title,
  availability,
  note,
  emptyLine = "nothing in the window",
  summary,
  children,
}: {
  title: string;
  availability: SectionAvailability;
  note?: string | null;
  emptyLine?: string;
  /** A one-line total shown beside the title when the section is available. */
  summary?: ReactNode;
  children: ReactNode;
}) {
  let body: ReactNode;
  switch (availability) {
    case "available":
      body = children;
      break;
    case "no_data":
      body = (
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          {emptyLine}
          {note ? ` (${note})` : ""}
        </p>
      );
      break;
    case "unavailable":
      body = (
        <p className="text-sm text-amber-700 dark:text-amber-400">
          {NOT_RECORDED}
          {note ? `: ${note}` : ""}
        </p>
      );
      break;
  }
  return (
    <section
      aria-label={title}
      className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900"
    >
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">{title}</h3>
        <div className="flex items-baseline gap-2 text-xs text-zinc-500 dark:text-zinc-400">
          {availability === "available" && summary != null && <span>{summary}</span>}
          {/* "available" is the normal case and goes unsaid; the other two
              change how the body reads, so they are named. */}
          {availability !== "available" && (
            <span className="rounded-full border border-zinc-300 px-2 py-0.5 dark:border-zinc-700">
              {availability.replace("_", " ")}
            </span>
          )}
        </div>
      </div>
      {body}
    </section>
  );
}

/** A compact key/value table for a section's rows; every cell is text. */
export function Rows({
  columns,
  rows,
  ariaLabel,
}: {
  columns: string[];
  rows: ReactNode[][];
  ariaLabel: string;
}) {
  return (
    // The scroller is on the table, not the page: without it the widest cell
    // pushes the whole page body sideways on a phone.
    <div
      className={TABLE.scroller}
      tabIndex={0}
      role="group"
      aria-label={`${ariaLabel}, scrollable`}
    >
    <table className={TABLE.table} aria-label={ariaLabel}>
      <thead className={TABLE.head}>
        <tr>
          {columns.map((column) => (
            <th key={column} className={TABLE.th}>
              <span className="inline-block first-letter:uppercase">{column}</span>
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="text-zinc-900 dark:text-zinc-100">
        {rows.map((row, index) => (
          <tr key={index} className={TABLE.row}>
            {row.map((cell, cellIndex) => (
              <td key={cellIndex} className={`${TABLE.td} max-w-md break-all`}>
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
    </div>
  );
}
