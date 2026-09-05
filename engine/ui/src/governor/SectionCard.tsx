import type { ReactNode } from "react";
import type { SectionAvailability } from "@rocky-types/brief";
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
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          {emptyLine}
          {note ? ` (${note})` : ""}
        </p>
      );
      break;
    case "unavailable":
      body = (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          {NOT_RECORDED}
          {note ? `: ${note}` : ""}
        </p>
      );
      break;
  }
  return (
    <section
      aria-label={title}
      className="rounded-md border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900"
    >
      <div className="mb-2 flex items-baseline justify-between gap-3">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">{title}</h3>
        <div className="flex items-baseline gap-2 text-[11px] text-zinc-500 dark:text-zinc-400">
          {availability === "available" && summary != null && <span>{summary}</span>}
          <span className="uppercase tracking-wide">{availability.replace("_", " ")}</span>
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
  rows: (string | number)[][];
  ariaLabel: string;
}) {
  return (
    <table className="w-full text-left text-xs" aria-label={ariaLabel}>
      <thead className="text-zinc-500 dark:text-zinc-400">
        <tr>
          {columns.map((column) => (
            <th key={column} className="pr-3 font-medium">
              {column}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="text-zinc-900 dark:text-zinc-100">
        {rows.map((row, index) => (
          <tr key={index} className="border-t border-zinc-100 align-top dark:border-zinc-800">
            {row.map((cell, cellIndex) => (
              <td key={cellIndex} className="pr-3 break-all">
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
