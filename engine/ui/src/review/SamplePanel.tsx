import { useCallback, useState } from "react";
import type { PreviewRowsOutput } from "@rocky-types/preview_rows";
import { apiGet } from "../api";
import { useResource } from "../estate/useResource";
import { ResourceState } from "./ResourceState";

/** Rows to ask for. Well under the route's cap of 500. */
export const SAMPLE_LIMIT = 20;

export type SampleLoader = (model: string) => Promise<PreviewRowsOutput>;

/**
 * The one call in the UI that can spend warehouse money, so it is the one call
 * that carries the consent header — written here, at its call site, and sent
 * only when the viewer has pressed the button.
 */
export const defaultSampleLoader: SampleLoader = (model) =>
  apiGet<PreviewRowsOutput>(
    `models/${encodeURIComponent(model)}/rows?limit=${SAMPLE_LIMIT}`,
    { headers: { "X-Rocky-Allow-Warehouse": "true" } },
  );

function SampleTable({ sample }: { sample: PreviewRowsOutput }) {
  return (
    <div className="space-y-2">
      <div className="overflow-x-auto">
        <table className="min-w-full text-left text-xs">
          <thead>
            <tr className="border-b border-zinc-200 dark:border-zinc-700">
              {sample.columns.map((column) => (
                <th key={column} className="px-2 py-1 font-mono font-semibold whitespace-nowrap">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sample.rows.map((row, index) => (
              // Rows carry no key of their own; a sample is a snapshot that is
              // replaced wholesale, never reordered in place.
              <tr key={index} className="border-b border-zinc-100 dark:border-zinc-800">
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex} className="px-2 py-1 font-mono whitespace-nowrap">
                    {cell === null ? (
                      <span className="text-zinc-400 dark:text-zinc-500">null</span>
                    ) : (
                      String(cell)
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-[11px] text-zinc-500 dark:text-zinc-400">
        {sample.row_count} {sample.row_count === 1 ? "row" : "rows"}
        {sample.truncated ? ` (capped at ${sample.limit_applied})` : ""} from{" "}
        {sample.adapter_kind}. Classification-tagged columns are masked by the engine before the
        rows leave it; a hashed column is pseudonymous, not anonymous.
      </p>
      <details className="text-[11px] text-zinc-500 dark:text-zinc-400">
        <summary className="cursor-pointer">the SQL that ran</summary>
        <pre className="mt-1 overflow-x-auto rounded bg-zinc-50 p-2 font-mono dark:bg-zinc-800">
          {sample.executed_sql}
        </pre>
      </details>
    </div>
  );
}

/**
 * A sample of one model's rows, fetched only when the viewer asks.
 *
 * Nothing is requested on load. A page that sampled on load would spend
 * warehouse money every time someone opened a link, and a refresh would spend
 * it again; the button says what it will do before it does it. Each refusal
 * renders as itself through [`ResourceState`], because "the adapter cannot
 * express this column's mask" and "nobody consented" are different problems
 * with different fixes.
 */
export function SamplePanel({
  model,
  load = defaultSampleLoader,
}: {
  model: string;
  load?: SampleLoader;
}) {
  const [asked, setAsked] = useState(false);
  const loader = useCallback(() => load(model), [load, model]);
  // The hook fetches on mount, so it is mounted only once the viewer asks.
  return (
    <section aria-label="Sample rows" className="space-y-2">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Sample rows</h3>
        <span className="font-mono text-[11px] text-zinc-500 dark:text-zinc-400">{model}</span>
      </div>
      {asked ? (
        <AskedSample loader={loader} />
      ) : (
        <div className="space-y-2">
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            Reading {SAMPLE_LIMIT} rows runs a query against the warehouse, which costs what that
            query costs. Nothing is read until you ask.
          </p>
          <button
            type="button"
            onClick={() => setAsked(true)}
            className="rounded border border-zinc-300 px-2 py-1 text-xs font-medium text-zinc-800 hover:bg-zinc-50 dark:border-zinc-600 dark:text-zinc-100 dark:hover:bg-zinc-800"
          >
            Show {SAMPLE_LIMIT} rows
          </button>
        </div>
      )}
    </section>
  );
}

/** Mounted only after the viewer asks, so the fetch happens then and not before. */
function AskedSample({ loader }: { loader: () => Promise<PreviewRowsOutput> }) {
  const sample = useResource(loader, [loader]);
  if (sample.kind !== "ready") {
    return <ResourceState resource={sample} loadingLine="running the query…" />;
  }
  return <SampleTable sample={sample.value} />;
}
