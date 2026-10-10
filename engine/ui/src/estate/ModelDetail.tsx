import type { ReactNode } from "react";
import { XMarkIcon } from "@heroicons/react/20/solid";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import { StatusCard } from "../components";
import { orNotRecorded } from "../format";
import { type Resource, useResource } from "./useResource";

/**
 * One model, from `GET /api/v1/models/{name}` (U1-P6): what it reads and
 * feeds, its typed columns, and its SQL as text. A cut SQL text says so.
 */
export function ModelDetail({
  name,
  load,
  onClose,
  actions,
}: {
  name: string;
  load: (name: string) => Promise<ModelDetailOutput>;
  onClose: () => void;
  /** Run and Plan for this one model, drawn under its name. */
  actions?: ReactNode;
}) {
  const detail = useResource(() => load(name), [name]);
  return (
    <aside
      aria-label={`Model ${name}`}
      className="rounded-lg border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900"
    >
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="text-xs font-medium text-zinc-500 dark:text-zinc-400">Model</p>
          <h3 className="font-mono text-base font-semibold break-all text-zinc-900 dark:text-zinc-100">
            {name}
          </h3>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="-mr-1 inline-flex size-9 shrink-0 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-100 hover:text-zinc-900 focus-visible:outline-2 focus-visible:outline-orange-500 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-100"
        >
          <XMarkIcon aria-hidden="true" className="size-5" />
          <span className="sr-only">Close</span>
        </button>
      </div>
      {actions !== undefined && <div className="mb-4">{actions}</div>}
      <DetailBody name={name} detail={detail} />
    </aside>
  );
}

function DetailBody({ name, detail }: { name: string; detail: Resource<ModelDetailOutput> }) {
  switch (detail.kind) {
    case "loading":
      return <p className="text-xs text-zinc-500">Loading {name}…</p>;
    case "refused":
      return (
        <StatusCard
          label={`refused (${detail.error.status})`}
          value={detail.error.envelope.code}
          tone="risk"
          sub={detail.error.envelope.remediation_hint ?? detail.error.envelope.message}
        />
      );
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={detail.message} />;
    case "ready": {
      const model = detail.value;
      return (
        <div className="space-y-4 text-sm">
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5">
            <dt className="text-zinc-500 dark:text-zinc-400">file</dt>
            <dd className="break-all text-zinc-900 dark:text-zinc-100">{model.file_path}</dd>
            <dt className="text-zinc-500 dark:text-zinc-400">upstream</dt>
            <dd>{model.upstream.length > 0 ? model.upstream.join(", ") : "none"}</dd>
            <dt className="text-zinc-500 dark:text-zinc-400">downstream</dt>
            <dd>{model.downstream.length > 0 ? model.downstream.join(", ") : "none"}</dd>
            <dt className="text-zinc-500 dark:text-zinc-400">columns</dt>
            <dd>
              {model.columns.length}
              {model.has_star ? " (the SELECT has a *, so the list may be short)" : ""}
            </dd>
          </dl>
          {model.typed_columns && model.typed_columns.length > 0 && (
            <table className="w-full text-left text-xs">
              <thead className="text-zinc-500 dark:text-zinc-400">
                <tr>
                  <th className="pr-2 font-medium">column</th>
                  <th className="pr-2 font-medium">type</th>
                  <th className="font-medium">nullable</th>
                </tr>
              </thead>
              <tbody>
                {model.typed_columns.map((column) => (
                  <tr key={column.name}>
                    <td className="pr-2">{column.name}</td>
                    <td className="pr-2 font-mono">{column.data_type_display}</td>
                    <td>{column.nullable ? "yes" : "no"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <div>
            <div className="mb-1 text-xs text-zinc-500 dark:text-zinc-400">
              SQL
              {model.sql_truncated
                ? ` (cut at ${model.sql.length} of ${model.sql_bytes} bytes; the server caps model detail)`
                : ""}
            </div>
            <pre className="max-h-64 overflow-auto rounded-md bg-zinc-50 p-3 font-mono text-xs text-zinc-800 dark:bg-zinc-950 dark:text-zinc-200">
              {model.sql}
            </pre>
          </div>
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            {orNotRecorded(model.upstream.length)} upstream, {orNotRecorded(model.downstream.length)}{" "}
            downstream
          </p>
        </div>
      );
    }
  }
}
