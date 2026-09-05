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
}: {
  name: string;
  load: (name: string) => Promise<ModelDetailOutput>;
  onClose: () => void;
}) {
  const detail = useResource(() => load(name), [name]);
  return (
    <aside
      aria-label={`Model ${name}`}
      className="rounded-md border border-zinc-200 bg-white p-3 dark:border-zinc-700 dark:bg-zinc-900"
    >
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">{name}</h3>
        <button
          type="button"
          onClick={onClose}
          className="text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
        >
          Close
        </button>
      </div>
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
        <div className="space-y-3 text-xs">
          <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1">
            <dt className="text-zinc-500">file</dt>
            <dd className="break-all text-zinc-900 dark:text-zinc-100">{model.file_path}</dd>
            <dt className="text-zinc-500">upstream</dt>
            <dd>{model.upstream.length > 0 ? model.upstream.join(", ") : "none"}</dd>
            <dt className="text-zinc-500">downstream</dt>
            <dd>{model.downstream.length > 0 ? model.downstream.join(", ") : "none"}</dd>
            <dt className="text-zinc-500">columns</dt>
            <dd>
              {model.columns.length}
              {model.has_star ? " (the SELECT has a *, so the list may be short)" : ""}
            </dd>
          </dl>
          {model.typed_columns && model.typed_columns.length > 0 && (
            <table className="w-full text-left">
              <thead className="text-zinc-500">
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
            <div className="mb-1 text-zinc-500">
              SQL
              {model.sql_truncated
                ? ` (cut at ${model.sql.length} of ${model.sql_bytes} bytes; the server caps model detail)`
                : ""}
            </div>
            <pre className="max-h-64 overflow-auto rounded bg-zinc-50 p-2 font-mono text-[11px] text-zinc-800 dark:bg-zinc-950 dark:text-zinc-200">
              {model.sql}
            </pre>
          </div>
          <p className="text-zinc-500">
            {orNotRecorded(model.upstream.length)} upstream, {orNotRecorded(model.downstream.length)}{" "}
            downstream
          </p>
        </div>
      );
    }
  }
}
