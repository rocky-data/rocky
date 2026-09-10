import { useState, type ReactNode } from "react";
import type { DagOutput } from "@rocky-types/dag";
import type { HistoryOutput } from "@rocky-types/history";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import type { ProjectOutput } from "@rocky-types/project";
import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import { apiGet } from "../api";
import { StatusCard } from "../components";
import { DagPanel } from "./DagPanel";
import { ModelDetail } from "./ModelDetail";
import { ProjectStrip } from "./ProjectStrip";
import { RunsPanel } from "./RunsPanel";
import { SchedulePanel } from "./SchedulePanel";
import { type Resource, useResource } from "./useResource";

/** The four producers this screen reads. Tests hand in fixtures. */
export interface EstateLoaders {
  project: () => Promise<ProjectOutput>;
  dag: () => Promise<DagOutput>;
  runs: () => Promise<HistoryOutput>;
  schedule: () => Promise<ScheduleStatusOutput>;
  detail: (name: string) => Promise<ModelDetailOutput>;
}

export const defaultLoaders: EstateLoaders = {
  project: () => apiGet<ProjectOutput>("project"),
  dag: () => apiGet<DagOutput>("dag"),
  runs: () => apiGet<HistoryOutput>("runs"),
  schedule: () => apiGet<ScheduleStatusOutput>("schedule"),
  detail: (name) => apiGet<ModelDetailOutput>(`models/${encodeURIComponent(name)}`),
};

/** Runs and the schedule are ledger reads and refresh on their own; the DAG composes per request and refreshes on demand. */
export const LEDGER_REFRESH_MS = 30_000;

/**
 * The estate: the DAG, the runs list, the schedule status. Each panel owns
 * its producer's state, so one refused route shows its envelope while the
 * others render.
 */
export function EstateScreen({
  loaders = defaultLoaders,
  refreshMs = LEDGER_REFRESH_MS,
  now,
}: {
  loaders?: EstateLoaders;
  refreshMs?: number;
  now?: number;
}) {
  const project = useResource(loaders.project, [loaders], refreshMs);
  const dag = useResource(loaders.dag, [loaders]);
  const runs = useResource(loaders.runs, [loaders], refreshMs);
  const schedule = useResource(loaders.schedule, [loaders], refreshMs);
  const [selected, setSelected] = useState<string | null>(null);

  const refreshAll = () => {
    project.reload();
    dag.reload();
    runs.reload();
    schedule.reload();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-medium uppercase tracking-wide text-zinc-500">Estate</h2>
        <button
          type="button"
          onClick={refreshAll}
          className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
        >
          Refresh
        </button>
      </div>

      <Panel title="Project" producer="GET /api/v1/project">
        <Loaded resource={project}>{(value) => <ProjectStrip project={value} now={now} />}</Loaded>
      </Panel>

      <Panel title="DAG" producer="GET /api/v1/dag">
        <Loaded resource={dag}>
          {(value) => (
            <div className={selected ? "grid gap-3 lg:grid-cols-[1fr_360px]" : ""}>
              <DagPanel dag={value} onSelect={setSelected} />
              {selected && (
                <ModelDetail
                  name={selected}
                  load={loaders.detail}
                  onClose={() => setSelected(null)}
                />
              )}
            </div>
          )}
        </Loaded>
      </Panel>

      <Panel title="Runs" producer="GET /api/v1/runs">
        <Loaded resource={runs}>{(value) => <RunsPanel history={value} now={now} />}</Loaded>
      </Panel>

      <Panel title="Schedule" producer="GET /api/v1/schedule">
        <Loaded resource={schedule}>
          {(value) => <SchedulePanel status={value} now={now} />}
        </Loaded>
      </Panel>
    </div>
  );
}

/**
 * One panel and the route behind it.
 *
 * The route is a tooltip, not a line of the page. It answers "where does this
 * come from" for the reader who asks, and costs nothing for the reader who
 * does not — the idiom the governor tabs already use (`GovernorScreen`).
 */
function Panel({ title, producer, children }: { title: string; producer: string; children: ReactNode }) {
  const descriptionId = `producer-${title.toLowerCase().replace(/\s+/g, "-")}`;
  return (
    <section aria-label={title}>
      {/*
        The route reaches a mouse through `title` and assistive technology
        through `aria-describedby`. Both, because `title` alone is a mouse
        affordance — not in the tab order, and inconsistently announced.
        The description element is `hidden`, which keeps it out of the
        reading order while `aria-describedby` still takes its text: an
        `sr-only` sibling would be read a second time, since an otherwise
        unused `title` already becomes the accessible description.
      */}
      <h3
        className="mb-2 text-base font-semibold text-zinc-900 dark:text-zinc-100"
        title={producer}
        aria-describedby={descriptionId}
      >
        {title}
      </h3>
      <span id={descriptionId} hidden>
        {producer}
      </span>
      {children}
    </section>
  );
}

function Loaded<T>({ resource, children }: { resource: Resource<T>; children: (value: T) => ReactNode }) {
  switch (resource.kind) {
    case "loading":
      return <p className="text-sm text-zinc-500">Loading…</p>;
    case "refused":
      return (
        <StatusCard
          label={`refused (${resource.error.status})`}
          value={resource.error.envelope.code}
          tone="risk"
          sub={resource.error.envelope.remediation_hint ?? resource.error.envelope.message}
        />
      );
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={resource.message} />;
    case "ready":
      return <>{children(resource.value)}</>;
  }
}
