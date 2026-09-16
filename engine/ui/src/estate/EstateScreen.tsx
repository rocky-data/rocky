import { useEffect, useMemo, useState, type ReactNode } from "react";
import type { DagOutput } from "@rocky-types/dag";
import type { HistoryOutput } from "@rocky-types/history";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import type { ModelListOutput } from "@rocky-types/model_list";
import type { ProjectOutput } from "@rocky-types/project";
import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import { apiGet } from "../api";
import { StatusCard } from "../components";
import { DagPanel } from "./DagPanel";
import { ModelDetail } from "./ModelDetail";
import { type CompiledModels, anyNotCompiled, compiledModels, isWholeList } from "./nodeRoute";
import { ProjectStrip } from "./ProjectStrip";
import { RunsPanel } from "./RunsPanel";
import { SchedulePanel } from "./SchedulePanel";
import { type Resource, useResource } from "./useResource";

/** The producers this screen reads. Tests hand in fixtures. */
export interface EstateLoaders {
  project: () => Promise<ProjectOutput>;
  dag: () => Promise<DagOutput>;
  /** The compiled model set: which DAG nodes the detail route can serve. */
  models: () => Promise<ModelListOutput>;
  runs: () => Promise<HistoryOutput>;
  schedule: () => Promise<ScheduleStatusOutput>;
  detail: (name: string) => Promise<ModelDetailOutput>;
}

export const defaultLoaders: EstateLoaders = {
  project: () => apiGet<ProjectOutput>("project"),
  dag: () => apiGet<DagOutput>("dag"),
  models: () => apiGet<ModelListOutput>("models"),
  runs: () => apiGet<HistoryOutput>("runs"),
  schedule: () => apiGet<ScheduleStatusOutput>("schedule"),
  detail: (name) => apiGet<ModelDetailOutput>(`models/${encodeURIComponent(name)}`),
};

/** Runs and the schedule are ledger reads and refresh on their own; the DAG composes per request and refreshes on demand. */
export const LEDGER_REFRESH_MS = 30_000;

/**
 * How often the model list is read again while the graph shows a model
 * outside it. `/dag` reads the files on disk; `/models` reads the last
 * compile, which `serve --watch` replaces a moment after a file changes. A
 * model marked "not compiled" only because the compile had not caught up
 * then opens within this interval, without a Refresh.
 */
export const COMPILE_RECHECK_MS = 5_000;

/**
 * The estate: the DAG, the runs list, the schedule status. Each panel owns
 * its producer's state, so one refused route shows its envelope while the
 * others render.
 */
export function EstateScreen({
  loaders = defaultLoaders,
  refreshMs = LEDGER_REFRESH_MS,
  recheckMs = COMPILE_RECHECK_MS,
  now,
}: {
  loaders?: EstateLoaders;
  refreshMs?: number;
  recheckMs?: number;
  now?: number;
}) {
  const project = useResource(loaders.project, [loaders], refreshMs);
  const dag = useResource(loaders.dag, [loaders]);
  // Read with the DAG on Refresh, and again on an interval only while the
  // graph shows a model the list does not have.
  const [recheck, setRecheck] = useState(false);
  const models = useResource(loaders.models, [loaders], recheck ? recheckMs : undefined);
  const runs = useResource(loaders.runs, [loaders], refreshMs);
  const schedule = useResource(loaders.schedule, [loaders], refreshMs);
  const [selected, setSelected] = useState<string | null>(null);
  // Bumped by Refresh, so an open model's detail is read again too.
  const [generation, setGeneration] = useState(0);

  // Keyed on the names, not on the list object. Every recheck delivers a new
  // object; an unchanged set keeps its identity, so the graph is not laid out
  // again for it.
  const fromList: CompiledModels =
    models.kind === "ready" ? compiledModels(models.value) : "unknown";
  const namesKey = fromList === "unknown" ? null : JSON.stringify([...fromList].sort());
  const compiled = useMemo<CompiledModels>(
    () => (namesKey === null ? "unknown" : new Set(JSON.parse(namesKey) as string[])),
    [namesKey],
  );

  const outsideCompile = dag.kind === "ready" && anyNotCompiled(dag.value.nodes, compiled);
  useEffect(() => setRecheck(outsideCompile), [outsideCompile]);

  // An open model the server no longer compiles closes: its node is now
  // drawn as having no detail, and the pane would contradict it.
  const shown = selected !== null && (compiled === "unknown" || compiled.has(selected)) ? selected : null;
  useEffect(() => {
    if (selected !== null && shown === null) setSelected(null);
  }, [selected, shown]);

  const refreshAll = () => {
    project.reload();
    dag.reload();
    models.reload();
    runs.reload();
    schedule.reload();
    setGeneration((g) => g + 1);
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

      <Panel title="DAG" producer="GET /api/v1/dag + GET /api/v1/models">
        <Loaded resource={dag}>
          {(value) => (
            <div className={shown ? "grid gap-3 lg:grid-cols-[1fr_360px]" : ""}>
              <div>
                <DagPanel dag={value} compiled={compiled} onSelect={setSelected} />
                <UnknownCompile models={models} />
              </div>
              {shown && (
                <ModelDetail
                  key={generation}
                  name={shown}
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

/**
 * Says so when the compiled model list could not be used, because then no
 * node is marked as outside the compile and one that is will open onto the
 * route's refusal. Nothing while the list is loading: that is not a failure.
 */
function UnknownCompile({ models }: { models: Resource<ModelListOutput> }) {
  let reason: string;
  switch (models.kind) {
    case "loading":
      return null;
    case "ready":
      if (isWholeList(models.value)) return null;
      reason = `the list says ${models.value.count} but carries ${models.value.models.length}`;
      break;
    case "refused":
      reason = `refused (${models.error.status}): ${models.error.envelope.code}`;
      break;
    case "unreachable":
      reason = "unreachable";
      break;
  }
  return (
    <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">
      Could not read which models the server compiled ({reason}). Every model is offered, and one
      outside the compile opens onto an error.
    </p>
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
