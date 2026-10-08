import type { JobStatus } from "@rocky-types/job_status";
import type { ProjectOutput } from "@rocky-types/project";
import {
  JobLine,
  WriteButton,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobClient,
} from "../operator";
import { reviewPath } from "../review/paths";
import { navigateTo } from "../router";

/** The plan id a finished plan job wrote, when its result names one. */
function planIdOf(job: JobStatus): string | null {
  const id = (job.result as { plan_id?: unknown } | undefined)?.plan_id;
  return typeof id === "string" && id.length > 0 ? id : null;
}

/**
 * Why the Plan button cannot help this project, or `undefined` when it can.
 *
 * `POST /api/v1/jobs/plan` runs `rocky plan` with no `--pipeline`. That
 * command plans one replication pipeline: it refuses a transformation-only
 * project, and a project with several pipelines needs `--pipeline`. A button
 * that always fails is worse than one that says why. Unknown (the project
 * read has not landed) leaves the button live; the job's own refusal is shown.
 */
export function planUnavailable(project: ProjectOutput | null): string | undefined {
  if (project === null) return undefined;
  const pipelines = project.pipelines;
  if (!pipelines.some((p) => p.pipeline_type === "replication")) {
    return "rocky plan plans a replication pipeline, and this project has none. Use Run, or plan in a terminal.";
  }
  if (pipelines.length > 1) {
    return "rocky plan needs --pipeline in a project with several pipelines. Plan in a terminal.";
  }
  return undefined;
}

/**
 * Run and Plan, for the whole project or for one model.
 *
 * Each submits a job (`POST /api/v1/jobs/run` or `/plan`), follows it by
 * polling `GET /api/v1/jobs/{id}`, and calls `onDone` once it ends so the
 * estate reads what the job changed. In read-only mode both are drawn
 * disabled with the reason. A run while another run, apply or approve holds
 * the project answers `409 mutation_in_progress`, said plainly.
 */
export function ProjectActions({
  model,
  jobs = defaultJobClient,
  onDone,
  planDisabledReason,
}: {
  /** One model (`--model`), or the whole project when absent. */
  model?: string;
  jobs?: JobClient;
  onDone?: () => void;
  /** Why Plan cannot help here ([`planUnavailable`]). */
  planDisabledReason?: string;
}) {
  const run = useJob("run", jobs, onDone);
  const plan = useJob("plan", jobs, onDone);
  const body = model === undefined ? {} : { model };
  const scope = model === undefined ? "the project" : model;
  const planned = plan.view.kind === "done" ? planIdOf(plan.view.job) : null;
  return (
    <div className="space-y-1">
      <div className="flex flex-wrap items-start gap-2">
        <WriteButton
          label={model === undefined ? "Plan" : "Plan this model"}
          busyLabel="Planning…"
          busy={jobBusy(plan.view)}
          disabledReason={planDisabledReason}
          onClick={() => plan.start(body)}
        />
        <WriteButton
          label={model === undefined ? "Run" : "Run this model"}
          busyLabel="Running…"
          busy={jobBusy(run.view)}
          onClick={() => run.start(body)}
        />
      </div>
      <JobLine label={`Plan ${scope}`} view={plan.view} />
      {planned !== null && (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">
          Plan written:{" "}
          <a
            href={reviewPath(planned)}
            onClick={(event) => {
              event.preventDefault();
              navigateTo(reviewPath(planned));
            }}
            className="font-mono underline"
          >
            {planned.slice(0, 12)}
          </a>
        </p>
      )}
      <JobLine label={`Run ${scope}`} view={run.view} />
    </div>
  );
}
