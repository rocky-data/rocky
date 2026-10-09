import type { ProjectOutput } from "@rocky-types/project";
import type { ReactNode } from "react";
import { Clip, ToneDot, runStatusTone, type Tone } from "../components";
import { formatInstant, orNotRecorded } from "../format";

/**
 * The project this sidecar serves, from `GET /api/v1/project`: what the
 * retired dashboard showed, as one strip of facts. Every value is text.
 */
export function ProjectStrip({ project, now }: { project: ProjectOutput; now?: number }) {
  const diagnosticsTone = project.diagnostics.has_errors
    ? "risk"
    : project.diagnostics.warnings > 0
      ? "warn"
      : "ok";
  // A compile that produced no result is its own state, with its reason:
  // the counts would describe a compile that no longer exists (#1823). And
  // before the first compile finishes there is nothing to count yet: that
  // is pending, not "0 diagnostics" in the healthy tone.
  const compilePending = project.models_compiled == null && !project.compile_error;
  const compileTone = compilePending ? "pending" : diagnosticsTone;
  const compileSub = project.compile_error
    ? `compile failed: ${project.compile_error}`
    : compilePending
      ? "compile pending"
      : `${project.diagnostics.total} diagnostics, ${project.diagnostics.warnings} warnings${
          project.diagnostics.has_errors ? ", errors" : ""
        }`;
  const list = (items: { name: string; kind: string }[]) =>
    items.length === 0 ? "none" : items.map((item) => `${item.name} (${item.kind})`).join(", ");

  return (
    // One strip, not five cards: the project is one thing, and five boxes of
    // equal weight read as five things to check. A cell keeps the card's
    // label, value, sub-line and tone dot, so no fact is lost.
    // The 1px gap over a border-coloured ground draws the lines between
    // cells, so no cell sets its own border and none doubles the frame's.
    <dl className="grid gap-px overflow-hidden rounded-lg border border-zinc-200 bg-zinc-200 sm:grid-cols-2 lg:grid-cols-5 dark:border-zinc-800 dark:bg-zinc-800">
      <Cell
        label="project"
        value={project.name}
        tone={project.config_error ? "risk" : "ok"}
        sub={project.config_error ?? orNotRecorded(project.config_path)}
      />
      <Cell
        label="pipelines"
        value={project.pipelines.length}
        sub={list(project.pipelines.map((p) => ({ name: p.name, kind: p.pipeline_type })))}
      />
      <Cell
        label="adapters"
        value={project.adapters.length}
        sub={list(project.adapters.map((a) => ({ name: a.name, kind: a.adapter_type })))}
      />
      <Cell
        label="models compiled"
        value={orNotRecorded(project.models_compiled)}
        tone={compileTone}
        sub={compileSub}
      />
      <Cell
        label="newest run"
        value={project.last_run ? <Clip value={project.last_run.run_id} keepEnds /> : orNotRecorded(null)}
        tone={project.last_run ? runStatusTone(project.last_run.status) : "pending"}
        sub={
          project.last_run
            ? `${project.last_run.status} · ${project.last_run.trigger} · ${project.last_run.models_executed} model(s) · ${formatInstant(project.last_run.started_at, now)}`
            : "the state store holds no run yet"
        }
      />
    </dl>
  );
}

/** One fact of the strip. Every value is text. */
function Cell({
  label,
  value,
  tone = "muted",
  sub,
}: {
  label: string;
  value: ReactNode;
  tone?: Tone;
  sub: ReactNode;
}) {
  return (
    <div
      data-tone={tone}
      className="min-w-0 bg-white p-4 dark:bg-zinc-900"
    >
      <dt className="flex items-center gap-2 text-xs font-medium text-zinc-500 dark:text-zinc-400">
        <ToneDot tone={tone} />
        <span className="inline-block first-letter:uppercase">{label}</span>
      </dt>
      {/* One definition per term: the sub-line belongs to the value, and a
          second <dd> is announced as an unlabelled definition of its own. */}
      <dd className="mt-1">
        <span className="block text-sm font-semibold break-words text-zinc-900 dark:text-zinc-100">{value}</span>
        <span className="mt-1 block text-xs break-words text-zinc-600 dark:text-zinc-400">{sub}</span>
      </dd>
    </div>
  );
}
