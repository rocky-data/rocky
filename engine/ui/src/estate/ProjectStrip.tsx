import type { ProjectOutput } from "@rocky-types/project";
import { StatusCard } from "../components";
import { formatInstant, orNotRecorded } from "../format";

/**
 * The project this sidecar serves, from `GET /api/v1/project`: what the
 * retired dashboard showed, as cards. Every value is text.
 */
export function ProjectStrip({ project, now }: { project: ProjectOutput; now?: number }) {
  const diagnosticsTone = project.diagnostics.has_errors
    ? "risk"
    : project.diagnostics.warnings > 0
      ? "warn"
      : "ok";
  const list = (items: { name: string; kind: string }[]) =>
    items.length === 0 ? "none" : items.map((item) => `${item.name} (${item.kind})`).join(", ");

  return (
    <div className="space-y-2">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatusCard
          label="project"
          value={project.name}
          tone={project.config_error ? "risk" : "ok"}
          sub={project.config_error ?? orNotRecorded(project.config_path)}
        />
        <StatusCard
          label="pipelines"
          value={project.pipelines.length}
          sub={list(project.pipelines.map((p) => ({ name: p.name, kind: p.pipeline_type })))}
        />
        <StatusCard
          label="adapters"
          value={project.adapters.length}
          sub={list(project.adapters.map((a) => ({ name: a.name, kind: a.adapter_type })))}
        />
        <StatusCard
          label="models compiled"
          value={orNotRecorded(project.models_compiled)}
          tone={diagnosticsTone}
          sub={`${project.diagnostics.total} diagnostics, ${project.diagnostics.warnings} warnings${
            project.diagnostics.has_errors ? ", errors" : ""
          }`}
        />
      </div>
      <StatusCard
        label="newest run"
        value={project.last_run ? project.last_run.run_id : orNotRecorded(null)}
        tone={project.last_run ? "muted" : "pending"}
        sub={
          project.last_run
            ? `${project.last_run.status} · ${project.last_run.trigger} · ${project.last_run.models_executed} model(s) · ${formatInstant(project.last_run.started_at, now)}`
            : "the state store holds no run yet"
        }
      />
    </div>
  );
}
