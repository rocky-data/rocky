import { render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import type { HistoryOutput } from "@rocky-types/history";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import type { ProjectOutput } from "@rocky-types/project";
import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import dagFixture from "@rocky-fixtures/dag.json";
import historyFixture from "@rocky-fixtures/history.json";
import { ApiError } from "../api";
import { NOT_RECORDED } from "../format";
import { EstateScreen, type EstateLoaders } from "./EstateScreen";

const capturedDag = dagFixture as unknown as DagOutput;
const capturedHistory = historyFixture as unknown as HistoryOutput;

const NOW = Date.parse("2026-09-05T08:00:00Z");

const project: ProjectOutput = {
  name: "playground",
  config_path: "/tmp/playground/rocky.toml",
  pipelines: [{ name: "playground", pipeline_type: "transformation" }],
  adapters: [{ name: "default", adapter_type: "duckdb" }],
  models_compiled: 2,
  diagnostics: { total: 1, warnings: 1, has_errors: false },
  last_run: {
    run_id: "run-SENTINEL",
    started_at: "2026-09-05T07:59:00Z",
    finished_at: "2026-09-05T07:59:10Z",
    status: "Success",
    models_executed: 2,
    trigger: "Manual",
  },
};

const emptySchedule: ScheduleStatusOutput = {
  counts: { config_errors: 0, enabled: 0, in_flight: 0, overdue: 0, scheduled: 0, throttled: 0 },
  now: "2026-09-05T08:00:00Z",
  pipelines: [],
  tick_lock: { state: "never" },
  timezone: "UTC",
};

const detail = (name: string): ModelDetailOutput => ({
  name,
  file_path: `models/${name}.sql`,
  sql: `SELECT 1 AS id -- ${name}`,
  sql_bytes: 20,
  sql_truncated: false,
  has_star: false,
  columns: [{ name: "id" }],
  upstream: [],
  downstream: [],
  typed_columns: [
    { name: "id", data_type: { type: "Int64" } as never, data_type_display: "INT64", nullable: false },
  ],
});

function loaders(overrides: Partial<EstateLoaders> = {}): EstateLoaders {
  return {
    project: async () => project,
    dag: async () => capturedDag,
    runs: async () => capturedHistory,
    schedule: async () => emptySchedule,
    detail: async (name) => detail(name),
    ...overrides,
  };
}

describe("EstateScreen", () => {
  it("renders the playground's captured DAG and its newest run", async () => {
    render(<EstateScreen loaders={loaders()} refreshMs={0} now={NOW} />);
    const list = await screen.findByRole("list", { name: "Models in the DAG" });
    const items = within(list)
      .getAllByRole("listitem")
      .map((item) => item.textContent ?? "");
    for (const node of capturedDag.nodes) {
      expect(items.some((text) => text.startsWith(`${node.label} (layer`))).toBe(true);
    }
    const runs = await screen.findByRole("table", { name: "Runs" });
    const newest = capturedHistory.runs[0];
    expect(within(runs).getAllByText(newest.status).length).toBeGreaterThan(0);
    expect(within(runs).getAllByTitle(newest.run_id).length).toBeGreaterThan(0);
    expect(screen.getByText(`${capturedHistory.count} run(s) in the newest 50`)).toBeInTheDocument();
    expect(screen.getByText("No schedules configured")).toBeInTheDocument();
  });

  it("renders a hostile model name and SQL as text, never as markup", async () => {
    const hostile = '<img src=x onerror="alert(1)">';
    const dag: DagOutput = {
      ...capturedDag,
      nodes: [{ id: hostile, label: hostile, kind: "Model" }],
      edges: [],
      execution_layers: [[hostile]],
    };
    const { container } = render(
      <EstateScreen
        loaders={loaders({
          dag: async () => dag,
          detail: async (name) => ({ ...detail(name), sql: `SELECT '${hostile}'` }),
        })}
        refreshMs={0}
        now={NOW}
      />,
    );
    const list = await screen.findByRole("list", { name: "Models in the DAG" });
    const items = within(list)
      .getAllByRole("listitem")
      .map((item) => item.textContent ?? "");
    expect(items.some((text) => text.startsWith(hostile))).toBe(true);
    expect(container.querySelector("img")).toBeNull();
  });

  it("renders unrecorded schedule fields as the status and a config error in red text", async () => {
    const schedule: ScheduleStatusOutput = {
      ...emptySchedule,
      counts: { ...emptySchedule.counts, scheduled: 2, enabled: 1, config_errors: 1 },
      pipelines: [
        {
          pipeline: "core",
          enabled: true,
          cron: "0 * * * *",
          next_fire_at: null,
          last_attempt_outcome: null,
          consecutive_failures: 0,
          awaiting_first_anchor: true,
          catchup: "skip",
        },
        {
          pipeline: "broken",
          enabled: false,
          config_error: "cron does not parse",
          consecutive_failures: 3,
          awaiting_first_anchor: false,
          catchup: "skip",
        },
      ],
      tick_lock: { state: "free", heartbeat_age_seconds: 4 },
    };
    render(<EstateScreen loaders={loaders({ schedule: async () => schedule })} refreshMs={0} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Schedules" });
    const core = within(table).getByText("core").closest("tr") as HTMLElement;
    expect(within(core).getAllByText(NOT_RECORDED)).toHaveLength(2);
    expect(within(core).getByText("0 * * * *")).toBeInTheDocument();
    expect(within(table).getByText(/config error: cron does not parse/)).toBeInTheDocument();
    expect(screen.getByText("free")).toBeInTheDocument();
  });

  it("shows a refused producer's envelope in its own panel while the others render", async () => {
    const refused = new ApiError(503, {
      code: "engine_not_ready",
      message: "no compile yet",
      remediation_hint: "retry shortly",
    });
    render(
      <EstateScreen
        loaders={loaders({
          dag: async () => {
            throw refused;
          },
        })}
        refreshMs={0}
        now={NOW}
      />,
    );
    expect(await screen.findByText("engine_not_ready")).toBeInTheDocument();
    expect(screen.getByText("retry shortly")).toBeInTheDocument();
    expect(await screen.findByRole("table", { name: "Runs" })).toBeInTheDocument();
  });

  it("opens a model's detail on selection and renders its SQL as text", async () => {
    render(<EstateScreen loaders={loaders()} refreshMs={0} now={NOW} />);
    await screen.findByRole("list", { name: "Models in the DAG" });
    // The canvas cannot be clicked in jsdom (React Flow paints after a
    // measure), so the detail pane is exercised through the same loader the
    // click reaches.
    const { getByText, getByRole } = within(
      render(
        <DetailHarness name={capturedDag.nodes[0].id} load={loaders().detail} />,
      ).container,
    );
    await waitFor(() => expect(getByRole("complementary")).toBeInTheDocument());
    expect(getByText(`SELECT 1 AS id -- ${capturedDag.nodes[0].id}`)).toBeInTheDocument();
    expect(getByText("INT64")).toBeInTheDocument();
  });
});

import { ModelDetail } from "./ModelDetail";

function DetailHarness({ name, load }: { name: string; load: EstateLoaders["detail"] }) {
  return <ModelDetail name={name} load={load} onClose={() => {}} />;
}

describe("ProjectStrip", () => {
  it("renders the project's config, pipelines, adapters and newest run", async () => {
    render(<EstateScreen loaders={loaders()} refreshMs={0} now={NOW} />);
    expect(await screen.findByText("playground (transformation)")).toBeInTheDocument();
    expect(screen.getByText("default (duckdb)")).toBeInTheDocument();
    expect(screen.getByText("/tmp/playground/rocky.toml")).toBeInTheDocument();
    expect(screen.getByText("1 diagnostics, 1 warnings")).toBeInTheDocument();
    expect(screen.getByText(/Success · Manual · 2 model\(s\)/)).toBeInTheDocument();
  });

  it("renders a hostile project name as text and a config error in the card", async () => {
    const hostile = '<b onmouseover="alert(1)">x</b>';
    const { container } = render(
      <EstateScreen
        loaders={loaders({
          project: async () => ({
            ...project,
            name: hostile,
            config_error: "rocky.toml: expected a table",
            pipelines: [],
            adapters: [],
          }),
        })}
        refreshMs={0}
        now={NOW}
      />,
    );
    expect(await screen.findByText(hostile)).toBeInTheDocument();
    expect(screen.getByText("rocky.toml: expected a table")).toBeInTheDocument();
    expect(container.querySelector("b")).toBeNull();
  });

  it("says when no run was recorded", async () => {
    render(
      <EstateScreen
        loaders={loaders({ project: async () => ({ ...project, last_run: undefined }) })}
        refreshMs={0}
        now={NOW}
      />,
    );
    expect(await screen.findByText("the state store holds no run yet")).toBeInTheDocument();
  });
});
