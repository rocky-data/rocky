/**
 * The estate screen with a real React Flow canvas, so a test can open a
 * model by clicking its node and then watch what Refresh does to the pane.
 * `EstateScreen.test.tsx` has no canvas and drives the pane through its
 * loader instead.
 */
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import type { HistoryOutput } from "@rocky-types/history";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import type { ModelListOutput } from "@rocky-types/model_list";
import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import historyFixture from "@rocky-fixtures/history.json";
import { installFlowCanvas, nodeElement } from "../test/flowCanvas";
import twoPipelinesDag from "../test/fixtures/dag-two-pipelines.json";
import twoPipelinesModels from "../test/fixtures/model-list-two-pipelines.json";
import { EstateScreen, type EstateLoaders } from "./EstateScreen";

beforeAll(installFlowCanvas);

const dag = twoPipelinesDag as unknown as DagOutput;
const compiledAtStart = twoPipelinesModels as unknown as ModelListOutput;
const RAW = "transformation:raw_orders";

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
  typed_columns: [],
});

function loaders(models: () => Promise<ModelListOutput>, load: EstateLoaders["detail"]): EstateLoaders {
  return {
    project: async () => ({
      name: "p",
      config_path: "rocky.toml",
      pipelines: [],
      adapters: [],
      diagnostics: { total: 0, warnings: 0, has_errors: false },
    }),
    dag: async () => dag,
    models,
    runs: async () => historyFixture as unknown as HistoryOutput,
    schedule: async () =>
      ({
        counts: { config_errors: 0, enabled: 0, in_flight: 0, overdue: 0, scheduled: 0, throttled: 0 },
        now: "2026-09-05T08:00:00Z",
        pipelines: [],
        tick_lock: { state: "never" },
        timezone: "UTC",
      }) as ScheduleStatusOutput,
    detail: load,
  };
}

/** Open `raw_orders` by clicking its node, and wait for its pane. */
async function openRawOrders(): Promise<void> {
  await waitFor(() => expect(nodeElement(RAW)).toHaveAttribute("role", "button"));
  fireEvent.click(nodeElement(RAW));
  await screen.findByRole("complementary", { name: "Model raw_orders" });
}

describe("the open model pane", () => {
  it("reads the open model again on Refresh", async () => {
    const load = vi.fn(async (name: string) => detail(name));
    render(
      <EstateScreen loaders={loaders(async () => compiledAtStart, load)} refreshMs={0} recheckMs={60_000} />,
    );
    await openRawOrders();
    await waitFor(() => expect(load).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    expect(load).toHaveBeenLastCalledWith("raw_orders");
  });

  it("closes when the list says the server no longer compiles that model", async () => {
    // raw_orders moved to another pipeline's directory and the server
    // recompiled: its node turns dashed, and a pane still showing its detail
    // would contradict the node beside it.
    let moved = false;
    const withoutRaw: ModelListOutput = {
      count: compiledAtStart.count - 1,
      models: compiledAtStart.models.filter((m) => m.name !== "raw_orders"),
    };
    const models = async () => (moved ? withoutRaw : compiledAtStart);
    render(
      <EstateScreen
        loaders={loaders(models, async (name) => detail(name))}
        refreshMs={0}
        recheckMs={60_000}
      />,
    );
    await openRawOrders();

    moved = true;
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() =>
      expect(screen.queryByRole("complementary", { name: "Model raw_orders" })).toBeNull(),
    );
    // React Flow copies new node data into its own store in an effect, so the
    // card can repaint a render after the pane has closed.
    await waitFor(() => expect(nodeElement(RAW).firstElementChild).toHaveClass("border-dashed"));
  });
});
