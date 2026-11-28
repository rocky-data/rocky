import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BriefOutput } from "@rocky-types/brief";
import type { HistoryOutput, RunHistoryRecord } from "@rocky-types/history";
import { ApiError } from "../api";
import { BriefScreen } from "./BriefScreen";
import { bucketCounts, groupLanes, laneHealth, modelFill, runBucket, windowPhrase } from "./runLaneModel";

const NOW = Date.parse("2026-09-05T08:00:00Z");

function run(
  id: string,
  status: string,
  started: string,
  pipeline: string | null,
  models: [string, string][] = [],
): RunHistoryRecord {
  return {
    run_id: id,
    status,
    started_at: started,
    trigger: "Manual",
    pipeline,
    duration_ms: 1000,
    models_executed: models.length,
    models: models.map(([name, s]) => ({ model_name: name, status: s, duration_ms: 10 })),
  };
}

// Newest first, as the route returns them.
const RUNS: RunHistoryRecord[] = [
  run("run-5", "PartialFailure", "2026-09-05T07:00:00Z", "ingest", [
    ["orders", "success"],
    ["products", "failed"],
  ]),
  run("run-4", "Success", "2026-09-05T06:00:00Z", "transform", [["fct_orders", "success"]]),
  run("run-3", "Failure", "2026-09-05T05:00:00Z", null),
  run("run-2", "SkippedIdempotent", "2026-09-05T04:00:00Z", "ingest"),
  run("run-1", "Success", "2026-09-05T03:00:00Z", "ingest", [["orders", "success"]]),
];

const HISTORY: HistoryOutput = { version: "1.95.0", command: "history", count: RUNS.length, runs: RUNS };

/** A digest with every section empty, so only the runs block carries numbers. */
const QUIET: BriefOutput = {
  version: "1.95.0",
  command: "brief",
  generated_at: "2026-09-05T07:59:00Z",
  since_mode: "7d",
  since_timestamp: "2026-08-29T07:59:00Z",
  escalations: { availability: "no_data", total: 0, ranking: "", pending: [] },
  agent_activity: {
    availability: "no_data",
    total: 0,
    allow: 0,
    require_review: 0,
    deny: 0,
    by_principal: [],
    decisions: [],
  },
  runs: { availability: "no_data", total: 0, succeeded: 0, partial_failure: 0, failed: 0, attention: [] },
  autonomy: { availability: "no_data", degraded_rules: [], active_freezes: [] },
  cost: { availability: "no_data", run_count: 0, total_duration_ms: 0, per_run: [] },
  drift: { availability: "no_data", events: [] },
  freshness: { availability: "no_data", models: [] },
  quality: { availability: "no_data", models: [] },
  scheduler: {
    availability: "no_data",
    scheduled_pipelines: 0,
    runs_in_window: 0,
    failed_in_window: 0,
    paused: [],
    consecutive_failures: [],
    incident_count: 0,
  },
} as BriefOutput;

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("run lane mapping", () => {
  it("reads each run status into one bucket, and the counts sum to the runs", () => {
    expect(runBucket("Success")).toBe("succeeded");
    expect(runBucket("PartialFailure")).toBe("partly_failed");
    expect(runBucket("Failure")).toBe("failed");
    expect(runBucket("SkippedInFlight")).toBe("skipped");
    expect(runBucket("SomethingNew")).toBe("other");
    expect(modelFill("success")).toBe("bg-ok");
    expect(modelFill("failed")).toBe("bg-fail");
    // A model status this does not know is neither ok nor failed.
    expect(modelFill("pending")).toBe("bg-line");
    const counts = bucketCounts(RUNS);
    expect(counts).toEqual({ succeeded: 2, partly_failed: 1, failed: 1, skipped: 1, other: 0 });
    expect(Object.values(counts).reduce((a, b) => a + b, 0)).toBe(RUNS.length);
  });

  it("groups by pipeline, oldest to newest in a lane, with the no-pipeline lane last", () => {
    const lanes = groupLanes(RUNS);
    expect(lanes.map((lane) => lane.pipeline)).toEqual(["ingest", "transform", null]);
    expect(lanes[0].runs.map((r) => r.run_id)).toEqual(["run-1", "run-2", "run-5"]);
    const health = laneHealth(lanes);
    expect(health[0]).toMatchObject({ ok: 1, total: 2, notOk: [{ model: "products", status: "failed" }] });
    expect(health[0].latest.run_id).toBe("run-5");
  });

  it("names the window: the route's cap, or all recorded runs when fewer", () => {
    expect(windowPhrase(50)).toBe("of the last 50 runs");
    expect(windowPhrase(5)).toBe("of the last 5 runs (all that are recorded)");
    expect(windowPhrase(1)).toBe("of the last 1 run (all that are recorded)");
  });
});

describe("Recent runs on the Inbox", () => {
  it("shows tiles with their window, one lane per pipeline, and each run links to its custody", async () => {
    render(<BriefScreen load={async () => QUIET} loadRuns={async () => HISTORY} now={NOW} />);
    await screen.findByRole("list", { name: "Run lanes" });
    const tiles = screen.getByRole("list", { name: "Run health" });
    const succeeded = within(tiles).getByRole("listitem", { name: "succeeded tile" });
    expect(succeeded).toHaveTextContent("2");
    expect(succeeded).toHaveTextContent("of the last 5 runs (all that are recorded)");
    expect(within(tiles).getByRole("listitem", { name: "failed tile" })).toHaveTextContent(/^Failed1of the last 5/);
    // Skipped has a run, so it gets a tile; other has none, so it does not.
    expect(within(tiles).getByRole("listitem", { name: "skipped tile" })).toHaveTextContent(/^Skipped1of/);
    expect(within(tiles).queryByRole("listitem", { name: "other status tile" })).toBeNull();
    let sum = 0;
    for (const item of within(tiles).getAllByRole("listitem")) {
      expect(item).toHaveTextContent(/of the last \d+ runs?/);
      sum += Number(/^\D+(\d+)of/.exec(item.textContent ?? "")?.[1]);
    }
    expect(sum).toBe(RUNS.length);

    const lanes = screen.getByRole("list", { name: "Run lanes" });
    const cell = within(lanes).getByRole("link", { name: /^run run-5 · Partly failed/ });
    expect(cell).toHaveAttribute("href", "/ui/governor/custody/run-5");
    expect(cell).toHaveAttribute("data-bucket", "partly_failed");
    expect(within(lanes).getAllByRole("link")).toHaveLength(RUNS.length);

    const health = screen.getByRole("listitem", { name: "ingest health" });
    expect(health).toHaveTextContent("1 of 2 models ok · products (failed)");
    expect(health).toHaveTextContent("newest run of this pipeline of the last 5 runs");
    for (const tile of within(screen.getByRole("list", { name: "Health by pipeline" })).getAllByRole("listitem")) {
      expect(tile).toHaveTextContent(/of the last \d+ runs?/);
    }
    expect(within(health).getByRole("link", { name: "run-5" })).toHaveAttribute(
      "href",
      "/ui/governor/custody/run-5",
    );
  });

  it("never shows an unread runs read as zero", async () => {
    const refused = async (): Promise<HistoryOutput> => {
      throw new ApiError(503, { code: "state_busy", message: "a run holds the state store" });
    };
    render(<BriefScreen load={async () => QUIET} loadRuns={refused} now={NOW} />);
    // The digest lands first, so the runs block has moved to its place under it.
    await screen.findByRole("region", { name: "Waiting on you" });
    const section = screen.getByRole("region", { name: "Recent runs" });
    await waitFor(() => expect(within(section).getAllByRole("img")[0].getAttribute("title")).toContain("state_busy"));
    const tiles = within(section).getByRole("list", { name: "Run health" });
    for (const name of ["succeeded tile", "partly failed tile", "failed tile"]) {
      const tile = within(tiles).getByRole("listitem", { name });
      expect(tile).not.toHaveTextContent(/\d+ ?of the last/);
      expect(tile.textContent).not.toMatch(/(^|[^\d])0([^\d]|$)/);
      const dash = within(tile).getByRole("img");
      expect(dash).toHaveTextContent("-");
      expect(dash.getAttribute("title")).toContain("state_busy");
    }
    expect(within(section).queryByRole("list", { name: "Run lanes" })).toBeNull();
  });

  it("says plainly when no run is recorded, a true empty read", async () => {
    const empty: HistoryOutput = { ...HISTORY, count: 0, runs: [] };
    render(<BriefScreen load={async () => QUIET} loadRuns={async () => empty} now={NOW} />);
    await screen.findByRole("region", { name: "Waiting on you" });
    const section = screen.getByRole("region", { name: "Recent runs" });
    expect(await within(section).findByText("No run is recorded in the state store yet.")).toBeInTheDocument();
  });

  it("keeps the runs block when the digest does not load", async () => {
    const load = async (): Promise<BriefOutput> => {
      throw new Error("engine down");
    };
    render(<BriefScreen load={load} loadRuns={async () => HISTORY} now={NOW} />);
    expect(await screen.findByRole("list", { name: "Run lanes" })).toBeInTheDocument();
  });

  it("places runs after what to act on and before the context", async () => {
    render(<BriefScreen load={async () => QUIET} loadRuns={async () => HISTORY} now={NOW} />);
    const lanes = await screen.findByRole("region", { name: "Recent runs" });
    const waiting = screen.getByRole("region", { name: "Waiting on you" });
    const summary = screen.getByRole("region", { name: "Summary" });
    const details = screen.getByText("Details");
    const follows = (a: Element, b: Element) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING) !== 0;
    expect(follows(summary, waiting)).toBe(true);
    expect(follows(waiting, lanes)).toBe(true);
    expect(follows(lanes, details)).toBe(true);
  });

  it("reads the runs once: no interval, and the window does not read them again", async () => {
    vi.useFakeTimers();
    const loadRuns = vi.fn(async () => HISTORY);
    const load = vi.fn(async () => QUIET);
    render(<BriefScreen load={load} loadRuns={loadRuns} now={NOW} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10 * 60_000);
    });
    expect(loadRuns).toHaveBeenCalledTimes(1);
    fireEvent.change(screen.getByLabelText("Window"), { target: { value: "24h" } });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10 * 60_000);
    });
    expect(load).toHaveBeenCalledTimes(2);
    expect(loadRuns).toHaveBeenCalledTimes(1);
  });
});
