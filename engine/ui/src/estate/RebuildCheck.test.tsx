import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { HistoryOutput, RunHistoryRecord } from "@rocky-types/history";
import type { RecipeHistoryOutput } from "@rocky-types/recipe_history";
import type { ReplayCheckModelOutput, ReplayCheckOutput } from "@rocky-types/replay_check";
import { ApiError } from "../api";
import { RebuildCheck, RecipeRuns, newestRunOf, recipeHashIn } from "./RebuildCheck";
import type { Resource } from "./useResource";

const NOW = Date.parse("2026-09-05T08:00:00Z");
const RUN = "run-20260905-070000-101";

function run(run_id: string, started_at: string, models: Array<[string, string | null]>): RunHistoryRecord {
  return {
    run_id,
    started_at,
    finished_at: started_at,
    status: "Success",
    trigger: "Manual",
    models_executed: models.length,
    duration_ms: 1,
    models: models.map(([model_name, hash]) => ({
      model_name,
      status: "success",
      duration_ms: 1,
      recipe_identity: hash === null ? null : { recipe_hash: hash },
    })),
  } as unknown as RunHistoryRecord;
}

function runsOf(...records: RunHistoryRecord[]): Resource<HistoryOutput> {
  return {
    kind: "ready",
    value: { command: "history", version: "1.0.0", count: records.length, runs: records } as HistoryOutput,
  };
}

/** One run of fct_revenue, with a recipe hash. */
const RUNS = runsOf(run(RUN, "2026-09-05T07:00:00Z", [["fct_revenue", "d865beb2aaaa"]]));

/** What the engine sends for a recorded, replayable model. */
function recorded(over: Partial<ReplayCheckModelOutput> = {}): ReplayCheckModelOutput {
  return {
    model_name: "fct_revenue",
    has_provenance: true,
    ir_parseable: true,
    nondeterministic: false,
    proof_class: "strong",
    reasons: [],
    verdict: "replayable",
    inputs: [{ upstream_key: "demo.demo.stg_orders", kind: "content", resolvable: true, reason: null }],
    ...over,
  };
}

/** What the engine sends when no provenance was written (the default: `[reuse]` is off). */
function noProvenance(): ReplayCheckModelOutput {
  return {
    model_name: "fct_revenue",
    verdict: "non_replayable",
    reasons: [
      "no provenance recorded for this model (the run was not content-addressed, or auditable reuse was disabled)",
    ],
    has_provenance: false,
    ir_parseable: false,
    nondeterministic: false,
    proof_class: null,
    inputs: [],
  };
}

function check(m: ReplayCheckModelOutput = recorded()): ReplayCheckOutput {
  return {
    command: "replay",
    version: "1.0.0",
    run_id: RUN,
    status: "success",
    model_count: 1,
    replayable_count: m.verdict === "replayable" ? 1 : 0,
    replayable: m.verdict === "replayable",
    models: [m],
  };
}

const HISTORY: RecipeHistoryOutput = {
  command: "history",
  version: "1.0.0",
  recipe_hash: "d865beb2aaaa",
  count: 2,
  executions: [
    {
      run_id: "run-A",
      model_name: "fct_revenue",
      started_at: "2026-09-05T07:00:00Z",
      status: "success",
      duration_ms: 10,
      sql_hash: "h",
      recipe_identity: { recipe_hash: "d865beb2aaaa", input_hash: "a41c000000000000000000009e02" },
    },
    {
      run_id: "run-B",
      model_name: "fct_revenue",
      started_at: "2026-09-05T06:00:00Z",
      status: "failure",
      duration_ms: 10,
      sql_hash: "h",
      recipe_identity: null,
    },
  ],
};

const never = async (): Promise<RecipeHistoryOutput> => {
  throw new Error("must not read");
};
const quiet = async (): Promise<RecipeHistoryOutput> => HISTORY;

function renderCheck(m: ReplayCheckModelOutput, runs = RUNS, loadHistory = quiet) {
  return render(<RebuildCheck name="fct_revenue" runs={runs} load={async () => check(m)} loadHistory={loadHistory} now={NOW} />);
}

describe("RebuildCheck", () => {
  it("says a clean model can be rebuilt, in green, citing the run as a link", async () => {
    renderCheck(recorded());
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("This table can be rebuilt from its recording");
    expect(verdict).toHaveTextContent("nothing in the SQL is volatile");
    expect(verdict.className).toContain("bg-ok-soft");
    expect(within(verdict).getByRole("link", { name: RUN })).toHaveAttribute("href", `/ui/governor/custody/${RUN}`);
  });

  it("maps the checks into the Why table and the inputs into the Inputs table", async () => {
    renderCheck(recorded());
    const why = await screen.findByRole("table", { name: "Why" });
    const rows = within(why).getAllByRole("row").slice(1);
    expect(rows.map((r) => within(r).getAllByRole("cell").map((c) => c.textContent))).toEqual([
      ["Program recorded", "Yes", "match strength: strong"],
      ["Program readable by this engine", "Yes", "the recorded program parses"],
      ["Inputs recorded", "Yes", "1 of 1 resolve"],
      ["Same result every time", "Yes", "no volatile construct found"],
    ]);
    const cells = within(within(screen.getByRole("table", { name: "Inputs" })).getAllByRole("row")[1]).getAllByRole("cell");
    expect(cells[0]).toHaveTextContent("demo.demo.stg_orders");
    expect(cells[1]).toHaveTextContent("content hash");
    expect(cells[2].firstElementChild!.className).toContain("bg-ok-soft");
  });

  it("never shows the engine's no-provenance placeholders as facts", async () => {
    renderCheck(noProvenance());
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("Not recorded");
    expect(verdict).toHaveTextContent("This project does not record provenance.");
    expect(verdict).toHaveTextContent("[reuse] enabled = true");
    expect(verdict.className).toContain("bg-surface-2");
    expect(verdict.className).not.toMatch(/fail|ok-soft|warn/);
    const why = screen.getByRole("table", { name: "Why" });
    const rows = within(why).getAllByRole("row").slice(1);
    // Program recorded is a plain, grey "No"; the three placeholder rows are grey dashes.
    expect(within(rows[0]).getByText("No").className).toContain("bg-surface-2");
    for (const row of rows.slice(1)) {
      const [, result, detail] = within(row).getAllByRole("cell");
      expect(within(result).getByRole("img")).toHaveTextContent("-");
      expect(within(detail).getByRole("img")).toHaveAttribute("title", expect.stringMatching(/recorded no provenance|did not parse/));
    }
    expect(why.textContent).not.toMatch(/does not parse|no declared inputs|no volatile construct|Yes/);
    expect(screen.queryByRole("table", { name: "Inputs" })).toBeNull();
    expect(screen.getByText("Inputs").parentElement!.querySelector("[role=img]")).toHaveAttribute(
      "title",
      expect.stringMatching(/no provenance/),
    );
  });

  it("says a failed provenance read is not a missing record", async () => {
    const failed = { ...noProvenance(), reasons: ["provenance read failed: io error — fail-closed"] };
    renderCheck(failed);
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("Not read: the state store did not answer");
    expect(verdict).not.toHaveTextContent("does not record provenance");
  });

  it("marks a recorded program that does not parse red, and leaves its determinism unread", async () => {
    const bad = recorded({
      verdict: "non_replayable",
      ir_parseable: false,
      reasons: ["embedded canonical ModelIr did not deserialize under the current engine (IR forward-compatibility break)"],
    });
    renderCheck(bad);
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("This table cannot be rebuilt exactly");
    expect(verdict.className).toContain("bg-fail-soft");
    const rows = within(screen.getByRole("table", { name: "Why" })).getAllByRole("row").slice(1);
    expect(within(rows[1]).getByText("No").className).toContain("bg-fail-soft");
    const [, result, detail] = within(rows[3]).getAllByRole("cell");
    expect(within(result).getByRole("img")).toBeInTheDocument();
    expect(within(detail).getByRole("img")).toHaveAttribute("title", expect.stringMatching(/did not parse/));
    // The inputs came from the provenance record, so they are real.
    expect(screen.getByRole("table", { name: "Inputs" })).toBeInTheDocument();
  });

  it("says why an input that cannot be read blocks a rebuild, in red", async () => {
    const bad = recorded({
      verdict: "non_replayable",
      reasons: ["no recorded watermark"],
      inputs: [{ upstream_key: "demo.demo.raw", kind: "watermark", resolvable: false, reason: "no recorded watermark" }],
    });
    renderCheck(bad);
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("no recorded watermark");
    expect(verdict.className).toContain("bg-fail-soft");
    const input = within(screen.getByRole("table", { name: "Inputs" })).getAllByRole("row")[1];
    expect(within(input).getByText("Does not resolve").className).toContain("bg-fail-soft");
    expect(within(input).getByText("freshness watermark")).toBeInTheDocument();
  });

  it("marks a volatile model amber: it can be rebuilt, but may differ", async () => {
    renderCheck(recorded({ nondeterministic: true }));
    const verdict = await screen.findByRole("status");
    expect(verdict.className).toContain("bg-warn-soft");
    expect(verdict).toHaveTextContent("a rebuild may differ");
    const row = within(screen.getByRole("table", { name: "Why" })).getByText("Same result every time").closest("tr")!;
    expect(within(row).getByText("No").className).toContain("bg-warn-soft");
  });

  it("reads the recipe hash from the run's own model record and lists that program's runs once", async () => {
    const history = vi.fn(async () => HISTORY);
    renderCheck(recorded(), RUNS, history);
    const table = await screen.findByRole("table", { name: "Runs of this exact program" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(within(rows[0]).getByRole("link", { name: "run-A" })).toHaveAttribute("href", "/ui/governor/custody/run-A");
    expect(within(rows[0]).getByText("Succeeded")).toBeInTheDocument();
    expect(within(rows[1]).getByText("Failed")).toBeInTheDocument();
    expect(within(rows[1]).getByRole("img")).toHaveAttribute("title", "this execution observed no inputs");
    expect(screen.getByText("rocky history --recipe d865beb2aaaa").tagName).toBe("CODE");
    expect(history).toHaveBeenCalledTimes(1);
    expect(history).toHaveBeenCalledWith("d865beb2aaaa");
  });

  it("shows a grey dash with a reason when the run recorded no recipe hash, and reads no history", async () => {
    const history = vi.fn(never);
    renderCheck(recorded(), runsOf(run(RUN, "2026-09-05T07:00:00Z", [["fct_revenue", null]])), history);
    await screen.findByRole("status");
    const dash = screen.getByText("Runs of this exact program").parentElement!.querySelector("[role=img]")!;
    expect(dash).toHaveTextContent("-");
    expect(dash.getAttribute("title")).toMatch(/no recipe hash was recorded/);
    expect(history).not.toHaveBeenCalled();
    expect(screen.getByText("rocky history --recipe <hash>").tagName).toBe("CODE");
    expect(screen.getByText(`rocky replay ${RUN} --check --model fct_revenue`).tagName).toBe("CODE");
  });

  it("checks the newest run that executed this model, not the newest run of anything", async () => {
    const load = vi.fn(async () => check());
    const runs = runsOf(
      run("run-other", "2026-09-05T07:30:00Z", [["other_model", "x"]]),
      run("run-old", "2026-09-05T05:00:00Z", [["fct_revenue", "old"]]),
      run("run-mine", "2026-09-05T06:00:00Z", [["fct_revenue", "mine"]]),
    );
    render(<RebuildCheck name="fct_revenue" runs={runs} load={load} loadHistory={quiet} />);
    await screen.findByRole("status");
    expect(load).toHaveBeenCalledTimes(1);
    expect(load).toHaveBeenCalledWith("run-mine", "fct_revenue");
  });

  it("says no run of this model is recorded when the loaded runs hold none", async () => {
    const load = vi.fn(never as never);
    render(
      <RebuildCheck
        name="fct_revenue"
        runs={runsOf(run("run-other", "2026-09-05T07:30:00Z", [["other_model", "x"]]))}
        load={load}
        loadHistory={never}
      />,
    );
    expect(screen.getByText("No run of this model recorded yet.")).toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });

  it("does not call an unread runs list 'no run recorded'", () => {
    const refused: Resource<HistoryOutput> = { kind: "unreachable", message: "down" };
    render(<RebuildCheck name="fct_revenue" runs={refused} load={never as never} loadHistory={never} />);
    expect(screen.getByText(/runs list could not be read/)).toBeInTheDocument();
    expect(screen.queryByText(/No run of this model/)).toBeNull();
  });

  it("shows run_not_found as 'No run recorded yet', not as an error", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
        runs={RUNS}
        load={async () => {
          throw new ApiError(404, { code: "run_not_found", message: "no runs" });
        }}
        loadHistory={never}
      />,
    );
    expect(await screen.findByText(/No run recorded yet/)).toBeInTheDocument();
    expect(screen.queryByText("run_not_found")).toBeNull();
  });

  it("shows model_not_found as a plain sentence", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
        runs={RUNS}
        load={async () => {
          throw new ApiError(404, { code: "model_not_found", message: "no" });
        }}
        loadHistory={never}
      />,
    );
    expect(await screen.findByText(/did not execute this model/)).toBeInTheDocument();
  });

  it("shows any other refusal as an envelope, never as a verdict", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
        runs={RUNS}
        load={async () => {
          throw new ApiError(503, { code: "state_busy", message: "busy", remediation_hint: "retry later" });
        }}
        loadHistory={never}
      />,
    );
    expect(await screen.findByText("state_busy")).toBeInTheDocument();
    expect(screen.getByText("retry later")).toBeInTheDocument();
    expect(screen.queryByRole("status")).toBeNull();
    expect(screen.queryByRole("table")).toBeNull();
  });

  it("shows an unreachable engine as such", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
        runs={RUNS}
        load={async () => {
          throw new Error("offline");
        }}
        loadHistory={never}
      />,
    );
    expect(await screen.findByText("unreachable")).toBeInTheDocument();
    expect(screen.queryByRole("table")).toBeNull();
  });

  it("reads once and does not poll", async () => {
    vi.useFakeTimers();
    try {
      const load = vi.fn(async () => check());
      render(<RebuildCheck name="fct_revenue" runs={RUNS} load={load} loadHistory={quiet} />);
      await vi.advanceTimersByTimeAsync(120_000);
      expect(load).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("run and hash lookup", () => {
  it("takes the last execution's hash when a run holds several, and skips one with no hash", () => {
    const runs = runsOf(
      run("r1", "2026-09-05T07:00:00Z", [
        ["m", "first"],
        ["m", "second"],
        ["m", null],
      ]),
    );
    if (runs.kind !== "ready") throw new Error("fixture");
    expect(recipeHashIn(runs.value, "r1", "m")).toBe("second");
    expect(recipeHashIn(runs.value, "missing", "m")).toBeNull();
    expect(newestRunOf(runs.value, "m")).toBe("r1");
    expect(newestRunOf(runs.value, "other")).toBeNull();
  });
});

describe("RecipeRuns", () => {
  it("says the store holds no execution, not a count of zero", async () => {
    render(<RecipeRuns hash="abc" load={async () => ({ ...HISTORY, executions: [], count: 0 })} />);
    expect(await screen.findByText("The store holds no execution of this program.")).toBeInTheDocument();
  });

  it("shows a refused read as an envelope", async () => {
    render(
      <RecipeRuns
        hash="abc"
        load={async () => {
          throw new ApiError(400, { code: "bad_request", message: "bad since" });
        }}
      />,
    );
    expect(await screen.findByText("bad_request")).toBeInTheDocument();
    expect(screen.queryByRole("table")).toBeNull();
  });
});
