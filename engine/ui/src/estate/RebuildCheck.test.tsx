import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { RecipeHistoryOutput } from "@rocky-types/recipe_history";
import type { ReplayCheckModelOutput, ReplayCheckOutput } from "@rocky-types/replay_check";
import { ApiError } from "../api";
import { RebuildCheck, RecipeRuns } from "./RebuildCheck";

const NOW = Date.parse("2026-09-05T08:00:00Z");

function model(over: Partial<ReplayCheckModelOutput> = {}): ReplayCheckModelOutput {
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

function check(m: ReplayCheckModelOutput = model()): ReplayCheckOutput {
  return {
    command: "replay",
    version: "1.0.0",
    run_id: "run-20260905-070000-101",
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

describe("RebuildCheck", () => {
  it("says a clean model can be rebuilt, in green, citing the run as a link", async () => {
    render(<RebuildCheck name="fct_revenue" load={async () => check()} loadHistory={never} now={NOW} />);
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("This table can be rebuilt from its recording");
    expect(verdict).toHaveTextContent("nothing in the SQL is volatile");
    expect(verdict.className).toContain("bg-ok-soft");
    expect(verdict.className).toContain("text-ok-ink");
    expect(within(verdict).getByRole("link", { name: "run-20260905-070000-101" })).toHaveAttribute(
      "href",
      "/ui/governor/custody/run-20260905-070000-101",
    );
  });

  it("maps the checks into the Why table and the inputs into the Inputs table", async () => {
    render(<RebuildCheck name="fct_revenue" load={async () => check()} loadHistory={never} />);
    const why = await screen.findByRole("table", { name: "Why" });
    const rows = within(why).getAllByRole("row").slice(1);
    expect(rows.map((r) => within(r).getAllByRole("cell").map((c) => c.textContent))).toEqual([
      ["Program recorded", "Yes", "match strength: strong"],
      ["Program readable by this engine", "Yes", "the recorded program parses"],
      ["Inputs recorded", "Yes", "1 of 1 resolve"],
      ["Same result every time", "Yes", "no volatile construct found"],
    ]);
    const inputs = within(screen.getByRole("table", { name: "Inputs" })).getAllByRole("row")[1];
    const cells = within(inputs).getAllByRole("cell");
    expect(cells[0]).toHaveTextContent("demo.demo.stg_orders");
    expect(cells[1]).toHaveTextContent("content hash");
    expect(cells[2]).toHaveTextContent("Resolves");
    expect(cells[2].firstElementChild!.className).toContain("bg-ok-soft");
  });

  it("says why a model cannot be rebuilt, in red, with the engine's reasons", async () => {
    const bad = model({
      verdict: "non_replayable",
      has_provenance: false,
      ir_parseable: false,
      proof_class: null,
      reasons: ["no provenance record for this run"],
      inputs: [{ upstream_key: "demo.demo.raw", kind: "watermark", resolvable: false, reason: "no recorded watermark" }],
    });
    render(<RebuildCheck name="fct_revenue" load={async () => check(bad)} loadHistory={never} />);
    const verdict = await screen.findByRole("status");
    expect(verdict).toHaveTextContent("This table cannot be rebuilt exactly");
    expect(verdict).toHaveTextContent("no provenance record for this run");
    expect(verdict.className).toContain("bg-fail-soft");
    expect(verdict.className).toContain("text-fail-ink");
    const input = within(screen.getByRole("table", { name: "Inputs" })).getAllByRole("row")[1];
    expect(within(input).getByText("Does not resolve").className).toContain("bg-fail-soft");
    expect(within(input).getByText("freshness watermark")).toBeInTheDocument();
    expect(within(input).getByText("no recorded watermark")).toBeInTheDocument();
    // A missing match strength is a grey dash with the reason.
    const dash = within(screen.getByRole("table", { name: "Why" })).getAllByRole("img")[0];
    expect(dash).toHaveAttribute("title", "no provenance record, so no match strength");
  });

  it("marks a volatile model amber: it can be rebuilt, but may differ", async () => {
    render(
      <RebuildCheck name="fct_revenue" load={async () => check(model({ nondeterministic: true }))} loadHistory={never} />,
    );
    const verdict = await screen.findByRole("status");
    expect(verdict.className).toContain("bg-warn-soft");
    expect(verdict.className).toContain("text-warn-ink");
    expect(verdict).toHaveTextContent("a rebuild may differ");
    const row = within(screen.getByRole("table", { name: "Why" })).getByText("Same result every time").closest("tr")!;
    expect(within(row).getByText("No").className).toContain("bg-warn-soft");
  });

  it("shows a grey dash with a reason where there is no recipe hash, and reads no history", async () => {
    const history = vi.fn(never);
    render(<RebuildCheck name="fct_revenue" load={async () => check()} loadHistory={history} />);
    await screen.findByRole("status");
    const dash = screen.getByText("Runs of this exact program").parentElement!.querySelector("[role=img]")!;
    expect(dash).toHaveTextContent("-");
    expect(dash.getAttribute("title")).toMatch(/does not carry a recipe hash/);
    expect(dash.className).toContain("text-muted");
    expect(history).not.toHaveBeenCalled();
    expect(screen.getByText("rocky replay latest --check --model fct_revenue").tagName).toBe("CODE");
    expect(screen.getByText("rocky history --recipe <hash>").tagName).toBe("CODE");
  });

  it("lists the runs of the exact program when a hash is known, once", async () => {
    const history = vi.fn(async () => HISTORY);
    render(<RebuildCheck name="fct_revenue" load={async () => check()} loadHistory={history} recipeHash="d865beb2aaaa" now={NOW} />);
    const table = await screen.findByRole("table", { name: "Runs of this exact program" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(within(rows[0]).getByRole("link", { name: "run-A" })).toHaveAttribute("href", "/ui/governor/custody/run-A");
    expect(within(rows[0]).getByText("Succeeded")).toBeInTheDocument();
    expect(within(rows[1]).getByText("Failed")).toBeInTheDocument();
    // A record with no recipe identity has no input hash: a grey dash.
    expect(within(rows[1]).getByRole("img")).toHaveAttribute("title", "this execution observed no inputs");
    expect(screen.getByText("rocky history --recipe d865beb2aaaa").tagName).toBe("CODE");
    expect(history).toHaveBeenCalledTimes(1);
    expect(history).toHaveBeenCalledWith("d865beb2aaaa");
  });

  it("shows run_not_found as 'No run recorded yet', not as an error", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
        load={async () => {
          throw new ApiError(404, { code: "run_not_found", message: "no runs" });
        }}
        loadHistory={never}
      />,
    );
    expect(await screen.findByText(/No run recorded yet/)).toBeInTheDocument();
    expect(screen.queryByText("run_not_found")).toBeNull();
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("shows model_not_found as a plain sentence", async () => {
    render(
      <RebuildCheck
        name="fct_revenue"
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
      render(<RebuildCheck name="fct_revenue" load={load} loadHistory={never} />);
      await vi.advanceTimersByTimeAsync(120_000);
      expect(load).toHaveBeenCalledTimes(1);
      expect(load).toHaveBeenCalledWith("fct_revenue");
    } finally {
      vi.useRealTimers();
    }
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
