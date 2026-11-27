import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { HistoryOutput } from "@rocky-types/history";
import type { ModelDetailOutput } from "@rocky-types/model_detail";
import type { ReplayCheckOutput } from "@rocky-types/replay_check";
import { ModelDetail } from "./ModelDetail";

const DETAIL = {
  name: "fct_revenue",
  file_path: "models/fct_revenue.sql",
  sql: "SELECT 1",
  sql_bytes: 8,
  sql_truncated: false,
  columns: ["id"],
  has_star: false,
  upstream: [],
  downstream: [],
  typed_columns: [],
} as unknown as ModelDetailOutput;

const RUNS = {
  kind: "ready",
  value: {
    command: "history",
    version: "1.0.0",
    count: 1,
    runs: [
      {
        run_id: "run-1",
        started_at: "2026-09-05T07:00:00Z",
        models: [{ model_name: "fct_revenue", status: "success", duration_ms: 1, recipe_identity: null }],
      },
    ],
  } as unknown as HistoryOutput,
} as const;

describe("ModelDetail", () => {
  it("shows the Rebuild check for the open model, from the newest run that executed it", async () => {
    const replayCheck = vi.fn(
      async () =>
        ({
          command: "replay",
          version: "1.0.0",
          run_id: "run-1",
          status: "success",
          model_count: 1,
          replayable_count: 0,
          replayable: false,
          models: [
            {
              model_name: "fct_revenue",
              verdict: "non_replayable",
              reasons: ["no provenance recorded for this model"],
              has_provenance: false,
              ir_parseable: false,
              nondeterministic: false,
              proof_class: null,
              inputs: [],
            },
          ],
        }) as ReplayCheckOutput,
    );
    render(
      <ModelDetail name="fct_revenue" load={async () => DETAIL} onClose={() => {}} runs={RUNS} replayCheck={replayCheck} />,
    );
    const section = await screen.findByRole("region", { name: "Rebuild check" });
    expect(await within(section).findByText("Not recorded")).toBeInTheDocument();
    expect(replayCheck).toHaveBeenCalledWith("run-1", "fct_revenue");
  });
});
