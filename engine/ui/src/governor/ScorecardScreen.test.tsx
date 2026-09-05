import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AuditScorecardOutput } from "@rocky-types/audit_scorecard";
import { ApiError } from "../api";
import { NOT_RECORDED } from "../format";
import { ScorecardScreen } from "./ScorecardScreen";

const NOW = Date.parse("2026-09-05T08:00:00Z");

const SCORECARD: AuditScorecardOutput = {
  version: "1.74.0",
  command: "audit",
  by: "principal",
  window: "30d",
  window_start: "2026-08-06T08:00:00Z",
  availability: "available",
  total_decisions: 10,
  groups: [
    {
      key: "agent",
      total: 8,
      allow: 4,
      require_review: 3,
      deny: 1,
      acceptance_rate: 0.5,
      review_rate: 0.375,
      denial_rate: 0.125,
      decision_refs: ["decision:1"],
    },
    {
      key: "<b>human</b>",
      total: 2,
      allow: 2,
      require_review: 0,
      deny: 0,
      acceptance_rate: 1,
      review_rate: 0,
      denial_rate: 0,
      decision_refs: [],
    },
  ],
  verify_after: { total: 4, passed: 3, failed: 1, pass_rate: 0.75 },
  unavailable_metrics: [
    {
      metric: "revert_rate",
      availability: "unavailable",
      note: "the ledger does not record reverts",
    },
  ],
};

describe("ScorecardScreen", () => {
  it("renders the groups with their rates, the verify-after aggregate and the unavailable metrics", async () => {
    const load = vi.fn(async () => SCORECARD);
    render(<ScorecardScreen load={load} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Scorecard groups" });
    const agent = within(table).getByText("agent").closest("tr") as HTMLElement;
    expect(within(agent).getByText("50.0%")).toBeInTheDocument();
    expect(within(agent).getByText("37.5%")).toBeInTheDocument();
    expect(within(agent).getByText("12.5%")).toBeInTheDocument();
    expect(screen.getByText("75.0%")).toBeInTheDocument();
    expect(screen.getByText("3 passed, 1 failed of 4")).toBeInTheDocument();
    const metrics = screen.getByRole("list", { name: "Unavailable metrics" });
    expect(within(metrics).getByText(/revert_rate/)).toBeInTheDocument();
    expect(
      within(metrics).getByText(new RegExp(`${NOT_RECORDED}, because the ledger does not record reverts`)),
    ).toBeInTheDocument();
    expect(load).toHaveBeenCalledWith("principal", "all");
  });

  it("renders a hostile group key as text", async () => {
    const { container } = render(<ScorecardScreen load={async () => SCORECARD} now={NOW} />);
    expect(await screen.findByText("<b>human</b>")).toBeInTheDocument();
    expect(container.querySelector("b")).toBeNull();
  });

  it("changes the grouping and the window through the selectors", async () => {
    const load = vi.fn(async () => SCORECARD);
    render(<ScorecardScreen load={load} now={NOW} />);
    await screen.findByRole("table", { name: "Scorecard groups" });
    fireEvent.change(screen.getByLabelText("Group by"), { target: { value: "rule" } });
    await waitFor(() => expect(load).toHaveBeenCalledWith("rule", "all"));
    fireEvent.change(screen.getByLabelText("Window"), { target: { value: "30d" } });
    await waitFor(() => expect(load).toHaveBeenCalledWith("rule", "30d"));
    expect(screen.getByText("GET /api/v1/audit/scorecard?by=rule&window=30d")).toBeInTheDocument();
  });

  it("shows the route's 400 envelope in the panel", async () => {
    const refused = new ApiError(400, {
      code: "bad_request",
      message: "invalid --window 'fortnight'",
      remediation_hint: "pass window=all or a <N>d / <N>h duration such as window=30d",
    });
    render(
      <ScorecardScreen
        load={async () => {
          throw refused;
        }}
        now={NOW}
      />,
    );
    expect(await screen.findByText("bad_request")).toBeInTheDocument();
    expect(screen.getByText(/pass window=all/)).toBeInTheDocument();
  });

  it("says when no verification row falls in the window", async () => {
    render(
      <ScorecardScreen
        load={async () => ({ ...SCORECARD, verify_after: null, availability: "no_data", groups: [] })}
        now={NOW}
      />,
    );
    expect(await screen.findByText("no verification row falls in the window")).toBeInTheDocument();
    expect(screen.getByText("no decision falls in the window")).toBeInTheDocument();
  });
});
