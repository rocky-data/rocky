import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { BriefOutput } from "@rocky-types/brief";
import { NOT_RECORDED } from "../format";
import { BriefScreen } from "./BriefScreen";

const NOW = Date.parse("2026-09-05T08:00:00Z");
const PLAN = "a".repeat(64);

/** Every availability appears once, and every citation is distinct. */
export const BRIEF: BriefOutput = {
  version: "1.74.0",
  command: "brief",
  generated_at: "2026-09-05T07:59:00Z",
  since_mode: "7d",
  since_timestamp: "2026-08-29T07:59:00Z",
  escalations: {
    availability: "available",
    total: 1,
    ranking: "blast_radius, change_class, staleness",
    pending: [
      {
        plan_id: PLAN,
        model: "fct_orders",
        capability: "schema_change.breaking",
        principal: "agent",
        effect: "require_review",
        kind: "evaluation",
        reason: "breaking change to a contract column",
        decision_ref: "decision:esc-1",
        timestamp: "2026-09-04T10:00:00Z",
      },
    ],
  },
  agent_activity: {
    availability: "available",
    total: 2,
    allow: 1,
    require_review: 1,
    deny: 0,
    by_principal: [{ principal: "agent", total: 2, allow: 1, require_review: 1, deny: 0 }],
    decisions: [
      {
        plan_id: PLAN,
        model: "fct_orders",
        capability: "propose",
        principal: "agent",
        effect: "allow",
        kind: "evaluation",
        rule_id: 0,
        reason: "propose is allowed for the output model",
        decision_ref: "decision:act-1",
        timestamp: "2026-09-04T09:00:00Z",
      },
      {
        plan_id: PLAN,
        model: "fct_orders",
        capability: "apply",
        principal: "agent",
        effect: "require_review",
        kind: "evaluation",
        rule_id: null,
        reason: "<img src=x onerror=alert(1)> hostile reason",
        decision_ref: "decision:act-2",
        timestamp: "2026-09-04T10:00:00Z",
      },
      // Listed, not counted (#2043): `total` above is 2 evaluations.
      {
        plan_id: "freeze:models/fct_orders",
        model: "models/fct_orders",
        capability: "apply",
        principal: "human",
        effect: "deny",
        kind: "freeze",
        rule_id: null,
        reason: "frozen by the on-call",
        decision_ref: "decision:act-3",
        timestamp: "2026-09-04T11:00:00Z",
      },
    ],
  },
  runs: {
    availability: "available",
    total: 3,
    succeeded: 2,
    partial_failure: 1,
    failed: 0,
    attention: [
      {
        run_id: "run-attention-1",
        status: "PartialFailure",
        trigger: "Schedule",
        started_at: "2026-09-04T11:00:00Z",
        finished_at: "2026-09-04T11:05:00Z",
        failed_models: [{ model_name: "dim_customer", status: "failed" }],
      },
    ],
  },
  drift: { availability: "no_data", events: [] },
  freshness: {
    availability: "unavailable",
    note: "no freshness checks are configured, so nothing was observed",
    models: [],
  },
  quality: { availability: "no_data", models: [] },
  cost: {
    availability: "available",
    run_count: 1,
    total_duration_ms: 65_000,
    total_cost_usd: null,
    total_bytes_scanned: 2_500_000,
    adapter_type: "duckdb",
    budget: null,
    per_run: [{ run_id: "run-cost-1", duration_ms: 65_000, bytes_scanned: 2_500_000, cost_usd: null }],
  },
  autonomy: {
    availability: "available",
    degraded_rules: [{ rule_id: 2, failures: 3, limit: 3, window: "24h" }],
    active_freezes: [
      { scope: "model=fct_*", principal: "agent", frozen_at: "2026-09-03T00:00:00Z", plan_id: "freeze:global" },
    ],
  },
  scheduler: {
    availability: "available",
    scheduled_pipelines: 2,
    runs_in_window: 4,
    failed_in_window: 1,
    paused: ["nightly"],
    consecutive_failures: [{ pipeline: "hourly", consecutive_failures: 2 }],
    incident_count: 1,
    latest_incident: ".rocky/incidents/20260904T110000Z-hourly.json",
  },
};

describe("BriefScreen", () => {
  it("renders every section by its availability, with every citation as text", async () => {
    const load = vi.fn(async () => BRIEF);
    render(<BriefScreen load={load} now={NOW} />);
    const needs = await screen.findByRole("region", { name: "Needs you" });
    expect(within(needs).getByRole("link", { name: PLAN })).toHaveAttribute(
      "href",
      `/ui/governor/custody/${PLAN}`,
    );
    expect(within(needs).getByText("decision:esc-1")).toBeInTheDocument();

    const activity = screen.getByRole("region", { name: "Agent activity" });
    expect(within(activity).getByText("decision:act-1")).toBeInTheDocument();
    expect(within(activity).getByText("decision:act-2")).toBeInTheDocument();
    // The freeze row is listed with its kind, and the summary counts
    // evaluations only.
    const freeze = within(activity).getByText("decision:act-3").closest("tr") as HTMLElement;
    expect(within(freeze).getByText("freeze")).toBeInTheDocument();
    expect(within(activity).getByText(/2 policy evaluations: 1 allow/)).toBeInTheDocument();

    const runs = screen.getByRole("region", { name: "Runs" });
    expect(within(runs).getByText("run-attention-1")).toBeInTheDocument();
    expect(within(runs).getByText("dim_customer (failed)")).toBeInTheDocument();

    const drift = screen.getByRole("region", { name: "Drift" });
    expect(within(drift).getByText("nothing in the window")).toBeInTheDocument();
    expect(within(drift).queryByRole("table")).toBeNull();

    const freshness = screen.getByRole("region", { name: "Freshness" });
    expect(
      within(freshness).getByText(
        `${NOT_RECORDED}: no freshness checks are configured, so nothing was observed`,
      ),
    ).toBeInTheDocument();
    expect(within(freshness).queryByRole("table")).toBeNull();

    const cost = screen.getByRole("region", { name: "Cost" });
    expect(within(cost).getByText("run-cost-1")).toBeInTheDocument();
    expect(within(cost).getAllByText(NOT_RECORDED).length).toBeGreaterThanOrEqual(2);

    const autonomy = screen.getByRole("region", { name: "Autonomy" });
    expect(within(autonomy).getByText("freeze:global")).toBeInTheDocument();
    expect(within(autonomy).getByText("rule 2")).toBeInTheDocument();

    const scheduler = screen.getByRole("region", { name: "Scheduler" });
    expect(within(scheduler).getByText("Paused: nightly")).toBeInTheDocument();
    expect(within(scheduler).getByText("hourly")).toBeInTheDocument();

    expect(load).toHaveBeenCalledWith("7d");
  });

  it("leads with one summary line, an inbox card per escalation and an effect bar", async () => {
    render(<BriefScreen load={async () => BRIEF} now={NOW} />);
    const summary = await screen.findByRole("list", { name: "Summary" });
    expect(within(summary).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "1 decision waits on you",
      "1 partial of 3 runs",
      "1 freeze(s), 1 degraded rule(s)",
    ]);

    // The card opens the plan in Review, and still cites its custody chain:
    // the link shows a cut id and keeps the whole one as its name.
    const needs = screen.getByRole("region", { name: "Needs you" });
    expect(within(needs).getByRole("link", { name: "Review plan" })).toHaveAttribute(
      "href",
      `/ui/review/${PLAN}`,
    );
    expect(within(needs).getByRole("link", { name: PLAN }).textContent).toBe(`${"a".repeat(12)}…`);

    const activity = screen.getByRole("region", { name: "Agent activity" });
    expect(within(activity).getByRole("img")).toHaveAccessibleName(
      "1 allowed, 1 needed review, 0 denied",
    );
  });

  it("never counts a section it could not read in the summary line", async () => {
    render(
      <BriefScreen
        load={async () => ({
          ...BRIEF,
          escalations: { ...BRIEF.escalations, availability: "unavailable", total: 0, pending: [] },
        })}
        now={NOW}
      />,
    );
    const summary = await screen.findByRole("list", { name: "Summary" });
    expect(summary.textContent).not.toMatch(/waits on you/);
  });

  it("renders a hostile reason as text, never as markup", async () => {
    const { container } = render(<BriefScreen load={async () => BRIEF} now={NOW} />);
    expect(await screen.findByText("<img src=x onerror=alert(1)> hostile reason")).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
  });

  it("changes the window through the selector and shows the window it got", async () => {
    const load = vi.fn(async (since: BriefOutput["since_mode"]) => ({ ...BRIEF, since_mode: since }));
    render(<BriefScreen load={load} now={NOW} />);
    await screen.findByRole("region", { name: "Needs you" });
    fireEvent.change(screen.getByLabelText("Window"), { target: { value: "24h" } });
    await waitFor(() => expect(load).toHaveBeenCalledWith("24h"));
    expect(await screen.findByText("GET /api/v1/brief?since=24h")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Window"), { target: { value: "last" } });
    await waitFor(() => expect(load).toHaveBeenCalledWith("last"));
    expect(screen.getByText("reads the Slack hook's cursor without moving it")).toBeInTheDocument();
  });
});
