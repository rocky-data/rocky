import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { BriefOutput } from "@rocky-types/brief";
import { NOT_RECORDED } from "../format";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import { ApiError } from "../api";
import {
  READ_ONLY_REASON,
  WriteAccessProvider,
  failureSummary,
  type JobClient,
  type WriteAccess,
} from "../operator";
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
    expect(summary.textContent).not.toMatch(/waits on you|no escalation/);
    expect(within(summary).getAllByRole("listitem")[0].textContent).toBe("escalations: not recorded");
  });

  it("says a quiet window in the section's own words", async () => {
    render(
      <BriefScreen
        load={async () => ({
          ...BRIEF,
          escalations: { ...BRIEF.escalations, availability: "no_data", total: 0, pending: [] },
          runs: { ...BRIEF.runs, availability: "no_data" },
        })}
        now={NOW}
      />,
    );
    const summary = await screen.findByRole("list", { name: "Summary" });
    expect(within(summary).getAllByRole("listitem").map((item) => item.textContent)).toEqual([
      "no escalation is pending",
      "no runs in the window",
      "1 freeze(s), 1 degraded rule(s)",
    ]);
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

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };

function job(kind: JobKind, state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: "job_1", kind, state, submitted_at: "2026-10-08T00:00:00Z", ...extra };
}

/** A job client that records submissions and answers each status read in turn. */
function fakeJobs(statuses: JobStatus[] = [job("freeze", "succeeded")]) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  let read = 0;
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: "job_1" };
    }),
    status: vi.fn(async () => statuses[Math.min(read++, statuses.length - 1)]),
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
  };
  return { client, submitted };
}

describe("Freeze and unfreeze on the brief", () => {
  const FROZEN = { principal: "agent", scope: "model=fct_*" };

  function renderBrief(access: WriteAccess, client: JobClient, load = vi.fn(async () => BRIEF)) {
    render(
      <WriteAccessProvider value={access}>
        <BriefScreen load={load} jobs={client} />
      </WriteAccessProvider>,
    );
    return load;
  }

  it("are disabled with the reason on a read-only page, and submit nothing", async () => {
    const { client, submitted } = fakeJobs();
    renderBrief(READ_ONLY, client);
    const freeze = await screen.findByRole("button", { name: "Freeze" });
    const unfreeze = screen.getByRole("button", { name: "Unfreeze agent on model=fct_*" });
    for (const button of [freeze, unfreeze]) {
      expect(button).toBeDisabled();
      expect(button).toHaveAttribute("title", READ_ONLY_REASON);
      fireEvent.click(button);
    }
    expect(screen.queryByRole("button", { name: "Confirm freeze" })).toBeNull();
    expect(submitted).toEqual([]);
  });

  it("freeze asks first, then submits only the fields that were filled in", async () => {
    const { client, submitted } = fakeJobs([job("freeze", "succeeded")]);
    const load = renderBrief(OPERATOR, client);
    await screen.findByRole("button", { name: "Freeze" });

    // The first press asks; Back drops the question and submits nothing.
    fireEvent.click(screen.getByRole("button", { name: "Freeze" }));
    expect(screen.getByText(/Freeze agent and human on every model\?/)).toBeInTheDocument();
    expect(submitted).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.queryByRole("button", { name: "Confirm freeze" })).toBeNull();
    expect(submitted).toEqual([]);

    // Nothing filled in: an empty body, which freezes both principals on every model.
    fireEvent.click(screen.getByRole("button", { name: "Freeze" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm freeze" }));
    await screen.findByText("Freeze: done.", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "freeze", body: {} }]);
    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));

    fireEvent.change(screen.getByLabelText("Principal"), { target: { value: "agent" } });
    fireEvent.change(screen.getByLabelText("Scope"), { target: { value: "  layer=gold " } });
    fireEvent.change(screen.getByLabelText("Reason"), { target: { value: " incident 42 " } });
    fireEvent.click(screen.getByRole("button", { name: "Freeze" }));
    expect(screen.getByText(/Freeze agent on layer=gold\?/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Confirm freeze" }));
    await waitFor(() =>
      expect(submitted.at(-1)).toEqual({
        kind: "freeze",
        body: { principal: "agent", scope: "layer=gold", reason: "incident 42" },
      }),
    );
  });

  it("unfreeze asks first, then names the exact principal and scope of the freeze", async () => {
    const { client, submitted } = fakeJobs([job("unfreeze", "succeeded")]);
    renderBrief(OPERATOR, client);
    fireEvent.click(await screen.findByRole("button", { name: "Unfreeze agent on model=fct_*" }));
    expect(screen.getByText("Lift the freeze on agent on model=fct_*? Those actions can run again.")).toBeInTheDocument();
    expect(submitted).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Confirm unfreeze" }));
    await screen.findByText("Unfreeze agent on model=fct_*: done.", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "unfreeze", body: FROZEN }]);
  });

  it("shows a freeze that binds nothing as a warning, not as plain done", async () => {
    const note =
      "freeze recorded but NOT enforced: no [policy] block configured in <path> Every enforcement seam short-circuits.";
    const { client } = fakeJobs([job("freeze", "succeeded", { result: { notes: [note] } })]);
    renderBrief(OPERATOR, client);
    fireEvent.click(await screen.findByRole("button", { name: "Freeze" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm freeze" }));
    const headline = await screen.findByText("Freeze: recorded, but NOT enforced.", {}, { timeout: 3000 });
    expect(headline).toHaveAttribute("role", "alert");
    expect(screen.getByText(note)).toBeInTheDocument();
    expect(screen.queryByText("Freeze: done.")).toBeNull();
  });

  it("shows an unfreeze note verbatim, without a warning", async () => {
    const note = "no matching active freeze markers to lift";
    const { client } = fakeJobs([job("unfreeze", "succeeded", { result: { notes: [note] } })]);
    renderBrief(OPERATOR, client);
    fireEvent.click(await screen.findByRole("button", { name: "Unfreeze agent on model=fct_*" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm unfreeze" }));
    const headline = await screen.findByText("Unfreeze agent on model=fct_*: done.", {}, { timeout: 3000 });
    expect(headline).not.toHaveAttribute("role", "alert");
    expect(screen.queryByText(/nothing was lifted/)).toBeNull();
    expect(screen.getByText(note)).toBeInTheDocument();
  });

  it("shows only string notes, capped in number and length", async () => {
    const long = "x".repeat(400);
    const notes = [1, null, { a: 1 }, "n1", "n2", "n3", "n4", "n5", "n6", long];
    const { client } = fakeJobs([job("freeze", "succeeded", { result: { notes } })]);
    renderBrief(OPERATOR, client);
    fireEvent.click(await screen.findByRole("button", { name: "Freeze" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm freeze" }));
    await screen.findByText("Freeze: done.", {}, { timeout: 3000 });
    const items = within(screen.getByRole("list", { name: "Freeze notes" })).getAllByRole("listitem");
    expect(items.map((item) => item.textContent)).toEqual(["n1", "n2", "n3", "n4", "n5"]);
    const capped = fakeJobs([job("freeze", "succeeded", { result: { notes: [long] } })]);
    cleanup();
    renderBrief(OPERATOR, capped.client);
    fireEvent.click(await screen.findByRole("button", { name: "Freeze" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm freeze" }));
    await screen.findByText("Freeze: done.", {}, { timeout: 3000 });
    const [item] = within(screen.getByRole("list", { name: "Freeze notes" })).getAllByRole("listitem");
    expect(item.textContent).toBe(`${"x".repeat(300)}…`);
  });

  it("lifts an unreadable marker with no principal and scope any, from a single row", async () => {
    const marker = {
      scope: "any",
      frozen_at: "",
      plan_id: "freeze:abc",
      unreadable_marker: true,
    } as const;
    const brief: BriefOutput = {
      ...BRIEF,
      autonomy: {
        ...BRIEF.autonomy,
        active_freezes: [
          { ...marker, principal: "agent" },
          { ...marker, principal: "human" },
        ],
      },
    };
    const { client, submitted } = fakeJobs([job("unfreeze", "succeeded")]);
    renderBrief(OPERATOR, client, vi.fn(async () => brief));
    const name = "unreadable marker (agent and human) on every model";
    const buttons = await screen.findAllByRole("button", { name: `Unfreeze ${name}` });
    expect(buttons).toHaveLength(1);
    fireEvent.click(buttons[0]);
    expect(
      screen.getByText(
        "Lift every unreadable marker and every freeze on scope any, for both agent and human? Those actions can run again.",
      ),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Confirm unfreeze" }));
    await screen.findByText(`Unfreeze ${name}: done.`, {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "unfreeze", body: { scope: "any" } }]);
  });

  it("says plainly when a run holds the project, and shows a failed freeze without its log lines", async () => {
    const busy: JobClient = {
      submit: vi.fn(async () => {
        throw new ApiError(409, {
          code: "mutation_in_progress",
          message: "another run, apply, approve, freeze, unfreeze, preview or product approval job is already in progress on this project",
        });
      }),
      status: vi.fn(),
      cancel: vi.fn(),
    };
    renderBrief(OPERATOR, busy);
    fireEvent.click(await screen.findByRole("button", { name: "Unfreeze agent on model=fct_*" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm unfreeze" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Another run, apply, approve, freeze, unfreeze, preview or product approval is already in progress");

    const failed = job("freeze", "failed", {
      error: [
        "2026-10-08T10:00:00.1Z DEBUG rocky_core: reading /home/me/state.redb",
        "Error: refusing to record the policy freeze: the config failed to load",
      ].join("\n"),
    });
    const summary = failureSummary(failed);
    expect(summary).toBe("Error: refusing to record the policy freeze: the config failed to load");
    expect(summary).not.toContain("/home/me");
  });
});
