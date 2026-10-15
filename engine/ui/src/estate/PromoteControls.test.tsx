import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { BranchListOutput } from "@rocky-types/branch_list";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import { ApiError } from "../api";
import { READ_ONLY_REASON, WriteAccessProvider, type JobClient, type WriteAccess } from "../operator";
import { PROMOTE_CONFIG_KEY, PromoteControls, promotePlanSummary } from "./PromoteControls";

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };
const PLAN_ID = "a".repeat(64);

const branches = (...names: string[]): BranchListOutput => ({
  version: "1",
  command: "branch list",
  total: names.length,
  branches: names.map((name) => ({
    name,
    schema_prefix: `branch__${name}`,
    created_by: "dev",
    created_at: "2026-10-16T00:00:00Z",
  })),
});

function job(kind: JobKind, state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: `job_${kind}`, kind, state, submitted_at: "2026-10-16T00:00:00Z", ...extra };
}

const planResult = (tables: string[], planId: string = PLAN_ID) => ({
  plan_id: planId,
  plan_kind: "promote",
  statements: tables.map((target) => ({ purpose: "promote branch 'fix' (full_refresh)", target, sql: "CREATE ..." })),
});

/** A job client that answers each kind with the job its map names. */
function fakeJobs(answers: Partial<Record<JobKind, JobStatus>>) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: `job_${kind}` };
    }),
    status: vi.fn(async (jobId: string) => {
      const kind = jobId.replace("job_", "") as JobKind;
      const answer = answers[kind];
      if (answer === undefined) throw new Error(`no answer for ${kind}`);
      return answer;
    }),
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
  };
  return { client, submitted };
}

function renderControls(
  props: {
    approvalRequired: boolean | null | undefined;
    client: JobClient;
    list?: BranchListOutput;
    loadBranches?: () => Promise<BranchListOutput>;
  },
  access: WriteAccess = OPERATOR,
) {
  const loadBranches = props.loadBranches ?? vi.fn(async () => props.list ?? branches("fix", "other"));
  const view = render(
    <WriteAccessProvider value={access}>
      <PromoteControls approvalRequired={props.approvalRequired} jobs={props.client} loadBranches={loadBranches} />
    </WriteAccessProvider>,
  );
  return { ...view, loadBranches };
}

const TABLES = ["warehouse.main.orders", "warehouse.main.customers"];

async function planThrough(client: JobClient) {
  renderControls({ approvalRequired: true, client });
  await screen.findByRole("button", { name: "Plan promote" });
  fireEvent.click(screen.getByRole("button", { name: "Plan promote" }));
  await screen.findByLabelText("Production tables this plan replaces", {}, { timeout: 3000 });
}

describe("Promote from the browser", () => {
  it("is absent when the config does not require a branch approval, and says why and which key", () => {
    const { client } = fakeJobs({});
    const { loadBranches } = renderControls({ approvalRequired: false, client });
    expect(screen.queryByRole("button", { name: /promote/i })).toBeNull();
    expect(screen.queryByRole("combobox")).toBeNull();
    const text = document.body.textContent ?? "";
    expect(text).toContain("Promote is not offered here");
    expect(text).toContain(PROMOTE_CONFIG_KEY);
    expect(text).toContain("rocky.toml");
    // Nothing is read for a panel that is not offered.
    expect(loadBranches).not.toHaveBeenCalled();
  });

  it("is absent, and says the config was not read, when the server could not say", () => {
    const { client } = fakeJobs({});
    renderControls({ approvalRequired: null, client });
    expect(screen.queryByRole("button", { name: /promote/i })).toBeNull();
    const text = document.body.textContent ?? "";
    expect(text).toContain("could not be read");
    expect(text).toContain(PROMOTE_CONFIG_KEY);
  });

  it("waits while the project has not answered, and offers nothing", () => {
    const { client } = fakeJobs({});
    renderControls({ approvalRequired: undefined, client });
    expect(screen.getByText("Loading…")).toBeTruthy();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("says there is nothing to promote when no branch exists", async () => {
    const { client } = fakeJobs({});
    renderControls({ approvalRequired: true, client, list: branches() });
    await screen.findByText(/There are no branches to promote/);
    expect(screen.queryByRole("button", { name: "Plan promote" })).toBeNull();
  });

  it("shows a refused branch list as that refusal", async () => {
    const { client } = fakeJobs({});
    const loadBranches = vi.fn(async () => {
      throw new ApiError(503, { code: "engine_busy", message: "busy", remediation_hint: "retry" });
    });
    renderControls({ approvalRequired: true, client, loadBranches });
    await screen.findByText("engine_busy");
  });

  it("draws Plan promote disabled on a read-only page and submits nothing", async () => {
    const { client, submitted } = fakeJobs({});
    renderControls({ approvalRequired: true, client }, READ_ONLY);
    const button = await screen.findByRole("button", { name: "Plan promote" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(submitted).toEqual([]);
  });

  it("plans the chosen branch, lists every production table, and applies nothing yet", async () => {
    const { client, submitted } = fakeJobs({
      promote: job("promote", "succeeded", { result: planResult(TABLES) }),
    });
    renderControls({ approvalRequired: true, client });
    await screen.findByRole("button", { name: "Plan promote" });
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "other" } });
    fireEvent.click(screen.getByRole("button", { name: "Plan promote" }));
    const list = await screen.findByLabelText("Production tables this plan replaces", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "promote", body: { name: "other" } }]);
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual(TABLES);
    expect(screen.getByText(/replaces 2 production tables/)).toBeTruthy();
    // The signing step is not on this page.
    expect(document.body.textContent).toContain("rocky branch approve");
    expect(screen.queryByRole("button", { name: /approve/i })).toBeNull();
    // Planning applied nothing.
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
  });

  it("confirms by naming every table, then applies exactly the returned plan id", async () => {
    const { client, submitted } = fakeJobs({
      promote: job("promote", "succeeded", { result: planResult(TABLES) }),
      apply: job("apply", "succeeded", {
        result: {
          targets: [
            { target: TABLES[0], succeeded: true },
            { target: TABLES[1], succeeded: true },
          ],
        },
      }),
    });
    await planThrough(client);
    fireEvent.click(screen.getByRole("button", { name: "Promote" }));
    const group = screen.getByRole("group");
    for (const table of TABLES) expect(group.textContent).toContain(table);
    expect(group.textContent).toContain("branch fix");
    expect(group.textContent).toContain("2 production tables");
    // Asking does not apply.
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Promote" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, replace these tables" }));
    await waitFor(() => expect(submitted.at(-1)).toEqual({ kind: "apply", body: { plan_id: PLAN_ID } }));
    // The plan is applied by id: no re-plan happened.
    expect(submitted.filter((s) => s.kind === "promote")).toHaveLength(1);
    const result = await screen.findByLabelText("Promote result", {}, { timeout: 3000 });
    expect(result.textContent).toContain(`${TABLES[0]}: replaced`);
    // A promote that landed does not offer the same apply again.
    expect(screen.queryByRole("button", { name: "Promote" })).toBeNull();
  });

  it("shows which table was not replaced when the apply failed part way", async () => {
    const { client } = fakeJobs({
      promote: job("promote", "succeeded", { result: planResult(TABLES) }),
      apply: job("apply", "failed", {
        error: "Error: promote failed for branch 'fix'",
        result: {
          targets: [
            { target: TABLES[0], succeeded: true },
            { target: TABLES[1], succeeded: false, error: "Binder Error at /Users/me/secret.sql" },
          ],
        },
      }),
    });
    await planThrough(client);
    fireEvent.click(screen.getByRole("button", { name: "Promote" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, replace these tables" }));
    const result = await screen.findByLabelText("Promote result", {}, { timeout: 3000 });
    expect(result.textContent).toContain(`${TABLES[0]}: replaced`);
    expect(result.textContent).toContain(`${TABLES[1]}: not replaced`);
    // The adapter's error text is not shown; only the cleaned job failure.
    expect(document.body.textContent).not.toContain("/Users/me");
    expect(document.body.textContent).toContain("promote failed for branch 'fix'");
  });

  it("shows the cleaned reason when the plan is refused, such as a missing approval", async () => {
    const { client, submitted } = fakeJobs({
      promote: job("promote", "failed", {
        error:
          "Error: branch promote requires 1 approval(s); found 0 valid, 0 invalid (no rejected artifacts). Run `rocky branch approve fix`.",
      }),
    });
    renderControls({ approvalRequired: true, client });
    fireEvent.click(await screen.findByRole("button", { name: "Plan promote" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("Plan promote: failed.");
    expect(alert.textContent).toContain("requires 1 approval(s)");
    expect(screen.queryByRole("button", { name: "Promote" })).toBeNull();
    expect(submitted.map((s) => s.kind)).toEqual(["promote"]);
  });

  it("says so, and offers no apply, when a succeeded plan job names no usable plan id", async () => {
    const { client } = fakeJobs({
      promote: job("promote", "succeeded", { result: planResult(TABLES, "not-a-plan-id") }),
    });
    renderControls({ approvalRequired: true, client });
    fireEvent.click(await screen.findByRole("button", { name: "Plan promote" }));
    await screen.findByText(/returned no plan id/, {}, { timeout: 3000 });
    expect(screen.queryByRole("button", { name: "Promote" })).toBeNull();
  });

  it("offers no apply for a plan that lists no tables", async () => {
    const { client } = fakeJobs({ promote: job("promote", "succeeded", { result: planResult([]) }) });
    renderControls({ approvalRequired: true, client });
    fireEvent.click(await screen.findByRole("button", { name: "Plan promote" }));
    await screen.findByText(/lists no tables/, {}, { timeout: 3000 });
    expect(screen.queryByRole("button", { name: "Promote" })).toBeNull();
  });

  it("drops a plan when another branch is chosen, so it cannot be applied under the wrong name", async () => {
    const { client } = fakeJobs({ promote: job("promote", "succeeded", { result: planResult(TABLES) }) });
    await planThrough(client);
    expect(screen.getByRole("button", { name: "Promote" })).toBeTruthy();
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "other" } });
    expect(screen.queryByLabelText("Production tables this plan replaces")).toBeNull();
    expect(screen.queryByRole("button", { name: "Promote" })).toBeNull();
  });
});

describe("promotePlanSummary", () => {
  it("reads only the plan id and the table names, never the SQL", () => {
    const summary = promotePlanSummary(job("promote", "succeeded", { result: planResult(TABLES) }));
    expect(summary).toEqual({ planId: PLAN_ID, tables: TABLES });
    expect(JSON.stringify(summary)).not.toContain("CREATE");
  });

  it("refuses a plan id that is not 64 lowercase hex characters", () => {
    for (const bad of ["", "A".repeat(64), "a".repeat(63), `${"a".repeat(63)}-`, "../x"]) {
      expect(promotePlanSummary(job("promote", "succeeded", { result: planResult(TABLES, bad) }))).toBeNull();
    }
    expect(promotePlanSummary(job("promote", "succeeded"))).toBeNull();
  });
});
