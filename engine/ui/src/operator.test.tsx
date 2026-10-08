import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import { ApiError } from "./api";
import { ProjectActions } from "./estate/ProjectActions";
import {
  READ_ONLY_REASON,
  WriteAccessProvider,
  accessFromScope,
  type JobClient,
  type WriteAccess,
} from "./operator";
import { PlanDetail, approverLine, type PlanLoaders } from "./review/PlanDetail";

const PLAN = "a".repeat(64);
const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };

function job(kind: JobKind, state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: "job_1", kind, state, submitted_at: "2026-10-08T00:00:00Z", ...extra };
}

/** A job client that records submissions and answers each status read in turn. */
function fakeJobs(statuses: JobStatus[] = [job("run", "succeeded")]) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  let read = 0;
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: "job_1" };
    }),
    status: vi.fn(async () => statuses[Math.min(read++, statuses.length - 1)]),
  };
  return { client, submitted };
}

describe("accessFromScope", () => {
  it("maps the engine's token_scope to what the page may do", () => {
    expect(accessFromScope("full")).toEqual({ kind: "operator", scope: "full" });
    expect(accessFromScope("read_only")).toEqual(READ_ONLY);
    // No token at all (a loopback server without --ui) accepts writes too.
    expect(accessFromScope(null)).toEqual({ kind: "operator", scope: null });
  });
});

describe("Run and Plan on the estate", () => {
  it("are disabled with the reason on a read-only page, and submit nothing", () => {
    const { client, submitted } = fakeJobs();
    render(
      <WriteAccessProvider value={READ_ONLY}>
        <ProjectActions jobs={client} />
      </WriteAccessProvider>,
    );
    for (const name of ["Run", "Plan"]) {
      const button = screen.getByRole("button", { name });
      expect(button).toBeDisabled();
      fireEvent.click(button);
    }
    expect(screen.getAllByText(READ_ONLY_REASON).length).toBeGreaterThan(0);
    expect(submitted).toEqual([]);
  });

  it("submit a run, follow the job to the end, and refresh what it changed", async () => {
    const { client, submitted } = fakeJobs([job("run", "running"), job("run", "succeeded")]);
    const onDone = vi.fn();
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} onDone={onDone} />
      </WriteAccessProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText("Run the project: done.", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "run", body: {} }]);
    expect(client.status).toHaveBeenCalledTimes(2);
    expect(onDone).toHaveBeenCalledTimes(1);
  });

  it("runs one model when given one", async () => {
    const { client, submitted } = fakeJobs();
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} model="orders" />
      </WriteAccessProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Run this model" }));
    await waitFor(() => expect(submitted).toEqual([{ kind: "run", body: { model: "orders" } }]));
  });

  it("says plainly when another change holds the project (409 mutation_in_progress)", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => {
        throw new ApiError(409, {
          code: "mutation_in_progress",
          message: "another run, apply or approve job is already in progress on this project",
        });
      }),
      status: vi.fn(),
    };
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} />
      </WriteAccessProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(/already in progress/);
    expect(alert).toHaveTextContent(/Wait for it to finish/);
    expect(client.status).not.toHaveBeenCalled();
  });

  it("shows a failed job's error", async () => {
    const { client } = fakeJobs([job("plan", "failed", { error: "config does not load" })]);
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} />
      </WriteAccessProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Plan" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("config does not load");
  });
});

const STATUS: ReviewStatusOutput = {
  version: "1.78.0",
  command: "review_status",
  plan_id: PLAN,
  kind: "run",
  reviewed: false,
};

const DIFF: ReviewOutput = {
  version: "1.78.0",
  command: "review",
  plan_id: PLAN,
  base_ref: "HEAD",
  approved: false,
  marker_written: false,
  breaking_changes: [],
  conditional_drops: [],
} as unknown as ReviewOutput;

const EMPTY_QUEUE = {
  version: "1.78.0",
  command: "review_queue",
  total: 0,
  ranking: "x",
  pending: [],
} as unknown as ReviewQueueOutput;

function planLoaders(status: () => Promise<ReviewStatusOutput>): PlanLoaders {
  return {
    status: vi.fn(status),
    diff: vi.fn(async () => DIFF),
    queue: vi.fn(async () => EMPTY_QUEUE),
    product: vi.fn(async () => {
      throw new Error("no product read in these tests");
    }),
  };
}

describe("Approve and Apply on the plan page", () => {
  it("approve over the HTTP API, then show who the marker names", async () => {
    let approved = false;
    const loaders = planLoaders(async () =>
      approved
        ? {
            ...STATUS,
            reviewed: true,
            reviewed_at: "2026-10-08T12:00:00Z",
            approver: { email: "ops@example.com", host: "box", source: "http_api" },
            breaking_change_count: 0,
          }
        : STATUS,
    );
    const { client, submitted } = fakeJobs([job("approve", "succeeded")]);
    const onApproved = client.status as ReturnType<typeof vi.fn>;
    onApproved.mockImplementation(async () => {
      approved = true;
      return job("approve", "succeeded");
    });
    render(
      <WriteAccessProvider value={OPERATOR}>
        <PlanDetail planId={PLAN} loaders={loaders} jobs={client} />
      </WriteAccessProvider>,
    );
    const section = await screen.findByRole("region", { name: "Approval" });
    // Apply waits for the approval.
    expect(within(section).getByRole("button", { name: "Apply" })).toBeDisabled();
    // The command stays visible as a secondary hint.
    expect(within(section).getByText(`rocky review ${PLAN} --approve`)).toBeTruthy();

    fireEvent.click(within(section).getByRole("button", { name: "Approve" }));
    await screen.findByText(/approved over the HTTP API by ops@example.com/);
    expect(submitted).toEqual([{ kind: "approve", body: { plan_id: PLAN } }]);
    const text = document.body.textContent ?? "";
    expect(text).not.toMatch(/from the browser/i);
    expect(text).not.toMatch(/by a human/i);
    // Now Apply is live, and posts the plan id and nothing else.
    const apply = screen.getByRole("button", { name: "Apply" });
    expect(apply).toBeEnabled();
    fireEvent.click(apply);
    await waitFor(() =>
      expect(submitted.at(-1)).toEqual({ kind: "apply", body: { plan_id: PLAN } }),
    );
  });

  it("has no Apply button for a product-bound plan, and never reads its digest", async () => {
    const loaders = planLoaders(async () => ({
      ...STATUS,
      reviewed: true,
      product_id: "product:revenue_daily",
      spec_digest: "sha256:from-the-plan",
      approver: { email: "dev@example.com", host: "box", source: "local" },
    }));
    const { client, submitted } = fakeJobs();
    render(
      <WriteAccessProvider value={OPERATOR}>
        <PlanDetail planId={PLAN} loaders={loaders} jobs={client} />
      </WriteAccessProvider>,
    );
    const section = await screen.findByRole("region", { name: "Approval" });
    expect(within(section).queryByRole("button", { name: "Apply" })).toBeNull();
    expect(section).toHaveTextContent("apply in a terminal");
    expect(section).toHaveTextContent(/must come from you, not from the plan/);
    // The command names a placeholder, never the plan's own digest.
    expect(section.textContent).not.toContain("sha256:from-the-plan");
    expect(submitted).toEqual([]);
  });

  it("draws Approve disabled with the reason on a read-only page", async () => {
    const loaders = planLoaders(async () => STATUS);
    const { client, submitted } = fakeJobs();
    render(
      <WriteAccessProvider value={READ_ONLY}>
        <PlanDetail planId={PLAN} loaders={loaders} jobs={client} />
      </WriteAccessProvider>,
    );
    const section = await screen.findByRole("region", { name: "Approval" });
    const approve = within(section).getByRole("button", { name: "Approve" });
    expect(approve).toBeDisabled();
    fireEvent.click(approve);
    expect(within(section).getAllByText(READ_ONLY_REASON).length).toBeGreaterThan(0);
    expect(submitted).toEqual([]);
  });
});

describe("approverLine", () => {
  it("names the channel, never the browser or a human", () => {
    expect(approverLine({ email: "a@b.c", host: "h", source: "http_api" })).toBe(
      "approved over the HTTP API by a@b.c",
    );
    expect(approverLine({ email: "a@b.c", host: "h", source: "local" })).toBe(
      "approved locally by a@b.c",
    );
    expect(approverLine(null)).toMatch(/names no approver/);
  });
});
