import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import { ApiError } from "./api";
import type { ProjectOutput } from "@rocky-types/project";
import { ProjectActions, planUnavailable } from "./estate/ProjectActions";
import {
  FORCE_STOP_LABEL,
  JobLine,
  READ_ONLY_REASON,
  WriteAccessProvider,
  accessFromScope,
  failureSummary,
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
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
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
      // The reason is the tooltip; the banner says it once, so it is not
      // repeated under every button.
      expect(button).toHaveAttribute("title", READ_ONLY_REASON);
      fireEvent.click(button);
    }
    expect(screen.queryAllByText(READ_ONLY_REASON)).toEqual([]);
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
      cancel: vi.fn(),
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

  it("draw Plan disabled, with the reason, where rocky plan cannot help", () => {
    const project = (types: string[]) =>
      ({ pipelines: types.map((t, i) => ({ name: `p${i}`, pipeline_type: t })) }) as unknown as ProjectOutput;
    expect(planUnavailable(null)).toBeUndefined();
    expect(planUnavailable(project(["replication"]))).toBeUndefined();
    expect(planUnavailable(project(["transformation"]))).toMatch(/has none/);
    expect(planUnavailable(project(["replication", "transformation"]))).toMatch(/--pipeline/);

    const { client, submitted } = fakeJobs();
    const reason = planUnavailable(project(["transformation"]));
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} planDisabledReason={reason} />
      </WriteAccessProvider>,
    );
    const plan = screen.getByRole("button", { name: "Plan" });
    expect(plan).toBeDisabled();
    fireEvent.click(plan);
    expect(screen.getByText(reason ?? "")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run" })).toBeEnabled();
    expect(submitted).toEqual([]);
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
    // The steps say where the plan stands, in words: approval is next.
    const steps = () =>
      within(within(section).getByRole("list", { name: "Steps" }))
        .getAllByRole("listitem")
        .map((item) => item.textContent);
    // A done step shows a check, so only the others carry their number.
    expect(steps()).toEqual(["Proposed: done", "2Approved: next", "3Applied: not yet"]);

    fireEvent.click(within(section).getByRole("button", { name: "Approve" }));
    await screen.findByText(/approved over the HTTP API by ops@example.com/);
    expect(submitted).toEqual([{ kind: "approve", body: { plan_id: PLAN } }]);
    const text = document.body.textContent ?? "";
    expect(text).not.toMatch(/from the browser/i);
    expect(text).not.toMatch(/by a human/i);
    // Approved is done. Applied is not known, never "next" or "done": the
    // status route records approval, not apply, so an applied plan would
    // look the same.
    expect(steps()).toEqual([
      "Proposed: done",
      "Approved: done",
      "3Applied: not knownThe plan status does not record apply.",
    ]);
    // Now Apply is live, and posts the plan id and nothing else.
    const apply = screen.getByRole("button", { name: "Apply" });
    expect(apply).toBeEnabled();
    fireEvent.click(apply);
    await waitFor(() =>
      expect(submitted.at(-1)).toEqual({ kind: "apply", body: { plan_id: PLAN } }),
    );
    // Only an apply this page saw succeed marks the last step done.
    await waitFor(() =>
      expect(steps()).toEqual(["Proposed: done", "Approved: done", "Applied: done"]),
    );
  });

  it("never marks Applied done after an apply that failed", async () => {
    const loaders = planLoaders(async () => ({
      ...STATUS,
      reviewed: true,
      approver: { email: "ops@example.com", host: "box", source: "http_api" },
    }));
    const { client } = fakeJobs([job("apply", "failed", { error: "Error: plan_models_changed" })]);
    render(
      <WriteAccessProvider value={OPERATOR}>
        <PlanDetail planId={PLAN} loaders={loaders} jobs={client} />
      </WriteAccessProvider>,
    );
    const section = await screen.findByRole("region", { name: "Approval" });
    fireEvent.click(within(section).getByRole("button", { name: "Apply" }));
    await within(section).findByRole("alert");
    const steps = within(within(section).getByRole("list", { name: "Steps" }))
      .getAllByRole("listitem")
      .map((item) => item.textContent);
    expect(steps).toEqual([
      "Proposed: done",
      "Approved: done",
      "3Applied: not knownThe plan status does not record apply.",
    ]);
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
    expect(approve).toHaveAttribute("title", READ_ONLY_REASON);
    expect(within(section).queryAllByText(READ_ONLY_REASON)).toEqual([]);
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

describe("a write button while its job runs", () => {
  it("is disabled and says running… until the job settles, and submits once", async () => {
    let finish: (value: JobStatus) => void = () => {};
    const submitted: Record<string, unknown>[] = [];
    let reads = 0;
    const client: JobClient = {
      submit: vi.fn(async (_kind, body) => {
        submitted.push(body);
        return { job_id: "job_1" };
      }),
      status: vi.fn(() => {
        reads += 1;
        if (reads === 1) return Promise.resolve(job("run", "running"));
        return new Promise<JobStatus>((resolve) => {
          finish = resolve;
        });
      }),
      cancel: vi.fn(),
    };
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={client} />
      </WriteAccessProvider>,
    );
    const run = screen.getByRole("button", { name: "Run" });
    // Two clicks before React re-renders: one job.
    fireEvent.click(run);
    fireEvent.click(run);
    const busy = await screen.findByRole("button", { name: "Run: running…" });
    expect(busy).toBeDisabled();
    fireEvent.click(busy);
    await waitFor(() => expect(reads).toBe(2), { timeout: 3000 });
    expect(screen.getByRole("button", { name: "Run: running…" })).toBeDisabled();
    expect(submitted).toEqual([{}]);

    finish(job("run", "succeeded"));
    const again = await screen.findByRole("button", { name: "Run" });
    expect(again).toBeEnabled();
    expect(submitted).toEqual([{}]);
  });
});

describe("a failed job, said concisely", () => {
  const STDERR = [
    '{"timestamp":"2026-10-08T10:00:00Z","level":"DEBUG","fields":{"sql":"SELECT secret FROM t"}}',
    "2026-10-08T10:00:00.1Z DEBUG rocky_core: reading /home/me/.cargo/registry/src/lib.rs",
    "DEBUG rocky_core: SELECT * FROM t",
    "Error: plan_models_changed: refusing to apply plan 'abc': models changed since this plan was made",
    "",
    "Caused by:",
    "    0: a model it runs was changed",
    '{"error_code":"TABLE_NOT_FOUND","message":"no such table"}',
    '{"level":"TRACE","message":"shutdown"}',
  ].join("\n");

  it("prefers the structured errors of the job's own output", () => {
    const failed = job("apply", "failed", {
      error: STDERR,
      result: {
        errors: [
          { asset_key: ["main", "dim_customer"], error: "model 'dim_customer' failed: boom" },
          { asset_key: [], error: "  " },
        ],
      },
    });
    expect(failureSummary(failed)).toBe("main.dim_customer: model 'dim_customer' failed: boom");
  });

  it("else shows the final Error: block, never a tracing line", () => {
    const summary = failureSummary(job("apply", "failed", { error: STDERR }));
    expect(summary.startsWith("Error: plan_models_changed")).toBe(true);
    expect(summary).toContain("Caused by:");
    // A warehouse error body is JSON too, but not a tracing event: kept.
    expect(summary).toContain("TABLE_NOT_FOUND");
    for (const leaked of ["SELECT", ".cargo/registry", "DEBUG", "TRACE"]) {
      expect(summary).not.toContain(leaked);
    }
    expect(failureSummary(job("apply", "failed", { error: null }))).toBe(
      "The job ended without a message.",
    );
    const long = failureSummary(job("apply", "failed", { error: `Error: ${"x".repeat(5000)}` }));
    expect(long.length).toBeLessThan(1_600);
  });

  it("renders only the concise failure on the page", () => {
    render(
      <JobLine label="Apply" view={{ kind: "done", job: job("apply", "failed", { error: STDERR }) }} />,
    );
    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent(/Apply: failed\. Error: plan_models_changed/);
    expect(alert.textContent).not.toContain("SELECT");
    expect(alert.textContent).not.toContain(".cargo/registry");
  });
});

describe("Cancel on a running job", () => {
  /** A job that runs until cancelled, then reads `cancelled`. */
  function cancellableJobs() {
    let cancelled = false;
    let answerCancel: (value: { job_id: string; signal: "terminate" | "kill" }) => void = () => {};
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", cancelled ? "cancelled" : "running")),
      cancel: vi.fn(
        () =>
          new Promise<{ job_id: string; signal: "terminate" | "kill" }>((resolve) => {
            answerCancel = resolve;
          }),
      ),
    };
    return {
      client,
      answer: (signal: "terminate" | "kill") => answerCancel({ job_id: "job_1", signal }),
      settle: () => {
        cancelled = true;
      },
    };
  }

  it("shows Cancel in operator mode, sends it once, offers a forced stop, and ends cancelled", async () => {
    const jobs = cancellableJobs();
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProjectActions jobs={jobs.client} />
      </WriteAccessProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    const cancel = await screen.findByRole("button", { name: "Cancel" });
    // Two clicks before the engine answers: one request (the busy guard).
    fireEvent.click(cancel);
    fireEvent.click(cancel);
    expect(jobs.client.cancel).toHaveBeenCalledTimes(1);
    expect(jobs.client.cancel).toHaveBeenCalledWith("job_1");
    expect(screen.getByRole("button", { name: "Cancel: stopping…" })).toBeDisabled();

    jobs.answer("terminate");
    const force = await screen.findByRole("button", { name: FORCE_STOP_LABEL });
    expect(force).toBeEnabled();
    // While the forced stop is being sent, the button keeps its label.
    fireEvent.click(force);
    expect(jobs.client.cancel).toHaveBeenCalledTimes(2);
    expect(screen.getByRole("button", { name: `${FORCE_STOP_LABEL}: stopping…` })).toBeDisabled();

    jobs.settle();
    expect(
      await screen.findByText("Run the project: cancelled.", {}, { timeout: 3000 }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: FORCE_STOP_LABEL })).toBeNull();
  });

  it("is not shown on a read-only page", () => {
    const jobs = cancellableJobs();
    render(
      <WriteAccessProvider value={READ_ONLY}>
        <JobLine
          label="Run the project"
          view={{ kind: "running", jobId: "job_1" }}
          cancel={{ view: { kind: "idle" }, stopping: false, request: () => void jobs.client.cancel("job_1") }}
        />
      </WriteAccessProvider>,
    );
    expect(screen.getByText(/running \(job/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
  });

  it("says why when the engine refuses the cancel", () => {
    render(
      <WriteAccessProvider value={OPERATOR}>
        <JobLine
          label="Run the project"
          view={{ kind: "running", jobId: "job_1" }}
          cancel={{
            view: {
              kind: "refused",
              error: new ApiError(409, { code: "job_not_running", message: "job 'job_1' is not running" }),
            },
            stopping: false,
            request: () => {},
          }}
        />
      </WriteAccessProvider>,
    );
    expect(screen.getByRole("alert")).toHaveTextContent(/Cancel refused\. job_not_running/);
  });
});
