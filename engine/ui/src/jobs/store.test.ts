import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobState, JobStatus } from "@rocky-types/job_status";
import { ApiError } from "../api";
import { JobStore, defaultJobClient, jobKey, type JobClient } from "./store";

function job(kind: JobKind, state: JobState, id = "job_1"): JobStatus {
  return {
    job_id: id,
    kind,
    state,
    submitted_at: "2026-11-26T10:00:00Z",
    started_at: "2026-11-26T10:00:00Z",
    finished_at: state === "running" ? null : "2026-11-26T10:00:02Z",
    result: undefined,
    error: state === "failed" ? "Error: boom" : null,
  } satisfies JobStatus;
}

function until(check: () => boolean, ms = 2000): Promise<void> {
  return vi.waitFor(() => expect(check()).toBe(true), { timeout: ms, interval: 5 });
}

describe("JobStore", () => {
  it("starts one job per key while one is in flight, and keys apart by subject", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("apply", "running")),
      cancel: vi.fn(),
    };
    const store = new JobStore(5);
    const a = jobKey("apply", "plan_a");
    store.start(a, "apply", "Apply change", client, { plan_id: "plan_a" });
    store.start(a, "apply", "Apply change", client, { plan_id: "plan_a" });
    store.start(jobKey("apply", "plan_b"), "apply", "Apply change", client, { plan_id: "plan_b" });
    expect(client.submit).toHaveBeenCalledTimes(2);
    expect(store.get(a)?.body).toEqual({ plan_id: "plan_a" });
    store.dispose();
  });

  it("reads again after a network failure, and never calls it a failed job", async () => {
    let reads = 0;
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => {
        reads += 1;
        if (reads <= 2) throw new TypeError("Failed to fetch");
        return job("run", "succeeded");
      }),
      cancel: vi.fn(),
    };
    const store = new JobStore(2);
    store.start("run", "run", "Run the project", client);
    await until(() => store.get("run")?.view.kind === "done");
    expect(reads).toBe(3);
    store.dispose();
  });

  it("marks a job lost, not failed, when the engine no longer knows it", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_9" })),
      status: vi.fn(async () => {
        throw new ApiError(404, { code: "job_not_found", message: "no such job" });
      }),
      cancel: vi.fn(),
    };
    const store = new JobStore(2);
    store.start("apply", "apply", "Apply change", client);
    await until(() => store.get("apply")?.view.kind === "lost");
    const view = store.get("apply")?.view;
    expect(view?.kind === "lost" && view.jobId).toBe("job_9");
    store.dispose();
  });

  it("calls a done listener once, and never after it left", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", "succeeded")),
      cancel: vi.fn(),
    };
    const store = new JobStore(2);
    const stayed = vi.fn();
    const left = vi.fn();
    store.onDone("run", stayed);
    store.onDone("run", left)();
    store.start("run", "run", "Run the project", client);
    await until(() => store.get("run")?.view.kind === "done");
    expect(stayed).toHaveBeenCalledTimes(1);
    expect(left).not.toHaveBeenCalled();
    store.dispose();
  });

  it("drops a late cancel answer once a newer job holds the slot", async () => {
    let answer: (value: { job_id: string; signal: "terminate" }) => void = () => {};
    let n = 0;
    let asked = false;
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: `job_${++n}` })),
      status: vi.fn(async (id: string) => job("run", id === "job_1" && asked ? "cancelled" : "running", id)),
      cancel: vi.fn(
        () =>
          new Promise<{ job_id: string; signal: "terminate" }>((resolve) => {
            answer = resolve;
          }),
      ),
    };
    const store = new JobStore(2);
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "running");
    store.cancel("run");
    asked = true;
    await until(() => store.get("run")?.view.kind === "done");
    store.start("run", "run", "Run", client);
    await until(() => {
      const view = store.get("run")?.view;
      return view?.kind === "running" && view.jobId === "job_2";
    });
    answer({ job_id: "job_1", signal: "terminate" });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(store.get("run")?.stopping).toBe(false);
    expect(store.get("run")?.cancel.kind).toBe("idle");
    store.dispose();
  });

  it("keeps a busy-project refusal as a card, and says any other refusal beside its button", async () => {
    const busy: JobClient = {
      submit: vi.fn(async () => {
        throw new ApiError(409, { code: "mutation_in_progress", message: "busy", running_job_id: "job_7" });
      }),
      status: vi.fn(),
      cancel: vi.fn(),
    };
    const bad: JobClient = {
      submit: vi.fn(async () => {
        throw new ApiError(400, { code: "bad_request", message: "no" });
      }),
      status: vi.fn(),
      cancel: vi.fn(),
    };
    const store = new JobStore(2);
    store.start("run", "run", "Run", busy);
    store.start("test", "test", "Test", bad);
    await until(() => store.get("run")?.view.kind === "refused" && store.get("test")?.view.kind === "refused");
    expect(store.get("run")?.closed).toBe(false);
    expect(store.get("test")?.closed).toBe(true);
    expect(busy.status).not.toHaveBeenCalled();
    store.dispose();
  });

  it("forgets an ended job once every page that showed it has left", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", "succeeded")),
      cancel: vi.fn(),
    };
    const store = new JobStore(2, 60_000);
    const first = store.attach("run");
    const second = store.attach("run");
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "done");
    first();
    await Promise.resolve();
    expect(store.get("run")?.forgotten).toBe(false);
    second();
    // Decided after the commit, so a page that remounts at once keeps it.
    expect(store.get("run")?.forgotten).toBe(false);
    await Promise.resolve();
    expect(store.get("run")?.forgotten).toBe(true);
    // The card keeps its own life.
    expect(store.get("run")?.closed).toBe(false);
    store.start("run", "run", "Run", client);
    expect(store.get("run")?.forgotten).toBe(false);
    store.dispose();
  });

  it("keeps an ended job when its page detaches and attaches again in one go", async () => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", "succeeded")),
      cancel: vi.fn(),
    };
    const store = new JobStore(2, 60_000);
    const leave = store.attach("run");
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "done");
    // A keyed refresh (or StrictMode) unmounts and mounts in one commit.
    leave();
    const again = store.attach("run");
    await Promise.resolve();
    expect(store.get("run")?.forgotten).toBe(false);
    again();
    store.dispose();
  });

  it("keeps a plan while the apply it is shown under runs or has an unseen end", async () => {
    let applied = false;
    const client: JobClient = {
      submit: vi.fn(async (kind: JobKind) => ({ job_id: kind })),
      status: vi.fn(async (id: string) =>
        job(id as JobKind, id === "apply" && !applied ? "running" : "succeeded", id),
      ),
      cancel: vi.fn(),
    };
    const store = new JobStore(2, 60_000);
    const plan = "compact";
    const apply = jobKey("apply", "optimize-tables");
    const leavePlan = store.attach(plan, apply);
    const leaveApply = store.attach(apply);
    store.start(plan, "compact", "Plan", client);
    await until(() => store.get(plan)?.view.kind === "done");
    store.start(apply, "apply", "Apply", client);
    await until(() => store.get(apply)?.view.kind === "running");
    leavePlan();
    leaveApply();
    await Promise.resolve();
    expect(store.get(plan)?.forgotten).toBe(false);
    applied = true;
    await until(() => store.get(apply)?.view.kind === "done");
    expect(store.get(plan)?.forgotten).toBe(false);
    // Seen on a return visit, then left: both go.
    const backPlan = store.attach(plan, apply);
    const backApply = store.attach(apply);
    backPlan();
    backApply();
    await until(() => store.get(plan)?.forgotten === true && store.get(apply)?.forgotten === true);
    store.dispose();
  });

  it("keeps a job a page left while it ran, so the next visit shows how it ended", async () => {
    let end = false;
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", end ? "succeeded" : "running")),
      cancel: vi.fn(),
    };
    const store = new JobStore(2, 60_000);
    const leave = store.attach("run");
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "running");
    leave();
    end = true;
    await until(() => store.get("run")?.view.kind === "done");
    expect(store.get("run")?.forgotten).toBe(false);
    store.dispose();
  });

  it("counts failed reads in a row, and starts again from zero after one that worked", async () => {
    let reads = 0;
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => {
        reads += 1;
        if (reads <= 3) throw new TypeError("Failed to fetch");
        return job("run", "running");
      }),
      cancel: vi.fn(),
    };
    const store = new JobStore(1, 60_000);
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.failedReads === 3);
    await until(() => store.get("run")?.failedReads === 0);
    expect(store.get("run")?.view.kind).toBe("running");
    store.dispose();
  });

  it("closes a succeeded card after its time, and keeps a failed one", async () => {
    const client: JobClient = {
      submit: vi.fn(async (kind: JobKind) => ({ job_id: kind })),
      status: vi.fn(async (id: string) => job(id as JobKind, id === "run" ? "succeeded" : "failed", id)),
      cancel: vi.fn(),
    };
    const store = new JobStore(2, 30);
    store.start("run", "run", "Run", client);
    store.start("apply", "apply", "Apply", client);
    await until(() => store.get("run")?.closed === true);
    expect(store.get("run")?.view.kind).toBe("done");
    expect(store.get("apply")?.view.kind).toBe("done");
    expect(store.get("apply")?.closed).toBe(false);
    store.dispose();
  });
});

describe("JobStore reads and cancels with deadlines and back-off", () => {
  it("doubles the wait between failed reads and caps it at 30 s", async () => {
    vi.useFakeTimers();
    try {
      const at: number[] = [];
      const client: JobClient = {
        submit: vi.fn(async () => ({ job_id: "job_1" })),
        status: vi.fn(async () => {
          at.push(Date.now());
          throw new TypeError("Failed to fetch");
        }),
        cancel: vi.fn(),
      };
      const store = new JobStore(1_000, 60_000);
      store.start("run", "run", "Run", client);
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(200_000);
      const waits = at.slice(1).map((t, i) => t - at[i]);
      expect(waits.slice(0, 8)).toEqual([2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000, 30_000]);
      expect(store.get("run")?.failedReads).toBe(at.length);
      store.dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([401, 403, 404])("marks the job lost, outcome unknown, on a %i read", async (status) => {
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_5" })),
      status: vi.fn(async () => {
        throw new ApiError(status, { code: "nope", message: "no" });
      }),
      cancel: vi.fn(),
    };
    const store = new JobStore(2);
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "lost");
    expect(client.status).toHaveBeenCalledTimes(1);
    store.dispose();
  });

  it("clears a refused cancel when the job ends", async () => {
    let end = false;
    const client: JobClient = {
      submit: vi.fn(async () => ({ job_id: "job_1" })),
      status: vi.fn(async () => job("run", end ? "succeeded" : "running")),
      cancel: vi.fn(async () => {
        throw new ApiError(409, { code: "job_not_running", message: "the job has ended" });
      }),
    };
    const store = new JobStore(2, 60_000);
    store.start("run", "run", "Run", client);
    await until(() => store.get("run")?.view.kind === "running");
    store.cancel("run");
    await until(() => store.get("run")?.cancel.kind === "refused");
    end = true;
    await until(() => store.get("run")?.view.kind === "done");
    expect(store.get("run")?.cancel.kind).toBe("idle");
    store.dispose();
  });
});

describe("defaultJobClient deadlines", () => {
  /** AbortSignal.timeout on the fake clock, so a test can pass the deadline. */
  function fakeTimeoutSignal() {
    return vi.spyOn(AbortSignal, "timeout").mockImplementation((ms: number) => {
      const controller = new AbortController();
      setTimeout(() => controller.abort(new DOMException("The operation timed out.", "TimeoutError")), ms);
      return controller.signal;
    });
  }

  /** A fetch that never answers, and rejects when its signal aborts. */
  function stalledFetch() {
    return vi.fn(
      (_input: RequestInfo | URL, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(init.signal?.reason));
        }),
    );
  }

  it("counts a stalled status read as a failed read, then reads again", async () => {
    vi.useFakeTimers();
    const timeout = fakeTimeoutSignal();
    const stalled = stalledFetch();
    vi.stubGlobal("fetch", stalled);
    try {
      const client: JobClient = {
        ...defaultJobClient,
        submit: vi.fn(async () => ({ job_id: "job_1" })),
      };
      const store = new JobStore(1_000, 60_000);
      store.start("run", "run", "Run", client);
      await vi.advanceTimersByTimeAsync(0);
      expect(stalled).toHaveBeenCalledTimes(1);
      expect(timeout).toHaveBeenCalledWith(15_000);
      expect(store.get("run")?.failedReads).toBe(0);
      await vi.advanceTimersByTimeAsync(14_999);
      expect(store.get("run")?.failedReads).toBe(0);
      await vi.advanceTimersByTimeAsync(1);
      expect(store.get("run")?.failedReads).toBe(1);
      expect(store.get("run")?.view.kind).toBe("running");
      // Backed off to 2 s, then read again.
      await vi.advanceTimersByTimeAsync(2_000);
      expect(stalled).toHaveBeenCalledTimes(2);
      store.dispose();
    } finally {
      vi.unstubAllGlobals();
      timeout.mockRestore();
      vi.useRealTimers();
    }
  });

  it("counts a stalled cancel as refused, and lets the button be pressed again", async () => {
    vi.useFakeTimers();
    const timeout = fakeTimeoutSignal();
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        calls.push(String(input));
        if (!String(input).endsWith("/cancel")) {
          return Promise.resolve(new Response(JSON.stringify(job("run", "running")), { status: 200 }));
        }
        return stalledFetch()(input, init);
      }),
    );
    try {
      const client: JobClient = { ...defaultJobClient, submit: vi.fn(async () => ({ job_id: "job_1" })) };
      const store = new JobStore(1_000, 60_000);
      store.start("run", "run", "Run", client);
      await vi.advanceTimersByTimeAsync(0);
      store.cancel("run");
      expect(store.get("run")?.cancel.kind).toBe("sending");
      await vi.advanceTimersByTimeAsync(15_000);
      const cancel = store.get("run")?.cancel;
      expect(cancel?.kind).toBe("refused");
      expect(cancel?.kind === "refused" && cancel.error.message).toMatch(/did not answer/);
      expect(store.get("run")?.terminates).toBe(0);
      // A second press goes out again.
      store.cancel("run");
      expect(store.get("run")?.cancel.kind).toBe("sending");
      expect(calls.filter((url) => url.endsWith("/cancel"))).toHaveLength(2);
      store.dispose();
    } finally {
      vi.unstubAllGlobals();
      timeout.mockRestore();
      vi.useRealTimers();
    }
  });
});
