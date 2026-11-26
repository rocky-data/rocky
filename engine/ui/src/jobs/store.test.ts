import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobState, JobStatus } from "@rocky-types/job_status";
import { ApiError } from "../api";
import { JobStore, jobKey, type JobClient } from "./store";

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
