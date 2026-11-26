import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { StrictMode, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobState, JobStatus } from "@rocky-types/job_status";
import { WriteAccessProvider, useJob, type WriteAccess } from "../operator";
import { JobTray, JobTrayProvider, RunningJobs, shortDuration } from "./JobTray";
import { JobStore, type JobClient } from "./store";

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };

function job(kind: JobKind, state: JobState, error: string | null = null): JobStatus {
  return {
    job_id: "job_1",
    kind,
    state,
    submitted_at: "2026-11-26T10:00:00Z",
    started_at: "2026-11-26T10:00:00Z",
    finished_at: state === "running" ? null : "2026-11-26T10:00:03Z",
    result: undefined,
    error,
  } satisfies JobStatus;
}

/** A job that runs until `finish` is called with its end state. */
function controlledJobs(kind: JobKind) {
  let end: JobStatus | null = null;
  const client: JobClient = {
    submit: vi.fn(async () => ({ job_id: "job_1" })),
    status: vi.fn(async () => end ?? job(kind, "running")),
    cancel: vi.fn(async () => ({ job_id: "job_1", signal: "terminate" as const })),
  };
  return { client, finish: (state: JobState, error: string | null = null) => (end = job(kind, state, error)) };
}

/** A page with one Apply button, as a change page has. */
function ApplyPage({ client }: { client: JobClient }) {
  const apply = useJob("apply", client, undefined, { subject: "abc", label: "Apply change abc" });
  return (
    <div>
      <button type="button" onClick={() => apply.start({ plan_id: "abc" })}>
        Apply
      </button>
      <p data-testid="page-view">{apply.view.kind === "done" ? `done ${apply.view.job.state}` : apply.view.kind}</p>
    </div>
  );
}

function Shell({ store, client }: { store: JobStore; client: JobClient }) {
  const [onPage, setOnPage] = useState(true);
  return (
    <WriteAccessProvider value={OPERATOR}>
      <JobTrayProvider store={store}>
        <RunningJobs />
        <button type="button" onClick={() => setOnPage((shown) => !shown)}>
          Switch page
        </button>
        {onPage && <ApplyPage client={client} />}
        <JobTray />
      </JobTrayProvider>
    </WriteAccessProvider>
  );
}

describe("JobTray", () => {
  it("keeps a job's card across a page change, and the page shows the job again when it comes back", async () => {
    const jobs = controlledJobs("apply");
    render(<Shell store={new JobStore(5, 60_000)} client={jobs.client} />);
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    const card = await screen.findByRole("article", { name: "Apply change abc" });
    await within(card).findByRole("status");
    expect(within(card).getByRole("status")).toHaveTextContent(/^Running$/);
    expect(card).toHaveTextContent(/Running · \d+ s/);
    expect(screen.getByText("1 job running")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Switch page" }));
    expect(screen.queryByTestId("page-view")).toBeNull();
    jobs.finish("succeeded");
    await waitFor(() => expect(card).toHaveTextContent("Done · 3 s"));
    expect(screen.queryByText(/job running/)).toBeNull();

    // It ended while the page was away: the page shows how it ended.
    fireEvent.click(screen.getByRole("button", { name: "Switch page" }));
    expect(screen.getByTestId("page-view")).toHaveTextContent("done succeeded");
    expect(jobs.client.submit).toHaveBeenCalledTimes(1);

    // Seen, then left: the next visit starts clean, not on an old result.
    fireEvent.click(screen.getByRole("button", { name: "Switch page" }));
    await act(async () => {
      await Promise.resolve();
    });
    fireEvent.click(screen.getByRole("button", { name: "Switch page" }));
    expect(screen.getByTestId("page-view")).toHaveTextContent("idle");
  });

  it("cancels from the card in operator mode, and says Stopping… once the stop is sent", async () => {
    const jobs = controlledJobs("run");
    render(<Shell store={new JobStore(5, 60_000)} client={jobs.client} />);
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    const card = await screen.findByRole("article", { name: "Apply change abc" });
    fireEvent.click(await within(card).findByRole("button", { name: "Cancel" }));
    expect(jobs.client.cancel).toHaveBeenCalledWith("job_1");
    await within(card).findByText("Stopping…");
    expect(within(card).getByRole("button", { name: "Force stop" })).toBeEnabled();
  });

  it("shows a failed write's first line and its details, and offers no way to send it again", async () => {
    const jobs = controlledJobs("apply");
    render(<Shell store={new JobStore(5, 60_000)} client={jobs.client} />);
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    const card = await screen.findByRole("article", { name: "Apply change abc" });
    jobs.finish("failed", "Error: plan_models_changed\nCaused by: the models moved");
    await waitFor(() => expect(within(card).getByRole("status")).toHaveTextContent("Error: plan_models_changed"));
    // The page that started it alerts; the card does not say it twice.
    expect(within(card).queryByRole("alert")).toBeNull();
    expect(within(card).getByText("Details")).toBeInTheDocument();
    const buttons = within(card)
      .getAllByRole("button")
      .map((button) => button.textContent || button.getAttribute("aria-label"));
    expect(buttons).toEqual(["Close: Apply change abc"]);
    fireEvent.click(within(card).getByRole("button", { name: "Close: Apply change abc" }));
    await waitFor(() => expect(screen.queryByRole("article", { name: "Apply change abc" })).toBeNull());
    expect(screen.getByTestId("page-view")).toHaveTextContent("done failed");
  });

  it("names the job that holds the project when a write is refused as busy", async () => {
    const { ApiError } = await import("../api");
    const client: JobClient = {
      submit: vi.fn(async () => {
        throw new ApiError(409, { code: "mutation_in_progress", message: "busy", running_job_id: "job_42" });
      }),
      status: vi.fn(),
      cancel: vi.fn(),
    };
    render(<Shell store={new JobStore(5, 60_000)} client={client} />);
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    const card = await screen.findByRole("article", { name: "Apply change abc" });
    expect(card).toHaveTextContent("Not started. Job job_42 holds the project. Try again when it ends.");
  });
});

describe("useJob on the shared store", () => {
  it("keeps a result through a keyed refresh and under StrictMode", async () => {
    const jobs = controlledJobs("apply");
    function Keyed() {
      const [generation, setGeneration] = useState(0);
      return (
        <>
          <button type="button" onClick={() => setGeneration((n) => n + 1)}>
            Refresh
          </button>
          <ApplyPage key={generation} client={jobs.client} />
        </>
      );
    }
    render(
      <StrictMode>
        <WriteAccessProvider value={OPERATOR}>
          <JobTrayProvider store={new JobStore(5, 60_000)}>
            <Keyed />
          </JobTrayProvider>
        </WriteAccessProvider>
      </StrictMode>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    jobs.finish("failed", "Error: boom");
    await waitFor(() => expect(screen.getByTestId("page-view")).toHaveTextContent("done failed"));
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await act(async () => {
      await Promise.resolve();
    });
    expect(screen.getByTestId("page-view")).toHaveTextContent("done failed");
  });

  it("refreshes once for a job whose subject the page moved away from, even after coming back", async () => {
    const jobs = controlledJobs("run");
    const onDone = vi.fn();
    function Runner({ model }: { model: string }) {
      const run = useJob("run", jobs.client, onDone, { subject: model, label: `Run ${model}` });
      return (
        <button type="button" onClick={() => run.start({ model })}>
          Run {model}
        </button>
      );
    }
    function Picker() {
      const [model, setModel] = useState("x");
      return (
        <>
          <button type="button" onClick={() => setModel((m) => (m === "x" ? "y" : "x"))}>
            Pick other
          </button>
          <Runner model={model} />
        </>
      );
    }
    render(
      <StrictMode>
        <WriteAccessProvider value={OPERATOR}>
          <JobTrayProvider store={new JobStore(5, 60_000)}>
            <Picker />
          </JobTrayProvider>
        </WriteAccessProvider>
      </StrictMode>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Run x" }));
    await waitFor(() => expect(jobs.client.status).toHaveBeenCalled());
    fireEvent.click(screen.getByRole("button", { name: "Pick other" }));
    fireEvent.click(screen.getByRole("button", { name: "Pick other" }));
    fireEvent.click(screen.getByRole("button", { name: "Pick other" }));
    jobs.finish("succeeded");
    await waitFor(() => expect(onDone).toHaveBeenCalledTimes(1));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(onDone).toHaveBeenCalledTimes(1);
  });
});

describe("shortDuration", () => {
  it("says seconds, then minutes and seconds", () => {
    expect(shortDuration(800)).toBe("1 s");
    expect(shortDuration(59_000)).toBe("59 s");
    expect(shortDuration(60_000)).toBe("1 min");
    expect(shortDuration(125_000)).toBe("2 min 5 s");
  });
});
