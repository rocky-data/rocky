import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import { READ_ONLY_REASON, WriteAccessProvider, type JobClient, type WriteAccess } from "../operator";
import { PreviewControls } from "./PreviewControls";

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };

function job(state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: "job_1", kind: "preview", state, submitted_at: "2026-10-15T00:00:00Z", ...extra };
}

function fakeJobs(status: JobStatus) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: "job_1" };
    }),
    status: vi.fn(async () => status),
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
  };
  return { client, submitted };
}

function renderControls(client: JobClient, access: WriteAccess = OPERATOR) {
  return render(
    <WriteAccessProvider value={access}>
      <PreviewControls jobs={client} />
    </WriteAccessProvider>,
  );
}

const copy = (model_name: string, copy_strategy: string, extra: Record<string, unknown> = {}) => ({
  model_name,
  source_schema: "raw",
  target_schema: "branch__x",
  copy_strategy,
  ...extra,
});

describe("Preview create", () => {
  it("is disabled on a read-only page and submits nothing", () => {
    const { client, submitted } = fakeJobs(job("succeeded"));
    renderControls(client, READ_ONLY);
    const button = screen.getByRole("button", { name: "Create preview" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(submitted).toEqual([]);
    expect(screen.queryByRole("group")).toBeNull();
  });

  it("asks first, says what it writes, and submits nothing until confirmed", async () => {
    const { client, submitted } = fakeJobs(job("succeeded", { result: { copy_set: [], prune_set: [] } }));
    renderControls(client);
    fireEvent.change(screen.getByLabelText("Branch name"), { target: { value: "  pr_x  " } });
    fireEvent.change(screen.getByLabelText("Base git ref"), { target: { value: " origin/main " } });
    fireEvent.click(screen.getByRole("button", { name: "Create preview" }));
    const group = screen.getByRole("group");
    expect(group.textContent).toContain("warehouse schema branch__pr_x");
    expect(group.textContent).toContain("against origin/main");
    expect(group.textContent).toContain("Production tables are only read");
    expect(submitted).toEqual([]);
    // Back drops the question without submitting.
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.queryByRole("group")).toBeNull();
    expect(submitted).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Create preview" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm create preview" }));
    await waitFor(() =>
      expect(submitted).toEqual([{ kind: "preview", body: { name: "pr_x", base: "origin/main" } }]),
    );
  });

  it("sends an empty body when both fields are blank, and says the name comes from git", async () => {
    const { client, submitted } = fakeJobs(job("succeeded", { result: {} }));
    renderControls(client);
    fireEvent.click(screen.getByRole("button", { name: "Create preview" }));
    expect(screen.getByRole("group").textContent).toContain("derived from the git branch");
    expect(screen.getByRole("group").textContent).toContain("against main");
    fireEvent.click(screen.getByRole("button", { name: "Confirm create preview" }));
    await waitFor(() => expect(submitted).toEqual([{ kind: "preview", body: {} }]));
  });

  async function runWith(result: Record<string, unknown>) {
    const { client } = fakeJobs(job("succeeded", { result }));
    renderControls(client);
    fireEvent.click(screen.getByRole("button", { name: "Create preview" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm create preview" }));
  }

  it("says done, the schema, the copies and that the changed models were not re-run", async () => {
    await runWith({
      branch_schema: "branch__pr_x",
      prune_set: [{ model_name: "orders", reason: "changed" }],
      copy_set: [copy("customers", "ctas"), copy("items", "ctas")],
    });
    await screen.findByText("Preview: done.", {}, { timeout: 3000 });
    const text = screen.getByRole("list", { name: "Preview result" }).textContent ?? "";
    expect(text).toContain("Warehouse schema: branch__pr_x");
    expect(text).toContain("Copied from production: 2 models.");
    expect(text).toContain("planned but not re-run: orders");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("warns, never says plain done, when a copy failed or the warehouse was not used", async () => {
    await runWith({
      branch_schema: "branch__pr_x",
      prune_set: [],
      copy_set: [copy("customers", "ctas"), copy("items", "failed"), copy("stock", "planning_only")],
    });
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toBe("Preview: branch registered, but not every model was copied.");
    expect(screen.queryByText("Preview: done.")).toBeNull();
    const text = screen.getByRole("list", { name: "Preview result" }).textContent ?? "";
    expect(text).toContain("Copied from production: 1 model.");
    expect(text).toContain("Copy failed for: items.");
    expect(text).toContain("Planned but not copied");
    expect(text).not.toContain("could not be used");
    expect(text).not.toContain("warehouse");
    expect(text).toContain("stock");
  });

  it("with no changed models but copies made, never says nothing was copied", async () => {
    await runWith({
      branch_schema: "branch__pr_x",
      prune_set: [],
      copy_set: [copy("customers", "ctas"), copy("orders", "ctas")],
    });
    await screen.findByText("Preview: done.", {}, { timeout: 3000 });
    const text = screen.getByRole("list", { name: "Preview result" }).textContent ?? "";
    expect(text).toContain("Copied from production: 2 models.");
    expect(text).toContain("No changed models.");
    expect(text).not.toContain("nothing was copied");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("warns, never says done, when nothing was copied and nothing changed", async () => {
    await runWith({ branch_schema: "branch__pr_x", prune_set: [], copy_set: [] });
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toBe("Preview: branch registered, but nothing was copied.");
    expect(screen.queryByText("Preview: done.")).toBeNull();
    const text = screen.getByRole("list", { name: "Preview result" }).textContent ?? "";
    expect(text).toContain("No changed models, so nothing was copied.");
  });

  it("shows only fixed words and names, never a message a result might carry", async () => {
    await runWith({
      branch_schema: "branch__pr_x",
      prune_set: [{ model_name: "orders", reason: "SELECT * FROM /Users/me/secret.csv" }],
      copy_set: [copy("items", "failed", { error: "Binder Error at /Users/me/project/models/x.sql" })],
      message: "token=abc123",
    });
    await screen.findByRole("alert", {}, { timeout: 3000 });
    const shown = document.body.textContent ?? "";
    for (const leaked of ["/Users/me", "SELECT", "token=", "Binder"]) {
      expect(shown).not.toContain(leaked);
    }
  });

  it("lists at most ten names, each cut short", async () => {
    const many = Array.from({ length: 14 }, (_, i) => copy(`m${i}`, "failed"));
    many.push(copy("x".repeat(200), "failed"));
    await runWith({ branch_schema: "b", prune_set: [], copy_set: many });
    await screen.findByRole("alert", {}, { timeout: 3000 });
    const text = screen.getByRole("list", { name: "Preview result" }).textContent ?? "";
    expect(text).toContain("m9");
    expect(text).not.toContain("m10");
    expect(text).toContain("and 5 more");
    expect(text).not.toContain("x".repeat(100));
  });

  it("shows a failed job's cleaned error, such as a branch name already in use", async () => {
    const { client } = fakeJobs(
      job("failed", {
        error: "Error: failed to register preview branch 'pr_x' in the state store\n\nCaused by:\n    branch 'pr_x' already exists",
      }),
    );
    renderControls(client);
    fireEvent.click(screen.getByRole("button", { name: "Create preview" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm create preview" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("Preview: failed.");
    expect(alert.textContent).toContain("branch 'pr_x' already exists");
  });
});
