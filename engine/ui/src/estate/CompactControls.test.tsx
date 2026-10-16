import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import { READ_ONLY_REASON, WriteAccessProvider, type JobClient, type WriteAccess } from "../operator";
import { CompactControls, compactPlanSummary } from "./CompactControls";

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };
const PLAN_ID = "b".repeat(64);

function job(kind: JobKind, state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: `job_${kind}`, kind, state, submitted_at: "2026-10-16T00:00:00Z", ...extra };
}

const stmts = (table: string) => [
  { purpose: "compact small files", sql: `OPTIMIZE ${table} WHERE true` },
  { purpose: "remove stale data files", sql: `VACUUM ${table} RETAIN 168 HOURS` },
];

const singleResult = (table = "cat.sch.orders", planId: string | null = PLAN_ID) => ({
  command: "compact",
  model: table,
  dry_run: false,
  target_size_mb: 256,
  statements: stmts(table),
  ...(planId === null ? {} : { plan_id: planId }),
});

const CATALOG_TABLES = ["cat.sch.orders", "cat.sch.customers", "cat.raw.events"];
const catalogResult = {
  command: "compact",
  catalog: "cat",
  scope: "catalog",
  dry_run: false,
  target_size_mb: 256,
  statements: CATALOG_TABLES.flatMap(stmts),
  tables: Object.fromEntries(CATALOG_TABLES.map((t) => [t, { statements: stmts(t) }])),
  totals: { table_count: 3, statement_count: 6 },
  plan_id: PLAN_ID,
};

function fakeJobs(answers: Partial<Record<JobKind, JobStatus>>) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: `job_${kind}` };
    }),
    status: vi.fn(async (jobId: string) => {
      const answer = answers[jobId.replace("job_", "") as JobKind];
      if (answer === undefined) throw new Error(`no answer for ${jobId}`);
      return answer;
    }),
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
  };
  return { client, submitted };
}

function renderControls(client: JobClient, access: WriteAccess = OPERATOR) {
  return render(
    <WriteAccessProvider value={access}>
      <CompactControls jobs={client} />
    </WriteAccessProvider>,
  );
}

function typeTable(value: string) {
  fireEvent.change(screen.getByLabelText("Table"), { target: { value } });
}

describe("Compact from the browser", () => {
  it("says archive is not here, and that the plan runs nothing", () => {
    const { client } = fakeJobs({});
    renderControls(client);
    const text = document.body.textContent ?? "";
    expect(text).toContain("Archiving is not offered here");
    expect(text).toContain("runs none");
    expect(screen.queryByRole("button", { name: /archive/i })).toBeNull();
  });

  it("draws Plan compact disabled on a read-only page and submits nothing", () => {
    const { client, submitted } = fakeJobs({});
    renderControls(client, READ_ONLY);
    typeTable("cat.sch.orders");
    const button = screen.getByRole("button", { name: "Plan compact" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(submitted).toEqual([]);
  });

  it("will not plan with an empty table", () => {
    const { client, submitted } = fakeJobs({});
    renderControls(client);
    const button = screen.getByRole("button", { name: "Plan compact" });
    expect(button).toBeDisabled();
    expect(screen.getByText("Type the full table name.")).toBeTruthy();
    fireEvent.click(button);
    expect(submitted).toEqual([]);
  });

  it("plans one table, shows its counts and tables, and applies nothing", async () => {
    const { client, submitted } = fakeJobs({ compact: job("compact", "succeeded", { result: singleResult() }) });
    renderControls(client);
    typeTable("  cat.sch.orders  ");
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    const list = await screen.findByLabelText("Tables this plan compacts", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "compact", body: { table: "cat.sch.orders" } }]);
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual(["cat.sch.orders"]);
    expect(screen.getByText(/Plan ready: 2 statements on 1 table, target file size 256 MB\. Nothing has run\./)).toBeTruthy();
    expect(document.body.textContent).toContain("OPTIMIZE cat.sch.orders WHERE true");
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
  });

  it("plans a catalog and lists every managed table in it", async () => {
    const { client, submitted } = fakeJobs({ compact: job("compact", "succeeded", { result: catalogResult }) });
    renderControls(client);
    fireEvent.click(screen.getByLabelText("Every managed table in a catalog"));
    fireEvent.change(screen.getByLabelText("Catalog"), { target: { value: "cat" } });
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    const list = await screen.findByLabelText("Tables this plan compacts", {}, { timeout: 3000 });
    expect(submitted).toEqual([{ kind: "compact", body: { catalog: "cat" } }]);
    expect(within(list).getAllByRole("listitem").map((li) => li.textContent)).toEqual([
      "cat.raw.events",
      "cat.sch.customers",
      "cat.sch.orders",
    ]);
    expect(screen.getByText(/6 statements on 3 tables/)).toBeTruthy();
  });

  it("confirms by naming every table, then applies exactly the plan id, with no re-plan", async () => {
    const { client, submitted } = fakeJobs({
      compact: job("compact", "succeeded", { result: catalogResult }),
      apply: job("apply", "succeeded", { result: { command: "compact apply" } }),
    });
    renderControls(client);
    fireEvent.click(screen.getByLabelText("Every managed table in a catalog"));
    fireEvent.change(screen.getByLabelText("Catalog"), { target: { value: "cat" } });
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    await screen.findByLabelText("Tables this plan compacts", {}, { timeout: 3000 });
    fireEvent.click(screen.getByRole("button", { name: "Compact" }));
    const group = screen.getByRole("group", { name: /Run OPTIMIZE and VACUUM/ });
    for (const table of CATALOG_TABLES) expect(group.textContent).toContain(table);
    expect(group.textContent).toContain("OPTIMIZE and VACUUM");
    expect(group.textContent).toContain("3 tables");
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(submitted.some((s) => s.kind === "apply")).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Compact" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, compact these tables" }));
    await waitFor(() => expect(submitted.at(-1)).toEqual({ kind: "apply", body: { plan_id: PLAN_ID } }));
    expect(submitted.filter((s) => s.kind === "compact")).toHaveLength(1);
    await screen.findByText("Compact: done.", {}, { timeout: 3000 });
    // A compaction that ran does not offer the same apply again.
    expect(screen.queryByRole("button", { name: "Compact" })).toBeNull();
  });

  it("shows the cleaned reason when the apply fails, such as a non-Databricks warehouse", async () => {
    const { client } = fakeJobs({
      compact: job("compact", "succeeded", { result: singleResult() }),
      apply: job("apply", "failed", {
        error: "Error: failed to generate compaction SQL\n\nCaused by:\n    OPTIMIZE is not supported on the duckdb dialect",
      }),
    });
    renderControls(client);
    typeTable("cat.sch.orders");
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    await screen.findByLabelText("Tables this plan compacts", {}, { timeout: 3000 });
    fireEvent.click(screen.getByRole("button", { name: "Compact" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, compact these tables" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("Compact: failed.");
    expect(alert.textContent).toContain("OPTIMIZE is not supported");
  });

  it("shows the cleaned reason when planning is refused, and offers no apply", async () => {
    const { client, submitted } = fakeJobs({
      compact: job("compact", "failed", {
        error: "Error: refusing to plan a compaction of 'orders': `rocky compact` takes a fully qualified table",
      }),
    });
    renderControls(client);
    typeTable("cat.sch.orders");
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("Plan compact: failed.");
    expect(screen.queryByRole("button", { name: "Compact" })).toBeNull();
    expect(submitted.map((s) => s.kind)).toEqual(["compact"]);
  });

  it("offers no apply, and says why, when the command could not save the plan", async () => {
    const { client } = fakeJobs({ compact: job("compact", "succeeded", { result: singleResult("cat.sch.orders", null) }) });
    renderControls(client);
    typeTable("cat.sch.orders");
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    await screen.findByText(/could not save this plan/, {}, { timeout: 3000 });
    expect(screen.queryByRole("button", { name: "Compact" })).toBeNull();
  });

  it("drops the plan when the table typed no longer matches it", async () => {
    const { client } = fakeJobs({ compact: job("compact", "succeeded", { result: singleResult() }) });
    renderControls(client);
    typeTable("cat.sch.orders");
    fireEvent.click(screen.getByRole("button", { name: "Plan compact" }));
    await screen.findByLabelText("Tables this plan compacts", {}, { timeout: 3000 });
    typeTable("cat.sch.other");
    expect(screen.queryByLabelText("Tables this plan compacts")).toBeNull();
    expect(screen.queryByRole("button", { name: "Compact" })).toBeNull();
  });
});

describe("compactPlanSummary", () => {
  it("reads the plan id, tables, counts and SQL from a single-table and a catalog result", () => {
    const one = compactPlanSummary(job("compact", "succeeded", { result: singleResult() }));
    expect(one).toMatchObject({ planId: PLAN_ID, tables: ["cat.sch.orders"], statementCount: 2, targetSizeMb: 256 });
    const many = compactPlanSummary(job("compact", "succeeded", { result: catalogResult }));
    expect(many.tables).toEqual(["cat.raw.events", "cat.sch.customers", "cat.sch.orders"]);
    expect(many.statementCount).toBe(6);
  });

  it("has no plan id unless it is 64 lowercase hex characters", () => {
    for (const bad of ["", "B".repeat(64), "b".repeat(63), "../x"]) {
      expect(compactPlanSummary(job("compact", "succeeded", { result: singleResult("a.b.c", bad) })).planId).toBeNull();
    }
    expect(compactPlanSummary(job("compact", "succeeded")).planId).toBeNull();
  });
});
