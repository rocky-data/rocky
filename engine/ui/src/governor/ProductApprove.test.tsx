import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { JobKind, JobStatus } from "@rocky-types/job_status";
import type { ProductJournalOutput } from "@rocky-types/product_journal";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { READ_ONLY_REASON, WriteAccessProvider, type JobClient, type WriteAccess } from "../operator";
import { ProductApprove, approvableSpec, productApproveSummary } from "./ProductApprove";
import { ProductTimeline, type ProductLoaders } from "./ProductsScreen";

const OPERATOR: WriteAccess = { kind: "operator", scope: "full" };
const READ_ONLY: WriteAccess = { kind: "read_only", reason: READ_ONLY_REASON };

const DIGEST = "sha256:" + "1".repeat(64);
const OLD_DIGEST = "sha256:" + "2".repeat(64);

function status(extra: Partial<ProductStatusOutput> = {}): ProductStatusOutput {
  return {
    version: "1",
    command: "product",
    product: "revenue_daily",
    spec_present: true,
    spec_digest: DIGEST,
    output_model: "revenue_daily_model",
    journal_rows: 0,
    staging_journal_present: false,
    ...extra,
  } as unknown as ProductStatusOutput;
}

const APPROVED = {
  approver: "dev@example.com",
  snapshot_path: ".rocky/x/approved.toml",
  spec_digest: OLD_DIGEST,
};

function job(state: JobStatus["state"], extra: Partial<JobStatus> = {}): JobStatus {
  return { job_id: "job_1", kind: "product_approve", state, submitted_at: "2026-10-16T00:00:00Z", ...extra };
}

function fakeJobs(answer: JobStatus) {
  const submitted: { kind: JobKind; body: Record<string, unknown> }[] = [];
  const client: JobClient = {
    submit: vi.fn(async (kind, body) => {
      submitted.push({ kind, body });
      return { job_id: "job_1" };
    }),
    status: vi.fn(async () => answer),
    cancel: vi.fn(async (jobId: string) => ({ job_id: jobId, signal: "terminate" as const })),
  };
  return { client, submitted };
}

function renderApprove(
  client: JobClient,
  st: ProductStatusOutput = status(),
  access: WriteAccess = OPERATOR,
  onDone?: () => void,
) {
  return render(
    <WriteAccessProvider value={access}>
      <ProductApprove name="revenue_daily" status={st} jobs={client} onDone={onDone} />
    </WriteAccessProvider>,
  );
}

describe("approvableSpec", () => {
  it("offers a spec that reads and is not approved, or differs from the approved one", () => {
    expect(approvableSpec(status())).toBe(DIGEST);
    expect(approvableSpec(status({ approval: APPROVED, spec_matches_approval: false }))).toBe(DIGEST);
  });

  it("offers nothing for an approved spec, a missing or broken spec, or one with no digest", () => {
    expect(approvableSpec(status({ approval: APPROVED, spec_matches_approval: true }))).toBeNull();
    expect(approvableSpec(status({ spec_present: false }))).toBeNull();
    expect(approvableSpec(status({ spec_error: "[spec-parse] bad" }))).toBeNull();
    expect(approvableSpec(status({ spec_digest: null }))).toBeNull();
    expect(approvableSpec(status({ spec_digest: "" }))).toBeNull();
  });
});

describe("Approve the spec", () => {
  it("draws the button disabled on a read-only page and submits nothing", () => {
    const { client, submitted } = fakeJobs(job("succeeded"));
    renderApprove(client, status(), READ_ONLY);
    const button = screen.getByRole("button", { name: "Approve spec" });
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(submitted).toEqual([]);
    expect(screen.queryByRole("group")).toBeNull();
  });

  it("asks first, names the product, the digest and the output model, and submits nothing until confirmed", async () => {
    const { client, submitted } = fakeJobs(
      job("succeeded", { result: { spec_digest: DIGEST, approver: "dev@example.com (http_api)", already_approved: false } }),
    );
    renderApprove(client);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    const group = screen.getByRole("group");
    expect(group.textContent).toContain("product revenue_daily");
    expect(group.textContent).toContain(DIGEST);
    expect(group.textContent).toContain("output model revenue_daily_model");
    expect(group.textContent).toContain("http_api");
    expect(submitted).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(screen.queryByRole("group")).toBeNull();
    expect(submitted).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    await waitFor(() =>
      expect(submitted).toEqual([
        { kind: "product_approve", body: { name: "revenue_daily", expect_spec_digest: DIGEST } },
      ]),
    );
  });

  it("says the approval it replaces when the spec changed since it was approved", () => {
    const { client } = fakeJobs(job("succeeded"));
    renderApprove(client, status({ approval: APPROVED, spec_matches_approval: false }));
    expect(screen.getByText(/differs from the approved one/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    expect(screen.getByRole("group").textContent).toContain(`replaces the approval of ${OLD_DIGEST}`);
  });

  it("shows the digest and the recorded approver after it succeeds, and refreshes the status", async () => {
    const { client } = fakeJobs(
      job("succeeded", { result: { spec_digest: DIGEST, approver: "dev@example.com (http_api)", already_approved: false } }),
    );
    const onDone = vi.fn();
    renderApprove(client, status(), OPERATOR, onDone);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    await screen.findByText(`Approve: approved ${DIGEST}.`, {}, { timeout: 3000 });
    expect(screen.getByText(/Recorded approver: dev@example.com \(http_api\)/)).toBeTruthy();
    expect(document.body.textContent).toContain("does not prove who was at the page");
    expect(screen.queryByRole("alert")).toBeNull();
    expect(onDone).toHaveBeenCalledTimes(1);
  });

  it("warns, never says done, when the approval covers a different spec than the one confirmed", async () => {
    const other = "sha256:" + "3".repeat(64);
    const { client } = fakeJobs(
      job("succeeded", { result: { spec_digest: other, approver: "dev@example.com (http_api)" } }),
    );
    renderApprove(client);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("the spec changed after this page read it");
    expect(alert.textContent).toContain(other);
    expect(alert.textContent).toContain(DIGEST);
  });

  it("says nothing was written for a spec that was already approved", async () => {
    const { client } = fakeJobs(
      job("succeeded", { result: { spec_digest: DIGEST, approver: "dev@example.com", already_approved: true } }),
    );
    renderApprove(client);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    await screen.findByText(`Approve: ${DIGEST} was already approved. Nothing was written.`, {}, { timeout: 3000 });
    expect(screen.queryByText(/Recorded approver/)).toBeNull();
  });

  it("shows the refusal, and says nothing was approved, when the spec changed since the page read it", async () => {
    const { client } = fakeJobs(
      job("failed", {
        error: `Error: [approval-digest-mismatch] the working spec of product 'revenue_daily' has digest sha256:${"3".repeat(64)}, not the expected ${DIGEST}; the spec changed since you read it. Nothing was written.`,
      }),
    );
    renderApprove(client);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("approval-digest-mismatch");
    expect(alert.textContent).toContain("Nothing was written");
    expect(screen.queryByText(/approved sha256/)).toBeNull();
  });

  it("shows the cleaned reason when the engine refuses, such as work in flight or no git identity", async () => {
    const { client } = fakeJobs(
      job("failed", {
        error:
          "Error: [approval-refused-in-flight] refusing to approve a new spec revision for product 'revenue_daily' while fulfillment work is in flight",
      }),
    );
    renderApprove(client);
    fireEvent.click(screen.getByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    const alert = await screen.findByRole("alert", {}, { timeout: 3000 });
    expect(alert.textContent).toContain("Approve: failed.");
    expect(alert.textContent).toContain("approval-refused-in-flight");
  });

  it("is not on the page for a spec that is already approved", () => {
    const { client } = fakeJobs(job("succeeded"));
    renderApprove(client, status({ approval: APPROVED, spec_matches_approval: true }));
    expect(screen.queryByRole("button", { name: "Approve spec" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Approve the spec" })).toBeNull();
  });
});

describe("productApproveSummary", () => {
  it("reads the digest, the approver and the already-approved flag, and nothing else", () => {
    expect(
      productApproveSummary(
        job("succeeded", { result: { spec_digest: DIGEST, approver: "a (http_api)", already_approved: true, snapshot_path: "/secret" } }),
      ),
    ).toEqual({ specDigest: DIGEST, approver: "a (http_api)", alreadyApproved: true });
    expect(productApproveSummary(job("succeeded"))).toBeNull();
    expect(productApproveSummary(job("succeeded", { result: { spec_digest: "" } }))).toBeNull();
  });
});

describe("The product page", () => {
  it("offers the approval beside the product's standing and reloads both after it", async () => {
    const reads = { status: 0 };
    const loaders: ProductLoaders = {
      list: vi.fn(),
      journal: vi.fn(
        async (): Promise<ProductJournalOutput> => ({
          version: "1",
          command: "product",
          product: "revenue_daily",
          product_id: "product:revenue_daily",
          count: 0,
          rows: [],
        }),
      ),
      status: vi.fn(async () => {
        reads.status += 1;
        return status();
      }),
    };
    const { client, submitted } = fakeJobs(
      job("succeeded", { result: { spec_digest: DIGEST, approver: "dev@example.com (http_api)" } }),
    );
    render(
      <WriteAccessProvider value={OPERATOR}>
        <ProductTimeline name="revenue_daily" loaders={loaders} jobs={client} />
      </WriteAccessProvider>,
    );
    fireEvent.click(await screen.findByRole("button", { name: "Approve spec" }));
    fireEvent.click(screen.getByRole("button", { name: "Yes, approve this spec" }));
    await waitFor(() =>
      expect(submitted).toEqual([
        { kind: "product_approve", body: { name: "revenue_daily", expect_spec_digest: DIGEST } },
      ]),
    );
    await waitFor(() => expect(reads.status).toBeGreaterThan(1), { timeout: 3000 });
  });
});
