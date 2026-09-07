import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import statusFixture from "@rocky-fixtures/review_status.json";
import { ApiError } from "../api";
import { PlanDetail, type PlanLoaders, describeFinding, productNameFromId } from "./PlanDetail";

// The status panel reads a payload captured from the live engine, so a shape
// change there fails this test rather than passing against a hand-written
// stand-in. The other three producers have no capture yet — the fixture roster
// covers `review_status` only — so their payloads are written here, typed.
const STATUS = statusFixture as ReviewStatusOutput;
const PLAN = STATUS.plan_id;

const DIFF: ReviewOutput = {
  version: "1.74.0",
  command: "review",
  plan_id: PLAN,
  base_ref: "HEAD",
  approved: false,
  marker_written: false,
  breaking_changes: [
    {
      change: {
        kind: "column_type_changed",
        model: "orders",
        column: "total",
        old_type: "INT64",
        new_type: "INT32",
        narrowing: true,
      },
      severity: "breaking",
    },
  ],
};

const QUEUE: ReviewQueueOutput = {
  version: "1.74.0",
  command: "review",
  ranking: "blast_radius × classification × staleness",
  total: 1,
  excluded_non_plan_rows: 0,
  pending: [
    {
      plan_id: PLAN,
      decision_ref: "2026-09-06T09:00:00Z|aaa|orders",
      timestamp: "2026-09-06T09:00:00Z",
      principal: "agent",
      capability: "schema_change.breaking",
      model: "orders",
      rule_id: 2,
      reason: "a breaking schema change needs a human",
      blast_radius: 7,
      classification_weight: 3,
      staleness_seconds: 10_800,
      score: 42.5,
      approve_command: `rocky review ${PLAN} --approve`,
    },
  ],
};

const PRODUCT: ProductStatusOutput = {
  version: "1.74.0",
  command: "product",
  name: "revenue_daily",
  spec_present: true,
  spec_digest: "sha256:1111",
  output_model: "revenue_daily",
} as unknown as ProductStatusOutput;

/** The queue after a sign-off: the escalation is resolved, so nothing pends. */
const EMPTY_QUEUE: ReviewQueueOutput = { ...QUEUE, total: 0, pending: [] };

function loaders(overrides: Partial<PlanLoaders> = {}): PlanLoaders {
  return {
    status: vi.fn(async () => STATUS),
    diff: vi.fn(async () => DIFF),
    queue: vi.fn(async () => QUEUE),
    product: vi.fn(async () => PRODUCT),
    ...overrides,
  };
}

describe("PlanDetail", () => {
  /// The queue is not a durable source for the model name: an approval marker
  /// resolves the escalation, so the entry disappears exactly when the table
  /// it built starts existing. Reading the queue alone meant the panel could
  /// never show real rows for a product's first plan.
  it("still offers a sample after the plan is signed off and has left the queue", async () => {
    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          // The live capture is of a plan that is not product-bound, so the
          // binding is set here: this test is about a product's plan.
          status: vi.fn(async () => ({
            ...STATUS,
            reviewed: true,
            product_id: "product:revenue_daily",
          })),
          queue: vi.fn(async () => EMPTY_QUEUE),
        })}
      />,
    );

    // Two cards say "signed off" once a marker exists — the review status and
    // the approval. Either is enough to know the queue has released the plan.
    await screen.findAllByText("signed off");
    const panel = await screen.findByRole("region", { name: "Sample rows" });
    expect(panel.textContent).toContain("revenue_daily");
    expect(screen.getByRole("button", { name: /Show \d+ rows/ })).toBeTruthy();
  });

  /// Absent is not empty. A missing panel would read as "this plan touches no
  /// data", which is a different claim with a different fix.
  it("says it has no model to sample rather than dropping the panel", async () => {
    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, reviewed: true, product_id: undefined })),
          queue: vi.fn(async () => EMPTY_QUEUE),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
  });

  /// A backfill escalation puts a display sentence in the queue's `model`
  /// field — "backfill: 3 model(s)". Feeding that to the samples route earns a
  /// 400 `invalid_model_name` on every click, so the offer must not be made.
  it("does not offer to sample a backfill, whose queue entry names no single model", async () => {
    const backfill: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          capability: "backfill",
          model: "backfill: 3 model(s)",
          blast_radius: undefined,
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => backfill),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    // and it says which of the two reasons applies, quoting what it was given
    expect(screen.getByText(/backfill: 3 model\(s\)/)).toBeTruthy();
  });

  it("shows the plan, its findings, the escalation and the approve command", async () => {
    render(<PlanDetail planId={PLAN} loaders={loaders()} />);

    await screen.findByText(STATUS.kind);
    expect(screen.getByText("awaiting a human")).toBeTruthy();
    await waitFor(() =>
      expect(screen.getByText(/orders.total changes type, INT64 to INT32/)).toBeTruthy(),
    );
    expect(screen.getByText(/\(narrowing\)/)).toBeTruthy();
    expect(screen.getByText("a breaking schema change needs a human")).toBeTruthy();
    expect(screen.getByText("#2")).toBeTruthy();
    // Approving is a command to copy, never a control on the page.
    expect(screen.getByText(`rocky review ${PLAN} --approve`)).toBeTruthy();
  });

  it("tells a skipped gate apart from a clean one", async () => {
    const skipped = render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          diff: vi.fn(async () => ({
            ...DIFF,
            breaking_changes: undefined,
            message: "the base compile failed",
          })),
        })}
      />,
    );
    await screen.findByText("the gate was skipped");
    expect(screen.getByText("the base compile failed")).toBeTruthy();
    expect(screen.queryByText("nothing")).toBeNull();
    skipped.unmount();

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({ diff: vi.fn(async () => ({ ...DIFF, breaking_changes: [] })) })}
      />,
    );
    await screen.findByText("nothing");
    expect(screen.queryByText("the gate was skipped")).toBeNull();
  });

  it("says the spec moved when the plan's digest no longer matches the product's", async () => {
    const moved = render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({
            ...STATUS,
            product_id: "product:revenue_daily",
            spec_digest: "sha256:0000",
          })),
        })}
      />,
    );
    await screen.findByText("the spec moved");
    expect(screen.getByText(/Applying this plan would be refused/)).toBeTruthy();
    moved.unmount();

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({
            ...STATUS,
            product_id: "product:revenue_daily",
            spec_digest: "sha256:1111",
          })),
        })}
      />,
    );
    await screen.findByText("unchanged");
    expect(screen.queryByText("the spec moved")).toBeNull();
  });

  it("reads no product at all when the plan is not product-bound", async () => {
    const product = vi.fn(async () => PRODUCT);
    render(<PlanDetail planId={PLAN} loaders={loaders({ product })} />);
    await screen.findByText(STATUS.kind);
    expect(screen.getByText("not product-bound")).toBeTruthy();
    expect(product).not.toHaveBeenCalled();
  });

  it("renders a refused plan read as the engine's own code", async () => {
    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => {
            throw new ApiError(404, {
              code: "plan_not_found",
              message: "no plan file",
              remediation_hint: "check the id",
            });
          }),
        })}
      />,
    );
    await waitFor(() => expect(screen.getByText("plan_not_found")).toBeTruthy());
    expect(screen.getByText("refused (404)")).toBeTruthy();
  });

  it("offers no control that could change anything", async () => {
    const { container } = render(<PlanDetail planId={PLAN} loaders={loaders()} />);
    await screen.findByText(STATUS.kind);
    // The sample panel's button is the only one, and it only reads.
    const buttons = Array.from(container.querySelectorAll("button")).map(
      (b) => b.textContent ?? "",
    );
    expect(buttons).toEqual(["Show 20 rows"]);
    expect(container.querySelector("form")).toBeNull();
  });

  /// U2-P0's XSS row, the diff half — the sink that did not exist until this
  /// lane did. A breaking finding names a model and a column, and neither is
  /// the operator's: they come from whatever SQL an agent wrote.
  it("renders a hostile model name, column and reason as text, never as markup", async () => {
    const hostile = '<img src=x onerror="alert(1)">';
    const { container } = render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          // Built from the typed finding above rather than written fresh, so
          // the change's discriminant keeps its literal type.
          diff: vi.fn(
            async (): Promise<ReviewOutput> => ({
              ...DIFF,
              breaking_changes: (DIFF.breaking_changes ?? []).map((finding) => ({
                ...finding,
                change: { ...finding.change, model: hostile, column: hostile },
              })),
            }),
          ),
          queue: vi.fn(async () => ({
            ...QUEUE,
            pending: [{ ...QUEUE.pending[0], reason: `${hostile} needs a human` }],
          })),
        })}
      />,
    );

    // Twice on the page: once as the escalation's reason, once inside the
    // finding's description. Both are sinks; both must be text.
    await screen.findByText(`${hostile} needs a human`);
    const escaped = hostile.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    expect(screen.getAllByText(new RegExp(escaped)).length).toBeGreaterThanOrEqual(2);
    // …and never as an element. React escapes by default; this fails loudly
    // if anyone reaches for dangerouslySetInnerHTML in this lane.
    expect(container.querySelector("img")).toBeNull();
  });

  it("names a product from its identity, and describes every finding kind", () => {
    expect(productNameFromId("product:revenue_daily")).toBe("revenue_daily");
    expect(productNameFromId("revenue_daily")).toBe("revenue_daily");
    const cases: [Record<string, unknown>, string][] = [
      [{ kind: "model_removed", model: "orders" }, "orders is removed"],
      [{ kind: "model_added", model: "orders" }, "orders is added"],
      [
        { kind: "column_dropped", model: "orders", column: "total", data_type: "INT64" },
        "orders.total is dropped (was INT64)",
      ],
      [
        {
          kind: "column_added",
          model: "orders",
          column: "total",
          data_type: "INT64",
          nullable: false,
        },
        "orders.total is added (INT64, not null)",
      ],
      [
        {
          kind: "column_nullability_changed",
          model: "orders",
          column: "total",
          old_nullable: false,
          new_nullable: true,
        },
        "orders.total becomes nullable",
      ],
    ];
    for (const [change, expected] of cases) {
      expect(describeFinding({ change, severity: "breaking" } as never)).toBe(expected);
    }
    // A kind this build does not know is still rendered, never dropped.
    expect(
      describeFinding({
        change: { kind: "some_future_kind", model: "orders" },
        severity: "warning",
      } as never),
    ).toBe("orders: some future kind");
  });
});
