import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import type { PreviewRowsOutput } from "@rocky-types/preview_rows";
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
      models: ["orders"],
      preview_model: "orders",
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

/** What the sample route answers, for the one test that lets a sample run. */
const SAMPLE: PreviewRowsOutput = {
  version: "1.74.0",
  command: "preview-rows",
  model: "orders",
  cte: null,
  columns: ["id"],
  rows: [[1]],
  row_count: 1,
  limit_applied: 20,
  truncated: false,
  executed_sql: "SELECT id FROM orders LIMIT 20",
  adapter_kind: "duckdb",
  duration_ms: 1,
};

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
    // The offer is the button, and it exists only once the model is known.
    // Waiting for it also waits out the fallback's "not known yet" section,
    // which shares the region's name and is what a slower product read
    // shows first.
    await screen.findByRole("button", { name: /Show \d+ rows/ });
    const panel = screen.getByRole("region", { name: "Sample rows" });
    expect(panel.textContent).toContain("revenue_daily");
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
  /// field — "backfill: 3 model(s)" — and the real names in `models`. Three
  /// models is not one to sample, so the offer is not made; the card names
  /// them instead.
  it("does not offer to sample a backfill over several models, and names them", async () => {
    const backfill: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          capability: "backfill",
          model: "backfill: 3 model(s)",
          models: ["orders", "customers", "payments"],
          preview_model: null,
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
    expect(screen.getByText(/This plan touches 3 models: orders, customers, payments/)).toBeTruthy();
  });

  /// The screen used a `^[a-zA-Z0-9_]+$` regex on the label to decide whether
  /// it was a model name. `backfill_3_models` passed, and a click would have
  /// sampled a real model of that name. The label is display text; only the
  /// recorded set can name a model (#1815).
  it("never treats an identifier-shaped label as the model to sample", async () => {
    const shaped: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          capability: "backfill",
          model: "backfill_3_models",
          models: ["orders", "customers", "payments"],
          preview_model: null,
          blast_radius: undefined,
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => shaped),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    expect(screen.queryByRole("region", { name: "Sample rows" })).toBeNull();
  });

  /// A row whose `models` is empty names no compiled model: a plan-level row
  /// from before the engine kept its set, a replication target, a removed
  /// model. That is "unknown", never "the label is the model".
  it("says the queue names no compiled model rather than parsing the label", async () => {
    const unrecorded: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          capability: "gc",
          model: "gc",
          models: [],
          preview_model: null,
          blast_radius: undefined,
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => unrecorded),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    expect(screen.getByText(/names no compiled model for this plan \(gc "gc"\)/)).toBeTruthy();
  });

  /// A graph key is not a licence to read. The engine says per row what the
  /// samples route would read (`preview_model`); a dotted model name, a
  /// model a restore recorded that is gone, a model with compile errors are
  /// keys the route refuses, and the screen must not offer them (#1815,
  /// review round two). Offering from `models` makes this fail.
  it("offers no sample for a model the samples route would refuse", async () => {
    const refused: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          model: "v2.fct_orders",
          models: ["v2.fct_orders"],
          preview_model: null,
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => refused),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    expect(
      screen.getByText(/The samples route would read none of the models this plan names \(v2\.fct_orders\)/),
    ).toBeTruthy();
  });

  /// A product-bound plan still in the queue whose row the engine marked
  /// unreadable (`preview_model: null` — a time-interval model, say) must not
  /// fall back to the product's output model: that offers the very read the
  /// route refuses. The product stands in only once the plan has left the
  /// queue (#1815, review round three).
  it("does not fall back to the product's model while the queue says none is readable", async () => {
    const unreadable: ReviewQueueOutput = {
      ...QUEUE,
      pending: [{ ...QUEUE.pending[0], models: ["revenue_daily"], preview_model: null }],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({
            ...STATUS,
            product_id: "product:revenue_daily",
            spec_digest: "sha256:1111",
          })),
          queue: vi.fn(async () => unreadable),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    // The product read did happen (the spec-drift card needs it)…
    await screen.findByText("unchanged");
    // …and still no offer.
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    expect(screen.getByText(/The samples route would read none of the models this plan names/))
      .toBeTruthy();
  });

  /// The apply-time gate records one decision per touched model, and the
  /// queue keeps one row per (plan, model): a plan over two models under two
  /// rules pends twice. Keeping only the first row showed one reason and one
  /// model beside a command that clears both (#1815).
  it("shows every escalation of one plan, and one command that clears them all", async () => {
    const twice: ReviewQueueOutput = {
      ...QUEUE,
      total: 2,
      pending: [
        {
          ...QUEUE.pending[0],
          decision_ref: "2026-09-06T09:00:00Z|aaa|orders",
          model: "orders",
          models: ["orders"],
          rule_id: 2,
          reason: "orders carries a classified column",
        },
        {
          ...QUEUE.pending[0],
          decision_ref: "2026-09-06T09:00:00Z|aaa|customers",
          model: "customers",
          models: ["customers"],
          preview_model: "customers",
          rule_id: 5,
          reason: "customers is a governed product input",
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => twice),
        })}
      />,
    );

    await screen.findByText("orders carries a classified column");
    expect(screen.getByText("customers is a governed product input")).toBeTruthy();
    expect(screen.getByText("#2")).toBeTruthy();
    expect(screen.getByText("#5")).toBeTruthy();
    expect(screen.getByText(/2 escalations name this plan/)).toBeTruthy();
    // Two models is not one to sample; both are named.
    expect(screen.getByText("no single model to sample")).toBeTruthy();
    expect(screen.getByText(/touches 2 models: orders, customers/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
    // One command, and it says what it clears.
    expect(screen.getAllByText(`rocky review ${PLAN} --approve`)).toHaveLength(1);
    expect(screen.getByText(/clears every one of the 2 escalations/)).toBeTruthy();
  });

  /// Backfill is not the only capability whose queue `model` holds a sentence:
  /// gc and restore write one too. The card must not call those a backfill —
  /// a restore covers one tombstoned model and a gc is a deletion.
  it("does not describe a restore plan as a backfill", async () => {
    const restore: ReviewQueueOutput = {
      ...QUEUE,
      pending: [
        {
          ...QUEUE.pending[0],
          capability: "restore",
          model: "restore: 1 tombstoned model",
          models: ["orders", "customers"],
          preview_model: null,
          blast_radius: undefined,
        },
      ],
    };

    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, product_id: undefined })),
          queue: vi.fn(async () => restore),
        })}
      />,
    );

    await screen.findByText("no single model to sample");
    expect(screen.getByText(/This plan touches 2 models: orders, customers/)).toBeTruthy();
    // The capability is shown where the engine put it — on the escalation —
    // and the sample card does not guess one.
    expect(screen.getByText("restore")).toBeTruthy();
    expect(screen.queryByText(/backfill/)).toBeNull();
  });

  /// "The queue does not name this plan" and "the queue could not be read" are
  /// different facts. Collapsing them told a reader an escalation was resolved
  /// when the server had refused the request.
  it("says the queue was refused rather than claiming the plan is not in it", async () => {
    render(
      <PlanDetail
        planId={PLAN}
        loaders={loaders({
          queue: vi.fn(async () => {
            throw new ApiError(503, {
              code: "state_unreadable",
              message: "the state store could not be read",
            });
          }),
        })}
      />,
    );

    // The queue feeds several panels, so its refusal shows in more than one.
    await screen.findAllByText(/state_unreadable/);
    expect(screen.queryByText("not in the queue")).toBeNull();
    // The sample fallback is one of them. It used to read the same `null`
    // as "no longer in the queue" and print that beside the refusal: an
    // unreadable queue is unknown, not empty (#1815).
    expect(screen.queryByText(/no longer in the queue/)).toBeNull();
    expect(screen.getByText("Which model to sample is not known yet.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /Show \d+ rows/ })).toBeNull();
  });

  /// Navigating from plan A straight to plan B, with B's reads slower. The
  /// heading updates at once; before this, every panel under it kept A's
  /// answer — A's status, A's sample, and A's approve command under B's
  /// heading — and the sample panel kept its consent, so B's warehouse query
  /// ran with no click (#1815). The check is synchronous after the rerender:
  /// no frame may show A under B.
  it("carries nothing of plan A under plan B's heading, and asks again before sampling", async () => {
    const PLAN_B = "b".repeat(64);
    let releaseB!: (value: ReviewStatusOutput) => void;
    const statusB = new Promise<ReviewStatusOutput>((resolve) => {
      releaseB = resolve;
    });
    const queue: ReviewQueueOutput = {
      ...QUEUE,
      total: 2,
      pending: [
        QUEUE.pending[0],
        {
          ...QUEUE.pending[0],
          plan_id: PLAN_B,
          model: "customers",
          models: ["customers"],
          preview_model: "customers",
          approve_command: `rocky review ${PLAN_B} --approve`,
        },
      ],
    };
    const sampled: string[] = [];
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      sampled.push(String(input));
      return new Response(JSON.stringify(SAMPLE), { status: 200 });
    });
    const originalFetch = globalThis.fetch;
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    try {
      const shared = loaders({
        status: vi.fn(async (planId: string) => (planId === PLAN ? STATUS : statusB)),
        diff: vi.fn(async (planId: string) => ({ ...DIFF, plan_id: planId })),
        queue: vi.fn(async () => queue),
      });
      const view = render(<PlanDetail planId={PLAN} loaders={shared} />);
      await screen.findByText("awaiting a human");
      expect(screen.getByText(`rocky review ${PLAN} --approve`)).toBeTruthy();
      // Consent for A's model, given by a click.
      fireEvent.click(screen.getByRole("button", { name: /Show \d+ rows/ }));
      await waitFor(() => expect(sampled).toHaveLength(1));
      expect(sampled[0]).toContain("/models/orders/rows");

      view.rerender(<PlanDetail planId={PLAN_B} loaders={shared} />);

      // Synchronously: B's heading, and nothing of A under it.
      expect(document.querySelector(`[title="${PLAN_B}"]`)).toBeTruthy();
      expect(document.querySelector(`[title="${PLAN}"]`)).toBeNull();
      expect(screen.queryByText(`rocky review ${PLAN} --approve`)).toBeNull();
      expect(screen.queryByText("awaiting a human")).toBeNull();
      expect(screen.getByText("reading the plan…")).toBeTruthy();

      releaseB({ ...STATUS, plan_id: PLAN_B });
      await screen.findByText(`rocky review ${PLAN_B} --approve`);
      // B's model is offered, not read: the button is back and the only
      // query that ever ran is A's.
      expect(screen.getByRole("button", { name: /Show \d+ rows/ })).toBeTruthy();
      expect(screen.getByRole("region", { name: "Sample rows" }).textContent).toContain(
        "customers",
      );
      expect(sampled).toHaveLength(1);
    } finally {
      globalThis.fetch = originalFetch;
    }
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
