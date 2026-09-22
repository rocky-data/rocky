import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { ReviewScreen } from "./ReviewScreen";
import type { PlanLoaders } from "./PlanDetail";
import { reviewPath } from "./paths";

const PLAN = "c".repeat(64);

const QUEUE: ReviewQueueOutput = {
  version: "1.74.0",
  command: "review",
  ranking: "blast_radius × change_class × staleness",
  total: 0,
  excluded_non_plan_rows: 0,
  pending: [],
};

const PLAN_LOADERS: PlanLoaders = {
  status: vi.fn(
    async (): Promise<ReviewStatusOutput> => ({
      version: "1.74.0",
      command: "review_status",
      plan_id: PLAN,
      kind: "backfill",
      reviewed: false,
    }),
  ),
  diff: vi.fn(
    async (): Promise<ReviewOutput> => ({
      version: "1.74.0",
      command: "review",
      plan_id: PLAN,
      base_ref: "HEAD",
      approved: false,
      marker_written: false,
      breaking_changes: [],
    }),
  ),
  queue: vi.fn(async () => QUEUE),
  product: vi.fn(async () => ({}) as ProductStatusOutput),
};

function at(path: string): void {
  window.history.pushState(null, "", path);
}

afterEach(() => at("/ui/"));

describe("ReviewScreen", () => {
  it("shows the queue at the lane root", async () => {
    at("/ui/review");
    render(<ReviewScreen queueLoad={async () => QUEUE} planLoaders={PLAN_LOADERS} />);
    await screen.findByText("Nothing is waiting for review");
  });

  it("shows one plan when the path names it", async () => {
    at(`/ui/review/${PLAN}`);
    render(<ReviewScreen queueLoad={async () => QUEUE} planLoaders={PLAN_LOADERS} />);
    await screen.findByText("backfill");
    expect(screen.queryByText("Nothing is waiting for review")).toBeNull();
  });

  // Every plan id above is 64-character hex, where `encodeURIComponent` is a
  // no-op: it would pass with or without the fix. A colon is the realistic
  // character that escapes (plan ids are written `.rocky/plans/<id>.json`,
  // and the engine's own tests carry ids like `freeze:m-1`).
  it("decodes a plan id that needs escaping before asking the loaders for it (#2090)", async () => {
    const planId = "draft:orders";
    at(reviewPath(planId));

    const statusCalls: string[] = [];
    const diffCalls: string[] = [];
    const queueWithEscalation: ReviewQueueOutput = {
      version: "1.74.0",
      command: "review",
      ranking: "blast_radius × change_class × staleness",
      total: 1,
      excluded_non_plan_rows: 0,
      pending: [
        {
          plan_id: planId,
          decision_ref: "2026-09-06T09:00:00Z|draft:orders|orders",
          timestamp: "2026-09-06T09:00:00Z",
          principal: "agent",
          capability: "schema_change.breaking",
          model: "orders",
          models: ["orders"],
          preview_model: null,
          rule_id: 2,
          reason: "a breaking schema change needs a human",
          blast_radius: 7,
          change_class_weight: 3,
          staleness_seconds: 10_800,
          score: 42.5,
          approve_command: `rocky review ${planId} --approve`,
        },
      ],
    };
    const loaders: PlanLoaders = {
      status: vi.fn(async (id: string): Promise<ReviewStatusOutput> => {
        statusCalls.push(id);
        return {
          version: "1.74.0",
          command: "review_status",
          plan_id: id,
          kind: "backfill",
          reviewed: false,
        };
      }),
      diff: vi.fn(async (id: string): Promise<ReviewOutput> => {
        diffCalls.push(id);
        return {
          version: "1.74.0",
          command: "review",
          plan_id: id,
          base_ref: "HEAD",
          approved: false,
          marker_written: false,
          breaking_changes: [],
        };
      }),
      queue: vi.fn(async () => queueWithEscalation),
      product: vi.fn(async () => ({}) as ProductStatusOutput),
    };

    render(<ReviewScreen planLoaders={loaders} />);

    // The escalation panel found the row: the filter compared the decoded id
    // against the real `plan_id`, not the still-escaped segment.
    await screen.findByText("a breaking schema change needs a human");
    expect(screen.queryByText("not in the queue")).toBeNull();

    // The loaders were asked for the real id, not the escaped segment
    // (`draft%3Aorders`), which the default loaders would otherwise encode a
    // second time.
    expect(statusCalls).toEqual([planId]);
    expect(diffCalls).toEqual([planId]);
  });
});
