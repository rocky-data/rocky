import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewOutput } from "@rocky-types/review";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { ReviewScreen } from "./ReviewScreen";
import type { PlanLoaders } from "./PlanDetail";

const PLAN = "c".repeat(64);

const QUEUE: ReviewQueueOutput = {
  version: "1.74.0",
  command: "review",
  ranking: "blast_radius × classification × staleness",
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
});
