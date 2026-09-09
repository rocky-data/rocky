import { render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ReviewQueueOutput } from "@rocky-types/review_queue";
import { ApiError } from "../api";
import { QueuePanel } from "./QueuePanel";

const NOW = Date.parse("2026-09-06T12:00:00Z");

const QUEUE: ReviewQueueOutput = {
  version: "1.74.0",
  command: "review",
  ranking: "blast_radius × classification × staleness",
  total: 2,
  excluded_non_plan_rows: 3,
  pending: [
    {
      plan_id: "a".repeat(64),
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
      approve_command: `rocky review ${"a".repeat(64)} --approve`,
    },
    {
      plan_id: "b".repeat(64),
      decision_ref: "2026-09-06T11:00:00Z|bbb|customers",
      timestamp: "2026-09-06T11:00:00Z",
      principal: "agent",
      capability: "schema_change.additive",
      model: "customers",
      models: ["customers"],
      preview_model: "customers",
      rule_id: null,
      reason: "the default effect asks for review",
      blast_radius: null,
      classification_weight: 1,
      staleness_seconds: 30,
      score: 3.25,
      approve_command: `rocky review ${"b".repeat(64)} --approve`,
    },
  ],
};

describe("QueuePanel", () => {
  it("lists what the engine returned, in the engine's order, with its ranking and exclusions", async () => {
    const load = vi.fn(async () => QUEUE);
    render(<QueuePanel load={load} now={NOW} />);

    await screen.findByText("2 waiting for review");
    expect(screen.getByText(/ranked by blast_radius/)).toBeTruthy();

    // The order on screen is the order the engine sent, never re-sorted here.
    const items = screen.getAllByRole("listitem").map((li) => li.textContent ?? "");
    expect(items).toHaveLength(2);
    expect(items[0]).toContain("orders");
    expect(items[1]).toContain("customers");

    // A blast radius the engine did not compute says so rather than showing 0.
    expect(items[1]).toContain("not computed");
    expect(items[0]).toContain("7");

    // The excluded rows are explained, not hidden: "2 waiting" beside a ledger
    // holding five rows is otherwise a question the screen leaves open.
    expect(screen.getByText(/3 further rows in the ledger/)).toBeTruthy();
  });

  /// On a plan-level row `model` is a label — "backfill: 3 model(s)" — and
  /// the engine reports the names in `models`. The row shows them; an
  /// ordinary row, whose set is its label, does not repeat itself.
  it("lists the recorded model set on a plan-level row, and only there", async () => {
    const load = vi.fn(async () => ({
      ...QUEUE,
      pending: [
        QUEUE.pending[0],
        {
          ...QUEUE.pending[1],
          capability: "backfill" as const,
          model: "backfill: 3 model(s)",
          models: ["orders", "customers", "payments"],
          preview_model: null,
        },
      ],
    }));
    render(<QueuePanel load={load} now={NOW} />);

    await screen.findByText("2 waiting for review");
    const [ordinary, planLevel] = screen.getAllByRole("listitem");
    expect(within(planLevel).getByText("models")).toBeTruthy();
    expect(within(planLevel).getByText("orders, customers, payments")).toBeTruthy();
    expect(within(ordinary).queryByText("models")).toBeNull();
  });

  it("says plainly when nothing is waiting", async () => {
    const load = vi.fn(async () => ({
      ...QUEUE,
      total: 0,
      excluded_non_plan_rows: 0,
      pending: [],
    }));
    render(<QueuePanel load={load} now={NOW} />);

    await screen.findByText("Nothing is waiting for review");
    expect(screen.getByText(/No escalation is outstanding/)).toBeTruthy();
    expect(screen.queryByText(/further rows in the ledger/)).toBeNull();
  });

  it("renders a refusal as the engine's own code, not as a generic failure", async () => {
    const load = vi.fn(async () => {
      throw new ApiError(503, {
        code: "engine_busy",
        message: "the state store is locked by a running job",
        remediation_hint: "retry in a moment",
      });
    });
    render(<QueuePanel load={load} now={NOW} />);

    await waitFor(() => expect(screen.getByText("engine_busy")).toBeTruthy());
    expect(screen.getByText("refused (503)")).toBeTruthy();
    expect(screen.getByText("retry in a moment")).toBeTruthy();
  });
});
