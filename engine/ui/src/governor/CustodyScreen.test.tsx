import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AuditForOutput } from "@rocky-types/audit_for";
import { NOT_RECORDED } from "../format";
import { CustodyScreen, MAX_SUBJECT_BYTES } from "./CustodyScreen";

const NOW = Date.parse("2026-09-05T08:00:00Z");
const PLAN = "b".repeat(64);

const CHAIN: AuditForOutput = {
  version: "1.74.0",
  command: "audit",
  subject: "fct_orders",
  subject_kind: "model",
  resolved: true,
  decisions: {
    availability: "available",
    total: 1,
    entries: [
      {
        timestamp: "2026-09-04T09:00:00Z",
        plan_id: PLAN,
        principal: "agent",
        capability: "apply",
        model: "fct_orders",
        effect: "require_review",
        rule_id: 1,
        reason: "<script>alert(1)</script> hostile reason",
      },
    ],
  },
  plan: {
    availability: "available",
    plan_id: PLAN,
    kind: "ai_authored",
    principal: "agent",
    diff_available: true,
    changes: [{ model: "fct_orders", capability: "schema_change.additive" }],
  },
  runs: {
    availability: "available",
    total: 1,
    runs: [
      {
        run_id: "run-custody-1",
        status: "Success",
        started_at: "2026-09-04T10:00:00Z",
        finished_at: "2026-09-04T10:01:00Z",
        triggering_identity: null,
      },
    ],
  },
  verify_after: {
    availability: "unavailable",
    note: "no verification row is recorded for this plan",
    total: 0,
    entries: [],
  },
  blast_radius: {
    availability: "available",
    model: "fct_orders",
    direct: ["mart_revenue"],
    transitive: ["mart_revenue", "<b>exec_dashboard</b>"],
    total: 2,
  },
};

describe("CustodyScreen", () => {
  it("renders the five links by availability with every id as text", async () => {
    const load = vi.fn(async () => CHAIN);
    render(<CustodyScreen subject="fct_orders" load={load} now={NOW} />);
    const decisions = await screen.findByRole("region", { name: "Decisions" });
    expect(within(decisions).getByRole("link", { name: PLAN })).toHaveAttribute(
      "href",
      `/ui/governor/custody/${PLAN}`,
    );
    expect(within(decisions).getByText("rule 1")).toBeInTheDocument();
    const runs = screen.getByRole("region", { name: "Runs" });
    expect(within(runs).getByRole("link", { name: "run-custody-1" })).toBeInTheDocument();
    expect(within(runs).getByText(NOT_RECORDED)).toBeInTheDocument();
    const verify = screen.getByRole("region", { name: "Verification after apply" });
    expect(
      within(verify).getByText(`${NOT_RECORDED}: no verification row is recorded for this plan`),
    ).toBeInTheDocument();
    expect(within(verify).queryByRole("table")).toBeNull();
    const blast = screen.getByRole("region", { name: "Blast radius" });
    expect(within(blast).getByText("Direct: mart_revenue")).toBeInTheDocument();
    expect(load).toHaveBeenCalledWith("fct_orders");
  });

  it("renders hostile values as text, never as markup", async () => {
    const { container } = render(<CustodyScreen subject="fct_orders" load={async () => CHAIN} now={NOW} />);
    expect(await screen.findByText("<script>alert(1)</script> hostile reason")).toBeInTheDocument();
    expect(screen.getByText(/<b>exec_dashboard<\/b>/)).toBeInTheDocument();
    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("b")).toBeNull();
  });

  it("says plainly when nothing references the subject", async () => {
    render(
      <CustodyScreen
        subject="nothing_here"
        load={async () => ({ ...CHAIN, subject: "nothing_here", resolved: false })}
        now={NOW}
      />,
    );
    expect(
      await screen.findByText(/nothing in the ledger, the run history, the plan files or the graph references it/),
    ).toBeInTheDocument();
  });

  it("submits the subject box to the address and refuses an over-long subject", async () => {
    render(<CustodyScreen subject={null} load={async () => CHAIN} now={NOW} />);
    expect(screen.getByText("No subject yet")).toBeInTheDocument();
    const box = screen.getByLabelText("Subject");
    fireEvent.change(box, { target: { value: "freeze:global" } });
    fireEvent.submit(box.closest("form") as HTMLFormElement);
    await waitFor(() => expect(window.location.pathname).toBe("/ui/governor/custody/freeze%3Aglobal"));

    fireEvent.change(box, { target: { value: "m".repeat(MAX_SUBJECT_BYTES + 1) } });
    expect(screen.getByText(`a subject is at most ${MAX_SUBJECT_BYTES} bytes`)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Trace" })).toBeDisabled();
  });
});
