import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AuditDecisionEntry, AuditOutput } from "@rocky-types/audit";
import { ApiError } from "../api";
import { AgentsScreen } from "./AgentsScreen";

const NOW = Date.parse("2026-09-05T08:00:00Z");

function entry(over: Partial<AuditDecisionEntry>): AuditDecisionEntry {
  return {
    capability: "apply",
    effect: "allow",
    kind: "evaluation",
    model: "orders",
    plan_id: "plan-x",
    principal: "agent",
    principal_id: "claude-dev",
    principal_id_source: "mcp_profile",
    principal_id_verified: false,
    reason: "r",
    rule_id: null,
    timestamp: "2026-09-05T07:00:00Z",
    ...over,
  };
}

const AUDIT: AuditOutput = {
  version: "1.0.0",
  command: "audit",
  decisions: [
    entry({ plan_id: "plan-a1", timestamp: "2026-09-05T01:00:00Z" }),
    entry({ plan_id: "plan-a2", effect: "require_review", capability: "propose", model: "customers", timestamp: "2026-09-05T02:00:00Z" }),
    entry({ plan_id: "plan-a3", effect: "deny", timestamp: "2026-09-05T03:00:00Z" }),
    entry({ plan_id: "plan-b1", principal_id: "ci-bot", principal_id_source: "env", timestamp: "2026-09-04T01:00:00Z" }),
    entry({ plan_id: "plan-u1", principal_id: "unnamed", principal_id_source: "default", effect: "require_review", timestamp: "2026-09-05T04:00:00Z" }),
    entry({ plan_id: "plan-u2", principal_id: "unnamed", principal_id_source: "default", timestamp: "2026-09-05T05:00:00Z" }),
    entry({ plan_id: "plan-n1", principal_id: null, principal_id_source: null, timestamp: "2026-09-03T01:00:00Z" }),
    entry({ plan_id: "plan-h1", principal: "human", principal_id: "hugo", principal_id_source: "flag" }),
    entry({ plan_id: "plan-ap", kind: "approval", principal_id: "claude-dev" }),
  ],
};

describe("AgentsScreen", () => {
  it("counts only agent evaluation rows in the stat cards", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    await screen.findByRole("table", { name: "Agents" });
    const card = (label: string) => screen.getByText(label).parentElement as HTMLElement;
    expect(within(card("Agents seen")).getByText("2")).toBeInTheDocument();
    expect(within(card("Agent requests")).getByText("7")).toBeInTheDocument();
    expect(within(card("Needed approval")).getByText("2")).toBeInTheDocument();
    expect(within(card("Rows with no name")).getByText("3")).toBeInTheDocument();
  });

  it("lists one row per group, most requests first, with an unknown channel", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Agents" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows.map((r) => within(r).getAllByRole("cell")[0].textContent)).toEqual([
      "claude-dev",
      "unnamed",
      "ci-bot",
      "unrecorded",
    ]);
    const first = within(rows[0]).getAllByRole("cell");
    expect(first[1]).toHaveTextContent("MCP profile");
    expect(first[3]).toHaveTextContent("3");
    expect(first[4]).toHaveTextContent("1 allowed · 1 needed approval · 1 blocked");
    for (const row of rows) {
      const channel = within(row).getAllByRole("cell")[2].firstElementChild as HTMLElement;
      expect(channel).toHaveTextContent("-");
      expect(channel).toHaveAttribute("title", "The audit log does not record an agent's channel yet");
    }
    expect(within(rows[1]).getAllByRole("cell")[1]).toHaveTextContent("nobody named it");
    expect(within(rows[2]).getAllByRole("cell")[1]).toHaveTextContent("environment variable");
    const unrecorded = within(rows[3]).getAllByRole("cell")[1].firstElementChild as HTMLElement;
    expect(unrecorded).toHaveAttribute("title", "No source recorded");
  });

  it("selects a row into the detail panel", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    const first = await screen.findByRole("region", { name: "Agent claude-dev" });
    expect(within(first).getByText(/named by MCP profile · not verified/)).toBeInTheDocument();
    const recent = within(first).getByRole("list", { name: "Recent requests" });
    expect(within(recent).getByText("Needs approval")).toBeInTheDocument();
    expect(within(recent).getByText("Allowed")).toBeInTheDocument();
    expect(within(recent).getByText("Blocked")).toBeInTheDocument();
    expect(within(recent).getByRole("link", { name: "plan-a2" })).toHaveAttribute(
      "href",
      "/ui/governor/custody/plan-a2",
    );
    expect(within(first).getByText("Rules match agents as a class, not by name.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "unrecorded" }));
    const other = screen.getByRole("region", { name: "Agent unrecorded" });
    expect(within(other).getByText("rocky audit --actor unrecorded")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Agent claude-dev" })).toBeNull();
  });

  it("re-reads with the chosen window", async () => {
    const load = vi.fn(async () => AUDIT);
    render(<AgentsScreen load={load} now={NOW} />);
    await screen.findByRole("table", { name: "Agents" });
    expect(load).toHaveBeenCalledWith(7);
    fireEvent.change(screen.getByLabelText("Window"), { target: { value: "30" } });
    await waitFor(() => expect(load).toHaveBeenCalledWith(30));
  });

  it("says so when no agent asked in the window", async () => {
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: [AUDIT.decisions[7]] })} now={NOW} />);
    expect(await screen.findByText("No agent requests in the last 7 days")).toBeInTheDocument();
  });

  it("shows a refused read as an envelope, never as zero", async () => {
    const refused = new ApiError(403, { code: "forbidden", message: "no", remediation_hint: "ask an admin" });
    render(
      <AgentsScreen
        load={async () => {
          throw refused;
        }}
        now={NOW}
      />,
    );
    expect(await screen.findByText("forbidden")).toBeInTheDocument();
    expect(screen.queryByText("Agent requests")).toBeNull();
  });
});
