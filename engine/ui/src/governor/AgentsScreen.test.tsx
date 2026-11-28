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
    entry({ plan_id: "freeze:global", kind: "freeze", principal_id: "claude-dev" }),
    entry({ plan_id: "plan-v1", kind: "verify_after_custody", principal_id: "claude-dev" }),
    entry({ plan_id: "plan-m1", model: "orders", effect: "deny", timestamp: "2026-09-05T06:00:00Z" }),
    entry({ plan_id: "plan-b2", principal_id: "ci-bot", principal_id_source: "flag", timestamp: "2026-09-04T02:00:00Z" }),
    entry({ plan_id: "plan-m1", model: "customers", timestamp: "2026-09-05T06:00:01Z" }),
    entry({ plan_id: "autoapply:run-1", capability: "schema_change.additive", principal_id: "ci-bot", principal_id_source: "env" }),
  ],
};

describe("AgentsScreen", () => {
  it("counts only agent evaluation rows in the stat cards", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    await screen.findByRole("table", { name: "Agents" });
    const card = (label: string) => screen.getByText(label, { selector: "div" }).parentElement as HTMLElement;
    expect(within(card("Distinct names")).getByText("2")).toBeInTheDocument();
    // 7 single-model requests plus plan-m1; autoapply, freeze and verify rows are out.
    const requests = card("Agent requests");
    expect(within(requests).getByText("9")).toBeInTheDocument();
    expect(within(requests).getByText("10 gate decisions, one per model")).toBeInTheDocument();
    expect(within(card("Needed approval")).getByText("2")).toBeInTheDocument();
    expect(within(card("Rows with no name")).getByText("3")).toBeInTheDocument();
    expect(screen.getByText(/leaves 1 such decision out/)).toBeInTheDocument();
  });

  it("lists one row per group, most requests first, with an unknown channel", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Agents" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows.map((r) => within(r).getAllByRole("cell")[0].textContent)).toEqual([
      "claude-dev",
      "ci-bot",
      "unnamed",
      "unrecorded",
    ]);
    const first = within(rows[0]).getAllByRole("cell");
    expect(first[1]).toHaveTextContent("MCP profile");
    // plan-m1 has two models: 4 requests, 5 decisions.
    expect(first[3]).toHaveTextContent("4(5 decisions)");
    expect(first[4]).toHaveTextContent("2 allowed · 1 needed approval · 2 blocked decisions");
    for (const row of rows) {
      const channel = within(row).getAllByRole("cell")[2].firstElementChild as HTMLElement;
      expect(channel).toHaveTextContent("-");
      expect(channel).toHaveAttribute("title", "The audit log does not record an agent's channel yet");
    }
    expect(within(rows[2]).getAllByRole("cell")[1]).toHaveTextContent("nobody named it");
    expect(within(rows[1]).getAllByRole("cell")[1]).toHaveTextContent(
      "--principal-id flag, environment variable",
    );
    const unrecorded = within(rows[3]).getAllByRole("cell")[1].firstElementChild as HTMLElement;
    expect(unrecorded).toHaveAttribute("title", "No source recorded");
  });

  it("selects a row into the detail panel", async () => {
    render(<AgentsScreen load={async () => AUDIT} now={NOW} />);
    const first = await screen.findByRole("region", { name: "Agent claude-dev" });
    expect(within(first).getByText(/named by MCP profile · not verified/)).toBeInTheDocument();
    expect(within(first).getByText("rocky audit --actor claude-dev --since 7d")).toBeInTheDocument();
    expect(within(first).getByRole("link", { name: "Audit log" })).toBeInTheDocument();
    const merged = within(within(first).getByRole("list", { name: "Recent requests" })).getAllByRole("listitem");
    expect(merged).toHaveLength(4);
    expect(within(merged[0]).getByText("customers, orders")).toBeInTheDocument();
    // plan-m1: one deny row and one allow row show the worst outcome.
    expect(within(merged[0]).getByText("Blocked")).toBeInTheDocument();
    const recent = within(first).getByRole("list", { name: "Recent requests" });
    expect(within(recent).getByText("Needed approval")).toBeInTheDocument();
    for (const pill of within(recent).getAllByText("Blocked")) {
      expect(pill.className).toContain("bg-block-soft");
      expect(pill.className).toContain("text-block-ink");
      expect(pill.className).not.toMatch(/fail|red/);
    }
    expect(screen.getByText(/A later approval is not shown here/)).toBeInTheDocument();
    expect(within(recent).getAllByText("Allowed").length).toBeGreaterThan(0);
    expect(within(recent).getAllByText("Blocked")).toHaveLength(2);
    expect(within(recent).getByRole("link", { name: "plan-a2" })).toHaveAttribute(
      "href",
      "/ui/governor/custody/plan-a2",
    );
    expect(within(first).getByText("Rules match agents as a class, not by name.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "unrecorded" }));
    const other = screen.getByRole("region", { name: "Agent unrecorded" });
    expect(within(other).getByText("rocky audit --actor unrecorded --since 7d")).toBeInTheDocument();
    expect(within(other).getByText("written before names were recorded · not verified")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "unnamed" }));
    expect(screen.getByText("nobody named it · not verified")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "unnamed" })).toHaveAttribute("aria-current", "true");
    expect(screen.queryByRole("region", { name: "Agent claude-dev" })).toBeNull();
  });

  it("shows the first five requests and the rest on demand", async () => {
    const many = Array.from({ length: 7 }, (_, i) =>
      entry({ plan_id: `plan-z${i}`, timestamp: `2026-09-05T0${i}:00:00Z` }),
    );
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: many })} now={NOW} />);
    const list = await screen.findByRole("list", { name: "Recent requests" });
    expect(within(list).getAllByRole("listitem")).toHaveLength(5);
    fireEvent.click(screen.getByRole("button", { name: "Show all 7" }));
    expect(within(list).getAllByRole("listitem")).toHaveLength(7);
    expect(within(list).getByRole("link", { name: "plan-z0" })).toBeInTheDocument();
  });

  it("counts each draft row as its own request, with its own outcome", async () => {
    const drafts = [
      entry({ plan_id: "draft:orders", effect: "deny", timestamp: "2026-09-05T01:00:00Z" }),
      entry({ plan_id: "draft:orders", effect: "allow", timestamp: "2026-09-05T02:00:00Z" }),
    ];
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: drafts })} now={NOW} />);
    const region = await screen.findByRole("region", { name: "Agent claude-dev" });
    const items = within(within(region).getByRole("list", { name: "Recent requests" })).getAllByRole("listitem");
    expect(items).toHaveLength(2);
    expect(within(items[0]).getByText("Allowed")).toBeInTheDocument();
    expect(within(items[1]).getByText("Blocked")).toBeInTheDocument();
    const card = screen.getByText("Agent requests").parentElement as HTMLElement;
    expect(within(card).getByText("2")).toBeInTheDocument();
  });

  it("says not verified when any row in the group is unverified", async () => {
    const rows = [
      entry({ plan_id: "plan-v1", principal_id_verified: true }),
      entry({ plan_id: "plan-v2", principal_id_verified: false }),
    ];
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: rows })} now={NOW} />);
    const region = await screen.findByRole("region", { name: "Agent claude-dev" });
    expect(within(region).getByText(/· not verified/)).toBeInTheDocument();
  });

  it("counts one plan under each of two names", async () => {
    const rows = [
      entry({ plan_id: "plan-s", principal_id: "alpha" }),
      entry({ plan_id: "plan-s", principal_id: "beta", model: "customers" }),
    ];
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: rows })} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Agents" });
    const body = within(table).getAllByRole("row").slice(1);
    expect(body).toHaveLength(2);
    for (const row of body) expect(within(row).getAllByRole("cell")[3]).toHaveTextContent("1(1 decision)");
  });

  it("shows a request with a review row and a deny row as Blocked, not as needing approval", async () => {
    const rows = [
      entry({ plan_id: "plan-q", effect: "require_review", timestamp: "2026-09-05T01:00:00Z" }),
      entry({ plan_id: "plan-q", effect: "deny", model: "customers", timestamp: "2026-09-05T02:00:00Z" }),
    ];
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: rows })} now={NOW} />);
    const region = await screen.findByRole("region", { name: "Agent claude-dev" });
    const items = within(within(region).getByRole("list", { name: "Recent requests" })).getAllByRole("listitem");
    expect(items).toHaveLength(1);
    expect(within(items[0]).getByText("Blocked")).toBeInTheDocument();
    const card = screen.getByText("Needed approval", { selector: "div" }).parentElement as HTMLElement;
    expect(within(card).getByText("0")).toBeInTheDocument();
  });

  it("says what was left out when only drift auto-apply rows exist", async () => {
    const only = [
      entry({ plan_id: "autoapply:r1" }),
      entry({ plan_id: "autoapply:r1", model: "customers" }),
    ];
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: only })} now={NOW} />);
    expect(await screen.findByText("No agent requests in the last 7 days")).toBeInTheDocument();
    expect(screen.getByText("2 decisions from rocky run's own drift fixes are left out.")).toBeInTheDocument();
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
    render(<AgentsScreen load={async () => ({ ...AUDIT, decisions: [AUDIT.decisions.find((d) => d.principal === "human")!] })} now={NOW} />);
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
