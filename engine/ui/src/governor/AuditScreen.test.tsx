import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AuditOutput } from "@rocky-types/audit";
import type { ProductListOutput } from "@rocky-types/product_list";
import { ApiError } from "../api";
import { AuditScreen, type AuditLoaders } from "./AuditScreen";

const NOW = Date.parse("2026-09-05T08:00:00Z");
const PLAN = "c".repeat(64);

const LEDGER: AuditOutput = {
  version: "1.74.0",
  command: "audit",
  decisions: [
    {
      timestamp: "2026-09-04T09:00:00Z",
      plan_id: PLAN,
      principal: "agent",
      capability: "propose",
      model: "revenue_daily",
      effect: "allow",
      rule_id: 0,
      reason: "propose is allowed",
    },
    {
      timestamp: "2026-09-04T10:00:00Z",
      plan_id: "freeze:global",
      principal: "human",
      capability: "apply",
      model: "*",
      effect: "deny",
      rule_id: null,
      reason: "<i>frozen</i> by the governor",
    },
  ],
};

const PRODUCTS: ProductListOutput = {
  version: "1.74.0",
  command: "product_list",
  count: 1,
  products: [
    {
      name: "revenue_daily",
      spec_present: true,
      artifact_problems: 0,
      staging_journal_present: false,
      journal_rows: 1,
    },
  ],
};

function loaders(overrides: Partial<AuditLoaders> = {}): AuditLoaders {
  return {
    ledger: vi.fn(async (product: string | null) =>
      product
        ? {
            ...LEDGER,
            product: { name: product, output_model: "revenue_daily" },
            decisions: LEDGER.decisions.filter((d) => d.model === "revenue_daily"),
          }
        : LEDGER,
    ),
    products: async () => PRODUCTS,
    ...overrides,
  };
}

describe("AuditScreen", () => {
  it("renders the whole ledger oldest first with plan links, and a hostile reason as text", async () => {
    const { container } = render(<AuditScreen loaders={loaders()} now={NOW} />);
    const table = await screen.findByRole("table", { name: "Policy decisions" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByRole("link", { name: PLAN })).toBeInTheDocument();
    expect(within(rows[1]).getByRole("link", { name: "freeze:global" })).toBeInTheDocument();
    expect(screen.getByText("<i>frozen</i> by the governor")).toBeInTheDocument();
    expect(container.querySelector("i")).toBeNull();
    expect(screen.getByText("2 decision(s), oldest first, the whole ledger")).toBeInTheDocument();
  });

  it("scopes the ledger through the product selector and shows the output model", async () => {
    const l = loaders();
    render(<AuditScreen loaders={l} now={NOW} />);
    await screen.findByRole("table", { name: "Policy decisions" });
    await waitFor(() => expect(screen.getByRole("option", { name: "revenue_daily" })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText("Product"), { target: { value: "revenue_daily" } });
    await waitFor(() => expect(l.ledger).toHaveBeenCalledWith("revenue_daily"));
    expect(await screen.findByText(/Scoped to/)).toBeInTheDocument();
    expect(screen.getByText("GET /api/v1/audit?product=revenue_daily")).toBeInTheDocument();
    const table = screen.getByRole("table", { name: "Policy decisions" });
    expect(within(table).getAllByRole("row").slice(1)).toHaveLength(1);
  });

  it("shows the route's 404 and 409 envelopes in the panel", async () => {
    for (const [status, code] of [
      [404, "product_not_found"],
      [409, "product_spec_invalid"],
    ] as const) {
      const refused = new ApiError(status, { code, message: "refused", remediation_hint: "fix it" });
      const { unmount } = render(
        <AuditScreen
          loaders={loaders({
            ledger: async () => {
              throw refused;
            },
          })}
          now={NOW}
        />,
      );
      expect(await screen.findByText(code)).toBeInTheDocument();
      unmount();
    }
  });

  it("explains an empty ledger, scoped or not", async () => {
    render(
      <AuditScreen
        loaders={loaders({
          ledger: async () => ({
            ...LEDGER,
            decisions: [],
            product: { name: "revenue_daily", output_model: "revenue_daily" },
          }),
        })}
        now={NOW}
      />,
    );
    expect(await screen.findByText("No decision recorded")).toBeInTheDocument();
    expect(screen.getByText(/No governed mutation touched revenue_daily/)).toBeInTheDocument();
  });
});
