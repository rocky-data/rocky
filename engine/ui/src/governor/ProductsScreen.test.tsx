import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ProductJournalOutput } from "@rocky-types/product_journal";
import type { ProductListOutput } from "@rocky-types/product_list";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { ApiError } from "../api";
import { ProductsScreen, type ProductLoaders, productPath } from "./ProductsScreen";

const PLAN = "d".repeat(64);

const LIST: ProductListOutput = {
  version: "1.74.0",
  command: "product",
  count: 2,
  products: [
    {
      name: "revenue_daily",
      spec_present: true,
      spec_digest: "sha256:1111",
      fulfill_state: "observed_passing",
      committed_phase: "b",
      journal_rows: 4,
      artifact_problems: 0,
      staging_journal_present: false,
      approval: null,
    },
    {
      name: "deleted_product",
      spec_present: false,
      spec_error: "spec-file-missing",
      fulfill_state: null,
      journal_rows: 2,
      artifact_problems: 1,
      staging_journal_present: false,
      approval: null,
    },
  ],
};

const STATUS: ProductStatusOutput = {
  version: "1.74.0",
  command: "product",
  product: "revenue_daily",
  spec_present: true,
  spec_digest: "sha256:1111",
  fulfill_state: "observed_passing",
  journal_rows: 4,
  staging_journal_present: false,
} as unknown as ProductStatusOutput;

const JOURNAL: ProductJournalOutput = {
  version: "1.74.0",
  command: "product",
  product: "revenue_daily",
  product_id: "product:revenue_daily",
  count: 3,
  rows: [
    {
      seq: 1,
      at: "2026-09-05T08:00:00Z",
      event: "spec approved",
      to_state: "spec_approved",
      spec_digest: "sha256:1111",
    },
    {
      seq: 2,
      at: "2026-09-05T09:00:00Z",
      event: "change proposed",
      from_state: "spec_approved",
      to_state: "proposed",
      plan_id: PLAN,
    },
    {
      seq: 3,
      at: "2026-09-05T10:00:00Z",
      // An event tag this build has never seen. It must render, not vanish.
      event: "quarantine lifted by the observer",
      from_state: "proposed",
      to_state: "observed_passing",
      idempotency_key: "key-abc",
    },
  ],
};

function loaders(overrides: Partial<ProductLoaders> = {}): ProductLoaders {
  return {
    list: vi.fn(async () => LIST),
    status: vi.fn(async () => STATUS),
    journal: vi.fn(async () => JOURNAL),
    ...overrides,
  };
}

describe("ProductsScreen", () => {
  it("lists every product, including one whose spec file is gone", async () => {
    render(<ProductsScreen name={null} loaders={loaders()} />);

    await screen.findByText("2 products");
    expect(screen.getByText("revenue_daily")).toBeTruthy();
    // A deleted spec still lists, with the loader's own reason — hiding it
    // would make a removed product look like one that never existed.
    expect(screen.getByText("deleted_product")).toBeTruthy();
    expect(screen.getByText(/no spec file: spec-file-missing/)).toBeTruthy();
    expect(screen.getByText("the loop has not run")).toBeTruthy();
  });

  it("renders every journal row in order, including an event it has never seen", async () => {
    render(<ProductsScreen name="revenue_daily" loaders={loaders()} />);

    await screen.findByText("spec approved");
    const rows = screen.getAllByRole("listitem").map((li) => li.textContent ?? "");
    expect(rows).toHaveLength(3);
    expect(rows[0]).toContain("spec approved");
    expect(rows[1]).toContain("change proposed");
    // The engine calls `event` a label to render, not an enum to switch on.
    expect(rows[2]).toContain("quarantine lifted by the observer");
    expect(rows[1]).toContain("spec_approved → proposed");
  });

  it("links a row that names a plan to that plan's review page", async () => {
    render(<ProductsScreen name="revenue_daily" loaders={loaders()} />);
    const link = await screen.findByRole("link", { name: /^plan / });
    expect(link.getAttribute("href")).toBe(`/ui/review/${PLAN}`);
  });

  it("says the loop has not run when the journal is empty", async () => {
    render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({ journal: vi.fn(async () => ({ ...JOURNAL, count: 0, rows: [] })) })}
      />,
    );
    await screen.findByText(/The fulfillment loop has not run/);
    expect(screen.queryByText("refused (404)")).toBeNull();
  });

  it("says a broken approval snapshot in those words", async () => {
    const broken = render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, snapshot_intact: false })),
        })}
      />,
    );
    await screen.findByText("broken");
    expect(screen.getByText(/no longer digest to the recorded value/)).toBeTruthy();
    broken.unmount();

    render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({ status: vi.fn(async () => ({ ...STATUS, snapshot_intact: true })) })}
      />,
    );
    await screen.findByText("observed_passing");
    expect(screen.queryByText("broken")).toBeNull();
  });

  it("renders a refusal as the engine's own code", async () => {
    render(
      <ProductsScreen
        name="nope"
        loaders={loaders({
          status: vi.fn(async () => {
            throw new ApiError(404, {
              code: "product_not_found",
              message: "no spec and no record",
              remediation_hint: "check the name",
            });
          }),
          journal: vi.fn(async () => {
            throw new ApiError(503, {
              code: "engine_not_ready",
              message: "no bound config",
              remediation_hint: "start the server in a project",
            });
          }),
        })}
      />,
    );
    await waitFor(() => expect(screen.getByText("product_not_found")).toBeTruthy());
    expect(screen.getByText("engine_not_ready")).toBeTruthy();
  });

  it("builds a product path that survives an awkward name", () => {
    expect(productPath("revenue daily")).toBe("/ui/governor/products/revenue%20daily");
  });
});
