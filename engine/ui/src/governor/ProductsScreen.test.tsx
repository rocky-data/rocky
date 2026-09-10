import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ProductJournalOutput } from "@rocky-types/product_journal";
import type { ProductListOutput } from "@rocky-types/product_list";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { ApiError } from "../api";
import {
  ProductsScreen,
  type ProductLoaders,
  describeSpecTrouble,
  productPath,
} from "./ProductsScreen";

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
      // `SpecRejected`'s Display is `[<code>] <message>`; the code is what
      // the screen words its label from, so the fixture must carry it.
      spec_error: "[spec-file-missing] spec file not found: products/gone.toml",
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

/**
 * Two journal rows whose idempotency keys differ ONLY in the trailing
 * sequence number — the shape the engine really writes. The fixture above
 * uses `key-abc`, which is under the truncation limit, so it never exercised
 * the rendering at all (#1756).
 */
const REAL_KEY_JOURNAL: ProductJournalOutput = {
  ...JOURNAL,
  count: 2,
  rows: [
    {
      seq: 1,
      at: "2026-09-05T08:00:00Z",
      event: "change proposed",
      to_state: "proposed",
      idempotency_key: "product:revenue_daily@sha256:5b1bf5c@21",
    },
    {
      seq: 2,
      at: "2026-09-05T09:00:00Z",
      event: "change proposed",
      to_state: "proposed",
      idempotency_key: "product:revenue_daily@sha256:5b1bf5c@22",
    },
  ],
};

describe("ProductsScreen", () => {
  it("renders two idempotency keys distinguishably (#1756)", async () => {
    render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({ journal: vi.fn(async () => REAL_KEY_JOURNAL) })}
      />,
    );

    await screen.findAllByText("change proposed");

    const full = [
      "product:revenue_daily@sha256:5b1bf5c@21",
      "product:revenue_daily@sha256:5b1bf5c@22",
    ];

    // The full key is always recoverable, which is what `shortId`'s own doc
    // comment promised and no call site did.
    for (const key of full) {
      expect(document.querySelector(`[title="${key}"]`)).toBeTruthy();
    }

    // And the two rows do not read identically. Before the fix both rendered
    // as `key product:reve` — cut mid-word, no ellipsis, and indistinguishable.
    const rendered = Array.from(document.querySelectorAll("[title]"))
      .filter((el) => full.includes(el.getAttribute("title") ?? ""))
      .map((el) => el.textContent ?? "");
    expect(rendered).toHaveLength(2);
    expect(rendered[0]).not.toBe(rendered[1]);
    expect(rendered[0]).toContain("@21");
    expect(rendered[1]).toContain("@22");
  });

  it("names both routes to a spec file, and no command that does not exist", async () => {
    const { container } = render(
      <ProductsScreen
        name={null}
        loaders={loaders({ list: vi.fn(async () => ({ ...LIST, products: [], count: 0 })) })}
      />,
    );
    await screen.findByText("This project declares no products");
    const copy = container.textContent ?? "";

    // Both routes. Verified: `rocky product --help` lists verify, compile,
    // status, list, journal and approve — none of which creates a spec. The
    // second route is real: rocky-fulfill writes "candidate spec
    // products/<name>.toml written (<digest>); awaiting approval".
    expect(copy).toContain("products/<name>.toml");
    expect(copy).toMatch(/you write it/);
    expect(copy).toContain("rocky fulfill");
    expect(copy).toMatch(/No rocky product subcommand creates one/);

    // And it must not invent a scaffold verb.
    expect(copy).not.toMatch(/rocky product (init|new|create|scaffold|add)/);
  });

  it("lists every product, including one whose spec file is gone", async () => {
    render(<ProductsScreen name={null} loaders={loaders()} />);

    await screen.findByText("2 products");
    expect(screen.getByText("revenue_daily")).toBeTruthy();
    // A deleted spec still lists, with the loader's own reason — hiding it
    // would make a removed product look like one that never existed.
    expect(screen.getByText("deleted_product")).toBeTruthy();
    expect(screen.getByText(/no spec file/)).toBeTruthy();
    expect(screen.getByText("the loop has not run")).toBeTruthy();
  });

  it("renders every journal row in order, including an event it has never seen", async () => {
    render(<ProductsScreen name="revenue_daily" loaders={loaders()} />);

    await screen.findByText("spec approved");
    // One header row plus one per journal row — nothing is filtered out.
    const rows = screen.getAllByRole("row").map((tr) => tr.textContent ?? "");
    expect(rows).toHaveLength(4);
    expect(rows[1]).toContain("spec approved");
    expect(rows[2]).toContain("change proposed");
    // The engine calls `event` a label to render, not an enum to switch on.
    expect(rows[3]).toContain("quarantine lifted by the observer");
    expect(rows[2]).toContain("spec_approved → proposed");
  });

  /// A filter would have to decide which events matter, and deciding that
  /// means switching on `event`. The count says what the table holds, so a
  /// reader can tell "all of it" from "some of it" without counting rows.
  it("says how many rows the journal has, and shows that many", async () => {
    const many = Array.from({ length: 40 }, (_, i) => ({
      seq: i + 1,
      at: "2026-09-05T08:00:00Z",
      event: `ownership acquired ${i + 1}`,
      to_state: "observing",
    }));
    render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({
          journal: vi.fn(async () => ({ ...JOURNAL, count: many.length, rows: many })),
        })}
      />,
    );

    await screen.findByText("40 rows, oldest first, as the engine recorded them.");
    expect(screen.getAllByRole("row")).toHaveLength(many.length + 1);
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

  /// U2-P0's XSS row, the timeline half. A journal event is a string the
  /// engine composed from a compiler error, a check failure or an agent's
  /// output — none of it the operator's, and this screen renders it verbatim
  /// on purpose, so verbatim has to mean text.
  it("renders a hostile event and product name as text, never as markup", async () => {
    const hostile = '<img src=x onerror="alert(1)">';
    const { container } = render(
      // The name reaches the heading and the custody link. An earlier
      // version of this test passed a benign name and only put the hostile
      // value in the event, so the heading was never exercised (#1815).
      <ProductsScreen
        name={hostile}
        loaders={loaders({
          journal: vi.fn(async () => ({
            ...JOURNAL,
            count: 1,
            rows: [{ seq: 1, at: "2026-09-05T08:00:00Z", event: hostile, to_state: "observing" }],
          })),
        })}
      />,
    );

    expect(await screen.findByText(hostile)).toBeTruthy();
    expect(screen.getByRole("heading", { name: hostile })).toBeTruthy();
    expect(screen.getByRole("link", { name: `product:${hostile}` })).toBeTruthy();
    expect(container.querySelector("img")).toBeNull();
  });

  /// Product A's standing and journal must not sit under product B's heading
  /// while B's reads are still in flight (#1815).
  it("carries nothing of product A under product B's heading", async () => {
    let releaseB!: (value: ProductStatusOutput) => void;
    const statusB = new Promise<ProductStatusOutput>((resolve) => {
      releaseB = resolve;
    });
    const shared = loaders({
      status: vi.fn(async (name: string) => (name === "revenue_daily" ? STATUS : statusB)),
    });
    const view = render(<ProductsScreen name="revenue_daily" loaders={shared} />);
    await screen.findByText("observed_passing");
    await screen.findAllByText("change proposed");

    view.rerender(<ProductsScreen name="costs_daily" loaders={shared} />);

    expect(screen.getByRole("heading", { name: "costs_daily" })).toBeTruthy();
    expect(screen.queryByText("observed_passing")).toBeNull();
    expect(screen.getByText("reading the product…")).toBeTruthy();

    releaseB({ ...STATUS, fulfill_state: "proposed" } as ProductStatusOutput);
    await screen.findByText("proposed");
    expect(screen.queryByText("observed_passing")).toBeNull();
  });

  it("builds a product path that survives an awkward name", () => {
    expect(productPath("revenue daily")).toBe("/ui/governor/products/revenue%20daily");
  });

  /// The engine reports a pending staging journal and deliberately does not
  /// resolve it. A screen that drops it makes an interrupted compile look
  /// like a finished one.
  it("shows an interrupted commit in the standing panel and in the list", async () => {
    render(
      <ProductsScreen
        name="revenue_daily"
        loaders={loaders({
          status: vi.fn(async () => ({ ...STATUS, staging_journal_present: true })),
        })}
      />,
    );
    await screen.findByText("a commit was interrupted");
  });

  /// Only `spec-file-missing` means the file is gone. A spec that exists and
  /// cannot be read, or one that parses badly, must not be reported as a
  /// deletion — the reader would go looking for a removal that never happened.
  describe("describeSpecTrouble", () => {
    it("says the file is gone only when the loader said it was missing", () => {
      expect(describeSpecTrouble("[spec-file-missing] spec file not found: a.toml")).toBe(
        "no spec file",
      );
      expect(describeSpecTrouble(null)).toBe("no spec file");
    });

    it("does not call an unreadable spec a missing one", () => {
      expect(
        describeSpecTrouble("[spec-file-unreadable] spec file exists but could not be read: a.toml"),
      ).toBe("spec file unreadable");
    });

    it("does not call a rejected spec a missing one", () => {
      expect(describeSpecTrouble("[not-toml] products/a.toml is not valid TOML")).toBe(
        "spec file unusable",
      );
      expect(describeSpecTrouble("[product-name-mismatch] declares product.name = 'x'")).toBe(
        "spec file unusable",
      );
    });
  });
});
