import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { PreviewRowsOutput } from "@rocky-types/preview_rows";
import { ApiError, apiGet } from "../api";
import { SAMPLE_LIMIT, SamplePanel } from "./SamplePanel";

const SAMPLE: PreviewRowsOutput = {
  version: "1.74.0",
  command: "preview-rows",
  model: "orders",
  cte: null,
  columns: ["id", "email", "total"],
  rows: [
    [1, "9f2b…", 100],
    [2, null, 250],
  ],
  row_count: 2,
  limit_applied: SAMPLE_LIMIT,
  truncated: false,
  executed_sql: "SELECT id, sha256(CAST(email AS VARCHAR)) AS email, total FROM (…) LIMIT 20",
  adapter_kind: "duckdb",
  duration_ms: 12,
};

describe("SamplePanel", () => {
  it("asks for nothing until the viewer asks, then asks exactly once", async () => {
    const load = vi.fn(async () => SAMPLE);
    render(<SamplePanel model="orders" load={load} />);

    // The whole point: opening a plan must not spend warehouse money.
    expect(load).not.toHaveBeenCalled();
    expect(screen.getByText(/runs a query against the warehouse/)).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: `Show ${SAMPLE_LIMIT} rows` }));
    await waitFor(() => expect(load).toHaveBeenCalledTimes(1));
    expect(load).toHaveBeenCalledWith("orders");
  });

  it("renders the rows, a null as null, and the SQL that ran", async () => {
    const load = vi.fn(async () => SAMPLE);
    render(<SamplePanel model="orders" load={load} />);
    fireEvent.click(screen.getByRole("button", { name: `Show ${SAMPLE_LIMIT} rows` }));

    await screen.findByText("email");
    expect(screen.getByText("9f2b…")).toBeTruthy();
    // An empty cell is the word null, never a blank a reader would misread as
    // an empty string.
    expect(screen.getByText("null")).toBeTruthy();
    expect(screen.getByText(/2 rows/)).toBeTruthy();
    expect(screen.getByText(/pseudonymous, not anonymous/)).toBeTruthy();
    expect(screen.getByText(/SELECT id, sha256/)).toBeTruthy();
  });

  it("renders each refusal as itself, so a reviewer can tell them apart", async () => {
    const refusals = [
      {
        status: 403,
        code: "warehouse_gated",
        hint: "set the `X-Rocky-Allow-Warehouse: true` header",
      },
      {
        status: 422,
        code: "masking_unsupported_by_adapter",
        hint: "this adapter cannot express the column's mask strategy",
      },
      { status: 504, code: "sample_timeout", hint: "narrow the model or lower `limit`" },
      { status: 503, code: "engine_busy", hint: "one sample runs at a time" },
    ];

    for (const refusal of refusals) {
      const load = vi.fn(async () => {
        throw new ApiError(refusal.status, {
          code: refusal.code,
          message: "refused",
          remediation_hint: refusal.hint,
        });
      });
      const view = render(<SamplePanel model="orders" load={load} />);
      fireEvent.click(screen.getByRole("button", { name: `Show ${SAMPLE_LIMIT} rows` }));
      await waitFor(() => expect(screen.getByText(refusal.code)).toBeTruthy());
      expect(screen.getByText(`refused (${refusal.status})`)).toBeTruthy();
      expect(screen.getByText(refusal.hint)).toBeTruthy();
      view.unmount();
    }
  });

  it("sends the consent header through the panel's own loader, and only there", async () => {
    const fetchMock = vi.fn(
      async () => new Response(JSON.stringify(SAMPLE), { status: 200 }),
    ) as unknown as typeof fetch;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = fetchMock;
    try {
      // No `load` prop: this is the real default loader the screen uses.
      render(<SamplePanel model="orders" />);
      fireEvent.click(screen.getByRole("button", { name: `Show ${SAMPLE_LIMIT} rows` }));
      await waitFor(() =>
        expect((fetchMock as unknown as { mock: { calls: unknown[] } }).mock.calls.length).toBe(1),
      );
      const [url, init] = (fetchMock as unknown as { mock: { calls: [string, RequestInit][] } }).mock
        .calls[0];
      expect(url).toContain(`/api/v1/models/orders/rows?limit=${SAMPLE_LIMIT}`);
      expect((init.headers as Record<string, string>)["X-Rocky-Allow-Warehouse"]).toBe("true");
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("carries no consent on a read that is not a sample", async () => {
    const fetchMock = vi.fn(async () => new Response("{}", { status: 200 }));
    await apiGet("review/queue", {
      fetch: fetchMock as unknown as typeof fetch,
      storage: { getItem: () => "tok" },
    });
    const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    const headers = init.headers as Record<string, string>;
    expect(headers["X-Rocky-Allow-Warehouse"]).toBeUndefined();
    expect(headers.Authorization).toBe("Bearer tok");
  });
});
