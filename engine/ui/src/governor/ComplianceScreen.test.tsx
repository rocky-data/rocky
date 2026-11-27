import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ComplianceOutput } from "@rocky-types/compliance";
import { ApiError } from "../api";
import { ComplianceScreen } from "./ComplianceScreen";

const OUTPUT: ComplianceOutput = {
  command: "compliance",
  version: "1.0.0",
  summary: { total_classified: 7, total_exceptions: 1, total_masked: 4 },
  exceptions: [
    {
      model: "orders",
      column: "shipping_address",
      env: "dev",
      reason: "no masking strategy resolves for classification tag 'pii'",
    },
  ],
  per_column: [
    {
      model: "customers",
      column: "email",
      classification: "pii",
      envs: [
        { env: "prod", enforced: true, masking_strategy: "hash" },
        { env: "dev", enforced: true, masking_strategy: "none" },
      ],
    },
    {
      model: "customers",
      column: "phone",
      classification: "pii",
      envs: [
        { env: "prod", enforced: true, masking_strategy: "redact" },
        { env: "dev", enforced: true, masking_strategy: "redact" },
      ],
    },
    {
      model: "orders",
      column: "shipping_address",
      classification: "pii",
      envs: [
        { env: "prod", enforced: true, masking_strategy: "partial" },
        { env: "dev", enforced: false, masking_strategy: "unresolved" },
      ],
    },
    {
      model: "payments",
      column: "notes",
      classification: "",
      envs: [
        { env: "prod", enforced: false, masking_strategy: "unresolved" },
        { env: "dev", enforced: false, masking_strategy: "unresolved" },
      ],
    },
  ],
};

const CLEAN: ComplianceOutput = {
  ...OUTPUT,
  exceptions: [],
  per_column: OUTPUT.per_column.slice(1, 2),
  summary: { total_classified: 2, total_exceptions: 0, total_masked: 2 },
};

function card(label: string): HTMLElement {
  return screen.getByText(label, { selector: "div" }).parentElement as HTMLElement;
}

describe("ComplianceScreen", () => {
  it("maps the output's fields into cards and the exceptions table", async () => {
    render(<ComplianceScreen load={async () => OUTPUT} />);
    const exceptions = await screen.findByRole("table", { name: "Exceptions" });
    const cells = within(within(exceptions).getAllByRole("row")[1]).getAllByRole("cell");
    expect(cells.map((c) => c.textContent)).toEqual([
      "orders",
      "shipping_address",
      "dev",
      "no masking strategy resolves for classification tag 'pii'",
    ]);
    expect(within(card("Classified columns")).getByText("4")).toBeInTheDocument();
    expect(within(card("Classified columns")).getByText("in 3 models")).toBeInTheDocument();
    // Only customers.phone is masked in every env.
    expect(within(card("Masked everywhere")).getByText("1")).toBeInTheDocument();
    expect(within(card("Exceptions")).getByText("1")).toBeInTheDocument();
    // customers.email dev (none) and payments.notes prod + dev (allow-listed).
    expect(within(card("Unmasked on purpose")).getByText("3")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("1 classified column has no masking strategy");
  });

  it("lists the envs the output names, and narrows to one without reading again", async () => {
    const load = vi.fn(async () => OUTPUT);
    render(<ComplianceScreen load={load} />);
    await screen.findByRole("table", { name: "Classified columns" });
    const picker = screen.getByLabelText("Environment");
    expect(within(picker).getAllByRole("option").map((o) => o.textContent)).toEqual([
      "All environments",
      "prod",
      "dev",
    ]);
    fireEvent.change(picker, { target: { value: "prod" } });
    expect(screen.queryByRole("table", { name: "Exceptions" })).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("Every classified column is masked, or left unmasked on purpose, in environment prod.");
    expect(within(card("Exceptions")).getByText("0")).toBeInTheDocument();
    fireEvent.change(picker, { target: { value: "dev" } });
    expect(screen.getByRole("table", { name: "Exceptions" })).toBeInTheDocument();
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("drops the other environments' chips and pairs when one environment is picked", async () => {
    render(<ComplianceScreen load={async () => OUTPUT} />);
    await screen.findByRole("table", { name: "Classified columns" });
    const email = () => screen.getByRole("list", { name: "Masking of customers.email" });
    expect(within(email()).getAllByRole("listitem")).toHaveLength(2);
    fireEvent.change(screen.getByLabelText("Environment"), { target: { value: "prod" } });
    expect(within(email()).getAllByRole("listitem").map((li) => li.textContent)).toEqual([
      expect.stringMatching(/^prod · hash/),
    ]);
    expect(within(email()).queryByText(/dev · none/)).toBeNull();
    // Only the prod pair of payments.notes counts as unmasked on purpose.
    expect(within(card("Unmasked on purpose")).getByText("1")).toBeInTheDocument();
  });

  it("draws red, green and amber chips from semantic tokens", async () => {
    render(<ComplianceScreen load={async () => OUTPUT} />);
    await screen.findByRole("table", { name: "Classified columns" });
    const email = screen.getByRole("list", { name: "Masking of customers.email" });
    const prod = within(email).getByText(/prod · hash/);
    expect(prod).toHaveAttribute("data-state", "masked");
    expect(prod.className).toContain("bg-ok-soft");
    expect(prod.className).toContain("text-ok-ink");
    const none = within(email).getByText(/dev · none/);
    expect(none).toHaveAttribute("data-state", "on_purpose");
    expect(none.className).toContain("bg-warn-soft");
    expect(none.className).toContain("text-warn-ink");
    const address = screen.getByRole("list", { name: "Masking of orders.shipping_address" });
    const red = within(address).getByText(/dev · unresolved/);
    expect(red).toHaveAttribute("data-state", "exception");
    expect(red.className).toContain("bg-fail-soft");
    expect(red.className).toContain("text-fail-ink");
    // Unresolved, but not an exception: the tag is on the allow list.
    const notes = screen.getByRole("list", { name: "Masking of payments.notes" });
    expect(within(notes).getByText(/prod · unresolved/)).toHaveAttribute("data-state", "on_purpose");
    for (const node of [prod, none, red]) expect(node.className).not.toMatch(/zinc|red-|green-|amber-/);
  });

  it("pins the env match: an exception in dev does not redden the same column's none in prod", async () => {
    const out: ComplianceOutput = {
      ...OUTPUT,
      exceptions: [{ model: "m", column: "c", env: "dev", reason: "r" }],
      per_column: [
        {
          model: "m",
          column: "c",
          classification: "pii",
          envs: [
            { env: "prod", enforced: true, masking_strategy: "none" },
            { env: "dev", enforced: false, masking_strategy: "unresolved" },
          ],
        },
      ],
    };
    render(<ComplianceScreen load={async () => out} />);
    const list = await screen.findByRole("list", { name: "Masking of m.c" });
    expect(within(list).getByText(/prod · none/)).toHaveAttribute("data-state", "on_purpose");
    expect(within(list).getByText(/dev · unresolved/)).toHaveAttribute("data-state", "exception");
  });

  it("counts a column unmasked in two envs as one column in the banner, and says the card's unit", async () => {
    const out: ComplianceOutput = {
      ...OUTPUT,
      exceptions: [
        { model: "m", column: "c", env: "dev", reason: "r" },
        { model: "m", column: "c", env: "qa", reason: "r" },
      ],
      per_column: [
        {
          model: "m",
          column: "c",
          classification: "pii",
          envs: [
            { env: "dev", enforced: false, masking_strategy: "unresolved" },
            { env: "qa", enforced: false, masking_strategy: "unresolved" },
          ],
        },
      ],
    };
    render(<ComplianceScreen load={async () => out} />);
    const banner = await screen.findByRole("status");
    expect(banner).toHaveTextContent("1 classified column has no masking strategy in at least one environment");
    const exceptions = card("Exceptions");
    expect(within(exceptions).getByText("2")).toBeInTheDocument();
    expect(within(exceptions).getByText("column × environment pairs, in 1 column")).toBeInTheDocument();
  });

  it("shows a [mask.default] column once, with no duplicate keys", async () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const out: ComplianceOutput = {
        ...OUTPUT,
        exceptions: [],
        per_column: [
          {
            model: "m",
            column: "c",
            classification: "pii",
            envs: [
              { env: "default", enforced: true, masking_strategy: "hash" },
              { env: "default", enforced: false, masking_strategy: "unresolved" },
            ],
          },
        ],
      };
      render(<ComplianceScreen load={async () => out} />);
      const list = await screen.findByRole("list", { name: "Masking of m.c" });
      expect(within(list).getAllByRole("listitem")).toHaveLength(1);
      // The worse of the two rows is kept.
      expect(within(list).getByRole("listitem")).toHaveAttribute("data-state", "on_purpose");
      expect(within(screen.getByLabelText("Environment")).getAllByRole("option")).toHaveLength(2);
      expect(spy.mock.calls.flat().join(" ")).not.toMatch(/same key/);
    } finally {
      spy.mockRestore();
    }
  });

  it("turns the Exceptions card red above zero and green at zero", async () => {
    const { unmount } = render(<ComplianceScreen load={async () => OUTPUT} />);
    await screen.findByRole("table", { name: "Classified columns" });
    expect(card("Exceptions")).toHaveAttribute("data-tone", "fail");
    expect(card("Exceptions").className).toContain("border-fail");
    unmount();
    render(<ComplianceScreen load={async () => CLEAN} />);
    await screen.findByRole("table", { name: "Classified columns" });
    expect(card("Exceptions")).toHaveAttribute("data-tone", "ok");
    expect(card("Exceptions").className).toContain("border-ok");
    expect(screen.getByText("nothing to fix")).toBeInTheDocument();
    expect(screen.getByText("None.")).toBeInTheDocument();
  });

  it("shows a grey dash with a reason for a missing classification", async () => {
    render(<ComplianceScreen load={async () => OUTPUT} />);
    const table = await screen.findByRole("table", { name: "Classified columns" });
    const row = within(table)
      .getAllByRole("row")
      .find((r) => within(r).queryByText("notes"))!;
    const dash = within(row).getByRole("img");
    expect(dash).toHaveTextContent("-");
    expect(dash).toHaveAttribute("title", "no classification tag was recorded");
    expect(dash.className).toContain("text-muted");
  });

  it("offers the CI gate as copyable code", async () => {
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    try {
      render(<ComplianceScreen load={async () => OUTPUT} />);
      await screen.findByRole("table", { name: "Classified columns" });
      expect(screen.getByText("rocky compliance --fail-on exception").tagName).toBe("CODE");
      fireEvent.click(screen.getByRole("button", { name: "Copy: rocky compliance --fail-on exception" }));
      expect(writeText).toHaveBeenCalledWith("rocky compliance --fail-on exception");
    } finally {
      Reflect.deleteProperty(navigator, "clipboard");
    }
  });

  it("says so when nothing is classified", async () => {
    render(<ComplianceScreen load={async () => ({ ...OUTPUT, per_column: [], exceptions: [] })} />);
    expect(await screen.findByText("No classified columns")).toBeInTheDocument();
  });

  it("shows a refused read as an envelope, never as zero", async () => {
    const refused = new ApiError(500, { code: "config_invalid", message: "bad", remediation_hint: "fix rocky.toml" });
    render(
      <ComplianceScreen
        load={async () => {
          throw refused;
        }}
      />,
    );
    expect(await screen.findByText("config_invalid")).toBeInTheDocument();
    expect(screen.getByText("fix rocky.toml")).toBeInTheDocument();
    expect(screen.queryByText("Classified columns")).toBeNull();
    expect(screen.queryByText("Exceptions")).toBeNull();
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("shows an unreachable engine, never as zero", async () => {
    render(
      <ComplianceScreen
        load={async () => {
          throw new Error("network down");
        }}
      />,
    );
    expect(await screen.findByText("unreachable")).toBeInTheDocument();
    expect(screen.queryByText("Masked everywhere")).toBeNull();
  });

  it("reads once and does not poll", async () => {
    vi.useFakeTimers();
    try {
      const load = vi.fn(async () => OUTPUT);
      render(<ComplianceScreen load={load} />);
      await vi.advanceTimersByTimeAsync(120_000);
      expect(load).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
