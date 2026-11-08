import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ProductMonthCost, ProductMonthCostOutput } from "@rocky-types/cost_by_product";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import type { ScheduleStatusOutput } from "@rocky-types/schedule_status";
import type { Resource } from "../estate/useResource";
import {
  DeliveryCostPanel,
  basisState,
  chargedUnitState,
  formatUsd,
  moneyState,
  pipelinesFor,
} from "./DeliveryCostPanel";

function row(overrides: Partial<ProductMonthCost> = {}): ProductMonthCost {
  return {
    product: "revenue_daily",
    output_model: "revenue_daily",
    built: [{ unit: "compute_ms", amount: 400 }],
    kept: [{ unit: "compute_ms", amount: 400 }],
    owner_residual: [],
    allocated_upstream: [
      {
        table: "raw",
        unit: "compute_ms",
        basis: "bytes",
        amount: 1000,
        of_total: 1000,
        low_confidence: false,
        provisional: false,
      },
    ],
    shared_not_charged: [],
    charged: [{ unit: "compute_ms", amount: 1400 }],
    money: 1.4,
    money_status: "priced",
    money_parts: [{ figure: "compute", status: "priced", usd: 1.4 }],
    ...overrides,
  };
}

function report(product: ProductMonthCost, history_complete = true): ProductMonthCostOutput {
  return {
    version: "1.91.0",
    command: "cost_by_product",
    month: "2026-11",
    timezone: "UTC",
    month_start: "2026-11-01T00:00:00+00:00",
    month_end: "2026-12-01T00:00:00+00:00",
    as_of: "2026-11-08T12:00:00+00:00",
    history_complete,
    products: [product],
    unallocated: [],
    shadow_branch: [],
    measured_total: [],
    measured_total_money: null,
    measured_total_money_status: "no_price",
    measured_total_money_parts: [],
    unallocated_money: null,
    unallocated_money_status: "no_price",
    unallocated_money_parts: [],
    not_measured: { failed_executions: 0, storage_unknown_tables: 0, unattributed_read_tables: 0 },
  };
}

const STATUS = {
  version: "1.91.0",
  command: "product",
  product: "revenue_daily",
  spec_present: true,
  journal_rows: 0,
  staging_journal_present: false,
  output_model: "revenue_daily",
} as ProductStatusOutput;

const SCHEDULE: ScheduleStatusOutput = {
  now: "2026-11-08T12:00:00Z",
  timezone: "UTC",
  counts: { config_errors: 0, enabled: 1, in_flight: 0, overdue: 0, scheduled: 2, throttled: 0 },
  tick_lock: { state: "free" } as ScheduleStatusOutput["tick_lock"],
  pipelines: [
    {
      pipeline: "nightly",
      awaiting_first_anchor: false,
      catchup: "latest",
      consecutive_failures: 0,
      enabled: true,
      cron: "0 2 * * *",
      latest_start: {
        state: "known",
        at: "2026-11-09T05:00:00Z",
        cron_check: "on_time",
        cron_too_late: false,
        product: "revenue_daily",
      },
    },
    {
      pipeline: "other",
      awaiting_first_anchor: false,
      catchup: "latest",
      consecutive_failures: 0,
      enabled: true,
    },
  ],
};

function ready<T>(value: T): Resource<T> {
  return { kind: "ready", value };
}

function renderPanel(
  cost: ProductMonthCostOutput,
  status: ProductStatusOutput = STATUS,
  schedule: Resource<ScheduleStatusOutput> = ready(SCHEDULE),
) {
  return render(
    <DeliveryCostPanel name="revenue_daily" status={status} month={ready(cost)} schedule={schedule} />,
  );
}

function chargedCard(): HTMLElement {
  return screen.getByText("charged, in USD").closest("[data-tone]") as HTMLElement;
}

describe("the five states of a cost figure", () => {
  it("measured: a priced figure split by bytes shows its USD", () => {
    renderPanel(report(row()));
    const card = chargedCard();
    expect(within(card).getByText("$1.40")).toBeInTheDocument();
    expect(card.querySelector("[data-figure-state]")?.getAttribute("data-figure-state")).toBe(
      "measured",
    );
  });

  it("estimated: a share split by estimated bytes makes the total an estimate", () => {
    const product = row({
      allocated_upstream: [{ ...row().allocated_upstream[0], basis: "estimated_bytes" }],
    });
    expect(moneyState(product)).toBe("estimated");
    renderPanel(report(product));
    expect(chargedCard().querySelector("[data-figure-state]")?.getAttribute("data-figure-state")).toBe(
      "estimated",
    );
    const upstream = screen.getByRole("table", { name: "What this product pays for tables it reads" });
    expect(within(upstream).getByText("estimated")).toBeInTheDocument();
  });

  it("rows only: a share split by rows scanned says so, and so does the total", () => {
    const product = row({
      allocated_upstream: [{ ...row().allocated_upstream[0], basis: "rows_scanned_unweighted" }],
    });
    expect(basisState("rows_scanned_unweighted")).toBe("rows_only");
    expect(moneyState(product)).toBe("rows_only");
    expect(chargedUnitState(product, "compute_ms")).toBe("rows_only");
    renderPanel(report(product));
    const upstream = screen.getByRole("table", { name: "What this product pays for tables it reads" });
    expect(within(upstream).getByText("rows only")).toBeInTheDocument();
    // The charged model time includes the rows-only share, so it is not "measured".
    const charged = screen
      .getAllByText("charged")
      .map((label) => label.closest("[data-tone]"))
      .find((card) => card !== null) as HTMLElement;
    expect(charged.querySelector("[data-figure-state]")?.getAttribute("data-figure-state")).toBe(
      "rows_only",
    );
  });

  it("not measured: no money figure and never $0", () => {
    renderPanel(report(row({ money: null, money_status: "not_measured", money_parts: [] })));
    const card = chargedCard();
    expect(within(card).getByText(/Rocky does not measure it/)).toBeInTheDocument();
    expect(card.textContent).not.toMatch(/\$0/);
    expect(card.querySelector("[data-figure-state]")?.getAttribute("data-figure-state")).toBe(
      "not_measured",
    );
  });

  it("no price: the amounts show, the money does not, and never $0", () => {
    renderPanel(
      report(
        row({
          money: null,
          money_status: "no_price",
          money_parts: [{ figure: "compute", status: "no_price", usd: null }],
        }),
      ),
    );
    const card = chargedCard();
    expect(within(card).getByText(/\[cost\] sets no price/)).toBeInTheDocument();
    expect(card.textContent).not.toMatch(/\$0/);
    expect(card.querySelector("[data-figure-state]")?.getAttribute("data-figure-state")).toBe(
      "no_price",
    );
    // The physical amount is still there: model time is measured.
    expect(screen.getAllByText(/1\.4s model time/).length).toBeGreaterThan(0);
    const parts = screen.getByRole("table", { name: "Money by figure" });
    expect(within(parts).getByText("none")).toBeInTheDocument();
  });

  it("a partial month says so and shows no total", () => {
    renderPanel(report(row({ money: null, money_status: "partial", money_parts: [] }), false));
    expect(screen.getByText("run history does not cover the whole month")).toBeInTheDocument();
    const card = chargedCard();
    expect(within(card).getByText("no total: the month is partial")).toBeInTheDocument();
    expect(card.textContent).not.toMatch(/\$/);
    // Every physical amount in the cards is marked partial too, never
    // "measured". The split-by chip in the upstream table still names how a
    // share was split, which stays true; its amount column says partial.
    const cards = Array.from(document.querySelectorAll("[data-tone]"));
    for (const one of cards) {
      expect(one.querySelector('[data-figure-state="measured"]')).toBeNull();
    }
    expect(
      document.querySelectorAll('[data-figure-state="partial"]').length,
    ).toBeGreaterThanOrEqual(4);
    expect(screen.getByText("charged (partial)")).toBeInTheDocument();
  });

  it("a partial month with a budget does not claim 'no breach'", () => {
    renderPanel(
      report(
        row({
          money: null,
          money_status: "partial",
          money_parts: [],
          budget: { verdict: "not_evaluated", on_breach: "warn", checks: [], reasons: ["history"] },
        }),
        false,
      ),
    );
    expect(screen.queryByText("No breach this month.")).toBeNull();
    expect(screen.getByText(/Breach events are not known/)).toBeInTheDocument();
  });

  it("a value a newer engine adds is never read as measured or priced", () => {
    const product = row({
      money_status: "estimated_somehow" as unknown as ProductMonthCost["money_status"],
      allocated_upstream: [
        { ...row().allocated_upstream[0], basis: "magic" as unknown as "bytes" },
      ],
    });
    expect(moneyState(product)).toBe("not_measured");
    expect(basisState("magic" as unknown as "bytes")).toBe("not_measured");
    renderPanel(report(product));
    const card = chargedCard();
    expect(card.textContent).not.toMatch(/\$/);
    expect(within(card).getByText(/status estimated_somehow/)).toBeInTheDocument();
  });

  it("names owner_pays as itself, and shows the report's caveats", () => {
    const empty = report(
      row({ allocated_upstream: [{ ...row().allocated_upstream[0], basis: "owner_pays" }] }),
    );
    renderPanel({
      ...empty,
      warnings: ["[product_budget.ghost] names no product that owns a model; it is not checked"],
      not_measured: { failed_executions: 2, storage_unknown_tables: 0, unattributed_read_tables: 3 },
    });
    expect(screen.getByText("owner pays")).toBeInTheDocument();
    expect(screen.getByText(/names no product that owns a model/)).toBeInTheDocument();
    expect(screen.getByText(/2 failed executions recorded no time/)).toBeInTheDocument();
    expect(screen.getByText(/3 tables were read with no recorded build/)).toBeInTheDocument();
  });

  it("a priced zero is a real $0.00, and a tiny figure keeps its digits", () => {
    expect(formatUsd(0)).toBe("$0.00");
    expect(formatUsd(0.0014)).toBe("$0.0014");
  });
});

describe("budget and breaches", () => {
  it("shows a breached budget with its limits and every breach event", () => {
    const check = {
      limit_type: "max_compute_hours" as const,
      limit: 0.0001,
      actual: 0.000389,
      verdict: "breached" as const,
    };
    renderPanel(
      report(
        row({
          budget: { verdict: "breached", on_breach: "warn", checks: [check] },
          breach_events: [
            { run_id: "run-1", at: "2026-11-05T02:00:10Z", breached: [check] },
            { run_id: "run-7", at: "2026-11-07T02:00:10Z", breached: [check] },
          ],
        }),
      ),
    );
    const budget = screen.getByText("budget").closest("[data-tone]") as HTMLElement;
    expect(budget.getAttribute("data-tone")).toBe("risk");
    expect(within(budget).getByText("breached")).toBeInTheDocument();
    const limits = screen.getByRole("table", { name: "Budget limits" });
    expect(within(limits).getByText("max_compute_hours")).toBeInTheDocument();
    const events = screen.getByRole("table", { name: "Breach events" });
    expect(within(events).getByText("run-1")).toBeInTheDocument();
    expect(within(events).getByText("run-7")).toBeInTheDocument();
  });

  it("says why breach events were not listed, and never 'no breach'", () => {
    renderPanel(
      report(
        row({
          budget: { verdict: "not_evaluated", on_breach: "warn", checks: [], reasons: ["history"] },
          breach_events_skipped: "breach events are not listed: too many runs",
        }),
      ),
    );
    expect(screen.getByText("breach events are not listed: too many runs")).toBeInTheDocument();
    expect(screen.queryByText("No breach this month.")).toBeNull();
    expect(screen.getByText("not evaluated")).toBeInTheDocument();
  });

  it("shows provisional and shared-not-charged rows", () => {
    renderPanel(
      report(
        row({
          allocated_upstream: [{ ...row().allocated_upstream[0], provisional: true }],
          shared_not_charged: [
            { table: "revenue_daily", reader: "adhoc", unit: "compute_ms", basis: "bytes", amount: 20 },
          ],
        }),
      ),
    );
    expect(screen.getByText(/provisional: a later reader/)).toBeInTheDocument();
    const shared = screen.getByRole("table", {
      name: "Shares of this product's tables that readers did not pay",
    });
    expect(within(shared).getByText("adhoc")).toBeInTheDocument();
  });

  it("a product with no row in the report is not shown as zero", () => {
    const empty = { ...report(row()), products: [] };
    renderPanel(empty);
    expect(screen.getByText(/has no row in the month's report/)).toBeInTheDocument();
    expect(chargedCardOrNull()).toBeNull();
  });
});

function chargedCardOrNull(): HTMLElement | null {
  return screen.queryByText("charged, in USD");
}

describe("delivery", () => {
  it("shows the deadline, a late delivery, the risk verdict, and the volumes", () => {
    const status = {
      ...STATUS,
      delivery: {
        product: "revenue_daily",
        output_model: "revenue_daily",
        deliver_by: "0 6 * * *",
        timezone: "UTC",
        on_time: false,
        late_by_seconds: 125,
        last_due_at: "2026-11-08T06:00:00Z",
        last_delivered_at: "2026-11-08T06:02:05Z",
        next_due_at: "2026-11-09T06:00:00Z",
        at_risk: null,
        estimate: { basis: "model_delivery", growth: false, n: 2, state: "unknown", window: 20 },
      },
      volume: { min_rows: 10, max_rows: 1000, row_count: 4, verdict: "below_min", severity: "error" },
      source_volumes: [
        { table: "raw.orders", min_rows: 1, severity: "warning", verdict: "not_evaluated" },
      ],
    } as ProductStatusOutput;
    renderPanel(report(row()), status);
    expect(screen.getByText("0 6 * * *")).toBeInTheDocument();
    expect(screen.getByText("late by 2m 05s")).toBeInTheDocument();
    expect(screen.getByText("no verdict")).toBeInTheDocument();
    expect(screen.getByText(/records the latest delivery only/)).toBeInTheDocument();
    const volumes = screen.getByRole("table", {
      name: "Declared volume ranges and their latest verdicts",
    });
    expect(within(volumes).getByText("below the minimum")).toBeInTheDocument();
    expect(within(volumes).getByText("not evaluated: no count yet")).toBeInTheDocument();
  });

  it("says when the spec declares no deadline", () => {
    renderPanel(report(row()));
    expect(screen.getByText(/No deadline/)).toBeInTheDocument();
  });

  it("lists only the pipelines that deliver this product, marked Experimental", () => {
    expect(pipelinesFor("revenue_daily", SCHEDULE).map((p) => p.pipeline)).toEqual(["nightly"]);
    renderPanel(report(row()));
    expect(screen.getByText("Experimental")).toBeInTheDocument();
    const table = screen.getByRole("table", { name: "Scheduled pipelines that deliver this product" });
    expect(within(table).getByText("nightly")).toBeInTheDocument();
    expect(within(table).queryByText("other")).toBeNull();
  });

  it("renders a refused route as its envelope, not as empty", () => {
    const refused = {
      kind: "unreachable",
      message: "connection refused",
    } as Resource<ScheduleStatusOutput>;
    renderPanel(report(row()), STATUS, refused);
    expect(screen.getByText("the engine did not answer")).toBeInTheDocument();
  });

  it("has no edit control", () => {
    renderPanel(report(row()));
    const panel = screen.getByRole("region", { name: "Delivery and cost" });
    expect(within(panel).queryAllByRole("button")).toHaveLength(0);
    expect(within(panel).queryAllByRole("textbox")).toHaveLength(0);
  });
});
