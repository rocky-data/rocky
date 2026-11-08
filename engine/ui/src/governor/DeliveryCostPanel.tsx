import type { ReactNode } from "react";
import type {
  AllocatedUpstreamCost,
  CostAmount,
  CostSplitBasis,
  CostUnit,
  MoneyStatus,
  ProductBudgetCheck,
  ProductBudgetLimitType,
  ProductBudgetVerdict,
  ProductMonthCost,
  ProductMonthCostOutput,
} from "@rocky-types/cost_by_product";
import type {
  DeliveryStatus,
  ProductSourceVolumeStatus,
  ProductStatusOutput,
  ProductVolumeStatus,
  VolumeVerdict,
} from "@rocky-types/product_status";
import type { SchedulePipelineStatus, ScheduleStatusOutput } from "@rocky-types/schedule_status";
import { StatusCard, type Tone } from "../components";
import type { Resource } from "../estate/useResource";
import { NOT_RECORDED, formatDuration, formatInstant } from "../format";
import { ResourceState } from "../review/ResourceState";
import { Rows } from "./SectionCard";

/**
 * The honest state of one cost figure. A figure is never shown as `0` or
 * `$0` unless the engine measured and priced it as that.
 *
 * - `measured`: the engine recorded it (model time, data-file storage), or
 *   split it by the bytes read.
 * - `estimated`: split by estimated bytes (rows times projected width), or
 *   storage counted by allocated blocks, which the engine calls an estimate.
 * - `rows_only`: split by rows scanned, with no weight for how wide a row
 *   is. A count with no byte measure behind it.
 * - `not_measured`: Rocky does not measure it, so it is unknown, not zero.
 * - `no_price`: measured, but `[cost]` sets no price for it, so there is no
 *   money figure.
 */
export type FigureState = "measured" | "estimated" | "rows_only" | "not_measured" | "no_price";

export const FIGURE_STATE_LABEL: Record<FigureState, string> = {
  measured: "measured",
  estimated: "estimated",
  rows_only: "rows only",
  not_measured: "not measured",
  no_price: "no price",
};

const FIGURE_STATE_TONE: Record<FigureState, string> = {
  measured: "border-emerald-300 text-emerald-800 dark:border-emerald-800 dark:text-emerald-300",
  estimated: "border-sky-300 text-sky-800 dark:border-sky-800 dark:text-sky-300",
  rows_only: "border-violet-300 text-violet-800 dark:border-violet-800 dark:text-violet-300",
  not_measured: "border-amber-300 text-amber-800 dark:border-amber-800 dark:text-amber-300",
  no_price: "border-zinc-300 text-zinc-700 dark:border-zinc-700 dark:text-zinc-300",
};

/** How a split's `basis` reads as a figure state. */
export function basisState(basis: CostSplitBasis): FigureState {
  switch (basis) {
    case "bytes":
      return "measured";
    case "estimated_bytes":
      return "estimated";
    case "rows_scanned_unweighted":
      return "rows_only";
    case "owner_pays":
      // Nothing usable was measured, so the owner kept the cost.
      return "not_measured";
  }
}

/** How a physical unit reads as a figure state. */
export function unitState(unit: CostUnit): FigureState {
  switch (unit) {
    case "compute_ms":
    case "storage_byte_hours_data_files":
      return "measured";
    case "storage_byte_hours_allocated_blocks":
      return "estimated";
  }
}

const STATE_RANK: Record<FigureState, number> = {
  measured: 0,
  estimated: 1,
  rows_only: 2,
  not_measured: 3,
  no_price: 4,
};

/** The weaker of two states: a figure is only as sure as its weakest part. */
function weaker(a: FigureState, b: FigureState): FigureState {
  return STATE_RANK[b] > STATE_RANK[a] ? b : a;
}

/**
 * The state of a product's charged money. `priced` alone is not enough to
 * call it measured: a share split by estimated bytes or by rows makes the
 * whole figure that sure and no surer. `partial` is not one of the five: it
 * is its own answer, with no total at all.
 */
export function moneyState(cost: ProductMonthCost): FigureState | "partial" {
  switch (cost.money_status) {
    case "partial":
      return "partial";
    case "no_price":
      return "no_price";
    case "not_measured":
      return "not_measured";
    case "priced":
      break;
  }
  let state: FigureState = "measured";
  for (const amount of cost.charged) {
    if (amount.amount > 0) state = weaker(state, unitState(amount.unit));
  }
  for (const share of cost.allocated_upstream) {
    if (share.amount > 0 && share.basis !== "owner_pays") {
      state = weaker(state, basisState(share.basis));
    }
  }
  return state;
}

/** A state written as text in a bordered chip: the word carries it, not the colour. */
export function FigureChip({ state }: { state: FigureState | "partial" }) {
  const label = state === "partial" ? "partial" : FIGURE_STATE_LABEL[state];
  const tone =
    state === "partial"
      ? "border-amber-300 text-amber-800 dark:border-amber-800 dark:text-amber-300"
      : FIGURE_STATE_TONE[state];
  return (
    <span
      data-figure-state={state}
      className={`inline-block rounded-full border px-2 py-0.5 text-[11px] font-medium ${tone}`}
    >
      {label}
    </span>
  );
}

/** USD as text. Only a priced figure reaches here. */
export function formatUsd(usd: number): string {
  if (usd !== 0 && Math.abs(usd) < 0.01) return `$${usd.toPrecision(2)}`;
  return `$${usd.toFixed(2)}`;
}

const BYTES_PER_GIB = 2 ** 30;

/** One physical amount as text, in the unit it was counted in. */
export function formatAmount(unit: CostUnit, amount: number): string {
  switch (unit) {
    case "compute_ms":
      return `${formatDuration(amount)} model time`;
    case "storage_byte_hours_data_files":
      return `${formatNumber(amount / BYTES_PER_GIB)} GiB-hours (data files)`;
    case "storage_byte_hours_allocated_blocks":
      return `${formatNumber(amount / BYTES_PER_GIB)} GiB-hours (allocated blocks)`;
  }
}

function formatNumber(value: number): string {
  if (value === 0) return "0";
  if (Math.abs(value) >= 100) return value.toFixed(0);
  return Number(value.toPrecision(3)).toString();
}

/** The money figure, or the reason there is none. Never `$0` for an unknown. */
function MoneyFigureText({ cost }: { cost: ProductMonthCost }) {
  const state = moneyState(cost);
  const text = (() => {
    switch (cost.money_status) {
      case "priced":
        return cost.money === null || cost.money === undefined
          ? NOT_RECORDED
          : formatUsd(cost.money);
      case "no_price":
        return "no money figure: [cost] sets no price";
      case "not_measured":
        return "no money figure: Rocky does not measure it";
      case "partial":
        return "no total: the month is partial";
    }
  })();
  return (
    <span className="inline-flex flex-wrap items-baseline gap-2">
      <span>{text}</span>
      <FigureChip state={state} />
    </span>
  );
}

/**
 * The state of a charged amount in one unit: the unit's own state, made
 * weaker by every share of that unit the product paid for a table it reads.
 * A charge that is mostly a rows-only share is not "measured".
 */
export function chargedUnitState(cost: ProductMonthCost, unit: CostUnit): FigureState {
  let state = unitState(unit);
  for (const share of cost.allocated_upstream) {
    if (share.unit === unit && share.amount > 0 && share.basis !== "owner_pays") {
      state = weaker(state, basisState(share.basis));
    }
  }
  return state;
}

function AmountsText({
  amounts,
  stateOf = unitState,
}: {
  amounts: CostAmount[];
  stateOf?: (unit: CostUnit) => FigureState;
}) {
  if (amounts.length === 0) {
    // An empty list is "nothing recorded in this unit", not "zero".
    return <span>{NOT_RECORDED}</span>;
  }
  return (
    <ul className="space-y-0.5">
      {amounts.map((amount) => (
        <li key={amount.unit} className="flex flex-wrap items-baseline gap-2">
          <span>{formatAmount(amount.unit, amount.amount)}</span>
          <FigureChip state={stateOf(amount.unit)} />
        </li>
      ))}
    </ul>
  );
}

function moneyStatusLabel(status: MoneyStatus): string {
  switch (status) {
    case "priced":
      return "priced";
    case "no_price":
      return "no price";
    case "not_measured":
      return "not measured";
    case "partial":
      return "partial";
  }
}

function verdictTone(verdict: ProductBudgetVerdict): Tone {
  switch (verdict) {
    case "in_budget":
      return "ok";
    case "breached":
      return "risk";
    case "not_evaluated":
      return "warn";
  }
}

function verdictLabel(verdict: ProductBudgetVerdict): string {
  switch (verdict) {
    case "in_budget":
      return "in budget";
    case "breached":
      return "breached";
    case "not_evaluated":
      // Never read as in budget.
      return "not evaluated";
  }
}

function formatLimit(type: ProductBudgetLimitType, value: number): string {
  switch (type) {
    case "max_compute_hours":
      return `${formatNumber(value)} h`;
    case "max_storage_gb_month":
      return `${formatNumber(value)} GB-month`;
    case "max_usd":
      return formatUsd(value);
  }
}

function checkCells(check: ProductBudgetCheck): ReactNode[] {
  return [
    <span key="t" className="font-mono text-xs">
      {check.limit_type}
      {check.unit ? ` (${check.unit})` : ""}
    </span>,
    formatLimit(check.limit_type, check.limit),
    check.actual === null || check.actual === undefined
      ? "not known"
      : formatLimit(check.limit_type, check.actual),
    <span key="v" data-verdict={check.verdict}>
      {verdictLabel(check.verdict)}
      {check.reason ? `: ${check.reason}` : ""}
    </span>,
  ];
}

function UpstreamTable({ rows }: { rows: AllocatedUpstreamCost[] }) {
  return (
    <Rows
      ariaLabel="What this product pays for tables it reads"
      columns={["table", "charged", "share", "split by", "notes"]}
      rows={rows.map((row) => [
        <span key="t" className="font-mono text-xs">
          {row.table}
        </span>,
        formatAmount(row.unit, row.amount),
        <span key="s" title={formatAmount(row.unit, row.of_total)}>
          {row.of_total > 0
            ? `${formatNumber((row.amount / row.of_total) * 100)}% of the table's cost`
            : NOT_RECORDED}
        </span>,
        <FigureChip key="b" state={basisState(row.basis)} />,
        <UpstreamNotes key="n" row={row} />,
      ])}
    />
  );
}

/** A share's caveats, one per line, so a long note never widens the table. */
function UpstreamNotes({ row }: { row: AllocatedUpstreamCost }) {
  const notes: string[] = [];
  if (row.provisional) notes.push("provisional: a later reader can still change it");
  if (row.low_confidence) notes.push("low confidence: the scan was very small");
  if (notes.length === 0) return <span>none</span>;
  return (
    <ul className="space-y-0.5 break-normal whitespace-normal">
      {notes.map((note) => (
        <li key={note}>{note}</li>
      ))}
    </ul>
  );
}

/** The cost half: one product's row of `rocky cost --month <month> --by product`. */
export function CostSection({
  name,
  month,
  costMonthError,
}: {
  name: string;
  month: Resource<ProductMonthCostOutput>;
  costMonthError?: string | null;
}) {
  return (
    <section aria-labelledby={`cost-${name}`} className="space-y-3">
      <h4 id={`cost-${name}`} className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        Cost this month
      </h4>
      {costMonthError ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          the product status could not compute this month: {costMonthError}
        </p>
      ) : null}
      {month.kind !== "ready" ? (
        <ResourceState resource={month} loadingLine="reading the month's cost…" />
      ) : (
        <CostBody name={name} report={month.value} />
      )}
    </section>
  );
}

function CostBody({ name, report }: { name: string; report: ProductMonthCostOutput }) {
  const cost = report.products.find((row) => row.product === name);
  const header = (
    <p className="text-xs text-zinc-600 dark:text-zinc-300">
      {report.month} in {report.timezone}, as of {formatInstant(report.as_of)}. Read-only: the
      budget is set in <code>rocky.toml</code>.
    </p>
  );
  if (cost === undefined) {
    return (
      <div className="space-y-2">
        {header}
        <p className="text-sm text-zinc-600 dark:text-zinc-300">
          This product has no row in the month&apos;s report: it owns no model the report could
          split cost to. That is not a cost of zero.
        </p>
      </div>
    );
  }
  return (
    <div className="space-y-3">
      {header}
      {!report.history_complete && (
        <StatusCard
          label="partial month"
          value="run history does not cover the whole month"
          tone="warn"
          sub="State retention may have swept runs this month needs. Every figure below is partial, and there is no money total."
        />
      )}
      <div className="grid gap-2 sm:grid-cols-2">
        <StatusCard
          label="charged, in USD"
          value={<MoneyFigureText cost={cost} />}
          tone={cost.money_status === "priced" ? "muted" : "warn"}
          sub="Compute and storage only. Egress and AI tokens are not measured."
        />
        <StatusCard
          label="charged"
          value={
            <AmountsText
              amounts={cost.charged}
              stateOf={(unit) => chargedUnitState(cost, unit)}
            />
          }
          sub="Kept plus what it paid for tables it reads."
        />
        <StatusCard
          label="kept"
          value={<AmountsText amounts={cost.kept} />}
          sub="What the product still pays for its own builds after other products pay for what they read."
        />
        <StatusCard
          label="built (memo, not summed)"
          value={<AmountsText amounts={cost.built} />}
          sub="What its own builds cost before any split."
        />
      </div>
      {cost.money_parts.length > 0 && (
        <Rows
          ariaLabel="Money by figure"
          columns={["figure", "status", "USD"]}
          rows={cost.money_parts.map((part) => [
            part.figure,
            moneyStatusLabel(part.status),
            part.status === "priced" && part.usd !== null && part.usd !== undefined
              ? formatUsd(part.usd)
              : "none",
          ])}
        />
      )}
      <div className="space-y-1">
        <h5 className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">
          Allocated upstream
        </h5>
        {cost.allocated_upstream.length === 0 ? (
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            It was charged for no table it reads.
          </p>
        ) : (
          <UpstreamTable rows={cost.allocated_upstream} />
        )}
      </div>
      {cost.shared_not_charged.length > 0 && (
        <div className="space-y-1">
          <h5 className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">
            Shared, not charged
          </h5>
          <Rows
            ariaLabel="Shares of this product's tables that readers did not pay"
            columns={["table", "reader", "share it keeps", "split by"]}
            rows={cost.shared_not_charged.map((row) => [
              <span key="t" className="font-mono text-xs">
                {row.table}
              </span>,
              <span key="r" className="font-mono text-xs">
                {row.reader}
              </span>,
              formatAmount(row.unit, row.amount),
              <FigureChip key="b" state={basisState(row.basis)} />,
            ])}
          />
        </div>
      )}
      <BudgetBlock cost={cost} />
    </div>
  );
}

function BudgetBlock({ cost }: { cost: ProductMonthCost }) {
  const budget = cost.budget;
  if (budget === null || budget === undefined) {
    return (
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        No monthly budget: <code>rocky.toml</code> has no{" "}
        <code>[product_budget.{cost.product}]</code>.
      </p>
    );
  }
  const events = cost.breach_events ?? [];
  return (
    <div className="space-y-2">
      <h5 className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">Monthly budget</h5>
      <StatusCard
        label="budget"
        value={<span data-verdict={budget.verdict}>{verdictLabel(budget.verdict)}</span>}
        tone={verdictTone(budget.verdict)}
        sub={
          budget.reasons && budget.reasons.length > 0
            ? budget.reasons.join("; ")
            : `On a breach Rocky does: ${budget.on_breach}. The run's exit code does not change.`
        }
      />
      {budget.checks.length > 0 && (
        <Rows
          ariaLabel="Budget limits"
          columns={["limit", "set to", "month to date", "verdict"]}
          rows={budget.checks.map(checkCells)}
        />
      )}
      {cost.breach_events_skipped ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          {cost.breach_events_skipped}
        </p>
      ) : events.length === 0 ? (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">No breach this month.</p>
      ) : (
        <Rows
          ariaLabel="Breach events"
          columns={["after run", "at", "limits exceeded"]}
          rows={events.map((event) => [
            <span key="r" className="font-mono text-xs">
              {event.run_id}
            </span>,
            formatInstant(event.at),
            event.breached
              .map(
                (check) =>
                  `${check.limit_type}: ${
                    check.actual === null || check.actual === undefined
                      ? "not known"
                      : formatLimit(check.limit_type, check.actual)
                  } over ${formatLimit(check.limit_type, check.limit)}`,
              )
              .join("; "),
          ])}
        />
      )}
    </div>
  );
}

function volumeVerdictLabel(verdict: VolumeVerdict): string {
  switch (verdict) {
    case "in_range":
      return "in range";
    case "below_min":
      return "below the minimum";
    case "above_max":
      return "above the maximum";
    case "not_evaluated":
      return "not evaluated: no count yet";
  }
}

function formatRange(min?: number | null, max?: number | null): string {
  const low = min === null || min === undefined ? "no minimum" : `${min}`;
  const high = max === null || max === undefined ? "no maximum" : `${max}`;
  return `${low} to ${high} rows`;
}

function volumeCells(
  table: string,
  volume: ProductVolumeStatus | ProductSourceVolumeStatus,
): ReactNode[] {
  return [
    <span key="t" className="font-mono text-xs">
      {table}
    </span>,
    formatRange(volume.min_rows, volume.max_rows),
    volume.row_count === null || volume.row_count === undefined
      ? NOT_RECORDED
      : `${volume.row_count} rows`,
    <span key="v" data-verdict={volume.verdict}>
      {volumeVerdictLabel(volume.verdict)}
    </span>,
    volume.severity,
    formatInstant(volume.counted_at ?? null),
  ];
}

function lateText(delivery: DeliveryStatus): { value: string; tone: Tone } {
  if (delivery.on_time === true) return { value: "on time", tone: "ok" };
  if (delivery.on_time === false) {
    const late = delivery.late_by_seconds;
    return {
      value:
        late === null || late === undefined
          ? "late"
          : `late by ${formatDuration(late * 1000)}`,
      tone: "risk",
    };
  }
  return { value: "no delivery has recorded a deadline yet", tone: "muted" };
}

function riskText(delivery: DeliveryStatus): { value: string; tone: Tone; sub: string } {
  const { estimate } = delivery;
  const basis = `${estimate.n} of the newest ${estimate.window} production runs, timing ${
    estimate.basis === "model_delivery" ? "run start to the model's finish" : "the whole run"
  }`;
  if (delivery.at_risk === true) {
    return {
      value: "at risk",
      tone: "risk",
      sub: `The p90 (${formatDuration(estimate.p90_ms ?? 0)}) is longer than the time left. From ${basis}.`,
    };
  }
  if (delivery.at_risk === false) {
    return { value: "not at risk", tone: "ok", sub: `From ${basis}.` };
  }
  return {
    value: "no verdict",
    tone: "muted",
    sub: `The duration estimate is unknown: it needs at least 5 runs and has ${estimate.n}.`,
  };
}

/** The delivery half: the deadline, the latest delivery, and the declared volumes. */
export function DeliverySection({ name, status }: { name: string; status: ProductStatusOutput }) {
  const delivery = status.delivery;
  const sources = status.source_volumes ?? [];
  const volumeRows: ReactNode[][] = [];
  if (status.volume) {
    volumeRows.push(volumeCells(`${status.output_model ?? name} (output)`, status.volume));
  }
  for (const source of sources) volumeRows.push(volumeCells(source.table, source));
  return (
    <section aria-labelledby={`delivery-${name}`} className="space-y-3">
      <h4 id={`delivery-${name}`} className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        Delivery
      </h4>
      {delivery ? (
        <>
          <div className="grid gap-2 sm:grid-cols-3">
            <StatusCard
              label="deadline"
              value={<span className="font-mono">{delivery.deliver_by}</span>}
              sub={`deliver_by, read in ${delivery.timezone}. Next due ${formatInstant(
                delivery.next_due_at ?? null,
              )}.`}
            />
            <StatusCard
              label="latest delivery"
              value={lateText(delivery).value}
              tone={lateText(delivery).tone}
              sub={`Finished ${formatInstant(delivery.last_delivered_at ?? null)}; it served the deadline ${formatInstant(
                delivery.last_due_at ?? null,
              )}.`}
            />
            <StatusCard
              label="next delivery"
              value={riskText(delivery).value}
              tone={riskText(delivery).tone}
              sub={riskText(delivery).sub}
            />
          </div>
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            The engine records the latest delivery only. This is not a history of every run.
          </p>
        </>
      ) : (
        <p className="text-sm text-zinc-600 dark:text-zinc-300">
          No deadline: the spec declares no <code>deliver_by</code>.
        </p>
      )}
      <div className="space-y-1">
        <h5 className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">Declared volumes</h5>
        {volumeRows.length === 0 ? (
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            The spec declares no row-count range for its output or its sources.
          </p>
        ) : (
          <Rows
            ariaLabel="Declared volume ranges and their latest verdicts"
            columns={["table", "declared range", "latest count", "verdict", "severity", "counted at"]}
            rows={volumeRows}
          />
        )}
      </div>
    </section>
  );
}

/** The pipelines whose deadline state names this product. */
export function pipelinesFor(name: string, schedule: ScheduleStatusOutput): SchedulePipelineStatus[] {
  return schedule.pipelines.filter(
    (pipeline) => pipeline.delivery?.product === name || pipeline.latest_start?.product === name,
  );
}

/** The scheduler's view of this product's deadline. Marked experimental, as the scheduler is. */
export function ScheduleSection({
  name,
  schedule,
}: {
  name: string;
  schedule: Resource<ScheduleStatusOutput>;
}) {
  return (
    <section aria-labelledby={`schedule-${name}`} className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <h4 id={`schedule-${name}`} className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          Schedules
        </h4>
        <span className="rounded-full border border-zinc-300 px-2 py-0.5 text-[11px] font-medium text-zinc-700 dark:border-zinc-700 dark:text-zinc-300">
          Experimental
        </span>
      </div>
      {schedule.kind !== "ready" ? (
        <ResourceState resource={schedule} loadingLine="reading the schedules…" />
      ) : (
        (() => {
          const rows = pipelinesFor(name, schedule.value);
          if (rows.length === 0) {
            return (
              <p className="text-xs text-zinc-600 dark:text-zinc-300">
                No scheduled pipeline is known to deliver this product.
              </p>
            );
          }
          return (
            <Rows
              ariaLabel="Scheduled pipelines that deliver this product"
              columns={["pipeline", "cron", "next fire", "latest start", "cron check", "warning"]}
              rows={rows.map((pipeline) => [
                <span key="p" className="font-mono text-xs">
                  {pipeline.pipeline}
                </span>,
                pipeline.cron ?? "none",
                formatInstant(pipeline.next_fire_at ?? null),
                pipeline.latest_start
                  ? pipeline.latest_start.state === "known"
                    ? formatInstant(pipeline.latest_start.at ?? null)
                    : `unknown (${(pipeline.latest_start.reasons ?? []).join(", ") || "no reason given"})`
                  : NOT_RECORDED,
                pipeline.latest_start?.cron_check ?? NOT_RECORDED,
                pipeline.latest_start?.warning ?? "none",
              ])}
            />
          );
        })()
      )}
    </section>
  );
}

/**
 * One product's delivery and cost, read only. Nothing here edits a deadline
 * or a budget: both live in files, and editing them from the browser waits
 * on its own authorization design.
 */
export function DeliveryCostPanel({
  name,
  status,
  month,
  schedule,
}: {
  name: string;
  status: ProductStatusOutput;
  month: Resource<ProductMonthCostOutput>;
  schedule: Resource<ScheduleStatusOutput>;
}) {
  return (
    <section
      aria-label="Delivery and cost"
      className="space-y-5 rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900"
    >
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">Delivery and cost</h3>
      <DeliverySection name={name} status={status} />
      <ScheduleSection name={name} schedule={schedule} />
      <CostSection name={name} month={month} costMonthError={status.cost_month_error} />
    </section>
  );
}
