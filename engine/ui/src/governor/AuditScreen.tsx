import { useCallback, useState } from "react";
import type { AuditOutput } from "@rocky-types/audit";
import type { ProductListOutput } from "@rocky-types/product_list";
import { apiGet } from "../api";
import { EmptyState, StatusCard } from "../components";
import { useResource } from "../estate/useResource";
import { formatInstant } from "../format";
import { CustodyLink } from "./links";
import { Rows } from "./SectionCard";

export interface AuditLoaders {
  /** The ledger, whole (`null`) or scoped to a product. */
  ledger: (product: string | null) => Promise<AuditOutput>;
  /** Every product the project knows, for the selector. */
  products: () => Promise<ProductListOutput>;
}

export const defaultAuditLoaders: AuditLoaders = {
  ledger: (product) =>
    apiGet<AuditOutput>(product ? `audit?product=${encodeURIComponent(product)}` : "audit"),
  products: () => apiGet<ProductListOutput>("products"),
};

/**
 * The policy-decision ledger, `GET /api/v1/audit`, whole or scoped to one
 * product's output model with `?product=`. Oldest first, as the CLI prints
 * it. Every plan id links to its custody chain.
 */
export function AuditScreen({ loaders = defaultAuditLoaders, now }: { loaders?: AuditLoaders; now?: number }) {
  const [product, setProduct] = useState<string | null>(null);
  const products = useResource(loaders.products, [loaders]);
  const ledgerLoader = useCallback(() => loaders.ledger(product), [loaders, product]);
  const ledger = useResource(ledgerLoader, [ledgerLoader]);

  const names = products.kind === "ready" ? products.value.products.map((p) => p.name) : [];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-xs text-zinc-600 dark:text-zinc-300" htmlFor="audit-product">
          Product
        </label>
        <select
          id="audit-product"
          value={product ?? ""}
          onChange={(event) => setProduct(event.target.value === "" ? null : event.target.value)}
          className="rounded border border-zinc-300 bg-white px-2 py-1 text-xs dark:border-zinc-700 dark:bg-zinc-900"
        >
          <option value="">all decisions</option>
          {names.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
        {products.kind === "refused" && (
          <span className="text-xs text-amber-700 dark:text-amber-400">
            product list refused: {products.error.envelope.code}
          </span>
        )}
        <button
          type="button"
          onClick={ledger.reload}
          className="rounded border border-zinc-300 px-2 py-1 text-xs text-zinc-700 hover:bg-zinc-100 dark:border-zinc-700 dark:text-zinc-200 dark:hover:bg-zinc-800"
        >
          Refresh
        </button>
        <code className="text-[11px] text-zinc-500 dark:text-zinc-400">
          GET /api/v1/audit{product ? `?product=${product}` : ""}
        </code>
      </div>
      {ledger.kind === "loading" && <p className="text-sm text-zinc-500">Loading the ledger…</p>}
      {ledger.kind === "refused" && (
        <StatusCard
          label={`refused (${ledger.error.status})`}
          value={ledger.error.envelope.code}
          tone="risk"
          sub={ledger.error.envelope.remediation_hint ?? ledger.error.envelope.message}
        />
      )}
      {ledger.kind === "unreachable" && (
        <StatusCard label="engine" value="unreachable" tone="risk" sub={ledger.message} />
      )}
      {ledger.kind === "ready" && <LedgerBody ledger={ledger.value} now={now} />}
    </div>
  );
}

function LedgerBody({ ledger, now }: { ledger: AuditOutput; now?: number }) {
  if (ledger.decisions.length === 0) {
    return (
      <EmptyState
        title="No decision recorded"
        detail={
          ledger.product
            ? `No governed mutation touched ${ledger.product.output_model}, the output model of ${ledger.product.name}.`
            : "Only mutating enforcement seams record decisions; reads never do."
        }
      />
    );
  }
  return (
    <div className="space-y-2">
      {ledger.product && (
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          Scoped to <code>{ledger.product.name}</code>, whose output model is{" "}
          <code>{ledger.product.output_model}</code>.
        </p>
      )}
      <Rows
        ariaLabel="Policy decisions"
        columns={["when", "principal", "capability", "model", "effect", "rule", "plan", "reason"]}
        rows={ledger.decisions.map((entry) => [
          formatInstant(entry.timestamp, now),
          entry.principal,
          entry.capability,
          <CustodyLink key={`m-${entry.plan_id}-${entry.model}`} subject={entry.model} />,
          entry.effect,
          entry.rule_id === null || entry.rule_id === undefined ? "default" : `rule ${entry.rule_id}`,
          <CustodyLink key={`p-${entry.plan_id}-${entry.model}`} subject={entry.plan_id} />,
          entry.reason,
        ])}
      />
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        {ledger.decisions.length} decision(s), oldest first, the whole ledger
      </p>
    </div>
  );
}
