import { useCallback } from "react";
import type { ProductJournalEntry, ProductJournalOutput } from "@rocky-types/product_journal";
import type { ProductListEntry, ProductListOutput } from "@rocky-types/product_list";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import { apiGet } from "../api";
import { StatusCard } from "../components";
import { useResource } from "../estate/useResource";
import { elideMiddle, formatInstant, shortId } from "../format";
import { navigateTo, pathForLane } from "../router";
import { ResourceState } from "../review/ResourceState";
import { reviewPath } from "../review/paths";
import { CustodyLink } from "./links";

export interface ProductLoaders {
  list: () => Promise<ProductListOutput>;
  status: (name: string) => Promise<ProductStatusOutput>;
  journal: (name: string) => Promise<ProductJournalOutput>;
}

export const defaultProductLoaders: ProductLoaders = {
  list: () => apiGet<ProductListOutput>("products"),
  status: (name) => apiGet<ProductStatusOutput>(`products/${encodeURIComponent(name)}`),
  journal: (name) => apiGet<ProductJournalOutput>(`products/${encodeURIComponent(name)}/journal`),
};

/** The path of one product's timeline. */
export function productPath(name: string): string {
  return `${pathForLane("governor", "products")}/${encodeURIComponent(name)}`;
}

function ProductRow({ entry }: { entry: ProductListEntry }) {
  const href = productPath(entry.name);
  return (
    <li className="rounded-md border border-zinc-200 p-3 dark:border-zinc-700">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <a
          href={href}
          onClick={(event) => {
            event.preventDefault();
            navigateTo(href);
          }}
          className="font-mono text-sm text-sky-700 underline-offset-2 hover:underline dark:text-sky-400"
        >
          {entry.name}
        </a>
        <span className="text-[11px] uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
          {entry.fulfill_state ?? "the loop has not run"}
        </span>
      </div>
      {!entry.spec_present && (
        // A deleted spec still lists. Hiding it would make a product that was
        // removed look like one that never existed.
        <p className="mt-1 text-xs text-amber-700 dark:text-amber-400">
          {describeSpecTrouble(entry.spec_error)}:{" "}
          {entry.spec_error ?? "the loader gave no reason"}
        </p>
      )}
      <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-zinc-600 sm:grid-cols-4 dark:text-zinc-300">
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">journal rows</dt>
          <dd>{entry.journal_rows}</dd>
        </div>
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">approval</dt>
          <dd>{entry.approval ? "recorded" : "none"}</dd>
        </div>
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">committed phase</dt>
          <dd>{entry.committed_phase ?? "none"}</dd>
        </div>
        <div>
          <dt className="text-zinc-500 dark:text-zinc-400">artifact problems</dt>
          <dd>{entry.artifact_problems}</dd>
        </div>
      </dl>
    </li>
  );
}

/** Every product the project knows, spec files and store records alike. */
export function ProductList({ load }: { load: () => Promise<ProductListOutput> }) {
  const products = useResource(load, [load]);
  if (products.kind !== "ready") {
    return (
      <section aria-label="Products" className="space-y-3">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Products</h2>
        <ResourceState resource={products} loadingLine="reading the products…" />
      </section>
    );
  }
  const { products: rows, count } = products.value;
  return (
    <section aria-label="Products" className="space-y-3">
      <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        {count === 0 ? "This project declares no products" : `${count} products`}
      </h2>
      {rows.length === 0 ? (
        <p className="text-sm text-zinc-600 dark:text-zinc-300">
          A product appears here once `products/&lt;name&gt;.toml` exists, or once the store holds a
          record for one.
        </p>
      ) : (
        <ul className="space-y-2">
          {rows.map((entry) => (
            <ProductRow key={entry.name} entry={entry} />
          ))}
        </ul>
      )}
    </section>
  );
}

function JournalRow({ entry }: { entry: ProductJournalEntry }) {
  const cell = "px-2 py-1.5 align-top";
  return (
    <tr className="border-t border-zinc-100 dark:border-zinc-800">
      <td className={`${cell} font-mono text-[11px] text-zinc-500 dark:text-zinc-400`}>
        {entry.seq}
      </td>
      <td className={`${cell} text-[11px] text-zinc-500 dark:text-zinc-400`}>
        {formatInstant(entry.at ?? null)}
      </td>
      {/* The engine documents `event` as a label to render, not an enum to
          switch on. Rendering the string means an event a newer engine adds
          shows up here instead of vanishing. The cell wraps rather than
          truncates: a repair round's event carries the compiler errors that
          caused it, and a truncated one would hide the reason. */}
      <td className={`${cell} text-zinc-900 dark:text-zinc-100`}>{entry.event}</td>
      <td className={`${cell} text-[11px] text-zinc-500 dark:text-zinc-400`}>
        {entry.from_state ? `${entry.from_state} → ` : ""}
        {entry.to_state}
      </td>
      <td className={`${cell} text-[11px] break-words text-zinc-500 dark:text-zinc-400`}>
        <div className="flex flex-wrap gap-x-3 gap-y-1">
          {entry.spec_digest != null && (
            <span title={entry.spec_digest}>spec {shortId(entry.spec_digest)}</span>
          )}
          {entry.idempotency_key != null && (
            // Not `shortId`: every key for this product begins
            // `product:<name>@`, so a leading slice distinguishes nothing, and
            // the product name is already the page title (#1756).
            <span title={entry.idempotency_key}>key {elideMiddle(entry.idempotency_key)}</span>
          )}
          {entry.plan_id != null && (
            <a
              href={reviewPath(entry.plan_id)}
              onClick={(clicked) => {
                clicked.preventDefault();
                navigateTo(reviewPath(entry.plan_id as string));
              }}
              className="font-mono text-sky-700 underline-offset-2 hover:underline dark:text-sky-400"
              title={entry.plan_id}
            >
              plan {shortId(entry.plan_id)}
            </a>
          )}
        </div>
      </td>
    </tr>
  );
}

/**
 * The journal as a table, every row present and in the engine's order.
 *
 * It is a table rather than a sequence of cards because of what the engine
 * actually writes: one product driven through the loop once produced **82
 * rows** — ownership taken and released around every phase, driver groups
 * started and ended, two repair rounds, four observations. Eighty-two cards is
 * not a timeline a person reads; eighty-two table rows is.
 *
 * Nothing is hidden and nothing is summarised. A filter would have to decide
 * which events matter, and deciding that means switching on `event`, which is
 * exactly what this screen must not do.
 */
function JournalTable({ rows }: { rows: ProductJournalEntry[] }) {
  return (
    <div className="overflow-x-auto rounded-md border border-zinc-200 dark:border-zinc-700">
      {/* `table-fixed` with declared widths, not auto layout. One repair-round
          event carries the whole compiler error list, and under auto layout
          that one cell widens its column past the container: every other
          column is squeezed, their contents wrap, and EVERY row inherits the
          height — measured at 115px each, 7217px for 82 rows, which is no
          better than the cards this replaced. Fixed widths keep a long event
          inside its own column. */}
      <table className="w-full table-fixed text-left text-sm">
        <colgroup>
          <col className="w-10" />
          {/* `when` holds a fixed-width string — "2026-09-06 20:44:26 UTC" —
              and w-40 sat within a few pixels of it, so some rows wrapped
              "UTC" onto a second line and others did not, for no reason a
              reader could see. w-44 clears it with room to spare. It cannot
              be `whitespace-nowrap`: under `table-fixed` that overflows the
              cell rather than widening it. */}
          <col className="w-44" />
          <col />
          <col className="w-44" />
          <col className="w-48" />
        </colgroup>
        <thead className="bg-zinc-50 text-[11px] text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400">
          <tr>
            <th scope="col" className="px-2 py-1.5 font-medium">
              #
            </th>
            <th scope="col" className="px-2 py-1.5 font-medium">
              when
            </th>
            <th scope="col" className="px-2 py-1.5 font-medium">
              what happened
            </th>
            <th scope="col" className="px-2 py-1.5 font-medium">
              state
            </th>
            <th scope="col" className="px-2 py-1.5 font-medium">
              pinned to
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((entry) => (
            <JournalRow key={entry.seq} entry={entry} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * How to word a missing-or-broken spec.
 *
 * `spec_error` is `[<code>] <message>` (`SpecRejected`'s Display). The code
 * discriminates: only `spec-file-missing` means the file is gone. Everything
 * else — unreadable, not TOML, a rejected field — means the file is there and
 * cannot be used, which has a different fix. Saying "no spec file" for all of
 * them tells a reviewer a product was deleted when it is sitting right there.
 */
export function describeSpecTrouble(specError: string | null | undefined): string {
  if (specError === null || specError === undefined) return "no spec file";
  if (specError.startsWith("[spec-file-missing]")) return "no spec file";
  if (specError.startsWith("[spec-file-unreadable]")) return "spec file unreadable";
  return "spec file unusable";
}

function Standing({ status }: { status: ProductStatusOutput }) {
  return (
    <section aria-label="Where it stands" className="space-y-2">
      <div className="grid gap-2 sm:grid-cols-3">
        <StatusCard label="loop state" value={status.fulfill_state ?? "the loop has not run"} />
        <StatusCard
          label="working spec"
          value={
            status.spec_digest ? (
              <span title={status.spec_digest}>{shortId(status.spec_digest)}</span>
            ) : (
              "not present"
            )
          }
          tone={status.spec_error ? "risk" : "muted"}
          sub={status.spec_error ?? undefined}
        />
        <StatusCard
          label="approval"
          value={status.approval ? "recorded" : "none"}
          sub={
            status.spec_matches_approval === false
              ? "the working spec differs from the approved one"
              : undefined
          }
        />
      </div>
      {status.snapshot_intact === false && (
        // The strongest thing this screen can say, so it is said plainly and
        // not folded into a count of problems.
        <StatusCard
          label="approval snapshot"
          value="broken"
          tone="risk"
          sub="The approved snapshot's bytes no longer digest to the recorded value. The approval cannot be trusted to describe what was approved."
        />
      )}
      {status.artifact_problems !== undefined && status.artifact_problems.length > 0 && (
        <ul className="space-y-1 text-xs text-amber-700 dark:text-amber-400">
          {status.artifact_problems.map((problem) => (
            <li key={problem}>{problem}</li>
          ))}
        </ul>
      )}
    </section>
  );
}

/** One product: where it stands, and everything that has happened to it. */
export function ProductTimeline({
  name,
  loaders = defaultProductLoaders,
}: {
  name: string;
  loaders?: ProductLoaders;
}) {
  const loadStatus = useCallback(() => loaders.status(name), [loaders, name]);
  const loadJournal = useCallback(() => loaders.journal(name), [loaders, name]);
  const status = useResource(loadStatus, [loadStatus]);
  const journal = useResource(loadJournal, [loadJournal]);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="font-mono text-sm font-semibold text-zinc-900 dark:text-zinc-100">{name}</h2>
        <span className="text-xs text-zinc-600 dark:text-zinc-300">
          every decision about it: <CustodyLink subject={`product:${name}`} />
        </span>
      </div>

      {status.kind === "ready" ? (
        <Standing status={status.value} />
      ) : (
        <section aria-label="Where it stands" className="space-y-2">
          <ResourceState resource={status} loadingLine="reading the product…" />
        </section>
      )}

      <section aria-label="What happened" className="space-y-2">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">What happened</h3>
        {journal.kind !== "ready" ? (
          <ResourceState resource={journal} loadingLine="reading the journal…" />
        ) : journal.value.count === 0 ? (
          // Not a refusal and not a blank: the loop has simply never run.
          <p className="text-sm text-zinc-600 dark:text-zinc-300">
            The fulfillment loop has not run for this product, so its journal is empty.
          </p>
        ) : (
          <>
            <p className="text-xs text-zinc-600 dark:text-zinc-300">
              {journal.value.count} {journal.value.count === 1 ? "row" : "rows"}, oldest first, as
              the engine recorded them.
            </p>
            <JournalTable rows={journal.value.rows} />
          </>
        )}
      </section>
    </div>
  );
}

/** The products tab: the list, or one product's timeline. */
export function ProductsScreen({
  name,
  loaders = defaultProductLoaders,
}: {
  name: string | null;
  loaders?: ProductLoaders;
}) {
  if (name !== null) {
    return <ProductTimeline name={name} loaders={loaders} />;
  }
  return <ProductList load={loaders.list} />;
}
