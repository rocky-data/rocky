import { useCallback } from "react";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import type { BreakingFinding, ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueEntry, ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ReviewStatusOutput } from "@rocky-types/review_status";
import { apiGet } from "../api";
import { Clip, StatusCard } from "../components";
import { type Resource, useResource } from "../estate/useResource";
import { formatInstant } from "../format";
import { CustodyLink } from "../governor/links";
import { ResourceState } from "./ResourceState";
import { SamplePanel } from "./SamplePanel";

export interface PlanLoaders {
  status: (planId: string) => Promise<ReviewStatusOutput>;
  diff: (planId: string) => Promise<ReviewOutput>;
  queue: () => Promise<ReviewQueueOutput>;
  product: (name: string) => Promise<ProductStatusOutput>;
}

export const defaultPlanLoaders: PlanLoaders = {
  status: (planId) => apiGet<ReviewStatusOutput>(`review/${encodeURIComponent(planId)}/status`),
  diff: (planId) => apiGet<ReviewOutput>(`review/${encodeURIComponent(planId)}`),
  queue: () => apiGet<ReviewQueueOutput>("review/queue"),
  product: (name) => apiGet<ProductStatusOutput>(`products/${encodeURIComponent(name)}`),
};

/** A `product:<name>` identity reduced to the name the products route takes. */
export function productNameFromId(productId: string): string {
  return productId.startsWith("product:") ? productId.slice("product:".length) : productId;
}

/**
 * What the queue says about one plan. Three answers, because the queue is a
 * resource and a resource has three ways to stand: it named the plan, it was
 * read and did not name it, or it could not be read. Only the middle one is
 * "not in the queue". The last is unknown, and unknown is never shown as
 * absent: a queue refused with `engine_busy` says nothing about whether the
 * plan is in it, and a screen that said "no longer in the queue" beside that
 * refusal was asserting what it could not know (#1815).
 *
 * A present plan carries EVERY row that names it. The apply-time gate records
 * one decision per touched model and the queue keeps one row per (plan,
 * model), so a plan over two models under two rules pends twice, with two
 * reasons. Keeping the first row showed one reason beside a command that
 * clears both (#1815).
 */
export type QueueLookup =
  | { kind: "unknown"; queue: Resource<ReviewQueueOutput> }
  | { kind: "absent" }
  | { kind: "present"; entries: ReviewQueueEntry[] };

export function lookupQueueEntry(
  queue: Resource<ReviewQueueOutput>,
  planId: string,
): QueueLookup {
  if (queue.kind !== "ready") return { kind: "unknown", queue };
  const entries = queue.value.pending.filter((row) => row.plan_id === planId);
  return entries.length === 0 ? { kind: "absent" } : { kind: "present", entries };
}

/** The distinct compiled models a plan's rows name, in the queue's order. */
export function modelsNamedBy(entries: ReviewQueueEntry[]): string[] {
  return Array.from(new Set(entries.flatMap((entry) => entry.models)));
}

/** One breaking finding as a sentence, from the tagged union the engine emits. */
export function describeFinding(finding: BreakingFinding): string {
  const change = finding.change as Record<string, unknown>;
  const kind = String(change.kind ?? "unknown");
  const model = String(change.model ?? "?");
  const column = change.column === undefined ? null : String(change.column);
  switch (kind) {
    case "model_removed":
      return `${model} is removed`;
    case "model_added":
      return `${model} is added`;
    case "column_dropped":
      return `${model}.${column} is dropped (was ${String(change.data_type)})`;
    case "column_added":
      return `${model}.${column} is added (${String(change.data_type)}${
        change.nullable === false ? ", not null" : ""
      })`;
    case "column_type_changed":
      return `${model}.${column} changes type, ${String(change.old_type)} to ${String(
        change.new_type,
      )}${change.narrowing === true ? " (narrowing)" : ""}`;
    case "column_nullability_changed":
      return `${model}.${column} becomes ${change.new_nullable === true ? "nullable" : "not null"}`;
    default:
      return `${model}${column === null ? "" : `.${column}`}: ${kind.replace(/_/g, " ")}`;
  }
}

function BreakingChanges({ diff }: { diff: ReviewOutput }) {
  // Absent and empty mean different things, and the difference is the whole
  // point of the panel: absent is "the gate did not run", empty is "the gate
  // ran and found nothing".
  if (diff.breaking_changes === undefined || diff.breaking_changes === null) {
    return (
      <StatusCard
        label="what it would break"
        value="the gate was skipped"
        tone="risk"
        sub={
          diff.message ??
          `The compile against ${diff.base_ref} did not complete, so nothing was compared. This is not the same as "nothing breaks".`
        }
      />
    );
  }
  if (diff.breaking_changes.length === 0) {
    return (
      <StatusCard
        label="what it would break"
        value="nothing"
        sub={`Compared against ${diff.base_ref}.`}
      />
    );
  }
  return (
    <section aria-label="What it would break" className="space-y-2">
      <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        What it would break
      </h3>
      <ul className="space-y-1">
        {diff.breaking_changes.map((finding, index) => (
          // Findings carry no id; the list is replaced wholesale on each read.
          <li key={index} className="flex items-baseline gap-2 text-sm">
            <span
              className={`rounded px-1 text-[10px] uppercase ${
                finding.severity === "breaking"
                  ? "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-100"
                  : "bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-100"
              }`}
            >
              {finding.severity}
            </span>
            <span className="text-zinc-800 dark:text-zinc-200">{describeFinding(finding)}</span>
          </li>
        ))}
      </ul>
      <p className="text-[11px] text-zinc-500 dark:text-zinc-400">
        Compared against {diff.base_ref}.
      </p>
    </section>
  );
}

function SpecDrift({
  status,
  product,
}: {
  status: ReviewStatusOutput;
  product: ProductStatusOutput;
}) {
  const planned = status.spec_digest ?? null;
  const current = product.spec_digest ?? product.committed_spec_digest ?? null;
  if (planned === null || current === null) {
    return (
      <StatusCard
        label="the spec it was planned against"
        value="not recorded"
        sub="One of the two digests is missing, so the screen cannot say whether the spec moved."
      />
    );
  }
  if (planned === current) {
    return (
      <StatusCard
        label="the spec it was planned against"
        value="unchanged"
        sub={
          <>
            Still <Clip value={planned} />.
          </>
        }
      />
    );
  }
  return (
    <StatusCard
      label="the spec it was planned against"
      value="the spec moved"
      tone="risk"
      sub={
        <>
          Planned against <Clip value={planned} />; the product is now <Clip value={current} />.
          Applying this plan would be refused, because apply checks the digest.
        </>
      }
    />
  );
}

function Escalation({ lookup, planId }: { lookup: QueueLookup; planId: string }) {
  // "The queue does not name this plan" and "the queue could not be read" are
  // different facts, and only the first is safe to state. Collapsing them told
  // a reader that an escalation had been resolved when the server had in fact
  // refused the request.
  if (lookup.kind === "unknown") {
    return (
      <section aria-label="Why it needs a human" className="space-y-2">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          Why it needs a human
        </h3>
        <ResourceState resource={lookup.queue} loadingLine="reading the review queue…" />
      </section>
    );
  }
  if (lookup.kind === "absent") {
    return (
      <StatusCard
        label="why it needs a human"
        value="not in the queue"
        sub="No outstanding escalation names this plan. It may already be approved, or policy may never have escalated it. The custody chain has the whole story."
      />
    );
  }
  const { entries } = lookup;
  return (
    <section aria-label="Why it needs a human" className="space-y-3">
      <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
        Why it needs a human
      </h3>
      {entries.length > 1 && (
        <p className="text-xs text-zinc-600 dark:text-zinc-300">
          {entries.length} escalations name this plan, one per model policy stopped. Approving
          the plan clears all of them.
        </p>
      )}
      {entries.map((entry) => (
        <div key={entry.decision_ref} className="space-y-2">
          <p className="text-sm text-zinc-800 dark:text-zinc-200">{entry.reason}</p>
          <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-zinc-600 sm:grid-cols-5 dark:text-zinc-300">
            <div>
              <dt className="text-zinc-500 dark:text-zinc-400">model</dt>
              <dd className="font-mono break-all">{entry.model}</dd>
            </div>
            <div>
              <dt className="text-zinc-500 dark:text-zinc-400">capability</dt>
              <dd>{entry.capability}</dd>
            </div>
            <div>
              <dt className="text-zinc-500 dark:text-zinc-400">principal</dt>
              <dd>{entry.principal}</dd>
            </div>
            <div>
              <dt className="text-zinc-500 dark:text-zinc-400">rule</dt>
              <dd>
                {entry.rule_id === undefined || entry.rule_id === null
                  ? "the default effect"
                  : `#${entry.rule_id}`}
              </dd>
            </div>
            <div>
              <dt className="text-zinc-500 dark:text-zinc-400">blast radius</dt>
              <dd>{entry.blast_radius ?? "not computed"}</dd>
            </div>
          </dl>
        </div>
      ))}
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Every decision recorded about this plan: <CustodyLink subject={planId} />
      </p>
    </section>
  );
}

function HowToApprove({
  status,
  entries,
}: {
  status: ReviewStatusOutput;
  entries: ReviewQueueEntry[];
}) {
  if (status.reviewed) {
    return (
      <StatusCard
        label="approval"
        value="signed off"
        sub={`${status.approver?.name ?? "someone"} on ${formatInstant(
          status.reviewed_at ?? null,
        )}. ${status.breaking_change_count ?? 0} breaking finding(s) were signed off.`}
      />
    );
  }
  // Every row of one plan carries the same command: approval is per plan.
  const command = entries[0]?.approve_command ?? `rocky review ${status.plan_id} --approve`;
  return (
    <section aria-label="How to approve" className="space-y-2">
      <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">How to approve</h3>
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Approving happens in a terminal, on purpose: the marker records a git identity, and this
        page holds a read-only token. Copy the command.
        {entries.length > 1 ? ` It clears every one of the ${entries.length} escalations above.` : ""}
      </p>
      <pre className="overflow-x-auto rounded bg-zinc-50 p-2 font-mono text-xs dark:bg-zinc-800">
        {command}
      </pre>
    </section>
  );
}

/**
 * Why there is no sample panel, said only as far as the payloads support.
 *
 * Absent is not empty. A missing panel reads as "this plan touches no data";
 * this says instead which fact is missing, and it is a different fact each
 * time: the queue named several models, or none; the product could not be
 * read; the queue could not be read. Only when the queue was READ and does
 * not name the plan, and no product names a model, does the screen say the
 * plan has left the queue — that is the one case it knows (#1815).
 */
function SampleFallback({
  lookup,
  productId,
  product,
}: {
  lookup: QueueLookup;
  productId: string | null;
  product: Resource<ProductStatusOutput>;
}) {
  const pending = (resource: Resource<unknown>, loadingLine: string) => (
    <section aria-label="Sample rows" className="space-y-2">
      <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Sample rows</h3>
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        Which model to sample is not known yet.
      </p>
      <ResourceState resource={resource} loadingLine={loadingLine} />
    </section>
  );
  if (lookup.kind === "unknown") {
    return pending(lookup.queue, "reading the review queue…");
  }
  if (productId !== null && product.kind !== "ready") {
    return pending(product, "reading the product…");
  }
  if (lookup.kind === "present") {
    const { entries } = lookup;
    const named = modelsNamedBy(entries);
    const subjects = entries.map((entry) => `${entry.capability} "${entry.model}"`).join(", ");
    return (
      <StatusCard
        label="sample rows"
        value="no single model to sample"
        sub={
          named.length === 0
            ? // The engine could not vouch for a compiled model behind the
              // row: its subject is not one (a replication target, a label),
              // the model is gone, or the compile could not name it. The
              // screen does not guess which.
              `The queue names no compiled model for this plan (${subjects}), so there is nothing to read rows from. Sample from the estate screen instead.`
            : `This plan touches ${named.length} models: ${named.join(", ")}. Sample each from the estate screen instead.`
        }
      />
    );
  }
  return (
    <StatusCard
      label="sample rows"
      value="no single model to sample"
      sub={
        productId !== null
          ? "The plan is no longer in the review queue and its product names no output model, so there is nothing to read rows from."
          : "Neither the review queue nor a product names a model for this plan, so there is nothing to read rows from. A plan that is not product-bound and no longer in the queue has no model on this screen."
      }
    />
  );
}

/**
 * One plan, read-only: what it is, what it would break, why policy stopped it,
 * whether the spec moved under it, a sample of the data, and the command that
 * would approve it.
 *
 * There is no control here that changes anything. The engine holds that too —
 * the UI token is read-only — but the screen is built as if it did not,
 * because a control that cannot be pressed is worse than one that is absent.
 */
export function PlanDetail({
  planId,
  loaders = defaultPlanLoaders,
}: {
  planId: string;
  loaders?: PlanLoaders;
}) {
  const loadStatus = useCallback(() => loaders.status(planId), [loaders, planId]);
  const loadDiff = useCallback(() => loaders.diff(planId), [loaders, planId]);
  const status = useResource(loadStatus, [loadStatus]);
  const diff = useResource(loadDiff, [loadDiff]);
  const queue = useResource(loaders.queue, [loaders]);

  const productId = status.kind === "ready" ? (status.value.product_id ?? null) : null;
  const loadProduct = useCallback(
    () =>
      productId === null
        ? Promise.reject(new Error("not product-bound"))
        : loaders.product(productNameFromId(productId)),
    [loaders, productId],
  );
  const product = useResource(loadProduct, [loadProduct]);

  if (status.kind !== "ready") {
    return (
      <section aria-label="The plan" className="space-y-3">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          Plan <Clip value={planId} />
        </h2>
        <ResourceState resource={status} loadingLine="reading the plan…" />
      </section>
    );
  }

  const lookup = lookupQueueEntry(queue, planId);
  const entries = lookup.kind === "present" ? lookup.entries : [];
  // Which model to sample. Each row's `models` is the set of compiled models
  // the engine vouches for — the real names, kept apart from `model`, which
  // is display text ("backfill: 3 model(s)", a replication target's table
  // name) and is never parsed here. A regex on it once decided a label was a
  // name, and `backfill_3_models` would have sampled a real model of that
  // name (#1815). The panel takes the plan's rows together, and only when
  // they name exactly one model.
  //
  // The queue is not a durable source: an approval marker resolves the
  // escalation, so the entry disappears the moment the plan is signed off —
  // and that is exactly when the table it built starts existing. The
  // product's own status carries `output_model`, and this screen already
  // reads it for the spec-drift card, so the fallback costs no request.
  const named = modelsNamedBy(entries);
  const fromQueue = named.length === 1 ? named[0] : null;
  const fromProduct =
    product.kind === "ready" ? (product.value.output_model ?? null) : null;
  const model = fromQueue ?? fromProduct;

  return (
    <div className="space-y-4">
      <section aria-label="The plan" className="space-y-2">
        <h2 className="font-mono text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          <Clip value={planId} />
        </h2>
        <div className="grid gap-2 sm:grid-cols-3">
          <StatusCard label="kind" value={status.value.kind} />
          <StatusCard
            label="review"
            value={status.value.reviewed ? "signed off" : "awaiting a human"}
            tone={status.value.reviewed ? "muted" : "risk"}
          />
          <StatusCard
            label="product"
            value={status.value.product_id ?? "not product-bound"}
          />
        </div>
      </section>

      {diff.kind === "ready" ? (
        <BreakingChanges diff={diff.value} />
      ) : (
        <section aria-label="What it would break" className="space-y-2">
          <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
            What it would break
          </h3>
          <ResourceState resource={diff} loadingLine="compiling both sides…" />
        </section>
      )}

      <Escalation lookup={lookup} planId={planId} />

      {productId !== null &&
        (product.kind === "ready" ? (
          <SpecDrift status={status.value} product={product.value} />
        ) : (
          <section aria-label="The spec it was planned against" className="space-y-2">
            <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
              The spec it was planned against
            </h3>
            <ResourceState resource={product} loadingLine="reading the product…" />
          </section>
        ))}

      {model !== null ? (
        <SamplePanel model={model} />
      ) : (
        <SampleFallback lookup={lookup} productId={productId} product={product} />
      )}

      <HowToApprove status={status.value} entries={entries} />
    </div>
  );
}
