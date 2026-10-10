import { useCallback, useState } from "react";
import { CheckIcon, ClipboardDocumentIcon } from "@heroicons/react/20/solid";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import type { BreakingFinding, ReviewOutput } from "@rocky-types/review";
import type { ReviewQueueEntry, ReviewQueueOutput } from "@rocky-types/review_queue";
import type { ApproverIdentity, ReviewStatusOutput } from "@rocky-types/review_status";
import { apiGet } from "../api";
import { Clip, StatusCard, ToneDot, type Tone } from "../components";
import { type Resource, useResource } from "../estate/useResource";
import { formatInstant } from "../format";
import { CustodyLink } from "../governor/links";
import {
  JobLine,
  WriteButton,
  defaultJobClient,
  jobBusy,
  useJob,
  useWriteAccess,
  type JobClient,
} from "../operator";
import { navigateTo } from "../router";
import { reviewPath } from "./paths";
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

/**
 * The distinct models the samples route would read for a plan's rows. The
 * engine decides this per row under the route's own admission rules
 * (`preview_model`); a graph key in `models` is for ranking and audit and is
 * not a licence to read — a dotted name, a model a restore plan recorded
 * that is gone from the project, a model that no longer compiles are all
 * keys the route refuses (#1815).
 */
export function previewTargetsOf(entries: ReviewQueueEntry[]): string[] {
  return Array.from(
    new Set(
      entries.flatMap((entry) =>
        entry.preview_model === null || entry.preview_model === undefined
          ? []
          : [entry.preview_model],
      ),
    ),
  );
}

function recordedType(value: unknown): string | null {
  if (value === undefined || value === null || value === "?") return null;
  return String(value);
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
    case "column_dropped": {
      const dataType = recordedType(change.data_type);
      return dataType === null
        ? `${model}.${column} is dropped (type not recorded)`
        : `${model}.${column} is dropped (was ${dataType})`;
    }
    case "column_added":
      return `${model}.${column} is added (${recordedType(change.data_type) ?? "type not recorded"}${
        change.nullable === false ? ", not null" : ""
      })`;
    case "column_type_changed":
      return `${model}.${column} changes type, ${recordedType(change.old_type) ?? "type not recorded"} to ${
        recordedType(change.new_type) ?? "type not recorded"
      }${change.narrowing === true ? " (narrowing)" : ""}`;
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
    <section aria-label="What it would break" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-3">
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
        What it would break
      </h3>
      <ul className="divide-y divide-zinc-100 dark:divide-zinc-800">
        {diff.breaking_changes.map((finding, index) => (
          // Findings carry no id; the list is replaced wholesale on each read.
          <li key={index} className="flex items-baseline gap-3 py-2 text-sm first:pt-0 last:pb-0">
            <span
              className={`shrink-0 rounded-md px-1.5 py-0.5 text-xs font-semibold first-letter:uppercase ${
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
      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        Compared against {diff.base_ref}.
      </p>
    </section>
  );
}

function ConditionalDrops({ diff }: { diff: ReviewOutput }) {
  if (diff.conditional_drops.length === 0) return null;
  return (
    <section aria-label="Conditional DROPs" className="space-y-2 rounded-lg border border-red-300 bg-white p-5 dark:border-red-900 dark:bg-zinc-900">
      <h3 className="text-base font-semibold text-red-800 dark:text-red-200">
        Conditional DROPs
      </h3>
      <p className="text-xs text-zinc-600 dark:text-zinc-300">
        These run only when the existing object has the named kind.
      </p>
      <ul className="space-y-2">
        {diff.conditional_drops.map((drop, index) => (
          <li key={index} className="rounded border border-red-200 p-2 text-sm dark:border-red-900">
            <span className="font-semibold">{drop.model}</span>: existing {drop.existing_kind} at {drop.target}
            <code className="block break-all font-mono text-xs">{drop.drop_sql}</code>
          </li>
        ))}
      </ul>
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
          Apply compares the digest you pass with the one this plan carries, not with the spec
          on disk. Passing the current digest refuses this plan; passing the digest it was
          planned against applies it, stale. Re-propose against the current spec.
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
      <section aria-label="Why it needs a human" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-2">
        <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
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
    <section aria-label="Why it needs a human" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-3">
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
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
          <dl className="grid grid-cols-2 gap-x-4 gap-y-3 border-t border-zinc-100 pt-3 text-sm text-zinc-700 sm:grid-cols-5 dark:border-zinc-800 dark:text-zinc-300 [&_dd]:break-words [&>div]:min-w-0">
            <div>
              <dt className="text-xs text-zinc-500 dark:text-zinc-400">model</dt>
              <dd className="font-mono break-all">{entry.model}</dd>
            </div>
            <div>
              <dt className="text-xs text-zinc-500 dark:text-zinc-400">capability</dt>
              <dd>{entry.capability}</dd>
            </div>
            <div>
              <dt className="text-xs text-zinc-500 dark:text-zinc-400">principal</dt>
              <dd>{entry.principal}</dd>
            </div>
            <div>
              <dt className="text-xs text-zinc-500 dark:text-zinc-400">rule</dt>
              <dd>
                {entry.rule_id === undefined || entry.rule_id === null
                  ? "the default effect"
                  : `#${entry.rule_id}`}
              </dd>
            </div>
            <div>
              <dt className="text-xs text-zinc-500 dark:text-zinc-400">blast radius</dt>
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

/**
 * Who approved, through which channel, said only as far as the marker says.
 * `http_api` names the channel (the server's job API), not the person, and
 * never "the browser": anything holding the token can call that route.
 */
export function approverLine(approver: ApproverIdentity | null | undefined): string {
  if (approver === null || approver === undefined) return "approved; the marker names no approver";
  const who = approver.email;
  switch (approver.source) {
    case "http_api":
      return `approved over the HTTP API by ${who}`;
    case "local":
      return `approved locally by ${who}`;
    case "ci_oidc":
      return `approved from CI (OIDC) by ${who}`;
    case "pat":
      return `approved with a personal access token by ${who}`;
  }
}

/** The terminal command a product-bound apply needs. The digest is the reader's. */
function productApplyCommand(planId: string): string {
  return `rocky apply ${planId} --expect-spec-digest <the spec digest you approved>`;
}

/**
 * Approve and Apply, or the reason they are not available.
 *
 * In operator mode the buttons submit `POST /api/v1/jobs/approve` and
 * `/jobs/apply` and follow the job. The terminal command stays visible as a
 * secondary hint. A read-only page draws the buttons disabled with the
 * reason. A plan bound to a data product has no Apply button at all: its
 * apply needs the spec digest the reader approved, and the page must never
 * read that digest back from the plan it is checking.
 */
function Approval({
  status,
  entries,
  jobs,
  onChanged,
}: {
  status: ReviewStatusOutput;
  entries: ReviewQueueEntry[];
  jobs: JobClient;
  onChanged: () => void;
}) {
  const access = useWriteAccess();
  const approve = useJob("approve", jobs, onChanged);
  const apply = useJob("apply", jobs, onChanged);
  const planId = status.plan_id;
  const productBound = status.product_id !== null && status.product_id !== undefined;
  // Every row of one plan carries the same command: approval is per plan.
  const command = entries[0]?.approve_command ?? `rocky review ${planId} --approve`;

  const applied = apply.view.kind === "done" && apply.view.job.state === "succeeded";

  return (
    <section
      aria-label="Approval"
      // Sticky only as tall as the window, and scrolls inside past that, so
      // the Apply note and the command never sit stuck below the fold.
      className="space-y-4 rounded-lg border border-zinc-300 bg-white p-5 lg:sticky lg:top-16 lg:max-h-[calc(100vh-5rem)] lg:overflow-y-auto dark:border-zinc-700 dark:bg-zinc-900"
    >
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">Approval</h3>
      <Steps
        steps={[
          { label: "Proposed", state: "done" },
          { label: "Approved", state: status.reviewed ? "done" : "current" },
          {
            label: "Applied",
            // The status route records approval, not apply. Applied is
            // known only when this page ran the apply and saw it succeed.
            // After approval it is unknown, not "next": a plan applied in a
            // terminal, or before a reload, looks exactly the same here.
            state: applied ? "done" : status.reviewed ? "unknown" : "later",
            // A product-bound plan is applied in a terminal, never here.
            note: productBound
              ? "Apply this plan in a terminal."
              : applied || !status.reviewed
                ? undefined
                : "The plan status does not record apply.",
          },
        ]}
      />
      {status.reviewed ? (
        <StatusCard
          label="approval"
          value="signed off"
          tone="ok"
          sub={`${approverLine(status.approver)} on ${formatInstant(
            status.reviewed_at ?? null,
          )}. ${status.breaking_change_count ?? 0} breaking finding(s) were signed off.`}
        />
      ) : (
        <div className="space-y-2">
          <WriteButton
            label="Approve"
            primary
            busy={jobBusy(approve.view)}
            onClick={() => approve.start({ plan_id: planId })}
          />
          <JobLine label="Approve" view={approve.view} cancel={approve.cancel} />
          {access.kind === "operator" && (
            <p className="text-xs text-zinc-600 dark:text-zinc-300">
              Approving records this server's git identity as the approver, over the HTTP API.
            </p>
          )}
          {entries.length > 1 && (
            <p className="text-xs text-zinc-600 dark:text-zinc-300">
              Approving clears every one of the {entries.length} escalations above.
            </p>
          )}
        </div>
      )}

      {productBound ? (
        <StatusCard
          label="apply"
          value="apply in a terminal"
          sub={
            <>
              This plan is bound to a data product. Its apply must carry the spec digest you
              approved, and that digest must come from you, not from the plan. Run{" "}
              <code className="break-all">{productApplyCommand(planId)}</code>.
            </>
          }
        />
      ) : (
        <div className="space-y-2">
          <WriteButton
            label="Apply"
            primary={status.reviewed}
            busy={jobBusy(apply.view)}
            disabledReason={status.reviewed ? undefined : "Approve the plan first."}
            onClick={() => apply.start({ plan_id: planId })}
          />
          <JobLine label="Apply" view={apply.view} cancel={apply.cancel} />
          {status.reviewed && (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              Apply runs the models on disk, and first checks they still match this plan. If
              you edited them after the plan, apply refuses (plan_models_changed): plan and
              approve again.
            </p>
          )}
        </div>
      )}

      {!status.reviewed && (
        <div className="space-y-1 border-t border-zinc-200 pt-4 dark:border-zinc-800">
          <p className="text-xs text-zinc-500 dark:text-zinc-400">Or approve in a terminal:</p>
          <CommandLine command={command} />
        </div>
      )}
    </section>
  );
}

type StepState = "done" | "current" | "later" | "unknown";

/**
 * Where the plan stands, as three steps. A list, so a screen reader reads
 * the order; each step says its state in words, not by colour alone.
 */
function Steps({ steps }: { steps: { label: string; state: StepState; note?: string }[] }) {
  const said: Record<StepState, string> = {
    done: "done",
    current: "next",
    later: "not yet",
    unknown: "not known",
  };
  return (
    <ol aria-label="Steps" className="space-y-3">
      {steps.map((step, index) => (
        <li key={step.label} className="flex items-start gap-3 text-sm">
          <span
            aria-hidden="true"
            className={`flex size-6 shrink-0 items-center justify-center rounded-full text-xs font-bold ${
              step.state === "done"
                ? "bg-emerald-500 text-white dark:text-zinc-950"
                : step.state === "current"
                  ? "border-2 border-orange-500 text-orange-600 dark:text-orange-400"
                  : step.state === "unknown"
                    ? "border-2 border-dashed border-zinc-400 text-zinc-500 dark:border-zinc-500 dark:text-zinc-400"
                    : "border-2 border-zinc-300 text-zinc-400 dark:border-zinc-700 dark:text-zinc-500"
            }`}
          >
            {step.state === "done" ? <CheckIcon className="size-4" /> : index + 1}
          </span>
          <span className="pt-0.5">
            <span
              className={
                step.state === "later" || step.state === "unknown"
                  ? "text-zinc-500 dark:text-zinc-400"
                  : "font-medium text-zinc-900 dark:text-zinc-100"
              }
            >
              {step.label}
            </span>
            <span className="sr-only">: {said[step.state]}</span>
            {step.note !== undefined && (
              <span className="block text-xs text-zinc-500 dark:text-zinc-400">{step.note}</span>
            )}
          </span>
        </li>
      ))}
    </ol>
  );
}

/** A terminal command with a copy button. The command stays on the page as text. */
function CommandLine({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);
  const canCopy = typeof navigator !== "undefined" && navigator.clipboard !== undefined;
  return (
    <div className="flex items-start gap-2 rounded-md bg-zinc-50 p-2 dark:bg-zinc-950">
      <pre className="min-w-0 flex-1 py-1 pl-1 font-mono text-xs break-all whitespace-pre-wrap text-zinc-800 dark:text-zinc-200">
        {command}
      </pre>
      {canCopy && (
        <button
          type="button"
          onClick={() => {
            navigator.clipboard.writeText(command).then(
              () => setCopied(true),
              () => setCopied(false),
            );
          }}
          className="inline-flex size-8 shrink-0 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-200 hover:text-zinc-900 focus-visible:outline-2 focus-visible:outline-orange-500 dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-100"
        >
          {copied ? (
            <CheckIcon aria-hidden="true" className="size-4" />
          ) : (
            <ClipboardDocumentIcon aria-hidden="true" className="size-4" />
          )}
          <span className="sr-only">{copied ? "Copied" : "Copy the command"}</span>
        </button>
      )}
    </div>
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
    <section aria-label="Sample rows" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-2">
      <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">Sample rows</h3>
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
    let sub: string;
    if (named.length === 0) {
      // The engine could not vouch for a compiled model behind the row: its
      // subject is not one (a replication target, a label), the model is
      // gone, or the compile could not name it. The screen does not guess
      // which.
      sub = `The queue names no compiled model for this plan (${subjects}), so there is nothing to read rows from. Sample from the estate screen instead.`;
    } else if (named.length > 1) {
      sub = `This plan touches ${named.length} models: ${named.join(", ")}. Sample each from the estate screen instead.`;
    } else {
      // One model named, and no preview target offered for it: the samples
      // route would refuse it. The engine applied the route's own rules;
      // the screen repeats them rather than guessing which one bit.
      sub = `The samples route would read none of the models this plan names (${named.join(", ")}): it reads one model at a time, in the current project, with no compile errors and not time-interval. Sample from the estate screen instead.`;
    }
    return <StatusCard label="sample rows" value="no single model to sample" sub={sub} />;
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
 * One plan: what it is, what it would break, why policy stopped it, whether
 * the spec moved under it, a sample of the data, and its approval.
 *
 * In operator mode the approval section can approve and apply; on a
 * read-only page the same buttons are disabled with the reason. The engine
 * enforces that too: a read-only session gets `403` on any job route.
 */
export function PlanDetail({
  planId,
  loaders = defaultPlanLoaders,
  jobs = defaultJobClient,
}: {
  planId: string;
  loaders?: PlanLoaders;
  /** Where Approve and Apply submit their jobs. Tests hand in a fake. */
  jobs?: JobClient;
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
  // the engine says the samples route would read exactly one model for them
  // (`preview_model`, decided under that route's own admission rules).
  //
  // The queue is not a durable source: an approval marker resolves the
  // escalation, so the entry disappears the moment the plan is signed off —
  // and that is exactly when the table it built starts existing. The
  // product's own status carries `output_model`, and this screen already
  // reads it for the spec-drift card, so the fallback costs no request.
  const targets = previewTargetsOf(entries);
  const fromQueue = targets.length === 1 ? targets[0] : null;
  // The product's output model stands in only once the plan has LEFT the
  // queue (approved, its table now real). While the plan is in the queue the
  // engine has already said which model, if any, the samples route would
  // read, and a null there is an answer — a product fallback beside it
  // offered a read the route refuses (#1815, review round three).
  const fromProduct =
    lookup.kind === "absent" && product.kind === "ready"
      ? (product.value.output_model ?? null)
      : null;
  const model = fromQueue ?? fromProduct;

  return (
    <div className="space-y-6">
      <section aria-label="The plan" className="space-y-3">
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          <a
            href={reviewPath()}
            onClick={(event) => {
              event.preventDefault();
              navigateTo(reviewPath());
            }}
            className="text-sky-700 hover:underline dark:text-sky-400"
          >
            Review queue
          </a>{" "}
          <span aria-hidden="true">/</span> Plan
        </p>
        <h2 className="font-mono text-xl font-semibold text-zinc-900 dark:text-white">
          <Clip value={planId} />
        </h2>
        <dl className="flex flex-wrap gap-2 text-sm">
          <Fact label="kind" value={status.value.kind} />
          <Fact
            label="review"
            tone={status.value.reviewed ? "ok" : "warn"}
            value={status.value.reviewed ? "signed off" : "awaiting a human"}
          />
          <Fact label="product" value={status.value.product_id ?? "not product-bound"} />
        </dl>
      </section>

      {/* `minmax(0, 1fr)` below lg too: an unsized track grows to its widest
          child, and the terminal command then pushed the page sideways. */}
      <div className="grid grid-cols-[minmax(0,1fr)] items-start gap-6 lg:grid-cols-[minmax(0,1fr)_22rem]">
        <div className="min-w-0 space-y-4">
          {diff.kind === "ready" ? (
            <>
              <BreakingChanges diff={diff.value} />
              <ConditionalDrops diff={diff.value} />
            </>
          ) : (
            <section aria-label="What it would break" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-2">
              <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
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
              <section aria-label="The spec it was planned against" className="rounded-lg border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-900 space-y-2">
                <h3 className="text-base font-semibold text-zinc-900 dark:text-zinc-100">
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
        </div>

        {(status.value.reviewed || diff.kind === "ready") && (
          <Approval
            status={status.value}
            entries={entries}
            jobs={jobs}
            onChanged={() => {
              status.reload();
              queue.reload();
            }}
          />
        )}
      </div>
    </div>
  );
}

/** One fact about the plan, as a pill: its name, then its value. */
function Fact({ label, value, tone }: { label: string; value: string; tone?: Tone }) {
  return (
    <div className="inline-flex max-w-full items-center gap-2 rounded-full border border-zinc-200 bg-white px-3 py-1 dark:border-zinc-800 dark:bg-zinc-900">
      <dt className="text-zinc-500 dark:text-zinc-400">{label}</dt>
      <dd className="flex min-w-0 items-center gap-1.5 font-medium break-all text-zinc-900 dark:text-zinc-100">
        {tone !== undefined && <ToneDot tone={tone} />}
        {value}
      </dd>
    </div>
  );
}
