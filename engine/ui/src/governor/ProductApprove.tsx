import { useState } from "react";
import type { JobStatus } from "@rocky-types/job_status";
import type { ProductStatusOutput } from "@rocky-types/product_status";
import {
  ConfirmedWrite,
  JobLine,
  defaultJobClient,
  jobBusy,
  useJob,
  type JobClient,
  type JobView,
} from "../operator";

/** What a succeeded `product approve` recorded, read from `result`. */
export interface ProductApproveSummary {
  specDigest: string;
  approver: string;
  alreadyApproved: boolean;
}

/**
 * Read the three fields the page says something about. The result is the
 * command's own JSON; nothing else in it is shown.
 */
export function productApproveSummary(job: JobStatus): ProductApproveSummary | null {
  const result = (job.result ?? {}) as Record<string, unknown>;
  if (typeof result.spec_digest !== "string" || result.spec_digest === "") return null;
  return {
    specDigest: result.spec_digest,
    approver: typeof result.approver === "string" ? result.approver : "",
    alreadyApproved: result.already_approved === true,
  };
}

/**
 * Whether the page offers to approve this product's spec: the spec reads, and
 * it is either not approved yet or differs from the approved one.
 */
export function approvableSpec(status: ProductStatusOutput): string | null {
  if (!status.spec_present) return null;
  if (status.spec_error !== null && status.spec_error !== undefined && status.spec_error !== "") return null;
  const digest = status.spec_digest;
  if (typeof digest !== "string" || digest === "") return null;
  const approved = status.approval !== null && status.approval !== undefined;
  return !approved || status.spec_matches_approval === false ? digest : null;
}

function succeededSummary(view: JobView): ProductApproveSummary | null {
  return view.kind === "done" && view.job.state === "succeeded" ? productApproveSummary(view.job) : null;
}

function ApproveResult({ summary, shown }: { summary: ProductApproveSummary; shown: string | null }) {
  // The command approves the spec as it is on disk when it runs. If that is
  // not the digest the person confirmed, say so rather than "done".
  const moved = shown !== null && summary.specDigest !== shown;
  return (
    <div className="space-y-1">
      <p
        role={moved ? "alert" : undefined}
        className={
          moved
            ? "text-xs font-medium text-amber-800 dark:text-amber-300"
            : "text-xs text-emerald-700 dark:text-emerald-300"
        }
      >
        {moved
          ? `Approve: the spec changed after this page read it. The approval covers ${summary.specDigest}, not ${shown}. Check the spec.`
          : summary.alreadyApproved
            ? `Approve: ${summary.specDigest} was already approved. Nothing was written.`
            : `Approve: approved ${summary.specDigest}.`}
      </p>
      {summary.approver !== "" && !summary.alreadyApproved && (
        <p className="break-words text-xs text-zinc-600 dark:text-zinc-300">
          Recorded approver: {summary.approver}. The label names the channel and the server&apos;s git
          identity. It does not prove who was at the page.
        </p>
      )}
    </div>
  );
}

/**
 * Approve a product's working spec from the browser: `rocky product approve
 * <name> --expect-digest <digest>` on this server. The page offers it when the
 * spec is not approved or has changed since the approval, and asks first,
 * naming the product and the spec digest it read.
 *
 * The digest the person confirmed is sent with the job, and the command
 * refuses, writing nothing, when the spec on disk has a different digest. The
 * result is still compared with the confirmed digest, as a second check. The recorded
 * approver is the server's git identity with the channel, `<email>
 * (http_api)`; the server refuses the approval when it has no usable
 * identity. The engine also refuses a new revision while fulfillment work is
 * in flight, and the job line shows that refusal.
 */
export function ProductApprove({
  name,
  status,
  jobs = defaultJobClient,
  onDone,
}: {
  name: string;
  status: ProductStatusOutput;
  jobs?: JobClient;
  onDone?: () => void;
}) {
  const approve = useJob("product_approve", jobs, onDone);
  // The digest the person confirmed, for the comparison with the result.
  const [confirmed, setConfirmed] = useState<string | null>(null);
  const digest = approvableSpec(status);
  const summary = succeededSummary(approve.view);
  if (digest === null && approve.view.kind === "idle") return null;
  const replaces =
    status.approval !== null && status.approval !== undefined ? status.approval.spec_digest : null;
  const output = status.output_model ?? null;
  return (
    <section aria-label="Approve the spec" className="space-y-2">
      <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Approve the spec</h3>
      {digest !== null && (
        <>
          <p className="text-xs text-zinc-600 dark:text-zinc-300">
            {replaces === null
              ? "The working spec has no approval yet."
              : "The working spec differs from the approved one."}{" "}
            The fulfillment loop acts only on an approved spec.
          </p>
          <ConfirmedWrite
            label="Approve spec"
            question={`Approve the working spec of product ${name} (digest ${digest}${
              output === null ? "" : `, output model ${output}`
            })?${
              replaces === null ? "" : ` This replaces the approval of ${replaces}.`
            } Only this exact digest is approved: if the spec changed since the page read it, the approval is refused and nothing is written. It records this server's git identity with the channel http_api. The engine also refuses it while fulfillment work is in flight.`}
            confirmLabel="Yes, approve this spec"
            busy={jobBusy(approve.view)}
            onConfirm={() => {
              setConfirmed(digest);
              // The digest the person confirmed rides along. The command
              // refuses, writing nothing, if the spec on disk now differs.
              approve.start({ name, expect_spec_digest: digest });
            }}
          />
        </>
      )}
      {summary === null ? (
        <JobLine label="Approve" view={approve.view} cancel={approve.cancel} />
      ) : (
        <ApproveResult summary={summary} shown={confirmed} />
      )}
    </section>
  );
}
