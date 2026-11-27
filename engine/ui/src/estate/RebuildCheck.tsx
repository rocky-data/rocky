import type { ReactNode } from "react";
import type { HistoryOutput } from "@rocky-types/history";
import type { RecipeHistoryOutput } from "@rocky-types/recipe_history";
import type { ReplayCheckInputOutput, ReplayCheckModelOutput, ReplayCheckOutput } from "@rocky-types/replay_check";
import { ApiError, apiGet } from "../api";
import { CommandBlock } from "../CommandBlock";
import { Clip, Missing, StatusCard, TABLE, runStatusLabel } from "../components";
import { formatInstant } from "../format";
import { CustodyLink } from "../governor/links";
import { type Resource, useResource } from "./useResource";

export type ReplayCheckLoader = (runId: string, model: string) => Promise<ReplayCheckOutput>;
export type RecipeHistoryLoader = (hash: string) => Promise<RecipeHistoryOutput>;

export const defaultReplayCheckLoader: ReplayCheckLoader = (runId, model) =>
  apiGet<ReplayCheckOutput>(`runs/${encodeURIComponent(runId)}/replay-check?model=${encodeURIComponent(model)}`);

export const defaultRecipeHistoryLoader: RecipeHistoryLoader = (hash) =>
  apiGet<RecipeHistoryOutput>(`recipes/${encodeURIComponent(hash)}/history`);

const NO_HASH =
  "no recipe hash was recorded for this model in this run; a failed execution, or a run from before hashes were kept, has none";
const NO_PROVENANCE =
  "this run recorded no provenance for the model; the engine's placeholder values are not shown as facts";

function isCode(error: ApiError, code: string): boolean {
  return error.status === 404 && error.envelope.code === code;
}

/** The newest run in the loaded list that executed `model`, or `null`. */
export function newestRunOf(history: HistoryOutput, model: string): string | null {
  const runs = history.runs
    .filter((run) => run.models.some((m) => m.model_name === model))
    .sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at));
  return runs.length > 0 ? runs[0].run_id : null;
}

/**
 * The recipe hash `model` recorded in run `runId`, from the runs list.
 *
 * A run can hold several executions of one model, and the replay check does
 * not say which one it classified. Take the last one in the record, which is
 * the one that ended the run; skip an execution that recorded no hash.
 */
export function recipeHashIn(history: HistoryOutput, runId: string, model: string): string | null {
  const run = history.runs.find((r) => r.run_id === runId);
  if (!run) return null;
  const hashes = run.models
    .filter((m) => m.model_name === model)
    .map((m) => m.recipe_identity?.recipe_hash)
    .filter((h): h is string => typeof h === "string" && h !== "");
  return hashes.length > 0 ? hashes[hashes.length - 1] : null;
}

/**
 * One model's rebuild check: can the recording of the newest run that
 * executed it rebuild the table. The run comes from the runs list, not from
 * `latest`, because `latest` is the newest run of anything and a run of
 * another model would blank this one. Nothing runs and nothing is written.
 */
export function RebuildCheck({
  name,
  runs,
  load = defaultReplayCheckLoader,
  loadHistory = defaultRecipeHistoryLoader,
  now,
}: {
  name: string;
  /** The runs list the screen already holds. */
  runs: Resource<HistoryOutput>;
  load?: ReplayCheckLoader;
  loadHistory?: RecipeHistoryLoader;
  now?: number;
}) {
  return (
    <section aria-label="Rebuild check" className="space-y-3 border-t border-line pt-4">
      <h4 className="text-sm font-semibold text-ink">Rebuild check</h4>
      <RunChoice name={name} runs={runs} load={load} loadHistory={loadHistory} now={now} />
    </section>
  );
}

function RunChoice({
  name,
  runs,
  load,
  loadHistory,
  now,
}: {
  name: string;
  runs: Resource<HistoryOutput>;
  load: ReplayCheckLoader;
  loadHistory: RecipeHistoryLoader;
  now?: number;
}) {
  switch (runs.kind) {
    case "loading":
      return <p className="text-xs text-muted">Reading the runs list…</p>;
    case "unreachable":
    case "refused":
      return (
        <p className="text-sm text-ink-2">
          <Missing reason="the runs list could not be read, so the run to check is not known" /> The runs list could not
          be read.
        </p>
      );
    case "ready": {
      const runId = newestRunOf(runs.value, name);
      if (runId === null) {
        return (
          <div className="space-y-1">
            <p className="text-sm text-ink-2">No run of this model among the 50 newest runs.</p>
            <p className="text-xs text-muted">An older run may hold it. Read it on the command line:</p>
            <CommandBlock command={`rocky history --model ${name}`} />
          </div>
        );
      }
      return (
        <RunCheck
          name={name}
          runId={runId}
          history={runs.value}
          load={load}
          loadHistory={loadHistory}
          now={now}
        />
      );
    }
  }
}

function RunCheck({
  name,
  runId,
  history,
  load,
  loadHistory,
  now,
}: {
  name: string;
  runId: string;
  history: HistoryOutput;
  load: ReplayCheckLoader;
  loadHistory: RecipeHistoryLoader;
  now?: number;
}) {
  const check = useResource(() => load(runId, name), [runId, name]);
  const recipeHash = recipeHashIn(history, runId, name);
  switch (check.kind) {
    case "loading":
      return <p className="text-xs text-muted">Checking {name}…</p>;
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={check.message} />;
    case "refused":
      if (isCode(check.error, "run_not_found")) {
        return <p className="text-sm text-ink-2">No run recorded yet. Run the project once; the check reads that recording.</p>;
      }
      if (isCode(check.error, "model_not_found")) {
        return <p className="text-sm text-ink-2">That run did not execute this model, so it has no recording.</p>;
      }
      return (
        <StatusCard
          label={`refused (${check.error.status})`}
          value={check.error.envelope.code}
          tone="risk"
          sub={check.error.envelope.remediation_hint ?? check.error.envelope.message}
        />
      );
    case "ready": {
      const model = check.value.models.find((m) => m.model_name === name);
      if (model === undefined) {
        return <p className="text-sm text-ink-2">That run did not execute this model, so it has no recording.</p>;
      }
      return (
        <Verdict
          name={name}
          output={check.value}
          model={model}
          loadHistory={loadHistory}
          recipeHash={recipeHash}
          now={now}
        />
      );
    }
  }
}

type VerdictTone = "ok" | "warn" | "fail" | "none";

const VERDICT_BOX: Record<VerdictTone, string> = {
  ok: "border-ok bg-ok-soft text-ok-ink",
  warn: "border-warn bg-warn-soft text-warn-ink",
  fail: "border-fail bg-fail-soft text-fail-ink",
  none: "border-line bg-surface-2 text-ink-2",
};

function Verdict({
  name,
  output,
  model,
  loadHistory,
  recipeHash,
  now,
}: {
  name: string;
  output: ReplayCheckOutput;
  model: ReplayCheckModelOutput;
  loadHistory: RecipeHistoryLoader;
  recipeHash: string | null;
  now?: number;
}) {
  // Without provenance the engine sends placeholders (ir_parseable false,
  // nondeterministic false, no inputs). They are not findings: show them as
  // not recorded. Provenance is written only when `[reuse] enabled`.
  const recorded = model.has_provenance;
  const readFailed = !recorded && model.reasons.some((r) => r.startsWith("provenance read failed"));
  const replayable = model.verdict === "replayable";
  const tone: VerdictTone = !recorded ? "none" : !replayable ? "fail" : model.nondeterministic ? "warn" : "ok";
  let headline: string;
  let why: ReactNode;
  if (!recorded) {
    headline = readFailed ? "Not read: the state store did not answer" : "Not recorded";
    why = readFailed ? (
      model.reasons.join("; ")
    ) : (
      <>
        This run recorded no provenance for this model. Rocky records it only for content-addressed models with{" "}
        <code>[reuse] enabled = true</code>.
        {model.reasons.length > 0 && <> The engine said: {model.reasons.join("; ")}</>}
      </>
    );
  } else if (replayable) {
    headline = "This table can be rebuilt from its recording";
    why = model.nondeterministic
      ? "The program and its inputs are recorded, but the SQL could not be shown deterministic (a volatile or unknown function), so a rebuild may differ."
      : "The program and its inputs are recorded, and nothing in the SQL is volatile.";
  } else {
    headline = "This table cannot be rebuilt exactly";
    why = model.reasons.length > 0 ? model.reasons.join("; ") : "The engine gave no reason.";
  }
  const inputsOk = model.inputs.filter((i) => i.resolvable).length;
  const historyCommand = `rocky history --recipe ${recipeHash ?? "<hash>"}`;
  const unrecorded = <Missing reason={readFailed ? "Not read: the state store did not answer" : NO_PROVENANCE} />;
  // The determinism scan reads the recorded program, so no parse, no answer.
  const noScan = !recorded || !model.ir_parseable;
  return (
    <div className="space-y-4">
      <div role="status" className={`rounded-md border px-4 py-3 text-sm ${VERDICT_BOX[tone]}`}>
        <div className="font-semibold">{headline}</div>
        <div className="mt-1">
          From run <CustodyLink subject={output.run_id} clip /> ({runStatusLabel(output.status)}). {why}
        </div>
      </div>

      <div>
        <h5 className="text-sm font-semibold text-ink">Why</h5>
        <p className="text-xs text-muted">Each check comes from the replay check of the newest run of this model.</p>
        <div className={`mt-1 ${TABLE.scroller}`}>
          <table className={TABLE.table} aria-label="Why">
            <thead className={TABLE.head}>
              <tr>
                {["Check", "Result", "Detail"].map((c) => (
                  <th key={c} scope="col" className={TABLE.th}>
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              <CheckRow
                label="Program recorded"
                ok={readFailed ? undefined : model.has_provenance}
                unevaluated={readFailed ? "Not read" : undefined}
                neutralNo
                detail={
                  model.proof_class ? (
                    `match strength: ${model.proof_class}`
                  ) : (
                    <Missing reason="no provenance record, so no match strength" />
                  )
                }
              />
              <CheckRow
                label="Program readable by this engine"
                ok={recorded ? model.ir_parseable : undefined}
                detail={
                  !recorded ? unrecorded : model.ir_parseable ? "the recorded program parses" : "the recorded program does not parse here"
                }
              />
              <CheckRow
                label="Inputs recorded"
                ok={!recorded ? undefined : model.inputs.length === 0 ? null : inputsOk === model.inputs.length}
                detail={
                  !recorded ? unrecorded : model.inputs.length === 0 ? "no declared inputs" : `${inputsOk} of ${model.inputs.length} resolve`
                }
              />
              <CheckRow
                label="Same result every time"
                ok={noScan ? undefined : !model.nondeterministic}
                detail={
                  !recorded ? (
                    unrecorded
                  ) : !model.ir_parseable ? (
                    <Missing reason="the recorded program did not parse, so its SQL was not scanned" />
                  ) : model.nondeterministic ? (
                    "the SQL could not be shown deterministic (a volatile or unknown function)"
                  ) : (
                    "no volatile construct found"
                  )
                }
                warnWhenNo
              />
            </tbody>
          </table>
        </div>
      </div>

      <div>
        <h5 className="text-sm font-semibold text-ink">Inputs</h5>
        {!recorded ? (
          <p className="text-sm text-ink-2">{unrecorded}</p>
        ) : model.inputs.length === 0 ? (
          <p className="text-sm text-ink-2">This model declares no upstream.</p>
        ) : (
          <div className={`mt-1 ${TABLE.scroller}`}>
            <table className={TABLE.table} aria-label="Inputs">
              <thead className={TABLE.head}>
                <tr>
                  {["Upstream", "Kind", "Status", "Note"].map((c) => (
                    <th key={c} scope="col" className={TABLE.th}>
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {model.inputs.map((input) => (
                  <InputRow key={input.upstream_key} input={input} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div>
        <h5 className="text-sm font-semibold text-ink">Runs of this exact program</h5>
        {recipeHash === null ? (
          <p className="text-sm text-ink-2">
            <Missing reason={NO_HASH} />
          </p>
        ) : (
          <RecipeRuns hash={recipeHash} load={loadHistory} now={now} />
        )}
      </div>

      <div className="space-y-1">
        <p className="text-xs text-muted">The same read on the command line:</p>
        <CommandBlock command={`rocky replay ${output.run_id} --check --model ${name}`} />
        <CommandBlock command={historyCommand} />
      </div>
    </div>
  );
}

function CheckRow({
  label,
  ok,
  detail,
  warnWhenNo = false,
  neutralNo = false,
  unevaluated = "the engine did not evaluate this check",
}: {
  label: string;
  /** `undefined` is a check the engine did not evaluate; `null` is a check with nothing to check. */
  ok: boolean | null | undefined;
  detail: ReactNode;
  warnWhenNo?: boolean;
  /** A "No" that is a plain fact, not a defect: shown grey. */
  neutralNo?: boolean;
  /** Why an `undefined` result is shown as a grey dash. */
  unevaluated?: string;
}) {
  const style =
    ok === undefined || ok === null || (neutralNo && !ok)
      ? "bg-surface-2 text-ink-2"
      : ok
        ? "bg-ok-soft text-ok-ink"
        : warnWhenNo
          ? "bg-warn-soft text-warn-ink"
          : "bg-fail-soft text-fail-ink";
  return (
    <tr className={TABLE.row}>
      <td className={TABLE.td}>{label}</td>
      <td className={TABLE.td}>
        {ok === undefined ? (
          <Missing reason={unevaluated} />
        ) : (
          <span className={`rounded-chip px-2 py-0.5 text-xs font-medium ${style}`}>
            {ok === null ? "Nothing to check" : ok ? "Yes" : "No"}
          </span>
        )}
      </td>
      <td className={`${TABLE.td} text-ink-2`}>{detail}</td>
    </tr>
  );
}

const KIND_WORDS: Record<string, string> = {
  content: "content hash",
  watermark: "freshness watermark",
};

function InputRow({ input }: { input: ReplayCheckInputOutput }) {
  return (
    <tr className={TABLE.row}>
      <td className={`${TABLE.td} font-mono`}>{input.upstream_key}</td>
      <td className={TABLE.td}>{KIND_WORDS[input.kind] ?? input.kind}</td>
      <td className={TABLE.td}>
        <span
          className={`rounded-chip px-2 py-0.5 text-xs font-medium ${input.resolvable ? "bg-ok-soft text-ok-ink" : "bg-fail-soft text-fail-ink"}`}
        >
          {input.resolvable ? "Resolves" : "Does not resolve"}
        </span>
      </td>
      <td className={`${TABLE.td} text-ink-2`}>
        {input.reason ? (
          input.reason
        ) : (
          <Missing reason={input.resolvable ? "the engine gave no note for an input that resolves" : "the engine gave no reason"} />
        )}
      </td>
    </tr>
  );
}

/** `GET /api/v1/recipes/{hash}/history`: every recorded execution of one exact program. */
export function RecipeRuns({ hash, load, now }: { hash: string; load: RecipeHistoryLoader; now?: number }) {
  const history = useResource(() => load(hash), [hash]);
  switch (history.kind) {
    case "loading":
      return <p className="text-xs text-muted">Loading runs…</p>;
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={history.message} />;
    case "refused":
      return (
        <StatusCard
          label={`refused (${history.error.status})`}
          value={history.error.envelope.code}
          tone="risk"
          sub={history.error.envelope.remediation_hint ?? history.error.envelope.message}
        />
      );
    case "ready": {
      const executions = history.value.executions;
      if (executions.length === 0) {
        return <p className="text-sm text-ink-2">The store holds no execution of this program.</p>;
      }
      return (
        <div className={TABLE.scroller}>
          <table className={TABLE.table} aria-label="Runs of this exact program">
            <thead className={TABLE.head}>
              <tr>
                {["When", "Run", "Model", "Input hash", "Status"].map((c) => (
                  <th key={c} scope="col" className={TABLE.th}>
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {executions.map((e, index) => {
                const inputHash = e.recipe_identity?.input_hash;
                return (
                  <tr key={`${e.run_id}-${e.model_name}-${index}`} className={TABLE.row}>
                    <td className={`${TABLE.td} text-ink-2`}>{formatInstant(e.started_at, now)}</td>
                    <td className={TABLE.td}>
                      <CustodyLink subject={e.run_id} clip />
                    </td>
                    <td className={`${TABLE.td} font-mono`}>{e.model_name}</td>
                    <td className={`${TABLE.td} font-mono`}>
                      {inputHash ? <Clip value={inputHash} keepEnds /> : <Missing reason="this execution observed no inputs" />}
                    </td>
                    <td className={TABLE.td}>{runStatusLabel(e.status)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      );
    }
  }
}
