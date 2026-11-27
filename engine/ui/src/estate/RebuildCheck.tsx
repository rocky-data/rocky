import type { ReactNode } from "react";
import type { RecipeHistoryOutput } from "@rocky-types/recipe_history";
import type { ReplayCheckInputOutput, ReplayCheckModelOutput, ReplayCheckOutput } from "@rocky-types/replay_check";
import { ApiError, apiGet } from "../api";
import { CommandBlock } from "../CommandBlock";
import { Clip, Missing, StatusCard, TABLE, runStatusLabel } from "../components";
import { formatInstant } from "../format";
import { CustodyLink } from "../governor/links";
import { type Resource, useResource } from "./useResource";

export type ReplayCheckLoader = (model: string) => Promise<ReplayCheckOutput>;
export type RecipeHistoryLoader = (hash: string) => Promise<RecipeHistoryOutput>;

export const defaultReplayCheckLoader: ReplayCheckLoader = (model) =>
  apiGet<ReplayCheckOutput>(`runs/latest/replay-check?model=${encodeURIComponent(model)}`);

export const defaultRecipeHistoryLoader: RecipeHistoryLoader = (hash) =>
  apiGet<RecipeHistoryOutput>(`recipes/${encodeURIComponent(hash)}/history`);

const NO_HASH =
  "the replay check does not carry a recipe hash; read it from rocky history, then run rocky history --recipe <hash>";

function isCode(error: ApiError, code: string): boolean {
  return error.status === 404 && error.envelope.code === code;
}

/**
 * One model's rebuild check, from `GET /api/v1/runs/latest/replay-check?model=<name>`:
 * can the recording of the latest run alone rebuild this table. Nothing runs and
 * nothing is written. A run that was never recorded is a state, not an error.
 */
export function RebuildCheck({
  name,
  load = defaultReplayCheckLoader,
  loadHistory = defaultRecipeHistoryLoader,
  recipeHash = null,
  now,
}: {
  name: string;
  load?: ReplayCheckLoader;
  loadHistory?: RecipeHistoryLoader;
  /** The program's recipe hash, when a producer gives one. The replay check does not. */
  recipeHash?: string | null;
  now?: number;
}) {
  const check = useResource(() => load(name), [name]);
  return (
    <section aria-label="Rebuild check" className="space-y-3 border-t border-line pt-4">
      <h4 className="text-sm font-semibold text-ink">Rebuild check</h4>
      <CheckBody name={name} check={check} loadHistory={loadHistory} recipeHash={recipeHash} now={now} />
    </section>
  );
}

function CheckBody({
  name,
  check,
  loadHistory,
  recipeHash,
  now,
}: {
  name: string;
  check: Resource<ReplayCheckOutput>;
  loadHistory: RecipeHistoryLoader;
  recipeHash: string | null;
  now?: number;
}) {
  switch (check.kind) {
    case "loading":
      return <p className="text-xs text-muted">Checking {name}…</p>;
    case "unreachable":
      return <StatusCard label="engine" value="unreachable" tone="risk" sub={check.message} />;
    case "refused":
      if (isCode(check.error, "run_not_found")) {
        return (
          <p className="text-sm text-ink-2">
            No run recorded yet. Run the project once; the check reads that recording.
          </p>
        );
      }
      if (isCode(check.error, "model_not_found")) {
        return <p className="text-sm text-ink-2">The latest run did not execute this model, so it has no recording.</p>;
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
        return <p className="text-sm text-ink-2">The latest run did not execute this model, so it has no recording.</p>;
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

type VerdictTone = "ok" | "warn" | "fail";

const VERDICT_BOX: Record<VerdictTone, string> = {
  ok: "border-ok bg-ok-soft text-ok-ink",
  warn: "border-warn bg-warn-soft text-warn-ink",
  fail: "border-fail bg-fail-soft text-fail-ink",
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
  const replayable = model.verdict === "replayable";
  const tone: VerdictTone = !replayable ? "fail" : model.nondeterministic ? "warn" : "ok";
  const headline = replayable ? "This table can be rebuilt from its recording" : "This table cannot be rebuilt exactly";
  const why = replayable
    ? model.nondeterministic
      ? "The program and its inputs are recorded, but its SQL uses a volatile construct, so a rebuild may differ."
      : "The program and its inputs are recorded, and nothing in the SQL is volatile."
    : model.reasons.length > 0
      ? model.reasons.join("; ")
      : "The engine gave no reason.";
  const inputsOk = model.inputs.filter((i) => i.resolvable).length;
  const historyCommand = `rocky history --recipe ${recipeHash ?? "<hash>"}`;
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
        <p className="text-xs text-muted">Each check comes from the replay check of the latest run.</p>
        <div className={`mt-1 ${TABLE.scroller}`}>
          <table className={TABLE.table} aria-label="Why">
            <thead className={TABLE.head}>
              <tr>
                <th scope="col" className={TABLE.th}>
                  Check
                </th>
                <th scope="col" className={TABLE.th}>
                  Result
                </th>
                <th scope="col" className={TABLE.th}>
                  Detail
                </th>
              </tr>
            </thead>
            <tbody>
              <CheckRow
                label="Program recorded"
                ok={model.has_provenance}
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
                ok={model.ir_parseable}
                detail={model.ir_parseable ? "the recorded program parses" : "the recorded program does not parse here"}
              />
              <CheckRow
                label="Inputs recorded"
                ok={model.inputs.length === 0 ? null : inputsOk === model.inputs.length}
                detail={model.inputs.length === 0 ? "no declared inputs" : `${inputsOk} of ${model.inputs.length} resolve`}
              />
              <CheckRow
                label="Same result every time"
                ok={!model.nondeterministic}
                detail={model.nondeterministic ? "the SQL has a volatile construct such as now() or random()" : "no volatile construct found"}
                warnWhenNo
              />
            </tbody>
          </table>
        </div>
      </div>

      <div>
        <h5 className="text-sm font-semibold text-ink">Inputs</h5>
        {model.inputs.length === 0 ? (
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
        <CommandBlock command={`rocky replay latest --check --model ${name}`} />
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
}: {
  label: string;
  /** `null` is a check with nothing to check. */
  ok: boolean | null;
  detail: ReactNode;
  warnWhenNo?: boolean;
}) {
  const style =
    ok === null ? "bg-surface-2 text-ink-2" : ok ? "bg-ok-soft text-ok-ink" : warnWhenNo ? "bg-warn-soft text-warn-ink" : "bg-fail-soft text-fail-ink";
  return (
    <tr className={TABLE.row}>
      <td className={TABLE.td}>{label}</td>
      <td className={TABLE.td}>
        <span className={`rounded-chip px-2 py-0.5 text-xs font-medium ${style}`}>
          {ok === null ? "Nothing to check" : ok ? "Yes" : "No"}
        </span>
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
        {input.reason ? input.reason : input.resolvable ? <Missing reason="the engine gave no note for an input that resolves" /> : <Missing reason="the engine gave no reason" />}
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
