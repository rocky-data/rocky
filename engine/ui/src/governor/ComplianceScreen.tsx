import { useState } from "react";
import type {
  ColumnClassificationStatus,
  ComplianceException,
  ComplianceOutput,
  EnvMaskingStatus,
} from "@rocky-types/compliance";
import { apiGet } from "../api";
import { CommandBlock } from "../CommandBlock";
import { EmptyState, Missing, ScreenHeader, StatusCard, TABLE } from "../components";
import { useResource } from "../estate/useResource";

export type ComplianceLoader = () => Promise<ComplianceOutput>;

/** One read of the whole project. The env picker narrows it here, so it never re-reads. */
export const defaultComplianceLoader: ComplianceLoader = () => apiGet<ComplianceOutput>("compliance");

const ALL = "";
const GATE_COMMAND = "rocky compliance --fail-on exception";

/** How one `(model, column, env)` stands. */
export type MaskState = "masked" | "exception" | "on_purpose";

/**
 * `exception` is the engine's word: no strategy resolved and the tag is not on
 * `[classifications] allow_unmasked`. The gate fails on it. `on_purpose` is an
 * unmasked column the gate lets pass: the strategy is the explicit `none`, or no
 * strategy resolved and the tag is on the allow list.
 */
export function maskState(
  column: ColumnClassificationStatus,
  env: EnvMaskingStatus,
  exceptions: ComplianceException[],
): MaskState {
  if (env.enforced && env.masking_strategy !== "none") return "masked";
  const listed = exceptions.some((e) => e.model === column.model && e.column === column.column && e.env === env.env);
  return listed ? "exception" : "on_purpose";
}

export const CHIP: Record<MaskState, string> = {
  masked: "bg-ok-soft text-ok-ink",
  exception: "bg-fail-soft text-fail-ink",
  on_purpose: "bg-warn-soft text-warn-ink",
};

const CHIP_WORDS: Record<MaskState, string> = {
  masked: "masked",
  exception: "exception",
  on_purpose: "unmasked on purpose",
};

function plural(n: number, one: string, many: string): string {
  return `${n} ${n === 1 ? one : many}`;
}

/**
 * The compliance tab, from `GET /api/v1/compliance`: are the classified columns
 * masked wherever policy says they should be. A static read of `rocky.toml` and
 * the model sidecars. It calls no warehouse.
 */
export function ComplianceScreen({ load = defaultComplianceLoader }: { load?: ComplianceLoader }) {
  const compliance = useResource(load, [load]);
  return (
    <div className="space-y-4">
      <ScreenHeader
        title="Compliance"
        detail="Which columns hold sensitive data, and whether each environment masks them."
      />
      {compliance.kind === "loading" && <p className="text-sm text-muted">Loading compliance…</p>}
      {compliance.kind === "refused" && (
        <StatusCard
          label={`refused (${compliance.error.status})`}
          value={compliance.error.envelope.code}
          tone="risk"
          sub={compliance.error.envelope.remediation_hint ?? compliance.error.envelope.message}
        />
      )}
      {compliance.kind === "unreachable" && (
        <StatusCard label="engine" value="unreachable" tone="risk" sub={compliance.message} />
      )}
      {compliance.kind === "ready" && <ComplianceBody output={compliance.value} />}
    </div>
  );
}

function envsOf(output: ComplianceOutput): string[] {
  const seen: string[] = [];
  for (const column of output.per_column) {
    for (const env of column.envs) if (!seen.includes(env.env)) seen.push(env.env);
  }
  for (const exception of output.exceptions) if (!seen.includes(exception.env)) seen.push(exception.env);
  return seen;
}

function ComplianceBody({ output }: { output: ComplianceOutput }) {
  const [picked, setPicked] = useState(ALL);
  const envs = envsOf(output);
  const env = envs.includes(picked) ? picked : ALL;

  const rows = output.per_column
    .map((column) => ({ column, envs: column.envs.filter((e) => env === ALL || e.env === env) }))
    .filter((row) => row.envs.length > 0);
  const exceptions = output.exceptions.filter((e) => env === ALL || e.env === env);
  const states = rows.flatMap((row) => row.envs.map((e) => maskState(row.column, e, output.exceptions)));
  const onPurpose = states.filter((s) => s === "on_purpose").length;
  const maskedEverywhere = rows.filter((row) =>
    row.envs.every((e) => maskState(row.column, e, output.exceptions) === "masked"),
  ).length;
  const models = new Set(rows.map((row) => row.column.model)).size;

  const picker = envs.length > 0 && (
    <div className="flex items-center gap-2">
      <label className="text-sm text-ink-2" htmlFor="compliance-env">
        Environment
      </label>
      <select
        id="compliance-env"
        value={env}
        onChange={(event) => setPicked(event.target.value)}
        className="h-9 rounded-md border border-line bg-surface px-2 text-sm text-ink"
      >
        <option value={ALL}>All environments</option>
        {envs.map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>
    </div>
  );

  if (rows.length === 0 && exceptions.length === 0) {
    return (
      <div className="space-y-4">
        {picker}
        <EmptyState
          title="No classified columns"
          detail={
            <>
              No model sidecar carries a <code>[classification]</code> block, so there is nothing to mask.
            </>
          }
        />
        <Footer />
      </div>
    );
  }

  const scope = env === ALL ? "any environment" : `environment ${env}`;
  return (
    <div className="space-y-4">
      {picker}
      <p
        role="status"
        className={`rounded-md border px-4 py-3 text-sm ${
          exceptions.length > 0 ? "border-fail bg-fail-soft text-fail-ink" : "border-ok bg-ok-soft text-ok-ink"
        }`}
      >
        {exceptions.length > 0
          ? `${plural(exceptions.length, "classified column has", "classified columns have")} no masking strategy in ${scope}, and the tag is not on the allow list.`
          : `Every classified column is masked, or left unmasked on purpose, in ${scope}.`}
      </p>

      <section aria-label="Exceptions" className="space-y-2">
        <h3 className="text-base font-semibold text-ink">Exceptions</h3>
        <p className="text-sm text-muted">
          Unmasked, and the tag is not on the <code>allow_unmasked</code> list. The gate fails on these.
        </p>
        {exceptions.length === 0 ? (
          <p className="text-sm text-ink-2">None.</p>
        ) : (
          <div className={TABLE.scroller}>
            <table className={TABLE.table} aria-label="Exceptions">
              <thead className={TABLE.head}>
                <tr>
                  {["Model", "Column", "Env", "Reason"].map((c) => (
                    <th key={c} scope="col" className={TABLE.th}>
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {exceptions.map((e) => (
                  <tr key={`${e.model}.${e.column}.${e.env}`} className={TABLE.row}>
                    <td className={`${TABLE.td} font-mono`}>{e.model}</td>
                    <td className={`${TABLE.td} font-mono`}>{e.column}</td>
                    <td className={TABLE.td}>{e.env}</td>
                    <td className={`${TABLE.td} text-ink-2`}>{e.reason}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Stat label="Classified columns" value={rows.length} sub={`in ${plural(models, "model", "models")}`} />
        <Stat label="Masked everywhere" value={maskedEverywhere} sub={`of ${rows.length}, in ${scope}`} />
        <Stat
          label="Unmasked on purpose"
          value={onPurpose}
          sub="strategy none, or the tag is on the allow list"
          tone={onPurpose > 0 ? "warn" : "none"}
        />
        <Stat
          label="Exceptions"
          value={exceptions.length}
          sub={exceptions.length === 0 ? "nothing to fix" : "not masked, and no allow-list entry"}
          tone={exceptions.length > 0 ? "fail" : "ok"}
        />
      </div>

      <section aria-label="Classified columns" className="space-y-2">
        <h3 className="text-base font-semibold text-ink">Classified columns</h3>
        <p className="text-sm text-muted">
          Classification comes from model config; masking from the environment policy.
        </p>
        <div className={TABLE.scroller}>
          <table className={TABLE.table} aria-label="Classified columns">
            <thead className={TABLE.head}>
              <tr>
                {["Model", "Column", "Class", "Masking per environment"].map((c) => (
                  <th key={c} scope="col" className={TABLE.th}>
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map(({ column, envs: columnEnvs }) => (
                <tr key={`${column.model}.${column.column}`} className={TABLE.row}>
                  <td className={`${TABLE.td} font-mono`}>{column.model}</td>
                  <td className={`${TABLE.td} font-mono`}>{column.column}</td>
                  <td className={TABLE.td}>
                    {column.classification === "" ? (
                      <Missing reason="no classification tag was recorded" />
                    ) : (
                      column.classification
                    )}
                  </td>
                  <td className={TABLE.td}>
                    <ul className="flex flex-wrap gap-1.5" aria-label={`Masking of ${column.model}.${column.column}`}>
                      {columnEnvs.map((e) => {
                        const state = maskState(column, e, output.exceptions);
                        return (
                          <li
                            key={e.env}
                            data-state={state}
                            title={CHIP_WORDS[state]}
                            className={`rounded-chip px-2 py-0.5 text-xs font-medium ${CHIP[state]}`}
                          >
                            {e.env} · {e.masking_strategy}
                            <span className="sr-only"> ({CHIP_WORDS[state]})</span>
                          </li>
                        );
                      })}
                    </ul>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <Footer />
    </div>
  );
}

function Footer() {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted">In CI, this command fails the build on a new unmasked column:</p>
      <CommandBlock command={GATE_COMMAND} />
    </div>
  );
}

const STAT_BORDER = {
  none: "border-line",
  ok: "border-ok",
  warn: "border-warn",
  fail: "border-fail",
} as const;

const STAT_VALUE = {
  none: "text-ink",
  ok: "text-ok-ink",
  warn: "text-warn-ink",
  fail: "text-fail-ink",
} as const;

function Stat({
  label,
  value,
  sub,
  tone = "none",
}: {
  label: string;
  value: number;
  sub?: string;
  tone?: keyof typeof STAT_BORDER;
}) {
  return (
    <div data-tone={tone} className={`rounded-card border bg-surface p-4 ${STAT_BORDER[tone]}`}>
      <div className="text-xs text-muted">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${STAT_VALUE[tone]}`}>{value}</div>
      {sub && <div className="mt-1 text-xs text-muted">{sub}</div>}
    </div>
  );
}
