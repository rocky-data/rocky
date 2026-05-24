import { RockyCliError } from "../rockyCli";
import type { PreviewRowsOutput } from "../types/generated/preview_rows";
import type { GridData } from "../webviews/resultGrid";

/**
 * Pure, vscode-free helpers behind `rocky.previewModel` / `rocky.previewCte`.
 * Split out from `previewRows.ts` so they're unit-testable without dragging in
 * the editor (views, webview) import graph.
 */

/** Structured failure envelope the engine emits on stdout for preview errors. */
export interface PreviewErrorEnvelope {
  error_kind: string;
  message?: string;
  adapter_kind?: string;
  columns?: string[];
}

/**
 * Build the `rocky preview rows` argument vector. `--output json` is appended
 * so the extension always gets a parseable payload (or, on failure, a
 * parseable error envelope on stdout).
 */
export function buildPreviewArgs(
  model: string,
  cte: string | undefined,
  limit: number,
  allowWarehouse: boolean,
  sqlFile?: string,
): string[] {
  const args = ["preview", "rows", "--model", model];
  if (cte) args.push("--cte", cte);
  if (sqlFile) args.push("--sql-file", sqlFile);
  args.push("--limit", String(limit), "--output", "json");
  if (allowWarehouse) args.push("--allow-warehouse");
  return args;
}

/**
 * Parse the engine's structured error envelope out of a {@link RockyCliError}.
 * `stdout` is populated even on non-zero exit, so the `{"error_kind": …}`
 * payload survives. Returns `undefined` when the error isn't a recognised
 * envelope (spawn failure, malformed JSON, plain CLI error).
 */
export function parseErrorEnvelope(err: unknown): PreviewErrorEnvelope | undefined {
  if (!(err instanceof RockyCliError) || !err.stdout) return undefined;
  try {
    const obj: unknown = JSON.parse(err.stdout);
    if (
      obj !== null &&
      typeof obj === "object" &&
      typeof (obj as Record<string, unknown>).error_kind === "string"
    ) {
      return obj as PreviewErrorEnvelope;
    }
  } catch {
    // stdout wasn't the JSON error envelope.
  }
  return undefined;
}

/** Map a `PreviewRowsOutput` into the webview grid payload. */
export function toGridData(out: PreviewRowsOutput): GridData {
  return {
    columns: out.columns,
    rows: out.rows,
    meta: {
      rowCount: out.row_count,
      truncated: out.truncated,
      executedSql: out.executed_sql,
      adapterKind: out.adapter_kind,
      model: out.model,
      cte: out.cte ?? null,
    },
  };
}
