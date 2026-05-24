import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({
  window: {
    createOutputChannel: () => ({
      appendLine: () => {},
      show: () => {},
      dispose: () => {},
    }),
  },
  workspace: {
    getConfiguration: () => ({ get: (_k: string, d: unknown) => d }),
    workspaceFolders: undefined,
  },
  ProgressLocation: { Notification: 15 },
  ViewColumn: { Beside: 2 },
}));

import { RockyCliError } from "../rockyCli";
import {
  buildPreviewArgs,
  parseErrorEnvelope,
  toGridData,
} from "../commands/previewRowsCore";
import type { PreviewRowsOutput } from "../types/generated/preview_rows";

describe("buildPreviewArgs", () => {
  it("model only", () => {
    expect(buildPreviewArgs("stg_orders", undefined, 100, false)).toEqual([
      "preview",
      "rows",
      "--model",
      "stg_orders",
      "--limit",
      "100",
      "--output",
      "json",
    ]);
  });

  it("includes --cte only when provided", () => {
    const args = buildPreviewArgs("m", "my_cte", 5, false);
    expect(args).toContain("--cte");
    expect(args[args.indexOf("--cte") + 1]).toBe("my_cte");
  });

  it("appends --allow-warehouse only when allowed", () => {
    expect(buildPreviewArgs("m", undefined, 100, false)).not.toContain(
      "--allow-warehouse",
    );
    expect(buildPreviewArgs("m", undefined, 100, true)).toContain(
      "--allow-warehouse",
    );
  });
});

describe("parseErrorEnvelope", () => {
  const envelope = (stdout: string): RockyCliError =>
    new RockyCliError("failed", "stderr text", 1, undefined, stdout);

  it("parses the error_kind envelope from stdout", () => {
    const err = envelope(
      JSON.stringify({ error_kind: "warehouse_gated", adapter_kind: "databricks" }),
    );
    expect(parseErrorEnvelope(err)).toEqual({
      error_kind: "warehouse_gated",
      adapter_kind: "databricks",
    });
  });

  it("returns undefined when stdout is not JSON", () => {
    expect(parseErrorEnvelope(envelope("boom, not json"))).toBeUndefined();
  });

  it("returns undefined when JSON lacks error_kind", () => {
    expect(parseErrorEnvelope(envelope('{"rows": []}'))).toBeUndefined();
  });

  it("returns undefined for a non-RockyCliError", () => {
    expect(parseErrorEnvelope(new Error("nope"))).toBeUndefined();
  });
});

describe("toGridData", () => {
  it("maps PreviewRowsOutput into the grid payload", () => {
    const out: PreviewRowsOutput = {
      version: "1.43.0",
      command: "preview-rows",
      model: "accounts",
      cte: null,
      columns: ["id", "owner_email"],
      rows: [["1", "***hashed***"]],
      row_count: 1,
      limit_applied: 100,
      truncated: false,
      executed_sql: "SELECT id, sha2(owner_email, 256) AS owner_email FROM ...",
      adapter_kind: "duckdb",
      duration_ms: 12,
    };
    const grid = toGridData(out);
    expect(grid.columns).toEqual(["id", "owner_email"]);
    expect(grid.rows).toEqual([["1", "***hashed***"]]);
    expect(grid.meta).toMatchObject({
      rowCount: 1,
      truncated: false,
      adapterKind: "duckdb",
      model: "accounts",
      cte: null,
      executedSql: out.executed_sql,
    });
  });
});
