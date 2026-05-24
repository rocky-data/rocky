import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({
  window: {
    createOutputChannel: () => ({ appendLine: () => {}, show: () => {}, dispose: () => {} }),
  },
  workspace: {
    getConfiguration: () => ({ get: (_k: string, d: unknown) => d }),
    workspaceFolders: undefined,
  },
  ProgressLocation: { Notification: 15 },
  ViewColumn: { Beside: 2 },
}));

import { planIdsFromFilenames } from "../commands/review";
import {
  canApply,
  formatBreakingChange,
  reviewVerdict,
} from "../webviews/reviewPanel";
import type { ReviewOutput } from "../types/generated/review";

const base = (over: Partial<ReviewOutput>): ReviewOutput => ({
  approved: false,
  base_ref: "HEAD",
  command: "review",
  marker_written: false,
  plan_id: "abc123",
  version: "1.43.0",
  breaking_changes: [],
  ...over,
});

describe("planIdsFromFilenames", () => {
  it("keeps plan json files and drops review markers + non-json", () => {
    expect(
      planIdsFromFilenames([
        "abc.json",
        "abc.reviewed.json",
        "def.json",
        "notes.txt",
      ]),
    ).toEqual(["abc", "def"]);
  });
});

describe("reviewVerdict", () => {
  it("approved once the marker is written", () => {
    expect(reviewVerdict(base({ approved: true }))).toBe("approved");
  });
  it("blocked when unapproved with breaking changes", () => {
    expect(
      reviewVerdict(
        base({
          approved: false,
          breaking_changes: [
            { change: { kind: "model_removed", model: "m" }, severity: "breaking" },
          ],
        }),
      ),
    ).toBe("blocked");
  });
  it("clean when unapproved with no breaking changes", () => {
    expect(reviewVerdict(base({ breaking_changes: [] }))).toBe("clean");
    expect(reviewVerdict(base({ breaking_changes: null }))).toBe("clean");
  });
});

describe("canApply", () => {
  it("is true only when approved", () => {
    expect(canApply(base({ approved: true }))).toBe(true);
    expect(canApply(base({ approved: false }))).toBe(false);
  });
});

describe("formatBreakingChange", () => {
  it("describes each change kind", () => {
    expect(formatBreakingChange({ kind: "model_removed", model: "stg" })).toBe(
      "model removed: stg",
    );
    expect(
      formatBreakingChange({
        kind: "column_dropped",
        model: "dim",
        column: "email",
        data_type: "STRING",
      }),
    ).toBe("column dropped: dim.email (STRING)");
    expect(
      formatBreakingChange({
        kind: "column_type_changed",
        model: "fct",
        column: "amt",
        old_type: "INT",
        new_type: "BIGINT",
        narrowing: false,
      }),
    ).toBe("type changed: fct.amt INT → BIGINT");
  });
});
