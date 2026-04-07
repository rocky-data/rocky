import { describe, expect, it, vi } from "vitest";

// ── Minimal vscode mock ─────────────────────────────────────────────────
vi.mock("vscode", () => ({
  FoldingRange: class {
    constructor(
      public start: number,
      public end: number,
      public kind?: number,
    ) {}
  },
  FoldingRangeKind: { Comment: 1, Imports: 2, Region: 3 },
  languages: {
    registerFoldingRangeProvider: vi.fn(),
  },
}));

import { RockyFoldingRangeProvider } from "../foldingProvider";

/** Build a fake TextDocument from a raw string. */
function fakeDoc(text: string) {
  const lines = text.split("\n");
  return {
    lineCount: lines.length,
    lineAt(i: number) {
      return { text: lines[i] };
    },
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } as any;
}

describe("RockyFoldingRangeProvider", () => {
  const provider = new RockyFoldingRangeProvider();

  it("folds a multi-line group block", () => {
    const doc = fakeDoc(
      [
        "from raw_orders",
        "group customer_id {",
        "    total: sum(amount),",
        "    cnt: count()",
        "}",
        "sort total desc",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    // The brace block from line 1 (group) to line 4 (})
    expect(ranges).toContainEqual(
      expect.objectContaining({ start: 1, end: 4, kind: 3 }),
    );
  });

  it("folds nested blocks (derive inside group)", () => {
    const doc = fakeDoc(
      [
        "from raw",
        "derive {",
        "    x: match status {",
        '        "a" => 1,',
        "        _ => 0",
        "    }",
        "}",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    // Outer derive: 1..6, inner match: 2..5
    const braceRanges = ranges.filter((r: { kind: number }) => r.kind === 3);
    expect(braceRanges).toHaveLength(2);
    expect(braceRanges).toContainEqual(
      expect.objectContaining({ start: 1, end: 6 }),
    );
    expect(braceRanges).toContainEqual(
      expect.objectContaining({ start: 2, end: 5 }),
    );
  });

  it("folds consecutive comment lines", () => {
    const doc = fakeDoc(
      [
        "-- This is a model",
        "-- that calculates revenue",
        "-- per customer",
        "from raw_orders",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    const commentRanges = ranges.filter((r: { kind: number }) => r.kind === 1);
    expect(commentRanges).toHaveLength(1);
    expect(commentRanges[0]).toMatchObject({ start: 0, end: 2 });
  });

  it("does not fold a single comment line", () => {
    const doc = fakeDoc(["-- single comment", "from raw_orders"].join("\n"));
    const ranges = provider.provideFoldingRanges(doc);
    const commentRanges = ranges.filter((r: { kind: number }) => r.kind === 1);
    expect(commentRanges).toHaveLength(0);
  });

  it("folds select block", () => {
    const doc = fakeDoc(
      [
        "from raw",
        "select {",
        "    order_id,",
        "    customer_id,",
        "    amount",
        "}",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    expect(ranges).toContainEqual(
      expect.objectContaining({ start: 1, end: 5, kind: 3 }),
    );
  });

  it("returns empty for a flat pipeline with no blocks", () => {
    const doc = fakeDoc(
      [
        "from raw_orders",
        "where status != \"cancelled\"",
        "sort amount desc",
        "take 10",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    expect(ranges).toHaveLength(0);
  });

  it("ignores braces inside string literals", () => {
    const doc = fakeDoc(
      [
        "from raw",
        "where name == \"{foo}\"",
        "take 5",
      ].join("\n"),
    );
    const ranges = provider.provideFoldingRanges(doc);
    const braceRanges = ranges.filter((r: { kind: number }) => r.kind === 3);
    expect(braceRanges).toHaveLength(0);
  });
});
