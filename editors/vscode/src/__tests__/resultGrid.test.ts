import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import { coerceCell } from "../webviews/resultGrid";

describe("coerceCell", () => {
  it("renders null/undefined as empty", () => {
    expect(coerceCell(null)).toBe("");
    expect(coerceCell(undefined)).toBe("");
  });

  it("passes strings through verbatim", () => {
    expect(coerceCell("owner1@test.org")).toBe("owner1@test.org");
    expect(coerceCell("")).toBe("");
  });

  it("stringifies numbers and booleans", () => {
    expect(coerceCell(42)).toBe("42");
    expect(coerceCell(0)).toBe("0");
    expect(coerceCell(true)).toBe("true");
  });

  it("renders objects and arrays as compact JSON", () => {
    expect(coerceCell({ a: 1, b: "x" })).toBe('{"a":1,"b":"x"}');
    expect(coerceCell([1, 2, 3])).toBe("[1,2,3]");
  });
});
