import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import { coerceCell, toCsv } from "../webviews/resultGrid";

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

describe("toCsv", () => {
  it("joins a simple grid", () => {
    expect(toCsv(["id", "name"], [["1", "Alice"], ["2", "Bob"]])).toBe(
      "id,name\r\n1,Alice\r\n2,Bob",
    );
  });

  it("quotes cells containing commas, quotes, or newlines", () => {
    expect(toCsv(["a"], [['x,y']])).toBe('a\r\n"x,y"');
    expect(toCsv(["a"], [['say "hi"']])).toBe('a\r\n"say ""hi"""');
    expect(toCsv(["a"], [["line1\nline2"]])).toBe('a\r\n"line1\nline2"');
  });

  it("emits only the header when there are no rows", () => {
    expect(toCsv(["a", "b"], [])).toBe("a,b");
  });
});
