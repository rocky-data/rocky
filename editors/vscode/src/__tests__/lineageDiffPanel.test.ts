import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import {
  diffVerdict,
  formatBlastRadius,
  formatColumnChange,
} from "../webviews/lineageDiffPanel";

describe("diffVerdict", () => {
  it("clean when nothing changed", () => {
    expect(
      diffVerdict({ added: 0, modified: 0, removed: 0, unchanged: 5, total_models: 5 }),
    ).toBe("clean");
  });
  it("warn when columns are modified or removed (downstream impact)", () => {
    expect(
      diffVerdict({ added: 0, modified: 1, removed: 0, unchanged: 4, total_models: 5 }),
    ).toBe("warn");
    expect(
      diffVerdict({ added: 0, modified: 0, removed: 2, unchanged: 3, total_models: 5 }),
    ).toBe("warn");
  });
  it("ok when the only changes are additions", () => {
    expect(
      diffVerdict({ added: 3, modified: 0, removed: 0, unchanged: 2, total_models: 5 }),
    ).toBe("ok");
  });
});

describe("formatColumnChange", () => {
  it("shows a type transition when types are present", () => {
    expect(
      formatColumnChange({
        column_name: "email",
        change_type: "type_changed",
        old_type: "STRING",
        new_type: "INT",
      }),
    ).toBe("email: STRING → INT");
  });
  it("shows just the column name when there are no types", () => {
    expect(
      formatColumnChange({ column_name: "ssn", change_type: "removed" }),
    ).toBe("ssn");
  });
});

describe("formatBlastRadius", () => {
  it("joins downstream consumers as model.column", () => {
    expect(
      formatBlastRadius([
        { model: "dim_customers", column: "email" },
        { model: "fct_revenue", column: "email_hash" },
      ]),
    ).toBe("dim_customers.email, fct_revenue.email_hash");
  });
  it("is a dash when there are no consumers", () => {
    expect(formatBlastRadius(undefined)).toBe("—");
    expect(formatBlastRadius([])).toBe("—");
  });
});
