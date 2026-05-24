import { describe, expect, it, vi } from "vitest";

// branchesView extends vscode.TreeItem at module load and transitively imports
// getStartedView, which instantiates a module-scope EventEmitter — so the mock
// must provide both as constructors.
vi.mock("vscode", () => ({
  EventEmitter: class {
    event(): { dispose(): void } {
      return { dispose() {} };
    }
    fire(): void {}
  },
  TreeItem: class {
    constructor(public label?: string) {}
  },
  TreeItemCollapsibleState: { None: 0 },
  ThemeIcon: class {
    constructor(public id?: string) {}
  },
}));

import { branchDescription, branchTooltip } from "../views/branchesView";
import type { BranchEntry } from "../types/generated/branch_list";

const entry: BranchEntry = {
  name: "fix-price",
  created_by: "alice@example.com",
  created_at: "2026-05-24T10:00:00Z",
  schema_prefix: "branch__fix_price",
  description: "tweak the price rounding",
};

describe("branchDescription", () => {
  it("is the creator", () => {
    expect(branchDescription(entry)).toBe("alice@example.com");
  });
});

describe("branchTooltip", () => {
  it("includes name, schema prefix, creator, and description", () => {
    const t = branchTooltip(entry);
    expect(t).toContain("Branch: fix-price");
    expect(t).toContain("Schema prefix: branch__fix_price");
    expect(t).toContain("Created by: alice@example.com");
    expect(t).toContain("tweak the price rounding");
  });

  it("omits the description line when absent", () => {
    const t = branchTooltip({ ...entry, description: null });
    expect(t).not.toContain("tweak");
    expect(t).toContain("Branch: fix-price");
  });
});
