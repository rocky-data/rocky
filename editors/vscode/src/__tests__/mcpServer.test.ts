import { describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// vscode mock — must precede the import that touches vscode. buildRockyMcpSpecs
// is pure (path only), but mcpServer.ts + its config.ts import reference the
// vscode module at load time, so a minimal stub is enough to resolve them.
// ---------------------------------------------------------------------------

vi.mock("vscode", () => ({
  workspace: { workspaceFolders: undefined },
  lm: {},
  window: {},
}));

import { buildRockyMcpSpecs } from "../mcpServer";

describe("buildRockyMcpSpecs", () => {
  it("returns no specs when there are no Rocky project roots", () => {
    expect(buildRockyMcpSpecs("rocky", [])).toEqual([]);
  });

  it("launches `rocky mcp --config <root>/rocky.toml` for a single root", () => {
    const specs = buildRockyMcpSpecs("rocky", ["/work/proj"]);
    expect(specs).toHaveLength(1);
    expect(specs[0]).toEqual({
      label: "Rocky",
      command: "rocky",
      args: ["mcp", "--config", "/work/proj/rocky.toml"],
      cwd: "/work/proj",
    });
  });

  it("honours a custom binary path from rocky.server.path", () => {
    const specs = buildRockyMcpSpecs("/opt/bin/rocky", ["/work/proj"]);
    expect(specs[0].command).toBe("/opt/bin/rocky");
    expect(specs[0].args).toEqual(["mcp", "--config", "/work/proj/rocky.toml"]);
  });

  it("disambiguates labels by folder name across multiple roots", () => {
    const specs = buildRockyMcpSpecs("rocky", ["/work/a", "/work/b"]);
    expect(specs.map((s) => s.label)).toEqual(["Rocky (a)", "Rocky (b)"]);
    // Each still targets its own config + cwd.
    expect(specs[0].args).toEqual(["mcp", "--config", "/work/a/rocky.toml"]);
    expect(specs[1].cwd).toBe("/work/b");
  });
});
