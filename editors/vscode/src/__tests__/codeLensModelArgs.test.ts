import { describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// VS Code mock — codeLens.ts only touches `vscode.EventEmitter` at module load
// (a provider class field). The arg-builders under test are pure.
// ---------------------------------------------------------------------------
vi.mock("vscode", () => {
  class EventEmitter {
    event = (): { dispose(): void } => ({ dispose: (): void => undefined });
    fire(): void {}
    dispose(): void {}
  }
  return { EventEmitter };
});

vi.mock("../config", () => ({
  getConfig: (): { serverPath: string } => ({ serverPath: "rocky" }),
}));

import { buildCompileModelArgs, buildRunModelArgs } from "../codeLens";

// A model file name an attacker could plant in a cloned repo, packed with
// shell metacharacters (PowerShell `$(...)`, backtick, quotes, `;`, `~`). The
// security invariant is that this string travels as ONE literal argv element to
// ProcessExecution, where no shell ever parses it — not concatenated into a
// command string the way the old `Terminal.sendText` path did.
const MALICIOUS = "$(calc); rm -rf ~ '\"`evil";

describe("codeLens model task argv", () => {
  it("run: model name is a single literal argv element", () => {
    expect(buildRunModelArgs(MALICIOUS)).toEqual([
      "run",
      "--filter",
      `name=${MALICIOUS}`,
      "--output",
      "json",
    ]);
  });

  it("compile: model name is a single literal argv element", () => {
    expect(buildCompileModelArgs(MALICIOUS)).toEqual([
      "compile",
      "--model",
      MALICIOUS,
      "--output",
      "json",
    ]);
  });

  it("never emits a joined shell string for the model name", () => {
    // No argv element is the model name spliced together with the flags — the
    // dangerous payload is always isolated in its own slot.
    for (const args of [
      buildRunModelArgs(MALICIOUS),
      buildCompileModelArgs(MALICIOUS),
    ]) {
      expect(args.some((a) => a.includes(" --output "))).toBe(false);
      expect(args).toContain("json");
    }
  });
});
