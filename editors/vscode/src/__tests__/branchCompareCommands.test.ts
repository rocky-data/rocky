import { describe, expect, it, vi, beforeEach } from "vitest";

const { runRockyJsonWithProgress, showRockyError, showJsonInEditor, promptForInput } = vi.hoisted(() => ({
  runRockyJsonWithProgress: vi.fn(),
  showRockyError: vi.fn(),
  showJsonInEditor: vi.fn(),
  promptForInput: vi.fn(),
}));

vi.mock("vscode", () => ({ window: { showErrorMessage: vi.fn(), showWarningMessage: vi.fn(), showInformationMessage: vi.fn() } }));
vi.mock("../rockyCli", () => ({ runRockyJsonWithProgress, showRockyError }));
vi.mock("../views/previewView", () => ({ firePreviewCreated: vi.fn() }));
vi.mock("../commands/ui", () => ({
  ensureWorkspace: () => true,
  promptForInput,
  showJsonInEditor,
  confirmAction: vi.fn(),
  showSqlInEditor: vi.fn(),
}));

import { compare } from "../commands/run";
import { previewCreate } from "../commands/preview";
import { branchApprove } from "../commands/branch";
import { BRANCH_NAME_EXAMPLE, PREVIEW_NAME_EXAMPLE } from "../commands/branchNames";

describe("branch name suggestions", () => {
  it("constructs prompt examples accepted by the CLI rule", async () => {
    promptForInput.mockReset();
    promptForInput.mockResolvedValueOnce("main").mockResolvedValue(undefined);
    runRockyJsonWithProgress.mockRejectedValue(new Error("stop after prompts"));
    await previewCreate();
    await branchApprove();
    const examples = promptForInput.mock.calls
      .map(([, options]) => options?.placeHolder)
      .filter((placeholder) => typeof placeholder === "string" && placeholder.startsWith("e.g., "))
      .map((placeholder: string) => placeholder.slice(6));
    expect(examples).toContain(BRANCH_NAME_EXAMPLE);
    expect(examples).toContain(PREVIEW_NAME_EXAMPLE);
    for (const name of examples) {
      expect(name.length).toBeGreaterThan(0);
      expect(name.length).toBeLessThanOrEqual(64);
      expect(name).toMatch(/^[A-Za-z0-9_]+$/);
    }
  });
});

describe("compare command", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    promptForInput.mockResolvedValue("client=acme");
  });

  it("shows structured error rows from stdout while retaining failed status", async () => {
    const row = { production_table: "mart.orders", shadow_table: "branch__fix_price.orders", production_count: 1, shadow_count: null, row_count_diff_pct: null, verdict: "error", reasons: ["shadow table missing"] };
    const output = { command: "compare", overall_verdict: "fail", tables_failed: 1, results: [row] };
    const error = { kind: "exit", stderr: "1 table failed", stdout: JSON.stringify(output) };
    runRockyJsonWithProgress.mockRejectedValue(error);

    await compare();

    expect(showRockyError).toHaveBeenCalledWith("Compare failed", error);
    expect(showJsonInEditor).toHaveBeenCalledWith(JSON.stringify(output));
  });

  it("does not show non-compare stdout as a compare result", async () => {
    runRockyJsonWithProgress.mockRejectedValue({ kind: "exit", stdout: '{"command":"doctor","results":[],"tables_failed":1}' });
    await compare();
    expect(showRockyError).toHaveBeenCalledOnce();
    expect(showJsonInEditor).not.toHaveBeenCalled();
  });
});
