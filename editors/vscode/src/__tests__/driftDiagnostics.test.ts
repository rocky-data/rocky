import { beforeEach, describe, expect, it, vi } from "vitest";

// --- vscode mock ---
const mockDiagnosticCollection = {
  set: vi.fn(),
  delete: vi.fn(),
  dispose: vi.fn(),
};

const mockSubscriptions: { push: ReturnType<typeof vi.fn> } = {
  push: vi.fn(),
};

vi.mock("vscode", () => {
  const DiagnosticSeverity = { Error: 0, Warning: 1, Information: 2, Hint: 3 };
  return {
    languages: {
      createDiagnosticCollection: vi.fn(() => mockDiagnosticCollection),
    },
    workspace: {
      onDidOpenTextDocument: vi.fn(() => ({ dispose: vi.fn() })),
      onDidSaveTextDocument: vi.fn(() => ({ dispose: vi.fn() })),
      onDidCloseTextDocument: vi.fn(() => ({ dispose: vi.fn() })),
      textDocuments: [],
      getConfiguration: () => ({
        get: <T,>(key: string, fallback: T): T => {
          if (key === "server.path") return "rocky" as unknown as T;
          if (key === "server.extraArgs") return [] as unknown as T;
          if (key === "inlayHints.enabled") return true as unknown as T;
          return fallback;
        },
      }),
      workspaceFolders: undefined,
    },
    window: {
      createOutputChannel: () => ({
        appendLine: vi.fn(),
        show: vi.fn(),
        dispose: vi.fn(),
      }),
      showErrorMessage: vi.fn(() => Promise.resolve(undefined)),
    },
    DiagnosticSeverity,
    Diagnostic: class {
      range: unknown;
      message: string;
      severity: number;
      code?: string;
      source?: string;
      relatedInformation?: unknown[];
      constructor(range: unknown, message: string, severity: number) {
        this.range = range;
        this.message = message;
        this.severity = severity;
      }
    },
    DiagnosticRelatedInformation: class {
      location: unknown;
      message: string;
      constructor(location: unknown, message: string) {
        this.location = location;
        this.message = message;
      }
    },
    Range: class {
      start: { line: number; character: number };
      end: { line: number; character: number };
      constructor(sl: number, sc: number, el: number, ec: number) {
        this.start = { line: sl, character: sc };
        this.end = { line: el, character: ec };
      }
    },
    Position: class {
      line: number;
      character: number;
      constructor(line: number, character: number) {
        this.line = line;
        this.character = character;
      }
    },
    Location: class {
      uri: unknown;
      range: unknown;
      constructor(uri: unknown, rangeOrPosition: unknown) {
        this.uri = uri;
        this.range = rangeOrPosition;
      }
    },
    Uri: {
      file: (p: string) => ({ fsPath: p, toString: () => `file://${p}` }),
    },
  };
});

vi.mock("../rockyCli", () => ({
  runRockyJson: vi.fn(),
  RockyCliError: class extends Error {
    stderr: string;
    exitCode: number | string | null;
    constructor(message: string, stderr: string, exitCode: number | string | null) {
      super(message);
      this.name = "RockyCliError";
      this.stderr = stderr;
      this.exitCode = exitCode;
    }
  },
}));

vi.mock("../output", () => ({
  getOutputChannel: () => ({
    appendLine: vi.fn(),
    show: vi.fn(),
    dispose: vi.fn(),
  }),
}));

import * as vscode from "vscode";
import { runRockyJson } from "../rockyCli";
import { registerDriftDiagnostics } from "../driftDiagnostics";
import type { CompileOutput } from "../types/generated/compile";

const runRockyJsonMock = runRockyJson as unknown as ReturnType<typeof vi.fn>;

function makeContext(): vscode.ExtensionContext {
  const subscriptions: vscode.Disposable[] = [];
  return { subscriptions } as unknown as vscode.ExtensionContext;
}

describe("registerDriftDiagnostics", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockDiagnosticCollection.set.mockReset();
    mockDiagnosticCollection.delete.mockReset();
  });

  it("creates a diagnostic collection named 'rocky-drift'", () => {
    const ctx = makeContext();
    registerDriftDiagnostics(ctx);
    expect(vscode.languages.createDiagnosticCollection).toHaveBeenCalledWith(
      "rocky-drift",
    );
  });

  it("registers open, save, and close listeners", () => {
    const ctx = makeContext();
    registerDriftDiagnostics(ctx);
    expect(vscode.workspace.onDidOpenTextDocument).toHaveBeenCalled();
    expect(vscode.workspace.onDidSaveTextDocument).toHaveBeenCalled();
    expect(vscode.workspace.onDidCloseTextDocument).toHaveBeenCalled();
  });

  it("pushes collection + 3 listeners into subscriptions", () => {
    const ctx = makeContext();
    registerDriftDiagnostics(ctx);
    // collection + onDidOpen + onDidSave + onDidClose = 4
    expect(ctx.subscriptions.length).toBe(4);
  });
});

describe("diagnostic mapping", () => {
  // We test the mapping indirectly by triggering the onDidOpen callback
  // with a mock document and verifying what gets set on the collection.

  beforeEach(() => {
    vi.clearAllMocks();
    mockDiagnosticCollection.set.mockReset();
    mockDiagnosticCollection.delete.mockReset();
    runRockyJsonMock.mockReset();
  });

  it("maps Rocky diagnostics to vscode diagnostics with correct severity", async () => {
    const compileOutput: CompileOutput = {
      command: "compile",
      version: "0.4.0",
      models: 1,
      execution_layers: 1,
      has_errors: false,
      compile_timings: {
        total_ms: 10,
        project_load_ms: 1,
        semantic_graph_ms: 2,
        typecheck_ms: 3,
        typecheck_join_keys_ms: 1,
        contracts_ms: 1,
      },
      diagnostics: [
        {
          code: "W001",
          message: "Column type changed from STRING to INT",
          model: "my_model",
          severity: "Warning",
          span: { file: "models/my_model.sql", line: 5, col: 10 },
        },
        {
          code: "E001",
          message: "Missing required column",
          model: "my_model",
          severity: "Error",
          span: { file: "models/my_model.sql", line: 12, col: 1 },
        },
        {
          code: "I001",
          message: "New column detected",
          model: "my_model",
          severity: "Info",
          span: null,
        },
      ],
    };

    runRockyJsonMock.mockResolvedValueOnce(compileOutput);

    // Capture the onDidOpen callback.
    let openCallback: ((doc: vscode.TextDocument) => void) | undefined;
    (vscode.workspace.onDidOpenTextDocument as ReturnType<typeof vi.fn>).mockImplementation(
      (cb: (doc: vscode.TextDocument) => void) => {
        openCallback = cb;
        return { dispose: vi.fn() };
      },
    );

    // Override textDocuments to be empty so activation doesn't trigger anything.
    Object.defineProperty(vscode.workspace, "textDocuments", { value: [], configurable: true });

    const ctx = makeContext();
    registerDriftDiagnostics(ctx);

    // Simulate opening a model file.
    const mockDoc = {
      fileName: "/project/models/my_model.sql",
      uri: vscode.Uri.file("/project/models/my_model.sql"),
    } as unknown as vscode.TextDocument;

    openCallback!(mockDoc);

    // Wait for the async refreshDiagnostics to complete.
    await vi.waitFor(() => {
      expect(mockDiagnosticCollection.set).toHaveBeenCalled();
    });

    const [uri, diags] = mockDiagnosticCollection.set.mock.calls[0];
    expect(uri.fsPath).toBe("/project/models/my_model.sql");
    expect(diags).toHaveLength(3);

    // Warning
    expect(diags[0].severity).toBe(1); // vscode.DiagnosticSeverity.Warning
    expect(diags[0].code).toBe("W001");
    expect(diags[0].message).toBe("Column type changed from STRING to INT");
    expect(diags[0].source).toBe("rocky");
    expect(diags[0].range.start.line).toBe(4); // 1-based → 0-based
    expect(diags[0].range.start.character).toBe(9);

    // Error
    expect(diags[1].severity).toBe(0); // vscode.DiagnosticSeverity.Error
    expect(diags[1].code).toBe("E001");
    expect(diags[1].range.start.line).toBe(11);

    // Info (no span — defaults to line 0, col 0)
    expect(diags[2].severity).toBe(2); // vscode.DiagnosticSeverity.Information
    expect(diags[2].range.start.line).toBe(0);
    expect(diags[2].range.start.character).toBe(0);
  });

  it("clears diagnostics for non-model files", async () => {
    let openCallback: ((doc: vscode.TextDocument) => void) | undefined;
    (vscode.workspace.onDidOpenTextDocument as ReturnType<typeof vi.fn>).mockImplementation(
      (cb: (doc: vscode.TextDocument) => void) => {
        openCallback = cb;
        return { dispose: vi.fn() };
      },
    );

    Object.defineProperty(vscode.workspace, "textDocuments", { value: [], configurable: true });

    const ctx = makeContext();
    registerDriftDiagnostics(ctx);

    const mockDoc = {
      fileName: "/project/README.md",
      uri: vscode.Uri.file("/project/README.md"),
    } as unknown as vscode.TextDocument;

    openCallback!(mockDoc);

    // Give async code a tick to run.
    await new Promise((r) => setTimeout(r, 10));

    expect(mockDiagnosticCollection.delete).toHaveBeenCalledWith(mockDoc.uri);
    expect(runRockyJsonMock).not.toHaveBeenCalled();
  });

  it("clears diagnostics on CLI failure rather than showing stale results", async () => {
    const { RockyCliError } = await import("../rockyCli");
    runRockyJsonMock.mockRejectedValueOnce(
      new RockyCliError("compile failed", "error output", 1),
    );

    let openCallback: ((doc: vscode.TextDocument) => void) | undefined;
    (vscode.workspace.onDidOpenTextDocument as ReturnType<typeof vi.fn>).mockImplementation(
      (cb: (doc: vscode.TextDocument) => void) => {
        openCallback = cb;
        return { dispose: vi.fn() };
      },
    );

    Object.defineProperty(vscode.workspace, "textDocuments", { value: [], configurable: true });

    const ctx = makeContext();
    registerDriftDiagnostics(ctx);

    const mockDoc = {
      fileName: "/project/models/broken_model.rocky",
      uri: vscode.Uri.file("/project/models/broken_model.rocky"),
    } as unknown as vscode.TextDocument;

    openCallback!(mockDoc);

    await vi.waitFor(() => {
      expect(mockDiagnosticCollection.delete).toHaveBeenCalledWith(mockDoc.uri);
    });
  });

  it("filters diagnostics to the current model only", async () => {
    const compileOutput: CompileOutput = {
      command: "compile",
      version: "0.4.0",
      models: 2,
      execution_layers: 1,
      has_errors: false,
      compile_timings: {
        total_ms: 10,
        project_load_ms: 1,
        semantic_graph_ms: 2,
        typecheck_ms: 3,
        typecheck_join_keys_ms: 1,
        contracts_ms: 1,
      },
      diagnostics: [
        {
          code: "W001",
          message: "Drift in target_model",
          model: "target_model",
          severity: "Warning",
          span: { file: "models/target_model.sql", line: 1, col: 1 },
        },
        {
          code: "W002",
          message: "Drift in other_model",
          model: "other_model",
          severity: "Warning",
          span: { file: "models/other_model.sql", line: 1, col: 1 },
        },
      ],
    };

    runRockyJsonMock.mockResolvedValueOnce(compileOutput);

    let openCallback: ((doc: vscode.TextDocument) => void) | undefined;
    (vscode.workspace.onDidOpenTextDocument as ReturnType<typeof vi.fn>).mockImplementation(
      (cb: (doc: vscode.TextDocument) => void) => {
        openCallback = cb;
        return { dispose: vi.fn() };
      },
    );

    Object.defineProperty(vscode.workspace, "textDocuments", { value: [], configurable: true });

    const ctx = makeContext();
    registerDriftDiagnostics(ctx);

    const mockDoc = {
      fileName: "/project/models/target_model.sql",
      uri: vscode.Uri.file("/project/models/target_model.sql"),
    } as unknown as vscode.TextDocument;

    openCallback!(mockDoc);

    await vi.waitFor(() => {
      expect(mockDiagnosticCollection.set).toHaveBeenCalled();
    });

    const [, diags] = mockDiagnosticCollection.set.mock.calls[0];
    expect(diags).toHaveLength(1);
    expect(diags[0].message).toBe("Drift in target_model");
  });
});
