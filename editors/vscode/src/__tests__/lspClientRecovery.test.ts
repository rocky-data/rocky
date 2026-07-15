import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Regression guard for the LSP crash-recovery bug: vscode-languageclient's
// default error handler auto-restarts a crashed server (Stopped→Starting→
// Running) WITHOUT going through launchClient(), so nothing reset the extension's
// LspState back to "Ready". Left stuck at "Failed", the CLI-compile diagnostics
// fallback kept running alongside the recovered server. handleStateChange now
// resets to Ready on any →Running.

// Captured onDidChangeState callback from the mocked LanguageClient.
let stateCb: ((e: { oldState: number; newState: number }) => void) | undefined;

const STATE = { Stopped: 1, Running: 2, Starting: 3 };

vi.mock("vscode-languageclient/node", () => {
  class LanguageClient {
    onDidChangeState(cb: (e: { oldState: number; newState: number }) => void) {
      stateCb = cb;
      return { dispose: vi.fn() };
    }
    start() {
      return Promise.resolve();
    }
    stop() {
      return Promise.resolve();
    }
    isRunning() {
      return true;
    }
  }
  return {
    LanguageClient,
    State: STATE,
    TransportKind: { stdio: 0 },
  };
});

const statusBarItem = {
  text: "",
  tooltip: "" as unknown,
  backgroundColor: undefined as unknown,
  command: "",
  show: vi.fn(),
  hide: vi.fn(),
  dispose: vi.fn(),
};

function makeEmitter() {
  const listeners: Array<(v: unknown) => void> = [];
  return {
    event: (cb: (v: unknown) => void) => {
      listeners.push(cb);
      return { dispose: vi.fn() };
    },
    fire: (v: unknown) => listeners.forEach((l) => l(v)),
    dispose: vi.fn(),
  };
}

vi.mock("vscode", () => ({
  StatusBarAlignment: { Left: 1, Right: 2 },
  ThemeColor: class {
    constructor(public id: string) {}
  },
  EventEmitter: class {
    private _e = makeEmitter();
    event = this._e.event;
    fire = this._e.fire;
    dispose = this._e.dispose;
  },
  window: {
    createStatusBarItem: vi.fn(() => statusBarItem),
    showErrorMessage: vi.fn(() => Promise.resolve(undefined)),
    showInformationMessage: vi.fn(() => Promise.resolve(undefined)),
  },
  languages: {
    onDidChangeDiagnostics: vi.fn(() => ({ dispose: vi.fn() })),
    getDiagnostics: vi.fn(() => []),
  },
  workspace: {
    onDidChangeConfiguration: vi.fn(() => ({ dispose: vi.fn() })),
    createFileSystemWatcher: vi.fn(() => ({ dispose: vi.fn() })),
    getConfiguration: vi.fn(() => ({ get: (_k: string, d: unknown) => d })),
  },
  commands: { registerCommand: vi.fn(() => ({ dispose: vi.fn() })) },
}));

vi.mock("../config", () => ({
  getConfig: () => ({ serverPath: "rocky", extraArgs: [], inlayHintsEnabled: true }),
}));

vi.mock("../output", () => ({
  getOutputChannel: () => ({ appendLine: vi.fn() }),
}));

// resolveLspBinary probes the filesystem for a sibling rocky-lsp; force the
// PATH-based fallback (existsSync → false) so no real binary is required.
vi.mock("node:fs", () => ({ existsSync: () => false }));

const flush = () => new Promise((r) => setTimeout(r, 0));

describe("LSP crash recovery", () => {
  beforeEach(() => {
    stateCb = undefined;
    vi.resetModules();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("resets Failed → Ready when the server auto-restarts (→Running)", async () => {
    const lsp = await import("../lspClient");
    const ctx = { subscriptions: [] } as unknown as import("vscode").ExtensionContext;

    lsp.startLspClient(ctx);
    await flush(); // let launchClient()/client.start() resolve → Ready
    expect(stateCb).toBeDefined();
    expect(lsp.getLspState().status).toBe("Ready");

    // Server crashes: Running → Stopped marks Failed.
    stateCb!({ oldState: STATE.Running, newState: STATE.Stopped });
    expect(lsp.getLspState().status).toBe("Failed");

    // Library auto-restarts: Stopped → Running must recover to Ready, not
    // leave the state stuck at Failed (which keeps the CLI fallback firing).
    stateCb!({ oldState: STATE.Stopped, newState: STATE.Running });
    expect(lsp.getLspState().status).toBe("Ready");
  });

  it("does not report Failed for a deliberate restart (expectedStop path stays Ready on recovery)", async () => {
    const lsp = await import("../lspClient");
    const ctx = { subscriptions: [] } as unknown as import("vscode").ExtensionContext;
    lsp.startLspClient(ctx);
    await flush();

    // A →Running when already Ready is a no-op (no spurious state churn).
    stateCb!({ oldState: STATE.Starting, newState: STATE.Running });
    expect(lsp.getLspState().status).toBe("Ready");
  });
});
