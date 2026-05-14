import { beforeEach, describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// vscode mock — must precede all imports that touch vscode
// ---------------------------------------------------------------------------

vi.mock("vscode", () => {
  class Uri {
    static joinPath(base: Uri, ...parts: string[]): Uri {
      return new Uri(base.fsPath + "/" + parts.join("/"));
    }
    constructor(public fsPath: string) {}
    toString(): string {
      return this.fsPath;
    }
  }

  const LanguageModelChatMessage = {
    User: (content: string) => ({ role: "user", content }),
  };

  return {
    Uri,
    LanguageModelChatMessage,
    chat: {
      createChatParticipant: vi.fn(() => ({
        iconPath: undefined as unknown,
        followupProvider: undefined as unknown,
        dispose: vi.fn(),
      })),
    },
    lm: {
      selectChatModels: vi.fn(async () => []),
    },
    window: {
      activeTextEditor: undefined as
        | { document: { fileName: string } }
        | undefined,
      showTextDocument: vi.fn(async () => undefined),
    },
    workspace: {
      openTextDocument: vi.fn(async (opts: { content: string }) => ({
        content: opts.content,
      })),
    },
  };
});

// ---------------------------------------------------------------------------
// AI command mocks — intercept runRockyWithProgress calls
// ---------------------------------------------------------------------------

vi.mock("../rockyCli", () => ({
  runRockyWithProgress: vi.fn(async () => ({ stdout: "", stderr: "" })),
  showRockyError: vi.fn(),
}));

// Imports must follow vi.mock.
import * as vscode from "vscode";
import * as rockyCli from "../rockyCli";
import { registerChatParticipant } from "../chatParticipant";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type HandlerFn = (
  request: {
    command: string | undefined;
    prompt: string;
  },
  context: object,
  stream: {
    markdown: ReturnType<typeof vi.fn>;
  },
  token: object,
) => Promise<{ metadata: { lastCommand: string | undefined } }>;

const runWithProgress = rockyCli.runRockyWithProgress as ReturnType<
  typeof vi.fn
>;

function makeStream(): { markdown: ReturnType<typeof vi.fn> } {
  return { markdown: vi.fn() };
}

function makeContext(): vscode.ExtensionContext {
  return {
    extensionUri: new vscode.Uri("/ext"),
    subscriptions: [],
  } as unknown as vscode.ExtensionContext;
}

function captureHandler(): HandlerFn {
  const createMock = vscode.chat.createChatParticipant as ReturnType<
    typeof vi.fn
  >;
  const call = createMock.mock.calls[0];
  return call[1] as HandlerFn;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("registerChatParticipant", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("creates a participant with id rocky-data.rocky", () => {
    registerChatParticipant(makeContext());
    expect(vscode.chat.createChatParticipant).toHaveBeenCalledWith(
      "rocky-data.rocky",
      expect.any(Function),
    );
  });

  it("sets iconPath and followupProvider on the participant", () => {
    const mockParticipant = {
      iconPath: undefined as unknown,
      followupProvider: undefined as unknown,
      dispose: vi.fn(),
    };
    (vscode.chat.createChatParticipant as ReturnType<typeof vi.fn>).mockReturnValueOnce(
      mockParticipant,
    );
    registerChatParticipant(makeContext());
    expect(mockParticipant.iconPath).toBeDefined();
    expect(mockParticipant.followupProvider).toBeDefined();
  });

  it("pushes the participant to context.subscriptions", () => {
    const ctx = makeContext();
    registerChatParticipant(ctx);
    expect(ctx.subscriptions).toHaveLength(1);
  });
});

describe("@rocky /generate", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("invokes rocky ai with the prompt as intent", async () => {
    runWithProgress.mockResolvedValueOnce({
      stdout: "model Foo {}",
      stderr: "",
    });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: "generate", prompt: "monthly revenue per customer" },
      {},
      stream,
      {},
    );
    expect(runWithProgress).toHaveBeenCalledTimes(1);
    const [, args] = runWithProgress.mock.calls[0] as [
      string,
      string[],
      ...unknown[]
    ];
    expect(args[0]).toBe("ai");
    expect(args[1]).toBe("monthly revenue per customer");
    expect(args).toContain("--format");
    expect(args).toContain("rocky");
  });

  it("streams the generated source as a fenced code block", async () => {
    runWithProgress.mockResolvedValueOnce({
      stdout: "model Foo { select * from bar }",
      stderr: "",
    });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: "generate", prompt: "fct_revenue" },
      {},
      stream,
      {},
    );
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    expect(calls.some((c) => c.includes("```rocky"))).toBe(true);
    expect(calls.some((c) => c.includes("model Foo { select * from bar }"))).toBe(true);
  });

  it("returns lastCommand=generate in metadata", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "model X {}", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const result = await handler(
      { command: "generate", prompt: "something" },
      {},
      makeStream(),
      {},
    );
    expect(result.metadata.lastCommand).toBe("generate");
  });

  it("shows an error message when generate fails", async () => {
    runWithProgress.mockRejectedValueOnce(new Error("CLI not found"));
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: "generate", prompt: "something" },
      {},
      stream,
      {},
    );
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    expect(calls.some((c) => c.includes("Error generating model"))).toBe(true);
  });

  it("emits help when prompt is empty", async () => {
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler({ command: "generate", prompt: "" }, {}, stream, {});
    expect(runWithProgress).not.toHaveBeenCalled();
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    expect(calls.some((c) => c.includes("/generate"))).toBe(true);
  });
});

describe("@rocky /explain", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("invokes rocky ai-explain without a model name when no editor is active", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    await handler({ command: "explain", prompt: "" }, {}, makeStream(), {});
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args[0]).toBe("ai-explain");
    expect(args).toContain("--all");
    expect(args).not.toContain("fct_revenue");
  });

  it("scopes to the active model when an editor is open", async () => {
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor = {
      document: { fileName: "/workspace/models/fct_revenue.rocky" },
    };
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    await handler({ command: "explain", prompt: "" }, {}, makeStream(), {});
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args).toContain("fct_revenue");
    expect(args).not.toContain("--all");
  });

  it("returns lastCommand=explain in metadata", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const result = await handler(
      { command: "explain", prompt: "" },
      {},
      makeStream(),
      {},
    );
    expect(result.metadata.lastCommand).toBe("explain");
  });
});

describe("@rocky /sync", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("invokes rocky ai-sync against all models", async () => {
    runWithProgress.mockResolvedValueOnce({
      stdout: "1 model updated",
      stderr: "",
    });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler({ command: "sync", prompt: "" }, {}, stream, {});
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args[0]).toBe("ai-sync");
    expect(args).toContain("models");
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    expect(calls.some((c) => c.includes("1 model updated"))).toBe(true);
  });

  it("returns lastCommand=sync in metadata", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const result = await handler(
      { command: "sync", prompt: "" },
      {},
      makeStream(),
      {},
    );
    expect(result.metadata.lastCommand).toBe("sync");
  });
});

describe("@rocky /test", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("uses explicit arg as the model name", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    await handler(
      { command: "test", prompt: "fct_daily_revenue" },
      {},
      makeStream(),
      {},
    );
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args[0]).toBe("ai-test");
    expect(args).toContain("fct_daily_revenue");
  });

  it("falls back to active editor when no arg is given", async () => {
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor = {
      document: { fileName: "/workspace/models/dim_customers.sql" },
    };
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    await handler({ command: "test", prompt: "" }, {}, makeStream(), {});
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args).toContain("dim_customers");
  });

  it("runs against all models when no arg and no active editor", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    await handler({ command: "test", prompt: "" }, {}, makeStream(), {});
    const [, args] = runWithProgress.mock.calls[0] as [string, string[], ...unknown[]];
    expect(args[0]).toBe("ai-test");
    expect(args).toContain("--all");
  });

  it("returns lastCommand=test in metadata", async () => {
    runWithProgress.mockResolvedValueOnce({ stdout: "", stderr: "" });
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const result = await handler(
      { command: "test", prompt: "" },
      {},
      makeStream(),
      {},
    );
    expect(result.metadata.lastCommand).toBe("test");
  });
});

describe("@rocky natural-language fallback", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  it("emits the help command listing when no LLM model is available", async () => {
    (vscode.lm.selectChatModels as ReturnType<typeof vi.fn>).mockResolvedValueOnce([]);
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: undefined, prompt: "what can you do?" },
      {},
      stream,
      {},
    );
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    const combined = calls.join("");
    expect(combined).toContain("/generate");
    expect(combined).toContain("/explain");
    expect(combined).toContain("/sync");
    expect(combined).toContain("/test");
  });

  it("calls the LLM when a copilot model is available", async () => {
    const sendRequest = vi.fn(async () => ({
      text: (async function* () {
        yield "Here is some advice.";
      })(),
    }));
    (vscode.lm.selectChatModels as ReturnType<typeof vi.fn>).mockResolvedValueOnce([
      { sendRequest },
    ]);
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: undefined, prompt: "how do I create a model?" },
      {},
      stream,
      {},
    );
    expect(sendRequest).toHaveBeenCalledTimes(1);
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    expect(calls.some((c) => c.includes("Here is some advice."))).toBe(true);
  });

  it("falls back to help listing when LLM throws", async () => {
    const sendRequest = vi.fn().mockRejectedValueOnce(new Error("no quota"));
    (vscode.lm.selectChatModels as ReturnType<typeof vi.fn>).mockResolvedValueOnce([
      { sendRequest },
    ]);
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const stream = makeStream();
    await handler(
      { command: undefined, prompt: "help me" },
      {},
      stream,
      {},
    );
    const calls: string[] = stream.markdown.mock.calls.map(
      (c: [string]) => c[0],
    );
    const combined = calls.join("");
    expect(combined).toContain("/generate");
  });

  it("returns lastCommand=undefined in metadata for NL requests", async () => {
    (vscode.lm.selectChatModels as ReturnType<typeof vi.fn>).mockResolvedValueOnce([]);
    registerChatParticipant(makeContext());
    const handler = captureHandler();
    const result = await handler(
      { command: undefined, prompt: "what is rocky?" },
      {},
      makeStream(),
      {},
    );
    expect(result.metadata.lastCommand).toBeUndefined();
  });
});

describe("followup provider", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (vscode.window as { activeTextEditor: unknown }).activeTextEditor =
      undefined;
  });

  function getFollowupProvider(): {
    provideFollowups: (result: { metadata: { lastCommand: string | undefined } }, ctx: object, token: object) => vscode.ChatFollowup[];
  } {
    const mockParticipant = {
      iconPath: undefined as unknown,
      followupProvider: undefined as unknown,
      dispose: vi.fn(),
    };
    (vscode.chat.createChatParticipant as ReturnType<typeof vi.fn>).mockReturnValueOnce(
      mockParticipant,
    );
    registerChatParticipant(makeContext());
    return mockParticipant.followupProvider as {
      provideFollowups: (result: { metadata: { lastCommand: string | undefined } }, ctx: object, token: object) => vscode.ChatFollowup[];
    };
  }

  it("suggests /test and /explain after /generate", () => {
    const provider = getFollowupProvider();
    const followups = provider.provideFollowups(
      { metadata: { lastCommand: "generate" } },
      {},
      {},
    );
    const prompts = followups.map((f) => f.prompt);
    expect(prompts).toContain("/test");
    expect(prompts).toContain("/explain");
    // No 'command' field — VS Code would otherwise produce "@rocky /test /test"
    for (const f of followups) {
      expect((f as { command?: string }).command).toBeUndefined();
    }
  });

  it("suggests /test after /explain", () => {
    const provider = getFollowupProvider();
    const followups = provider.provideFollowups(
      { metadata: { lastCommand: "explain" } },
      {},
      {},
    );
    expect(followups.some((f) => f.prompt === "/test")).toBe(true);
    for (const f of followups) {
      expect((f as { command?: string }).command).toBeUndefined();
    }
  });

  it("suggests /explain after /sync", () => {
    const provider = getFollowupProvider();
    const followups = provider.provideFollowups(
      { metadata: { lastCommand: "sync" } },
      {},
      {},
    );
    expect(followups.some((f) => f.prompt === "/explain")).toBe(true);
    for (const f of followups) {
      expect((f as { command?: string }).command).toBeUndefined();
    }
  });

  it("returns empty array for /test (no natural next step)", () => {
    const provider = getFollowupProvider();
    const followups = provider.provideFollowups(
      { metadata: { lastCommand: "test" } },
      {},
      {},
    );
    expect(followups).toHaveLength(0);
  });

  it("returns empty array when lastCommand is undefined", () => {
    const provider = getFollowupProvider();
    const followups = provider.provideFollowups(
      { metadata: { lastCommand: undefined } },
      {},
      {},
    );
    expect(followups).toHaveLength(0);
  });
});
