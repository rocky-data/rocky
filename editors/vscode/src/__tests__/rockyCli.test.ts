import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({
  workspace: {
    getConfiguration: () => ({
      get: <T,>(key: string, fallback: T): T => {
        if (key === "server.path") return "/usr/local/bin/rocky" as unknown as T;
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
    withProgress: vi.fn(),
  },
  ProgressLocation: { Notification: 15 },
}));

vi.mock("child_process", () => ({
  execFile: vi.fn(),
}));

// Imports must follow vi.mock so the mocked modules are bound.
import * as cp from "child_process";
import { runRocky, runRockyJson, RockyCliError } from "../rockyCli";

type ExecCallback = (
  err: (Error & { code?: number }) | null,
  stdout: string,
  stderr: string,
) => void;

const execFileMock = cp.execFile as unknown as ReturnType<typeof vi.fn>;

function fakeChild(): { kill: ReturnType<typeof vi.fn> } {
  return { kill: vi.fn() };
}

function mockSuccess(stdout: string, stderr = ""): void {
  execFileMock.mockImplementationOnce(
    (_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
      cb(null, stdout, stderr);
      return fakeChild();
    },
  );
}

function mockFailure(message: string, stderr: string, code = 1): void {
  execFileMock.mockImplementationOnce(
    (_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
      const err = Object.assign(new Error(message), { code });
      cb(err, "", stderr);
      return fakeChild();
    },
  );
}

describe("runRocky", () => {
  beforeEach(() => {
    execFileMock.mockReset();
  });

  it("invokes execFile with the configured server path and args", async () => {
    mockSuccess("ok");
    await runRocky(["compile", "--output", "json"]);
    expect(execFileMock).toHaveBeenCalledTimes(1);
    const call = execFileMock.mock.calls[0];
    expect(call[0]).toBe("/usr/local/bin/rocky");
    expect(call[1]).toEqual(["compile", "--output", "json"]);
  });

  it("resolves with stdout/stderr on success", async () => {
    mockSuccess('{"ok":true}', "warn line");
    const result = await runRocky(["doctor"]);
    expect(result.stdout).toBe('{"ok":true}');
    expect(result.stderr).toBe("warn line");
  });

  it("rejects with RockyCliError on non-zero exit", async () => {
    mockFailure("nonzero exit", "compile failed: missing file", 1);
    await expect(runRocky(["compile"])).rejects.toBeInstanceOf(RockyCliError);
    mockFailure("nonzero exit", "compile failed: missing file", 1);
    await expect(runRocky(["compile"])).rejects.toMatchObject({
      stderr: "compile failed: missing file",
      exitCode: 1,
    });
  });

  it("kills the child process when the abort signal fires", async () => {
    const killSpy = vi.fn();
    execFileMock.mockImplementationOnce(
      (_cmd: string, _args: string[], _opts: unknown, _cb: ExecCallback) => {
        // Never invoke the callback — leave the promise pending so we can
        // observe the abort handler firing the kill spy.
        return { kill: killSpy };
      },
    );

    const controller = new AbortController();
    void runRocky(["run"], { signal: controller.signal });
    controller.abort();
    expect(killSpy).toHaveBeenCalledWith("SIGTERM");
  });
});

describe("runRockyJson", () => {
  beforeEach(() => {
    execFileMock.mockReset();
  });

  it("parses JSON stdout", async () => {
    mockSuccess('{"version":"0.3.0","ok":true}');
    const result = await runRockyJson<{ version: string; ok: boolean }>([
      "doctor",
    ]);
    expect(result.version).toBe("0.3.0");
    expect(result.ok).toBe(true);
  });

  it("throws RockyCliError on invalid JSON", async () => {
    mockSuccess("not json at all");
    await expect(runRockyJson(["doctor"])).rejects.toBeInstanceOf(RockyCliError);
  });
});
