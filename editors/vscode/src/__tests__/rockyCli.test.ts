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
  spawn: vi.fn(),
}));

// Imports must follow vi.mock so the mocked modules are bound.
import * as cp from "child_process";
import {
  RockyCliError,
  clearCliVersionCache,
  getCliVersion,
  invalidateRockyCache,
  runRocky,
  runRockyJson,
  runRockyJsonCached,
} from "../rockyCli";

type ExecCallback = (
  err: (Error & { code?: number }) | null,
  stdout: string,
  stderr: string,
) => void;

const execFileMock = cp.execFile as unknown as ReturnType<typeof vi.fn>;
const spawnMock = cp.spawn as unknown as ReturnType<typeof vi.fn>;

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

  it("captures stdout on RockyCliError so callers can recover JSON payloads", async () => {
    // `rocky doctor` exits 2 when health is critical but still emits valid
    // JSON on stdout. Callers (the doctor handler) read err.stdout to recover
    // the payload instead of dropping it on the floor.
    const doctorJson =
      '{"command":"doctor","overall":"critical","checks":[],"suggestions":[]}';
    execFileMock.mockImplementationOnce(
      (_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
        const err = Object.assign(new Error("exit 2"), { code: 2 });
        cb(err, doctorJson, "");
        return fakeChild();
      },
    );
    await expect(
      runRocky(["doctor", "--output", "json"]),
    ).rejects.toMatchObject({
      exitCode: 2,
      stdout: doctorJson,
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

  it("removes the abort listener after the command completes successfully", async () => {
    const killSpy = vi.fn();
    let storedCallback: ExecCallback | undefined;
    execFileMock.mockImplementationOnce(
      (_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
        storedCallback = cb;
        return { kill: killSpy };
      },
    );

    const controller = new AbortController();
    const promise = runRocky(["run"], { signal: controller.signal });

    // Simulate successful completion.
    storedCallback!(null, '{"ok":true}', "");
    await promise;

    // After success, aborting the signal must NOT kill the (already-done) child.
    controller.abort();
    expect(killSpy).not.toHaveBeenCalled();
  });

  it("removes the abort listener after the command fails", async () => {
    const killSpy = vi.fn();
    let storedCallback: ExecCallback | undefined;
    execFileMock.mockImplementationOnce(
      (_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
        storedCallback = cb;
        return { kill: killSpy };
      },
    );

    const controller = new AbortController();
    const promise = runRocky(["run"], { signal: controller.signal });

    // Simulate failure.
    const err = Object.assign(new Error("exit 1"), { code: 1 });
    storedCallback!(err, "", "error output");
    await expect(promise).rejects.toBeInstanceOf(RockyCliError);

    // After failure, aborting must NOT kill the already-done child.
    controller.abort();
    expect(killSpy).not.toHaveBeenCalled();
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

  it("sets kind='parse' and empty stderr on JSON parse failure", async () => {
    mockSuccess("not json at all");
    await expect(runRockyJson(["doctor"])).rejects.toMatchObject({
      kind: "parse",
      stderr: "",
    });
  });

  it("parse error message does not contain the raw JSON output", async () => {
    const rawOutput = '{"broken": true, some garbage}';
    mockSuccess(rawOutput);
    const err = await runRockyJson(["doctor"]).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RockyCliError);
    expect((err as RockyCliError).stderr).toBe("");
    // The message must NOT embed the raw output verbatim.
    expect((err as RockyCliError).message).not.toContain(rawOutput);
    // The raw output is available on stdout for diagnostics, but not in the user-visible message.
    expect((err as RockyCliError).stdout).toBe(rawOutput);
  });
});

describe("getCliVersion", () => {
  beforeEach(() => {
    execFileMock.mockReset();
    clearCliVersionCache();
  });

  it("parses the version number from `rocky --version` output", async () => {
    mockSuccess("rocky 1.26.0\n");
    const v = await getCliVersion();
    expect(v).toBe("1.26.0");
    expect(execFileMock).toHaveBeenCalledTimes(1);
    expect(execFileMock.mock.calls[0][1]).toEqual(["--version"]);
  });

  it("caches the result across consecutive calls", async () => {
    mockSuccess("rocky 1.26.0");
    const first = await getCliVersion();
    const second = await getCliVersion();
    expect(first).toBe("1.26.0");
    expect(second).toBe("1.26.0");
    expect(execFileMock).toHaveBeenCalledTimes(1);
  });

  it("returns undefined when the binary is missing", async () => {
    mockFailure("ENOENT", "spawn rocky ENOENT", 0);
    const v = await getCliVersion();
    expect(v).toBeUndefined();
  });

  it("returns undefined when output has no recognizable version", async () => {
    mockSuccess("?? something weird ??");
    const v = await getCliVersion();
    expect(v).toBeUndefined();
  });

  it("re-shells after the cache is cleared", async () => {
    mockSuccess("rocky 1.26.0");
    expect(await getCliVersion()).toBe("1.26.0");
    clearCliVersionCache();
    mockSuccess("rocky 1.27.0");
    expect(await getCliVersion()).toBe("1.27.0");
    expect(execFileMock).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// Unbounded-output spawn path — used for catalog/history/optimize/discover so
// large warehouse output doesn't hit execFile's 16 MiB maxBuffer cap.
// ---------------------------------------------------------------------------

interface FakeChildHandlers {
  stdout: { on: ReturnType<typeof vi.fn> };
  stderr: { on: ReturnType<typeof vi.fn> };
  on: ReturnType<typeof vi.fn>;
  kill: ReturnType<typeof vi.fn>;
}

interface FakeSpawnDriver {
  emitStdout: (chunk: string) => void;
  emitStderr: (chunk: string) => void;
  emitClose: (code: number | null, signal?: string | null) => void;
  emitError: (err: Error) => void;
  child: FakeChildHandlers;
}

function mockSpawnOnce(): FakeSpawnDriver {
  const stdoutListeners: ((chunk: Buffer) => void)[] = [];
  const stderrListeners: ((chunk: Buffer) => void)[] = [];
  const closeListeners: ((code: number | null, signal?: string | null) => void)[] = [];
  const errorListeners: ((err: Error) => void)[] = [];

  const child: FakeChildHandlers = {
    stdout: {
      on: vi.fn((event: string, cb: (chunk: Buffer) => void) => {
        if (event === "data") stdoutListeners.push(cb);
      }),
    },
    stderr: {
      on: vi.fn((event: string, cb: (chunk: Buffer) => void) => {
        if (event === "data") stderrListeners.push(cb);
      }),
    },
    on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
      if (event === "close") {
        closeListeners.push(
          cb as (code: number | null, signal?: string | null) => void,
        );
      } else if (event === "error") {
        errorListeners.push(cb as (err: Error) => void);
      }
    }),
    kill: vi.fn(),
  };

  spawnMock.mockImplementationOnce(() => child);

  return {
    emitStdout: (chunk: string): void => {
      for (const l of stdoutListeners) l(Buffer.from(chunk, "utf8"));
    },
    emitStderr: (chunk: string): void => {
      for (const l of stderrListeners) l(Buffer.from(chunk, "utf8"));
    },
    emitClose: (code: number | null, signal: string | null = null): void => {
      for (const l of closeListeners) l(code, signal);
    },
    emitError: (err: Error): void => {
      for (const l of errorListeners) l(err);
    },
    child,
  };
}

describe("runRocky (unbounded subcommands → spawn)", () => {
  beforeEach(() => {
    execFileMock.mockReset();
    spawnMock.mockReset();
  });

  it("routes `catalog` through spawn, not execFile", async () => {
    const driver = mockSpawnOnce();
    const promise = runRocky(["catalog", "--output", "json"]);
    driver.emitStdout('{"rows":42}');
    driver.emitClose(0);
    const result = await promise;
    expect(result.stdout).toBe('{"rows":42}');
    expect(spawnMock).toHaveBeenCalledTimes(1);
    expect(execFileMock).not.toHaveBeenCalled();
    expect(spawnMock.mock.calls[0][0]).toBe("/usr/local/bin/rocky");
    expect(spawnMock.mock.calls[0][1]).toEqual(["catalog", "--output", "json"]);
  });

  it.each(["catalog", "history", "optimize", "discover"])(
    "routes `%s` through spawn",
    async (subcommand) => {
      const driver = mockSpawnOnce();
      const promise = runRocky([subcommand]);
      driver.emitStdout('{"ok":true}');
      driver.emitClose(0);
      await promise;
      expect(spawnMock).toHaveBeenCalledTimes(1);
    },
  );

  it("keeps bounded subcommands (compile, run, doctor) on execFile", async () => {
    mockSuccess('{"ok":true}');
    await runRocky(["compile"]);
    expect(execFileMock).toHaveBeenCalledTimes(1);
    expect(spawnMock).not.toHaveBeenCalled();
  });

  it("ignores top-level flags when classifying the subcommand", async () => {
    const driver = mockSpawnOnce();
    const promise = runRocky(["--verbose", "catalog"]);
    driver.emitStdout("{}");
    driver.emitClose(0);
    await promise;
    expect(spawnMock).toHaveBeenCalledTimes(1);
  });

  it("accumulates multiple stdout chunks into a single payload", async () => {
    const driver = mockSpawnOnce();
    const promise = runRocky(["catalog"]);
    driver.emitStdout('{"part1": "');
    driver.emitStdout('value", "part2": ');
    driver.emitStdout('"more"}');
    driver.emitClose(0);
    const result = await promise;
    expect(result.stdout).toBe('{"part1": "value", "part2": "more"}');
  });

  it("rejects with RockyCliError on non-zero exit and preserves stdout", async () => {
    const driver = mockSpawnOnce();
    const promise = runRocky(["catalog"]);
    driver.emitStdout('{"partial":true}');
    driver.emitStderr("catalog: warehouse offline");
    driver.emitClose(2);
    await expect(promise).rejects.toMatchObject({
      stderr: "catalog: warehouse offline",
      stdout: '{"partial":true}',
      exitCode: 2,
    });
  });

  it("rejects with RockyCliError on spawn `error` event (ENOENT)", async () => {
    const driver = mockSpawnOnce();
    const promise = runRocky(["catalog"]);
    driver.emitError(
      Object.assign(new Error("spawn rocky ENOENT"), { code: "ENOENT" }),
    );
    await expect(promise).rejects.toMatchObject({
      exitCode: null,
    });
  });

  it("kills the child when the abort signal fires", async () => {
    const driver = mockSpawnOnce();
    const controller = new AbortController();
    void runRocky(["catalog"], { signal: controller.signal });
    controller.abort();
    expect(driver.child.kill).toHaveBeenCalledWith("SIGTERM");
  });

  it("kills the child and rejects when the timeout fires", async () => {
    vi.useFakeTimers();
    try {
      const driver = mockSpawnOnce();
      const promise = runRocky(["catalog"], { timeoutMs: 100 });
      vi.advanceTimersByTime(100);
      expect(driver.child.kill).toHaveBeenCalledWith("SIGTERM");
      // Caller still has to observe the close event before the promise settles.
      driver.emitClose(null, "SIGTERM");
      await expect(promise).rejects.toBeInstanceOf(RockyCliError);
    } finally {
      vi.useRealTimers();
    }
  });
});

// ---------------------------------------------------------------------------
// runRockyJsonCached — catalog/compile result cache + in-flight coalescing.
// `compile` is a bounded subcommand, so it routes through execFileMock.
// ---------------------------------------------------------------------------

describe("runRockyJsonCached", () => {
  beforeEach(() => {
    execFileMock.mockReset();
    invalidateRockyCache();
  });

  it("returns the cached result on the second call without re-spawning", async () => {
    mockSuccess('{"models_detail":[]}');
    const first = await runRockyJsonCached(["compile", "--output", "json"]);
    const second = await runRockyJsonCached(["compile", "--output", "json"]);

    expect(first).toEqual({ models_detail: [] });
    expect(second).toEqual({ models_detail: [] });
    // Only the first call shelled out; the second was served from the cache.
    expect(execFileMock).toHaveBeenCalledTimes(1);
  });

  it("coalesces two concurrent identical calls into one spawn", async () => {
    mockSuccess('{"ok":true}');
    const [a, b] = await Promise.all([
      runRockyJsonCached(["compile", "--output", "json"]),
      runRockyJsonCached(["compile", "--output", "json"]),
    ]);
    expect(a).toEqual({ ok: true });
    expect(b).toEqual({ ok: true });
    expect(execFileMock).toHaveBeenCalledTimes(1);
  });

  it("keys on the full argument vector — compile and compile --model differ", async () => {
    mockSuccess('{"scope":"all"}');
    mockSuccess('{"scope":"model"}');
    const all = await runRockyJsonCached(["compile", "--output", "json"]);
    const one = await runRockyJsonCached([
      "compile",
      "--model",
      "orders",
      "--output",
      "json",
    ]);
    expect(all).toEqual({ scope: "all" });
    expect(one).toEqual({ scope: "model" });
    expect(execFileMock).toHaveBeenCalledTimes(2);
  });

  it("re-spawns after invalidateRockyCache()", async () => {
    mockSuccess('{"gen":1}');
    expect(await runRockyJsonCached(["compile", "--output", "json"])).toEqual({
      gen: 1,
    });
    invalidateRockyCache();
    mockSuccess('{"gen":2}');
    expect(await runRockyJsonCached(["compile", "--output", "json"])).toEqual({
      gen: 2,
    });
    expect(execFileMock).toHaveBeenCalledTimes(2);
  });

  it("does not cache a rejection — a later call re-spawns and can succeed", async () => {
    mockFailure("boom", "compile failed", 1);
    await expect(
      runRockyJsonCached(["compile", "--output", "json"]),
    ).rejects.toBeInstanceOf(RockyCliError);

    mockSuccess('{"recovered":true}');
    expect(await runRockyJsonCached(["compile", "--output", "json"])).toEqual({
      recovered: true,
    });
    expect(execFileMock).toHaveBeenCalledTimes(2);
  });
});
