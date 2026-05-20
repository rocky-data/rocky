import { execFile, spawn } from "child_process";
import type { ExecFileException } from "child_process";
import * as vscode from "vscode";
import { getConfig, getWorkspaceFolder } from "./config";
import { getOutputChannel } from "./output";

/**
 * Subcommands whose stdout can exceed the 16 MiB `execFile` `maxBuffer` cap
 * on a real warehouse. We route these through {@link runRockyUnbounded},
 * which streams stdout via `spawn()` and accumulates it manually instead of
 * relying on Node's bounded internal buffer.
 *
 * Keep the set conservative — anything in here pays a slightly heavier
 * stream-assembly cost than `execFile`. Add a subcommand only when its
 * output has a credible chance of exceeding 16 MiB.
 */
const UNBOUNDED_OUTPUT_SUBCOMMANDS: ReadonlySet<string> = new Set([
  "catalog",
  "history",
  "optimize",
  "discover",
]);

/**
 * Returns `true` when {@link args} begins with a subcommand listed in
 * {@link UNBOUNDED_OUTPUT_SUBCOMMANDS}. Top-level flags such as `--output`
 * never count, so the check looks for the first non-flag token.
 */
function isUnboundedOutputCommand(args: readonly string[]): boolean {
  for (const arg of args) {
    if (arg.startsWith("-")) continue;
    return UNBOUNDED_OUTPUT_SUBCOMMANDS.has(arg);
  }
  return false;
}

export interface RunRockyOptions {
  /** Working directory for the spawned process. Defaults to the workspace root. */
  cwd?: string;
  /** Hard timeout in milliseconds. Forwarded to `execFile`. */
  timeoutMs?: number;
  /** Optional cancellation signal — aborting sends SIGTERM to the child. */
  signal?: AbortSignal;
}

export interface RockyResult {
  stdout: string;
  stderr: string;
}

/**
 * Error thrown by {@link runRocky} on non-zero exit, spawn failure, timeout,
 * or JSON-parse failure (when called via {@link runRockyJson}).
 *
 * `stdout` is captured even on non-zero exit because some Rocky commands
 * (notably `rocky doctor`) signal status via the exit code while still
 * emitting a valid JSON payload on stdout — callers that know to expect
 * this can recover the payload from the thrown error.
 *
 * `kind` distinguishes the error origin:
 * - `"exit"` — the CLI exited with a non-zero code or the spawn failed.
 * - `"parse"` — `stdout` was not valid JSON; `stderr` is `""`.
 */
export class RockyCliError extends Error {
  constructor(
    message: string,
    public readonly stderr: string,
    public readonly exitCode: number | string | null,
    cause?: Error,
    public readonly stdout: string = "",
    public readonly kind: "exit" | "parse" = "exit",
  ) {
    super(message, cause ? { cause } : undefined);
    this.name = "RockyCliError";
  }
}

/**
 * Spawn the rocky CLI with the given arguments. Resolves with stdout/stderr
 * on success and rejects with {@link RockyCliError} otherwise.
 *
 * Every invocation is logged to the shared "Rocky" output channel so the user
 * can audit what the extension is running.
 *
 * Subcommands whose output can exceed `execFile`'s 16 MiB cap (catalog,
 * history, optimize, discover) are streamed through `spawn()` instead so the
 * full payload survives. See {@link UNBOUNDED_OUTPUT_SUBCOMMANDS}.
 */
export function runRocky(
  args: string[],
  opts: RunRockyOptions = {},
): Promise<RockyResult> {
  if (isUnboundedOutputCommand(args)) {
    return runRockyUnbounded(args, opts);
  }
  const { serverPath } = getConfig();
  const cwd = opts.cwd ?? getWorkspaceFolder();
  const channel = getOutputChannel();
  const start = Date.now();

  channel.appendLine(`$ ${serverPath} ${args.join(" ")}`);

  return new Promise((resolve, reject) => {
    let removeAbortListener: (() => void) | undefined;

    const child = execFile(
      serverPath,
      args,
      {
        cwd,
        timeout: opts.timeoutMs,
        maxBuffer: 16 * 1024 * 1024,
        encoding: "utf8",
      },
      (err: ExecFileException | null, stdout: string, stderr: string) => {
        // Always clean up the abort listener, regardless of success or failure.
        removeAbortListener?.();

        const elapsed = Date.now() - start;
        if (err) {
          if (stderr) channel.appendLine(stderr.trimEnd());
          channel.appendLine(
            `[${elapsed}ms] command failed (exit ${err.code ?? "?"}): ${err.message}`,
          );
          reject(
            new RockyCliError(
              err.message,
              stderr ?? "",
              err.code ?? null,
              err,
              stdout ?? "",
            ),
          );
          return;
        }
        channel.appendLine(`[${elapsed}ms] ok`);
        resolve({ stdout, stderr });
      },
    );

    if (opts.signal) {
      if (opts.signal.aborted) {
        child.kill("SIGTERM");
      } else {
        const onAbort = (): void => {
          child.kill("SIGTERM");
        };
        opts.signal.addEventListener("abort", onAbort, { once: true });
        removeAbortListener = (): void => {
          opts.signal!.removeEventListener("abort", onAbort);
        };
      }
    }
  });
}

/**
 * Stream-based counterpart to {@link runRocky}. Used for subcommands whose
 * stdout can exceed `execFile`'s 16 MiB `maxBuffer` cap — `rocky catalog`,
 * `rocky history`, `rocky optimize`, and `rocky discover` against a real
 * warehouse all reproducibly cross this line.
 *
 * The contract matches {@link runRocky} exactly: identical `RockyResult`
 * shape on success, identical {@link RockyCliError} shape on failure
 * (including `stdout` captured on non-zero exit, so callers like the doctor
 * handler still recover their JSON payload). Timeout, abort-signal, and
 * output-channel logging all behave the same.
 */
function runRockyUnbounded(
  args: string[],
  opts: RunRockyOptions = {},
): Promise<RockyResult> {
  const { serverPath } = getConfig();
  const cwd = opts.cwd ?? getWorkspaceFolder();
  const channel = getOutputChannel();
  const start = Date.now();

  channel.appendLine(`$ ${serverPath} ${args.join(" ")}`);

  return new Promise((resolve, reject) => {
    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    let removeAbortListener: (() => void) | undefined;
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    let timedOut = false;

    const child = spawn(serverPath, args, { cwd });

    const settle = (
      err: (Error & { code?: number | string }) | null,
      exitCode: number | string | null,
    ): void => {
      if (settled) return;
      settled = true;
      removeAbortListener?.();
      if (timeoutHandle) clearTimeout(timeoutHandle);

      const stdout = Buffer.concat(stdoutChunks).toString("utf8");
      const stderr = Buffer.concat(stderrChunks).toString("utf8");
      const elapsed = Date.now() - start;

      if (err) {
        if (stderr) channel.appendLine(stderr.trimEnd());
        channel.appendLine(
          `[${elapsed}ms] command failed (exit ${exitCode ?? "?"}): ${err.message}`,
        );
        reject(
          new RockyCliError(
            err.message,
            stderr,
            exitCode,
            err,
            stdout,
          ),
        );
        return;
      }
      channel.appendLine(`[${elapsed}ms] ok`);
      resolve({ stdout, stderr });
    };

    child.stdout?.on("data", (chunk: Buffer) => {
      stdoutChunks.push(chunk);
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderrChunks.push(chunk);
    });
    child.on("error", (err) => {
      // spawn failure (e.g. ENOENT) — surfaces here instead of via `close`.
      settle(err as Error & { code?: string }, null);
    });
    child.on("close", (code, signal) => {
      if (timedOut) {
        const err = Object.assign(new Error("Command timed out"), {
          code: "ETIMEDOUT",
        });
        settle(err, code ?? signal ?? null);
        return;
      }
      if (code === 0) {
        settle(null, 0);
        return;
      }
      const exitCode = code ?? signal ?? null;
      const err = Object.assign(
        new Error(`Command failed with exit code ${exitCode ?? "?"}`),
        { code: exitCode ?? undefined },
      );
      settle(err, exitCode);
    });

    if (opts.timeoutMs && opts.timeoutMs > 0) {
      timeoutHandle = setTimeout(() => {
        timedOut = true;
        child.kill("SIGTERM");
      }, opts.timeoutMs);
    }

    if (opts.signal) {
      if (opts.signal.aborted) {
        child.kill("SIGTERM");
      } else {
        const onAbort = (): void => {
          child.kill("SIGTERM");
        };
        opts.signal.addEventListener("abort", onAbort, { once: true });
        removeAbortListener = (): void => {
          opts.signal!.removeEventListener("abort", onAbort);
        };
      }
    }
  });
}

/**
 * Spawn rocky and parse its stdout as JSON. Throws {@link RockyCliError} when
 * the CLI fails or the output is not valid JSON.
 */
export async function runRockyJson<T = unknown>(
  args: string[],
  opts: RunRockyOptions = {},
): Promise<T> {
  const { stdout } = await runRocky(args, opts);
  try {
    return JSON.parse(stdout) as T;
  } catch (err) {
    throw new RockyCliError(
      `Rocky CLI returned malformed JSON: ${(err as Error).message}`,
      "",
      0,
      err as Error,
      stdout,
      "parse",
    );
  }
}

/**
 * Wrap {@link runRocky} in a cancellable progress notification. The notification
 * cancel button propagates as an `AbortSignal` to the underlying process.
 */
export async function runRockyWithProgress(
  title: string,
  args: string[],
  opts: RunRockyOptions = {},
): Promise<RockyResult> {
  return vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title,
      cancellable: true,
    },
    async (_progress, token) => {
      const controller = new AbortController();
      token.onCancellationRequested(() => controller.abort());
      return runRocky(args, { ...opts, signal: controller.signal });
    },
  );
}

/** {@link runRockyWithProgress} + JSON parsing. */
export async function runRockyJsonWithProgress<T = unknown>(
  title: string,
  args: string[],
  opts: RunRockyOptions = {},
): Promise<T> {
  const { stdout } = await runRockyWithProgress(title, args, opts);
  try {
    return JSON.parse(stdout) as T;
  } catch (err) {
    throw new RockyCliError(
      `Rocky CLI returned malformed JSON: ${(err as Error).message}`,
      "",
      0,
      err as Error,
      stdout,
      "parse",
    );
  }
}

interface VersionCache {
  value: string | undefined;
  fetchedAt: number;
}

let versionCache: VersionCache | undefined;
const VERSION_TTL_MS = 60_000;

/**
 * Returns the installed Rocky CLI version string (e.g., `"1.26.0"`), or
 * `undefined` when the binary is missing or fails to respond. The result is
 * cached for 60 seconds so repeated tree refreshes don't fork+exec on every
 * tick. Errors are swallowed — this helper is for display only.
 */
export async function getCliVersion(): Promise<string | undefined> {
  const now = Date.now();
  if (versionCache && now - versionCache.fetchedAt < VERSION_TTL_MS) {
    return versionCache.value;
  }
  let value: string | undefined;
  try {
    const { stdout } = await runRocky(["--version"], { timeoutMs: 5_000 });
    value = parseVersion(stdout);
  } catch {
    value = undefined;
  }
  versionCache = { value, fetchedAt: now };
  return value;
}

/** Force the next {@link getCliVersion} call to re-shell. */
export function clearCliVersionCache(): void {
  versionCache = undefined;
}

function parseVersion(stdout: string): string | undefined {
  const match = stdout.trim().match(/(\d+\.\d+\.\d+(?:[-.][\w.+-]+)?)/);
  return match?.[1];
}

/**
 * Display a Rocky CLI error to the user with a "Show Logs" action that opens
 * the output channel containing the full stderr trace.
 */
export function showRockyError(prefix: string, err: unknown): void {
  const channel = getOutputChannel();
  if (err instanceof RockyCliError) {
    const message = err.stderr.trim() || err.message;
    vscode.window
      .showErrorMessage(`${prefix}: ${message}`, "Show Logs")
      .then((choice) => {
        if (choice === "Show Logs") channel.show();
      });
    return;
  }
  const message = (err as Error)?.message ?? String(err);
  vscode.window.showErrorMessage(`${prefix}: ${message}`);
}
