import { execFile } from "child_process";
import type { ExecFileException } from "child_process";
import * as vscode from "vscode";
import { getConfig, getWorkspaceFolder } from "./config";
import { getOutputChannel } from "./output";

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
 */
export class RockyCliError extends Error {
  constructor(
    message: string,
    public readonly stderr: string,
    public readonly exitCode: number | string | null,
    cause?: Error,
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
 */
export function runRocky(
  args: string[],
  opts: RunRockyOptions = {},
): Promise<RockyResult> {
  const { serverPath } = getConfig();
  const cwd = opts.cwd ?? getWorkspaceFolder();
  const channel = getOutputChannel();
  const start = Date.now();

  channel.appendLine(`$ ${serverPath} ${args.join(" ")}`);

  return new Promise((resolve, reject) => {
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
        const elapsed = Date.now() - start;
        if (err) {
          if (stderr) channel.appendLine(stderr.trimEnd());
          channel.appendLine(
            `[${elapsed}ms] command failed (exit ${err.code ?? "?"}): ${err.message}`,
          );
          reject(
            new RockyCliError(err.message, stderr ?? "", err.code ?? null, err),
          );
          return;
        }
        channel.appendLine(`[${elapsed}ms] ok`);
        resolve({ stdout, stderr });
      },
    );

    if (opts.signal) {
      const onAbort = (): void => {
        child.kill("SIGTERM");
      };
      if (opts.signal.aborted) {
        child.kill("SIGTERM");
      } else {
        opts.signal.addEventListener("abort", onAbort, { once: true });
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
      `failed to parse rocky JSON output: ${(err as Error).message}`,
      stdout,
      0,
      err as Error,
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
      `failed to parse rocky JSON output: ${(err as Error).message}`,
      stdout,
      0,
      err as Error,
    );
  }
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
