import * as fs from "node:fs";
import * as path from "node:path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";
import { getConfig } from "./config";
import { getOutputChannel } from "./output";

let client: LanguageClient | undefined;
let statusBarItem: vscode.StatusBarItem;

interface DiagnosticTotals {
  errors: number;
  warnings: number;
}

export function getClient(): LanguageClient | undefined {
  return client;
}

/**
 * Creates the status bar, registers diagnostic + config-change listeners, and
 * kicks off the initial LSP launch in the background.
 *
 * Sync return mirrors the original `activate()` behavior — extension activation
 * does not block on the language server connecting.
 */
export function startLspClient(context: vscode.ExtensionContext): void {
  statusBarItem = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Left,
    0,
  );
  statusBarItem.command = "rocky.restartServer";
  statusBarItem.text = "$(loading~spin) Rocky: Starting...";
  statusBarItem.tooltip = "Click to restart Rocky language server";
  statusBarItem.show();
  context.subscriptions.push(statusBarItem);

  context.subscriptions.push(
    vscode.languages.onDidChangeDiagnostics(updateStatusBarFromDiagnostics),
  );

  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (
        event.affectsConfiguration("rocky.server.path") ||
        event.affectsConfiguration("rocky.server.extraArgs") ||
        event.affectsConfiguration("rocky.inlayHints.enabled")
      ) {
        getOutputChannel().appendLine(
          "Rocky configuration changed; restarting language server...",
        );
        void restartLspClient();
      }
    }),
  );

  void launchClient();
}

async function launchClient(): Promise<void> {
  const cfg = getConfig();

  // §P3.11 — prefer the standalone `rocky-lsp` binary when it's
  // installed next to `rocky`. It's ~6 MB instead of ~47 MB and skips
  // loading the whole adapter graph, so fork+exec is faster.
  // Resolution rules:
  //   • if the user set an explicit `rocky.server.path` to a full path,
  //     look for `rocky-lsp` in the same directory and use it if present.
  //   • if serverPath is just "rocky" (the default), leave PATH-based
  //     resolution to the OS by using the literal "rocky-lsp" command.
  //     The OS returns ENOENT if absent; we fall back to `rocky lsp`.
  const lspBin = resolveLspBinary(cfg.serverPath);
  const serverOptions: ServerOptions = lspBin
    ? {
        command: lspBin,
        args: cfg.extraArgs,
        transport: TransportKind.stdio,
      }
    : {
        command: cfg.serverPath,
        args: ["lsp", ...cfg.extraArgs],
        transport: TransportKind.stdio,
      };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [
      { scheme: "file", language: "rocky" },
      { scheme: "file", language: "sql", pattern: "**/models/**/*.sql" },
    ],
    synchronize: {
      fileEvents: [
        vscode.workspace.createFileSystemWatcher("**/*.rocky"),
        vscode.workspace.createFileSystemWatcher("**/*.toml"),
        vscode.workspace.createFileSystemWatcher("**/models/**/*.sql"),
      ],
    },
    initializationOptions: {
      inlayHints: { enabled: cfg.inlayHintsEnabled },
    },
  };

  client = new LanguageClient(
    "rocky",
    "Rocky Language Server",
    serverOptions,
    clientOptions,
  );

  try {
    await client.start();
    statusBarItem.text = "$(check) Rocky: Ready";
    statusBarItem.backgroundColor = undefined;
  } catch (err) {
    handleStartupFailure(err as Error);
  }
}

/**
 * Resolve the standalone `rocky-lsp` binary when the user hasn't forced a
 * specific path. Returns `undefined` if we can't confirm its presence —
 * the caller falls back to `rocky lsp` in that case.
 *
 * Only returns a path we've verified exists on disk, so startup never
 * falls into a "command not found" failure purely because of this
 * optimisation. Users without rocky-lsp installed see no behaviour
 * change.
 */
function resolveLspBinary(serverPath: string): string | undefined {
  const candidateNames =
    process.platform === "win32"
      ? ["rocky-lsp.exe", "rocky-lsp"]
      : ["rocky-lsp"];

  // Case 1: user set an explicit path like `/opt/rocky/bin/rocky` —
  // look for a sibling `rocky-lsp` in the same directory.
  if (serverPath !== "rocky" && serverPath.includes(path.sep)) {
    const dir = path.dirname(serverPath);
    for (const name of candidateNames) {
      const full = path.join(dir, name);
      if (isExecutableFile(full)) {
        return full;
      }
    }
    return undefined;
  }

  // Case 2: default "rocky" (or a bare name) — walk PATH and return
  // the first rocky-lsp we find. Keeps startup synchronous and avoids
  // the "spawn ENOENT" trap that would hit users without the split
  // binary installed.
  const pathEnv = process.env.PATH ?? "";
  const pathSep = process.platform === "win32" ? ";" : ":";
  for (const dir of pathEnv.split(pathSep)) {
    if (!dir) continue;
    for (const name of candidateNames) {
      const full = path.join(dir, name);
      if (isExecutableFile(full)) {
        return full;
      }
    }
  }
  return undefined;
}

function isExecutableFile(p: string): boolean {
  try {
    return fs.statSync(p).isFile();
  } catch {
    return false;
  }
}

function handleStartupFailure(err: Error): void {
  const channel = getOutputChannel();
  const cfg = getConfig();
  channel.appendLine(`Failed to start Rocky language server: ${err.message}`);

  statusBarItem.text = "$(error) Rocky: Failed";
  statusBarItem.backgroundColor = new vscode.ThemeColor(
    "statusBarItem.errorBackground",
  );

  const isMissingBinary =
    /ENOENT/i.test(err.message) ||
    /not found/i.test(err.message) ||
    /no such file/i.test(err.message);

  const message = isMissingBinary
    ? `Rocky CLI not found at "${cfg.serverPath}". Install Rocky or set rocky.server.path.`
    : `Rocky language server failed to start: ${err.message}`;

  const actions = isMissingBinary
    ? ["Configure Path", "Show Logs"]
    : ["Show Logs"];

  vscode.window.showErrorMessage(message, ...actions).then((choice) => {
    if (choice === "Configure Path") {
      void vscode.commands.executeCommand(
        "workbench.action.openSettings",
        "rocky.server.path",
      );
    } else if (choice === "Show Logs") {
      channel.show();
    }
  });
}

export async function restartLspClient(): Promise<void> {
  statusBarItem.text = "$(loading~spin) Rocky: Restarting...";
  if (client) {
    try {
      await client.stop();
    } catch (err) {
      getOutputChannel().appendLine(
        `Error stopping language server: ${(err as Error).message}`,
      );
    }
  }
  await launchClient();
  if (client?.isRunning?.()) {
    vscode.window.showInformationMessage("Rocky language server restarted.");
  }
}

export async function stopLspClient(): Promise<void> {
  await client?.stop();
  client = undefined;
}

function updateStatusBarFromDiagnostics(): void {
  if (!client) return;
  const totals = collectDiagnosticTotals();

  if (totals.errors === 0 && totals.warnings === 0) {
    statusBarItem.text = "$(check) Rocky: Ready";
    statusBarItem.backgroundColor = undefined;
    statusBarItem.tooltip = "Click to restart Rocky language server";
    return;
  }

  const segments: string[] = [];
  if (totals.errors > 0) {
    segments.push(`${totals.errors} error${totals.errors === 1 ? "" : "s"}`);
  }
  if (totals.warnings > 0) {
    segments.push(
      `${totals.warnings} warning${totals.warnings === 1 ? "" : "s"}`,
    );
  }
  const icon = totals.errors > 0 ? "$(error)" : "$(warning)";
  statusBarItem.text = `${icon} Rocky: ${segments.join(", ")}`;
  statusBarItem.backgroundColor =
    totals.errors > 0
      ? new vscode.ThemeColor("statusBarItem.errorBackground")
      : new vscode.ThemeColor("statusBarItem.warningBackground");
  statusBarItem.tooltip = buildStatusTooltip();
}

function collectDiagnosticTotals(): DiagnosticTotals {
  let errors = 0;
  let warnings = 0;
  for (const [, diags] of vscode.languages.getDiagnostics()) {
    for (const d of diags) {
      if (d.source !== "rocky") continue;
      if (d.severity === vscode.DiagnosticSeverity.Error) errors++;
      else if (d.severity === vscode.DiagnosticSeverity.Warning) warnings++;
    }
  }
  return { errors, warnings };
}

function buildStatusTooltip(): string {
  const lines: string[] = [];
  for (const [uri, diags] of vscode.languages.getDiagnostics()) {
    const rockyDiags = diags.filter((d) => d.source === "rocky");
    const errors = rockyDiags.filter(
      (d) => d.severity === vscode.DiagnosticSeverity.Error,
    ).length;
    const warnings = rockyDiags.filter(
      (d) => d.severity === vscode.DiagnosticSeverity.Warning,
    ).length;
    if (errors === 0 && warnings === 0) continue;
    const name = uri.path.split("/").pop() ?? uri.fsPath;
    const counts: string[] = [];
    if (errors > 0) counts.push(`${errors}E`);
    if (warnings > 0) counts.push(`${warnings}W`);
    lines.push(`${name}: ${counts.join(" ")}`);
  }
  lines.push("");
  lines.push("Click to restart Rocky language server");
  return lines.join("\n");
}
