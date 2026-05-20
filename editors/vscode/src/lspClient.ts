import * as fs from "node:fs";
import * as path from "node:path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  State,
  TransportKind,
} from "vscode-languageclient/node";
import { getConfig } from "./config";
import { getOutputChannel } from "./output";

let client: LanguageClient | undefined;
let statusBarItem: vscode.StatusBarItem;

/** Disposable for the current client's `onDidChangeState` subscription. */
let stateSubscription: vscode.Disposable | undefined;

/**
 * `true` while we're deliberately stopping the client as part of a restart
 * or shutdown. Suppresses the crash handler so a normal Running→Stopped
 * transition doesn't flap the status bar into a "Failed" state.
 */
let expectedStop = false;

/**
 * In-flight restart promise. While set, additional `restartLspClient()` calls
 * await the same promise instead of racing a second `client.stop()` /
 * `new LanguageClient(...)` pair.
 */
let pendingRestart: Promise<void> | undefined;

interface DiagnosticTotals {
  errors: number;
  warnings: number;
}

// ---------------------------------------------------------------------------
// Status bar segment cache — updated lazily, never on every keystroke.
// ---------------------------------------------------------------------------

interface StatusBarSegmentCache {
  warehouse?: string;
  lastRunAge?: string;
  driftCount?: number;
  branchState?: string;
  /** Timestamp (ms) of the last cache refresh. */
  refreshedAt: number;
}

let segmentCache: StatusBarSegmentCache = { refreshedAt: 0 };

/** Minimum interval between segment-cache refreshes (30 seconds). */
const SEGMENT_CACHE_TTL_MS = 30_000;

/**
 * Returns the list of active status bar segment IDs from settings.
 * Returns an empty array when the setting is absent or empty (opt-in).
 */
function getActiveSegments(): string[] {
  return vscode.workspace
    .getConfiguration("rocky")
    .get<string[]>("statusBar.segments", []);
}

/**
 * Refresh the segment cache from persistent Rocky state files in the
 * workspace.  This is intentionally cheap — it only reads local files,
 * never spawns a CLI process.  The cache is considered fresh for
 * {@link SEGMENT_CACHE_TTL_MS} ms to avoid hammering the filesystem on
 * every `onDidChangeDiagnostics` event.
 */
function maybeRefreshSegmentCache(): void {
  const now = Date.now();
  if (now - segmentCache.refreshedAt < SEGMENT_CACHE_TTL_MS) return;
  segmentCache.refreshedAt = now;

  const segments = getActiveSegments();
  if (segments.length === 0) return;

  const folder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  if (!folder) return;

  // Warehouse: read from rocky.toml if present (simple regex — no TOML parser).
  if (segments.includes("warehouse")) {
    try {
      const tomlPath = path.join(folder, "rocky.toml");
      const tomlContent = fs.readFileSync(tomlPath, "utf8");
      const match = /type\s*=\s*["']?(\w+)["']?/.exec(tomlContent);
      segmentCache.warehouse = match?.[1];
    } catch {
      segmentCache.warehouse = undefined;
    }
  }

  // Branch state: read from `.rocky-state/branch` if it exists.
  if (segments.includes("branchState")) {
    try {
      const branchFile = path.join(folder, ".rocky-state", "branch");
      segmentCache.branchState = fs.readFileSync(branchFile, "utf8").trim();
    } catch {
      segmentCache.branchState = undefined;
    }
  }
}

/** Format seconds-since-last-run as a human-readable age string. */
function formatRunAge(lastRunAt: string | undefined): string | undefined {
  if (!lastRunAt) return undefined;
  try {
    const ms = Date.now() - new Date(lastRunAt).getTime();
    if (ms < 0) return undefined;
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  } catch {
    return undefined;
  }
}

/**
 * Build the optional enrichment suffix for the status bar text.
 * Each segment silently no-ops when its cached value is unavailable.
 */
function buildSegmentSuffix(): string {
  const segments = getActiveSegments();
  if (segments.length === 0) return "";

  maybeRefreshSegmentCache();

  const parts: string[] = [];

  if (segments.includes("warehouse") && segmentCache.warehouse) {
    parts.push(segmentCache.warehouse);
  }

  if (segments.includes("lastRunAge") && segmentCache.lastRunAge) {
    const age = formatRunAge(segmentCache.lastRunAge);
    if (age) parts.push(age);
  }

  if (
    segments.includes("driftCount") &&
    typeof segmentCache.driftCount === "number"
  ) {
    parts.push(`${segmentCache.driftCount} drift`);
  }

  if (segments.includes("branchState") && segmentCache.branchState) {
    parts.push(`$(git-branch) ${segmentCache.branchState}`);
  }

  return parts.length > 0 ? ` · ${parts.join(" · ")}` : "";
}

/**
 * Current lifecycle state of the Rocky language server. Independent of
 * diagnostics — those flow separately through the status bar.
 */
export type LspStatus =
  | "Starting"
  | "Restarting"
  | "Ready"
  | "Stopped"
  | "Failed";

export interface LspState {
  status: LspStatus;
  /** Last error message if `status === "Failed"`, otherwise undefined. */
  error?: string;
}

let currentLspState: LspState = { status: "Stopped" };
const lspStateEmitter = new vscode.EventEmitter<LspState>();

/** Fires whenever the language server transitions between lifecycle states. */
export const onDidChangeLspState: vscode.Event<LspState> =
  lspStateEmitter.event;

/** Returns the most recent lifecycle state. */
export function getLspState(): LspState {
  return currentLspState;
}

function setLspState(status: LspStatus, error?: string): void {
  currentLspState = error !== undefined ? { status, error } : { status };
  lspStateEmitter.fire(currentLspState);
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

  setLspState("Starting");

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
      // Invalidate the segment cache when the segments setting changes so the
      // status bar updates immediately without waiting for the next TTL.
      if (event.affectsConfiguration("rocky.statusBar.segments")) {
        segmentCache = { refreshedAt: 0 };
        updateStatusBarFromDiagnostics();
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

  // Dispose any previous state listener before subscribing on the new client
  // — without this, listeners accumulate across every restart.
  stateSubscription?.dispose();
  stateSubscription = client.onDidChangeState((event) => {
    handleStateChange(event.oldState, event.newState);
  });

  try {
    await client.start();
    statusBarItem.text = `$(check) Rocky: Ready${buildSegmentSuffix()}`;
    statusBarItem.backgroundColor = undefined;
    setLspState("Ready");
  } catch (err) {
    handleStartupFailure(err as Error);
  }
}

/**
 * React to LSP state transitions. The vscode-languageclient state machine
 * is `Stopped (1) | Running (2) | Starting (3)`. A Running→Stopped flip
 * outside a deliberate restart/shutdown means the server crashed; surface
 * it on the status bar and offer a Restart action.
 */
function handleStateChange(_oldState: State, newState: State): void {
  // During a deliberate stop or restart we own the status bar text and
  // {@link setLspState}, so silently ignore the transition.
  if (expectedStop) return;
  if (newState !== State.Stopped) return;
  // The startup-failure path already calls {@link setLspState}; don't
  // double-report when start() rejected.
  if (currentLspState.status === "Failed") return;

  const channel = getOutputChannel();
  channel.appendLine(
    "Rocky language server stopped unexpectedly. Diagnostics will not refresh until it is restarted.",
  );

  statusBarItem.text = "$(error) Rocky: Stopped";
  statusBarItem.backgroundColor = new vscode.ThemeColor(
    "statusBarItem.errorBackground",
  );
  statusBarItem.tooltip = "Rocky language server stopped — click to restart";

  setLspState("Failed", "Language server stopped unexpectedly");

  vscode.window
    .showErrorMessage(
      "Rocky language server stopped unexpectedly.",
      "Restart",
      "Show Logs",
    )
    .then((choice) => {
      if (choice === "Restart") {
        void restartLspClient();
      } else if (choice === "Show Logs") {
        channel.show();
      }
    });
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

  setLspState("Failed", err.message);

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
  // Serialize concurrent restarts. Without this guard, two near-simultaneous
  // config changes can each call `await client.stop()` (fast on an already-
  // stopped client) and then both enter `launchClient()`, where they assign
  // `client = new LanguageClient(...)` back-to-back. The first instance is
  // orphaned but its child process keeps running.
  if (pendingRestart) {
    return pendingRestart;
  }
  pendingRestart = doRestart();
  try {
    await pendingRestart;
  } finally {
    pendingRestart = undefined;
  }
}

async function doRestart(): Promise<void> {
  statusBarItem.text = "$(loading~spin) Rocky: Restarting...";
  setLspState("Restarting");
  if (client) {
    expectedStop = true;
    try {
      await client.stop();
    } catch (err) {
      getOutputChannel().appendLine(
        `Error stopping language server: ${(err as Error).message}`,
      );
    } finally {
      expectedStop = false;
    }
  }
  await launchClient();
  if (client?.isRunning?.()) {
    vscode.window.showInformationMessage("Rocky language server restarted.");
  }
}

export async function stopLspClient(): Promise<void> {
  expectedStop = true;
  try {
    await client?.stop();
  } finally {
    expectedStop = false;
  }
  stateSubscription?.dispose();
  stateSubscription = undefined;
  client = undefined;
  setLspState("Stopped");
}

function updateStatusBarFromDiagnostics(): void {
  if (!client) return;
  const totals = collectDiagnosticTotals();

  // Keep the driftCount segment in sync with the live diagnostic totals.
  // This is free (no filesystem or CLI work) so we update it every time.
  segmentCache.driftCount =
    totals.errors + totals.warnings > 0
      ? totals.errors + totals.warnings
      : undefined;

  const suffix = buildSegmentSuffix();

  if (totals.errors === 0 && totals.warnings === 0) {
    statusBarItem.text = `$(check) Rocky: Ready${suffix}`;
    statusBarItem.backgroundColor = undefined;
    statusBarItem.tooltip = "Click to restart Rocky language server";
    return;
  }

  const diagSegments: string[] = [];
  if (totals.errors > 0) {
    diagSegments.push(`${totals.errors} error${totals.errors === 1 ? "" : "s"}`);
  }
  if (totals.warnings > 0) {
    diagSegments.push(
      `${totals.warnings} warning${totals.warnings === 1 ? "" : "s"}`,
    );
  }
  const icon = totals.errors > 0 ? "$(error)" : "$(warning)";
  statusBarItem.text = `${icon} Rocky: ${diagSegments.join(", ")}${suffix}`;
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
