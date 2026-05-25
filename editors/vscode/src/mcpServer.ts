import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";
import { getConfig } from "./config";

// ---------------------------------------------------------------------------
// Rocky MCP server registration
//
// Registers `rocky mcp` as a Model Context Protocol stdio server so VS Code's
// agent mode can drive Rocky through the engine's typed read-only tools
// (compile, lineage, inspect_schema, sample_rows, breaking_change, catalog,
// history, metrics, optimize, …). The tool surface is defined once, in the
// Rust server — the extension only points the editor at the binary, so the
// editor's AI stays in lockstep with whatever the installed `rocky` exposes.
//
// This complements the `@rocky` chat participant (which keeps its four
// single-shot slash commands): agent mode gets the full typed toolset.
// ---------------------------------------------------------------------------

/** The contribution id; must match `contributes.mcpServerDefinitionProviders` in package.json. */
const PROVIDER_ID = "rocky-data.rocky-mcp";

/** A vscode-free description of one `rocky mcp` stdio server to launch. */
export interface RockyMcpServerSpec {
  /** Human-readable server name shown in the editor's MCP UI. */
  label: string;
  /** The `rocky` binary to launch. */
  command: string;
  /** Arguments: `mcp --config <abs rocky.toml>`. */
  args: string[];
  /** Working directory — the project root, so relative paths in rocky.toml resolve. */
  cwd: string;
}

/**
 * Build a launch spec for every project root that has a `rocky.toml`. Pure (no
 * vscode runtime) so the launch contract is unit-testable. When more than one
 * root is present the label is disambiguated by folder name.
 */
export function buildRockyMcpSpecs(
  serverPath: string,
  roots: string[],
): RockyMcpServerSpec[] {
  const disambiguate = roots.length > 1;
  return roots.map((root) => ({
    label: disambiguate ? `Rocky (${path.basename(root)})` : "Rocky",
    command: serverPath,
    args: ["mcp", "--config", path.join(root, "rocky.toml")],
    cwd: root,
  }));
}

/** Workspace folders whose root holds a `rocky.toml`. */
function findRockyProjectRoots(): string[] {
  const roots: string[] = [];
  for (const folder of vscode.workspace.workspaceFolders ?? []) {
    const root = folder.uri.fsPath;
    if (fs.existsSync(path.join(root, "rocky.toml"))) roots.push(root);
  }
  return roots;
}

/**
 * Register the Rocky MCP server-definition provider so the editor's agent mode
 * discovers `rocky mcp` for every Rocky project in the workspace.
 *
 * The MCP definition-provider API is finalized in VS Code 1.101+; a host (or a
 * test harness) that predates it won't expose the entry point, so we guard and
 * degrade to a no-op rather than throw on activation.
 */
export function registerRockyMcpProvider(context: vscode.ExtensionContext): void {
  const lm = vscode.lm as typeof vscode.lm & {
    registerMcpServerDefinitionProvider?: (
      id: string,
      provider: vscode.McpServerDefinitionProvider,
    ) => vscode.Disposable;
  };
  if (typeof lm.registerMcpServerDefinitionProvider !== "function") return;

  // `context.extension` is normally present, but optional-chain it so a
  // startup race or stubbed host can't throw on the activation path.
  const version =
    (context.extension?.packageJSON as { version?: string } | undefined)
      ?.version ?? "0.0.0";

  // Re-discover when the workspace folders change or a rocky.toml appears /
  // disappears — keeps the editor's server list current without a reload.
  const didChange = new vscode.EventEmitter<void>();
  context.subscriptions.push(didChange);
  context.subscriptions.push(
    vscode.workspace.onDidChangeWorkspaceFolders(() => didChange.fire()),
  );
  const watcher = vscode.workspace.createFileSystemWatcher("**/rocky.toml");
  watcher.onDidCreate(() => didChange.fire());
  watcher.onDidDelete(() => didChange.fire());
  context.subscriptions.push(watcher);

  const provider: vscode.McpServerDefinitionProvider = {
    onDidChangeMcpServerDefinitions: didChange.event,
    provideMcpServerDefinitions() {
      const { serverPath } = getConfig();
      return buildRockyMcpSpecs(serverPath, findRockyProjectRoots()).map((s) => {
        const def = new vscode.McpStdioServerDefinition(
          s.label,
          s.command,
          s.args,
          {},
          version,
        );
        def.cwd = vscode.Uri.file(s.cwd);
        return def;
      });
    },
  };

  context.subscriptions.push(
    lm.registerMcpServerDefinitionProvider(PROVIDER_ID, provider),
  );
}
