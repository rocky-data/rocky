import * as vscode from "vscode";
import { restartLspClient } from "../lspClient";
import { aiExplain, aiGenerate, aiSync, aiTest } from "./ai";
import { branchApprove, branchPromote } from "./branch";
import { commandPalette } from "./commandPalette";
import { ci, compile, validate } from "./compile";
import { hooksList, hooksTest } from "./hooks";
import { catalog, history, metrics } from "./inspect";
import { showLineage } from "./lineage";
import { importDbt, validateMigration } from "./migration";
import { doctor, optimize } from "./ops";
import { previewCost, previewCreate, previewDiff } from "./preview";
import { compare, discover, plan, run } from "./run";
import { archive, compact, profileStorage } from "./storage";
import { test } from "./test";

/**
 * Registers every Rocky command on the extension context. Adding a new command
 * is one line here plus a `commands` entry in package.json.
 */
export function registerCommands(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    // Command palette (Cmd+Shift+R / Ctrl+Shift+R)
    vscode.commands.registerCommand("rocky.commandPalette", commandPalette),

    // LSP lifecycle
    vscode.commands.registerCommand("rocky.restartServer", restartLspClient),

    // Language intelligence
    vscode.commands.registerCommand("rocky.showLineage", showLineage),

    // Compile / validate / CI
    vscode.commands.registerCommand("rocky.compile", compile),
    vscode.commands.registerCommand("rocky.validate", validate),
    vscode.commands.registerCommand("rocky.ci", ci),
    vscode.commands.registerCommand("rocky.test", test),

    // Pipeline execution
    vscode.commands.registerCommand("rocky.run", run),
    vscode.commands.registerCommand("rocky.plan", plan),
    vscode.commands.registerCommand("rocky.discover", discover),
    vscode.commands.registerCommand("rocky.compare", compare),

    // State inspection
    vscode.commands.registerCommand("rocky.history", history),
    vscode.commands.registerCommand("rocky.metrics", metrics),
    vscode.commands.registerCommand("rocky.catalog", catalog),

    // Storage management
    vscode.commands.registerCommand("rocky.compact", compact),
    vscode.commands.registerCommand("rocky.archive", archive),
    vscode.commands.registerCommand("rocky.profileStorage", profileStorage),

    // AI features
    vscode.commands.registerCommand("rocky.aiGenerate", aiGenerate),
    vscode.commands.registerCommand("rocky.aiSync", aiSync),
    vscode.commands.registerCommand("rocky.aiExplain", aiExplain),
    vscode.commands.registerCommand("rocky.aiTest", aiTest),

    // Migration
    vscode.commands.registerCommand("rocky.importDbt", importDbt),
    vscode.commands.registerCommand(
      "rocky.validateMigration",
      validateMigration,
    ),

    // Hooks
    vscode.commands.registerCommand("rocky.hooksList", hooksList),
    vscode.commands.registerCommand("rocky.hooksTest", hooksTest),

    // Health & analysis
    vscode.commands.registerCommand("rocky.doctor", doctor),
    vscode.commands.registerCommand("rocky.optimize", optimize),

    // Preview (PR-bundle)
    vscode.commands.registerCommand("rocky.previewCreate", previewCreate),
    vscode.commands.registerCommand("rocky.previewDiff", previewDiff),
    vscode.commands.registerCommand("rocky.previewCost", previewCost),

    // Branch approval / promote — gated production writes from a branch
    vscode.commands.registerCommand("rocky.branchApprove", branchApprove),
    vscode.commands.registerCommand("rocky.branchPromote", branchPromote),
  );
}
