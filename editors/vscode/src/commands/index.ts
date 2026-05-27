import * as vscode from "vscode";
import { restartLspClient } from "../lspClient";
import { aiExplain, aiGenerate, aiSync, aiTest } from "./ai";
import { branchApprove, branchPromote } from "./branch";
import { commandPalette } from "./commandPalette";
import { ci, compile, validate } from "./compile";
import { showDag } from "./dag";
import { devtools } from "./devtools";
import { hooksList, hooksTest } from "./hooks";
import {
  init,
  openDocumentation,
  openOutputChannel,
  playground,
  reportBug,
  viewMarketplace,
} from "./info";
import { compliance } from "./governance";
import { catalog, history, metrics } from "./inspect";
import { showLineage } from "./lineage";
import { lineageDiff } from "./lineageDiff";
import { importDbt, validateMigration } from "./migration";
import { doctor, optimize } from "./ops";
import { previewCost, previewCreate, previewDiff } from "./preview";
import { previewCte, previewModel } from "./previewRows";
import { reviewPlan } from "./review";
import { compare, discover, plan, run } from "./run";
import { archive, compact, profileStorage } from "./storage";
import { trace } from "./trace";
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
    vscode.commands.registerCommand("rocky.lineageDiff", lineageDiff),

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

    // Governance
    vscode.commands.registerCommand("rocky.compliance", compliance),

    // Execution plan (topological run schedule)
    vscode.commands.registerCommand("rocky.dag", showDag),

    // Run trace (timeline of the latest run)
    vscode.commands.registerCommand("rocky.trace", trace),

    // Preview (PR-bundle)
    vscode.commands.registerCommand("rocky.previewCreate", previewCreate),
    vscode.commands.registerCommand("rocky.previewDiff", previewDiff),
    vscode.commands.registerCommand("rocky.previewCost", previewCost),

    // Inline row preview → Query Results panel
    vscode.commands.registerCommand("rocky.previewModel", previewModel),
    vscode.commands.registerCommand("rocky.previewCte", previewCte),

    // Branch approval / promote — gated production writes from a branch
    vscode.commands.registerCommand("rocky.branchApprove", branchApprove),
    vscode.commands.registerCommand("rocky.branchPromote", branchPromote),

    // Plan review/apply safety gate (breaking-change report + approve + apply)
    vscode.commands.registerCommand("rocky.reviewPlan", reviewPlan),

    // Sidebar / Get Started utilities
    vscode.commands.registerCommand("rocky.openOutputChannel", openOutputChannel),
    vscode.commands.registerCommand("rocky.openDocumentation", openDocumentation),
    vscode.commands.registerCommand("rocky.reportBug", reportBug),
    vscode.commands.registerCommand("rocky.viewMarketplace", viewMarketplace),
    vscode.commands.registerCommand("rocky.init", init),
    vscode.commands.registerCommand("rocky.playground", playground),

    // Webview foundation smoke test (React + ReactFlow toolchain)
    vscode.commands.registerCommand("rocky.devtools", devtools),
  );
}
