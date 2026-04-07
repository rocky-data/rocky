import * as vscode from "vscode";
import { runRockyJsonWithProgress, showRockyError } from "../rockyCli";
import type {
  CompareResult,
  DiscoverResult,
  PlanResult,
  RunResult,
} from "../types/rockyJson";
import { showRunSummary } from "../webviews/runSummary";
import {
  confirmAction,
  ensureWorkspace,
  promptForInput,
  showJsonInEditor,
  showSqlInEditor,
} from "./ui";

const FILTER_PLACEHOLDER = "e.g., client=acme";

export async function run(): Promise<void> {
  if (!ensureWorkspace()) return;

  const filter = await promptForInput("Pipeline filter (key=value)", {
    placeHolder: FILTER_PLACEHOLDER,
    required: true,
  });
  if (!filter) return;

  const confirmed = await confirmAction(
    `Run Rocky pipeline with filter "${filter}"?`,
    "Run",
    "This executes against your warehouse and may take several minutes.",
  );
  if (!confirmed) return;

  try {
    const result = await runRockyJsonWithProgress<RunResult>(
      `Running pipeline (${filter})...`,
      ["run", "--filter", filter, "--output", "json"],
      { timeoutMs: 0 }, // disable hard timeout — runs can be long
    );

    const copied = result.tables_copied ?? 0;
    const failed = result.tables_failed ?? 0;
    const skipped = result.tables_skipped ?? 0;
    const drifted = result.drift?.tables_drifted ?? 0;
    const summary = `Rocky run: ${copied} copied, ${failed} failed, ${skipped} skipped, ${drifted} drifted`;

    if (failed > 0) {
      vscode.window.showErrorMessage(summary);
    } else {
      vscode.window.showInformationMessage(summary);
    }
    showRunSummary(result);
  } catch (err) {
    showRockyError("Run failed", err);
  }
}

export async function plan(): Promise<void> {
  if (!ensureWorkspace()) return;

  const filter = await promptForInput("Pipeline filter (key=value)", {
    placeHolder: FILTER_PLACEHOLDER,
    required: true,
  });
  if (!filter) return;

  try {
    const result = await runRockyJsonWithProgress<PlanResult>(
      `Planning pipeline (${filter})...`,
      ["plan", "--filter", filter, "--output", "json"],
      { timeoutMs: 60000 },
    );

    const statements = result.statements ?? [];
    if (statements.length === 0) {
      vscode.window.showInformationMessage("Rocky plan: no statements.");
      return;
    }

    const sql = statements
      .map(
        (s) =>
          `-- [${s.purpose ?? "?"}] ${s.target ?? ""}\n${(s.sql ?? "").trim()};`,
      )
      .join("\n\n");

    await showSqlInEditor(sql);
  } catch (err) {
    showRockyError("Plan failed", err);
  }
}

export async function discover(): Promise<void> {
  if (!ensureWorkspace()) return;

  try {
    const result = await runRockyJsonWithProgress<DiscoverResult>(
      "Discovering sources...",
      ["discover", "--output", "json"],
      { timeoutMs: 120000 },
    );

    const sources = result.sources ?? [];
    const totalTables = sources.reduce(
      (sum, s) => sum + (s.tables?.length ?? 0),
      0,
    );
    vscode.window.showInformationMessage(
      `Rocky discover: ${sources.length} source${sources.length === 1 ? "" : "s"}, ${totalTables} table${totalTables === 1 ? "" : "s"}.`,
    );
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Discover failed", err);
  }
}

export async function compare(): Promise<void> {
  if (!ensureWorkspace()) return;

  const filter = await promptForInput("Pipeline filter (key=value)", {
    placeHolder: FILTER_PLACEHOLDER,
    required: true,
  });
  if (!filter) return;

  try {
    const result = await runRockyJsonWithProgress<CompareResult>(
      `Comparing shadow vs production (${filter})...`,
      ["compare", "--filter", filter, "--output", "json"],
      { timeoutMs: 0 },
    );

    const verdict = result.overall_verdict ?? "unknown";
    const summary = `Rocky compare: ${verdict} — ${result.tables_passed ?? 0} passed, ${result.tables_warned ?? 0} warned, ${result.tables_failed ?? 0} failed`;

    if (verdict === "fail") {
      vscode.window.showErrorMessage(summary);
    } else if (verdict === "warn") {
      vscode.window.showWarningMessage(summary);
    } else {
      vscode.window.showInformationMessage(summary);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("Compare failed", err);
  }
}
