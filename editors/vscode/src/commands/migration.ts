import * as vscode from "vscode";
import {
  runRockyJsonWithProgress,
  runRockyWithProgress,
  showRockyError,
} from "../rockyCli";
import type { ImportDbtResult } from "../types/rockyJson";
import {
  confirmAction,
  ensureWorkspace,
  promptForInput,
  showJsonInEditor,
} from "./ui";

export async function validateMigration(): Promise<void> {
  const dbtProject = await vscode.window.showOpenDialog({
    canSelectFolders: true,
    canSelectFiles: false,
    openLabel: "Select dbt Project",
    title: "Select the dbt project directory to validate against",
  });

  if (!dbtProject || dbtProject.length === 0) return;

  try {
    const { stdout } = await runRockyWithProgress(
      "Validating migration...",
      ["validate-migration", "--dbt-project", dbtProject[0].fsPath],
    );
    await showJsonInEditor(stdout, vscode.ViewColumn.Active);
  } catch (err) {
    showRockyError("Validation failed", err);
  }
}

type ManifestMode = "auto" | "specify" | "regex";

const MANIFEST_MODES: Array<{
  label: string;
  description: string;
  value: ManifestMode;
}> = [
  {
    label: "Auto-detect",
    description: "Read target/manifest.json if present",
    value: "auto",
  },
  {
    label: "Specify path…",
    description: "Pick a manifest.json explicitly",
    value: "specify",
  },
  {
    label: "Force regex import",
    description: "Skip manifest.json even if available",
    value: "regex",
  },
];

/**
 * Multi-step wizard for `rocky import-dbt`. Native QuickInput steps; the back
 * button is supported for free at the dbt-folder selection (showOpenDialog is
 * always the first step).
 */
export async function importDbt(): Promise<void> {
  if (!ensureWorkspace()) return;

  // Step 1 — dbt project folder.
  const dbtProject = await vscode.window.showOpenDialog({
    canSelectFolders: true,
    canSelectFiles: false,
    openLabel: "Select dbt Project",
    title: "Select the dbt project directory to import",
  });
  if (!dbtProject || dbtProject.length === 0) return;
  const dbtPath = dbtProject[0].fsPath;

  // Step 2 — output directory.
  const output = await promptForInput("Output directory for Rocky models", {
    placeHolder: "e.g., models",
    value: "models",
    required: true,
  });
  if (!output) return;

  // Step 3 — manifest mode.
  const modePick = await vscode.window.showQuickPick(MANIFEST_MODES, {
    placeHolder: "How should manifest.json be located?",
  });
  if (!modePick) return;

  let manifestPath: string | undefined;
  if (modePick.value === "specify") {
    const picked = await vscode.window.showOpenDialog({
      canSelectFiles: true,
      canSelectFolders: false,
      filters: { JSON: ["json"] },
      openLabel: "Select manifest.json",
      title: "Select the dbt manifest.json",
    });
    if (!picked || picked.length === 0) return;
    manifestPath = picked[0].fsPath;
  }

  // Step 4 — confirm.
  const confirmed = await confirmAction(
    `Import dbt project into "${output}"?`,
    "Import",
    `Source: ${dbtPath}\nManifest: ${
      modePick.value === "regex"
        ? "regex (forced)"
        : manifestPath ?? "auto-detect"
    }`,
  );
  if (!confirmed) return;

  // Step 5 — run.
  const args = [
    "import-dbt",
    "--dbt-project",
    dbtPath,
    "--output-dir",
    output,
  ];
  if (modePick.value === "regex") args.push("--no-manifest");
  if (manifestPath) args.push("--manifest", manifestPath);

  try {
    const result = await runRockyJsonWithProgress<ImportDbtResult>(
      "Importing dbt project...",
      args,
      { timeoutMs: 120000 },
    );

    // Round 13: ImportDbtResult is now an alias for the generated
    // ImportDbtOutput, which renames `models_imported` to `imported` and
    // surfaces warnings as a count plus a `warning_details` array.
    const models = result.imported ?? 0;
    const tests = result.tests_converted ?? 0;
    const warnings = result.warnings ?? 0;
    const summary = `Imported ${models} model${models === 1 ? "" : "s"}, ${tests} test${tests === 1 ? "" : "s"}${warnings > 0 ? `, ${warnings} warning${warnings === 1 ? "" : "s"}` : ""}.`;
    if (warnings > 0) {
      vscode.window.showWarningMessage(`Rocky dbt import: ${summary}`);
    } else {
      vscode.window.showInformationMessage(`Rocky dbt import: ${summary}`);
    }
    await showJsonInEditor(JSON.stringify(result));
  } catch (err) {
    showRockyError("dbt import failed", err);
  }
}
