import * as path from "path";
import * as vscode from "vscode";
import { runRockyJson, RockyCliError } from "./rockyCli";
import type { DeclarativeTestResult, TestOutput } from "./types/generated/test";

const SIDECAR_GLOB = "**/models/**/*.toml";

/** Extended `TestOutput` shape when `--declarative` is used. */
type DeclarativeTestOutput = TestOutput;

// ---------------------------------------------------------------------------
// Lightweight TOML [[tests]] parser
// ---------------------------------------------------------------------------

interface ParsedTest {
  /** e.g. "not_null", "unique", "accepted_values", "row_count_range" */
  type: string;
  /** Column under test, if declared. */
  column?: string;
  /** 0-based line number of the `[[tests]]` header in the sidecar. */
  line: number;
}

/**
 * Extract `[[tests]]` entries from a `.toml` sidecar without a full TOML
 * parser.  The sidecar format is well-defined: each `[[tests]]` block
 * contains `type = "..."` and optionally `column = "..."`.
 */
function parseTestsFromToml(text: string): ParsedTest[] {
  const results: ParsedTest[] = [];
  const lines = text.split("\n");
  let inTestBlock = false;
  let current: Partial<ParsedTest> & { line: number } = { line: 0 };

  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i].trim();

    // Detect start of a [[tests]] block.
    if (trimmed === "[[tests]]") {
      // Flush previous block if it had a type.
      if (inTestBlock && current.type) {
        results.push({
          type: current.type,
          column: current.column,
          line: current.line,
        });
      }
      inTestBlock = true;
      current = { line: i };
      continue;
    }

    // Any other section header ends the current [[tests]] block.
    if (/^\[/.test(trimmed) && trimmed !== "[[tests]]") {
      if (inTestBlock && current.type) {
        results.push({
          type: current.type,
          column: current.column,
          line: current.line,
        });
      }
      inTestBlock = false;
      current = { line: 0 };
      continue;
    }

    if (!inTestBlock) continue;

    // Parse key = "value" lines inside a [[tests]] block.
    const kv = trimmed.match(/^(\w+)\s*=\s*"([^"]*)"/);
    if (!kv) continue;
    const key = kv[1] ?? "";
    const value = kv[2] ?? "";
    if (key === "type" && value) current.type = value;
    if (key === "column" && value) current.column = value;
  }

  // Flush last block.
  if (inTestBlock && current.type) {
    results.push({
      type: current.type,
      column: current.column,
      line: current.line,
    });
  }

  return results;
}

// ---------------------------------------------------------------------------
// Test ID helpers
// ---------------------------------------------------------------------------

/**
 * Build a unique test-item ID.
 *
 * Format: `model:<model>:<type>:<column>` or `model:<model>:<type>` when
 * the test has no column (e.g. `expression`, `row_count_range`).
 */
function testItemId(model: string, type: string, column?: string): string {
  return column
    ? `model:${model}:${type}:${column}`
    : `model:${model}:${type}`;
}

/** Derive the model name from a sidecar URI (strip `.toml` extension). */
function modelNameFor(uri: vscode.Uri): string {
  return path.basename(uri.fsPath, ".toml");
}

// ---------------------------------------------------------------------------
// TestController wiring
// ---------------------------------------------------------------------------

/**
 * Registers a VS Code Test Controller that surfaces per-assertion granularity
 * for Rocky's declarative `[[tests]]` (defined in `.toml` model sidecars).
 *
 * Unlike the companion controller in `testExplorer.ts` (which maps one
 * TestItem per `.rocky` model file and runs local DuckDB tests), this
 * controller:
 *
 * 1. Scans `models/**\/*.toml` for `[[tests]]` entries.
 * 2. Creates a two-level tree: model -> individual test assertions.
 * 3. Runs `rocky test --declarative --model <name> --output json`.
 * 4. Maps each `DeclarativeTestResult` back to its TestItem.
 *
 * Discovery is **lazy**: tests are resolved on demand when the user first
 * expands the test panel, not at extension startup.
 */
export function registerDeclarativeTestProvider(
  context: vscode.ExtensionContext,
): vscode.TestController {
  const controller = vscode.tests.createTestController(
    "rocky-declarative",
    "Rocky Declarative Tests",
  );
  context.subscriptions.push(controller);

  // Lazy discovery: `resolveHandler` is called by VS Code the first time the
  // Test Explorer panel is opened, and again whenever a specific item is
  // expanded.  Passing `undefined` as the item means "resolve roots".
  controller.resolveHandler = async (item) => {
    if (!item) {
      await discoverAllTests(controller);
    }
    // Individual items are fully resolved during discovery — nothing to do
    // for leaf expansion.
  };

  // Refresh handler — wired to the toolbar refresh button.
  controller.refreshHandler = async () => {
    await discoverAllTests(controller);
  };

  // Run profile.
  controller.createRunProfile(
    "Run",
    vscode.TestRunProfileKind.Run,
    (request, token) => runHandler(controller, request, token),
    /* isDefault */ true,
  );

  // Keep the tree in sync as sidecars come and go.
  const watcher = vscode.workspace.createFileSystemWatcher(SIDECAR_GLOB);
  watcher.onDidCreate((uri) => void addOrUpdateSidecar(controller, uri));
  watcher.onDidChange((uri) => void addOrUpdateSidecar(controller, uri));
  watcher.onDidDelete((uri) => removeSidecar(controller, uri));
  context.subscriptions.push(watcher);

  return controller;
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

async function discoverAllTests(
  controller: vscode.TestController,
): Promise<void> {
  const files = await vscode.workspace.findFiles(SIDECAR_GLOB);
  // Rebuild the tree from scratch.
  controller.items.replace([]);

  for (const uri of files) {
    await addOrUpdateSidecar(controller, uri);
  }
}

async function addOrUpdateSidecar(
  controller: vscode.TestController,
  uri: vscode.Uri,
): Promise<void> {
  const doc = await vscode.workspace.openTextDocument(uri);
  const text = doc.getText();
  const tests = parseTestsFromToml(text);

  const model = modelNameFor(uri);

  // Remove any previous entry for this model.
  controller.items.delete(model);

  if (tests.length === 0) return;

  // Create parent item for the model.
  const modelItem = controller.createTestItem(model, model, uri);
  modelItem.range = new vscode.Range(0, 0, 0, 0);

  // Add child items for each test assertion.
  for (const t of tests) {
    const id = testItemId(model, t.type, t.column);
    const label = t.column ? `${t.type}: ${t.column}` : t.type;
    const child = controller.createTestItem(id, label, uri);
    child.range = new vscode.Range(t.line, 0, t.line, 0);
    modelItem.children.add(child);
  }

  controller.items.add(modelItem);
}

function removeSidecar(
  controller: vscode.TestController,
  uri: vscode.Uri,
): void {
  controller.items.delete(modelNameFor(uri));
}

// ---------------------------------------------------------------------------
// Run handler
// ---------------------------------------------------------------------------

async function runHandler(
  controller: vscode.TestController,
  request: vscode.TestRunRequest,
  token: vscode.CancellationToken,
): Promise<void> {
  const run = controller.createTestRun(request);

  // Collect the set of models we need to invoke.  If the user selected
  // individual leaf tests, group them by their parent model so we issue one
  // CLI invocation per model rather than per-assertion.
  const modelQueue = collectModelsToRun(controller, request);

  for (const [modelName, items] of modelQueue) {
    if (token.isCancellationRequested) {
      for (const item of items) run.skipped(item);
      continue;
    }

    // Mark all items as started.
    for (const item of items) run.started(item);

    const start = Date.now();
    const abort = new AbortController();
    const cancelSub = token.onCancellationRequested(() => abort.abort());

    try {
      const result = await runRockyJson<DeclarativeTestOutput>(
        ["test", "--declarative", "--model", modelName, "--output", "json"],
        { signal: abort.signal, timeoutMs: 120_000 },
      );
      const elapsed = Date.now() - start;

      if (!result.declarative) {
        // No declarative results — mark every item as passed (no tests ran).
        for (const item of items) run.passed(item, elapsed);
        continue;
      }

      mapResults(run, items, result.declarative.results, elapsed);
    } catch (err) {
      const elapsed = Date.now() - start;
      const detail =
        err instanceof RockyCliError
          ? err.stderr.trim() || err.message
          : (err as Error).message;
      for (const item of items) {
        run.errored(item, new vscode.TestMessage(detail), elapsed);
      }
    } finally {
      cancelSub.dispose();
    }
  }

  run.end();
}

/**
 * Build a map from model name to the TestItems that should be reported.
 * Handles both "run all" (no include list) and partial selections.
 */
function collectModelsToRun(
  controller: vscode.TestController,
  request: vscode.TestRunRequest,
): Map<string, vscode.TestItem[]> {
  const map = new Map<string, vscode.TestItem[]>();

  const push = (modelName: string, item: vscode.TestItem): void => {
    const arr = map.get(modelName) ?? [];
    arr.push(item);
    map.set(modelName, arr);
  };

  const addAll = (parent: vscode.TestItem): void => {
    parent.children.forEach((child) => {
      if (!request.exclude?.includes(child)) {
        push(parent.id, child);
      }
    });
  };

  if (request.include) {
    for (const item of request.include) {
      if (request.exclude?.includes(item)) continue;

      if (item.children.size > 0) {
        // Parent (model) item — include all its children.
        addAll(item);
      } else {
        // Leaf (individual test) item.
        const modelName = item.parent?.id ?? item.id;
        push(modelName, item);
      }
    }
  } else {
    // Run everything.
    controller.items.forEach((model) => addAll(model));
  }

  return map;
}

/**
 * Match CLI `DeclarativeTestResult` entries to their corresponding TestItems
 * and report pass/fail/error/skip.
 */
function mapResults(
  run: vscode.TestRun,
  items: vscode.TestItem[],
  results: DeclarativeTestResult[],
  elapsed: number,
): void {
  const resultMap = new Map<string, DeclarativeTestResult>();
  for (const r of results) {
    const id = testItemId(r.model, r.test_type, r.column ?? undefined);
    resultMap.set(id, r);
  }

  for (const item of items) {
    const r = resultMap.get(item.id);
    if (!r) {
      // No matching result — the CLI didn't report on this test.
      run.skipped(item);
      continue;
    }

    switch (r.status) {
      case "pass":
        run.passed(item, elapsed);
        break;
      case "fail": {
        const detail = r.detail ?? `${r.test_type} failed`;
        const message = new vscode.TestMessage(detail);
        if (r.severity === "warning") {
          // Warnings still count as failures in the Test Explorer so the
          // user sees the yellow icon, but we annotate the message.
          message.message = `[warning] ${detail}`;
        }
        run.failed(item, message, elapsed);
        break;
      }
      case "error": {
        const detail = r.detail ?? `${r.test_type} execution error`;
        run.errored(item, new vscode.TestMessage(detail), elapsed);
        break;
      }
      default:
        run.skipped(item);
    }
  }
}
