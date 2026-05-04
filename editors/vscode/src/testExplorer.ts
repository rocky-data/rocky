import * as vscode from "vscode";
import { runRockyJson, RockyCliError } from "./rockyCli";
import type { TestResult } from "./types/rockyJson";

const MODELS_GLOB = "**/models/**/*.rocky";

/**
 * Wires Rocky models into VS Code's Test Explorer. Each `.rocky` file under
 * `**\/models\/**` becomes a test item; running an item shells out to
 * `rocky test --model NAME --output json` and reports pass/fail back.
 *
 * Phase 3 keeps the tree flat (one item per model). Per-contract granularity
 * will come once the CLI exposes it via either a `rocky test --list` flag or a
 * custom `rocky/listTests` LSP request.
 */
export function registerTestExplorer(
  context: vscode.ExtensionContext,
): vscode.TestController {
  const controller = vscode.tests.createTestController("rocky", "Rocky");
  context.subscriptions.push(controller);

  // Initial population.
  void refreshTests(controller);

  // Refresh handler — wired to the toolbar refresh button in the Test panel.
  controller.refreshHandler = async () => {
    await refreshTests(controller);
  };

  // Run profile.
  controller.createRunProfile(
    "Run",
    vscode.TestRunProfileKind.Run,
    (request, token) => runHandler(controller, request, token),
    /* isDefault */ true,
  );

  // Keep the tree in sync as files come and go.
  const watcher = vscode.workspace.createFileSystemWatcher(MODELS_GLOB);
  watcher.onDidCreate((uri) => addModel(controller, uri));
  watcher.onDidDelete((uri) => removeModel(controller, uri));
  context.subscriptions.push(watcher);

  return controller;
}

async function refreshTests(controller: vscode.TestController): Promise<void> {
  const files = await vscode.workspace.findFiles(MODELS_GLOB);
  // Replace the entire collection in one go to keep ordering stable.
  const items = files.map((uri) => makeTestItem(controller, uri));
  controller.items.replace(items);
}

function addModel(
  controller: vscode.TestController,
  uri: vscode.Uri,
): void {
  controller.items.add(makeTestItem(controller, uri));
}

function removeModel(
  controller: vscode.TestController,
  uri: vscode.Uri,
): void {
  controller.items.delete(modelIdFor(uri));
}

function modelIdFor(uri: vscode.Uri): string {
  return vscode.workspace.asRelativePath(uri);
}

function makeTestItem(
  controller: vscode.TestController,
  uri: vscode.Uri,
): vscode.TestItem {
  const id = modelIdFor(uri);
  const item = controller.createTestItem(id, id, uri);
  // Anchor to line 1 so "Reveal in editor" lands on the file top.
  item.range = new vscode.Range(0, 0, 0, 0);
  return item;
}

async function runHandler(
  controller: vscode.TestController,
  request: vscode.TestRunRequest,
  token: vscode.CancellationToken,
): Promise<void> {
  const run = controller.createTestRun(request);
  const queue: vscode.TestItem[] = [];

  if (request.include) {
    request.include.forEach((t) => queue.push(t));
  } else {
    controller.items.forEach((t) => queue.push(t));
  }

  for (const item of queue) {
    if (token.isCancellationRequested) {
      run.skipped(item);
      continue;
    }
    if (request.exclude?.includes(item)) {
      run.skipped(item);
      continue;
    }

    run.started(item);
    const start = Date.now();

    const controller2 = new AbortController();
    const cancelSub = token.onCancellationRequested(() =>
      controller2.abort(),
    );

    try {
      const result = await runRockyJson<TestResult>(
        ["test", "--model", item.id, "--output", "json"],
        { signal: controller2.signal, timeoutMs: 120000 },
      );
      const elapsed = Date.now() - start;
      const failed = result.failed ?? 0;
      if (failed === 0) {
        run.passed(item, elapsed);
      } else {
        const lines = (result.failures ?? [])
          .filter((f) => f.name === item.id || f.name === undefined)
          .map((f) => f.error);
        const message = new vscode.TestMessage(
          lines.length > 0 ? lines.join("\n") : `${failed} test(s) failed`,
        );
        run.failed(item, message, elapsed);
      }
    } catch (err) {
      const elapsed = Date.now() - start;
      const detail =
        err instanceof RockyCliError
          ? err.stderr.trim() || err.message
          : (err as Error).message;
      run.errored(item, new vscode.TestMessage(detail), elapsed);
    } finally {
      cancelSub.dispose();
    }
  }

  run.end();
}
