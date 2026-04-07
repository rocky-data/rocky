import * as vscode from "vscode";
import { getConfig } from "./config";

export interface RockyTaskDefinition extends vscode.TaskDefinition {
  type: "rocky";
  /** Rocky subcommand to run, e.g. "compile", "test", "ci", "validate". */
  command: string;
  /** Optional extra args appended after the subcommand. */
  args?: string[];
}

const AUTO_DETECTED: Array<{ command: string; label: string; args?: string[] }> = [
  { command: "compile", label: "compile", args: ["--output", "json"] },
  { command: "validate", label: "validate" },
  { command: "test", label: "test", args: ["--output", "json"] },
  { command: "ci", label: "ci", args: ["--output", "json"] },
];

/**
 * Provides VS Code tasks (Cmd+Shift+B / Run Task…) for the most common Rocky
 * workflows. Users can also write their own task entries in tasks.json with
 * `"type": "rocky"` and a custom `command` / `args` pair — `resolveTask`
 * fills in the executable for them.
 */
export class RockyTaskProvider implements vscode.TaskProvider {
  static readonly type = "rocky";

  provideTasks(): vscode.Task[] {
    return AUTO_DETECTED.map(({ command, label, args }) =>
      this.buildTask({ type: "rocky", command, args }, label),
    );
  }

  resolveTask(task: vscode.Task): vscode.Task | undefined {
    const def = task.definition as RockyTaskDefinition;
    if (!def || def.type !== RockyTaskProvider.type) return undefined;
    if (!def.command) return undefined;
    return this.buildTask(def, def.command);
  }

  private buildTask(
    definition: RockyTaskDefinition,
    label: string,
  ): vscode.Task {
    const cfg = getConfig();
    const args = [definition.command, ...(definition.args ?? [])];
    const exec = new vscode.ProcessExecution(cfg.serverPath, args);
    const task = new vscode.Task(
      definition,
      vscode.TaskScope.Workspace,
      label,
      "rocky",
      exec,
    );
    task.detail = `rocky ${args.join(" ")}`;
    task.group =
      definition.command === "test" || definition.command === "ci"
        ? vscode.TaskGroup.Test
        : vscode.TaskGroup.Build;
    return task;
  }
}

export function registerTaskProvider(
  context: vscode.ExtensionContext,
): RockyTaskProvider {
  const provider = new RockyTaskProvider();
  context.subscriptions.push(
    vscode.tasks.registerTaskProvider(RockyTaskProvider.type, provider),
  );
  return provider;
}
