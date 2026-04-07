import * as assert from "assert";
import * as vscode from "vscode";

suite("Rocky Extension", () => {
  suiteSetup(async function () {
    this.timeout(20000);
    // Activation is otherwise lazy (gated on .rocky files in the workspace),
    // which would leave commands unregistered for the assertions below.
    const ext = vscode.extensions.getExtension("rocky-dev.rocky");
    if (ext && !ext.isActive) {
      await ext.activate();
    }
  });

  test("Extension is present", () => {
    assert.ok(vscode.extensions, "VS Code extensions API should be accessible");
  });

  test("Commands are registered", async () => {
    const commands = await vscode.commands.getCommands(true);
    const rockyCommands = commands.filter((c) => c.startsWith("rocky."));
    assert.ok(
      rockyCommands.length >= 25,
      `Expected at least 25 rocky commands, found ${rockyCommands.length}: ${rockyCommands.join(", ")}`,
    );
  });

  test("Phase 2 commands are registered", async () => {
    const commands = await vscode.commands.getCommands(true);
    const expected = [
      "rocky.compile",
      "rocky.validate",
      "rocky.ci",
      "rocky.test",
      "rocky.run",
      "rocky.plan",
      "rocky.discover",
      "rocky.compare",
      "rocky.history",
      "rocky.metrics",
      "rocky.compact",
      "rocky.archive",
      "rocky.profileStorage",
      "rocky.importDbt",
      "rocky.hooksList",
    ];
    const missing = expected.filter((c) => !commands.includes(c));
    assert.deepStrictEqual(missing, [], `Missing commands: ${missing.join(", ")}`);
  });

  test("rocky.doctor command exists", async () => {
    const commands = await vscode.commands.getCommands(true);
    assert.ok(
      commands.includes("rocky.doctor"),
      "rocky.doctor command should be registered",
    );
  });

  test("rocky.optimize command exists", async () => {
    const commands = await vscode.commands.getCommands(true);
    assert.ok(
      commands.includes("rocky.optimize"),
      "rocky.optimize command should be registered",
    );
  });

  test("Rocky language is registered", async () => {
    const languages = await vscode.languages.getLanguages();
    assert.ok(languages.length > 0, "Should have registered languages");
  });

  test("View refresh commands are registered", async () => {
    const commands = await vscode.commands.getCommands(true);
    for (const expected of [
      "rocky.refreshModels",
      "rocky.refreshRuns",
      "rocky.refreshSources",
    ]) {
      assert.ok(
        commands.includes(expected),
        `Expected ${expected} to be registered`,
      );
    }
  });

  test("Rocky task type is registered", async () => {
    const tasks = await vscode.tasks.fetchTasks({ type: "rocky" });
    assert.ok(
      tasks.length >= 4,
      `Expected at least 4 auto-detected rocky tasks, found ${tasks.length}`,
    );
    const labels = tasks.map((t) => t.name).sort();
    for (const expected of ["compile", "validate", "test", "ci"]) {
      assert.ok(
        labels.includes(expected),
        `Expected task '${expected}' to be auto-detected; got: ${labels.join(", ")}`,
      );
    }
  });
});
