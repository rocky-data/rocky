import * as path from "path";
import { runTests } from "@vscode/test-electron";

async function main() {
  try {
    const extensionDevelopmentPath = path.resolve(__dirname, "../../");
    const extensionTestsPath = path.resolve(__dirname, "./suite/index");

    // Pin a specific VS Code version instead of the default `"stable"`, which
    // causes @vscode/test-electron to query
    // `https://update.code.visualstudio.com/api/releases/stable`. That
    // endpoint rate-limits aggressively (HTTP 429 with a text/html body) and
    // the response then fails JSON.parse, killing the test run with
    // "Failed to parse response ... as JSON". Pinning a version skips the
    // lookup entirely. Override with VSCODE_TEST_VERSION when needed.
    //
    // Keep this in sync with `engines.vscode` in package.json — a test host
    // older than `engines.vscode` fails to activate the extension silently,
    // which shows up as "0 commands registered" in the suite.
    const version = process.env.VSCODE_TEST_VERSION ?? "1.116.0";

    await runTests({
      version,
      extensionDevelopmentPath,
      extensionTestsPath,
      // Open the extension folder itself as the workspace so APIs that depend
      // on `workspace.workspaceFolders` (e.g. task providers) work in tests.
      launchArgs: [extensionDevelopmentPath],
    });
  } catch (err) {
    console.error("Failed to run tests:", err);
    process.exit(1);
  }
}

main();
