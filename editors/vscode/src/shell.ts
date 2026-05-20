/**
 * Shell-quoting helpers shared by every site that builds a command string for
 * `vscode.Terminal.sendText()`. The terminal pipes the string straight through
 * the user's shell, so every interpolated token must be escaped — otherwise
 * spaces, quotes or shell metacharacters would split the argument or run
 * arbitrary code.
 */

/**
 * Single-quote an argument for POSIX shells; on Windows quote with `"` and
 * double any embedded quotes.
 *
 * The returned value is a single argv token after the shell parses it,
 * regardless of whether the input contains whitespace or quotes.
 */
export function shellQuote(arg: string): string {
  if (process.platform === "win32") {
    return `"${arg.replace(/"/g, '""')}"`;
  }
  return `'${arg.replace(/'/g, "'\\''")}'`;
}
