import * as vscode from "vscode";

/**
 * Basic document formatting for `.rocky` files.
 *
 * Rules (intentionally lightweight — no full parser):
 * 1. Trim trailing whitespace on every line.
 * 2. Collapse runs of 3+ blank lines into exactly 2.
 * 3. Ensure file ends with exactly one newline.
 * 4. Normalize indentation inside brace blocks:
 *    - Lines between `{` and `}` are indented by one `tabSize` level
 *      relative to the opening line.
 *    - Closing `}` aligns with the opening keyword.
 * 5. Top-level pipeline keywords (`from`, `where`, `group`, `derive`,
 *    `select`, `join`, `sort`, `take`, `distinct`, `replicate`) are
 *    left-aligned (zero indent) when they appear outside a brace block.
 */
export class RockyDocumentFormattingProvider
  implements vscode.DocumentFormattingEditProvider
{
  provideDocumentFormattingEdits(
    document: vscode.TextDocument,
    options: vscode.FormattingOptions,
  ): vscode.TextEdit[] {
    const indent = options.insertSpaces ? " ".repeat(options.tabSize) : "\t";
    const original = document.getText();
    const formatted = formatRocky(original, indent);

    if (formatted === original) return [];

    const fullRange = new vscode.Range(
      document.positionAt(0),
      document.positionAt(original.length),
    );
    return [vscode.TextEdit.replace(fullRange, formatted)];
  }
}

// ── Rocky pipeline keywords (top-level step starters) ────────────────────

const PIPELINE_KEYWORD_RE =
  /^(from|where|group|derive|select|join|sort|take|distinct|replicate)\b/;

// ── Core formatter ──────────────────────────────────────────────────────

function formatRocky(text: string, indent: string): string {
  let lines = text.split("\n");

  // 1. Trim trailing whitespace
  lines = lines.map((l) => l.trimEnd());

  // 2. Normalize indentation based on brace nesting
  lines = normalizeIndentation(lines, indent);

  // 3. Collapse 3+ consecutive blank lines into 2
  lines = collapseBlankLines(lines);

  // 4. Ensure single trailing newline
  let result = lines.join("\n");
  result = result.replace(/\n*$/, "\n");

  return result;
}

/**
 * Re-indents lines so that content inside `{ ... }` blocks is indented one
 * level deeper than the opening line, while top-level pipeline keywords sit
 * at column 0 (or the current nesting level).
 */
function normalizeIndentation(lines: string[], indent: string): string[] {
  const out: string[] = [];
  let depth = 0;

  for (const raw of lines) {
    const trimmed = raw.trim();

    // Blank lines and pure-comment lines: preserve as-is (just trimmed of
    // trailing whitespace, which already happened).
    if (trimmed === "" || trimmed.startsWith("--")) {
      // Comments at depth > 0 get indented to current depth
      if (trimmed.startsWith("--") && depth > 0) {
        out.push(indent.repeat(depth) + trimmed);
      } else {
        out.push(trimmed);
      }
      continue;
    }

    // Closing brace: decrease depth *before* emitting.
    const leadingCloses = countLeading(trimmed, "}");
    if (leadingCloses > 0) {
      depth = Math.max(0, depth - leadingCloses);
    }

    // Emit line at current depth.
    // Top-level pipeline keywords (depth == 0) are left-aligned.
    if (depth === 0 && PIPELINE_KEYWORD_RE.test(trimmed)) {
      out.push(trimmed);
    } else {
      out.push(indent.repeat(depth) + trimmed);
    }

    // Opening braces: increase depth *after* emitting.
    const netOpens = countBraces(trimmed);
    depth = Math.max(0, depth + netOpens);
  }

  return out;
}

/**
 * Count how many leading `}` characters appear (handles `}}` edge case in
 * nested blocks).  Only counts braces at the very start of the trimmed line.
 */
function countLeading(trimmed: string, ch: string): number {
  let n = 0;
  for (const c of trimmed) {
    if (c === ch) n++;
    else break;
  }
  return n;
}

/**
 * Returns the *net* brace count for a line (opens minus closes), ignoring
 * braces inside string literals and after `--` comments.
 */
function countBraces(line: string): number {
  const code = stripTrailingComment(line);
  let net = 0;
  let inSingle = false;
  let inDouble = false;
  for (const ch of code) {
    if (ch === "'" && !inDouble) inSingle = !inSingle;
    else if (ch === '"' && !inSingle) inDouble = !inDouble;
    else if (!inSingle && !inDouble) {
      if (ch === "{") net++;
      else if (ch === "}") net--;
    }
  }
  return net;
}

/** Collapse 3+ consecutive blank lines into exactly 2. */
function collapseBlankLines(lines: string[]): string[] {
  const out: string[] = [];
  let blankRun = 0;
  for (const line of lines) {
    if (line === "") {
      blankRun++;
      if (blankRun <= 2) out.push(line);
    } else {
      blankRun = 0;
      out.push(line);
    }
  }
  return out;
}

/** Strip trailing `-- ...` comment from a line, respecting string literals. */
function stripTrailingComment(line: string): string {
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < line.length - 1; i++) {
    const ch = line[i];
    if (ch === "'" && !inDouble) inSingle = !inSingle;
    else if (ch === '"' && !inSingle) inDouble = !inDouble;
    else if (ch === "-" && line[i + 1] === "-" && !inSingle && !inDouble) {
      return line.slice(0, i);
    }
  }
  return line;
}

export function registerFormattingProvider(
  context: vscode.ExtensionContext,
): void {
  const provider = new RockyDocumentFormattingProvider();
  context.subscriptions.push(
    vscode.languages.registerDocumentFormattingEditProvider(
      { scheme: "file", language: "rocky" },
      provider,
    ),
  );
}
