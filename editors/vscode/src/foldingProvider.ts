import * as vscode from "vscode";

/**
 * Provides folding ranges for `.rocky` files.
 *
 * Foldable regions:
 * - **Brace blocks** — `group { ... }`, `derive { ... }`, `select { ... }`,
 *   `join ... { ... }`, `match { ... }` (multi-line only)
 * - **Comment blocks** — consecutive `--` comment lines
 * - **Pipeline steps** — consecutive non-blank, non-comment lines between
 *   top-level keywords (`from`, `where`, `group`, `derive`, `select`, `join`,
 *   `sort`, `take`, `distinct`, `replicate`) fold as a group when the step
 *   spans multiple lines (e.g. a multi-line `group` without braces)
 */
export class RockyFoldingRangeProvider implements vscode.FoldingRangeProvider {
  provideFoldingRanges(
    document: vscode.TextDocument,
  ): vscode.FoldingRange[] {
    const ranges: vscode.FoldingRange[] = [];

    this.foldBraceBlocks(document, ranges);
    this.foldCommentBlocks(document, ranges);

    return ranges;
  }

  // ── Brace blocks ──────────────────────────────────────────────────────

  /**
   * Scans for `{` at end-of-line (ignoring trailing whitespace / comments)
   * and pairs it with the matching `}` on a subsequent line.  Handles
   * nesting (e.g. `match` inside `derive`).
   */
  private foldBraceBlocks(
    document: vscode.TextDocument,
    ranges: vscode.FoldingRange[],
  ): void {
    const openStack: number[] = [];

    for (let i = 0; i < document.lineCount; i++) {
      const text = document.lineAt(i).text;
      // Strip trailing comment (naive: first `--` outside a string)
      const code = stripTrailingComment(text);

      for (const ch of code) {
        if (ch === "{") {
          openStack.push(i);
        } else if (ch === "}") {
          const start = openStack.pop();
          if (start !== undefined && i > start) {
            ranges.push(
              new vscode.FoldingRange(start, i, vscode.FoldingRangeKind.Region),
            );
          }
        }
      }
    }
  }

  // ── Comment blocks ────────────────────────────────────────────────────

  /**
   * Groups consecutive `-- ...` comment lines into a single foldable block.
   */
  private foldCommentBlocks(
    document: vscode.TextDocument,
    ranges: vscode.FoldingRange[],
  ): void {
    let blockStart: number | undefined;

    for (let i = 0; i < document.lineCount; i++) {
      const trimmed = document.lineAt(i).text.trimStart();
      const isComment = trimmed.startsWith("--");

      if (isComment) {
        if (blockStart === undefined) {
          blockStart = i;
        }
      } else {
        if (blockStart !== undefined && i - 1 > blockStart) {
          ranges.push(
            new vscode.FoldingRange(
              blockStart,
              i - 1,
              vscode.FoldingRangeKind.Comment,
            ),
          );
        }
        blockStart = undefined;
      }
    }

    // Flush trailing comment block
    if (
      blockStart !== undefined &&
      document.lineCount - 1 > blockStart
    ) {
      ranges.push(
        new vscode.FoldingRange(
          blockStart,
          document.lineCount - 1,
          vscode.FoldingRangeKind.Comment,
        ),
      );
    }
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/**
 * Returns the portion of `line` before the first `--` that is not inside a
 * string literal.  Good enough for brace-counting purposes.
 */
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

export function registerFoldingProvider(
  context: vscode.ExtensionContext,
): void {
  const provider = new RockyFoldingRangeProvider();
  context.subscriptions.push(
    vscode.languages.registerFoldingRangeProvider(
      { scheme: "file", language: "rocky" },
      provider,
    ),
  );
}
