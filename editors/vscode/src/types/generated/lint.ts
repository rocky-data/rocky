/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/lint.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Severity of a `rocky lint` finding. `rocky lint` exits non-zero when it reports at least one `error`.
 */
export type LintSeverity = "error" | "warning" | "info";

/**
 * JSON output for `rocky lint`.
 */
export interface LintOutput {
  /**
   * Files whose SQL did not parse. The AST rules (`S001`, `S003`, `S004`) did not run on them; the text rules did.
   */
  ast_rules_skipped: string[];
  command: string;
  /**
   * Finding counts by severity.
   */
  counts: LintCounts;
  /**
   * Number of `.sql` files read.
   */
  files_checked: number;
  /**
   * Files `--fix` changed.
   */
  files_fixed: string[];
  /**
   * Findings left after any `--fix` pass, ordered by file, line and column.
   */
  findings: LintFinding[];
  /**
   * Findings `--fix` rewrote. `0` without `--fix`.
   */
  fixed: number;
  version: string;
  [k: string]: unknown;
}
/**
 * Finding counts by severity for `rocky lint`.
 */
export interface LintCounts {
  error: number;
  info: number;
  warning: number;
  [k: string]: unknown;
}
/**
 * One `rocky lint` finding.
 */
export interface LintFinding {
  /**
   * Rule code, e.g. `S001`.
   */
  code: string;
  /**
   * 1-based column, counted in characters.
   */
  col: number;
  file: string;
  /**
   * `true` when `rocky lint --fix` can rewrite this finding.
   */
  fixable: boolean;
  /**
   * Short suggestion for the fix.
   */
  hint: string;
  /**
   * 1-based line.
   */
  line: number;
  message: string;
  /**
   * Short rule name, e.g. `ambiguous-column`.
   */
  rule: string;
  severity: LintSeverity;
  [k: string]: unknown;
}
