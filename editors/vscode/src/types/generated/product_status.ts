/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_status.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product status`.
 */
export interface ProductStatusOutput {
  /**
   * The approval record, when one exists.
   */
  approval?: ProductApprovalOutput | null;
  /**
   * Byte-verification problems against the committed manifest (empty = every committed artifact matches its recorded hash).
   */
  artifact_problems?: string[];
  command: string;
  /**
   * The committed lowering phase, when a manifest exists.
   */
  committed_phase?: string | null;
  /**
   * The committed manifest's spec digest, when one exists.
   */
  committed_spec_digest?: string | null;
  /**
   * The persisted fulfillment state tag, when one exists.
   */
  fulfill_state?: string | null;
  /**
   * Number of fulfillment journal rows recorded for this product.
   */
  journal_rows: number;
  /**
   * The resolved output model, when the spec parses.
   */
  output_model?: string | null;
  /**
   * The product name the status was asked for.
   */
  product: string;
  /**
   * `product:<name>`, when the spec parses.
   */
  product_id?: string | null;
  /**
   * Whether the approval snapshot's bytes still digest to the recorded value. `null` without an approval; `false` is tamper.
   */
  snapshot_intact?: boolean | null;
  /**
   * The working spec's digest, when it parses.
   */
  spec_digest?: string | null;
  /**
   * Why the spec failed to parse, when it did not.
   */
  spec_error?: string | null;
  /**
   * Whether the working spec equals the approved revision. `null` when either side is missing.
   */
  spec_matches_approval?: boolean | null;
  /**
   * Whether `products/<name>.toml` exists and parses.
   */
  spec_present: boolean;
  /**
   * Whether an uncommitted staging journal is pending. Status never mutates; the next compile resolves it.
   */
  staging_journal_present: boolean;
  version: string;
  [k: string]: unknown;
}
/**
 * The approval record, as echoed by compile/status output.
 */
export interface ProductApprovalOutput {
  /**
   * RFC3339 approval instant.
   */
  approved_at?: string | null;
  /**
   * Best-effort approver identity.
   */
  approver: string;
  /**
   * Project-root-relative path of the immutable snapshot file.
   */
  snapshot_path: string;
  /**
   * The approved `sha256:<hex>` spec digest.
   */
  spec_digest: string;
  [k: string]: unknown;
}
