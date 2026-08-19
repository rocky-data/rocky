/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_compile.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product compile`.
 */
export interface ProductCompileOutput {
  /**
   * The current approval record, when one exists. Compile is a READER of the approval: when present, the snapshot bytes were re-verified against the record digest before this output was produced.
   */
  approval?: ProductApprovalOutput | null;
  /**
   * The artifacts this phase committed (manifest excluded).
   */
  artifacts: ProductArtifactOutput[];
  command: string;
  /**
   * Project-root-relative path of the committed lowering manifest.
   */
  manifest_path: string;
  /**
   * The resolved output model name.
   */
  output_model: string;
  /**
   * Which lowering phase this run committed: `lowered_contract` (Phase A) or `merged` (Phase B).
   */
  phase: string;
  /**
   * `product:<name>`.
   */
  product_id: string;
  /**
   * `sha256:<hex>` over the compiled spec's raw bytes.
   */
  spec_digest: string;
  /**
   * Whether the compiled spec's digest equals the approved digest. `null` when no approval exists. `false` is not an error at compile time — it means the working spec has moved past the approval (the loop's supersession trigger).
   */
  spec_matches_approval?: boolean | null;
  /**
   * Project-root-relative spec path.
   */
  spec_path: string;
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
/**
 * One emitted artifact, as reported by `rocky product compile`.
 */
export interface ProductArtifactOutput {
  /**
   * Project-root-relative POSIX path.
   */
  path: string;
  /**
   * `sha256:<hex>` over the committed bytes.
   */
  sha256: string;
  [k: string]: unknown;
}
