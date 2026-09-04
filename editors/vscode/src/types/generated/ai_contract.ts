/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/ai_contract.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky ai-contract <model>`.
 *
 * Reports the AI-drafted data contract for a model, grounded in the observed per-column profile of its target table. The drafted contract is compile-verified against the model before it's reported, so a successful response is a contract that `rocky compile` accepts.
 */
export interface AiContractOutput {
  /**
   * Number of LLM attempts taken to reach a compile-verified contract.
   */
  attempts: number;
  command: string;
  /**
   * The drafted contract serialized as `.contract.toml`.
   */
  contract_toml: string;
  /**
   * The model the contract was drafted for.
   */
  model: string;
  /**
   * The observed per-column profile that grounded the draft.
   */
  profile: AiContractColumnProfile[];
  /**
   * Path the contract was written to, when `--save` was passed.
   */
  saved_path?: string | null;
  /**
   * Declared column types the compiler could NOT check, one message each.
   *
   * The drafting loop only fails on error-severity diagnostics. A column whose type Rocky could not infer reports `I003` instead, and the declared type is then accepted without being compared to anything. With a cold schema cache that is every leaf column, so an empty `errors` list is not evidence the declared types are right. Empty when every declared type was actually checked.
   */
  unverified_types?: string[];
  version: string;
  [k: string]: unknown;
}
/**
 * Observed profile of one column, as reported by `rocky ai-contract`.
 */
export interface AiContractColumnProfile {
  distinct: number;
  max?: string | null;
  min?: string | null;
  name: string;
  null_rate: number;
  nulls: number;
  /**
   * Observed low-cardinality domain. Empty above the cardinality cap. This is reported as evidence; it is not encoded into the contract file.
   */
  observed_values: string[];
  rows: number;
  /**
   * Inferred Rocky type name.
   */
  type: string;
  [k: string]: unknown;
}
