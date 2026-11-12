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
   * Optional columns the draft declares that the model does not produce.
   *
   * `W010`, dropped by the same error-only filter as `unverified_types`. A required column that is missing fails the draft outright (`E010`); an optional one only warns, so a drafted column name the model never emits went unreported. Empty when every declared column exists.
   */
  unmatched_columns?: string[];
  /**
   * `nullable = false` claims the draft withheld, one message per column.
   *
   * The column had no NULLs in the profiled table, but Rocky cannot prove it non-null from the model SQL and the source schemas it has, so `E012` would refuse the claim. The draft leaves `nullable` unset for it, which promises nothing, instead of writing `nullable = true`. Empty when every `nullable = false` in the draft was proven.
   */
  unproven_non_null?: string[];
  /**
   * Leaf source tables Rocky could not describe in the warehouse, one message each (`schema.table: reason`).
   *
   * `rocky ai-contract` reads the column types of every table the project's models read and no model writes, so the drafted types and `nullable = false` claims are checked against the warehouse. A table listed here kept its schema-cache entry, or stayed untyped without one. Empty when every leaf source was described.
   */
  unresolved_sources?: string[];
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
