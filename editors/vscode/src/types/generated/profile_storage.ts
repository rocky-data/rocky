/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/profile_storage.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky profile-storage`.
 */
export interface ProfileStorageOutput {
  command: string;
  model: string;
  profile_sql: string;
  recommendations: EncodingRecommendationOutput[];
  version: string;
  [k: string]: unknown;
}
export interface EncodingRecommendationOutput {
  column: string;
  data_type: string;
  estimated_cardinality: string;
  reasoning: string;
  recommended_encoding: string;
  [k: string]: unknown;
}
