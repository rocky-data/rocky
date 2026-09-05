/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/health.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Liveness payload for the auth-exempt `GET /api/v1/health`.
 *
 * Server-lifecycle only: it says the process answers HTTP and which engine release it is. It carries no project state, so it is safe to hand to a prober that holds no token.
 */
export interface HealthOutput {
  /**
   * Always `"ok"` when the server answers at all.
   */
  status: string;
  /**
   * Engine release (`CARGO_PKG_VERSION`).
   */
  version: string;
  [k: string]: unknown;
}
