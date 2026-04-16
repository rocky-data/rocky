/* eslint-disable */

/**
 * Health check status.
 */
export type HealthStatus = "healthy" | "warning" | "critical";

/**
 * Doctor output structure.
 */
export interface DoctorOutput {
  checks: HealthCheck[];
  command: string;
  overall: string;
  suggestions: string[];
  [k: string]: unknown;
}
/**
 * A single health check result.
 */
export interface HealthCheck {
  duration_ms: number;
  message: string;
  name: string;
  status: HealthStatus;
  [k: string]: unknown;
}
