/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/hooks_list.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky hooks list`.
 */
export interface HooksListOutput {
  hooks: HookEntry[];
  total: number;
  /**
   * Configured `[hook.webhooks]` endpoints. Each URL is cut to its scheme and host, because a webhook URL often carries a token in its path or query. Empty for a project with no webhooks.
   */
  webhooks: WebhookEntry[];
  [k: string]: unknown;
}
export interface HookEntry {
  command: string;
  env_keys: string[];
  event: string;
  on_failure: string;
  timeout_ms: number;
  [k: string]: unknown;
}
/**
 * One configured webhook in [`HooksListOutput`].
 */
export interface WebhookEntry {
  /**
   * Whether the webhook is fire-and-forget.
   */
  async_mode: boolean;
  event: string;
  /**
   * Names of the headers the webhook sets. Never their values.
   */
  header_keys: string[];
  method: string;
  on_failure: string;
  /**
   * The preset the webhook was built from, when it names one.
   */
  preset?: string | null;
  retry_count: number;
  timeout_ms: number;
  /**
   * The endpoint's scheme and host only (`https://hooks.example.com`). Never the path, query, port or credentials. A URL that does not parse prints as `<redacted>`.
   */
  url: string;
  [k: string]: unknown;
}
