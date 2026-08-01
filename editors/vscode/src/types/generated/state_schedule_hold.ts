/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/state_schedule_hold.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky state schedule pause|resume`.
 *
 * The runtime schedule hold: `paused = true` after a pause, `false` after a resume; `changed = false` means the flag already held the requested value (the command is idempotent — a repeated pause is not a second action).
 */
export interface ScheduleHoldOutput {
  /**
   * `false` when the flag already held the requested value.
   */
  changed: boolean;
  command: string;
  /**
   * The hold state after this command.
   */
  paused: boolean;
  /**
   * The pipeline whose schedule the hold applies to.
   */
  pipeline: string;
  /**
   * The state file this hold was written to. A scheduler reads the flag only if it uses the SAME file — a `serve --scheduler --state-path X` is controlled by running this command with that same `--state-path`, not by the project default. Surfaced so a wrong-instance pause can never be silently "successful".
   */
  state_path: string;
  version: string;
  [k: string]: unknown;
}
