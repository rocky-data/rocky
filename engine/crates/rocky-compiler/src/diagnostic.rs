//! Compiler diagnostics with source spans and suggestions.
//!
//! Used by both the type checker and contract validator to report issues.
//! Diagnostics can be converted to miette `Report`s for rich terminal
//! rendering with source spans, underlines, and help text.
//!
//! §P3.5: `code` and `message` are `Arc<str>` so cloning a `Diagnostic`
//! (hot-path in the LSP publish loop) is a refcount bump instead of a
//! `String` allocation. The JSON wire format is unchanged because serde's
//! `rc` feature makes `Arc<str>` (de)serialize transparently as a string.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Diagnostic code registry
// ---------------------------------------------------------------------------

// Errors — type checking
/// Join key type mismatch between two upstream models, with no common
/// supertype. The same comparison emits [`W001`] instead when one exists.
pub const E001: &str = "E001";

// Errors — contract validation
/// Required column missing from model output.
pub const E010: &str = "E010";
/// Column type mismatch (contract vs model output).
pub const E011: &str = "E011";
/// Nullability violation (contract says non-nullable, model says nullable).
pub const E012: &str = "E012";
/// Protected column has been removed.
pub const E013: &str = "E013";
/// A nullable column appeared that the contract does not declare, under
/// `[rules] no_new_nullable`.
pub const E014: &str = "E014";

// Errors — time_interval validation
//
// The authoritative description of each code lives on
// `rocky_compiler::typecheck::check_time_interval_strategy`, which emits them.
/// `time_column` not in model output schema.
pub const E020: &str = "E020";
/// `time_column` is not a date/timestamp type.
pub const E021: &str = "E021";
/// `time_column` is nullable (must be NOT NULL).
pub const E022: &str = "E022";
/// `time_column` failed SQL identifier validation.
pub const E023: &str = "E023";
/// Neither `@start_date` nor `@end_date` referenced in the model SQL.
pub const E024: &str = "E024";
/// `granularity = "hour"` requires a TIMESTAMP column, not DATE.
pub const E025: &str = "E025";
/// `first_partition` is not a valid canonical key for the granularity.
pub const E026: &str = "E026";
/// Budget exceeded — projected spend exceeds the declared per-model cost ceiling.
///
/// Emitted by `rocky compile` when the DAG-propagated cost estimate for a
/// model exceeds either `max_usd` or `max_bytes_scanned` declared in the
/// model's `[budget]` sidecar block.  Estimates are computed by
/// [`rocky_core::cost::propagate_costs`] using catalog-sourced table stats
/// when available, falling back to conservative stub statistics.
pub const E027: &str = "E027";
/// Required run variable (`@var(name)`) referenced but not supplied.
///
/// Emitted by `rocky compile` / `rocky run` / `rocky emit-sql` when a model's
/// SQL references `@var(name)` with no `--var name=value` supplied and no
/// inline `@var(name, default)`. The fix is to pass `--var name=value` or to
/// give the reference an inline default. Distinct from the config-time
/// `${ENV}` interpolation, which resolves while parsing `rocky.toml`.
pub const E028: &str = "E028";

// Errors — imported producer contracts
/// Consumer references a column that an imported producer snapshot dropped.
///
/// Emitted by `rocky compile` when a project declares an `[imports.<name>]`
/// block and one of its models reads a column that the producer's published
/// snapshot no longer outputs (detected by diffing the import's `baseline`
/// against its current `snapshot`). This is the cross-team-contracts gate:
/// a producer's breaking change fails the consumer's compile before the
/// consumer ships SQL that reads a column that no longer exists.
pub const E030: &str = "E030";

/// Imported producer narrowed the type of a column the consumer reads.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` baseline→snapshot
/// diff reports a `ColumnTypeChanged { narrowing: true }` on a producer
/// column that one of the consumer's models references (Int64 → Int32,
/// Decimal precision shrink, Timestamp → Date, …). The producer's new type
/// can no longer represent every value the consumer was reading, so the
/// change can truncate or reject the consumer's data.
pub const E031: &str = "E031";

/// Imported producer tightened a column the consumer reads from nullable to
/// NOT NULL.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` baseline→snapshot
/// diff reports a `ColumnNullabilityChanged { old_nullable: true,
/// new_nullable: false }` on a producer column a consumer model references.
/// Conservatively an error: the consumer may have logic (or downstream
/// contracts) built around the column being nullable. The producer-side
/// classifier rates this `Warning`; the consumer-read direction is stricter
/// because the assumption being broken lives on the consumer side, and Rocky
/// has no consumer-side nullability hint yet to distinguish "relies on NULL"
/// from "doesn't care". When that hint lands, this can relax to a warning.
pub const E032: &str = "E032";

/// Imported snapshot's recipe hash does not match the configured `pin`.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` block sets a
/// concrete `pin` (recipe-hash hex) and the vendored snapshot's recipe hash
/// differs. Signals the consumer is compiling against a producer snapshot it
/// has not pinned to — the vendored file drifted from the agreed contract.
pub const E033: &str = "E033";

/// Imported snapshot declares a format version newer than this build of rocky
/// can read.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` snapshot's
/// `snapshot_version` exceeds the version this binary understands. The
/// snapshot is readable-but-unhonorable, so the contract is failed closed
/// (rather than silently skipped, which would look enforced but check nothing)
/// — upgrade rocky to read the producer's newer snapshot format.
pub const E034: &str = "E034";

/// Managed-Iceberg `format_options` declares a combination the warehouse
/// rejects at execution.
///
/// Emitted by `rocky compile` when a model sets `format = "iceberg_table"`
/// with `format_options` that Databricks managed Iceberg refuses: `partition_by`
/// and `cluster_by` set together (mutually exclusive), or an engine-managed
/// `write.format.*` table property. Surfacing this at compile time turns a
/// first-run warehouse error into a clear diagnostic that names the offending
/// option, before any warehouse call. The constraint logic is shared with the
/// DDL generator's run-path guard via
/// [`rocky_ir::lakehouse::validate_managed_iceberg_options`], so the two checks
/// can never drift. (FR-044)
pub const E035: &str = "E035";

/// A transformation model declares `type = "incremental"` with no watermark.
///
/// Emitted by `rocky compile` (`check_incremental_strategy` in `typecheck.rs`).
/// Without a `timestamp_column` (alias `watermark`) the strategy could only
/// lower to a plain `INSERT INTO <target> <model SQL>` with no filter, so every
/// run after the first would append the whole result again (#1990). The error
/// points at the watermark config and the `@incremental_filter` placeholder,
/// and names the other strategies that work. Replication pipelines are
/// unaffected: their `incremental` copy applies its own watermark. `E036` is
/// taken by the target-collision check in `compile.rs`, which emits it as a
/// literal.
pub const E037: &str = "E037";

/// An `ephemeral` model is used in a way that cannot work.
///
/// Ephemeral models are supported: each consumer executes with the model's
/// SQL inlined as a `__rocky_ephemeral__<model>` CTE (`ephemeral.rs`). Until
/// that inlining existed (#1996) this code refused `type = "ephemeral"`
/// outright; it now marks only the uses inlining cannot serve:
///
/// - `[[tests]]` on an ephemeral model — there is no table to test;
/// - a qualified read of an ephemeral model's nominal `[target]`
///   (`main.eph_orders`) — no table backs that name;
/// - a consumer the inliner cannot rewrite (the SQL is not one parseable
///   `SELECT`, or a `WITH RECURSIVE` CTE would capture a name the inlined
///   SQL reads);
/// - `rocky run --model <ephemeral>` — there is nothing to build (emitted
///   by the CLI, not by `rocky compile`).
pub const E038: &str = "E038";

/// A direct projection reads a column absent from a complete in-project model.
///
/// Emitted only when Rocky can prove the upstream model's output names are
/// complete. External sources are covered separately, by [`E041`] / [`W041`],
/// which weigh where the source schema came from.
pub const E039: &str = "E039";

/// A `.rocky` string literal contains a backslash, whose SQL meaning varies
/// by target dialect. Use a `.sql` model with the target's own escaping.
pub const E040: &str = "E040";

/// An aggregating query reads a column that is neither grouped nor inside an
/// aggregate, in its SELECT list, HAVING, or ORDER BY.
///
/// Emitted by `rocky compile` only when the column provably belongs to a
/// relation in the same query scope (an upstream model, a known source
/// schema, a CTE, or a derived table). Unresolved names, `GROUP BY ALL`,
/// projection aliases, and arguments of unknown functions stay silent. See
/// `rocky_compiler::group_by` for the full rule set.
pub const E044: &str = "E044";
/// An aggregate's argument type has no overload on the target dialect, and the
/// dialect does not cast it implicitly — e.g. `SUM(VARCHAR)` on DuckDB,
/// BigQuery or Trino. The statement can never run. Emitted by `rocky compile`
/// from [`crate::operand_check`], which carries the per-dialect table and its
/// documentation sources. Where the dialect casts at run time instead, the
/// same argument is [`W042`].
pub const E042: &str = "E042";

/// A comparison (`=`, `<>`, `<`, `IN`, `BETWEEN`, join `ON`, …) pairs types
/// the target dialect refuses outright — e.g. `INT64 = STRING` on BigQuery or
/// `bigint = varchar` on Trino. Emitted by `rocky compile` from
/// [`crate::operand_check`]. Where the dialect casts implicitly and failure
/// depends on the data, the same pair is [`W043`]. A same-named join key whose
/// type differs across upstream models stays [`E001`]/[`W001`]'s.
pub const E043: &str = "E043";
/// A direct column reference names a column absent from an external source
/// whose schema Rocky holds as authoritative.
///
/// Emitted by `rocky compile` (and the compile `rocky run` performs before it
/// executes) from [`crate::source_refs::check_source_column_refs`]. The source
/// schema counts as authoritative when it was introspected live during this
/// invocation, when it came from a schema-cache entry younger than
/// `[cache.schemas] trusted_max_age_seconds`, or when strict sources are on
/// (`rocky compile --strict-sources` or `[cache.schemas] strict_sources`).
/// Without one of those the same finding is [`W041`].
///
/// It fires only when the reference binds unambiguously: every relation the
/// name could resolve against is a known source (no CTE, derived table,
/// in-project model or table function in scope), no `SELECT` alias or
/// relation name matches it, a `a.b` reference cannot also be read as a
/// struct field, and the name is neither quoted (`"x"` is a string in BigQuery
/// and, by default, Databricks) nor `_`-prefixed (warehouse metadata columns).
/// Anything else keeps the conservative `Unknown` result. The
/// message names the column and the source, and suggests close column names.
pub const E041: &str = "E041";
/// A user-defined function (`functions/`) or a call to one is invalid.
///
/// Emitted by `rocky compile` (`rocky_compiler::udf`). Attributed to the
/// function when its definition cannot be created — `language` other than
/// `"sql"` (Python UDFs are refused), a bad identifier or type, a missing
/// body, a duplicate name, a call cycle between functions. Attributed to the
/// calling model when a call passes the wrong number of arguments, names an
/// invalid function, or passes an argument whose type no supported warehouse
/// converts to the declared parameter type (e.g. a `DATE` into a `BIGINT`).
/// Also the code `rocky run` / `rocky plan` use to refuse function DDL on a
/// warehouse that cannot create it (Trino). Calls that Rocky cannot verify
/// are [`W051`], never this.
pub const E051: &str = "E051";
/// A freshness declaration cannot be checked as written.
///
/// Emitted by `rocky compile` for a transformation pipeline's
/// `[[pipeline.<name>.sources]]` `freshness` block when it declares neither
/// `warn_after` nor `error_after`, a duration that does not parse (`"12h"`,
/// `"3600s"`, `"7d"`), an `error_after` shorter than `warn_after`, a
/// `loaded_at_field` that is not a plain identifier, or a `filter` carrying a
/// statement terminator. Also emitted for a model's own sidecar `[freshness]`
/// block (not an inherited one) whose `time_column` is absent from the
/// model's output when that output is
/// provably complete (no `SELECT *`, every projection named): the model's own
/// SQL decides its output, so the absence is a fact, not a stale schema.
pub const E050: &str = "E050";
/// A transformation `incremental` model's watermark filter has no safe place.
///
/// Emitted by `rocky compile` (`check_incremental_strategy` in `typecheck.rs`)
/// when the model declares a watermark but its SQL has no
/// `@incremental_filter` placeholder and lineage cannot prove the watermark is
/// a direct passthrough of one input column (so filtering the output is not
/// the same as filtering the input); when the watermark is missing from a
/// provably complete output schema or is not a plain column name; or when
/// `@incremental_filter` appears in a model whose strategy is not
/// `incremental`. The suggestion says where the placeholder goes.
pub const E046: &str = "E046";
/// A `type = "snapshot"` model's config is invalid.
///
/// Emitted by `rocky compile` (`check_snapshot_strategy` in `snapshot.rs`):
/// a missing `unique_key` or `strategy`, `strategy = "timestamp"` without
/// `updated_at`, `strategy = "check"` without `check_cols`, a key or change
/// column that is an expression rather than a column name, an `updated_at` or
/// `check_cols` entry the model's own explicit projection does not output,
/// a unique key the projection computes non-deterministically (`random()`,
/// `uuid()`, `now()`), an output column that collides with a snapshot metadata
/// column, or an invalid metadata column name or `valid_to_current`.
/// Absence is only an error when the model lists its columns itself; under
/// `SELECT *` the compile-time schema may be stale, so it is W049 instead.
pub const E049: &str = "E049";
/// A model references a `private` model outside that model's ownership group.
///
/// Emitted by `rocky compile` for each such reference, on the consumer. Also
/// emitted on a `private` model that belongs to no group (it could never be
/// referenced). On the cross-project path it fires when a consumer's
/// `[[sources]]` entry reads a producer model the producer did not publish as
/// `public`. See `rocky_core::model_governance`.
pub const E047: &str = "E047";

/// A model-version problem: a version declaration whose `latest_version` is
/// not declared, a declared version with no `<name>_v<N>` model, or a
/// reference to a version that is not declared (or to the bare name of a
/// versioned model whose `latest_alias` is off).
pub const E048: &str = "E048";
/// A model's `[redshift]` table options cannot render.
///
/// Emitted by `rocky compile` (`redshift_options::check_redshift_table_options`)
/// for an invalid `dist_key` / `sort_key` column name, a contradictory
/// combination (`dist_style = "key"` without `dist_key`, `dist_key` with
/// another `dist_style`, `sort_style = "auto"` with columns, more than 8
/// interleaved sort columns), or `[redshift]` on a strategy that builds no
/// table (`view`, `materialized_view`, `dynamic_table`, `content_addressed`,
/// `ephemeral`) or alongside a lakehouse `format`. The option rules are shared
/// with the Redshift dialect's SQL-generation guard, so the two cannot drift.
pub const E052: &str = "E052";

// Warnings
/// Unused model (no downstream consumers).
pub const W001: &str = "W001";
/// Duplicate column in model output.
pub const W002: &str = "W002";
/// Classification tag on a model column doesn't resolve to any `[mask]` /
/// `[mask.<env>]` strategy and isn't listed in `[classifications.allow_unmasked]`.
/// One diagnostic per unresolved `(model, column, tag)` triple.
pub const W004: &str = "W004";
/// Model has at least one temporal output column (DATE / TIMESTAMP /
/// TIMESTAMP_NTZ) but no `freshness` declaration in scope — neither a
/// per-model sidecar block nor a project-level `[freshness]` default.
/// Soft hint that the model would benefit from a freshness expectation.
/// Suppressed by adding a `[freshness]` block (per-model or project).
pub const W005: &str = "W005";
/// `merge` strategy declares a `unique_key` column the model does not output.
///
/// Emitted by `rocky compile` when a model sets `[strategy] type = "merge"`
/// and one of its `unique_key` entries does not name a column in the model's
/// output schema. Without it a typo'd merge key compiles clean and only fails
/// once the warehouse rejects the generated `MERGE ... ON` clause, so the
/// mistake surfaces mid-run rather than at compile time. One diagnostic per
/// missing key, so a multi-column `unique_key` reports every typo at once.
///
/// # Why a warning and not an error
///
/// The check is only as good as the compiler's ability to enumerate a model's
/// output columns, and that enumeration is best-effort — it is recovered from
/// lineage extraction, which is not a full SQL semantic analysis. Every case it
/// cannot enumerate is a potential false positive, and a false positive on an
/// error breaks a valid build. Gating hard on a soft signal is the wrong trade,
/// so this reports and does not block. Run it, read it, and fix the typo it
/// finds; a build is never failed on it.
///
/// # When it is skipped
///
/// Only models whose output schema is *provably complete* are checked —
/// [`crate::semantic::ModelSchema::schema_is_complete`], which requires both no
/// `SELECT *` and no projection item lineage could not name. A star may expand
/// from a raw source Rocky has no schema for; an unnamed non-identifier
/// projection (`SELECT (order_id)`) yields no schema entry at all. In either
/// case "the column is absent" would be an artefact of incomplete enumeration
/// rather than a fact about the model.
///
/// # Case sensitivity
///
/// Column names are matched **case-insensitively**, which is the right default
/// but is not uniformly sound — see
/// `rocky_compiler::typecheck::check_merge_strategy` for the per-adapter survey
/// and the Snowflake limitation it accepts.
pub const W006: &str = "W006";
/// A freshness `loaded_at_field` / `time_column` may not be readable as a
/// load time.
///
/// Emitted by `rocky compile` when the column's known type is concrete and
/// not DATE / TIMESTAMP / TIMESTAMP_NTZ, or when a source's
/// `loaded_at_field` is absent from the source schema the compiler holds.
///
/// # Why a warning and not an error
///
/// Source schemas reach the compiler from a seed (`--with-seed`) or the
/// schema cache, and either can be stale: the warehouse may have gained the
/// column since. A stale schema must never refuse a build, so an absent
/// source column only warns. A model column whose output is not provably
/// complete warns for the same reason. `rocky freshness` reports the real
/// outcome against the warehouse as `runtime_error`.
pub const W050: &str = "W050";
/// Contract defines a column not in model output (but not required).
pub const W010: &str = "W010";
/// Contract exists for a model not found in the project.
pub const W011: &str = "W011";
/// An `[imports.<name>]` snapshot (or its baseline) could not be loaded, so
/// the import's cross-team-contract checks (E030/E033) were skipped. Not an
/// error — the consumer compiles, it just isn't verified against that
/// producer this run.
pub const W012: &str = "W012";
/// The project's `rocky.toml` is present and could not be read, so the
/// project-level checks that depend on it did not run (#1625).
///
/// Same shape as [`W012`] one level up: the compile itself still succeeds —
/// models parse and typecheck without a project config — but `[mask]` /
/// `[classifications.allow_unmasked]` (W004), the `[freshness]` default
/// (W005) and the warehouse schema cache all came through empty because the
/// file could not be parsed, not because the project declares nothing.
///
/// Emitted by the long-running surfaces — `rocky lsp` and `rocky serve` —
/// which stay usable on a broken config by design rather than refusing.
/// Every one-shot entry point (`rocky lineage`, the MCP tools, `rocky plan`)
/// refuses instead: those return an answer a caller acts on, and an answer
/// computed from a config that never loaded is wrong rather than degraded.
///
/// A project with NO `rocky.toml` does not emit this. Absence is an ordinary
/// project fact; unreadability is a failure to read.
pub const W013: &str = "W013";

/// Imported producer added a column. Surfaced (at info severity) only to
/// consumers that read the producer via `SELECT *`, where an added column
/// shifts positional projection.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` baseline→snapshot
/// diff reports a `ColumnAdded` on a producer target that a consumer model
/// reads with `SELECT *` (or otherwise can't enumerate its columns). A
/// consumer that selects explicit columns is unaffected, so this is gated on
/// the `SELECT *` / unfiltered case rather than the column-reference filter
/// the E03x codes use (no consumer references a brand-new column by name).
pub const W030: &str = "W030";

/// Imported producer widened the type of a column the consumer reads.
///
/// Emitted by `rocky compile` when an `[imports.<name>]` baseline→snapshot
/// diff reports a `ColumnTypeChanged { narrowing: false }` on a producer
/// column a consumer model references (Int32 → Int64, Decimal precision
/// grow, …). The new type holds every value the old one did, so existing
/// reads keep working — but the consumer's own declared output type may now
/// be too small, hence a warning rather than silence.
pub const W031: &str = "W031";

/// An aggregate's argument is implicitly cast at run time — e.g. `SUM(VARCHAR)`
/// on Snowflake or Databricks — so the query fails on the first value that
/// does not convert. Also emitted for [`E042`]'s cases when no target dialect
/// is known. Escalate with `rocky compile --deny-warnings W042`.
pub const W042: &str = "W042";

/// A comparison relies on a value-dependent implicit cast — e.g. a `BIGINT`
/// column compared with a `VARCHAR` column on DuckDB, Snowflake or Databricks,
/// which fails at run time on the first text value that does not parse. A
/// string literal that parses as a number is not reported. Escalate with
/// `rocky compile --deny-warnings W043`.
pub const W043: &str = "W043";
/// A direct column reference names a column absent from an external source
/// schema that may be out of date.
///
/// Same finding as [`E041`], at warning severity, for a source schema Rocky
/// cannot treat as current: one read from a seed file
/// (`rocky compile --with-seed`) or from a schema-cache entry older than
/// `[cache.schemas] trusted_max_age_seconds` (unset by default, so every cache
/// entry). The warehouse may already carry the column, so the compile still
/// succeeds. Refresh the schema (fix the seed, or re-warm the cache with
/// `rocky discover --with-schemas`), or escalate to [`E041`] with
/// `rocky compile --strict-sources` or `[cache.schemas] strict_sources = true`.
pub const W041: &str = "W041";
/// A call to a user-defined function could not be fully verified.
///
/// Emitted by `rocky compile` (`rocky_compiler::udf`) when an argument's
/// inferred type (or the declared parameter type) is unknown, when an
/// argument relies on the warehouse converting it implicitly (e.g. a `VARCHAR`
/// into a `BIGINT` parameter), or when a function body could not be parsed.
/// A warning, not an error: the warehouse may well accept the call. Certain
/// mismatches are [`E051`].
pub const W051: &str = "W051";
/// A transformation `incremental` model sets `lookback` without `unique_key`.
///
/// Emitted by `rocky compile` (`check_incremental_strategy` in `typecheck.rs`).
/// A lookback re-reads rows at or below the target's watermark; appended
/// without a key to merge on, those rows land in the target again on every
/// run. A warning, not an error: an append-only consumer may tolerate it.
pub const W046: &str = "W046";
/// A `type = "snapshot"` model's config is valid but risky.
///
/// Emitted by `rocky compile` (`check_snapshot_strategy` in `snapshot.rs`):
/// a `unique_key` the compiled SELECT does not output (it may be a
/// `[[surrogate_key]]` column, added at run time), `strategy = "check"`
/// comparing many columns (every run compares each one
/// for every key), `updated_at` whose inferred type is not a timestamp or
/// date, or a key / change column missing from a `SELECT *` model's
/// compile-time schema, which may be stale.
pub const W049: &str = "W049";
/// A model references a model version whose `deprecation_date` has passed or
/// falls within the next 30 days. The reference still compiles; move it to
/// the latest version. The date is checked against today's UTC date, or
/// `ROCKY_GOVERNANCE_TODAY` (`YYYY-MM-DD`) when set.
pub const W048: &str = "W048";
/// A `[redshift]` `dist_key` / `sort_key` names a column the model does not
/// output.
///
/// Emitted only when the model's output columns are provably complete (the
/// W006 guard). A warning, not an error, because that enumeration comes from
/// lineage extraction; Redshift rejects the `CREATE TABLE` at run time if the
/// column really is missing.
pub const W052: &str = "W052";

// Info
/// Model dependency inferred from SQL.
pub const I001: &str = "I001";
/// Some columns have unknown types — source schemas would complete the check.
///
/// Emitted only on *partial* inference (some columns typed, some not), not for
/// `SELECT *` as such: see the sole emitter in `typecheck.rs`.
pub const I002: &str = "I002";

/// A contract column's declared type could not be checked, because Rocky did
/// not infer a type for the column.
///
/// Emitted by `validate_contract` (one per column) when a `.contract.toml`
/// column sets `type = "..."` and the model's inferred type for that column is
/// [`crate::types::RockyType::Unknown`]. The `E011` type check does not run for
/// that column, so a wrong declared type would otherwise pass in silence
/// (#1240).
///
/// # Why info and not a warning
///
/// `rocky test` and `rocky ci` compile with empty source schemas
/// (`rocky-engine::test_runner`, `rocky-engine::ci`), so under those commands
/// every column that takes its type from a source table infers `Unknown` — a
/// literal or an expression over literals still resolves. That is most of a
/// typical model. At warning severity this would fire on those columns in
/// every project that ships a contract — noise at the severity users are
/// asked to act on. It would also flip `rocky ci`'s reported exit code
/// from 0 to 4 for each of those projects: `rocky_engine::ci::CiResult::
/// exit_code` returns 4 when any diagnostic is a warning, and `rocky ci`
/// prints that number and puts it in its JSON. (The process itself still
/// exits 0 — the CLI only calls `process::exit` when compile or tests fail —
/// so the shell status would not move, but every reader of that field would.)
/// Info reports the gap and moves nothing. When `rocky test` / `rocky ci`
/// gain a source-schema producer, this can be reconsidered.
///
/// # How to clear it
///
/// Give the compiler source schemas. Many commands read them from the schema
/// cache, written by `rocky run` / `rocky discover --with-schemas` — both on
/// replication pipelines only (`discover` refuses a transformation-only
/// pipeline). `rocky compile` also accepts a seed file via `--with-seed`,
/// the route that works for a transformation-only project. Several
/// commands do not — they build a `CompilerConfig` with an empty map, so
/// nothing clears this code under them today. `rocky test` and `rocky ci`
/// are the two that matter here (`rocky_engine::test_runner`,
/// `rocky_engine::ci`); `rocky emit-sql`, `rocky preview-rows` and
/// `rocky retention-status` do the same. For the current list, run
/// `rg 'source_schemas:\s*(std::collections::)?HashMap::new\(\)'` — the
/// three commands above spell it `std::collections::HashMap::new()`, so a
/// search for the short form alone finds none of them. It matches explicit
/// initializers only; a caller that builds the map elsewhere and passes it in
/// empty will not show up.
///
/// A `CAST` is *not* a general fix. `refine_casts` in `typecheck.rs` refines a
/// cast column only when the cast's input type is already known, so
/// `SELECT CAST(id AS BIGINT) AS id FROM source.raw.users` with no schema for
/// `source.raw.users` still infers `Unknown`. And a warehouse-dependent
/// expression — `AVG` over a `DECIMAL` input (#1238) — stays `Unknown` even
/// with source schemas, because the result type is not knowable at this
/// layer.
pub const I003: &str = "I003";

// Lints — portability + blast-radius
/// Construct is not portable to the configured target dialect.
/// Error severity, opt-in via `--target-dialect`. Emitted by the CLI.
pub const P001: &str = "P001";
/// `SELECT *` model has downstream consumers that reference specific
/// columns of its output — a schema change in the star's source would
/// silently propagate. Warning severity, always-on.
pub const P002: &str = "P002";

/// Severity level of a diagnostic.
///
/// Serialized in PascalCase (`"Error"`, `"Warning"`, `"Info"`) to stay
/// compatible with existing dagster fixtures and the hand-written
/// `Severity` StrEnum in `integrations/dagster/src/dagster_rocky/types.py`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

/// Location in a source file.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceSpan {
    pub file: String,
    pub line: usize,
    pub col: usize,
}

/// A compiler diagnostic (error, warning, or informational message).
///
/// `code` and `message` use `Arc<str>` (§P3.5) — cloning a `Diagnostic`
/// in the LSP publish loop becomes a refcount bump. Construction still
/// accepts any `Into<String>` / `&str` via the helper constructors below;
/// the arc wrap happens once at construction time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Diagnostic {
    /// Severity level.
    pub severity: Severity,
    /// Diagnostic code (e.g., "E001", "W001").
    pub code: Arc<str>,
    /// Human-readable message.
    pub message: Arc<str>,
    /// Source location (if available).
    pub span: Option<SourceSpan>,
    /// Which model this diagnostic relates to.
    pub model: String,
    /// Suggested fix (if any).
    pub suggestion: Option<String>,
}

impl Diagnostic {
    /// Create an error diagnostic.
    pub fn error(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Create a warning diagnostic.
    pub fn warning(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Create an info diagnostic.
    pub fn info(code: &str, model: &str, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Info,
            code: Arc::from(code),
            message: Arc::from(message.into()),
            span: None,
            model: model.to_string(),
            suggestion: None,
        }
    }

    /// Add a source span.
    #[must_use]
    pub fn with_span(mut self, span: SourceSpan) -> Self {
        self.span = Some(span);
        self
    }

    /// Add a suggestion.
    #[must_use]
    pub fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }

    /// Build an E027 budget-exceeded diagnostic for a USD cost ceiling.
    ///
    /// Emitted when the DAG-propagated cost estimate for a model exceeds the
    /// `max_usd` value declared in the model's `[budget]` sidecar block.
    ///
    /// The suggestion on all four E027 constructors says "reduce the query's
    /// scan volume" and deliberately does not say "optimize". A diagnostic
    /// is served to an untrusted MCP drafting worker through `compile` and
    /// `draft_model`, and `optimize` is a tool the worker profile does not
    /// serve — so the word steers it at a verb it cannot call. Rocky owns
    /// this sentence, so the remedy is the reword; the sweep that catches a
    /// regression is `worker_result_text_names_no_excluded_tool` in
    /// `rocky-mcp/tests/roundtrip.rs` (this crate cannot depend on
    /// rocky-mcp — the dependency runs the other way).
    #[must_use]
    pub fn budget_exceeded(model: &str, projected_usd: f64, ceiling_usd: f64) -> Self {
        Self::error(
            E027,
            model,
            format!("budget exceeded — projected ${projected_usd:.4} > ceiling ${ceiling_usd:.4}",),
        )
        .with_suggestion(format!(
            "raise [budget] max_usd above ${ceiling_usd:.4} in the model sidecar, \
             or reduce the query's scan volume"
        ))
    }

    /// Build an E027 budget-exceeded diagnostic for a bytes-scanned ceiling.
    ///
    /// Emitted when the DAG-propagated byte estimate for a model exceeds the
    /// `max_bytes_scanned` value declared in the model's `[budget]` sidecar
    /// block.
    #[must_use]
    pub fn budget_exceeded_bytes(model: &str, projected_bytes: u64, ceiling_bytes: u64) -> Self {
        Self::error(
            E027,
            model,
            format!(
                "budget exceeded — projected {projected_bytes} bytes > ceiling {ceiling_bytes} bytes scanned",
            ),
        )
        .with_suggestion(format!(
            "raise [budget] max_bytes_scanned above {ceiling_bytes} in the model sidecar, \
             or reduce the query's scan volume"
        ))
    }

    /// Build a **warning**-severity E027 USD budget diagnostic.
    ///
    /// Used at plan time when the model's `on_breach = "warn"` policy means a
    /// ceiling breach should be surfaced as advisory rather than blocking.
    /// The message is identical to [`Self::budget_exceeded`]; only severity
    /// differs.
    #[must_use]
    pub fn budget_exceeded_warn(model: &str, projected_usd: f64, ceiling_usd: f64) -> Self {
        Self::warning(
            E027,
            model,
            format!("budget exceeded — projected ${projected_usd:.4} > ceiling ${ceiling_usd:.4}",),
        )
        .with_suggestion(format!(
            "raise [budget] max_usd above ${ceiling_usd:.4} in the model sidecar, \
             or reduce the query's scan volume"
        ))
    }

    /// Build a **warning**-severity E027 bytes-scanned budget diagnostic.
    ///
    /// Used at plan time when the model's `on_breach = "warn"` policy means a
    /// ceiling breach should be surfaced as advisory rather than blocking.
    #[must_use]
    pub fn budget_exceeded_bytes_warn(
        model: &str,
        projected_bytes: u64,
        ceiling_bytes: u64,
    ) -> Self {
        Self::warning(
            E027,
            model,
            format!(
                "budget exceeded — projected {projected_bytes} bytes > ceiling {ceiling_bytes} bytes scanned",
            ),
        )
        .with_suggestion(format!(
            "raise [budget] max_bytes_scanned above {ceiling_bytes} in the model sidecar, \
             or reduce the query's scan volume"
        ))
    }

    /// Is this an error?
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }

    /// Render this diagnostic as a miette `Report` with rich source spans.
    ///
    /// If `source_text` is provided, the diagnostic will include an underlined
    /// source snippet pointing at the error location. Without source text,
    /// falls back to a plain message with file:line:col.
    pub fn to_miette(&self, source_text: Option<&str>) -> miette::Report {
        let severity_prefix = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        };

        if let (Some(span), Some(src)) = (&self.span, source_text) {
            // Convert line:col to byte offset for miette
            if let Some(offset) = line_col_to_offset(src, span.line, span.col) {
                let diag = RichDiagnostic {
                    message: format!("{severity_prefix}[{}]: {}", self.code, self.message),
                    src: miette::NamedSource::new(&span.file, src.to_string()),
                    span: Some(miette::SourceSpan::new(offset.into(), 1)),
                    help: self.suggestion.clone(),
                    code: self.code.to_string(),
                    severity: self.severity,
                };
                return miette::Report::new(diag);
            }
        }

        // Fallback: no source text or can't resolve offset — plain diagnostic
        let diag = RichDiagnostic {
            message: format!("{severity_prefix}[{}]: {}", self.code, self.message),
            src: miette::NamedSource::new(
                self.span
                    .as_ref()
                    .map(|s| s.file.as_str())
                    .unwrap_or(&self.model),
                String::new(),
            ),
            span: None,
            help: self.suggestion.clone(),
            code: self.code.to_string(),
            severity: self.severity,
        };
        miette::Report::new(diag)
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let severity = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
            Severity::Info => "info",
        };

        write!(f, "{severity}[{}]: {}", self.code, self.message)?;

        if let Some(ref span) = self.span {
            write!(f, "\n --> {}:{}:{}", span.file, span.line, span.col)?;
        }

        if let Some(ref suggestion) = self.suggestion {
            write!(f, "\n = help: {suggestion}")?;
        }

        Ok(())
    }
}

/// A miette-compatible diagnostic that carries source text and spans.
///
/// This is the internal rendering type — callers create these via
/// `Diagnostic::to_miette()` or by constructing directly for parser errors.
#[derive(Debug, miette::Diagnostic, thiserror::Error)]
#[error("{message}")]
pub struct RichDiagnostic {
    /// Formatted message with severity prefix and code.
    pub message: String,

    /// The source code being diagnosed.
    #[source_code]
    pub src: miette::NamedSource<String>,

    /// Span highlighting the error location.
    #[label("here")]
    pub span: Option<miette::SourceSpan>,

    /// Actionable fix suggestion.
    #[help]
    pub help: Option<String>,

    /// Diagnostic code (e.g., "E001").
    code: String,

    /// Severity (not rendered by miette, but available for callers).
    #[allow(dead_code)]
    severity: Severity,
}

/// Convert 1-based line and column to a byte offset in source text.
fn line_col_to_offset(source: &str, line: usize, col: usize) -> Option<usize> {
    let mut current_line = 1;
    let mut line_start = 0;
    for (i, ch) in source.char_indices() {
        if current_line == line {
            let offset = line_start + col.saturating_sub(1);
            return if offset <= source.len() {
                Some(offset)
            } else {
                None
            };
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + 1;
        }
    }
    if current_line == line {
        let offset = line_start + col.saturating_sub(1);
        return if offset <= source.len() {
            Some(offset)
        } else {
            None
        };
    }
    None
}

/// Render a collection of diagnostics as rich miette output.
///
/// For each diagnostic that has a `span` with a matching `file` key in
/// `source_map`, the full source is embedded so miette can underline the
/// error. Diagnostics without source information render as plain messages.
pub fn render_diagnostics(
    diagnostics: &[Diagnostic],
    source_map: &std::collections::HashMap<String, String>,
) -> String {
    use std::fmt::Write;

    let mut buf = String::new();
    for d in diagnostics {
        let src = d
            .span
            .as_ref()
            .and_then(|s| source_map.get(&s.file).map(std::string::String::as_str));

        let report = d.to_miette(src);
        // Use miette's GraphicalReportHandler for pretty output.
        let mut rendered = String::new();
        let handler = miette::GraphicalReportHandler::new();
        if handler
            .render_report(&mut rendered, report.as_ref())
            .is_ok()
        {
            let _ = writeln!(buf, "{rendered}");
        } else {
            // Fallback to Display
            let _ = writeln!(buf, "  {d}");
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_col_to_offset() {
        let src = "SELECT *\nFROM foo\nWHERE x = 1";
        assert_eq!(line_col_to_offset(src, 1, 1), Some(0));
        assert_eq!(line_col_to_offset(src, 2, 1), Some(9));
        assert_eq!(line_col_to_offset(src, 3, 7), Some(24));
    }

    #[test]
    fn test_diagnostic_to_miette_with_source() {
        let src = "SELECT *\nFROM foo\nWHERE x = 1";
        let d = Diagnostic::error("E001", "my_model", "type mismatch on column 'x'")
            .with_span(SourceSpan {
                file: "models/my_model.sql".to_string(),
                line: 3,
                col: 7,
            })
            .with_suggestion("add explicit CAST to match types");

        let report = d.to_miette(Some(src));
        let output = format!("{report:?}");
        assert!(output.contains("E001"));
    }

    #[test]
    fn test_diagnostic_to_miette_without_source() {
        let d = Diagnostic::warning("W001", "my_model", "implicit coercion");
        let report = d.to_miette(None);
        let output = format!("{report:?}");
        assert!(output.contains("W001"));
    }

    #[test]
    fn test_render_diagnostics_basic() {
        let d = Diagnostic::error("E001", "test", "something broke").with_suggestion("fix it");
        let rendered = render_diagnostics(&[d], &std::collections::HashMap::new());
        assert!(rendered.contains("E001"));
        assert!(rendered.contains("something broke"));
    }

    #[test]
    fn test_budget_exceeded_constructs_correctly() {
        let d = Diagnostic::budget_exceeded("fct_orders", 12.50, 10.00);
        assert_eq!(d.severity, Severity::Error);
        assert_eq!(d.code.as_ref(), E027);
        assert_eq!(d.model, "fct_orders");
        assert!(
            d.message.contains("12.5000"),
            "message must include projected cost, got: {}",
            d.message
        );
        assert!(
            d.message.contains("10.0000"),
            "message must include ceiling cost, got: {}",
            d.message
        );
        assert!(
            d.suggestion.is_some(),
            "budget_exceeded must include a suggestion"
        );
        assert!(d.is_error());
    }

    #[test]
    fn test_budget_exceeded_bytes_constructs_correctly() {
        let d = Diagnostic::budget_exceeded_bytes("fct_orders", 5_000_000, 1_000_000);
        assert_eq!(d.severity, Severity::Error);
        assert_eq!(d.code.as_ref(), E027);
        assert_eq!(d.model, "fct_orders");
        assert!(
            d.message.contains("5000000"),
            "message must include projected bytes, got: {}",
            d.message
        );
        assert!(
            d.message.contains("1000000"),
            "message must include ceiling bytes, got: {}",
            d.message
        );
        assert!(d.suggestion.is_some());
        assert!(d.is_error());
    }

    #[test]
    fn test_budget_exceeded_serializes() {
        let d = Diagnostic::budget_exceeded("my_model", 5.0, 3.0);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("E027"));
        assert!(json.contains("my_model"));
        // Round-trip
        let back: Diagnostic = serde_json::from_str(&json).unwrap();
        assert_eq!(back.code.as_ref(), E027);
    }

    #[test]
    fn test_e027_constant_value() {
        assert_eq!(E027, "E027");
    }
}
