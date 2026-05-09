# Rocky monorepo — security audit

**Date:** 2026-05-08
**Scope:** `engine/` (20 Rust crates), `integrations/dagster/` (Python), `editors/vscode/` (TypeScript), `examples/playground/`, `scripts/`, `.git-hooks/`
**Method:** parallel static review across 7 concern areas (secrets, SQL, subprocess, fs/deserialization, network/TLS, LSP/IPC, AI), plus `npm audit`. `cargo audit` and `pip-audit` were not installed in this environment (see Limitations).

## Executive summary

The Rocky codebase shows mature security discipline. There are **no critical findings** and the warehouse-adapter SQL paths — the largest blast-radius surface — are systematically gated by a centralized `validate_identifier()` regex check before interpolation. Hardened defaults observed across `rocky-server` (loopback-only without a Bearer token, constant-time token compare, empty-CORS default), redacted credential wrappers, `execFile`-style subprocess invocation, and CSP+nonce in webviews.

The findings below are **mostly Medium or Low**. The two highest-impact ones are operational rather than structural:

1. **Server-supplied URL follow-up with re-sent `Authorization` header** in the Trino and Airbyte adapters (M-1) — credential exposure if the configured coordinator/API host is compromised or a TLS MITM is in play.
2. **Unbounded LSP document cache** (M-2) — DoS / OOM if a malicious LSP client (or a buggy editor extension) sends a multi-GB `textDocument/didChange`.

| Severity | Count |
|----------|-------|
| Critical | 0 |
| High     | 0 |
| Medium   | 5 |
| Low      | 9 |
| Notes / positive observations | 12 |

---

## Findings — Medium

### M-1 — `Authorization` header re-sent to server-supplied follow-up URLs (Trino & Airbyte)

| Field | Value |
|---|---|
| Files | `engine/crates/rocky-trino/src/connector.rs:342`, `engine/crates/rocky-airbyte/src/client.rs:167-171` |
| CWE | CWE-200 (information exposure) / CWE-918 (SSRF) |
| Severity | Medium |

The Trino connector polls follow-up pages by GETting the server-supplied `nextUri` with the same headers (`headers.clone()` includes the `Authorization` Bearer/Basic header). The Airbyte client iterates pagination with the server-supplied `next` URL and re-issues `bearer_auth(...)` per request via the shared client.

If the configured coordinator/API host is compromised, on-path (TLS MITM with a misconfigured trust store or downgraded host), or simply mis-typed in `rocky.toml` to point at an attacker, the server can hand back a `nextUri`/`next` pointing at any host on the internet — and the credential is forwarded.

**Remediation:**
- Validate that `next` is on the same scheme + host as `base_url` / `coordinator_url`. Reject otherwise.
- Alternatively, switch to relative-path-only pagination (Trino docs allow this; Airbyte's API returns absolute URLs but the same-host invariant can be checked).
- For defense-in-depth: configure the `reqwest::Client` with `redirect::Policy::limited` and strip `Authorization` on cross-origin redirects.

### M-2 — Unbounded LSP document cache (DoS / OOM)

| Field | Value |
|---|---|
| File | `engine/crates/rocky-server/src/lsp.rs:843-852` |
| CWE | CWE-770 (allocation without limits) |
| Severity | Medium (DoS only; trust boundary is the editor) |

`did_change` inserts the full `change.text` into `documents: HashMap<String, String>` with no per-document size cap and no global eviction. A malicious or buggy LSP client can OOM the language server with a single multi-GB `didChange`, taking down the editor session. tower-lsp's JSON-RPC framing does not bound `Content-Length`.

**Remediation:**
- Reject `did_change` whose `text.len() > 100 MB` (configurable).
- Add an LRU policy or open-document count cap on `documents`.
- Document the limit in `ServerCapabilities.experimental` so well-behaved clients know.

### M-3 — Hook commands run via `sh -c` from `rocky.toml`

| Field | Value |
|---|---|
| File | `engine/crates/rocky-core/src/hooks/mod.rs:728-768`, `:834-898` |
| CWE | CWE-78 (command injection by design) |
| Severity | Medium (trust-boundary, not a bug — but worth tightening docs) |

Pre/post-hook strings are passed to `sh -c <command>`. This is the documented design, and the runtime context (`{run_id}`, `{event}`, etc.) is delivered via stdin as JSON, **not** interpolated into the command string. The hazard is when users build hook commands by string-formatting untrusted values (e.g., a webhook payload, a Fivetran response) into the `command` field of `rocky.toml`.

**Remediation:**
- Add a doc note in [`rocky-config`](../.claude/skills/rocky-config/SKILL.md) and `hooks` section explicitly forbidding interpolation of untrusted values into `command`.
- Optional: add a `--strict-hooks` lint that flags `command` strings containing `$(...)`, backticks, or unquoted `$VAR` substitutions.

### M-4 — Path traversal via `models = "..."` in `rocky.toml`

| Field | Value |
|---|---|
| File | `engine/crates/rocky-cli/src/scope.rs:131-153` |
| CWE | CWE-22 |
| Severity | Medium (low real-world impact: user controls their own `rocky.toml`) |

`models_dir = project_root.join(models_base.trim_end_matches('/'))` is not canonicalized, and `models_base` comes from the user-controlled glob in `rocky.toml`. A malicious or careless config like `models = "../../etc"` will read outside the project root. Within `models/`, `read_dir` follows symlinks (Rust default).

The realistic threat is **less** "attacker exploits config" (they wrote their own config) and **more** "second user runs `rocky` against a shared/cloned project that secretly resolves `models` outside the repo, leaking unrelated SQL/secrets through error messages or LSP hovers."

**Remediation:**
- After joining, `canonicalize()` and assert `models_dir.starts_with(&project_root_canonical)`.
- Optionally skip symlinked entries in `read_dir` traversal.

### M-5 — `GOOGLE_APPLICATION_CREDENTIALS` file mode not validated

| Field | Value |
|---|---|
| File | `engine/crates/rocky-bigquery/src/auth.rs:158-162` |
| CWE | CWE-732 (incorrect permission assignment) |
| Severity | Medium (configuration risk, not a code bug) |

The BigQuery adapter reads the service-account JSON from the path in `GOOGLE_APPLICATION_CREDENTIALS` without warning if it's group/world-readable. On shared CI runners this is the most common foot-gun for SA-key leakage.

**Remediation:**
- On Unix, `tracing::warn!` if `metadata.permissions().mode() & 0o077 != 0`. Don't refuse — just warn loudly.
- Document the recommended `chmod 600` in `rocky-bigquery` README.

---

## Findings — Low

### L-1 — `npm audit` high (dev-only): `fast-uri` ≤ 3.1.1

| Field | Value |
|---|---|
| Package | `fast-uri` (transitively via `ajv`) |
| Advisories | GHSA-q3j6-qgpj-74h6, GHSA-v39h-62p7-jpjc |
| Tree | dev-only (does not ship in the VSIX) |
| Severity | Low (dev-time only) |

Affects schema-validation tooling at build time. No runtime exposure. **Remediation:** `npm update ajv` or add a `package.json` override pinning `fast-uri ≥ 3.1.2`.

### L-2 — Snowflake login error body returned verbatim to logs

| File | `engine/crates/rocky-snowflake/src/auth.rs:373-387` |
| CWE | CWE-209 (information exposure through error message) |
| Severity | Low |

`AuthError::ApiError { status, body: text }` captures the unvalidated response body. Snowflake's API doesn't return tokens in errors today, but defensive truncation (e.g., first 500 chars) is cheap.

### L-3 — Fivetran error response surfaced unredacted

| File | `engine/crates/rocky-fivetran/src/client.rs:217-231` |
| Severity | Low (Fivetran error docs don't contain credentials) |

Same shape as L-2 — defensive truncation is cheap insurance.

### L-4 — Webhook `secret` field is `String`, not `RedactedString`

| File | `engine/crates/rocky-core/src/hooks/webhook.rs` (`WebhookConfig`) |
| Severity | Low |

The HMAC secret used for outbound `X-Rocky-Signature` is stored as a plain `String`. It is not currently logged, but a future `Debug` print of `WebhookConfig` would leak it. **Remediation:** wrap in the existing `RedactedString` newtype.

### L-5 — `rocky-ai` HTTP client has no timeouts

| File | `engine/crates/rocky-ai/src/client.rs:113` |
| Severity | Low (availability) |

`reqwest::Client::new()` with no `.timeout()` / `.connect_timeout()`. Other adapters all set 10s connect / 120s request. A stalled Anthropic call can pin the pipeline. **Remediation:** add timeouts matching other adapters.

### L-6 — AI features have no documented opt-in / data-handling notice

| Files | `engine/crates/rocky-ai/src/prompt.rs`, `docs/src/content/docs/guides/ai-features.md` |
| Severity | Low (transparency / governance) |

Setting `ANTHROPIC_API_KEY` is the only gate. The prompt sends model SQL, table names, and column names to Anthropic. No row data or credentials are sent (verified by reading prompt.rs / explain.rs / testgen.rs). Still: a `[ai] enabled = false` config switch and a first-run notice would close the surprise gap.

### L-7 — Test code uses `unsafe { std::env::set_var/remove_var }`

| Files | `engine/crates/rocky-cli/src/pipes.rs:188-193`, `commands/run_audit.rs`, `rocky-core/src/models.rs` |
| Severity | Low (test-only, no runtime impact) |

Aligns with the [`rust-unsafe`](../engine/.claude/skills/rust-unsafe/SKILL.md) skill — the two legitimate unsafe sites today are mmap + test env-var mutation. Consider centralizing the env-mutation `unsafe` into a single `ScopedEnvGuard` helper to make future audits cheaper.

### L-8 — BigQuery `describe_table_sql` interpolation bug (functional, not security)

| File | `engine/crates/rocky-bigquery/src/dialect.rs:107-114` |
| Severity | Low (correctness; **not** an injection — identifier validation is upstream) |

Interpolates a backtick-quoted FQTN into a WHERE clause matching `INFORMATION_SCHEMA.COLUMNS.table_name`. The system column is unquoted, so the query returns zero rows. Track as a bug, not a vulnerability — flagged here only because the SQL audit passed through it.

### L-9 — `rocky-core/src/mmap.rs` `unsafe { Mmap::map }` over project files

| File | `engine/crates/rocky-core/src/mmap.rs:41` |
| Severity | Low (documented design trade-off) |

Concurrent edits to project SQL/TOML files during a compile produce a "garbled read → parse error," not UB. Documented. Aligned with the `rust-unsafe` skill's allowlist.

---

## Positive observations (defenses that are working)

1. **Centralized identifier validation.** `engine/crates/rocky-sql/src/validation.rs` enforces `^[a-zA-Z0-9_]+$` for table/schema/column names; called from every adapter's MERGE / ALTER / CREATE / DROP path. Drift remediation, MERGE keys, partition columns, GRANT/REVOKE principals all gated.
2. **Tag/metadata reserved-character rejection.** `rocky-core/src/catalog.rs:224-238` rejects `--`, `/*`, `*/`, `'`, `;`, CR/LF in tag keys/values with explicit injection-attempt tests.
3. **`RedactedString` everywhere.** Databricks/Snowflake/BigQuery/Trino/Fivetran/Airbyte/AI credentials are wrapped; `Debug` redacts; `.expose()` is called only at HTTP request boundaries.
4. **`rocky-server` HTTP API hardened.** `engine/crates/rocky-server/src/api.rs:108-121` refuses to bind a non-loopback host without a Bearer token. `auth.rs:82-93` does constant-time token compare. Default CORS is empty (same-origin only). Method allowlist limited to `GET/POST/OPTIONS`.
5. **Fivetran path-traversal hardening.** `encode_path_segment()` in `rocky-fivetran/src/client.rs:15-18` percent-encodes IDs before URL-path interpolation.
6. **Subprocess invocation always argv-based.** Dagster's `dagster_rocky/resource.py` builds `subprocess.Popen(argv_list, …)`; VS Code's `rockyCli.ts:62-70` uses `execFile` (no shell). No `shell=True` or `child_process.exec()` in product code.
7. **VS Code webviews use CSP + nonces.** `lineage.ts`, `doctor.ts`, `runSummary.ts` set `script-src 'nonce-...'`; `localResourceRoots` constrains script load. `escapeHtml` is consistent across `htmlUtil.ts`.
8. **Webview message allowlist.** `doctor.ts:20-37` validates `command` against a `Set<...>` before dispatch; CSRF-style command-injection vector closed.
9. **Constant-time HMAC / token compare.** `rocky-server/src/auth.rs` `constant_time_eq` (with tests). Outbound webhook HMAC built via `hmac::Hmac<Sha256>`.
10. **No insecure-TLS escape hatches.** Repo-wide grep for `danger_accept_invalid_certs`, `accept_invalid_hostnames`, `rejectUnauthorized: false`, `verify=False` returns no matches in product code.
11. **No unsafe Python deserialization.** No `pickle.load`, `yaml.load` (without `SafeLoader`), `eval`, or `exec` in Dagster integration. JSON-only parsing.
12. **Dagster argv redaction for logged invocations.** `_redact_argv` / `_REDACTED_ARGV_FLAGS` in `resource.py:99-140` masks credential-bearing flags when logged via `context.log`, while the subprocess receives the real value (correct: argv isn't readable to other users on a modern kernel).

---

## Limitations

- `cargo audit` and `pip-audit` were not installed in the audit environment. The repo's CI runs `cargo audit` weekly via `engine-weekly.yml`. Recommend running `pip-audit` against `integrations/dagster/uv.lock` similarly. (The repo already has `cargo-deny` per `rust-dep-hygiene`.)
- This is a static review. Dynamic checks (fuzzing the LSP, fuzzing the SQL builder per dialect, replay testing the Trino/Airbyte clients against an attacker-controlled mock) are out of scope.
- The `rocky-ai` prompt contents were inspected at the structural level; a deeper review of every prompt template's potential for prompt-injection-from-warehouse-metadata is recommended **if** future work starts piping column comments / table descriptions / `dbt`-style docs into prompts.

---

## Recommended next actions (prioritized)

| # | Action | Effort | Owner |
|---|---|---|---|
| 1 | Validate same-host on Trino `nextUri` and Airbyte `next` (M-1) | S | engine adapters |
| 2 | Cap LSP `did_change` text size (M-2) | XS | rocky-server |
| 3 | `npm update ajv` to pull `fast-uri ≥ 3.1.2` (L-1) | XS | vscode |
| 4 | Canonicalize+contain `models_dir` in `scope.rs` (M-4) | XS | rocky-cli |
| 5 | Warn on world-readable `GOOGLE_APPLICATION_CREDENTIALS` (M-5) | XS | rocky-bigquery |
| 6 | Wrap `WebhookConfig.secret` in `RedactedString` (L-4) | XS | rocky-core |
| 7 | Add `.timeout()` / `.connect_timeout()` to `rocky-ai` client (L-5) | XS | rocky-ai |
| 8 | Doc note: hooks may not interpolate untrusted values (M-3) | XS | docs |
| 9 | Add `[ai] enabled = false` opt-out + first-run notice (L-6) | S | rocky-ai + docs |
| 10 | Add `pip-audit` to `engine-weekly.yml` Dagster job | XS | CI |
