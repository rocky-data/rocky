# rocky-trino

Trino warehouse adapter for [Rocky](https://rocky-data.dev/).

**Status:** v0, experimental. The adapter advertises `is_experimental: true`,
so the runtime logs a warning when it's selected. Coverage is intentionally
narrow — the goal of the v0 PR is to exercise `rocky-adapter-sdk` from
outside the first-party adapters and surface gaps, not ship a polished
production adapter.

## What's in v0

- **Connector:** async REST client for `POST /v1/statement` with `nextUri`
  polling, aggregating result pages into row vectors. Hand-rolled over
  `reqwest` (no high-level Trino client crate).
- **Auth:** HTTP Basic (`Authorization: Basic <user:pw>`) and JWT bearer
  (`Authorization: Bearer <token>`). Credentials are wrapped in
  `RedactedString` so `Debug` output never leaks raw secrets.
- **Dialect:** double-quoted identifiers, three-part `<catalog>.<schema>.<table>`
  references, `DESCRIBE <table>` for column introspection, ANSI-style
  `INSERT INTO`, `CREATE TABLE AS`, and `TABLESAMPLE BERNOULLI`.
- **Adapter trait:** `dialect`, `execute_statement`, `execute_query`, and
  `describe_table` against `rocky_core::traits::WarehouseAdapter`.

## Not in v0 (follow-ups)

- **MERGE** — Trino's MERGE support is connector-dependent (Iceberg yes,
  Hive limited). The dialect's `merge_into` returns "not supported in v0".
- **OAuth 2.0 / Kerberos / SPNEGO auth** — Basic + JWT only.
- **`rocky init trino` template, Docker-Trino conformance harness, and
  the playground POC** — separate small PRs.
- **Governance / loader / batch checks** — Trino's GRANT semantics depend
  on the underlying connector; deferred until there's a concrete ask.
- **`row_hash_expr`** — checksum-bisection diff against Trino is a
  follow-up; the dialect uses the trait default (errors at bisection time).
