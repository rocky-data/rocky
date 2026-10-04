You are one worker in a coordinated effort to close Rocky's capability gaps against dbt v2. A coordinator session will merge every worker's branch into one PR. Work only on your package below.

## Ground rules (all workers)

- Repo: rocky-data/rocky, base = origin/main at c1a5f41 (engine 1.76.0). Read `AGENTS.md`, `engine/AGENTS.md`, `AGENT_REVIEW.md`, and `.claude/skills/rocky-dev/SKILL.md` first. Follow the more specific skill when it applies (`rocky-codegen` if you touch any `*Output` struct, `rocky-dsl-change` if you touch DSL syntax, `rocky-config` for `rocky.toml` keys).
- Branch: work on `BRANCH` (create it from origin/main). Commit and push to it with `git push -u origin BRANCH`. Do NOT open a pull request. Do NOT push anywhere else.
- Commits: conventional commits scoped by crate, e.g. `feat(engine/rocky-compiler): ...`. NEVER add `Co-Authored-By` trailers (repo rule). Do not mention model names in commits or code.
- Diagnostic codes: use ONLY the codes reserved for your package (listed below). Other workers own other codes. Add each code to `engine/crates/rocky-compiler/src/diagnostic.rs` with a doc comment in the existing style, and to the docs diagnostic reference page if one lists codes.
- Keep your diff focused. Do not refactor unrelated code. Do not reformat files you don't otherwise touch. This minimizes merge conflicts with the other workers, who edit `typecheck.rs`, `engine/rocky/src/main.rs`, and `diagnostic.rs` in parallel. Prefer adding new modules/files over large edits to shared hot files; keep edits to `main.rs` and `diagnostic.rs` small and additive.
- If you change any CLI JSON output struct, run `just codegen` and commit the regenerated schemas/bindings. Update hand-written SDK models in `sdk/python/src/rocky_sdk/types.py` if a new top-level field is added (see `sdk/python/AGENTS.md`).
- Validation before every push (must be clean): from `engine/`: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings` (or the exact command in engine/AGENTS.md / `.github/workflows/engine-ci.yml`), and `cargo test` for every crate you touched plus `rocky-cli` integration tests that exercise your feature. Mirror whatever engine-ci.yml runs.
- Tests: add unit tests AND at least one end-to-end CLI test (DuckDB, no credentials) that proves the user-visible behavior, including valid controls that must stay clean (no false refusals). False refusals of valid SQL are worse than misses.
- Docs: update the public docs page(s) under `docs/src/content/docs/` that describe your feature (follow `docs/STYLE.md`), and `CHANGELOG` entries if the repo keeps an Unreleased section for the engine.
- Finish with a short report as your final message: what you built, files touched, tests added, validation commands run and their results, known limits, and answers to: "What are you least confident about right now?" and "What's the most important thing I'm missing about this situation?"
- Be autonomous. Do not wait for human input. If a design choice is ambiguous, pick the conservative option (no false refusals, opt-in for risky behavior), document it, and continue.

## Reference corpus (DuckDB). Seed:
```sql
CREATE SCHEMA raw;
CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, order_date DATE);
CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);
```
Cases (model name: SQL). Defects that dbt v2 strict mode refuses at compile time:
- D1 `stg_orders: SELECT order_id, customer_id, order_total FROM raw.orders` (missing source column)
- D2 `bad_agg: SELECT customer_id, SUM(customer_name) AS s FROM raw.customers GROUP BY customer_id`
- D3 two models: `stg_orders: SELECT order_id, customer_id, amount AS order_amount FROM raw.orders`; `fct_revenue: SELECT order_id, amount FROM stg_orders` (Rocky already emits E039)
- D5 `bad_join: SELECT o.order_id, c.customer_name FROM raw.orders o JOIN raw.customers c ON o.customer_id = c.customer_name`
- D6 `bad_group: SELECT customer_id, status, SUM(amount) AS t FROM raw.orders GROUP BY customer_id`
Valid controls that MUST compile clean (exit 0, no new error):
- C2 `stg_orders: SELECT order_id, customer_id, amount FROM raw.orders`; `fct_revenue: SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name`
- V1 `SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders` (DuckDB lateral alias)
- V2 `SELECT 10::BIGINT = '10'::VARCHAR AS equal_value` (valid DuckDB coercion; dbt wrongly refuses this — Rocky must not)
- V3 `SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped`
- V4 `SELECT sha256(customer_name) AS customer_hash FROM raw.customers` (outside inference; stays Unknown, no error)
- G1-S2 `WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) SELECT stg_orders.amount FROM stg_orders` (CTE shadows a model)
- G1-S3 `stg_orders: SELECT * FROM raw.orders`; `SELECT s.amount FROM stg_orders AS s`
- G1-S4 stale source schema: the compile-time seed for raw.orders LACKS `amount`, but the warehouse has it; `stg_orders: SELECT order_id, customer_id, amount AS order_amount FROM raw.orders`. Must stay exit 0 when the schema came from a seed/cache (stale schemas must not refuse).
Rocky compile command used: `rocky compile --with-seed --output json` (seed.sql beside rocky.toml).
