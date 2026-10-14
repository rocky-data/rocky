# Rocky Explained: a short tour

A plain-English tour of Rocky. Each section is short and links to the full page on [rocky-data.dev](https://rocky-data.dev).

| # | Section |
|---|---|
| 1 | [What Rocky is](#1-what-rocky-is) |
| 2 | [The core loop](#2-the-core-loop) |
| 3 | [Checking your project](#3-checking-your-project) |
| 4 | [State and incremental loads](#4-state-and-incremental-loads) |
| 5 | [Branches, previews and replay](#5-branches-previews-and-replay) |
| 6 | [The browser UI](#6-the-browser-ui) |
| 7 | [The VS Code extension](#7-the-vs-code-extension) |
| 8 | [Governance and policy](#8-governance-and-policy) |
| 9 | [Agents and MCP](#9-agents-and-mcp) |
| 10 | [Data products and `rocky fulfill`](#10-data-products-and-rocky-fulfill) |
| 11 | [Adapters](#11-adapters) |
| 12 | [Safety limits in one table](#12-safety-limits-in-one-table) |
| 13 | [Quick reference](#13-quick-reference) |
| 14 | [Not covered here](#14-not-covered-here) |

---

## 1. What Rocky is

Rocky is a typed compiler and run engine for SQL.

You write SQL and TOML. Rocky compiles the project and reports the problems it can find without a warehouse. A later command sends the generated SQL to your warehouse.

Storage and compute stay in the warehouse. Rocky owns the graph between your code and the warehouse.

```
You write this:            Rocky does this:              Warehouse gets this:
─────────────────          ──────────────────────────    ──────────────────────
SELECT                     1. Parse SQL into a typed     INSERT INTO
  order_id,                   tree                         orders_summary
  SUM(amount) AS total     2. Resolve dependencies       SELECT
FROM raw_orders            3. Type-check columns           order_id,
GROUP BY order_id          4. Validate contracts           SUM(amount) AS total
                           5. Generate dialect SQL       FROM raw_orders
                           6. Run it                     WHERE updated_at > '...'
                                                         GROUP BY order_id
```

There is no Jinja and no string substitution. Raw SQL is the main input. An optional DSL exists for the cases SQL handles badly.

A clean compile does not prove that every warehouse query is valid. It does not prove the result values are correct. A separate run can still fail at the warehouse.

Rocky's terms are in the [glossary](https://rocky-data.dev/reference/glossary/). The full introduction is [here](https://rocky-data.dev/getting-started/introduction/).

---

## 2. The core loop

Every Rocky workflow follows three steps.

```
  compile ──▶ plan ──▶ apply / run ──▶ warehouse
     │          │           │
     │          │           └─ writes data, runs checks, records a run
     │          └─ writes .rocky/plans/<plan_id>.json
     └─ finds problems with no warehouse connection
```

| Step | Command | What it does |
|---|---|---|
| Compile | `rocky compile` | Type-checks the project. Writes nothing. |
| Plan | `rocky plan` | Records the exact work as a file. Replication pipelines only. |
| Apply | `rocky apply <plan_id>` | Runs a saved plan. |
| Run | `rocky run` | Plans and applies in one step. Good for local work and automation. |

`rocky run` does these things, in order:

1. Mint a `run_id`. Every action is tagged with it.
2. Load `rocky.toml` and build the adapters.
3. Discover the sources.
4. Compile, then sort the models by dependency.
5. For each model, in parallel within a layer: detect drift, generate SQL, execute it, run checks, queue the watermark.
6. Commit the watermarks in one batch.
7. Fire the post-run hooks and print JSON.

The exit code is `0` for success, `2` for partial success, `1` for failure and `130` for Ctrl-C.

Not every step runs for every pipeline type. Drift detection, watermarks and hooks belong to the replication path.

Full page: [execution flow](https://rocky-data.dev/concepts/execution-flow/).

---

## 3. Checking your project

The compiler reads your models and produces a typed project plus diagnostics. Each diagnostic has a `code`, a severity, a message, a span and a suggestion.

```
  load project ─▶ build graph ─▶ type-check ─▶ contracts ─▶ lints ─▶ diagnostics
```

| Code | Meaning |
|---|---|
| `E001` | A join key has no common type between two upstream models. |
| `E010` to `E014` | A contract violation: a column is missing, retyped, or changes nullability. |
| `E035` | Managed-Iceberg `format_options` the warehouse would reject. |
| `E036` | Two models write the same target table. |
| `E037` | An `incremental` transformation model has no watermark (`timestamp_column`). |
| `W001` | Implicit type coercion on a join key. |
| `P001` | A dialect-portability lint. A Snowflake-only construct in a Databricks project, or the reverse. |
| `P002` | A `SELECT *` model feeds consumers that read specific columns. |
| `I003` | A contract declared a type Rocky could not infer, so it did not check it. |

Rocky checks only what it can resolve. With no source schema, a column type is `Unknown`. Then a contract on it is not checked, and you get `I003`.

`rocky compile` adds some codes the language server and `rocky serve` never report. `rocky test` runs models on an in-memory DuckDB. `rocky emit-sql` renders plain runnable SQL with no warehouse connection, so you can always leave Rocky.

Full pages: [compiler](https://rocky-data.dev/concepts/compiler/) and [testing](https://rocky-data.dev/concepts/data-quality-checks/).

---

## 4. State and incremental loads

Rocky keeps its state in a small embedded database (redb) next to your project. There is no external database and no manifest file.

The state store holds watermarks, run progress, run history, partitions, idempotency keys, branches and the policy ledger.

A **watermark** is the timestamp of the newest row Rocky has already loaded. The next run loads only rows newer than it.

```
  First run:    CREATE TABLE target AS SELECT * FROM source
                watermark = MAX(updated_at) in the target

  Next run:     INSERT INTO target SELECT * FROM source
                WHERE updated_at > <watermark>
```

Rocky reads the new watermark from the target, not the source. New rows can land in the source during a run. The target holds only what Rocky wrote, so the watermark never moves past unprocessed data.

- A failed table keeps its old watermark. A sibling's failure does not hold back a successful table.
- `rocky run --resume-latest` skips the tables that already copied. It works for replication runs only.
- A transformation model declares `timestamp_column` and `@incremental_filter` instead. Without a watermark it is `E037`.
- The skip-unchanged gate is off by default. Turn it on with `skip_unchanged = true` under `[run]`, or with `--skip-unchanged`. It is a best-effort optimization, not a promise that a rebuild would give the same rows.

Full pages: [incremental](https://rocky-data.dev/concepts/incremental/), [state management](https://rocky-data.dev/concepts/state-management/) and [skip and defer](https://rocky-data.dev/guides/skip-and-defer/).

---

## 5. Branches, previews and replay

**Branches.** A branch is a named, isolated schema.

```
  rocky branch create <name>     records a schema prefix: branch__<name>
  rocky run --branch <name>      writes every model to that prefix
  rocky branch compare           diffs the branch against production
  rocky branch approve           writes an approval artifact
  rocky branch promote           copies each table into production
```

- A branch name is 1 to 64 characters from `[A-Za-z0-9_]`.
- `branch approve` stamps the artifact with a blake3 digest of its own JSON. That is an integrity digest, not a signature. Nothing holds a key.
- `branch promote` runs `CREATE OR REPLACE TABLE <prod> AS SELECT * FROM <branch>`. The branch tables stay where they are.
- `branch delete` removes the record. It drops no warehouse table.
- A table that `branch compare` cannot read reports `verdict: "error"` and a `null` count, never `0`.

**Previews.** `rocky preview create` plans a PR workflow. It runs nothing. You then run `rocky run --branch <name>`. `preview diff` and `preview cost` read that run.

`preview diff` compares recorded `rows_affected` counts by default. Pass `--algorithm=bisection` for a row-content diff.

**Replay.** Three commands read a recorded run from the state store:

| Command | Shows |
|---|---|
| `rocky replay <run-id\|latest>` | SQL hashes, row counts and timings. `--check` audits whether the recording is enough to re-execute. |
| `rocky trace <run-id\|latest>` | A timeline with concurrency lanes. |
| `rocky cost <run-id\|latest>` | Per-model cost. It is an allocation, not a warehouse bill. |

Full pages: [shadow mode](https://rocky-data.dev/concepts/shadow-mode/), [preview a PR](https://rocky-data.dev/guides/preview-a-pr/), [verify a run](https://rocky-data.dev/guides/verify-a-run/) and [`rocky branch`](https://rocky-data.dev/reference/commands/core-pipeline/#rocky-branch).

---

## 6. The browser UI

`rocky serve --ui` serves a browser UI at `/ui/`. It needs no editor. It shows models and the DAG, runs, plans waiting for review, and the recorded policy decisions.

```
  rocky serve --ui
        │
        ├─▶ prints a /login?t=<token> link   (opening it sets a session cookie)
        └─▶ serves /ui/ and /api/v1/…        (the same JSON as the CLI)
```

The bind and the flags decide what the UI may do:

| How the server starts | What the UI may do |
|---|---|
| Loopback bind, no `--token`, `--allowed-host` or `--allowed-origin` | Operator mode. The UI can run, plan, approve and apply as the user who started the server. |
| `--read-only` | View only. |
| `--allowed-host` or `--allowed-origin` | View only. |
| Any other bind | Needs `--token --read-only`. |

**Operator mode on loopback can approve and apply.** An SSH tunnel or `kubectl port-forward` looks local. Use `--read-only` whenever someone else can reach your port.

A read-only scope answers `403` to any method except `GET`, `HEAD` and `OPTIONS`. Release binaries and the container image include the UI.

Full page: [the browser UI](https://rocky-data.dev/guides/browser-ui/). Reference: [`rocky serve`](https://rocky-data.dev/reference/commands/development/#rocky-serve).

---

## 7. The VS Code extension

The extension starts a language server (LSP) and talks to it over stdio. LSP is the protocol an editor uses to ask a language tool for types, errors and completions.

The extension prefers the standalone `rocky-lsp` binary. It falls back to `rocky lsp`.

```
  you edit orders.sql ──▶ language server recompiles (300 ms after you stop typing)
                      ◀── diagnostics: red squiggle on the bad line
  you hover "amount"  ──▶ typed column, lineage and consumers
```

The server gives you:

- Inline diagnostics for the codes the compiler emits.
- Hover types, go to definition, find references and rename.
- Completion, signature help, inlay hints and code actions.
- Formatting for `.rocky` files.

The extension adds its own commands, such as "Open Compiled SQL", "Preview Model Rows", "Show Model Lineage" and "Run Pipeline".

Full page: [IDE setup](https://rocky-data.dev/guides/ide-setup/).

---

## 8. Governance and policy

Rocky enforces governance from files in the project.

| Area | What it does | The limit |
|---|---|---|
| **Contracts** | Declare required columns and types. Violations are `E010` to `E014` at compile time. | A type Rocky cannot infer is not checked (`I003`). |
| **Masking** | Hash, redact or partly mask classified columns per environment. `rocky compliance --fail-on exception` gates CI. | A classification tag with no mask strategy is `W004`. |
| **Role graph** | Declare roles with `inherits` and `permissions`. On Databricks, Rocky creates the group `rocky_role_<name>` and grants it. | **Rocky never revokes grants.** It sends no `REVOKE` and deletes no group. Only Databricks applies roles. |
| **Plan gate** | `rocky apply` refuses an AI-authored plan without an approval marker. The same refusal covers backfill, gc and restore plans. | **The marker is not signed.** It records that an approval was made on this machine, not who made it. |
| **Policy** | A `[policy]` block grades what a `human` or an `agent` may do: allow, require review, or deny. | With no `[policy]` block, nothing is recorded. |

**The plan gate.** An agent proposes a plan. A person reviews it. Then `rocky apply` runs it.

```
  MCP propose ──▶ .rocky/plans/<plan_id>.json
  rocky review <plan_id>            compares the models against --base (default HEAD)
  rocky review <plan_id> --approve  writes .rocky/plans/<plan_id>.reviewed.json
  rocky apply <plan_id>             refused without a marker that names this plan
```

The classifier grades each change `Breaking`, `Warning` or `Info`. `[policy]` rules run first, so policy can only tighten the gate.

**Reading the policy plane.**

- `rocky policy check --principal <p> --capability <c> --model <m>` explains one decision. `rocky policy show`, `rocky policy test` and `rocky policy freeze` (the kill switch) do the rest.
- `rocky audit` lists recorded decisions. It is best effort. If the ledger write fails for an ordinary rule, the action still proceeds unrecorded. A rule with `autonomy_budget` or `verify_after` fails closed.
- `rocky brief --since 24h` renders the estate digest. A section with no recorded signal says `unavailable`, never all-clear.
- `rocky review --queue` ranks the plans a rule sent to review.

**Checks run after the write.** An inline check on a run cannot undo the write. A replication run that copied data and then failed a check exits `2`.

Full pages: [governance](https://rocky-data.dev/guides/governance/), [model governance](https://rocky-data.dev/concepts/model-governance/) and [`rocky policy`](https://rocky-data.dev/reference/commands/governance-reclamation/#rocky-policy).

---

## 9. Agents and MCP

`rocky mcp` runs a Model Context Protocol (MCP) server over stdio. MCP is the protocol an agent harness uses to call tools. The server runs on your machine. There is no Rocky-hosted service.

An agent can compile, read lineage and schemas, sample rows, draft models, contracts and checks, and record a plan with `propose`.

**No MCP tool runs `rocky apply`.** No tool runs SQL that changes the warehouse.

```
  agent ──▶ rocky mcp
              ├─▶ draft_* tools ──▶ files under models/
              └─▶ propose ───────▶ .rocky/plans/<plan_id>.json
  person ──▶ rocky review <plan_id> --approve
  person ──▶ rocky apply <plan_id> ──▶ warehouse
```

- The default profile refuses to write an approval marker. Only `rocky mcp --profile approver` can.
- `--profile worker` serves a minimal drafting set. The profile is fixed when the server starts.
- `sample_rows`, `profile_column`, `inspect_schema` and `drift_preview` read the warehouse.

Full pages: [MCP authoring](https://rocky-data.dev/concepts/mcp-authoring/) and [operating Rocky with agents](https://rocky-data.dev/concepts/operating-rocky-with-agents/). Reference: [`rocky mcp`](https://rocky-data.dev/reference/commands/ai/#rocky-mcp).

---

## 10. Data products and `rocky fulfill`

**`rocky fulfill` is experimental.**

A product spec at `products/<name>.toml` declares what a data product must be: grain, columns, checks, freshness and classifications. It adds no runtime semantics.

`rocky product compile` lowers the spec onto primitives the engine already enforces: contracts, declarative tests, sidecar metadata and policy posture. A field that cannot lower is refused at parse time.

The other subcommands are `verify`, `status`, `list`, `journal` and `approve`. `approve` is a human authority transition on the current spec revision.

`rocky fulfill <product>` drives this loop:

```
  elicit ─▶ approve-spec ─▶ lower ─▶ draft ─▶ verify ─▶ propose ─▶ human review
                                                                       │
                                  observe ◀── digest-gated apply ◀─────┘
```

One run advances the loop as far as it can without a human. Then it stops. It prints the state, why it stopped and the next command.

**Observation follows `apply`, so a failing output can already be live.** The exit codes are their own set:

| Exit code | Meaning |
|---|---|
| `0` | Clean stop. |
| `2` | Blocked. |
| `3` | Parked for a human. |
| `4` | Applied, but failing a check the product declares about itself. |

Full pages: [product commands](https://rocky-data.dev/reference/commands/products/) and [`rocky fulfill`](https://rocky-data.dev/reference/commands/fulfill/).

---

## 11. Adapters

An adapter separates what to do from how to talk to one warehouse. There are two families.

```
  SOURCE adapters (find what exists)      WAREHOUSE adapters (run the SQL)
  ──────────────────────────────────      ────────────────────────────────
  Fivetran, Airbyte, Iceberg catalog      Databricks        GA, most mileage
  DuckDB, BigQuery, manual list           DuckDB            local and tests
                                          Snowflake, BigQuery, Trino,
  No data is extracted. The data          PostgreSQL, Redshift, SQL Server,
  already lives in the warehouse.         ClickHouse, Spark       Beta
```

- Databricks is GA. Every other warehouse adapter except DuckDB is Beta. The core run loop works there, and conformance coverage is still growing.
- A `manual` source lists its tables in `rocky.toml`. `rocky validate` refuses a `manual` adapter with no `schemas` (`V057`).
- The `SqlDialect` trait translates one logical operation into each warehouse's SQL. An upsert is `MERGE INTO` on Databricks and Snowflake, for example.
- Some strategies are not portable. `dynamic_table` works only on Snowflake. Other adapters return a "not supported" error when Rocky generates the SQL.
- A new adapter plugs in through the Adapter SDK with no change to the core engine.

Full pages: [adapters](https://rocky-data.dev/guides/adapter-sdk/) and [capability matrix](https://rocky-data.dev/concepts/capability-matrix/).

---

## 12. Safety limits in one table

Read this table before you trust any automation.

| Limit | Where it applies |
|---|---|
| A clean compile does not prove warehouse validity or correct values. | Section 1 |
| The approval marker is not signed. It does not say who approved. | Section 8 |
| `rocky fulfill` is experimental. A failing output can already be live. | Section 10 |
| A check after a write cannot undo the write. | Section 8 |
| Rocky never revokes grants. Only Databricks applies roles. | Section 8 |
| Operator mode on loopback can approve and apply. | Section 6 |
| The branch approval digest is not a signature. | Section 5 |
| `branch promote` copies tables. `branch delete` drops none. | Section 5 |
| No MCP tool runs `rocky apply`. | Section 9 |
| The audit ledger is best effort for ordinary rules. | Section 8 |
| Snowflake, BigQuery and the other non-Databricks warehouse adapters are Beta. | Section 11 |
| A hook cannot gate a run. `rocky run` ignores every hook outcome, even `on_failure = "abort"`. | [Hooks](https://rocky-data.dev/concepts/hooks/) |
| `rocky tick` and webhook ingress are experimental. | [Running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/) |
| `rocky tick --dry-run` starts no pipeline, but it still opens the state store read-write. | [Running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/) |

---

## 13. Quick reference

`--config`, `--state-path` and `--state-namespace` are **global flags**. They go before the subcommand. Write `rocky -c rocky.toml validate`, not `rocky validate -c rocky.toml`. `--output` may go on either side.

| You want to... | Command |
|---|---|
| Check config and adapter wiring (no API calls) | `rocky -c rocky.toml validate` |
| Type-check your models | `rocky compile --models models/` |
| Test models locally (no warehouse) | `rocky test --models models/` |
| Save the work as a plan | `rocky -c rocky.toml plan` |
| Run a saved plan | `rocky -c rocky.toml apply <plan_id>` |
| Run the pipeline | `rocky -c rocky.toml run` |
| Run only the sources that match a `key=value` | `rocky -c rocky.toml run --filter source=shopify` |
| Resume a failed replication run | `rocky -c rocky.toml run --resume-latest` |
| Check watermark state | `rocky -c rocky.toml state` |
| See run history | `rocky history` |
| Trace column lineage | `rocky lineage orders_summary --column total` |
| Check the health of everything | `rocky -c rocky.toml doctor` |
| Generate a model draft | `rocky ai "create a daily revenue summary by region"` |
| Render runnable SQL offline | `rocky emit-sql --models models/ --out-dir sql/` |
| Read the project in a browser | `rocky serve --ui` |
| See what needs a human | `rocky brief --since 24h`, `rocky review --queue` |
| Explain a policy decision | `rocky policy check --principal agent --capability apply --model fct_orders` |
| Read the decision ledger | `rocky audit`, or `rocky audit --for <table\|run\|plan>` |
| Inspect a finished run | `rocky replay latest`, `rocky trace latest`, `rocky cost latest` |

Next: [Introduction](https://rocky-data.dev/getting-started/introduction/), [Quickstart](https://rocky-data.dev/getting-started/quickstart/) and the [CLI reference](https://rocky-data.dev/reference/cli/).

---

## 14. Not covered here

This tour leaves out the topics below. Each links to the full page.

| Topic | Page |
|---|---|
| The Rocky DSL (`.rocky` files) | [Rocky DSL](https://rocky-data.dev/concepts/rocky-dsl/) |
| Dagster integration | [Dagster introduction](https://rocky-data.dev/dagster/introduction/) |
| The Python SDK | [Python SDK introduction](https://rocky-data.dev/python-sdk/introduction/) |
| Hooks and webhooks | [Hooks](https://rocky-data.dev/concepts/hooks/) |
| The scheduler `rocky tick` and webhook ingress (experimental) | [Running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/) |
| Snapshots and history tables (SCD Type 2) | [Silver layer](https://rocky-data.dev/concepts/silver-layer/) |
| The container image | [Run the image](https://rocky-data.dev/guides/run-the-image/) |
| Crates and the intermediate representation (IR) | [Architecture](https://rocky-data.dev/concepts/architecture/) |
