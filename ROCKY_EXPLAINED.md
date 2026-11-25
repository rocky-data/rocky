# Rocky Explained: a short tour

A plain-English tour of Rocky. Each section links to the full page on [rocky-data.dev](https://rocky-data.dev). Terms: [glossary](https://rocky-data.dev/reference/glossary/).

---

## 1. What Rocky is

Rocky is a typed compiler and run engine for SQL. You write SQL and TOML. Rocky compiles the project and reports the problems it can find without a warehouse. A later command sends the generated SQL to your warehouse. Storage and compute stay in the warehouse.

There is no Jinja and no string substitution. Raw SQL is the main input. An optional DSL exists for the cases SQL handles badly.

A clean compile does not prove that every warehouse query is valid, or that the result values are correct. A run can still fail at the warehouse.

Full page: [Introduction](https://rocky-data.dev/getting-started/introduction/).

---

## 2. The core loop

```
  compile ──▶ plan ──▶ apply / run ──▶ warehouse
     │          │           └─ writes data, runs checks, records a run
     │          └─ writes .rocky/plans/<plan_id>.json
     └─ finds problems with no warehouse connection
```

| Command | What it does |
|---|---|
| `rocky compile` | Type-checks the project. Writes nothing. |
| `rocky plan` | Records the work as a file. Replication pipelines only. |
| `rocky apply <plan_id>` | Runs a saved plan. |
| `rocky run` | Plans and applies in one step. |

The exit code of `rocky run` is `0` for success, `2` for partial success, `1` for failure and `130` for Ctrl-C. Drift detection, watermarks and hooks belong to the replication path only.

Full page: [execution flow](https://rocky-data.dev/concepts/execution-flow/).

---

## 3. Checking your project

The compiler produces a typed project plus diagnostics. Each has a `code`, a severity, a message, a span and a suggestion.

| Code | Meaning |
|---|---|
| `E010` to `E014` | A contract violation: a column is missing, retyped, or changes nullability. |
| `E036` | Two models write the same target table. |
| `E037` | An `incremental` transformation model has no watermark (`timestamp_column`). |
| `P001` | A Snowflake-only construct in a Databricks project, or the reverse. |
| `I003` | A contract declared a type Rocky could not infer, so it did not check it. |

Rocky checks only what it can resolve. With no source schema a column type is `Unknown`, and a contract on it is not checked (`I003`). `rocky test` runs models on an in-memory DuckDB. `rocky emit-sql` renders plain runnable SQL offline, so you can always leave Rocky.

Full pages: [compiler](https://rocky-data.dev/concepts/compiler/) and [testing](https://rocky-data.dev/concepts/data-quality-checks/).

---

## 4. State and incremental loads

Rocky keeps its state in an embedded database (redb) next to your project. It holds watermarks, run progress and history, partitions, idempotency keys, branches and the policy ledger.

A **watermark** is the timestamp of the newest row Rocky has loaded. The next run loads rows with `updated_at > <watermark>`. Rocky reads the new watermark from the target, not the source, so it never moves past unprocessed data.

- A failed table keeps its old watermark. `rocky run --resume-latest` skips tables that already copied (replication only).
- A transformation model declares `timestamp_column` and `@incremental_filter`. Without a watermark it is `E037`.
- The skip-unchanged gate is off by default: `skip_unchanged = true` under `[run]`, or `--skip-unchanged`. It is best effort, not a promise of identical rows.

Full pages: [incremental](https://rocky-data.dev/concepts/incremental/), [state management](https://rocky-data.dev/concepts/state-management/) and [skip and defer](https://rocky-data.dev/guides/skip-and-defer/).

---

## 5. Branches, previews and replay

```
  rocky branch create <name>     records a schema prefix: branch__<name>
  rocky run --branch <name>      writes every model to that prefix
  rocky branch compare           diffs the branch against production
  rocky branch approve           writes an approval artifact
  rocky branch promote           copies each table into production
```

- A branch name is 1 to 64 characters from `[A-Za-z0-9_]`.
- `branch approve` stamps a blake3 digest of its own JSON. That is an integrity digest, not a signature.
- `branch promote` runs `CREATE OR REPLACE TABLE <prod> AS SELECT * FROM <branch>`. `branch delete` drops no warehouse table.
- A table that `branch compare` cannot read reports `verdict: "error"` and a `null` count, never `0`.
- `rocky preview create` plans a PR workflow and runs nothing. `rocky replay <run-id|latest>`, `rocky trace` and `rocky cost` read a recorded run. Cost is an allocation, not a warehouse bill.

Full pages: [shadow mode](https://rocky-data.dev/concepts/shadow-mode/), [preview a PR](https://rocky-data.dev/guides/preview-a-pr/) and [verify a run](https://rocky-data.dev/guides/verify-a-run/).

---

## 6. The browser UI and VS Code

`rocky serve --ui` serves a browser UI at `/ui/`: models and the DAG, runs, plans waiting for review, and policy decisions. It prints a `/login?t=<token>` link that sets a session cookie.

| How the server starts | What the UI may do |
|---|---|
| Loopback bind, no `--token`, `--allowed-host` or `--allowed-origin` | Operator mode: run, plan, approve and apply as the user who started the server. |
| `--read-only`, `--allowed-host` or `--allowed-origin` | View only. |
| Any other bind | Needs `--token --read-only`. |

**Operator mode on loopback can approve and apply.** An SSH tunnel or `kubectl port-forward` looks local. Use `--read-only` whenever someone else can reach your port.

The VS Code extension runs a language server over stdio (`rocky-lsp`, or `rocky lsp`). It gives diagnostics, hover types, go to definition, rename and completion.

Full pages: [the browser UI](https://rocky-data.dev/guides/browser-ui/) and [IDE setup](https://rocky-data.dev/guides/ide-setup/).

---

## 7. Governance and policy

| Area | What it does | The limit |
|---|---|---|
| **Contracts** | Declare required columns and types (`E010` to `E014`). | A type Rocky cannot infer is not checked (`I003`). |
| **Masking** | Hash, redact or partly mask classified columns. `rocky compliance --fail-on exception` gates CI. | A classification tag with no mask strategy is `W004`. |
| **Roles** | `inherits` and `permissions`. On Databricks, Rocky creates the group `rocky_role_<name>` and grants it. | **Rocky never revokes grants.** Only Databricks applies roles. |
| **Plan gate** | `rocky apply` refuses an AI-authored plan without an approval marker. | **The marker is not signed.** It records an approval on this machine, not who made it. |
| **Policy** | A `[policy]` block grades what a `human` or an `agent` may do: allow, require review, or deny. | With no `[policy]` block, nothing is recorded. |

```
  MCP propose ──▶ .rocky/plans/<plan_id>.json
  rocky review <plan_id> --approve   writes .rocky/plans/<plan_id>.reviewed.json
  rocky apply <plan_id>              refused without a marker that names this plan
```

`[policy]` rules run first, so policy can only tighten the gate.

- `rocky policy check --principal <p> --capability <c> --model <m>` explains one decision. `policy show`, `policy test` and `policy freeze` (the kill switch) do the rest.
- `rocky audit` lists recorded decisions. It is best effort: if the ledger write fails for an ordinary rule, the action proceeds unrecorded. A rule with `autonomy_budget` or `verify_after` fails closed.
- `rocky brief --since 24h` renders the estate digest. A section with no signal says `unavailable`, never all-clear.
- **Checks run after the write.** An inline check cannot undo the write. A replication run that copied data and then failed a check exits `2`.

Full pages: [governance](https://rocky-data.dev/guides/governance/), [model governance](https://rocky-data.dev/concepts/model-governance/) and [`rocky policy`](https://rocky-data.dev/reference/commands/governance-reclamation/#rocky-policy).

---

## 8. Agents and MCP

`rocky mcp` runs a Model Context Protocol (MCP) server over stdio, on your machine. An agent can compile, read lineage and schemas, sample rows, draft models, contracts and checks, and record a plan with `propose`. **No MCP tool runs `rocky apply`**, and no tool runs SQL that changes the warehouse.

The default profile refuses to write an approval marker. Only `rocky mcp --profile approver` can. `--profile worker` serves a minimal drafting set. The profile is fixed at server start.

Full pages: [MCP authoring](https://rocky-data.dev/concepts/mcp-authoring/), [operating Rocky with agents](https://rocky-data.dev/concepts/operating-rocky-with-agents/) and [`rocky mcp`](https://rocky-data.dev/reference/commands/ai/#rocky-mcp).

---

## 9. Data products and `rocky fulfill`

**`rocky fulfill` is experimental.** A spec at `products/<name>.toml` declares what a data product must be: grain, columns, checks, freshness and classifications. `rocky product compile` lowers it onto contracts, declarative tests, sidecar metadata and policy posture. A field that cannot lower is refused at parse time.

The loop is `elicit`, `approve-spec`, `lower`, `draft`, `verify`, `propose`, human review, digest-gated `apply`, `observe`.

`rocky fulfill <product>` advances the loop as far as it can without a human, then prints the state and the next command. **Observation follows `apply`, so a failing output can already be live.** Exit codes: `0` clean stop, `2` blocked, `3` parked for a human, `4` applied but failing a check the product declares about itself.

Full pages: [product commands](https://rocky-data.dev/reference/commands/products/) and [`rocky fulfill`](https://rocky-data.dev/reference/commands/fulfill/).

---

## 10. Adapters

An adapter separates what to do from how to talk to one warehouse.

Databricks is GA. DuckDB serves local work and tests. Snowflake, BigQuery, Trino, PostgreSQL, Redshift, SQL Server, ClickHouse and Spark are Beta. Source adapters (Fivetran, Airbyte, Iceberg catalog, manual list) find what exists and extract no data.

- `rocky validate` refuses a `manual` source with no `schemas` (`V057`).
- Some strategies are not portable. `dynamic_table` works only on Snowflake. Other adapters return a "not supported" error.

Full pages: [adapters](https://rocky-data.dev/guides/adapter-sdk/) and [capability matrix](https://rocky-data.dev/concepts/capability-matrix/).

---

## 11. Limits worth knowing

- A hook cannot gate a run. `rocky run` ignores every hook outcome, even `on_failure = "abort"`. See [Hooks](https://rocky-data.dev/concepts/hooks/).
- `rocky tick` and webhook ingress are experimental. `rocky tick --dry-run` starts no pipeline but still opens the state store read-write. See [Running without an orchestrator](https://rocky-data.dev/guides/running-without-an-orchestrator/).
- The unsigned approval marker, operator mode on loopback, best-effort audit and no-revoke grants are in sections 6 to 8.

---

## 12. Quick reference

`--config`, `--state-path` and `--state-namespace` are **global flags**. They go before the subcommand: `rocky -c rocky.toml validate`, not `rocky validate -c rocky.toml`. `--output` may go on either side.

| You want to... | Command |
|---|---|
| Check config and adapter wiring (no API calls) | `rocky -c rocky.toml validate` |
| Type-check your models | `rocky compile --models models/` |
| Test models locally (no warehouse) | `rocky test --models models/` |
| Save the work as a plan, then run it | `rocky -c rocky.toml plan`, `rocky -c rocky.toml apply <plan_id>` |
| Run the pipeline | `rocky -c rocky.toml run` |
| Resume a failed replication run | `rocky -c rocky.toml run --resume-latest` |
| Check watermark state, run history | `rocky -c rocky.toml state`, `rocky history` |
| Generate a model draft | `rocky ai "create a daily revenue summary by region"` |
| See what needs a human | `rocky brief --since 24h`, `rocky review --queue` |
| Explain a policy decision | `rocky policy check --principal agent --capability apply --model fct_orders` |
| Read the decision ledger | `rocky audit`, or `rocky audit --for <table\|run\|plan>` |

Next: [Quickstart](https://rocky-data.dev/getting-started/quickstart/) and the [CLI reference](https://rocky-data.dev/reference/cli/). Not covered here: [Rocky DSL](https://rocky-data.dev/concepts/rocky-dsl/), [Dagster](https://rocky-data.dev/dagster/introduction/), [Python SDK](https://rocky-data.dev/python-sdk/introduction/), [hooks](https://rocky-data.dev/concepts/hooks/), [snapshots (SCD Type 2)](https://rocky-data.dev/concepts/silver-layer/), [the container image](https://rocky-data.dev/guides/run-the-image/) and [architecture](https://rocky-data.dev/concepts/architecture/).
