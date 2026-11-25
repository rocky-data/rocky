---
title: Embedding Rocky
description: The four ways to drive Rocky from your own code, which one to pick, how to pin versions, and how to run the HTTP API as a sidecar.
sidebar:
  order: 21
---

Rocky is an engine you drive from your own software. Every command emits machine-readable JSON. Four transports carry the same typed payloads, so the transport you pick changes how your code reaches the data, not the data itself.

```
  caller                transport                          engine
  ─────────────────────────────────────────────────────────────────
  any language ───► spawn `rocky <verb> --output json` ──┐
  Python code  ───► RockyClient (rocky-sdk) ─────────────┤
  LLM agent    ───► `rocky mcp` over stdio ──────────────┼──► rocky
  your service ───► HTTP GET/POST /api/v1 ───────────────┘    engine
                    (served by `rocky serve`)
```

## Choose an integration pattern

| Pattern | How you call it | Best fit |
|---|---|---|
| **Subprocess** | Run `rocky <verb> --output json` and parse stdout | Shell scripts, CI steps, any language without a Rocky binding |
| **Python SDK** | `RockyClient` from the `rocky-sdk` package | Python applications, notebooks, and orchestrators (Dagster builds on this) |
| **MCP** | `rocky mcp` over stdio | AI agents that inspect, author, or verify models |
| **Serve API** | `rocky serve`, then HTTP `GET`/`POST /api/v1/...` | Long-lived services that need a persistent read surface and async job submission |

Start with **subprocess**: it needs only the binary on `$PATH`. Move to the **SDK** for typed Python results. Use **MCP** when the caller is an LLM agent. Use the **serve API** when you need a warm process, or want to submit long work as a job and poll for it.

## Subprocess

Run a command with `--output json` and read the payload from stdout. The exit code tells you success or failure. The JSON carries the detail.

```bash
rocky plan --output json --filter client=acme > plan.json
```

Every command's JSON shape is a typed contract. See [JSON output](/reference/json-output/) for the fields and the [JSON contract](/advanced/json-contract/) for the stability rules.

## Python SDK

`RockyClient` in the `rocky-sdk` package runs the binary for you and parses each payload into a Pydantic model.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")
plan = client.plan(filter="client=acme")
print(plan.plan_id)          # content-addressed; pass to client.apply() to execute
result = client.apply(plan.plan_id)
```

A [plan](/reference/glossary/#plan) is a reviewable record of what a run will do, keyed by a `plan_id`. You build it, inspect it, then apply it.

See the [SDK introduction](/python-sdk/introduction/) and the [recipes](/python-sdk/recipes/). The Dagster integration is a thin adapter over this client.

## MCP

`rocky mcp` runs a Model Context Protocol server over stdio. It gives an LLM agent Rocky's read and authoring tools: compile, plan preview, lineage, schema inspection, and the tools that draft and apply models. See [authoring with MCP](/concepts/mcp-authoring/) and [operating Rocky with agents](/concepts/operating-rocky-with-agents/). For deterministic automation, prefer subprocess or the SDK.

## Serve API

`rocky serve` starts an HTTP server under `/api/v1`. It compiles once at startup, serves reads from the warm graph, and runs mutating work as a background job.

```bash
rocky --config rocky.toml serve --port 8080
```

Read routes return the same payloads as the matching CLI command, byte for byte. A caller on the HTTP API and a caller on the SDK see identical data. The window, grouping and product are query parameters. The OpenAPI document lists their accepted values.

| Route | Matches | Differs by |
|---|---|---|
| `GET /api/v1/models/{name}/lineage` | `rocky lineage <name> --output json` | - |
| `GET /api/v1/compile` | `rocky compile --output json` | - |
| `GET /api/v1/products` | `rocky product list --output json` | - |
| `GET /api/v1/products/{name}` | `rocky product status <name> --output json` | - |
| `GET /api/v1/products/{name}/journal` | `rocky product journal <name> --output json` | - |
| `GET /api/v1/review/queue` | `rocky review --queue --output json` | `staleness_seconds` and `score` derive from the request instant |
| `GET /api/v1/review/{plan_id}/status` | `rocky review <plan-id> --status --output json` | - |
| `GET /api/v1/review/{plan_id}` | `rocky review <plan-id> --output json` | `approved` is always false. It never approves and never writes the sign-off marker. |
| `GET /api/v1/brief` | `rocky brief --output json` | `generated_at`, `since_timestamp`. It never advances the digest cursor, so `since` defaults to `7d`, not `last`. |
| `GET /api/v1/audit/scorecard` | `rocky audit --scorecard --output json` | `window_start` |
| `GET /api/v1/custody/{subject}` | `rocky audit --for <subject> --output json` | - |
| `GET /api/v1/audit` | `rocky audit --output json` (`?product=<name>` matches `--product <name>`) | - |
| `GET /api/v1/cost/month?month=<YYYY-MM>` | `rocky cost --month <YYYY-MM> --by product --output json` | `as_of`. With no `month`, it reports the current month in `[schedule].timezone`. |
| `GET /api/v1/policy` | `rocky policy show --output json` | See below |

To approve a plan, use `POST /api/v1/jobs/approve` or `rocky review <plan-id> --approve`.

`GET /api/v1/policy` returns the rules in file order with the fields that decide an outcome (scope, effect, `verify_after`, not `conditions`, which the engine parses and never evaluates), and the freezes in force that the read could see. It never writes. On a remote `[state]` backend it does not download the authoritative ledger, because that would replace the local file. It then reports `freeze_sources.ledger` as `local_mirror`, so a caller can tell a complete answer from a possibly stale one.

Six routes have no CLI command to match: `GET /api/v1/health`, `/project`, `/models`, `/models/{name}`, `/dag/layers` and `/dag/status`. They return typed payloads with published schemas. `GET /api/v1/meta` lists the `estate` capability for the five estate routes and `project` for the project route.

- `/project` returns the config directory's name, the pipelines and adapters, the compiled model count, the diagnostics counts and the newest run.
- Model detail caps the SQL text at 256 KiB. A cut is reported through `sql_truncated` and `sql_bytes`.
- Each type-checked column carries its structured `data_type`, which keeps a struct field's own nullability, and a `data_type_display` label such as `DECIMAL(10,2)`.

### Two reads that compute, and what they cost

Most read routes project state the engine already has. Two do work per request, so they carry bounds.

`GET /api/v1/review/{plan_id}` compiles the project twice, at the working tree and at `HEAD`, to find what a plan would break. On a project of a thousand models each compile takes about 140 milliseconds. One diff runs at a time. A second waits about two seconds, then gets `503` with a `Retry-After`. The base is always `HEAD` and is not a parameter, because a ref from a query string would become an argument to `git`.

`GET /api/v1/models/{name}/rows` runs the model's compiled `SELECT` against the warehouse and returns at most `limit` rows, with classification-tagged columns masked. It runs the same code as `rocky preview rows`.

```bash
curl -H "Authorization: Bearer $TOKEN" \
     -H "X-Rocky-Allow-Warehouse: true" \
     "http://127.0.0.1:8080/api/v1/models/customer_orders/rows?limit=50"
```

Four things bound it:

- **Consent, per request.** A remote adapter needs the `X-Rocky-Allow-Warehouse: true` header on every call. Without it the answer is `403 warehouse_gated`, before anything compiles. A local DuckDB adapter needs none. A `GET` can come from a browser prefetch or a restored tab, and those cannot set a custom header.
- **A row cap.** `limit` is 1 to 500. Anything else is `400`.
- **A timeout, and one at a time.** The call gets 30 seconds, the compile included. After that the answer is `504 sample_timeout`, and the warehouse may still be running the query. A second concurrent sample gets `503` with a `Retry-After`. A compile that outlives the deadline keeps the sample lane until it finishes.
- **No caching.** The response carries `Cache-Control: no-store`.

A sample that cannot be masked is refused. Rocky can express a mask on Databricks, Snowflake and DuckDB. On any other adapter, a model with a masked column gets `422 masking_unsupported_by_adapter`. The `hash` strategy is an unsalted SHA-256, so a hashed email address or phone number is recoverable from its digest. Treat a hashed column as pseudonymous, not anonymous.

Ad-hoc SQL is not exposed over HTTP. The CLI's `--sql-file` has no route.

### Mutations are jobs you poll

A mutating route does not block. You submit the work, get an id back, and poll for the result:

```
  POST /api/v1/jobs/run ─────► 202 Accepted  { "job_id": ... }
       /api/v1/jobs/plan                │
       /api/v1/jobs/apply               │
       /api/v1/jobs/approve             │
       /api/v1/jobs/test                │
       /api/v1/jobs/ci                  │
       /api/v1/jobs/freeze              │
       /api/v1/jobs/unfreeze            │
       /api/v1/jobs/preview             │
        ┌───────────────────────────────┘
        ▼
  GET /api/v1/jobs/{id} ──► running ───► poll again
                        ├─► succeeded ─┐
                        ├─► failed ────┤
                        └─► cancelled ─┴─► body embeds the canonical
                                           RunOutput or PlanOutput
```

The polled result is the same payload the CLI would have produced. A submitted job becomes `running` right away. The schema also declares `queued`, but this server never uses it. Poll until the state is `succeeded`, `failed` or `cancelled`.

The routes `/jobs/promote` and `/jobs/compact` are not in the diagram. They plan and mutate nothing. Every route validates its body first and answers `400 bad_request` before the job starts, or before it takes the lock. All sit behind the same write gate as `run`: a read-only token gets `403`.

| Route | Runs | Body | Lock |
|---|---|---|---|
| `/jobs/test` | `rocky test` in local DuckDB | optional `{"model": "<name>"}` | none |
| `/jobs/ci` | `rocky ci` (compile and test, no warehouse credentials) | ignored | none |
| `/jobs/freeze`, `/jobs/unfreeze` | `rocky policy freeze`, `rocky policy unfreeze` | optional `{"principal": "agent" \| "human", "scope": "<selector>", "reason": "<text>"}` | mutation |
| `/jobs/preview` | `rocky preview create` | optional `{"name": "<branch>", "base": "<ref>"}` | mutation |
| `/jobs/promote` | `rocky plan promote <name>` | `{"name": "<branch>", "base": "<ref>", "pipeline": "<name>"}`, `name` required | none |
| `/jobs/compact` | `rocky compact <table>` or `rocky compact --catalog <name>` | exactly one of `{"table": "<catalog.schema.table>"}` or `{"catalog": "<name>"}` | none |
| `/jobs/product_approve` | `rocky product approve <name>` | `{"name": "<product>", "expect_spec_digest": "<digest>"}`, both required | mutation |
| `/jobs/approve` | `rocky review <plan_id> --approve` | `{"plan_id": "<64 lowercase hex>"}` | mutation |
| `/jobs/apply` | `rocky apply <plan_id>` | `plan_id`, optional `expect_spec_digest` | mutation |

Jobs that take the mutation lock answer `409 mutation_in_progress` while another one holds it. A run holds the local state store for its whole run, even on a remote `[state]` backend, so retry a refused freeze when that run ends. To stop a running job, cancel it.

**Freeze and unfreeze.** With no `principal`, both are named. With no `scope`, every model is. The server answers `400 bad_request` for a principal that is not `agent` or `human`, a scope `rocky policy` does not recognise (or one padded with spaces), or a reason that is empty, starts with `-`, holds control characters or is over 500 characters. To lift a freeze, send the `principal` and `scope` it was made with. A freeze marker whose body cannot be read is lifted only with no `principal` and scope `any` (the brief marks its rows `unreadable_marker`). The job result carries the command's `notes`, such as "freeze recorded but NOT enforced" when the project has no `[policy]` block. A run this server did not start also holds the local store but not the server's lock. A freeze then fails on the store lock. The [browser UI guide](/guides/browser-ui/#freeze-and-unfreeze) covers the remote-backend case.

**Preview.** `name` is 1 to 64 letters, digits or underscores. With none, the server derives it from the current branch of its git checkout, so only the first call from a checkout succeeds. `base` is a ref (default `main`) with no spaces or control characters, at most 200 characters, not starting with `-`. The job registers the branch in the local state store file (not uploaded to a remote `[state]` backend) and copies the unchanged models from their production schema into the warehouse schema `branch__<name>`, using the warehouse credentials in `rocky.toml`. Production tables are only read. It does not re-run the changed models (`run_status` is `planned`). A copy that fails for one model is `copy_strategy: "failed"` in the result and the job still succeeds, so check each `copy_set` entry. A name already in use fails the job. A job that fails after it registered the branch leaves the branch, so a retry needs a new name. The changed models come from `git diff` between `base` and `HEAD`, resolved against the repository root. The child gets the server's `--models` when one was given. It reads no contracts.

**Promote.** `name` is 1 to 64 letters, digits, `_`, `.` or `-`, not starting with `-`. The optional `base` is a git ref (default `main`). The job runs the branch's approval and breaking-change gates and writes a promote plan. It replaces no table. The result is the plan output: `plan_id` and one `statements` entry per production table the plan would replace (`target` is the table). The server answers `409 branch_approval_not_required` unless `rocky.toml` sets `[branch.approval] required = true`. It reads the config on each request and answers `500 project_read_failed` when it cannot. The child runs without `ROCKY_BRANCH_APPROVAL_SKIP`. Apply the plan with `POST /api/v1/jobs/apply`. `GET /api/v1/branches` returns the same bytes as `rocky branch list --output json`, and `GET /api/v1/project` carries `branch_approval_required`.

**Compact.** A table has three parts and a catalog has one. Each part is letters, digits or `_`. The job writes a compaction plan and runs no statement. For a replication pipeline, a `catalog` plan asks the source adapter to discover tables. The result is the `compact` output: `plan_id` (absent when the plan could not be saved), `statements`, and for a catalog also `tables` and `totals`. Apply it with `POST /api/v1/jobs/apply` and that `plan_id`. The apply runs `OPTIMIZE` and `VACUUM` on Databricks and takes the lock. There is no `dry_run` field, because the command's `--dry-run` still writes an applicable plan.

**Apply.** `/jobs/apply` reads the plan before it takes the lock, then answers:

| Status | When |
|---|---|
| `409 archive_apply_terminal_only` | Archive plan. Archive deletes rows and vacuums with no retention window, so apply it in a terminal. |
| `409 branch_approval_not_required` | Promote plan, and the config does not set `required = true`. |
| `409 promote_plan_not_approved` | Promote plan with no valid approval, fewer than `min_approvers`, or built with the gate skipped. |

A plan it cannot read goes on to the child, which fails on it. `/jobs/apply` and `/jobs/approve` answer `400 invalid_plan_id` for any other `plan_id`.

**Spec digests.** A plan bound to a data product needs `expect_spec_digest`, passed as `rocky apply --expect-spec-digest <digest>`. A plan that is not bound is refused with it. The engine's refusals are plain errors, not coded ones. The digest must come from the caller's own record of the approved spec (`approval.spec_digest` in `rocky product status`, the `approval:` line), never from the plan, whose digest is not a secret. The server answers `400 bad_request` before it takes the lock if the value is empty, holds a space or a control character, starts with `-`, or is over 200 characters. `/jobs/product_approve` takes the same digest as `expect_spec_digest` and passes it as `--expect-digest`. The `name` there is an ASCII letter or `_`, then letters, digits or `_`. The job approves the working spec only if its digest is exactly that value. Otherwise it fails with `approval-digest-mismatch` and writes nothing.

**Approver identity.** An approval records the approver as `<server git email> (http_api)`: the channel, not the person. If `git config user.email` cannot be resolved, or is literally `unknown`, the job fails with `approver_identity_unresolved` and writes nothing. Job subprocesses carry `ROCKY_SESSION_SOURCE=http_api`, so run records show the session source `http_api`. That does not prove a browser made the call. The ledger row names the server's principal id, read from `ROCKY_PRINCIPAL_ID` in the server's environment (else `unnamed`; a `--principal-id` flag on `rocky serve` does not reach the job), not the caller.

`POST /api/v1/jobs/{id}/cancel` stops a job this server process started. It takes no body. The first request sends the job's process group `SIGTERM` and answers `202 {"job_id": ..., "signal": "terminate"}`. A run of a replication pipeline handles it like Ctrl-C: it finishes the copies in flight, saves their state and exits. Other jobs stop at once. Once a `SIGTERM` was delivered, the next request sends `SIGKILL` and answers `"signal": "kill"`. On Windows every request kills the job. The route answers `202` only after the signal reached the job. The job then ends `cancelled`, or `succeeded` if it still exits 0. It keeps the mutation lane until its subprocess has exited. Errors:

| Status | When |
|---|---|
| `409 job_not_running` | The job finished, or exited before the signal reached it. |
| `409 job_not_cancellable` | This process did not start the job, such as a scheduled run. |
| `404 job_not_found` | Unknown id. |

The [browser UI guide](/guides/browser-ui/#cancel-a-running-job) lists what a cancelled job leaves behind.

There is no timeout route today. A mutating job holds the single mutation lane until its subprocess exits, so a hung job blocks the next `run` or `apply` (`409`) until it is cancelled. On restart the server marks any job the previous process left `running` or `queued` as `failed`, with the error `interrupted by engine restart`. A state this version does not recognise is reported as `failed`, so polling always ends.

### Every failure carries an error envelope

Every route except `GET /api/v1/health` returns a structured error body on failure. The body carries a stable `code`, a human `message`, and an optional `remediation_hint`. Switch on the code. An unknown path answers `404` with `code: "route_not_found"`. A wrong method answers `405` with `code: "method_not_allowed"`. A request the HTTP stack rejects before the router, such as malformed framing, has no envelope.

The generated OpenAPI 3.1 document, **[openapi.json](/openapi.json)**, lists every route, schema and status code. It comes from the same typed schemas as the CLI, so it cannot drift.

### The browser UI

`rocky serve --ui` serves a browser UI at `/ui/`. The page signs in through `GET /login` and a session cookie. Embedders keep using `Authorization: Bearer <token>`. The UI files are public. The [browser UI guide](/guides/browser-ui/) covers sign-in, [operator mode](/guides/browser-ui/#operator-mode) and the host and origin checks.

For the command flags, see [`rocky serve`](/reference/commands/development/). For where the server sits in the engine, see the [architecture overview](/concepts/architecture/).

## Pinning and upgrades

The `codegen-drift` check fails any change to a payload's shape that does not regenerate the committed schemas. New fields are serde-defaulted and optional. **Your parser must tolerate unknown fields.** Adding a field does not change the API version. A removed field, a rename or a type change would arrive as a new API version.

CI guarantees **shape**, not values. A string field stays a string across a minor release, but the exact string is best-effort. A frozen-value corpus is planned and not in force yet.

`GET /api/v1/meta` reports the engine version, the state-schema version, a hash of the full schema set (`schemas_hash`), `token_scope` (`"full"`, `"read_only"`, or `null` when no token is configured), a per-request hash of the resolved config, and the routes and capabilities this build serves. Feature-detect against `capabilities` and `routes`, not the version string.

- Pin the engine to a minor version. Read `/api/v1/meta` on startup to confirm the build matches what you tested.
- Watch `schemas_hash` across upgrades. A change is your signal to re-review payloads.

## Running the API as a sidecar

Run `rocky serve` next to your application as a single-tenant sidecar. It is not a shared multi-tenant server.

**Binding.** The server binds to `127.0.0.1:8080` by default, loopback only. It refuses to bind a non-loopback host such as `0.0.0.0` unless you also configure a bearer token. An unauthenticated API therefore cannot leak model SQL and run history onto the network by accident.

**Authentication.** Auth is one optional shared-secret bearer token, passed with `--token` or the `ROCKY_SERVE_TOKEN` environment variable. When you set a token, the check runs on every request whose path is not exempt. Two paths are exempt, whatever the method: `/api/v1/health`, and the webhook `/api/v1/hooks/trigger/{pipeline}` — bearer-exempt because it authenticates with its own HMAC over the raw body (and, on a loopback bind with `--scheduler` and no secret, accepts an unsigned `POST`). A CORS preflight is answered before the check, and the browser UI's own files under `/ui/` are public. When you set no token, a loopback server asks no request for one. It still refuses a request whose `Host` does not name it (`421`) or whose `Origin` is another site (`403`), so a web page cannot reach it by DNS rebinding; a client that calls `http://127.0.0.1:<port>` or `http://localhost:<port>` is unaffected. Behind a reverse proxy that sends its own name as `Host`, pass that name with `--allowed-host`. This is a single secret, not a user system.

**Token scope.** A token reaches every route by default. `--token-scope read-only` (or `--read-only`, or `ROCKY_SERVE_TOKEN_SCOPE`) narrows it to `GET`, `HEAD`, and `OPTIONS`; every other method gets `403 forbidden_read_only_token`. Hand that token to anything you do not fully trust with a warehouse write — a browser UI above all, where a leaked token would otherwise reach `POST /api/v1/jobs/run`. The rule is the HTTP method, not a list of paths, so routes added in future are covered as well.

The scope restricts the token. It is not a whole-server perimeter, and one gap is worth knowing before a browser reaches this sidecar: the webhook route authenticates itself and ignores the token. Run `--scheduler` on loopback without `ROCKY_WEBHOOK_SECRET` and it accepts an unsigned `POST`, then queues work for the scheduler. Script running in the page can do that with no token at all. Set `ROCKY_WEBHOOK_SECRET`. Put the sidecar behind your own gateway if you need per-user identity, TLS, rate limiting, or a public perimeter. The server does not terminate TLS. The `X-Rocky-Principal` header is recorded for audit only, and never authorizes anything.

**Job principals and policy gating.** A job submitted over HTTP runs `rocky <verb>` as a subprocess. That subprocess inherits the *sidecar's* environment, and takes nothing from the request. The `X-Rocky-Principal` header does not set the principal the policy plane evaluates.

So if agent-scoped `[policy]` rules gate `run` and `apply` in your setup, set `ROCKY_PRINCIPAL=agent` in the sidecar's environment when you launch it. Without that variable, jobs run as the default `human` principal and the agent-scoped rules never fire, whatever the header says. Run one sidecar per principal class if you need both: one with `ROCKY_PRINCIPAL=agent` for agent traffic, and a separate one for human-driven automation.

**Multi-tenancy.** One `rocky serve` process serves one project configuration. For several tenants, run one sidecar per tenant with that tenant's config. Do not multiplex tenants through one server. One process per tenant keeps each tenant's state store, compiled graph, and credentials apart.

**One mutation at a time.** The server admits one mutating job (`run`, `apply` or `approve`) per project at a time. A second mutating submission during that job returns `409` and puts the in-flight job's id in `running_job_id`. Your client can then poll that job instead of colliding with it. A `plan` job mutates nothing and is never blocked. Reads stay available throughout. A read that finds the state store held by another process, a `rocky run` in a terminal for example, returns a retryable `503 engine_busy` rather than an error.

**Reads queue, one at a time.** Every state-backed read (`/runs`, `/schedule`, `/audit`, `/brief`, `/products`, `/review/queue`, and the rest) opens the state store for the request, and the store's file lock allows one open at a time. The server queues its own reads on one permit, so two clients refreshing together wait for each other instead of racing for the lock. Routes served from the compile result (`/models`, `/dag/layers`, `/dag`) never take the queue. The queue sets the ceiling of one process.

**A store a read cannot serve.** A read takes no write transaction against a store that holds anything, and never creates a table in one. A store written by a Rocky older than schema v22 lacks tables this server reads. `rocky serve` migrates such a store at startup — its job sweep opens the store read-write before the router serves — so after an upgrade the first read already answers. What can still happen is a store that changes under a running server (a file copied or restored over it), or a startup sweep that could not open the store; then every state-backed read answers `409 state_needs_migration`, naming the missing tables and the stamp on disk, until one read-write open migrates it: restart `rocky serve`, or run any read-write command such as `rocky run`, once. It is not the retryable `503`: retrying never migrates a store. Tell an operator. A project that has never run is not this case: its store is created empty on first read and answers "nothing yet".

### What one process carries

Measured with `scripts/serve-ceiling.py` on 2026-09-05, against the playground project, on an Apple M3 Pro: a staircase of concurrent clients, each looping over the reads the browser UI makes, 20 seconds per step.

| Concurrent clients | Requests per second | p99 latency |
|---|---|---|
| 1 | 90 | 20 ms |
| 2 | 95 | 45 ms |
| 4 | 97 | 69 ms |
| 8 | 96 | 140 ms |
| 16 | 97 | 274 ms |

Zero errors at every step, and the process's memory did not move. Throughput saturates near 95 store-backed reads per second on that machine, about 10 ms per read; latency then grows with the queue, about 15 ms per waiting request. A viewer costs its refresh rate times the reads per refresh: at one read per second per viewer, that machine carries about 90 viewers, and 8 viewers refreshing at the same instant wait 140 ms. The number is machine-specific by construction. Reproduce it on yours:

```bash
rocky serve --port 8420 --token T &
python3 scripts/serve-ceiling.py --url http://127.0.0.1:8420 --token T --pid $!
```

The script names any route the server cannot answer, and refuses to report a ceiling its own client was the limit of.

Treat the sidecar as part of your deployment: one process per project, bound to loopback or sitting behind your gateway, with the bearer token provisioned as a secret. The [OpenAPI document](/openapi.json) describes everything your application needs.
