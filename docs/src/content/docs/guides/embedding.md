---
title: Embedding Rocky
description: The four ways to drive Rocky from your own tool (subprocess, Python SDK, MCP, and the serve HTTP API), with guidance on which to pick, how to pin versions, and how to run the API as a sidecar
sidebar:
  order: 10
---

Rocky is an engine you drive from your own software. Every command emits machine-readable JSON, the state ledger is an open format, and the same typed payloads are available over four different transports. This guide covers those four integration patterns, how to choose between them, and the operational details that matter when you build Rocky into a larger system.

## Choose an integration pattern

All four patterns expose the same underlying data. They differ in how your code reaches it and what lifecycle they assume.

| Pattern | How you call it | Best fit |
|---|---|---|
| **Subprocess** | Run `rocky <verb> --output json` and parse stdout | Shell scripts, CI steps, any language without a Rocky binding |
| **Python SDK** | `RockyClient` from the `rocky-sdk` package | Python applications, notebooks, and orchestrators (Dagster builds on this) |
| **MCP** | `rocky mcp` over stdio | AI agents that inspect, author, or verify models |
| **Serve API** | `rocky serve`, then HTTP `GET`/`POST /api/v1/...` | Long-lived services that need a persistent read surface and async job submission |

A rule of thumb: reach for the **subprocess** pattern first because it has no dependencies beyond the binary on `$PATH`. Move to the **SDK** when you are in Python and want typed results. Use **MCP** when the consumer is an LLM agent. Stand up the **serve API** only when you need a process that stays warm across many requests or when you want to submit long-running work as a job and poll for it.

## Subprocess

Invoke a command with `--output json` and read the payload from stdout. The exit code tells you success or failure; the JSON carries the detail.

```bash
rocky plan --output json --filter client=acme > plan.json
```

Every command's JSON shape is a typed contract, not an ad-hoc dump. The full field reference lives in [JSON output](/reference/json-output/), and the stability rules that govern it are in the [JSON contract](/advanced/json-contract/). Parse the payload, branch on the exit code, and you have a working integration in any language.

## Python SDK

The `rocky-sdk` package wraps the subprocess pattern in a typed client. `RockyClient` runs the binary for you and parses each payload into a Pydantic model, so you get autocompletion and validation instead of raw dictionaries.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")
plan = client.plan(filter="client=acme")
print(plan.plan_id)          # content-addressed; pass to client.apply() to execute
result = client.apply(plan.plan_id)
```

Start with the [SDK introduction](/python-sdk/introduction/) for setup and the client surface, and see the [recipes](/python-sdk/recipes/) for common task patterns. The Dagster integration is a thin adapter over this same client, so anything you can express with `RockyClient` maps cleanly into assets and checks.

## MCP

`rocky mcp` runs a Model Context Protocol server over stdio. It exposes Rocky's read and authoring tools to an LLM agent: compile, plan preview, lineage, schema inspection, and the write-path tools that draft and apply models. An agent connected over MCP works against grounded engine data rather than guessing.

See [authoring with MCP](/concepts/mcp-authoring/) for the tool surface and [operating Rocky with agents](/concepts/operating-rocky-with-agents/) for the workflow an agent should follow. MCP is the right transport when the consumer reasons about your project in natural language; for deterministic automation, prefer the subprocess or SDK patterns.

## Serve API

`rocky serve` starts an HTTP server that holds a compiled graph in memory and answers requests under `/api/v1`. This is the pattern for a long-lived service: the compile happens once at startup, reads are served warm, and mutating work is submitted as a background job.

```bash
rocky serve --config rocky.toml --port 8080
```

The canonical read routes return the same payloads as the matching CLI command, byte for byte. `GET /api/v1/models/{name}/lineage` returns exactly what `rocky lineage <name> --output json` prints, and `GET /api/v1/compile` matches `rocky compile --output json`. A consumer on the HTTP API and a consumer on the SDK see identical data.

Mutations use a job model rather than a blocking call. You `POST /api/v1/jobs/run` (or `plan`, or `apply`), receive `202 Accepted` with a `job_id`, and poll `GET /api/v1/jobs/{id}` until the job reaches a terminal state. The terminal response embeds the canonical `RunOutput` or `PlanOutput`, so the polled result is the same payload the CLI would have produced.

A submitted job persists as `running` right away. The `queued` job state is declared in the schema but not used by the current server — submissions never sit in a queue, so you do not need to special-case `queued` today. Poll until the state is one of the terminal values (`succeeded`, `failed`) rather than matching on the full set.

There is no cancel or timeout route today. Once a mutating job starts it holds the single mutation lane until its subprocess exits; a hung job blocks the next `run`/`apply` submission (a second one returns `409` while the lane is held). The only way to clear a stuck mutating job is to restart the sidecar. On restart the server reconciles the durable job ledger: any job left non-terminal by the previous process is marked `failed` with the error `interrupted by engine restart`, so a client polling until a terminal state always terminates rather than polling a dead job forever. A cancel route is planned; until it lands, treat sidecar restart as the escape hatch.

Every route except `GET /api/v1/health` returns a structured error body on failure, never an empty response. The body carries a stable `code`, a human `message`, and an optional `remediation_hint`, so your code can switch on the machine-readable code and surface the hint to an operator. This includes the router-level failures: an unknown path answers a `404` with `code: "route_not_found"`, and a known path called with the wrong method answers a `405` with `code: "method_not_allowed"`, both enveloped. The one exception is a request the HTTP stack rejects before it reaches the router (malformed HTTP framing, or a connection dropped mid-body); that is answered below the application layer and carries no envelope.

The complete route reference, request and response schemas, and status codes are published as a generated OpenAPI 3.1 document: **[openapi.json](/openapi.json)**. It is generated from the same typed schemas that back the CLI, so it never drifts from what the server actually returns. Load it into any OpenAPI tool to explore the surface or generate a client.

For the command flags, see [`rocky serve`](/reference/commands/development/), and for where the server sits in the engine, see the [architecture overview](/concepts/architecture/).

## Pinning and upgrades

The `/api/v1` surface returns canonical typed payloads, and those shapes are held stable by CI. The `codegen-drift` check fails any change that alters a payload's shape without regenerating the committed schemas, so a shape change can never land silently. New fields are added in a backward-compatible way: they are serde-defaulted and optional, so **your parser must tolerate unknown fields**. A field that is only added, never removed or retyped, does not change the API version.

What CI guarantees today is **shape** stability, not value stability. The drift check compares the structure of each payload, not the specific values inside shape-invisible primitives. A string field stays a string across a minor release, but the exact string it carries is best-effort, not frozen. Pin against shape, and treat value semantics as subject to change within a minor version. A stronger guarantee, a frozen-value corpus that would trip on any value change, is a planned addition and is not in force yet. This section states what is true today rather than what is aspirational.

To detect the running engine and its contract fingerprint at runtime, read `GET /api/v1/meta`. It reports the engine version, the state-schema version, a hash of the full schema set, a per-request hash of the resolved config, and the list of routes and capabilities this build serves. Feature-detect against `capabilities` and `routes` rather than sniffing the version string, and compare `schemas_hash` between deployments to know whether any payload shape moved.

Recommended practice:

- Pin the engine to a minor version and read `/api/v1/meta` on startup to confirm the deployed build matches what you tested against.
- Tolerate unknown fields in every parser.
- Watch `schemas_hash` across upgrades; a change there is your signal to re-review payloads.
- A backward-incompatible reshape (a removed field, a rename, a type change) would arrive as a new API version, not as a silent change to `/api/v1`.

## Running the API as a sidecar

`rocky serve` is designed to run next to your application as a single-tenant sidecar, not as a shared multi-tenant server.

**Binding.** The server binds to `127.0.0.1:8080` by default, loopback only. It refuses to bind a non-loopback host (such as `0.0.0.0`) unless you also configure a bearer token, so the class of bug where an unauthenticated API leaks model SQL and run history onto the network cannot happen by accident.

**Authentication.** Auth is an optional shared-secret bearer token, passed with `--token` or the `ROCKY_SERVE_TOKEN` environment variable. When a token is set, every route except `GET /api/v1/health` requires it. This is a single secret, not a user system. If you need per-user identity, TLS, rate limiting, or a public perimeter, put the sidecar behind your own gateway. The server does not terminate TLS and does not try to be an edge service. The `X-Rocky-Principal` header is recorded for audit only and is never an authorization input.

**Job principals and policy gating.** A job submitted over HTTP runs `rocky <verb>` as a subprocess that inherits the *sidecar's* environment, not anything from the request. The per-request `X-Rocky-Principal` header is audit metadata only; it does not set the principal the policy plane evaluates. So if you embed an agent-driven flow and rely on agent-scoped `[policy]` rules to gate `run`/`apply` over HTTP, set `ROCKY_PRINCIPAL=agent` in the sidecar's environment when you launch it. Without that, jobs execute as the default `human` principal and the agent-scoped rules never fire, regardless of what the header says. Run one sidecar per principal class if you need both: an `ROCKY_PRINCIPAL=agent` sidecar for agent traffic and a separate one for human-driven automation.

**Multi-tenancy.** A single `rocky serve` process serves one project configuration. For multiple tenants, run one sidecar per tenant with that tenant's config, rather than trying to multiplex tenants through one server. This keeps each tenant's state store, compiled graph, and credentials isolated by process.

**One mutation at a time.** The server admits a single mutating job (`run` or `apply`) at a time per project. A second mutating submission while one is in flight returns `409` with the in-flight job's id in `running_job_id`, so your client can poll the running job instead of colliding with it. Non-mutating `plan` jobs are never blocked by this permit. Reads stay available throughout, and a read that briefly races the state lock returns a retryable `503` rather than an error.

Treat the sidecar as part of your deployment: one process per project, bound to loopback or behind your gateway, with the bearer token provisioned as a secret. From your application's side, everything you need is described in the [OpenAPI document](/openapi.json).
