---
title: Embedding Rocky
description: The four ways to drive Rocky from your own code, which one to pick, how to pin versions, and how to run the HTTP API as a sidecar.
sidebar:
  order: 10
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

This guide covers all four patterns, how to choose between them, and the operational details that matter when you build Rocky into a larger system.

## Choose an integration pattern

The four patterns differ in how your code calls Rocky and in what process lifecycle they assume.

| Pattern | How you call it | Best fit |
|---|---|---|
| **Subprocess** | Run `rocky <verb> --output json` and parse stdout | Shell scripts, CI steps, any language without a Rocky binding |
| **Python SDK** | `RockyClient` from the `rocky-sdk` package | Python applications, notebooks, and orchestrators (Dagster builds on this) |
| **MCP** | `rocky mcp` over stdio | AI agents that inspect, author, or verify models |
| **Serve API** | `rocky serve`, then HTTP `GET`/`POST /api/v1/...` | Long-lived services that need a persistent read surface and async job submission |

Reach for the **subprocess** pattern first. It needs nothing but the binary on `$PATH`. Move to the **SDK** when you write Python and want typed results. Use **MCP** when the caller is an LLM agent. Stand up the **serve API** when you need a process that stays warm across many requests. Use it too when you want to submit long-running work as a job and poll for it.

## Subprocess

Run a command with `--output json` and read the payload from stdout. The exit code tells you success or failure. The JSON carries the detail.

```bash
rocky plan --output json --filter client=acme > plan.json
```

Every command's JSON shape is a typed contract, not an ad-hoc dump. The field reference lives in [JSON output](/reference/json-output/). The rules that keep those shapes stable are in the [JSON contract](/advanced/json-contract/). Parse the payload, branch on the exit code, and you have a working integration in any language.

## Python SDK

The `rocky-sdk` package wraps the subprocess pattern in a typed client. `RockyClient` runs the binary for you and parses each payload into a Pydantic model. You get autocompletion and validation instead of raw dictionaries.

```python
from rocky_sdk import RockyClient

client = RockyClient(config_path="rocky.toml")
plan = client.plan(filter="client=acme")
print(plan.plan_id)          # content-addressed; pass to client.apply() to execute
result = client.apply(plan.plan_id)
```

A [plan](/reference/glossary/#plan) is a reviewable record of what a run will do, keyed by a `plan_id`. You build it, inspect it, then apply it.

Start with the [SDK introduction](/python-sdk/introduction/) for setup and the client surface. The [recipes](/python-sdk/recipes/) cover common tasks. The Dagster integration is a thin adapter over this same client, so anything you express with `RockyClient` maps onto assets and checks.

## MCP

`rocky mcp` runs a Model Context Protocol server over stdio. It hands an LLM agent Rocky's read and authoring tools: compile, plan preview, lineage, schema inspection, and the write-path tools that draft and apply models. The agent then works from real engine data instead of guessing.

See [authoring with MCP](/concepts/mcp-authoring/) for the tool surface, and [operating Rocky with agents](/concepts/operating-rocky-with-agents/) for the workflow an agent should follow. Use MCP when the caller reasons about your project in natural language. For deterministic automation, prefer the subprocess or SDK patterns.

## Serve API

`rocky serve` starts an HTTP server that holds a compiled graph in memory and answers requests under `/api/v1`. Use it for a long-lived service. It compiles once at startup, serves reads from the warm graph, and runs mutating work as a background job.

```bash
rocky --config rocky.toml serve --port 8080
```

The read routes return the same payloads as the matching CLI command, byte for byte. `GET /api/v1/models/{name}/lineage` returns exactly what `rocky lineage <name> --output json` prints. `GET /api/v1/compile` matches `rocky compile --output json`. A caller on the HTTP API and a caller on the SDK see identical data.

### Mutations are jobs you poll

A mutating route does not block. You submit the work, get an id back, and poll for the result:

```
  POST /api/v1/jobs/run ─────► 202 Accepted  { "job_id": ... }
       /api/v1/jobs/plan                │
       /api/v1/jobs/apply               │
        ┌───────────────────────────────┘
        ▼
  GET /api/v1/jobs/{id} ──► running ───► poll again
                        ├─► succeeded ─┐
                        └─► failed ────┴─► body embeds the canonical
                                           RunOutput or PlanOutput
```

The polled result is the same payload the CLI would have produced.

A submitted job becomes `running` right away. The schema also declares a `queued` state, but this server never uses it, because submissions never sit in a queue. Poll until the state is `succeeded` or `failed` rather than matching on the full set.

There is no cancel route and no timeout route today. A mutating job holds the single mutation lane until its subprocess exits. While that lane is held, the next `run` or `apply` submission returns `409`, so a hung job blocks the next one. Restarting the sidecar is the only way to clear it. On restart the server reconciles its durable job ledger: it marks any job the previous process left non-terminal as `failed`, with the error `interrupted by engine restart`. A client that polls for a terminal state therefore always terminates. A cancel route is planned.

### Every failure carries an error envelope

Every route except `GET /api/v1/health` returns a structured error body on failure, never an empty response. The body carries a stable `code`, a human `message`, and an optional `remediation_hint`. Switch on the code in your own code, and show the hint to an operator.

Router-level failures use the same envelope. An unknown path answers `404` with `code: "route_not_found"`. A known path called with the wrong method answers `405` with `code: "method_not_allowed"`. One case has no envelope: a request the HTTP stack rejects before it reaches the router, such as malformed HTTP framing or a connection dropped mid-body.

The full route reference, request and response schemas, and status codes are published as a generated OpenAPI 3.1 document: **[openapi.json](/openapi.json)**. It is generated from the same typed schemas that back the CLI, so it cannot drift from what the server returns. Load it into any OpenAPI tool to explore the surface or generate a client.

For the command flags, see [`rocky serve`](/reference/commands/development/). For where the server sits in the engine, see the [architecture overview](/concepts/architecture/).

## Pinning and upgrades

CI holds the shape of every `/api/v1` payload stable. The `codegen-drift` check fails any change that alters a payload's shape without regenerating the committed schemas, so a shape change cannot land silently. New fields arrive in a backward-compatible way: they are serde-defaulted and optional. **Your parser must tolerate unknown fields.** Adding a field, without removing or retyping any other, does not change the API version.

CI guarantees **shape**, not values. The drift check compares the structure of each payload. It does not compare the specific values inside a primitive field. A string field stays a string across a minor release, but the exact string it carries is best-effort. Pin against shape, and expect value semantics to change within a minor version. A frozen-value corpus that would trip on any value change is planned, and is not in force yet.

Read `GET /api/v1/meta` to identify the engine you are talking to at runtime. It reports the engine version, the state-schema version, and a hash of the full schema set. It also reports a per-request hash of the resolved config, and the routes and capabilities this build serves. Feature-detect against `capabilities` and `routes` rather than parsing the version string. Compare `schemas_hash` between deployments to see whether any payload shape moved.

Recommended practice:

- Pin the engine to a minor version and read `/api/v1/meta` on startup to confirm the deployed build matches what you tested against.
- Tolerate unknown fields in every parser.
- Watch `schemas_hash` across upgrades; a change there is your signal to re-review payloads.
- A backward-incompatible reshape (a removed field, a rename, a type change) would arrive as a new API version, not as a silent change to `/api/v1`.

## Running the API as a sidecar

Run `rocky serve` next to your application as a single-tenant sidecar. It is not a shared multi-tenant server.

**Binding.** The server binds to `127.0.0.1:8080` by default, loopback only. It refuses to bind a non-loopback host such as `0.0.0.0` unless you also configure a bearer token. An unauthenticated API therefore cannot leak model SQL and run history onto the network by accident.

**Authentication.** Auth is one optional shared-secret bearer token, passed with `--token` or the `ROCKY_SERVE_TOKEN` environment variable. When you set a token, every route except `GET /api/v1/health` requires it. This is a single secret, not a user system. Put the sidecar behind your own gateway if you need per-user identity, TLS, rate limiting, or a public perimeter. The server does not terminate TLS. The `X-Rocky-Principal` header is recorded for audit only, and never authorizes anything.

**Job principals and policy gating.** A job submitted over HTTP runs `rocky <verb>` as a subprocess. That subprocess inherits the *sidecar's* environment, and takes nothing from the request. The `X-Rocky-Principal` header does not set the principal the policy plane evaluates.

So if agent-scoped `[policy]` rules gate `run` and `apply` in your setup, set `ROCKY_PRINCIPAL=agent` in the sidecar's environment when you launch it. Without that variable, jobs run as the default `human` principal and the agent-scoped rules never fire, whatever the header says. Run one sidecar per principal class if you need both: one with `ROCKY_PRINCIPAL=agent` for agent traffic, and a separate one for human-driven automation.

**Multi-tenancy.** One `rocky serve` process serves one project configuration. For several tenants, run one sidecar per tenant with that tenant's config. Do not multiplex tenants through one server. One process per tenant keeps each tenant's state store, compiled graph, and credentials apart.

**One mutation at a time.** The server admits one mutating job (`run` or `apply`) per project at a time. A second mutating submission during that job returns `409` and puts the in-flight job's id in `running_job_id`. Your client can then poll that job instead of colliding with it. A `plan` job mutates nothing and is never blocked. Reads stay available throughout. A read that briefly races the state lock returns a retryable `503` rather than an error.

Treat the sidecar as part of your deployment: one process per project, bound to loopback or sitting behind your gateway, with the bearer token provisioned as a secret. The [OpenAPI document](/openapi.json) describes everything your application needs.
