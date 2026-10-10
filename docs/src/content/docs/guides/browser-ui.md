---
title: The Browser UI
description: "What rocky serve --ui shows: eleven areas in a sidebar, five of which open a screen today. How to open it, operator mode, and what it can and cannot do."
sidebar:
  order: 5.8
---

`rocky serve --ui` serves a browser UI for one project. It shows the project's models and runs, the plans waiting for a human, and the policy decisions and product history the engine recorded. On your own machine, the UI can also make changes (see [Operator mode](#operator-mode)). Every value on the page comes from a typed `/api/v1` payload. Most of those payloads are the same ones the CLI prints with `--output json`.

![A tour of the Rocky browser UI: the estate with its DAG, the review queue, an agent's breaking change awaiting a human, the governor brief, a model's custody chain, and a data product's journal](/demo-ui-tour.gif)

The UI ships inside the release binaries and the container image. A sidebar lists eleven areas. Five open a screen today:

```
  Needs you          what waits for you now
  Projects           -
  Estate             models, the DAG, runs and schedules
  Runs               -
  Scheduler          -
  Review             plans that need a human, one plan in full
  Policies           -
  Products           each data product, and its journal
  Governance         scorecard, custody, audit
  Agents & Clusters  -
  Settings           -
```

The other six are not links. Each shows, under its name, what is true instead. They are listed in [Areas without a screen](#areas-without-a-screen). On a screen narrower than 1024 pixels the sidebar folds behind an **Areas** button.

## Open the UI

On your own machine, start the server from the project directory:

```bash
rocky serve --ui
```

The server prints one address:

```
Rocky UI: http://127.0.0.1:8080/login?t=<secret>
```

Open that address, or add `--open` to open it in your default browser. The link signs you in. See [Sign-in and the session cookie](#sign-in-and-the-session-cookie).

With no token configured, the server generates a per-process token. It works for every request until the server stops. The server prints a note on stderr when it generates one. Each start makes a new token, so an old address stops working after a restart.

On a loopback host, the generated token has full scope. This is [operator mode](#operator-mode). For a view-only UI, add `--read-only`:

```bash
rocky serve --ui --read-only
```

The address is also printed on stdout, so it lands in anything that captures stdout (a terminal log, `docker logs`, a service journal) and in browser history. On a shared host, an ssh port-forward, a devcontainer or Codespace, or behind a proxy, choose the token yourself and use `--read-only`.

To choose the token, or to keep the same token across restarts:

```bash
rocky serve --ui --token "$(openssl rand -hex 16)" --read-only
```

The server generates a token only on a loopback host (`127.0.0.1`, `::1` or `localhost`). On any other host, `--ui` refuses to start without `--token` and `--read-only` (or `--token-scope read-only`).

### Sign-in and the session cookie

The token travels in the query of one request, `GET /login?t=<token>`. The server checks it in constant time. If it matches, the server answers `303` to `/ui/` and sets a session cookie, `rocky_ui_<tag>` (the tag differs per server). The cookie is `HttpOnly` and `SameSite=Strict`. It usually ends when the browser closes (browsers that restore sessions can keep it). The server adds `Secure` when a TLS proxy sends `X-Forwarded-Proto: https`.

The cookie holds a keyed hash of the token, not the token. The key is new for each server process, so a restart ends every browser session. The page never holds the token. Its API calls carry the cookie.

The server logs no request URIs. The `/login` answers send `Cache-Control: no-store` and `Referrer-Policy: no-referrer`. The redirect takes the token out of the address bar. The server never echoes the token.

A reverse proxy in front of the server may log the query string of `/login`. Configure it not to log that query, or use a token you rotate.

A cookie session can also write, but only from the page. A write must carry an `Origin` header that names this server exactly (the same host and port), or an `--allowed-origin` entry, and the header `X-Rocky-UI: 1`. Another page on the same machine shares the cookie, because cookies do not separate ports, so it is refused. Otherwise the answer is `403 ui_write_not_from_ui`. A read-only token stays read-only in a cookie session. Scripts and embedders keep using `Authorization: Bearer <token>`.

If the link is stale or wrong, `/login` answers `401` with a page that says so. It has a field for the token. Without a session, `/ui/` shows **session expired**. Open the newest `Rocky UI:` link from the console.

To run the UI somewhere other than your own machine, use the [container image](/guides/run-the-image/) or the [Helm chart](/guides/kubernetes/). The chart serves the UI by default.

## Needs you

The brief is the estate digest that `rocky brief` prints, for a window you pick (7 days by default). A summary line comes first: how many decisions wait on you, how the runs went, and whether a freeze or a degraded rule is in force. Then there is a card for each part of the digest.

The first card, which gives the area its name, is what needs you. Each pending plan is its own row, with a **Review plan** button that opens it in Review. The rest are the agents' policy decisions, runs, autonomy (degraded rules and active freezes), cost, drift, freshness, quality and the scheduler. A bar over the policy decisions shows how many were allowed, needed review, or were denied.

![The brief: one pending plan under Needs you, ten agent decisions with their capability, effect and rule, two successful runs, no degraded autonomy, and a cost card](/ui-governor-brief.png)

A card says so when its data was not available. A signal the ledger does not hold shows as **not recorded**, never as a zero. In the summary line, a part with nothing in the window says so in its own words, such as "no runs in the window". A part the engine could not read says **not recorded**, never zero.

## Estate

The printed address opens Estate, even though Needs you is first in the sidebar. It shows the project as the engine compiled it. The strip at the top names the config file, the pipelines and adapters, the compiled models with their diagnostics, and the newest run.

![The estate screen for the playground project: a project strip with one transformation pipeline, one DuckDB adapter and three compiled models, the newest run, Plan, Run and Refresh buttons, and a DAG of raw_orders, customer_orders and revenue_summary](/ui-estate.png)

Below the project strip, the DAG draws every model and the edges between them. Click a model to open its detail: its columns (with their types, where the compiler inferred them) and its compiled SQL. The server caps the SQL at 256 KiB and says so when it cuts it. Further down, the estate lists recent runs and the pipelines that declare a `[schedule]`. The Schedule panel also shows the webhook demands waiting in the spool, which no tick has claimed yet. If the server cannot read the spool, the panel shows that error instead of a count.

## Review

The Review screen lists the plans that the policy plane sent to a human, by a rule or by the default effect. The engine ranks the queue by a score: blast radius × change class × staleness. A breaking schema change weighs 3, a bare `apply`, `promote` or `backfill` weighs 2, and anything else weighs 1. So a wide-reaching breaking change that has waited long comes first. It is the same list `rocky review --queue` prints.

Open a plan to see why it waits:

![One plan in the Review screen: an agent's run plan awaiting a human, a breaking finding that the email column of dim_customer is dropped, the default policy effect that required review, a sample-rows button, an Approve button, and the rocky review --approve command to copy](/ui-review.png)

The plan screen shows:

- **What it would break.** The breaking-change findings against `HEAD`, or why that check could not run.
- **Why it needs a human.** The rule, the capability, the principal and the blast radius behind the `require_review` decision.
- **The spec it was planned against.** For a product plan only: whether the product spec changed after the plan was made.
- **Sample rows.** Nothing is read until you ask. The button runs the model's query against the warehouse for up to 20 rows, and that query has a cost. The engine masks each classified column before the rows leave it, and refuses the whole sample rather than return a classified column it cannot mask. The refusal names every column and tag, and both ways out.

  The workspace `[mask]` block decides first. A strategy masks the column, and `none` returns it raw, which is an operator's decision rather than a gap. `[classifications.allow_unmasked]` is only the fallback for a tag that block does not answer: a tag with a strategy is masked even when the list names it.

  What is left is refused: a tag with no `[mask]` entry that the list does not name, and a tag answered only under `[mask.<env>]`, which this path does not read.

  The button appears only when the plan names exactly one model to sample.
- **Approval.** A panel beside the evidence, or under it on a narrow screen. It shows three steps: Proposed, Approved and Applied. Then the Approve and Apply buttons, and the command to copy.

In operator mode, the plan screen can approve and apply the plan. With a read-only token, the buttons are disabled and say why. You can always approve in a terminal:

```bash
rocky review <plan-id> --approve
```

A plan bound to a data product shows no Apply button. Apply it in a terminal. The spec digest must come from you, not from the plan.

The plan status records approval, not apply. So the Applied step shows as done only after an apply from this page succeeds. Otherwise, once the plan is approved, Applied shows as not known. The page cannot tell an applied plan from one that waits. The run itself is on Estate.

Apply runs the models as they are on disk, not SQL stored in the plan. So it checks the plan's models before it runs them. If a model was added, removed or edited after the plan was made, apply refuses with `plan_models_changed`: "models changed since this plan was made; plan again". Plan again, review the new plan, and apply that. A person's apply compares the models only, so a different environment or an edit to another pipeline does not refuse it. Approving compares the models only too, so a plan made in a shell can be approved here. It also means a config edit made after approval is not caught. The models can still change between this check and the run's own compile. An agent's apply is checked again inside the run. A plan whose models did not compile at plan time has no fingerprint and is not checked. A plan made before this check refuses with `plan_snapshot_missing`; plan again.

## Products

Products shows each [data product](/reference/commands/products/): its fulfillment loop state, its working spec digest, whether its approval is recorded, and its journal. The journal lists every event the engine recorded for the product, in order.

![The products screen for revenue_daily: the loop state is observing, the approval is recorded, and the journal lists 82 events from ownership acquired through elicitation, spec approval, drafting and repair](/ui-governor-product.png)

## Governance

Governance answers "what did the agents do, and was it allowed?" It is the one area with tabs of its own: Scorecard, Custody and Audit.

### Scorecard

The scorecard shows acceptance, review and denial rates for a window you pick. Group them by principal, by rule, or by the model decided on. It matches `rocky audit --scorecard`. It lists the metrics the ledger cannot support, and gives the reason for each.

### Custody

Enter a subject to trace its chain of custody. A subject is a model, a run id, a plan id, or another id the ledger records, such as `freeze:global`. It shows the decisions about it, the plan, the runs that applied it, any verification after apply, and its blast radius. It matches `rocky audit --for <subject>`.

![The Custody tab for the model revenue_daily: ten policy decisions, the latest plan, two apply runs, no verification row, and a blast radius of zero downstream models](/ui-governor-custody.png)

### Audit

The audit tab is the whole policy decision ledger, oldest first. Filter it to one product. It matches `rocky audit` and `rocky audit --product <name>`.

## Areas without a screen

Six of the eleven areas open nothing today. The sidebar folds them under **Coming later**, so the five areas that work come first. Open it to see each name with the reason under it, as plain text. A reason says where the same information is now, when it is somewhere. The reasons stay hidden until you open the fold:

| Area | What the sidebar says |
|---|---|
| Projects | One project for now: the one this server runs. |
| Runs | No page of its own yet. The runs table is on Estate. |
| Scheduler | No page of its own yet. The schedule status is on Estate. |
| Policies | No page yet. The engine serves the rules at `/api/v1/policy`. |
| Agents & Clusters | No page yet. Agent activity is on Needs you. |
| Settings | No page yet. The engine serves them at `/api/v1/settings`. |

A route with no page is not the same as nothing at all. The two API routes named above answer today, and the [Embedding guide](/guides/embedding/) covers them.

## Operator mode

Operator mode lets the page make changes: run, plan, approve and apply, and cancel a running job. They run as the OS user who started the server, like the VS Code extension. It is on when the token has full scope. The page then shows **Operator mode — changes run as this server's user** at all times. With a read-only token, the page shows the write controls disabled. The label at the top gives the reason once, and each button's tooltip repeats it. While a job runs, its button is disabled and says `running…`. A failed job shows its errors, or the final `Error:` lines, never the server's log lines.

### Cancel a running job

While a job runs, operator mode shows a **Cancel** button under it. A read-only page does not show it. The button calls `POST /api/v1/jobs/{id}/cancel`:

```
Cancel ─▶ SIGTERM to the job's process group (handled like Ctrl-C) ─▶ the button becomes "Force stop"
Force stop ─▶ SIGKILL to the job's process group                ─▶ the job stops at once
```

The engine answers only once the signal reached the job. The job then ends `cancelled`. If it exits 0 anyway, it ends `succeeded`. A job that exited before the signal reached it answers `409 job_not_running`. A cancelled run, apply or approve keeps the project's single write slot until its process has exited, so a new one waits for it (`409 mutation_in_progress`).

What a cancelled job leaves:

- **A run or apply of a replication pipeline, after Cancel**, stops like Ctrl-C in a terminal. It starts no new table copies and lets the copies in flight finish. It saves their watermarks, marks the other tables `Interrupted`, and exits `130`. Run history shows it as a partial failure. `rocky run --resume-latest` copies the rest.
- **Any other job, after Cancel** (models, a plan, an approval), stops at once. Rocky has no clean-stop step for these yet ([#1606](https://github.com/rocky-data/rocky/issues/1606)). A warehouse statement in flight is cut, and the warehouse keeps or drops it whole. A state write in flight is all or nothing. Models that finished stay built. Run the job again to finish.
- **After Force stop**, any job stops like a crash. An incremental replication table can have its rows landed before its watermark was saved. On DuckDB, Databricks, Snowflake and BigQuery, the next run of that table reads the watermark back from the target table first, so it does not copy the same rows twice. On other warehouses, prefer Cancel.

A plan file cut part-way no longer reads as its plan id, so Rocky refuses to apply it.

Cancel reaches only jobs this server process started. A scheduled run (`--scheduler`) answers `409 job_not_cancellable`, and a finished job answers `409 job_not_running`. On Windows there is no SIGTERM, so Cancel kills the job at once. Stopping `rocky serve` with Ctrl-C or `SIGTERM` sends each of its running jobs the same SIGTERM as Cancel, and waits up to 5 seconds for it to be sent.

`rocky serve --ui` turns it on by itself when all of these hold:

- The bind is loopback (`127.0.0.1`, `::1` or `localhost`).
- You gave no token.
- You gave neither `--allowed-host` nor `--allowed-origin`.

The server then prints one note on stderr. It says the UI can make changes as the server's user, and to use `--read-only` for a view-only UI.

```bash
rocky serve --ui --read-only    # view-only UI
```

Know these limits:

- **The printed link is a write credential.** In operator mode the `/login?t=` link carries a full-scope token. Anything that captures the server's stdout holds it: `docker logs`, CI logs, terminal scrollback, a log shipper. So does browser history. Use `--read-only` wherever the output is captured or shared.
- **A server behind a proxy is shared.** With `--allowed-host` or `--allowed-origin`, the server stays read-only. It generates a read-only token. It refuses `--token-scope full`. Writes from the UI need a token for each person.
- **A tunnel looks local.** An SSH `-L` tunnel or `kubectl port-forward` to a loopback port cannot be detected. The server still looks local. Use `--read-only` whenever someone else can reach your port.
- **A local proxy can look local too.** A reverse proxy on the same machine (nginx, Caddy) that forwards with `Host: 127.0.0.1` passes the host check with no `--allowed-host`, so the server stays in operator mode. Start it with `--read-only`, or name the proxy host with `--allowed-host`, which makes the server read-only.
- **`--open` shows the token.** It puts the address, token included, in the opener's command line. At full scope, another local OS user who reads the process list could act with it. On a shared machine, pass `--token` and use `--read-only`.
- **The link stays in browser history.** The `/login?t=` link, token included, is kept in history and may sync to your other devices. It works until the server stops. Restart the server to end it.
- **Other local pages get the cookie.** Browsers do not separate cookies by port, so a page from another server on this machine receives the session cookie. That server's owner could replay it. The cookie cannot write from such a page, because a write needs this server's exact origin and `X-Rocky-UI: 1`. Each server uses its own cookie name, so two `rocky serve --ui` on one machine do not end each other's sessions.
- **One token does everything.** It can plan, approve and apply, as the CLI user can. Approving in the UI is not a second person's sign-off.
- **The webhook route has its own secret.** `/api/v1/hooks/trigger/{pipeline}` checks an HMAC signature. It is the one write route outside the Bearer token.

An approval from the UI records the approver source `http_api` and the server's git identity. If the server cannot resolve `git config user.email`, or it is literally `unknown`, the approve job fails with `approver_identity_unresolved`. Run records from HTTP jobs show the session source `http_api`. That names the HTTP API as the source. It does not prove a browser made the call.

## What the UI cannot do

- **With a read-only token, it cannot write.** A read-only token gets `403 forbidden_read_only_token` on `POST /api/v1/jobs/run` and every other token-checked write.
- **It cannot apply a product-bound plan.** Apply it in a terminal.
- **It cannot change policy.** The Governance screens report decisions, and `/api/v1/policy` reads the rules back. The rules themselves live in the `[policy]` block of `rocky.toml`, and `rocky policy freeze` and `rocky policy unfreeze` are the CLI's only policy writes.

## How the server protects the page

With `--ui`, the server adds checks that a plain `rocky serve` does not run:

- On a host that is not loopback, `--ui` refuses to start without a token, and refuses a full-scope token. On a loopback host with no token, it generates a per-process token: full scope, or read-only when `--read-only` or `--allowed-host`/`--allowed-origin` is set. A loopback server with `--allowed-host` or `--allowed-origin` refuses a full-scope token.
- A request whose `Host` is not a loopback name, the bind host, or an `--allowed-host` entry gets `421 host_not_allowed`. This defends against DNS rebinding, where an attacker's domain is made to resolve to `127.0.0.1`. The check also runs on a loopback `rocky serve` without `--ui`. `GET /api/v1/health` skips this check, so a load balancer probe still works.
- A request whose `Origin` is neither the server's own nor an `--allowed-origin` entry gets `403 origin_not_allowed`. The check reads the origin's host, not the whole origin, so an `http` or `https` origin on an allowed host passes whatever its port. On a loopback server that includes `http://localhost:5173`, a local dev server. `GET /api/v1/health` skips this check too.
- Every UI file response carries a Content Security Policy. The page loads scripts and fonts from this server only, and styles from this server or inline. Nothing may frame it.
- `--ui --scheduler` refuses to start without `ROCKY_WEBHOOK_SECRET`, because a browser can reach the webhook route.

Behind a reverse proxy, name the proxy host with `--allowed-host`:

```bash
rocky serve --ui --host 0.0.0.0 --token "$TOKEN" --read-only \
  --allowed-host rocky.internal
```

The [`rocky serve` reference](/reference/commands/development/#rocky-serve) lists every flag. The [Embedding guide](/guides/embedding/) covers the HTTP API behind the page.
