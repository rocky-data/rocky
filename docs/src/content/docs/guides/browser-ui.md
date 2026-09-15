---
title: The Browser UI
description: "What rocky serve --ui shows: the estate, the review queue, and the governor's brief, custody, audit and product screens. How to open it, and what it can and cannot do."
sidebar:
  order: 5.8
---

`rocky serve --ui` serves a browser UI for one project. It shows the project's models and runs, the plans waiting for a human, and the policy decisions and product history the engine recorded. The UI is read-only. Every value on the page comes from a typed `/api/v1` payload. Most of those payloads are the same ones the CLI prints with `--output json`.

![A tour of the Rocky browser UI: the estate with its DAG, the review queue, an agent's breaking change awaiting a human, the governor brief, a model's custody chain, and a data product's journal](/demo-ui-tour.gif)

The UI ships inside the release binaries and the container image. It has three areas:

```
  Estate     models, the DAG, runs and schedules        what is there
  Review     plans that need a human, one plan in full  what waits for you
  Governor   brief, scorecard, custody, audit, products what happened, and why
```

## Open the UI

Start the server with a read-only token:

```bash
rocky serve --ui --token "$(openssl rand -hex 16)" --token-scope read-only
```

The server prints one address:

```
Rocky UI: http://127.0.0.1:8080/ui/#token=<secret>
```

Open that address. The page reads the token from the part after `#`, keeps it for the browser tab, and removes it from the address bar. Browsers never send that part to a server, so the token is in no access log.

A tab opened without the token shows **No token for this tab**. Open the printed address again to fix it.

To run the UI somewhere other than your own machine, use the [container image](/guides/run-the-image/) or the [Helm chart](/guides/kubernetes/). The chart serves the UI by default.

## Estate

The estate is the first screen. It shows the project as the engine compiled it. The strip at the top names the config file, the pipelines and adapters, the compiled models with their diagnostics, and the newest run.

![The estate screen for the playground project: a project strip with one transformation pipeline, one DuckDB adapter and three compiled models, the newest run, and a DAG of raw_orders, customer_orders and revenue_summary](/ui-estate.png)

Below the project strip, the DAG draws every model and the edges between them. Click a model to open its detail: its columns (with their types, where the compiler inferred them) and its compiled SQL. The server caps the SQL at 256 KiB and says so when it cuts it. Further down, the estate lists recent runs and the pipelines that declare a `[schedule]`.

## Review

The Review screen lists the plans that a policy rule sent to a human. The engine ranks the queue by a score: blast radius × classification × staleness. A wide change to classified data that has waited long comes first. It is the same list `rocky review --queue` prints.

Open a plan to see why it waits:

![One plan in the Review screen: an agent's run plan awaiting a human, a breaking finding that the email column of dim_customer is dropped, the default policy effect that required review, a sample-rows button, and the rocky review --approve command to copy](/ui-review.png)

The plan screen shows:

- **What it would break.** The breaking-change findings against `HEAD`, or why that check could not run.
- **Why it needs a human.** The rule, the capability, the principal and the blast radius behind the `require_review` decision.
- **The spec it was planned against.** For a product plan only: whether the product spec changed after the plan was made.
- **Sample rows.** Nothing is read until you ask. The button runs the model's query against the warehouse for up to 20 rows, and that query has a cost. The engine masks classification-tagged columns before the rows leave it. A plan that names more than one model has no single model to sample, so the button does not appear.
- **How to approve.** The command to copy.

You approve in a terminal, not on the page. The approval marker records a git identity, and the page holds a read-only token:

```bash
rocky review <plan-id> --approve
```

## Governor

The Governor area answers "what did the agents do, and was it allowed?" It has five tabs.

### Brief

The brief is the estate digest that `rocky brief` prints, for a window you pick (7 days by default). It has a card for each part of the digest: what needs you, the agents' policy decisions, runs, autonomy (degraded rules and active freezes), cost, drift, freshness, quality and the scheduler.

![The Governor brief: one pending plan under Needs you, ten agent decisions with their capability, effect and rule, two successful runs, no degraded autonomy, and a cost card](/ui-governor-brief.png)

Each card says whether its data was available. A signal the ledger does not hold shows as **not recorded**, never as a zero.

### Scorecard

The scorecard shows acceptance, review and denial rates for a window you pick. Group them by principal, by rule, or by the model decided on. It matches `rocky audit --scorecard`. It lists the metrics the ledger cannot support, and gives the reason for each.

### Custody

Enter a subject to trace its chain of custody. A subject is a model, a run id, a plan id, or a ledger id such as `product:revenue_daily`. It shows the decisions about it, the plan, the runs that applied it, any verification after apply, and its blast radius. It matches `rocky audit --for <subject>`.

![The Custody tab for the model revenue_daily: ten policy decisions, the latest plan, two apply runs, no verification row, and a blast radius of zero downstream models](/ui-governor-custody.png)

### Audit

The audit tab is the whole policy decision ledger, oldest first. Filter it to one product. It matches `rocky audit` and `rocky audit --product <name>`.

### Products

The products tab shows each [data product](/reference/commands/products/): its fulfillment loop state, its working spec digest, whether its approval is recorded, and its journal. The journal lists every event the engine recorded for the product, in order.

![The Products tab for revenue_daily: the loop state is observing, the approval is recorded, and the journal lists 82 events from ownership acquired through elicitation, spec approval, drafting and repair](/ui-governor-product.png)

## What the UI cannot do

The page reads. It does not write.

- **It cannot start a run.** The UI token must be read-only. A read-only token gets `403 forbidden_read_only_token` on `POST /api/v1/jobs/run` and every other token-checked write. The one route that ignores the token is the webhook route, which checks its own HMAC signature. The page does not hold that secret. To submit jobs over HTTP, run a second `rocky serve` without `--ui`, or use the CLI.
- **It cannot approve a plan.** Review shows the command. You run it in a terminal.
- **It cannot change policy.** The Governor screens report decisions. The rules live in the `[policy]` block of `rocky.toml`, and `rocky policy freeze` and `rocky policy unfreeze` are the CLI's only policy writes.

## How the server protects the page

With `--ui`, the server adds checks that a plain `rocky serve` does not run:

- `--ui` refuses to start without a token, or with a token that is not read-only.
- A request whose `Host` is not a loopback name, the bind host, or an `--allowed-host` entry gets `421 host_not_allowed`. This defends against DNS rebinding, where an attacker's domain is made to resolve to `127.0.0.1`. `GET /api/v1/health` skips this check, so a load balancer probe still works.
- A request whose `Origin` is neither the server's own nor an `--allowed-origin` entry gets `403 origin_not_allowed`.
- Every UI file response carries a Content Security Policy. The page loads scripts, styles and fonts from this server only, and nothing may frame it.
- `--ui --scheduler` refuses to start without `ROCKY_WEBHOOK_SECRET`, because a browser can reach the webhook route.

Behind a reverse proxy, name the proxy host with `--allowed-host`:

```bash
rocky serve --ui --host 0.0.0.0 --token "$TOKEN" --token-scope read-only \
  --allowed-host rocky.internal
```

The [`rocky serve` reference](/reference/commands/development/#rocky-serve) lists every flag. The [Embedding guide](/guides/embedding/) covers the HTTP API behind the page.
