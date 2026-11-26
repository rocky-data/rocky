---
title: The Browser UI
description: "What rocky serve --ui shows: eleven areas in a sidebar, five of which open a screen today. How to open it, operator mode, and what it can and cannot do."
sidebar:
  order: 3
---

`rocky serve --ui` serves a browser UI for one project. It shows the project's models and runs, the changes waiting for approval, and the policy decisions and product history the engine recorded. On your own machine, the UI can also make changes (see [Operator mode](#operator-mode)). Every value on the page comes from a typed `/api/v1` payload. Most are the same payloads the CLI prints with `--output json`.

![A tour of the Rocky browser UI: the Project screen with its lineage, the changes waiting for approval, an agent's breaking change that needs approval, the Inbox, a model's trace, and a data product's delivery and cost](/demo-ui-tour.gif)

The UI ships inside the release binaries and the container image. A sidebar lists eleven areas. Five open a screen today:

```
  Inbox              what waits for you now
  Project            models, their lineage, runs and schedules
  Changes            changes that need approval, one change in full
  Data products      each data product, and its history
  Oversight          agent activity, trace, audit log
```

The other six are not links. See [Areas without a screen](#areas-without-a-screen). On a screen narrower than 1024 pixels the sidebar folds behind an **Areas** button.

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

With no token configured on a loopback host (`127.0.0.1`, `::1` or `localhost`), the server generates a per-process token and prints a note on stderr. It works until the server stops. Each start makes a new one, so an old address stops working after a restart. On any other host, `--ui` refuses to start without `--token` and `--read-only` (or `--token-scope read-only`).

The generated token has full scope. This is [operator mode](#operator-mode). For a view-only UI, add `--read-only`:

```bash
rocky serve --ui --read-only
rocky serve --ui --token "$(openssl rand -hex 16)" --read-only   # a chosen token survives restarts
```

The address also goes to stdout, so it lands in a terminal log, `docker logs`, a service journal and browser history. On a shared host, an ssh port-forward, a devcontainer, a Codespace or behind a proxy, choose the token yourself and use `--read-only`.

### Sign-in and the session cookie

The token travels in the query of one request, `GET /login?t=<token>`. The server checks it in constant time. On a match it answers `303` to `/ui/` and sets a session cookie, `rocky_ui_<tag>` (the tag differs per server; `HttpOnly`, `SameSite=Strict`, a session cookie; `Secure` when a TLS proxy sends `X-Forwarded-Proto: https`). The cookie holds a keyed hash of the token, with a key that is new for each server process, so a restart ends every session. The page never holds the token.

The server logs no request URIs and never echoes the token. The `/login` answers send `Cache-Control: no-store` and `Referrer-Policy: no-referrer`. A reverse proxy may log the query string of `/login`: configure it not to, or rotate the token.

A cookie session can also write, but only from the page. A write must carry an `Origin` that names this server exactly (or an `--allowed-origin` entry) and the header `X-Rocky-UI: 1`, or the answer is `403 ui_write_not_from_ui`. A read-only token stays read-only in a cookie session. Scripts and embedders keep using `Authorization: Bearer <token>`.

If the link is stale or wrong, `/login` answers `401` with a page that has a field for the token. Without a session, `/ui/` shows **Session expired**. Open the newest `Rocky UI:` link from the console.

To run the UI somewhere other than your own machine, use the [container image](/guides/run-the-image/) or the [Helm chart](/guides/kubernetes/). The chart serves the UI by default.

## Inbox

The sidebar shows a badge on Inbox and on Changes when decisions wait on you. The number is the count the Inbox lists. The page reads it again on each navigation, and every 30 seconds while the tab is visible. No badge means nothing waits, or the count could not be read (for example, while a run holds the state store).

The page shows the digest that `rocky brief` prints, for a window you pick (7 days by default). A summary line comes first: decisions waiting, how the runs went, and whether a change freeze or degraded rule is in force. Cards follow. The first lists each change that waits for approval as a row with a **Review change** button. The others are agent activity (with a bar of allowed, needed approval and blocked), runs, agent limits (degraded rules and active change freezes), cost, drift, freshness, quality and the scheduler.

![The Inbox: a badge of 1 on Inbox and on Changes, a summary line saying one change waits and two runs succeeded, one change for revenue_daily with a Review change button, and a bar over ten agent decisions, five allowed and five that needed approval](/ui-governor-brief.png)

A card says so when its data was not available. A signal the engine could not read shows as **not recorded**, never as zero. A part with nothing in the window says so in its own words, such as "no runs in the window".

## Project

The printed address opens Project, even though Inbox is first in the sidebar. It shows the project as the engine compiled it. The strip at the top names the config file, the pipelines and adapters, the compiled models with their diagnostics, and the newest run.

![The Project screen for the playground project: a project strip with one transformation pipeline, one DuckDB adapter and three compiled models, the newest run, Plan a change (disabled, because the project has no replication pipeline), Run, Test, CI and Refresh buttons, and the lineage of raw_orders, customer_orders and revenue_summary](/ui-estate.png)

Below the strip, the Lineage panel draws every model and the edges between them. Click a model to open its columns (with types, where the compiler inferred them) and its compiled SQL. The server caps the SQL at 256 KiB and says so when it cuts it. Further down are the recent runs and the pipelines that declare a `[schedule]`. The Schedule panel also shows the queued triggers: webhook calls waiting in the spool that the scheduler has not picked up yet. If the server cannot read the spool, the panel shows that error instead of a count.

## Changes

The Changes screen lists the changes that policy marked **Needs approval**, by a rule or by the default for agents. The CLI calls a change a plan: this is the list `rocky review --queue` prints. The engine ranks the queue by impact (downstream models affected) × change class × staleness. A breaking schema change weighs 3, a bare `apply`, `promote` or `backfill` weighs 2, and anything else weighs 1.

Open a change to see why it waits:

![One change in the Changes screen: an agent's run change that needs approval, a breaking finding that the email column of dim_customer is dropped, the default policy effect that asked for approval, a sample-rows button, and an Approval panel with the Proposed, Approve and Apply steps, an Approve change button and the rocky review --approve command to copy](/ui-review.png)

The change screen shows:

- **What it would break.** The breaking-change findings against `HEAD`, or why that check could not run.
- **Why it needs approval.** The rule, the action, the actor and the impact behind the `require_review` decision.
- **The spec this change was made against.** For a data product change only: whether the product spec changed after the change was made.
- **Sample rows.** Nothing is read until you ask. The button runs the model's query against the warehouse for up to 20 rows, and that query has a cost. It appears only when the change names exactly one model to sample.
- **Approval.** A panel with three steps (Proposed, Approve, Apply), the **Approve change** and **Apply change** buttons, and the command to copy.

Sample rows are masked before they leave the engine. The workspace `[mask]` block decides first: a strategy masks the column, and `none` returns it raw. `[classifications.allow_unmasked]` is only the fallback for a tag that block does not answer. The engine refuses the whole sample when a classified column has no `[mask]` entry and is not in the list, or is answered only under `[mask.<env>]`. The refusal names every column and tag, and both ways out.

In operator mode, the change screen can approve and apply the change. With a read-only token, the buttons are disabled and say why. You can always approve in a terminal:

```bash
rocky review <plan-id> --approve
```

An approval records a row in the audit log before it writes the review marker. An approval from this page records the channel `http_api` and an `unnamed` actor (the CLI's principal). The approve job drops the server's `ROCKY_PRINCIPAL_ID` and `ROCKY_PRINCIPAL`, because they name the operator who started the server, not you. See [`rocky review`](/reference/commands/governance-reclamation/#rocky-review).

A change bound to a data product shows a **Spec digest you approved** box instead. It is always empty when the page opens. Type the digest of the spec you approved: it is `approval.spec_digest` in `rocky product status <name>`, never the change's own digest, or the change would be compared with itself. Apply stays off until the change is approved and the box holds a digest. The page asks you to confirm and sends exactly that digest. The engine refuses the apply if the digests differ, and the page shows the engine's last `Error:` lines (paths and secrets removed) naming both. The terminal form:

```bash
rocky apply <plan-id> --expect-spec-digest <the spec digest you approved>
```

The change status records approval, not apply. The Apply step shows as done only after an apply from this page succeeds. Otherwise, once the change is approved, it shows as not known. The run itself is on Project.

Apply runs the models as they are on disk, so it checks the change's models first. If a model was added, removed or edited after the change was made, apply refuses with `plan_models_changed`: plan again, review, and apply that. A plan made before the check refuses with `plan_snapshot_missing`. The check compares models only, so a config edit after approval does not refuse it. See [What apply checks for staleness](/reference/commands/core-pipeline/#what-apply-checks-for-staleness).

## Data products

Data products shows each [data product](/reference/commands/products/): its status (the fulfillment loop state), its current spec digest, whether its approval is recorded, and its history. The history lists every event the engine recorded for the product, in order.

![The data product screen for revenue_daily: the status is observing, the approval is recorded, and the Delivery and cost panel shows no deadline, no declared volumes, no known schedule, and this month's cost with no money figure because [cost] sets no price](/ui-governor-product.png)

In operator mode, a product whose spec has no approval, or whose current spec differs from the approved one, shows an **Approve the spec** section. See [Approve a product spec](#approve-a-product-spec).

### Delivery and cost

Each product's page has a **Delivery and cost** panel. It is read-only: you change a deadline or a budget in the files, not here.

- **Delivery** shows the spec's `deliver_by` deadline, whether the latest delivery was on time or how late it was, and whether the next one is at risk. The engine keeps the latest delivery only. The declared row-count ranges of the output and the sources show with their latest verdict.
- **Schedules** lists the scheduled pipelines that deliver this product, with their latest start. It is marked Experimental.
- **Cost this month** is the product's row of `rocky cost --month <month> --by product`, read from `GET /api/v1/cost/month`: its total cost, net cost and own build cost, its shared upstream cost (what it pays for tables it reads), shares nobody paid, and its `[product_budget.<name>]` verdict with each breach event.

Each cost figure says how sure it is: **measured**, **estimated** (split by estimated bytes, or storage by allocated blocks), **rows only** (split by rows scanned), **not measured** or **no price**. A figure Rocky did not measure, or one `[cost]` gives no price, never shows as `0` or `$0`. When run history does not cover the whole month, the panel says the month is partial and shows no money total. A share a later reader can still change is marked provisional.

## Oversight

Oversight answers "what did the agents do, and was it allowed?" It has four tabs, each matching a CLI view:

| Tab | Shows | Matches |
|---|---|---|
| Agent activity | Allowed, needed-approval and blocked rates for a window. Group by actor, rule or model decided on. Lists the metrics the audit log cannot support, with the reason. | `rocky audit --scorecard` |
| Agents | Each agent that asked to change something, from the audit log: how it was named, its requests (one per change) and their outcomes. | `rocky audit` |
| Trace | Every recorded decision about a subject: a model, a run id, a change id, or another recorded id such as `freeze:global`. Shows its decisions, change, applying runs, post-apply checks and impact. | `rocky audit --for <subject>` |
| Audit log | The whole policy decision log, oldest first. Filter to one data product. | `rocky audit`, `rocky audit --product <name>` |

### Agents

The Agents tab lists each caller that is not a person and asked to change something. It reads the audit log for the last 1, 7 or 30 days. One row is one agent: how it was named, how many requests it made and how they ended (allowed, needed approval, blocked).

A request is one change, found by its plan id. A change that touches several models writes one gate decision per model, so the Requests column shows both counts. The outcome bar counts decisions. Under the table, the Audit log link opens the whole log. The detail panel shows the `rocky audit --actor <id> --since <N>d` command that lists one agent's rows.

Names are self-asserted. An agent names itself with `ROCKY_PRINCIPAL_ID`, a flag or its MCP profile. Rocky records the name but does not verify it. Treat a name as a label, not a proof. Rows with no name show as `unnamed` or `unrecorded`.

The audit log does not record an agent's channel yet, so the Channel column shows a dash, with that reason as its tooltip. Rules and change freezes act on all agents as a class, never on one name.

When `rocky run` applies a schema drift on its own, the audit log records it as an agent decision too, under a plan id that starts with `autoapply:`. The tab leaves those decisions out and says how many.

![The Trace tab for the model revenue_daily: a table of twelve policy decisions, agent proposals and breaking-change evaluations and the human approvals that applied them](/ui-governor-custody.png)

## Areas without a screen

Six of the eleven areas open nothing today. The sidebar folds them under **No page yet**. Open the fold to see each name with its reason as plain text. The reasons stay hidden until you open it.

| Area | What the sidebar says |
|---|---|
| Projects | One project for now: the one this server runs. |
| Runs | No page of its own yet. The runs table is on Project. |
| Scheduler | No page of its own yet. The schedule status is on Project. |
| Policies | No page yet. The engine serves the rules at `/api/v1/policy`. |
| Agents & Clusters | No page yet. Agent activity is on Oversight. |
| Settings | No page yet. The engine serves them at `/api/v1/settings`. |

The two API routes named above answer today. The [Embedding guide](/guides/embedding/) covers them.

## Operator mode

Operator mode lets the page make changes: run, plan a change, test, run CI, approve and apply, start and lift a change freeze, create a branch, promote a branch (only when the project requires a branch approval), approve a product's spec, plan and apply a table optimization, and cancel a running job. They run as the OS user who started the server, like the VS Code extension. It is on when the token has full scope. The page then shows **Operator mode — changes run as this server's user** at all times.

With a read-only token, the write controls are disabled and say why. While a job runs, its button says `running…`. A failed job shows its errors, or the final `Error:` lines, never the server's log lines.

`rocky serve --ui` turns operator mode on by itself when the bind is loopback, you gave no token, and you gave neither `--allowed-host` nor `--allowed-origin`. The server then prints one note on stderr. A server with `--allowed-host` or `--allowed-origin` is shared: it generates a read-only token and refuses `--token-scope full`.

Know these limits:

- **The printed link is a write credential.** The `/login?t=` link carries a full-scope token. Anything that captures stdout holds it (`docker logs`, CI logs, scrollback, a log shipper, synced browser history). It works until the server stops. Use `--read-only` wherever output is captured or shared.
- **A tunnel or local proxy looks local.** An SSH `-L` tunnel, `kubectl port-forward`, or a same-machine proxy (nginx, Caddy) that sends `Host: 127.0.0.1` keeps the server in operator mode. Use `--read-only`, or name the proxy host with `--allowed-host`.
- **`--open` shows the token** in the opener's command line, which other local users can read with `ps`. On a shared machine, pass `--token` and `--read-only`.
- **Other local pages get the cookie.** Browsers do not separate cookies by port. A page from another server on this machine receives it but cannot write with it, because a write needs this server's exact origin and `X-Rocky-UI: 1`. Each server uses its own cookie name, so two servers do not end each other's sessions.
- **One token does everything.** It can plan, approve and apply, as the CLI user can. Approving in the UI is not a second person's sign-off.
- **The webhook route has its own secret.** `/api/v1/hooks/trigger/{pipeline}` checks an HMAC signature. It is the one write route outside the Bearer token.

An approval from the UI records the approver source `http_api` and the server's git identity. That names the channel. It does not prove a browser made the call. Every write job takes the project's single write slot. While another holds it, a new write answers `409 mutation_in_progress`: retry when the job ends. The routes, bodies and error codes are in the [Embedding guide](/guides/embedding/#mutations-are-jobs-you-poll).

### Change freeze

A change freeze blocks the matching actions until you lift it. The CLI calls it a freeze. The **Inbox** has a **Change freeze** form below the agent limits section. Pick an actor (`agent`, `human` or both) and a scope (`any`, `layer=<n>`, `model=<glob>`, `classification=<v>` or `tag=<key>[=<value>]`), add a reason if you want one, and press **Start change freeze**. The page asks you to confirm. Each change freeze in force has a **Lift change freeze** button that lifts that freeze's actor and scope.

A durable freeze marker whose body cannot be read is in force for both actors on every model. It is listed once, as **unreadable marker**. Its lift sends no actor and scope `any`, and lifts every unreadable marker and every change freeze on scope `any`, for both actors. The confirm text says so.

The buttons run `rocky policy freeze` and `rocky policy unfreeze` on the server. The page shows the result's notes under the button. A change freeze with no `[policy]` block is recorded but enforced nowhere, and shows as a warning. Both commands open the local state store, even on a remote `[state]` backend, and a run holds that store for its whole run. So a change freeze answers `409 mutation_in_progress` while a run, apply or approve is in progress. A run this server did not start holds the store but not the slot, so a change freeze then fails on the store lock. On a remote backend with durable markers, the marker is already written and the run stops at its next fence, but a retry writes a second marker. Retry after that run ends. The audit log row names the server's actor id (`ROCKY_PRINCIPAL_ID` in its environment, else `unnamed`), not the person at the page.

### Create a branch

The **Project** screen has a **Create branch** panel. The CLI calls it a preview. Give a branch name (optional) and a base ref (optional, default `main`). The page asks you to confirm, then runs `rocky preview create` on the server. With no name, the server derives one from the current branch of its git checkout, so every click from the same checkout gives the same name, and a name already in use fails the job. The job registers the branch locally and copies each unchanged model from production into the warehouse schema `branch__<name>`. Production is only read. Depending on the warehouse, each copy is a metadata-only clone or a full copy, which costs compute and storage. The confirm text says so.

The job does not re-run the changed models. The page lists them as "planned but not re-run".

- A copy that fails for one model does not fail the job. The page warns, names the model, and shows "planned but not copied". The server log says why.
- A branch with no changed models copies nothing, with a warning.
- If the job fails after it registered the branch, the branch stays registered, so a retry needs a new name.
- The page shows only model names, the schema name and fixed words, never warehouse error text.

The project folder must be inside a git repository, because the changed models come from `git diff` between the base ref and `HEAD`.

### Promote a branch

The **Project** screen has a **Promote branch** panel. Promote replaces production tables with the contents of a branch, so the page offers it only when `rocky.toml` sets `[branch.approval] required = true` (shown as `branch_approval_required` in `GET /api/v1/project`). Otherwise the panel shows one line that says why and names the key. The page cannot sign the branch approval: a browser click would record it as `local` although it came over the network, and one session could approve and promote its own branch. Sign it in a terminal:

```bash
rocky branch approve <branch>
```

```
Plan promote branch ──▶ change planned, tables listed ──▶ Promote branch (confirm) ──▶ production tables replaced
 (checks the approval                nothing in production              applies that exact change,
  and breaking changes)              has changed yet                    it is not planned again
```

1. Pick a branch and press **Plan promote branch**. The server runs `rocky plan promote <branch>`, writes a promote plan and replaces nothing. The page lists every production table the change would replace. A refused plan shows the engine's last `Error:` lines.
2. Press **Promote branch**. The confirm text names every production table that will be replaced. The page then applies the change by its id, through the same apply as the Changes screen. Rocky cannot undo it. The result lists each table as replaced or not replaced.

The server enforces the same rules for calls that skip the page (`409 branch_approval_not_required`, `409 promote_plan_not_approved`). See [Apply](/guides/embedding/#mutations-are-jobs-you-poll). For a promote change, the change screen shows Apply to production disabled, with the reason and the terminal command, unless the project requires a branch approval.

### Approve a product spec

A product's page shows **Approve spec** when the current spec has no approval, or differs from the approved one. The confirm question names the product, the spec digest the page read, the output model, and the approval it replaces. The server then runs `rocky product approve <name> --expect-digest <digest>`. If the spec on disk changed after the page read it, the command refuses with `approval-digest-mismatch`, names both digests and writes nothing: read the spec again. The result also says when the spec was already approved. The engine refuses a new spec revision while fulfillment work is in flight, and the page shows that refusal.

The approver is stored as `<server git email> (http_api)`. A terminal approval stores the bare email. The label does not prove who was at the page. If the server cannot read `git config user.email`, or it is `unknown`, the job fails with `approver_identity_unresolved` and writes nothing.

### Optimize tables

The **Project** screen has an **Optimize tables** panel. It runs `OPTIMIZE` and `VACUUM` on Delta tables, on Databricks only, in two steps. The CLI calls it compact.

1. Choose **One table** (`catalog.schema.table`) or **Every managed table in a catalog**, then press **Plan optimize tables**. The server runs `rocky compact <table>` or `rocky compact --catalog <name>`, which writes a plan file and runs no statement. The page shows the statement count, the table count, the target file size and every table. If the command could not save the plan, the page says so and offers no apply.
2. Press **Optimize tables**. The confirm text names every table. The page applies the change by its id, so what runs is the change you saw. Rocky cannot undo it.

The page never passes `--dry-run`, because that still writes an applicable plan. The plan step is the preview, and it takes no write slot. The panel has no archive option. See [What the UI cannot do](#what-the-ui-cannot-do).

### Jobs in the corner

Every job the page starts also gets a card in the bottom-right corner. The card stays when you open another screen. While any job runs, the top bar says how many: "1 job running". It counts only the jobs this browser tab started, because the engine has no list of jobs to read.

| The job | Its card |
|---|---|
| Running | How long it has run, and **Cancel** in operator mode |
| Succeeded | "Done" and how long it ran. The card closes after 6 seconds. |
| Failed | The first line of the failure, with the rest under **Details**. It stays until you close it. |
| Cancelled | "Cancelled". It stays until you close it. |
| Not started | The job that holds the project. A second write is refused at once, never queued. |
| Cannot reach the engine | The page keeps trying. The job may still be running. |
| Outcome unknown | The page could no longer read the job, which may have finished. Check Runs or the audit log first. |

A card never starts a write again. A failed apply, approval or change freeze is started again from its own screen, behind that screen's confirm step.

If you leave a screen while its job runs and come back, the screen shows that job, and how it ended. After you have seen the end and left again, the screen starts clean. Reloading the page drops every card, but a running job keeps running.

The engine reports no progress while a job runs, and keeps no log the page can read. So a card shows neither.

### Cancel a running job

While a job runs, operator mode shows a **Cancel** button under it, and on its card (not on a read-only page). It calls `POST /api/v1/jobs/{id}/cancel`:

```
Cancel ─▶ SIGTERM to the job's process group (handled like Ctrl-C) ─▶ the button becomes "Force stop"
Force stop ─▶ SIGKILL to the job's process group                ─▶ the job stops at once
```

The job ends `cancelled`, or `succeeded` if it exits 0 anyway. It keeps the write slot until its process has exited. The error codes (`409 job_not_running`, `409 job_not_cancellable`) are in the [Embedding guide](/guides/embedding/#mutations-are-jobs-you-poll). What a cancelled job leaves:

- **A run or apply of a replication pipeline, after Cancel**, stops like Ctrl-C. It starts no new table copies, lets the copies in flight finish, saves their watermarks, marks the other tables `Interrupted` and exits `130`. Run history shows a partial failure. `rocky run --resume-latest` copies the rest.
- **Any other job, after Cancel** (models, a plan, an approval), stops at once. Rocky has no clean-stop step for these yet ([#1606](https://github.com/rocky-data/rocky/issues/1606)). A warehouse statement in flight is cut, and the warehouse keeps or drops it whole. A state write in flight is all or nothing. Models that finished stay built.
- **After Force stop**, any job stops like a crash. An incremental replication table can have its rows landed before its watermark was saved. On DuckDB, Databricks, Snowflake and BigQuery, the next run reads the watermark back from the target table first, so it does not copy the same rows twice. On other warehouses, prefer Cancel.

A plan file cut part-way is refused at apply. Cancel reaches only jobs this server process started (a scheduled run answers `409 job_not_cancellable`). On Windows there is no SIGTERM, so Cancel kills the job at once. Stopping `rocky serve` with Ctrl-C or `SIGTERM` sends each running job the same SIGTERM as Cancel, and waits up to 5 seconds for it to be sent.

## What the UI cannot do

- **With a read-only token, it cannot write.** A read-only token gets `403 forbidden_read_only_token` on `POST /api/v1/jobs/run` and every other token-checked write.
- **It cannot apply or run an archive plan.** Archive deletes rows and then vacuums with no retention window, so it cannot be undone. The change screen shows no Apply button for an archive change, and the server answers `409 archive_apply_terminal_only` to `POST /api/v1/jobs/apply` for one. `rocky archive` stays in a terminal. Apply the plan there with `rocky apply <plan-id>`.
- **It cannot sign a branch approval.** Sign it in a terminal with `rocky branch approve <branch>`.
- **It cannot know the spec digest for you.** A change bound to a data product applies only with a digest you type, and the digest must come from the approval.
- **It cannot edit policy rules.** The Oversight screens report decisions, and `/api/v1/policy` reads the rules back. The rules live in the `[policy]` block of `rocky.toml`. The only policy writes are `rocky policy freeze` and `rocky policy unfreeze`. In operator mode the page runs them for you (see [Change freeze](#change-freeze)).

## How the server protects the page

With `--ui`, the server adds the checks in [Security defaults](/guides/embedding/#security-defaults): `--ui` refuses to start on a non-loopback host without a token and refuses a full-scope token there, the host and origin checks run (also on a loopback server without `--ui`), and `--ui --scheduler` refuses to start without `ROCKY_WEBHOOK_SECRET`. Every UI file response carries a Content Security Policy: scripts and fonts from this server only, styles from this server or inline, and no framing.

Behind a reverse proxy, name the proxy host with `--allowed-host`:

```bash
rocky serve --ui --host 0.0.0.0 --token "$TOKEN" --read-only \
  --allowed-host rocky.internal
```

The [`rocky serve` reference](/reference/commands/development/#rocky-serve) lists every flag. The [Embedding guide](/guides/embedding/) covers the HTTP API.
