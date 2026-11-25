---
title: The Browser UI
description: "What rocky serve --ui shows: eleven areas in a sidebar, five of which open a screen today. How to open it, operator mode, and what it can and cannot do."
sidebar:
  order: 3
---

`rocky serve --ui` serves a browser UI for one project. It shows the project's models and runs, the plans waiting for a human, and the policy decisions and product history the engine recorded. On your own machine, the UI can also make changes (see [Operator mode](#operator-mode)). Every value on the page comes from a typed `/api/v1` payload. Most are the same payloads the CLI prints with `--output json`.

![A tour of the Rocky browser UI: the estate with its DAG, the review queue, an agent's breaking change awaiting a human, the governor brief, a model's custody chain, and a data product's delivery and cost](/demo-ui-tour.gif)

The UI ships inside the release binaries and the container image. A sidebar lists eleven areas. Five open a screen today:

```
  Needs you          what waits for you now
  Estate             models, the DAG, runs and schedules
  Review             plans that need a human, one plan in full
  Products           each data product, and its journal
  Governance         scorecard, custody, audit
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

With no token configured, the server generates a per-process token and prints a note on stderr. The token works until the server stops. Each start makes a new token, so an old address stops working after a restart.

On a loopback host, the generated token has full scope. This is [operator mode](#operator-mode). For a view-only UI, add `--read-only`:

```bash
rocky serve --ui --read-only
```

The address also goes to stdout, so it lands in a terminal log, `docker logs`, a service journal and browser history. On a shared host, an ssh port-forward, a devcontainer, a Codespace or behind a proxy, choose the token yourself and use `--read-only`. A chosen token also survives restarts:

```bash
rocky serve --ui --token "$(openssl rand -hex 16)" --read-only
```

The server generates a token only on a loopback host (`127.0.0.1`, `::1` or `localhost`). On any other host, `--ui` refuses to start without `--token` and `--read-only` (or `--token-scope read-only`).

### Sign-in and the session cookie

The token travels in the query of one request, `GET /login?t=<token>`. The server checks it in constant time. If it matches, the server answers `303` to `/ui/` and sets a session cookie, `rocky_ui_<tag>` (the tag differs per server). The cookie is `HttpOnly` and `SameSite=Strict`. It usually ends when the browser closes. The server adds `Secure` when a TLS proxy sends `X-Forwarded-Proto: https`.

The cookie holds a keyed hash of the token, not the token. The key is new for each server process, so a restart ends every browser session. The page never holds the token.

The server logs no request URIs and never echoes the token. The `/login` answers send `Cache-Control: no-store` and `Referrer-Policy: no-referrer`, and the redirect takes the token out of the address bar. A reverse proxy may log the query string of `/login`: configure it not to, or rotate the token.

A cookie session can also write, but only from the page. A write must carry an `Origin` header that names this server exactly (the same host and port), or an `--allowed-origin` entry, and the header `X-Rocky-UI: 1`. Another page on the same machine shares the cookie, because cookies do not separate ports, so it is refused. Otherwise the answer is `403 ui_write_not_from_ui`. A read-only token stays read-only in a cookie session. Scripts and embedders keep using `Authorization: Bearer <token>`.

If the link is stale or wrong, `/login` answers `401` with a page that has a field for the token. Without a session, `/ui/` shows **Session expired**. Open the newest `Rocky UI:` link from the console.

To run the UI somewhere other than your own machine, use the [container image](/guides/run-the-image/) or the [Helm chart](/guides/kubernetes/). The chart serves the UI by default.

## Needs you

The sidebar shows a badge on Needs you and on Review when decisions wait on you. The number is the count Needs you lists. The page reads it again on each navigation, and every 30 seconds while the tab is visible. No badge means nothing waits, or the count could not be read (for example, while a run holds the state store).

The page shows the estate digest that `rocky brief` prints, for a window you pick (7 days by default). A summary line comes first: decisions waiting, how the runs went, and whether a freeze or degraded rule is in force. Cards follow. The first lists each pending plan as a row with a **Review plan** button. The others are policy decisions (with a bar of allowed, needs-review and denied), runs, autonomy (degraded rules and active freezes), cost, drift, freshness, quality and the scheduler.

![The Needs you screen: a badge of 1 on Needs you and on Review, a summary line saying one decision waits and two runs succeeded, one pending plan for revenue_daily with a Review plan button, and a bar over ten agent decisions, five allowed and five needing review](/ui-governor-brief.png)

A card says so when its data was not available. A signal the engine could not read shows as **not recorded**, never as zero. A part with nothing in the window says so in its own words, such as "no runs in the window".

## Estate

The printed address opens Estate, even though Needs you is first in the sidebar. It shows the project as the engine compiled it. The strip at the top names the config file, the pipelines and adapters, the compiled models with their diagnostics, and the newest run.

![The estate screen for the playground project: a project strip with one transformation pipeline, one DuckDB adapter and three compiled models, the newest run, Plan (disabled, because the project has no replication pipeline), Run, Test, CI and Refresh buttons, and a DAG of raw_orders, customer_orders and revenue_summary](/ui-estate.png)

Below the strip, the DAG draws every model and the edges between them. Click a model to open its columns (with types, where the compiler inferred them) and its compiled SQL. The server caps the SQL at 256 KiB and says so when it cuts it. Further down are the recent runs and the pipelines that declare a `[schedule]`. The Schedule panel also shows the webhook demands waiting in the spool, which no tick has claimed yet. If the server cannot read the spool, the panel shows that error instead of a count.

## Review

The Review screen lists the plans that the policy plane sent to a human, by a rule or by the default effect. It is the same list `rocky review --queue` prints. The engine ranks the queue by blast radius × change class × staleness. A breaking schema change weighs 3, a bare `apply`, `promote` or `backfill` weighs 2, and anything else weighs 1.

Open a plan to see why it waits:

![One plan in the Review screen: an agent's run plan awaiting a human, a breaking finding that the email column of dim_customer is dropped, the default policy effect that required review, a sample-rows button, and an Approval panel with the Proposed, Approved and Applied steps, an Approve button and the rocky review --approve command to copy](/ui-review.png)

The plan screen shows:

- **What it would break.** The breaking-change findings against `HEAD`, or why that check could not run.
- **Why it needs a human.** The rule, the capability, the principal and the blast radius behind the `require_review` decision.
- **The spec it was planned against.** For a product plan only: whether the product spec changed after the plan was made.
- **Sample rows.** Nothing is read until you ask. The button runs the model's query against the warehouse for up to 20 rows, and that query has a cost. It appears only when the plan names exactly one model to sample.
- **Approval.** A panel with three steps (Proposed, Approved, Applied), the Approve and Apply buttons, and the command to copy.

Sample rows are masked before they leave the engine. The workspace `[mask]` block decides first: a strategy masks the column, and `none` returns it raw, which is an operator's decision. `[classifications.allow_unmasked]` is only the fallback for a tag that block does not answer, so a tag with a strategy is masked even when the list names it. The engine refuses the whole sample when a classified column has no `[mask]` entry and is not in the list, or is answered only under `[mask.<env>]` (this path does not read it). The refusal names every column and tag, and both ways out.

In operator mode, the plan screen can approve and apply the plan. With a read-only token, the buttons are disabled and say why. You can always approve in a terminal:

```bash
rocky review <plan-id> --approve
```

An approval records a row in the policy-decision ledger before it writes the review marker. An approval from this page records the channel `http_api` and an `unnamed` principal. The approve job drops the server's `ROCKY_PRINCIPAL_ID` and `ROCKY_PRINCIPAL`, because they name the operator who started the server, not you. See [`rocky review`](/reference/commands/governance-reclamation/#rocky-review).

A plan bound to a data product shows a **Spec digest you approved** box instead. It is always empty when the page opens, and the page never fills it in. Type the digest of the spec you approved. Apply stays off until the plan is approved and the box holds a digest. The page then asks you to confirm, and sends exactly that digest (without surrounding spaces). The engine compares it with the digest the plan carries and refuses the apply if they differ. The refusal shows the engine's last `Error:` lines, with paths and secrets removed, and names both digests.

The check only works when your digest comes from the approval: it is `approval.spec_digest` in `rocky product status <name>`. Do not copy the plan's own digest from the spec card, because then the plan is compared with itself. The same apply works in a terminal:

```bash
rocky apply <plan-id> --expect-spec-digest <the spec digest you approved>
```

The plan status records approval, not apply. The Applied step shows as done only after an apply from this page succeeds. Otherwise, once the plan is approved, Applied shows as not known. The run itself is on Estate.

Apply runs the models as they are on disk, not SQL stored in the plan. So it checks the plan's models first. If a model was added, removed or edited after the plan was made, apply refuses with `plan_models_changed`: "models changed since this plan was made; plan again". Plan again, review the new plan, and apply that.

- The check compares models only. A different environment, an edit to another pipeline or a config edit after approval does not refuse it. A plan made in a shell can be approved here.
- The models can still change between this check and the run's own compile. An agent's apply is checked again inside the run.
- A plan whose models did not compile at plan time has no fingerprint and is not checked.
- A plan made before this check refuses with `plan_snapshot_missing`. Plan again.

## Products

Products shows each [data product](/reference/commands/products/): its fulfillment loop state, its working spec digest, whether its approval is recorded, and its journal. The journal lists every event the engine recorded for the product, in order.

![The products screen for revenue_daily: the loop state is observing, the approval is recorded, and the Delivery and cost panel shows no deadline, no declared volumes, no known schedule, and this month's cost with no money figure because [cost] sets no price](/ui-governor-product.png)

In operator mode, a product whose spec has no approval, or whose working spec differs from the approved one, shows an **Approve the spec** section. See [Approve a product spec](#approve-a-product-spec).

### Delivery and cost

Each product's page has a **Delivery and cost** panel. It is read-only: you change a deadline or a budget in the files, not here.

- **Delivery** shows the spec's `deliver_by` deadline, whether the latest delivery was on time or how late it was, and whether the next one is at risk. The engine keeps the latest delivery only. The declared row-count ranges of the output and the sources show with their latest verdict.
- **Schedules** lists the scheduled pipelines that deliver this product, with their latest start. It is marked Experimental.
- **Cost this month** is the product's row of `rocky cost --month <month> --by product`, read from `GET /api/v1/cost/month`: what it was charged, kept and built, what it paid for tables it reads, shares nobody paid, and its `[product_budget.<name>]` verdict with each breach event.

Each cost figure says how sure it is: **measured**, **estimated** (split by estimated bytes, or storage by allocated blocks), **rows only** (split by rows scanned), **not measured** or **no price**. A figure Rocky did not measure, or one `[cost]` gives no price, never shows as `0` or `$0`. When run history does not cover the whole month, the panel says the month is partial and shows no money total. A share a later reader can still change is marked provisional.

## Governance

Governance answers "what did the agents do, and was it allowed?" It has three tabs, each matching a CLI view:

| Tab | Shows | Matches |
|---|---|---|
| Scorecard | Acceptance, review and denial rates for a window. Group by principal, rule or model decided on. Lists the metrics the ledger cannot support, with the reason. | `rocky audit --scorecard` |
| Custody | The chain of custody for a subject: a model, a run id, a plan id, or another recorded id such as `freeze:global`. Shows its decisions, plan, applying runs, verification after apply and blast radius. | `rocky audit --for <subject>` |
| Audit | The whole policy decision ledger, oldest first. Filter to one product. | `rocky audit`, `rocky audit --product <name>` |

![The Custody tab for the model revenue_daily: a table of twelve policy decisions, agent proposals and breaking-change evaluations and the human approvals that applied them](/ui-governor-custody.png)

## Areas without a screen

Six of the eleven areas open nothing today. The sidebar folds them under **Coming later**. Open the fold to see each name with its reason as plain text. The reasons stay hidden until you open it.

| Area | What the sidebar says |
|---|---|
| Projects | One project for now: the one this server runs. |
| Runs | No page of its own yet. The runs table is on Estate. |
| Scheduler | No page of its own yet. The schedule status is on Estate. |
| Policies | No page yet. The engine serves the rules at `/api/v1/policy`. |
| Agents & Clusters | No page yet. Agent activity is on Needs you. |
| Settings | No page yet. The engine serves them at `/api/v1/settings`. |

The two API routes named above answer today. The [Embedding guide](/guides/embedding/) covers them.

## Operator mode

Operator mode lets the page make changes: run, plan, test, run CI, approve and apply, freeze and unfreeze policy, create a preview, promote a branch (only when the project requires a branch approval), approve a product's spec, plan and apply a compaction, and cancel a running job. They run as the OS user who started the server, like the VS Code extension. It is on when the token has full scope. The page then shows **Operator mode — changes run as this server's user** at all times.

With a read-only token, the page shows the write controls disabled. The label at the top gives the reason once, and each button's tooltip repeats it. While a job runs, its button is disabled and says `running…`. A failed job shows its errors, or the final `Error:` lines, never the server's log lines.

`rocky serve --ui` turns operator mode on by itself when all of these hold:

- The bind is loopback (`127.0.0.1`, `::1` or `localhost`).
- You gave no token.
- You gave neither `--allowed-host` nor `--allowed-origin`.

The server then prints one note on stderr. It says the UI can make changes as the server's user, and to use `--read-only` for a view-only UI.

Know these limits:

- **The printed link is a write credential.** In operator mode the `/login?t=` link carries a full-scope token. Anything that captures the server's stdout holds it: `docker logs`, CI logs, terminal scrollback, a log shipper. So does browser history, which may sync to your other devices. The link works until the server stops. Use `--read-only` wherever the output is captured or shared.
- **A server behind a proxy is shared.** With `--allowed-host` or `--allowed-origin`, the server stays read-only. It generates a read-only token and refuses `--token-scope full`. Writes from the UI need a token for each person.
- **A tunnel looks local.** An SSH `-L` tunnel or `kubectl port-forward` to a loopback port cannot be detected. Use `--read-only` whenever someone else can reach your port.
- **A local proxy can look local too.** A reverse proxy on the same machine (nginx, Caddy) that forwards with `Host: 127.0.0.1` passes the host check with no `--allowed-host`, so the server stays in operator mode. Start it with `--read-only`, or name the proxy host with `--allowed-host`.
- **`--open` shows the token.** It puts the address, token included, in the opener's command line. At full scope, another local OS user who reads the process list could act with it. On a shared machine, pass `--token` and use `--read-only`.
- **Other local pages get the cookie.** Browsers do not separate cookies by port, so a page from another server on this machine receives the session cookie. That page cannot write with it, because a write needs this server's exact origin and `X-Rocky-UI: 1`. Each server uses its own cookie name, so two `rocky serve --ui` on one machine do not end each other's sessions.
- **One token does everything.** It can plan, approve and apply, as the CLI user can. Approving in the UI is not a second person's sign-off.
- **The webhook route has its own secret.** `/api/v1/hooks/trigger/{pipeline}` checks an HMAC signature. It is the one write route outside the Bearer token.

An approval from the UI records the approver source `http_api` and the server's git identity. Run records from HTTP jobs show the session source `http_api`. That names the HTTP API as the source. It does not prove a browser made the call.

Every write job takes the project's single write slot. While a run, apply, approve, freeze, unfreeze, preview or product approval holds it, a new write answers `409 mutation_in_progress`. Retry when the job ends.

### Freeze and unfreeze

The **Needs you** screen has a **Freeze** form below the autonomy section. Pick a principal (`agent`, `human` or both) and a scope (`any`, `layer=<n>`, `model=<glob>`, `classification=<v>` or `tag=<key>[=<value>]`), and add a reason if you want one. The page asks you to confirm before it freezes. Each freeze in force has an **Unfreeze** button that asks the same way and lifts that freeze's principal and scope.

One kind of row is the exception. A durable freeze marker whose body cannot be read is in force for both principals on every model. It is listed once, as **unreadable marker**. Its Unfreeze sends no principal and scope `any`, the only form `rocky policy unfreeze` accepts for it. It lifts every unreadable marker and every freeze on scope `any`, for both agent and human. The confirm text says so.

The buttons run `rocky policy freeze` and `rocky policy unfreeze` on the server. A result can succeed and still need your attention, so the page shows the result's notes under the button. A freeze with no `[policy]` block in `rocky.toml` is recorded but enforced nowhere, and the page shows it as a warning, not as done.

Both commands open the local state store, even on a remote `[state]` backend, and a run holds that store for its whole run. So a browser freeze answers `409 mutation_in_progress` while a run, apply or approve is in progress. To stop a running run, use **Cancel**. A run this server did not start (from a terminal or an orchestrator) holds the store but not the server's slot, so a freeze then fails on the store lock. On a remote `[state]` backend with durable markers, the marker is already written and the run stops at its next fence, but a retry writes a second marker. Retry after that run ends.

The ledger row names the server's principal id, not the person at the page. The server reads it from `ROCKY_PRINCIPAL_ID` in its own environment, else `unnamed`. A `--principal-id` flag on `rocky serve` does not reach these jobs. The row does not record that the freeze came from the browser.

### Create a preview

The **Estate** screen has a **Preview** panel. Give a branch name (optional) and a base ref (optional, default `main`). The page asks you to confirm, then runs `rocky preview create` on the server. The name takes letters, digits and underscores, up to 64. With no name, the server derives one from the current branch of its git checkout, so every click from the same checkout gives the same name. A name already in use fails the job, so give a new name each time.

The command registers the branch in the local state store file on the server (not uploaded to a remote `[state]` backend), and records the server's OS user as creator. It then copies each unchanged model from its production schema into the warehouse schema `branch__<name>`, using the warehouse credentials in `rocky.toml`. Production tables are only read. Depending on the warehouse, each copy is a metadata-only clone or a full copy, which costs compute and storage. The confirm text says this before you start.

The command does not re-run the changed models. The page lists them as "planned but not re-run".

- A copy that fails for one model does not fail the job. The page shows a warning that names the model, and "planned but not copied" for a model that was not copied. The server log says why.
- A preview with no changed models copies nothing, and the page shows a warning.
- If the job fails after it registered the branch (for example, the warehouse is unreachable), the branch stays registered, so a retry needs a new name.
- The page shows only model names, the schema name and fixed words from the result, never warehouse error text.

The command finds the changed models with `git diff` between the base ref and `HEAD`. The project folder must be inside a git repository, and may be a subfolder of it.

### Promote a branch

The **Estate** screen has a **Promote** panel. Promote replaces production tables with the contents of a branch, so the page offers it only when `rocky.toml` sets `[branch.approval] required = true`. The server reads that key on every request and shows it as `branch_approval_required` in `GET /api/v1/project`. With the key off, or when the config cannot be read, the panel shows one line that says why the control is absent and names the key.

The key makes the engine ask for a signed branch approval before it plans a promote. The page does not sign that approval. Sign it in a terminal, from the project folder:

```bash
rocky branch approve <branch>
```

Promote then takes two steps, so you see what changes before it changes.

```
Plan promote ──▶ plan written, tables listed ──▶ Promote (confirm) ──▶ production tables replaced
 (checks the approval            nothing in production             applies that exact plan,
  and breaking changes)          has changed yet                   it is not planned again
```

1. Pick a branch and press **Plan promote**. The server runs `rocky plan promote <branch>`. It checks the signed approval and the breaking changes, writes a promote plan, and replaces nothing. The page lists every production table the plan would replace. A refused plan shows the engine's last `Error:` lines, for example that no valid approval was found.
2. Press **Promote**. The confirm text names every production table that will be replaced. After you confirm, the page applies the plan by its id, through the same apply as the Review screen. Apply writes the warehouse with the server's credentials, and Rocky cannot undo it. The result lists each table as replaced or not replaced.

The server enforces the same rule, so a call that skips the page is checked too. `POST /api/v1/jobs/promote` and an apply of a promote plan answer `409 branch_approval_not_required` unless the config sets `required = true`. An apply of a promote plan built without the approval gate answers `409 promote_plan_not_approved`. That covers a plan built while `required` was off, a plan built with the gate bypassed, and a plan that holds fewer approvals than `min_approvers`. The promote job runs without `ROCKY_BRANCH_APPROVAL_SKIP`, whatever the server's environment holds.

For a promote plan, the plan screen shows Apply disabled, with the reason and the terminal command, unless the project requires a branch approval.

The page has no **Approve** button for a branch. A browser click would record the approval as `local`, although it came over the network, and one session could then approve and promote its own branch.

### Approve a product spec

A product's page on the **Products** screen shows **Approve spec** when the working spec has no approval yet, or differs from the approved one. The page asks you to confirm. The question names the product, the spec digest the page read, the output model, and the approval it replaces, when there is one. After you confirm, the server runs `rocky product approve <name> --expect-digest <digest>`, with the digest the page showed.

The approval covers exactly that spec. If the spec on disk changed after the page read it, the command refuses with `approval-digest-mismatch`, names both digests, and writes nothing. Read the spec again and approve the digest you mean. The result also says when the spec was already approved and nothing was written. The engine refuses a new spec revision while fulfillment work is in flight. The page shows that refusal.

The approver is recorded with its channel, as `rocky review --approve` records it. The approval stores `<server git email> (http_api)`. A terminal approval stores the bare email. The label names the channel and the server's git identity. It does not prove who was at the page. If the server cannot read `git config user.email`, or it is literally `unknown`, the job fails with `approver_identity_unresolved`. Nothing is written in that case, and the snapshot file is not created either.

### Compact tables

The **Estate** screen has a **Compact** panel. Compact runs `OPTIMIZE` (it rewrites small files) and `VACUUM` (it removes stale data files) on Delta tables. It works on Databricks only. It takes two steps, so you see what runs before it runs.

1. Choose **One table** and type its full name, `catalog.schema.table`, or choose **Every managed table in a catalog** and type the catalog. Press **Plan compact**. The server runs `rocky compact <table>` or `rocky compact --catalog <name>`. The command writes a plan file and runs no statement. For a replication pipeline, a catalog plan asks the source adapter to discover tables. The page shows the number of statements, the table count, the target file size and every table. The statements are under **The statements**. If the command could not save the plan, the page says so and offers no apply.
2. Press **Compact**. The confirm text names every table. After you confirm, the page applies the plan by its id, through the same apply as the Review screen, so what runs is the plan you saw. Apply uses this server's warehouse credentials, and Rocky cannot undo it.

The page never passes `--dry-run`. The command's `--dry-run` prints "no statements executed", but it still writes a plan that apply can run. The plan step is the preview. The plan step takes no write slot.

Archive is different: it deletes rows and then vacuums with no retention window. The Compact panel has no archive option. See [What the UI cannot do](#what-the-ui-cannot-do).

### Cancel a running job

While a job runs, operator mode shows a **Cancel** button under it. A read-only page does not show it. The button calls `POST /api/v1/jobs/{id}/cancel`:

```
Cancel ─▶ SIGTERM to the job's process group (handled like Ctrl-C) ─▶ the button becomes "Force stop"
Force stop ─▶ SIGKILL to the job's process group                ─▶ the job stops at once
```

The engine answers only once the signal reached the job. The job then ends `cancelled`. If it exits 0 anyway, it ends `succeeded`. A job that exited before the signal reached it answers `409 job_not_running`. A cancelled job keeps the write slot until its process has exited.

What a cancelled job leaves:

- **A run or apply of a replication pipeline, after Cancel**, stops like Ctrl-C in a terminal. It starts no new table copies and lets the copies in flight finish. It saves their watermarks, marks the other tables `Interrupted`, and exits `130`. Run history shows it as a partial failure. `rocky run --resume-latest` copies the rest.
- **Any other job, after Cancel** (models, a plan, an approval), stops at once. Rocky has no clean-stop step for these yet ([#1606](https://github.com/rocky-data/rocky/issues/1606)). A warehouse statement in flight is cut, and the warehouse keeps or drops it whole. A state write in flight is all or nothing. Models that finished stay built. Run the job again to finish.
- **After Force stop**, any job stops like a crash. An incremental replication table can have its rows landed before its watermark was saved. On DuckDB, Databricks, Snowflake and BigQuery, the next run of that table reads the watermark back from the target table first, so it does not copy the same rows twice. On other warehouses, prefer Cancel.

A plan file cut part-way is refused at apply.

Cancel reaches only jobs this server process started. A scheduled run (`--scheduler`) answers `409 job_not_cancellable`. On Windows there is no SIGTERM, so Cancel kills the job at once. Stopping `rocky serve` with Ctrl-C or `SIGTERM` sends each running job the same SIGTERM as Cancel, and waits up to 5 seconds for it to be sent.

## What the UI cannot do

- **With a read-only token, it cannot write.** A read-only token gets `403 forbidden_read_only_token` on `POST /api/v1/jobs/run` and every other token-checked write.
- **It cannot apply or run an archive plan.** Archive deletes rows and then vacuums with no retention window, so it cannot be undone. The plan screen shows no Apply button for an archive plan, and the server answers `409 archive_apply_terminal_only` to `POST /api/v1/jobs/apply` for one. `rocky archive` stays in a terminal. Apply the plan there with `rocky apply <plan-id>`.
- **It cannot sign a branch approval.** Sign it in a terminal with `rocky branch approve <branch>`.
- **It cannot know the spec digest for you.** A product-bound plan applies only with a digest you type, and the digest must come from the approval.
- **It cannot change policy.** The Governance screens report decisions, and `/api/v1/policy` reads the rules back. The rules live in the `[policy]` block of `rocky.toml`. `rocky policy freeze` and `rocky policy unfreeze` are the CLI's only policy writes.

## How the server protects the page

With `--ui`, the server adds checks that a plain `rocky serve` does not run:

- `--ui` refuses to start on a non-loopback host without a token, and refuses a full-scope token there. See [Open the UI](#open-the-ui).
- A request whose `Host` is not a loopback name, the bind host, or an `--allowed-host` entry gets `421 host_not_allowed`. This defends against DNS rebinding, where an attacker's domain is made to resolve to `127.0.0.1`. The check also runs on a loopback `rocky serve` without `--ui`. `GET /api/v1/health` skips this check, so a load balancer probe still works.
- A request whose `Origin` is neither the server's own nor an `--allowed-origin` entry gets `403 origin_not_allowed`. The check reads the origin's host, not the whole origin, so an `http` or `https` origin on an allowed host passes whatever its port. On a loopback server that includes `http://localhost:5173`, a local dev server. `GET /api/v1/health` skips this check too.
- Every UI file response carries a Content Security Policy. The page loads scripts and fonts from this server only, and styles from this server or inline. Nothing may frame it.
- `--ui --scheduler` refuses to start without `ROCKY_WEBHOOK_SECRET`, because a browser can reach the webhook route.

Behind a reverse proxy, name the proxy host with `--allowed-host`:

```bash
rocky serve --ui --host 0.0.0.0 --token "$TOKEN" --read-only \
  --allowed-host rocky.internal
```

The [`rocky serve` reference](/reference/commands/development/#rocky-serve) lists every flag. The [Embedding guide](/guides/embedding/) covers the HTTP API.
