---
title: Product Commands
description: Spec-driven data products — verify the trust posture, lower a spec onto engine primitives, approve a revision, and read product status
sidebar:
  order: 7
---

A product spec is one TOML file at `products/<name>.toml`. It declares WHAT a data product must be: its grain, columns, checks, freshness, and classifications. The spec adds no new runtime machinery. Every field lowers onto something the engine already enforces — a contract, declarative tests, sidecar metadata, or the policy posture. A field that cannot lower is refused at parse time, never shimmed. Two fields are also read from the spec itself by `rocky run`. The delivery deadline (below) lowers to nothing. The volume range (below) lowers to a test and is also checked after each production build.

The verbs on this page are the deterministic half of spec fulfillment. They parse, verify, lower, and approve. The agent loop that drafts SQL is [`rocky fulfill`](/reference/commands/fulfill/), and it is gated by the same policy plane as every other agent action.

```
products/<name>.toml ──verify──▶ posture + tags + collisions
        │
        ├──approve──▶ immutable snapshot + approval record (one transaction)
        │
        └──compile──▶ Phase A: models/<model>.contract.toml
                      Phase B: metadata merged into models/<model>.toml
                      (staged writes; the manifest rename commits)
```

### Delivery deadline

A product can say when its output must arrive. Add two optional keys to `[product]`:

```toml
[product]
name = "revenue_daily"
intent = "Daily gross revenue per client in EUR"
deliver_by = "0 6 * * *"     # a 5-field cron expression; each occurrence is a deadline
timezone = "Europe/Lisbon"   # IANA name; defaults to UTC
```

The parser checks `deliver_by` with the scheduler's own cron parser (`deliver-by-unparseable`). It refuses an unknown timezone (`timezone-unknown`) and a `timezone` without `deliver_by` (`timezone-without-deliver-by`).

Each production `rocky run` that builds the product's output model records the deadline that build served. A build covers the first deadline at or after it finished. If an earlier deadline was still open when it finished, the build serves that one instead, and it is late. A late build also covers every deadline that passed before it finished, so it reports one lateness, not one per missed deadline. A previous build with no recorded deadline is no baseline, so the first build after a spec gains `deliver_by` is judged like a first delivery. Lateness is wall-clock time from the deadline to the moment the model finished. Shadow and branch runs record no deadline. `rocky run` reads `products/` under the directory of the `rocky.toml` it runs.

The scheduler status (`GET /api/v1/schedule`) derives a `latest_start` from each deadline for every scheduled pipeline that feeds it. It warns when a cron starts too late. See [the scheduler status](/guides/running-without-an-orchestrator/#the-resident-scheduler-rocky-serve---scheduler-experimental).

### Volume range

A product can say how many rows its output table should hold after a load. Add an optional table to `[product.output]`:

```toml
[product.output.volume]
min_rows = 1_000         # optional, inclusive
max_rows = 5_000_000     # optional, inclusive; at least one bound is required
severity = "warning"     # "warning" (the default) or "error"
```

The count is the table's total rows (`SELECT COUNT(*)`), not the rows one load wrote. So the output model must be a full-refresh table. A total on an incremental, merge or partitioned table only grows, and a view stores no rows.

The parser refuses a block with no bound (`volume-missing-bound`). `min_rows = 0` alone counts as a bound. It also refuses `min_rows` above `max_rows` (`volume-min-above-max`), a severity other than `warning` or `error` (`volume-severity-unknown`), an unknown key (`unknown-key`), and a negative or non-integer bound (`invalid-value`). The spec does not name the model's strategy, so `rocky product verify` checks it (`volume-strategy-unsupported`, below).

The range lowers to one `row_count_range` test in the model sidecar, with the same bounds and severity. It replaces any `row_count_range` test the worker wrote, so the model has one range: the spec's. `rocky test --declarative` and `rocky run` then judge the same range.

After each successful production build of the output model, `rocky run` counts the rows once, on the same warehouse connection:

```
build ok ──▶ COUNT(*) ──▶ in range ────────────▶ in_range
                │      ├─▶ out of range, warning ─▶ a `volume_warnings` line; delivery as usual
                │      └─▶ out of range, error ───▶ the model is marked failed
                └─ count failed or cancelled ──────▶ not_evaluated (never a failure)
```

At `error` severity the table is already written; Rocky cannot undo that. The model is marked failed after the fact. Downstream models are withheld as for any failed model. The run status is `partial_failure`. The build gets no `due_at`, so it never counts as a delivery on time. A non-full-refresh output reaching `rocky run` is `not_evaluated` with `volume-strategy-unsupported` in its reason. Shadow and branch runs count nothing. A backfill (`rocky apply` of a backfill plan) counts nothing either.

The result is the `volume` object on the model's entry in `materializations`: `product`, `model`, `row_count`, `min_rows`, `max_rows`, `severity`, `verdict` (`in_range`, `below_min`, `above_max`, `not_evaluated`) and `reason`. The run records `row_count` in its history, and `rocky product status` reports it. When the model's `[target] table` differs from the model name, the history entry is named by the table, so `rocky history --model` takes the table name; `rocky product status` finds the count by the model name.

---

## `rocky product verify`

Check that a product may be fulfilled, without writing anything.

```bash
rocky product verify revenue_daily
```

Four checks run in order, fail-closed:

1. **Trust posture.** `rocky.toml` must carry the frozen `propose_only` posture: a `[policy]` block with `default_agent_effect = "require_review"`, an agent `propose` rule that resolves `allow` and is scoped to exactly this product's output model (no `any`, no glob, no extra predicates, no autonomy budget), and agent `apply` resolving `require_review` or `deny`. A policy that merely *happens* to resolve safely is rejected — authority that reaches past the product is global agent authority. The verdicts come from the engine's own policy evaluator, run against the attributes the lowering is *about to create* (the post-image), so the gate cannot be evaded by the change under review.
2. **Classification tags.** Every tag in `output.classifications` must resolve to a `[mask]` strategy, a `[mask.<env>]` override, or `[classifications].allow_unmasked`. This is an error here; plain `rocky compile` only warns (W004). Resolution only: whether masking is applied is warehouse-dependent.
3. **Identity collisions.** The product name must not be claimed by another spec file, and the output model must not be claimed by another product.
4. **Volume strategy.** When the spec declares `[product.output.volume]` and the model exists (`models/<model>.sql` or `models/<model>.rocky`, plus `models/<model>.toml`), the model must be a full-refresh table (`volume-strategy-unsupported`). The strategy is read the way the compiler reads it, including `_defaults.toml`. Before the model is drafted, verify cannot judge the strategy, so this check passes; the Phase-B compile runs it again. If a model that is not a full-refresh table still reaches `rocky run`, the run does not count it: the verdict is `not_evaluated` with reason `volume-strategy-unsupported`, never a failure.

Exit codes: `0` pass · `1` needs input (a paste-ready `[policy]` block is printed) · `2` fail.

### JSON output

`ProductVerifyOutput`: `status` (`pass` | `needs_input` | `fail`), `reason`, `paste_block`, and the resolved `propose_effect` / `apply_effect`.

---

## `rocky product compile`

Verify, then lower the spec. Refuses unless `verify` passes.

```bash
rocky product compile revenue_daily
```

Lowering runs in two phases because the drafting tool rewrites the model sidecar wholesale — metadata lowered before drafting would be destroyed:

- **Phase A** (before drafting) renders `models/<model>.contract.toml` — the spec-owned contract the draft must compile against.
- **Phase B** (after the drafted sidecar exists) merges the spec-owned metadata into `models/<model>.toml`: sources, the product tag, classifications, freshness, and the generated tests (grain uniqueness, `not_null` per non-nullable column, one `expression` test per check, one `row_count_range` for a declared volume). The worker's `name`, `intent`, and appended tests survive, except a worker `row_count_range` with no `filter` when the spec declares a volume. A filtered worker range survives. The generated range carries `generated_by = "product.output.volume"`, so removing the volume block removes that range on the next lowering; `rocky test` ignores the key.

The command picks the phase itself: Phase A on a fresh product, Phase B once `models/<model>.toml` exists. Re-running is safe — the merge is idempotent.

Every generation commits through staged same-directory writes, journaled, with the lowering manifest renamed **last** as the commit marker. The next run recovers from the journal. If the marker landed, the new generation stands and the staging files are cleaned up. If it did not, the previous generation is restored exactly. The journal is treated as untrusted input during recovery — forged, traversing, symlinked, or foreign entries are refused before anything mutates.

Recovery reads the marker first. If it cannot read it at all — a permission fault, a link swapped in — it changes nothing and fails with `commit-io`, because rolling back there would restore backups over a commit that may have succeeded.

If the marker says the generation **committed**, recovery finishes the job: it deletes each `.ff-staged` and `.ff-prev` sibling and keeps the new files. It does not inspect them, so an orphan `.ff-prev` is removed rather than questioned.

If the marker says it did **not** commit, recovery restores the previous generation, and refuses instead of guessing in two cases. Each keeps the journal, so the next run asks the same question:

- A `.ff-prev` backup exists that the journal does not record. Rocky will not restore a backup it did not create, and fails with `commit-unexpected-backup` naming the file to inspect.
- A new file is in place that recovery cannot read to identify. It is left alone, with `commit-io`.

A restore reads every recorded `.ff-prev` backup before it changes anything. It writes the bytes it read to a new file, syncs it, and renames that file over the final. It never renames the backup itself into place. On Unix a backup that is a symlink or has a second hard link is refused with `commit-io`, and nothing is restored. A crash during a restore leaves the backup and the journal, so the next run repeats it.

On Unix, `product compile` also refuses a directory it writes into that is itself a symlink, for example `models -> models_real`, with `commit-io` ("taking directory custody for the commit failed"). This holds even when the link points inside the project. Replace the link with the real directory.

If an approval exists, compile re-verifies the snapshot's bytes against the approval digest before doing anything. A mismatch is tamper, and nothing proceeds.

### JSON output

`ProductCompileOutput`: the committed `phase`, each artifact with its `sha256`, the `manifest_path`, and the approval echo (`spec_matches_approval` is `false` when the working spec has moved past the approved revision — not an error; the loop treats it as supersession).

---

## `rocky product approve`

Record a human's approval of the current spec revision.

```bash
rocky product approve revenue_daily
rocky product approve revenue_daily --expect-digest sha256:<hex>
```

With `--expect-digest`, the command approves the working spec only if its digest is exactly the one given (as `rocky product status` prints it). A different digest refuses with `approval-digest-mismatch` before the store is opened, and nothing is written. Use it to approve exactly the spec you read. Without the flag, the spec on disk when the command runs is approved. The browser UI always passes it.

Approval is one authority transition, in a fixed order:

1. The approved bytes are written to `.rocky/fulfillment/<name>/approved-<digest-hex>.toml` — immutable and digest-addressed. A digest-named file is never overwritten, so no reader can ever observe new-digest-old-bytes.
2. One state-store transaction records the approval, moves the product's fulfillment state to `spec_approved`, and appends the journal row. All or nothing.

A crash between step 1 and step 2 leaves only an orphan snapshot file — harmless; the next approve completes over it. Approving the already-approved digest is a no-op. When two people race, the second approve loses its compare-and-swap cleanly and prints the winning digest.

The approver identity is the same best-effort git identity the review marker records. It is an attribution, not an authentication.

The command reads the channel from `ROCKY_SESSION_SOURCE`, as `rocky review --approve` does. In a terminal, `approver` is the bare email. In a job that `rocky serve` started (the browser UI, or `POST /api/v1/jobs/product_approve`), `ROCKY_SESSION_SOURCE=http_api` is set and `approver` is `<email> (http_api)`. That job refuses with `approver_identity_unresolved`, and writes nothing, when the git identity cannot be read or is literally `unknown`. The label names the channel, not the person.

### JSON output

`ProductApproveOutput`: `spec_digest`, `approver`, `approved_at`, `snapshot_path`, `previous_state`, and `already_approved`.

---

## `rocky product status`

Read-only report. Writes nothing, recovers nothing.

```bash
rocky product status revenue_daily
```

Reports: the spec's identity (or its parse error), the committed lowering phase, byte-verification of every committed artifact against the manifest, whether a crashed commit's journal is pending (the next `compile` resolves it), the approval record with snapshot integrity, whether the working spec still matches the approved revision, and the persisted fulfillment state with its journal row count.

When the spec declares `deliver_by`, status also reports `delivery`:

| Field | Meaning |
|---|---|
| `on_time`, `late_by_seconds` | Whether the latest production delivery met the deadline it served, and by how much it missed. `null` until a run records a deadline. |
| `last_due_at`, `last_delivered_at` | That deadline, and when the output model finished. |
| `next_due_at` | The deadline the next delivery must meet. A time in the past means it is already late. |
| `at_risk` | `true` when the p90 duration is longer than the time left before `next_due_at`. `null` when the estimate is `unknown`. |
| `estimate` | `state` (`known` or `unknown`), `n`, `window` (20), `p50_ms`, `p90_ms`, `growth`. |

The estimate times each of the last 20 successful production deliveries from run start to the output model's finish. With fewer than 5, it is `unknown` and gives no verdict. From 10 runs on, when the newest 5 all ran slower than the p90 of the older ones, `growth` is `true` and `p90_ms` is the slowest of those 5.

When the spec declares `[product.output.volume]`, status also reports `volume`:

| Field | Meaning |
|---|---|
| `min_rows`, `max_rows`, `severity` | The declared range and severity. |
| `row_count`, `counted_at`, `run_id` | The newest production build that recorded a count, whatever its status. A build an `error` breach marked failed still shows its count. Absent until a build is counted. |
| `verdict` | That count judged against the range the spec declares now: `in_range`, `below_min`, `above_max`, or `not_evaluated` when there is no count. |

### JSON output

`ProductStatusOutput` — every field above, machine-readable.

---

## `rocky product list`

Read-only. One row per product the project knows, sorted by name.

```bash
rocky product list
rocky product list --output json
```

A product is listed when either is true: `products/<name>.toml` exists, or the state store holds a fulfillment or approval record under that name. So a product whose spec file was deleted still appears, with `spec_present = false` and the loader's reason in `spec_error`. A project with no `products/` directory and no records lists nothing and exits `0`.

Each row is a projection of `rocky product status <name>`: spec identity or its error, the committed lowering phase, the count of artifact byte-verification problems, whether a staging journal is pending, the approval record, whether the working spec matches the approved revision, the fulfillment state, and the journal row count. The two commands are built by the same code, so a row can never disagree with the status it summarises.

The same payload is served over HTTP by `rocky serve` at `GET /api/v1/products`, and one product's status at `GET /api/v1/products/{name}`. The server and the CLI both read `products/` under the directory of the `rocky.toml` they use. With the default `--config rocky.toml`, that is the working directory. The server reads the state store named by `--state-path`, or the default under its own models directory, namespaced when `--state-namespace` or the config's `[state] namespacing` resolves one, the same derivation every command makes. So a server started with the namespace the CLI uses answers from the store the CLI wrote. Both reads open the store read-only: a store at the current schema version is never written, an older one is migrated forward as every read command does, and a store written by a newer engine is refused.

### JSON output

`ProductListOutput` — `products` (one `ProductListEntry` each) and `count`.

---

## `rocky product journal`

Read-only. A product's fulfillment journal: every persisted transition, in append order.

```bash
rocky product journal revenue_daily
rocky product journal revenue_daily --output json
```

Each row is the loop's own record of one transition: its sequence number, when it happened, what happened in plain words (`spec approved`, …), the state before and after, and the spec digest, plan id and idempotency key involved when the event concerns one. The rows are read through the same store function the fulfillment loop reads through, so what the command shows is what the loop sees. `rocky product status` reports only the count of these rows.

A product is known by the same rule `status` and `list` use: `products/<name>.toml` exists, or the state store holds a fulfillment or approval record under the name. A known product with no rows prints an empty journal and exits `0`. A product the project does not know exits `1`, because an empty journal for a typo would read as "nothing happened".

The same payload is served over HTTP by `rocky serve` at `GET /api/v1/products/{name}/journal`. An unknown name answers `404 product_not_found`.

### JSON output

`ProductJournalOutput` — `product`, `product_id`, `rows` (one `ProductJournalEntry` each) and `count`.

---

## Validation codes

`rocky validate` checks `products/` offline with its own code band:

| Code | Severity | Meaning |
|------|----------|---------|
| `V050` | ok | Every spec in `products/` parsed. |
| `V051` | error | A spec fails the strict parser; the message carries the parser's stable reject code. |
| `V052` | error | A spec's `product.name` disagrees with its file name. |
| `V053` | error | Two specs claim the same output model. |
