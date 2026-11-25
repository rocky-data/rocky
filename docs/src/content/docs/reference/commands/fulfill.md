---
title: Fulfill Commands
description: The fulfillment loop — drive a product spec from elicitation to an applied plan, one gated step at a time
sidebar:
  order: 8
---

`rocky fulfill` is the loop half of spec fulfillment (EXPERIMENTAL). The [product verbs](/reference/commands/products/) parse, verify, lower, and approve — deterministically. `rocky fulfill` drives them: it dispatches an untrusted drafting agent, re-verifies everything the agent touched, hands the result to the engine's governed propose, waits for the approval marker your `rocky review --approve` writes, and applies with the digest gate. One invocation advances as far as it can without you, then stops and tells you the exact next command.

```
rocky fulfill <product>
   │
   ├─ no spec yet ──▶ agent drafts a candidate ──▶ you: rocky fulfill approve-spec <product>
   ├─ spec approved ─▶ verify posture ─▶ lower contract ─▶ agent drafts SQL
   │                   (worker profile: read + compile/test + draft_model only)
   ├─ runner re-verifies from disk ─▶ governed propose ─▶ you: rocky review <plan> --approve
   ├─ digest-gated apply ─▶ observe (tests + freshness + the declared data checks)
   └─ a declared check FAILS on the live table ─▶ observed_failing ─▶ repair round
                                               ─▶ a NEW plan ─▶ you approve again
```

Every stop prints the state, why the loop stopped, and the next command. Exit codes: `0` clean stop (including a waiting ask and `observing`) · `2` blocked · `3` parked at `applying_unknown` for a human · `4` `observed_failing` — the plan applied, and the applied output is failing a check the product declared about itself. `1` stays the generic command-error code, so a script can tell a parked receipt from a crashed command.

---

## `rocky fulfill <product>`

Advance the product's state machine.

```bash
rocky fulfill <product> [--retry]
```

| Argument or flag | Type | Default | Description |
|---|---|---|---|
| `product` | `string` | **(required)** | Product name, read from `products/<name>.toml`. |
| `--retry` | `bool` | `false` | Re-enter a `blocked` product after you fix the printed remedy. |

Pass `--state-path` with a store the worker cannot write: the loop refuses the default store. See [What v0 does not defend](#what-v0-does-not-defend). Example: `rocky --state-path .rocky/state.redb fulfill revenue_daily`.

The loop trusts nothing it did not verify itself:

- The drafting agent runs in its own process group on a worker-profile MCP server. That server offers it read, compile, test, and `draft_model`. It offers no tool for contracts, metadata, data checks, proposals, reviews, or schedules. That is a closed tool route, not a limit on the agent. The agent is an ordinary process in your project directory, so one that can write files can write those files directly. The later gates are what catch that, not the tool list. The agent's last `draft_model` call is its hand-off: the tool mirrors what it wrote into the task outbox, and when the agent is gone the runner commits `models/<model>.sql` and `models/<model>.toml` only if they still match that hand-off byte for byte. Anything else that edits those two files inside the drafting window — a person, a second tool — makes the round refuse with the file named, rather than committing bytes nobody handed off. The whole group is killed when the task ends, so helpers and accidental stragglers do not outlive the task. A process that puts itself in a new session with `setsid` leaves the group and is beyond any process-group kill — that is part of the hostile-local-agent residual below, not a covered case.
- After drafting, every spec-owned artifact is byte-verified against the committed lowering manifest. Drift means `blocked` — the loop names the tampered file. The manifest covers the files the lowering emits: the contract and the model sidecar. It does not cover `models/test_definitions.toml`, which a `[[use_test]]` entry resolves against. The check-set comparison described below covers that.
- The plan reaches the review queue only through the engine's one governed propose path, as the `agent` principal, under your `[policy]`.
- The apply recomputes the spec digest from the approved snapshot and passes `--expect-spec-digest`. The engine refuses a mismatch even if the loop did not.
- Only a `Succeeded` outcome is ever journaled as applied. An apply deflected as already-running keeps waiting. A resumed crash asks the idempotency store for an authoritative receipt; a backend that cannot answer leaves the state for a human, never a blind retry.


### When the applied output is wrong

The checks your spec declares about its output (the grain, the `checks` list, the non-null columns) need a table to run against. They cannot run before the apply, so the loop reports them deferred at verify and runs them at observation.

A check that fails there is different from every other failure: the wrong data is already live. The loop does not roll it back. It does this:

1. It records `observed_failing`, never a healthy-looking `observing`, and prints which check failed and what it measured.
2. On your next run, it reads the checks again. A reading that has gone green releases the product. Nothing trusts the stored verdict.
3. If a check still fails, it spends one repair round: the drafting agent gets the failing check and its actual values, and rewrites the model.
4. It proposes the repaired model as a new plan with a new id. You review and approve it like the first. The earlier marker cannot be reused, and a bare apply of the repaired plan refuses.

Repair rounds are bounded. Repeated failures land `blocked`, naming the check that would not go green.

Two things report and do not route: staleness (usually a scheduling fact, which rewriting SQL cannot fix) and warning-severity checks (declared as not disqualifying). Both are printed and journaled, and neither starts a repair. A check the loop cannot evaluate (its SQL will not run, or a column is missing) did not pass: the loop holds at `applied` and says so.

Before it runs the checks, the loop compares them with the set it verified for this plan. The comparison uses what the model loader produces, so an edited `[[use_test]]` entry in `models/test_definitions.toml` is covered. It also covers the table the checks run against, and the warehouse is fixed at that moment, so a later edit to `rocky.toml` cannot move the query. A set that does not match is not run: the loop holds and tells you to restore what changed.

This catches a change made after the plan was verified. A check already on disk at that point is part of the verified set, including one an agent wrote. Verification pins the check set. It does not judge it. Read the model's sidecar to know what will run.

Two invocations never fight: every state write is a compare-and-swap, and a loop that finds a live owner prints its pid and exits. A crashed owner is taken over automatically — a dead pid is detected by its start time, so a recycled pid never counts as alive.

### JSON output

`rocky fulfill <product> --output json` emits one `FulfillOutput` document: `state`, `message`, `next_command`, `spec_digest`, and the pinned `plan_id` while one is in flight.

## `rocky fulfill approve-spec <product>`

Approve the current candidate spec. Pass the same `--state-path` as the loop, or the command refuses.

```bash
rocky --state-path .rocky/state.redb fulfill approve-spec revenue_daily
```

This is the same authority transition as [`rocky product approve`](/reference/commands/products/#rocky-product-approve) — one implementation, two spellings. The snapshot file is written first, immutable and digest-addressed; then one state-store transaction records the approval, moves the loop state, and appends the journal row. A second approver racing you fails cleanly and is shown the winning digest.

## Configuration

The loop reads one block in `rocky.toml`:

```toml
[fulfill]
# briefs_dir = "briefs"   # optional overrides for the agent task briefs

[fulfill.driver]
type = "subprocess"                       # or "replay"
command = ["claude", "-p", "{brief}"]     # your agent command; {brief} is replaced
env_allow = ["ANTHROPIC_API_KEY"]         # the worker sees ONLY these variables
timeout_seconds = 900
kill_grace_seconds = 30
```

Bring your own model: the command template is the whole integration. `type = "replay"` executes a recorded session file instead — deterministic and credential-free, which is how CI exercises the loop.

### Wrapping the worker in an OS sandbox

Put the sandbox program first in `command`. Rocky starts `command[0]` as given and checks only that `command` is not empty and that exactly one argument contains `{brief}`. The wrapper becomes the leader of the worker's process group.

```toml
[fulfill.driver]
type = "subprocess"
command = ["/usr/bin/sandbox-exec", "-f", "/etc/rocky/agent.sb", "claude", "-p", "{brief}"]
env_allow = ["ANTHROPIC_API_KEY", "PATH"]    # the worker starts with an empty environment
```

On Linux, `["/usr/bin/unshare", "--pid", "--fork", "claude", "-p", "{brief}"]` has the same shape. The sandbox profile is yours to set up. Rocky does not require a wrapper, check that it works, or provide OS-level containment. A descendant that leaves the group (for example with `setsid`) can outlive the group kill unless your sandbox stops it. See [#1491](https://github.com/rocky-data/rocky/issues/1491).

## What v0 does not defend

The worker runs on the same machine as the runner and the review markers, and markers are unsigned. The gates defend against mistakes, prompt-injection-shaped drift and tool misuse, not against a hostile local process acting as your user. Do not point the driver at an agent binary you do not trust. Signed approvals and OS sandboxing are named follow-up work. The full boundary, for any agent, is in [Operating Rocky with agents](/concepts/operating-rocky-with-agents/), "What the three gates do not defend against".

**The outbox.** `.rocky/fulfillment/<product>/outbox/` is the one part of that directory the worker is meant to write: elicitation writes `candidate_spec.toml` and optionally `questions.json` there, and `draft_model` mirrors its draft there. The loop deletes and recreates `outbox/` before every dispatch, so a worker that may write only that directory needs an inherited default (a umask or a parent ACL), not a one-off `chmod`. The loop reads the outbox only after the worker exits and its process group is killed.

Every file the loop takes from the outbox, and every `briefs_dir` override, is read the same way. On Unix the read refuses a symlink at the file and a file with a second hard link, so a link planted to a file outside the project is never copied into `products/`, `models/` or the worker's prompt. Crash recovery restores a `.ff-prev` backup under the same rules and publishes the bytes it read through a new file.

The loop checks a digest on the elicitation candidate before writing `products/<product>.toml`. With `type = "subprocess"` the loop computes that digest itself from the candidate it read after the worker exited, so it records which bytes were accepted and proves nothing about what the worker meant. With `type = "replay"` the digest comes from the recorded session, so it is a real check.

**Keep these closed to the principal that runs the worker.** An unsigned marker with a matching plan id is accepted as approval, so the claims above hold only while the worker cannot write:

| Path | Why |
|---|---|
| `.rocky/plans/` | Review markers. |
| `.rocky/fulfillment/<product>/approved-<digest>.toml` | The approved-spec snapshot the apply digest gate reads. |
| `.rocky/fulfillment/<product>/` except `outbox/` | Task transcripts, the committed lowering manifest, and its staged and backup copies (`phase-staging.json`, `lowering-manifest.json.ff-staged`, `lowering-manifest.json.ff-prev`). The contract's and sidecar's `.ff-staged` / `.ff-prev` copies sit in `models/`, inside the repair window below. |
| The state store (`--state-path`) | The loop's compare-and-swap state, its journal and its approval record. |
| `products/<product>.toml` | The working spec the next `approve-spec` snapshots. |
| The `briefs_dir` overrides | Trusted configuration that becomes the worker's instructions. |
| `rocky.toml` | The runner reloads it on every invocation and spawns `[fulfill.driver].command` from it. `[policy]` and `[state]` come from it too. |

**The state store must sit outside every directory the worker can write.** The default store lives inside `models/` (`models/.rocky-state.redb`), so `rocky fulfill` and `rocky fulfill approve-spec` refuse to run on it. They also refuse a namespaced store under `models/.rocky-state/`, or a path that resolves into `models/` through `..` or a symlink. Pass `--state-path` with a location the worker cannot write. `.rocky/state.redb` is safe only if the worker cannot write `.rocky/`.

Pass the same `--state-path` to every `rocky fulfill`, `rocky fulfill approve-spec` and `rocky review` call. Every next step the loop prints carries it. `rocky fulfill approve-spec` without it refuses. `rocky review` and `rocky product approve` do not check where the store is, so without the flag an approval recorded in the default store never reaches the loop.

The worker's own `rocky mcp --profile worker` records its `draft_model` policy decisions in a state store, so give it the same `--state-path`. With `type = "replay"` the loop starts the worker as `rocky --state-path <store> mcp --profile worker`. With `type = "subprocess"`, add the flag before `mcp` in your agent's MCP config: `"args": ["--state-path", ".rocky/state.redb", "mcp", "--profile", "worker"]`. Without it, those decisions are missing from the custody chain and the brief. With a remote state backend (`s3`, `gcs`, `tiered`) and a `[policy]` block, the worker never writes the shared ledger. It records each draft decision in the local store only. The loop's `propose` reads those rows before it downloads remote state, then publishes them with its own decision in one compare-and-swap step, so the worker needs read-only state credentials at most. `propose` publishes only rows with the shape of a draft decision (`draft:` and the other draft prefixes, an `agent` `propose` evaluation of a compiled model), after its own verdict, and never replaces a row the shared ledger holds. A forged row cannot change the propose verdict.

A worker that can also write anything in the closed list is the hostile-local-process case, and nothing here defends against it. If your worker and your runner share a user, treat that as the concession it is.

Two limits have their own tracking issues. A descendant that puts itself in a new session with `setsid` leaves the process group and survives the group kill. OS-level sandboxing is the fix ([#1491](https://github.com/rocky-data/rocky/issues/1491)). Rocky opens committed files with `O_NOFOLLOW` and creates them with `O_EXCL`, but uses no directory-relative system calls, so a directory component swapped between the check and the open stays a window. Both flags and the hard-link check are Unix only: on Windows the backup, outbox and brief-override reads follow a link and do not count hard links.

## Repair rounds

A red verification sends the loop back to the agent for a repair round. The repair rewrites the merged sidecar file, as intended. If the next verification is green the loop carries on to propose. A repair that leaves it red is retried up to the repair budget, and a product that exhausts the budget stops at `blocked` with the last failure printed. The loop does not report its own repair as tampering.

The loop authorizes that write. Before it dispatches a repair worker it re-checks **every** hash the committed manifest records. Drift had no authorized writer, so it is tamper: the product moves to `blocked` and nothing is rewritten. Only when every file verifies does the loop demote the manifest to its contract-only phase, which returns the sidecar to the writable set. The next merge re-records the hashes from what it merged. Only the commit protocol writes hashes.

- **The window is real.** Between the repair dispatch and the merge that closes it, the sidecar is covered by no hash, and the merge preserves keys and `[[tests]]` entries the lowering does not own. Content added during the window survives into the committed artifact. Using it needs a process that can write your models directory, which is the same access that can forge an approval marker. Trusted handling of the repair agent's output bytes is tracked in [#1515](https://github.com/rocky-data/rocky/issues/1515).
- **Do not run `rocky product compile` against a product a loop is driving.** It would re-merge the previous round's sidecar, so it refuses whenever the fulfillment record carries an owner or worker-group stamp. Stop the loop, or let it reach its next stop. A loop killed outright leaves its stamp behind. `rocky fulfill <product>` takes that record over by checking the owner's start time.

Only `rocky fulfill <product>` demotes a merged generation to its contract-only phase. `rocky product compile` only lowers a first contract for a product with no committed manifest. The engine refuses any other caller that does not hold the loop's own compare-and-swapped record.
