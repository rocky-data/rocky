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
   │                   (worker profile: read + compile/test + draft tools only)
   ├─ runner re-verifies from disk ─▶ governed propose ─▶ you: rocky review <plan> --approve
   └─ digest-gated apply ─▶ observe (tests + freshness)
```

Every stop prints the state, why the loop stopped, and the next command. Exit codes: `0` clean stop (including a waiting ask and `observing`) · `2` blocked · `3` parked at `applying_unknown` for a human. `1` stays the generic command-error code, so a script can tell a parked receipt from a crashed command.

---

## `rocky fulfill <product>`

Advance the product's state machine.

```bash
rocky fulfill revenue_daily
```

The loop trusts nothing it did not verify itself:

- The drafting agent runs in its own process group on a worker-profile MCP server. It can read, compile, test, and draft models. It cannot touch contracts, metadata, proposals, reviews, or schedules. The whole group is killed when the task ends, so helpers and accidental stragglers do not outlive the task. A process that puts itself in a new session with `setsid` leaves the group and is beyond any process-group kill — that is part of the hostile-local-agent residual below, not a covered case.
- After drafting, every spec-owned artifact is byte-verified against the committed lowering manifest. Drift means `blocked` — the loop names the tampered file.
- The plan reaches the review queue only through the engine's one governed propose path, as the `agent` principal, under your `[policy]`.
- The apply recomputes the spec digest from the approved snapshot and passes `--expect-spec-digest`. The engine refuses a mismatch even if the loop did not.
- Only a `Succeeded` outcome is ever journaled as applied. An apply deflected as already-running keeps waiting. A resumed crash asks the idempotency store for an authoritative receipt; a backend that cannot answer leaves the state for a human, never a blind retry.

`--retry` re-enters a `blocked` product after you fix the printed remedy.

Two invocations never fight: every state write is a compare-and-swap, and a loop that finds a live owner prints its pid and exits. A crashed owner is taken over automatically — a dead pid is detected by its start time, so a recycled pid never counts as alive.

### JSON output

`rocky fulfill <product> --output json` emits one `FulfillOutput` document: `state`, `message`, `next_command`, `spec_digest`, and the pinned `plan_id` while one is in flight.

## `rocky fulfill approve-spec <product>`

Approve the current candidate spec. This is the same authority transition as [`rocky product approve`](/reference/commands/products/#rocky-product-approve) — one implementation, two spellings. The snapshot file is written first, immutable and digest-addressed; then one state-store transaction records the approval, moves the loop state, and appends the journal row. A second approver racing you fails cleanly and is shown the winning digest.

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

## What v0 does not defend

The worker runs on the same machine as the runner and the review markers, and markers are unsigned. The gates defend against mistakes, prompt-injection-shaped drift, and tool misuse — not against a hostile local process acting as your user. Do not point the driver at an agent binary you do not trust. Signed approvals and OS sandboxing are named follow-up work.

Two limits have their own tracking issues. A descendant that puts itself in a new session with `setsid` leaves the process group, is re-parented by the operating system, and survives the group kill; OS-level sandboxing is the fix ([#1491](https://github.com/rocky-data/rocky/issues/1491)). Rocky opens committed files with `O_NOFOLLOW` and creates them with `O_EXCL`, but it does not use directory-relative system calls, so a directory component swapped between the check and the open stays a window. `O_NOFOLLOW` is a Unix flag; on Windows one backup read follows a link.

The full boundary, and how it applies to any agent rather than just this loop, is set out in [Operating Rocky with agents](/concepts/operating-rocky-with-agents/), "What the three gates do not defend against".

## Known issue: a repair round is reported as tampering

A red first verification sends the loop into a repair round. That repair rewrites the merged sidecar file, which is exactly what it is meant to do. The integrity check that follows compares the file against the hash recorded **before** the repair, so the loop reports its own authorized write as tampering and moves the product to `blocked`.

This fails closed. The product stops at `blocked` and the apply never runs, so nothing reaches your warehouse. It does mean that a product whose first verification is red cannot finish today, which is the case repair exists for. Tracked in [#1493](https://github.com/rocky-data/rocky/issues/1493).
