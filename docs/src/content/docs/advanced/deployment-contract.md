---
title: Deployment Contract
description: "What happens to a running Rocky when a pod dies, when two overlap, and when a rollout replaces one with another, per state backend. The one rule the self-host surface rests on, and the loss windows the engine does not hide."
sidebar:
  order: 6
---

This page is the contract between Rocky and the machine that runs it. It states what the engine does when the process dies, when two processes overlap, and when a rollout replaces one with another. Each statement names the mechanism behind it. Where the engine cannot promise something, the page says so.

The engine parts this page is about:

| Part | What it is | Where it lives |
|---|---|---|
| The state store | One redb file per project: watermarks, run history, the ledgers, and the scheduler's own cursors and claims | `models/.rocky-state.redb` on the project's volume |
| The scheduler | The resident loop inside `rocky serve --scheduler`, or one `rocky tick` per timer fire | The same volume: `.rocky/tick.lock`, `.rocky/pending-demands/` |
| Remote state | A copy of the syncable tables on a backend (`s3`, `gcs`, `valkey`, `tiered`), for a project that runs on more than one machine over time | `[state] backend` in `rocky.toml` |

## The one rule

**One scheduler per project, one replica, replaced in place, on a persistent volume.** In Kubernetes terms: `replicas: 1`, `strategy: Recreate`, a persistent volume claim. In Compose terms: one service, never scaled.

The rule follows from one fact: the scheduler's state never leaves its volume. The state store keeps a set of tables that every sync backend strips from every upload, because they describe one machine: the reconciler's cursors and claims (`schedule_state`, `schedule_claims`), the HTTP job records (`jobs`), the fulfillment loop's state (`fulfill_state`), the product approvals (`product_approvals`), and the schema cache (`schema_cache`, unless you set `[cache.schemas] replicate = true`). A second machine that pulls the same remote state inherits the watermarks and none of the scheduler's memory. Two schedulers are two independent cursors over one set of pipelines, and both fire what is due.

```
                one volume                          two volumes
  ┌──────────────────────────┐          ┌────────────┐      ┌────────────┐
  │ scheduler A   scheduler B│          │ scheduler A│      │ scheduler B│
  │      │  tick.lock  │     │          │  cursor A  │      │  cursor B  │
  │      └──── one ────┘     │          └─────┬──────┘      └─────┬──────┘
  │      wins, one skips     │                │  both fire         │
  └──────────────────────────┘                ▼                    ▼
    contention avoided, once                the warehouse, twice
```

## When the process dies

**Same volume, restarted.** The new process finds the cursors, the claims and the spool where the old one left them, and resumes.

- A cron pipeline with occurrences missed while the process was down follows its `catchup` setting: `latest` (the default) fires one demand at the most recent missed occurrence; `skip` advances the anchor and runs nothing. There is no `all`; runs are watermark-driven, so replaying every occurrence would cost compute and load no extra data.
- A webhook the old process accepted (answered `202`) is on disk, `fsync`'d before the `202` was sent, and the new process consumes it.
- A scheduled run that was in flight is a child process. If it outlived the reconciler, its run record is honored when the new process reads the store. If it died too, the demand it was claimed for is finalized as a failure and not retried. That is the spool's documented loss window: at most once, never twice.

**Fresh volume.** An ephemeral volume, or a rollout that lost the claim, starts the scheduler with no memory.

- A cron pipeline records its anchor at first sight and does not fire. Nothing missed is replayed.
- An `after` pipeline waits for its upstream to succeed again.
- A freshness pipeline measures its own run-staleness from the run history it can see. With no history, its budget reads as exceeded, and it fires once its throttle allows.
- An accepted webhook that had not been consumed is gone with the volume.

This is why the rule says persistent volume. The state backend does not change it: remote state carries watermarks and history, and never the cursors.

## When two processes overlap

**Same volume, block storage.** Two schedulers on one volume contend and one runs.

- Every tick takes a non-blocking advisory lock on `.rocky/tick.lock` and refreshes a heartbeat file while it holds it. The loser skips with `tick_in_progress`. The lock avoids contention; it is not the correctness boundary. A wedged holder never releases a kernel lock, so correctness lives in the claim records, which a lock cannot bypass.
- Writers to the state store serialise on `<store>.redb.lock`. Read-only opens skip that lock; they contend only on redb's own file lock, briefly, and a read that loses that contention retries a few times before it reports the store as busy. Inside one `rocky serve` the reads queue on one permit, so its own readers never contend with each other; two processes still can.

**Same volume, network storage.** The two locks above are `flock` calls. Rocky has not probed `flock` on NFS or another network filesystem, so "shared volume" on this page means block storage: one node, or a volume that moves whole with the pod.

**Separate volumes.** Both fire. The state backend decides only how loud the collision is:

| `[state] concurrency_control` | Backend | What the loser's run does at the end |
|---|---|---|
| `"off"` (default) | any | Uploads unconditionally. Last writer wins, silently: the other run's watermarks are overwritten. |
| `"cas"` | `s3`, `gcs`, `tiered` | Uploads only if the remote still carries the generation this run downloaded. The loser fails closed: a nonzero exit and an error naming the race, and the winner's state stands. |
| `"cas"` | `valkey` | No generation to compare against; falls back to an unconditional upload and warns once at the start of the run. |
| `"cas"` | `local` | No remote write at all; the file on the volume is the state. |

`cas` turns a silent overwrite into a loud failure. It does not stop the second run from happening; only the rule above does.

## When a rollout replaces the process

A rolling update runs the old pod and the new pod at the same time, on separate volumes or on one. On one volume it is the overlap above, one winner per tick, with the new pod contending until the old one drains. On separate volumes it is two schedulers for the length of the rollout. Neither is the rule. Use `Recreate`: stop the old pod, let it drain, start the new one on the same volume.

The drain: `SIGTERM` makes `rocky serve` stop accepting connections, finish in-flight requests, and wait for a running scheduled child up to `--drain-timeout-seconds` (default 60). The pod's `terminationGracePeriodSeconds`, or the container's stop grace, must sit above that number, or the child is killed mid-run and its occurrence is recorded as a failure.

Closing the tiered-backend seams tracked as issue 1242 does not make a rolling update safe. The double fire lives in the scheduler tables that never sync, not in the sync.

## Mixed versions during an upgrade

The state store carries a schema version. Two directions:

- **A newer engine opens an older store.** It migrates the store forward on first open. Every command does this; there is no separate migration step.
- **An older engine opens a newer store.** `rocky serve` and every inspection command (`state`, `history`, `doctor`, `metrics`, the branch commands) refuse to open it. `rocky run` and `rocky load` follow `[state] on_schema_mismatch`: the default, `recreate`, logs one warning, starts from a fresh local state, runs once as a full refresh, and never writes that downgraded state back to a shared backend; `fail` refuses like the rest.

So a fleet mid-upgrade is safe in one direction only. Upgrade every process that shares a volume or a remote backend together, old stopped before new started, and keep a copy of the store from before the upgrade: rolling back with the history intact means restoring that copy under the older engine. The changelog names every release that changes the schema version.

## Webhooks and timers

**Webhook ingress reaches the scheduler's volume.** `POST /api/v1/hooks/trigger/{pipeline}` is accepted by writing the demand into `.rocky/pending-demands/` on the volume the reconciler reads, and duplicates are detected by a hard link on that same volume. A webhook delivered to a pod that mounts a different volume is accepted there and consumed by nobody. Route ingress to the scheduler's pod; with one replica that is the only pod.

**`rocky tick` from a timer.** A CronJob or a systemd timer that runs `rocky tick` is the scheduler without the resident process. Every tick pod must mount the same volume: the tick lock and the cursors are on it. Two timers on two volumes are two schedulers.

## Per backend

| Backend | Syncs | Never syncs | Overlap on separate volumes |
|---|---|---|---|
| `local` | nothing; the file is the state | everything | not applicable: there is one volume by definition |
| `s3`, `gcs` | watermarks, run history, the artifact, policy and idempotency ledgers | the local-only set above | both fire; `cas` makes the loser fail closed |
| `valkey` | the same tables, to the cache | the local-only set | both fire; `cas` is not available, last writer wins |
| `tiered` | the same tables, to the durable tier, with the cache kept coherent under `cas` | the local-only set | both fire; `cas` makes the loser fail closed |

The persistent volume is required on every row. The backend chooses the blast radius of a collision. It never removes the rule.

## What the engine does not promise

- A webhook demand claimed by a reconciler that then dies, whose child also dies before recording a run, is lost. At most once.
- A scheduled run killed by a stop grace shorter than the drain is recorded as a failure, not retried.
- Two schedulers on two volumes both run. No backend prevents it; `cas` only makes the second one fail at the end.
- `flock` on network storage is unprobed.

## Related pages

- [Run the Container Image](/guides/run-the-image/): the image, `docker run`, and the minikube example that follows this contract.
- [State management](/concepts/state-management/#remote-state-persistence): the backends and the sync lifecycle.
- [Configuration](/reference/configuration/#concurrent-writers): `concurrency_control`, `on_schema_mismatch`, `catchup`.
- [Failure modes](/advanced/failure-modes/): what a failed run looks like from the outside.
