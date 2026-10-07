# ADR-TRUST — Who is acting, what makes an approval valid, and where the trust boundaries are

**Status:** Proposed — awaiting ratification

**Work package:** WP-00 (credential containment), WP-04 (governance semantics), WP-07 (editor), WP-08 (supply chain)
**Closes (once ratified and implemented):** the design half of audit findings **RD-026** (secret-bearing CI runs mutable PR code), **RD-035** (local principal and approval controls are not a hostile-agent boundary), **RD-043** (the editor has no workspace-trust gate) and **RD-045** (mutable or unverified downloads). Answers ADR-CONTRACTS Open question E (break-glass for a refused promote).
**Siblings:** ADR-CONTRACTS decides what the promote gate refuses. This ADR decides who may override a refusal, and how. ADR-STATE-SESSION and ADR-CONCURRENCY own the durability of the state ledger that this ADR writes audit rows to.

> Scope boundary. This ADR owns identity and authority: who a principal is, how Rocky knows it, what an approval proves, and which process may write what. It does not decide policy rules (which capability a principal may use on which model). The policy evaluator in `rocky-core/src/policy.rs` stays as it is. This ADR changes the inputs it trusts.

---

## Context

### The problem

Rocky lets agents draft and apply changes to a warehouse. A gate that stops an agent must know two facts it cannot learn today:

1. **Who is acting.** Every identity Rocky records is self-asserted. Any process can claim any name.
2. **Who approved.** Every approval Rocky checks is an unkeyed hash or a parsed file. Anyone who can write the file can make a valid one.

So the gates defend against mistakes, prompt-injection drift and tool misuse. They do not defend against a hostile process with write access to the project. The code already says so in two places. The ADR makes that boundary a decision, and says what moves it.

### How a principal is established today

A principal has two parts. The **class** is the enforcement axis. The **id** is a label.

| Part | Type | Set by | Verified? |
|---|---|---|---|
| Class | `PolicyPrincipal { Human, Agent }` (`rocky-core/src/config.rs`) | `--principal`, then `ROCKY_PRINCIPAL` (`engine/rocky/src/main.rs::resolve_cli_principal`) | No |
| Id | `PrincipalRef { id, source }` (`config.rs::PrincipalRef::resolve`) | `--principal-id`, then `ROCKY_PRINCIPAL_ID`, then `mcp-<profile>`, then `unnamed` (`main.rs::resolve_cli_principal_id`) | No. Every output shows `verified: false`. |

Facts that matter for the decisions below:

- **The CLI class defaults to `human`.** `ROCKY_PRINCIPAL=agent` raises it. An explicit `--principal human` lowers it again, with a stderr warning only (`resolve_cli_principal`). This is the downgrade RD-035 names.
- **The enforced class is the stricter of two.** `PersistedPlan::enforcement_principal` (`rocky-cli/src/plan_store.rs`) takes the stricter of the runtime class and the plan kind's default (`default_principal_for_kind`: `AiAuthored` and `Backfill` are `agent`). The stored `principal` field on a plan is display only. It is not read by a gate, because "an unkeyed hash is attacker-recomputable" (the doc comment on `enforcement_principal`).
- **The id is never an enforcement input.** `PrincipalId`'s doc says "no gate reads it, and nothing verifies it". `PrincipalIdSource` has four values (`Flag`, `Env`, `McpProfile`, `Default`). Its doc says signed sources (a local key, CI OIDC) come later, and that a new variant bumps the state schema.
- **A plan records no author id.** `PersistedPlan` has `principal: Option<PolicyPrincipal>` (a class) and no `PrincipalRef`. So nothing today can check "the approver is not the author".
- **Decision rows carry the id.** `PolicyDecisionRecord::principal_ref` (`rocky-core/src/state.rs`) holds an optional `PrincipalRef`.
- **The MCP server has profiles, not identities.** `McpProfile { Default, Approver, Worker }` (`rocky-mcp/src/tools.rs`). The `actor` field doc says the gate still evaluates the `agent` class. `Approver` adds the approve action; its doc says approval "is still attributed to the operator's git identity, never to a verified human".

### What an approval is today

Rocky has three approval surfaces. They grew separately and prove different things.

| Surface | Written by | Stored at | Bound to | Integrity | Approver | Expiry |
|---|---|---|---|---|---|---|
| Plan review marker | `rocky review <plan> --approve` (`commands/review.rs::write_review_marker`) | `.rocky/plans/<plan_id>.reviewed.json` | `plan_id`, which is blake3 of `{kind, payload}` (`plan_store.rs::compute_plan_id`, re-checked in `read_plan`) | None on the marker. Apply checks it parses and names the plan (`review.rs::review_marker_state`). | Git identity, or `unknown` | None |
| Branch approval | `rocky branch approve` (`commands/branch.rs::run_branch_approve`) | `./.rocky/approvals/<branch>/<id>.json`, relative to the working directory (`approvals_dir_for_branch`) | `branch_state_hash`: branch name, schema prefix, config bytes and the `<config_dir>/models` tree (`branch.rs::compute_branch_state_hash`) | blake3 over canonical JSON, unkeyed (`branch.rs::sign_artifact`, `verify_signature`; `SignatureAlgorithm::Blake3CanonicalJson` in `output.rs`) | Git email, name and host (`ApproverIdentity`) | `max_age_seconds`, default 86400 |
| Product approval | `rocky product approve` (`commands/product.rs`) | Ledger row `ProductApprovalRecord` (`rocky-core/src/fulfill.rs`), plus an immutable snapshot file | `spec_digest` (sha256 of the snapshot) | The ledger write is a compare-and-swap. The snapshot is re-hashed on read. | Free-text string, "an honesty caveat, not an authentication claim" | None |

The MCP server adds one keyed check. `review_token_parts` (`rocky-mcp/src/tools.rs`) computes `blake3::keyed_hash` with a random per-process key. It proves that a confirm call saw the same review as the dry run. It is not an approval, and the key dies with the process.

Two defects follow from this table:

- **`min_approvers` counts files, not people.** `run_approval_gate` (`branch.rs`) compares `approvals_used.len()` with `min_approvers`. One person who writes two artifacts meets `min_approvers = 2`.
- **`ApproverSource` has three values, and only one is used.** `Local`, `CiOidc`, `Pat` exist (`output.rs`). Its doc says only `Local` is emitted.

### What bypasses exist today

`rocky branch promote` already has four ways past a gate. None needs any authority beyond running the command.

| Bypass | Where | Recorded as |
|---|---|---|
| `--skip-approval` | `main.rs`, `Command::Branch` promote args | `AuditEventKind::ApprovalSkipped` |
| `ROCKY_BRANCH_APPROVAL_SKIP` | `branch.rs::APPROVAL_SKIP_ENV` | `ApprovalSkipped`, with the env value as reason |
| `--allow-breaking` | `main.rs`, promote args | `BreakingChangesAllowed` |
| Gate could not run (fail open) | `branch.rs::run_breaking_change_gate_for_plan` | `BreakingChangesGateSkipped` |

The `AuditEvent` doc (`output.rs`) says these are "routed to stdout JSON only in v1; persistent audit storage is a follow-up". So an override leaves no durable record.

### What the trust boundary is today

The code concedes the hostile local process in two places:

- `rocky-fulfill/src/lib.rs` (v0 threat model). Markers are unsigned, so "a hostile local process could forge one". The worker profile removes the approval tools. The subprocess driver kills the worker's process group at task end. "The residual — a worker that persists code outside its process group, or any hostile same-user process — is not defended in v0."
- `rocky-cli/src/plan_store.rs` (trust boundary, #1943). `.rocky/plans/` is a trusted input. Whoever can write it can already write models and config, which decide more.

The worker's MCP surface is an explicit allowlist (`tools.rs::WORKER_PROFILE_TOOLS`: twelve read, compile, test and `draft_model` tools). The allowlist doc says a worker with a file writer can still write a sidecar, because the driver runs with no filesystem confinement (#1491, #1515).

### What the CI and editor boundaries are today

- **CI.** `.github/SECURITY_ENVIRONMENTS.md` puts the model API key in two GitHub environments, `credentialed-pr-review` and `credentialed-ci`. Each needs a reviewer and has no administrator bypass. The file says that with one maintainer, "self-review cannot be prevented. The approval is a deliberate pause before secrets are released, not a second person." Whether the repository settings match this file is not visible from the tree. Not verified.
- **Editor.** `editors/vscode` has no `isTrusted` check and no `untrustedWorkspaces` capability (a search of `editors/vscode/src` and `package.json` finds neither). This is RD-043 as filed.
- **Downloads.** The audit cited `scripts/vendor_rocky.sh` for RD-045. That script is not in `scripts/` on `main`. The other RD-045 sites (workflow installers) were not re-traced. Not verified.

---

## Decision

### 1. Who is a principal

A principal is one actor: a person, an agent, or a CI job. It has a **class** and an **id**. The id has a **source**, and the source decides whether the id is verified.

```
  source               verified?   may be enforced on?
  ──────               ─────────   ───────────────────
  flag / env           no          display and audit only
  mcp_profile          no          display and audit only
  default (unnamed)    no          display and audit only
  local_key  (new)     yes         yes; labelled "local key, weaker"
  ci_oidc    (new)     yes         yes
  serve_token (later)  yes         yes (named API tokens)
```

- **Human.** A person proves identity with a local Ed25519 key. The key's public half is in the identity root (§2.3). The private half never sits in the project tree.
- **CI job.** A CI job proves identity with an OIDC token (a signed statement from the CI provider that names the repository, workflow and ref). Rocky records the job and the repository. `ApproverSource::CiOidc` becomes a real, emitted value.
- **Agent.** An agent is a class, not a credential. An agent may hold a verified id only through CI OIDC (ruled 2026-10-04: OIDC is required for agent approvals; local keys serve solo and dev setups and are labelled weaker). An agent running locally stays unverified.

Rules:

- **A gate may read an id only when its source is verified.** An unverified id stays a label, as today.
- **The class stays self-asserted on the CLI.** Rocky cannot stop a process from unsetting `ROCKY_PRINCIPAL`. So `--principal human` over `ROCKY_PRINCIPAL=agent` stays a warned downgrade. What changes is what a downgrade buys: any action that needs a human approval needs a **verified** human signature (§2). A self-declared human class satisfies nothing that an agent could not.
- **The MCP server is always `agent` class.** No profile can raise it to `human`.
- **How a verified id reaches a gate.** Rocky does not verify the person running a command. A verified id enters a gate in one of two ways only: (a) inside a signed approval record that the gate checks, or (b) through an OIDC token presented at invocation in CI. The id of the person running `rocky apply` stays unverified unless one of these carries it. So a human-only action is gated by a signed approval, not by who typed the command.
- **A plan records its author.** `PersistedPlan` gains an author `PrincipalRef`. The field stays outside the `plan_id` digest, so it is a claim. Its use is the self-approval check in §2.2, where a forged author can only make an approval *harder* to accept, never easier (see §2.2).

### 2. What an approval is

An approval is a signed statement: *principal P approves content C, for gate G, until time T.* All three surfaces in the Context table move onto this one model. Their storage can stay where it is.

#### 2.1 What the approval binds

| Surface | Content digest it signs |
|---|---|
| Plan review | `plan_id` (blake3 of kind and payload), plus the base ref and the breaking-change count the reviewer saw |
| Branch approval | `branch_state_hash`, plus the base and head commit ids |
| Product approval | `spec_digest` |

The signature is a **detached Ed25519 signature** over the canonical JSON of the approval record. It is stored beside the record, not inside it. A new `SignatureAlgorithm` variant carries it. The record names the signer's key id or OIDC subject, never an email (privacy: the current `ApproverIdentity.email` is dropped from the signed record; git identity may stay as an unsigned display field).

#### 2.2 What makes an approval valid

An approval is valid only when **all** of these hold. Each failure is a distinct refusal reason, extending today's `RejectedApproval.reason` values.

1. The signature verifies against a key in the identity root (`bad_signature`).
2. The key is not revoked (`revoked_key`, with the key id).
3. The signed digest equals the current content digest (`state_hash_mismatch`, as today).
4. The approval is not expired, and not dated in the future (`expired`, as today). Plan review markers gain an expiry; today they have none.
5. The signer is allowed for this gate (`signer_not_allowed`). `allowed_signers` lists key ids and OIDC subjects, not emails.
6. **The signer is not the author** (`self_approval`) when the plan is agent-authored (by class or by kind). A worker never holds a key, so it cannot sign at all. This rule also stops an agent with an OIDC identity from approving its own plan.
7. **For an agent-authored plan, the signer is human class with a verified id.** An agent OIDC identity may approve only where policy names it.
8. `min_approvers` counts **distinct signers**, not files.

Why a forged author field cannot help an attacker: rule 6 compares the signer with the author and refuses on a match. Changing the author to someone else removes a refusal only for an approval that is otherwise valid, which still needs a key the attacker does not have. Rule 7 keys off the enforced class (`enforcement_principal`), not the stored field.

#### 2.3 Key custody and the identity root

- **Private keys** live outside the project tree, in the user's config directory or a hardware key. Rocky refuses to load a private key from inside the project root.
- **The identity root** is the list of public keys and OIDC issuers that every signature is checked against. Where it lives is Open question A.
- **Revocation** removes a key from the root. An approval signed by a removed key is refused (rule 2), even if it was signed before removal.
- **Rotation** adds the new key, re-signs open approvals, then removes the old key.

#### 2.4 A missing or bad signature refuses

Ruled 2026-10-07: a bad or missing signature **refuses**. A warning on an authority check is fail-open.

The one exception is a **migration window for existing blake3 approvals**:

- Default: refuse. A project opts in to the window with an explicit setting (for example `[trust] legacy_blake3 = "warn"`).
- In the window, a valid blake3 branch approval or an unsigned review marker is accepted with a warning that names it. Each one leaves the window when it is re-signed. `rocky review --approve`, `rocky branch approve` and `rocky product approve` gain a re-sign path.
- The window never covers a new approval. Every approval written after the upgrade is signed.
- The setting is removed at a set release. See Open question C.

How big the window is, stated plainly. Branch approvals expire after 24 hours by default (`BranchApprovalConfig::max_age_seconds`), so most expire on their own. The window matters for projects with a long `max_age_seconds`, and for review markers and product approvals, which never expire today.

#### 2.5 Approvals may stay in the checkout

RD-035 asks to keep signed approvals outside the agent-writable checkout. With signatures, the location of the **record** no longer decides integrity: a forged record fails rule 1 without the key. What must stay outside the worker's reach is the **private key**. So:

- Approval records stay in `.rocky/` and in the ledger.
- `approvals_dir_for_branch` resolves against the project root, not the working directory.
- The ruling of 2026-09-17 on `.rocky/plans/` (a trusted input, #1943) stands. Signing adds attribution and tamper evidence. It does not add containment. See Open question F.

### 3. Trust boundaries

Three parties act on a project. Each has a fixed set of things it may write.

```
  ┌───────────────────────────── one machine, one OS user (v0) ─────────────────────────────┐
  │                                                                                          │
  │  OPERATOR (human)            TRUSTED LOOP (rocky fulfill runner)    UNTRUSTED WORKER      │
  │  holds: signing key          holds: warehouse credentials           holds: nothing        │
  │  writes: approvals,          writes: plans, ledger rows,            writes: outbox,       │
  │          config, trust root          confined model re-write                models/<m>.*  │
  │  reads: everything           reads: everything                      reads: grounding,     │
  │                                                                            state (r/o)    │
  └──────────────────────────────────────────────────────────────────────────────────────────┘
        signature check ──▶ gate ◀── policy (class + verified id)
```

**The worker** (an agent process under `rocky mcp --profile worker`, or the subprocess the fulfillment driver runs):

- May write: its task outbox, and `models/<model>.sql` and `.toml` through `draft_model`.
- May read: the grounding tools on the worker allowlist, and the governance state **read-only**. Governance state means policy decision rows, approvals, product approval records and fulfillment records. A worker tool never writes them. A cache (for example the schema cache that `compile` may use through `state_path()`) is not governance state and may stay writable. Which allowlisted tools write the ledger today, and which tables, was not traced. The implementing change traces each one and adds a test per tool.
- Never holds: a signing key, warehouse write credentials, an OIDC token that can approve.
- Never reaches: approve, propose, schedule, or any tool not on the allowlist (unchanged).

**The trusted loop** writes plans and ledger rows, and re-writes model files only from a verified hand-off (`rocky-fulfill/src/handoff.rs`). It holds warehouse credentials. It does not hold a human's signing key, so it cannot approve on a human's behalf.

**The operator** holds the key and is the only party that can make a human approval.

**What a signature does not stop.** On one machine under one OS user, a hostile worker can read any file the operator can read. If the operator's private key is a readable file, the worker can sign. Signatures move the boundary only when the key is out of the worker's reach: a different OS user, a hardware key, or CI OIDC. Until an OS sandbox for the worker exists (#1491), Rocky documentation must not describe a local agent setup as safe against a hostile agent. The supported claim stays the v0 one: the gates defend against mistakes, prompt-injection drift and tool misuse. CI OIDC approvals are the first setup where the stronger claim holds.

**The editor.** An untrusted VS Code workspace starts no `rocky` process and runs no warehouse command. The extension declares limited support for untrusted workspaces: syntax highlighting and snippets only. Logged argument vectors are structured and redacted. This closes the design half of RD-043.

**CI.** Two decisions:

- The GitHub environment gate in `.github/SECURITY_ENVIRONMENTS.md` is ratified as written: a reviewer gate with no administrator bypass, branch-restricted to `main`. On a one-maintainer repository it is a deliberate pause, not a second person. RD-026 closes when the repository settings match that file and one live run on `main` confirms it.
- CI and release jobs execute only pinned, integrity-checked downloads. A release publishes checksums and a signature made with the release job's CI OIDC identity. RD-045 closes against a fresh trace of every download site, because the audit's cited script is gone.

### 4. Break-glass

Break-glass is a signed approval that overrides one refused gate. It answers ADR-CONTRACTS Open question E with that ADR's Option 2: the gate ships fail-closed first, and this mechanism is the only exception.

**Who may break glass.** A human-class principal with a verified id that is listed in a separate `break_glass_signers` set in the identity root. Never an agent class. Never the worker profile. Never an unverified id. A CI OIDC identity may not break glass.

**What it bypasses.** Exactly one named gate, for exactly one content digest:

| May bypass | May never bypass |
|---|---|
| Too few branch approvals | A policy `deny` |
| A breaking-change finding | An active freeze |
| A breaking-change gate that could not run (ADR-CONTRACTS §7) | Non-authoritative state (`StateAuthority::Indeterminate`, ADR-AUTHORITY) |
| | The plan integrity check (`read_plan`) |
| | Signature validity itself |

**How it is recorded.** The break-glass record holds: the signer, the gate, the refusal reason text the gate printed, the content digest, the base and head commit ids, and the time. Rocky writes it to the durable state ledger **before** the action runs. If that write fails, the action fails. The stdout-only `AuditEvent` is not a record.

**Lifetime.** Single use (bound to one digest) and short-lived. The default expiry is one hour.

**What it replaces.** `--skip-approval`, `ROCKY_BRANCH_APPROVAL_SKIP` and `--allow-breaking` are removed. The fail-open `BreakingChangesGateSkipped` path becomes a refusal (ADR-CONTRACTS §7). Before 2.0 there is no compatibility shim. Each removal gets a changelog entry that names the break-glass command that replaces it.

### 5. Already ruled, recorded here

- **A `Run` capability** (ruled 2026-10-06): deferred, because no trigger tool exists. When one is built, it is a new `PolicyCapability::Run` variant, never a reuse of `Apply`. A test that "a deny must deny a trigger" lands first. The trust model above applies to it unchanged.
- **An empty touched set** (ruled 2026-10-06): `PolicyGate::Allow` on an empty set is correct for a plan apply. Only an apply-shaped caller may reach it. On `main` as of this draft, `apply.rs` returns `Err(PolicyGate::Allow)` for an empty `touched` set with no check on the caller's shape. The ruled guard is not present at that site.

---

## Open questions for ratification

**A. Where does the identity root live?**

- *Option 1 — a `[trust]` section in `rocky.toml`.* Simple and reviewable. But the worker can write `rocky.toml`, so it could add its own key.
- *Option 2 — a file outside the project* (user or system config). The worker cannot reach it under a separate OS user. But it does not travel with the repository, and CI needs its own copy.
- *Option 3 — `[trust]` in `rocky.toml`, checked against the base commit.* A signature is verified against the root **as it is at the base ref**, not the working tree. A change to the root is itself a change that needs a signature from a key in the old root. This is the same "trust the base, read the candidate as data" pattern the credential-containment check uses for `.github`.

**Recommendation: Option 3.** It keeps one source in the repository and stops a worker from approving with a key it added.

**B. How does an OIDC-backed approval verify offline?**

A custody bundle must verify on a clean machine. An OIDC token is checked against the issuer's published keys, which rotate and need the network.

- *Option 1 — bundle the issuer key snapshot* with the approval. Verifies offline. Trusts the snapshot.
- *Option 2 — CI signs with a short-lived Ed25519 key*, and the OIDC token certifies that key once. The bundle carries the key, the token and the issuer key snapshot.
- *Option 3 — require the network* to verify OIDC approvals.

**Recommendation: Option 2.** One signature format for all approvals, and offline verification.

**C. When does the blake3 migration window end?**

The ruling says each legacy approval leaves the window when it is re-signed. It does not say when the setting itself goes.

- *Option 1 — remove the setting one minor release after signed approvals ship.*
- *Option 2 — remove it at 2.0.*
- *Option 3 — keep it until a project has no legacy approvals left.* The project's own state decides.

**Recommendation: Option 1.** Before 2.0 there is no back-compat promise, and a long window keeps a fail-open path alive.

**D. Does "signer is not the author" apply to human-authored plans?**

- *Option 1 — always.* Strongest. Blocks a solo maintainer from approving their own plan.
- *Option 2 — agent-authored plans only (as decided in §2.2), with an opt-in for human-authored plans* (for example `require_distinct_approver = true`).

**Recommendation: Option 2.** It matches the one-maintainer posture already ratified for CI environments.

**E. Do local-key approvals satisfy an agent-authored plan's gate?**

The 2026-10-04 ruling requires OIDC for *agent* approvals, and labels local keys weaker. It does not say whether a *human's* local-key approval of an agent's plan is enough.

- *Option 1 — yes, labelled "local key" in the record and in `rocky audit`.*
- *Option 2 — only when `[trust]` allows local keys for that gate.*

**Recommendation: Option 1.** The solo setup needs it, and the label keeps it honest.

**F. Revisit the 2026-09-17 ruling that `.rocky/plans/` is trusted?**

- *Option 1 — keep it.* Signatures bind the approval. The plan file stays as trusted as models and config.
- *Option 2 — distrust the plan file* and re-derive promote SQL at apply.

**Recommendation: Option 1.** A worker that can write a plan can also write models and config. Signing does not change that. An OS sandbox would.

---

## Consequences

### What changes

- `rocky-core/src/config.rs`: `PrincipalIdSource` gains `LocalKey` and `CiOidc`. A `[trust]` section holds the identity root, revocations, `allowed_signers` and `break_glass_signers`.
- `rocky-cli/src/plan_store.rs`: `PersistedPlan` gains an author `PrincipalRef`.
- `rocky-cli/src/commands/branch.rs`, `review.rs`, `product.rs`: one approval model with a detached Ed25519 signature. `evaluate_artifact` gains the rules in §2.2. `min_approvers` counts distinct signers. `approvals_dir_for_branch` resolves against the project root.
- `rocky-cli/src/output.rs`: a new `SignatureAlgorithm` variant. `ApproverSource::CiOidc` is emitted. New `RejectedApproval` reasons. A break-glass record type.
- `--skip-approval`, `ROCKY_BRANCH_APPROVAL_SKIP` and `--allow-breaking` are removed (breaking CLI change).
- `rocky-mcp`: the worker profile opens the ledger read-only.
- `editors/vscode`: a workspace-trust gate and redacted argv logging.
- A direct Ed25519 dependency. `ring` is in `Cargo.lock` only as a transitive dependency today.

### Migration and compatibility

- **No back-compat shim before 2.0.** The migration window in §2.4 is the only exception, and it has an end.
- **State schema bump.** A new `PrincipalIdSource` variant and the break-glass ledger row both change meaning for an older binary. The `PrincipalIdSource` doc already requires a bump.
- **Codegen cascade.** `ApprovalArtifact`, `ApprovalSignature`, `SignatureAlgorithm`, `ApproverSource` and `AuditEvent` derive `JsonSchema` in `output.rs`. Changes need `just codegen` and `just regen-fixtures`.
- **Privacy.** Signed records name a key id or OIDC subject, not an email.

### What it does and does NOT close

- **Closes, once implemented:** RD-035 (the boundary is stated and moved where a key is out of reach), the design half of RD-026, RD-043 and RD-045.
- **Does NOT close:**
  - **A hostile same-user process.** Not defended until the worker runs under a separate OS identity or sandbox (#1491). §3 says so.
  - **RD-026 closure.** It needs the repository settings to match `SECURITY_ENVIRONMENTS.md` and a live run. That is a settings task, not code.
  - **RD-045 closure.** It needs a fresh trace of every download site.
  - **Per-principal budgets.** This ADR gives them a verified id to key on. The budget rules are separate work.

### What it unblocks

- **Signed approvals** for plan review, branch promote and product approval.
- **CI identity through OIDC**, including agent approvals in CI.
- **Break-glass for a refused promote** (ADR-CONTRACTS Open question E).
- **Per-principal budgets** (cost, rows, reach), keyed on a verified id.
- **Run-trigger gating**, when a trigger tool exists.
- **An offline-verifiable custody bundle.**
- **Named API tokens** for the HTTP API, as a third verified source.
- **The hostile-agent boundary** documentation (WP-04).

---

## Alternatives considered

| Alternative | Why rejected |
|---|---|
| **Warn on a missing or bad signature** | Ruled out 2026-10-07. A warning on an authority check is fail-open. |
| **Keep blake3 and add a secret key (HMAC)** | Every verifier would need the secret, so every verifier could sign. It also cannot verify offline without sharing the secret. |
| **Make `--principal human` refuse when `ROCKY_PRINCIPAL=agent`** | A process can unset the variable. The check would look like a boundary and not be one. Requiring a verified signature for human-only actions is the real fix. |
| **Move approvals outside the checkout** (RD-035's suggested fix) | With signatures, the record's location does not decide integrity. The key's location does. Moving records would break the CI and repository workflows that read them. |
| **Keep `--skip-approval` and `--allow-breaking` with a durable audit row** | Anyone who can run the command can still bypass. An audit row after the fact does not stop the write. |
| **Use git commit signatures as approvals** | They sign a commit, not a plan, a branch state or a spec digest. And an approval is not always a commit. |
| **Sigstore keyless signing for everything** | Needs the network to sign and a public log. Kept as a possible OIDC backend under Open question B, not the only path. |

---

## Validation

Every assertion must fail with the fix reverted (`scripts/mutation-check.sh`, per `AGENT_REVIEW.md`).

**Principals (§1)**
- An unverified id never changes a gate result. The same plan under two different `--principal-id` values gets the same verdict.
- `--principal human` under `ROCKY_PRINCIPAL=agent` cannot satisfy a human-approval gate without a verified human signature.
- The MCP server's class is `agent` under every profile.

**Approvals (§2)**
- An agent approves its own plan ⇒ refused `self_approval`. The record names the agent.
- A plan edited after signing ⇒ refused at apply.
- A signature from a revoked key ⇒ refused, with the key id.
- Two approvals by one signer with `min_approvers = 2` ⇒ refused.
- An unsigned review marker with the migration window off ⇒ refused. With it on ⇒ accepted with a warning naming the marker.
- A private key inside the project root ⇒ refused to load.
- A key added to `[trust]` in the working tree but not at the base ref ⇒ its signature is refused (if Open question A takes Option 3).
- A CI job approves through OIDC ⇒ the record names the job and the repository.

**Trust boundaries (§3)**
- From the worker profile: no tool writes the ledger, writes an approval, or reaches a non-allowlisted tool.
- An untrusted VS Code workspace starts no `rocky` process.
- A secret-like argument is redacted in the extension's output channel.
- A tampered download is rejected by checksum in each CI and release download site.

**Break-glass (§4)**
- A break-glass by an agent class, an unverified id, or a signer not in `break_glass_signers` ⇒ refused.
- A break-glass over a policy `deny`, a freeze, or `Indeterminate` state ⇒ refused.
- A valid break-glass over a gate that could not run ⇒ proceeds, and the ledger row holds the refusal text and both commit ids.
- A ledger write failure during break-glass ⇒ the action fails.
- A break-glass record reused for a second digest ⇒ refused.
