# Credentialed workflow environments

Rocky's model API key must be stored only as an environment secret. Do not
define `ANTHROPIC_API_KEY` as a repository or organization secret: a workflow
file changed on a same-repository branch can request secrets at those broader
scopes before the change is reviewed.

Configure these environments in the repository settings:

## `credentialed-pr-review`

- Add `ANTHROPIC_API_KEY` as an environment secret.
- Require at least one trusted reviewer and prevent self-review.
- Disable administrator bypass.
- Restrict deployment branches to `main`. The workflow accepts only PRs whose
  live base is `main` in this repository, loads tooling from
  `github.workflow_sha`, and separately binds approval to the exact candidate
  SHA.

Adding the `ai-review` label starts a credential-free context job. The
credentialed job waits for the environment reviewer, then rechecks the live PR
head and label before reading the exact current-run artifact. Every
`synchronize` event cancels the old run and removes the label, so a new head
requires the label and environment approval again.

## `credentialed-ci`

- Add `ANTHROPIC_API_KEY` as an environment secret.
- Require at least one trusted reviewer and prevent self-review.
- Disable administrator bypass.
- Restrict deployment branches to `main` only.
- Do not allow tags or arbitrary branches.

`.github/workflows/engine-evals-live.yml` checks out `github.sha` and refuses to
run unless `github.ref` is `refs/heads/main`. Pull-request evals live in
`engine-evals.yml`; that workflow has no environment or secret reference and
runs only the deterministic self-test and structured-error contract.

After migration, remove any repository/org copy of `ANTHROPIC_API_KEY`, then
verify both environments' deployment history and protection rules in GitHub's
settings UI.

## Required merge checks

After this policy first lands on `main`, configure the `main` branch ruleset or
branch protection to require both GitHub Actions status checks:

- `Credential containment policy`
- `Credential-containment regression tests`

These two checks are not equivalent, and the difference matters when you set up
branch protection. `Credential containment policy` (`ci-security-policy.yml`) is
the enforcing gate: it runs under `pull_request_target`, loads the checker from
`github.workflow_sha`, fetches the candidate `.github` tree as data, and
byte-compares every frozen trust root against the trusted base.
`Credential-containment regression tests` (`ci-security-policy-tests.yml`) runs
the candidate's own test files under the ordinary `pull_request` model, so on its
own it is advisory: a hostile pull request could rewrite the tests to pass. Its
integrity comes from the policy gate, because `test_credential_containment.py` is
itself a frozen trust root, so any tampering fails the policy check. If the
settings UI only lets you require one status, require `Credential containment
policy`.

Both workflows intentionally run on every pull request, without a path filter,
so their required statuses cannot remain absent on a non-`.github` change. Pin
the required checks to the GitHub Actions source if the settings UI offers that
choice. Require branches to be up to date before merging, do not grant ordinary
bypass access, and keep any emergency override limited to named maintainers.

Verify the rule with a disposable pull request that makes a policy regression:
the first check must fail from trusted `main` tooling, the candidate regression
suite must report its own result, and GitHub must refuse the merge. Restore the
test branch without merging it. A green policy job is advisory until these
required-check rules are enabled.

The trusted policy workflow never checks out the candidate revision. It uses a
read-only token to fetch the exact candidate commit's `.github` Git tree as
data, rejects truncated trees, symlinks, submodules, traversal-shaped paths,
identity mismatches, oversized entries, and oversized aggregate snapshots, and
then runs the trusted checker against restrictive regular files. This avoids
executing candidate code in the default branch's cache security domain.

## Validating RD-026 before closure

The offline regression tests cannot exercise the parts of this boundary that
only exist at runtime: the `pull_request_target` and `workflow_run` events, the
environment approval gate, and the live GitHub REST API responses that the
credentialed jobs authenticate. Those workflows also cannot run from a pull
request, because `pull_request_target` and `workflow_run` execute from the base
branch, so they have no coverage until this change is on `main`. Treat RD-026 as
closed only after these pass on the merged revision:

1. Configure both environments and the two required checks above, then confirm
   with a disposable regression pull request that the policy gate fails from
   trusted `main` tooling and that GitHub refuses the merge.
2. Add the `ai-review` label to a real pull request and approve the
   `credentialed-pr-review` environment. Confirm the credentialed job posts a
   review comment. This is the first execution of the live path. If the
   `Fetch and validate current-run review context` step fails, read its
   annotation: the validator names the exact run-object field that did not match
   (for example `head_sha`), which localizes any wrong assumption about the
   `pull_request_target` run shape.
3. Reproduce the RD-026 acceptance test. After approval, push a new commit to the
   same pull request. Confirm the `synchronize` event removes the label and
   cancels the run, and that a re-labeled run reads only the new head's context.
   The secret-bearing job must never consume the earlier approved artifact for a
   changed head.
4. Confirm `rocky-preview-comment` upserts exactly one comment on the same pull
   request and that a later push updates that comment rather than duplicating it.

Record the run URLs in the remediation ledger. Until these pass on `main`, the
code is merge-ready but RD-026 stays In review.

## Updating frozen trust roots

The credential-containment workflow compares the candidate copies of its
security checker, tests, trusted actions, and boundary helpers byte-for-byte
with the workflow revision. The executable `.github/scripts` directory also has
an exact entry allowlist so a sibling module cannot shadow a trusted helper's
imports. This deliberately makes an ordinary pull request that changes a trust
root fail closed.

Use this bootstrap procedure for an intentional trust-root update:

1. Put only the reviewed trust-root changes in a dedicated pull request and
   obtain independent security review.
2. Expect the credential-containment check to reject the changed frozen file.
   Run the ordinary unprivileged policy-test workflow, the full local unit
   suite, `actionlint`, and `shellcheck` against the exact candidate SHA.
3. Confirm that no `pull_request_target` or `workflow_run` job executes or
   imports candidate code before credentials become available.
4. Have an authorized maintainer use the narrow branch-protection override to
   merge that reviewed SHA. Do not disable the policy or broaden its allowlist.
5. Treat the new `main` revision as the trusted checker, rerun the containment
   workflow, and confirm it accepts the repository before any other merge.
