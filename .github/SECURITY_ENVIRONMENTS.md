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
