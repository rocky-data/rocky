# Credentialed workflow environments

Store Rocky's model API key only as an environment secret. Do not define `ANTHROPIC_API_KEY` as a repository or organization secret: a workflow file changed on a same-repository branch can request secrets at those broader scopes before review.

Configure these environments in the repository settings.

## `credentialed-pr-review`

- Add `ANTHROPIC_API_KEY` as an environment secret.
- Require at least one trusted reviewer. With one maintainer, self-review cannot be prevented. The approval is a deliberate pause before secrets are released, not a second person.
- Disable administrator bypass.
- Restrict deployment branches to `main` only. Do not allow tags or arbitrary branches.

The workflow accepts only PRs whose live base is `main` in this repository, loads tooling from `github.workflow_sha`, and binds approval to the exact candidate SHA.

Adding the `ai-review` label starts a credential-free context job. The credentialed job waits for the environment reviewer, then rechecks the live PR head and label before reading the exact current-run artifact. Every `synchronize` event cancels the old run and removes the label, so a new head needs the label and environment approval again.

## `credentialed-ci`

- Add `ANTHROPIC_API_KEY` as an environment secret.
- Require at least one trusted reviewer. With one maintainer, self-review cannot be prevented. The approval is a deliberate pause before secrets are released, not a second person.
- Disable administrator bypass.
- Restrict deployment branches to `main` only. Do not allow tags or arbitrary branches.

`.github/workflows/engine-evals-live.yml` checks out `github.sha` and refuses to run unless `github.ref` is `refs/heads/main`. Pull-request evals live in `engine-evals.yml`. That workflow has no environment or secret reference and runs only the deterministic self-test and the structured-error contract.

After setup, remove any repository or organization copy of `ANTHROPIC_API_KEY`. Then verify both environments' deployment history and protection rules in the GitHub settings UI.

## Required merge checks

Require both GitHub Actions status checks on the `main` branch ruleset (status: enabled, with branches required to be up to date):

- `Credential containment policy`
- `Credential-containment regression tests`

They are not equivalent:

- `Credential containment policy` (`ci-security-policy.yml`) is the enforcing gate. It runs under `pull_request_target`, loads the checker from `github.workflow_sha`, fetches the candidate `.github` tree as data, and byte-compares every frozen trust root against the trusted base.
- `Credential-containment regression tests` (`ci-security-policy-tests.yml`) runs the candidate's own tests under the ordinary `pull_request` model, so alone it is advisory: a hostile PR could rewrite the tests. Its integrity comes from the policy gate, because `test_credential_containment.py` is itself a frozen trust root.

If the settings UI lets you require only one, require `Credential containment policy`.

Both workflows run on every pull request, with no path filter, so their required statuses never stay absent. Pin the checks to the GitHub Actions source if the UI offers it. Require up-to-date branches, grant no ordinary bypass, and limit any emergency override to named maintainers.

These checks are not optional hardening, and other required checks are no substitute. The engine contexts report green on changes that skip their expensive jobs, so a PR that bundles an engine edit with a hostile `.github` edit reports them all green. Only the containment check examines the `.github` tree. If it is present but not required, GitHub reports the PR mergeable and the change lands with no override.

To verify the rule, open a disposable PR that makes a policy regression. The policy check must fail from trusted `main` tooling, the regression suite must report its own result, and GitHub must refuse the merge. Do not merge it. A green policy job is advisory until the required-check rules are on.

The trusted policy workflow never checks out the candidate revision. It uses a read-only token to fetch the candidate commit's `.github` Git tree as data. It rejects truncated trees, symlinks, submodules, traversal-shaped paths, identity mismatches, oversized entries and oversized aggregate snapshots. It then runs the trusted checker against restrictive regular files, so no candidate code runs in the default branch's cache security domain.

## Release and deployment action sources

The containment rules above govern jobs reachable from a pull request. Release,
publish and deployment workflows are a different trust domain: they run from a
tag or a push, never execute candidate code, and so none of the pull-request
rules applied to them. They do hold a write token and the publishing secrets,
which made an unreviewed action source in one of their steps a direct path into
the artifacts this project ships — `engine/install.sh` serves exactly those
binaries.

Every job not reachable from a pull request may only run actions in `RELEASE_ACTION_SOURCES`, the pinned allowlist in the checker. Parsing fails closed: a step whose `uses:` cannot be read as exactly one pinned remote source is a violation, and a repository-local action in such a job is rejected. Adding an action to a release workflow is a trust-root change and follows the bootstrap procedure below.

Pin provenance is not self-verifying. A pinned commit that shares a long prefix with the intended tag but diverges looks identical by eye and fails to resolve at run time. Confirm any new pin against its upstream tag before you add it.

These jobs must use the canonical block form the checker understands: block mapping jobs, a `steps:` list at six-space indentation, no flow-style `jobs: {...}`, no job image, no reusable-workflow call. A step whose scalar spans several lines with a continuation beginning `uses:` is also rejected. Detecting that shape needs a real YAML parser, which the checker forgoes so the same file runs in the minimal policy job. It fails closed and the author reformats. The restriction is safe the other way: a scalar cannot conceal an executed step, because step splitting keys off the six-space prefix.

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
5. Treat the new `main` revision as the trusted checker and confirm it accepts the
   repository before any other merge. `ci-security-policy.yml` triggers only on
   `pull_request_target`, so there is no way to rerun it against `main` on demand:
   the confirmation run is **the next pull request opened after the bootstrap
   merge**. Open one that does not itself touch a trust root, and require its
   `Credential containment policy` result to be green before merging anything
   else. A second trust-root pull request opened immediately would report red for
   its own bootstrap reasons and cannot confirm the previous one.
