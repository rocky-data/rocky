#!/usr/bin/env bash
# Configure GitHub rulesets + code-security features for rocky-data/rocky.
#
# Run this AFTER making the repo public. Rulesets, secret scanning,
# push protection, and private vulnerability reporting are all gated
# behind the public-visibility + Free-plan combination, so none of this
# takes effect while the repo is private on Free.
#
# Usage: bash scripts/setup-rulesets.sh
#
# Rulesets creation (sections 1–3) is NOT idempotent — re-running this
# script will create duplicate rulesets. Delete existing ones first if
# you need to re-apply. Sections 4–5 (repo PATCH / feature PUTs) are
# safely idempotent.

set -euo pipefail

REPO="rocky-data/rocky"

# --- Required status checks ---
#
# After the monorepo consolidation, CI is split across path-filtered workflows
# (engine-ci, dagster-ci, vscode-ci, …). A PR touching only editors/vscode/
# never triggers engine-ci, so requiring `Test` / `Clippy` / `Format` as
# always-required checks would block every cross-cutting merge.
#
# The clean fix is a "ci-gate" pattern: add an aggregator workflow that always
# runs (no path filter) and waits for whichever path-filtered jobs were
# triggered, then mark only the gate job as required. Until that's set up,
# this script leaves the required-status-checks list empty so PRs aren't
# blocked. Branch protection still enforces PR + non-fast-forward + deletion
# protection.
#
# To enable required status checks later, populate STATUS_CHECKS with the
# job names (or `workflow / job` references) you want to enforce.
STATUS_CHECKS='[]'

echo "=== Setting up rulesets for $REPO ==="

# 1. Main branch protection
echo "  Creating main-protection ruleset..."
gh api "repos/$REPO/rulesets" \
  --method POST \
  --input - <<EOF
{
  "name": "main-protection",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/main"],
      "exclude": []
    }
  },
  "bypass_actors": [
    {
      "actor_id": 1,
      "actor_type": "OrganizationAdmin",
      "bypass_mode": "always"
    }
  ],
  "rules": [
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 0,
        "dismiss_stale_reviews_on_push": true,
        "require_code_owner_review": true,
        "require_last_push_approval": false,
        "required_review_thread_resolution": true
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": false,
        "required_status_checks": ${STATUS_CHECKS}
      }
    },
    {
      "type": "deletion"
    },
    {
      "type": "non_fast_forward"
    }
  ]
}
EOF

# 2. All branches — no force push
echo "  Creating all-branches-no-force-push ruleset..."
gh api "repos/$REPO/rulesets" \
  --method POST \
  --input - <<EOF
{
  "name": "all-branches-no-force-push",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["~ALL"],
      "exclude": []
    }
  },
  "bypass_actors": [
    {
      "actor_id": 1,
      "actor_type": "OrganizationAdmin",
      "bypass_mode": "always"
    }
  ],
  "rules": [
    {
      "type": "non_fast_forward"
    }
  ]
}
EOF

# 3. Tag protection
echo "  Creating tag-protection ruleset..."
gh api "repos/$REPO/rulesets" \
  --method POST \
  --input - <<EOF
{
  "name": "tag-protection",
  "target": "tag",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["~ALL"],
      "exclude": []
    }
  },
  "bypass_actors": [
    {
      "actor_id": 1,
      "actor_type": "OrganizationAdmin",
      "bypass_mode": "always"
    }
  ],
  "rules": [
    {
      "type": "creation"
    },
    {
      "type": "update"
    },
    {
      "type": "deletion"
    }
  ]
}
EOF

# 4. Repository settings
echo "  Configuring repo settings..."
gh api "repos/$REPO" --method PATCH \
  --field delete_branch_on_merge=true \
  --field allow_update_branch=true \
  --field allow_auto_merge=true \
  --silent

# 5. Code security features (public-only)
#
# Secret scanning, push protection, and private vulnerability reporting
# are free on public repos but unavailable on Free-plan private repos —
# which is why this script must run AFTER the visibility flip.
echo "  Enabling secret scanning and push protection..."
gh api "repos/$REPO" --method PATCH --input - >/dev/null <<'JSON'
{
  "security_and_analysis": {
    "secret_scanning":                 {"status": "enabled"},
    "secret_scanning_push_protection": {"status": "enabled"}
  }
}
JSON

echo "  Enabling private vulnerability reporting..."
gh api "repos/$REPO/private-vulnerability-reporting" --method PUT

# Dependabot security updates: works on private Free too, but the script
# should leave a public repo in a known-good state regardless of prior state.
echo "  Enabling Dependabot security updates..."
gh api "repos/$REPO/automated-security-fixes" --method PUT

# CodeQL default setup is NOT scripted — for a monorepo it's cleaner to
# configure languages + scan roots once via the UI under Settings → Code
# security → Code scanning → Set up. Rust isn't covered by CodeQL; the
# cargo audit job in engine-weekly.yml handles that side.

echo "  Done with $REPO"
echo

echo "=== Verifying rulesets ==="
gh api "repos/$REPO/rulesets" --jq '.[].name'

echo
echo "All rulesets and security features configured successfully!"
echo
echo "NOTE: required_status_checks is currently empty. To enforce CI status,"
echo "      add a ci-gate aggregator workflow and populate STATUS_CHECKS in"
echo "      this script with the gate job's name."
echo
echo "NEXT: enable CodeQL default setup via the UI (Settings → Code security"
echo "      → Code scanning → Set up → Default). Select JS/TS and Python."
