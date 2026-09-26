#!/usr/bin/env bash
set -euo pipefail

raw=${1:-}
head_ref=${2:-}
pr_number=${3:-}

slug() {
  printf '%s' "$1" | LC_ALL=C tr -c 'A-Za-z0-9' '_' | sed 's/__*/_/g; s/^_//; s/_$//'
}

if [[ -n "$raw" ]]; then
  name=$(slug "$raw")
  if [[ -z "$name" ]]; then
    echo 'branch_name must contain an ASCII letter or digit after slugging' >&2
    exit 1
  fi
  printf '%s' "${name:0:64}"
elif [[ "$pr_number" =~ ^[0-9]+$ ]]; then
  prefix="pr_${pr_number}_"
  head_slug=$(slug "${head_ref:-head}")
  head_slug=${head_slug:-head}
  max_slug=$((64 - ${#prefix}))
  printf '%s' "${prefix}${head_slug:0:max_slug}"
fi
