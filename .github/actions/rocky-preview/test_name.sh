#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
name() { bash "$here/name.sh" "$@"; }

[[ $(name '' fix-price 42) == pr_42_fix_price ]] || exit 1
[[ $(name '' 'feature/fix-price' 2180) == pr_2180_feature_fix_price ]] || exit 1
[[ $(name '' fix_price 43) == pr_43_fix_price ]] || exit 1
[[ $(name '' fix-price 42) != $(name '' fix_price 43) ]] || exit 1
[[ $(name 'fix-price' '' '') == fix_price ]] || exit 1
if name '---' fix-price 42 >/dev/null 2>&1; then
  echo 'an explicit empty slug must fail' >&2
  exit 1
fi

long_name=$(name '' "$(printf 'x%.0s' {1..100})" 123)
[[ ${#long_name} -eq 64 ]] || exit 1
[[ "$long_name" =~ ^[A-Za-z0-9_]+$ ]] || exit 1

echo 'preview action names: pass'
