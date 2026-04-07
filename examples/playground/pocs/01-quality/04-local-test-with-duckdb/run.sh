#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb

rocky validate
rocky test --models models                    > expected/test.json
rocky test --models models --model good_mart  > expected/test_single.json

echo "POC complete: rocky test (full + --model good_mart) both pass."
