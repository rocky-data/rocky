#!/bin/bash
# SessionStart hook for Claude Code cloud sessions.
#
# A thin wrapper: the install logic lives in scripts/setup-agent-tools.sh so
# every agent client can run the same setup. Local sessions are left alone.
set -uo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
bash "$root/scripts/setup-agent-tools.sh"
exit 0
