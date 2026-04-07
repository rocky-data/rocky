#!/usr/bin/env bash
set -euo pipefail
event_json=$(cat)
echo "[hook] pipeline_success: $event_json"
