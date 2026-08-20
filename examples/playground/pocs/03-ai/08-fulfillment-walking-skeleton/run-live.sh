#!/usr/bin/env bash
# LIVE LANE — the capability proof (the required Phase-1 completion gate).
#
# A REAL worker (`claude -p`) drives the fulfillment loop from a truly cold,
# empty directory: it samples the source, writes its OWN candidate spec, and
# authors the model SQL itself — no recorded session. The same engine gates as
# run.sh apply. This is what answers "can an agent actually do this", which the
# replay lane (machinery only) cannot.
#
# Bounded by the loop's OWN budget (max_compile_iters, max_repair_rounds) plus a
# per-task timeout in [fulfill.driver]. It is NOT wrapped in an outer retry: one
# bounded run. If the worker does not converge, the evidence is still banked and
# the outcome reported — we do not burn repeated live runs.
#
# The `: "${ANTHROPIC_API_KEY:?}"` guard below keeps this file out of the
# credential-free smoke lane (run-all-duckdb.sh skips any run.sh with that guard;
# this is run-live.sh, and it carries the guard so the intent is explicit).
set -uo pipefail
: "${ANTHROPIC_API_KEY:?Set ANTHROPIC_API_KEY to run the live lane (it is the worker credential)}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRODUCT="revenue_daily"
BUNDLE="$HERE/expected/live"

die() { echo "LIVE FAIL: $*"; exit 1; }

# --- Locate the tools and build the PATH the worker will inherit (env_allow). ---
command -v rocky >/dev/null 2>&1 || die "rocky not on PATH (build the engine; see README)"
command -v claude >/dev/null 2>&1 || die "claude not on PATH (the live worker binary)"
command -v duckdb >/dev/null 2>&1 || die "duckdb CLI required"
rocky product --help >/dev/null 2>&1 || die "this rocky has no 'product' verb (too old)"
ROCKY_ABS="$(command -v rocky)"
WORKER_PATH="$(dirname "$ROCKY_ABS"):$(dirname "$(command -v claude)")"
command -v node >/dev/null 2>&1 && WORKER_PATH="$WORKER_PATH:$(dirname "$(command -v node)")"
WORKER_PATH="$WORKER_PATH:/usr/bin:/bin:/usr/sbin:/sbin"
export PATH="$WORKER_PATH:$PATH"

echo "=================================================================="
echo " Fulfillment walking skeleton — LIVE lane (real claude -p worker)"
echo " worker: $(claude --version 2>&1 | head -1)"
echo " engine: $(rocky --version 2>&1 | head -1)"
echo "=================================================================="

# --- A fresh, cold working directory: no recorded SQL, no pre-made spec. ---
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/models" "$WORK/data" "$WORK/briefs"
cp "$HERE/models/_defaults.toml" "$WORK/models/_defaults.toml"
cp "$HERE/data/seed.sql" "$WORK/data/seed.sql"
cp "$HERE/data/warehouse_seed.sql" "$WORK/data/warehouse_seed.sql"
# The project-level brief override that grounds the worker in the exact (closed)
# Rocky spec schema. Without it, a cold worker emits warehouse types / extra keys
# and Phase A rejects the spec. drafting/repair fall back to the compiled defaults.
cp "$HERE/briefs/elicitation.md" "$WORK/briefs/elicitation.md"
cp "$HERE/briefs/drafting.md" "$WORK/briefs/drafting.md"
duckdb "$WORK/wh.duckdb" < "$WORK/data/warehouse_seed.sql" >/dev/null 2>&1 || die "warehouse seed failed"

# The subprocess driver: `claude -p {brief}` against the worker-profile MCP.
# env_allow adds PATH + HOME to the frozen ANTHROPIC_API_KEY credential — the
# driver env_clears, and the `claude` binary + its node runtime + config need
# PATH/HOME to start (documented deviation from the bare frozen shape).
cat > "$WORK/rocky.toml" <<TOML
[adapter]
type = "duckdb"
path = "wh.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target]
adapter = "default"

[pipeline.p.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { models = ["revenue_daily"] }
effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["revenue_daily"] }
effect = "require_review"

[fulfill]
briefs_dir = "briefs"

[fulfill.driver]
type = "subprocess"
command = [
  "claude", "-p", "{brief}",
  "--mcp-config", "mcp-worker.json",
  "--strict-mcp-config",
  "--allowedTools", "mcp__rocky__list,mcp__rocky__inspect_schema,mcp__rocky__sample_rows,mcp__rocky__profile_column,mcp__rocky__compile,mcp__rocky__test,mcp__rocky__draft_model,Write,Read,Edit,Glob,Grep,LS,TodoWrite",
  "--dangerously-skip-permissions",
]
env_allow = ["ANTHROPIC_API_KEY", "PATH", "HOME"]
timeout_seconds = 300
kill_grace_seconds = 15
TOML

# claude spawns `rocky mcp --profile worker` itself (the driver spawns only the
# leader). --strict-mcp-config ignores the user's other MCP servers.
cat > "$WORK/mcp-worker.json" <<JSON
{ "mcpServers": { "rocky": { "command": "$ROCKY_ABS", "args": ["mcp", "--profile", "worker"] } } }
JSON

cd "$WORK"
rj() { local out="$1"; shift; rocky --output json "$@" >"$out" 2>"${out%.json}.err"; echo $?; }

echo; echo "[live 1/4] cold elicitation — the worker samples and writes its own candidate spec"
[ -e "products/${PRODUCT}.toml" ] && die "not a cold start: products/${PRODUCT}.toml already exists"
code=$(rj live_elicit.json fulfill "$PRODUCT")
STATE="$(jq -r '.state // "none"' live_elicit.json 2>/dev/null)"
echo "    fulfill(elicit) exit=$code state=$STATE"
if [ "$code" != "0" ] || [ "$STATE" != "needs_input" ]; then
  echo "    --- elicitation stop / error ---"; jq -r '.message // .' live_elicit.json 2>/dev/null | head; tail -8 live_elicit.err
  # bank whatever transcript exists before bailing
  mkdir -p "$BUNDLE"; cp -R .rocky/fulfillment/"$PRODUCT"/transcripts "$BUNDLE/" 2>/dev/null
  die "the live worker did not reach needs_input(spec_approval) in one bounded run (see expected/live/)"
fi
[ -e "products/${PRODUCT}.toml" ] || die "the runner did not write products/${PRODUCT}.toml from the worker hand-off"
echo "    the worker authored this spec:"; sed 's/^/      /' "products/${PRODUCT}.toml"

echo; echo "[live 2/4] human approves the spec, worker drafts the SQL, loop proposes a plan"
code=$(rj live_approve.json fulfill approve-spec "$PRODUCT"); echo "    approve-spec exit=$code state=$(jq -r .state live_approve.json)"
code=$(rj live_draft.json fulfill "$PRODUCT")
STATE="$(jq -r '.state // "none"' live_draft.json 2>/dev/null)"
PLAN="$(jq -r '.plan_id // empty' live_draft.json 2>/dev/null)"
echo "    fulfill(draft+propose) exit=$code state=$STATE plan=${PLAN:0:12}"
if [ -z "$PLAN" ]; then
  echo "    --- drafting stop / error ---"; jq -r '.message // .' live_draft.json 2>/dev/null | head; tail -12 live_draft.err
  mkdir -p "$BUNDLE"; cp -R .rocky/fulfillment/"$PRODUCT"/transcripts "$BUNDLE/" 2>/dev/null
  die "the live worker did not converge to a proposed plan in one bounded run (see expected/live/)"
fi

echo; echo "[live 3/4] human approves the plan; the loop applies (digest-gated) and observes"
code=$(rj live_review.json review "$PLAN" --approve); echo "    review --approve exit=$code"
code=$(rj live_apply.json fulfill "$PRODUCT")
STATE="$(jq -r '.state // "none"' live_apply.json 2>/dev/null)"
echo "    fulfill(apply+observe) exit=$code state=$STATE"
[ "$STATE" = "observing" ] || { tail -8 live_apply.err; die "the loop did not reach observing after applying the worker's model"; }

echo; echo "[live 4/4] the worker's OWN output passes the same gates (asserts 5,7,9)"
# 5: the plan is product-bound
[ "$(jq -r '.payload.product_id' ".rocky/plans/${PLAN}.json")" = "product:${PRODUCT}" ] || die "the worker's plan is not product-bound"
# 7: the warehouse table exists with rows
ROWS="$(duckdb -csv -noheader wh.duckdb "SELECT COUNT(*) FROM out.${PRODUCT}" 2>/dev/null)"
[ "${ROWS:-0}" -ge 1 ] || die "no rows materialised from the worker's model"
# 9: the generated composite-unique grain test RUNS green (declarative, against
# the warehouse). Plain `rocky test` runs only the model; --declarative runs the
# sidecar [[tests]]. A CLEAN run needs ALL of: 0 errored, 0 failed, 0 WARNED
# (Rocky counts warning-severity failures under `warned` and STILL exits 0), the
# composite grain test PASSING, passed==total, and a zero exit code. This is the
# banked runner_reverify_test.json.
code=$(rj live_test.json test --models models/ --declarative)
LTOTAL="$(jq -r '.declarative.total // "err"' live_test.json)"; LPASS="$(jq -r '.declarative.passed // "err"' live_test.json)"
LERR="$(jq -r '.declarative.errored // 1' live_test.json)"; LFAIL="$(jq -r '.declarative.failed // 1' live_test.json)"; LWARN="$(jq -r '.declarative.warned // 1' live_test.json)"
[ "$LERR" = "0" ] || die "the worker's declarative tests ERRORED=$LERR (malformed declarations): $(jq -c '.declarative.results[]?|select(.status=="error")|{test_type,detail}' live_test.json)"
[ "$LFAIL" = "0" ] || die "the worker's model failed a declarative test: $(jq -c '.declarative.results[]?|select(.status=="fail" and .severity!="warning")' live_test.json)"
[ "$LWARN" = "0" ] || die "the worker's declarative tests WARNED=$LWARN (a warning-severity failure exits 0 but is NOT clean): $(jq -c '.declarative.results[]?|select(.severity=="warning" and .status!="pass")|{test_type,detail}' live_test.json)"
[ "$LPASS" = "$LTOTAL" ] || die "the worker's declarative run is not all-pass: passed=$LPASS != total=$LTOTAL"
jq -e '.declarative.results[] | select(.test_type == "composite" and .status == "pass")' live_test.json >/dev/null \
  || die "the composite-unique grain test did not run and pass under --declarative"
[ "$code" = "0" ] || die "rocky test --declarative exit $code despite errored=0 failed=0 warned=0"
echo "    OK  plan product-bound; out.${PRODUCT} has $ROWS row(s); $LTOTAL declarative tests all pass ($LPASS/$LTOTAL; 0 failed, 0 errored, 0 warned)"

# --- Bank the evidence bundle (committed under expected/live/). ---
echo; echo "Banking the evidence bundle -> $BUNDLE"
rm -rf "$BUNDLE"; mkdir -p "$BUNDLE"
cp -R .rocky/fulfillment/"$PRODUCT"/transcripts "$BUNDLE/transcripts" 2>/dev/null || true
cp "products/${PRODUCT}.toml"      "$BUNDLE/worker_candidate_spec.toml"
cp "models/${PRODUCT}.sql"         "$BUNDLE/worker_authored.sql"
# The merged sidecar makes the declarative [[tests]] visible in the evidence.
cp "models/${PRODUCT}.toml"        "$BUNDLE/model_sidecar.toml"
SQL_HASH="$(shasum -a 256 "models/${PRODUCT}.sql" | awk '{print $1}')"
cp live_draft.json "$BUNDLE/runner_propose.json"
cp live_apply.json "$BUNDLE/runner_observe.json"
cp live_test.json  "$BUNDLE/runner_reverify_test.json"
# redact the git-derived approver so the committed bundle carries no personal identifier
rocky --output json product status "$PRODUCT" 2>/dev/null | jq '(.approval.approver) |= "<redacted: git-derived committer>"' > "$BUNDLE/runner_product_status.json"
duckdb -csv wh.duckdb "SELECT * FROM out.${PRODUCT} ORDER BY 1,2" > "$BUNDLE/materialized_snapshot.csv" 2>/dev/null
LAG="$(jq -r '.message | capture("lag (?<l>[0-9]+)s").l' live_apply.json 2>/dev/null)"
BUD="$(jq -r '.message | capture("budget (?<b>[0-9]+)s").b' live_apply.json 2>/dev/null)"

cat > "$BUNDLE/INDEX.md" <<EOF
# Live lane evidence bundle — $(date -u +%Y-%m-%dT%H:%M:%SZ)

A real \`claude -p\` worker drove the fulfillment loop end to end from a cold,
empty directory. No recorded SQL. This is the capability proof.

| item | value |
|---|---|
| worker | $(claude --version 2>&1 | head -1) |
| engine | $(rocky --version 2>&1 | head -1) |
| plan id | \`$PLAN\` |
| authored SQL sha256 | \`$SQL_HASH\` |
| materialised rows | $ROWS |
| declarative tests | ${LTOTAL} total, all pass (0 failed, 0 errored) |
| freshness at apply | lag ${LAG}s vs budget ${BUD}s |
| final state | observing |

## Files
- \`worker_candidate_spec.toml\` — the spec the worker wrote (the runner then digested + approved it).
- \`worker_authored.sql\` — the model SQL the worker authored (sha256 above).
- \`model_sidecar.toml\` — the merged sidecar, so the declarative \`[[tests]]\` are visible.
- \`transcripts/\` — the driver transcripts (worker stdout/stderr per task).
- \`runner_propose.json\` / \`runner_observe.json\` — the loop's own stops.
- \`runner_reverify_test.json\` — \`rocky test --declarative\`: ${LTOTAL} tests, all pass, 0 failed, 0 errored.
- \`runner_product_status.json\` — the loop's journaled state.
- \`materialized_snapshot.csv\` — the warehouse table the worker's model produced.

## Ledger
PASS — one bounded run reached \`observing\`. The worker AUTHORED the SQL itself
(\`worker_authored.sql\`, sha256 ${SQL_HASH}); its candidate spec is the
\`briefs/elicitation.md\` schema template with an \`intent\` filled in — grounding,
NOT a from-scratch design (convergence needs this override; on the *compiled*
brief a cold worker designs a plausible but off-schema spec). The worker's SQL
cleared compile, the declarative tests (\`rocky test --declarative\`: ${LTOTAL}
tests, all pass, 0 failed, 0 errored), the product-bound plan, human review, and
the digest-gated apply. Freshness was
observed (lag ${LAG}s vs ${BUD}s), not enforced. SQL authorship is genuine;
from-scratch spec design against the closed schema is the open capability — on the
*compiled* brief a cold worker designs a plausible but off-schema spec.
EOF

echo
echo "=================================================================="
echo "LIVE PASS: a real worker took revenue_daily from nothing to a live table."
echo "  plan=$PLAN"
echo "  authored SQL sha256=$SQL_HASH"
echo "  rows=$ROWS  freshness: lag=${LAG}s budget=${BUD}s"
echo "  evidence bundle: $BUNDLE"
echo "=================================================================="
