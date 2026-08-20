#!/usr/bin/env bash
# Fulfillment walking skeleton — the loop's happy path end to end, one binary.
# (Repair recovery is a known engine gap, #1493 — see README; not exercised here.)
#
# `set -uo pipefail` (NOT -e): this POC asserts FAILURE paths, so expected
# non-zero exits are captured explicitly (`; echo EXIT=$?`) and checked. Every
# numbered assert prints `FAIL: <n>` and exits 1 when its engine gate does not
# hold. Precedent: pocs/05-orchestration/09-idempotency-key/run.sh.
#
# Credential-free: the [fulfill.driver] is `replay`, so no ANTHROPIC_API_KEY is
# read and `rocky mcp --profile worker` is driven from a RECORDED session.
# This proves the MACHINERY (custody chain + gates), not agent capability —
# that is what run-live.sh proves. See README "What this does and does not prove".
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

PRODUCT="revenue_daily"
MANIFEST=".rocky/fulfillment/${PRODUCT}/lowering-manifest.json"

fail() { echo "FAIL: $1"; exit 1; }

# Mutation-pass hook. `MUTATE=<n>` breaks assert <n>'s gate so mutation-pass.sh
# can prove the assert is non-vacuous (it must then print FAIL: <n>). Unset in
# normal runs. Mutations run in a throwaway copy of the POC, never the tree.
MUT="${MUTATE:-0}"
mut() { [ "$MUT" = "$1" ]; }

# --- Fail-fast: the binary must carry the fulfillment verbs (E1/E2). ---
command -v rocky >/dev/null 2>&1 || fail "0 (rocky not on PATH — see README: build the engine and add engine/target/release to PATH)"
rocky product --help >/dev/null 2>&1 || fail "0 (this rocky has no 'product' command — build from a worktree that carries FF-WP-E1/E2; see README)"
rocky fulfill --help >/dev/null 2>&1 || fail "0 (this rocky has no 'fulfill' command — see README)"
command -v jq >/dev/null 2>&1 || fail "0 (jq is required for the assertions)"
command -v duckdb >/dev/null 2>&1 || fail "0 (duckdb CLI is required to seed and inspect the warehouse)"

# --- Truly cold start: wipe generated state (keep only committed inputs). ---
rm -rf products .rocky wh.duckdb
rm -f  models/.rocky-state.redb models/.rocky-state.redb.lock .rocky-state.redb .rocky-state.redb.lock
rm -f  "models/${PRODUCT}.sql" "models/${PRODUCT}.toml" "models/${PRODUCT}.contract.toml"
# Wipe the ephemeral goldens but PRESERVE the committed live evidence bundle
# (expected/live/), which run-live.sh banks and which must survive a replay run.
mkdir -p expected
find expected -mindepth 1 -maxdepth 1 ! -name live -exec rm -rf {} + 2>/dev/null || true

# Seed the persistent warehouse so the apply materialises real revenue.
duckdb wh.duckdb < data/warehouse_seed.sql >/dev/null 2>&1 || fail "0 (warehouse seed failed)"

# rocky --output json <verb...>  → stdout JSON captured to $1, returns the exit code.
rj() { local out="$1"; shift; rocky --output json "$@" >"$out" 2>"${out%.json}.err"; echo $?; }

echo "=================================================================="
echo " Fulfillment walking skeleton — replay lane (credential-free)"
echo " spec -> lowering -> human gate -> engine backstop -> apply"
echo "=================================================================="

# ------------------------------------------------------------------ Assert 1
# COLD START. products/<name>.toml must NOT exist beforehand; the replayed
# elicitation's typed DriverOutcome produces the candidate bytes and the RUNNER
# writes them. Engine gate: Init --Elicit--> confined candidate write -->
# needs_input(spec_approval), with the hand-off digest re-verified.
echo; echo "[1] cold start: elicitation writes the candidate, loop stops for approval"
# MUTATION 1: lie about the candidate digest -> the runner refuses the hand-off,
# the candidate is never written, the loop blocks instead of stopping for approval.
mut 1 && { jq '.tasks.elicitation.outcome.expected_digest="sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"' replay/session.json > .mut && mv .mut replay/session.json; }
[ -e "products/${PRODUCT}.toml" ] && fail "1 (products/${PRODUCT}.toml existed before the first fulfill — not a cold start)"
code=$(rj expected/01_cold.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "1 (cold fulfill exit $code, want 0; $(cat expected/01_cold.err))"
[ "$(jq -r .state expected/01_cold.json)" = "needs_input" ] || fail "1 (state $(jq -r .state expected/01_cold.json), want needs_input)"
[ -e "products/${PRODUCT}.toml" ] || fail "1 (the runner did not write products/${PRODUCT}.toml)"
jq -e '.next_command | test("approve-spec")' expected/01_cold.json >/dev/null || fail "1 (next_command is not approve-spec)"
echo "    OK  state=needs_input, products/${PRODUCT}.toml written by the runner"

# ------------------------------------------------------------------ Assert 3
# NEGATIVE LOWERING (isolated scratch project, so the live product is untouched).
# Each broken spec variant must be REJECTED naming its field/code. These are the
# engine's spec-schema gates (parse-time in rocky-core; classification is
# verify-time in rocky-cli), not shell theater.
echo; echo "[3] negative lowering: each broken spec is refused with its stable code"
# MUTATION 3: replace the auto_apply variant with the VALID spec -> it lowers
# cleanly, no reject code, so the last check_reject must fail.
mut 3 && cp replay/candidate_spec.toml broken-specs/auto-apply.toml
S3="$(mktemp -d)"
mkdir -p "$S3/models" "$S3/products"
cp rocky.toml "$S3/rocky.toml"; cp models/_defaults.toml "$S3/models/_defaults.toml"
check_reject() { # <file> <expected-code>
  local spec="$1" want="$2" rc
  cp "$spec" "$S3/products/${PRODUCT}.toml"
  ( cd "$S3" && rocky --output json product compile "$PRODUCT" >c.json 2>c.err ); rc=$?
  # A refusal is BOTH a non-zero exit AND the stable code named — not just the code
  # appearing somewhere. A valid spec (mutation 3) exits 0 and fails this.
  if [ "$rc" != "0" ] && grep -q "$want" "$S3/c.json" "$S3/c.err" 2>/dev/null; then
    echo "    OK  $(basename "$spec")  refused (exit $rc) -> [$want]"
  else
    echo "    got: exit=$rc  $(head -c 200 "$S3/c.json" "$S3/c.err" 2>/dev/null)"
    fail "3 ($(basename "$spec") must refuse with a NON-ZERO exit AND code [$want]; got exit $rc)"
  fi
}
check_reject broken-specs/unknown-key.toml            "unknown-key"
check_reject broken-specs/warehouse-type.toml         "type-not-rocky"
check_reject broken-specs/freshness-no-time-column.toml "freshness-missing-time-column"
check_reject broken-specs/glob-source.toml            "source-not-exact-triple"
check_reject broken-specs/unresolved-classification.toml "classification-unresolved"
check_reject broken-specs/auto-apply.toml             "trust-not-propose-only"
rm -rf "$S3"

# ------------------------------------------------------------------ Assert 4
# POLICY VERIFY (isolated scratch project). Strip [policy] -> `rocky product
# verify` returns needs_input with a PASTE-READY block; the loop stops with the
# SAME block, verbatim. Engine gate: the three-check posture verification.
echo; echo "[4] policy posture: stripped [policy] -> paste-ready block, compared verbatim"
S4="$(mktemp -d)"
mkdir -p "$S4/models" "$S4/replay" "$S4/data" "$S4/products"
# Delete from the policy comment to EOF (removes [policy] AND the trailing
# [fulfill] block), then re-append the driver so there is exactly one.
sed '/^# The agent-policy posture the loop verifies/,$d' rocky.toml > "$S4/rocky.toml"
cat >> "$S4/rocky.toml" <<'EOF'
[fulfill]

[fulfill.driver]
type = "replay"
session = "replay/session.json"
EOF
grep -c '^\[fulfill\]' "$S4/rocky.toml" | grep -qx 1 || fail "4 (scratch config has a duplicate [fulfill] block)"
# MUTATION 4: DON'T strip [policy] -> posture verify passes, no paste_block,
# so the needs_input assertion must fail.
mut 4 && cp rocky.toml "$S4/rocky.toml"
cp models/_defaults.toml "$S4/models/_defaults.toml"
cp replay/session.json "$S4/replay/session.json"
cp data/seed.sql "$S4/data/seed.sql"
cp "products/${PRODUCT}.toml" "$S4/products/${PRODUCT}.toml"
( cd "$S4" && rocky --output json product verify "$PRODUCT" >v.json 2>v.err; echo "verify EXIT=$?" >v.code )
[ "$(jq -r .status "$S4/v.json")" = "needs_input" ] || fail "4 (verify status $(jq -r .status "$S4/v.json"), want needs_input)"
jq -r '.paste_block' "$S4/v.json" > "$S4/paste_block.txt"
[ -s "$S4/paste_block.txt" ] || fail "4 (product verify produced no paste_block)"
# The loop must stop for policy and embed the SAME paste block, verbatim. The
# posture gate is at spec_approved, so approve first (cold -> spec_approval ->
# approve-spec -> spec_approved -> posture verify -> needs_input(policy)).
( cd "$S4" && rocky --output json fulfill "$PRODUCT" >f0.json 2>f0.err )
( cd "$S4" && rocky --output json fulfill approve-spec "$PRODUCT" >fa.json 2>fa.err )
( cd "$S4" && rocky --output json fulfill "$PRODUCT" >f.json 2>f.err )
jq -r '.message' "$S4/f.json" > "$S4/loop_msg.txt"
[ "$(jq -r .state "$S4/f.json")" = "needs_input" ] || fail "4 (loop did not stop at needs_input for policy)"
grep -q '^policy posture needs a human edit' "$S4/loop_msg.txt" || fail "4 (loop stop is not the policy-posture ask)"
# BYTE-FOR-BYTE: the loop embeds the paste-block verbatim after a fixed preamble.
# Extract from the block's first line ([policy]) to EOF and diff against the
# `rocky product verify` block. No order-independent fallback: exact match or FAIL.
sed -n '/^\[policy\]/,$p' "$S4/loop_msg.txt" > "$S4/loop_block.txt"
if ! diff "$S4/paste_block.txt" "$S4/loop_block.txt" >/dev/null 2>&1; then
  echo "    --- product verify block (left) vs loop block (right) ---"; diff "$S4/paste_block.txt" "$S4/loop_block.txt" | head -20
  fail "4 (the loop's paste-block is NOT byte-identical to rocky product verify)"
fi
echo "    OK  the loop's needs_input(policy) block is byte-identical to rocky product verify ($(wc -c < "$S4/paste_block.txt" | tr -d ' ') bytes)"
cp "$S4/paste_block.txt" expected/04_paste_block.txt
rm -rf "$S4"

# ------------------------------------------------------------------ Assert 2
# APPROVE + MANIFEST TOTALITY. Approve the spec, then one fulfill drives the
# loop through snapshot verify -> posture -> Phase A -> drafting -> Phase B ->
# verify -> propose, stopping for plan review. The committed lowering manifest
# must be TOTAL: phase "merged", every spec field mapped, zero rejects.
echo; echo "[2] approve spec + drive the loop; manifest is total (merged, 0 rejects)"
code=$(rj expected/02_approve.json fulfill approve-spec "$PRODUCT")
[ "$code" = "0" ] || fail "2 (approve-spec exit $code; $(cat expected/02_approve.err))"
[ "$(jq -r .state expected/02_approve.json)" = "spec_approved" ] || fail "2 (state after approve $(jq -r .state expected/02_approve.json), want spec_approved)"
APPROVED_DIGEST="$(jq -r .spec_digest expected/02_approve.json)"
code=$(rj expected/02_drive.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "2 (drive-to-review exit $code; $(cat expected/02_drive.err))"
[ "$(jq -r .state expected/02_drive.json)" = "needs_input" ] || fail "2 (state $(jq -r .state expected/02_drive.json), want needs_input(plan_approval))"
[ -f "$MANIFEST" ] || fail "2 (no lowering manifest at $MANIFEST)"
# MUTATION 2: inject a REJECTED field into the committed manifest -> the
# zero-rejects totality assertion below must catch it. The loop still reached
# needs_input (drive succeeded), so this mutation exercises the manifest-totality
# gate itself, not the earlier drive-exit check.
mut 2 && { jq '.fields["product.output.grain"].disposition = "reject" | .fields["product.output.grain"].reject_reason = "injected by mutation 2 (F4)"' "$MANIFEST" > .mut && mv .mut "$MANIFEST"; }
[ "$(jq -r .phase "$MANIFEST")" = "merged" ] || fail "2 (manifest phase $(jq -r .phase "$MANIFEST"), want merged)"
[ "$(jq -r .spec_digest "$MANIFEST")" = "$APPROVED_DIGEST" ] || fail "2 (manifest spec_digest != approved digest)"
NFIELDS=$(jq -r '.fields | length' "$MANIFEST")
REJECTS=$(jq -r '[.fields[] | select(.disposition == "reject" or .reject_reason != null)] | length' "$MANIFEST")
UNMAPPED=$(jq -r '[.fields[] | select(.disposition == "pending")] | length' "$MANIFEST")
[ "$NFIELDS" -ge 8 ] || fail "2 (manifest maps only $NFIELDS fields)"
[ "$REJECTS" = "0" ] || fail "2 (manifest has $REJECTS rejected field(s))"
[ "$UNMAPPED" = "0" ] || fail "2 (manifest has $UNMAPPED still-pending field(s) in the merged phase)"
echo "    OK  manifest merged, $NFIELDS fields mapped, 0 rejects, digest bound to the approval"

# ------------------------------------------------------------------ Assert 5
# The proposed plan on disk carries the product binding: product_id + spec_digest.
echo; echo "[5] the proposed plan carries product_id + spec_digest"
PLAN1="$(jq -r .plan_id expected/02_drive.json)"
[ -n "$PLAN1" ] && [ "$PLAN1" != "null" ] || fail "5 (no plan_id pinned after propose)"
PLAN_JSON=".rocky/plans/${PLAN1}.json"
[ -f "$PLAN_JSON" ] || fail "5 (plan JSON $PLAN_JSON missing)"
# MUTATION 5: strip the product binding from the persisted plan -> assert 5's
# product_id check must catch it (it reads the real plan payload, not memory).
mut 5 && { jq '.payload.product_id="product:not_revenue_daily"' "$PLAN_JSON" > .mut && mv .mut "$PLAN_JSON"; }
[ "$(jq -r '.payload.product_id' "$PLAN_JSON")" = "product:${PRODUCT}" ] || fail "5 (plan payload.product_id wrong)"
[ "$(jq -r '.payload.spec_digest' "$PLAN_JSON")" = "$APPROVED_DIGEST" ] || fail "5 (plan payload.spec_digest != approved digest)"
echo "    OK  plan ${PLAN1:0:12}… bound to product:${PRODUCT} @ ${APPROVED_DIGEST:0:19}…"

# ------------------------------------------------------------------ Assert 6
# D2 SEMANTICS: supersession is on RE-APPROVAL, not on a candidate edit.
#  (a) Editing the candidate while a plan waits does NOT supersede — the state
#      stays needs_input and the SAME plan stays reviewable (the engine surfaces
#      the revised candidate digest at the spec_approval gate, not the plan gate;
#      here the invariant under test is "a mere edit never fences a live plan").
#  (b) Re-approving the edited candidate via `rocky fulfill approve-spec` DOES
#      supersede: the loop re-enters spec_approved (the CAS fence) and a NEW plan
#      replaces the old one, which is orphaned (never applied).
echo; echo "[6] D2: candidate edit does not supersede; re-approval does (new plan)"
# MUTATION 6: skip the edit -> re-approval sees the SAME digest, nothing is
# superseded, and the "fresh plan id" assertion must fail.
if ! mut 6; then
  printf '# reviewer note: confirmed refunds are excluded\n%s' "$(cat products/${PRODUCT}.toml)" > products/${PRODUCT}.toml.new
  mv products/${PRODUCT}.toml.new products/${PRODUCT}.toml
fi
EDITED_DIGEST="sha256:$(shasum -a 256 products/${PRODUCT}.toml | awk '{print $1}')"
code=$(rj expected/06_edit.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "6 (fulfill after edit exit $code; $(cat expected/06_edit.err))"
[ "$(jq -r .state expected/06_edit.json)" = "needs_input" ] || fail "6 (edit changed the state; want needs_input)"
[ "$(jq -r .plan_id expected/06_edit.json)" = "$PLAN1" ] || fail "6 (a mere candidate edit must NOT supersede the pending plan)"
echo "    OK  candidate edited (new digest ${EDITED_DIGEST:0:19}…) — plan ${PLAN1:0:12}… still reviewable, not superseded"
code=$(rj expected/06_reapprove.json fulfill approve-spec "$PRODUCT")
[ "$code" = "0" ] || fail "6 (re-approve exit $code; $(cat expected/06_reapprove.err))"
[ "$(jq -r .state expected/06_reapprove.json)" = "spec_approved" ] || fail "6 (re-approval did not re-enter spec_approved: the fence)"
[ "$(jq -r .spec_digest expected/06_reapprove.json)" = "$EDITED_DIGEST" ] || fail "6 (re-approval did not bind the edited digest)"
code=$(rj expected/06_reprop.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "6 (re-propose exit $code; $(cat expected/06_reprop.err))"
PLAN2="$(jq -r .plan_id expected/06_reprop.json)"
[ -n "$PLAN2" ] && [ "$PLAN2" != "null" ] || fail "6 (no new plan after re-approval)"
[ "$PLAN2" != "$PLAN1" ] || fail "6 (re-approval did not supersede: same plan id)"
# The approved digest the loop now carries is the edited one — assert 7 applies THIS.
APPROVED_DIGEST="$EDITED_DIGEST"
echo "    OK  re-approval fenced spec_approved and superseded plan ${PLAN1:0:12}… -> fresh plan ${PLAN2:0:12}…"

# ------------------------------------------------------------------ Assert 8
# ENGINE BACKSTOP IN ISOLATION. A fresh scratch product driven to a REVIEWED
# plan: a bare apply of a product-bound plan refuses; a WRONG --expect-spec-digest
# refuses; the RIGHT one executes. This proves the gate is the ENGINE's, not the
# loop's — the loop is bypassed by calling `rocky apply` directly.
echo; echo "[8] engine backstop (isolated): bare apply + wrong digest refuse, right applies"
S8="$(mktemp -d)"
mkdir -p "$S8/models" "$S8/replay" "$S8/data"
cp rocky.toml "$S8/rocky.toml"; cp models/_defaults.toml "$S8/models/_defaults.toml"
cp replay/session.json "$S8/replay/session.json"; cp data/seed.sql "$S8/data/seed.sql"
duckdb "$S8/wh.duckdb" < data/warehouse_seed.sql >/dev/null 2>&1
(
  cd "$S8"
  rocky --output json fulfill "$PRODUCT" >/dev/null 2>&1
  rocky --output json fulfill approve-spec "$PRODUCT" >/dev/null 2>&1
  rocky --output json fulfill "$PRODUCT" >drive.json 2>drive.err
)
P8="$(jq -r .plan_id "$S8/drive.json")"
[ -n "$P8" ] && [ "$P8" != "null" ] || fail "8 (isolated scratch never proposed a plan)"
GOOD8="$(jq -r .spec_digest "$S8/drive.json")"
( cd "$S8" && rocky review "$P8" --approve >/dev/null 2>&1 )   # marker present; digest is the ONLY gate left
( cd "$S8" && rocky apply "$P8" >bare.out 2>bare.err ); bare=$?
[ "$bare" != "0" ] || fail "8 (bare apply of a product-bound plan must refuse)"
grep -qiE 'expect-spec-digest|product-bound' "$S8/bare.err" || fail "8 (bare-apply refusal did not name --expect-spec-digest)"
# MUTATION 8: feed the CORRECT digest where the assert expects a wrong one ->
# the apply succeeds, so the "must refuse" assertion must fail.
WRONG8="sha256:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
mut 8 && WRONG8="$GOOD8"
( cd "$S8" && rocky apply "$P8" --expect-spec-digest "$WRONG8" >wrong.out 2>wrong.err ); wrong=$?
[ "$wrong" != "0" ] || fail "8 (WRONG --expect-spec-digest must refuse)"
grep -qiE 'digest' "$S8/wrong.err" || fail "8 (wrong-digest refusal did not mention the digest)"
( cd "$S8" && rocky apply "$P8" --expect-spec-digest "$GOOD8" >right.out 2>right.err ); right=$?
[ "$right" = "0" ] || fail "8 (correct --expect-spec-digest must apply; $(cat "$S8/right.err"))"
echo "    OK  bare refuse + wrong-digest refuse + right-digest apply (the engine compares, not the loop)"
rm -rf "$S8"

# ------------------------------------------------------------------ Assert 7
# APPROVAL GATE (main product). Before review: a bare apply of the pending plan
# refuses (no marker). Approve via `rocky review <id> --approve`; the LOOP then
# applies and reaches observing. The warehouse table exists with real rows.
echo; echo "[7] approval gate: bare apply refuses; review --approve -> loop applies"
( rocky apply "$PLAN2" >expected/07_bare.out 2>expected/07_bare.err ); bare=$?
[ "$bare" != "0" ] || fail "7 (bare apply of the un-reviewed plan must refuse)"
# MUTATION 7: skip the human review -> no sign-off marker, so the loop cannot
# leave needs_input(plan_approval) and never reaches observing.
if mut 7; then
  echo "    (mutation 7: skipping rocky review --approve)"
else
  code=$(rj expected/07_review.json review "$PLAN2" --approve)
  [ "$code" = "0" ] || fail "7 (review --approve exit $code; $(cat expected/07_review.err))"
fi
code=$(rj expected/07_apply.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "7 (apply+observe exit $code; $(cat expected/07_apply.err))"
STATE7="$(jq -r .state expected/07_apply.json)"
[ "$STATE7" = "observing" ] || fail "7 (state $STATE7 after apply, want observing)"
jq -e '.message | test("applied")' expected/07_apply.json >/dev/null || fail "7 (observing message does not report applied)"
ROWS="$(duckdb -csv -noheader wh.duckdb "SELECT COUNT(*) FROM out.${PRODUCT}" 2>/dev/null)"
[ "${ROWS:-0}" -ge 1 ] || fail "7 (warehouse table out.${PRODUCT} has no rows)"
echo "    OK  loop applied -> observing; out.${PRODUCT} has $ROWS row(s)"

# ------------------------------------------------------------------ Assert 9
# OUTPUT VALIDATION. `rocky test` runs the GENERATED contract tests on the
# materialised data. Assert the composite-unique grain test was GENERATED (in
# the merged sidecar) and that the model's tests RAN green — not just an overall
# tally. The grain [client_id] is non-vacuous: two distinct clients materialised.
echo; echo "[9] output validation: composite-unique grain test RUNS (declarative) + CLEAN"
# MUTATION 9: inject a DUPLICATE (client_id, day) row into the materialised output
# -> the declarative composite-unique test must FAIL (failed != 0). Mutates what
# the ENGINE reads (the warehouse table), not a shell-visible sidecar line.
mut 9 && duckdb wh.duckdb "INSERT INTO out.${PRODUCT} SELECT * FROM out.${PRODUCT} ORDER BY client_id, day LIMIT 1" >/dev/null 2>&1
# MUTATION 9e: append a MALFORMED declaration (a `unique` test with no column) to
# the sidecar -> it compiles but ERRORS at test time (errored != 0).
mut 9e && printf '\n[[tests]]\ntype = "unique"\n' >> "models/${PRODUCT}.toml"
# MUTATION 9w: append a WARNING-severity test that fails on the data -> Rocky
# counts it under `warned` (NOT `failed`) and STILL exits 0, so only the
# warned==0 / passed==total checks catch it (the freshness-style silent-pass hole).
mut 9w && printf '\n[[tests]]\ntype = "expression"\nexpression = "revenue_eur > 1000000"\nseverity = "warning"\n' >> "models/${PRODUCT}.toml"
# (a) the composite-unique test was GENERATED into the merged sidecar from the grain,
grep -q 'type = "composite"' "models/${PRODUCT}.toml" && grep -q 'kind = "unique"' "models/${PRODUCT}.toml" \
  || fail "9 (no composite-unique grain test in the merged sidecar)"
# (b) and the declarative run is CLEAN across ALL THREE failure dimensions. Plain
# `rocky test` executes only the model; the sidecar [[tests]] run solely under
# --declarative (against the warehouse). Rocky exits 0 when failed==0 && errored==0
# EVEN IF a warning-severity test failed (counted under `warned`), so a clean run
# needs: 0 errored, 0 failed, 0 WARNED, passed==total, the composite PASSING, exit 0.
code=$(rj expected/09_test.json test --models models/ --declarative)
DTOTAL="$(jq -r '.declarative.total // "err"' expected/09_test.json 2>/dev/null)"
DPASS="$(jq -r '.declarative.passed // "err"' expected/09_test.json 2>/dev/null)"
DFAIL="$(jq -r '.declarative.failed // "err"' expected/09_test.json 2>/dev/null)"
DERR="$(jq -r '.declarative.errored // "err"' expected/09_test.json 2>/dev/null)"
DWARN="$(jq -r '.declarative.warned // "err"' expected/09_test.json 2>/dev/null)"
[ "$DERR" = "0" ]  || fail "9 (declarative tests ERRORED=$DERR — a malformed declaration: $(jq -c '.declarative.results[]? | select(.status=="error") | {test_type, detail}' expected/09_test.json 2>/dev/null))"
[ "$DFAIL" = "0" ] || fail "9 (declarative tests failed=$DFAIL: $(jq -c '.declarative.results[]? | select(.status=="fail" and .severity!="warning")' expected/09_test.json 2>/dev/null))"
[ "$DWARN" = "0" ] || fail "9 (declarative tests WARNED=$DWARN — a warning-severity failure exits 0 but is NOT clean: $(jq -c '.declarative.results[]? | select(.severity=="warning" and .status!="pass") | {test_type, detail}' expected/09_test.json 2>/dev/null))"
[ "$DPASS" = "$DTOTAL" ] || fail "9 (declarative passed=$DPASS != total=$DTOTAL — some test did not pass)"
jq -e '.declarative.results[] | select(.test_type == "composite" and .status == "pass")' expected/09_test.json >/dev/null \
  || fail "9 (the composite-unique grain test did not run and pass under --declarative)"
[ "$code" = "0" ] || fail "9 (rocky test --declarative exit $code despite errored=0 failed=0 — investigate)"
PAIRS="$(duckdb -csv -noheader wh.duckdb "SELECT COUNT(*) FROM (SELECT DISTINCT client_id, day FROM out.${PRODUCT})" 2>/dev/null)"
[ "${PAIRS:-0}" -ge 3 ] || fail "9 (grain uniqueness is vacuous: <3 distinct (client_id, day) pairs)"
echo "    OK  $DTOTAL declarative tests, all pass ($DPASS/$DTOTAL; 0 failed, 0 errored, 0 warned); composite grain over $PAIRS distinct pairs"

# ------------------------------------------------------------------ Assert 10
# STALENESS (honest freshness NEGATIVE case; observed, not enforced). Fresh
# apply -> lag < budget. Backdate the materialised output, re-observe: the loop
# reports lag > budget (STALE). Both stops reach `observing` (exit 0): staleness
# is a runner FINDING, never a gate. The loop's journal (via product status) has
# grown and the state is observing.
echo; echo "[10] staleness: fresh observe (lag<budget), then stale after backdating (lag>budget)"
FRESH_LAG=$(jq -r '.message | capture("lag (?<l>[0-9]+)s").l' expected/07_apply.json 2>/dev/null)
FRESH_BUD=$(jq -r '.message | capture("budget (?<b>[0-9]+)s").b' expected/07_apply.json 2>/dev/null)
[ -n "$FRESH_LAG" ] && [ -n "$FRESH_BUD" ] || fail "10 (fresh observe did not report lag/budget)"
[ "$FRESH_LAG" -le "$FRESH_BUD" ] || fail "10 (fresh data should be within budget: lag $FRESH_LAG > budget $FRESH_BUD)"
# Age the materialised output beyond the budget, then re-observe (Observing -> Observe).
# MUTATION 10: skip the backdating -> the data stays fresh, so the "stale" branch
# never exceeds the budget and the assertion must fail.
mut 10 || duckdb wh.duckdb "UPDATE out.${PRODUCT} SET loaded_at = TIMESTAMP '2020-01-01 00:00:00'" >/dev/null 2>&1 || fail "10 (could not backdate the output)"
code=$(rj expected/10_stale.json fulfill "$PRODUCT")
[ "$code" = "0" ] || fail "10 (re-observe exit $code, staleness must NOT block; $(cat expected/10_stale.err))"
[ "$(jq -r .state expected/10_stale.json)" = "observing" ] || fail "10 (staleness must stay observing, got $(jq -r .state expected/10_stale.json))"
STALE_LAG=$(jq -r '.message | capture("lag (?<l>[0-9]+)s").l' expected/10_stale.json 2>/dev/null)
STALE_BUD=$(jq -r '.message | capture("budget (?<b>[0-9]+)s").b' expected/10_stale.json 2>/dev/null)
[ -n "$STALE_LAG" ] || fail "10 (stale re-observe did not report a lag)"
[ "$STALE_LAG" -gt "$STALE_BUD" ] || fail "10 (backdated data should be STALE: lag $STALE_LAG <= budget $STALE_BUD)"
# The loop's journal is readable via product status --output json (state + row count).
code=$(rj expected/10_status.json product status "$PRODUCT")
[ "$(jq -r .fulfill_state expected/10_status.json)" = "observing" ] || fail "10 (status fulfill_state not observing)"
JROWS=$(jq -r '.journal_rows' expected/10_status.json)
[ "${JROWS:-0}" -ge 10 ] || fail "10 (journal has only $JROWS rows; the loop did not record its history)"
echo "    OK  fresh lag ${FRESH_LAG}s < ${FRESH_BUD}s; stale lag ${STALE_LAG}s > ${STALE_BUD}s; journal=$JROWS rows"

echo
echo "=================================================================="
echo "POC complete: spec -> lowering -> propose -> human gate -> digest-gated apply,"
echo "with 6 refusal paths exercised (negatives, policy, supersession, backstop, staleness)."
echo "This is the MACHINERY proof (replay driver). run-live.sh is the capability proof."
echo "=================================================================="
