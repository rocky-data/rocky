#!/usr/bin/env bash
#
# C4a acceptance: the Rocky Helm chart, proved on a local cluster.
#
# Run it, do not read a summary of it. Every command and every result is
# printed, so the transcript is a byproduct of the run and not a retelling.
#
#   ./deploy/helm/acceptance.sh 2>&1 | tee deploy/helm/ACCEPTANCE.txt
#
# What it needs:
#   - minikube (or any cluster kubectl points at), helm, kubectl, docker
#   - an image the cluster can pull. Build one from a release:
#       mkdir -p ctx/arm64
#       gh release download engine-v1.73.0 --repo rocky-data/rocky \
#         --pattern rocky-aarch64-unknown-linux-gnu.tar.gz --dir ctx
#       tar xzf ctx/rocky-aarch64-unknown-linux-gnu.tar.gz -C ctx/arm64
#       docker buildx build --platform linux/arm64 -f engine/Dockerfile \
#         -t rocky:local --load ctx
#       minikube image load rocky:local
#
# What it CANNOT prove, and does not claim to:
#   - the page answering, and the 421 on a foreign Host. Both need --ui and
#     --allowed-host, which shipped after engine-v1.73.0 (rocky #1687).
#   - that the chart's DEFAULT image tag is pullable from GHCR. The run
#     overrides the image, so a broken default is invisible here.
#   These are row 1's release half and stay owed. A CI job closes them.
#
# No secret value is passed to helm at any point, so nothing secret can reach
# this transcript. No absolute path is printed either: the transcript is
# committed to a public repository.
#
# This harness is mutation-checked, because a script that can only report
# success is worth nothing. Run it with a storage class that does not exist:
#
#   STORAGE_CLASS=this-class-does-not-exist ./deploy/helm/acceptance.sh
#
# The claim then never binds. Expect 14 failures and exit 1: the eight refusal
# rows still pass, because they render and never touch the cluster, and every
# row that needs a running pod fails. An earlier draft of this script returned
# 0 in that state.

set -uo pipefail

# Run from the repository root and use a RELATIVE chart path. The transcript
# is committed to a public repository, and an absolute path would carry the
# author's home directory and worktree name into it.
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1
CHART="deploy/helm/rocky"
RELEASE=rocky-acc
NS=rocky-acceptance
IMAGE_REPO="${IMAGE_REPO:-rocky}"
IMAGE_TAG="${IMAGE_TAG:-local}"
STORAGE_CLASS="${STORAGE_CLASS:-standard}"
SECRET=rocky-serve

# A project to put on the volume. Any Rocky project works; the default is one
# the image itself can generate:
#   mkdir -p /tmp/rocky-acc && docker run --rm -v /tmp/rocky-acc:/data \
#     rocky:local playground demo
PROJECT_DIR="${PROJECT_DIR:-/tmp/rocky-acc/demo}"

PASS=0
FAIL=0

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# Every command is echoed, and a non-zero exit is COUNTED, not swallowed. The
# first draft of this script let `kubectl rollout status` fail against a
# misspelled deployment name and carried on; four later rows then failed for a
# reason that had nothing to do with the chart.
run() {
  printf '\n$ %s\n' "$*"
  eval "$@"
  local rc=$?
  if [ $rc -ne 0 ]; then
    printf '  FAIL  the command above exited %d\n' "$rc"; FAIL=$((FAIL + 1))
  fi
  return $rc
}

# check <name> <expected> <actual>
check() {
  if [ "$2" = "$3" ]; then
    printf '  PASS  %s  (%s)\n' "$1" "$3"; PASS=$((PASS + 1))
  else
    printf '  FAIL  %s  expected %s, got %s\n' "$1" "$2" "$3"; FAIL=$((FAIL + 1))
  fi
}

# refuses <name> <needle> <helm args...>
# The render must fail AND the message must contain the needle. A refusal that
# fires with the wrong reason is not the refusal we wrote.
refuses() {
  local name="$1" needle="$2"; shift 2
  local out rc
  out="$(helm template "$RELEASE" "$CHART" "$@" 2>&1)"; rc=$?
  if [ $rc -eq 0 ]; then
    printf '  FAIL  %s  rendered instead of refusing\n' "$name"; FAIL=$((FAIL + 1))
  elif ! grep -qF "$needle" <<<"$out"; then
    printf '  FAIL  %s  refused, but not for our reason\n' "$name"
    printf '        wanted: %s\n' "$needle"
    printf '%s\n' "$out" | sed 's/^/        | /' | head -6
    FAIL=$((FAIL + 1))
  else
    printf '  PASS  %s\n' "$name"
    printf '%s\n' "$out" | grep -F "$needle" | sed 's/^/        | /'
    PASS=$((PASS + 1))
  fi
}

cleanup() {
  local rc=$?
  printf '\n\n== Cleanup\n'
  helm uninstall "$RELEASE" -n "$NS" >/dev/null 2>&1
  kubectl delete ns "$NS" --wait=false >/dev/null 2>&1
  # Never let cleanup decide the exit status. The first draft ended the
  # trap with `true`, so the script exited 0 with two failures on screen.
  exit $rc
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
say "0  Provenance"
# ---------------------------------------------------------------------------
run "date -u +%Y-%m-%dT%H:%M:%SZ"
run "helm version --short"
run "kubectl version --client=true --output=yaml > /dev/null && kubectl version --client=true | head -2"
run "minikube version --short"
run "kubectl config current-context"
run "kubectl get nodes -o wide"
printf '\nchart:  %s\n' "$CHART"
printf 'image:  %s:%s\n' "$IMAGE_REPO" "$IMAGE_TAG"
printf 'engine: %s\n' "$(docker run --rm "$IMAGE_REPO:$IMAGE_TAG" --version 2>&1)"
run "helm lint '$CHART' --set existingSecret.name=x --set persistence.storageClassName=$STORAGE_CLASS"

BASE=(--set "image.repository=$IMAGE_REPO"
      --set "image.tag=$IMAGE_TAG"
      --set image.pullPolicy=Never
      --set "existingSecret.name=$SECRET"
      --set "persistence.storageClassName=$STORAGE_CLASS")

# ---------------------------------------------------------------------------
say "Row 3  Every refusal fires, and says why"
# ---------------------------------------------------------------------------
refuses "replicaCount=2 is refused with the contract's sentence" \
  "One scheduler per project, one replica, replaced in place, on a persistent volume." \
  "${BASE[@]}" --set replicaCount=2
refuses "strategy=RollingUpdate is refused" \
  "only Recreate is allowed" \
  "${BASE[@]}" --set strategy=RollingUpdate
refuses "a grace at or below drain+60 is refused, with the arithmetic" \
  "It must be greater than 360" \
  "${BASE[@]}" --set scheduling.mode=resident \
  --set scheduling.resident.drainTimeoutSeconds=300 \
  --set terminationGracePeriodSeconds=200 \
  --set existingSecret.webhookSecretKey=w
refuses "accessMode=ReadWriteMany is refused" \
  "only ReadWriteOnce is allowed" \
  "${BASE[@]}" --set persistence.accessMode=ReadWriteMany
refuses "a cron tick that may overlap itself is refused" \
  "contention avoidance, not the correctness boundary" \
  "${BASE[@]}" --set scheduling.mode=cron --set scheduling.cron.concurrencyPolicy=Allow
refuses "an unnamed Secret is refused" \
  "existingSecret.name is empty" \
  --set "image.repository=$IMAGE_REPO" --set "persistence.storageClassName=$STORAGE_CLASS"
refuses "an inherited storage class is refused" \
  "storageClassName is empty" \
  --set "image.repository=$IMAGE_REPO" --set "existingSecret.name=$SECRET"
refuses "an Ingress with no host is refused" \
  "ingress.host is empty" \
  "${BASE[@]}" --set ingress.enabled=true

# ---------------------------------------------------------------------------
say "Row 4  No secret reaches the rendered output"
# ---------------------------------------------------------------------------
RENDER="$(helm template "$RELEASE" "$CHART" "${BASE[@]}" 2>&1)"
check "the chart renders no Secret object at all" \
  0 "$(grep -c '^kind: Secret' <<<"$RENDER")"
check "the token is a secretKeyRef, not a value" \
  1 "$(grep -c 'secretKeyRef' <<<"$RENDER")"
check "no data: block that could carry one" \
  0 "$(grep -cE '^\s*(data|stringData):' <<<"$RENDER")"
printf '\n  the only mention of the secret is by name:\n'
grep -A3 secretKeyRef <<<"$RENDER" | sed 's/^/    | /'

# ---------------------------------------------------------------------------
say "Row 1 (partial)  One pod, and the API answers"
# ---------------------------------------------------------------------------
run "kubectl create namespace $NS"
# The value never passes through helm. It is created here, read by name only.
run "kubectl create secret generic $SECRET -n $NS --from-literal=token=\"\$(openssl rand -hex 32)\""
run "helm install $RELEASE '$CHART' -n $NS ${BASE[*]}"
# Never concatenate the name. rocky.fullname collapses when the release name
# already contains the chart name, so "$RELEASE-rocky" is wrong for this
# release and right for others. Ask the cluster.
DEPLOY="$(kubectl get deploy -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
printf '\n  deployment: %s\n' "$DEPLOY"
run "kubectl rollout status deploy/$DEPLOY -n $NS --timeout=180s"

check "exactly one pod" 1 "$(kubectl get pods -n "$NS" -l app.kubernetes.io/instance=$RELEASE --no-headers 2>/dev/null | grep -c .)"
POD1="$(kubectl get pod -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
UID1="$(kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.metadata.uid}')"
printf '  pod  %s\n  uid  %s\n' "$POD1" "$UID1"
CLAIM_BEFORE="$(kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.spec.volumes[0].persistentVolumeClaim.claimName}')"

say "The volume it actually bound"
run "kubectl get pvc -n $NS -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,VOLUME:.spec.volumeName,CLASS:.spec.storageClassName,MODE:.spec.accessModes"
PV="$(kubectl get pvc -n "$NS" -o jsonpath='{.items[0].spec.volumeName}')"
run "kubectl get pv $PV -o custom-columns=NAME:.metadata.name,DRIVER:.spec.hostPath.path,RECLAIM:.spec.persistentVolumeReclaimPolicy,CLASS:.spec.storageClassName"
printf '\n  NOTE  a single-node cluster cannot settle whether a class is block-backed.\n'
printf '        The contract limits its locking claims to block storage, and flock\n'
printf '        on network storage is unprobed. That stays an operator decision.\n'

say "The project, delivered to the volume"
# NOT `kubectl cp` into the Rocky pod. That needs tar inside the target
# container, and the image is distroless:
#
#   $ kubectl cp ./project ns/rocky-pod:/data
#   OCI runtime exec failed: exec: "tar": executable file not found in $PATH
#
# The published guide tells readers to do exactly that
# (docs/.../guides/run-the-image.md, the minikube section). It cannot work.
#
# This is the documented "pre-populated PVC" option instead: a throwaway pod
# that has tar mounts the same claim, takes the copy, and goes away. On one
# node a second pod may mount an RWO claim, so the server keeps running.
printf '\n  why not kubectl cp into the server: the image is distroless and has no tar\n'
run "kubectl run loader -n $NS --image=busybox:1.36 --restart=Never --overrides='{\"spec\":{\"containers\":[{\"name\":\"loader\",\"image\":\"busybox:1.36\",\"command\":[\"sleep\",\"300\"],\"volumeMounts\":[{\"name\":\"data\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"data\",\"persistentVolumeClaim\":{\"claimName\":\"$(kubectl get pvc -n "$NS" -o jsonpath='{.items[0].metadata.name}')\"}}]}}'"
run "kubectl wait --for=condition=Ready pod/loader -n $NS --timeout=120s"
# The trailing /. copies the CONTENTS, so rocky.toml lands at /data/rocky.toml.
printf '\n$ kubectl cp <your project>/. %s/loader:/data\n' "$NS"
kubectl cp "$PROJECT_DIR/." "$NS/loader:/data"
if [ $? -ne 0 ]; then printf '  FAIL  the copy failed\n'; FAIL=$((FAIL + 1)); fi
# The copy arrives root-owned. The server runs as 65532 and must write the
# state store under models/, so hand the tree over.
run "kubectl exec -n $NS loader -- chown -R 65532:65532 /data"
run "kubectl exec -n $NS loader -- ls -la /data"
run "kubectl delete pod loader -n $NS --wait=true"
# The server compiled an empty /data at startup. Restart it so it sees the
# project, exactly as an operator would after populating the volume.
run "kubectl rollout restart deploy/$DEPLOY -n $NS"
run "kubectl rollout status deploy/$DEPLOY -n $NS --timeout=180s"

say "Write real state, so the upgrade row is not vacuous"
run "kubectl exec -n $NS deploy/$DEPLOY -- /usr/local/bin/rocky --version"
run "kubectl exec -n $NS deploy/$DEPLOY -- /usr/local/bin/rocky run --output json | tail -c 600"
RUNS_BEFORE="$(kubectl exec -n "$NS" "deploy/$DEPLOY" -- /usr/local/bin/rocky history --output json 2>/dev/null | grep -c '"run_id"')"
printf '\n  run records on the volume before the upgrade: %s\n' "$RUNS_BEFORE"
# A zero here makes the upgrade row vacuous, so it is a failure, not a pass.
if [ "$RUNS_BEFORE" -gt 0 ]; then
  printf '  PASS  real state exists before the upgrade  (%s run records)\n' "$RUNS_BEFORE"; PASS=$((PASS + 1))
else
  printf '  FAIL  no run record was written, so the upgrade row below would prove nothing\n'; FAIL=$((FAIL + 1))
fi

say "The API answers, and the token is enforced"
kubectl port-forward -n "$NS" "svc/$DEPLOY" 18080:8080 >/dev/null 2>&1 &
PF=$!
# Wait for the tunnel, do not guess at it. A curl against a tunnel that is not
# up yet returns 000, and three 000s look like three failures of the server.
for _ in $(seq 1 30); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:18080/api/v1/health)" != "000" ] && break
  sleep 1
done
check "GET /api/v1/health with no token" 200 "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/api/v1/health)"
# /api/v1/runs, not /api/v1/project: the project route is part of the UI-era
# API and answers 404 on this build. Asserting 200 there would be asserting
# the release's surface against a binary that predates it.
check "GET /api/v1/runs with no token" 401 "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18080/api/v1/runs)"
TOKEN="$(kubectl get secret "$SECRET" -n "$NS" -o jsonpath='{.data.token}' | base64 -d)"
check "GET /api/v1/runs with the token" 200 "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" http://127.0.0.1:18080/api/v1/runs)"
PROJECT_CODE="$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOKEN" http://127.0.0.1:18080/api/v1/project)"
printf '  note  /api/v1/project answers %s here. It is a UI-era route and this\n' "$PROJECT_CODE"
printf '        binary predates it. The 401 above still proves the token is enforced.\n'
printf '\n  GATED  "the page answers on the Ingress host" and "a foreign Host is 421"\n'
printf '         need --ui and --allowed-host. Neither exists in engine-v1.73.0.\n'
printf '         Proof:\n'
docker run --rm "$IMAGE_REPO:$IMAGE_TAG" serve --allowed-host x --help 2>&1 | head -2 | sed 's/^/         | /'
kill $PF 2>/dev/null

# ---------------------------------------------------------------------------
say "Row 2  helm upgrade replaces the pod, and the state survives"
# ---------------------------------------------------------------------------
run "helm upgrade $RELEASE '$CHART' -n $NS ${BASE[*]} --set podAnnotations.acceptance=upgrade-2"
run "kubectl rollout status deploy/$DEPLOY -n $NS --timeout=180s"
POD2="$(kubectl get pod -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
UID2="$(kubectl get pod -n "$NS" "$POD2" -o jsonpath='{.metadata.uid}')"
printf '  pod before  %s  %s\n  pod after   %s  %s\n' "$POD1" "$UID1" "$POD2" "$UID2"
if [ "$UID1" != "$UID2" ]; then
  printf '  PASS  the pod was really replaced\n'; PASS=$((PASS + 1))
else
  printf '  FAIL  same pod uid: the upgrade changed nothing, so this row proves nothing\n'; FAIL=$((FAIL + 1))
fi
check "still exactly one pod" 1 "$(kubectl get pods -n "$NS" -l app.kubernetes.io/instance=$RELEASE --no-headers 2>/dev/null | grep -c .)"
check "the same claim is mounted" "$CLAIM_BEFORE" \
  "$(kubectl get pod -n "$NS" "$POD2" -o jsonpath='{.spec.volumes[0].persistentVolumeClaim.claimName}')"
RUNS_AFTER="$(kubectl exec -n "$NS" "deploy/$DEPLOY" -- /usr/local/bin/rocky history --output json 2>/dev/null | grep -c '"run_id"')"
if [ "$RUNS_AFTER" -gt 0 ] && [ "$RUNS_AFTER" = "$RUNS_BEFORE" ]; then
  printf '  PASS  the run history survived the replacement  (%s records, both sides)\n' "$RUNS_AFTER"; PASS=$((PASS + 1))
else
  printf '  FAIL  run history: %s before, %s after\n' "$RUNS_BEFORE" "$RUNS_AFTER"; FAIL=$((FAIL + 1))
fi

# ---------------------------------------------------------------------------
say "Result"
# ---------------------------------------------------------------------------
printf '  passed %d\n  failed %d\n' "$PASS" "$FAIL"
printf '\n  still owed, and not claimed above:\n'
printf '    - the page, and the 421 on a foreign Host  (needs an engine release with --ui)\n'
printf '    - the chart default image pulled from GHCR (needs the package to be public)\n'
printf '    - a block-backed storage class             (a single node cannot settle it)\n'
[ "$FAIL" -eq 0 ]
