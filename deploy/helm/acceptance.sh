#!/usr/bin/env bash
#
# C4a acceptance: the Rocky Helm chart, proved on a local cluster.
#
# Run it, do not read a summary of it. Every command and every result is
# printed, so the transcript is a byproduct of the run and not a retelling.
#
#   ./deploy/helm/acceptance.sh > deploy/helm/ACCEPTANCE.txt 2>&1
#
# What it needs:
#   - minikube (or any cluster kubectl points at) with an ingress-nginx
#     controller in the ingress-nginx namespace (`minikube addons enable ingress`),
#     helm, kubectl, docker
#   - by default, the chart's OWN default image: ghcr.io/rocky-data/rocky at the
#     chart's appVersion, pulled from the registry. That is the claim under test.
#     To run against a local build instead:
#       IMAGE_REPO=rocky IMAGE_TAG=local PULL_POLICY=Never ./deploy/helm/acceptance.sh
#     (build one from a release as docs/guides/run-the-image/ shows, then
#     `minikube image load rocky:local`).
#   - a project to put on the volume; see PROJECT_DIR below.
#
# What it CANNOT prove, and does not claim to:
#   - that the storage class is block-backed. A single node cannot settle it.
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
# The claim then never binds. Expect 13 passes (the ten refusal rows and the
# three render rows, which never touch the cluster), 26 failures, and exit 1 —
# measured 2026-09-15 against engine 1.74.0. An earlier draft returned 0 in
# that state, and a later one passed five rows with no pod running: a Pending
# pod counted as a pod, its uid changed on upgrade, an unbound claim name
# equalled itself, and the controller's 503 page matched `<html`. Those rows
# now assert the running STATE, not the declared shape.

set -uo pipefail

# Run from the repository root and use a RELATIVE chart path. The transcript
# is committed to a public repository, and an absolute path would carry the
# author's home directory and worktree name into it.
cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1
CHART="deploy/helm/rocky"
RELEASE=rocky-acc
NS=rocky-acceptance
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/rocky-data/rocky}"
# Empty means the chart's appVersion, which is the default under test.
IMAGE_TAG="${IMAGE_TAG:-}"
PULL_POLICY="${PULL_POLICY:-IfNotPresent}"
STORAGE_CLASS="${STORAGE_CLASS:-standard}"
INGRESS_HOST="${INGRESS_HOST:-rocky.acceptance.test}"
SECRET=rocky-serve

# The tag the run actually uses, for the provenance lines below.
APP_VERSION="$(helm show chart "$CHART" | awk '/^appVersion:/ { gsub(/"/, "", $2); print $2 }')"
EFFECTIVE_TAG="${IMAGE_TAG:-$APP_VERSION}"

# A project to put on the volume. Any Rocky project works; the default is one
# the image itself can generate:
#   mkdir -p /tmp/rocky-acc && docker run --rm -v /tmp/rocky-acc:/data \
#     ghcr.io/rocky-data/rocky:<tag> playground demo
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

# code <curl args...>: the HTTP status only, 000 when nothing answered.
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$@"; }

# Wait for a port-forward to answer, do not guess at it. A curl against a
# tunnel that is not up yet returns 000, and three 000s look like three
# failures of the server.
wait_tunnel() {
  local url="$1"; shift
  for _ in $(seq 1 30); do
    [ "$(code "$@" "$url")" != "000" ] && return 0
    sleep 1
  done
  return 1
}

PF_SVC=""
PF_ING=""
cleanup() {
  local rc=$?
  printf '\n\n== Cleanup\n'
  [ -n "$PF_SVC" ] && kill "$PF_SVC" 2>/dev/null
  [ -n "$PF_ING" ] && kill "$PF_ING" 2>/dev/null
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
printf '\nchart:       %s\n' "$CHART"
printf 'appVersion:  %s\n' "$APP_VERSION"
printf 'image:       %s:%s%s\n' "$IMAGE_REPO" "$EFFECTIVE_TAG" "$([ -z "$IMAGE_TAG" ] && printf '  (the chart default; no image.tag was set)')"
printf 'pull policy: %s\n' "$PULL_POLICY"
printf 'engine:      %s\n' "$(docker run --rm "$IMAGE_REPO:$EFFECTIVE_TAG" --version 2>&1)"
run "helm lint '$CHART' --set existingSecret.name=x --set persistence.storageClassName=$STORAGE_CLASS"

say "The Ingress controller the page is proved through"
# Row 1 is proved THROUGH the controller, so it must be up before the Ingress
# object is created: ingress-nginx admits Ingress objects with a webhook, and
# an install that races the controller fails on that webhook.
run "kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=180s"
run "kubectl get svc -n ingress-nginx ingress-nginx-controller"

BASE=(--set "image.repository=$IMAGE_REPO"
      --set "image.pullPolicy=$PULL_POLICY"
      --set "existingSecret.name=$SECRET"
      --set "persistence.storageClassName=$STORAGE_CLASS")
# The default under test is the chart's own tag, so image.tag is set ONLY when
# the caller asked for a different one.
[ -n "$IMAGE_TAG" ] && BASE+=(--set "image.tag=$IMAGE_TAG")
INGRESS=(--set ingress.enabled=true
         --set "ingress.host=$INGRESS_HOST"
         --set ingress.className=nginx)

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
refuses "an Ingress with the UI off is refused unless acknowledged" \
  "serve.ui.enabled is false" \
  "${BASE[@]}" "${INGRESS[@]}" --set serve.ui.enabled=false
refuses "the resident scheduler with the UI on needs a webhook secret" \
  "serve.ui.enabled with scheduling.mode=resident needs" \
  "${BASE[@]}" --set scheduling.mode=resident

# ---------------------------------------------------------------------------
say "Row 4  No secret reaches the rendered output"
# ---------------------------------------------------------------------------
RENDER="$(helm template "$RELEASE" "$CHART" "${BASE[@]}" "${INGRESS[@]}" 2>&1)"
check "the chart renders no Secret object at all" \
  0 "$(grep -c '^kind: Secret' <<<"$RENDER")"
check "the token is a secretKeyRef, not a value" \
  1 "$(grep -c 'secretKeyRef' <<<"$RENDER")"
check "no data: block that could carry one" \
  0 "$(grep -cE '^\s*(data|stringData):' <<<"$RENDER")"
printf '\n  the only mention of the secret is by name:\n'
grep -A3 secretKeyRef <<<"$RENDER" | sed 's/^/    | /'
printf '\n  the serve arguments the chart renders by default:\n'
sed -n '/^          args:/,/^          env:/p' <<<"$RENDER" | sed '$d' | sed 's/^/    | /'

# ---------------------------------------------------------------------------
say "Row 1  One pod, the page on the Ingress host, a foreign Host is 421"
# ---------------------------------------------------------------------------
# A previous run's cleanup deletes the namespace without waiting. Creating it
# again while it is still Terminating fails, and that failure would be counted
# against the chart.
kubectl wait --for=delete "ns/$NS" --timeout=120s >/dev/null 2>&1
run "kubectl create namespace $NS"
# The value never passes through helm. It is created here, read by name only.
run "kubectl create secret generic $SECRET -n $NS --from-literal=token=\"\$(openssl rand -hex 32)\""
run "helm install $RELEASE '$CHART' -n $NS ${BASE[*]} ${INGRESS[*]}"
# Never concatenate the name. rocky.fullname collapses when the release name
# already contains the chart name, so "$RELEASE-rocky" is wrong for this
# release and right for others. Ask the cluster.
DEPLOY="$(kubectl get deploy -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
printf '\n  deployment: %s\n' "$DEPLOY"
run "kubectl rollout status deploy/$DEPLOY -n $NS --timeout=300s"

# Running pods only. A Pending pod is still a pod, so a bare count passed
# under the mutation check with no volume bound and nothing serving.
running_pods() { kubectl get pods -n "$NS" -l app.kubernetes.io/instance=$RELEASE --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c .; }
check "exactly one RUNNING pod" 1 "$(running_pods)"
POD1="$(kubectl get pod -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
UID1="$(kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.metadata.uid}')"
printf '  pod  %s\n  uid  %s\n' "$POD1" "$UID1"
# The BOUND volume, not the claim name in the pod spec: the spec names the
# claim whether or not anything ever bound to it.
PV_BEFORE="$(kubectl get pvc -n "$NS" -o jsonpath='{.items[0].spec.volumeName}')"

say "The image it actually runs"
# The chart's default is only proved if the registry served it. The image ID
# names the registry and the digest the kubelet pulled; a locally loaded image
# would carry docker.io or a bare name here.
IMAGE_RUNNING="$(kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.status.containerStatuses[0].image}')"
IMAGE_ID="$(kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.status.containerStatuses[0].imageID}')"
printf '  image    %s\n  imageID  %s\n' "$IMAGE_RUNNING" "$IMAGE_ID"
if [ "$PULL_POLICY" != "Never" ]; then
  check "the running image is the chart default, by digest, from the registry" \
    1 "$(grep -c "^$IMAGE_REPO@sha256:" <<<"$IMAGE_ID")"
else
  printf '  note  pull policy Never: the default-image row is not claimed by this run\n'
fi
printf '\n  the arguments the pod was started with:\n'
kubectl get pod -n "$NS" "$POD1" -o jsonpath='{.spec.containers[0].args}' | sed 's/^/    | /'; printf '\n'

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

say "The page answers on the Ingress host, THROUGH the controller"
# Through the controller, not straight at the Service: ingress-nginx routes by
# Host, so this is the path a browser takes. The Host header stands in for DNS.
kubectl port-forward -n ingress-nginx svc/ingress-nginx-controller 18081:80 >/dev/null 2>&1 &
PF_ING=$!
wait_tunnel "http://127.0.0.1:18081/healthz" || printf '  FAIL  the controller tunnel never answered\n'
check "GET /ui/ on the Ingress host, through the controller" \
  200 "$(code -H "Host: $INGRESS_HOST" http://127.0.0.1:18081/ui/)"
# Rocky's page, by its own title. nginx's error pages are HTML too, so a bare
# `<html` match passed under the mutation check against a 503 from the
# controller with no endpoint behind it.
check "and it is Rocky's page, not the controller's error body" \
  1 "$(curl -s --max-time 5 -H "Host: $INGRESS_HOST" http://127.0.0.1:18081/ui/ | grep -c -m1 '<title>Rocky</title>')"
check "GET /api/v1/health on the Ingress host, through the controller" \
  200 "$(code -H "Host: $INGRESS_HOST" http://127.0.0.1:18081/api/v1/health)"
printf '  note  a foreign Host through the controller answers %s. That is the\n' \
  "$(code -H 'Host: evil.example' http://127.0.0.1:18081/ui/)"
printf '        controller'"'"'s default backend, not Rocky, so it proves nothing about\n'
printf '        the server'"'"'s host guard. The 421 is asserted below, straight at the Service.\n'
kill $PF_ING 2>/dev/null; PF_ING=""

say "The API answers, the token is enforced, a foreign Host is 421"
kubectl port-forward -n "$NS" "svc/$DEPLOY" 18080:8080 >/dev/null 2>&1 &
PF_SVC=$!
wait_tunnel "http://127.0.0.1:18080/api/v1/health" || printf '  FAIL  the service tunnel never answered\n'
check "GET /api/v1/health with no token" 200 "$(code http://127.0.0.1:18080/api/v1/health)"
check "GET /api/v1/runs with no token" 401 "$(code http://127.0.0.1:18080/api/v1/runs)"
TOKEN="$(kubectl get secret "$SECRET" -n "$NS" -o jsonpath='{.data.token}' | base64 -d)"
check "GET /api/v1/runs with the token" 200 "$(code -H "Authorization: Bearer $TOKEN" http://127.0.0.1:18080/api/v1/runs)"
check "GET /api/v1/project with the token" 200 "$(code -H "Authorization: Bearer $TOKEN" http://127.0.0.1:18080/api/v1/project)"
check "GET /ui/ with a foreign Host is refused 421" \
  421 "$(code -H 'Host: evil.example' http://127.0.0.1:18080/ui/)"
check "GET /api/v1/runs with a foreign Host and the token is refused 421" \
  421 "$(code -H 'Host: evil.example' -H "Authorization: Bearer $TOKEN" http://127.0.0.1:18080/api/v1/runs)"
check "GET /api/v1/health with a foreign Host still answers (the probe exemption)" \
  200 "$(code -H 'Host: evil.example' http://127.0.0.1:18080/api/v1/health)"
kill $PF_SVC 2>/dev/null; PF_SVC=""

# ---------------------------------------------------------------------------
say "Row 2  helm upgrade replaces the pod, and the state survives"
# ---------------------------------------------------------------------------
run "helm upgrade $RELEASE '$CHART' -n $NS ${BASE[*]} ${INGRESS[*]} --set podAnnotations.acceptance=upgrade-2"
run "kubectl rollout status deploy/$DEPLOY -n $NS --timeout=180s"
POD2="$(kubectl get pod -n "$NS" -l app.kubernetes.io/instance=$RELEASE -o jsonpath='{.items[0].metadata.name}')"
UID2="$(kubectl get pod -n "$NS" "$POD2" -o jsonpath='{.metadata.uid}')"
PHASE2="$(kubectl get pod -n "$NS" "$POD2" -o jsonpath='{.status.phase}')"
printf '  pod before  %s  %s\n  pod after   %s  %s  (%s)\n' "$POD1" "$UID1" "$POD2" "$UID2" "$PHASE2"
# A new uid alone is not a replacement: a Pending pod is re-created on upgrade
# too and never ran. The replacement must be RUNNING.
if [ "$UID1" != "$UID2" ] && [ "$PHASE2" = "Running" ]; then
  printf '  PASS  the pod was really replaced, and the replacement runs\n'; PASS=$((PASS + 1))
else
  printf '  FAIL  uid %s -> %s, phase %s: not a running replacement, so this row proves nothing\n' "$UID1" "$UID2" "$PHASE2"; FAIL=$((FAIL + 1))
fi
check "still exactly one RUNNING pod" 1 "$(running_pods)"
PV_AFTER="$(kubectl get pvc -n "$NS" -o jsonpath='{.items[0].spec.volumeName}')"
if [ -n "$PV_BEFORE" ] && [ "$PV_BEFORE" = "$PV_AFTER" ]; then
  printf '  PASS  the same bound volume is mounted  (%s)\n' "$PV_AFTER"; PASS=$((PASS + 1))
else
  printf '  FAIL  bound volume: %s before, %s after (empty means nothing ever bound)\n' "${PV_BEFORE:-<none>}" "${PV_AFTER:-<none>}"; FAIL=$((FAIL + 1))
fi
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
printf '    - a block-backed storage class   (a single node cannot settle it)\n'
printf '    - the same run as a CI job       (this transcript is the interim evidence row 1 allows)\n'
[ "$FAIL" -eq 0 ]
