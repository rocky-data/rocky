---
title: Run Rocky on Kubernetes
description: "A Helm chart for self-hosting Rocky: one replica, replaced in place, on a persistent volume. What the chart refuses, and why each refusal exists."
sidebar:
  order: 7
---

The repository ships a Helm chart at [`deploy/helm/rocky/`](https://github.com/rocky-data/rocky/tree/main/deploy/helm/rocky). It is community-supported: an example to start from, not a supported deployment. It encodes the [deployment contract](/advanced/deployment-contract/) and refuses the settings that break it.

## Install

You create the Secret. The chart never creates one and accepts no secret value, so nothing secret reaches `helm template` output, the release state, or your shell history.

```bash
kubectl create namespace rocky
kubectl create secret generic rocky-serve -n rocky \
  --from-literal=token="$(openssl rand -hex 32)"

helm install rocky ./deploy/helm/rocky -n rocky \
  --set existingSecret.name=rocky-serve \
  --set persistence.storageClassName=<a block-backed class>
```

Then put a project on the volume, and reach it:

```bash
kubectl port-forward -n rocky svc/rocky 8080:8080
# http://localhost:8080/
```

## Put your project on the volume

The chart does not deliver your project. The image mounts it at `/data`, so `/data/rocky.toml` and `/data/models/` must exist. The state store is written to `/data/models/.rocky-state.redb` and the scheduler's files to `/data/.rocky/`, so both live on the volume by construction.

Two ways. Pick one.

**An init container that clones it.** The right answer when the project lives in git, because a new pod always starts from the repository.

```yaml
initContainers:
  - name: clone
    image: alpine/git:latest
    args: ["clone", "--depth", "1", "https://github.com/you/your-project", "/data"]
    volumeMounts:
      - name: data
        mountPath: /data
```

**A pre-populated volume.** Copy the project in once, before the server needs it.

:::caution
`kubectl cp` into the Rocky pod does not work. `kubectl cp` runs `tar` inside the target container, and the Rocky image is distroless — it has no `tar`, no shell, and no package manager.

```
$ kubectl cp ./my-project rocky/rocky-0:/data
OCI runtime exec failed: exec: "tar": executable file not found in $PATH
```

Copy through a throwaway pod that mounts the same claim and does have `tar`:

```bash
kubectl run loader -n rocky --image=busybox:1.36 --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"loader","image":"busybox:1.36",
    "command":["sleep","300"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}],
    "volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"rocky"}}]}}'
kubectl wait --for=condition=Ready pod/loader -n rocky --timeout=120s

kubectl cp ./my-project/. rocky/loader:/data
kubectl exec -n rocky loader -- chown -R 65532:65532 /data
kubectl delete pod loader -n rocky

kubectl rollout restart deploy/rocky -n rocky
```

The `chown` matters. The copy arrives owned by root, and the server runs as uid 65532 and must write the state store under `models/`.
:::

## What the chart refuses

Every refusal prints the reason. A schema can say a value is wrong; it cannot say why, so each rule lives in one place — the template that can explain itself.

| Setting | Refused | Why |
|---|---|---|
| `replicaCount` | anything but `1` | One scheduler per project. A second pod inherits the watermarks and none of the scheduler's memory, so both fire what is due |
| `strategy` | anything but `Recreate` | A rolling update overlaps two pods. That is two schedulers for the length of the rollout |
| `terminationGracePeriodSeconds` | at or below `drainTimeoutSeconds + 60` | See below |
| `persistence.accessMode` | anything but `ReadWriteOnce` | A shared-access volume invites the second writer |
| `persistence.storageClassName` | empty | Never inherited from the cluster default. See below |
| `scheduling.mode` | two schedulers at once | It is one value: `resident`, `cron` or `disabled` |
| `scheduling.cron.concurrencyPolicy` | anything but `Forbid` | A tick that starts while the last one runs is a second scheduler |
| `existingSecret.name` | empty | The server binds `0.0.0.0` in a pod, and that requires a token |
| `ingress.host` | empty when the Ingress is on | The host is passed as `--allowed-host`; without it the page is reachable by a name the server refuses |

### The stop grace must cover the kill grace

On shutdown the server waits `--drain-timeout-seconds` (default 60) for a running scheduled child. A child still running then is **not** killed at once: it gets its own `SIGTERM` and a **further fixed 60 seconds** before `SIGKILL`.

```
  drain the child   --drain-timeout-seconds, default 60
  then SIGTERM it   a further 60s, fixed, not configurable
  worst case        120s with the defaults
```

The chart defaults to 125 and refuses anything at or below `drainTimeoutSeconds + 60`, printing the arithmetic for your value.

### Name the storage class yourself

The contract's locking claims hold on **block** storage. An advisory `flock` on NFS or other network storage is unprobed, and `ReadWriteOnce` is an access mode, not proof of the filesystem underneath. The chart will not guess from the cluster default. Name a block-backed class, or point `persistence.existingClaim` at a claim you made deliberately.

### One scheduler, chosen once

`scheduling.mode` is a single value because two schedulers are two independent cursors over one set of pipelines.

```
  resident   the loop inside `rocky serve --scheduler`
  cron       a CronJob running `rocky tick` on the same volume
  disabled   neither
```

The advisory lock on `.rocky/tick.lock` does not make two of them safe. That lock is contention avoidance, not the correctness boundary: correctness lives in the claim state machine, and the contract's rule stands regardless.

In `cron` mode the CronJob mounts the same claim as the Deployment. On a multi-node cluster a `ReadWriteOnce` claim pins both to one node.

## The health probe is shallow

Both probes use `/api/v1/health`, which is exempt from the bearer token and from the host check. It answers as soon as the listener is bound, which happens after the startup sweep **attempt**.

It does **not** prove the sweep succeeded, that the project compiled, that the state store is readable, or that the scheduler is making progress. A project that fails to compile still answers `ok`. The chart's probe thresholds are conservative on purpose: a failed liveness probe restarts the pod, and a restart during a scheduled run is the loss window the contract describes.

## The browser UI

`serve.ui.enabled` defaults to `false`. `rocky serve --ui` and `--allowed-host` shipped after `engine-v1.73.0`, which is the chart's `appVersion`, so the default image cannot serve the page. Turn the UI on and raise `image.tag` together, once a release carries them.

With the UI on, the Ingress host is passed as `--allowed-host` and a request carrying any other `Host` is refused `421`. Note that a foreign host usually never reaches Rocky at all: it matches no Ingress rule, so the controller's own default backend answers `404` first.

## Upgrade and roll back

`helm upgrade` replaces the pod in place on the same claim. The volume carries the run history, the watermarks and the scheduler's cursors, so they survive the replacement. The PVC the chart creates is annotated `helm.sh/resource-policy: keep`, so uninstalling the release does not delete your project or its history.

Rolling back across a state-schema change follows the same rules as any other deployment. The [deployment contract](/advanced/deployment-contract/) and the [image guide](/guides/run-the-image/) cover what the engine does and does not promise.

## Related pages

- [Deployment contract](/advanced/deployment-contract/) — the rule this chart encodes, and the loss windows
- [Run the image](/guides/run-the-image/) — the container itself, and a Compose example
