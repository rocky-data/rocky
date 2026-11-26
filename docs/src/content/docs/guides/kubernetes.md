---
title: Run Rocky on Kubernetes
description: "A Helm chart for self-hosting Rocky: one replica, replaced in place, on a persistent volume. What the chart refuses, and why each refusal exists."
sidebar:
  order: 12
---

The repository ships a Helm chart at [`deploy/helm/rocky/`](https://github.com/rocky-data/rocky/tree/main/deploy/helm/rocky). It is community-supported: an example to start from, not a supported deployment. It encodes the [deployment contract](/advanced/deployment-contract/) and refuses the settings that break it.

## Install

You create the Secret. The chart creates none and accepts no secret value, so no secret reaches `helm template` output, release state or shell history.

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
# http://localhost:8080/login?t=<token>
```

## Put your project on the volume

The chart does not deliver your project. `/data/rocky.toml` and `/data/models/` must exist. The state store (`/data/models/.rocky-state.redb`) and the scheduler's files (`/data/.rocky/`) live on the volume by construction.

**An init container that clones it.** Use this when the project lives in git: a new pod always starts from the repository.

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
`kubectl cp` into the Rocky pod fails (`exec: "tar": executable file not found`): it runs `tar` in the target container, and the distroless image has no `tar` or shell. Copy through a throwaway pod that mounts the same claim and does have `tar`:

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

The `chown` matters: the copy arrives owned by root, and the server runs as uid 65532.
:::

## What the chart refuses

Every refusal prints its reason.

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

On shutdown the server waits `--drain-timeout-seconds` (default 60) for a scheduled child, then sends it `SIGTERM` and waits a **further fixed 60 seconds** before `SIGKILL`. See [Stop and drain](/guides/run-the-image/#stop-and-drain). The chart defaults to 125 and refuses anything at or below `drainTimeoutSeconds + 60`, printing the arithmetic.

### Name the storage class yourself

The contract's locking claims hold on **block** storage. An advisory `flock` on NFS or other network storage is unprobed, and `ReadWriteOnce` does not prove the filesystem. Name a block-backed class, or point `persistence.existingClaim` at a claim you made deliberately.

### One scheduler, chosen once

`scheduling.mode` is a single value because two schedulers are two independent cursors over one set of pipelines.

```
  resident   the loop inside `rocky serve --scheduler`
  cron       a CronJob running `rocky tick` on the same volume
  disabled   neither
```

The advisory lock on `.rocky/tick.lock` does not make two safe: it avoids contention, and correctness lives in the claim state machine. In `cron` mode the CronJob mounts the same claim as the Deployment, so on a multi-node cluster a `ReadWriteOnce` claim pins both to one node.

## The health probe is shallow

Both probes use `/api/v1/health`, exempt from the bearer token and the host check. It answers once the listener is bound, after the startup sweep **attempt**. It does **not** prove the sweep succeeded, the project compiled, the state store is readable or the scheduler is progressing: a project that fails to compile still answers `ok`. The thresholds are conservative on purpose, because a restart during a scheduled run is the loss window the contract describes.

## The browser UI

`serve.ui.enabled` defaults to `true` and serves the page at `/ui/` (the default image is the chart's `appVersion`). Set it to `false` for the API alone. With `scheduling.mode=resident` the UI also needs `existingSecret.webhookSecretKey`, so the scheduler's webhook route is not reachable with the page's read-only token.

With the UI on, the Ingress host is passed as `--allowed-host` and any other `Host` is refused `421`. A foreign host usually never reaches Rocky: it matches no Ingress rule and the controller's default backend answers `404`.

## Upgrade and roll back

`helm upgrade` replaces the pod in place on the same claim, which carries the run history, watermarks and scheduler cursors. The chart's PVC is annotated `helm.sh/resource-policy: keep`, so uninstalling does not delete your project or its history.

:::caution[A reinstall adopts the old volume]
`helm uninstall` leaves the claim, so installing again with the same release name and namespace **adopts it**, with the old project and state store. To start clean, delete the claim (the run history goes with it):

```bash
helm uninstall rocky -n rocky
kubectl delete pvc rocky -n rocky
```

Run `kubectl get pvc -n rocky` before you reinstall.
:::

Rolling back across a state-schema change follows the same rules as any other deployment. The [deployment contract](/advanced/deployment-contract/) and the [image guide](/guides/run-the-image/) cover what the engine does and does not promise.

## Related pages

- [Deployment contract](/advanced/deployment-contract/) — the rule this chart encodes, and the loss windows
- [Run the image](/guides/run-the-image/) — the container itself, and a Compose example
