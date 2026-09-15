{{/*
The refusals.

values.schema.json can say a value is wrong. It cannot say why. These carry the
deployment contract's own sentences, so an operator reads the reason and not
just the rejected value.

Rendered by every template through `rocky.guards`, so `helm template` fails as
loudly as `helm install`.
*/}}

{{- define "rocky.guards" -}}
{{- include "rocky.guard.replicas" . -}}
{{- include "rocky.guard.strategy" . -}}
{{- include "rocky.guard.scheduling" . -}}
{{- include "rocky.guard.grace" . -}}
{{- include "rocky.guard.secret" . -}}
{{- include "rocky.guard.storage" . -}}
{{- include "rocky.guard.ingress" . -}}
{{- end -}}

{{- define "rocky.guard.replicas" -}}
{{- if ne (int .Values.replicaCount) 1 -}}
{{- fail (printf "\nrocky: replicaCount is %d.\n\nOne scheduler per project, one replica, replaced in place, on a persistent volume.\n\nThe state store keeps tables that describe one machine: the reconciler's cursors\nand claims, the HTTP job records, the fulfillment loop's state, the product\napprovals and the schema cache. A second pod inherits the watermarks and none of\nthe scheduler's memory. Two schedulers are two independent cursors over one set\nof pipelines, and both fire what is due.\n\nSee docs/advanced/deployment-contract/." (int .Values.replicaCount)) -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.strategy" -}}
{{- if ne .Values.strategy "Recreate" -}}
{{- fail (printf "\nrocky: strategy is %q, and only Recreate is allowed.\n\nOne replica, REPLACED IN PLACE. A RollingUpdate starts the new pod before the\nold one exits, so two processes hold the same project for the overlap. That is\ntwo schedulers, and the same volume cannot make it one.\n\nSee docs/advanced/deployment-contract/." .Values.strategy) -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.scheduling" -}}
{{- $mode := .Values.scheduling.mode -}}
{{- if not (has $mode (list "resident" "cron" "disabled")) -}}
{{- fail (printf "\nrocky: scheduling.mode is %q. It must be resident, cron or disabled.\n\nIt is one value on purpose. Two of them is two schedulers." $mode) -}}
{{- end -}}
{{- if and (eq $mode "cron") (ne .Values.scheduling.cron.concurrencyPolicy "Forbid") -}}
{{- fail (printf "\nrocky: scheduling.cron.concurrencyPolicy is %q, and only Forbid is allowed.\n\nA second tick that starts while the first is running is a second scheduler.\nThe advisory flock on .rocky/tick.lock does not make it safe: the lock is\ncontention avoidance, not the correctness boundary\n(rocky-core/src/schedule/lock.rs)." .Values.scheduling.cron.concurrencyPolicy) -}}
{{- end -}}
{{- if and (eq $mode "cron") (not (or .Values.persistence.existingClaim .Values.persistence.storageClassName)) -}}
{{- fail "\nrocky: scheduling.mode is cron and there is no persistent volume.\n\nA tick reads and writes the same project: the state store, the cursors, the\nclaims and the spool. Without the volume every tick starts from nothing." -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.grace" -}}
{{- $drain := 0 -}}
{{- if eq .Values.scheduling.mode "resident" -}}
{{- $drain = int .Values.scheduling.resident.drainTimeoutSeconds -}}
{{- end -}}
{{- $floor := add $drain 60 -}}
{{- if le (int .Values.terminationGracePeriodSeconds) $floor -}}
{{- fail (printf "\nrocky: terminationGracePeriodSeconds is %d. It must be greater than %d.\n\nOn shutdown Rocky waits the drain timeout (%ds) for a running scheduled child.\nIf the child is still going it sends SIGTERM, then allows a further fixed 60s\n(KILL_GRACE, rocky-core/src/schedule/spawn.rs:182) before SIGKILL. So the worst\ncase is drainTimeoutSeconds + 60.\n\nA shorter grace lets Kubernetes SIGKILL the pod while a warehouse write is\nstill going. The run is then recorded as failed and the write is half-done." (int .Values.terminationGracePeriodSeconds) $floor $drain) -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.secret" -}}
{{- if not .Values.existingSecret.name -}}
{{- fail "\nrocky: existingSecret.name is empty.\n\nThe server binds 0.0.0.0 in a pod, and a non-loopback bind requires a token so\nmodel SQL and run history do not leak on the network.\n\nThis chart never creates a Secret and accepts no secret value, so nothing\nsecret can reach `helm template` output, the release state, or a shell\ntranscript. Create it yourself and name it here:\n\n  kubectl create secret generic rocky-serve \\\n    --from-literal=token=\"$(openssl rand -hex 32)\"\n\n  --set existingSecret.name=rocky-serve" -}}
{{- end -}}
{{- if and .Values.serve.ui.enabled (eq .Values.scheduling.mode "resident") (not .Values.existingSecret.webhookSecretKey) -}}
{{- fail "\nrocky: serve.ui.enabled with scheduling.mode=resident needs\nexistingSecret.webhookSecretKey.\n\nThe UI is handed a read-only token. Without a separate webhook secret the\nscheduler's webhook route would be reachable with it." -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.storage" -}}
{{- if not (or .Values.persistence.existingClaim .Values.persistence.storageClassName) -}}
{{- fail "\nrocky: persistence.storageClassName is empty.\n\nName one deliberately; the chart will not inherit the cluster default. The\ncontract's locking claims hold on BLOCK storage. An advisory flock on NFS or\nother network storage is unprobed, and ReadWriteOnce is an access mode, not\nproof of the filesystem underneath.\n\n  --set persistence.storageClassName=<a block-backed class>\n\nOr point at a claim you made yourself, with persistence.existingClaim." -}}
{{- end -}}
{{- if ne .Values.persistence.accessMode "ReadWriteOnce" -}}
{{- fail (printf "\nrocky: persistence.accessMode is %q, and only ReadWriteOnce is allowed.\n\nA shared-access volume invites the second writer the contract refuses." .Values.persistence.accessMode) -}}
{{- end -}}
{{- end -}}

{{- define "rocky.guard.ingress" -}}
{{- if and .Values.ingress.enabled (not .Values.ingress.host) -}}
{{- fail "\nrocky: ingress.enabled is true and ingress.host is empty.\n\nThe host is not decoration. It is passed to the server as --allowed-host, and\na request carrying any other Host is refused 421. Without it the page is\nreachable by a name the server rejects." -}}
{{- end -}}
{{- if and .Values.ingress.enabled (not .Values.serve.ui.enabled) (not .Values.ingress.acknowledgeNoHostCheck) -}}
{{- fail "\nrocky: ingress.enabled is true and serve.ui.enabled is false.\n\n--allowed-host is passed only with --ui, so with the UI off the server accepts\nany Host that reaches it. The bearer token still gates every route, so this is\nnot an open door; it is one guard short of the intended shape.\n\nTurn the UI on, reach the API through a port-forward, or say so on purpose:\n\n  --set ingress.acknowledgeNoHostCheck=true" -}}
{{- end -}}
{{- end -}}
