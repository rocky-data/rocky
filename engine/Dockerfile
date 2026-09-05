# syntax=docker/dockerfile:1.7
#
# The Rocky engine image: one per `engine-v*` release.
#
# Nothing is compiled here. The image carries the release's own Linux binary,
# copied out of the archive the release matrix attached, so the image is the
# release, bit for bit, and the build takes seconds. The `image` job in
# .github/workflows/engine-release.yml prepares the build context and pushes
# to ghcr.io/rocky-data/rocky; the guide at
# docs/src/content/docs/guides/run-the-image.md shows the local build.
#
# Build context layout:
#   <context>/amd64/rocky
#   <context>/arm64/rocky
# `TARGETARCH` picks the directory for the platform being built, so one
# `docker buildx build --platform linux/amd64,linux/arm64` produces both
# manifests without emulation.
#
# Base: distroless `cc`, the `nonroot` variant. It ships glibc, libstdc++ (the
# DuckDB adapter needs it) and the CA certificates the warehouse connectors
# need, and no shell or package manager. It runs as uid 65532. Pinned by the
# index digest; bump it in a normal PR and ship the bump as a new patch
# release. A tag is never rebuilt in place.

# An empty, nonroot-owned /data. WORKDIR creates a directory as root, and a
# root-owned working directory is one the server cannot write its state store
# into, so the directory is made here and copied in with the right owner.
FROM scratch AS data
WORKDIR /data

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f

ARG TARGETARCH
COPY --chmod=0755 ${TARGETARCH}/rocky /usr/local/bin/rocky
COPY --from=data --chown=65532:65532 /data /data

# /data is the project: rocky.toml, models/ and, under models/, the state
# store. Mount it; nothing else in the image is written to.
WORKDIR /data
VOLUME ["/data"]

# `rocky serve` listens here by default. Inside a container the server must
# bind every interface to be reachable through a published port, which is why
# the default command passes `--host 0.0.0.0`; keep that flag when you pass
# your own `serve` arguments.
EXPOSE 8080

# `rocky serve` drains in-flight requests and a running scheduled child on
# SIGTERM. Docker waits 10 seconds by default before SIGKILL; a scheduled run
# can need more. `docker run --stop-timeout` and a pod's
# `terminationGracePeriodSeconds` set the grace.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/rocky"]
CMD ["serve", "--host", "0.0.0.0"]
