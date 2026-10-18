# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.2

# The build stage runs on the build host and cross-compiles, so a
# multi-arch build needs no emulation.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags "-s -w -X github.com/rfdrew/kopia-docker-watcher/internal/version.Version=${VERSION}" \
      -o /out/kopia-docker-watcher ./cmd/kopia-docker-watcher \
 && mkdir -p /out/state

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/kopia-docker-watcher /usr/local/bin/kopia-docker-watcher

# Directory of STATE_FILE. It belongs to nonroot, so a named volume mounted
# here starts with the same owner and the watcher can write to it.
COPY --from=build --chown=65532:65532 /out/state /var/lib/kopia-docker-watcher

USER 65532:65532

EXPOSE 8080

# distroless has no shell or curl: the binary probes its own /healthz on
# LISTEN_ADDR.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD ["/usr/local/bin/kopia-docker-watcher", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/kopia-docker-watcher"]
