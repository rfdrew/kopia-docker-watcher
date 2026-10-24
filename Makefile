GO            ?= go
GOLANGCI_LINT ?= golangci-lint
GORELEASER    ?= goreleaser
PYTHON        ?= python3

BINARY  := kopia-docker-watcher
PKG     := github.com/rfdrew/kopia-docker-watcher
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
# Commit time in UTC, the format goreleaser uses.
DATE    ?= $(shell TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd 2>/dev/null || echo unknown)
LDFLAGS := -s -w -X $(PKG)/internal/version.Version=$(VERSION) \
	-X $(PKG)/internal/version.Commit=$(COMMIT) -X $(PKG)/internal/version.Date=$(DATE)

IMAGE   ?= kopia-docker-watcher:dev

# Integration tests start kopia server in Docker, by default the latest
# release pinned in internal/kopiatest; KOPIA_IMAGE overrides the image.
KOPIA_IMAGE ?=

# Extra flags for `make snapshot`, e.g. SNAPSHOT_FLAGS=--skip=docker
# without a Docker daemon.
SNAPSHOT_FLAGS ?=

.PHONY: all build test test-integration lint fmt tidy docker release-check snapshot docs-check clean

all: lint test build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/$(BINARY)

test:
	$(GO) test -race ./...

test-integration:
	KOPIA_IMAGE=$(KOPIA_IMAGE) $(GO) test -tags integration -race -count=1 -timeout 20m -run '^TestIntegration' ./...

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	$(GOLANGCI_LINT) fmt ./...

tidy:
	$(GO) mod tidy

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

# Validates .goreleaser.yaml.
release-check:
	$(GORELEASER) check

# Local release dry run into dist/: binaries, archives, checksums and
# per-platform images tagged <version>-amd64/-arm64. Nothing is published.
snapshot:
	$(GORELEASER) release --snapshot --clean $(SNAPSHOT_FLAGS)

# The Ukrainian translation of README.md and docs/ (README.uk.md, docs/uk/):
# a missing translation is an error, an outdated one a warning. Then the links
# and anchors of the documentation in both languages.
docs-check:
	scripts/check-docs-translations.sh
	$(PYTHON) scripts/check-docs-links.py

clean:
	rm -rf bin dist
