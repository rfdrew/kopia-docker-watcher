GO            ?= go
GOLANGCI_LINT ?= golangci-lint

BINARY  := kopia-docker-watcher
PKG     := github.com/rfdrew/kopia-docker-watcher
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X $(PKG)/internal/version.Version=$(VERSION)

# Integration tests start kopia server in Docker, by default the latest
# release pinned in internal/kopiatest; KOPIA_IMAGE overrides the image.
KOPIA_IMAGE ?=

.PHONY: all build test test-integration lint fmt tidy clean

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

clean:
	rm -rf bin
