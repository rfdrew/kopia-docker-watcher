package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr.String())
	}

	if got := strings.TrimSpace(stdout.String()); got != version.Version {
		t.Fatalf("stdout = %q, want %q", got, version.Version)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}

	if !strings.Contains(stderr.String(), `unknown command "bogus"`) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
