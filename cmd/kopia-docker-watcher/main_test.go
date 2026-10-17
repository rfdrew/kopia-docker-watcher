package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

func env(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}

	return func(k string) string { return m[k] }
}

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run(t.Context(), []string{"version"}, env(), &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr.String())
	}

	if got := strings.TrimSpace(stdout.String()); got != version.Version {
		t.Fatalf("stdout = %q, want %q", got, version.Version)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run(t.Context(), []string{"bogus"}, env(), &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}

	if !strings.Contains(stderr.String(), `unknown command "bogus"`) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunInvalidConfigLogsEachProblem(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run(t.Context(), nil, env("KOPIA_EXEC_CONTAINER", "kopia", "LOG_LEVEL", "loud"), &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	lines := jsonLines(t, stderr.Bytes())
	if len(lines) != 3 {
		t.Fatalf("got %d log lines, want 3 (url, exec container, log level):\n%s", len(lines), stderr.String())
	}

	for _, l := range lines {
		if l["level"] != "ERROR" || l["msg"] != "invalid configuration" {
			t.Errorf("unexpected line %v", l)
		}
	}
}

func TestRunWatcherStartsAndStops(t *testing.T) {
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	code := run(ctx, []string{"run"}, env(
		"KOPIA_SERVER_URL", "http://kopia:51515",
		"KOPIA_SERVER_PASSWORD", "secret",
		"KOPIA_PASSWORD", "repo",
	), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr.String())
	}

	if strings.Contains(stderr.String(), "secret") {
		t.Errorf("log leaks the password: %s", stderr.String())
	}

	var msgs []string
	for _, l := range jsonLines(t, stderr.Bytes()) {
		msgs = append(msgs, l["level"].(string)+" "+l["msg"].(string))
	}

	if len(msgs) != 3 || !strings.HasPrefix(msgs[0], "WARN KOPIA_PASSWORD is ignored") ||
		msgs[1] != "INFO starting kopia-docker-watcher" || msgs[2] != "INFO stopped" {
		t.Errorf("unexpected log: %q", msgs)
	}
}

func jsonLines(t *testing.T, b []byte) []map[string]any {
	t.Helper()

	var out []map[string]any

	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("not a JSON log line: %q", line)
		}

		out = append(out, m)
	}

	return out
}
