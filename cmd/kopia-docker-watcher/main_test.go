package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

func env(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}

	return func(k string) string { return m[k] }
}

// validEnv points at a closed loopback port so the startup check of
// kopia server fails fast without leaving the machine.
func validEnv(extra ...string) func(string) string {
	return env(append([]string{
		"KOPIA_SERVER_URL", "http://127.0.0.1:1",
		"KOPIA_SERVER_PASSWORD", "secret",
	}, extra...)...)
}

// syncBuffer collects output written from several goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

// memListener is an in-memory listener; the sandbox the tests may run in
// does not allow binding ports.
type memListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newMemListener() *memListener {
	return &memListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *memListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *memListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *memListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080} }

func (l *memListener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()

	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		return nil, syscall.ECONNREFUSED
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type testDeps struct {
	deps

	stdout, stderr *syncBuffer
	ln             *memListener
	listenAddr     string
}

func newTestDeps(getenv func(string) string) *testDeps {
	td := &testDeps{stdout: &syncBuffer{}, stderr: &syncBuffer{}, ln: newMemListener()}
	td.deps = deps{
		getenv: getenv,
		stdin:  strings.NewReader(""),
		stdout: td.stdout,
		stderr: td.stderr,
		listen: func(_, addr string) (net.Listener, error) {
			td.listenAddr = addr
			return td.ln, nil
		},
		httpClient: &http.Client{Transport: &http.Transport{DialContext: td.ln.dial}, Timeout: 5 * time.Second},
	}

	return td
}

func TestRunVersion(t *testing.T) {
	td := newTestDeps(env())

	if code := run(t.Context(), []string{"version"}, td.deps); code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, td.stderr)
	}

	if got := strings.TrimSpace(td.stdout.String()); got != version.Version {
		t.Fatalf("stdout = %q, want %q", got, version.Version)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	td := newTestDeps(env())

	if code := run(t.Context(), []string{"bogus"}, td.deps); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}

	if !strings.Contains(td.stderr.String(), `unknown command "bogus"`) {
		t.Fatalf("stderr = %q", td.stderr)
	}
}

func TestRunInvalidConfigLogsEachProblem(t *testing.T) {
	td := newTestDeps(env("KOPIA_EXEC_CONTAINER", "kopia", "LOG_LEVEL", "loud"))

	if code := run(t.Context(), nil, td.deps); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	lines := jsonLines(t, td.stderr.String())
	if len(lines) != 3 {
		t.Fatalf("got %d log lines, want 3 (url, exec container, log level):\n%s", len(lines), td.stderr)
	}

	for _, l := range lines {
		if l["level"] != "ERROR" || l["msg"] != "invalid configuration" {
			t.Errorf("unexpected line %v", l)
		}
	}
}

func TestRunWatcherStartsAndStops(t *testing.T) {
	td := newTestDeps(validEnv("KOPIA_PASSWORD", "repo", "LISTEN_ADDR", "127.0.0.1:9090"))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if code := run(ctx, []string{"run"}, td.deps); code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, td.stderr)
	}

	if td.listenAddr != "127.0.0.1:9090" {
		t.Errorf("listened on %q", td.listenAddr)
	}

	if strings.Contains(td.stderr.String(), "secret") {
		t.Errorf("log leaks the password: %s", td.stderr)
	}

	var msgs []string
	for _, l := range jsonLines(t, td.stderr.String()) {
		msgs = append(msgs, l["level"].(string)+" "+l["msg"].(string))
	}

	want := []string{"WARN KOPIA_PASSWORD is ignored", "INFO starting kopia-docker-watcher", "INFO serving HTTP",
		"INFO HTTP server stopped", "INFO stopped"}
	if len(msgs) != len(want) {
		t.Fatalf("log = %q, want %q", msgs, want)
	}

	for i := range want {
		if !strings.HasPrefix(msgs[i], want[i]) {
			t.Errorf("log line %d = %q, want %q", i, msgs[i], want[i])
		}
	}
}

func TestRunWatcherListenError(t *testing.T) {
	td := newTestDeps(validEnv())
	td.listen = func(string, string) (net.Listener, error) { return nil, syscall.EADDRINUSE }

	if code := run(t.Context(), nil, td.deps); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}

	if !strings.Contains(td.stderr.String(), "cannot listen") {
		t.Errorf("stderr = %s", td.stderr)
	}
}

// The healthcheck subcommand probes the running watcher end to end.
func TestHealthcheckAgainstRunningWatcher(t *testing.T) {
	td := newTestDeps(validEnv())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)

	go func() { done <- run(ctx, nil, td.deps) }()

	var (
		code int
		hc   *testDeps
	)

	// The server starts serving right after Listen; retry briefly.
	for range 50 {
		hc = newTestDeps(env())
		hc.httpClient = td.httpClient

		if code = run(t.Context(), []string{"healthcheck"}, hc.deps); code == 0 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if code != 0 {
		t.Errorf("healthcheck exit code = %d: %s", code, hc.stderr)
	}

	cancel()

	if code := <-done; code != 0 {
		t.Fatalf("watcher exit code = %d: %s", code, td.stderr)
	}

	// After shutdown nothing answers.
	hc = newTestDeps(env())
	hc.httpClient = td.httpClient

	if code := run(t.Context(), []string{"healthcheck"}, hc.deps); code != 1 {
		t.Errorf("healthcheck after shutdown exit code = %d", code)
	}
}

func TestHealthcheckFailures(t *testing.T) {
	td := newTestDeps(env("LISTEN_ADDR", "8080"))

	if code := run(t.Context(), []string{"healthcheck"}, td.deps); code != 1 || !strings.Contains(td.stderr.String(), "invalid listen address") {
		t.Errorf("bad LISTEN_ADDR: exit %d, stderr %s", code, td.stderr)
	}

	td = newTestDeps(env())
	td.httpClient = &http.Client{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return nil, syscall.ECONNREFUSED },
	}}

	if code := run(t.Context(), []string{"healthcheck"}, td.deps); code != 1 || !strings.Contains(td.stderr.String(), "127.0.0.1:8080") {
		t.Errorf("nothing listening: exit %d, stderr %s", code, td.stderr)
	}
}

func jsonLines(t *testing.T, s string) []map[string]any {
	t.Helper()

	var out []map[string]any

	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not a JSON log line: %q", line)
		}

		out = append(out, m)
	}

	return out
}
