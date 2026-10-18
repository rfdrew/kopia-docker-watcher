package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// harness wires a Reconciler to backendtest.Fake, fakeEngine and a real
// state store in a temporary directory.
type harness struct {
	t         *testing.T
	fake      *backendtest.Fake
	engine    *fakeEngine
	store     *state.Store
	statePath string
	metrics   *recordedMetrics
	logs      *syncBuffer
	out       *syncBuffer
	r         *Reconciler
}

type harnessOption func(*Options)

func newHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()

	h := &harness{
		t:         t,
		fake:      backendtest.New(),
		engine:    newFakeEngine(),
		statePath: filepath.Join(t.TempDir(), "state.json"),
		metrics:   &recordedMetrics{},
		logs:      &syncBuffer{},
		out:       &syncBuffer{},
	}

	o := Options{
		Backend:        h.fake,
		Docker:         h.engine,
		Logger:         slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:        h.metrics,
		Output:         h.out,
		MinBackoff:     time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		EventDelay:     time.Millisecond,
		RestartRecheck: 20 * time.Millisecond,
	}

	for _, fn := range opts {
		fn(&o)
	}

	if o.Store == nil {
		st, err := state.Open(h.statePath, state.Options{ReadOnly: o.DryRun})
		if err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = st.Close() })

		h.store = st
		o.Store = st
	}

	r, err := New(o)
	if err != nil {
		t.Fatal(err)
	}

	h.r = r

	return h
}

func withStrict(o *Options) { o.Strict = true }

func withHostPrefix(p string) harnessOption {
	return func(o *Options) { o.HostPrefix = p }
}

// pass runs one pass over p.
func (h *harness) pass(p string) passResult {
	h.t.Helper()

	return h.r.reconcile(context.Background(), p)
}

// mustPass runs one pass over p that must end without an error.
func (h *harness) mustPass(p string) passResult {
	h.t.Helper()

	res := h.pass(p)
	if res.err != nil {
		h.t.Fatalf("pass over %s: %v\nlog:\n%s", p, res.err, h.logs)
	}

	return res
}

func (h *harness) src(p string) backend.Source { return h.fake.Source(p) }

// mustPolicy checks the policy kopia server has for p.
func (h *harness) mustPolicy(p, want string) {
	h.t.Helper()

	got, ok := h.fake.Policy(h.src(p))
	if !ok {
		h.t.Fatalf("no policy for %s", p)
	}

	assertJSON(h.t, "policy of "+p, got, want)
}

// mustNoSource checks that kopia server does not know p.
func (h *harness) mustNoSource(p string) {
	h.t.Helper()

	if h.fake.Known(h.src(p)) {
		pol, _ := h.fake.Policy(h.src(p))
		h.t.Fatalf("%s is registered with %s", p, pol)
	}
}

// applied returns what the state holds for p.
func (h *harness) applied(p string) (backend.Applied, bool) {
	rec, ok := h.store.Get(h.src(p))
	return rec.Applied, ok
}

// writes returns the number of ApplyPlan calls that wrote something for p.
func (h *harness) writes(p string) int {
	n := 0

	for _, c := range h.fake.CallsFor(h.src(p), backendtest.ApplyPlan) {
		if c.Plan.Action != backend.ActionNone {
			n++
		}
	}

	return n
}

// start runs the reconciler until the test ends, reporting every pass on
// the returned channel.
func (h *harness) start() <-chan passEvent {
	h.t.Helper()

	passes := make(chan passEvent, 1024)
	h.r.afterPass = func(p string, res passResult) {
		select {
		case passes <- passEvent{path: p, res: res}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		h.r.Run(ctx)
		close(done)
	}()

	h.t.Cleanup(func() {
		cancel()
		<-done
	})

	return passes
}

type passEvent struct {
	path string
	res  passResult
}

// waitPass waits for the next pass over p that satisfies ok.
func waitPass(t *testing.T, passes <-chan passEvent, p string, ok func(passResult) bool) passResult {
	t.Helper()

	timeout := time.After(5 * time.Second)

	for {
		select {
		case ev := <-passes:
			if ev.path == p && (ok == nil || ok(ev.res)) {
				return ev.res
			}
		case <-timeout:
			t.Fatalf("no matching pass over %s", p)
			return passResult{}
		}
	}
}

// waitPasses waits for a successful pass over each of paths, in any order.
func waitPasses(t *testing.T, passes <-chan passEvent, paths ...string) {
	t.Helper()

	left := map[string]bool{}
	for _, p := range paths {
		left[p] = true
	}

	timeout := time.After(5 * time.Second)

	for len(left) > 0 {
		select {
		case ev := <-passes:
			if ev.res.err == nil {
				delete(left, ev.path)
			}
		case <-timeout:
			t.Fatalf("no successful pass over %v", left)
		}
	}
}

func succeeded(res passResult) bool { return res.err == nil }

// eventually waits until cond holds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s", what)
		}

		time.Sleep(time.Millisecond)
	}
}

func assertJSON(t *testing.T, what string, got json.RawMessage, want string) {
	t.Helper()

	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: %v in %s", what, err, got)
	}

	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: bad expectation %s: %v", what, want, err)
	}

	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)

	if !bytes.Equal(gb, wb) {
		t.Errorf("%s = %s, want %s", what, gb, wb)
	}
}

// recordedMetrics records ReconcileError calls.
type recordedMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func (m *recordedMetrics) ReconcileError(path string, reason Reason) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.counts == nil {
		m.counts = map[string]int{}
	}

	m.counts[path+" "+string(reason)]++
}

func (m *recordedMetrics) count(path string, reason Reason) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.counts[path+" "+string(reason)]
}

// syncBuffer is a bytes.Buffer safe for concurrent writes.
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

// logged reports whether a log line has msg and every given substring.
func (h *harness) logged(msg string, subs ...string) bool {
	for line := range strings.SplitSeq(h.logs.String(), "\n") {
		if !strings.Contains(line, `"msg":"`+msg) {
			continue
		}

		all := true

		for _, s := range subs {
			if !strings.Contains(line, s) {
				all = false
				break
			}
		}

		if all {
			return true
		}
	}

	return false
}
