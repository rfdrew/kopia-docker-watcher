package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// Identity of runKopia.
const (
	fakeUser = "root"
	fakeHost = "nas"
)

// reply is a canned answer of runKopia.
type reply struct {
	status int
	header map[string]string
	body   string
}

// runKopia is an in-memory kopia server for the run subcommand. It answers
// the calls of the REST backend the way kopia server does (internal/server
// at 01938f96) as far as the reconciler needs them:
//
//   - GET / serves the CSRF token; an API request without it is a 401
//     without WWW-Authenticate;
//   - GET /api/v1/repo/status answers the queued replies, then connected;
//   - GET /api/v1/sources filters by host, userName and path;
//   - POST /api/v1/sources stores the policy and makes the source known;
//   - GET /api/v1/policy returns the stored policy or a 400 NOT_FOUND;
//   - PUT /api/v1/policy rejects manual combined with anything else, as
//     kopia server's validation does, else stores the policy;
//   - POST /api/v1/policy/resolve returns the updates as the effective
//     policy and one upcoming time per cron schedule;
//   - GET /api/v1/tasks lists no tasks, GET /api/v1/snapshots no
//     snapshots;
//   - POST /api/v1/sources/upload records the path and succeeds for a
//     known source.
type runKopia struct {
	t *testing.T

	mu sync.Mutex
	// index, when set, answers GET / instead of the page.
	index *reply
	// statuses answer repo/status in turn; connected after them.
	statuses []reply
	policies map[string]json.RawMessage
	known    map[string]bool
	calls    []string
	// putGate, when set, holds every PUT until it is closed or the request
	// is canceled; putStarted receives a value as a PUT begins to wait.
	putGate    chan struct{}
	putStarted chan struct{}
	// uploads are the paths of the sources/upload requests.
	uploads []string
}

func newRunKopia(t *testing.T) *runKopia {
	return &runKopia{t: t, policies: map[string]json.RawMessage{}, known: map[string]bool{}}
}

func (f *runKopia) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	f.mu.Lock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)
	index := f.index
	gate := f.putGate
	f.mu.Unlock()

	if r.URL.Path == "/" {
		if index != nil {
			writeReply(w, *index)
			return
		}

		_, _ = io.WriteString(w, `<html><head><meta name="kopia-csrf-token" content="tok"></head></html>`)

		return
	}

	if r.Header.Get(kopiaclient.CSRFTokenHeader) != "tok" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if r.Method == http.MethodPut && gate != nil {
		select {
		case f.putStarted <- struct{}{}:
		default:
		}

		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	q := r.URL.Query()

	switch r.Method + " " + r.URL.Path {
	case "GET /api/v1/repo/status":
		if len(f.statuses) > 0 {
			rep := f.statuses[0]
			f.statuses = f.statuses[1:]
			writeReply(w, rep)

			return
		}

		writeJSONResponse(w, map[string]any{"connected": true, "hostname": fakeHost, "username": fakeUser})
	case "GET /api/v1/sources":
		var sources []kopiaclient.SourceStatus

		for _, p := range sortedKeys(f.known) {
			si := kopiaclient.SourceInfo{Host: fakeHost, UserName: fakeUser, Path: p}
			if (q.Get("host") == "" || q.Get("host") == si.Host) && (q.Get("userName") == "" || q.Get("userName") == si.UserName) &&
				(q.Get("path") == "" || q.Get("path") == si.Path) {
				sources = append(sources, kopiaclient.SourceStatus{Source: si, Status: "IDLE"})
			}
		}

		writeJSONResponse(w, kopiaclient.SourcesResponse{LocalUsername: fakeUser, LocalHost: fakeHost, Sources: sources})
	case "POST /api/v1/sources":
		var req kopiaclient.CreateSourceRequest
		if err := json.Unmarshal(body, &req); err != nil {
			f.t.Errorf("POST /api/v1/sources body %s: %v", body, err)
		}

		if !f.validPolicy(w, req.Policy) {
			return
		}

		f.policies[req.Path] = req.Policy
		f.known[req.Path] = true
		writeJSONResponse(w, map[string]any{"snapshotted": false})
	case "GET /api/v1/policy":
		f.checkSource(r)

		pol, ok := f.policies[q.Get("path")]
		if !ok {
			writeKopiaError(w, http.StatusBadRequest, "NOT_FOUND", "policy not found")
			return
		}

		_, _ = w.Write(pol)
	case "PUT /api/v1/policy":
		f.checkSource(r)

		if !f.validPolicy(w, body) {
			return
		}

		f.policies[q.Get("path")] = slices.Clone(body)
		f.known[q.Get("path")] = true
		writeJSONResponse(w, map[string]any{})
	case "POST /api/v1/policy/resolve":
		f.checkSource(r)

		var req struct {
			Updates                  json.RawMessage `json:"updates"`
			NumUpcomingSnapshotTimes int             `json:"numUpcomingSnapshotTimes"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			f.t.Errorf("policy/resolve body %s: %v", body, err)
		}

		sched := scheduling(f.t, req.Updates)
		upcoming := []time.Time{}

		if _, ok := sched["cron"]; ok && sched["manual"] == nil {
			upcoming = append(upcoming, time.Date(2026, 10, 19, 2, 0, 0, 0, time.UTC))
		}

		writeJSONResponse(w, map[string]any{"effective": req.Updates, "upcomingSnapshotTimes": upcoming})
	case "GET /api/v1/tasks":
		writeJSONResponse(w, map[string]any{"tasks": []any{}})
	case "GET /api/v1/snapshots":
		f.checkSource(r)
		writeJSONResponse(w, map[string]any{"snapshots": []any{}})
	case "POST /api/v1/sources/upload":
		f.checkSource(r)

		p := q.Get("path")
		if !f.known[p] {
			writeKopiaError(w, http.StatusNotFound, "NOT_FOUND", "no source matches")
			return
		}

		f.uploads = append(f.uploads, p)
		src := fakeUser + "@" + fakeHost + ":" + p
		writeJSONResponse(w, map[string]any{"sources": map[string]any{src: map[string]any{"success": true}}})
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		writeKopiaError(w, http.StatusBadRequest, "MALFORMED_REQUEST", "unexpected")
	}
}

// validPolicy rejects manual combined with another scheduling field.
func (f *runKopia) validPolicy(w http.ResponseWriter, policy json.RawMessage) bool {
	sched := scheduling(f.t, policy)
	if sched["manual"] != nil && len(sched) > 1 {
		writeKopiaError(w, http.StatusInternalServerError, "INTERNAL",
			"failed to validate policy: invalid scheduling policy: manual cannot be combined with other scheduling policies")

		return false
	}

	return true
}

func (f *runKopia) checkSource(r *http.Request) {
	q := r.URL.Query()
	if q.Get("host") != fakeHost || q.Get("userName") != fakeUser || !strings.HasPrefix(q.Get("path"), "/") {
		f.t.Errorf("%s %s: not a source of %s@%s", r.Method, r.URL, fakeUser, fakeHost)
	}
}

func (f *runKopia) policy(p string) (json.RawMessage, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pol, ok := f.policies[p]

	return pol, ok
}

func (f *runKopia) setPolicy(p, policy string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.policies[p] = json.RawMessage(policy)
}

func (f *runKopia) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

func (f *runKopia) count(call string) int {
	n := 0

	for _, c := range f.recorded() {
		if c == call {
			n++
		}
	}

	return n
}

// scheduling returns the scheduling section of a policy.
func scheduling(t *testing.T, policy json.RawMessage) map[string]any {
	var p struct {
		Scheduling map[string]any `json:"scheduling"`
	}

	if len(policy) > 0 {
		if err := json.Unmarshal(policy, &p); err != nil {
			t.Errorf("policy %s: %v", policy, err)
		}
	}

	return p.Scheduling
}

func writeReply(w http.ResponseWriter, rep reply) {
	for k, v := range rep.header {
		w.Header().Set(k, v)
	}

	w.WriteHeader(rep.status)
	_, _ = io.WriteString(w, rep.body)
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

// ctxTransport serves requests with an http.Handler in-process, like
// handlerTransport, and fails a request whose context ended meanwhile, as
// a real transport does.
type ctxTransport struct{ h http.Handler }

func (t ctxTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if r.Body == nil {
		r.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)

	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

// watchEngine is a Docker Engine whose event subscriptions stay open until
// they are canceled.
type watchEngine struct {
	mu         sync.Mutex
	containers []docker.Container
	subs       []context.Context
	events     chan docker.Event
	closed     bool
}

func newWatchEngine(containers ...docker.Container) *watchEngine {
	return &watchEngine{containers: containers, events: make(chan docker.Event)}
}

func (e *watchEngine) Containers(context.Context) ([]docker.Container, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.containers), nil
}

func (e *watchEngine) Inspect(context.Context, string) (docker.Container, error) {
	return docker.Container{}, errors.New("run does not inspect")
}

func (e *watchEngine) Events(ctx context.Context, _ time.Time) (<-chan docker.Event, <-chan error) {
	e.mu.Lock()
	e.subs = append(e.subs, ctx)
	e.mu.Unlock()

	errs := make(chan error, 1)

	go func() {
		<-ctx.Done()
		errs <- ctx.Err()
	}()

	return e.events, errs
}

func (e *watchEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.closed = true

	return nil
}

// setState changes the state of a container.
func (e *watchEngine) setState(id string, st docker.State) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range e.containers {
		if e.containers[i].ID == id {
			e.containers[i].State = st
		}
	}
}

// subscriptions returns the contexts of the event subscriptions.
func (e *watchEngine) subscriptions() []context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()

	return slices.Clone(e.subs)
}

func (e *watchEngine) isClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.closed
}

func appContainer() docker.Container {
	return docker.Container{ID: "c0ffee", Name: "app", State: docker.StateRunning, Labels: map[string]string{
		"kopia.enable": "true", "kopia.path": "/srv/app",
	}}
}

// watcherRun is a run subcommand running in the background.
type watcherRun struct {
	td     *testDeps
	kopia  *runKopia
	engine *watchEngine
	state  string
	ticks  chan time.Time
	// interval is what the ticker was created with.
	interval chan time.Duration
	cancel   context.CancelFunc
	done     chan int
}

type watcherOptions struct {
	env   []string
	args  []string
	grace time.Duration
	// retry is the first delay between attempts to reach kopia server.
	retry time.Duration
}

func startWatcher(t *testing.T, k *runKopia, e *watchEngine, opts watcherOptions) *watcherRun {
	t.Helper()

	stateFile := filepath.Join(t.TempDir(), "state.json")

	wr := &watcherRun{
		kopia:    k,
		engine:   e,
		state:    stateFile,
		ticks:    make(chan time.Time),
		interval: make(chan time.Duration, 1),
		done:     make(chan int, 1),
	}

	envs := append([]string{"KOPIA_SERVER_URL", "http://kopia:51515", "KOPIA_SERVER_PASSWORD", "secret",
		"STATE_FILE", stateFile}, opts.env...)

	wr.td = newTestDeps(env(envs...))
	wr.td.kopiaTransport = ctxTransport{k}
	wr.td.newDocker = func() (docker.Engine, io.Closer, error) { return e, e, nil }
	wr.td.minKopiaRetry = cmp.Or(opts.retry, time.Millisecond)
	wr.td.maxKopiaRetry = 2 * time.Millisecond
	wr.td.shutdownGrace = opts.grace
	wr.td.newTicker = func(d time.Duration) (<-chan time.Time, func()) {
		wr.interval <- d
		return wr.ticks, func() {}
	}

	ctx, cancel := context.WithCancel(t.Context())
	wr.cancel = cancel

	t.Cleanup(cancel)

	go func() { wr.done <- run(ctx, append([]string{"run"}, opts.args...), wr.td.deps) }()

	return wr
}

// wait returns the exit code; the run must end within timeout.
func (wr *watcherRun) wait(t *testing.T, timeout time.Duration) int {
	t.Helper()

	select {
	case code := <-wr.done:
		return code
	case <-time.After(timeout):
		t.Fatalf("run did not return within %v; log:\n%s", timeout, wr.td.stderr)
		return -1
	}
}

// waitLog waits for a log line whose message starts with msg and returns it.
func (wr *watcherRun) waitLog(t *testing.T, msg string) map[string]any {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		if l := findLog(wr.td.stderr.String(), msg); l != nil {
			return l
		}

		select {
		case code := <-wr.done:
			wr.done <- code
			if l := findLog(wr.td.stderr.String(), msg); l != nil {
				return l
			}

			t.Fatalf("run returned %d before logging %q; log:\n%s", code, msg, wr.td.stderr)
		case <-time.After(5 * time.Millisecond):
		}
	}

	t.Fatalf("no log line %q; log:\n%s", msg, wr.td.stderr)

	return nil
}

// recv receives from ch, failing the test if the run ends or nothing comes.
func recv[T any](t *testing.T, wr *watcherRun, ch <-chan T) T {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case code := <-wr.done:
		wr.done <- code
		t.Fatalf("run returned %d; log:\n%s", code, wr.td.stderr)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out; log:\n%s", wr.td.stderr)
	}

	var zero T

	return zero
}

// waitFor polls cond.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func findLog(s, msg string) map[string]any {
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}

		if text, _ := m["msg"].(string); strings.HasPrefix(text, msg) {
			return m
		}
	}

	return nil
}

// logMessages returns the messages of the log lines that are one of msgs,
// in order.
func logMessages(s string, msgs ...string) []string {
	var out []string

	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}

		if text, _ := m["msg"].(string); slices.Contains(msgs, text) {
			out = append(out, text)
		}
	}

	return out
}

func cronOf(t *testing.T, k *runKopia, p string) any {
	t.Helper()

	pol, ok := k.policy(p)
	if !ok {
		return nil
	}

	return scheduling(t, pol)["cron"]
}

func TestRunWaitsForKopia(t *testing.T) {
	k := newRunKopia(t)
	k.statuses = []reply{
		{status: http.StatusServiceUnavailable, body: "starting"},
		{status: http.StatusOK, body: `{"connected":false}`},
		{status: http.StatusBadGateway, body: "proxy"},
	}

	wr := startWatcher(t, k, newWatchEngine(), watcherOptions{})

	wr.waitLog(t, "watching containers")

	var attempts []string

	for _, line := range jsonLines(t, wr.td.stderr.String()) {
		msg := line["msg"].(string)
		if strings.HasPrefix(msg, "kopia server") && strings.Contains(msg, "waiting") {
			attempts = append(attempts, msg+" #"+jsonNumber(line["attempt"])+" "+line["delay"].(string))
		}
	}

	want := []string{
		"kopia server is not reachable yet, waiting #1 1ms",
		"kopia server has no repository connected yet, waiting #2 2ms",
		"kopia server is not reachable yet, waiting #3 2ms",
	}
	if !slices.Equal(attempts, want) {
		t.Errorf("attempts = %q, want %q", attempts, want)
	}

	if l := findLog(wr.td.stderr.String(), "connected to kopia server"); l == nil || l["hostname"] != fakeHost {
		t.Errorf("connected line = %v", l)
	}

	if n := k.count("GET /api/v1/repo/status"); n != 4 {
		t.Errorf("repo/status asked %d times, want 4", n)
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}
}

func jsonNumber(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// A stop while kopia server is not there yet ends the run at once, without
// a state file.
func TestRunStopsWhileWaitingForKopia(t *testing.T) {
	k := newRunKopia(t)
	for range 1000 {
		k.statuses = append(k.statuses, reply{status: http.StatusServiceUnavailable})
	}

	wr := startWatcher(t, k, newWatchEngine(), watcherOptions{retry: time.Hour})

	wr.waitLog(t, "kopia server is not reachable yet")
	wr.cancel()

	if code := wr.wait(t, 5*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	if _, err := os.Stat(wr.state); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("state file: %v", err)
	}

	got := logMessages(wr.td.stderr.String(), "watching containers", "HTTP server stopped", "stopped")
	if want := []string{"HTTP server stopped", "stopped"}; !slices.Equal(got, want) {
		t.Errorf("log = %q, want %q", got, want)
	}
}

func TestRunKopiaFatalErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		index  *reply
		status *reply
		want   string
	}{
		{
			name:  "no UI",
			index: &reply{status: http.StatusNotFound, body: "404 page not found"},
			want:  "start kopia server without --no-ui",
		},
		{
			name:   "no UI API",
			status: &reply{status: http.StatusNotFound, body: "404 page not found"},
			want:   "start kopia server without --no-ui",
		},
		{
			name:  "wrong credentials",
			index: &reply{status: http.StatusUnauthorized, header: map[string]string{"WWW-Authenticate": `Basic realm="Kopia"`}},
			want:  "kopia server rejected the credentials: check KOPIA_SERVER_USERNAME and KOPIA_SERVER_PASSWORD",
		},
		{
			name:  "not the server user",
			index: &reply{status: http.StatusForbidden, body: "access denied"},
			want:  "KOPIA_SERVER_USERNAME must be the user of kopia server's --server-username",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := newRunKopia(t)
			k.index = tc.index

			if tc.status != nil {
				k.statuses = []reply{*tc.status}
			}

			wr := startWatcher(t, k, newWatchEngine(), watcherOptions{})

			if code := wr.wait(t, 5*time.Second); code != 1 {
				t.Fatalf("exit code = %d, want 1; log:\n%s", code, wr.td.stderr)
			}

			l := findLog(wr.td.stderr.String(), "kopia server")
			for _, line := range jsonLines(t, wr.td.stderr.String()) {
				if line["level"] == "ERROR" {
					l = line
				}
			}

			if msg, _ := l["msg"].(string); !strings.Contains(msg, tc.want) {
				t.Errorf("error line = %v, want %q", l, tc.want)
			}

			// One attempt, no retries.
			if n := k.count("GET /"); n != 1 {
				t.Errorf("GET / sent %d times: %q", n, k.recorded())
			}

			if wr.td.listenAddr == "" {
				t.Error("/healthz was not served while checking kopia server")
			}

			if findLog(wr.td.stderr.String(), "HTTP server stopped") == nil {
				t.Errorf("HTTP server not stopped; log:\n%s", wr.td.stderr)
			}
		})
	}
}

// The watcher registers the source of a running container after the first
// resync, and every RECONCILE_INTERVAL puts back the fields of the labels
// that were changed in kopia server.
func TestRunReconcilesPeriodically(t *testing.T) {
	k := newRunKopia(t)
	e := newWatchEngine(appContainer())

	wr := startWatcher(t, k, e, watcherOptions{env: []string{"RECONCILE_INTERVAL", "7m"}})

	if d := recv(t, wr, wr.interval); d != 7*time.Minute {
		t.Errorf("ticker interval = %v, want 7m", d)
	}

	waitFor(t, "the registration", func() bool { return k.count("POST /api/v1/sources") == 1 })
	waitFor(t, "the policy", func() bool { return jsonNumber(cronOf(t, k, "/srv/app")) == `["0 * * * *"]` })

	// An edit in the web UI stays until the next full reconcile.
	k.setPolicy("/srv/app", `{"scheduling":{"cron":["5 5 * * *"]},"retention":{"keepDaily":3}}`)

	select {
	case wr.ticks <- time.Now():
	case <-time.After(10 * time.Second):
		t.Fatalf("the watcher took no tick; log:\n%s", wr.td.stderr)
	}

	waitFor(t, "the label's cron back", func() bool { return jsonNumber(cronOf(t, k, "/srv/app")) == `["0 * * * *"]` })

	pol, _ := k.policy("/srv/app")
	if !strings.Contains(string(pol), `"keepDaily":3`) {
		t.Errorf("a field the labels do not set was lost: %s", pol)
	}

	if findLog(wr.td.stderr.String(), "starting the periodic full reconcile") != nil {
		t.Errorf("the debug line is logged at info level")
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	data, err := os.ReadFile(wr.state)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(data), "/srv/app") {
		t.Errorf("state file has no record of /srv/app:\n%s", data)
	}

	if !e.isClosed() {
		t.Error("Docker client not closed")
	}
}

// A container that stops pauses its source on its event, without a full
// reconcile.
func TestRunPausesOnEvent(t *testing.T) {
	k := newRunKopia(t)
	e := newWatchEngine(appContainer())

	wr := startWatcher(t, k, e, watcherOptions{})

	waitFor(t, "the policy", func() bool { return cronOf(t, k, "/srv/app") != nil })

	e.setState("c0ffee", docker.StateExited)
	e.events <- docker.Event{Kind: docker.KindDied, Action: "die", ContainerID: "c0ffee", Name: "app",
		Labels: appContainer().Labels, Time: time.Now()}

	waitFor(t, "the pause", func() bool {
		pol, _ := k.policy("/srv/app")
		return jsonNumber(scheduling(t, pol)) == `{"manual":true}`
	})

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}
}

// A container start queues a snapshot through sources/upload. The REST
// backend cannot follow snapshots yet: that is logged once, and the
// watcher goes on.
func TestRunSnapshotOnStart(t *testing.T) {
	k := newRunKopia(t)
	e := newWatchEngine(appContainer())

	wr := startWatcher(t, k, e, watcherOptions{})

	waitFor(t, "the policy", func() bool { return cronOf(t, k, "/srv/app") != nil })

	start := docker.Event{Kind: docker.KindStarted, Action: "start", ContainerID: "c0ffee", Name: "app",
		Labels: appContainer().Labels, Time: time.Now()}
	e.events <- start

	uploads := func() []string {
		k.mu.Lock()
		defer k.mu.Unlock()

		return slices.Clone(k.uploads)
	}

	const notFollowed = "the backend cannot follow snapshots yet; their results are not checked"

	waitFor(t, "the snapshot", func() bool { return len(uploads()) == 1 })
	wr.waitLog(t, notFollowed)

	// Another start within the minimum interval queues none.
	e.events <- start

	time.Sleep(3 * time.Second)

	if got := uploads(); !slices.Equal(got, []string{"/srv/app"}) {
		t.Errorf("uploads = %q", got)
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	if n := len(logMessages(wr.td.stderr.String(), notFollowed)); n != 1 {
		t.Errorf("warned %d times", n)
	}

	// The wait the backend cannot follow is not kept; the trigger time is.
	data, err := os.ReadFile(wr.state)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), "snapshotWaits") || !strings.Contains(string(data), "lastTriggered") {
		t.Errorf("state file:\n%s", data)
	}
}

// SNAPSHOT_ON_WATCHER_START takes a snapshot of the running containers at
// the start; by default the start takes none.
func TestRunSnapshotOnWatcherStart(t *testing.T) {
	for _, tt := range []struct {
		env  []string
		want int
	}{{nil, 0}, {[]string{"SNAPSHOT_ON_WATCHER_START", "true"}, 1}} {
		t.Run(fmt.Sprint(tt.env), func(t *testing.T) {
			k := newRunKopia(t)
			e := newWatchEngine(appContainer())

			wr := startWatcher(t, k, e, watcherOptions{env: tt.env})

			uploads := func() int {
				k.mu.Lock()
				defer k.mu.Unlock()

				return len(k.uploads)
			}

			// The decision waits for a pass EventDelay after the policy
			// write.
			wr.waitLog(t, "source registered")

			if tt.want > 0 {
				waitFor(t, "the snapshot", func() bool { return uploads() == tt.want })
			}

			// Well past the pass that decides.
			time.Sleep(2*reconcile.DefaultEventDelay + time.Second)

			if n := uploads(); n != tt.want {
				t.Errorf("%d uploads, want %d; log:\n%s", n, tt.want, wr.td.stderr)
			}

			wr.cancel()
			wr.wait(t, 10*time.Second)
		})
	}
}

func TestRunDryRun(t *testing.T) {
	k := newRunKopia(t)
	e := newWatchEngine(appContainer())

	wr := startWatcher(t, k, e, watcherOptions{args: []string{"--dry-run"}})

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	out := wr.td.stdout.String()
	if !strings.Contains(out, fakeUser+"@"+fakeHost+":/srv/app: would register the source") {
		t.Errorf("stdout has no plan for /srv/app:\n%s", out)
	}

	for _, c := range k.recorded() {
		if !strings.HasPrefix(c, "GET ") && c != "POST /api/v1/policy/resolve" {
			t.Errorf("dry run sent %s", c)
		}
	}

	if _, err := os.Stat(wr.state); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("state file: %v", err)
	}

	if wr.td.listenAddr != "" {
		t.Errorf("dry run listened on %q", wr.td.listenAddr)
	}

	if len(e.subscriptions()) != 0 {
		t.Error("dry run subscribed to Docker events")
	}

	if findLog(wr.td.stderr.String(), "dry run done") == nil {
		t.Errorf("no final line; log:\n%s", wr.td.stderr)
	}
}

func TestRunDryRunFailures(t *testing.T) {
	t.Run("owners disagree", func(t *testing.T) {
		a, b := appContainer(), appContainer()
		b.ID, b.Name = "beef", "app-2"
		b.Labels = map[string]string{"kopia.enable": "true", "kopia.path": "/srv/app", "kopia.schedule": "0 3 * * *"}

		wr := startWatcher(t, newRunKopia(t), newWatchEngine(a, b), watcherOptions{args: []string{"--dry-run"}})

		if code := wr.wait(t, 10*time.Second); code != 1 {
			t.Fatalf("exit code = %d, want 1; log:\n%s", code, wr.td.stderr)
		}

		if l := findLog(wr.td.stderr.String(), "dry run found problems"); l == nil || !strings.Contains(l["error"].(string), "/srv/app") {
			t.Errorf("log:\n%s", wr.td.stderr)
		}
	})

	// A dry run does not wait for kopia server.
	t.Run("not connected", func(t *testing.T) {
		k := newRunKopia(t)
		k.statuses = []reply{{status: http.StatusOK, body: `{"connected":false}`}}

		wr := startWatcher(t, k, newWatchEngine(appContainer()), watcherOptions{args: []string{"--dry-run"}})

		if code := wr.wait(t, 10*time.Second); code != 1 {
			t.Fatalf("exit code = %d, want 1; log:\n%s", code, wr.td.stderr)
		}

		if findLog(wr.td.stderr.String(), "kopia server has no repository connected") == nil {
			t.Errorf("log:\n%s", wr.td.stderr)
		}

		if n := k.count("GET /api/v1/repo/status"); n != 1 {
			t.Errorf("repo/status asked %d times", n)
		}
	})
}

// A policy write in progress at a stop finishes within the grace, and the
// state records it.
func TestRunShutdownFinishesWrite(t *testing.T) {
	k := newRunKopia(t)
	k.putGate = make(chan struct{})
	k.putStarted = make(chan struct{}, 1)

	e := newWatchEngine(appContainer())
	wr := startWatcher(t, k, e, watcherOptions{grace: 5 * time.Second})

	recv(t, wr, k.putStarted)
	wr.cancel()
	wr.waitLog(t, "shutting down")

	// Docker events stop at once.
	waitFor(t, "the event subscription to end", func() bool {
		subs := e.subscriptions()
		return len(subs) > 0 && subs[len(subs)-1].Err() != nil
	})

	close(k.putGate)

	if code := wr.wait(t, 5*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	got := logMessages(wr.td.stderr.String(), "shutting down", "stopped following Docker events", "reconciler stopped",
		"state saved", "HTTP server stopped", "stopped")
	want := []string{"shutting down", "stopped following Docker events", "reconciler stopped", "state saved",
		"HTTP server stopped", "stopped"}

	if !slices.Equal(got, want) {
		t.Errorf("shutdown log = %q, want %q", got, want)
	}

	// The intent is stored before the write; only a finished write leaves
	// the record without a pending patch and with the stored policy's hash.
	rec := stateRecord(t, wr.state, "/srv/app")
	if rec.Pending != nil || rec.StoredHash == "" || !slices.Equal(rec.Patch.Scheduling.Cron, []string{"0 * * * *"}) {
		t.Errorf("state record = %+v, want the applied patch without a pending one", rec)
	}
}

// stateRecord reads the record of path p from a state file.
func stateRecord(t *testing.T, file, p string) backend.Applied {
	t.Helper()

	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	var st struct {
		Sources []struct {
			Source backend.Source `json:"source"`
			backend.Applied
		} `json:"sources"`
	}

	if err := json.Unmarshal(data, &st); err != nil {
		t.Fatalf("state file %s: %v", data, err)
	}

	for _, e := range st.Sources {
		if e.Source.Path == p {
			return e.Applied
		}
	}

	t.Fatalf("state file has no record of %s:\n%s", p, data)

	return backend.Applied{}
}

// A policy write that outlasts the grace is canceled, so the watcher stops
// before Docker kills it.
func TestRunShutdownCancelsWriteAfterGrace(t *testing.T) {
	const grace = 300 * time.Millisecond

	k := newRunKopia(t)
	k.putGate = make(chan struct{})
	k.putStarted = make(chan struct{}, 1)

	wr := startWatcher(t, k, newWatchEngine(appContainer()), watcherOptions{grace: grace})

	recv(t, wr, k.putStarted)

	start := time.Now()

	wr.cancel()

	if code := wr.wait(t, 5*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}

	if took := time.Since(start); took < grace || took > grace+2*time.Second {
		t.Errorf("shutdown took %v with a grace of %v", took, grace)
	}

	if l := findLog(wr.td.stderr.String(), "shutting down"); l == nil || l["grace"] != grace.String() {
		t.Errorf("shutting down line = %v", l)
	}

	if findLog(wr.td.stderr.String(), "state saved") == nil {
		t.Errorf("state not saved; log:\n%s", wr.td.stderr)
	}
}

func TestDefaultShutdownFitsDockerStop(t *testing.T) {
	// docker stop waits 10 s by default before it kills the container.
	if total := defaultShutdownGrace + shutdownTimeout; total >= 9*time.Second {
		t.Errorf("shutdown may take %v", total)
	}
}

func TestRunStateLocked(t *testing.T) {
	k := newRunKopia(t)
	first := startWatcher(t, k, newWatchEngine(), watcherOptions{})
	first.waitLog(t, "watching containers")

	td := newTestDeps(env("KOPIA_SERVER_URL", "http://kopia:51515", "KOPIA_SERVER_PASSWORD", "secret",
		"STATE_FILE", first.state))
	td.kopiaTransport = ctxTransport{k}
	td.newDocker = func() (docker.Engine, io.Closer, error) {
		t.Error("Docker client created without a state")
		return nil, nil, errors.New("no")
	}

	if code := run(t.Context(), []string{"run"}, td.deps); code != 1 {
		t.Fatalf("exit code = %d, want 1; log:\n%s", code, td.stderr)
	}

	if l := findLog(td.stderr.String(), "cannot open the state file"); l == nil || !strings.Contains(l["error"].(string), "in use") {
		t.Errorf("log:\n%s", td.stderr)
	}

	first.cancel()
	first.wait(t, 10*time.Second)
}

// failingListener fails Accept, so the HTTP server fails while the watcher
// waits for kopia server.
type failingListener struct{}

func (failingListener) Accept() (net.Conn, error) { return nil, errors.New("accept failed") }
func (failingListener) Close() error              { return nil }
func (failingListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080} }

func TestRunHTTPServerFails(t *testing.T) {
	k := newRunKopia(t)
	for range 1000 {
		k.statuses = append(k.statuses, reply{status: http.StatusServiceUnavailable})
	}

	td := newTestDeps(env("KOPIA_SERVER_URL", "http://kopia:51515", "KOPIA_SERVER_PASSWORD", "secret",
		"STATE_FILE", filepath.Join(t.TempDir(), "state.json")))
	td.kopiaTransport = ctxTransport{k}
	td.minKopiaRetry = time.Hour
	td.listen = func(string, string) (net.Listener, error) { return failingListener{}, nil }

	if code := run(t.Context(), []string{"run"}, td.deps); code != 1 {
		t.Fatalf("exit code = %d, want 1; log:\n%s", code, td.stderr)
	}

	if findLog(td.stderr.String(), "HTTP server failed") == nil {
		t.Errorf("log:\n%s", td.stderr)
	}
}

func TestRunFlags(t *testing.T) {
	for _, tc := range []struct {
		args           []string
		code           int
		stdout, stderr string
	}{
		{args: []string{"run", "--help"}, code: 0, stdout: "kopia-docker-watcher run --dry-run [--strict]"},
		{args: []string{"run", "--bogus"}, code: 2, stderr: "flag provided but not defined: -bogus"},
		{args: []string{"run", "now"}, code: 2, stderr: `run: unexpected argument "now"`},
	} {
		td := newTestDeps(validEnv())
		td.listen = func(string, string) (net.Listener, error) {
			t.Errorf("%q: listened", tc.args)
			return nil, errors.New("no")
		}

		if code := run(t.Context(), tc.args, td.deps); code != tc.code {
			t.Errorf("%q: exit code = %d, want %d", tc.args, code, tc.code)
		}

		if !strings.Contains(td.stdout.String(), tc.stdout) || !strings.Contains(td.stderr.String(), tc.stderr) {
			t.Errorf("%q: stdout = %s, stderr = %s", tc.args, td.stdout, td.stderr)
		}

		if tc.code == 2 && !strings.Contains(td.stderr.String(), "Usage:") {
			t.Errorf("%q: no usage on stderr: %s", tc.args, td.stderr)
		}
	}
}

// --strict turns label warnings into errors.
func TestRunStrict(t *testing.T) {
	c := appContainer()
	c.Labels["kopia.unknown"] = "x"

	for _, tc := range []struct {
		args []string
		code int
	}{
		{args: []string{"--dry-run"}, code: 0},
		{args: []string{"--dry-run", "--strict"}, code: 1},
	} {
		wr := startWatcher(t, newRunKopia(t), newWatchEngine(c), watcherOptions{args: tc.args})

		if code := wr.wait(t, 10*time.Second); code != tc.code {
			t.Errorf("%q: exit code = %d, want %d; log:\n%s", tc.args, code, tc.code, wr.td.stderr)
		}
	}
}
