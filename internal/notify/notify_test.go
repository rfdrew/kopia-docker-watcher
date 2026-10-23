package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

const token = "123456:ABC-secret-token"

// webhook is a fake webhook: it records the requests and answers with the
// statuses the test queues, then 200.
type webhook struct {
	mu       sync.Mutex
	requests []recorded
	statuses []int
	header   http.Header
	// block, if set, holds every request until it is closed.
	block chan struct{}
	// err, if set, fails every request in the transport.
	err error
}

type recorded struct {
	URL    string
	Header http.Header
	Body   []byte
}

func (w *webhook) RoundTrip(req *http.Request) (*http.Response, error) {
	if w.block != nil {
		select {
		case <-w.block:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	body, _ := io.ReadAll(req.Body)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.requests = append(w.requests, recorded{URL: req.URL.String(), Header: req.Header.Clone(), Body: body})

	if w.err != nil {
		return nil, w.err
	}

	status := http.StatusOK
	if len(w.statuses) > 0 {
		status, w.statuses = w.statuses[0], w.statuses[1:]
	}

	rec := httptest.NewRecorder()
	for k, v := range w.header {
		rec.Header()[k] = v
	}

	rec.WriteHeader(status)
	_, _ = io.WriteString(rec, `{"ok":false,"description":"busy"}`)

	return rec.Result(), nil
}

func (w *webhook) recorded() []recorded {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]recorded(nil), w.requests...)
}

type counter struct {
	mu sync.Mutex
	n  map[Result]int
}

func (c *counter) Notification(r Result) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.n == nil {
		c.n = map[Result]int{}
	}

	c.n[r]++
}

func (c *counter) get(r Result) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.n[r]
}

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

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type fixture struct {
	n       *Notifier
	hook    *webhook
	metrics *counter
	logs    *syncBuffer
	clock   *testClock
}

func newFixture(t *testing.T, format, rawURL string, mod ...func(*Options)) *fixture {
	t.Helper()

	f := &fixture{
		hook:    &webhook{},
		metrics: &counter{},
		logs:    &syncBuffer{},
		clock:   &testClock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)},
	}

	opts := Options{
		URL:        rawURL,
		Format:     format,
		ErrorAfter: 15 * time.Minute,
		Timeout:    time.Second,
		RetryDelay: time.Millisecond,
		Transport:  f.hook,
		Logger:     slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:    f.metrics,
		Now:        f.clock.Now,
	}

	for _, fn := range mod {
		fn(&opts)
	}

	n, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}

	f.n = n

	return f
}

// run starts the sender until the test ends.
func (f *fixture) run(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		f.n.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// waitResults waits until the metrics count want of each result.
func (f *fixture) waitResults(t *testing.T, want map[Result]int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		ok := true
		for r, n := range want {
			ok = ok && f.metrics.get(r) == n
		}

		if ok {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("results = %v, want %v\nlog:\n%s", f.metrics.n, want, f.logs)
		}

		time.Sleep(time.Millisecond)
	}
}

// queued takes the events waiting for the sender.
func (f *fixture) queued() []Event {
	var out []Event

	for {
		select {
		case e := <-f.n.queue:
			out = append(out, e)
		default:
			return out
		}
	}
}

func kinds(evs []Event) []Kind {
	out := make([]Kind, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}

	return out
}

func mustKinds(t *testing.T, f *fixture, want ...Kind) []Event {
	t.Helper()

	evs := f.queued()

	if got := kinds(evs); strings.Join(toStrings(got), ",") != strings.Join(toStrings(want), ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}

	return evs
}

func toStrings(ks []Kind) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}

	return out
}

// Snapshots: one notification when they start failing, none while they
// keep failing, one when one succeeds again.
func TestSnapshotNotifications(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x")
	f.n.SetHost("nas")
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"app-db-1", "app-web-1"}})

	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded})
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotIncomplete, Error: "1 error"})
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotCanceled})
	mustKinds(t, f)

	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotUnknown, Error: "lost"})
	evs := mustKinds(t, f, SnapshotFailed)

	e := evs[0]
	if e.Path != "/data/app" || e.Host != "nas" || e.Reason != "failed" || e.Error != "disk full" ||
		strings.Join(e.Containers, ",") != "app-db-1,app-web-1" {
		t.Errorf("event = %+v", e)
	}

	if want := "The snapshot of /data/app on nas (app-db-1, app-web-1) failed: disk full."; e.Message() != want {
		t.Errorf("message = %q, want %q", e.Message(), want)
	}

	f.clock.advance(time.Hour)
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotIdentical})
	evs = mustKinds(t, f, SnapshotRecovered)

	if !evs[0].Since.Equal(e.Time) || evs[0].Error != "" {
		t.Errorf("recovered = %+v", evs[0])
	}

	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded})
	mustKinds(t, f)

	// An unknown end counts; a forgotten source is forgotten, failing or
	// not.
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotUnknown})
	evs = mustKinds(t, f, SnapshotFailed)

	if !strings.Contains(evs[0].Message(), "ended in an unknown state.") {
		t.Errorf("message = %q", evs[0].Message())
	}

	f.n.ForgetSource("/data/app")
	f.n.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded})
	mustKinds(t, f)
}

// Passes: an error the watcher does not retry is notified at once, a
// retried one after ErrorAfter, each once, and the next pass without an
// error notifies the recovery.
func TestPassNotifications(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x")

	// A configuration error.
	f.n.ReconcileError("/data/app", reconcile.ReasonConfig)
	f.n.PassDone("/data/app", reconcile.PassReport{Err: errors.New("invalid policy: bad cron")})
	evs := mustKinds(t, f, ReconcileFailed)

	if evs[0].Reason != "config" || evs[0].Error != "invalid policy: bad cron" {
		t.Errorf("event = %+v", evs[0])
	}

	// The same error again, now only logged at debug level by the
	// reconciler, so without a reason.
	f.n.PassDone("/data/app", reconcile.PassReport{Err: errors.New("invalid policy: bad cron")})
	mustKinds(t, f)

	f.n.PassDone("/data/app", reconcile.PassReport{})
	mustKinds(t, f, ReconcileRecovered)

	f.n.PassDone("/data/app", reconcile.PassReport{})
	mustKinds(t, f)

	// A problem with the labels fails the path too.
	f.n.ReconcileError("/data/app", reconcile.ReasonOwnersDisagree)
	f.n.PassDone("/data/app", reconcile.PassReport{Problems: []error{errors.New("app-1 and app-2 disagree")}})
	evs = mustKinds(t, f, ReconcileFailed)

	if evs[0].Reason != "owners_disagree" || evs[0].Error != "app-1 and app-2 disagree" {
		t.Errorf("event = %+v", evs[0])
	}

	f.n.PassDone("/data/app", reconcile.PassReport{})
	mustKinds(t, f, ReconcileRecovered)

	// A retried error that goes away within ErrorAfter is not notified.
	unavailable := reconcile.PassReport{Err: errors.New("kopia server unavailable"), Retry: true}

	f.n.ReconcileError("/data/app", reconcile.ReasonUnavailable)
	f.n.PassDone("/data/app", unavailable)
	f.clock.advance(10 * time.Minute)
	f.n.PassDone("/data/app", unavailable)
	f.n.PassDone("/data/app", reconcile.PassReport{})
	mustKinds(t, f)

	// One that lasts is, once.
	f.n.ReconcileError("/data/app", reconcile.ReasonUnavailable)
	f.n.PassDone("/data/app", unavailable)
	start := f.clock.Now()

	f.clock.advance(14 * time.Minute)
	f.n.ReconcileError("/data/app", reconcile.ReasonUnavailable)
	f.n.PassDone("/data/app", unavailable)
	mustKinds(t, f)

	f.clock.advance(time.Minute)
	f.n.ReconcileError("/data/app", reconcile.ReasonUnavailable)
	f.n.PassDone("/data/app", unavailable)
	evs = mustKinds(t, f, ReconcileFailed)

	if !evs[0].Since.Equal(start) || evs[0].Reason != "unavailable" ||
		!strings.Contains(evs[0].Message(), "It has been failing since 2026-10-23T12:10:00Z.") {
		t.Errorf("event = %+v: %s", evs[0], evs[0].Message())
	}

	f.clock.advance(time.Hour)
	f.n.PassDone("/data/app", unavailable)
	mustKinds(t, f)

	f.n.PassDone("/data/app", reconcile.PassReport{})
	evs = mustKinds(t, f, ReconcileRecovered)

	if !evs[0].Since.Equal(start) {
		t.Errorf("recovered = %+v", evs[0])
	}

	// A retried error followed by one that is not is notified at once.
	f.n.PassDone("/data/app", unavailable)
	f.n.ReconcileError("/data/app", reconcile.ReasonConfig)
	f.n.PassDone("/data/app", reconcile.PassReport{Err: errors.New("bad path")})
	mustKinds(t, f, ReconcileFailed)

	f.n.PassDone("/data/app", reconcile.PassReport{})
	mustKinds(t, f, ReconcileRecovered)

	// A problem with the labels is notified at once, with its reason,
	// also in a pass that ends with an error that is retried.
	f.n.ReconcileError("/data/app", reconcile.ReasonLabels)
	f.n.ReconcileError("/data/app", reconcile.ReasonTemporary)
	f.n.PassDone("/data/app", reconcile.PassReport{Err: errors.New("500"), Retry: true,
		Problems: []error{errors.New("invalid schedule")}})
	evs = mustKinds(t, f, ReconcileFailed)

	if evs[0].Reason != "labels" || evs[0].Error != "500; invalid schedule" {
		t.Errorf("event = %+v", evs[0])
	}

	// Errors about no path are left to the passes.
	f.n.ReconcileError("", reconcile.ReasonDocker)
	mustKinds(t, f)
}

// Every format posts what its service documents.
func TestFormats(t *testing.T) {
	failed := Event{
		Kind: SnapshotFailed, Path: "/data/app", Containers: []string{"app"}, Host: "nas", Reason: "failed",
		Error: "disk full", Since: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC), Time: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC),
	}
	recovered := failed
	recovered.Kind, recovered.Error = SnapshotRecovered, ""

	t.Run("json", func(t *testing.T) {
		f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/hook?token="+token)
		f.run(t)
		f.n.enqueue(failed)
		f.waitResults(t, map[Result]int{ResultSent: 1})

		r := f.hook.recorded()[0]
		if r.URL != "https://hooks.example.com/hook?token="+token || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %v", r.URL, r.Header)
		}

		var p map[string]any
		if err := json.Unmarshal(r.Body, &p); err != nil {
			t.Fatal(err)
		}

		for k, want := range map[string]any{
			"event": "snapshot_failed", "status": "failing", "title": "Backup failed: /data/app",
			"message": "The snapshot of /data/app on nas (app) failed: disk full.", "path": "/data/app",
			"host": "nas", "reason": "failed", "error": "disk full", "since": "2026-10-23T12:00:00Z",
			"time": "2026-10-23T12:00:00Z",
		} {
			if p[k] != want {
				t.Errorf("%s = %v, want %v", k, p[k], want)
			}
		}

		if c, ok := p["containers"].([]any); !ok || len(c) != 1 || c[0] != "app" {
			t.Errorf("containers = %v", p["containers"])
		}

		if _, ok := p["watcher_version"]; !ok {
			t.Error("no watcher_version")
		}
	})

	t.Run("ntfy", func(t *testing.T) {
		f := newFixture(t, config.WebhookNtfy, "https://ntfy.example.com/backups")
		f.run(t)
		f.n.enqueue(failed)
		f.waitResults(t, map[Result]int{ResultSent: 1})

		withUmlaut := recovered
		withUmlaut.Path = "/data/äpp"
		f.n.enqueue(withUmlaut)
		f.waitResults(t, map[Result]int{ResultSent: 2})

		reqs := f.hook.recorded()

		r := reqs[0]
		if r.URL != "https://ntfy.example.com/backups" || string(r.Body) != failed.Message() ||
			r.Header.Get("Title") != "Backup failed: /data/app" || r.Header.Get("Priority") != "high" ||
			r.Header.Get("Tags") != "warning" || r.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Errorf("request = %s %v %q", r.URL, r.Header, r.Body)
		}

		r = reqs[1]
		if r.Header.Get("Title") != "=?utf-8?b?QmFja3VwIHdvcmtzIGFnYWluOiAvZGF0YS/DpHBw?=" ||
			r.Header.Get("Priority") != "default" || r.Header.Get("Tags") != "white_check_mark" {
			t.Errorf("request = %v", r.Header)
		}
	})

	t.Run("slack", func(t *testing.T) {
		f := newFixture(t, config.WebhookSlack, "https://hooks.slack.com/services/T0/B0/"+token)
		f.run(t)

		e := failed
		e.Error = "a <b> & c"
		f.n.enqueue(e)
		f.waitResults(t, map[Result]int{ResultSent: 1})

		var p map[string]string
		if err := json.Unmarshal(f.hook.recorded()[0].Body, &p); err != nil {
			t.Fatal(err)
		}

		want := "*Backup failed: /data/app*\nThe snapshot of /data/app on nas (app) failed: a &lt;b&gt; &amp; c."
		if len(p) != 1 || p["text"] != want {
			t.Errorf("body = %v, want text %q", p, want)
		}
	})

	t.Run("telegram", func(t *testing.T) {
		f := newFixture(t, config.WebhookTelegram,
			"https://api.telegram.org/bot"+token+"/sendMessage?chat_id=-100123&message_thread_id=7&parse_mode=")
		f.run(t)
		f.n.enqueue(failed)
		f.waitResults(t, map[Result]int{ResultSent: 1})

		r := f.hook.recorded()[0]
		if r.URL != "https://api.telegram.org/bot"+token+"/sendMessage" {
			t.Errorf("URL = %s", r.URL)
		}

		var p map[string]any
		if err := json.Unmarshal(r.Body, &p); err != nil {
			t.Fatal(err)
		}

		if p["chat_id"] != float64(-100123) || p["message_thread_id"] != float64(7) || p["parse_mode"] != "" ||
			p["text"] != "Backup failed: /data/app\n\n"+failed.Message() {
			t.Errorf("body = %v", p)
		}
	})
}

// A temporary failure is tried again, honoring Retry-After; one that
// trying again cannot fix is not.
func TestRetries(t *testing.T) {
	e := Event{Kind: ReconcileFailed, Path: "/data/app"}

	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x", func(o *Options) {
		o.MaxRetryDelay = 50 * time.Millisecond
	})
	f.hook.statuses = []int{http.StatusInternalServerError, http.StatusTooManyRequests}
	f.hook.header = http.Header{"Retry-After": {"1"}}
	f.run(t)

	start := time.Now()

	f.n.enqueue(e)
	f.waitResults(t, map[Result]int{ResultSent: 1})

	if n := len(f.hook.recorded()); n != 3 {
		t.Errorf("%d requests, want 3", n)
	}

	// Retry-After is capped by MaxRetryDelay.
	if d := time.Since(start); d < 50*time.Millisecond || d > 3*time.Second {
		t.Errorf("took %v", d)
	}

	// All attempts fail.
	f.hook.mu.Lock()
	f.hook.statuses = []int{502, 503, 504, 500}
	f.hook.mu.Unlock()

	f.n.enqueue(e)
	f.waitResults(t, map[Result]int{ResultSent: 1, ResultFailed: 1})

	if n := len(f.hook.recorded()); n != 3+DefaultAttempts {
		t.Errorf("%d requests, want %d", n, 3+DefaultAttempts)
	}

	// A client error is not tried again.
	f.hook.mu.Lock()
	f.hook.statuses = []int{http.StatusBadRequest}
	f.hook.mu.Unlock()

	f.n.enqueue(e)
	f.waitResults(t, map[Result]int{ResultSent: 1, ResultFailed: 2})

	if n := len(f.hook.recorded()); n != 4+DefaultAttempts {
		t.Errorf("%d requests, want %d", n, 4+DefaultAttempts)
	}

	if !strings.Contains(f.logs.String(), "400 Bad Request: {") {
		t.Errorf("log lacks the response: %s", f.logs)
	}
}

// The token in the URL shows neither in the log nor in the errors.
func TestTokenNotLogged(t *testing.T) {
	f := newFixture(t, config.WebhookTelegram, "https://api.telegram.org/bot"+token+"/sendMessage?chat_id=1",
		func(o *Options) { o.Attempts = 2 })
	f.hook.err = errors.New("connection refused")
	f.run(t)

	f.n.enqueue(Event{Kind: SnapshotFailed, Path: "/data/app"})
	f.waitResults(t, map[Result]int{ResultFailed: 1})

	logs := f.logs.String()
	if strings.Contains(logs, token) || strings.Contains(logs, "sendMessage") {
		t.Errorf("the log shows the URL:\n%s", logs)
	}

	if !strings.Contains(logs, "https://api.telegram.org: connection refused") {
		t.Errorf("the log lacks the error:\n%s", logs)
	}

	if _, err := New(Options{URL: "ftp://x/" + token}); err == nil || strings.Contains(err.Error(), token) {
		t.Errorf("New: %v", err)
	}
}

// The reconciler never waits for the sender: a full queue drops, and a
// stop drops what waits.
func TestNeverBlocks(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x", func(o *Options) { o.QueueSize = 2 })
	f.hook.block = make(chan struct{})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		f.n.Run(ctx)
		close(done)
	}()

	finished := make(chan struct{})

	go func() {
		defer close(finished)

		for i := range 10 {
			p := "/data/app" + string(rune('0'+i))
			f.n.SnapshotFinished(p, backend.SnapshotResult{Outcome: backend.SnapshotFailed})
		}
	}()

	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("SnapshotFinished blocked")
	}

	// The sender holds one, two wait, the rest were dropped; the stop
	// drops all three.
	cancel()
	<-done

	f.waitResults(t, map[Result]int{ResultDropped: 10, ResultSent: 0})

	if !strings.Contains(f.logs.String(), "too many are waiting") {
		t.Errorf("log:\n%s", f.logs)
	}
}

// Hooks: a failure of each hook of each container is notified once, and
// so is the next success after it.
func TestHookNotifications(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x")
	f.n.SetHost("nas")
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"api", "db"}, Hooks: true})

	pre := func(container, err string) reconcile.HookResult {
		return reconcile.HookResult{Hook: reconcile.HookPreExec, Container: container, Error: err}
	}

	f.n.HookFinished("/data/app", pre("db", ""))
	mustKinds(t, f)

	f.n.HookFinished("/data/app", pre("db", "exited with code 1"))
	f.n.HookFinished("/data/app", pre("db", "exited with code 1"))
	evs := mustKinds(t, f, HookFailed)

	e := evs[0]
	if e.Reason != "pre-exec" || e.Container != "db" || e.Error != "exited with code 1" {
		t.Errorf("event = %+v", e)
	}

	if want := "The pre-exec hook in db for /data/app on nas (api, db) failed, so no snapshot was taken: exited with code 1."; e.Message() != want {
		t.Errorf("message = %q, want %q", e.Message(), want)
	}

	if e.Title() != "Backup hook failed: /data/app" || !e.Failing() {
		t.Errorf("title = %q", e.Title())
	}

	// Another container's success does not make db's hook fine.
	f.n.HookFinished("/data/app", pre("api", ""))
	f.n.HookFinished("/data/app", pre("db", "exited with code 1"))
	mustKinds(t, f)

	// The post-exec hook is followed apart.
	f.n.HookFinished("/data/app", reconcile.HookResult{Hook: reconcile.HookPostExec, Container: "db", Error: "timed out"})
	evs = mustKinds(t, f, HookFailed)

	if want := "The post-exec hook in db for /data/app on nas (api, db) failed: timed out."; evs[0].Message() != want {
		t.Errorf("message = %q, want %q", evs[0].Message(), want)
	}

	f.clock.advance(time.Hour)
	f.n.HookFinished("/data/app", pre("db", ""))
	evs = mustKinds(t, f, HookRecovered)

	if !evs[0].Since.Equal(e.Time) || evs[0].Reason != "pre-exec" || evs[0].Container != "db" || evs[0].Failing() {
		t.Errorf("recovered = %+v", evs[0])
	}

	// A source whose containers no longer have hooks forgets the failing
	// ones: a failure after they come back is notified.
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"api", "db"}})
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"api", "db"}, Hooks: true})
	f.n.HookFinished("/data/app", reconcile.HookResult{Hook: reconcile.HookPostExec, Container: "db", Error: "timed out"})
	mustKinds(t, f, HookFailed)

	// So does a source that loses the container.
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"api"}, Hooks: true})
	f.n.HookFinished("/data/app", reconcile.HookResult{Hook: reconcile.HookPostExec, Container: "db"})
	mustKinds(t, f)

	// A forgotten source forgets its failing hooks.
	f.n.HookFinished("/data/app", pre("api", "exited with code 2"))
	mustKinds(t, f, HookFailed)
	f.n.ForgetSource("/data/app")
	f.n.HookFinished("/data/app", pre("api", ""))
	mustKinds(t, f)
}

func TestReturnNotifications(t *testing.T) {
	f := newFixture(t, config.WebhookJSON, "https://hooks.example.com/x")
	f.n.SetHost("nas")
	f.n.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"db"}, Hooks: true})

	back := func(container, mode, err string) reconcile.ReturnResult {
		return reconcile.ReturnResult{Container: container, Mode: mode, Error: err}
	}

	f.n.ContainerReturned("/data/app", back("db", reconcile.ModeStop, ""))
	mustKinds(t, f)

	f.n.ContainerReturned("/data/app", back("api", reconcile.ModeStop, "port is already allocated"))
	f.n.ContainerReturned("/data/app", back("api", reconcile.ModeStop, "port is already allocated"))
	evs := mustKinds(t, f, ReturnFailed)

	e := evs[0]
	if e.Reason != reconcile.ModeStop || e.Container != "api" || !e.Failing() {
		t.Errorf("event = %+v", e)
	}

	want := "kopia-docker-watcher cannot start api again after the snapshot of /data/app on nas (db); it keeps trying: " +
		"port is already allocated."
	if e.Message() != want {
		t.Errorf("message = %q, want %q", e.Message(), want)
	}

	if e.Title() != "Container left down after a backup: api" {
		t.Errorf("title = %q", e.Title())
	}

	f.n.ContainerReturned("/data/app", back("cache", reconcile.ModePause, "conflict"))

	if evs := mustKinds(t, f, ReturnFailed); !strings.HasPrefix(evs[0].Message(), "kopia-docker-watcher cannot unpause cache again") {
		t.Errorf("message = %q", evs[0].Message())
	}

	f.clock.advance(time.Hour)
	f.n.ContainerReturned("/data/app", back("api", reconcile.ModeStop, ""))
	evs = mustKinds(t, f, ReturnRecovered)

	if evs[0].Container != "api" || evs[0].Failing() || !evs[0].Since.Equal(e.Time) ||
		evs[0].Title() != "Container runs again after a backup: api" {
		t.Errorf("recovered = %+v", evs[0])
	}

	// One that someone else removed is no longer held, and says so.
	f.n.ContainerReturned("/data/app", reconcile.ReturnResult{Container: "cache", Mode: reconcile.ModePause, Note: "it is gone"})
	evs = mustKinds(t, f, ReturnRecovered)

	if evs[0].Title() != "Container no longer held after a backup: cache" || !strings.HasPrefix(evs[0].Message(),
		"kopia-docker-watcher no longer tries to bring back cache after the snapshot of /data/app on nas (db), as it is gone;") {
		t.Errorf("recovered = %q: %q", evs[0].Title(), evs[0].Message())
	}

	// A forgotten source forgets its containers.
	f.n.ContainerReturned("/data/app", back("api", reconcile.ModeStop, "port is already allocated"))
	mustKinds(t, f, ReturnFailed)
	f.n.ForgetSource("/data/app")
	f.n.ContainerReturned("/data/app", back("api", reconcile.ModeStop, ""))
	mustKinds(t, f)
}
