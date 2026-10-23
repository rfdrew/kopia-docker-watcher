package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// probe is a check whose result the test sets.
type probe struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (p *probe) set(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.err = err
}

func (p *probe) check(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++

	return p.err
}

func newTestChecker(t *testing.T, logs *bytes.Buffer, names ...string) (*Checker, *clock) {
	t.Helper()

	clk := &clock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	c := New(Options{
		FailAfter:  time.Minute,
		StartGrace: 2 * time.Minute,
		Now:        clk.Now,
		Logger:     slog.New(slog.NewTextHandler(logs, nil)),
	}, names...)

	return c, clk
}

// serve returns what /healthz answers.
func serve(t *testing.T, c *Checker) (int, Report) {
	t.Helper()

	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody))

	var r Report
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	return rec.Code, r
}

func mustServe(t *testing.T, c *Checker, code int, status Status, comps map[string]Status) Report {
	t.Helper()

	gotCode, r := serve(t, c)
	if gotCode != code || r.Status != status {
		t.Fatalf("/healthz = %d %s, want %d %s: %+v", gotCode, r.Status, code, status, r)
	}

	for name, want := range comps {
		if got := r.Components[name].Status; got != want {
			t.Errorf("component %s = %s, want %s: %+v", name, got, want, r.Components[name])
		}
	}

	return r
}

// The watcher starts with every component pending, which is starting
// within the grace period and ok after it.
func TestPendingComponents(t *testing.T) {
	var logs bytes.Buffer

	c, clk := newTestChecker(t, &logs, "docker", "kopia", "reconcile")

	r := mustServe(t, c, http.StatusOK, StatusStarting,
		map[string]Status{"docker": StatusPending, "kopia": StatusPending, "reconcile": StatusPending})
	if r.Components["docker"].Note != "not started" {
		t.Errorf("note = %q", r.Components["docker"].Note)
	}

	c.Pending("docker", "waiting for kopia server")

	var kopia probe

	c.Set("kopia", kopia.check)

	r = mustServe(t, c, http.StatusOK, StatusStarting, map[string]Status{"kopia": StatusPending})
	if r.Components["kopia"].Note != "first check running" || r.Components["docker"].Note != "waiting for kopia server" {
		t.Errorf("notes = %+v", r.Components)
	}

	c.CheckNow(t.Context())
	mustServe(t, c, http.StatusOK, StatusStarting, map[string]Status{"kopia": StatusOK, "docker": StatusPending})

	clk.advance(2 * time.Minute)
	mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"kopia": StatusOK, "docker": StatusPending})
}

// A component fails once its checks failed for FailAfter; within the
// grace period that makes the watcher starting, after it unhealthy, and
// it is ok again with the next successful check.
func TestFailingComponent(t *testing.T) {
	var logs bytes.Buffer

	c, clk := newTestChecker(t, &logs, "docker", "kopia")

	var docker, kopia probe

	c.Set("docker", docker.check)
	c.Set("kopia", kopia.check)

	kopia.set(errors.New("connection refused"))
	c.CheckNow(t.Context())

	// One failed check is no failure yet.
	r := mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"docker": StatusOK, "kopia": StatusOK})
	if r.Components["kopia"].Error != "connection refused" {
		t.Errorf("kopia = %+v", r.Components["kopia"])
	}

	// Failing within the grace period.
	clk.advance(time.Minute)
	c.CheckNow(t.Context())
	r = mustServe(t, c, http.StatusOK, StatusStarting, map[string]Status{"docker": StatusOK, "kopia": StatusFailing})

	if !r.Components["kopia"].FailingSince.Equal(clk.Now().Add(-time.Minute)) || !r.Components["kopia"].LastOK.IsZero() {
		t.Errorf("kopia = %+v", r.Components["kopia"])
	}

	if !strings.Contains(logs.String(), "health check failing") {
		t.Errorf("no warning logged: %s", logs.String())
	}

	// Unhealthy after it.
	clk.advance(time.Minute)
	mustServe(t, c, http.StatusServiceUnavailable, StatusUnhealthy, map[string]Status{"kopia": StatusFailing})

	// Recovered.
	kopia.set(nil)
	c.CheckNow(t.Context())
	r = mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"kopia": StatusOK})

	if r.Components["kopia"].Error != "" || !r.Components["kopia"].LastOK.Equal(clk.Now()) {
		t.Errorf("kopia = %+v", r.Components["kopia"])
	}

	if !strings.Contains(logs.String(), "health check passes again") {
		t.Errorf("no recovery logged: %s", logs.String())
	}

	// After the grace period a failure needs FailAfter as well, counted
	// from the first failed check.
	docker.set(errors.New("daemon is busy"))
	c.CheckNow(t.Context())
	mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"docker": StatusOK})

	clk.advance(30 * time.Second)
	c.CheckNow(t.Context())
	mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"docker": StatusOK})

	clk.advance(30 * time.Second)
	c.CheckNow(t.Context())
	mustServe(t, c, http.StatusServiceUnavailable, StatusUnhealthy, map[string]Status{"docker": StatusFailing, "kopia": StatusOK})

	// A success in between starts the count again.
	docker.set(nil)
	c.CheckNow(t.Context())
	docker.set(errors.New("daemon is busy"))
	clk.advance(50 * time.Second)
	c.CheckNow(t.Context())
	mustServe(t, c, http.StatusOK, StatusOK, map[string]Status{"docker": StatusOK})
}

// Run checks right away, on Set and every Interval, and bounds every check
// by Timeout; a check that ends because Run stops changes nothing.
func TestRun(t *testing.T) {
	c := New(Options{Interval: 10 * time.Millisecond, Timeout: 20 * time.Millisecond, FailAfter: time.Nanosecond,
		StartGrace: time.Nanosecond}, "slow")

	var fast probe

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		c.Run(ctx)
		close(done)
	}()

	c.Set("fast", fast.check)
	c.Set("slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		r := c.Report()
		if r.Components["slow"].Status == StatusFailing && r.Components["fast"].Status == StatusOK {
			if !strings.Contains(r.Components["slow"].Error, "deadline exceeded") {
				t.Errorf("slow = %+v", r.Components["slow"])
			}

			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("report = %+v", r)
		}

		time.Sleep(5 * time.Millisecond)
	}

	fast.mu.Lock()
	calls := fast.calls
	fast.mu.Unlock()

	if calls < 2 {
		t.Errorf("fast check ran %d times", calls)
	}

	cancel()
	<-done
}
