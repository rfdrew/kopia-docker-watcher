package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

func temporary(op string) error {
	return &backend.Error{Op: op, Kind: backend.ErrTemporary, Err: errors.New("500 from kopia server")}
}

// A temporary error retries the path after a delay until it goes through;
// the delay grows with every failure in a row and starts over after a
// success.
func TestRetryAfterTemporaryError(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	h.fake.FailNext(backendtest.ApplyPlan, temporary("ApplyPlan"))
	h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))
	h.fake.FailNext(backendtest.PlanSource, &backend.Error{Op: "PlanSource", Kind: backend.ErrUnavailable, Err: errors.New("connection refused")})

	passes := h.start()
	h.r.Enqueue(app)

	for i := range 3 {
		res := waitPass(t, passes, app, nil)
		if res.err == nil || !res.retry {
			t.Fatalf("pass %d: err %v, retry %v", i, res.err, res.retry)
		}
	}

	waitPass(t, passes, app, succeeded)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if n, u := h.metrics.count(app, ReasonTemporary), h.metrics.count(app, ReasonUnavailable); n != 2 || u != 1 {
		t.Errorf("%d temporary and %d unavailable errors counted", n, u)
	}

	h.r.mu.Lock()
	failures := h.r.failures[app]
	h.r.mu.Unlock()

	if failures != 0 {
		t.Errorf("failures = %d after a success", failures)
	}
}

func TestBackoff(t *testing.T) {
	for n, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 9: 256 * time.Second, 10: 5 * time.Minute, 1000: 5 * time.Minute} {
		if got := backoff(time.Second, 5*time.Minute, n); got != want {
			t.Errorf("backoff(%d) = %v, want %v", n, got, want)
		}
	}
}

// A configuration error is reported and not retried: a missing path is
// not registered, and the path is tried again only when asked.
func TestConfigErrorNotRetried(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.SetPathMissing(app)

	passes := h.start()
	h.r.Enqueue(app)

	res := waitPass(t, passes, app, nil)
	if !backend.IsConfig(res.err) || res.retry {
		t.Fatalf("err %v, retry %v", res.err, res.retry)
	}

	time.Sleep(50 * time.Millisecond)

	if n := len(h.fake.CallsTo(backendtest.ApplyPlan)); n != 1 {
		t.Errorf("%d ApplyPlan calls, want 1", n)
	}

	h.mustNoSource(app)

	if h.metrics.count(app, ReasonConfig) != 1 || !h.logged("kopia server rejects the source", app) {
		t.Errorf("config error not reported:\n%s", h.logs)
	}
}

// A cron kopia server rejects: a new source is not registered, a known one
// keeps its last applied patch.
func TestRejectedSchedule(t *testing.T) {
	h := newHarness(t)
	h.fake.RejectCron("0 25 * * *")

	id := h.engine.run("app", enable, pathLabel(app), scheduleLabel("0 25 * * *"))

	if res := h.pass(app); !backend.IsConfig(res.err) {
		t.Fatalf("new source with a bad cron: %v", res.err)
	}

	h.mustNoSource(app)

	h.engine.remove(id)
	id = h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), scheduleLabel("0 25 * * *"))

	res := h.mustPass(app)
	if len(res.problems) != 1 || !backend.IsConfig(res.problems[0]) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if a, _ := h.applied(app); !a.Patch.Equal(cronPatch("0 * * * *")) || a.Pending != nil {
		t.Errorf("applied = %+v", a)
	}
}

// kopia server refuses on write what the schedule check let through: the
// source falls back to its last applied patch and is still paused and
// unpaused.
func TestApplyRejectedFallsBack(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.remove(id)
	id = h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	// The schedule check lets the new cron through; the write refuses it.
	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method == backendtest.ApplyPlan && c.Plan.Applied.Patch.Equal(cronPatch("30 2 * * *")) {
			return &backend.Error{Op: "ApplyPlan", Kind: backend.ErrConfig, Err: errors.New("failed to validate policy")}
		}

		return nil
	})

	res := h.mustPass(app)
	if len(res.problems) != 1 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if a, _ := h.applied(app); a.Pending != nil || !a.Patch.Equal(cronPatch("0 * * * *")) {
		t.Errorf("applied = %+v", a)
	}

	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
}

// A Docker that cannot list containers retries the path.
func TestDockerErrorRetried(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.engine.failNext(errors.New("cannot connect to the Docker daemon"))

	passes := h.start()
	h.r.Enqueue(app)

	if res := waitPass(t, passes, app, nil); res.err == nil || !res.retry {
		t.Fatalf("err %v, retry %v", res.err, res.retry)
	}

	waitPass(t, passes, app, succeeded)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if h.metrics.count(app, ReasonDocker) != 1 {
		t.Error("Docker error not counted")
	}
}

// A full reconcile whose container listing fails is tried again.
func TestScanRetried(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.engine.failNext(errors.New("cannot connect to the Docker daemon"))

	passes := h.start()
	h.r.ReconcileAll()

	waitPass(t, passes, app, succeeded)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// An error that is neither temporary nor about the source's configuration
// is reported and not retried.
func TestOtherErrorNotRetried(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.FailNext(backendtest.PlanSource, &backend.Error{Op: "PlanSource", Err: errors.New("401 Unauthorized")})

	res := h.pass(app)
	if res.err == nil || res.retry {
		t.Fatalf("err %v, retry %v", res.err, res.retry)
	}

	if h.metrics.count(app, ReasonFailed) != 1 {
		t.Error("not counted")
	}
}

// kopia server's identity changes: the pass is retried, the identity asked
// again, and every path is reconciled under the new one.
func TestIdentityChange(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.engine.run("db", enable, pathLabel("/data/db"))

	passes := h.start()
	h.r.ReconcileAll()
	waitPasses(t, passes, app, "/data/db")

	h.fake.SetIdentity("root", "kopia-2")
	h.r.Enqueue(app)

	newSrc := func(p string) backend.Source { return backend.Source{Host: "kopia-2", UserName: "root", Path: p} }

	eventually(t, "both sources registered under the new identity", func() bool {
		return h.fake.Known(newSrc(app)) && h.fake.Known(newSrc("/data/db"))
	})

	if !h.logged("kopia server's identity changed; reconciling every path") {
		t.Errorf("identity change not logged:\n%s", h.logs)
	}
}

// The context ends during a pass: nothing is reported or retried.
func TestCanceledPass(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method == backendtest.ApplyPlan {
			cancel()
		}

		return nil
	})

	res := h.r.reconcile(ctx, app)
	if res.err != nil || res.retry {
		t.Errorf("err %v, retry %v", res.err, res.retry)
	}

	if h.metrics.count(app, ReasonFailed)+h.metrics.count(app, ReasonTemporary) != 0 {
		t.Error("a canceled pass was counted")
	}
}
