package reconcile

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
)

type recordedObserver struct {
	mu      sync.Mutex
	reports []PassReport
}

func (o *recordedObserver) PassDone(_ string, rep PassReport) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.reports = append(o.reports, rep)
}

func (o *recordedObserver) get() []PassReport {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]PassReport(nil), o.reports...)
}

// The observer is told how every pass of Run ended, before afterPass.
func TestObserver(t *testing.T) {
	obs := &recordedObserver{}
	h := newHarness(t, func(o *Options) { o.Observer = obs })
	h.engine.run("app", enable, pathLabel(app))
	h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))

	passes := h.start()
	h.r.Enqueue(app)

	waitPass(t, passes, app, nil)
	waitPass(t, passes, app, succeeded)

	reports := obs.get()
	if len(reports) != 2 {
		t.Fatalf("reports = %+v", reports)
	}

	if !reports[0].Failed() || !reports[0].Retry || !strings.Contains(reports[0].Err.Error(), "500 from kopia server") {
		t.Errorf("first report = %+v", reports[0])
	}

	if reports[1].Failed() || reports[1].Retry {
		t.Errorf("second report = %+v", reports[1])
	}
}

// CheckProgress finds a pass that runs too long, and paths that wait while
// no pass ends.
func TestCheckProgress(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Workers = 1 })
	h.engine.run("app", enable, pathLabel(app))
	h.engine.run("db", enable, pathLabel("/data/db"))

	if err := h.r.CheckProgress(time.Now().Add(time.Hour), time.Minute); err != nil {
		t.Errorf("before Run: %v", err)
	}

	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method == backendtest.PlanSource && c.Source.Path == app {
			select {
			case entered <- struct{}{}:
			default:
			}

			<-release
		}

		return nil
	})

	passes := h.start()
	h.r.Enqueue(app)
	<-entered
	h.r.Enqueue("/data/db")

	p := h.r.Progress()
	if p.Running.IsZero() || p.Passes != 1 || p.Oldest.IsZero() || p.Queued != 1 || !p.LastDone.IsZero() {
		t.Errorf("progress = %+v", p)
	}

	if err := h.r.CheckProgress(time.Now(), time.Minute); err != nil {
		t.Errorf("a pass that just started: %v", err)
	}

	err := h.r.CheckProgress(time.Now().Add(2*time.Minute), time.Minute)
	if err == nil || !strings.Contains(err.Error(), "a pass has been running for 2m") {
		t.Errorf("a pass that runs too long: %v", err)
	}

	close(release)
	waitPass(t, passes, app, succeeded)
	waitPass(t, passes, "/data/db", succeeded)

	if p := h.r.Progress(); p.Passes != 0 || p.Queued != 0 || p.LastDone.IsZero() {
		t.Errorf("progress = %+v", p)
	}

	if err := h.r.CheckProgress(time.Now().Add(time.Hour), time.Minute); err != nil {
		t.Errorf("nothing to do: %v", err)
	}
}

// Paths that wait while no pass ends, as when the workers are gone.
func TestCheckProgressQueued(t *testing.T) {
	h := newHarness(t)
	start := time.Now()

	h.r.progressMu.Lock()
	h.r.runStart = start
	h.r.progressMu.Unlock()

	h.r.queue.add(app)

	if err := h.r.CheckProgress(start.Add(time.Minute), time.Minute); err != nil {
		t.Errorf("within the limit: %v", err)
	}

	err := h.r.CheckProgress(start.Add(2*time.Minute), time.Minute)
	if err == nil || !strings.Contains(err.Error(), "1 paths are waiting, and no pass has started or ended for 2m") {
		t.Errorf("err = %v", err)
	}

	h.r.progressMu.Lock()
	h.r.lastPassDone = start.Add(90 * time.Second)
	h.r.progressMu.Unlock()

	if err := h.r.CheckProgress(start.Add(2*time.Minute), time.Minute); err != nil {
		t.Errorf("a pass ended lately: %v", err)
	}

	// After a quiet stretch, a pass that started lately counts as
	// progress too.
	h.r.progressMu.Lock()
	h.r.passStarts["/data/db"] = start.Add(170 * time.Second)
	h.r.progressMu.Unlock()

	if err := h.r.CheckProgress(start.Add(3*time.Minute), time.Minute); err != nil {
		t.Errorf("a pass started lately: %v", err)
	}
}
