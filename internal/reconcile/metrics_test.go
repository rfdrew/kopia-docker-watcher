package reconcile

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// mustManaged checks what was last reported about the source of p.
func (h *harness) mustManaged(p string, want SourceMetrics) {
	h.t.Helper()

	got, ok := h.metrics.source(p)
	if !ok || got.Paused != want.Paused || !slices.Equal(got.Owners, want.Owners) {
		h.t.Fatalf("metrics of %s = %+v, managed %t; want %+v\nlog:\n%s", p, got, ok, want, h.logs)
	}
}

// mustNotManaged checks that the source of p does not count as managed.
func (h *harness) mustNotManaged(p string) {
	h.t.Helper()

	if got, ok := h.metrics.source(p); ok {
		h.t.Fatalf("%s counts as managed: %+v\nlog:\n%s", p, got, h.logs)
	}

	if t, ok := h.metrics.lastSuccess(p); ok {
		h.t.Fatalf("%s has a last success: %v", p, t)
	}
}

// mustLastSuccess checks the last success reported for p.
func (h *harness) mustLastSuccess(p string, want time.Time) {
	h.t.Helper()

	if got, ok := h.metrics.lastSuccess(p); !ok || !got.Equal(want) {
		h.t.Fatalf("last success of %s = %v, %t; want %v\nlog:\n%s", p, got, ok, want, h.logs)
	}
}

// listings returns the ListSnapshots calls for p.
func (h *harness) listings(p string) int {
	return len(h.fake.CallsFor(h.src(p), backendtest.ListSnapshots))
}

// A pass reports the source it manages: active while a container runs,
// paused once none does, with its owners, stopped ones included. A source
// just registered has no last success.
func TestMetricsSource(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.mustPass(app)

	h.mustManaged(app, SourceMetrics{Owners: []string{"app"}})
	h.mustLastSuccess(app, time.Time{})

	h.engine.create("worker", docker.StateExited, enable, pathLabel(app), noOnStart)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	h.mustManaged(app, SourceMetrics{Paused: true, Owners: []string{"app", "worker"}})

	h.engine.setState(id, docker.StateRunning)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	h.mustManaged(app, SourceMetrics{Owners: []string{"app", "worker"}})
}

// The last success counts the snapshots kopia server takes on schedule,
// which a pass learns of from the source's status. A checkpoint of a
// running upload, or a snapshot incomplete otherwise, does not count: the
// source's snapshots are then listed for the last complete one, once for
// each such last snapshot, and a listing that fails leaves the last
// success as it was.
func TestMetricsLastSuccess(t *testing.T) {
	h := newHarness(t)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.mustPass(app)

	scheduled := t0.Add(-time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: scheduled, EndTime: scheduled.Add(time.Minute)})
	h.mustPass(app)
	h.mustLastSuccess(app, scheduled)

	if n := h.listings(app); n != 0 {
		t.Fatalf("%d listings for a complete last snapshot", n)
	}

	// An upload saves a checkpoint.
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0, EndTime: t0.Add(time.Minute), Incomplete: "checkpoint"})
	h.mustPass(app)
	h.mustLastSuccess(app, scheduled)
	h.mustPass(app)
	h.mustLastSuccess(app, scheduled)

	if n := h.listings(app); n != 1 {
		t.Fatalf("%d listings for one checkpoint, want 1", n)
	}

	// The upload ends; the next one is canceled, and the listing fails.
	done := t0.Add(time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0, EndTime: done})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: done, EndTime: done, Incomplete: "canceled"})
	h.fake.FailNextFor(src, backendtest.ListSnapshots, temporary("ListSnapshots"))
	h.mustPass(app)
	h.mustLastSuccess(app, scheduled)

	if !h.logged("cannot list the snapshots of the source to find its last complete one") {
		t.Errorf("the failed listing is not logged:\n%s", h.logs)
	}

	h.mustPass(app)
	h.mustLastSuccess(app, t0)

	// A complete snapshot again needs no listing.
	n := h.listings(app)
	later := t0.Add(2 * time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: later, EndTime: later})
	h.mustPass(app)
	h.mustLastSuccess(app, later)

	if got := h.listings(app); got != n {
		t.Errorf("listed the snapshots for a complete last snapshot")
	}
}

// A source whose snapshots are all incomplete has no last success.
func TestMetricsNoCompleteSnapshot(t *testing.T) {
	h := newHarness(t)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0, EndTime: t0, Incomplete: "checkpoint"})
	h.mustPass(app)

	h.mustLastSuccess(app, time.Time{})
}

// The waiter reports how each snapshot the watcher queued ended, with the
// times of its task, once: a waiter whose wait is gone from the state
// reports nothing.
func TestMetricsSnapshotFinished(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	w := h.onlyWait(app)
	failed := backend.SnapshotResult{Outcome: backend.SnapshotFailed, TaskID: "7", Error: "upload error: boom",
		TaskStart: t0, TaskEnd: t0.Add(time.Minute)}
	h.fake.QueueSnapshotResult(h.src(app), failed)
	h.r.wait(t.Context(), w)

	if got := h.metrics.results(app); len(got) != 1 || !reflect.DeepEqual(got[0], failed) {
		t.Fatalf("results = %+v, want %+v", got, failed)
	}

	// The pass acts on the result and drops the wait; a waiter that comes
	// late reports nothing.
	h.mustPass(app)

	if ws := h.waits(app); len(ws) != 0 {
		t.Fatalf("waits = %+v", ws)
	}

	h.r.wait(t.Context(), w)

	if got := h.metrics.results(app); len(got) != 1 {
		t.Errorf("results = %+v, want the first only", got)
	}
}

// A run kopia server lost track of is no failure while its snapshot may be
// listed: the waiter leaves it to the pass, which reports it only when it
// finds no snapshot of the run, and once, also when it has to queue the
// snapshot once more in a later pass.
func TestMetricsLostRun(t *testing.T) {
	setup := func(t *testing.T) (*harness, state.SnapshotWait) {
		h := newHarness(t)
		withClock(h)
		id := h.engine.run("app", enable, pathLabel(app))
		h.mustPass(app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)

		w := h.onlyWait(app)
		h.fake.QueueSnapshotResult(h.src(app), backend.SnapshotResult{Outcome: backend.SnapshotUnknown, TaskID: "3",
			Error: "kopia server no longer knows task 3"})
		h.r.wait(t.Context(), w)

		if got := h.metrics.results(app); len(got) != 0 {
			t.Fatalf("the waiter reported %+v", got)
		}

		return h, w
	}

	t.Run("saved", func(t *testing.T) {
		h, w := setup(t)
		h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: w.Ticket.TriggeredAt, EndTime: w.Ticket.TriggeredAt})
		h.mustPass(app)

		if got := h.metrics.results(app); len(got) != 0 || len(h.waits(app)) != 0 {
			t.Errorf("results %+v, waits %+v", got, h.waits(app))
		}
	})

	t.Run("lost", func(t *testing.T) {
		h, _ := setup(t)
		h.fake.FailNextFor(h.src(app), backendtest.TriggerSnapshot, temporary("TriggerSnapshot"))

		if res := h.pass(app); res.err == nil {
			t.Fatal("queuing the snapshot again did not fail")
		}

		want := []backend.SnapshotResult{{Outcome: backend.SnapshotUnknown, TaskID: "3", Error: "kopia server no longer knows task 3"}}
		if got := h.metrics.results(app); !reflect.DeepEqual(got, want) {
			t.Fatalf("results = %+v, want %+v", got, want)
		}

		h.mustPass(app)

		if w := h.onlyWait(app); !w.Retried {
			t.Fatalf("the snapshot was not queued once more: %+v", w)
		}

		if got := h.metrics.results(app); len(got) != 1 {
			t.Errorf("results = %+v, want one", got)
		}
	})
}

// Once the cleanup has deleted the source, nothing is reported about it
// any more; until then it counts as a paused source without owners, with
// its last success.
func TestMetricsForgottenAfterCleanup(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.abandon([]string{cleanupAll}, snap(-time.Hour))

		h.mustManaged(app, SourceMetrics{Paused: true})
		h.mustLastSuccess(app, t0.Add(-time.Hour))

		c.add(grace)
		h.fullPass(app)
		h.mustNoSource(app)
		h.mustNotManaged(app)

		// Later passes leave it forgotten.
		h.fullPass(app)
		h.mustNotManaged(app)
	})

	// The policy of a source without snapshots goes, kopia server forgets
	// the source, and the pass after forgets its record too.
	t.Run("policy", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.abandon([]string{cleanupPolicy})

		c.add(grace)
		res := h.fullPass(app)
		h.mustNoSource(app)

		if !res.recheck {
			t.Fatal("no recheck after the policy was deleted")
		}

		h.mustPass(app)

		if _, ok := h.store.Get(h.src(app)); ok {
			t.Fatal("the record is left")
		}

		h.mustNotManaged(app)
	})
}

// A source the watcher does not manage is not reported: one kopia server
// refuses to register, or no longer knows, also while the state keeps its
// record, one not registered until a container runs, one outside
// WATCHER_PATH_PREFIX, and nothing in a dry run.
func TestMetricsNotManaged(t *testing.T) {
	t.Run("registration refused", func(t *testing.T) {
		h := newHarness(t)
		h.fake.SetPathMissing(app)
		h.engine.run("app", enable, pathLabel(app), noOnStart)

		if res := h.pass(app); res.err == nil {
			t.Fatal("the registration went through")
		}

		h.mustNotManaged(app)
	})

	// kopia server forgot a source the watcher managed, and refuses it
	// again: the path is gone from its host.
	t.Run("registration refused again", func(t *testing.T) {
		h := newHarness(t)
		h.engine.run("app", enable, pathLabel(app), noOnStart)
		h.mustPass(app)
		h.mustManaged(app, SourceMetrics{Owners: []string{"app"}})

		if err := h.fake.DeletePolicy(t.Context(), h.src(app)); err != nil || h.fake.Known(h.src(app)) {
			t.Fatalf("the fake still knows the source: %v", err)
		}

		h.fake.SetPathMissing(app)

		if res := h.pass(app); res.err == nil {
			t.Fatal("the registration went through")
		}

		if _, ok := h.store.Get(h.src(app)); !ok {
			t.Fatal("the record is gone")
		}

		h.mustNotManaged(app)
	})

	// A paused source deleted behind the watcher's back keeps its record,
	// as its owners asked for the pause, but kopia server has nothing of it.
	t.Run("forgotten by kopia server", func(t *testing.T) {
		h, _ := cleanupHarness(t)
		h.abandon(nil)
		h.mustManaged(app, SourceMetrics{Paused: true})

		if err := h.fake.DeletePolicy(t.Context(), h.src(app)); err != nil || h.fake.Known(h.src(app)) {
			t.Fatalf("the fake still knows the source: %v", err)
		}

		h.fullPass(app)

		if _, ok := h.store.Get(h.src(app)); !ok {
			t.Fatal("the record is gone")
		}

		h.mustNotManaged(app)
	})

	t.Run("only created", func(t *testing.T) {
		h := newHarness(t)
		h.engine.create("app", docker.StateCreated, enable, pathLabel(app), noOnStart)
		h.mustPass(app)
		h.mustNotManaged(app)
	})

	t.Run("outside WATCHER_PATH_PREFIX", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.engine.run("app", enable, pathLabel(app), noOnStart)
		h.mustPass(app)
		h.mustManaged(app, SourceMetrics{Owners: []string{"app"}})

		h.restart(c, withWatcherPrefix("/srv"))
		h.mustPass(app)
		h.mustNotManaged(app)
	})

	// A dry run over a source the state has, and over one it would
	// register.
	t.Run("dry run", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.engine.run("app", enable, pathLabel(app), noOnStart)
		h.engine.run("db", enable, pathLabel("/data/db"), noOnStart)
		h.mustPass(app)

		h.restart(c, func(o *Options) { o.DryRun = true })
		h.metrics.resetCalls()
		h.mustPass(app)
		h.mustPass("/data/db")

		if got := h.metrics.recorded(); len(got) != 0 {
			t.Errorf("calls in a dry run: %q", got)
		}
	})
}

// A pass that fails before its policy step leaves what was reported as it
// was; one that fails after it reports the source as its policy now is.
func TestMetricsFailedPass(t *testing.T) {
	h := newHarness(t)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart, onStop)
	h.mustPass(app)

	h.fake.FailNextFor(src, backendtest.PlanSource, temporary("PlanSource"))
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))

	if res := h.pass(app); res.err == nil {
		t.Fatal("the pass did not fail")
	}

	h.mustManaged(app, SourceMetrics{Owners: []string{"app"}})

	reports := h.metrics.sourceReports(app)

	// The stop snapshot cannot be queued after the pause is written.
	h.fake.FailNextFor(src, backendtest.TriggerSnapshot, temporary("TriggerSnapshot"))

	if res := h.pass(app); res.err == nil || !h.paused(app) {
		t.Fatalf("pass = %v, paused %t", res.err, h.paused(app))
	}

	h.mustManaged(app, SourceMetrics{Paused: true, Owners: []string{"app"}})

	if n := h.metrics.sourceReports(app); n != reports+1 {
		t.Errorf("%d reports, want %d", n, reports+1)
	}
}
