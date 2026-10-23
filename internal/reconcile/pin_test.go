package reconcile

import (
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const pinLabel = "kopia.pin-on-stop=before-update-{date}-{time}"

// stop stops the container id at the test clock's time, with an event of
// that time, and runs a pass over p.
func (h *harness) stop(c *testClock, id, p string) {
	h.t.Helper()

	h.engine.setState(id, docker.StateExited)

	ev := h.engine.event(docker.KindDied, id)
	ev.Time = c.now()
	h.r.HandleEvent(ev)
	h.mustPass(p)
}

// pins returns the pins of the snapshot of p that started at start.
func (h *harness) pins(p string, start time.Time) []string {
	h.t.Helper()

	for _, s := range h.fake.Snapshots(h.src(p)) {
		if s.StartTime.Equal(start) {
			return s.Pins
		}
	}

	h.t.Fatalf("no snapshot of %s started at %v", p, start)

	return nil
}

// The stop snapshot gets the pin of the container that stopped, filled in
// with the time of the stop, in the same edit as its description; the pin
// of another owner of the path, which keeps running, is not added.
func TestPinOnStop(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel, "kopia.description=App")
	h.engine.run("other", enable, pathLabel(app), onStop, "kopia.pin-on-stop=other")
	h.mustPass(app)

	c.add(time.Minute)
	h.stop(c, id, app)

	want := []string{"before-update-2026-10-18-12-01-00"}

	w := h.onlyWait(app)
	if got := h.record(app).WaitPins[w.ID]; !slices.Equal(got, want) {
		t.Fatalf("wait pins = %v, want %v", got, want)
	}

	h.r.wait(t.Context(), w)
	h.mustPass(app)

	got := h.fake.Annotations(src)
	if len(got) != 1 || !got[0].StartTime.Equal(w.Ticket.TriggeredAt) || got[0].Description != "App, other" ||
		!slices.Equal(got[0].AddPins, want) {
		t.Fatalf("annotations = %+v", got)
	}

	if p := h.pins(app, w.Ticket.TriggeredAt); !slices.Equal(p, want) {
		t.Errorf("pins = %v", p)
	}

	if rec := h.record(app); rec.WaitPins != nil || len(rec.SnapshotWaits) != 0 {
		t.Errorf("record = %+v", rec)
	}

	if !h.logged("snapshot pinned", "before-update-2026-10-18-12-01-00") {
		t.Errorf("log:\n%s", h.logs)
	}
}

// A run that saves nothing, because nothing changed since the last
// snapshot, pins the last complete snapshot, which holds the same files,
// and leaves its description alone.
func TestPinOnStopIdentical(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)

	last := t0.Add(-time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: last.Add(-time.Hour), EndTime: last.Add(-time.Hour)})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: last, EndTime: last, Description: "old"})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: last.Add(time.Minute), Incomplete: "checkpoint"})

	c.add(time.Minute)
	h.stop(c, id, app)
	h.fake.QueueSnapshotResult(src, backend.SnapshotResult{Outcome: backend.SnapshotIdentical, TaskID: "1"})
	h.r.wait(t.Context(), h.onlyWait(app))
	h.mustPass(app)

	got := h.fake.Annotations(src)
	if len(got) != 1 || !got[0].StartTime.Equal(last) || got[0].Description != "" || len(got[0].AddPins) != 1 {
		t.Fatalf("annotations = %+v\nlog:\n%s", got, h.logs)
	}

	for _, s := range h.fake.Snapshots(src) {
		if s.StartTime.Equal(last) && (s.Description != "old" || len(s.Pins) != 1) || !s.StartTime.Equal(last) && len(s.Pins) != 0 {
			t.Errorf("snapshot = %+v", s)
		}
	}

	if len(h.waits(app)) != 0 {
		t.Errorf("waits = %+v", h.waits(app))
	}
}

// A run that fails pins nothing, with a warning.
func TestPinOnStopFailed(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)

	c.add(time.Minute)
	h.stop(c, id, app)
	h.fake.QueueSnapshotResult(src, backend.SnapshotResult{Outcome: backend.SnapshotFailed, TaskID: "1", Error: "disk full"})
	h.r.wait(t.Context(), h.onlyWait(app))
	h.mustPass(app)

	if got := h.fake.Annotations(src); len(got) != 0 {
		t.Errorf("annotations = %+v", got)
	}

	if !h.logged("not pinning the stop snapshot: the run did not save a complete one") || h.record(app).WaitPins != nil {
		t.Errorf("log:\n%s", h.logs)
	}
}

// A stop that a snapshot already pending absorbs pins the snapshot that
// counts as the stop snapshot: the one the pending run saves, or, if it
// saves none, the one taken again.
func TestPinOnStopAbsorbed(t *testing.T) {
	setup := func(t *testing.T) (*harness, *testClock) {
		h := newHarness(t)
		c := withClock(h)
		id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
		h.mustPass(app)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPending})

		c.add(time.Minute)
		h.stop(c, id, app)

		if n := h.triggers(app); n != 0 {
			t.Fatalf("%d triggers while pending", n)
		}

		if got := h.record(app).StopAbsorbedPins; !slices.Equal(got, []string{"before-update-2026-10-18-12-01-00"}) {
			t.Fatalf("absorbed pins = %v", got)
		}

		return h, c
	}

	t.Run("saved", func(t *testing.T) {
		h, c := setup(t)
		src := h.src(app)

		start := c.now().Add(time.Second)
		h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: start, EndTime: start, Description: "set elsewhere"})
		h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle, LastSnapshot: &backend.SnapshotInfo{StartTime: start}})
		h.mustPass(app)

		got := h.fake.Annotations(src)
		if len(got) != 1 || !got[0].StartTime.Equal(start) || got[0].Description != "" || len(got[0].AddPins) != 1 {
			t.Fatalf("annotations = %+v", got)
		}

		if rec := h.record(app); !rec.StopAbsorbedAt.IsZero() || rec.StopAbsorbedPins != nil {
			t.Errorf("record = %+v", rec)
		}
	})

	t.Run("lost", func(t *testing.T) {
		h, _ := setup(t)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
		h.mustPass(app)

		w := h.onlyWait(app)
		if got := h.record(app).WaitPins[w.ID]; !w.Stop || !slices.Equal(got, []string{"before-update-2026-10-18-12-01-00"}) {
			t.Errorf("wait = %+v, pins %v", w, got)
		}
	})
}

// A stop absorbed by a pending run the watcher follows gives that run the
// pin; when it saves nothing, the last complete snapshot gets it.
func TestPinOnStopAbsorbedByOwnSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(-time.Hour), EndTime: t0.Add(-time.Hour)})

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})

	c.add(time.Minute)
	h.stop(c, id, app)

	w := h.onlyWait(app)
	if got := h.record(app).WaitPins[w.ID]; !w.Stop || len(got) != 1 {
		t.Fatalf("wait = %+v, pins %v", w, got)
	}

	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle})
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotIdentical})
	h.mustPass(app)

	if p := h.pins(app, t0.Add(-time.Hour)); !slices.Equal(p, []string{"before-update-2026-10-18-12-01-00"}) {
		t.Errorf("pins = %v\nlog:\n%s", p, h.logs)
	}

	if n := h.triggers(app); n != 1 {
		t.Errorf("%d triggers", n)
	}
}

// The pins survive a restart of the watcher between the snapshot and its
// pin, also when the container is gone by then.
func TestPinOnStopAfterRestart(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)

	c.add(time.Minute)
	h.stop(c, id, app)
	h.r.HandleEvent(h.engine.remove(id))

	w := h.onlyWait(app)
	h.r.wait(t.Context(), w)

	h.restart(c)
	h.mustPass(app)

	if p := h.pins(app, w.Ticket.TriggeredAt); !slices.Equal(p, []string{"before-update-2026-10-18-12-01-00"}) {
		t.Errorf("pins = %v\nlog:\n%s", p, h.logs)
	}

	if got := h.fake.Annotations(src); len(got) != 1 {
		t.Errorf("annotations = %+v", got)
	}
}

// A run that kopia server lost is taken again with the pins of the first.
func TestPinOnStopRetried(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)

	c.add(time.Minute)
	h.stop(c, id, app)
	first := h.onlyWait(app)

	c.add(time.Minute)
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
	h.mustPass(app)

	w := h.onlyWait(app)
	if got := h.record(app).WaitPins; w.ID == first.ID || !w.Retried || len(got) != 1 ||
		!slices.Equal(got[w.ID], []string{"before-update-2026-10-18-12-01-00"}) {
		t.Errorf("wait = %+v, pins %v", w, got)
	}
}

// An invalid pin is a warning and adds no pin; so is a pin without a
// snapshot at the stop, which takes none.
func TestPinOnStopLabelProblems(t *testing.T) {
	for _, tt := range []struct {
		name     string
		labels   []string
		warning  string
		triggers int
	}{
		{"invalid", []string{onStop, "kopia.pin-on-stop=before update"}, `invalid pin \"before update\"`, 1},
		{"without snapshot-on-stop", []string{"kopia.pin-on-stop=keep"}, "has no effect unless kopia.snapshot-on-stop is true", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)

			id := h.engine.run("app", append([]string{enable, pathLabel(app)}, tt.labels...)...)
			h.mustPass(app)

			c.add(time.Minute)
			h.stop(c, id, app)

			if n := h.triggers(app); n != tt.triggers {
				t.Errorf("%d triggers, want %d", n, tt.triggers)
			}

			if rec := h.record(app); rec.WaitPins != nil {
				t.Errorf("wait pins = %v", rec.WaitPins)
			}

			if !h.logged("", tt.warning) {
				t.Errorf("no warning %q in the log:\n%s", tt.warning, h.logs)
			}
		})
	}
}

// A run that saved nothing pins the last snapshot from before it was
// queued, not one taken after the stop, as of the updated service, when
// the pin comes late.
func TestPinOnStopIdenticalIgnoresLaterSnapshots(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, pinLabel)
	h.mustPass(app)

	before := t0.Add(-time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: before, EndTime: before})

	c.add(time.Minute)
	h.stop(c, id, app)
	h.fake.QueueSnapshotResult(src, backend.SnapshotResult{Outcome: backend.SnapshotIdentical, TaskID: "1"})
	h.r.wait(t.Context(), h.onlyWait(app))

	// Listing fails at the pass that would pin; a scheduled snapshot of
	// the updated service comes before the next.
	h.fake.FailNext(backendtest.ListSnapshots, temporary("ListSnapshots"))
	h.mustPass(app)

	if len(h.waits(app)) != 1 {
		t.Fatalf("the wait is not kept after a passing failure: %+v", h.waits(app))
	}

	after := c.now().Add(5 * time.Minute)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: after, EndTime: after})
	c.add(10 * time.Minute)
	h.mustPass(app)

	if p := h.pins(app, before); len(p) != 1 {
		t.Errorf("pins of the snapshot before = %v\nlog:\n%s", p, h.logs)
	}

	if p := h.pins(app, after); len(p) != 0 {
		t.Errorf("pins of the later snapshot = %v", p)
	}
}

// When the snapshot of an earlier absorbed stop is saved but cannot be
// pinned yet, a new stop neither clears it nor merges into it: the earlier
// snapshot gets the earlier pin, and the new stop is decided at a later
// pass, with its own.
func TestPinOnStopEarlierAbsorbedHeld(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	a := h.engine.run("a", enable, pathLabel(app), onStop, "kopia.pin-on-stop=a-{time}")
	b := h.engine.run("b", enable, pathLabel(app), onStop, "kopia.pin-on-stop=b-{time}")
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})

	c.add(time.Minute)
	h.stop(c, a, app)

	// The pending run saves S1, and another one is pending when b stops;
	// describing S1 fails then.
	s1 := c.now().Add(time.Second)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: s1, EndTime: s1})
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending, LastSnapshot: &backend.SnapshotInfo{StartTime: s1}})
	h.fake.FailNext(backendtest.ListSnapshots, temporary("ListSnapshots"))

	c.add(time.Minute)
	h.stop(c, b, app)

	if rec := h.record(app); !slices.Equal(rec.StopAbsorbedPins, []string{"a-12-01-00"}) {
		t.Fatalf("absorbed pins after the failure = %v", rec.StopAbsorbedPins)
	}

	// The kept stop of b: S1 gets a's pin, and b's stop is absorbed by the
	// run pending now.
	h.mustPass(app)

	if p := h.pins(app, s1); !slices.Equal(p, []string{"a-12-01-00"}) {
		t.Errorf("pins of S1 = %v\nlog:\n%s", p, h.logs)
	}

	if rec := h.record(app); !slices.Equal(rec.StopAbsorbedPins, []string{"b-12-02-00"}) || !rec.StopAbsorbedPinsAt.Equal(rec.StopAbsorbedAt) {
		t.Errorf("absorbed stop = %v %v %v", rec.StopAbsorbedAt, rec.StopAbsorbedPins, rec.StopAbsorbedPinsAt)
	}
}

// A new stop snapshot queued while the saved snapshot of an absorbed stop
// cannot be pinned yet leaves the absorbed stop for a later pass.
func TestPinOnStopAbsorbedKeptForNewStop(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	a := h.engine.run("a", enable, pathLabel(app), onStop, "kopia.pin-on-stop=a")
	b := h.engine.run("b", enable, pathLabel(app), onStop, "kopia.pin-on-stop=b")
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})

	c.add(time.Minute)
	h.stop(c, a, app)

	s1 := c.now().Add(time.Second)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: s1, EndTime: s1})
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle, LastSnapshot: &backend.SnapshotInfo{StartTime: s1}})
	h.fake.FailNext(backendtest.ListSnapshots, temporary("ListSnapshots"))

	c.add(time.Minute)
	h.stop(c, b, app)

	w := h.onlyWait(app)
	if rec := h.record(app); rec.StopAbsorbedAt.IsZero() || !slices.Equal(rec.WaitPins[w.ID], []string{"b"}) {
		t.Fatalf("record = %+v", rec)
	}

	h.mustPass(app)

	if p := h.pins(app, s1); !slices.Equal(p, []string{"a"}) {
		t.Errorf("pins of S1 = %v\nlog:\n%s", p, h.logs)
	}

	if rec := h.record(app); !rec.StopAbsorbedAt.IsZero() {
		t.Errorf("absorbed stop left: %+v", rec)
	}
}

// Pins of an absorbed stop that belong to another stop, as an older
// watcher leaves them, are not used.
func TestPinOnStopStaleAbsorbedPins(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	at := c.now()
	if err := h.store.Update(src, func(r *state.Record) error {
		r.StopAbsorbedAt = at
		r.StopAbsorbedDescription = "app"
		r.StopAbsorbedPins = []string{"stale"}
		r.StopAbsorbedPinsAt = at.Add(-time.Hour)

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	start := at.Add(time.Second)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: start, EndTime: start})
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle, LastSnapshot: &backend.SnapshotInfo{StartTime: start}})
	c.add(time.Minute)
	h.mustPass(app)

	if p := h.pins(app, start); len(p) != 0 {
		t.Errorf("pins = %v", p)
	}
}

// A source whose containers run hooks takes its stop snapshot without them,
// and pins it all the same.
func TestPinOnStopWithHooks(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop, preExec, postExec, pinLabel)
	h.passAndSettle(app)

	c.add(time.Minute)
	h.engine.setState(id, docker.StateExited)

	ev := h.engine.event(docker.KindDied, id)
	ev.Time = c.now()
	h.r.HandleEvent(ev)
	h.passAndSettle(app)

	h.mustTriggers(app, 1)
	h.mustExecs()

	w := h.onlyWait(app)
	if got := h.record(app).WaitPins[w.ID]; !w.Stop || !slices.Equal(got, []string{"before-update-2026-10-18-12-01-00"}) {
		t.Fatalf("wait = %+v, pins %v", w, got)
	}

	h.r.wait(t.Context(), w)
	h.passAndSettle(app)

	if p := h.pins(app, w.Ticket.TriggeredAt); !slices.Equal(p, []string{"before-update-2026-10-18-12-01-00"}) {
		t.Errorf("pins = %v\nlog:\n%s", p, h.logs)
	}
}
