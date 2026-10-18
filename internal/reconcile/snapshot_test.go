package reconcile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	onStop     = "kopia.snapshot-on-stop=true"
	noOnStart  = "kopia.snapshot-on-start=false"
	noInterval = "kopia.min-interval=0"
)

var t0 = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// testClock is the clock of the reconciler and the fake in a test.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

// withClock gives the reconciler and the fake a test clock at t0.
func withClock(h *harness) *testClock {
	c := &testClock{t: t0}
	h.r.now = c.now
	h.fake.SetClock(c.now)

	return c
}

func withSnapshotOnWatcherStart(o *Options) { o.SnapshotOnWatcherStart = true }

func withBackend(b backend.Backend) harnessOption {
	return func(o *Options) { o.Backend = b }
}

// triggers returns the TriggerSnapshot calls for p.
func (h *harness) triggers(p string) int {
	return len(h.fake.CallsFor(h.src(p), backendtest.TriggerSnapshot))
}

// record returns the state record of p.
func (h *harness) record(p string) state.Record {
	rec, _ := h.store.Get(h.src(p))
	return rec
}

// waits returns the snapshot waits of p.
func (h *harness) waits(p string) []state.SnapshotWait {
	return h.record(p).SnapshotWaits
}

// onlyWait returns the one snapshot wait of p.
func (h *harness) onlyWait(p string) state.SnapshotWait {
	h.t.Helper()

	ws := h.waits(p)
	if len(ws) != 1 {
		h.t.Fatalf("waits of %s = %+v, want one\nlog:\n%s", p, ws, h.logs)
	}

	return ws[0]
}

// finish stores res as the result of every wait of p, as a waiter does.
func (h *harness) finish(p string, res state.SnapshotResult) {
	h.t.Helper()

	err := h.store.Update(h.src(p), func(r *state.Record) error {
		for i := range r.SnapshotWaits {
			res := res
			r.SnapshotWaits[i].Result = &res
		}

		return nil
	})
	if err != nil {
		h.t.Fatal(err)
	}
}

// fullPass runs one pass over p as a full reconcile queues it.
func (h *harness) fullPass(p string) passResult {
	h.t.Helper()

	h.r.restoreFlags(p, false, true)

	return h.mustPass(p)
}

// count returns the number of log lines with msg.
func (h *harness) count(msg string) int {
	return strings.Count(h.logs.String(), `"msg":"`+msg)
}

// paused reports whether the policy of p is paused.
func (h *harness) paused(p string) bool {
	pol, ok := h.fake.Policy(h.src(p))
	return ok && policyPaused(pol)
}

// A start takes a snapshot, which a waiter follows; the pass that gets the
// result logs it and describes the snapshot.
func TestStartSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), "kopia.description=Main DB", "kopia.tags=env:prod,app:db")

	h.mustPass(app)

	if n := h.triggers(app); n != 0 {
		t.Fatalf("registration took %d snapshots", n)
	}

	if m := h.record(app).AnnotateAfter; m == nil || !m.Equal(t0) {
		t.Errorf("mark = %v, want the start of the registering pass", m)
	}

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	if n := h.triggers(app); n != 1 {
		t.Fatalf("%d triggers, want 1\nlog:\n%s", n, h.logs)
	}

	w := h.onlyWait(app)
	if w.Reason != "start of app" || w.Stop || w.Description != "Main DB [env=prod app=db]" || w.ID == "" ||
		w.Result != nil || !w.Ticket.TriggeredAt.Equal(c.now()) {
		t.Errorf("wait = %+v", w)
	}

	if got := h.record(app).LastTriggered; !got.Equal(c.now()) {
		t.Errorf("last triggered = %v", got)
	}

	// The waiter stores the result and queues the path.
	h.r.wait(t.Context(), w)

	if w := h.onlyWait(app); w.Result == nil || w.Result.Outcome != backend.SnapshotSucceeded || !w.Result.SnapshotStart.Equal(c.now()) {
		t.Fatalf("wait after the waiter = %+v", w)
	}

	h.mustPass(app)

	if ws := h.waits(app); len(ws) != 0 {
		t.Errorf("waits after the result = %+v", ws)
	}

	if got := h.fake.Annotations(h.src(app)); len(got) != 1 || got[0].Description != "Main DB [env=prod app=db]" ||
		!got[0].StartTime.Equal(c.now()) {
		t.Errorf("annotations = %+v", got)
	}

	if !h.logged("snapshot finished", `"outcome":"succeeded"`) || h.count("kopia.tags are written into the snapshot description") != 1 {
		t.Errorf("log:\n%s", h.logs)
	}
}

// A pass that writes the policy decides on a start snapshot only in a
// pass after it: the write can make kopia server start a missed scheduled
// snapshot, which is then pending.
func TestStartSnapshotAfterPolicyWrite(t *testing.T) {
	for _, tt := range []struct {
		name    string
		pending bool
	}{{"idle", false}, {"kopia started one", true}} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			withClock(h)
			id := h.engine.run("app", enable, pathLabel(app))
			h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

			res := h.mustPass(app)
			if !res.recheck || h.triggers(app) != 0 {
				t.Fatalf("registering pass: recheck %v, %d triggers", res.recheck, h.triggers(app))
			}

			if tt.pending {
				h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPending})
			}

			res = h.mustPass(app)

			want := 1
			if tt.pending {
				want = 0
			}

			if n := h.triggers(app); n != want || res.recheck {
				t.Errorf("second pass: %d triggers, want %d; recheck %v\nlog:\n%s", n, want, res.recheck, h.logs)
			}

			if got := h.r.takePending(app); len(got) != 0 {
				t.Errorf("events left: %v", eventNames(got))
			}
		})
	}
}

// The decision is put off once: a pass that writes again decides.
func TestStartSnapshotPutOffOnce(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	// The labels change, so the next pass writes the policy again.
	h.engine.mu.Lock()
	ctr := h.engine.containers[id]
	ctr.Labels["kopia.schedule"] = "5 * * * *"
	h.engine.containers[id] = ctr
	h.engine.mu.Unlock()

	h.mustPass(app)

	if n := h.triggers(app); n != 1 {
		t.Errorf("%d triggers\nlog:\n%s", n, h.logs)
	}
}

// A stop pauses the source at once and takes a stop snapshot if the
// container's label asks for one.
func TestStopSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name   string
		labels []string
		want   int
	}{
		{"asked", []string{onStop}, 1},
		{"default", nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			withClock(h)
			id := h.engine.run("app", append([]string{enable, pathLabel(app)}, tt.labels...)...)
			h.mustPass(app)

			h.engine.setState(id, docker.StateExited)
			h.r.HandleEvent(h.engine.event(docker.KindDied, id))
			h.mustPass(app)

			if !h.paused(app) {
				t.Error("not paused")
			}

			if n := h.triggers(app); n != tt.want {
				t.Fatalf("%d triggers, want %d", n, tt.want)
			}

			if tt.want == 0 {
				return
			}

			w := h.onlyWait(app)
			if !w.Stop || w.Reason != "stop of app" {
				t.Errorf("wait = %+v", w)
			}

			// The pause is written before the snapshot is queued.
			calls := h.fake.Calls()
			pause := slices.IndexFunc(calls, func(c backendtest.Call) bool {
				return c.Method == backendtest.ApplyPlan && policyPaused(c.Plan.Policy)
			})
			trigger := slices.IndexFunc(calls, func(c backendtest.Call) bool { return c.Method == backendtest.TriggerSnapshot })

			if pause < 0 || trigger < pause {
				t.Errorf("pause at call %d, trigger at %d", pause, trigger)
			}
		})
	}
}

// A container that is gone by the time of the pass takes its stop
// snapshot by the labels in the event, which also describe it.
func TestStopSnapshotOfRemovedContainer(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop, "kopia.description=Main DB")
	h.mustPass(app)

	c.add(time.Minute)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)

	if !h.paused(app) {
		t.Error("not paused")
	}

	w := h.onlyWait(app)
	if !w.Stop || w.Description != "Main DB" {
		t.Errorf("wait = %+v", w)
	}

	// The description is kept with the wait, as the containers are gone.
	h.r.wait(t.Context(), w)
	h.mustPass(app)

	if got := h.fake.Annotations(h.src(app)); len(got) != 1 || got[0].Description != "Main DB" {
		t.Errorf("annotations = %+v", got)
	}
}

// A die after which the container is restarting is no stop: a crash loop
// takes no stop snapshots, and its starts are held back by min-interval.
// When the container ends up exited without an event, the kept die takes
// the stop snapshot.
func TestCrashLoop(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	crash := func() {
		t.Helper()

		c.add(30 * time.Second)
		h.engine.setState(id, docker.StateRestarting)
		h.r.HandleEvent(h.engine.event(docker.KindDied, id))

		if res := h.mustPass(app); !res.restarting {
			t.Fatal("no recheck of the restarting container")
		}

		h.engine.setState(id, docker.StateRunning)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)
	}

	for range 5 {
		crash()
	}

	if n := h.triggers(app); n != 1 {
		t.Fatalf("%d triggers in the crash loop, want the first start only\nlog:\n%s", n, h.logs)
	}

	if w := h.onlyWait(app); w.Stop || w.Reason != "start of app" {
		t.Errorf("wait = %+v", w)
	}

	if h.paused(app) {
		t.Error("a crash loop paused the source")
	}

	// The last crash ends exited, without an event.
	c.add(30 * time.Second)
	h.engine.setState(id, docker.StateRestarting)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	if n := h.triggers(app); n != 1 {
		t.Fatalf("a die while restarting took a snapshot")
	}

	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)

	if n := h.triggers(app); n != 2 || !h.paused(app) {
		t.Fatalf("after the exit: %d triggers, paused %v", n, h.paused(app))
	}

	if ws := h.waits(app); !ws[1].Stop || ws[1].Reason != "stop of app" {
		t.Errorf("waits = %+v", ws)
	}
}

// SNAPSHOT_ON_WATCHER_START takes a snapshot of every running container
// at the first full reconcile, as far as min-interval allows; without it
// a restart of the watcher takes none.
func TestSnapshotOnWatcherStart(t *testing.T) {
	for _, tt := range []struct {
		name   string
		opts   []harnessOption
		recent bool
		want   int
	}{
		{"off", nil, false, 0},
		{"on", []harnessOption{withSnapshotOnWatcherStart}, false, 1},
		{"on, recent snapshot", []harnessOption{withSnapshotOnWatcherStart}, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.opts...)
			c := withClock(h)
			h.engine.run("app", enable, pathLabel(app))
			h.engine.create("db", docker.StateExited, enable, pathLabel("/data/db"))

			if tt.recent {
				h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now().Add(-time.Minute)})
			}

			var (
				mu   sync.Mutex
				seen = map[string]int{}
			)

			h.r.afterPass = func(p string, res passResult) {
				mu.Lock()
				defer mu.Unlock()

				// The registering pass of app puts the decision off to a
				// second one.
				if res.err == nil && !res.recheck {
					seen[p]++
				}
			}

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})

			go func() {
				h.r.Run(ctx)
				close(done)
			}()

			t.Cleanup(func() {
				cancel()
				<-done
			})

			passed := func(p string, n int) func() bool {
				return func() bool {
					mu.Lock()
					defer mu.Unlock()

					return seen[p] >= n
				}
			}

			h.r.ReconcileAll()
			eventually(t, "the passes of the first full reconcile", func() bool { return passed(app, 1)() && passed("/data/db", 1)() })

			if n := h.triggers(app); n != tt.want {
				t.Errorf("%d triggers of app, want %d\nlog:\n%s", n, tt.want, h.logs)
			}

			if n := h.triggers("/data/db"); n != 0 {
				t.Errorf("a stopped container took %d snapshots", n)
			}

			if tt.want > 0 && !h.logged("snapshot queued", "watcher start: app") {
				t.Errorf("log:\n%s", h.logs)
			}

			// A later full reconcile is no watcher start, also once
			// min-interval would allow a snapshot.
			eventually(t, "the result is handled", func() bool { return len(h.waits(app)) == 0 })
			c.add(time.Hour)

			mu.Lock()
			n := seen[app]
			mu.Unlock()

			h.r.ReconcileAll()
			eventually(t, "the passes of the second full reconcile", func() bool { return passed(app, n+1)() && passed("/data/db", 2)() })

			if n := h.triggers(app); n != tt.want {
				t.Errorf("after another full reconcile: %d triggers", n)
			}
		})
	}
}

// min-interval counts from the watcher's own trigger too: a snapshot that
// failed saved nothing, but holds the next start back.
func TestMinIntervalCountsFailedAttempts(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	start := func() {
		t.Helper()
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)
	}

	start()
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "permission denied"})
	h.mustPass(app)

	if !h.logged("snapshot failed", "permission denied") || len(h.waits(app)) != 0 {
		t.Fatalf("failed snapshot not handled\nlog:\n%s", h.logs)
	}

	c.add(9 * time.Minute)
	start()

	if n := h.triggers(app); n != 1 {
		t.Fatalf("%d triggers 9 minutes after a failed one", n)
	}

	c.add(time.Minute)
	start()

	if n := h.triggers(app); n != 2 {
		t.Fatalf("%d triggers 10 minutes after a failed one", n)
	}
}

// A min-interval of 0 lets every start take a snapshot; a longer one
// counts from the last snapshot.
func TestMinIntervalLabel(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	idle := h.engine.run("app", enable, pathLabel(app), noInterval)
	slow := h.engine.run("slow", enable, pathLabel("/data/slow"), "kopia.min-interval=1h")
	h.mustPass(app)
	h.mustPass("/data/slow")
	h.fake.AddSnapshot(h.src("/data/slow"), backend.SnapshotInfo{StartTime: c.now().Add(-50 * time.Minute)})

	for range 3 {
		h.r.HandleEvent(h.engine.event(docker.KindStarted, idle))
		h.mustPass(app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, slow))
		h.mustPass("/data/slow")
	}

	if a, s := h.triggers(app), h.triggers("/data/slow"); a != 3 || s != 0 {
		t.Errorf("triggers: app %d, slow %d", a, s)
	}

	c.add(10 * time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, slow))
	h.mustPass("/data/slow")

	if s := h.triggers("/data/slow"); s != 1 {
		t.Errorf("slow: %d triggers an hour after its last snapshot", s)
	}
}

// While a snapshot uploads, a start takes none but a stop does; kopia
// server runs it after the current one.
func TestStopWhileUploading(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop, noInterval)
	h.mustPass(app)
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusUploading})

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	if n := h.triggers(app); n != 0 {
		t.Fatalf("a start took a snapshot while uploading")
	}

	c.add(time.Second)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	if n := h.triggers(app); n != 1 || !h.onlyWait(app).Stop {
		t.Fatalf("%d triggers at a stop while uploading", n)
	}
}

// While a snapshot is pending, a start takes none, and the pending one
// counts as the stop snapshot. If kopia server loses it, the stop
// snapshot is taken once more.
func TestStopWhilePending(t *testing.T) {
	setup := func(t *testing.T) (*harness, *testClock, string) {
		h := newHarness(t)
		c := withClock(h)
		id := h.engine.run("app", enable, pathLabel(app), onStop)
		h.mustPass(app)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPending})

		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)

		c.add(time.Minute)
		h.engine.setState(id, docker.StateExited)
		h.r.HandleEvent(h.engine.event(docker.KindDied, id))
		h.mustPass(app)

		if n := h.triggers(app); n != 0 {
			t.Fatalf("%d triggers while pending", n)
		}

		if got := h.record(app).StopAbsorbedAt; !got.Equal(c.now()) || !h.logged("not queuing a snapshot: the pending one") {
			t.Fatalf("absorbed at %v\nlog:\n%s", got, h.logs)
		}

		return h, c, id
	}

	t.Run("still pending", func(t *testing.T) {
		h, _, _ := setup(t)
		h.mustPass(app)

		if n := h.triggers(app); n != 0 || h.record(app).StopAbsorbedAt.IsZero() {
			t.Errorf("%d triggers", n)
		}
	})

	t.Run("uploading", func(t *testing.T) {
		h, _, _ := setup(t)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusUploading})
		h.mustPass(app)

		if n := h.triggers(app); n != 0 || h.record(app).StopAbsorbedAt.IsZero() {
			t.Errorf("%d triggers while the absorbing run uploads", n)
		}
	})

	t.Run("saved", func(t *testing.T) {
		h, c, _ := setup(t)
		c.add(time.Minute)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
		h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now(), EndTime: c.now()})
		h.mustPass(app)

		if n := h.triggers(app); n != 0 || !h.record(app).StopAbsorbedAt.IsZero() {
			t.Errorf("%d triggers, absorbed at %v", n, h.record(app).StopAbsorbedAt)
		}
	})

	t.Run("lost", func(t *testing.T) {
		h, c, _ := setup(t)
		c.add(time.Minute)
		h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
		// A canceled snapshot saved when kopia server stopped is no stop
		// snapshot.
		h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now(), Incomplete: "canceled"})
		h.mustPass(app)

		if n := h.triggers(app); n != 1 || !h.record(app).StopAbsorbedAt.IsZero() {
			t.Fatalf("%d triggers, absorbed at %v", n, h.record(app).StopAbsorbedAt)
		}

		if w := h.onlyWait(app); !w.Stop || !w.Retried {
			t.Errorf("wait = %+v", w)
		}
	})
}

// A stop that a pending snapshot the watcher queued absorbs makes that
// snapshot a stop snapshot, taken again if it is lost.
func TestStopAbsorbedByOwnSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPending})

	c.add(time.Minute)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	// The wait becomes a stop snapshot, and the stop is kept too, in case
	// the wait's run was not the pending one.
	if w := h.onlyWait(app); !w.Stop || w.Reason != "start of app, stop of app" || !h.record(app).StopAbsorbedAt.Equal(c.now()) {
		t.Fatalf("wait = %+v, absorbed at %v", w, h.record(app).StopAbsorbedAt)
	}

	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
	h.mustPass(app)

	if n := h.triggers(app); n != 2 || !h.record(app).StopAbsorbedAt.IsZero() {
		t.Errorf("%d triggers, want the lost stop snapshot again once; absorbed at %v", n, h.record(app).StopAbsorbedAt)
	}

	if w := h.onlyWait(app); !w.Stop || !w.Retried {
		t.Errorf("wait = %+v", w)
	}
}

// A snapshot whose task kopia server forgot is taken once more if no
// complete snapshot started after it; not a third time.
func TestUnknownResult(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		h := newHarness(t)
		c := withClock(h)
		id := h.engine.run("app", enable, pathLabel(app), onStop, "kopia.description=Main DB")
		h.mustPass(app)
		h.engine.setState(id, docker.StateExited)
		h.r.HandleEvent(h.engine.event(docker.KindDied, id))
		h.r.HandleEvent(h.engine.remove(id))
		h.mustPass(app)

		first := h.onlyWait(app)

		c.add(time.Hour)
		h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
		h.mustPass(app)

		again := h.onlyWait(app)
		if h.triggers(app) != 2 || again.ID == first.ID || !again.Stop || !again.Retried ||
			again.Reason != "again: stop of app" || again.Description != "Main DB" {
			t.Fatalf("after an unknown result: %d triggers, wait %+v\nlog:\n%s", h.triggers(app), again, h.logs)
		}

		h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
		h.mustPass(app)

		if n := h.triggers(app); n != 2 || len(h.waits(app)) != 0 {
			t.Errorf("a retried snapshot was taken again: %d triggers, waits %+v", n, h.waits(app))
		}
	})

	t.Run("start ignores the lost attempt", func(t *testing.T) {
		h := newHarness(t)
		withClock(h)
		id := h.engine.run("app", enable, pathLabel(app))
		h.mustPass(app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)

		h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
		h.mustPass(app)

		if n := h.triggers(app); n != 2 || !h.onlyWait(app).Retried {
			t.Errorf("%d triggers", n)
		}
	})

	t.Run("saved since", func(t *testing.T) {
		h := newHarness(t)
		c := withClock(h)
		id := h.engine.run("app", enable, pathLabel(app))
		h.mustPass(app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)

		h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now(), EndTime: c.now()})
		h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
		h.mustPass(app)

		if n := h.triggers(app); n != 1 || len(h.waits(app)) != 0 {
			t.Errorf("%d triggers, waits %+v", n, h.waits(app))
		}
	})
}

// Each container of a shared path decides on the snapshots of its own
// starts and stops.
func TestSharedPathPerContainer(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	a := h.engine.run("a", enable, pathLabel(app), onStop)
	b := h.engine.run("b", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.setState(b, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, b))
	h.mustPass(app)

	if n := h.triggers(app); n != 0 || h.paused(app) {
		t.Fatalf("stop of b: %d triggers, paused %v", n, h.paused(app))
	}

	h.engine.setState(a, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, a))
	h.mustPass(app)

	if n := h.triggers(app); n != 1 || !h.paused(app) {
		t.Fatalf("stop of a: %d triggers, paused %v", n, h.paused(app))
	}

	if w := h.onlyWait(app); w.Reason != "stop of a" || w.Description != "a, b" {
		t.Errorf("wait = %+v", w)
	}
}

// The description joins the description labels, or names, of the path's
// containers and their tags.
func TestDescription(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	h.engine.run("app", enable, pathLabel(app), "kopia.description=Main", "kopia.tags=env:prod")
	worker := h.engine.run("worker", enable, pathLabel(app), "kopia.tags=env:prod,team:x")
	h.engine.run("other", enable, pathLabel(app), "kopia.description=Main")
	h.mustPass(app)

	h.r.HandleEvent(h.engine.event(docker.KindStarted, worker))
	h.mustPass(app)

	if w := h.onlyWait(app); w.Description != "Main, worker [env=prod team=x]" {
		t.Errorf("description = %q", w.Description)
	}
}

// The one-off containers of docker compose run take no snapshots.
func TestOneOffContainersTakeNoSnapshots(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	h.engine.run("app", enable, pathLabel(app), onStop)
	run := h.engine.run("app-run-1", enable, pathLabel(app), onStop, noInterval, oneOffLabel+"=True")
	h.mustPass(app)

	h.r.HandleEvent(h.engine.event(docker.KindStarted, run))
	h.mustPass(app)
	h.engine.setState(run, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, run))
	h.r.HandleEvent(h.engine.remove(run))
	h.mustPass(app)

	if n := h.triggers(app); n != 0 {
		t.Errorf("%d triggers", n)
	}
}

// A container whose labels have an error takes the snapshots its snapshot
// labels ask for; a bad snapshot label itself takes none.
func TestSnapshotsWithLabelErrors(t *testing.T) {
	for _, tt := range []struct {
		label string
		want  int
	}{
		{"kopia.typo=1", 1},
		{"kopia.snapshot-on-stop=yes", 0},
	} {
		t.Run(tt.label, func(t *testing.T) {
			// Registered before the labels went wrong.
			h := newHarness(t, withStrict)
			withClock(h)
			id := h.engine.run("app", enable, pathLabel(app), onStop)
			h.mustPass(app)

			h.engine.mu.Lock()
			ctr := h.engine.containers[id]
			k, v, _ := strings.Cut(tt.label, "=")
			ctr.Labels[k] = v
			ctr.State = docker.StateExited
			h.engine.containers[id] = ctr
			h.engine.mu.Unlock()

			h.r.HandleEvent(h.engine.event(docker.KindDied, id))
			h.mustPass(app)

			if n := h.triggers(app); n != tt.want || !h.paused(app) {
				t.Errorf("%d triggers, paused %v\nlog:\n%s", n, h.paused(app), h.logs)
			}
		})
	}
}

// A stop or a start the watcher saw no event of shows in the pause.
func TestStartsAndStopsWithoutEvents(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)

	if n := h.triggers(app); n != 1 || !h.paused(app) {
		t.Fatalf("missed stop: %d triggers, paused %v", n, h.paused(app))
	}

	if w := h.onlyWait(app); !w.Stop || w.Reason != "stop of app without an event" {
		t.Errorf("wait = %+v", w)
	}

	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotSucceeded})
	h.mustPass(app)

	// The start lifts the pause, which is a policy write: the second pass
	// decides.
	h.engine.setState(id, docker.StateRunning)

	if res := h.mustPass(app); !res.recheck || h.paused(app) || h.triggers(app) != 1 {
		t.Fatalf("missed start: recheck %v, paused %v, %d triggers", res.recheck, h.paused(app), h.triggers(app))
	}

	h.r.now = func() time.Time { return t0.Add(time.Hour) }
	h.mustPass(app)

	if n := h.triggers(app); n != 2 || h.onlyWait(app).Reason != "start of app without an event" {
		t.Errorf("%d triggers, waits %+v\nlog:\n%s", n, h.waits(app), h.logs)
	}
}

// A temporary error of the trigger retries the pass with the event kept;
// the pause it applied stays and is not written again.
func TestTriggerRetried(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.fake.FailNextFor(h.src(app), backendtest.TriggerSnapshot, temporary("TriggerSnapshot"))

	if res := h.pass(app); !res.retry || !h.paused(app) {
		t.Fatalf("retry %v, paused %v", res.retry, h.paused(app))
	}

	writes := h.writes(app)
	h.mustPass(app)

	if h.triggers(app) != 2 || len(h.waits(app)) != 1 || h.writes(app) != writes {
		t.Errorf("%d triggers, %d waits, %d writes then %d", h.triggers(app), len(h.waits(app)), writes, h.writes(app))
	}
}

// A dry run takes no snapshots, also with SNAPSHOT_ON_WATCHER_START.
func TestDryRunTakesNoSnapshots(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := state.Open(h.statePath, state.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	opts := h.opts
	opts.Store, opts.DryRun, opts.SnapshotOnWatcherStart = ro, true, true

	r, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}

	r.HandleEvent(h.engine.event(docker.KindStarted, id))

	if err := r.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}

	if calls := h.fake.CallsTo(backendtest.TriggerSnapshot); len(calls) != 0 {
		t.Errorf("%d triggers", len(calls))
	}
}

// The full reconcile describes the snapshots after the mark that have no
// description, scheduled ones included, and moves the mark; older ones,
// checkpoints and described ones stay as they are.
func TestSweep(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(-3 * time.Hour)})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(-2 * time.Hour)})
	h.engine.run("app", enable, pathLabel(app), "kopia.description=Main")
	h.fullPass(app)

	if m := h.record(app).AnnotateAfter; m == nil || !m.Equal(t0.Add(-2*time.Hour)) {
		t.Fatalf("mark = %v, want the last snapshot before the watcher", m)
	}

	c.add(3 * time.Hour)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(time.Hour), EndTime: t0.Add(time.Hour)})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(90 * time.Minute), Description: "by hand"})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(2 * time.Hour), Incomplete: "checkpoint"})

	// An event's pass does not sweep.
	h.mustPass(app)

	if calls := h.fake.CallsFor(src, backendtest.ListSnapshots); len(calls) != 0 {
		t.Fatalf("an event's pass listed snapshots")
	}

	h.fullPass(app)

	if got := h.fake.Annotations(src); len(got) != 1 || got[0].Description != "Main" || !got[0].StartTime.Equal(t0.Add(time.Hour)) {
		t.Fatalf("annotations = %+v", got)
	}

	if m := h.record(app).AnnotateAfter; m == nil || !m.Equal(t0.Add(90*time.Minute)) {
		t.Errorf("mark = %v", m)
	}

	for _, s := range h.fake.Snapshots(src)[:2] {
		if s.Description != "" {
			t.Errorf("a snapshot from before the watcher was described: %+v", s)
		}
	}

	// The checkpoint's upload ends.
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(2 * time.Hour), EndTime: t0.Add(3 * time.Hour)})
	h.fullPass(app)
	h.fullPass(app)

	if got := h.fake.Annotations(src); len(got) != 2 || !got[1].StartTime.Equal(t0.Add(2*time.Hour)) {
		t.Errorf("annotations = %+v", got)
	}

	// The mark is past everything: no more listings.
	n := len(h.fake.CallsFor(src, backendtest.ListSnapshots))
	h.fullPass(app)

	if got := len(h.fake.CallsFor(src, backendtest.ListSnapshots)); got != n {
		t.Errorf("listed snapshots again with nothing after the mark")
	}
}

// A record from before the mark existed gets the mark at the last
// snapshot: nothing older is described.
func TestSweepOldRecord(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(time.Hour)})

	if err := h.store.Update(src, func(r *state.Record) error {
		r.AnnotateAfter = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	h.fullPass(app)

	if got := h.fake.Annotations(src); len(got) != 0 {
		t.Errorf("annotations = %+v", got)
	}

	if m := h.record(app).AnnotateAfter; m == nil || !m.Equal(t0.Add(time.Hour)) {
		t.Errorf("mark = %v", m)
	}
}

// The sweep leaves the snapshots of the watcher's waits to them, stops
// after maxAnnotations, skips a snapshot kopia server refuses to edit and
// stops at a temporary error.
func TestSweepLimits(t *testing.T) {
	setup := func(t *testing.T, n int) (*harness, backend.Source) {
		h := newHarness(t)
		withClock(h)
		h.engine.run("app", enable, pathLabel(app))
		h.mustPass(app)

		src := h.src(app)
		for i := 1; i <= n; i++ {
			at := t0.Add(time.Duration(i) * time.Minute)
			h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: at, EndTime: at})
		}

		return h, src
	}

	t.Run("own wait", func(t *testing.T) {
		h, src := setup(t, 3)

		if err := h.store.Update(src, func(r *state.Record) error {
			// The wait's snapshot may start up to clockSkew before its
			// trigger by kopia server's clock.
			r.SnapshotWaits = []state.SnapshotWait{{ID: "w", Ticket: backend.SnapshotTicket{Source: src, TriggeredAt: t0.Add(3 * time.Minute)}}}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		h.fullPass(app)

		if got := h.fake.Annotations(src); len(got) != 1 || !got[0].StartTime.Equal(t0.Add(time.Minute)) {
			t.Errorf("annotations = %+v", got)
		}
	})

	t.Run("cap", func(t *testing.T) {
		h, src := setup(t, maxAnnotations+5)
		h.fullPass(app)

		if got := len(h.fake.Annotations(src)); got != maxAnnotations {
			t.Fatalf("%d described in one pass", got)
		}

		h.fullPass(app)

		if got := len(h.fake.Annotations(src)); got != maxAnnotations+5 {
			t.Errorf("%d described after two passes", got)
		}
	})

	t.Run("errors", func(t *testing.T) {
		h, src := setup(t, 3)
		h.fake.FailNextFor(src, backendtest.AnnotateSnapshot,
			&backend.Error{Op: "AnnotateSnapshot", Kind: backend.ErrConfig, Err: errors.New("rejected")})
		h.fake.FailNextFor(src, backendtest.AnnotateSnapshot, temporary("AnnotateSnapshot"))

		// A rejected description and a temporary failure stop the sweep
		// where they happen, one attempt each.
		for range 2 {
			n := len(h.fake.CallsFor(src, backendtest.AnnotateSnapshot))
			h.fullPass(app)

			if m := h.record(app).AnnotateAfter; len(h.fake.Annotations(src)) != 0 || !m.Equal(t0) ||
				len(h.fake.CallsFor(src, backendtest.AnnotateSnapshot)) != n+1 {
				t.Fatalf("mark = %v, annotations %+v", m, h.fake.Annotations(src))
			}
		}

		// A snapshot that is gone is skipped.
		h.fake.FailNextFor(src, backendtest.AnnotateSnapshot, &backend.Error{Op: "AnnotateSnapshot", Err: backend.ErrSnapshotNotFound})
		h.fullPass(app)

		if got := h.fake.Annotations(src); len(got) != 2 || !h.record(app).AnnotateAfter.Equal(t0.Add(3*time.Minute)) {
			t.Errorf("annotations = %+v, mark %v", got, h.record(app).AnnotateAfter)
		}
	})
}

// notImplemented is the fake without some snapshot methods, like the REST
// backend before they are implemented.
type notImplemented struct {
	*backendtest.Fake
	methods map[backendtest.Method]bool
	// calls counts the calls answered with ErrNotImplemented.
	calls *atomicCounter
}

type atomicCounter struct {
	mu sync.Mutex
	n  map[backendtest.Method]int
}

func (c *atomicCounter) add(m backendtest.Method) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.n == nil {
		c.n = map[backendtest.Method]int{}
	}

	c.n[m]++
}

func (c *atomicCounter) get(m backendtest.Method) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.n[m]
}

func (b notImplemented) err(m backendtest.Method, src backend.Source) error {
	b.calls.add(m)

	return &backend.Error{Op: string(m), Source: src, Err: backend.ErrNotImplemented}
}

func (b notImplemented) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	if b.methods[backendtest.TriggerSnapshot] {
		return backend.SnapshotTicket{}, b.err(backendtest.TriggerSnapshot, src)
	}

	return b.Fake.TriggerSnapshot(ctx, src)
}

func (b notImplemented) WaitSnapshot(ctx context.Context, t backend.SnapshotTicket, save func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	if b.methods[backendtest.WaitSnapshot] {
		return backend.SnapshotResult{}, b.err(backendtest.WaitSnapshot, t.Source)
	}

	return b.Fake.WaitSnapshot(ctx, t, save)
}

func (b notImplemented) AnnotateSnapshot(ctx context.Context, src backend.Source, start time.Time, desc string, pins []string) error {
	if b.methods[backendtest.AnnotateSnapshot] {
		return b.err(backendtest.AnnotateSnapshot, src)
	}

	return b.Fake.AnnotateSnapshot(ctx, src, start, desc, pins)
}

func (b notImplemented) ListSnapshots(ctx context.Context, src backend.Source) ([]backend.SnapshotInfo, error) {
	if b.methods[backendtest.ListSnapshots] {
		return nil, b.err(backendtest.ListSnapshots, src)
	}

	return b.Fake.ListSnapshots(ctx, src)
}

// The REST backend returns ErrNotImplemented for the snapshot methods
// M3-A has not implemented yet: it is logged once, nothing is retried, and
// the rest goes on.
func TestSnapshotMethodsNotImplemented(t *testing.T) {
	fake := backendtest.New()
	b := notImplemented{Fake: fake, calls: &atomicCounter{}, methods: map[backendtest.Method]bool{
		backendtest.WaitSnapshot: true, backendtest.AnnotateSnapshot: true, backendtest.ListSnapshots: true,
	}}
	h := newHarness(t, withBackend(b))
	h.fake = fake
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), noInterval)

	waited := make(chan struct{}, 8)
	h.r.afterWait = func(state.SnapshotWait) { waited <- struct{}{} }

	passes := h.start()
	h.r.ReconcileAll()
	waitPasses(t, passes, app)
	eventually(t, "the registration", func() bool { return h.fake.Known(h.src(app)) })

	for range 2 {
		c.add(time.Minute)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("no waiter\nlog:\n%s", h.logs)
		}
	}

	eventually(t, "the waits are dropped", func() bool { return len(h.waits(app)) == 0 })

	if n := b.calls.get(backendtest.WaitSnapshot); n != 2 {
		t.Errorf("%d WaitSnapshot calls, want one per snapshot", n)
	}

	// Descriptions cannot be written either, and the snapshots are not
	// listed again.
	fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now().Add(time.Second)})
	h.r.ReconcileAll()
	eventually(t, "a listing", func() bool { return b.calls.get(backendtest.ListSnapshots) == 1 })

	for drained := false; !drained; {
		select {
		case <-passes:
		default:
			drained = true
		}
	}

	h.r.ReconcileAll()
	waitPasses(t, passes, app)

	if n := b.calls.get(backendtest.ListSnapshots); n != 1 {
		t.Errorf("%d listings", n)
	}

	if a, d := h.count("the backend cannot follow snapshots yet"), h.count("the backend cannot describe snapshots yet"); a != 1 || d != 1 {
		t.Errorf("warnings: follow %d, describe %d\nlog:\n%s", a, d, h.logs)
	}

	// And a backend that cannot even queue them.
	fake2 := backendtest.New()
	h2 := newHarness(t, withBackend(notImplemented{Fake: fake2, methods: map[backendtest.Method]bool{backendtest.TriggerSnapshot: true}}))
	h2.fake = fake2
	withClock(h2)
	id2 := h2.engine.run("app", enable, pathLabel(app), noInterval)
	h2.mustPass(app)

	for range 2 {
		h2.r.HandleEvent(h2.engine.event(docker.KindStarted, id2))

		if res := h2.pass(app); res.err != nil || res.retry {
			t.Fatalf("pass: %v", res.err)
		}
	}

	if got := h2.r.takePending(app); len(got) != 0 || h2.count("the backend cannot queue snapshots") != 1 {
		t.Errorf("pending %v\nlog:\n%s", eventNames(got), h2.logs)
	}
}

// WaitSnapshot runs outside the passes: while a snapshot runs, a stop is
// paused at once.
func TestWaitOutsideThePass(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)
	h.fake.HoldSnapshots(src)
	c.add(time.Minute)

	passes := h.start()
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	eventually(t, "the task is saved", func() bool {
		ws := h.waits(app)
		return len(ws) == 1 && ws[0].Ticket.TaskID != ""
	})

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	waitPass(t, passes, app, func(passResult) bool { return h.paused(app) })

	if w := h.onlyWait(app); w.Result != nil {
		t.Fatalf("the held snapshot ended: %+v", w)
	}

	// The passes start no second waiter for the snapshot.
	h.r.ReconcileAll()
	waitPasses(t, passes, app)

	if n := len(h.fake.CallsFor(src, backendtest.WaitSnapshot)); n != 1 {
		t.Errorf("%d WaitSnapshot calls for one snapshot", n)
	}

	h.fake.ReleaseSnapshots(src)
	eventually(t, "the result is handled", func() bool { return len(h.waits(app)) == 0 && len(h.fake.Annotations(src)) == 1 })
}

// A wait stays in the state when the watcher stops, and the next watcher
// follows it with the saved task and acts on its result.
func TestWaitResumedAfterRestart(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), "kopia.description=Main")
	h.mustPass(app)
	h.fake.HoldSnapshots(src)
	c.add(time.Minute)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		h.r.Run(ctx)
		close(done)
	}()

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	eventually(t, "the task is saved", func() bool {
		ws := h.waits(app)
		return len(ws) == 1 && ws[0].Ticket.TaskID != ""
	})

	cancel()
	<-done

	w := h.onlyWait(app)
	if w.Result != nil || w.Ticket.TaskID == "" {
		t.Fatalf("wait after the stop = %+v", w)
	}

	// The next watcher.
	h.fake.ReleaseSnapshots(src)

	r2, err := New(h.opts)
	if err != nil {
		t.Fatal(err)
	}

	r2.now = h.r.now

	ctx2, cancel2 := context.WithCancel(t.Context())
	done2 := make(chan struct{})

	go func() {
		r2.Run(ctx2)
		close(done2)
	}()

	t.Cleanup(func() {
		cancel2()
		<-done2
	})

	r2.ReconcileAll()
	eventually(t, "the result is handled", func() bool { return len(h.waits(app)) == 0 && len(h.fake.Annotations(src)) == 1 })

	calls := h.fake.CallsFor(src, backendtest.WaitSnapshot)
	if last := calls[len(calls)-1]; last.Ticket.TaskID != w.Ticket.TaskID {
		t.Errorf("the resumed wait used task %q, want %q", last.Ticket.TaskID, w.Ticket.TaskID)
	}

	if got := h.fake.Annotations(src); got[0].Description != "Main" {
		t.Errorf("annotations = %+v", got)
	}
}

// The waiter retries temporary errors, keeps the wait on others, and on an
// identity change asks for the identity again.
func TestWaitErrors(t *testing.T) {
	setup := func(t *testing.T) (*harness, state.SnapshotWait) {
		h := newHarness(t)
		withClock(h)
		id := h.engine.run("app", enable, pathLabel(app))
		h.mustPass(app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
		h.mustPass(app)

		return h, h.onlyWait(app)
	}

	t.Run("temporary", func(t *testing.T) {
		h, w := setup(t)
		h.fake.FailNextFor(w.Ticket.Source, backendtest.WaitSnapshot, temporary("WaitSnapshot"))
		h.r.wait(t.Context(), w)

		if w := h.onlyWait(app); w.Result == nil || len(h.fake.CallsTo(backendtest.WaitSnapshot)) != 2 {
			t.Errorf("wait = %+v", w)
		}
	})

	t.Run("other", func(t *testing.T) {
		h, w := setup(t)
		h.fake.FailNextFor(w.Ticket.Source, backendtest.WaitSnapshot, &backend.Error{Op: "WaitSnapshot", Err: errors.New("401")})
		h.r.wait(t.Context(), w)

		if w := h.onlyWait(app); w.Result != nil || !h.logged("cannot follow the snapshot; trying again at the next pass") {
			t.Errorf("wait = %+v", w)
		}
	})

	t.Run("identity", func(t *testing.T) {
		h, w := setup(t)
		h.fake.SetIdentity("admin", "nas")
		h.r.wait(t.Context(), w)

		rec, _ := h.store.Get(w.Ticket.Source)
		if len(rec.SnapshotWaits) != 1 || rec.SnapshotWaits[0].Result != nil || h.r.identity.known {
			t.Errorf("waits = %+v, identity known %v", rec.SnapshotWaits, h.r.identity.known)
		}
	})
}

// A wait of another identity's record, or one the watcher cannot use, is
// left alone.
func TestUnusableWaitsLeftAlone(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	other := backend.Source{Host: "old", UserName: "root", Path: app}
	bad := []state.SnapshotWait{
		{ID: "", Ticket: backend.SnapshotTicket{Source: src, TriggeredAt: t0}},
		{ID: "x", Ticket: backend.SnapshotTicket{Source: other, TriggeredAt: t0}},
	}

	if err := h.store.Update(src, func(r *state.Record) error {
		r.SnapshotWaits = bad
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	passes := h.start()
	h.r.ReconcileAll()
	waitPasses(t, passes, app)

	if got := h.waits(app); len(got) != 2 || len(h.fake.CallsTo(backendtest.WaitSnapshot)) != 0 {
		t.Errorf("waits = %+v", got)
	}
}

// Waits the watcher cannot use do not absorb a stop and do not hold back
// the descriptions.
func TestUnusableWaitsIgnored(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	if err := h.store.Update(src, func(r *state.Record) error {
		r.SnapshotWaits = []state.SnapshotWait{{ID: "", Ticket: backend.SnapshotTicket{Source: src, TriggeredAt: c.now().Add(30 * time.Second)}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		at := c.now().Add(time.Duration(i) * time.Minute)
		h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: at, EndTime: at})
	}

	h.fullPass(app)

	if got := h.fake.Annotations(src); len(got) != 2 {
		t.Errorf("annotations = %+v", got)
	}

	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	if rec := h.record(app); rec.StopAbsorbedAt.IsZero() || rec.SnapshotWaits[0].Stop {
		t.Errorf("record = %+v", rec)
	}
}

// A new wait ID never repeats one of the record.
func TestNewWaitID(t *testing.T) {
	waits := []state.SnapshotWait{{ID: t0.Format(time.RFC3339Nano)}, {ID: t0.Format(time.RFC3339Nano) + "-2"}}

	if got := newWaitID(waits, t0); got != t0.Format(time.RFC3339Nano)+"-3" {
		t.Errorf("id = %q", got)
	}

	if got := newWaitID(nil, t0.In(time.FixedZone("x", 3600))); got != "2026-10-18T12:00:00Z" {
		t.Errorf("id = %q", got)
	}
}

func TestCompact(t *testing.T) {
	ev := func(id string, kind docker.Kind, name string) docker.Event {
		return docker.Event{ContainerID: id, Kind: kind, Name: name}
	}

	got := compact([]docker.Event{
		ev("a", docker.KindStarted, "1"), ev("a", docker.KindDied, "2"), ev("b", docker.KindDied, "3"),
		ev("a", docker.KindStarted, "4"), ev("a", docker.KindDied, "5"),
	})

	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}

	if fmt.Sprint(names) != "[3 4 5]" {
		t.Errorf("compact = %v", names)
	}
}

// A description that fails for a passing reason keeps the wait, logged
// once, and the next pass describes the snapshot with the description
// worked out when it was queued.
func TestDescriptionRetried(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, "kopia.description=Main DB")
	h.mustPass(app)

	c.add(time.Minute)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)
	h.r.wait(t.Context(), h.onlyWait(app))

	h.fake.FailNextFor(src, backendtest.AnnotateSnapshot, temporary("AnnotateSnapshot"))
	h.mustPass(app)

	if w := h.onlyWait(app); !w.Logged || len(h.fake.Annotations(src)) != 0 {
		t.Fatalf("wait = %+v", w)
	}

	h.mustPass(app)

	if got := h.fake.Annotations(src); len(got) != 1 || got[0].Description != "Main DB" || len(h.waits(app)) != 0 {
		t.Errorf("annotations = %+v, waits %+v", got, h.waits(app))
	}

	if n := h.count("snapshot finished"); n != 1 {
		t.Errorf("logged %d times", n)
	}
}

// A stop that a pending snapshot absorbs keeps its description for that
// snapshot, also when its container is gone by then.
func TestAbsorbedStopDescription(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop, "kopia.description=Main DB")
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})

	c.add(time.Minute)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)

	if rec := h.record(app); rec.StopAbsorbedDescription != "Main DB" || h.triggers(app) != 0 {
		t.Fatalf("record = %+v", rec)
	}

	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: c.now().Add(time.Second), EndTime: c.now().Add(time.Second)})
	h.mustPass(app)

	if got := h.fake.Annotations(src); len(got) != 1 || got[0].Description != "Main DB" || !h.record(app).StopAbsorbedAt.IsZero() {
		t.Errorf("annotations = %+v, record %+v", got, h.record(app))
	}
}

// A snapshot taken again keeps the description of the first attempt.
func TestRetryKeepsDescription(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop, "kopia.description=Main DB", "kopia.tags=env:prod")
	h.engine.run("sidecar", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)

	const want = "Main DB, sidecar [env=prod]"

	if w := h.onlyWait(app); w.Description != want {
		t.Fatalf("description = %q", w.Description)
	}

	c.add(time.Minute)
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
	h.mustPass(app)

	if w := h.onlyWait(app); !w.Retried || w.Description != want {
		t.Errorf("wait = %+v", w)
	}
}

// The tags warning comes from the tags label, not from brackets in the
// description.
func TestTagsWarning(t *testing.T) {
	for _, tt := range []struct {
		labels []string
		want   int
	}{
		{[]string{"kopia.description=Main DB [primary]"}, 0},
		{[]string{"kopia.tags=env:prod"}, 1},
	} {
		h := newHarness(t)
		withClock(h)
		id := h.engine.run("app", append([]string{enable, pathLabel(app), noInterval}, tt.labels...)...)
		h.mustPass(app)

		for range 2 {
			h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
			h.mustPass(app)
		}

		if n := h.count("kopia.tags are written into the snapshot description"); n != tt.want || h.triggers(app) != 2 {
			t.Errorf("%v: warned %d times, %d triggers", tt.labels, n, h.triggers(app))
		}
	}
}

// A stop seen only in the pause is asked for again when queuing it fails:
// the retry no longer sees the change of the pause.
func TestStopWithoutEventRetried(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	h.engine.setState(id, docker.StateExited)
	h.fake.FailNextFor(h.src(app), backendtest.TriggerSnapshot, temporary("TriggerSnapshot"))

	if res := h.pass(app); !res.retry || !h.paused(app) {
		t.Fatalf("retry %v, paused %v", res.retry, h.paused(app))
	}

	h.mustPass(app)

	if w := h.onlyWait(app); h.triggers(app) != 2 || !w.Stop || w.Reason != "stop of app without an event" {
		t.Errorf("%d triggers, wait %+v", h.triggers(app), w)
	}
}

// A one-off container's start and stop explain the changes of the pause
// they cause: no stop snapshot of the main container comes of them.
func TestOneOffChangesThePause(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	main := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)

	h.engine.setState(main, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, main))
	h.mustPass(app)
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotSucceeded})
	h.mustPass(app)

	run := h.engine.run("app-run-1", enable, pathLabel(app), onStop, oneOffLabel+"=True")
	h.r.HandleEvent(h.engine.event(docker.KindStarted, run))
	h.mustPass(app)

	if h.paused(app) {
		t.Fatal("the one-off did not lift the pause")
	}

	h.engine.setState(run, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, run))
	h.r.HandleEvent(h.engine.remove(run))
	h.mustPass(app)

	if n := h.triggers(app); n != 1 || !h.paused(app) {
		t.Errorf("%d triggers, paused %v\nlog:\n%s", n, h.paused(app), h.logs)
	}
}

// A run that gets its upload slot during the pass's policy write still
// counts as the stop snapshot: the stop counts from the start of the pass.
func TestStopAbsorbedAtPassStart(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusPending})

	start := c.now()
	h.fake.SetHook(func(call backendtest.Call) error {
		if call.Method == backendtest.ApplyPlan {
			c.add(time.Second)
		}

		return nil
	})

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)
	h.fake.SetHook(nil)

	if got := h.record(app).StopAbsorbedAt; !got.Equal(start) {
		t.Fatalf("absorbed at %v, want %v", got, start)
	}

	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusIdle})
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: start.Add(500 * time.Millisecond), EndTime: c.now()})
	h.mustPass(app)

	if n := h.triggers(app); n != 0 || !h.record(app).StopAbsorbedAt.IsZero() {
		t.Errorf("%d triggers", n)
	}
}

// A lost start snapshot is taken again without the container's minimum
// interval: the first trigger passed it, and no snapshot came of it.
func TestLostStartRetriedWithoutMinInterval(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), "kopia.min-interval=5m")
	h.mustPass(app)
	h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now().Add(-6 * time.Minute)})

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)
	h.finish(app, state.SnapshotResult{Outcome: backend.SnapshotUnknown})
	h.mustPass(app)

	if n := h.triggers(app); n != 2 {
		t.Errorf("%d triggers", n)
	}
}

// A wait that could not be finished for a week is dropped.
func TestOldWaitDropped(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	if err := h.store.Update(src, func(r *state.Record) error {
		r.SnapshotWaits = []state.SnapshotWait{{ID: "old", Ticket: backend.SnapshotTicket{Source: src, TriggeredAt: c.now().Add(-8 * 24 * time.Hour)}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	h.mustPass(app)

	if ws := h.waits(app); len(ws) != 0 || !h.logged("dropping a snapshot wait") {
		t.Errorf("waits = %+v", ws)
	}
}

// A second stop while the snapshot the first one queued waits behind the
// current upload counts that snapshot again rather than queuing another,
// which kopia server would drop.
func TestSecondStopWhileQueued(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	src := h.src(app)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)
	h.fake.SetStatus(src, backend.SourceStatus{Status: backend.StatusUploading, CurrentTask: "5"})

	stop := func() {
		t.Helper()
		h.engine.setState(id, docker.StateExited)
		h.r.HandleEvent(h.engine.event(docker.KindDied, id))
		h.mustPass(app)
	}

	stop()

	// The REST backend lists the running task in the ticket.
	if err := h.store.Update(src, func(r *state.Record) error {
		r.SnapshotWaits[0].Ticket.TasksBefore = []string{"5"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	h.engine.setState(id, docker.StateRunning)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)
	stop()

	if w := h.onlyWait(app); h.triggers(app) != 1 || w.Reason != "stop of app, stop of app" {
		t.Errorf("%d triggers, wait %+v", h.triggers(app), w)
	}
}

// waitTracker is the fake with a WaitSnapshot that holds until its context
// ends and then takes a moment to return, like a request that is torn
// down.
type waitTracker struct {
	*backendtest.Fake
	returned atomicFlag
}

type atomicFlag struct {
	mu  sync.Mutex
	set bool
}

func (f *atomicFlag) store() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.set = true
}

func (f *atomicFlag) load() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.set
}

func (b *waitTracker) WaitSnapshot(ctx context.Context, _ backend.SnapshotTicket, _ func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)
	b.returned.store()

	return backend.SnapshotResult{}, ctx.Err()
}

// Run returns only after its snapshot waiters.
func TestRunWaitsForWaiters(t *testing.T) {
	fake := backendtest.New()
	b := &waitTracker{Fake: fake}
	h := newHarness(t, withBackend(b))
	h.fake = fake
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		h.r.Run(ctx)
		close(done)
	}()

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	eventually(t, "a waiter", func() bool {
		h.r.mu.Lock()
		defer h.r.mu.Unlock()

		return len(h.r.waiting) == 1
	})

	cancel()
	<-done

	if !b.returned.load() {
		t.Error("Run returned before its waiter")
	}
}

// While kopia server does not run snapshots of the source (paused through
// its control API), none is queued.
func TestNoSnapshotWhilePaused(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	id := h.engine.run("app", enable, pathLabel(app), onStop)
	h.mustPass(app)
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPaused})

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.mustPass(app)

	if n := h.triggers(app); n != 0 || !h.logged("not queuing a snapshot: kopia server does not run snapshots") {
		t.Errorf("%d triggers\nlog:\n%s", n, h.logs)
	}
}

// A pass of a full reconcile that fails keeps its flags for the retry,
// which describes the snapshots.
func TestFlagsKeptForTheRetry(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	src := h.src(app)
	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)
	h.fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: t0.Add(time.Minute), EndTime: t0.Add(time.Minute)})

	h.r.restoreFlags(app, false, true)
	h.fake.FailNextFor(src, backendtest.PlanSource, temporary("PlanSource"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("pass: %v", res.err)
	}

	h.mustPass(app)

	if got := h.fake.Annotations(src); len(got) != 1 {
		t.Errorf("annotations = %+v", got)
	}
}
