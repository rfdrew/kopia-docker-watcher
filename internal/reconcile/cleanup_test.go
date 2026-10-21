package reconcile

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	cleanupAll    = "kopia.cleanup-on-remove=all"
	cleanupPolicy = "kopia.cleanup-on-remove=policy"
	cleanupPause  = "kopia.cleanup-on-remove=pause"
	deletePinned  = "kopia.cleanup-delete-pinned=true"

	grace = time.Hour
)

func withGrace(o *Options) { o.CleanupGrace = grace }

func withWatcherPrefix(p string) harnessOption {
	return func(o *Options) { o.WatcherPathPrefix = p }
}

// cleanupHarness is a harness with CLEANUP_GRACE of an hour and a test
// clock.
func cleanupHarness(t *testing.T, opts ...harnessOption) (*harness, *testClock) {
	t.Helper()

	h := newHarness(t, append([]harnessOption{withGrace}, opts...)...)

	return h, withClock(h)
}

// snap is a complete snapshot that started d after t0, with pins.
func snap(d time.Duration, pins ...string) backend.SnapshotInfo {
	return backend.SnapshotInfo{StartTime: t0.Add(d), EndTime: t0.Add(d + time.Minute), Pins: pins}
}

// abandon registers app for a container with the labels, adds the
// snapshots, then removes the container and runs the pass that pauses
// the source.
func (h *harness) abandon(lbls []string, snaps ...backend.SnapshotInfo) {
	h.t.Helper()

	id := h.engine.run("app", append([]string{enable, pathLabel(app), noOnStart}, lbls...)...)
	h.mustPass(app)

	for _, s := range snaps {
		h.fake.AddSnapshot(h.src(app), s)
	}

	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)

	if !h.paused(app) {
		h.t.Fatalf("not paused\nlog:\n%s", h.logs)
	}
}

// deletes returns the calls that delete something of p.
func (h *harness) deletes(p string) []backendtest.Call {
	return slices.Concat(h.fake.CallsFor(h.src(p), backendtest.DeletePolicy), h.fake.CallsFor(h.src(p), backendtest.DeleteSnapshots))
}

// mustKeep checks that nothing of p was deleted.
func (h *harness) mustKeep(p string, snapshots int) {
	h.t.Helper()

	if c := h.deletes(p); len(c) != 0 {
		h.t.Fatalf("delete calls %+v\nlog:\n%s", c, h.logs)
	}

	if !h.fake.Known(h.src(p)) || len(h.fake.Snapshots(h.src(p))) != snapshots {
		h.t.Fatalf("source known %t with %d snapshots, want %d", h.fake.Known(h.src(p)), len(h.fake.Snapshots(h.src(p))), snapshots)
	}

	if _, ok := h.store.Get(h.src(p)); !ok {
		h.t.Fatal("the record is gone")
	}
}

// restart is a new watcher process on the same state file, kopia server
// and Docker, with opts on top of the first one's options.
func (h *harness) restart(c *testClock, opts ...harnessOption) {
	h.t.Helper()

	if err := h.store.Close(); err != nil {
		h.t.Fatal(err)
	}

	st, err := state.Open(h.statePath, state.Options{})
	if err != nil {
		h.t.Fatal(err)
	}

	h.t.Cleanup(func() { _ = st.Close() })

	o := h.opts
	o.Store = st

	for _, fn := range opts {
		fn(&o)
	}

	r, err := New(o)
	if err != nil {
		h.t.Fatal(err)
	}

	r.now = c.now
	h.store, h.r, h.opts = st, r, o
}

// By default an abandoned source is only paused, however long it has been
// without a container.
func TestCleanupPauseByDefault(t *testing.T) {
	for _, tt := range []struct {
		name   string
		labels []string
	}{
		{"no label", nil},
		{"pause", []string{cleanupPause}},
		{"unknown mode", []string{"kopia.cleanup-on-remove=everything"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h, c := cleanupHarness(t)
			h.abandon(tt.labels, snap(-time.Hour))

			c.add(30 * 24 * time.Hour)
			h.fullPass(app)

			h.mustKeep(app, 1)

			if !h.paused(app) {
				t.Error("not paused")
			}

			if h.logged("no container backs the source up any more") {
				t.Error("a cleanup was announced")
			}
		})
	}
}

// all deletes the source with its snapshots and its policy once
// CLEANUP_GRACE has passed, and the state forgets it.
func TestCleanupAll(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll}, snap(-2*time.Hour), snap(-time.Hour))

	if rec := h.record(app); !rec.OrphanedSince.Equal(t0) || len(rec.Owners) != 1 || rec.Owners[0].Gone.IsZero() {
		t.Fatalf("record = %+v", rec)
	}

	if !h.logged("no container backs the source up any more", `"cleanup":"all"`, `"cleanup_due":"2026-10-18 13:00:00 +00:00"`) {
		t.Errorf("the cleanup is not announced:\n%s", h.logs)
	}

	// Not before CLEANUP_GRACE.
	c.add(grace - time.Second)
	h.fullPass(app)
	h.mustKeep(app, 2)

	c.add(time.Second)
	h.fullPass(app)

	calls := h.fake.CallsFor(h.src(app), backendtest.DeleteSnapshots)
	if len(calls) != 1 || calls[0].Pinned {
		t.Fatalf("DeleteSnapshots calls = %+v", calls)
	}

	h.mustNoSource(app)

	if _, ok := h.store.Get(h.src(app)); ok {
		t.Error("the record is left")
	}

	if !h.logged("cleanup: deleted the abandoned source with its snapshots and its policy", `"cleanup":"all"`, `"snapshots":2`,
		`"source":"root@kopia:/data/app"`) {
		t.Errorf("the deletion is not logged:\n%s", h.logs)
	}

	// The path is no longer managed.
	h.fake.ResetCalls()
	h.fullPass(app)

	if n := len(h.fake.Calls()); n > 1 {
		t.Errorf("calls after the cleanup: %+v", h.fake.Calls())
	}
}

// A pinned snapshot stays with the paused policy: only the others go, and
// once only pinned ones are left, nothing more is deleted.
func TestCleanupAllKeepsPinned(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll}, snap(-3*time.Hour), snap(-2*time.Hour, "before-upgrade"), snap(-time.Hour))

	c.add(grace)
	h.fullPass(app)

	if calls := h.fake.CallsFor(h.src(app), backendtest.DeleteSnapshots); len(calls) != 1 || calls[0].Pinned {
		t.Fatalf("DeleteSnapshots calls = %+v", calls)
	}

	if left := h.fake.Snapshots(h.src(app)); len(left) != 1 || !slices.Equal(left[0].Pins, []string{"before-upgrade"}) {
		t.Errorf("snapshots left = %+v", left)
	}

	if !h.paused(app) {
		t.Error("the policy is not paused")
	}

	if _, ok := h.store.Get(h.src(app)); !ok {
		t.Error("the record is gone")
	}

	if !h.logged("cleanup: deleted the snapshots of the abandoned source that have no pin", `"cleanup":"all"`, `"snapshots":2`, `"pinned":1`) {
		t.Errorf("the deletion is not logged:\n%s", h.logs)
	}

	// Nothing more to delete, and that is said once.
	c.add(time.Hour)
	h.fullPass(app)
	h.fullPass(app)

	if n := len(h.fake.CallsFor(h.src(app), backendtest.DeleteSnapshots)); n != 1 {
		t.Errorf("%d DeleteSnapshots calls", n)
	}

	if n := h.count("keeping the pinned snapshots of the abandoned source"); n != 1 {
		t.Errorf("logged %d times", n)
	}
}

// With cleanup-delete-pinned the pinned snapshots go too, and the source as
// a whole.
func TestCleanupAllDeletesPinned(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll, deletePinned}, snap(-2*time.Hour, "keep"), snap(-time.Hour))

	c.add(grace)
	h.fullPass(app)

	if calls := h.fake.CallsFor(h.src(app), backendtest.DeleteSnapshots); len(calls) != 1 || !calls[0].Pinned {
		t.Fatalf("DeleteSnapshots calls = %+v", calls)
	}

	h.mustNoSource(app)

	if !h.logged("cleanup: deleted the abandoned source", `"snapshots":2`, `"pinned_deleted":1`) {
		t.Errorf("the deletion is not logged:\n%s", h.logs)
	}
}

func TestCleanupPolicy(t *testing.T) {
	// A source without snapshots loses its policy, which makes kopia server
	// forget it; the pass after forgets the record.
	t.Run("without snapshots", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.abandon([]string{cleanupPolicy})

		c.add(grace - time.Second)
		h.fullPass(app)
		h.mustKeep(app, 0)

		c.add(time.Second)

		if res := h.fullPass(app); !res.recheck {
			t.Error("no pass soon after the delete")
		}

		if n := len(h.fake.CallsFor(h.src(app), backendtest.DeletePolicy)); n != 1 {
			t.Fatalf("%d DeletePolicy calls", n)
		}

		if len(h.fake.CallsTo(backendtest.DeleteSnapshots)) != 0 {
			t.Error("snapshots deleted")
		}

		h.mustNoSource(app)

		if !h.logged("cleanup: deleted the policy of the abandoned source", `"cleanup":"policy"`, `"snapshots":0`) {
			t.Errorf("the deletion is not logged:\n%s", h.logs)
		}

		h.mustPass(app)

		if _, ok := h.store.Get(h.src(app)); ok {
			t.Error("the record is left")
		}

		if !h.logged("forgot the abandoned source: kopia server no longer knows it") {
			t.Errorf("not logged:\n%s", h.logs)
		}
	})

	// A source with snapshots would inherit the host or global schedule
	// without its policy: it stays paused.
	t.Run("with snapshots", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.abandon([]string{cleanupPolicy}, snap(-time.Hour))

		c.add(grace)
		h.fullPass(app)
		h.fullPass(app)

		h.mustKeep(app, 1)

		if !h.paused(app) {
			t.Error("not paused")
		}

		if n := h.count("keeping the paused policy of the abandoned source"); n != 1 {
			t.Errorf("logged %d times", n)
		}
	})

	// A snapshot that comes between the listing and the delete keeps the
	// source: the next pass pauses it again.
	t.Run("a snapshot in between", func(t *testing.T) {
		h, c := cleanupHarness(t)
		h.abandon([]string{cleanupPolicy})

		h.fake.SetHook(func(call backendtest.Call) error {
			if call.Method == backendtest.DeletePolicy {
				h.fake.AddSnapshot(h.src(app), snap(time.Hour))
			}

			return nil
		})

		c.add(grace)
		h.fullPass(app)
		h.fake.SetHook(nil)

		if _, ok := h.fake.Policy(h.src(app)); ok || !h.fake.Known(h.src(app)) {
			t.Fatal("want the source known without a policy")
		}

		h.mustPass(app)

		if !h.paused(app) {
			t.Error("not paused again")
		}

		if _, ok := h.store.Get(h.src(app)); !ok {
			t.Error("the record is gone")
		}

		if n := len(h.fake.CallsFor(h.src(app), backendtest.DeletePolicy)); n != 1 {
			t.Errorf("%d DeletePolicy calls", n)
		}
	})
}

// A container that backs the source up again before CLEANUP_GRACE calls
// the cleanup off, and the source runs again.
func TestCleanupCalledOff(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll}, snap(-time.Hour))

	c.add(grace / 2)
	h.engine.run("app", enable, pathLabel(app), noOnStart, cleanupAll)
	h.mustPass(app)

	if h.paused(app) {
		t.Error("still paused")
	}

	if rec := h.record(app); !rec.OrphanedSince.IsZero() {
		t.Errorf("record = %+v", rec)
	}

	if !h.logged("a container backs the abandoned source up again; its cleanup is called off", `"cleanup":"all"`) {
		t.Errorf("not logged:\n%s", h.logs)
	}

	c.add(2 * grace)
	h.fullPass(app)
	h.mustKeep(app, 1)
}

// A source whose container was removed while the watcher was not running
// is found by the full reconcile after the start, and CLEANUP_GRACE counts
// from then.
func TestCleanupAfterDowntime(t *testing.T) {
	h, c := cleanupHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart, cleanupAll)
	h.mustPass(app)
	h.fake.AddSnapshot(h.src(app), snap(-time.Hour))

	// The watcher stops; the container goes; the watcher starts a day later.
	h.engine.remove(id)
	c.add(24 * time.Hour)
	h.restart(c)

	paths, err := h.r.paths(t.Context())
	if err != nil || !slices.Contains(paths, app) {
		t.Fatalf("paths = %v, %v", paths, err)
	}

	h.fullPass(app)

	if !h.paused(app) {
		t.Error("not paused")
	}

	if rec := h.record(app); !rec.OrphanedSince.Equal(c.now()) {
		t.Errorf("orphaned since %v, want %v", rec.OrphanedSince, c.now())
	}

	c.add(grace - time.Second)
	h.fullPass(app)
	h.mustKeep(app, 1)

	c.add(time.Second)
	h.fullPass(app)
	h.mustNoSource(app)
}

func TestCleanupWatcherPathPrefix(t *testing.T) {
	// A label path outside the prefix is a label error and makes no source.
	t.Run("label path outside", func(t *testing.T) {
		h := newHarness(t, withWatcherPrefix("/data/apps"))
		id := h.engine.run("app", enable, pathLabel(app), "kopia.path=/data/apps/web,"+app)
		h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

		if !h.logged("container labels have errors", "outside WATCHER_PATH_PREFIX /data/apps") {
			t.Errorf("not reported:\n%s", h.logs)
		}

		paths, err := h.r.paths(t.Context())
		if err != nil || !slices.Equal(paths, []string{"/data/apps/web"}) {
			t.Errorf("paths = %v, %v", paths, err)
		}

		h.mustPass("/data/apps/web")
		h.mustPass(app)
		h.mustNoSource(app)

		if !h.fake.Known(h.src("/data/apps/web")) {
			t.Error("the source inside the prefix is not registered")
		}
	})

	// A source of the state outside the prefix is neither paused nor
	// cleaned up.
	t.Run("state source outside", func(t *testing.T) {
		h, c := cleanupHarness(t)
		id := h.engine.run("app", enable, pathLabel(app), noOnStart, cleanupAll)
		h.mustPass(app)
		h.fake.AddSnapshot(h.src(app), snap(-time.Hour))

		h.restart(c, withWatcherPrefix("/srv"))
		h.r.HandleEvent(h.engine.remove(id))
		h.fake.ResetCalls()

		for range 2 {
			c.add(2 * grace)

			if _, err := h.r.paths(t.Context()); err != nil {
				t.Fatal(err)
			}

			h.fullPass(app)
		}

		if calls := h.fake.Calls(); len(calls) != 0 {
			t.Errorf("calls = %+v", calls)
		}

		h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

		if rec := h.record(app); !rec.OrphanedSince.IsZero() || len(rec.Owners) != 1 || !rec.Owners[0].Gone.IsZero() {
			t.Errorf("record = %+v", rec)
		}

		if n := h.count("the state has a source outside WATCHER_PATH_PREFIX"); n != 1 {
			t.Errorf("warned %d times:\n%s", n, h.logs)
		}
	})

	// The check right before a delete holds even if a pass got that far.
	t.Run("delete refused", func(t *testing.T) {
		h, _ := cleanupHarness(t, withWatcherPrefix("/srv"))
		ps := &pass{Reconciler: h.r, ctx: t.Context(), path: app, log: h.r.log, src: h.src(app)}

		if err := ps.checkPrefix(); err == nil || !strings.Contains(err.Error(), "outside WATCHER_PATH_PREFIX /srv") {
			t.Errorf("checkPrefix = %v", err)
		}

		if err := ps.cleanupPolicy(nil); err == nil || len(h.deletes(app)) != 0 {
			t.Errorf("cleanupPolicy = %v, calls %+v", err, h.deletes(app))
		}

		if err := ps.cleanupAll(cleanupPlan{}, nil); err == nil || len(h.deletes(app)) != 0 {
			t.Errorf("cleanupAll = %v, calls %+v", err, h.deletes(app))
		}
	})
}

// A dry run says what the cleanup would delete, and when, and deletes
// nothing.
func TestCleanupDryRun(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll}, snap(-2*time.Hour, "keep"), snap(-time.Hour))
	h.engine.run("db", enable, pathLabel("/data/db"), noOnStart, cleanupPolicy)
	h.mustPass("/data/db")

	dryRun := func() string {
		t.Helper()

		if err := h.store.Close(); err != nil {
			t.Fatal(err)
		}

		before, err := os.ReadFile(h.statePath)
		if err != nil {
			t.Fatal(err)
		}

		ro, err := state.Open(h.statePath, state.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}

		out := &syncBuffer{}
		o := h.opts
		o.Store, o.DryRun, o.Output = ro, true, out

		r, err := New(o)
		if err != nil {
			t.Fatal(err)
		}

		r.now = c.now

		if err := r.RunOnce(t.Context()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}

		if after, _ := os.ReadFile(h.statePath); !bytes.Equal(before, after) {
			t.Errorf("state file changed:\n%s\n->\n%s", before, after)
		}

		h.restart(c)

		return out.String()
	}

	// The container of /data/db is gone too, but no pass has seen it.
	h.engine.mu.Lock()
	for id, ctr := range h.engine.containers {
		if ctr.Name == "db" {
			delete(h.engine.containers, id)
		}
	}
	h.engine.mu.Unlock()

	out := dryRun()

	for _, want := range []string{
		"cleanup: kopia.cleanup-on-remove=all at 2026-10-18 13:00:00 +00:00, after CLEANUP_GRACE (1h0m0s) without a container",
		"cleanup: kopia.cleanup-on-remove=policy at 2026-10-18 13:00:00 +00:00",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}

	c.add(grace)

	out = dryRun()

	for _, want := range []string{
		"root@kopia:/data/app: no change",
		"cleanup: would delete 1 snapshots without a pin; 1 pinned ones and the paused policy stay (kopia.cleanup-on-remove=all)",
		// db is paused only by this dry run: its policy would be deleted
		// by the pass that pauses it.
		"root@kopia:/data/db: would update the policy",
		"cleanup: kopia.cleanup-on-remove=policy at 2026-10-18 14:00:00 +00:00",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}

	if !h.logged("dry run: would delete", `"source":"root@kopia:/data/app"`, `"cleanup":"all"`, `"snapshots":1`) {
		t.Errorf("not logged:\n%s", h.logs)
	}

	if c := h.deletes(app); len(c) != 0 {
		t.Errorf("delete calls = %+v", c)
	}

	if n := len(h.fake.Snapshots(h.src(app))); n != 2 {
		t.Errorf("%d snapshots left", n)
	}
}

// Owners that ask for different modes get the most cautious one, and
// pinned snapshots go only if every owner lets them.
func TestCleanupModesOfOwners(t *testing.T) {
	for _, tt := range []struct {
		name         string
		web, worker  []string
		wantDelete   backendtest.Method
		wantPinned   bool
		wantSnapshot int
	}{
		{"all and pause", []string{cleanupAll}, nil, "", false, 2},
		{"all and policy", []string{cleanupAll}, []string{cleanupPolicy}, "", false, 2},
		{"all and all", []string{cleanupAll}, []string{cleanupAll}, backendtest.DeleteSnapshots, false, 1},
		{"pinned allowed by one", []string{cleanupAll, deletePinned}, []string{cleanupAll}, backendtest.DeleteSnapshots, false, 1},
		{"pinned allowed by both", []string{cleanupAll, deletePinned}, []string{cleanupAll, deletePinned}, backendtest.DeleteSnapshots, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h, c := cleanupHarness(t)
			web := h.engine.run("web", append([]string{enable, pathLabel(app), noOnStart}, tt.web...)...)
			worker := h.engine.run("worker", append([]string{enable, pathLabel(app), noOnStart}, tt.worker...)...)
			h.mustPass(app)
			h.fake.AddSnapshot(h.src(app), snap(-2*time.Hour, "keep"))
			h.fake.AddSnapshot(h.src(app), snap(-time.Hour))

			h.engine.remove(web)
			h.engine.remove(worker)
			h.mustPass(app)

			c.add(grace)
			h.fullPass(app)

			calls := h.deletes(app)

			switch {
			case tt.wantDelete == "" && len(calls) != 0:
				t.Errorf("delete calls = %+v", calls)
			case tt.wantDelete != "" && (len(calls) != 1 || calls[0].Method != tt.wantDelete || calls[0].Pinned != tt.wantPinned):
				t.Errorf("delete calls = %+v", calls)
			}

			if n := len(h.fake.Snapshots(h.src(app))); n != tt.wantSnapshot {
				t.Errorf("%d snapshots left, want %d", n, tt.wantSnapshot)
			}
		})
	}
}

// An owner that left shortly before the last one still counts for the
// mode, whichever order docker compose down removes them in; one that
// left long before no longer does.
func TestCleanupModeOfOwnersThatLeft(t *testing.T) {
	setup := func(t *testing.T) (*harness, *testClock, string, string) {
		t.Helper()

		h, c := cleanupHarness(t)
		careful := h.engine.run("careful", enable, pathLabel(app), noOnStart, cleanupPause)
		eager := h.engine.run("eager", enable, pathLabel(app), noOnStart, cleanupAll)
		h.mustPass(app)
		h.fake.AddSnapshot(h.src(app), snap(-time.Hour))

		return h, c, careful, eager
	}

	t.Run("shortly before", func(t *testing.T) {
		h, c, careful, eager := setup(t)

		h.r.HandleEvent(h.engine.remove(careful))
		h.mustPass(app)
		c.add(ownerLinger - time.Second)
		h.r.HandleEvent(h.engine.remove(eager))
		h.mustPass(app)

		if rec := h.record(app); len(rec.Owners) != 2 {
			t.Fatalf("owners = %+v", rec.Owners)
		}

		c.add(2 * grace)
		h.fullPass(app)
		h.mustKeep(app, 1)
	})

	t.Run("long before", func(t *testing.T) {
		h, c, careful, eager := setup(t)

		h.r.HandleEvent(h.engine.remove(careful))
		h.mustPass(app)
		c.add(ownerLinger + time.Second)
		h.fullPass(app)

		if rec := h.record(app); len(rec.Owners) != 1 || rec.Owners[0].Name != "eager" {
			t.Fatalf("owners = %+v", rec.Owners)
		}

		h.r.HandleEvent(h.engine.remove(eager))
		h.mustPass(app)
		c.add(grace)
		h.fullPass(app)
		h.mustNoSource(app)
	})
}

// The cleanup waits for the snapshot at the stop the watcher queued.
func TestCleanupWaitsForStopSnapshot(t *testing.T) {
	h, c := cleanupHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart, onStop, cleanupAll)
	h.mustPass(app)

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	h.mustPass(app)

	w := h.onlyWait(app)
	if !w.Stop {
		t.Fatalf("wait = %+v", w)
	}

	c.add(2 * grace)
	h.fullPass(app)

	if c := h.deletes(app); len(c) != 0 {
		t.Fatalf("deleted before the stop snapshot finished: %+v", c)
	}

	// The snapshot ends; the pass that acts on it cleans up.
	h.r.wait(t.Context(), w)
	h.fullPass(app)

	if len(h.waits(app)) != 0 && h.fake.Known(h.src(app)) {
		t.Fatalf("waits = %+v", h.waits(app))
	}

	h.mustNoSource(app)
}

// The cleanup waits while a snapshot is pending or uploading.
func TestCleanupWaitsForRunningSnapshot(t *testing.T) {
	for _, st := range []backend.Status{backend.StatusPending, backend.StatusUploading} {
		t.Run(string(st), func(t *testing.T) {
			h, c := cleanupHarness(t)
			h.abandon([]string{cleanupAll}, snap(-time.Hour))

			c.add(grace)
			h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: st})
			h.fullPass(app)
			h.mustKeep(app, 1)

			h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
			h.fullPass(app)
			h.mustNoSource(app)
		})
	}
}

// A container the state remembers as an owner that still exists lost the
// path only because the watcher's configuration changed: its source is
// paused, as every source without owners, but not cleaned up.
func TestCleanupNotWhileOwnersExist(t *testing.T) {
	h, c := cleanupHarness(t)
	h.engine.run("app", enable, pathLabel(app), noOnStart, cleanupAll)
	h.mustPass(app)
	h.fake.AddSnapshot(h.src(app), snap(-time.Hour))

	// LABEL_PREFIX changes, but the labels do not.
	h.restart(c, func(o *Options) { o.LabelPrefix = "backup" })
	h.fullPass(app)

	c.add(2 * grace)
	h.fullPass(app)
	h.fullPass(app)

	h.mustKeep(app, 1)

	if n := h.count("not cleaning up the source"); n != 1 || !h.logged("not cleaning up the source", "the containers that backed it up still exist") {
		t.Errorf("warned %d times:\n%s", n, h.logs)
	}
}

// The state forgets a source kopia server no longer knows once its owners
// asked for a cleanup and CLEANUP_GRACE has passed; with the pause it
// keeps the record, as before.
func TestCleanupForgetsUnknownSource(t *testing.T) {
	for _, tt := range []struct {
		labels []string
		forget bool
	}{
		{[]string{cleanupAll}, true},
		{[]string{cleanupPolicy}, true},
		{nil, false},
	} {
		t.Run(strings.Join(tt.labels, ","), func(t *testing.T) {
			h, c := cleanupHarness(t)
			h.abandon(tt.labels)

			// Someone deletes the source in the web UI.
			if err := h.fake.DeletePolicy(t.Context(), h.src(app)); err != nil {
				t.Fatal(err)
			}

			h.fullPass(app)

			if _, ok := h.store.Get(h.src(app)); !ok {
				t.Fatal("forgotten before CLEANUP_GRACE")
			}

			c.add(grace)
			h.fullPass(app)

			if _, ok := h.store.Get(h.src(app)); ok == tt.forget {
				t.Errorf("record kept: %t", ok)
			}

			h.mustNoSource(app)
		})
	}
}

// A failed delete is retried, and a source deleted by an answer that was
// lost is forgotten by the next pass.
func TestCleanupDeleteFails(t *testing.T) {
	h, c := cleanupHarness(t)
	h.abandon([]string{cleanupAll}, snap(-time.Hour))
	c.add(grace)

	h.fake.FailNext(backendtest.DeleteSnapshots, temporary("DeleteSnapshots"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("res = %+v", res)
	}

	if !h.fake.Known(h.src(app)) || len(h.fake.Snapshots(h.src(app))) != 1 {
		t.Fatal("deleted by a call that failed before it took effect")
	}

	h.fake.FailNextAfter(backendtest.DeleteSnapshots, temporary("DeleteSnapshots"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("res = %+v", res)
	}

	h.mustNoSource(app)

	if _, ok := h.store.Get(h.src(app)); !ok {
		t.Fatal("the record is gone")
	}

	h.mustPass(app)

	if _, ok := h.store.Get(h.src(app)); ok {
		t.Error("the record is left")
	}
}

// The state records the owners with what their labels ask of the cleanup,
// and writes only what changes.
func TestOwnersRecorded(t *testing.T) {
	h, c := cleanupHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart, cleanupAll, deletePinned,
		"com.docker.compose.project=shop", "com.docker.compose.config-hash=1")
	h.mustPass(app)

	rec := h.record(app)
	if len(rec.Owners) != 1 || !rec.OwnersUpdated.Equal(t0) || !rec.OrphanedSince.IsZero() {
		t.Fatalf("record = %+v", rec)
	}

	o := rec.Owners[0]
	if o.ID != id || o.Name != "app" || o.ComposeProject != "shop" || o.Cleanup != "all" || !o.DeletePinned ||
		!strings.HasPrefix(o.LabelsHash, "sha256:") || !o.Gone.IsZero() {
		t.Errorf("owner = %+v", o)
	}

	// Nothing changes: nothing is written.
	c.add(time.Minute)
	h.fullPass(app)

	if rec := h.record(app); !rec.OwnersUpdated.Equal(t0) {
		t.Errorf("updated at %v", rec.OwnersUpdated)
	}

	// The hash follows the labels under the prefix only.
	labels := map[string]string{"kopia.enable": "true", "kopia.path": app, "other": "1"}
	same := labelsHash("kopia", map[string]string{"kopia.enable": "true", "kopia.path": app, "other": "2"})

	if labelsHash("kopia", labels) != same || labelsHash("kopia", map[string]string{"kopia.enable": "true"}) == same {
		t.Error("labelsHash")
	}

	// A label that cannot be read asks for the pause.
	h.engine.run("worker", enable, pathLabel(app), "kopia.cleanup-on-remove=everything", "kopia.cleanup-delete-pinned=yes")
	h.mustPass(app)

	rec = h.record(app)
	if len(rec.Owners) != 2 || rec.Owners[1].Name != "worker" || rec.Owners[1].Cleanup != "pause" || rec.Owners[1].DeletePinned ||
		!rec.OwnersUpdated.Equal(t0.Add(time.Minute)) {
		t.Errorf("owners = %+v", rec.Owners)
	}
}

// A dry run records no owners.
func TestOwnersNotRecordedInDryRun(t *testing.T) {
	h, _ := cleanupHarness(t)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.mustPass(app)

	before := h.record(app)
	h.engine.run("worker", enable, pathLabel(app), noOnStart)

	h.r.dryRun = true
	h.mustPass(app)

	if got := h.record(app); !slices.EqualFunc(got.Owners, before.Owners, state.Owner.Equal) {
		t.Errorf("owners = %+v, want %+v", got.Owners, before.Owners)
	}
}

func TestMergeOwners(t *testing.T) {
	a := state.Owner{ID: "a", Name: "a", Cleanup: "all"}
	b := state.Owner{ID: "b", Name: "b"}
	gone := func(o state.Owner, at time.Time) state.Owner {
		o.Gone = at
		return o
	}

	for _, tt := range []struct {
		name          string
		prev, cur     []state.Owner
		orphaned      time.Time
		want          []state.Owner
		wantOrphaned  time.Time
		wantMode      string
		wantDeletable bool
	}{
		{name: "new", cur: []state.Owner{b, a}, want: []state.Owner{a, b}, wantMode: "pause"},
		{name: "one leaves", prev: []state.Owner{a, b}, cur: []state.Owner{b}, want: []state.Owner{gone(a, t0), b}, wantMode: "pause"},
		{name: "left long ago", prev: []state.Owner{gone(a, t0.Add(-ownerLinger-time.Second)), b}, cur: []state.Owner{b},
			want: []state.Owner{b}, wantMode: "pause"},
		{name: "comes back", prev: []state.Owner{gone(a, t0.Add(-time.Minute))}, cur: []state.Owner{a}, orphaned: t0.Add(-time.Minute),
			want: []state.Owner{a}, wantMode: "all"},
		{name: "last leaves", prev: []state.Owner{gone(b, t0.Add(-time.Minute)), a}, want: []state.Owner{gone(a, t0), gone(b, t0.Add(-time.Minute))},
			wantOrphaned: t0, wantMode: "pause"},
		// Once the source has no owner, the window counts from the last
		// departure, not from now.
		{name: "orphaned for long", prev: []state.Owner{gone(a, t0.Add(-48*time.Hour)), gone(b, t0.Add(-48*time.Hour-ownerLinger))},
			orphaned: t0.Add(-48 * time.Hour), want: []state.Owner{gone(a, t0.Add(-48*time.Hour)), gone(b, t0.Add(-48*time.Hour-ownerLinger))},
			wantOrphaned: t0.Add(-48 * time.Hour), wantMode: "pause"},
		{name: "orphaned, the earlier left long before", prev: []state.Owner{gone(a, t0.Add(-time.Hour)), gone(b, t0.Add(-2*time.Hour))},
			orphaned: t0.Add(-time.Hour), want: []state.Owner{gone(a, t0.Add(-time.Hour))}, wantOrphaned: t0.Add(-time.Hour), wantMode: "all"},
		{name: "never had owners", wantOrphaned: t0, wantMode: "pause"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, orphaned := mergeOwners(tt.prev, tt.cur, t0, tt.orphaned)
			if !slices.EqualFunc(got, tt.want, state.Owner.Equal) || !orphaned.Equal(tt.wantOrphaned) {
				t.Errorf("mergeOwners = %+v, %v\nwant %+v, %v", got, orphaned, tt.want, tt.wantOrphaned)
			}

			if mode, pinned := cleanupOf(got); string(mode) != tt.wantMode || pinned != tt.wantDeletable {
				t.Errorf("cleanupOf = %s, %t", mode, pinned)
			}
		})
	}
}

func TestCleanupOf(t *testing.T) {
	for _, tt := range []struct {
		owners []state.Owner
		mode   string
		pinned bool
	}{
		{nil, "pause", false},
		{[]state.Owner{{Cleanup: "all", DeletePinned: true}}, "all", true},
		{[]state.Owner{{Cleanup: "all", DeletePinned: true}, {Cleanup: "all"}}, "all", false},
		{[]state.Owner{{Cleanup: "all", DeletePinned: true}, {Cleanup: "policy", DeletePinned: true}}, "policy", false},
		{[]state.Owner{{Cleanup: "policy"}, {Cleanup: ""}}, "pause", false},
		{[]state.Owner{{Cleanup: "all"}, {Cleanup: "everything"}}, "pause", false},
	} {
		if mode, pinned := cleanupOf(tt.owners); string(mode) != tt.mode || pinned != tt.pinned {
			t.Errorf("cleanupOf(%+v) = %s, %t", tt.owners, mode, pinned)
		}
	}
}
