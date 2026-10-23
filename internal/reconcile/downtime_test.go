package reconcile

import (
	"context"
	"errors"
	"fmt"
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
	stopDuring  = "kopia.stop-during-backup=true"
	pauseDuring = "kopia.pause-during-backup=true"
)

// runHooked runs the passes over p until its snapshot with hooks has
// ended, at most limit of them.
func (h *harness) runHooked(p string, limit int) {
	h.t.Helper()

	for range limit {
		h.passAndSettle(p)

		if h.record(p).HookRun == nil {
			return
		}
	}

	h.t.Fatalf("the snapshot with hooks of %s did not end: %+v\nlog:\n%s", p, h.record(p).HookRun, h.logs)
}

// mustActions checks the stops, starts, pauses and unpauses so far.
func (h *harness) mustActions(want ...string) {
	h.t.Helper()

	if got := h.engine.actionLog(); !slices.Equal(got, want) {
		h.t.Fatalf("container actions = %q, want %q\nlog:\n%s", got, want, h.logs)
	}
}

// mustState checks the state of a container.
func (h *harness) mustState(id string, want docker.State) {
	h.t.Helper()

	if got := h.engine.container(id).State; got != want {
		h.t.Fatalf("container %s is %s, want %s\nlog:\n%s", id, got, want, h.logs)
	}
}

// mustNotHeld checks that the state holds no container down.
func (h *harness) mustNotHeld() {
	h.t.Helper()

	if held := h.store.Held(); len(held) != 0 {
		h.t.Fatalf("held = %+v\nlog:\n%s", held, h.logs)
	}
}

// ownEvents hands the events a stop and a start of the container cause to
// HandleEvent, as Docker sends them.
func (h *harness) dockerEvent(id, action string) {
	kind := docker.KindDied
	if action == "start" {
		kind = docker.KindStarted
	}

	c := h.engine.container(id)
	h.r.HandleEvent(docker.Event{Kind: kind, Action: action, ContainerID: id, Name: c.Name, Labels: c.Labels, Time: time.Now()})
}

// pendingFor returns the events kept for p.
func (h *harness) pendingFor(p string) []docker.Event {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()

	return slices.Clone(h.r.pending[p])
}

// A container with stop-during-backup is stopped after the pre-exec hook
// and started before the post-exec hook. Its own die, stop and start take
// no snapshot and leave the schedule running, as it counts as running while
// it is down.
func TestStopDuringBackup(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop, noInterval, preExec, postExec, stopDuring)

	h.passAndSettle(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
	h.mustNext(app, t0.Add(time.Hour))

	c.add(time.Hour)

	// The pre-exec hook, then the stop.
	h.passAndSettle(app)
	h.mustExecs(preRun)
	h.mustActions()

	h.passAndSettle(app)
	h.mustActions("stop db")
	h.mustState(id, docker.StateExited)

	held := h.store.Held()
	if len(held) != 1 || held[0].ID != id || held[0].Mode != state.HeldStop || held[0].StartedAt.IsZero() ||
		held[0].Source != h.src(app) {
		t.Fatalf("held = %+v", held)
	}

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookDown || !run.DownDone || run.DownError != "" ||
		!run.ReturnBy.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("hook run %+v", run)
	}

	h.dockerEvent(id, "die")
	h.dockerEvent(id, "stop")

	if evs := h.pendingFor(app); len(evs) != 0 {
		t.Fatalf("own events kept: %+v", evs)
	}

	// Down, it still counts as running: the snapshot is queued, the
	// schedule is not paused, and no stop snapshot is taken.
	h.passAndSettle(app)
	h.mustTriggers(app, 1)

	if s := h.schedule(app); s == nil || s.Paused {
		t.Fatalf("schedule %+v", s)
	}

	// The snapshot ended: start, then the post-exec hook.
	h.passAndSettle(app)
	h.mustActions("stop db", "start db")
	h.mustExecs(preRun, postRun)
	h.mustState(id, docker.StateRunning)
	h.mustNotHeld()

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run left: %+v", run)
	}

	h.dockerEvent(id, "start")

	if evs := h.pendingFor(app); len(evs) != 0 {
		t.Fatalf("own start kept: %+v", evs)
	}

	h.passAndSettle(app)
	h.mustTriggers(app, 1)
	h.mustExecs(preRun, postRun)

	returns := h.metrics.returnResults(app)
	if len(returns) != 1 || returns[0].Container != "db" || returns[0].Mode != ModeStop || returns[0].Error != "" {
		t.Fatalf("returns = %+v", returns)
	}

	if !h.logged("container stopped for the snapshot", `"container":"db"`) || !h.logged("container started again after the snapshot") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// An event the watcher did not cause is not taken for its own: a start of
// the container by someone else still asks for a snapshot.
func TestOwnEventsCounted(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("db", enable, pathLabel(app))

	h.r.expect(id, "die", "stop")

	ev := func(action string) docker.Event {
		return docker.Event{Kind: docker.KindDied, Action: action, ContainerID: id, Name: "db"}
	}

	if !h.r.ownEvent(ev("die")) || !h.r.ownEvent(ev("stop")) {
		t.Fatal("the expected events are not the watcher's own")
	}

	if h.r.ownEvent(ev("die")) {
		t.Fatal("a second die is the watcher's own")
	}

	// A change that failed takes its events back.
	withdraw := h.r.expect(id, "die", "stop")
	withdraw()

	if h.r.ownEvent(ev("die")) {
		t.Fatal("a withdrawn die is the watcher's own")
	}

	h.r.expect(id, "start")
	c.add(ownEventWindow + time.Second)

	if h.r.ownEvent(docker.Event{Kind: docker.KindStarted, Action: "start", ContainerID: id}) {
		t.Fatal("a start after the window is the watcher's own")
	}

	if len(h.r.expected) != 0 {
		t.Fatalf("expectations left: %+v", h.r.expected)
	}
}

// A container with pause-during-backup is paused around the snapshot; a
// paused owner counts as running, as with any pause.
func TestPauseDuringBackup(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, pauseDuring, "kopia.max-downtime=10m")

	h.passAndSettle(app)
	c.add(time.Hour)

	// Without a pre-exec hook, the pause comes at once.
	h.passAndSettle(app)
	h.mustActions("pause db")
	h.mustState(id, docker.StatePaused)

	if run := h.record(app).HookRun; run == nil || !run.ReturnBy.Equal(t0.Add(time.Hour+10*time.Minute)) {
		t.Fatalf("hook run %+v", run)
	}

	h.runHooked(app, 4)
	h.mustActions("pause db", "unpause db")
	h.mustState(id, docker.StateRunning)
	h.mustTriggers(app, 1)
	h.mustNotHeld()

	if s := h.schedule(app); s == nil || s.Paused {
		t.Fatalf("schedule %+v", s)
	}

	if r := h.metrics.returnResults(app); len(r) != 1 || r[0].Mode != ModePause {
		t.Fatalf("returns = %+v", r)
	}
}

// A paused owner, paused by someone else, counts as running: its source is
// not paused.
func TestPausedOwnerRuns(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart)

	h.mustPass(app)
	h.engine.setState(id, docker.StatePaused)
	h.mustPass(app)

	if h.paused(app) {
		t.Fatal("a paused owner paused the source")
	}
}

// The container comes back whatever became of the snapshot: when it
// failed, and when kopia server refused to queue it.
func TestStopReturnsAfterFailure(t *testing.T) {
	t.Run("failed snapshot", func(t *testing.T) {
		h := newHarness(t)
		c := withClock(h)
		withHooks(h)
		id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)

		h.passAndSettle(app)
		h.fake.QueueSnapshotResult(h.src(app), backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})

		c.add(time.Hour)
		h.runHooked(app, 5)

		h.mustActions("stop db", "start db")
		h.mustExecs(postRun)
		h.mustState(id, docker.StateRunning)
		h.mustNotHeld()
	})

	t.Run("refused", func(t *testing.T) {
		h := newHarness(t)
		c := withClock(h)
		withHooks(h)
		id := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring)

		h.passAndSettle(app)
		c.add(time.Hour)
		h.passAndSettle(app)
		h.mustActions("stop db")

		h.fake.FailNextFor(h.src(app), backendtest.TriggerSnapshot, &backend.Error{Op: "TriggerSnapshot",
			Err: errors.New("repository is read-only")})

		res := h.pass(app)
		h.settle()

		if res.err == nil {
			t.Fatal("the refused trigger did not fail the pass")
		}

		h.mustActions("stop db", "start db")
		h.mustState(id, docker.StateRunning)
		h.mustTriggers(app, 1)
		h.mustNotHeld()

		if run := h.record(app).HookRun; run != nil {
			t.Fatalf("hook run %+v", run)
		}
	})
}

// A container that cannot be stopped takes no snapshot; the post-exec
// hooks run, and nothing else is stopped.
func TestStopFails(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)
	h.engine.run("app", "kopia.depends-on=db")

	h.engine.setLifecycle(func(_ context.Context, action string, c docker.Container) error {
		if action == "stop" && c.Name == "db" {
			return errors.New("403 Forbidden")
		}

		return nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.runHooked(app, 4)

	// The stop is tried twice: a failed call may be one cut short.
	h.mustActions("stop app", "stop db", "stop db", "start app")
	h.mustState(id, docker.StateRunning)
	h.mustTriggers(app, 0)
	h.mustExecs(postRun)
	h.mustNotHeld()

	if !h.logged("no snapshot: a container could not be stopped or paused for it", "403 Forbidden") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// withReturnLoop runs the retry loop of h until the test ends; wake makes
// it look at the held containers again, as after the clock moved.
func withReturnLoop(h *harness) (wake func()) {
	h.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		h.r.returnLoop(ctx)
		close(done)
	}()

	h.t.Cleanup(func() {
		cancel()
		<-done
	})

	return h.r.wakeReturns
}

// When the snapshot runs for longer than max-downtime, the container comes
// back before it ends, without a pass, and the post-exec hook runs once it
// has.
func TestStopMaxDowntime(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	wake := withReturnLoop(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring, "kopia.max-downtime=30m")

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	if held := h.store.Held(); len(held) != 1 || !held[0].ReturnBy.Equal(t0.Add(time.Hour+30*time.Minute)) || !held[0].Down {
		t.Fatalf("held = %+v", held)
	}

	h.fake.HoldSnapshots(h.src(app))
	h.mustPass(app)
	h.mustTriggers(app, 1)

	c.add(29 * time.Minute)
	wake()
	h.mustPass(app)
	h.mustActions("stop db")

	c.add(time.Minute)
	wake()
	eventually(t, "the container comes back", func() bool { return h.engine.container(id).State == docker.StateRunning })
	eventually(t, "it is no longer held", func() bool { return len(h.store.Held()) == 0 })

	h.mustActions("stop db", "start db")
	h.mustExecs()

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookSnapshot {
		t.Fatalf("hook run %+v", run)
	}

	if !h.logged("bringing back a container before its snapshot has ended: max-downtime has passed") {
		t.Fatalf("not logged:\n%s", h.logs)
	}

	h.fake.ReleaseSnapshots(h.src(app))
	h.settle()
	h.runHooked(app, 2)

	h.mustActions("stop db", "start db")
	h.mustExecs(postRun)
	h.mustNotHeld()
}

// max-downtime holds while kopia server fails: the passes cannot get to
// the snapshot steps, and the retry loop brings the container back all the
// same.
func TestStopMaxDowntimeWithoutPasses(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	wake := withReturnLoop(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring, "kopia.max-downtime=20m")

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)
	h.mustState(id, docker.StateExited)

	unavailable := &backend.Error{Op: "PlanSource", Kind: backend.ErrUnavailable, Err: errors.New("connection refused")}

	for range 3 {
		h.fake.FailNext(backendtest.PlanSource, unavailable)

		if res := h.pass(app); res.err == nil {
			t.Fatal("the pass did not fail")
		}
	}

	h.mustTriggers(app, 0)
	h.mustState(id, docker.StateExited)

	c.add(20 * time.Minute)
	wake()
	eventually(t, "the container comes back", func() bool { return h.engine.container(id).State == docker.StateRunning })

	// Once kopia server answers, the run ends without a snapshot.
	h.runHooked(app, 2)
	h.mustTriggers(app, 0)
	h.mustNotHeld()

	if !h.logged("no snapshot: the containers had to come back before it could be queued") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// A stop whose call fails while Docker goes on stopping the container,
// as when a proxy drops the connection, is waited for: the container is
// down, not on its way, when the snapshot is queued.
func TestStopCallCutShort(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring)

	first := true

	h.engine.setLifecycle(func(_ context.Context, action string, _ docker.Container) error {
		if action == "stop" && first {
			first = false

			go h.engine.setState(id, docker.StateExited)

			return errors.New("unexpected EOF")
		}

		return nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	if run := h.record(app).HookRun; run == nil || run.DownError != "" || run.DownTaken != 1 {
		t.Fatalf("hook run %+v", run)
	}

	h.runHooked(app, 3)
	h.mustTriggers(app, 1)
	h.mustActions("stop db", "stop db", "start db")
	h.mustState(id, docker.StateRunning)
}

// A container whose stop a stop of the watcher cut short still runs as
// Docker started it when the watcher comes back: it is looked at again
// until it is down, then started.
func TestStopNotDownYet(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	id := h.engine.run("db", enable, pathLabel(app), stopDuring)

	held := state.Held{ID: id, Name: "db", Mode: state.HeldStop, Source: h.src(app), Run: "r", At: t0, ReturnBy: t0.Add(time.Hour),
		StartedAt: h.engine.container(id).StartedAt}
	if err := h.store.UpdateHeld(func([]state.Held) ([]state.Held, error) { return []state.Held{held}, nil }); err != nil {
		t.Fatal(err)
	}

	h.r.returnLeftDown(context.Background())
	h.mustActions()

	if got := h.store.Held(); len(got) != 1 || !got[0].Released {
		t.Fatalf("held = %+v", got)
	}

	if !h.logged("the container is not down yet; looking again later") {
		t.Fatalf("not logged:\n%s", h.logs)
	}

	// Docker has stopped it meanwhile.
	h.engine.setState(id, docker.StateExited)
	c.add(time.Minute)

	todo, _ := h.r.returnsDue(c.now())
	if len(todo) != 1 {
		t.Fatalf("due = %+v", todo)
	}

	h.r.bringBack(context.Background(), h.r.log, todo, false)
	h.mustActions("start db")
	h.mustNotHeld()

	// Long after, a container that still runs as started is no longer
	// taken for one on its way down.
	if err := h.store.UpdateHeld(func([]state.Held) ([]state.Held, error) {
		held.StartedAt = h.engine.container(id).StartedAt
		return []state.Held{held}, nil
	}); err != nil {
		t.Fatal(err)
	}

	c.add(stopCallTimeout)
	h.r.returnLeftDown(context.Background())
	h.mustNotHeld()
	h.mustActions("start db")
}

// A watcher that stops after the containers went down, but before the
// snapshot is queued, brings them back; the run takes no snapshot of them
// running.
func TestStopLostBeforeSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	if run := h.record(app).HookRun; run == nil || !run.DownDone || run.DownTaken != 1 {
		t.Fatalf("hook run %+v", run)
	}

	h.restartHooks(c)
	h.r.returnLeftDown(context.Background())
	h.mustState(id, docker.StateRunning)

	h.runHooked(app, 2)
	h.mustTriggers(app, 0)
	h.mustExecs(postRun)

	if !h.logged("no snapshot: the containers came back before it could be queued") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// notPersistedStore keeps the held containers in memory only, as with a
// full disk.
type notPersistedStore struct{ *state.Store }

func (s notPersistedStore) UpdateHeld(fn func([]state.Held) ([]state.Held, error)) error {
	if err := s.Store.UpdateHeld(fn); err != nil {
		return err
	}

	return fmt.Errorf("write state file: %w", state.ErrNotPersisted)
}

// Without STATE_FILE no container is taken down: a crash would leave it
// down for good.
func TestStopNeedsStateFile(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.r.store = notPersistedStore{h.store}
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)

	h.passAndSettle(app)
	c.add(time.Hour)
	h.runHooked(app, 3)

	h.mustActions()
	h.mustTriggers(app, 0)
	h.mustExecs(postRun)
	h.mustState(id, docker.StateRunning)
	h.mustNotHeld()

	if !h.logged("no snapshot: STATE_FILE cannot be written") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// A container that goes down with the stopped one, and that another
// snapshot holds down already, makes the snapshot wait.
func TestHeldSharedDependent(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	other := "/data/other"
	db := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring)
	web := h.engine.run("web", enable, pathLabel(other), noOnStart, stopDuring, "kopia.depends-on=db")

	h.passAndSettle(app)
	h.passAndSettle(other)
	c.add(time.Hour)

	// web's snapshot goes first and holds web down.
	h.fake.HoldSnapshots(h.src(other))
	h.passAndSettle(other)
	h.mustPass(other)
	h.mustActions("stop web")

	// db's snapshot needs web, which goes down with db.
	h.mustPass(app)
	h.mustActions("stop web")
	h.mustState(db, docker.StateRunning)

	if w := h.r.heldWaiters[web]; !slices.Equal(w, []string{app}) {
		t.Fatalf("waiters = %v", w)
	}

	h.fake.ReleaseSnapshots(h.src(other))
	h.settle()
	h.runHooked(other, 2)
	h.mustActions("stop web", "start web")

	h.runHooked(app, 4)
	h.mustActions("stop web", "start web", "stop web", "stop db", "start db", "start web")
	h.mustNotHeld()
}

// An owner another snapshot holds down makes a snapshot whose plan takes
// nothing down wait too, before it is queued.
func TestHeldOwnerAfterPreExec(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	other := "/data/other"
	id := h.engine.run("db", enable, pathLabel(app+","+other), noOnStart, preExec, stopDuring)

	h.passAndSettle(app)
	h.passAndSettle(other)
	c.add(time.Hour)

	// The pre-exec hook of app's snapshot runs; meanwhile other's snapshot
	// holds db down.
	h.passAndSettle(app)
	h.mustExecs(preRun)

	if err := h.engine.Stop(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	held := state.Held{ID: id, Name: "db", Mode: state.HeldStop, Source: h.src(other), Run: "r", At: c.now(),
		ReturnBy: c.now().Add(time.Hour), StartedAt: h.engine.container(id).StartedAt, Down: true}
	if err := h.store.UpdateHeld(func([]state.Held) ([]state.Held, error) { return []state.Held{held}, nil }); err != nil {
		t.Fatal(err)
	}

	h.passAndSettle(app)
	h.mustTriggers(app, 0)

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookPre {
		t.Fatalf("hook run %+v", run)
	}

	h.r.bringBack(context.Background(), h.r.log, h.store.Held(), false)
	h.mustState(id, docker.StateRunning)

	h.runHooked(app, 4)
	h.mustTriggers(app, 1)
}

// While a hook of another source runs in a container, no snapshot takes it
// down; the snapshot goes once the hook has ended. A container brought
// back stays held until the post-exec hooks of its run are done.
func TestDowntimeWaitsForHooks(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	other := "/data/other"
	id := h.engine.run("db", enable, pathLabel(app+","+other), noOnStart, postExec, stopDuring)

	release := make(chan struct{})

	h.engine.setExec(func(ctx context.Context, _ docker.Container, _ []string) (docker.ExecResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}

		return docker.ExecResult{}, ctx.Err()
	})

	h.passAndSettle(app)
	h.passAndSettle(other)
	c.add(time.Hour)

	// app's snapshot: down, queued, ended; its post-exec hook hangs.
	h.passAndSettle(app)
	h.passAndSettle(app)
	h.mustPass(app)

	eventually(t, "the post-exec hook runs", func() bool { return len(h.engine.execLog()) == 1 })
	h.mustState(id, docker.StateRunning)

	if held := h.store.Held(); len(held) != 1 || !held[0].Back {
		t.Fatalf("held = %+v", held)
	}

	// other's snapshot waits for db.
	h.mustPass(other)
	h.mustActions("stop db", "start db")

	// A hook of another source keeps it up too.
	h.r.mu.Lock()
	h.r.execs[id]++
	h.r.mu.Unlock()

	close(release)
	h.settle()
	h.mustNotHeld()

	h.mustPass(other)
	h.mustActions("stop db", "start db")

	if !h.logged("the snapshot waits: a hook of another source runs in a container it takes down") {
		t.Fatalf("not logged:\n%s", h.logs)
	}

	h.r.mu.Lock()
	delete(h.r.execs, id)
	h.r.mu.Unlock()

	h.runHooked(other, 4)
	h.mustActions("stop db", "start db", "stop db", "start db")
}

// The containers that depend on the stopped one, by name or by compose
// service, directly or not, are stopped first and started after it;
// those that do not run are left alone.
func TestDependsOnOrder(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)

	compose := func(service string) []string {
		return []string{"com.docker.compose.project=shop", "com.docker.compose.service=" + service}
	}

	db := h.engine.run("shop-database-1", append([]string{enable, pathLabel(app), noOnStart, stopDuring}, compose("database")...)...)
	api := h.engine.run("shop-api-1", append([]string{"kopia.depends-on=database"}, compose("api")...)...)
	worker := h.engine.run("worker", "kopia.depends-on=shop-api-1, shop-database-1")
	h.engine.create("old", docker.StateExited, "kopia.depends-on=shop-database-1")
	h.engine.run("other", append([]string{"kopia.depends-on=database"}, "com.docker.compose.project=blog")...)

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	h.mustActions("stop worker", "stop shop-api-1", "stop shop-database-1")

	for _, id := range []string{db, api, worker} {
		h.mustState(id, docker.StateExited)
	}

	h.runHooked(app, 4)
	h.mustActions("stop worker", "stop shop-api-1", "stop shop-database-1",
		"start shop-database-1", "start shop-api-1", "start worker")
	h.mustNotHeld()

	if r := h.metrics.returnResults(app); len(r) != 3 {
		t.Fatalf("returns = %+v", r)
	}
}

// A depends-on label that names no container, or a cycle, is a label error:
// nothing is stopped and no snapshot is taken.
func TestDependsOnErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		labels  [][]string
		problem string
	}{
		{"missing", [][]string{{"api", "kopia.depends-on=db,ghost"}}, `api: \"ghost\" names no container`},
		{"cycle", [][]string{{"api", "kopia.depends-on=db,worker"}, {"worker", "kopia.depends-on=api"}},
			"api: the container depends on itself through kopia.depends-on"},
		{"self", [][]string{{"api", "kopia.depends-on=api,db"}}, "api: the container depends on itself"},
		{"not a name", [][]string{{"api", "kopia.depends-on=db cache"}}, `api: \"db cache\" is not a container or service name`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)
			withHooks(h)
			h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)

			for _, l := range tt.labels {
				h.engine.run(l[0], l[1:]...)
			}

			h.passAndSettle(app)
			c.add(time.Hour)
			h.runHooked(app, 3)

			h.mustActions()
			h.mustTriggers(app, 0)
			h.mustExecs(postRun)
			h.mustNotHeld()

			if !h.logged("no snapshot: the containers of the source cannot be stopped or paused for it", tt.problem) {
				t.Fatalf("not logged:\n%s", h.logs)
			}

			if n := h.metrics.count(app, ReasonLabels); n != 1 {
				t.Fatalf("%d label errors counted", n)
			}

			// A full reconcile reports the label, once.
			if _, err := h.r.paths(context.Background()); err != nil {
				t.Fatal(err)
			}

			if _, err := h.r.paths(context.Background()); err != nil {
				t.Fatal(err)
			}

			if n := h.count("container labels have errors: the snapshots that stop a container it depends on are not taken"); n < 1 ||
				n > len(tt.labels) {
				t.Fatalf("reported %d times:\n%s", n, h.logs)
			}
		})
	}
}

// Stopping and pausing together, or a stop label with an error, fails the
// snapshot instead of taking it with the container up.
func TestDowntimeLabelErrors(t *testing.T) {
	for _, tt := range []struct {
		name   string
		labels []string
		opts   []harnessOption
	}{
		{"both", []string{stopDuring, pauseDuring}, nil},
		{"strict", []string{"kopia.stop-during-backup=yes"}, []harnessOption{withStrict}},
		{"bad max-downtime", []string{stopDuring, "kopia.max-downtime=1d"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, tt.opts...)
			c := withClock(h)
			withHooks(h)
			h.engine.run("db", append([]string{enable, pathLabel(app), noOnStart, postExec}, tt.labels...)...)

			h.passAndSettle(app)

			// The labels cannot be applied, so the source is not
			// registered; it is a source kopia server knows already.
			if !h.fake.Known(h.src(app)) {
				h.fake.SetPolicy(h.src(app), `{"scheduling":{"cron":["0 * * * *"]}}`)
				h.passAndSettle(app)
			}

			c.add(time.Hour)
			h.runHooked(app, 3)

			h.mustActions()
			h.mustTriggers(app, 0)

			if !h.logged("no snapshot: the containers of the source cannot be stopped or paused for it") {
				t.Fatalf("not logged:\n%s", h.logs)
			}
		})
	}
}

// withSeparateHooks gives the passes of h a Run context and a hook context
// of their own, as Run does: stop cancels the first, as when the watcher
// gets SIGTERM, and the hooks go on with the second.
func withSeparateHooks(h *harness) (hookCtx context.Context, stop func()) {
	h.t.Helper()

	withHooks(h)

	runCtx, cancelRun := context.WithCancel(context.Background())
	hookCtx, cancelHooks := context.WithCancel(context.Background())

	h.t.Cleanup(func() {
		cancelRun()
		cancelHooks()
		h.settle()
	})

	h.r.mu.Lock()
	h.r.runCtx, h.r.hookCtx = runCtx, hookCtx
	h.r.mu.Unlock()

	return hookCtx, cancelRun
}

// When the watcher stops during the snapshot, the container comes back at
// once, the snapshot done or not; the run goes on after the restart.
func TestStopReturnsOnShutdown(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	hookCtx, stop := withSeparateHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	h.fake.HoldSnapshots(h.src(app))
	h.mustPass(app)
	h.mustTriggers(app, 1)
	h.mustState(id, docker.StateExited)

	stop()
	h.r.returnHeld(hookCtx, true)

	h.mustState(id, docker.StateRunning)
	h.mustActions("stop db", "start db")
	h.mustNotHeld()

	h.settle()
	h.fake.ReleaseSnapshots(h.src(app))
	h.restartHooks(c)
	h.runHooked(app, 3)

	h.mustExecs(postRun)
	h.mustActions("stop db", "start db")
}

// When the watcher stops while it takes the containers down, it takes no
// more down and brings back those it took; after the restart the run takes
// no snapshot and runs the post-exec hook.
func TestStopDuringTakeDown(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	_, stop := withSeparateHooks(h)
	db := h.engine.run("db", enable, pathLabel(app), noOnStart, postExec, stopDuring)
	api := h.engine.run("api", "kopia.depends-on=db")

	h.engine.setLifecycle(func(_ context.Context, action string, c docker.Container) error {
		if action == "stop" && c.Name == "api" {
			// SIGTERM comes while api stops.
			stop()
		}

		return nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.mustPass(app)
	h.settle()

	h.mustActions("stop api", "start api")
	h.mustState(api, docker.StateRunning)
	h.mustState(db, docker.StateRunning)
	h.mustNotHeld()

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookDown || run.DownDone {
		t.Fatalf("hook run %+v", run)
	}

	h.engine.setLifecycle(nil)
	h.restartHooks(c)
	h.runHooked(app, 2)

	h.mustTriggers(app, 0)
	h.mustExecs(postRun)
	h.mustActions("stop api", "start api")

	if !h.logged("no snapshot: the watcher stopped while it took the containers down") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// Run, stopped by its context while the snapshot of a container it stopped
// runs, starts the container before it returns.
func TestRunReturnsOnShutdown(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.ShutdownGrace = 5 * time.Second })
	h.fake.SetSchedule(kopiaSchedule)
	id := h.engine.run("db", enable, pathLabel(app), noInterval, stopDuring)

	h.fake.HoldSnapshots(h.src(app))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		h.r.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	// The first pass registers the source; a start of the container then
	// takes a snapshot with it stopped.
	h.r.Enqueue(app)
	eventually(t, "the source is registered", func() bool { return h.fake.Known(h.src(app)) })
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	eventually(t, "the snapshot is queued", func() bool { return h.triggers(app) == 1 })
	h.mustState(id, docker.StateExited)

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	h.mustState(id, docker.StateRunning)
	h.mustActions("stop db", "start db")
	h.mustNotHeld()
}

// After a restart, the containers the state lists as held come back before
// any pass, unless someone else touched them meanwhile.
func TestReturnAfterRestart(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)

	ours := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring)
	frozen := h.engine.run("cache", enable, pathLabel(app), noOnStart, pauseDuring)
	restarted := h.engine.run("web", "kopia.depends-on=db")
	running := h.engine.run("api", "kopia.depends-on=db")
	gone := "gone-1"

	src := h.src(app)
	held := []state.Held{
		{ID: restarted, Name: "web", Mode: state.HeldStop, Source: src, Run: "r", Order: 0, At: t0, Down: true,
			StartedAt: h.engine.container(restarted).StartedAt},
		{ID: running, Name: "api", Mode: state.HeldStop, Source: src, Run: "r", Order: 1, At: t0, Down: true,
			StartedAt: h.engine.container(running).StartedAt},
		{ID: ours, Name: "db", Mode: state.HeldStop, Source: src, Run: "r", Order: 2, At: t0, Down: true,
			StartedAt: h.engine.container(ours).StartedAt},
		{ID: gone, Name: "gone", Mode: state.HeldStop, Source: src, Run: "r", Order: 3, At: t0, Down: true, StartedAt: t0},
		{ID: frozen, Name: "cache", Mode: state.HeldPause, Source: src, Run: "r", Order: 4, At: t0, Down: true,
			StartedAt: h.engine.container(frozen).StartedAt},
	}

	if err := h.store.UpdateHeld(func([]state.Held) ([]state.Held, error) { return held, nil }); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{ours, restarted} {
		if err := h.engine.Stop(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}

	if err := h.engine.Pause(context.Background(), frozen); err != nil {
		t.Fatal(err)
	}

	// Someone else started web and stopped it again.
	if err := h.engine.Start(context.Background(), restarted); err != nil {
		t.Fatal(err)
	}

	if err := h.engine.Stop(context.Background(), restarted); err != nil {
		t.Fatal(err)
	}

	before := len(h.engine.actionLog())

	h.restart(c)

	firstPass := make(chan []string, 1)

	h.r.afterPass = func(string, passResult) {
		select {
		case firstPass <- h.engine.actionLog()[before:]:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		h.r.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	h.r.Enqueue(app)

	select {
	case first := <-firstPass:
		if !slices.Equal(first, []string{"unpause cache", "start db"}) {
			t.Fatalf("actions before the first pass = %q\nlog:\n%s", first, h.logs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no pass")
	}

	h.mustState(ours, docker.StateRunning)
	h.mustState(frozen, docker.StateRunning)
	h.mustState(restarted, docker.StateExited)
	h.mustNotHeld()

	if !h.logged("not bringing back a container someone else started meanwhile", `"container":"web"`) ||
		!h.logged("not bringing back a container that runs again", `"container":"api"`) ||
		!h.logged("not bringing back a container that is gone", `"container":"gone"`) {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// A container that cannot be started again stays held, counts as running,
// and is tried again until it comes back; the snapshots that need it wait.
func TestReturnRetried(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, stopDuring)

	fail := true

	h.engine.setLifecycle(func(_ context.Context, action string, _ docker.Container) error {
		if action == "start" && fail {
			return errors.New("port is already allocated")
		}

		return nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.runHooked(app, 4)

	h.mustState(id, docker.StateExited)

	held := h.store.Held()
	if len(held) != 1 || !held[0].Released {
		t.Fatalf("held = %+v", held)
	}

	if r := h.metrics.returnResults(app); len(r) != 1 || !strings.Contains(r[0].Error, "port is already allocated") {
		t.Fatalf("returns = %+v", r)
	}

	// Still a running owner, and the next scheduled snapshot waits.
	c.add(time.Hour)
	h.passAndSettle(app)

	// The policy of a source with hooks is manual anyway.
	if h.schedule(app).Paused {
		t.Fatal("a container the watcher failed to bring back paused the source")
	}

	h.mustTriggers(app, 1)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("a run started while its container is held: %+v", run)
	}

	fail = false

	if failed, _ := h.r.bringBack(context.Background(), h.r.log, h.store.Held(), false); len(failed) != 0 {
		t.Fatalf("failed = %v", failed)
	}

	h.mustState(id, docker.StateRunning)
	h.mustNotHeld()

	if r := h.metrics.returnResults(app); len(r) != 2 || r[1].Error != "" || r[1].Downtime <= 0 {
		t.Fatalf("returns = %+v", r)
	}

	// The scheduled time that waited is taken now.
	h.runHooked(app, 4)
	h.mustTriggers(app, 2)
}

// A container shared by two sources is held for one snapshot at a time:
// the other source waits, its pause unchanged, and goes once it is back.
func TestHeldSharedContainer(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	other := "/data/other"
	id := h.engine.run("db", enable, pathLabel(app+","+other), noOnStart, stopDuring)

	h.passAndSettle(app)
	h.passAndSettle(other)
	c.add(time.Hour)

	h.fake.HoldSnapshots(h.src(app))
	h.passAndSettle(app)
	h.mustPass(app)
	h.mustActions("stop db")

	// The snapshot of the first source is held: no settling.
	res := h.mustPass(other)
	h.mustTriggers(other, 0)

	if h.schedule(other).Paused || res.wake.IsZero() {
		t.Fatalf("schedule %+v, wake %v", h.schedule(other), res.wake)
	}

	if run := h.record(other).HookRun; run != nil {
		t.Fatalf("the other source started a run: %+v", run)
	}

	if w := h.r.heldWaiters[id]; !slices.Equal(w, []string{other}) {
		t.Fatalf("waiters = %v", w)
	}

	h.fake.ReleaseSnapshots(h.src(app))
	h.settle()
	h.runHooked(app, 2)
	h.mustActions("stop db", "start db")

	h.runHooked(other, 4)
	h.mustTriggers(other, 1)
	h.mustActions("stop db", "start db", "stop db", "start db")
	h.mustNotHeld()
}

// A pause or an unpause that finds its work done already, as when someone
// else paused or unpaused the container just before, succeeds: the
// snapshot is taken, and the container counts as back.
func TestPauseAlreadyDone(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("cache", enable, pathLabel(app), noOnStart, pauseDuring)

	h.engine.setLifecycle(func(_ context.Context, action string, _ docker.Container) error {
		switch action {
		case "pause":
			h.engine.setState(id, docker.StatePaused)
		case "unpause":
			h.engine.setState(id, docker.StateRunning)
		}

		return nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	if run := h.record(app).HookRun; run == nil || run.DownError != "" || run.DownTaken != 1 {
		t.Fatalf("hook run %+v\nlog:\n%s", run, h.logs)
	}

	h.mustState(id, docker.StatePaused)
	h.runHooked(app, 3)

	h.mustTriggers(app, 1)
	h.mustActions("pause cache", "unpause cache")
	h.mustState(id, docker.StateRunning)
	h.mustNotHeld()

	if r := h.metrics.returnResults(app); len(r) != 1 || r[0].Error != "" || r[0].Mode != ModePause {
		t.Fatalf("returns = %+v", r)
	}
}

// The fake engine keeps docker.Engine's contract for a pause or an unpause
// with nothing to do, and for one that cannot be done.
func TestFakeEnginePauseContract(t *testing.T) {
	f := newFakeEngine()
	id := f.run("cache")
	ctx := context.Background()

	for _, step := range []struct {
		do   func(context.Context, string) error
		want docker.State
	}{
		{f.Pause, docker.StatePaused}, {f.Pause, docker.StatePaused}, {f.Unpause, docker.StateRunning}, {f.Unpause, docker.StateRunning},
	} {
		if err := step.do(ctx, id); err != nil || f.container(id).State != step.want {
			t.Fatalf("state %s, error %v; want %s", f.container(id).State, err, step.want)
		}
	}

	f.setState(id, docker.StateExited)

	if err := f.Pause(ctx, id); !errors.Is(err, docker.ErrNotRunning) {
		t.Errorf("pause of a stopped container: %v", err)
	}

	if err := f.Unpause(ctx, id); !errors.Is(err, docker.ErrNotPaused) {
		t.Errorf("unpause of a stopped container: %v", err)
	}
}

// A stop the watcher causes for stop-during-backup is no stop of the
// container: with snapshot-on-stop and pin-on-stop, and its own die and
// stop delivered as Docker sends them, it takes no stop snapshot and adds
// no pin, while the container's own stop afterwards does both.
func TestStopDuringBackupNoPin(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop, "kopia.pin-on-stop=before-update", stopDuring)

	h.passAndSettle(app)
	c.add(time.Hour)

	h.passAndSettle(app)
	h.mustActions("stop db")
	h.dockerEvent(id, "die")
	h.dockerEvent(id, "stop")

	// Dropped as the watcher's own; and held down, db still counts as
	// running, which keeps a die that came late from asking either.
	if evs := h.pendingFor(app); len(evs) != 0 {
		t.Fatalf("own events kept: %+v", evs)
	}

	h.passAndSettle(app)
	h.passAndSettle(app)
	h.mustActions("stop db", "start db")
	h.dockerEvent(id, "start")

	h.passAndSettle(app)
	h.mustTriggers(app, 1)

	pins := func() (n int) {
		for _, call := range h.fake.CallsFor(h.src(app), backendtest.AnnotateSnapshot) {
			n += len(call.AddPins)
		}

		return n
	}

	if n := pins(); n != 0 {
		t.Fatalf("%d pins added for the watcher's own stop\nlog:\n%s", n, h.logs)
	}

	if rec := h.record(app); len(rec.WaitPins) != 0 || rec.HookRun != nil {
		t.Fatalf("record %+v", rec)
	}

	// The container's own stop takes the stop snapshot, with the pin.
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.passAndSettle(app)
	h.passAndSettle(app)

	h.mustTriggers(app, 2)

	if n := pins(); n != 1 {
		t.Fatalf("%d pins for the container's own stop, want 1\nlog:\n%s", n, h.logs)
	}
}
