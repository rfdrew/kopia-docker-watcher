package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// A source whose containers are all removed keeps the watcher's schedule,
// paused; once a container is created again, a time that passed meanwhile
// is a missed one, as after a stop.
func TestHooksScheduleOutlivesContainers(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.time-of-day=13:00")

	h.passAndSettle(app)
	h.mustNext(app, t0.Add(time.Hour))

	h.r.HandleEvent(h.engine.remove(id))
	h.passAndSettle(app)

	if s := h.schedule(app); s == nil || !s.Paused || !s.Next.Equal(t0.Add(time.Hour)) {
		t.Fatalf("schedule %+v\nlog:\n%s", s, h.logs)
	}

	if h.logged("kopia server takes the scheduled snapshots of the source again") {
		t.Fatalf("hand-back logged for a source without containers:\n%s", h.logs)
	}

	c.add(90 * time.Minute)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.time-of-day=13:00")
	h.passAndSettle(app)

	h.mustExecs(preRun)

	if run := h.record(app).HookRun; run == nil || !strings.HasPrefix(run.Reason, "missed schedule at ") {
		t.Fatalf("hook run %+v", run)
	}
}

// A paused source whose container is recreated with hooks and started
// while the watcher did not see it takes the start snapshot, with hooks,
// though its policy stays manual.
func TestHooksUnseenStartIntoHooks(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.engine.create("db", docker.StateExited, enable, pathLabel(app))

	h.engine.setState(id, docker.StateRunning)
	h.passAndSettle(app)
	h.engine.setState(id, docker.StateExited)
	h.passAndSettle(app)

	if !h.paused(app) {
		t.Fatal("the source is not paused")
	}

	h.engine.remove(id)
	h.engine.run("db", enable, pathLabel(app), preExec)

	h.passAndSettle(app)
	h.passAndSettle(app)

	h.mustExecs(preRun)
	h.mustTriggers(app, 1)

	if w := h.onlyWait(app); !strings.Contains(w.Reason, "without an event") {
		t.Fatalf("wait %+v", w)
	}
}

// When kopia server's clock is far behind the watcher's, the schedule
// follows the watcher's clock, with a warning.
func TestHooksClockSkew(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.fake.SetClock(func() time.Time { return c.now().Add(-150 * time.Minute) })
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec)

	h.passAndSettle(app)
	h.mustNext(app, t0.Add(time.Hour))

	if !h.logged("kopia server's clock is behind the watcher's") {
		t.Fatalf("no warning:\n%s", h.logs)
	}

	c.add(time.Hour)
	h.takeScheduled(app)
	h.mustTriggers(app, 1)
	h.mustNext(app, t0.Add(2*time.Hour))
}

// A new schedule drops a due time of the old one: the old schedule's
// rules do not decide on it.
func TestHooksScheduleChangeWhileDue(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.interval=30m")

	h.passAndSettle(app)
	h.mustNext(app, t0.Add(30*time.Minute))

	h.engine.setState(id, docker.StateExited)
	h.passAndSettle(app)

	c.add(time.Hour)
	h.engine.remove(id)
	id = h.engine.create("db", docker.StateExited, enable, pathLabel(app), noOnStart, preExec, "kopia.time-of-day=12:50",
		"kopia.run-missed=false")
	h.passAndSettle(app)

	h.mustNext(app, time.Date(2026, 10, 19, 12, 50, 0, 0, time.UTC))

	if s := h.schedule(app); s.Interval || s.RunMissed {
		t.Fatalf("schedule %+v", s)
	}

	h.engine.setState(id, docker.StateRunning)
	h.passAndSettle(app)
	h.mustExecs()
	h.mustTriggers(app, 0)
}

// A full reconcile takes a change of the inherited schedule: a later time,
// or none, replaces the stored one, but a time due within moments stays.
func TestHooksScheduleSweep(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec)

	h.passAndSettle(app)
	h.mustNext(app, t0.Add(time.Hour))

	// kopia server's clock, a little ahead, is past 13:00 already.
	c.add(59*time.Minute + 30*time.Second)
	h.fake.SetClock(func() time.Time { return c.now().Add(time.Minute) })
	h.fullPass(app)
	h.settle()
	h.mustNext(app, t0.Add(time.Hour))

	// The parent policy turns the schedule off.
	h.fake.SetClock(c.now)
	h.fake.SetSchedule(func(json.RawMessage, time.Time, int) []time.Time { return nil })
	c.add(-30 * time.Minute)
	h.fullPass(app)
	h.settle()
	h.mustNext(app, time.Time{})

	c.add(time.Hour)
	h.passAndSettle(app)
	h.mustExecs()
}

// A scheduled time that comes with a stop absorbed by a pending snapshot
// gets a pass soon after, which takes it with hooks.
func TestHooksScheduleWithAbsorbedStop(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	web := h.engine.run("web", enable, pathLabel(app), noOnStart, onStop)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec)

	h.passAndSettle(app)

	c.add(time.Hour)
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusPending})
	h.engine.setState(web, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, web))

	res := h.passAndSettle(app)
	if !res.recheck {
		t.Fatalf("no pass soon after: %+v\nlog:\n%s", res, h.logs)
	}

	// The pending snapshot saved one.
	h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: c.now().Add(time.Second), EndTime: c.now().Add(time.Minute)})
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusIdle})
	h.passAndSettle(app)
	h.mustExecs(preRun)
}

// A TriggerSnapshot error that is not tried again ends the run without a
// snapshot: the post-exec hooks run, and the next time is taken.
func TestHooksTriggerFails(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.passAndSettle(app)

	c.add(time.Hour)
	h.passAndSettle(app)
	h.fake.FailNext(backendtest.TriggerSnapshot, &backend.Error{Op: "TriggerSnapshot", Err: errors.New("repository is read-only")})

	if res := h.pass(app); res.err == nil || res.retry {
		t.Fatalf("pass %+v", res)
	}

	h.settle()
	h.mustExecs(preRun, postRun)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run left: %+v", run)
	}

	// The failed call counts.
	c.add(time.Hour)
	h.takeScheduled(app)
	h.mustTriggers(app, 2)
}

// A start whose snapshot with hooks failed to queue at first is not taken
// twice: the run took the start.
func TestHooksStartTakenOnce(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), postExec)

	h.passAndSettle(app)

	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.fake.FailNext(backendtest.TriggerSnapshot, temporary("TriggerSnapshot"))

	if res := h.pass(app); res.err == nil || !res.retry {
		t.Fatalf("pass %+v", res)
	}

	// The retry queues the snapshot, which runs for longer than the
	// minimum interval; the failed call counts.
	h.passAndSettle(app)
	h.mustTriggers(app, 2)

	c.add(11 * time.Minute)

	for range 3 {
		h.passAndSettle(app)
	}

	h.mustTriggers(app, 2)
	h.mustExecs(postRun)
}

// A snapshot at a stop waits while the pre-exec hooks of another
// container run, and is taken after the snapshot with hooks is queued.
func TestHooksStopWaitsForPreExec(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	web := h.engine.run("web", enable, pathLabel(app), noOnStart, onStop)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec)

	release := make(chan struct{})
	h.engine.setExec(func(ctx context.Context, _ docker.Container, _ []string) (docker.ExecResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}

		return docker.ExecResult{}, ctx.Err()
	})

	h.passAndSettle(app)

	c.add(time.Hour)
	h.mustPass(app)

	h.engine.setState(web, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, web))
	h.mustPass(app)
	h.mustTriggers(app, 0)

	if !h.logged("the snapshot at the stop waits for the pre-exec hooks under way") {
		t.Fatalf("not held:\n%s", h.logs)
	}

	close(release)
	h.settle()

	h.passAndSettle(app)
	h.mustTriggers(app, 2)

	waits := h.waits(app)
	if len(waits) != 2 || waits[0].Stop || !waits[1].Stop {
		t.Fatalf("waits %+v", waits)
	}
}

// Containers run their hooks in name order; the first pre-exec hook that
// fails stops the others and the snapshot, and every post-exec hook runs.
func TestHooksSeveralContainers(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)

	for _, name := range []string{"c", "a", "b"} {
		h.engine.run(name, enable, pathLabel(app), noOnStart, "kopia.pre-exec=dump "+name, "kopia.post-exec=clean "+name)
	}

	h.engine.setExec(func(_ context.Context, ctr docker.Container, cmd []string) (docker.ExecResult, error) {
		if ctr.Name == "b" && strings.Contains(cmd[2], "dump") {
			return docker.ExecResult{ExitCode: 1}, nil
		}

		return docker.ExecResult{}, nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.takeScheduled(app)

	h.mustExecs("a: sh -c dump a", "b: sh -c dump b", "a: sh -c clean a", "b: sh -c clean b", "c: sh -c clean c")
	h.mustTriggers(app, 0)

	var failed []string

	for _, r := range h.metrics.hookResults(app) {
		if r.Error != "" {
			failed = append(failed, r.Hook+" "+r.Container)
		}
	}

	if !slices.Equal(failed, []string{"pre-exec b"}) {
		t.Fatalf("failed hooks %q", failed)
	}
}

// A stop of the watcher that cuts the pre-exec hooks short stores nothing:
// after the restart no snapshot is taken, and the post-exec hooks run.
func TestHooksStopDuringPreExec(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.engine.setExec(func(ctx context.Context, _ docker.Container, cmd []string) (docker.ExecResult, error) {
		if cmd[0] == "sh" {
			<-ctx.Done()
			return docker.ExecResult{}, ctx.Err()
		}

		return docker.ExecResult{}, nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.mustPass(app)

	h.stopHooks()

	if run := h.record(app).HookRun; run == nil || run.PreDone || run.Phase != state.HookPre {
		t.Fatalf("hook run %+v", run)
	}

	if results := h.metrics.hookResults(app); len(results) != 0 {
		t.Fatalf("hook results of a stop: %+v", results)
	}

	h.restartHooks(c)
	h.passAndSettle(app)

	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 0)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run %+v", run)
	}
}

// A stop of the watcher that cuts the post-exec hooks short runs them
// again after the restart.
func TestHooksStopDuringPostExec(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.engine.setExec(func(ctx context.Context, _ docker.Container, cmd []string) (docker.ExecResult, error) {
		if cmd[0] == "rm" {
			<-ctx.Done()
			return docker.ExecResult{}, ctx.Err()
		}

		return docker.ExecResult{}, nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)
	h.passAndSettle(app)
	h.mustPass(app)

	h.stopHooks()

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookPost {
		t.Fatalf("hook run %+v", run)
	}

	h.engine.setExec(nil)
	h.restartHooks(c)
	h.passAndSettle(app)

	h.mustExecs(preRun, postRun, postRun)
	h.mustTriggers(app, 1)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run %+v", run)
	}

	if !h.logged("running the post-exec hooks again") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// A restart while the snapshot of a run is under way follows the snapshot
// again, then runs the post-exec hooks.
func TestHooksRestartDuringSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.passAndSettle(app)
	c.add(time.Hour)
	h.passAndSettle(app)

	h.fake.HoldSnapshots(h.src(app))
	h.mustPass(app)

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookSnapshot {
		t.Fatalf("hook run %+v", run)
	}

	h.stopHooks()
	h.fake.ReleaseSnapshots(h.src(app))
	h.restartHooks(c)

	h.passAndSettle(app)
	h.passAndSettle(app)

	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 1)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run %+v", run)
	}
}
