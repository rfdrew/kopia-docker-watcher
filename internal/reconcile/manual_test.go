package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// mustTrigger asks for a snapshot of target, which must make one trigger.
func (h *harness) mustTrigger(target string, force bool) *Trigger {
	h.t.Helper()

	ts, err := h.r.Trigger(context.Background(), target, force)
	if err != nil {
		h.t.Fatalf("Trigger(%q): %v", target, err)
	}

	if len(ts) != 1 {
		h.t.Fatalf("Trigger(%q) made %d triggers", target, len(ts))
	}

	return ts[0]
}

// mustResult returns the result of a trigger that has ended.
func (h *harness) mustResult(t *Trigger) TriggerResult {
	h.t.Helper()

	select {
	case <-t.Done():
		return t.Result()
	case <-time.After(5 * time.Second):
		h.t.Fatalf("the trigger of %s did not end\nlog:\n%s", t.Path(), h.logs)
		return TriggerResult{}
	}
}

// mustOpen checks that a trigger has not ended.
func (h *harness) mustOpen(t *Trigger) {
	h.t.Helper()

	select {
	case <-t.Done():
		h.t.Fatalf("the trigger of %s ended: %+v\nlog:\n%s", t.Path(), t.Result(), h.logs)
	default:
	}
}

// A manual trigger takes a snapshot through the next pass and ends with
// its result; the snapshot counts for the minimum interval like one at a
// start.
func TestTriggerTakesSnapshot(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	tr := h.mustTrigger("app", false)
	h.mustOpen(tr)
	h.passAndSettle(app)

	res := h.mustResult(tr)
	if !res.OK() || res.Outcome != string(backend.SnapshotSucceeded) || res.Path != app || res.TaskID == "" ||
		res.Source != h.src(app).String() || res.Hooks {
		t.Fatalf("result = %+v", res)
	}

	h.mustTriggers(app, 1)

	if !h.logged("snapshot queued", "manual trigger of app") {
		t.Fatalf("reason not logged:\n%s", h.logs)
	}

	if rec := h.record(app); rec.LastTriggered.IsZero() {
		t.Fatalf("LastTriggered not set: %+v", rec)
	}
}

// The minimum interval holds a manual trigger back, unless it is forced.
func TestTriggerMinInterval(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart, "kopia.min-interval=1h")
	h.passAndSettle(app)

	first := h.mustTrigger("app", false)
	h.passAndSettle(app)
	h.mustResult(first)

	c.add(10 * time.Minute)

	second := h.mustTrigger(app, false)
	h.passAndSettle(app)

	res := h.mustResult(second)
	if res.Outcome != TriggerSkipped || !strings.Contains(res.Message, "minimum interval") ||
		!strings.Contains(res.Message, "--force") || res.OK() {
		t.Fatalf("result = %+v", res)
	}

	h.mustTriggers(app, 1)

	forced := h.mustTrigger(app, true)
	h.passAndSettle(app)

	if res := h.mustResult(forced); !res.OK() {
		t.Fatalf("forced result = %+v", res)
	}

	h.mustTriggers(app, 2)
}

// A snapshot that is uploading counts: the trigger takes none.
func TestTriggerWhileUploading(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)
	h.fake.SetStatus(h.src(app), backend.SourceStatus{Status: backend.StatusUploading, CurrentTask: "7"})

	tr := h.mustTrigger("app", true)
	h.passAndSettle(app)

	if res := h.mustResult(tr); res.Outcome != TriggerSkipped || !strings.Contains(res.Message, "uploading already") {
		t.Fatalf("result = %+v", res)
	}

	h.mustTriggers(app, 0)
}

// A snapshot that fails ends the trigger with the failure.
func TestTriggerSnapshotFails(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)
	h.fake.QueueSnapshotResult(h.src(app), backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})

	tr := h.mustTrigger("app", false)
	h.passAndSettle(app)

	if res := h.mustResult(tr); res.OK() || res.Outcome != string(backend.SnapshotFailed) || res.Message != "disk full" {
		t.Fatalf("result = %+v", res)
	}
}

// A trigger that comes before the source is registered waits through the
// pass that writes the policy and is taken by the pass after it.
func TestTriggerAfterPolicyWrite(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)

	tr := h.mustTrigger("app", false)
	res := h.passAndSettle(app)

	if !res.recheck {
		t.Fatalf("no recheck after the policy write: %+v", res)
	}

	h.mustOpen(tr)
	h.mustTriggers(app, 0)

	h.passAndSettle(app)

	if res := h.mustResult(tr); !res.OK() {
		t.Fatalf("result = %+v", res)
	}
}

// A temporary error of kopia server keeps the trigger for the retry; one
// that is not retried ends it.
func TestTriggerErrors(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	tr := h.mustTrigger("app", false)
	h.fake.FailNext(backendtest.TriggerSnapshot, &backend.Error{Op: "TriggerSnapshot", Kind: backend.ErrTemporary,
		Err: errors.New("503")})

	if res := h.pass(app); !res.retry {
		t.Fatalf("pass = %+v", res)
	}

	h.mustOpen(tr)
	h.passAndSettle(app)

	if res := h.mustResult(tr); !res.OK() {
		t.Fatalf("result = %+v", res)
	}

	tr = h.mustTrigger("app", true)
	h.fake.FailNext(backendtest.TriggerSnapshot, &backend.Error{Op: "TriggerSnapshot", Kind: backend.ErrConfig,
		Err: errors.New("path not found")})

	if res := h.pass(app); res.err == nil || res.retry {
		t.Fatalf("pass = %+v", res)
	}

	if res := h.mustResult(tr); res.Outcome != TriggerError || !strings.Contains(res.Message, "path not found") {
		t.Fatalf("result = %+v", res)
	}
}

// A trigger names a container by name, with or without the slash, or by
// the start of its ID, or a path: a source path, or a label path under
// HOST_PREFIX. A container with several paths triggers each of them.
func TestTriggerTargets(t *testing.T) {
	const (
		prefix = "/host"
		data   = "/data/app"
		other  = "/data/other"
	)

	h := newHarness(t, withHostPrefix(prefix))
	withClock(h)
	withHooks(h)

	id := h.engine.run("app", enable, "kopia.path="+data+","+other, noOnStart)
	h.engine.run("off", pathLabel(data))
	h.engine.run("nopath", enable)

	paths := func(target string) []string {
		t.Helper()

		ts, err := h.r.Trigger(context.Background(), target, false)
		if err != nil {
			t.Fatalf("Trigger(%q): %v", target, err)
		}

		var out []string
		for _, tr := range ts {
			out = append(out, tr.Path())
		}

		return out
	}

	h.engine.add(docker.Container{ID: "abcdef0123456789", Name: "long", State: docker.StateRunning,
		Labels: map[string]string{"kopia.enable": "true", "kopia.path": data}})
	h.engine.add(docker.Container{ID: "abcdef9876543210", Name: "long2", State: docker.StateRunning})

	if got := paths("abcdef01"); len(got) != 1 || got[0] != prefix+data {
		t.Errorf("Trigger by ID prefix = %q", got)
	}

	if _, err := h.r.Trigger(context.Background(), "abcdef", false); err == nil || !strings.Contains(err.Error(), "2 containers") {
		t.Errorf("ambiguous prefix: %v", err)
	}

	want := []string{prefix + data, prefix + other}

	for _, target := range []string{"app", "/app", id} {
		if got := paths(target); strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("Trigger(%q) = %q, want %q", target, got, want)
		}
	}

	for _, target := range []string{prefix + data, data} {
		if got := paths(target); len(got) != 1 || got[0] != prefix+data {
			t.Errorf("Trigger(%q) = %q", target, got)
		}
	}

	for target, msg := range map[string]string{
		"nope":       `no container named "nope"`,
		"off":        "does not enable the backup",
		"nopath":     "backs up no path",
		"/data/none": "no container backs up /data/none",
		"":           "no container or path given",
	} {
		if _, err := h.r.Trigger(context.Background(), target, false); err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("Trigger(%q) = %v, want %q", target, err, msg)
		}
	}
}

// A container that no longer backs up the path ends its trigger without a
// snapshot.
func TestTriggerContainerGone(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.engine.run("app", enable, pathLabel(app), noOnStart)
	h.engine.run("db", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	tr := h.mustTrigger("app", false)
	h.engine.remove(id)
	h.passAndSettle(app)

	if res := h.mustResult(tr); res.Outcome != TriggerSkipped || !strings.Contains(res.Message, "no longer backs up") {
		t.Fatalf("result = %+v", res)
	}
}

// Without Run, or once it stops, the reconciler takes no trigger.
func TestTriggerNotRunning(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	if _, err := h.r.Trigger(context.Background(), "app", false); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("err = %v", err)
	}

	if _, err := h.r.Sources(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Sources err = %v", err)
	}
}

// A trigger of a source with hooks is a snapshot with hooks: the result
// comes once the post-exec hooks have run.
func TestTriggerWithHooks(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)
	h.passAndSettle(app)

	tr := h.mustTrigger("db", false)
	h.passAndSettle(app)
	h.mustExecs(preRun)
	h.mustOpen(tr)

	h.runHooked(app, 3)
	h.mustExecs(preRun, postRun)

	res := h.mustResult(tr)
	if !res.OK() || !res.Hooks || res.Outcome != string(backend.SnapshotSucceeded) || res.TaskID == "" {
		t.Fatalf("result = %+v", res)
	}
}

// A pre-exec hook that fails ends the trigger with its error, once the
// post-exec hooks have run; a post-exec hook that fails is reported too.
func TestTriggerHooksFail(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)
	h.engine.setExec(func(_ context.Context, _ docker.Container, cmd []string) (docker.ExecResult, error) {
		return docker.ExecResult{ExitCode: 1, Output: []byte(cmd[0] + " failed\n")}, nil
	})
	h.passAndSettle(app)

	tr := h.mustTrigger(app, true)
	h.runHooked(app, 3)

	res := h.mustResult(tr)
	if res.OK() || res.Outcome != TriggerError || !strings.Contains(res.Message, "a pre-exec hook failed") ||
		!strings.Contains(res.PostError, "exited with code 1") {
		t.Fatalf("result = %+v", res)
	}

	h.mustTriggers(app, 0)
}

// When the watcher stops, the triggers it still follows end.
func TestTriggerEndsAtStop(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app), noOnStart)

	passes := make(chan passEvent, 64)
	h.r.afterPass = func(p string, res passResult) { passes <- passEvent{path: p, res: res} }

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
	waitPass(t, passes, app, succeeded)
	h.fake.HoldSnapshots(h.src(app))

	tr := h.mustTrigger("app", false)

	eventually(t, "the snapshot is queued", func() bool { return h.triggers(app) == 1 })
	h.mustOpen(tr)

	cancel()
	<-done

	if res := h.mustResult(tr); res.Outcome != string(backend.SnapshotUnknown) || !strings.Contains(res.Message, "stopped") {
		t.Fatalf("result = %+v", res)
	}

	if _, err := h.r.Trigger(context.Background(), "app", false); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("Trigger after the stop: %v", err)
	}
}

// Sources reports what the passes found, the state and kopia server's
// status; a path whose pass has a problem is listed with it.
func TestSources(t *testing.T) {
	const (
		paused = "/data/paused"
		hooked = "/data/hooked"
		broken = "/data/broken"
	)

	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("app", enable, pathLabel(app), noOnStart)
	old := h.engine.run("old", enable, pathLabel(paused), noOnStart)
	h.engine.run("db", enable, pathLabel(hooked), noOnStart, preExec)
	h.engine.run("bad1", enable, pathLabel(broken), "kopia.schedule=1h")
	h.engine.run("bad2", enable, pathLabel(broken), "kopia.schedule=2h")

	for _, p := range []string{app, paused, hooked, broken} {
		h.passAndSettle(p)
	}

	h.engine.setState(old, docker.StateExited)
	h.passAndSettle(paused)

	h.fake.AddSnapshot(h.src(app), backend.SnapshotInfo{StartTime: t0.Add(-time.Hour), EndTime: t0})
	h.passAndSettle(app)

	rep, err := h.r.Sources(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if rep.Warning != "" {
		t.Errorf("warning %q", rep.Warning)
	}

	got := map[string]SourceReport{}
	for _, s := range rep.Sources {
		got[s.Path] = s
	}

	if len(rep.Sources) != 4 {
		t.Fatalf("sources = %+v", rep.Sources)
	}

	if s := got[app]; s.Paused || s.Schedule != ScheduleKopia || strings.Join(s.Owners, ",") != "app" ||
		!s.LastSuccess.Equal(t0.Add(-time.Hour)) || s.Error != "" || s.Source != h.src(app).String() || s.Status == "" {
		t.Errorf("app = %+v", s)
	}

	if s := got[paused]; !s.Paused || strings.Join(s.Owners, ",") != "old" {
		t.Errorf("paused = %+v", s)
	}

	if s := got[hooked]; s.Schedule != ScheduleWatcher || !s.NextSnapshot.Equal(t0.Add(time.Hour)) {
		t.Errorf("hooked = %+v", s)
	}

	if s := got[broken]; s.Error == "" {
		t.Errorf("broken = %+v", s)
	}

	// kopia server does not answer: the rest is reported.
	h.fake.FailNext(backendtest.ListSources, &backend.Error{Op: "ListSources", Kind: backend.ErrUnavailable,
		Err: errors.New("connection refused")})

	rep, err = h.r.Sources(context.Background())
	if err != nil || !strings.Contains(rep.Warning, "connection refused") || len(rep.Sources) != 4 {
		t.Fatalf("Sources = %+v, %v", rep, err)
	}
}

// StopOwnersFor stops the running owners with the containers that depend
// on them, runs the restore, and starts them again in the reverse order.
// Their events are the watcher's own, and while they are down the source
// is not paused and takes no snapshot.
func TestStopOwnersFor(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	db := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop)
	web := h.engine.run("web", "kopia.depends-on=db")
	h.passAndSettle(app)

	var progress []string

	err := h.r.StopOwnersFor(context.Background(), h.src(app), func(s string) { progress = append(progress, s) },
		func(ctx context.Context) error {
			h.mustState(db, docker.StateExited)
			h.mustState(web, docker.StateExited)

			if held := h.store.Held(); len(held) != 2 || !strings.HasPrefix(held[0].Run, restoreRunPrefix) {
				t.Errorf("held = %+v", held)
			}

			h.dockerEvent(db, "die")
			h.dockerEvent(db, "stop")

			if evs := h.pendingFor(app); len(evs) != 0 {
				t.Errorf("own events kept: %+v", evs)
			}

			// A trigger waits for the restore. The restore counts as a
			// goroutine that runs hooks, so the pass is not settled.
			tr := h.mustTrigger("db", true)
			h.mustPass(app)
			h.mustOpen(tr)
			h.mustTriggers(app, 0)

			if !h.logged("the snapshot waits for the restore of the source under way") {
				t.Errorf("wait not logged:\n%s", h.logs)
			}

			if rec := h.record(app); rec.SavedScheduling != nil {
				t.Errorf("paused while held: %+v", rec)
			}

			return ctx.Err()
		})
	if err != nil {
		t.Fatal(err)
	}

	h.mustActions("stop web", "stop db", "start db", "start web")
	h.mustState(db, docker.StateRunning)
	h.mustState(web, docker.StateRunning)
	h.mustNotHeld()

	if strings.Join(progress, "|") != "stopping web (stop), db (stop)|starting the stopped containers again" {
		t.Errorf("progress = %q", progress)
	}

	// The trigger kept during the restore is taken now.
	h.passAndSettle(app)
	h.mustTriggers(app, 1)
}

// StopOwnersFor refuses while a snapshot with hooks of the source is under
// way, while a container it needs is held for a snapshot, and while
// another restore of the source runs; it stops nothing then.
func TestStopOwnersForBusy(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	noop := func(context.Context) error { return nil }

	if err := h.store.Update(h.src(app), func(r *state.Record) error {
		r.HookRun = &state.HookRun{ID: "x", Phase: state.HookPre}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := h.r.StopOwnersFor(context.Background(), h.src(app), func(string) {}, noop); err == nil ||
		!strings.Contains(err.Error(), "snapshot with hooks") {
		t.Fatalf("err = %v", err)
	}

	if err := h.store.Update(h.src(app), func(r *state.Record) error {
		r.HookRun = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	err := h.r.StopOwnersFor(context.Background(), h.src(app), func(string) {}, func(context.Context) error {
		err := h.r.StopOwnersFor(context.Background(), h.src(app), func(string) {}, noop)
		if err == nil || !strings.Contains(err.Error(), "another restore") {
			t.Errorf("nested err = %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	h.mustActions("stop db", "start db")
}

// The containers come back when the watcher stops during the restore: its
// context ends.
func TestStopOwnersForWatcherStops(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	db := h.engine.run("db", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	runCtx, stop := context.WithCancel(context.Background())
	h.r.mu.Lock()
	h.r.runCtx = runCtx
	h.r.mu.Unlock()

	err := h.r.StopOwnersFor(context.Background(), h.src(app), func(string) {}, func(ctx context.Context) error {
		h.mustState(db, docker.StateExited)
		stop()
		<-ctx.Done()

		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}

	h.mustState(db, docker.StateRunning)
	h.mustNotHeld()
}
