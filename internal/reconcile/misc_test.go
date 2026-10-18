package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

func TestNewChecksOptions(t *testing.T) {
	fake := backendtest.New()
	engine := newFakeEngine()

	st, err := state.Open(t.TempDir()+"/state.json", state.Options{})
	if err != nil {
		t.Fatal(err)
	}

	defer st.Close()

	for name, o := range map[string]Options{
		"no backend":  {Docker: engine, Store: st},
		"no docker":   {Backend: fake, Store: st},
		"no store":    {Backend: fake, Docker: engine},
		"host prefix": {Backend: fake, Docker: engine, Store: st, HostPrefix: "host/"},
		"unclean":     {Backend: fake, Docker: engine, Store: st, HostPrefix: "/host/"},
	} {
		if _, err := New(o); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	r, err := New(Options{Backend: fake, Docker: engine, Store: st, Workers: -1, MinBackoff: -1})
	if err != nil {
		t.Fatal(err)
	}

	if r.workers != DefaultWorkers || r.minBackoff != DefaultMinBackoff || r.labelPrefix != "kopia" {
		t.Errorf("defaults: workers %d, min backoff %v, prefix %q", r.workers, r.minBackoff, r.labelPrefix)
	}
}

// Enqueue takes only absolute clean paths.
func TestEnqueueChecksPaths(t *testing.T) {
	h := newHarness(t)
	h.r.Enqueue("data/app", "/data/app/", "/data/../data/app", app)

	if got := getNow(t, h.r.queue); got != app {
		t.Errorf("got %s", got)
	}

	assertEmpty(t, h.r.queue)

	if !h.logged("not reconciling a path that is not absolute and clean", "data/app") {
		t.Errorf("not logged:\n%s", h.logs)
	}
}

// With a custom label prefix only its labels count.
func TestLabelPrefix(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.LabelPrefix = "backup" })
	h.engine.run("app", "backup.enable=true", "backup.path="+app, "backup.schedule=5 * * * *")
	h.engine.run("other", enable, pathLabel("/data/other"))

	paths, err := h.r.paths(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(paths, ",") != app {
		t.Errorf("paths = %v", paths)
	}

	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["5 * * * *"]}}`)
}

// When the labels set a field inside one the policy holds as a value,
// PlanSource rejects them and the pass plans again with the last applied
// patch.
func TestPlanRejectedFallsBack(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.SetPolicy(h.src(app), `{"scheduling":"weird","retention":{"keepDaily":7}}`)

	res := h.mustPass(app)
	if len(res.problems) != 1 || !backend.IsConfig(res.problems[0]) {
		t.Fatalf("problems = %v", res.problems)
	}

	if n := len(h.fake.CallsTo(backendtest.PlanSource)); n != 2 {
		t.Errorf("%d plans", n)
	}

	h.mustPolicy(app, `{"scheduling":"weird","retention":{"keepDaily":7}}`)
}

// failingStore refuses changes.
type failingStore struct {
	*state.Store
	err error
}

func (s *failingStore) Update(backend.Source, func(*state.Record) error) error { return s.err }

// A store that refuses the intent keeps the plan from being applied; one
// that only fails to write the file does not.
func TestStoreErrors(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	h.r.store = &failingStore{Store: h.store, err: state.ErrClosed}

	res := h.pass(app)
	if res.err == nil || res.retry || h.writes(app) != 0 {
		t.Fatalf("err %v, retry %v, %d writes", res.err, res.retry, h.writes(app))
	}

	if h.metrics.count(app, ReasonState) != 1 {
		t.Error("not counted")
	}

	h.r.store = &failingStore{Store: h.store, err: errors.Join(state.ErrNotPersisted, errors.New("disk full"))}
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A new container with a bad path label is reported from its event, before
// any full reconcile.
func TestEventReportsLabelErrors(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel("data/app"))

	h.r.HandleEvent(h.engine.event(docker.KindChanged, id))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	if h.metrics.count("", ReasonLabels) != 1 || !h.logged("container labels have errors", "not an absolute path") {
		t.Errorf("not reported once:\n%s", h.logs)
	}
}

// The events that queued a pass are logged with what it applied.
func TestPassLogsEvents(t *testing.T) {
	h := newHarness(t)
	passes := h.start()

	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	waitPass(t, passes, app, succeeded)

	if !h.logged("source registered", `"events":["start app"]`) {
		t.Errorf("events not logged:\n%s", h.logs)
	}
}

// Events of a pass that is retried are kept for the retry.
func TestPendingEventsKeptForRetry(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("err %v", res.err)
	}

	h.r.HandleEvent(h.engine.event(docker.KindDied, id))

	if got := eventNames(h.r.takePending(app)); strings.Join(got, ",") != "start app,die app" {
		t.Errorf("pending = %v", got)
	}
}

// When Run's context ends, a running pass gets ShutdownGrace to finish,
// and Run returns only after it did.
func TestShutdownGrace(t *testing.T) {
	for _, tc := range []struct {
		grace   time.Duration
		applied bool
	}{{5 * time.Second, true}, {0, false}} {
		h := newHarness(t, func(o *Options) { o.ShutdownGrace = tc.grace })
		h.engine.run("app", enable, pathLabel(app))

		entered, release := make(chan struct{}), make(chan struct{})

		h.fake.SetHook(func(c backendtest.Call) error {
			if c.Method == backendtest.ApplyPlan {
				close(entered)
				<-release
			}

			return nil
		})

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			h.r.Run(ctx)
			close(done)
		}()

		h.r.Enqueue(app)
		<-entered
		cancel()

		select {
		case <-done:
			t.Fatalf("grace %v: Run returned during a pass", tc.grace)
		case <-time.After(20 * time.Millisecond):
		}

		close(release)
		<-done

		if h.fake.Known(h.src(app)) != tc.applied {
			t.Errorf("grace %v: registered %v", tc.grace, h.fake.Known(h.src(app)))
		}
	}
}

// A crash-looping owner brings a pass every few seconds; an error that does
// not change is reported once, and again at the next full reconcile.
func TestRepeatedErrorReportedOnce(t *testing.T) {
	h := newHarness(t)
	h.engine.create("app", docker.StateRestarting, enable, pathLabel(app))
	h.fake.SetPathMissing(app)

	for range 3 {
		if res := h.pass(app); !backend.IsConfig(res.err) {
			t.Fatalf("err %v", res.err)
		}
	}

	if n := h.metrics.count(app, ReasonConfig); n != 1 {
		t.Errorf("reported %d times", n)
	}

	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	h.pass(app)

	if n := h.metrics.count(app, ReasonConfig); n != 2 {
		t.Errorf("reported %d times after a full reconcile", n)
	}
}

func TestRunOnceWhileRunning(t *testing.T) {
	h := newHarness(t)
	h.start()

	eventually(t, "running", h.r.running.Load)

	if err := h.r.RunOnce(t.Context()); err == nil {
		t.Error("RunOnce along with Run")
	}
}

// A container whose enable label is misspelled gets the hint.
func TestScanReportsMisspelledEnable(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", "kopia.Enable=true", pathLabel(app))

	paths, err := h.r.paths(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) != 0 || !h.logged("container labels have warnings", "did you mean kopia.enable") {
		t.Errorf("paths %v, log:\n%s", paths, h.logs)
	}
}

// A dry run also prints the paths it would not register and the passes
// that fail, and the labels of a paused source that would change.
func TestDryRunPrintsEveryPath(t *testing.T) {
	h := newHarness(t)
	stopped := h.engine.run("stopped", enable, pathLabel("/data/stopped"))
	h.mustPass("/data/stopped")
	h.engine.setState(stopped, docker.StateExited)
	h.mustPass("/data/stopped")

	h.engine.remove(stopped)
	h.engine.create("stopped", docker.StateExited, enable, pathLabel("/data/stopped"), scheduleLabel("30 2 * * *"))
	h.engine.run("bad", enable, pathLabel("/data/bad"), scheduleLabel("0 25 * * *"))
	h.engine.run("web", enable, pathLabel("/data/shared"))
	h.engine.run("worker", enable, pathLabel("/data/shared"), scheduleLabel("30 2 * * *"))
	h.engine.run("down", enable, pathLabel("/data/down"))
	h.fake.RejectCron("0 25 * * *")
	h.fake.FailNextFor(h.src("/data/down"), backendtest.PlanSource, temporary("PlanSource"))

	if err := h.store.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := state.Open(h.statePath, state.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}

	out := &syncBuffer{}

	r, err := New(Options{Backend: h.fake, Docker: h.engine, Store: ro, DryRun: true, Output: out})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.RunOnce(t.Context()); err == nil {
		t.Error("RunOnce: no error")
	}

	for _, want := range []string{
		"root@kopia:/data/bad: not applied: kopia server rejects the schedule",
		"root@kopia:/data/shared: not registered: no container backs it up with labels that can be applied",
		"problem: /data/shared: containers that share the path ask for different policies",
		"root@kopia:/data/down: error: ",
		"root@kopia:/data/stopped: no change",
		`labels: scheduling.cron=["0 * * * *"] -> scheduling.cron=["30 2 * * *"] (applies when a container starts)`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// kopia server refuses a write that keeps the labels as they are, here a
// pause: there is nothing to fall back to, so the policy is written once
// and the error is the source's.
func TestApplyRejectedWithoutNewLabels(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.fake.SetPolicyCheck(func(json.RawMessage) error { return errors.New("refused") })
	h.engine.setState(id, docker.StateExited)
	h.fake.ResetCalls()

	res := h.pass(app)
	if !backend.IsConfig(res.err) || len(res.problems) != 0 {
		t.Errorf("err %v, problems %v", res.err, res.problems)
	}

	if n := h.writes(app); n != 1 {
		t.Errorf("%d writes", n)
	}
}

// A registration kopia server refuses leaves no record in the state: the
// path is not a source the watcher manages.
func TestRefusedRegistrationLeavesNoRecord(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.fake.SetPathMissing(app)

	if res := h.pass(app); !backend.IsConfig(res.err) {
		t.Fatalf("err %v", res.err)
	}

	if _, ok := h.applied(app); ok {
		t.Error("a record is left")
	}

	// The container goes; nothing touches a source the web UI makes later.
	h.engine.remove(id)
	h.fake.SetPolicy(h.src(app), `{"scheduling":{"cron":["0 3 * * *"]}}`)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 3 * * *"]}}`)
}

// A refused registration of a source that has a record (kopia server lost
// it) puts the record back as it was.
func TestRefusedRegistrationRestoresRecord(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	before, _ := h.applied(app)

	if err := h.fake.DeletePolicy(t.Context(), h.src(app)); err != nil {
		t.Fatal(err)
	}

	h.fake.SetPathMissing(app)

	if res := h.pass(app); !backend.IsConfig(res.err) {
		t.Fatalf("err %v", res.err)
	}

	if after, ok := h.applied(app); !ok || !after.Equal(before) {
		t.Errorf("record = %+v, %v; want %+v", after, ok, before)
	}
}

// A problem that goes away is logged as resolved.
func TestProblemResolved(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app))
	h.mustPass(app)

	worker := h.engine.run("worker", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.mustPass(app)
	h.engine.remove(worker)
	h.mustPass(app)

	if !h.logged("the problems of the path are resolved", app) {
		t.Errorf("not logged:\n%s", h.logs)
	}
}

// Sources of another identity in the state are reported when the identity
// is first known.
func TestStateOfAnotherIdentity(t *testing.T) {
	h := newHarness(t)
	old := backend.Source{Host: "kopia-old", UserName: "root", Path: app}

	if err := h.store.Update(old, func(*state.Record) error { return nil }); err != nil {
		t.Fatal(err)
	}

	h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)
	h.mustPass(app)

	if !h.logged("the state has sources of another kopia server identity", "root@kopia-old") {
		t.Errorf("not logged:\n%s", h.logs)
	}

	if strings.Count(h.logs.String(), "the state has sources of another") != 1 {
		t.Error("logged more than once")
	}
}

// A full reconcile leaves a path that waits for a retry to its retry.
func TestScanKeepsRetryDelay(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.MinBackoff, o.MaxBackoff = time.Hour, time.Hour })
	h.engine.run("app", enable, pathLabel(app))
	h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))

	passes := h.start()
	h.r.Enqueue(app)
	waitPass(t, passes, app, func(res passResult) bool { return res.retry })

	h.r.ReconcileAll()
	h.engine.run("db", enable, pathLabel("/data/db"))
	h.r.ReconcileAll()
	waitPass(t, passes, "/data/db", succeeded)

	if n := len(h.fake.CallsFor(h.src(app), backendtest.PlanSource)); n != 1 {
		t.Errorf("%d passes over a path that waits for its retry", n)
	}
}
