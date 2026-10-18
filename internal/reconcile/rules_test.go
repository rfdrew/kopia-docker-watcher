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
)

// The retry delay doubles with every temporary error in a row. Only lower
// bounds are checked, so a slow machine cannot fail the test.
func TestRetryDelayGrows(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.MinBackoff, o.MaxBackoff = 30*time.Millisecond, time.Second })
	h.engine.run("app", enable, pathLabel(app))

	for range 3 {
		h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))
	}

	passes := h.start()
	h.r.Enqueue(app)

	var at []time.Time

	for range 4 {
		waitPass(t, passes, app, nil)
		at = append(at, time.Now())
	}

	// 30, 60 and 120 ms.
	if d := at[3].Sub(at[2]); d < 110*time.Millisecond {
		t.Errorf("third retry after %v", d)
	}
}

// A failing listing of a full reconcile is retried after a growing delay.
func TestScanRetryDelayGrows(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.MinBackoff, o.MaxBackoff = 40*time.Millisecond, time.Second })
	h.engine.run("app", enable, pathLabel(app))

	for range 3 {
		h.engine.failNext(errors.New("cannot connect to the Docker daemon"))
	}

	passes := h.start()
	start := time.Now()
	h.r.ReconcileAll()
	waitPass(t, passes, app, succeeded)

	// 40+80+160 ms with a growing delay, 120 ms without.
	if d := time.Since(start); d < 220*time.Millisecond {
		t.Errorf("first pass after %v", d)
	}
}

// A MaxBackoff below MinBackoff is raised to it.
func TestMaxBackoffRaised(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.MinBackoff, o.MaxBackoff = time.Hour, time.Millisecond })

	if d := backoff(h.r.minBackoff, h.r.maxBackoff, 1); d != time.Hour {
		t.Errorf("first retry after %v", d)
	}
}

// A restarting owner counts as running for the policy too: it decides over
// a leftover stopped container.
func TestRestartingOwnerDecidesPolicy(t *testing.T) {
	h := newHarness(t)
	h.engine.create("app", docker.StateRestarting, enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.engine.create("app-run-1", docker.StateExited, enable, pathLabel(app))

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)
}

// After a full reconcile, as in production, where KindResync comes first,
// an error that repeats is reported once.
func TestRepeatedErrorAfterScanReportedOnce(t *testing.T) {
	h := newHarness(t)
	h.engine.create("app", docker.StateRestarting, enable, pathLabel(app))
	h.fake.SetPathMissing(app)

	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		h.pass(app)
	}

	if n := h.metrics.count(app, ReasonConfig); n != 1 {
		t.Errorf("reported %d times", n)
	}
}

// A temporary error between two passes that find the same configuration
// error does not make it reported again.
func TestTemporaryErrorKeepsReported(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.SetPathMissing(app)

	h.pass(app)
	h.fake.FailNext(backendtest.PlanSource, temporary("PlanSource"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("err %v", res.err)
	}

	h.pass(app)

	if n := h.metrics.count(app, ReasonConfig); n != 1 {
		t.Errorf("reported %d times", n)
	}
}

// At most maxPending events are kept per path, the latest ones.
func TestPendingEventsBounded(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))

	for i := range 40 {
		ev := h.engine.event(docker.KindStarted, id)
		ev.Name = fmt.Sprint(i)
		h.r.HandleEvent(ev)
	}

	got := h.r.takePending(app)
	if len(got) != maxPending || got[0].Name != "8" || got[len(got)-1].Name != "39" {
		t.Errorf("%d pending, first %s, last %s", len(got), got[0].Name, got[len(got)-1].Name)
	}
}

// Events put back for a retry come before those that came during the
// pass.
func TestRestoredEventsFirst(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	die := h.engine.event(docker.KindDied, id)
	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method == backendtest.PlanSource {
			h.r.HandleEvent(die)
			return temporary("PlanSource")
		}

		return nil
	})

	if res := h.pass(app); !res.retry {
		t.Fatalf("err %v", res.err)
	}

	if got := eventNames(h.r.takePending(app)); strings.Join(got, ",") != "start app,die app" {
		t.Errorf("pending = %v", got)
	}
}

// A pass that ends without an error uses up its events.
func TestEventsUsedUp(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.mustPass(app)

	if got := h.r.takePending(app); len(got) != 0 {
		t.Errorf("pending after a pass = %v", eventNames(got))
	}
}

// A path named twice in one label gets each event once.
func TestPathOnceInLabel(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app+","+app+"/"))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	if got := h.r.takePending(app); len(got) != 1 {
		t.Errorf("pending = %v", eventNames(got))
	}
}

// A container whose labels are fine is not reported.
func TestValidLabelsNotReported(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	if h.logged("container labels have") {
		t.Errorf("valid labels reported:\n%s", h.logs)
	}
}

// A container that does not enable the backup has no path errors; its
// other problems are warnings.
func TestDisabledBadPathNotAnError(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", "kopia.enable=false", pathLabel("data/app"), "kopia.typo=1")

	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	if h.metrics.count("", ReasonLabels) != 0 || h.logged("container labels have errors") {
		t.Errorf("reported as errors:\n%s", h.logs)
	}
}

// The destroy event of a container never seen reports nothing.
func TestDestroyNotReported(t *testing.T) {
	h := newHarness(t)
	h.r.HandleEvent(docker.Event{Kind: docker.KindRemoved, Action: "destroy", ContainerID: "gone", Name: "gone",
		Labels: labelMap(enable, pathLabel("data/app"))})

	if h.metrics.count("", ReasonLabels) != 0 {
		t.Errorf("labels of a removed container reported:\n%s", h.logs)
	}
}

// reversedDocker lists containers newest first, as the Docker API does.
type reversedDocker struct{ *fakeEngine }

func (d reversedDocker) Containers(ctx context.Context) ([]docker.Container, error) {
	cs, err := d.fakeEngine.Containers(ctx)
	slices.Reverse(cs)

	return cs, err
}

// Owner names are sorted whatever order Docker lists the containers in.
func TestOwnersSorted(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app))
	h.engine.run("worker", enable, pathLabel(app))
	h.r.docker = reversedDocker{h.engine}

	h.mustPass(app)

	plans := h.fake.CallsTo(backendtest.PlanSource)
	if got := plans[0].Desired.Owners; !slices.Equal(got, []string{"web", "worker"}) {
		t.Errorf("owners = %v", got)
	}
}

// Passes over different paths run at the same time.
func TestPassesRunConcurrently(t *testing.T) {
	h := newHarness(t)
	h.engine.run("a", enable, pathLabel("/data/a"))
	h.engine.run("b", enable, pathLabel("/data/b"))

	enteredB := make(chan struct{})

	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method != backendtest.ApplyPlan {
			return nil
		}

		switch c.Source.Path {
		case "/data/b":
			close(enteredB)
		case "/data/a":
			select {
			case <-enteredB:
			case <-time.After(2 * time.Second):
				return temporary("ApplyPlan: b never ran alongside")
			}
		}

		return nil
	})

	passes := h.start()
	h.r.Enqueue("/data/a", "/data/b")

	if res := waitPass(t, passes, "/data/a", nil); res.err != nil {
		t.Errorf("a: %v", res.err)
	}
}

// An event's paths are reconciled EventDelay after it.
func TestEventDelay(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.EventDelay = 300 * time.Millisecond })
	id := h.engine.run("app", enable, pathLabel(app))
	passes := h.start()

	start := time.Now()
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	waitPass(t, passes, app, nil)

	if d := time.Since(start); d < 250*time.Millisecond {
		t.Errorf("pass %v after the event", d)
	}
}

// New labels of a paused source are stored, although nothing is written
// to kopia server, and its unpause uses them.
func TestPausedNewLabelsStored(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)
	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)

	h.engine.remove(id)
	id = h.engine.create("app", docker.StateExited, enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.fake.ResetCalls()
	h.mustPass(app)

	if n := h.writes(app); n != 0 {
		t.Errorf("%d writes for a paused source", n)
	}

	a, _ := h.applied(app)
	if !a.Patch.Equal(cronPatch("30 2 * * *")) {
		t.Errorf("applied patch = %+v", a.Patch)
	}

	assertJSON(t, "saved", a.SavedScheduling, `{"cron":["30 2 * * *"]}`)

	h.engine.setState(id, docker.StateRunning)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)
}

// A temporary error of the schedule check is retried; it does not fall
// back to the last applied patch.
func TestPreviewTemporaryRetried(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.fake.FailNext(backendtest.PreviewSchedule, temporary("PreviewSchedule"))

	res := h.pass(app)
	if !res.retry || len(res.problems) != 0 || h.metrics.count(app, ReasonConfig) != 0 {
		t.Errorf("err %v, retry %v, problems %v", res.err, res.retry, res.problems)
	}

	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)
}

// A stale pass does not forget an identity another pass has refreshed.
func TestForgetIdentityOnlyTheOneUsed(t *testing.T) {
	h := newHarness(t)
	h.r.identity = identity{userName: "root", host: "kopia-2", known: true}
	h.r.forgetIdentity(backend.Source{UserName: "root", Host: "kopia", Path: app})

	if !h.r.identity.known {
		t.Error("fresh identity forgotten by a stale pass")
	}
}

// An incomplete identity is not kept; the next pass asks again.
func TestIncompleteIdentityNotKept(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.SetIdentity("root", "")

	if res := h.pass(app); res.err == nil {
		t.Fatal("no error with an incomplete identity")
	}

	h.fake.SetIdentity("root", "kopia")
	h.mustPass(app)
}

// A dry run prints the next snapshot times.
func TestDryRunNextSnapshots(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.DryRun = true })
	h.fake.SetUpcoming(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	h.engine.run("app", enable, pathLabel(app))
	h.pass(app)

	if !strings.Contains(h.out.String(), "next snapshots: 2026-01-01 01:00:00") {
		t.Errorf("output:\n%s", h.out)
	}
}

// The log of a paused source has no next snapshot.
func TestPausedNoNextSnapshot(t *testing.T) {
	h := newHarness(t)
	h.fake.SetUpcoming(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC))
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	if !h.logged("source registered", "next_snapshot") {
		t.Errorf("no next snapshot logged:\n%s", h.logs)
	}

	h.engine.remove(id)
	h.engine.create("app", docker.StateExited, enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.mustPass(app)

	if h.logged("source policy updated", `"paused":true`, "next_snapshot") {
		t.Errorf("next snapshot of a paused source logged:\n%s", h.logs)
	}
}

// A dry run notes an apply that was not confirmed.
func TestDryRunNotesPending(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.FailNextAfter(backendtest.ApplyPlan, temporary("ApplyPlan"))

	if res := h.pass(app); !res.retry {
		t.Fatalf("err %v", res.err)
	}

	h.r.dryRun = true
	h.pass(app)

	if !strings.Contains(h.out.String(), "note: the last apply was not confirmed") {
		t.Errorf("output:\n%s", h.out)
	}
}

// A dry run says nothing about a path nobody backs up.
func TestDryRunQuietForUnmanagedPath(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.DryRun = true })
	h.pass("/data/nobody")

	if out := h.out.String(); out != "" {
		t.Errorf("output:\n%s", out)
	}
}

// RunOnce can run again after it returned, and so can Run.
func TestRunOnceTwice(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	for i := range 2 {
		if err := h.r.RunOnce(t.Context()); err != nil {
			t.Fatalf("RunOnce %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	h.r.Run(ctx)
}

// After kopia server refuses a write, the fallback's intent is stored
// before the fallback is applied: when that write takes effect and its
// answer is lost, the state keeps the scheduling section the fallback's
// pause replaced, not the refused one's.
func TestFallbackIntentStored(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	// Recreated stopped with a cron that only the write refuses.
	h.engine.remove(id)
	h.engine.create("app", docker.StateExited, enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	h.fake.SetHook(func(c backendtest.Call) error {
		if c.Method == backendtest.ApplyPlan && c.Plan.Applied.Patch.Equal(cronPatch("30 2 * * *")) {
			return &backend.Error{Op: "ApplyPlan", Kind: backend.ErrConfig, Err: errors.New("failed to validate policy")}
		}

		return nil
	})
	h.fake.FailNextAfter(backendtest.ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrUnavailable, Err: errors.New("timeout")})

	if res := h.pass(app); !res.retry || len(res.problems) != 1 {
		t.Fatalf("err %v, problems %v", res.err, res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	a, _ := h.applied(app)
	assertJSON(t, "saved", a.SavedScheduling, `{"cron":["0 * * * *"]}`)

	if a.Pending == nil {
		t.Error("the intent is not pending")
	}
}
