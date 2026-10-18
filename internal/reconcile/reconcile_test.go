package reconcile

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	enable = "kopia.enable=true"
	app    = "/data/app"
)

func pathLabel(p string) string { return "kopia.path=" + p }

func scheduleLabel(s string) string { return "kopia.schedule=" + s }

// A new container registers its source with the default schedule; the next
// pass changes nothing.
func TestRegister(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))

	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if len(h.fake.CallsTo(backendtest.PreviewSchedule)) != 1 {
		t.Errorf("schedule checked %d times", len(h.fake.CallsTo(backendtest.PreviewSchedule)))
	}

	a, ok := h.applied(app)
	if !ok || a.Pending != nil || a.StoredHash == "" || !a.Patch.Equal(cronPatch("0 * * * *")) {
		t.Errorf("applied = %+v, %v", a, ok)
	}

	h.fake.ResetCalls()
	h.mustPass(app)

	if n := h.writes(app); n != 0 {
		t.Errorf("%d writes on a settled source", n)
	}

	if len(h.fake.CallsTo(backendtest.PreviewSchedule)) != 0 {
		t.Error("schedule checked again without a change")
	}

	if !h.logged("source registered", app) {
		t.Errorf("registration not logged:\n%s", h.logs)
	}
}

func cronPatch(crons ...string) backend.PolicyPatch {
	return backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: crons}}
}

// Two containers share a path and one of them is stopped: the source runs
// on the running one's behalf; it is paused when both are stopped and
// unpaused when either runs again.
func TestSharedPathOneStopped(t *testing.T) {
	h := newHarness(t)
	web := h.engine.run("web", enable, pathLabel(app))
	worker := h.engine.create("worker", docker.StateExited, enable, pathLabel(app))

	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	plans := h.fake.CallsTo(backendtest.PlanSource)
	if got := plans[len(plans)-1].Desired; got.Paused || !slices.Equal(got.Owners, []string{"web", "worker"}) {
		t.Errorf("desired = paused %v, owners %v", got.Paused, got.Owners)
	}

	h.engine.setState(web, docker.StateExited)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	h.engine.setState(worker, docker.StateRunning)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A crash-looping owner is restarting between its runs: the source is not
// paused, and the pass asks to look again.
func TestCrashLoopDoesNotPause(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	writes := h.writes(app)

	for _, st := range []docker.State{docker.StateRestarting, docker.StateRunning, docker.StateRestarting} {
		h.engine.setState(id, st)

		res := h.mustPass(app)
		if res.restarting != (st == docker.StateRestarting) {
			t.Errorf("%s: restarting = %v", st, res.restarting)
		}
	}

	if n := h.writes(app) - writes; n != 0 {
		t.Errorf("%d writes during a crash loop", n)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A restarting owner that ends up exited without any event is found by the
// recheck, and the source is paused.
func TestRestartingExitsWithoutEvent(t *testing.T) {
	h := newHarness(t)
	id := h.engine.create("app", docker.StateRestarting, enable, pathLabel(app))
	passes := h.start()

	h.r.Enqueue(app)
	waitPass(t, passes, app, func(res passResult) bool { return res.err == nil && res.restarting })
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	// No event: Docker gave up restarting.
	h.engine.setState(id, docker.StateExited)
	waitPass(t, passes, app, func(res passResult) bool { return res.err == nil && !res.restarting })
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
}

// A container recreated with a new schedule changes the policy; one
// recreated with a new path pauses the old source and registers the new
// one.
func TestRecreateWithNewLabels(t *testing.T) {
	h := newHarness(t)
	passes := h.start()

	id := h.engine.run("app", enable, pathLabel(app))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	waitPass(t, passes, app, succeeded)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	// docker compose up with a new schedule: the old container goes, a new
	// one comes.
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.r.HandleEvent(h.engine.remove(id))
	id = h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.r.HandleEvent(h.engine.event(docker.KindChanged, id))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	eventually(t, "new schedule applied", func() bool {
		p, _ := h.fake.Policy(h.src(app))
		return bytes.Contains(p, []byte(`30 2 * * *`))
	})
	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)

	// Recreated with another path.
	h.r.HandleEvent(h.engine.remove(id))
	id = h.engine.run("app", enable, pathLabel("/data/app2"), scheduleLabel("30 2 * * *"))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	eventually(t, "new path registered and old one paused", func() bool {
		old, _ := h.fake.Policy(h.src(app))
		return h.fake.Known(h.src("/data/app2")) && bytes.Contains(old, []byte(`"manual":true`))
	})
	h.mustPolicy("/data/app2", `{"scheduling":{"cron":["30 2 * * *"]}}`)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
}

// Emptying the schedule label hands the schedule back to inheritance: the
// cron the watcher set goes, and fields set in the web UI stay. Only the
// last applied patch tells the two apart.
func TestEmptiedLabelRemovesField(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.mustPass(app)

	h.fake.SetPolicy(h.src(app), `{"scheduling":{"cron":["30 2 * * *"],"runMissed":true},"retention":{"keepDaily":7}}`)
	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), scheduleLabel(""))

	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"runMissed":true},"retention":{"keepDaily":7}}`)

	if a, _ := h.applied(app); !a.Patch.IsZero() {
		t.Errorf("applied patch = %+v", a.Patch)
	}
}

// A dry run prints the plan and writes nothing: no ApplyPlan, no state.
func TestDryRunWritesNothing(t *testing.T) {
	// An earlier run registered the source.
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	// Now the labels change and a second container comes.
	h.engine.remove(id)
	h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.engine.run("db", enable, pathLabel("/data/db"))

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

	store := &countingStore{Store: ro}
	out := &syncBuffer{}

	r, err := New(Options{Backend: h.fake, Docker: h.engine, Store: store, DryRun: true, Output: out})
	if err != nil {
		t.Fatal(err)
	}

	h.fake.ResetCalls()

	if err := r.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if n := len(h.fake.CallsTo(backendtest.ApplyPlan)); n != 0 {
		t.Errorf("%d ApplyPlan calls", n)
	}

	if n := store.updates.Load(); n != 0 {
		t.Errorf("%d state updates", n)
	}

	after, err := os.ReadFile(h.statePath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(before, after) {
		t.Errorf("state file changed:\n%s\n->\n%s", before, after)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
	h.mustNoSource("/data/db")

	for _, want := range []string{
		"root@kopia:/data/app: would update the policy",
		`change: scheduling.cron: ["0 * * * *"] -> ["30 2 * * *"]`,
		"root@kopia:/data/db: would register the source",
		"owners: db (running)",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// countingStore counts the changes asked of a store.
type countingStore struct {
	*state.Store
	updates atomic.Int32
}

func (s *countingStore) Update(src backend.Source, fn func(*state.Record) error) error {
	s.updates.Add(1)
	return s.Store.Update(src, fn)
}
