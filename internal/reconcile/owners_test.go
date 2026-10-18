package reconcile

import (
	"errors"
	"strings"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// Owners that ask for different schedules: the error is logged and
// counted, the last applied patch stays, and the pause still follows the
// owners.
func TestOwnersDisagree(t *testing.T) {
	h := newHarness(t)
	web := h.engine.run("web", enable, pathLabel(app))
	h.mustPass(app)

	worker := h.engine.run("worker", enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	res := h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), `web: scheduling.cron=["0 * * * *"]; worker: scheduling.cron=["30 2 * * *"]`) {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if h.metrics.count(app, ReasonOwnersDisagree) != 1 || !h.logged("running containers disagree on the policy of the path") {
		t.Errorf("disagreement not reported:\n%s", h.logs)
	}

	// Both stop: the source is paused, and the disagreement is only a
	// warning now, since the labels of whichever starts first decide.
	h.engine.setState(web, docker.StateExited)
	h.engine.setState(worker, docker.StateExited)

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems while stopped = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	if !h.logged("stopped containers that share the path ask for different policies") {
		t.Errorf("no warning:\n%s", h.logs)
	}

	// The odd one goes: the owners agree again.
	h.engine.remove(worker)
	h.engine.setState(web, docker.StateRunning)

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A stopped container keeps its old labels; it does not hold back the
// running one's, as with a leftover one-off of docker compose run.
func TestStoppedOwnerDoesNotDecide(t *testing.T) {
	h := newHarness(t)
	h.engine.create("app-run-1", docker.StateExited, enable, pathLabel(app))
	h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)
}

// docker compose recreates a container with new labels: the new one is
// created while the old one still stops. Nothing is reported, and the new
// labels apply once it runs.
func TestComposeRecreate(t *testing.T) {
	h := newHarness(t)
	old := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.create("1a2b3c_app", docker.StateCreated, enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	if res := h.mustPass(app); len(res.problems) != 0 || h.writes(app) != 1 {
		t.Errorf("problems %v, %d writes", res.problems, h.writes(app))
	}

	h.engine.setState(old, docker.StateExited)
	h.engine.remove(old)
	h.engine.create("app", docker.StateRunning, enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)
}

// A source is registered once a container that backs it up runs: a
// container that was only created may not have its bind mounts yet.
func TestRegisterWhenRunning(t *testing.T) {
	h := newHarness(t)
	id := h.engine.create("app", docker.StateCreated, enable, pathLabel(app))

	h.mustPass(app)
	h.mustNoSource(app)

	h.engine.setState(id, docker.StateRunning)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if n := h.writes(app); n != 1 {
		t.Errorf("%d writes", n)
	}
}

// Owners that disagree on a path the watcher has never registered: nothing
// is registered.
func TestOwnersDisagreeNewSource(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app))
	h.engine.run("worker", enable, pathLabel(app), scheduleLabel("30 2 * * *"))

	if res := h.mustPass(app); len(res.problems) != 1 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustNoSource(app)
}

// Owners agree when they ask for the same fields, whatever the label text.
func TestOwnersAgree(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app), scheduleLabel("0 * * * *"))
	h.engine.run("worker", enable, pathLabel(app+"/"))
	h.engine.create("old", docker.StateExited, enable, pathLabel(" "+app+" "))

	if res := h.mustPass(app); len(res.problems) != 0 {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	plans := h.fake.CallsTo(backendtest.PlanSource)
	if got := plans[0].Desired.Owners; strings.Join(got, ",") != "old,web,worker" {
		t.Errorf("owners = %v", got)
	}
}

// In strict mode an unknown label keeps a container's labels from being
// applied: a known source keeps its patch, a new one is not registered.
func TestInvalidLabelsFallBack(t *testing.T) {
	h := newHarness(t, withStrict)
	id := h.engine.run("app", enable, pathLabel(app))
	h.mustPass(app)

	h.engine.remove(id)
	id = h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"), "kopia.retention=7")

	res := h.mustPass(app)
	if len(res.problems) != 1 || !strings.Contains(res.problems[0].Error(), "kopia.retention") {
		t.Errorf("problems = %v", res.problems)
	}

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if h.metrics.count(app, ReasonLabels) != 1 {
		t.Error("not counted")
	}

	// The pause still follows the container.
	h.engine.setState(id, docker.StateExited)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	h.engine.run("db", enable, pathLabel("/data/db"), "kopia.typo=1")
	h.mustPass("/data/db")
	h.mustNoSource("/data/db")
}

// A path that is not absolute is a label error: it makes no source, and
// the container's other paths work.
func TestRelativePath(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel("data/app,"+app))

	h.r.ReconcileAll()
	paths, err := h.r.paths(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(paths, ",") != app {
		t.Errorf("paths = %v", paths)
	}

	if h.metrics.count("", ReasonLabels) != 1 || !h.logged("container labels have errors", `data/app`, "not an absolute path") {
		t.Errorf("label error not reported:\n%s", h.logs)
	}

	// Reported once per set of labels.
	if _, err := h.r.paths(t.Context()); err != nil {
		t.Fatal(err)
	}

	if h.metrics.count("", ReasonLabels) != 1 {
		t.Error("reported again")
	}
}

// With HOST_PREFIX the label path counts from it.
func TestHostPrefix(t *testing.T) {
	h := newHarness(t, withHostPrefix("/host"))
	id := h.engine.run("app", enable, pathLabel("data/app/"))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))

	paths, err := h.r.paths(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if strings.Join(paths, ",") != "/host/data/app" {
		t.Fatalf("paths = %v", paths)
	}

	h.mustPass("/host/data/app")
	h.mustPolicy("/host/data/app", `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A path whose containers are all gone is paused with its fields; a path
// the watcher never managed is not touched.
func TestOwnerless(t *testing.T) {
	h := newHarness(t)
	id := h.engine.run("app", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.mustPass(app)

	h.engine.remove(id)
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	a, _ := h.applied(app)
	if !a.Patch.Equal(cronPatch("30 2 * * *")) {
		t.Errorf("applied patch = %+v", a.Patch)
	}

	assertJSON(t, "saved", a.SavedScheduling, `{"cron":["30 2 * * *"]}`)

	// Not managed: a source of the web UI.
	other := h.src("/data/other")
	h.fake.SetPolicy(other, `{"scheduling":{"cron":["0 3 * * *"]}}`)
	h.fake.ResetCalls()
	h.mustPass("/data/other")

	if n := len(h.fake.Calls()); n != 0 {
		t.Errorf("%d calls for a source the watcher never managed", n)
	}

	// A container that turns the backup off frees the path, which is
	// paused.
	h.engine.run("app", "kopia.enable=false", pathLabel(app))
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	// A new container with the path unpauses it with its own labels.
	h.engine.run("app2", enable, pathLabel(app))
	h.mustPass(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// A source in the state whose containers are gone and that kopia server
// does not know is not registered again.
func TestOwnerlessNotRegistered(t *testing.T) {
	h := newHarness(t)
	h.engine.run("app", enable, pathLabel(app))
	h.fake.FailRegisterPut(&backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("timeout")})
	h.fake.FailNext(backendtest.ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("timeout")})

	if res := h.pass(app); !res.retry {
		t.Fatalf("err %v", res.err)
	}

	if _, ok := h.applied(app); !ok {
		t.Fatal("no intent stored")
	}

	h.engine = newFakeEngine()
	h.r.docker = h.engine
	h.mustPass(app)
	h.mustNoSource(app)
}

// HandleEvent queues the paths of the container from the event's labels,
// a destroy included, and turns a resync into a full reconcile.
func TestHandleEvent(t *testing.T) {
	h := newHarness(t)
	passes := h.start()

	id := h.engine.run("app", enable, pathLabel(app+",/data/logs"))
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	waitPasses(t, passes, app, "/data/logs")

	h.r.HandleEvent(h.engine.remove(id))
	waitPasses(t, passes, app, "/data/logs")
	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
	h.mustPolicy("/data/logs", `{"scheduling":{"manual":true}}`)

	h.engine.run("db", enable, pathLabel("/data/db"))
	h.r.HandleEvent(docker.Event{Kind: docker.KindResync})

	// The state's sources are part of a full reconcile.
	waitPasses(t, passes, "/data/db", app, "/data/logs")
}

// RunOnce returns the problems of every pass.
func TestRunOnceReportsProblems(t *testing.T) {
	h := newHarness(t)
	h.engine.run("web", enable, pathLabel(app))
	h.engine.run("worker", enable, pathLabel(app), scheduleLabel("30 2 * * *"))
	h.engine.run("db", enable, pathLabel("/data/db"))

	err := h.r.RunOnce(t.Context())
	if err == nil || !strings.Contains(err.Error(), "ask for different policies") {
		t.Fatalf("RunOnce: %v", err)
	}

	h.mustPolicy("/data/db", `{"scheduling":{"cron":["0 * * * *"]}}`)
}
