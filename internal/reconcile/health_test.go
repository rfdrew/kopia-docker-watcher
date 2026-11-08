package reconcile

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

const requireHealthy = "kopia.require-healthy=true"

// runHealthy runs a container that asks for health, with the health given,
// and returns its ID.
func (h *harness) runHealthy(name string, health docker.Health, labels ...string) string {
	id := h.engine.run(name, append([]string{enable, pathLabel(app), requireHealthy}, labels...)...)
	h.engine.setHealth(id, health)

	return id
}

// mustSkips checks the snapshots of p held back for health so far.
func (h *harness) mustSkips(p string, want int) {
	h.t.Helper()

	if got := h.metrics.unhealthySkips(p); got != want {
		h.t.Fatalf("%d snapshots of %s held back for health, want %d\nlog:\n%s", got, p, want, h.logs)
	}
}

// mustHealthMetrics checks what was last reported about the health of the
// source of p.
func (h *harness) mustHealthMetrics(p string, gated, unhealthy bool) {
	h.t.Helper()

	s, ok := h.metrics.source(p)
	if !ok || s.HealthGated != gated || s.Unhealthy != unhealthy {
		h.t.Fatalf("metrics of %s = %+v, managed %t; want gated %t, unhealthy %t\nlog:\n%s", p, s, ok, gated, unhealthy, h.logs)
	}
}

// startEvents returns the start events kept for p.
func (h *harness) startEvents(p string) []docker.Event {
	return slices.DeleteFunc(h.pendingFor(p), func(ev docker.Event) bool { return ev.Kind != docker.KindStarted })
}

// The label puts the source on the watcher's own schedule, as hooks do, so
// that a change of the health writes nothing to kopia server; it does not
// count as hooks for the metrics.
func TestHealthOwnSchedule(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy, noOnStart)

	h.passAndSettle(app)

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
	h.mustNext(app, t0.Add(time.Hour))
	h.mustHealthMetrics(app, true, false)

	if s, _ := h.metrics.source(app); s.Hooks {
		t.Fatalf("source metrics = %+v, want no hooks", s)
	}

	writes := h.writes(app)

	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthUnhealthy))
	h.passAndSettle(app)
	h.mustHealthMetrics(app, true, true)

	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
	h.passAndSettle(app)
	h.mustHealthMetrics(app, true, false)

	if h.writes(app) != writes {
		t.Fatalf("changes of the health wrote %d policies", h.writes(app)-writes)
	}

	// The scheduled time is taken by the watcher.
	c.add(time.Hour)
	h.runHooked(app, 4)
	h.mustTriggers(app, 1)
	h.mustNext(app, t0.Add(2*time.Hour))

	// Without the label kopia server takes the schedule back.
	h.engine.remove(id)
	h.engine.run("db", enable, pathLabel(app), noOnStart)
	h.passAndSettle(app)

	h.mustPolicy(app, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if s := h.schedule(app); s != nil {
		t.Fatalf("schedule left: %+v", s)
	}

	h.mustHealthMetrics(app, false, false)
}

// A snapshot at a start waits while the container is starting and is taken
// once it is healthy.
func TestHealthStartWaits(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy)

	h.passAndSettle(app)

	c.add(time.Minute)
	h.engine.setHealth(id, docker.HealthStarting)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.passAndSettle(app)

	h.mustTriggers(app, 0)
	h.mustHealthMetrics(app, true, true)

	if evs := h.startEvents(app); len(evs) != 1 || evs[0].ContainerID != id {
		t.Fatalf("start events kept = %+v\nlog:\n%s", evs, h.logs)
	}

	// Another pass while it is starting keeps waiting.
	h.passAndSettle(app)
	h.mustTriggers(app, 0)

	if len(h.startEvents(app)) != 1 {
		t.Fatalf("start event not kept: %+v", h.pendingFor(app))
	}

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
	h.runHooked(app, 4)

	h.mustTriggers(app, 1)
	h.mustSkips(app, 0)
	h.mustHealthMetrics(app, true, false)

	if len(h.startEvents(app)) != 0 {
		t.Fatalf("start event left: %+v", h.pendingFor(app))
	}
}

// A snapshot at a start is not taken once the container is unhealthy, also
// when it becomes healthy later.
func TestHealthStartDropped(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy)

	h.passAndSettle(app)

	c.add(time.Minute)
	h.engine.setHealth(id, docker.HealthStarting)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.passAndSettle(app)

	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthUnhealthy))
	h.passAndSettle(app)

	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)

	if !h.logged("not taking the snapshot at the start: a container is not healthy", "db (unhealthy)") {
		t.Fatalf("no skip logged:\n%s", h.logs)
	}

	if len(h.startEvents(app)) != 0 {
		t.Fatalf("start event kept: %+v", h.pendingFor(app))
	}

	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
	h.passAndSettle(app)

	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)
}

// A snapshot at a stop is taken whatever the health: Docker reports a
// stopped container unhealthy.
func TestHealthStopSnapshot(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy, onStop)

	h.passAndSettle(app)

	h.engine.setHealth(id, docker.HealthUnhealthy)
	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.passAndSettle(app)

	h.mustTriggers(app, 1)
	h.mustSkips(app, 0)

	if w := h.onlyWait(app); !w.Stop {
		t.Fatalf("wait = %+v, want a stop", w)
	}

	if h.logged("taking the snapshot at the stop without hooks") {
		t.Fatalf("a warning about hooks for a source without them:\n%s", h.logs)
	}
}

// A scheduled time that comes while the container is not healthy is held
// back, logged and counted once, and judged as a missed one once it is
// healthy again.
func TestHealthScheduleHeld(t *testing.T) {
	for _, tt := range []struct {
		name  string
		after time.Duration
		taken bool
	}{
		{"next one far", 10 * time.Minute, true},
		{"next one soon", 40 * time.Minute, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)
			withHooks(h)
			id := h.runHealthy("db", docker.HealthHealthy, noOnStart)

			h.passAndSettle(app)

			h.r.HandleEvent(h.engine.setHealth(id, docker.HealthUnhealthy))
			h.passAndSettle(app)

			due := t0.Add(time.Hour)
			c.add(time.Hour)
			h.passAndSettle(app)

			h.mustTriggers(app, 0)
			h.mustNext(app, due)
			h.mustSkips(app, 1)

			if run := h.record(app).HookRun; run != nil {
				t.Fatalf("hook run %+v", run)
			}

			if h.r.armedAt(app, due) {
				t.Fatal("the held time is still armed")
			}

			if h.count("the scheduled snapshot is held back") != 1 {
				t.Fatalf("hold not logged once:\n%s", h.logs)
			}

			// Another pass holds it back without counting it again.
			c.add(tt.after / 2)
			h.passAndSettle(app)
			h.mustSkips(app, 1)

			if h.count("the scheduled snapshot is held back") != 1 {
				t.Fatalf("hold logged again:\n%s", h.logs)
			}

			c.add(tt.after / 2)
			h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
			h.passAndSettle(app)

			if !h.logged("the scheduled snapshot is no longer held back") {
				t.Fatalf("end of the hold not logged:\n%s", h.logs)
			}

			if !tt.taken {
				h.mustTriggers(app, 0)
				h.mustNext(app, t0.Add(2*time.Hour))

				if !h.logged("skipping a scheduled snapshot the watcher missed") {
					t.Fatalf("no skip logged:\n%s", h.logs)
				}

				return
			}

			if run := h.record(app).HookRun; run == nil || !strings.HasPrefix(run.Reason, "missed schedule at ") {
				t.Fatalf("hook run %+v\nlog:\n%s", run, h.logs)
			}

			h.runHooked(app, 4)
			h.mustTriggers(app, 1)
			h.mustNext(app, t0.Add(2*time.Hour))
		})
	}
}

// A scheduled time that comes while the container is starting is held back
// too, and taken once it is healthy, if the next one is far.
func TestHealthScheduleWhileStarting(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy, noOnStart)

	h.passAndSettle(app)

	c.add(time.Hour)
	h.engine.setHealth(id, docker.HealthStarting)
	h.passAndSettle(app)

	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
	h.runHooked(app, 4)

	h.mustTriggers(app, 1)
	h.mustNext(app, t0.Add(2*time.Hour))
}

// A manual trigger is declined while a container is not healthy, unless
// forced.
func TestHealthManualTrigger(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	h.runHealthy("db", docker.HealthUnhealthy, noOnStart)

	h.passAndSettle(app)

	tr := h.mustTrigger("db", false)
	h.passAndSettle(app)

	res := h.mustResult(tr)
	if res.Outcome != TriggerSkipped || res.Message != "not healthy: db (unhealthy)" {
		t.Fatalf("result = %+v", res)
	}

	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)

	forced := h.mustTrigger("db", true)
	h.runHooked(app, 4)

	h.mustTriggers(app, 1)
	h.mustSkips(app, 1)

	// The run has no hooks of its own: the label alone puts the source on
	// the watcher's schedule.
	if res := h.mustResult(forced); !res.OK() || res.Hooks {
		t.Fatalf("forced result = %+v\nlog:\n%s", res, h.logs)
	}

	if !h.logged("taking the snapshot asked for by hand with force") {
		t.Fatalf("force not logged:\n%s", h.logs)
	}
}

// A container without a health check is not held back; that is logged once.
func TestHealthNoHealthCheck(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthNone)

	h.passAndSettle(app)

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.runHooked(app, 4)

	h.mustTriggers(app, 1)
	h.mustHealthMetrics(app, true, false)

	if h.count("the container has no health check") != 1 {
		t.Fatalf("missing health check not logged once:\n%s", h.logs)
	}
}

// Every running owner that asks for health must be healthy. Owners that do
// not ask, and stopped ones, do not count.
func TestHealthSeveralOwners(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	db := h.runHealthy("db", docker.HealthUnhealthy, noOnStart)
	web := h.engine.run("web", enable, pathLabel(app))
	h.engine.setHealth(web, docker.HealthUnhealthy)

	h.passAndSettle(app)

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, web))
	h.passAndSettle(app)

	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)

	if !h.logged("not taking the snapshot at the start", "db (unhealthy)") || h.logged("not taking the snapshot at the start", "web (") {
		t.Fatalf("wrong containers logged:\n%s", h.logs)
	}

	// With db stopped, the start of web is taken.
	h.engine.setState(db, docker.StateExited)
	h.passAndSettle(app)
	h.mustHealthMetrics(app, true, false)

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, web))
	h.runHooked(app, 4)

	h.mustTriggers(app, 1)
}

// A change of the health queues the paths of a container that asks for
// health, and only of such a container.
func TestHealthEvents(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	db := h.runHealthy("db", docker.HealthHealthy)
	web := h.engine.run("web", enable, pathLabel("/data/web"))

	h.passAndSettle(app)
	h.passAndSettle("/data/web")

	h.r.HandleEvent(h.engine.setHealth(web, docker.HealthUnhealthy))

	if evs := h.pendingFor("/data/web"); len(evs) != 0 {
		t.Fatalf("events kept for a container that does not ask for health: %+v", evs)
	}

	h.r.HandleEvent(h.engine.setHealth(db, docker.HealthUnhealthy))

	evs := h.pendingFor(app)
	if len(evs) != 1 || evs[0].Kind != docker.KindHealth || evs[0].Action != "health_status: unhealthy" {
		t.Fatalf("events kept = %+v", evs)
	}

	// The pass takes no snapshot for it, and keeps nothing.
	h.passAndSettle(app)
	h.mustTriggers(app, 0)

	if evs := h.pendingFor(app); len(evs) != 0 {
		t.Fatalf("events left = %+v", evs)
	}
}

// The health of a container that the listing does not tell, as a daemon
// before API 1.52 leaves it for a paused one, comes from an inspect.
func TestHealthInspected(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthUnhealthy, noOnStart)
	h.engine.setState(id, docker.StatePaused)
	h.engine.unlist(id)

	h.passAndSettle(app)

	if h.engine.inspected(id) == 0 {
		t.Fatal("the container was not inspected")
	}

	h.mustHealthMetrics(app, true, true)

	c.add(time.Hour)
	h.passAndSettle(app)
	h.mustTriggers(app, 0)
	h.mustSkips(app, 1)

	// A container that asks for no health is not inspected.
	other := h.engine.run("web", enable, pathLabel("/data/web"))
	h.engine.unlist(other)
	h.passAndSettle("/data/web")

	if n := h.engine.inspected(other); n != 0 {
		t.Fatalf("inspected %d times", n)
	}
}

// A container the watcher pauses for a snapshot does not hold back the
// source meanwhile, though Docker reports a paused container unhealthy.
func TestHealthHeldContainer(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.runHealthy("db", docker.HealthHealthy, noOnStart, pauseDuring)

	h.engine.setLifecycle(func(_ context.Context, action string, ctr docker.Container) error {
		if action == "pause" {
			h.engine.setHealth(ctr.ID, docker.HealthUnhealthy)
		}

		return nil
	})

	h.passAndSettle(app)

	c.add(time.Hour)

	sawHeld := false

	for range 5 {
		h.passAndSettle(app)

		if len(h.store.Held()) > 0 {
			sawHeld = true

			h.mustHealthMetrics(app, true, false)
		}

		if h.record(app).HookRun == nil {
			break
		}
	}

	if !sawHeld {
		t.Fatalf("the container was never held\nlog:\n%s", h.logs)
	}

	h.mustTriggers(app, 1)
	h.mustSkips(app, 0)
	h.mustState(id, docker.StateRunning)
}

// Docker reports a container the watcher paused for a snapshot unhealthy
// until its next check. The snapshots of another source that waited for the
// container wait for that check too, and the scheduled time is taken on
// time; without a health event, the health counts as it is after
// returnHealthWait.
func TestHealthAfterReturn(t *testing.T) {
	for _, tt := range []struct {
		name    string
		healthy bool
	}{
		{"healthy at the next check", true},
		{"no event", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)
			withHooks(h)
			other := "/data/other"
			id := h.engine.run("db", enable, pathLabel(app+","+other), requireHealthy, noOnStart, pauseDuring,
				"kopia.run-missed=false")
			h.engine.setHealth(id, docker.HealthHealthy)

			h.engine.setLifecycle(func(_ context.Context, action string, ctr docker.Container) error {
				if action == "pause" {
					h.engine.setHealth(ctr.ID, docker.HealthUnhealthy)
				}

				return nil
			})

			h.passAndSettle(app)
			h.passAndSettle(other)
			c.add(time.Hour)

			h.fake.HoldSnapshots(h.src(app))
			h.passAndSettle(app)
			h.mustPass(app)
			h.mustActions("pause db")

			// The other source waits for the container held for the first.
			h.mustPass(other)
			h.mustTriggers(other, 0)

			h.fake.ReleaseSnapshots(h.src(app))
			h.settle()
			h.runHooked(app, 2)
			h.mustActions("pause db", "unpause db")

			// Back, but Docker still reports it unhealthy: the other
			// source waits for the next check, armed, and counts nothing.
			res := h.passAndSettle(other)
			h.mustTriggers(other, 0)
			h.mustSkips(other, 0)
			h.mustHealthMetrics(other, true, false)

			due := t0.Add(time.Hour)
			if !h.r.armedAt(other, due) || res.wake.IsZero() {
				t.Fatalf("armed %v, wake %v", h.r.armed, res.wake)
			}

			if !tt.healthy {
				c.add(returnHealthWait)
				h.passAndSettle(other)
				h.mustTriggers(other, 0)
				h.mustSkips(other, 1)
				h.mustHealthMetrics(other, true, true)

				return
			}

			c.add(30 * time.Second)
			h.r.HandleEvent(h.engine.setHealth(id, docker.HealthHealthy))
			h.passAndSettle(other)

			if run := h.record(other).HookRun; run == nil || run.Reason != "schedule" {
				t.Fatalf("hook run %+v\nlog:\n%s", run, h.logs)
			}

			h.runHooked(other, 4)
			h.mustTriggers(other, 1)
			h.mustSkips(other, 0)
			h.mustSkips(app, 0)
		})
	}
}

// A source on the watcher's schedule only for the label does not count as
// one with hooks for the metrics, also once its containers are gone; one
// whose containers had hooks still does.
func TestHealthHooksMetricsWithoutOwners(t *testing.T) {
	for _, tt := range []struct {
		name  string
		extra []string
		hooks bool
	}{
		{"health only", nil, false},
		{"with hooks", []string{preExec}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			withClock(h)
			withHooks(h)
			id := h.runHealthy("db", docker.HealthHealthy, append([]string{noOnStart}, tt.extra...)...)

			h.passAndSettle(app)

			h.engine.remove(id)
			h.passAndSettle(app)

			if s, ok := h.metrics.source(app); !ok || s.Hooks != tt.hooks || len(s.Owners) != 0 || s.HealthGated {
				t.Fatalf("metrics = %+v, managed %t; want hooks %t", s, ok, tt.hooks)
			}

			if s := h.schedule(app); s == nil {
				t.Fatal("the source left the watcher's schedule")
			}
		})
	}
}
