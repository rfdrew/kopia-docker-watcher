package reconcile

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	preExec  = "kopia.pre-exec=pg_dump -f /data/app/db.sql"
	postExec = `kopia.post-exec=["rm","/data/app/db.sql"]`

	preRun  = "db: sh -c pg_dump -f /data/app/db.sql"
	postRun = "db: rm /data/app/db.sql"
)

// kopiaSchedule works the upcoming snapshot times of a scheduling section
// out as kopia server's policy/resolve does, in UTC: an interval counts
// from now, truncated to the interval; times of day and cron expressions
// give their next time after now. The cron expressions may use numbers,
// * and */n in the minute and hour fields only.
func kopiaSchedule(section json.RawMessage, now time.Time, n int) []time.Time {
	var s struct {
		IntervalSeconds int64 `json:"intervalSeconds"`
		TimeOfDay       []struct {
			Hour int `json:"hour"`
			Min  int `json:"min"`
		} `json:"timeOfDay"`
		Cron []string `json:"cron"`
	}

	if err := json.Unmarshal(section, &s); err != nil {
		panic(err)
	}

	var out []time.Time

	for range n {
		var (
			next time.Time
			ok   bool
		)

		take := func(t time.Time) {
			if !ok || t.Before(next) {
				next, ok = t, true
			}
		}

		if s.IntervalSeconds > 0 {
			iv := time.Duration(s.IntervalSeconds) * time.Second
			take(later(now.Add(iv).Truncate(iv), now))
		}

		for _, tod := range s.TimeOfDay {
			t := time.Date(now.Year(), now.Month(), now.Day(), tod.Hour, tod.Min, 0, 0, time.UTC)
			if now.After(t) {
				t = t.Add(24 * time.Hour)
			}

			take(t)
		}

		for _, c := range s.Cron {
			if t, found := nextCron(c, now); found {
				take(t)
			}
		}

		if !ok {
			break
		}

		out = append(out, next)
		now = next.Add(time.Second)
	}

	return out
}

// nextCron returns the first minute after now that cron expression c
// matches.
func nextCron(c string, now time.Time) (time.Time, bool) {
	f := strings.Fields(c)

	match := func(field string, v int) bool {
		switch {
		case field == "*":
			return true
		case strings.HasPrefix(field, "*/"):
			step, _ := strconv.Atoi(field[2:])
			return v%step == 0
		default:
			n, _ := strconv.Atoi(field)
			return n == v
		}
	}

	for t := now.Truncate(time.Minute).Add(time.Minute); t.Before(now.Add(49 * time.Hour)); t = t.Add(time.Minute) {
		if match(f[0], t.Minute()) && match(f[1], t.Hour()) {
			return t, true
		}
	}

	return time.Time{}, false
}

func TestKopiaSchedule(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 10, 18, h, m, 0, 0, time.UTC) }

	tests := []struct {
		section string
		want    []time.Time
	}{
		{`{"cron":["0 * * * *"]}`, []time.Time{at(13, 0), at(14, 0)}},
		{`{"intervalSeconds":1800}`, []time.Time{at(12, 30), at(13, 0)}},
		{`{"timeOfDay":[{"hour":2,"min":30}]}`, []time.Time{at(26, 30), at(50, 30)}},
		{`{"cron":["*/20 * * * *"],"intervalSeconds":3600}`, []time.Time{at(12, 20), at(12, 40)}},
		{`{}`, nil},
	}

	for _, tt := range tests {
		got := kopiaSchedule(json.RawMessage(tt.section), at(12, 0).Add(time.Second), 2)
		if !slices.EqualFunc(got, tt.want, time.Time.Equal) {
			t.Errorf("%s: %v, want %v", tt.section, got, tt.want)
		}
	}
}

// withHooks makes the passes of h run hooks and follow snapshots, as under
// Run, and gives the fake kopia server's way of working out schedules,
// with runMissed from its default policy.
func withHooks(h *harness) {
	h.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h.t.Cleanup(func() {
		cancel()
		h.settle()
	})

	h.stopHooks = func() {
		cancel()
		h.settle()
	}

	h.r.mu.Lock()
	h.r.runCtx = ctx
	h.r.hookCtx = ctx
	h.r.mu.Unlock()

	h.fake.SetSchedule(kopiaSchedule)
	h.fake.SetInheritedScheduling(`{"runMissed":true}`)
}

// settle waits until no hook runs and no snapshot is followed.
func (h *harness) settle() {
	h.r.hookers.Wait()
	h.r.waiters.Wait()
}

// passAndSettle runs a pass over p that must succeed, then settles.
func (h *harness) passAndSettle(p string) passResult {
	h.t.Helper()

	res := h.mustPass(p)
	h.settle()

	return res
}

// schedule returns the watcher's schedule of p; nil if none.
func (h *harness) schedule(p string) *state.Schedule {
	return h.record(p).Schedule
}

// mustNext checks the next scheduled time of p.
func (h *harness) mustNext(p string, want time.Time) {
	h.t.Helper()

	s := h.schedule(p)
	if s == nil || !s.Next.Equal(want) {
		h.t.Fatalf("schedule of %s = %+v, want next %s\nlog:\n%s", p, s, want, h.logs)
	}
}

// mustExecs checks the hook commands run so far.
func (h *harness) mustExecs(want ...string) {
	h.t.Helper()

	if got := h.engine.execLog(); !slices.Equal(got, want) {
		h.t.Fatalf("hooks run = %q, want %q\nlog:\n%s", got, want, h.logs)
	}
}

// mustTriggers checks the number of snapshots queued of p.
func (h *harness) mustTriggers(p string, want int) {
	h.t.Helper()

	if n := h.triggers(p); n != want {
		h.t.Fatalf("%d snapshots queued of %s, want %d\nlog:\n%s", n, p, want, h.logs)
	}
}

// takeScheduled runs the passes of a snapshot with hooks to its end: the
// one that takes the snapshot, which runs the pre-exec hooks, the one that
// queues it, and the one that runs the post-exec hooks once it ended.
func (h *harness) takeScheduled(p string) {
	h.t.Helper()

	for range 3 {
		h.passAndSettle(p)
	}

	if run := h.record(p).HookRun; run != nil {
		h.t.Fatalf("the snapshot with hooks of %s did not end: %+v\nlog:\n%s", p, run, h.logs)
	}
}

// A source with hooks is off kopia server's schedule: the watcher works
// the times out from the cron of its labels and takes the snapshots, with
// pre-exec before and post-exec after.
func TestHooksCronSchedule(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	res := h.passAndSettle(app)

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)

	if a, _ := h.applied(app); string(a.SavedScheduling) != `{"cron":["0 * * * *"]}` {
		t.Fatalf("saved scheduling = %s", a.SavedScheduling)
	}

	next := t0.Add(time.Hour)
	h.mustNext(app, next)

	if !res.nextRun.Equal(next) || !h.r.armedAt(app, next) {
		t.Fatalf("timer = %v, armed %v; want %v", res.nextRun, h.r.armed, next)
	}

	// The preview asked for the policy without the pause.
	calls := h.fake.CallsFor(h.src(app), backendtest.PreviewSchedule)
	if len(calls) == 0 || strings.Contains(string(calls[len(calls)-1].Policy), "manual") {
		t.Fatalf("preview calls %+v", calls)
	}

	// Nothing before the time.
	c.add(59 * time.Minute)
	h.passAndSettle(app)
	h.mustExecs()
	h.mustTriggers(app, 0)

	c.add(time.Minute)
	h.passAndSettle(app)
	h.mustExecs(preRun)
	h.mustTriggers(app, 0)
	h.mustNext(app, t0.Add(2*time.Hour))

	if run := h.record(app).HookRun; run == nil || !run.PreDone || run.PreError != "" || run.Reason != "schedule" {
		t.Fatalf("hook run = %+v", run)
	}

	if rec := h.record(app); !rec.LastTriggered.Equal(next) {
		t.Fatalf("last triggered %v", rec.LastTriggered)
	}

	// The next pass queues the snapshot, the one after its end runs the
	// post-exec hook.
	h.passAndSettle(app)
	h.mustTriggers(app, 1)
	h.mustExecs(preRun)

	if run := h.record(app).HookRun; run == nil || run.Phase != state.HookSnapshot || run.WaitID == "" {
		t.Fatalf("hook run = %+v", run)
	}

	h.passAndSettle(app)
	h.mustExecs(preRun, postRun)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run left: %+v", run)
	}

	if len(h.waits(app)) != 0 {
		t.Fatalf("waits left: %+v", h.waits(app))
	}

	results := h.metrics.hookResults(app)
	if len(results) != 2 || results[0].Hook != HookPreExec || results[1].Hook != HookPostExec ||
		results[0].Error != "" || results[1].Error != "" || results[0].Container != "db" {
		t.Fatalf("hook results = %+v", results)
	}

	if s, _ := h.metrics.source(app); !s.Hooks {
		t.Fatalf("source metrics = %+v", s)
	}

	// The snapshot got the description.
	if calls := h.fake.CallsFor(h.src(app), backendtest.AnnotateSnapshot); len(calls) != 1 || calls[0].Description != "db" {
		t.Fatalf("annotations %+v", calls)
	}
}

// An interval counts from now, truncated, as with kopia server; one missed
// while the watcher did not run is taken at once.
func TestHooksIntervalSchedule(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.interval=30m")

	h.passAndSettle(app)
	h.mustNext(app, t0.Add(30*time.Minute))

	if s := h.schedule(app); !s.Interval || !s.RunMissed {
		t.Fatalf("schedule %+v", s)
	}

	c.add(30 * time.Minute)
	h.takeScheduled(app)
	h.mustTriggers(app, 1)
	h.mustNext(app, t0.Add(time.Hour))

	// The watcher stops for two hours.
	c.add(2 * time.Hour)

	h.restartHooks(c)
	h.passAndSettle(app)

	if run := h.record(app).HookRun; run == nil || !strings.HasPrefix(run.Reason, "missed schedule at ") {
		t.Fatalf("hook run %+v\nlog:\n%s", run, h.logs)
	}

	h.takeScheduled(app)
	h.mustTriggers(app, 2)
	h.mustNext(app, t0.Add(3*time.Hour))
}

// restartHooks restarts the watcher of h, as restart does, with its
// passes running hooks; it waits for those of the first one first.
func (h *harness) restartHooks(c *testClock) {
	h.t.Helper()

	h.settle()
	h.restart(c)
	withHooks(h)
}

// Times of day missed while the watcher did not run are taken late only
// with run-missed, and only when the next one is more than half an hour
// away.
func TestHooksMissedTimeOfDay(t *testing.T) {
	for _, tt := range []struct {
		name      string
		runMissed string
		downtime  time.Duration
		taken     bool
	}{
		{"run missed", "true", 15 * time.Hour, true},
		{"kopia's default", "", 15 * time.Hour, true},
		{"without run missed", "false", 15 * time.Hour, false},
		{"next one soon", "true", 38*time.Hour + 10*time.Minute, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)
			withHooks(h)
			h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.time-of-day=02:30",
				"kopia.run-missed="+tt.runMissed)

			h.passAndSettle(app)

			first := time.Date(2026, 10, 19, 2, 30, 0, 0, time.UTC)
			h.mustNext(app, first)

			c.add(tt.downtime)

			h.restartHooks(c)
			h.passAndSettle(app)

			if tt.taken {
				h.mustExecs(preRun)
				h.takeScheduled(app)
				h.mustTriggers(app, 1)
			} else {
				h.mustExecs()
				h.mustTriggers(app, 0)

				if !h.logged("skipping a scheduled snapshot the watcher missed") {
					t.Fatalf("no skip logged:\n%s", h.logs)
				}
			}

			// The schedule moved on to the next time of day after now.
			want := first.Add(24 * time.Hour)
			if now := c.now(); !want.After(now) {
				want = first.Add(48 * time.Hour)
			}

			h.mustNext(app, want)
		})
	}
}

// A pre-exec hook that fails takes no snapshot; the post-exec hook runs all
// the same, and the failure goes to the log and Metrics.
func TestHooksPreExecFails(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)
	h.engine.setExec(func(_ context.Context, _ docker.Container, cmd []string) (docker.ExecResult, error) {
		if cmd[0] == "sh" {
			return docker.ExecResult{ExitCode: 1, Output: []byte("pg_dump: connection refused\n")}, nil
		}

		return docker.ExecResult{}, nil
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.takeScheduled(app)

	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 0)

	results := h.metrics.hookResults(app)
	if len(results) != 2 || results[0].Error != "exited with code 1" || results[1].Error != "" {
		t.Fatalf("hook results = %+v", results)
	}

	if !h.logged("pre-exec hook failed", `"output":"pg_dump: connection refused"`, `"container":"db"`) ||
		!h.logged("no snapshot: a pre-exec hook failed") {
		t.Fatalf("failure not logged:\n%s", h.logs)
	}

	// The attempt counts for the minimum interval and moves the schedule.
	h.mustNext(app, t0.Add(2*time.Hour))
}

// A hook that runs out of time fails like one that exits with an error.
func TestHooksTimeout(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.hook-timeout=10ms")
	h.engine.setExec(func(ctx context.Context, _ docker.Container, _ []string) (docker.ExecResult, error) {
		<-ctx.Done()
		return docker.ExecResult{}, ctx.Err()
	})

	h.passAndSettle(app)
	c.add(time.Hour)
	h.takeScheduled(app)
	h.mustTriggers(app, 0)

	if results := h.metrics.hookResults(app); len(results) != 1 || !strings.HasPrefix(results[0].Error, "did not finish within 10ms") {
		t.Fatalf("hook results = %+v", results)
	}
}

// The post-exec hook runs after a snapshot that failed too.
func TestHooksPostExecAfterFailedSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.passAndSettle(app)
	h.fake.QueueSnapshotResult(h.src(app), backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})

	c.add(time.Hour)
	h.takeScheduled(app)

	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 1)

	if !h.logged("snapshot failed", `"error":"disk full"`) {
		t.Fatalf("failure not logged:\n%s", h.logs)
	}
}

// While no owner runs, the scheduled snapshots stop, without a policy
// write; a time that passes meanwhile is a missed one once the owner runs
// again.
func TestHooksPause(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.run-missed=false")

	h.passAndSettle(app)
	writes := h.writes(app)

	h.engine.setState(id, docker.StateExited)
	res := h.passAndSettle(app)

	if s := h.schedule(app); s == nil || !s.Paused {
		t.Fatalf("schedule %+v", s)
	}

	if h.writes(app) != writes || !res.nextRun.IsZero() || h.r.armedAt(app, t0.Add(time.Hour)) {
		t.Fatalf("pause wrote %d policies, timer %v", h.writes(app)-writes, res.nextRun)
	}

	if !h.logged("scheduled snapshots paused") {
		t.Fatalf("pause not logged:\n%s", h.logs)
	}

	c.add(2 * time.Hour)
	h.passAndSettle(app)
	h.mustExecs()
	h.mustTriggers(app, 0)

	h.engine.setState(id, docker.StateRunning)
	res = h.passAndSettle(app)

	h.mustExecs()
	h.mustTriggers(app, 0)
	h.mustNext(app, t0.Add(3*time.Hour))

	if h.writes(app) != writes || !res.nextRun.Equal(t0.Add(3*time.Hour)) {
		t.Fatalf("resume wrote %d policies, timer %v", h.writes(app)-writes, res.nextRun)
	}

	if !h.logged("scheduled snapshots resumed") || !h.logged("skipping a scheduled snapshot the watcher missed") {
		t.Fatalf("resume not logged:\n%s", h.logs)
	}
}

// A source whose container gets hooks goes on the watcher's schedule, and
// back to kopia server's once no container has hooks; neither change takes
// a snapshot of its own.
func TestHooksComeAndGo(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), "kopia.schedule=30 2 * * *")

	h.passAndSettle(app)
	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)

	h.engine.remove(id)
	id = h.engine.run("db", enable, pathLabel(app), "kopia.schedule=30 2 * * *", preExec)
	h.passAndSettle(app)

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
	h.mustNext(app, time.Date(2026, 10, 19, 2, 30, 0, 0, time.UTC))

	// A start that no event told of would show as a lifted pause in the
	// policy; a pass after the one that wrote it would take it.
	h.passAndSettle(app)
	h.mustTriggers(app, 0)

	h.engine.remove(id)
	h.engine.run("db", enable, pathLabel(app), "kopia.schedule=30 2 * * *")
	res := h.passAndSettle(app)
	h.passAndSettle(app)

	h.mustPolicy(app, `{"scheduling":{"cron":["30 2 * * *"]}}`)

	if s := h.schedule(app); s != nil || !res.nextRun.IsZero() || len(h.r.armed) != 0 {
		t.Fatalf("schedule left: %+v, timer %v, armed %v", s, res.nextRun, h.r.armed)
	}

	if a, _ := h.applied(app); a.SavedScheduling != nil {
		t.Fatalf("saved scheduling left: %s", a.SavedScheduling)
	}

	h.mustExecs()
	h.mustTriggers(app, 0)

	if !h.logged("kopia server takes the scheduled snapshots of the source again") {
		t.Fatalf("hand-back not logged:\n%s", h.logs)
	}
}

// A snapshot at a stop is taken without hooks: the container has stopped.
func TestHooksStopSnapshot(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop, preExec, postExec)

	h.passAndSettle(app)

	h.engine.setState(id, docker.StateExited)
	h.r.HandleEvent(h.engine.event(docker.KindDied, id))
	h.passAndSettle(app)

	h.mustTriggers(app, 1)
	h.mustExecs()

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run %+v", run)
	}

	if !h.logged("taking the snapshot at the stop without hooks") {
		t.Fatalf("no warning:\n%s", h.logs)
	}
}

// A stop that no event told of shows in the watcher's own pause of a
// source with hooks, as it shows in the policy of one without.
func TestHooksUnseenStop(t *testing.T) {
	h := newHarness(t)
	withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), noOnStart, onStop, preExec)

	h.passAndSettle(app)

	h.engine.setState(id, docker.StateExited)
	h.passAndSettle(app)

	h.mustTriggers(app, 1)
	h.mustExecs()

	if w := h.onlyWait(app); !w.Stop || !strings.Contains(w.Reason, "without an event") {
		t.Fatalf("wait %+v", w)
	}
}

// A start snapshot of a source with hooks runs them, as the minimum
// interval allows.
func TestHooksStartSnapshot(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	id := h.engine.run("db", enable, pathLabel(app), preExec, postExec)

	h.passAndSettle(app)
	h.mustExecs()

	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.takeScheduled(app)

	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 1)

	// Another start within the minimum interval takes none.
	c.add(time.Minute)
	h.r.HandleEvent(h.engine.event(docker.KindStarted, id))
	h.passAndSettle(app)
	h.mustExecs(preRun, postRun)
	h.mustTriggers(app, 1)
}

// A scheduled time that comes while a snapshot with hooks is under way is
// judged as a missed one once it ends, as kopia server does once its
// upload ends: taken with runMissed when the next time is far enough.
func TestHooksScheduleWaitsForRun(t *testing.T) {
	for _, tt := range []struct {
		name      string
		runMissed string
		taken     bool
	}{
		{"run missed", "", true},
		{"without run missed", "false", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			c := withClock(h)
			withHooks(h)
			h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, "kopia.run-missed="+tt.runMissed)

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
			h.mustNext(app, t0.Add(2*time.Hour))

			// The pre-exec hook still runs at the next time.
			c.add(time.Hour)
			h.mustPass(app)
			h.mustNext(app, t0.Add(2*time.Hour))

			if run := h.record(app).HookRun; run == nil || run.Reason != "schedule" || run.PreDone {
				t.Fatalf("hook run %+v", run)
			}

			close(release)
			h.settle()

			// The first run queues its snapshot and ends; the time that came
			// meanwhile is judged then.
			h.passAndSettle(app)
			h.passAndSettle(app)
			h.mustTriggers(app, 1)
			h.mustNext(app, t0.Add(3*time.Hour))

			if !tt.taken {
				if run := h.record(app).HookRun; run != nil {
					t.Fatalf("hook run %+v", run)
				}

				if !h.logged("skipping a scheduled snapshot the watcher missed") {
					t.Fatalf("no skip logged:\n%s", h.logs)
				}

				return
			}

			if run := h.record(app).HookRun; run == nil || !strings.HasPrefix(run.Reason, "missed schedule at ") {
				t.Fatalf("hook run %+v\nlog:\n%s", run, h.logs)
			}

			h.takeScheduled(app)
			h.mustTriggers(app, 2)
		})
	}
}

// A stop of the watcher that cut the pre-exec hooks short takes no
// snapshot after the restart, but runs the post-exec hooks.
func TestHooksRestartDuringPreExec(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec, postExec)

	h.passAndSettle(app)

	err := h.store.Update(h.src(app), func(r *state.Record) error {
		r.HookRun = &state.HookRun{ID: "cut", Started: t0, Phase: state.HookPre, Reason: "schedule"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	c.add(time.Minute)

	h.restartHooks(c)
	h.passAndSettle(app)

	h.mustExecs(postRun)
	h.mustTriggers(app, 0)

	if run := h.record(app).HookRun; run != nil {
		t.Fatalf("hook run %+v", run)
	}

	if !h.logged("no snapshot: the watcher stopped while the pre-exec hooks ran") {
		t.Fatalf("not logged:\n%s", h.logs)
	}
}

// A pre-exec label with an error fails the snapshot without running
// anything; the source stays on the watcher's schedule.
func TestHooksLabelError(t *testing.T) {
	h := newHarness(t)
	c := withClock(h)
	withHooks(h)
	h.engine.run("db", enable, pathLabel(app), noOnStart, preExec)

	h.passAndSettle(app)

	// The container is recreated with a broken label: its policy fields
	// fall back to the last patch, and its hooks cannot run.
	h.engine.remove(h.engine.containerID("db"))
	h.engine.run("db", enable, pathLabel(app), noOnStart, `kopia.pre-exec=["pg_dump",`)

	c.add(time.Hour)
	h.takeScheduled(app)

	h.mustPolicy(app, `{"scheduling":{"manual":true}}`)
	h.mustExecs()
	h.mustTriggers(app, 0)

	if results := h.metrics.hookResults(app); len(results) != 1 || !strings.Contains(results[0].Error, "the hook labels have errors") {
		t.Fatalf("hook results = %+v", results)
	}
}
