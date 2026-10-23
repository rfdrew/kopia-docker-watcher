package rest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// t0 is when the tests trigger snapshots, by the watcher's clock and kopia
// server's alike.
var t0 = time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

// taskDesc is kopia server's description of a snapshot task of path p that
// started at start.
func taskDesc(p string, start time.Time) string {
	return src(p).String() + " at " + start.Format(time.RFC3339)
}

// snapTask returns a snapshot task of path p that has uploaded, so it
// reports counters; a finished one ended a minute after it started.
func snapTask(id, p string, start time.Time, status kopiaclient.TaskStatus) kopiaclient.TaskInfo {
	t := kopiaclient.TaskInfo{ID: id, Kind: kopiaclient.TaskKindSnapshot, Description: taskDesc(p, start), StartTime: start, Status: status,
		Counters: map[string]kopiaclient.CounterValue{"Hashed Files": {Value: 1}}}

	if status.IsFinished() {
		end := start.Add(time.Minute)
		t.EndTime = &end
	}

	return t
}

// finish ends the task id with status, a minute after it started.
func (f *fakeKopia) finish(id string, status kopiaclient.TaskStatus, errMsg string) {
	for i := range f.tasks {
		if f.tasks[i].ID == id {
			end := f.tasks[i].StartTime.Add(time.Minute)
			f.tasks[i].Status, f.tasks[i].EndTime, f.tasks[i].ErrorMessage = status, &end, errMsg

			return
		}
	}

	panic("fakeKopia: no task " + id)
}

// start adds a running task, listed first as the newest.
func (f *fakeKopia) start(t kopiaclient.TaskInfo) {
	f.tasks = append([]kopiaclient.TaskInfo{t}, f.tasks...)
}

func (f *fakeKopia) setStatus(si kopiaclient.SourceInfo, status, currentTask string) {
	s := f.sources[si]
	s.status, s.currentTask = status, currentTask
}

// script runs steps[i], with the fake locked, before the i+1-th request of
// route from now on is served; requests after the last step are served as
// they are.
func (f *fakeKopia) script(route string, steps ...func(f *fakeKopia)) {
	n := 0

	f.set(func(f *fakeKopia) {
		prev := f.hook
		f.hook = func(r fakeRequest) {
			if prev != nil {
				prev(r)
			}

			if r.Route != route {
				return
			}

			if n++; n <= len(steps) && steps[n-1] != nil {
				f.set(steps[n-1])
			}
		}
	})
}

// newWaitBackend returns a Backend whose clock says t0 and whose polls do
// not wait; delays records the intervals it would have waited.
func newWaitBackend(t *testing.T, f *fakeKopia) (*Backend, *[]time.Duration) {
	t.Helper()

	b := newBackend(t, f)
	b.now = func() time.Time { return t0 }

	var (
		mu     sync.Mutex
		delays []time.Duration
	)

	b.after = func(d time.Duration) <-chan time.Time {
		mu.Lock()
		delays = append(delays, d)
		mu.Unlock()

		ch := make(chan time.Time, 1)
		ch <- t0

		return ch
	}

	return b, &delays
}

// waitCtx bounds a WaitSnapshot of a test, which would otherwise poll
// until the test binary times out when a regression keeps it waiting.
func waitCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}

// saves records the tickets WaitSnapshot saves.
type saves struct {
	mu      sync.Mutex
	tickets []backend.SnapshotTicket
}

func (s *saves) save(t backend.SnapshotTicket) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t.TasksBefore = slices.Clone(t.TasksBefore)
	s.tickets = append(s.tickets, t)
}

func (s *saves) all() []backend.SnapshotTicket {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.tickets)
}

func snapshot(id string, start, end time.Time, incomplete string) kopiaclient.Snapshot {
	return kopiaclient.Snapshot{ID: id, StartTime: start, EndTime: end, IncompleteReason: incomplete, RootID: "root-" + id,
		Summary: &kopiaclient.DirectorySummary{}}
}

func described(s kopiaclient.Snapshot, d string) kopiaclient.Snapshot {
	s.Description = d

	return s
}

func assertResult(t *testing.T, got, want backend.SnapshotResult) {
	t.Helper()

	if got.Outcome != want.Outcome || got.TaskID != want.TaskID || !reflect.DeepEqual(got.Snapshot, want.Snapshot) {
		t.Errorf("result = %+v (snapshot %+v)\nwant %+v (snapshot %+v)", got, got.Snapshot, want, want.Snapshot)
	}
}

// A run that waits for an upload slot, uploads past a checkpoint and
// succeeds.
func TestWaitSnapshotSucceeded(t *testing.T) {
	f := newFakeKopia()
	b, delays := newWaitBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")

	old := t0.Add(-24 * time.Hour)
	f.addSource(si, &kopiaclient.SnapshotManifest{StartTime: old, EndTime: old.Add(time.Minute)})
	f.addSource(info("/data/app2"), nil)
	f.set(func(f *fakeKopia) {
		f.snapshots[si] = []kopiaclient.Snapshot{snapshot("s0", old, old.Add(time.Minute), "")}
		f.tasks = []kopiaclient.TaskInfo{
			snapTask("5", "/data/app2", t0.Add(-time.Second), kopiaclient.TaskRunning),
			snapTask("3", "/data/app", old, kopiaclient.TaskSuccess),
		}
	})

	ticket, err := b.TriggerSnapshot(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(ticket.TasksBefore, []string{"3"}) || !ticket.LastSnapshotStart.Equal(old) || !ticket.TriggeredAt.Equal(t0) {
		t.Fatalf("ticket = %+v", ticket)
	}

	start := t0.Add(2 * time.Second)

	f.resetRequests()
	f.script("GET sources",
		// The identity check: waiting for an upload slot, no task yet.
		func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusPending, "") },
		// The run uploads.
		func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusUploading, "6")
			f.start(snapTask("6", "/data/app", t0.Add(time.Second), kopiaclient.TaskRunning))
		},
	)
	f.script("GET tasks/6",
		nil,
		func(f *fakeKopia) {
			f.finish("6", kopiaclient.TaskSuccess, "")
			f.setStatus(si, kopiaclient.SourceStatusIdle, "")
			f.snapshots[si] = append(f.snapshots[si],
				snapshot("cp", start, start.Add(30*time.Second), incompleteCheckpoint),
				snapshot("s1", start, start.Add(50*time.Second), ""))
		},
	)

	var sv saves

	res, err := b.WaitSnapshot(waitCtx(t), ticket, sv.save)
	if err != nil {
		t.Fatal(err)
	}

	assertResult(t, res, backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskID: "6",
		Snapshot: &backend.SnapshotInfo{StartTime: start, EndTime: start.Add(50 * time.Second)}})

	if res.Error != "" {
		t.Errorf("Error = %q", res.Error)
	}

	// The task's times, which f.finish puts a minute apart.
	if !res.TaskStart.Equal(t0.Add(time.Second)) || !res.TaskEnd.Equal(t0.Add(time.Second+time.Minute)) {
		t.Errorf("task times = %v, %v", res.TaskStart, res.TaskEnd)
	}

	want := ticket
	want.TaskID, want.TaskDescription = "6", taskDesc("/data/app", t0.Add(time.Second))

	if got := sv.all(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Errorf("saved %+v\nwant one %+v", got, want)
	}

	assertRoutes(t, f, "GET sources", "GET tasks", "GET sources", "GET tasks", "GET tasks/6", "GET tasks/6", "GET snapshots")

	if r := f.requestsTo("GET snapshots"); len(r) != 1 || target(r[0].Query) != si || r[0].Query.Get("all") != "1" {
		t.Errorf("snapshot listing = %+v", r)
	}

	// The interval doubles from pollMin up to pollMax.
	b.pollMin, b.pollMax = time.Second, 3*time.Second
	*delays = nil

	f.set(func(f *fakeKopia) {
		f.hook = nil
		f.tasks[0].Status, f.tasks[0].EndTime = kopiaclient.TaskRunning, nil
	})
	f.script("GET tasks/6", nil, nil, nil, func(f *fakeKopia) { f.finish("6", kopiaclient.TaskSuccess, "") })

	if _, err := b.WaitSnapshot(waitCtx(t), want, nil); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(*delays, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second}) {
		t.Errorf("delays = %v", *delays)
	}
}

// A run that starts and fails between two polls is found among the
// finished tasks.
func TestWaitSnapshotFailedBetweenPolls(t *testing.T) {
	for _, logFails := range []bool{false, true} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)
		s := src("/data/app")
		si := info("/data/app")

		var logged syncBuffer

		b.log = slog.New(slog.NewTextHandler(&logged, nil))

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusPending, "")
			f.taskLogs["7"] = []string{"1|uploading", `3|Error when processing "db": permission denied`, "2|slow", "3|upload error: boom"}
		})

		ticket := backend.SnapshotTicket{Source: s, TriggeredAt: t0}

		f.script("GET sources", nil, func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusIdle, "")
			f.start(snapTask("7", "/data/app", t0, kopiaclient.TaskRunning))
			f.finish("7", kopiaclient.TaskFailed, "upload error: boom")
		})

		if logFails {
			f.inject("GET tasks/7/logs", injection{status: http.StatusBadGateway, body: "bad gateway"})
		}

		var sv saves

		res, err := b.WaitSnapshot(waitCtx(t), ticket, sv.save)
		if err != nil {
			t.Fatal(err)
		}

		assertResult(t, res, backend.SnapshotResult{Outcome: backend.SnapshotFailed, TaskID: "7"})

		if !res.TaskStart.Equal(t0) || !res.TaskEnd.Equal(t0.Add(time.Minute)) {
			t.Errorf("task times = %v, %v", res.TaskStart, res.TaskEnd)
		}

		want := `upload error: boom; task log: Error when processing "db": permission denied; upload error: boom`
		if logFails {
			want = "upload error: boom"

			if !strings.Contains(logged.String(), "could not read the log") {
				t.Errorf("log = %s", logged.String())
			}
		}

		if res.Error != want {
			t.Errorf("Error = %q\nwant %q", res.Error, want)
		}

		if got := sv.all(); len(got) != 1 || got[0].TaskID != "7" {
			t.Errorf("saved %+v", got)
		}

		assertRoutes(t, f, "GET sources", "GET tasks", "GET sources", "GET tasks", "GET tasks/7/logs")
	}
}

// A failed task without an error message or error lines in its log.
func TestWaitSnapshotFailedQuietly(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	si := info("/data/app")

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{snapTask("2", "/data/app", t0, kopiaclient.TaskFailed)}
		f.taskLogs["2"] = []string{"0|debug", "1|info", "2|warning"}
	})

	res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if res.Outcome != backend.SnapshotFailed || res.Error != "the task failed without an error message" {
		t.Errorf("result = %+v", res)
	}

	// At most the last few error lines.
	f.set(func(f *fakeKopia) {
		f.taskLogs["2"] = nil
		for _, l := range []string{"a", "b", "c", "d", "e", "f", "g"} {
			f.taskLogs["2"] = append(f.taskLogs["2"], "3|"+l)
		}
	})

	res, err = b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if want := "the task failed without an error message; task log: c; d; e; f; g"; res.Error != want {
		t.Errorf("Error = %q\nwant %q", res.Error, want)
	}
}

// Triggered while the source uploads, the run queued after the current one
// is followed, not the current one.
func TestWaitSnapshotQueuedBehindUpload(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.setStatus(si, kopiaclient.SourceStatusUploading, "4")
		f.tasks = []kopiaclient.TaskInfo{snapTask("4", "/data/app", t0.Add(-time.Hour), kopiaclient.TaskRunning)}
	})

	ticket, err := b.TriggerSnapshot(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(ticket.TasksBefore, []string{"4"}) {
		t.Fatalf("TasksBefore = %v", ticket.TasksBefore)
	}

	f.resetRequests()
	f.script("GET sources",
		nil,
		nil,
		// The run before ends; the queued one waits for its slot.
		func(f *fakeKopia) {
			f.finish("4", kopiaclient.TaskSuccess, "")
			f.setStatus(si, kopiaclient.SourceStatusPending, "")
			f.snapshots[si] = []kopiaclient.Snapshot{snapshot("prev", t0.Add(-time.Hour), t0.Add(-time.Minute), "")}
		},
		nil,
		func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusUploading, "5")
			f.start(snapTask("5", "/data/app", t0.Add(time.Minute), kopiaclient.TaskRunning))
		},
	)
	f.script("GET tasks/5", func(f *fakeKopia) {
		f.finish("5", kopiaclient.TaskSuccess, "")
		f.snapshots[si] = append(f.snapshots[si], snapshot("ours", t0.Add(61*time.Second), t0.Add(90*time.Second), ""))
	})

	res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
	if err != nil {
		t.Fatal(err)
	}

	assertResult(t, res, backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskID: "5",
		Snapshot: &backend.SnapshotInfo{StartTime: t0.Add(61 * time.Second), EndTime: t0.Add(90 * time.Second)}})

	if n := len(f.requestsTo("GET sources")); n != 5 {
		t.Errorf("%d polls of the status before the task showed, want 5", n)
	}
}

// While the source waits for an upload slot there is no task: the wait
// goes on, however long, and the source is never taken for idle.
func TestWaitSnapshotPendingWithoutTask(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	si := info("/data/app")

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusPending, "") })

	steps := make([]func(*fakeKopia), 20)
	steps[19] = func(f *fakeKopia) {
		f.start(snapTask("1", "/data/app", t0.Add(time.Hour), kopiaclient.TaskRunning))
		f.finish("1", kopiaclient.TaskCanceled, "")
	}
	f.script("GET sources", steps...)

	res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil {
		t.Fatal(err)
	}

	assertResult(t, res, backend.SnapshotResult{Outcome: backend.SnapshotCanceled, TaskID: "1"})

	if n := len(f.requestsTo("GET tasks")); n != 20 {
		t.Errorf("%d task listings, want 20", n)
	}
}

// A run kopia server dropped, e.g. when it restarted, leaves the source
// idle without a task: after two such polls in a row the outcome is
// unknown. One idle poll between two runs is not enough.
func TestWaitSnapshotDropped(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}

	for _, status := range []string{
		kopiaclient.SourceStatusIdle, kopiaclient.SourceStatusPaused, kopiaclient.SourceStatusStopped,
		kopiaclient.SourceStatusRemote, "",
	} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		if status != "" {
			f.addSource(si, nil)
			f.set(func(f *fakeKopia) { f.setStatus(si, status, "") })
		}

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil {
			t.Fatal(err)
		}

		if res.Outcome != backend.SnapshotUnknown || res.TaskID != "" || res.Error == "" {
			t.Errorf("%q: result = %+v", status, res)
		}

		assertRoutes(t, f, "GET sources", "GET tasks", "GET sources", "GET tasks")
	}

	// Idle for a moment between two runs, then the queued run starts.
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)

	f.addSource(si, nil)
	f.script("GET sources",
		nil,
		func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusPending, "") },
		func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusIdle, "") },
		func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusUploading, "9")
			f.start(snapTask("9", "/data/app", t0, kopiaclient.TaskRunning))
			f.finish("9", kopiaclient.TaskCanceled, "")
		},
	)

	res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
	if err != nil || res.Outcome != backend.SnapshotCanceled || res.TaskID != "9" {
		t.Errorf("result = %+v, %v", res, err)
	}

	// Pending and uploading, the run is queued.
	for _, status := range []string{kopiaclient.SourceStatusPending, kopiaclient.SourceStatusUploading} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) { f.setStatus(si, status, "") })
		f.script("GET sources", nil, nil, nil, func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusIdle, "")
		})

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown {
			t.Errorf("%q: result = %+v, %v", status, res, err)
		}

		if n := len(f.requestsTo("GET sources")); n != 5 {
			t.Errorf("%q: %d polls, want 5", status, n)
		}
	}

	// The states between runs, and FAILED, which a failed refresh of the
	// source's status leaves until the next run, hide a pending run only
	// while a snapshot task holds an upload slot.
	for _, status := range []string{kopiaclient.SourceStatusInitializing, kopiaclient.SourceStatusFailed, "UNKNOWN"} {
		for _, slot := range []bool{false, true} {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) {
				f.setStatus(si, status, "")
				f.tasks = []kopiaclient.TaskInfo{
					{ID: "8", Kind: "Maintenance", Description: "Periodic maintenance", StartTime: t0, Status: kopiaclient.TaskRunning},
					snapTask("7", "/data/other", t0, kopiaclient.TaskSuccess),
				}

				if slot {
					f.start(snapTask("9", "/data/other", t0, kopiaclient.TaskRunning))
				}
			})

			polls := 2
			if slot {
				polls = 5

				f.script("GET sources", nil, nil, nil, func(f *fakeKopia) { f.finish("9", kopiaclient.TaskSuccess, "") })
			}

			res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
			if err != nil || res.Outcome != backend.SnapshotUnknown {
				t.Errorf("%q, slot held %v: result = %+v, %v", status, slot, res, err)
			}

			if n := len(f.requestsTo("GET sources")); n != polls {
				t.Errorf("%q, slot held %v: %d polls, want %d", status, slot, n, polls)
			}
		}
	}
}

// Which task is the run's: a snapshot task of exactly this source, not one
// listed before the trigger, not started before it (give or take the clock
// skew), and of those the earliest.
func TestWaitSnapshotPicksTheRunsTask(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0, TasksBefore: []string{"10"}}

	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)

	f.addSource(si, nil)

	noise := []kopiaclient.TaskInfo{
		// Listed before the trigger.
		snapTask("10", "/data/app", t0.Add(time.Second), kopiaclient.TaskSuccess),
		// Another source, and one whose path starts with this one's.
		snapTask("11", "/data/app2", t0.Add(time.Second), kopiaclient.TaskSuccess),
		{ID: "12", Kind: kopiaclient.TaskKindSnapshot, Description: taskDesc("/data/app at 2026-10-18T09:00:00Z", t0), StartTime: t0.Add(time.Second),
			Status: kopiaclient.TaskSuccess, EndTime: new(t0.Add(time.Minute))},
		// Another user and another host.
		{ID: "13", Kind: kopiaclient.TaskKindSnapshot, Description: "other@kopia:/data/app at 2026-10-18T09:00:01Z", StartTime: t0.Add(time.Second),
			Status: kopiaclient.TaskSuccess, EndTime: new(t0.Add(time.Minute))},
		{ID: "14", Kind: kopiaclient.TaskKindSnapshot, Description: "root@laptop:/data/app at 2026-10-18T09:00:01Z", StartTime: t0.Add(time.Second),
			Status: kopiaclient.TaskSuccess, EndTime: new(t0.Add(time.Minute))},
		// Another kind with the description of this source.
		{ID: "15", Kind: "Maintenance", Description: taskDesc("/data/app", t0), StartTime: t0.Add(time.Second),
			Status: kopiaclient.TaskSuccess, EndTime: new(t0.Add(time.Minute))},
		// No time in the description.
		{ID: "16", Kind: kopiaclient.TaskKindSnapshot, Description: "root@kopia:/data/app at noon", StartTime: t0.Add(time.Second),
			Status: kopiaclient.TaskSuccess, EndTime: new(t0.Add(time.Minute))},
		// Started before the trigger, beyond the clock skew.
		snapTask("17", "/data/app", t0.Add(-clockSkew-time.Second), kopiaclient.TaskSuccess),
	}

	// None of them is the run's.
	f.set(func(f *fakeKopia) { f.tasks = slices.Clone(noise) })

	res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
	if err != nil || res.Outcome != backend.SnapshotUnknown {
		t.Fatalf("result = %+v, %v", res, err)
	}

	// Within the clock skew, and of two the earlier; of two that started
	// at once, the one listed later, which kopia server created first.
	for _, tc := range []struct {
		tasks []kopiaclient.TaskInfo
		want  string
	}{
		{[]kopiaclient.TaskInfo{snapTask("20", "/data/app", t0.Add(-clockSkew), kopiaclient.TaskCanceled)}, "20"},
		{[]kopiaclient.TaskInfo{
			snapTask("22", "/data/app", t0.Add(2*time.Second), kopiaclient.TaskCanceled),
			snapTask("21", "/data/app", t0.Add(time.Second), kopiaclient.TaskCanceled),
		}, "21"},
		{[]kopiaclient.TaskInfo{
			snapTask("21", "/data/app", t0.Add(time.Second), kopiaclient.TaskCanceled),
			snapTask("22", "/data/app", t0.Add(2*time.Second), kopiaclient.TaskCanceled),
		}, "21"},
		{[]kopiaclient.TaskInfo{
			snapTask("24", "/data/app", t0.Add(time.Second), kopiaclient.TaskCanceled),
			snapTask("23", "/data/app", t0.Add(time.Second), kopiaclient.TaskCanceled),
		}, "23"},
	} {
		f.set(func(f *fakeKopia) { f.tasks = append(slices.Clone(tc.tasks), noise...) })

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil || res.TaskID != tc.want {
			t.Errorf("%v: result = %+v, %v; want task %s", tc.tasks, res, err, tc.want)
		}
	}
}

// The snapshot of a task that succeeded: the earliest that is not a
// checkpoint and started within the task.
func TestWaitSnapshotFindsTheSnapshot(t *testing.T) {
	si := info("/data/app")
	start := t0.Add(time.Second) // the task's start; it ends a minute later
	in := start.Add(time.Second)

	tests := []struct {
		name  string
		snaps []kopiaclient.Snapshot
		want  backend.SnapshotResult
		err   string
	}{
		{"one", []kopiaclient.Snapshot{snapshot("a", in, in.Add(time.Second), "")},
			backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(time.Second)}}, ""},
		{"checkpoints and older snapshots are not it", []kopiaclient.Snapshot{
			snapshot("old", start.Add(-time.Nanosecond), in, ""),
			snapshot("cp", in, in.Add(time.Second), incompleteCheckpoint),
			snapshot("a", in, in.Add(2*time.Second), ""),
			snapshot("later", start.Add(time.Minute+time.Nanosecond), start.Add(2*time.Minute), ""),
		}, backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(2 * time.Second)}}, ""},
		{"at the edges of the task", []kopiaclient.Snapshot{snapshot("a", start, start, "")},
			backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: start, EndTime: start}}, ""},
		{"the end of the task", []kopiaclient.Snapshot{snapshot("a", start.Add(time.Minute), start.Add(time.Minute), "")},
			backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: start.Add(time.Minute), EndTime: start.Add(time.Minute)}}, ""},
		{"the earliest of two", []kopiaclient.Snapshot{
			snapshot("b", in.Add(time.Second), in.Add(2*time.Second), ""),
			snapshot("a", in, in.Add(3*time.Second), ""),
		}, backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(3 * time.Second)}}, ""},
		{"of two started at once, the one that ended first", []kopiaclient.Snapshot{
			snapshot("b", in, in.Add(3*time.Second), "x"),
			snapshot("a", in, in.Add(2*time.Second), ""),
		}, backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(2 * time.Second)}}, ""},
		// kopia server skipped a snapshot identical to the last one; a
		// long upload may have saved checkpoints before.
		{"identical", []kopiaclient.Snapshot{
			snapshot("old", t0.Add(-time.Hour), t0.Add(-time.Hour), ""),
			snapshot("cp", in, in.Add(time.Second), incompleteCheckpoint),
		}, backend.SnapshotResult{Outcome: backend.SnapshotIdentical}, ""},
		// Without an earlier snapshot nothing can be identical: the
		// snapshot was deleted since, e.g. by the retention of a run right
		// after.
		{"none at all", nil, backend.SnapshotResult{Outcome: backend.SnapshotUnknown},
			"the task saved no snapshot, and the source has no complete one it could be identical to: the snapshot has been deleted since"},
		{"only later ones", []kopiaclient.Snapshot{snapshot("later", start.Add(time.Minute+time.Nanosecond), start.Add(2*time.Minute), "")},
			backend.SnapshotResult{Outcome: backend.SnapshotUnknown},
			"the task saved no snapshot, and the source has no complete one it could be identical to: the snapshot has been deleted since"},
		// kopia server compares a new snapshot with the latest complete
		// one: never identical to a checkpoint or a canceled snapshot.
		{"only incomplete ones before", []kopiaclient.Snapshot{
			snapshot("cp", start.Add(-time.Hour), start.Add(-time.Hour), incompleteCheckpoint),
			snapshot("canceled", start.Add(-time.Hour), start.Add(-time.Hour), "canceled"),
		}, backend.SnapshotResult{Outcome: backend.SnapshotUnknown},
			"the task saved no snapshot, and the source has no complete one it could be identical to: the snapshot has been deleted since"},
		// The upper bound alone: a later run's snapshot is not the task's.
		{"identical, then a later run", []kopiaclient.Snapshot{
			snapshot("old", start.Add(-time.Hour), start.Add(-time.Hour), ""),
			snapshot("later", start.Add(time.Minute+time.Nanosecond), start.Add(2*time.Minute), ""),
		}, backend.SnapshotResult{Outcome: backend.SnapshotIdentical}, ""},
		// Canceled through sources/cancel or a pause, the task succeeds.
		{"canceled", []kopiaclient.Snapshot{snapshot("a", in, in.Add(time.Second), "canceled")},
			backend.SnapshotResult{Outcome: backend.SnapshotCanceled,
				Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(time.Second), Incomplete: "canceled"}},
			"the upload was canceled: the snapshot is incomplete"},
		{"limit reached", []kopiaclient.Snapshot{snapshot("a", in, in.Add(time.Second), "limit reached")},
			backend.SnapshotResult{Outcome: backend.SnapshotIncomplete,
				Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in.Add(time.Second), Incomplete: "limit reached"}},
			"the snapshot is incomplete: limit reached"},
		{"with a description", []kopiaclient.Snapshot{described(snapshot("a", in, in, ""), "set in the web UI")},
			backend.SnapshotResult{Outcome: backend.SnapshotSucceeded,
				Snapshot: &backend.SnapshotInfo{StartTime: in, EndTime: in, Description: "set in the web UI"}}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) {
				f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", start, kopiaclient.TaskSuccess)}
				f.snapshots[si] = tt.snaps
			})

			res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
			if err != nil {
				t.Fatal(err)
			}

			tt.want.TaskID = "1"
			assertResult(t, res, tt.want)

			if res.Error != tt.err {
				t.Errorf("Error = %q, want %q", res.Error, tt.err)
			}
		})
	}
}

// Errors of files kopia server could not read make a saved snapshot
// incomplete; errors the policy ignores do not.
func TestWaitSnapshotErrorsInTheSnapshot(t *testing.T) {
	si := info("/data/app")
	in := t0.Add(time.Second)

	withErrors := snapshot("a", in, in, "")
	withErrors.Summary = &kopiaclient.DirectorySummary{FatalErrorCount: 7, IgnoredErrorCount: 1}

	for _, p := range []string{"a", "b", "c", "d", "e", "f"} {
		withErrors.Summary.FailedEntries = append(withErrors.Summary.FailedEntries, struct {
			EntryPath string `json:"path"`
			Error     string `json:"error"`
		}{EntryPath: p, Error: "permission denied"})
	}

	// kopia server lists the entries of ignored errors as failed too.
	ignored := snapshot("a", in, in, "")
	ignored.Summary = &kopiaclient.DirectorySummary{IgnoredErrorCount: 1, FailedEntries: withErrors.Summary.FailedEntries[:1]}

	one := snapshot("a", in, in, "")
	one.Summary = &kopiaclient.DirectorySummary{FatalErrorCount: 1}
	one.Summary.FailedEntries = append(one.Summary.FailedEntries, struct {
		EntryPath string `json:"path"`
		Error     string `json:"error"`
	}{EntryPath: "x", Error: "permission denied"})

	noSummary := snapshot("a", in, in, "")
	noSummary.Summary = nil

	tests := []struct {
		snap    kopiaclient.Snapshot
		outcome backend.SnapshotOutcome
		err     string
	}{
		{withErrors, backend.SnapshotIncomplete,
			"the snapshot has 7 errors and 1 ignored; failed entries, ignored ones included: a: permission denied; b: permission denied; " +
				"c: permission denied; d: permission denied; e: permission denied"},
		{one, backend.SnapshotIncomplete, "the snapshot has 1 error; failed entries: x: permission denied"},
		{ignored, backend.SnapshotSucceeded, ""},
		{noSummary, backend.SnapshotSucceeded, ""},
	}

	for _, tt := range tests {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", t0, kopiaclient.TaskSuccess)}
			f.snapshots[si] = []kopiaclient.Snapshot{tt.snap}
		})

		res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
		if err != nil {
			t.Fatal(err)
		}

		if res.Outcome != tt.outcome || res.Error != tt.err || res.Snapshot == nil {
			t.Errorf("result = %+v\nwant %s, %q", res, tt.outcome, tt.err)
		}
	}

	// Errors without failed entries.
	withErrors.Summary.FailedEntries = nil

	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", t0, kopiaclient.TaskSuccess)}
		f.snapshots[si] = []kopiaclient.Snapshot{withErrors}
	})

	res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil || res.Outcome != backend.SnapshotIncomplete || res.Error != "the snapshot has 7 errors and 1 ignored" {
		t.Errorf("result = %+v, %v", res, err)
	}
}

// A waiting watcher restarts with the ticket it saved and reads the task
// alone. kopia server numbers its tasks anew when it restarts: an ID that
// is gone, or that another task has now, is an unknown outcome.
func TestWaitSnapshotKnownTask(t *testing.T) {
	si := info("/data/app")
	start := t0.Add(time.Second)
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0, TasksBefore: []string{"1"},
		TaskID: "6", TaskDescription: taskDesc("/data/app", start)}

	t.Run("still running, then done", func(t *testing.T) {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusIdle, "") // never consulted
			f.tasks = []kopiaclient.TaskInfo{snapTask("6", "/data/app", start, kopiaclient.TaskRunning)}
		})
		f.script("GET tasks/6", nil, func(f *fakeKopia) {
			f.finish("6", kopiaclient.TaskSuccess, "")
			f.snapshots[si] = []kopiaclient.Snapshot{snapshot("a", start, start, "")}
		})

		var sv saves

		res, err := b.WaitSnapshot(waitCtx(t), ticket, sv.save)
		if err != nil || res.Outcome != backend.SnapshotSucceeded || res.TaskID != "6" {
			t.Errorf("result = %+v, %v", res, err)
		}

		if len(sv.all()) != 0 {
			t.Errorf("saved %+v: nothing new", sv.all())
		}

		assertRoutes(t, f, "GET sources", "GET tasks/6", "GET tasks/6", "GET snapshots")
	})

	t.Run("the ID belongs to another task", func(t *testing.T) {
		for _, other := range []kopiaclient.TaskInfo{
			snapTask("6", "/data/app", t0.Add(time.Hour), kopiaclient.TaskSuccess),
			snapTask("6", "/data/other", start, kopiaclient.TaskSuccess),
			{ID: "6", Kind: "Maintenance", Description: "Periodic maintenance", Status: kopiaclient.TaskRunning},
		} {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) { f.tasks = []kopiaclient.TaskInfo{other} })

			res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
			if err != nil || res.Outcome != backend.SnapshotUnknown || res.TaskID != "6" || !strings.Contains(res.Error, "no longer knows task 6") {
				t.Errorf("%s: result = %+v, %v", other.Description, res, err)
			}

			assertRoutes(t, f, "GET sources", "GET tasks/6")
		}
	})

	t.Run("the task is gone", func(t *testing.T) {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown || res.TaskID != "6" {
			t.Errorf("result = %+v, %v", res, err)
		}

		assertRoutes(t, f, "GET sources", "GET tasks/6")
	})

	// Without a task yet, after a restart of kopia server: the new task
	// of the source has an ID of the old list, so it is not taken for the
	// run's, and the source turns idle.
	t.Run("restart before the task showed", func(t *testing.T) {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", t0.Add(time.Hour), kopiaclient.TaskSuccess)}
		})

		noTask := ticket
		noTask.TaskID, noTask.TaskDescription = "", ""

		res, err := b.WaitSnapshot(waitCtx(t), noTask, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown || res.TaskID != "" {
			t.Errorf("result = %+v, %v", res, err)
		}
	})
}

// kopia server keeps queued runs in memory and numbers its tasks anew when
// it restarts: the newest task listed before the trigger tells a new run
// of kopia server, whose task of the source is not the run's.
func TestWaitSnapshotRestart(t *testing.T) {
	si := info("/data/app")
	anchorStart := t0.Add(-10 * time.Second)
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0, TasksBefore: []string{"3e5"},
		LastTaskID: "3e7", LastTaskStart: anchorStart}

	maint := func(id string, start time.Time) kopiaclient.TaskInfo {
		end := start.Add(time.Second)

		return kopiaclient.TaskInfo{ID: id, Kind: "Maintenance", Description: "Periodic maintenance", StartTime: start, EndTime: &end,
			Status: kopiaclient.TaskSuccess}
	}

	tests := []struct {
		name  string
		tasks []kopiaclient.TaskInfo
		lost  string // "" if the run is followed
	}{
		{"a new run of kopia server, few tasks yet", []kopiaclient.TaskInfo{
			snapTask("2", "/data/app", t0.Add(10*time.Minute), kopiaclient.TaskSuccess),
			maint("1", t0.Add(5*time.Minute)),
		}, "it lists no task numbered 3e7 or above"},
		{"no tasks at all", nil, "it lists no task numbered 3e7 or above"},
		{"another task under the ID", []kopiaclient.TaskInfo{
			maint("3e8", t0.Add(time.Hour)),
			maint("3e7", t0.Add(time.Hour)),
		}, "task 3e7 is another task now"},
		// The newer tasks of the new run of kopia server finished and
		// pushed the anchor's ID out; the source's long upload still runs.
		{"the run's task numbered below the trigger's", []kopiaclient.TaskInfo{
			maint("3e8", t0.Add(time.Hour)),
			snapTask("300", "/data/app", t0.Add(time.Minute), kopiaclient.TaskRunning),
		}, "the task 300 of the source is numbered at or below 3e7"},
		{"another task under the ID, started earlier", []kopiaclient.TaskInfo{
			maint("3e7", anchorStart.Add(-time.Nanosecond)),
		}, "task 3e7 is another task now"},
		{"the same run of kopia server", []kopiaclient.TaskInfo{
			snapTask("3e8", "/data/app", t0.Add(time.Second), kopiaclient.TaskFailed),
			maint("3e7", anchorStart),
			snapTask("3e5", "/data/app", t0.Add(-time.Hour), kopiaclient.TaskSuccess),
		}, ""},
		// The anchor finished and 50 newer tasks pushed it out.
		{"the anchor pushed out", []kopiaclient.TaskInfo{
			snapTask("3e8", "/data/app", t0.Add(time.Second), kopiaclient.TaskFailed),
		}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) {
				f.setStatus(si, kopiaclient.SourceStatusUploading, "")
				f.tasks = tt.tasks
			})

			var sv saves

			res, err := b.WaitSnapshot(waitCtx(t), ticket, sv.save)
			if err != nil {
				t.Fatal(err)
			}

			if tt.lost == "" {
				if res.Outcome != backend.SnapshotFailed || res.TaskID != "3e8" {
					t.Errorf("result = %+v", res)
				}

				return
			}

			if res.Outcome != backend.SnapshotUnknown || res.TaskID != "" || !strings.Contains(res.Error, tt.lost) ||
				!strings.Contains(res.Error, "kopia server restarted since the trigger") {
				t.Errorf("result = %+v\nwant unknown: %s", res, tt.lost)
			}

			if len(sv.all()) != 0 {
				t.Errorf("saved %+v", sv.all())
			}

			assertRoutes(t, f, "GET sources", "GET tasks")
		})
	}

	// An ID that is not a number tells nothing either way.
	t.Run("an ID that is not a number", func(t *testing.T) {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("run-1", "/data/app", t0.Add(time.Second), kopiaclient.TaskFailed), maint("3e7", anchorStart)}
		})

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotFailed || res.TaskID != "run-1" {
			t.Errorf("result = %+v, %v", res, err)
		}
	})

	// A ticket made before the trigger recorded the newest task follows
	// any new task, as before; one of a kopia server that had run no task
	// yet has nothing to compare with.
	for _, last := range []string{"", "0"} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("2", "/data/app", t0.Add(time.Second), kopiaclient.TaskFailed), maint("1", t0)}
		})

		tk := ticket
		tk.LastTaskID, tk.LastTaskStart = last, time.Time{}

		res, err := b.WaitSnapshot(waitCtx(t), tk, nil)
		if err != nil || res.Outcome != backend.SnapshotFailed || res.TaskID != "2" {
			t.Errorf("LastTaskID %q: result = %+v, %v", last, res, err)
		}
	}
}

// kopia server keeps the 50 finished tasks numbered highest. Each poll
// that does not find the run moves LastTaskID to the newest task listed
// and saves the ticket, so a long backlog of other sources' runs never
// hides the run's task; tasks dropped between two looks might have.
func TestWaitSnapshotEvicted(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0, TasksBefore: []string{"10"},
		LastTaskID: "10", LastTaskStart: t0.Add(-30 * time.Second)}

	// others returns finished snapshot tasks of other sources numbered
	// from to to, newest first, as kopia server lists them.
	others := func(from, to int) []kopiaclient.TaskInfo {
		var out []kopiaclient.TaskInfo

		for id := to; id >= from; id-- {
			out = append(out, snapTask(strconv.FormatInt(int64(id), 16), "/data/other", t0.Add(time.Duration(id)*time.Second), kopiaclient.TaskSuccess))
		}

		return out
	}

	t.Run("a backlog seen poll by poll", func(t *testing.T) {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusPending, "")
			// The source's previous run, which saved a snapshot right
			// before the trigger, and the first runs of the backlog.
			f.tasks = append(others(0x11, 0x2f), snapTask("10", "/data/app", t0.Add(-30*time.Second), kopiaclient.TaskSuccess))
			f.snapshots[si] = []kopiaclient.Snapshot{snapshot("prev", t0.Add(-29*time.Second), t0.Add(-time.Second), "")}
		})
		// The tasks up to 2f are dropped by the next look, which finds the
		// run's task.
		f.script("GET sources", nil, func(f *fakeKopia) {
			f.tasks = append([]kopiaclient.TaskInfo{snapTask("61", "/data/app", t0.Add(time.Hour), kopiaclient.TaskFailed)}, others(0x30, 0x60)...)
		})

		var sv saves

		res, err := b.WaitSnapshot(waitCtx(t), ticket, sv.save)
		if err != nil || res.Outcome != backend.SnapshotFailed || res.TaskID != "61" {
			t.Fatalf("result = %+v, %v", res, err)
		}

		got := sv.all()
		if len(got) != 2 || got[0].TaskID != "" || got[0].LastTaskID != "2f" || !got[0].LastTaskStart.Equal(t0.Add(0x2f*time.Second)) ||
			got[1].TaskID != "61" || got[1].LastTaskID != "2f" {
			t.Errorf("saved %+v", got)
		}
	})

	for _, tc := range []struct {
		name  string
		tasks []kopiaclient.TaskInfo
		lost  bool
	}{
		{"every task since listed", append(append([]kopiaclient.TaskInfo{snapTask("40", "/data/app", t0.Add(time.Hour), kopiaclient.TaskFailed)},
			others(0x11, 0x3f)...), snapTask("10", "/data/app", t0.Add(-30*time.Second), kopiaclient.TaskSuccess)), false},
		// The anchor itself was dropped: nothing above it is missing.
		{"the anchor dropped", append([]kopiaclient.TaskInfo{snapTask("40", "/data/app", t0.Add(time.Hour), kopiaclient.TaskFailed)},
			others(0x11, 0x3f)...), false},
		{"a task above the anchor dropped", append([]kopiaclient.TaskInfo{snapTask("40", "/data/app", t0.Add(time.Hour), kopiaclient.TaskFailed)},
			others(0x12, 0x3f)...), true},
		{"the newest above the anchor dropped", others(0x11, 0x3e), false},
		// The watcher was down while more than 50 tasks finished.
		{"a resumed wait", append([]kopiaclient.TaskInfo{snapTask("90", "/data/app", t0.Add(time.Hour), kopiaclient.TaskFailed)},
			others(0x40, 0x8f)...), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) { f.tasks = tc.tasks })

			res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)

			switch {
			case err != nil:
				t.Fatal(err)
			case tc.lost:
				if res.Outcome != backend.SnapshotUnknown || res.TaskID != "" || !strings.Contains(res.Error, "no longer lists every task since task 10") {
					t.Errorf("result = %+v", res)
				}

				assertRoutes(t, f, "GET sources", "GET tasks")
			case tc.name == "the newest above the anchor dropped":
				// No task of the run, the source idle: dropped.
				if res.Outcome != backend.SnapshotUnknown || !strings.Contains(res.Error, "dropped the run") {
					t.Errorf("result = %+v", res)
				}
			default:
				if res.Outcome != backend.SnapshotFailed {
					t.Errorf("result = %+v", res)
				}
			}
		})
	}
}

// A task that succeeded without uploading anything: kopia server stopped
// the source while the run waited for an upload slot.
func TestWaitSnapshotNotUploaded(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	si := info("/data/app")

	task := snapTask("1", "/data/app", t0, kopiaclient.TaskSuccess)
	task.Counters = nil

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{task}
		f.snapshots[si] = []kopiaclient.Snapshot{snapshot("old", t0.Add(-time.Hour), t0.Add(-time.Hour), "")}
	})

	res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil || res.Outcome != backend.SnapshotUnknown || res.TaskID != "1" || res.Snapshot != nil ||
		res.Error != "the task ended without uploading: kopia server stopped the source before the run" {
		t.Errorf("result = %+v, %v", res, err)
	}
}

// A task canceled through tasks/{id}/cancel: CANCELING while the upload
// stops, then CANCELED, with what the upload had read saved as an
// incomplete snapshot, if anything.
func TestWaitSnapshotCanceledTask(t *testing.T) {
	si := info("/data/app")
	start := t0.Add(time.Second)
	saved := snapshot("a", start.Add(time.Second), start.Add(2*time.Second), "canceled")

	for _, tc := range []struct {
		snaps []kopiaclient.Snapshot
		want  *backend.SnapshotInfo
	}{
		{[]kopiaclient.Snapshot{saved}, &backend.SnapshotInfo{StartTime: saved.StartTime, EndTime: saved.EndTime, Incomplete: "canceled"}},
		{nil, nil},
		// Canceled at the very end, the upload saved a complete snapshot:
		// still canceled, with it.
		{[]kopiaclient.Snapshot{snapshot("b", start.Add(time.Second), start.Add(2*time.Second), "")},
			&backend.SnapshotInfo{StartTime: start.Add(time.Second), EndTime: start.Add(2 * time.Second)}},
	} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", start, kopiaclient.TaskCanceling)}
			f.snapshots[si] = tc.snaps
		})
		f.script("GET tasks/1", nil, func(f *fakeKopia) { f.finish("1", kopiaclient.TaskCanceled, "") })

		res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
		if err != nil {
			t.Fatal(err)
		}

		assertResult(t, res, backend.SnapshotResult{Outcome: backend.SnapshotCanceled, TaskID: "1", Snapshot: tc.want})

		if res.Error != "the task was canceled" {
			t.Errorf("Error = %q", res.Error)
		}

		assertRoutes(t, f, "GET sources", "GET tasks", "GET tasks/1", "GET tasks/1", "GET snapshots")
	}

	// The snapshots cannot be read.
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", start, kopiaclient.TaskCanceled)}
	})
	f.inject("GET snapshots", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`})

	_, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	assertError(t, err, opWaitSnapshot, src("/data/app"), backend.ErrTemporary)
}

// The log of a task that succeeded without a snapshot tells an identical
// snapshot kopia server skipped from one the retention of a later run
// deleted; without it, an earlier complete snapshot decides.
func TestWaitSnapshotUnsavedLog(t *testing.T) {
	si := info("/data/app")
	start := t0.Add(time.Second)
	earlier := snapshot("old", t0.Add(-time.Hour), t0.Add(-time.Hour), "")

	for _, tc := range []struct {
		name  string
		log   []string
		inj   *injection
		snaps []kopiaclient.Snapshot
		want  backend.SnapshotOutcome
	}{
		{"skipped as identical", []string{"0|uploading", "0|Not saving snapshot because no files have been changed since previous snapshot"}, nil,
			nil, backend.SnapshotIdentical},
		{"saved, then deleted", []string{"0|created snapshot k1234"}, nil, []kopiaclient.Snapshot{earlier}, backend.SnapshotUnknown},
		{"a log that tells neither", []string{"1|uploading"}, nil, []kopiaclient.Snapshot{earlier}, backend.SnapshotIdentical},
		{"a log that cannot be read", nil, &injection{status: http.StatusBadGateway, body: "bad gateway"}, []kopiaclient.Snapshot{earlier},
			backend.SnapshotIdentical},
		{"a log that cannot be read, no complete snapshot before", nil, &injection{status: http.StatusBadGateway, body: "bad gateway"},
			nil, backend.SnapshotUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) {
				f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", start, kopiaclient.TaskSuccess)}
				f.snapshots[si] = tc.snaps
				f.taskLogs["1"] = tc.log
			})

			if tc.inj != nil {
				f.inject("GET tasks/1/logs", *tc.inj)
			}

			res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
			if err != nil || res.Outcome != tc.want || res.TaskID != "1" || res.Snapshot != nil || (res.Error == "") != (tc.want == backend.SnapshotIdentical) {
				t.Errorf("result = %+v, %v; want %s", res, err, tc.want)
			}

			assertRoutes(t, f, "GET sources", "GET tasks", "GET snapshots", "GET tasks/1/logs")
		})
	}
}

// After a run, kopia server refreshes the source's status, which can take
// up to 30 seconds, before it starts the run queued after it. A FAILED
// status left by an earlier refresh shows no sign of the queued run then,
// but the end of the source's last task does.
func TestWaitSnapshotRefreshAfterRun(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0.Add(-time.Minute), TasksBefore: []string{"5"}}

	for _, tc := range []struct {
		ended time.Duration // how long before now the source's last task ended
		polls int
	}{
		{20 * time.Second, 5},
		{refreshTime - time.Second, 5},
		{refreshTime, 2},
		{2 * time.Minute, 2},
	} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		task := snapTask("5", "/data/app", t0.Add(-tc.ended-time.Minute), kopiaclient.TaskSuccess)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.setStatus(si, kopiaclient.SourceStatusFailed, "")
			f.tasks = []kopiaclient.TaskInfo{task}
		})
		f.script("GET sources", nil, nil, nil, func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusIdle, "") })

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown {
			t.Errorf("ended %v ago: result = %+v, %v", tc.ended, res, err)
		}

		if n := len(f.requestsTo("GET sources")); n != tc.polls {
			t.Errorf("ended %v ago: %d polls, want %d", tc.ended, n, tc.polls)
		}
	}
}

// A run is queued behind an upload of any source, but not behind a task of
// another kind, and never for a source idle, paused, stopped or read-only.
func TestWaitSnapshotQueuedStates(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}

	for _, tc := range []struct {
		status string
		task   kopiaclient.TaskInfo
		queued bool
	}{
		{kopiaclient.SourceStatusFailed, snapTask("9", "/data/other", t0, kopiaclient.TaskCanceling), true},
		{kopiaclient.SourceStatusFailed, snapTask("9", "/data/other", t0, kopiaclient.TaskRunning), true},
		{kopiaclient.SourceStatusFailed, snapTask("9", "/data/other", t0.Add(-time.Hour), kopiaclient.TaskSuccess), false},
		// Another source's run that ended a moment ago tells nothing.
		{kopiaclient.SourceStatusFailed, snapTask("9", "/data/other", t0.Add(-time.Minute), kopiaclient.TaskSuccess), false},
		{kopiaclient.SourceStatusFailed, kopiaclient.TaskInfo{ID: "9", Kind: "Maintenance", Status: kopiaclient.TaskCanceling}, false},
		{kopiaclient.SourceStatusIdle, snapTask("9", "/data/other", t0, kopiaclient.TaskRunning), false},
		{kopiaclient.SourceStatusPaused, snapTask("9", "/data/other", t0, kopiaclient.TaskRunning), false},
		{kopiaclient.SourceStatusStopped, snapTask("9", "/data/other", t0, kopiaclient.TaskRunning), false},
		{kopiaclient.SourceStatusRemote, snapTask("9", "/data/other", t0, kopiaclient.TaskRunning), false},
	} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.setStatus(si, tc.status, "")
			f.tasks = []kopiaclient.TaskInfo{tc.task}
		})
		f.script("GET sources", nil, nil, nil, func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusIdle, "") })

		res, err := b.WaitSnapshot(waitCtx(t), ticket, nil)

		polls := 2
		if tc.queued {
			polls = 5
		}

		if n := len(f.requestsTo("GET sources")); err != nil || res.Outcome != backend.SnapshotUnknown || n != polls {
			t.Errorf("%s with %s %s: %+v, %v, %d polls; want %d", tc.status, tc.task.Kind, tc.task.Status, res, err, n, polls)
		}
	}
}

// TriggerSnapshot marks the newest task by its number: kopia server prints
// its counter in hexadecimal and lists the newest task first.
func TestTriggerSnapshotAnchor(t *testing.T) {
	for _, tc := range []struct {
		ids   []string
		want  string
		start time.Time
	}{
		{nil, "0", time.Time{}},
		{[]string{"10", "a", "9"}, "10", t0.Add(0x10 * time.Second)},
		{[]string{"a", "9", "10"}, "10", t0.Add(0x10 * time.Second)},
		{[]string{"ff", "100"}, "100", t0.Add(0x100 * time.Second)},
		{[]string{"x", "3"}, "3", t0.Add(3 * time.Second)},
	} {
		f := newFakeKopia()
		b := newBackend(t, f)

		f.addSource(info("/data/app"), nil)
		f.set(func(f *fakeKopia) {
			for _, id := range tc.ids {
				n, _ := strconv.ParseUint(id, 16, 64)
				f.tasks = append(f.tasks, kopiaclient.TaskInfo{ID: id, Kind: "Maintenance", StartTime: t0.Add(time.Duration(n) * time.Second)})
			}
		})

		ticket, err := b.TriggerSnapshot(t.Context(), src("/data/app"))
		if err != nil || ticket.LastTaskID != tc.want || !ticket.LastTaskStart.Equal(tc.start) {
			t.Errorf("%v: ticket = %+v, %v; want LastTaskID %s started %v", tc.ids, ticket, err, tc.want, tc.start)
		}
	}
}

// The clock skew allowed between the watcher and kopia server is a minute.
func TestWaitSnapshotClockSkew(t *testing.T) {
	si := info("/data/app")

	for _, tc := range []struct {
		start time.Time
		found bool
	}{
		{t0.Add(-30 * time.Second), true},
		{t0.Add(-60 * time.Second), true},
		{t0.Add(-61 * time.Second), false},
		{t0.Add(-2 * time.Minute), false},
	} {
		f := newFakeKopia()
		b, _ := newWaitBackend(t, f)

		f.addSource(si, nil)
		f.set(func(f *fakeKopia) {
			f.tasks = []kopiaclient.TaskInfo{snapTask("2", "/data/app", tc.start, kopiaclient.TaskFailed)}
		})

		res, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
		if err != nil || (res.Outcome == backend.SnapshotFailed) != tc.found {
			t.Errorf("task started %v before the trigger: result = %+v, %v", t0.Sub(tc.start), res, err)
		}
	}
}

// The caller stops waiting.
func TestWaitSnapshotContextDone(t *testing.T) {
	si := info("/data/app")
	ticket := backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}

	f := newFakeKopia()
	b := newBackend(t, f)
	b.pollMin, b.pollMax = time.Hour, time.Hour

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusPending, "") })

	ctx, cancel := context.WithCancel(t.Context())
	f.script("GET tasks", func(*fakeKopia) { cancel() })

	_, err := b.WaitSnapshot(ctx, ticket, nil)
	assertError(t, err, opWaitSnapshot, src("/data/app"), nil)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}

	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = b.WaitSnapshot(ctx, ticket, nil)
	assertError(t, err, opWaitSnapshot, src("/data/app"), nil)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

// A request that fails ends the wait with its error; the caller waits
// again with the saved ticket.
func TestWaitSnapshotErrors(t *testing.T) {
	si := info("/data/app")
	s := src("/data/app")
	ticket := backend.SnapshotTicket{Source: s, TriggeredAt: t0}

	tests := []struct {
		name  string
		route string
		inj   injection
		task  kopiaclient.TaskStatus
		known bool
		want  error
	}{
		{"identity check", "GET sources", injection{status: http.StatusServiceUnavailable, body: "unavailable"}, kopiaclient.TaskRunning, false, backend.ErrUnavailable},
		{"tasks", "GET tasks", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`}, kopiaclient.TaskRunning, false, backend.ErrTemporary},
		{"task", "GET tasks/1", injection{err: errors.New("connection reset")}, kopiaclient.TaskRunning, true, backend.ErrUnavailable},
		{"task without a JSON 404", "GET tasks/1", injection{status: http.StatusNotFound, body: "404 page not found"}, kopiaclient.TaskRunning, true, nil},
		{"snapshots", "GET snapshots", injection{status: http.StatusBadRequest, body: `{"code":"NOT_CONNECTED","error":"not connected"}`}, kopiaclient.TaskSuccess, false, backend.ErrUnavailable},
		{"unknown task status", "", injection{}, "PAUSED", false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeKopia()
			b, _ := newWaitBackend(t, f)

			f.addSource(si, nil)
			f.set(func(f *fakeKopia) {
				f.setStatus(si, kopiaclient.SourceStatusPending, "")
				f.tasks = []kopiaclient.TaskInfo{snapTask("1", "/data/app", t0, tt.task)}
			})

			tk := ticket
			if tt.known {
				tk.TaskID, tk.TaskDescription = "1", taskDesc("/data/app", t0)
			}

			if tt.route != "" {
				f.inject(tt.route, tt.inj)
			}

			res, err := b.WaitSnapshot(waitCtx(t), tk, nil)
			if err == nil {
				t.Fatalf("result = %+v, no error", res)
			}

			assertError(t, err, opWaitSnapshot, s, tt.want)

			if tt.task == "PAUSED" && !errors.Is(err, kopiaclient.ErrUnexpectedResponse) {
				t.Errorf("err = %v, want ErrUnexpectedResponse", err)
			}
		})
	}
}

// The status read of a later poll fails.
func TestWaitSnapshotPollFails(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	si := info("/data/app")

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) { f.setStatus(si, kopiaclient.SourceStatusPending, "") })
	// An injection made while the first request is served answers the
	// second.
	f.script("GET sources", func(f *fakeKopia) {
		f.injections["GET sources"] = append(f.injections["GET sources"], injection{status: http.StatusBadRequest,
			body: `{"code":"NOT_CONNECTED","error":"not connected"}`})
	})

	_, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	assertError(t, err, opWaitSnapshot, src("/data/app"), backend.ErrUnavailable)
	assertRoutes(t, f, "GET sources", "GET tasks", "GET sources")
}

// The caller stops waiting while the log of a failed task is read: the
// outcome is known, and the log is left out quietly.
func TestWaitSnapshotFailedLogCanceled(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)
	si := info("/data/app")

	var logged syncBuffer

	b.log = slog.New(slog.NewTextHandler(&logged, nil))

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{snapTask("2", "/data/app", t0, kopiaclient.TaskFailed)}
		f.tasks[0].ErrorMessage = "upload error: boom"
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	f.script("GET tasks/2/logs", func(*fakeKopia) { cancel() })
	f.inject("GET tasks/2/logs", injection{err: context.Canceled})

	res, err := b.WaitSnapshot(ctx, backend.SnapshotTicket{Source: src("/data/app"), TriggeredAt: t0}, nil)
	if err != nil || res.Outcome != backend.SnapshotFailed || res.Error != "upload error: boom" {
		t.Errorf("result = %+v, %v", res, err)
	}

	if logged.String() != "" {
		t.Errorf("log = %s", logged.String())
	}
}

// WaitSnapshot checks the source like every method: complete, absolute,
// kopia server's own.
func TestWaitSnapshotSourceChecks(t *testing.T) {
	f := newFakeKopia()
	b, _ := newWaitBackend(t, f)

	for _, s := range []backend.Source{{Host: fakeHost, UserName: fakeUser}, src("data/app")} {
		_, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: s, TaskID: "1"}, nil)
		assertError(t, err, opWaitSnapshot, s, nil)
	}

	assertRoutes(t, f)

	other := backend.Source{Host: "laptop", UserName: fakeUser, Path: "/data/app"}

	_, err := b.WaitSnapshot(waitCtx(t), backend.SnapshotTicket{Source: other, TaskID: "1"}, nil)
	if !errors.Is(err, backend.ErrWrongIdentity) {
		t.Errorf("err = %v, want ErrWrongIdentity", err)
	}

	assertRoutes(t, f, "GET sources")
}

// A source whose status is PAUSED is triggered all the same: kopia server
// shows PAUSED once it dropped a run of a source paused through the control
// API, and still after the source is resumed.
func TestTriggerSnapshotPaused(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	s := f.addSource(info("/data/app"), nil)

	for _, status := range []string{kopiaclient.SourceStatusPaused, kopiaclient.SourceStatusPending, kopiaclient.SourceStatusUploading} {
		s.status = status

		if _, err := b.TriggerSnapshot(t.Context(), src("/data/app")); err != nil {
			t.Errorf("%s: %v", status, err)
		}
	}

	if n := f.uploads(info("/data/app")); n != 3 {
		t.Errorf("%d uploads, want 3", n)
	}
}

func editRequests(t *testing.T, f *fakeKopia) []kopiaclient.EditSnapshotsRequest {
	t.Helper()

	var out []kopiaclient.EditSnapshotsRequest

	for _, r := range f.requestsTo("POST snapshots/edit") {
		var req kopiaclient.EditSnapshotsRequest
		if err := json.Unmarshal(r.Body, &req); err != nil {
			t.Fatal(err)
		}

		out = append(out, req)
	}

	return out
}

func (f *fakeKopia) snapshotAt(si kopiaclient.SourceInfo, id string) kopiaclient.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range f.snapshots[si] {
		if s.ID == id {
			return s
		}
	}

	panic("fakeKopia: no snapshot " + id)
}

// AnnotateSnapshot finds the snapshot by its start time, right before each
// edit, since an edit gives it a new ID, and edits only what changes.
func TestAnnotateSnapshot(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")
	start := t0.Add(time.Second)

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.snapshots[si] = []kopiaclient.Snapshot{
			snapshot("old", t0.Add(-time.Hour), t0.Add(-time.Hour), ""),
			snapshot("cp", start, start.Add(10*time.Second), incompleteCheckpoint),
			snapshot("late", start, start.Add(50*time.Second), ""),
			snapshot("first", start, start.Add(40*time.Second), ""),
			snapshot("next", start.Add(time.Nanosecond), start.Add(time.Minute), ""),
		}
		f.snapshots[info("/data/app2")] = []kopiaclient.Snapshot{snapshot("other", start, start, "")}
	})

	if err := b.AnnotateSnapshot(t.Context(), s, start, "app [env=prod]", []string{"keep", "keep", "update"}); err != nil {
		t.Fatal(err)
	}

	desc := "app [env=prod]"
	want := []kopiaclient.EditSnapshotsRequest{{Snapshots: []string{"first"}, Description: &desc, AddPins: []string{"keep", "update"}}}

	if got := editRequests(t, f); !reflect.DeepEqual(got, want) {
		t.Fatalf("edits = %+v\nwant %+v", got, want)
	}

	if r := f.requestsTo("GET snapshots"); len(r) != 1 || target(r[0].Query) != si || r[0].Query.Get("all") != "1" {
		t.Errorf("listing = %+v", r)
	}

	edited := f.snapshotAt(si, "edited-1")
	if edited.Description != desc || !slices.Equal(edited.Pins, []string{"keep", "update"}) {
		t.Errorf("snapshot = %+v", edited)
	}

	// Again: nothing changes, nothing is sent.
	f.resetRequests()

	if err := b.AnnotateSnapshot(t.Context(), s, start, desc, []string{"update"}); err != nil {
		t.Fatal(err)
	}

	assertRoutes(t, f, "GET sources", "GET snapshots")

	// A new pin: the snapshot is found under its new ID, and the
	// description, unchanged, is left out.
	f.resetRequests()

	if err := b.AnnotateSnapshot(t.Context(), s, start, desc, []string{"keep", "pin-on-stop"}); err != nil {
		t.Fatal(err)
	}

	want = []kopiaclient.EditSnapshotsRequest{{Snapshots: []string{"edited-1"}, AddPins: []string{"pin-on-stop"}}}
	if got := editRequests(t, f); !reflect.DeepEqual(got, want) {
		t.Errorf("edits = %+v\nwant %+v", got, want)
	}

	// A new description alone.
	f.resetRequests()

	if err := b.AnnotateSnapshot(t.Context(), s, start, "app", nil); err != nil {
		t.Fatal(err)
	}

	if got := editRequests(t, f); len(got) != 1 || got[0].Description == nil || *got[0].Description != "app" || len(got[0].AddPins) != 0 {
		t.Errorf("edits = %+v", got)
	}

	// An empty description leaves the snapshot's own: alone it sends
	// nothing, with a pin only the pin.
	f.resetRequests()

	if err := b.AnnotateSnapshot(t.Context(), s, start, "", nil); err != nil {
		t.Fatal(err)
	}

	assertRoutes(t, f, "GET sources", "GET snapshots")

	f.resetRequests()

	if err := b.AnnotateSnapshot(t.Context(), s, start, "", []string{"before-update"}); err != nil {
		t.Fatal(err)
	}

	if got := editRequests(t, f); len(got) != 1 || got[0].Description != nil || !reflect.DeepEqual(got[0].AddPins, []string{"before-update"}) {
		t.Errorf("edits = %+v", got)
	}

	if got := f.snapshotAt(si, "edited-4"); got.Description != "app" || !got.StartTime.Equal(start) ||
		!slices.Contains(got.Pins, "before-update") {
		t.Errorf("snapshot = %+v", got)
	}

	// The other snapshots are untouched.
	if got := f.snapshotAt(si, "late"); got.Description != "" || len(got.Pins) != 0 {
		t.Errorf("late = %+v", got)
	}
}

// No snapshot started at the time, or only a checkpoint: ErrSnapshotNotFound,
// final, of neither kind.
func TestAnnotateSnapshotNotFound(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")
	start := t0.Add(time.Second)

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) {
		f.snapshots[si] = []kopiaclient.Snapshot{
			snapshot("cp", start, start, incompleteCheckpoint),
			snapshot("a", start.Add(time.Nanosecond), start.Add(time.Second), ""),
		}
		f.snapshots[info("/data/app2")] = []kopiaclient.Snapshot{snapshot("other", t0, t0, "")}
	})

	for _, tm := range []time.Time{start, t0, start.Add(-time.Nanosecond)} {
		err := b.AnnotateSnapshot(t.Context(), s, tm, "d", []string{"p"})
		assertError(t, err, opAnnotateSnapshot, s, nil)

		if !errors.Is(err, backend.ErrSnapshotNotFound) {
			t.Errorf("%v: err = %v, want ErrSnapshotNotFound", tm, err)
		}
	}

	// A source kopia server does not know has no snapshots.
	err := b.AnnotateSnapshot(t.Context(), src("/data/none"), start, "d", nil)
	if !errors.Is(err, backend.ErrSnapshotNotFound) {
		t.Errorf("unknown source: err = %v", err)
	}

	if len(f.requestsTo("POST snapshots/edit")) != 0 {
		t.Error("an edit was sent")
	}

	// The start time is compared as an instant, in any time zone.
	if err := b.AnnotateSnapshot(t.Context(), s, start.Add(time.Nanosecond).In(time.FixedZone("x", 3600)), "d", nil); err != nil {
		t.Errorf("another zone: %v", err)
	}
}

// Another client edits the snapshot between the listing and the edit: the
// old ID is gone, which is temporary; the next call finds the new one.
func TestAnnotateSnapshotEditedInBetween(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")

	f.addSource(si, nil)
	f.set(func(f *fakeKopia) { f.snapshots[si] = []kopiaclient.Snapshot{snapshot("a", t0, t0, "")} })
	f.script("POST snapshots/edit", func(f *fakeKopia) {
		f.snapshots[si][0].ID, f.snapshots[si][0].Description = "web-ui", "edited in the web UI"
	})

	err := b.AnnotateSnapshot(t.Context(), s, t0, "d", nil)
	assertError(t, err, opAnnotateSnapshot, s, backend.ErrTemporary)

	if err := b.AnnotateSnapshot(t.Context(), s, t0, "d", nil); err != nil {
		t.Fatal(err)
	}

	if got := editRequests(t, f); len(got) != 2 || got[1].Snapshots[0] != "web-ui" {
		t.Errorf("edits = %+v", got)
	}
}

// ListSnapshots lists every manifest of the source, checkpoints and
// snapshots of the same content included, sorted by start and then end
// time, whatever order the answer has.
func TestListSnapshots(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")
	si := info("/data/app")

	f.addSource(si, nil)

	same := snapshot("same", t0.Add(time.Hour), t0.Add(time.Hour), "")
	same.RootID = "root-a"
	described := snapshot("a", t0, t0.Add(time.Minute), "")
	described.Description = "app"

	f.set(func(f *fakeKopia) {
		f.snapshots[si] = []kopiaclient.Snapshot{
			same, described,
			snapshot("cp", t0, t0.Add(time.Second), incompleteCheckpoint),
			snapshot("x", t0.Add(2*time.Hour), t0.Add(3*time.Hour), "canceled"),
		}
		f.snapshots[info("/data/app2")] = []kopiaclient.Snapshot{snapshot("other", t0, t0, "")}
	})

	want := []backend.SnapshotInfo{
		{StartTime: t0, EndTime: t0.Add(time.Second), Incomplete: incompleteCheckpoint},
		{StartTime: t0, EndTime: t0.Add(time.Minute), Description: "app"},
		{StartTime: t0.Add(time.Hour), EndTime: t0.Add(time.Hour)},
		{StartTime: t0.Add(2 * time.Hour), EndTime: t0.Add(3 * time.Hour), Incomplete: "canceled"},
	}

	got, err := b.ListSnapshots(t.Context(), s)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ListSnapshots = %+v, %v\nwant %+v", got, err, want)
	}

	if r := f.requestsTo("GET snapshots"); len(r) != 1 || target(r[0].Query) != si || r[0].Query.Get("all") != "1" {
		t.Errorf("listing = %+v", r)
	}

	// kopia server sorts the answer; the backend does not count on it.
	f.inject("GET snapshots", injection{status: http.StatusOK, body: `{"snapshots":[` +
		`{"id":"c","startTime":"2026-10-18T10:00:00Z","endTime":"2026-10-18T10:00:01Z"},` +
		`{"id":"b","startTime":"2026-10-18T09:00:00Z","endTime":"2026-10-18T09:00:02Z"},` +
		`{"id":"a","startTime":"2026-10-18T09:00:00Z","endTime":"2026-10-18T09:00:01Z","incomplete":"checkpoint"}]}`})

	got, err = b.ListSnapshots(t.Context(), s)
	want = []backend.SnapshotInfo{
		{StartTime: t0, EndTime: t0.Add(time.Second), Incomplete: incompleteCheckpoint},
		{StartTime: t0, EndTime: t0.Add(2 * time.Second)},
		{StartTime: t0.Add(time.Hour), EndTime: t0.Add(time.Hour + time.Second)},
	}

	if err != nil || len(got) != len(want) {
		t.Fatalf("ListSnapshots = %+v, %v", got, err)
	}

	for i := range want {
		if !got[i].StartTime.Equal(want[i].StartTime) || !got[i].EndTime.Equal(want[i].EndTime) || got[i].Incomplete != want[i].Incomplete {
			t.Errorf("ListSnapshots[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	// No snapshots, or a source kopia server does not know: none.
	for _, p := range []string{"/data/empty", "/data/none"} {
		if p == "/data/empty" {
			f.addSource(info(p), nil)
		}

		got, err := b.ListSnapshots(t.Context(), src(p))
		if err != nil || got != nil {
			t.Errorf("%s: ListSnapshots = %+v, %v", p, got, err)
		}
	}
}
