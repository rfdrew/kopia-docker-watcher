//go:build integration

package rest

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
)

// TestIntegrationSnapshots runs the snapshot lifecycle of the REST backend
// against a real kopia server (kopiatest.DefaultImage unless KOPIA_IMAGE
// names another): TriggerSnapshot, WaitSnapshot and AnnotateSnapshot.
//
// Every source is registered paused, so kopia server never snapshots it on
// its own: sources/upload still runs it. The subtests run in order; the
// last restarts kopia server.
func TestIntegrationSnapshots(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	c, err := kopiaclient.New(kopiaclient.Options{
		URL:             s.URL,
		Username:        kopiatest.UIUser,
		Password:        kopiatest.UIPassword,
		CertFingerprint: s.CertFingerprint,
		Timeout:         time.Minute,
		UserAgent:       "kopia-docker-watcher-integration-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitConnected(t, c)

	b := New(c, slog.New(slog.NewTextHandler(t.Output(), nil)))
	b.pollMin, b.pollMax = 100*time.Millisecond, time.Second
	ctx := t.Context()

	dirs := []string{"/data/snap", "/data/same", "/data/gone", "/data/par1", "/data/par2"}
	s.Exec(t, append([]string{"mkdir", "-p"}, dirs...)...)

	for _, d := range dirs {
		s.Exec(t, "sh", "-c", "echo one > "+d+"/file.txt")
	}

	user, host, err := b.ServerIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}

	source := func(p string) backend.Source { return backend.Source{Host: host, UserName: user, Path: p} }

	for _, d := range dirs {
		r := mustReconcile(t, b, memState{}, backend.Desired{Source: source(d), Patch: cron("0 3 * * *"), Paused: true})
		if r.Plan.Action != backend.ActionRegister {
			t.Fatalf("%s: Action = %v", d, r.Plan.Action)
		}
	}

	// snapshot triggers a snapshot and waits for it; it returns the
	// tickets WaitSnapshot saved too.
	snapshot := func(src backend.Source) (backend.SnapshotTicket, backend.SnapshotResult, []backend.SnapshotTicket, error) {
		ticket, err := b.TriggerSnapshot(ctx, src)
		if err != nil {
			return ticket, backend.SnapshotResult{}, nil, err
		}

		wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		var sv saves

		res, err := b.WaitSnapshot(wctx, ticket, sv.save)

		return ticket, res, sv.all(), err
	}

	// run is snapshot for the test's goroutine. WaitSnapshot saves the
	// run's task once.
	run := func(t *testing.T, src backend.Source) (backend.SnapshotTicket, backend.SnapshotResult, []backend.SnapshotTicket) {
		t.Helper()

		ticket, res, saved, err := snapshot(src)
		if err != nil {
			t.Fatalf("snapshot of %s: %v", src, err)
		}

		if len(saved) != 1 || saved[0].TaskID != res.TaskID || !strings.HasPrefix(saved[0].TaskDescription, src.String()+" at ") {
			t.Errorf("%s: saved %+v for task %q", src, saved, res.TaskID)
		}

		return ticket, res, saved
	}

	// settled waits until kopia server reports start as the start of the
	// source's last snapshot: it refreshes the source's status only after
	// the task of a run has ended.
	settled := func(t *testing.T, src backend.Source, start time.Time) {
		t.Helper()

		deadline := time.Now().Add(30 * time.Second)

		for {
			sources, err := b.ListSources(ctx)
			if err != nil {
				t.Fatal(err)
			}

			for _, st := range sources {
				if st.Source == src && st.LastSnapshot != nil && st.LastSnapshot.StartTime.Equal(start) {
					return
				}
			}

			if time.Now().After(deadline) {
				t.Fatalf("the last snapshot of %s is not the one of %v: %+v", src, start, sources)
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

	// snapshotsAt returns the snapshots of src that started at start.
	snapshotsAt := func(t *testing.T, src backend.Source, start time.Time) []kopiaclient.Snapshot {
		t.Helper()

		resp, err := c.ListSnapshots(ctx, toInfo(src), true)
		if err != nil {
			t.Fatal(err)
		}

		var out []kopiaclient.Snapshot

		for _, sn := range resp.Snapshots {
			if sn.StartTime.Equal(start) {
				out = append(out, sn)
			}
		}

		return out
	}

	var known backend.SnapshotTicket // a ticket with its task, for the restart

	t.Run("a snapshot is taken, then described and pinned", func(t *testing.T) {
		src := source("/data/snap")

		ticket, res, saved := run(t, src)
		if res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil || res.Error != "" {
			t.Fatalf("result = %+v", res)
		}

		known = saved[0]

		if !ticket.LastSnapshotStart.IsZero() {
			t.Errorf("LastSnapshotStart = %v before the first snapshot", ticket.LastSnapshotStart)
		}

		start := res.Snapshot.StartTime

		before := snapshotsAt(t, src, start)
		if len(before) != 1 || before[0].IncompleteReason != "" || before[0].Description != "" {
			t.Fatalf("snapshots at %v = %+v", start, before)
		}

		if err := b.AnnotateSnapshot(ctx, src, start, "snap [env=test]", []string{"keep"}); err != nil {
			t.Fatal(err)
		}

		after := snapshotsAt(t, src, start)
		if len(after) != 1 || after[0].Description != "snap [env=test]" || !slices.Equal(after[0].Pins, []string{"keep"}) {
			t.Fatalf("after the edit: %+v", after)
		}

		// kopia server saved the edit as a new manifest.
		if after[0].ID == before[0].ID {
			t.Errorf("the edit kept the ID %s", after[0].ID)
		}

		// Nothing changes: no edit, the ID stays.
		if err := b.AnnotateSnapshot(ctx, src, start, "snap [env=test]", []string{"keep"}); err != nil {
			t.Fatal(err)
		}

		if again := snapshotsAt(t, src, start); len(again) != 1 || again[0].ID != after[0].ID {
			t.Errorf("a repeated annotation changed the snapshot: %+v", again)
		}

		// A second pin: found under its new ID.
		if err := b.AnnotateSnapshot(ctx, src, start, "snap [env=test]", []string{"pin-on-stop"}); err != nil {
			t.Fatal(err)
		}

		pinned := snapshotsAt(t, src, start)
		if len(pinned) != 1 || pinned[0].ID == after[0].ID {
			t.Fatalf("after the second edit: %+v", pinned)
		}

		pins := slices.Sorted(slices.Values(pinned[0].Pins))
		if !slices.Equal(pins, []string{"keep", "pin-on-stop"}) || pinned[0].Description != "snap [env=test]" {
			t.Errorf("after the second edit: %+v", pinned[0])
		}

		// No snapshot started then.
		err := b.AnnotateSnapshot(ctx, src, start.Add(-time.Second), "x", nil)
		if !errors.Is(err, backend.ErrSnapshotNotFound) || kindOf(err) != nil {
			t.Errorf("err = %v, want ErrSnapshotNotFound", err)
		}

		// Without ignoreIdenticalSnapshots, an unchanged source gets a new
		// snapshot, and the wait finds that one, not the first.
		settled(t, src, start)

		ticket, res, _ = run(t, src)
		if res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil || !res.Snapshot.StartTime.After(start) {
			t.Fatalf("second snapshot: %+v (first started %v)", res, start)
		}

		if !ticket.LastSnapshotStart.Equal(start) || !slices.Contains(ticket.TasksBefore, known.TaskID) {
			t.Errorf("second ticket = %+v, first task %s", ticket, known.TaskID)
		}

		if res.TaskID == known.TaskID {
			t.Errorf("the second wait followed the first task %s", res.TaskID)
		}

		// Both snapshots, in the order they started, the first with its
		// description.
		list, err := b.ListSnapshots(ctx, src)
		if err != nil || len(list) != 2 || !list[0].StartTime.Equal(start) || list[0].Description != "snap [env=test]" ||
			!list[1].StartTime.Equal(res.Snapshot.StartTime) || list[1].Incomplete != "" {
			t.Errorf("ListSnapshots = %+v, %v", list, err)
		}
	})

	t.Run("an unchanged source with ignoreIdenticalSnapshots", func(t *testing.T) {
		src := source("/data/same")
		setPolicy(t, c, src, `{"scheduling":{"manual":true},"retention":{"ignoreIdenticalSnapshots":true}}`)

		_, first, _ := run(t, src)
		if first.Outcome != backend.SnapshotSucceeded || first.Snapshot == nil {
			t.Fatalf("first: %+v", first)
		}

		settled(t, src, first.Snapshot.StartTime)

		ticket, res, _ := run(t, src)
		if res.Outcome != backend.SnapshotIdentical || res.Snapshot != nil || res.TaskID == "" {
			t.Fatalf("unchanged: %+v", res)
		}

		if !ticket.LastSnapshotStart.Equal(first.Snapshot.StartTime) {
			t.Errorf("LastSnapshotStart = %v, want %v", ticket.LastSnapshotStart, first.Snapshot.StartTime)
		}

		// The identical snapshot was not saved, so there is nothing to
		// annotate.
		resp, err := c.ListSnapshots(ctx, toInfo(src), true)
		if err != nil || len(resp.Snapshots) != 1 {
			t.Errorf("snapshots = %+v, %v", resp, err)
		}

		s.Exec(t, "sh", "-c", "echo two >> /data/same/file.txt")

		if _, res, _ := run(t, src); res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil {
			t.Errorf("changed: %+v", res)
		}
	})

	t.Run("a run that fails", func(t *testing.T) {
		src := source("/data/gone")
		s.Exec(t, "rm", "-rf", "/data/gone")

		_, res, _ := run(t, src)
		if res.Outcome != backend.SnapshotFailed || res.Snapshot != nil || !strings.Contains(res.Error, "/data/gone") {
			t.Errorf("result = %+v", res)
		}
	})

	t.Run("two sources at once", func(t *testing.T) {
		// kopia server uploads one source at a time by default: the other
		// waits for the slot, pending without a task.
		paths := []string{"/data/par1", "/data/par2"}
		results := make([]backend.SnapshotResult, len(paths))
		errs := make([]error, len(paths))

		var wg sync.WaitGroup

		for i, p := range paths {
			wg.Go(func() { _, results[i], _, errs[i] = snapshot(source(p)) })
		}

		wg.Wait()

		for i, p := range paths {
			if errs[i] != nil || results[i].Outcome != backend.SnapshotSucceeded || results[i].Snapshot == nil {
				t.Errorf("%s: %+v, %v", p, results[i], errs[i])
			}
		}

		if results[0].TaskID == results[1].TaskID {
			t.Errorf("both followed task %s", results[0].TaskID)
		}
	})

	t.Run("a task kopia server does not know", func(t *testing.T) {
		src := source("/data/snap")
		ticket := backend.SnapshotTicket{Source: src, TriggeredAt: time.Now(), TaskID: "fffff", TaskDescription: src.String() + " at 2026-10-18T09:00:00Z"}

		res, err := b.WaitSnapshot(ctx, ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown || res.TaskID != "fffff" {
			t.Errorf("result = %+v, %v", res, err)
		}
	})

	t.Run("kopia server restarts", func(t *testing.T) {
		if known.TaskID == "" {
			t.Skip("no task from the first subtest")
		}

		// A run queued before the restart, still without a task: the
		// ticket marks the newest task kopia server listed.
		tasks, err := c.ListTasks(ctx)
		if err != nil || len(tasks) == 0 {
			t.Fatalf("tasks = %+v, %v", tasks, err)
		}

		queued := backend.SnapshotTicket{Source: source("/data/snap"), TriggeredAt: time.Now(),
			LastTaskID: tasks[0].ID, LastTaskStart: tasks[0].StartTime}

		for _, tk := range tasks {
			if isTaskOf(tk, queued.Source) {
				queued.TasksBefore = append(queued.TasksBefore, tk.ID)
			}
		}

		s.Restart(t)
		waitConnected(t, c)

		// kopia server lost its tasks: the ID is gone or another task's.
		res, err := b.WaitSnapshot(ctx, known, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown {
			t.Errorf("result = %+v, %v", res, err)
		}

		// And the queued run, which the new kopia server has not heard of.
		res, err = b.WaitSnapshot(ctx, queued, nil)
		if err != nil || res.Outcome != backend.SnapshotUnknown || !strings.Contains(res.Error, "restarted since the trigger") {
			t.Errorf("queued before the restart: %+v, %v", res, err)
		}

		// Snapshots survive, and so does the lifecycle.
		if _, res, _ := run(t, source("/data/snap")); res.Outcome != backend.SnapshotSucceeded {
			t.Errorf("after the restart: %+v", res)
		}
	})
}
