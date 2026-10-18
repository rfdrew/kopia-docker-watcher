package rest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// Polling of WaitSnapshot.
const (
	// defaultPollMin is the first interval between two polls of a run; it
	// doubles up to defaultPollMax. Most snapshots of a container's volumes
	// take seconds, a large one hours.
	defaultPollMin = time.Second
	defaultPollMax = 30 * time.Second

	// idlePolls is how many polls in a row must find the source neither
	// pending nor uploading, and no task of the run, before WaitSnapshot
	// takes the run for dropped. Between the end of one run and the start
	// of the run queued after it, kopia server shows the source idle for a
	// moment.
	idlePolls = 2

	// clockSkew is how much earlier than the ticket's TriggeredAt, by the
	// watcher's clock, a task may have started by kopia server's clock and
	// still be the run's.
	clockSkew = time.Minute

	// maxErrorLines is how many error lines of a failed task's log, or
	// failed entries of an incomplete snapshot, go into
	// SnapshotResult.Error.
	maxErrorLines = 5
)

// incompleteCheckpoint is the incomplete reason of the intermediate
// snapshots kopia server saves during a long upload.
const incompleteCheckpoint = "checkpoint"

// TriggerSnapshot implements backend.Backend with sources/upload, which
// queues a run of the source and returns no task ID. Before the upload the
// ticket records the start of the source's last snapshot and the IDs of the
// source's snapshot tasks kopia server lists, so that WaitSnapshot can tell
// the run's task from earlier ones.
//
// While the source is pending or uploading, kopia server queues at most one
// run after the current one, so a trigger then may merge with a run queued
// before. The run of the ticket is the first that starts after the trigger:
// if the source was pending, that is the pending run, which reads the files
// after the trigger all the same.
//
// A source kopia server has not picked up yet is ErrTemporary, as after a
// policy write whose refresh failed. A source kopia server does not
// snapshot is an error of neither kind: its repository is connected
// read-only (status REMOTE), or the source is paused through the control
// API (PAUSED), which makes kopia server drop a queued run.
func (b *Backend) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	status, err := b.lookup(ctx, opTriggerSnapshot, src)
	if err != nil {
		return backend.SnapshotTicket{}, err
	}

	if status != nil {
		switch status.Status {
		case kopiaclient.SourceStatusRemote:
			return backend.SnapshotTicket{}, &backend.Error{Op: opTriggerSnapshot, Source: src,
				Err: errors.New("kopia server does not snapshot the source: its repository is connected read-only")}
		case backend.StatusPaused:
			return backend.SnapshotTicket{}, &backend.Error{Op: opTriggerSnapshot, Source: src,
				Err: errors.New("kopia server does not snapshot the source: it is paused through the control API")}
		}
	}

	tasks, err := b.client.ListTasks(ctx)
	if err != nil {
		return backend.SnapshotTicket{}, fail(ctx, opTriggerSnapshot, src, reqRead, err)
	}

	ticket := backend.SnapshotTicket{Source: src, TriggeredAt: b.now()}

	if status != nil && status.LastSnapshot != nil {
		ticket.LastSnapshotStart = status.LastSnapshot.StartTime
	}

	for _, t := range tasks {
		if isTaskOf(t, src) {
			ticket.TasksBefore = append(ticket.TasksBefore, t.ID)
		}
	}

	if err := b.client.StartSnapshot(ctx, toInfo(src)); err != nil {
		return backend.SnapshotTicket{}, fail(ctx, opTriggerSnapshot, src, reqUpload, err)
	}

	return ticket, nil
}

// isTaskOf reports whether t is a snapshot task of src. kopia server
// describes one as "user@host:path at <time>", the time in RFC 3339; the
// time must parse, so the task of a path that merely starts with src's
// path and " at " is not src's.
func isTaskOf(t kopiaclient.TaskInfo, src backend.Source) bool {
	if t.Kind != kopiaclient.TaskKindSnapshot {
		return false
	}

	at, ok := strings.CutPrefix(t.Description, src.String()+" at ")
	if !ok {
		return false
	}

	_, err := time.Parse(time.RFC3339, at)

	return err == nil
}

// WaitSnapshot implements backend.Backend by polling kopia server, at
// intervals that grow from one second to 30 seconds.
//
// Until the run's task is known, each poll reads the status of the source
// (GET /api/v1/sources) and then the task list (GET /api/v1/tasks), in this
// order, so that a run which starts in between shows in the list. The run's
// task is the earliest snapshot task of the source that is not one of the
// ticket's TasksBefore and started at TriggeredAt or later, give or take
// clockSkew between the watcher's clock and kopia server's. The source's
// currentTask is not used: it names whatever run is uploading, which may
// have started before the trigger. While there is no such task, the run is
// queued if the source is pending (waiting for an upload slot, with no task
// yet) or uploading (the run before it). When idlePolls polls in a row find
// the source in another state, or unknown to kopia server, kopia server has
// dropped the run, as it does when it restarts, and the outcome is
// SnapshotUnknown.
//
// Once found, the task goes into the ticket, which is passed to save, and
// later polls read the task alone (GET /api/v1/tasks/{id}). kopia server
// keeps the 50 latest finished tasks and numbers tasks anew when it
// restarts, so a task that is gone, or whose ID has another description
// now, is SnapshotUnknown too.
//
// The snapshot of a task that succeeded is looked up among the source's
// snapshots (GET /api/v1/snapshots with all=1): the earliest that is not a
// checkpoint and started between the start and the end of the task. None
// means that kopia server skipped a snapshot identical to the last one
// (SnapshotIdentical). One that is incomplete, or has errors the policy
// does not ignore, is SnapshotIncomplete. The error message of a failed
// task comes with the error lines of its log (GET
// /api/v1/tasks/{id}/logs), if the log can be read.
//
// A request that fails ends the wait with its error, of the kinds of a
// read; the caller waits again with the last ticket save received.
func (b *Backend) WaitSnapshot(ctx context.Context, ticket backend.SnapshotTicket, save func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	src := ticket.Source

	// The first poll uses the status the identity check reads.
	status, err := b.lookup(ctx, opWaitSnapshot, src)
	if err != nil {
		return backend.SnapshotResult{}, err
	}

	idle := 0
	delay := b.pollMin

	for {
		var task *kopiaclient.TaskInfo

		if ticket.TaskID == "" {
			tasks, err := b.client.ListTasks(ctx)
			if err != nil {
				return backend.SnapshotResult{}, fail(ctx, opWaitSnapshot, src, reqRead, err)
			}

			task = runTask(tasks, ticket)

			switch {
			case task != nil:
				ticket.TaskID, ticket.TaskDescription = task.ID, task.Description

				if save != nil {
					save(ticket)
				}
			case isQueued(status):
				idle = 0
			default:
				if idle++; idle >= idlePolls {
					return backend.SnapshotResult{Outcome: backend.SnapshotUnknown,
						Error: "kopia server dropped the run: no task of it, and the source is neither pending nor uploading"}, nil
				}
			}
		} else {
			task, err = b.client.GetTask(ctx, ticket.TaskID)

			switch {
			case errors.Is(err, kopiaclient.ErrNotFound):
				return gone(ticket), nil
			case err != nil:
				return backend.SnapshotResult{}, fail(ctx, opWaitSnapshot, src, reqRead, err)
			case task.Description != ticket.TaskDescription:
				// kopia server restarted and gave the ID to another task.
				return gone(ticket), nil
			}
		}

		if task != nil {
			if res, done, err := b.outcome(ctx, src, task); done {
				return res, err
			}
		}

		select {
		case <-ctx.Done():
			return backend.SnapshotResult{}, &backend.Error{Op: opWaitSnapshot, Source: src, Err: ctx.Err()}
		case <-b.after(delay):
		}

		delay = min(2*delay, b.pollMax)

		if ticket.TaskID == "" {
			if status, err = b.lookup(ctx, opWaitSnapshot, src); err != nil {
				return backend.SnapshotResult{}, err
			}
		}
	}
}

// runTask returns the task of the ticket's run from a task list: the
// earliest snapshot task of the source that is not one of TasksBefore and
// did not start before the trigger. Of tasks that started at the same time,
// the one kopia server created first, listed later, is taken.
func runTask(tasks []kopiaclient.TaskInfo, ticket backend.SnapshotTicket) *kopiaclient.TaskInfo {
	since := ticket.TriggeredAt.Add(-clockSkew)

	var run *kopiaclient.TaskInfo

	for i := range tasks {
		t := &tasks[i]

		if !isTaskOf(*t, ticket.Source) || slices.Contains(ticket.TasksBefore, t.ID) || t.StartTime.Before(since) {
			continue
		}

		if run == nil || !t.StartTime.After(run.StartTime) {
			run = t
		}
	}

	return run
}

// isQueued reports whether a run triggered before may still start: the
// source waits for an upload slot or uploads. Any state but those of an
// idle or stopped source counts, since kopia server passes through others,
// such as FAILED when it cannot refresh the source's status, on its way
// from one run to the next.
func isQueued(status *backend.SourceStatus) bool {
	if status == nil {
		return false
	}

	switch status.Status {
	case backend.StatusIdle, backend.StatusPaused, backend.StatusStopped, kopiaclient.SourceStatusRemote:
		return false
	default:
		return true
	}
}

// gone is the outcome of a run whose task kopia server no longer lists.
func gone(ticket backend.SnapshotTicket) backend.SnapshotResult {
	return backend.SnapshotResult{Outcome: backend.SnapshotUnknown, TaskID: ticket.TaskID,
		Error: fmt.Sprintf("kopia server no longer knows task %s (%s): it keeps the 50 latest finished tasks, and loses them when it restarts",
			ticket.TaskID, ticket.TaskDescription)}
}

// outcome returns how the run of task ended; done is false while it runs.
func (b *Backend) outcome(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo) (res backend.SnapshotResult, done bool, err error) {
	switch task.Status {
	case kopiaclient.TaskRunning, kopiaclient.TaskCanceling:
		return backend.SnapshotResult{}, false, nil
	case kopiaclient.TaskSuccess:
		res, err = b.saved(ctx, src, task)

		return res, true, err
	case kopiaclient.TaskFailed:
		return backend.SnapshotResult{Outcome: backend.SnapshotFailed, TaskID: task.ID, Error: b.failure(ctx, src, task)}, true, nil
	case kopiaclient.TaskCanceled:
		return backend.SnapshotResult{Outcome: backend.SnapshotCanceled, TaskID: task.ID, Error: "the task was canceled"}, true, nil
	default:
		return backend.SnapshotResult{}, true, &backend.Error{Op: opWaitSnapshot, Source: src,
			Err: fmt.Errorf("%w: task %s has the status %q", kopiaclient.ErrUnexpectedResponse, task.ID, task.Status)}
	}
}

// saved returns the outcome of a task that succeeded, from the snapshot it
// saved.
func (b *Backend) saved(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo) (backend.SnapshotResult, error) {
	snaps, err := b.snapshots(ctx, opWaitSnapshot, src)
	if err != nil {
		return backend.SnapshotResult{}, err
	}

	var snap *kopiaclient.Snapshot

	for i := range snaps {
		s := &snaps[i]

		if s.IncompleteReason == incompleteCheckpoint || s.StartTime.Before(task.StartTime) ||
			task.EndTime != nil && s.StartTime.After(*task.EndTime) {
			continue
		}

		if snap == nil || s.StartTime.Before(snap.StartTime) || s.StartTime.Equal(snap.StartTime) && s.EndTime.Before(snap.EndTime) {
			snap = s
		}
	}

	if snap == nil {
		return backend.SnapshotResult{Outcome: backend.SnapshotIdentical, TaskID: task.ID}, nil
	}

	res := backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskID: task.ID, Snapshot: fromSnapshot(*snap)}

	switch {
	case snap.IncompleteReason != "":
		res.Outcome = backend.SnapshotIncomplete
		res.Error = "the snapshot is incomplete: " + snap.IncompleteReason
	case snap.Summary != nil && snap.Summary.FatalErrorCount > 0:
		res.Outcome = backend.SnapshotIncomplete
		res.Error = entryErrors(snap.Summary)
	}

	return res, nil
}

// entryErrors describes the errors of a snapshot with the first failed
// entries.
func entryErrors(sum *kopiaclient.DirectorySummary) string {
	msg := fmt.Sprintf("the snapshot has %d errors", sum.FatalErrorCount)

	lines := make([]string, 0, maxErrorLines)

	for _, e := range sum.FailedEntries {
		if len(lines) == maxErrorLines {
			break
		}

		lines = append(lines, e.EntryPath+": "+e.Error)
	}

	if len(lines) > 0 {
		msg += ": " + strings.Join(lines, "; ")
	}

	return msg
}

// failure returns the error of a failed task: its error message and the
// last error lines of its log. A log that cannot be read is left out.
func (b *Backend) failure(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo) string {
	msg := task.ErrorMessage
	if msg == "" {
		msg = "the task failed without an error message"
	}

	logs, err := b.client.TaskLogs(ctx, task.ID)
	if err != nil {
		level := slog.LevelWarn
		if ctx.Err() != nil {
			// The caller stopped waiting, e.g. at shutdown.
			level = slog.LevelDebug
		}

		b.log.Log(ctx, level, "could not read the log of a failed snapshot task", "source", src.String(), "task", task.ID, "error", err)

		return msg
	}

	var lines []string

	for _, e := range logs {
		if e.Level >= kopiaclient.TaskLogError {
			lines = append(lines, e.Message)
		}
	}

	if len(lines) > maxErrorLines {
		lines = lines[len(lines)-maxErrorLines:]
	}

	if len(lines) > 0 {
		msg += "; task log: " + strings.Join(lines, "; ")
	}

	return msg
}

// AnnotateSnapshot implements backend.Backend with snapshots/edit. The
// snapshot's ID is read from GET /api/v1/snapshots with all=1 right before
// the edit and never kept: an edit saves a new manifest under a new ID and
// deletes the old one. The description and the pins go in one edit, and
// only what changes does: the description when it differs, the pins the
// snapshot does not carry yet. When nothing changes, nothing is sent, so a
// repeated call leaves the snapshot and its ID alone. A snapshot edited or
// deleted by another client between the listing and the edit makes the
// edit fail with ErrTemporary; the next call lists the snapshots again.
func (b *Backend) AnnotateSnapshot(ctx context.Context, src backend.Source, startTime time.Time, description string, addPins []string) error {
	if _, err := b.lookup(ctx, opAnnotateSnapshot, src); err != nil {
		return err
	}

	snaps, err := b.snapshots(ctx, opAnnotateSnapshot, src)
	if err != nil {
		return err
	}

	var snap *kopiaclient.Snapshot

	for i := range snaps {
		s := &snaps[i]

		if !s.StartTime.Equal(startTime) || s.IncompleteReason == incompleteCheckpoint {
			continue
		}

		if snap == nil || s.EndTime.Before(snap.EndTime) {
			snap = s
		}
	}

	if snap == nil {
		return &backend.Error{Op: opAnnotateSnapshot, Source: src,
			Err: fmt.Errorf("%w: none started at %s", backend.ErrSnapshotNotFound, startTime.Format(time.RFC3339Nano))}
	}

	req := kopiaclient.EditSnapshotsRequest{Snapshots: []string{snap.ID}}

	if snap.Description != description {
		req.Description = &description
	}

	for _, p := range addPins {
		if !slices.Contains(snap.Pins, p) && !slices.Contains(req.AddPins, p) {
			req.AddPins = append(req.AddPins, p)
		}
	}

	if req.Description == nil && len(req.AddPins) == 0 {
		return nil
	}

	if _, err := b.client.EditSnapshots(ctx, req); err != nil {
		return fail(ctx, opAnnotateSnapshot, src, reqEditSnapshots, err)
	}

	return nil
}

// ListSnapshots implements backend.Backend with GET /api/v1/snapshots and
// all=1: every manifest of the source, checkpoints and snapshots of the
// same content as an earlier one included. They are sorted by start time,
// those that started at once by end time, as kopia server sorts them.
func (b *Backend) ListSnapshots(ctx context.Context, src backend.Source) ([]backend.SnapshotInfo, error) {
	if _, err := b.lookup(ctx, opListSnapshots, src); err != nil {
		return nil, err
	}

	snaps, err := b.snapshots(ctx, opListSnapshots, src)
	if err != nil {
		return nil, err
	}

	var out []backend.SnapshotInfo

	for _, s := range snaps {
		out = append(out, *fromSnapshot(s))
	}

	slices.SortStableFunc(out, func(x, y backend.SnapshotInfo) int {
		if c := x.StartTime.Compare(y.StartTime); c != 0 {
			return c
		}

		return x.EndTime.Compare(y.EndTime)
	})

	return out, nil
}

// snapshots returns every manifest of src (GET /api/v1/snapshots with
// all=1): without all, kopia server merges snapshots of the same content
// into the oldest of them.
func (b *Backend) snapshots(ctx context.Context, op string, src backend.Source) ([]kopiaclient.Snapshot, error) {
	resp, err := b.client.ListSnapshots(ctx, toInfo(src), true)
	if err != nil {
		return nil, fail(ctx, op, src, reqRead, err)
	}

	return resp.Snapshots, nil
}

func fromSnapshot(s kopiaclient.Snapshot) *backend.SnapshotInfo {
	return &backend.SnapshotInfo{
		StartTime:   s.StartTime,
		EndTime:     s.EndTime,
		Description: s.Description,
		Incomplete:  s.IncompleteReason,
	}
}
