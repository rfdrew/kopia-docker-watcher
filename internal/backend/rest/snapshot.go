package rest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
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

	// idlePolls is how many polls in a row must find no task of the run
	// and nothing the run could be queued behind before WaitSnapshot takes
	// the run for dropped. Between the end of one run and the start of the
	// run queued after it, kopia server shows the source idle for a moment.
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

// Incomplete reasons of kopia server's snapshots: intermediate snapshots of
// a long upload, and what a canceled upload had read.
const (
	incompleteCheckpoint = "checkpoint"
	incompleteCanceled   = "canceled"
)

// TriggerSnapshot implements backend.Backend with sources/upload, which
// queues a run of the source and returns no task ID. Before the upload the
// ticket records the start of the source's last snapshot, the IDs of the
// source's snapshot tasks kopia server lists, and the newest task of any
// kind, so that WaitSnapshot can tell the run's task from earlier ones and
// notice a restart of kopia server.
//
// While the source is pending or uploading, kopia server queues at most one
// run after the current one, so a trigger then may merge with a run queued
// before. The run of the ticket is the first that starts after the trigger:
// if the source was pending, that is the pending run, which reads the files
// after the trigger all the same.
//
// A source kopia server has not picked up yet is ErrTemporary, as after a
// policy write whose refresh failed. A source of a repository connected
// read-only (status REMOTE) is an error of neither kind: kopia server does
// not snapshot it. A source paused through the control API is triggered all
// the same, since its status does not tell: kopia server shows PAUSED only
// once it has dropped a run, and still after the source is resumed. A run it
// drops is SnapshotUnknown to WaitSnapshot.
func (b *Backend) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	status, err := b.lookup(ctx, opTriggerSnapshot, src)
	if err != nil {
		return backend.SnapshotTicket{}, err
	}

	if status != nil && status.Status == kopiaclient.SourceStatusRemote {
		return backend.SnapshotTicket{}, &backend.Error{Op: opTriggerSnapshot, Source: src,
			Err: errors.New("kopia server does not snapshot the source: its repository is connected read-only")}
	}

	tasks, err := b.client.ListTasks(ctx)
	if err != nil {
		return backend.SnapshotTicket{}, fail(ctx, opTriggerSnapshot, src, reqRead, err)
	}

	ticket := backend.SnapshotTicket{Source: src, TriggeredAt: b.now(), LastTaskID: "0"}

	if status != nil && status.LastSnapshot != nil {
		ticket.LastSnapshotStart = status.LastSnapshot.StartTime
	}

	var newest uint64

	for _, t := range tasks {
		if isTaskOf(t, src) {
			ticket.TasksBefore = append(ticket.TasksBefore, t.ID)
		}

		if id, ok := taskNumber(t.ID); ok && id > newest {
			newest = id
			ticket.LastTaskID, ticket.LastTaskStart = t.ID, t.StartTime
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

// taskNumber returns the number of a task ID: kopia server numbers its
// tasks with a counter it prints in hexadecimal.
func taskNumber(id string) (uint64, bool) {
	n, err := strconv.ParseUint(id, 16, 64)

	return n, err == nil
}

// WaitSnapshot implements backend.Backend by polling kopia server, at
// intervals that grow from one second to 30 seconds.
//
// Until the run's task is known, each poll reads the status of the source
// (GET /api/v1/sources) and then the task list (GET /api/v1/tasks), in this
// order, so that a run which starts in between shows in the list. The run's
// task is the earliest snapshot task of the source that is not one of the
// ticket's TasksBefore, is numbered above LastTaskID and started at
// TriggeredAt or later, give or take clockSkew between the watcher's clock
// and kopia server's. The source's currentTask is not used: it names
// whatever run is uploading, which may have started before the trigger.
//
// kopia server keeps queued runs in memory only and numbers its tasks anew
// when it restarts. A list without LastTaskID or a task numbered above it,
// with another task under that ID, or whose run's task is numbered at or
// below it shows a restart, and the outcome is SnapshotUnknown. kopia
// server also keeps only the 50 finished tasks numbered highest, so while
// the run's task is not found, every poll moves LastTaskID to the newest
// task it lists and passes the ticket to save: a task numbered above
// LastTaskID that is missing from a list was dropped since the last look,
// and could have been the run's. That is SnapshotUnknown too; it takes 50
// tasks finishing between two polls, or a wait resumed after the watcher
// was down for a while.
//
// Without the run's task, the run is queued while the source is pending
// (waiting for an upload slot, with no task yet) or uploading (the run
// before it). FAILED, which a failed refresh of the source's status leaves
// until the next run, and the states of a source manager kopia server
// starts, INITIALIZING and UNKNOWN, can hide a queued run too: one that
// waits for an upload slot, which only a snapshot task can hold, or one
// that starts once kopia server has refreshed the source after its last
// run, which takes up to 30 seconds. When idlePolls polls in a row find
// none of that, kopia server has dropped the run, as it does for a source
// paused through the control API, and the outcome is SnapshotUnknown.
//
// Once found, the task goes into the ticket, which is passed to save, and
// later polls read the task alone (GET /api/v1/tasks/{id}). A task that is
// gone, or whose ID has another description now, is SnapshotUnknown too.
// kopia server drops the finished task numbered lowest first, so a long
// run's task can vanish the moment it ends if 50 tasks numbered above it
// have finished meanwhile, as with parallel uploads; the caller can still
// find its snapshot with ListSnapshots.
//
// The snapshot of a task that succeeded is looked up among the source's
// snapshots (GET /api/v1/snapshots with all=1): the earliest that is not a
// checkpoint and started between the start and the end of the task. One
// that is incomplete, or has errors the policy does not ignore, is
// SnapshotIncomplete; one of an upload canceled through sources/cancel or
// control/cancel-snapshot, a pause or a stop of the source,
// SnapshotCanceled. None means that kopia server skipped a snapshot
// identical to the last one (SnapshotIdentical), unless the task never
// uploaded (kopia server stopped the source before the run could start),
// its log says it saved a snapshot, or, if the log tells neither, the
// source has no earlier complete snapshot the run could be identical to:
// the snapshot was deleted since, e.g. by the retention of the next run,
// and the outcome is SnapshotUnknown. A canceled task's snapshot is looked
// up the same way. The error message of a failed task comes with the error
// lines of its log (GET /api/v1/tasks/{id}/logs), if the log can be read.
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

			var lost string

			task, lost = findRun(tasks, ticket)

			switch {
			case lost != "":
				return backend.SnapshotResult{Outcome: backend.SnapshotUnknown, Error: lost}, nil
			case task != nil:
				ticket.TaskID, ticket.TaskDescription = task.ID, task.Description

				if save != nil {
					save(ticket)
				}
			default:
				if advance(&ticket, tasks) && save != nil {
					save(ticket)
				}

				if isQueued(status, tasks, src, b.now()) {
					idle = 0
				} else if idle++; idle >= idlePolls {
					return backend.SnapshotResult{Outcome: backend.SnapshotUnknown,
						Error: "kopia server dropped the run: no task of it, and nothing it could be queued behind"}, nil
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

// findRun returns the task of the ticket's run from a task list, nil if
// the run has not started; lost says why the run cannot be followed any
// more.
func findRun(tasks []kopiaclient.TaskInfo, ticket backend.SnapshotTicket) (run *kopiaclient.TaskInfo, lost string) {
	anchor, anchored := taskNumber(ticket.LastTaskID)
	if !anchored {
		// A ticket made before LastTaskID.
		return runTask(tasks, ticket), ""
	}

	if anchor > 0 {
		if lost := restarted(tasks, anchor, ticket); lost != "" {
			return nil, lost
		}
	}

	if newest, _ := newestTask(tasks); newest > anchor && gap(tasks, anchor, newest) {
		return nil, fmt.Sprintf("kopia server no longer lists every task since task %s, the newest it listed before: "+
			"it keeps the 50 finished tasks numbered highest, and the run's task may have been one of those it dropped", ticket.LastTaskID)
	}

	run = runTask(tasks, ticket)
	if run == nil {
		return nil, ""
	}

	if id, ok := taskNumber(run.ID); ok && id <= anchor {
		return nil, fmt.Sprintf("kopia server restarted since the trigger and lost the run: the task %s of the source is numbered at or below %s",
			run.ID, ticket.LastTaskID)
	}

	return run, ""
}

// advance moves the ticket's LastTaskID to the newest task of a list in
// which findRun found neither the run's task nor a gap, and reports whether
// it moved.
func advance(ticket *backend.SnapshotTicket, tasks []kopiaclient.TaskInfo) bool {
	anchor, anchored := taskNumber(ticket.LastTaskID)
	if !anchored {
		return false
	}

	newest, t := newestTask(tasks)
	if newest <= anchor {
		return false
	}

	ticket.LastTaskID, ticket.LastTaskStart = t.ID, t.StartTime

	return true
}

// newestTask returns the task numbered highest and its number; 0 and nil
// for a list without one.
func newestTask(tasks []kopiaclient.TaskInfo) (uint64, *kopiaclient.TaskInfo) {
	var (
		newest uint64
		task   *kopiaclient.TaskInfo
	)

	for i := range tasks {
		if id, ok := taskNumber(tasks[i].ID); ok && id > newest {
			newest, task = id, &tasks[i]
		}
	}

	return newest, task
}

// restarted returns why a task list shows that kopia server restarted
// since the ticket's LastTaskID, anchor, was listed; "" if it does not.
// Within one run of kopia server nothing else gets that ID, and a task
// numbered at or above it stays listed: kopia server drops the finished
// tasks numbered lowest.
func restarted(tasks []kopiaclient.TaskInfo, anchor uint64, ticket backend.SnapshotTicket) string {
	for _, t := range tasks {
		if id, ok := taskNumber(t.ID); ok && id == anchor && !t.StartTime.Equal(ticket.LastTaskStart) {
			return fmt.Sprintf("kopia server restarted since the trigger and lost the run: task %s is another task now", t.ID)
		}
	}

	if newest, _ := newestTask(tasks); newest < anchor {
		return fmt.Sprintf("kopia server restarted since the trigger and lost the run: it lists no task numbered %s or above", ticket.LastTaskID)
	}

	return ""
}

// gap reports whether some task numbered above from and up to to is
// missing from the list: kopia server dropped it with the finished tasks
// numbered lowest.
func gap(tasks []kopiaclient.TaskInfo, from, to uint64) bool {
	var listed uint64

	for _, t := range tasks {
		if id, ok := taskNumber(t.ID); ok && id > from && id <= to {
			listed++
		}
	}

	return listed < to-from
}

// runTask returns the earliest snapshot task of the ticket's source that is
// not one of TasksBefore and did not start before the trigger. Of tasks
// that started at the same time, the one kopia server created first,
// listed later, is taken.
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

// refreshTime is how long kopia server may take to refresh a source's
// status after a run, before it starts the run queued after it: its
// refresh timeout, give or take clockSkew.
const refreshTime = 30*time.Second + clockSkew

// isQueued reports whether a run triggered before may still start, with
// the source's status and the task list read just after it, at now by the
// watcher's clock. It may while the source waits for an upload slot or
// uploads the run before. A source idle, paused, stopped, read-only or
// unknown to kopia server has no run queued. Any other state is FAILED,
// which a failed refresh leaves until the next run, or one of a source
// manager kopia server is starting: a run is queued then while a snapshot
// task holds an upload slot, or while kopia server refreshes the source
// after a run of it that ended less than refreshTime ago.
func isQueued(status *backend.SourceStatus, tasks []kopiaclient.TaskInfo, src backend.Source, now time.Time) bool {
	if status == nil {
		return false
	}

	switch status.Status {
	case backend.StatusPending, backend.StatusUploading:
		return true
	case backend.StatusIdle, backend.StatusPaused, backend.StatusStopped, kopiaclient.SourceStatusRemote:
		return false
	}

	return slices.ContainsFunc(tasks, func(t kopiaclient.TaskInfo) bool {
		switch {
		case t.Kind != kopiaclient.TaskKindSnapshot:
			return false
		case t.Status == kopiaclient.TaskRunning || t.Status == kopiaclient.TaskCanceling:
			return true
		default:
			return isTaskOf(t, src) && t.EndTime != nil && t.EndTime.After(now.Add(-refreshTime))
		}
	})
}

// gone is the outcome of a run whose task kopia server no longer lists.
func gone(ticket backend.SnapshotTicket) backend.SnapshotResult {
	return backend.SnapshotResult{Outcome: backend.SnapshotUnknown, TaskID: ticket.TaskID,
		Error: fmt.Sprintf("kopia server no longer knows task %s (%s): it keeps the 50 finished tasks numbered highest, and loses them when it restarts",
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
		// Canceled through tasks/{id}/cancel; the upload saved what it had
		// read, as when canceled through sources/cancel.
		snap, _, err := b.taskSnapshot(ctx, src, task)
		if err != nil {
			return backend.SnapshotResult{}, true, err
		}

		res = backend.SnapshotResult{Outcome: backend.SnapshotCanceled, TaskID: task.ID, Error: "the task was canceled"}
		if snap != nil {
			res.Snapshot = fromSnapshot(*snap)
		}

		return res, true, nil
	default:
		return backend.SnapshotResult{}, true, &backend.Error{Op: opWaitSnapshot, Source: src,
			Err: fmt.Errorf("%w: task %s has the status %q", kopiaclient.ErrUnexpectedResponse, task.ID, task.Status)}
	}
}

// taskSnapshot returns the snapshot a task saved: the earliest that is not
// a checkpoint and started within the task, nil if none did. It returns
// the source's snapshots too.
func (b *Backend) taskSnapshot(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo) (*kopiaclient.Snapshot, []kopiaclient.Snapshot, error) {
	snaps, err := b.snapshots(ctx, opWaitSnapshot, src)
	if err != nil {
		return nil, nil, err
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

	return snap, snaps, nil
}

// saved returns the outcome of a task that succeeded, from the snapshot it
// saved.
func (b *Backend) saved(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo) (backend.SnapshotResult, error) {
	snap, snaps, err := b.taskSnapshot(ctx, src, task)
	if err != nil {
		return backend.SnapshotResult{}, err
	}

	if snap == nil {
		return b.unsaved(ctx, src, task, snaps), nil
	}

	res := backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskID: task.ID, Snapshot: fromSnapshot(*snap)}

	switch {
	case snap.IncompleteReason == incompleteCanceled:
		res.Outcome = backend.SnapshotCanceled
		res.Error = "the upload was canceled: the snapshot is incomplete"
	case snap.IncompleteReason != "":
		res.Outcome = backend.SnapshotIncomplete
		res.Error = "the snapshot is incomplete: " + snap.IncompleteReason
	case snap.Summary != nil && snap.Summary.FatalErrorCount > 0:
		res.Outcome = backend.SnapshotIncomplete
		res.Error = entryErrors(snap.Summary)
	}

	return res, nil
}

// Lines kopia server logs, at the debug level, in the task of a run that
// skipped a snapshot identical to the latest one or saved one.
const (
	logIdentical = "Not saving snapshot because no files have been changed"
	logSaved     = "created snapshot "
)

// unsaved returns the outcome of a task that succeeded without a snapshot.
// kopia server skips a snapshot identical to the latest one, a complete
// one. A run whose source kopia server stopped while the run waited for an
// upload slot ends the same way, but uploads nothing and so reports no
// counters; and a snapshot the run saved may have been deleted since, by
// the retention of the next run. The task's log tells the two apart, if it
// can be read; otherwise a source without an earlier complete snapshot has
// nothing the run could have been identical to.
func (b *Backend) unsaved(ctx context.Context, src backend.Source, task *kopiaclient.TaskInfo, snaps []kopiaclient.Snapshot) backend.SnapshotResult {
	res := backend.SnapshotResult{Outcome: backend.SnapshotUnknown, TaskID: task.ID}

	if len(task.Counters) == 0 {
		res.Error = "the task ended without uploading: kopia server stopped the source before the run"

		return res
	}

	logs, err := b.client.TaskLogs(ctx, task.ID)
	if err != nil {
		b.log.DebugContext(ctx, "could not read the log of a snapshot task", "source", src.String(), "task", task.ID, "error", err)
	}

	for _, e := range logs {
		switch {
		case strings.HasPrefix(e.Message, logIdentical):
			res.Outcome = backend.SnapshotIdentical

			return res
		case strings.HasPrefix(e.Message, logSaved):
			res.Error = "the task saved a snapshot that has been deleted since"

			return res
		}
	}

	complete := slices.ContainsFunc(snaps, func(s kopiaclient.Snapshot) bool {
		return s.IncompleteReason == "" && s.StartTime.Before(task.StartTime)
	})

	if !complete {
		res.Error = "the task saved no snapshot, and the source has no complete one it could be identical to: the snapshot has been deleted since"

		return res
	}

	res.Outcome = backend.SnapshotIdentical

	return res
}

// entryErrors describes the errors of a snapshot. kopia server lists the
// entries that failed with the errors the policy ignores, and tells them
// apart only by count.
func entryErrors(sum *kopiaclient.DirectorySummary) string {
	msg := "the snapshot has " + count(sum.FatalErrorCount, "error")
	if sum.IgnoredErrorCount > 0 {
		msg += fmt.Sprintf(" and %d ignored", sum.IgnoredErrorCount)
	}

	lines := make([]string, 0, maxErrorLines)

	for _, e := range sum.FailedEntries {
		if len(lines) == maxErrorLines {
			break
		}

		lines = append(lines, e.EntryPath+": "+e.Error)
	}

	if len(lines) > 0 {
		which := "failed entries"
		if sum.IgnoredErrorCount > 0 {
			which += ", ignored ones included"
		}

		msg += "; " + which + ": " + strings.Join(lines, "; ")
	}

	return msg
}

// count returns n and noun, in the plural unless n is 1.
func count(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}

	return fmt.Sprintf("%d %ss", n, noun)
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
		Pins:        slices.Clone(s.Pins),
	}
}
