package reconcile

import (
	"errors"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// Metrics receives what the reconciler reports for /metrics: what goes
// wrong, the sources it manages and how its snapshots end. The calls about
// one path come from its passes, which never overlap, except
// SnapshotFinished, which mostly comes from a snapshot waiter.
// Implementations are safe for concurrent use and do not block: a waiter
// calls SnapshotFinished with the state store locked.
type Metrics interface {
	// ReconcileError counts one problem with a path. The path is empty for
	// a problem that is not about one source: the label errors of a
	// container, counted once per container and set of labels, and a full
	// reconcile that cannot list the containers.
	ReconcileError(path string, reason Reason)

	// Source reports the source of a path that the watcher manages, after
	// a pass brought its policy in line with its owners.
	Source(path string, s SourceMetrics)

	// LastSuccess reports the start of the last complete snapshot of the
	// source of a path that Source reported; zero if it has none.
	LastSuccess(path string, start time.Time)

	// ForgetSource drops what was reported about the source of a path that
	// the watcher does not manage, or no longer does. A path never reported
	// is no error.
	ForgetSource(path string)

	// SnapshotFinished reports how a snapshot the watcher queued of the
	// source of a path ended, once per snapshot, unless the watcher
	// restarts meanwhile. A run kopia server lost track of
	// (SnapshotUnknown) is reported only when no snapshot of it is found,
	// by the pass that looked, without the task's times.
	SnapshotFinished(path string, res backend.SnapshotResult)
}

// SourceMetrics is what a pass reports about a source the watcher manages.
type SourceMetrics struct {
	// Paused reports that the source is paused, as no owner runs.
	Paused bool
	// Owners are the names of the containers that back the source up,
	// stopped ones included, sorted; none for a source that lost them all.
	Owners []string
}

type noMetrics struct{}

func (noMetrics) ReconcileError(string, Reason)                   {}
func (noMetrics) Source(string, SourceMetrics)                    {}
func (noMetrics) LastSuccess(string, time.Time)                   {}
func (noMetrics) ForgetSource(string)                             {}
func (noMetrics) SnapshotFinished(string, backend.SnapshotResult) {}

// reportMetrics reports the source of the pass as managed, once its policy
// step has succeeded, or drops what was reported about it when the
// watcher does not manage it: the state has no record of it, kopia server
// does not know it and the pass did not register it, also when the
// registration failed, or the path is outside WATCHER_PATH_PREFIX. A pass
// that failed before it could tell leaves what was reported as it was; one
// that failed after its policy step reports the source without its last
// snapshot. A dry run reports nothing.
func (ps *pass) reportMetrics(err error) {
	switch {
	case ps.dryRun:
		return
	case ps.outside:
		ps.forgetSource()
		return
	case ps.src == (backend.Source{}):
		// The pass failed before it knew the source.
		return
	}

	if _, ok := ps.store.Get(ps.src); !ok {
		ps.forgetSource()
		return
	}

	switch {
	case ps.policyDone:
	case ps.unknown:
		// kopia server does not know the source, and the pass did not
		// register it.
		ps.forgetSource()
		return
	default:
		return
	}

	ps.metrics.Source(ps.path, SourceMetrics{Paused: ps.want.Paused, Owners: ownerNames(ps.owners)})

	if err != nil {
		return
	}

	if start, ok := ps.lastSuccess(); ok {
		ps.metrics.LastSuccess(ps.path, start)
	}
}

// lastComplete is the last complete snapshot of a source found by listing
// its snapshots while its last snapshot was the incomplete one that
// started at incomplete. Until another one becomes the last snapshot, only
// a deletion can change it.
type lastComplete struct {
	incomplete, complete time.Time
}

// lastSuccess returns the start of the last complete snapshot of the
// source, zero if it has none. kopia server's last snapshot tells, unless
// it is a checkpoint of an upload, which kopia server saves as the upload
// goes on, or incomplete otherwise; then the source's snapshots are
// listed, once per such last snapshot. ok is false when listing them
// failed.
func (ps *pass) lastSuccess() (start time.Time, ok bool) {
	var last *backend.SnapshotInfo
	if status := ps.plan.Status; status != nil {
		last = status.LastSnapshot
	}

	ps.mu.Lock()
	memo, had := ps.lastComplete[ps.path]

	if last == nil || last.Incomplete == "" {
		delete(ps.lastComplete, ps.path)
	}
	ps.mu.Unlock()

	switch {
	case last == nil:
		// Also a source the pass registered: it has no snapshot yet.
		return time.Time{}, true
	case last.Incomplete == "":
		return last.StartTime, true
	case had && memo.incomplete.Equal(last.StartTime):
		return memo.complete, true
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		return time.Time{}, false
	case err != nil:
		ps.log.Warn("cannot list the snapshots of the source to find its last complete one; trying again at the next pass",
			"source", ps.src.String(), "error", err.Error())

		return time.Time{}, false
	}

	for _, s := range snaps {
		if s.Incomplete == "" && s.StartTime.After(start) {
			start = s.StartTime
		}
	}

	ps.mu.Lock()
	ps.lastComplete[ps.path] = lastComplete{incomplete: last.StartTime, complete: start}
	ps.mu.Unlock()

	return start, true
}

// forgetSource drops what was reported about the source of the pass's
// path, and what the reconciler remembers for that.
func (ps *pass) forgetSource() {
	ps.mu.Lock()
	delete(ps.lastComplete, ps.path)
	ps.mu.Unlock()

	ps.metrics.ForgetSource(ps.path)
}
