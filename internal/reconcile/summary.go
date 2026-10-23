package reconcile

import (
	"fmt"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// passOutcome sorts how a pass ended, for the summary of the first full
// reconcile.
type passOutcome int

const (
	// outcomeCanceled: the watcher is shutting down; the pass is not
	// counted.
	outcomeCanceled passOutcome = iota
	outcomeRegistered
	outcomeUpdated
	outcomeUpToDate
	// outcomeSkipped: the watcher does not manage the source of the path,
	// as when no container backs it up with labels that can be applied.
	outcomeSkipped
	outcomeFailed
)

// summarize says how the pass ended and whether its source is paused.
func (ps *pass) summarize(err error) (passOutcome, bool) {
	switch {
	case err != nil:
		return outcomeFailed, false
	case ps.wrote && ps.plan != nil && ps.plan.Action == backend.ActionRegister:
		return outcomeRegistered, ps.want.Paused
	case ps.wrote:
		return outcomeUpdated, ps.want.Paused
	case ps.policyDone:
		return outcomeUpToDate, ps.want.Paused
	default:
		return outcomeSkipped, false
	}
}

// scanSummary counts what the passes of the first full reconcile did, so
// that the log says once, at INFO, what the watcher found at its start.
// Without it a start that finds nothing to do logs nothing after the event
// stream connects: sources that are up to date are logged at DEBUG.
type scanSummary struct {
	// pending are the paths of the first full reconcile that have not had
	// a pass yet.
	pending map[string]bool
	started time.Time

	paths, containers, enabled int

	registered, updated, upToDate, skipped, failed, paused int
}

// beginFirstScan starts the summary of the first full reconcile, which
// queued paths after listing containers. Call it with r.mu held.
func (r *Reconciler) beginFirstScan(paths []string, containers []docker.Container) *scanSummary {
	s := &scanSummary{
		pending:    make(map[string]bool, len(paths)),
		started:    r.now(),
		paths:      len(paths),
		containers: len(containers),
	}

	for _, p := range paths {
		s.pending[p] = true
	}

	for _, c := range containers {
		if r.parse(c).spec.Enable {
			s.enabled++
		}
	}

	if len(paths) > 0 {
		r.firstScan = s
	}

	return s
}

// logFirstScanStart logs what the first full reconcile found, and its
// summary at once when it queued no path.
func (r *Reconciler) logFirstScanStart(s *scanSummary) {
	r.log.Info("first full reconcile", "containers", s.containers, "backup_enabled", s.enabled, "paths", s.paths)

	if s.paths == 0 {
		r.logFirstScanDone(s)
	}
}

// noteFirstScan counts the pass over p if the first full reconcile queued
// p and p has not had a pass since, and logs the summary after the last
// such pass.
func (r *Reconciler) noteFirstScan(p string, res passResult) {
	if res.outcome == outcomeCanceled {
		return
	}

	r.mu.Lock()

	s := r.firstScan
	if s == nil || !s.pending[p] {
		r.mu.Unlock()

		return
	}

	delete(s.pending, p)

	switch res.outcome {
	case outcomeRegistered:
		s.registered++
	case outcomeUpdated:
		s.updated++
	case outcomeUpToDate:
		s.upToDate++
	case outcomeSkipped:
		s.skipped++
	case outcomeFailed:
		s.failed++
	}

	if res.paused {
		s.paused++
	}

	done := len(s.pending) == 0
	if done {
		r.firstScan = nil
	}

	r.mu.Unlock()

	if done {
		r.logFirstScanDone(s)
	}
}

func (r *Reconciler) logFirstScanDone(s *scanSummary) {
	attrs := []any{
		"paths", s.paths,
		"registered", s.registered,
		"updated", s.updated,
		"up_to_date", s.upToDate,
		"paused", s.paused,
		"skipped", s.skipped,
		"failed", s.failed,
		"took", r.now().Sub(s.started).Round(time.Millisecond).String(),
	}

	if s.enabled == 0 {
		attrs = append(attrs, "hint", fmt.Sprintf("no container has the label %s.enable=true", r.labelPrefix))
	}

	r.log.Info("first full reconcile done", attrs...)
}
