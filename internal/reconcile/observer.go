package reconcile

import (
	"context"
	"fmt"
	"time"
)

// Observer is told how every pass of Run ends, for the notifications.
// A pass canceled by the shutdown is not reported. PassDone is called by
// the worker of the pass, after the pass's calls to Metrics, so the errors
// a pass reported through Metrics.ReconcileError come before its
// PassDone. Implementations are safe for concurrent use and do not block.
type Observer interface {
	PassDone(path string, rep PassReport)
}

// PassReport is how a pass over a path ended.
type PassReport struct {
	// Err ended the pass before the source reached the desired state; nil
	// when it did.
	Err error
	// Retry reports that Err is temporary and the path is retried after a
	// delay.
	Retry bool
	// Problems kept the labels from being applied as they are; the pass
	// went on without them.
	Problems []error
}

// Failed reports whether the pass ended with an error or a problem.
func (r PassReport) Failed() bool {
	return r.Err != nil || len(r.Problems) > 0
}

// Progress is what Run is doing, for the health check.
type Progress struct {
	// Running is when Run started; zero before.
	Running time.Time
	// Queued is the number of paths ready for a pass that no worker has
	// taken yet; those waiting for a delay do not count.
	Queued int
	// Passes is the number of passes running; Oldest and Newest are when
	// the longest and the latest running one started, zero when none runs.
	Passes         int
	Oldest, Newest time.Time
	// LastDone is when the last pass ended; zero before the first.
	LastDone time.Time
}

// Progress returns what Run is doing.
func (r *Reconciler) Progress() Progress {
	queued := r.queue.readyLen()

	r.progressMu.Lock()
	defer r.progressMu.Unlock()

	p := Progress{Running: r.runStart, Queued: queued, Passes: len(r.passStarts), LastDone: r.lastPassDone}

	for _, t := range r.passStarts {
		if p.Oldest.IsZero() || t.Before(p.Oldest) {
			p.Oldest = t
		}

		if t.After(p.Newest) {
			p.Newest = t
		}
	}

	return p
}

// CheckProgress returns an error when Run looks stuck at now: a pass has
// run for longer than stall, or paths have been waiting for a pass while
// none started or ended for longer than stall. Before Run it returns nil.
func (r *Reconciler) CheckProgress(now time.Time, stall time.Duration) error {
	p := r.Progress()

	switch {
	case p.Running.IsZero():
		return nil
	case p.Passes > 0 && now.Sub(p.Oldest) > stall:
		return fmt.Errorf("a pass has been running for %v, longer than %v", now.Sub(p.Oldest).Round(time.Second), stall)
	case p.Queued > 0:
		last := p.LastDone
		for _, t := range []time.Time{p.Running, p.Newest} {
			if last.Before(t) {
				last = t
			}
		}

		if now.Sub(last) > stall {
			return fmt.Errorf("%d paths are waiting, and no pass has started or ended for %v, longer than %v",
				p.Queued, now.Sub(last).Round(time.Second), stall)
		}
	}

	return nil
}

// passStarted records that a pass over p started.
func (r *Reconciler) passStarted(p string) {
	r.progressMu.Lock()
	defer r.progressMu.Unlock()

	r.passStarts[p] = time.Now()
}

// passDone records that the pass over p ended, and reports it to the
// observer unless passCtx was canceled meanwhile.
func (r *Reconciler) passDone(passCtx context.Context, p string, res passResult) {
	r.progressMu.Lock()
	delete(r.passStarts, p)
	r.lastPassDone = time.Now()
	r.progressMu.Unlock()

	if passCtx.Err() != nil {
		return
	}

	r.observer.PassDone(p, PassReport{Err: res.err, Retry: res.retry, Problems: res.problems})
}

type noObserver struct{}

func (noObserver) PassDone(string, PassReport) {}
