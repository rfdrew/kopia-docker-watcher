package reconcile

import (
	"context"
	"sync"
	"time"
)

// queue holds the paths waiting for a pass. A path waits at most once, and
// a path whose pass is running is not handed out again until done: adding
// it meanwhile marks it, and done queues it again, so a change seen during
// a pass is never lost and two passes over one path never overlap.
//
// A path can also wait for a delayed add, one per path. addAfter queues a
// path later; of several delayed adds of one path the earliest wins. A
// retry after a temporary error (retryAfter) takes precedence: until it is
// due, delayed and full-reconcile adds of the path wait for it, so events
// do not cut the growing retry delay short; the retry pass reads
// everything afresh anyway.
type queue struct {
	mu sync.Mutex
	// ready holds the queued paths in order; queued is the same set.
	ready  []string
	queued map[string]bool
	// processing holds the paths handed out and not yet done; dirty those
	// added again meanwhile.
	processing map[string]bool
	dirty      map[string]bool
	// waiting holds the delayed add of each path; one timer per path is
	// armed.
	waiting map[string]*delayed
	closed  bool

	// wake holds one signal that a path may be ready. A worker that takes
	// a path and sees more passes the signal on, so one buffered signal
	// wakes as many workers as there are paths.
	wake chan struct{}
}

type delayed struct {
	due   time.Time
	timer *time.Timer
	// retry marks the retry after a temporary error.
	retry bool
}

func newQueue() *queue {
	return &queue{
		queued:     map[string]bool{},
		processing: map[string]bool{},
		dirty:      map[string]bool{},
		waiting:    map[string]*delayed{},
		wake:       make(chan struct{}, 1),
	}
}

// add queues p now.
func (q *queue) add(p string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.addLocked(p)
}

func (q *queue) addLocked(p string) {
	switch {
	case q.closed:
	case q.processing[p]:
		q.dirty[p] = true
	case !q.queued[p]:
		q.queued[p] = true
		q.ready = append(q.ready, p)
		q.signal()
	}
}

// addUnlessRetrying queues p now, unless p waits for a retry: a full
// reconcile does not cut a retry delay short. A path whose pass is running
// is marked to be queued again, since that pass may have read the
// containers before the change the full reconcile is about; done drops
// the mark if the pass ends in a retry.
func (q *queue) addUnlessRetrying(p string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if w, ok := q.waiting[p]; ok && w.retry {
		return
	}

	q.addLocked(p)
}

// addAfter queues p after d, unless an earlier delayed add of p or a retry
// is pending. A d of zero or less queues p now, unless it waits for a
// retry.
func (q *queue) addAfter(p string, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	w, ok := q.waiting[p]

	switch {
	case q.closed:
	case ok && w.retry:
	case d <= 0:
		q.addLocked(p)
	case !ok || time.Now().Add(d).Before(w.due):
		q.arm(p, d, false)
	}
}

// retryAfter queues p after d for a retry, in place of any delayed add of
// p.
func (q *queue) retryAfter(p string, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		q.arm(p, max(d, time.Nanosecond), true)
	}
}

// arm sets the delayed add of p. q.mu is held.
func (q *queue) arm(p string, d time.Duration, retry bool) {
	if w, ok := q.waiting[p]; ok {
		w.timer.Stop()
	}

	w := &delayed{due: time.Now().Add(d), retry: retry}
	w.timer = time.AfterFunc(d, func() { q.fire(p, w) })
	q.waiting[p] = w
}

// fire queues p when its delayed add w is due, unless another one has
// replaced it: a timer that Stop could not stop any more must not drop the
// one that replaced it.
func (q *queue) fire(p string, w *delayed) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.waiting[p] != w {
		return
	}

	delete(q.waiting, p)
	q.addLocked(p)
}

// get waits for a queued path and hands it out; false when ctx is done or
// the queue is closed. The caller calls done with the path after its pass.
func (q *queue) get(ctx context.Context) (string, bool) {
	for {
		q.mu.Lock()

		if q.closed || ctx.Err() != nil {
			// Pass the signal on to the next waiting worker.
			q.signal()
			q.mu.Unlock()

			return "", false
		}

		if len(q.ready) > 0 {
			p := q.ready[0]
			q.ready[0] = ""
			q.ready = q.ready[1:]
			delete(q.queued, p)
			q.processing[p] = true

			if len(q.ready) > 0 {
				q.signal()
			}

			q.mu.Unlock()

			return p, true
		}

		q.mu.Unlock()

		select {
		case <-q.wake:
		case <-ctx.Done():
			return "", false
		}
	}
}

// done ends the pass over p and queues p again if it was added meanwhile,
// unless p now waits for a retry, whose pass reads everything afresh.
func (q *queue) done(p string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.processing, p)

	if !q.dirty[p] {
		return
	}

	delete(q.dirty, p)

	if w, ok := q.waiting[p]; !ok || !w.retry {
		q.addLocked(p)
	}
}

// close stops the pending delayed adds; later adds are ignored and get
// returns false.
func (q *queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true

	for p, w := range q.waiting {
		w.timer.Stop()
		delete(q.waiting, p)
	}

	q.signal()
}

// signal wakes a waiting worker. q.mu is held.
func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
