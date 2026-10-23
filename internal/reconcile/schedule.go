package reconcile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	// scheduleAction is the reason of a scheduled snapshot.
	scheduleAction = "schedule"

	// missedWindow: as kopia server does, a missed scheduled snapshot of a
	// schedule without an interval is taken late only when the next one is
	// further away than this.
	missedWindow = 30 * time.Minute

	// scheduleTimes is how many upcoming times the watcher asks kopia
	// server for: the first that is after the watcher's clock is taken, so
	// one that kopia server's clock, a little behind, still sees ahead
	// does not come twice. When kopia server's clock is so far behind
	// that none is, it asks for scheduleTimesFar.
	scheduleTimes    = previewCount
	scheduleTimesFar = 100
)

// nextSchedule is the next time of a schedule and what the effective
// schedule says about times the watcher misses.
type nextSchedule struct {
	next                time.Time
	interval, runMissed bool
}

// ownSchedule runs the watcher's schedule of a source whose owners run
// hooks (Desired.OwnSchedule): kopia server's schedule of it is off, so
// that the hooks run around every snapshot. It works out the next time from
// the policy the source would have without the hooks and the pause, with
// policy/resolve, keeps it in the state, and returns the request for the
// scheduled snapshot when it is due.
//
// A time is due once it has passed. It is taken when this process had armed
// a timer for it and the source was not paused meanwhile; one that passed
// while the watcher did not run, while no owner ran, or while a snapshot
// with hooks was under way, is a missed one, which is taken late only when
// the effective schedule has an interval, or runs missed snapshots and the
// next time is more than missedWindow away, as kopia server does once its
// upload has ended. While no owner runs, nothing is taken, and a time that
// comes meanwhile is judged once one runs again; a source whose containers
// are all gone keeps its schedule for when they come back.
//
// The next time is worked out again from now once the due one is taken or
// skipped, when the scheduling section of the path's policy changes, which
// drops a time of the old schedule that is due, and at every full
// reconcile, which sees changes of the parent policies; that one keeps a
// time due within clockSkew, which kopia server's clock, a little ahead,
// may already be past.
func (ps *pass) ownSchedule(rec state.Record) ([]snapRequest, error) {
	ps.sched = rec.Schedule

	if !ps.want.OwnSchedule {
		if rec.Schedule != nil {
			ps.dropSchedule()
		}

		return nil, nil
	}

	basis, err := schedulingBasis(ps.plan.Unpaused)
	if err != nil {
		return nil, err
	}

	now := ps.now()
	fresh := rec.Schedule == nil

	var sched state.Schedule
	if !fresh {
		sched = *rec.Schedule
	}

	before := sched
	attrs := []any{"source", ps.src.String(), "owners", ownerNames(ps.owners)}

	switch {
	case fresh:
		ps.log.Info("the watcher takes the scheduled snapshots of the source, as its containers run hooks; "+
			"kopia server's schedule of it stays off", attrs...)
	case ps.want.Paused && !sched.Paused:
		ps.log.Info("scheduled snapshots paused: no container of the source runs", attrs...)
	case !ps.want.Paused && sched.Paused:
		ps.log.Info("scheduled snapshots resumed", attrs...)
	}

	sched.Paused = ps.want.Paused

	var reqs []snapRequest

	due := !sched.Next.IsZero() && !now.Before(sched.Next)

	switch {
	case fresh || sched.Basis != basis:
		// A new schedule: a time of the old one is not taken, even if due.
		ns, err := ps.nextScheduled(now)
		if err != nil {
			return nil, err
		}

		setNext(&sched, ns)
	case due && !sched.Paused:
		reqs, err = ps.dueRequest(rec, &sched, now)
		if err != nil {
			return nil, err
		}
	case !due && ps.sweepDue:
		ns, err := ps.nextScheduled(now)
		if err != nil {
			return nil, err
		}

		if sched.Next.After(now) && !sched.Next.After(now.Add(clockSkew)) && (ns.next.IsZero() || sched.Next.Before(ns.next)) {
			// A time due within moments stays: kopia server's clock, a
			// little ahead of the watcher's, may already be past it.
			ns.next = sched.Next
		}

		setNext(&sched, ns)
	}

	sched.Basis = basis

	if fresh || !sameSchedule(sched, before) {
		ps.updateRecord(ps.src, func(r *state.Record) bool {
			if r.Schedule != nil && sameSchedule(*r.Schedule, sched) {
				return false
			}

			s := sched
			r.Schedule = &s

			return true
		})
	}

	ps.sched = &sched

	return reqs, nil
}

// dueRequest decides on the scheduled time that is due: it returns the
// request for its snapshot, or skips a missed one, which moves sched on to
// the next time.
func (ps *pass) dueRequest(rec state.Record, sched *state.Schedule, now time.Time) ([]snapRequest, error) {
	at := sched.Next
	onTime := ps.armedAt(ps.path, at)
	attrs := []any{"source", ps.src.String(), "due", at.Format(timeFormat)}

	if onTime && rec.HookRun != nil {
		// As kopia server does with a time that comes during an upload,
		// it is judged as a missed one when the run has ended.
		ps.disarm()
		ps.log.Debug("the scheduled time came while a snapshot with hooks is under way; it counts as missed", attrs...)

		return nil, nil
	}

	ns, err := ps.nextScheduled(later(now, at))
	if err != nil {
		return nil, err
	}

	reason := scheduleAction

	if !onTime {
		reason = "missed " + scheduleAction + " at " + at.Format(timeFormat)

		var skip string

		switch {
		case sched.Interval:
		case !sched.RunMissed:
			skip = "the schedule does not run missed snapshots"
		case !ns.next.IsZero() && !ns.next.After(now.Add(missedWindow)):
			skip = "the next one is due within " + missedWindow.String()
		}

		if skip != "" {
			ps.log.Info("skipping a scheduled snapshot the watcher missed: "+skip, append(attrs, "next", formatTime(ns.next))...)
			setNext(sched, ns)

			return nil, nil
		}

		if rec.HookRun != nil {
			ps.log.Debug("the missed scheduled snapshot waits for the snapshot with hooks under way", attrs...)
			return nil, nil
		}
	}

	ps.due, ps.dueNext = at, ns

	return []snapRequest{{scheduled: true, reason: reason}}, nil
}

// takeDue moves the schedule of r past the due time the pass takes or
// drops. It runs inside a store update.
func (ps *pass) takeDue(r *state.Record) {
	if ps.due.IsZero() || r.Schedule == nil || !r.Schedule.Next.Equal(ps.due) {
		return
	}

	s := *r.Schedule
	setNext(&s, ps.dueNext)
	r.Schedule = &s

	ps.sched = &s
	ps.due = time.Time{}
}

// sameSchedule reports whether a and b hold the same, times compared as
// instants.
func sameSchedule(a, b state.Schedule) bool {
	return a.Next.Equal(b.Next) && a.Basis == b.Basis && a.Interval == b.Interval && a.RunMissed == b.RunMissed &&
		a.Paused == b.Paused
}

// setNext sets the next time of sched and what the effective schedule says
// about missed ones.
func setNext(sched *state.Schedule, ns nextSchedule) {
	sched.Next, sched.Interval, sched.RunMissed = ns.next, ns.interval, ns.runMissed
}

// nextScheduled asks kopia server for the schedule the source would have
// without its hooks and the pause, and returns the first time after the
// given one. It uses the schedule check of the pass when it previewed the
// same policy, moments ago.
func (ps *pass) nextScheduled(after time.Time) (nextSchedule, error) {
	preview := ps.preview

	if preview == nil || !bytes.Equal(ps.previewOf, ps.plan.Unpaused) {
		p, err := ps.backend.PreviewSchedule(ps.ctx, ps.src, ps.plan.Unpaused, scheduleTimes)
		if err != nil {
			return nextSchedule{}, err
		}

		preview = &p
	}

	ns := nextSchedule{next: firstAfter(preview.Times, after)}

	if ns.next.IsZero() && len(preview.Times) > 0 {
		far, err := ps.backend.PreviewSchedule(ps.ctx, ps.src, ps.plan.Unpaused, scheduleTimesFar)
		if err != nil {
			return nextSchedule{}, err
		}

		ns.next = firstAfter(far.Times, after)
		ps.warnOnce("clock "+ps.path, "kopia server's clock is behind the watcher's by more than the time between two "+
			"scheduled snapshots; the watcher's schedule of sources with hooks follows the watcher's clock",
			"source", ps.src.String(), "watcher_time", after.Format(timeFormat), "next_by_kopia", formatTime(preview.Times[0]))
	}

	var eff struct {
		IntervalSeconds int64 `json:"intervalSeconds"`
		RunMissed       *bool `json:"runMissed"`
	}

	if len(preview.Scheduling) > 0 {
		if err := json.Unmarshal(preview.Scheduling, &eff); err != nil {
			return nextSchedule{}, &backend.Error{Op: "PreviewSchedule", Source: ps.src,
				Err: fmt.Errorf("effective scheduling section: %w", err)}
		}
	}

	// As kopia server's scheduler reads it: runMissed is off unless set,
	// though kopia server's default policy sets it.
	ns.interval = eff.IntervalSeconds > 0
	ns.runMissed = eff.RunMissed != nil && *eff.RunMissed

	return ns, nil
}

// firstAfter returns the first of times that is after t; zero if none.
func firstAfter(times []time.Time, t time.Time) time.Time {
	for _, x := range times {
		if x.After(t) {
			return x
		}
	}

	return time.Time{}
}

// armSchedule arms the timer of the watcher's schedule of the source for
// its next time, or disarms it while the source is paused or has no
// schedule. A time that has passed is not armed again: the pass that takes
// it comes when the snapshot with hooks under way ends, or soon after a
// policy write.
func (ps *pass) armSchedule() {
	sched := ps.sched

	ps.mu.Lock()
	defer ps.mu.Unlock()

	switch {
	case !ps.want.OwnSchedule || sched == nil || sched.Paused || sched.Next.IsZero():
		delete(ps.armed, ps.path)
	case sched.Next.After(ps.now()):
		ps.armed[ps.path] = sched.Next
		ps.nextRun = sched.Next
	}
}

// disarm forgets the time armed for the path of the pass.
func (ps *pass) disarm() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.armed, ps.path)
}

// armedAt reports whether this process armed the timer of path for t.
func (r *Reconciler) armedAt(path string, t time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	a, ok := r.armed[path]

	return ok && a.Equal(t)
}

// dropSchedule hands the schedule of a source whose containers no longer
// have hooks back to kopia server: the policy step of the pass has put
// back its scheduling section, unless the source is paused.
func (ps *pass) dropSchedule() {
	ps.log.Info("kopia server takes the scheduled snapshots of the source again: no container of it has hooks",
		"source", ps.src.String(), "owners", ownerNames(ps.owners), "paused", ps.want.Paused)

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.Schedule == nil {
			return false
		}

		r.Schedule = nil

		return true
	})

	ps.disarm()
}

// schedulingBasis returns the scheduling section of policy as compact
// JSON; empty without one.
func schedulingBasis(policy json.RawMessage) (string, error) {
	var p struct {
		Scheduling json.RawMessage `json:"scheduling"`
	}

	if err := json.Unmarshal(policy, &p); err != nil {
		return "", fmt.Errorf("policy without the pause: %w", err)
	}

	if len(p.Scheduling) == 0 || string(p.Scheduling) == "null" {
		return "", nil
	}

	var b bytes.Buffer
	if err := json.Compact(&b, p.Scheduling); err != nil {
		return "", fmt.Errorf("policy without the pause: %w", err)
	}

	return b.String(), nil
}

// formatTime formats t for the log; "none" for zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "none"
	}

	return t.Format(timeFormat)
}
