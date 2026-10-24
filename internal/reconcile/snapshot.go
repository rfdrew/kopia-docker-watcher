package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	// watcherStartAction is the Action of the start events that the first
	// full reconcile makes up for SNAPSHOT_ON_WATCHER_START.
	watcherStartAction = "watcher start"
	// unseenStartAction and unseenStopAction are the Actions of the events
	// a pass makes up for a container that started or stopped without an
	// event the watcher saw.
	unseenStartAction = "start without an event"
	unseenStopAction  = "stop without an event"

	// oneOffLabel marks the one-off containers of docker compose run. They
	// copy the labels of their service but come and go with a task, so
	// their starts and stops take no snapshots.
	oneOffLabel = "com.docker.compose.oneoff"

	// maxAnnotations bounds the descriptions one pass tries, so that a
	// long backlog does not hold the path's handler; the rest waits for
	// the next full reconcile.
	maxAnnotations = 20

	// clockSkew is how far kopia server's clock may be behind the
	// watcher's. The sweep keeps that far away from the snapshots of the
	// watcher's own waits.
	clockSkew = time.Minute

	// maxWaitAge is how long a snapshot wait is kept at most. A wait that
	// cannot be followed or acted on for so long is dropped, so that it
	// does not hold back the descriptions of the source for good.
	maxWaitAge = 7 * 24 * time.Hour
)

// snapRequest is a snapshot a pass is asked to take of its source.
type snapRequest struct {
	// stop marks a snapshot at a stop: the minimum interval does not hold
	// it back.
	stop bool
	// reason says for the log why, e.g. "start of app".
	reason string
	// minInterval is the minimum interval of the container that started.
	minInterval time.Duration
	// container is the container that asks, for the description: it may
	// be gone already.
	container describedContainer
	// event is the event to keep when the decision is put off to a later
	// pass.
	event *docker.Event

	// retry marks a snapshot taken once more because the first was lost.
	// A start taken again does not count the lost attempt against the
	// minimum interval.
	retry bool
	// replaces is the ID of the wait whose snapshot is taken again; the
	// wait goes once the request is settled.
	replaces string
	// absorbed marks the retry of a stop that a pending snapshot absorbed.
	absorbed bool
	// description is the description of the snapshot taken again.
	description string
	// pins are the pins a stop asks for, or those of the snapshot taken
	// again.
	pins []string

	// scheduled marks the snapshot the watcher's own schedule of a source
	// with hooks takes: like kopia server's scheduled snapshots, neither
	// the minimum interval nor a snapshot that is pending or uploading
	// holds it back.
	scheduled bool

	// manual is the trigger of a snapshot asked for by hand, taken like
	// one at a start; with its force set, the minimum interval does not
	// hold it back.
	manual *Trigger
}

// describedContainer is what a container's labels say about the
// description of its snapshots.
type describedContainer struct {
	name string
	// description and tags are set only when the labels can be applied.
	description string
	tags        []labels.Tag
}

// snapLabels is what a container's labels say about its snapshots.
type snapLabels struct {
	// onStart and onStop are set when the label asks for the snapshot and
	// has no error; the other labels need not be valid.
	onStart, onStop bool
	minInterval     time.Duration
	// oneOff marks a one-off container of docker compose run.
	oneOff    bool
	described describedContainer
	// pin is the pin-on-stop template, set with onStop when the label has
	// no problem.
	pin string
}

// snapshotLabels reads what the labels of pc say about its snapshots.
func (r *Reconciler) snapshotLabels(pc parsed) snapLabels {
	ok := func(key string) bool { return r.labelOK(pc, key) }

	sl := snapLabels{
		onStart:     pc.spec.SnapshotOnStart && ok(labels.KeySnapshotOnStart),
		onStop:      pc.spec.SnapshotOnStop && ok(labels.KeySnapshotOnStop),
		minInterval: pc.spec.MinInterval,
		oneOff:      strings.EqualFold(pc.container.Labels[oneOffLabel], "true"),
		described:   describedContainer{name: pc.container.Name},
	}

	if !ok(labels.KeyMinInterval) {
		sl.minInterval = labels.DefaultMinInterval
	}

	if sl.onStop && ok(labels.KeyPinOnStop) {
		sl.pin = pc.spec.PinOnStop
	}

	if pc.valid() {
		sl.described.description = pc.spec.Description
		sl.described.tags = slices.Clone(pc.spec.Tags)
	}

	return sl
}

// waitKey tells the snapshot waits followed by this process apart.
type waitKey struct {
	src backend.Source
	id  string
}

// errNoChange makes Store.Update leave a record alone.
var errNoChange = errors.New("no change")

// snapshots runs the snapshot steps of a pass whose policy step succeeded,
// for a source kopia server knows: it sets the description mark of a
// source that has none, follows the snapshots the state lists as queued,
// acts on those that ended, moves a snapshot with hooks on, runs the
// watcher's own schedule of a source with hooks, takes the snapshot the
// events, the owners' states and that schedule ask for and, in a full
// reconcile, describes the snapshots that have no description. Only a
// failure to queue a snapshot or to work the schedule out fails the pass,
// so that its events are kept and the pass is retried; everything else is
// logged and tried again later.
func (ps *pass) snapshots() error {
	if ps.plan == nil || !ps.plan.Known && ps.plan.Action != backend.ActionRegister {
		return nil
	}

	// Nil after a registration: the source has no snapshot yet.
	status := ps.plan.Status

	rec, _ := ps.store.Get(ps.src)
	ps.sched = rec.Schedule
	mark := ps.mark(rec, status)
	ps.annotateAfter = mark

	var (
		finished []state.SnapshotWait
		expired  = map[string]bool{}
	)

	for _, w := range rec.SnapshotWaits {
		switch {
		case !ps.usable(w, true):
		case ps.started.Sub(w.Ticket.TriggeredAt) > maxWaitAge && !ps.followed(w):
			ps.log.Warn("dropping a snapshot wait that could not be finished for a week", "source", ps.src.String(),
				"reason", w.Reason, "triggered_at", w.Ticket.TriggeredAt.Format(timeFormat))

			expired[w.ID] = true
		case w.Result != nil:
			finished = append(finished, w)
		default:
			ps.startWait(w)
		}
	}

	ps.removeWaits(expired)

	reqs, identical := ps.handleFinished(finished, rec.SnapshotWaits, status, mark)
	reqs = append(reqs, ps.absorbedRetry(rec, status, mark, identical)...)
	reqs = append(reqs, ps.requests()...)
	reqs = append(reqs, ps.manualRequests()...)

	if err := ps.hookRun(status); err != nil {
		return err
	}

	rec, _ = ps.store.Get(ps.src)

	scheduled, err := ps.ownSchedule(rec)
	if err != nil {
		return err
	}

	if err := ps.trigger(append(reqs, scheduled...), status); err != nil {
		return err
	}

	ps.armSchedule()

	if ps.sweepDue {
		ps.sweep(status, mark)
	}

	return nil
}

// usable reports whether the pass can act on the wait w of its source's
// record. Others, such as one written by a newer watcher, are left alone,
// and logged at debug level if log is set.
func (ps *pass) usable(w state.SnapshotWait, log bool) bool {
	if w.ID != "" && w.Ticket.Source == ps.src && !w.Ticket.TriggeredAt.IsZero() {
		return true
	}

	if log {
		ps.log.Debug("leaving alone a snapshot wait the watcher cannot use", "source", ps.src.String(), "id", w.ID,
			"ticket_source", w.Ticket.Source.String())
	}

	return false
}

// mark returns the description mark of the source, setting it first if
// the record has none: to the start of the source's last snapshot, so that
// the snapshots taken before the watcher managed the source stay as they
// are. When kopia server reports no last snapshot, as for a source it does
// not know yet or one whose status it could not read, the mark is the
// start of the pass, before anything the pass wrote could start a
// snapshot.
func (ps *pass) mark(rec state.Record, status *backend.SourceStatus) *time.Time {
	if rec.AnnotateAfter != nil {
		return rec.AnnotateAfter
	}

	m := ps.started
	if status != nil && status.LastSnapshot != nil {
		m = status.LastSnapshot.StartTime
	}

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.AnnotateAfter != nil {
			m = *r.AnnotateAfter
			return false
		}

		r.AnnotateAfter = &m

		return true
	})

	return &m
}

// handleFinished logs how the snapshots of finished waits ended, describes
// those they saved and removes the waits. A wait whose snapshot could not
// be described for a passing reason stays, with its result logged, so that
// a later pass describes it with the description worked out when it was
// queued.
//
// When kopia server lost track of a run, its snapshot is looked for among
// the source's snapshots: one that is complete and started after the run
// was queued, and before the next run the watcher queued. If there is none,
// the snapshot is asked for once more; its wait goes once that request is
// settled.
//
// It also returns when the stops happened that waits which ended without
// a new snapshot, as nothing had changed, counted for: those runs read
// the files after the stop.
func (ps *pass) handleFinished(finished, waits []state.SnapshotWait, status *backend.SourceStatus, mark *time.Time) ([]snapRequest, []time.Time) {
	var (
		reqs      []snapRequest
		identical []time.Time
		done      = map[string]bool{}
		logged    = map[string]bool{}
	)

	for _, w := range finished {
		res := w.Result
		start := res.SnapshotStart

		if res.Outcome == backend.SnapshotUnknown {
			found, saved, decided := ps.lostRun(w, waits, status)
			if !decided {
				if !w.Logged {
					ps.logResult(w)
				}

				logged[w.ID] = true

				continue
			}

			if !saved {
				if !w.Logged {
					ps.logResult(w)
				}

				ps.reportLost(w)

				if !w.Retried {
					// The first trigger passed the minimum interval, and no
					// snapshot came of it.
					reqs = append(reqs, snapRequest{stop: w.Stop, reason: "again: " + w.Reason, retry: true, replaces: w.ID,
						description: w.Description, pins: ps.waitPins(w.ID)})
					logged[w.ID] = true

					continue
				}

				done[w.ID] = true

				continue
			}

			if !w.Logged {
				ps.log.Info("snapshot finished: kopia server lost track of the run, but its snapshot is listed",
					"source", ps.src.String(), "reason", w.Reason, "error", res.Error, "start_time", found.Format(timeFormat))
			}

			start = found
		} else if !w.Logged {
			ps.logResult(w)
		}

		if res.Outcome == backend.SnapshotIdentical && w.Stop && !w.StopAt.IsZero() {
			identical = append(identical, w.StopAt)
		}

		if !ps.annotateFinished(w, start, waits, mark) {
			logged[w.ID] = true
			continue
		}

		done[w.ID] = true
	}

	ps.removeWaits(done)

	if len(logged) > 0 {
		ps.updateRecord(ps.src, func(r *state.Record) bool {
			changed := false

			for i := range r.SnapshotWaits {
				if w := &r.SnapshotWaits[i]; logged[w.ID] && !w.Logged {
					w.Logged, changed = true, true
				}
			}

			return changed
		})
	}

	return reqs, identical
}

// reportLost reports to Metrics a run kopia server lost track of that saved
// no snapshot, once per wait in this process: a pass that cannot settle
// the wait, as when queuing the snapshot again fails, meets it again.
func (ps *pass) reportLost(w state.SnapshotWait) {
	key := waitKey{src: ps.src, id: w.ID}

	ps.mu.Lock()
	seen := ps.reportedLost[key]
	ps.reportedLost[key] = true
	ps.mu.Unlock()

	if !seen {
		ps.metrics.SnapshotFinished(ps.path, backend.SnapshotResult{Outcome: backend.SnapshotUnknown, TaskID: w.Result.TaskID,
			Error: w.Result.Error})
	}
}

// lostRun looks for the snapshot of a run kopia server lost track of: the
// first complete snapshot that started after the run was queued (and after
// the last stop it counts for) by the watcher's clock, after the last
// snapshot kopia server reported then by its own clock, and before the
// next run the watcher queued. found is its start; saved reports whether
// there is one. A backend that cannot list snapshots leaves the source's
// last snapshot to tell. decided is false when listing failed for a
// passing reason.
func (ps *pass) lostRun(w state.SnapshotWait, waits []state.SnapshotWait, status *backend.SourceStatus) (found time.Time, saved, decided bool) {
	since := later(w.Ticket.TriggeredAt, w.StopAt)

	byLast := func() (time.Time, bool, bool) {
		if savedAfter(status, since, w.Ticket.LastSnapshotStart) {
			return status.LastSnapshot.StartTime, true, true
		}

		return time.Time{}, false, true
	}

	if ps.cannotDescribe() {
		return byLast()
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnNotDescribing()

		return byLast()
	case err != nil:
		ps.log.Warn("cannot list the snapshots of the source to find the one of a lost run; trying again at the next pass",
			"source", ps.src.String(), "error", err.Error())

		return time.Time{}, false, false
	}

	until := ps.nextTrigger(w, waits)

	slices.SortStableFunc(snaps, func(a, b backend.SnapshotInfo) int { return a.StartTime.Compare(b.StartTime) })

	for _, sn := range snaps {
		switch {
		case sn.Incomplete != "", sn.StartTime.Before(since),
			!w.Ticket.LastSnapshotStart.IsZero() && !sn.StartTime.After(w.Ticket.LastSnapshotStart):
			continue
		case !until.IsZero() && !sn.StartTime.Before(until):
			return time.Time{}, false, true
		}

		return sn.StartTime, true, true
	}

	return time.Time{}, false, true
}

// logResult logs how the snapshot of the finished wait w ended.
func (ps *pass) logResult(w state.SnapshotWait) {
	res := w.Result
	attrs := []any{"source", ps.src.String(), "reason", w.Reason, "outcome", string(res.Outcome)}

	if res.TaskID != "" {
		attrs = append(attrs, "task", res.TaskID)
	}

	if !res.SnapshotStart.IsZero() {
		attrs = append(attrs, "start_time", res.SnapshotStart.Format(timeFormat))
	}

	if res.Error != "" {
		attrs = append(attrs, "error", res.Error)
	}

	switch res.Outcome {
	case backend.SnapshotSucceeded:
		ps.log.Info("snapshot finished", attrs...)
	case backend.SnapshotIdentical:
		ps.log.Info("snapshot finished without saving a new one: nothing changed since the last", attrs...)
	case backend.SnapshotIncomplete:
		ps.log.Warn("snapshot finished incomplete or with errors", attrs...)
	case backend.SnapshotFailed:
		ps.log.Error("snapshot failed", attrs...)
	case backend.SnapshotCanceled:
		ps.log.Warn("snapshot canceled", attrs...)
	default:
		ps.log.Warn("the result of the snapshot is unknown: kopia server lost track of it, perhaps it restarted", attrs...)
	}
}

// savedAfter reports whether the source's last snapshot is complete,
// started at t or later by the watcher's clock, and started after prev by
// kopia server's, prev being the start of the last snapshot kopia server
// reported at t (zero if it reported none). A checkpoint or a canceled
// snapshot does not count: kopia server saves one when an upload is cut
// short. Neither does an upload that was running at t, whose start is
// prev when it saved a checkpoint and before t otherwise. If kopia
// server's clock is behind, a snapshot may be taken once more than needed.
func savedAfter(status *backend.SourceStatus, t, prev time.Time) bool {
	if status == nil || status.LastSnapshot == nil || status.LastSnapshot.Incomplete != "" {
		return false
	}

	start := status.LastSnapshot.StartTime

	return !start.Before(t) && (prev.IsZero() || start.After(prev))
}

// later returns the later of a and b.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

// followed reports whether this process follows the snapshot of w.
func (ps *pass) followed(w state.SnapshotWait) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.waiting[waitKey{src: w.Ticket.Source, id: w.ID}]
}

// absorbedRetry asks for the stop snapshot once more when the pending
// snapshot that absorbed a stop is over but no complete snapshot started
// after the stop: kopia server lost the run, as when it restarts. While a
// snapshot is pending or uploading, the run has not ended.
//
// When the pending snapshot saved one, it gets the description worked out
// at the stop: the containers may be gone by now.
//
// A run the watcher followed that absorbed the stop and ended without a
// new snapshot, as nothing had changed, was the stop snapshot too.
func (ps *pass) absorbedRetry(rec state.Record, status *backend.SourceStatus, mark *time.Time, identical []time.Time) []snapRequest {
	at := rec.StopAbsorbedAt

	switch {
	case at.IsZero():
		return nil
	case slices.ContainsFunc(identical, at.Equal):
		ps.clearAbsorbed(at)

		return nil
	case status != nil && (status.Status == backend.StatusPending || status.Status == backend.StatusUploading):
		return nil
	case savedAfter(status, at, rec.StopAbsorbedLastStart):
		if ps.describeAbsorbed(rec, mark) {
			ps.clearAbsorbed(at)
		} else {
			ps.absorbedHeld = true
		}

		return nil
	}

	return []snapRequest{{stop: true, reason: "again: the stop at " + at.Format(timeFormat) + ", whose pending snapshot saved nothing",
		retry: true, absorbed: true, description: rec.StopAbsorbedDescription, pins: absorbedPins(rec)}}
}

// describeAbsorbed gives the first complete snapshot after an absorbed
// stop, the one the pending run saved, the description worked out at the
// stop and the pins the stop asked for. It reports whether the stop is done
// with; after a passing failure it is kept for a later pass.
func (ps *pass) describeAbsorbed(rec state.Record, mark *time.Time) bool {
	desc, pins := rec.StopAbsorbedDescription, absorbedPins(rec)
	if desc == "" && len(pins) == 0 || ps.cannotDescribe() {
		return true
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnNotDescribing()

		return true
	case err != nil:
		ps.log.Warn("cannot list the snapshots of the source to describe the stop snapshot; trying again at the next pass",
			"source", ps.src.String(), "error", err.Error())

		return false
	}

	slices.SortStableFunc(snaps, func(a, b backend.SnapshotInfo) int { return a.StartTime.Compare(b.StartTime) })

	for _, s := range snaps {
		if s.Incomplete != "" || s.StartTime.Before(rec.StopAbsorbedAt) ||
			!rec.StopAbsorbedLastStart.IsZero() && !s.StartTime.After(rec.StopAbsorbedLastStart) {
			continue
		}

		if s.Description != "" || mark != nil && !s.StartTime.After(*mark) || slices.ContainsFunc(ps.described, s.StartTime.Equal) {
			desc = ""
		}

		pins = slices.DeleteFunc(slices.Clone(pins), func(p string) bool { return slices.Contains(s.Pins, p) })
		if desc == "" && len(pins) == 0 {
			return true
		}

		err := ps.annotate(ps.src, s.StartTime, desc, pins)

		return err == nil || errors.Is(err, backend.ErrSnapshotNotFound) || errors.Is(err, backend.ErrNotImplemented) ||
			backend.IsConfig(err)
	}

	return true
}

// clearAbsorbed clears the stop absorbed at at.
func (ps *pass) clearAbsorbed(at time.Time) {
	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if !r.StopAbsorbedAt.Equal(at) {
			return false
		}

		clearAbsorbedFields(r)

		return true
	})
}

// clearAbsorbedFields clears the absorbed stop of r.
func clearAbsorbedFields(r *state.Record) {
	r.StopAbsorbedAt = time.Time{}
	r.StopAbsorbedDescription = ""
	r.StopAbsorbedLastStart = time.Time{}
	r.StopAbsorbedPins = nil
	r.StopAbsorbedPinsAt = time.Time{}
}

// removeWaits removes the waits with the given IDs from the record.
func (ps *pass) removeWaits(ids map[string]bool) {
	if len(ids) == 0 {
		return
	}

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		n := len(r.SnapshotWaits)
		r.SnapshotWaits = slices.DeleteFunc(r.SnapshotWaits, func(w state.SnapshotWait) bool { return ids[w.ID] })
		prunePins(r)

		return len(r.SnapshotWaits) != n
	})
}

// requests works out the snapshots the events of the pass ask for, one
// container at a time, from what the container's labels say and the state
// it is in now. A start asks for a snapshot if the container runs; a die
// or stop asks for one if the container has stopped or is gone. A die
// after which the container is restarting is no stop: the event is kept
// for a later pass, which sees whether the container runs again or stays
// stopped.
//
// A start or stop the watcher missed, while it was not running or while
// Docker's events were cut off, shows in the pause: when the pass pauses
// a source it had running, or lifts its pause, and no event asked for a
// snapshot of that kind, the owners' labels decide as if the event had
// come.
func (ps *pass) requests() []snapRequest {
	type seen struct {
		last        docker.Event
		start       *docker.Event
		watcher     bool
		unseenStart bool
		unseenStop  bool
		died        *docker.Event
	}

	var (
		order []string
		byID  = map[string]*seen{}
	)

	for _, ev := range ps.events {
		if ev.Kind != docker.KindStarted && ev.Kind != docker.KindDied {
			continue
		}

		s, ok := byID[ev.ContainerID]
		if !ok {
			s = &seen{}
			byID[ev.ContainerID] = s
			order = append(order, ev.ContainerID)
		}

		s.last = ev

		if ev.Kind == docker.KindStarted {
			s.start = &ev
			s.watcher = s.watcher || ev.Action == watcherStartAction
			s.unseenStart = s.unseenStart || ev.Action == unseenStartAction
		} else {
			s.died = &ev
			s.unseenStop = s.unseenStop || ev.Action == unseenStopAction
		}
	}

	var (
		reqs                  []snapRequest
		startAsked, stopAsked bool
		held                  = ps.heldByID()
	)

	for _, id := range order {
		s := byID[id]

		pc, owns := ps.eventOwner(id, s.last)
		if !pc.spec.Enable || !owns {
			continue
		}

		sl := ps.snapshotLabels(pc)
		c, found := ps.container(id)
		// A container the watcher holds down for a snapshot still runs.
		_, isHeld := held[id]
		running := found && (c.State.Running() || isHeld)

		// An event of a one-off container explains a change of the pause
		// as well, but asks for no snapshot.
		startAsked = startAsked || running && s.start != nil
		stopAsked = stopAsked || !running && s.died != nil

		if sl.oneOff {
			ps.log.Debug("no snapshot for a one-off container of docker compose run", "container", s.last.Name)
			continue
		}

		switch {
		case running:
			if c.State == docker.StateRestarting && s.died != nil {
				ps.keep = append(ps.keep, *s.died)
			}

			if s.start != nil && sl.onStart {
				reason := "start of " + s.last.Name

				switch {
				case s.watcher:
					reason = watcherStartAction + ": " + s.last.Name
				case s.unseenStart:
					reason = "start of " + s.last.Name + " without an event"
				}

				reqs = append(reqs, snapRequest{reason: reason, minInterval: sl.minInterval, container: sl.described, event: s.start})
			}
		case s.died != nil && sl.onStop:
			reason := "stop of " + s.last.Name
			if s.unseenStop {
				reason += " without an event"
			}

			reqs = append(reqs, snapRequest{stop: true, reason: reason, container: sl.described, event: s.died,
				pins: stopPins(sl, s.died.Time, ps.started)})
		}
	}

	return append(reqs, ps.unseenRequests(startAsked, stopAsked)...)
}

// unseenRequests asks for the snapshots of starts and stops no event told
// of: when the pass paused a source the watcher had running, the stopped
// owners whose labels ask for a snapshot at a stop; when it lifted the
// pause, the running owners whose labels ask for one at a start. It makes
// up an event for each, which the pass keeps if it fails, so that its
// retry, which no longer sees the change of the pause, asks again.
//
// The policy tells whether the source was paused, unless the watcher's own
// schedule was in force: the policy of a source with hooks stays manual
// while its owners run, and the schedule records the pause.
func (ps *pass) unseenRequests(startAsked, stopAsked bool) []snapRequest {
	// A source that goes on the watcher's schedule keeps a manual policy,
	// so a pause lifted meanwhile needs no write.
	if !ps.plan.Known || ps.sched == nil && !ps.want.OwnSchedule && ps.plan.Action == backend.ActionNone {
		return nil
	}

	wasPaused := policyPaused(ps.plan.Current)
	if ps.sched != nil {
		wasPaused = ps.sched.Paused
	}

	var reqs []snapRequest

	for _, o := range ps.owners {
		switch {
		case o.snap.oneOff:
		case !wasPaused && ps.want.Paused && !stopAsked && o.snap.onStop:
			ev := docker.Event{Kind: docker.KindDied, Action: unseenStopAction, ContainerID: o.id, Name: o.name, Labels: o.labels,
				Time: ps.started}
			ps.events = append(ps.events, ev)
			reqs = append(reqs, snapRequest{stop: true, reason: "stop of " + o.name + " without an event", container: o.snap.described,
				event: &ev, pins: stopPins(o.snap, ps.started, ps.started)})
		case wasPaused && !ps.want.Paused && !startAsked && o.snap.onStart && o.running():
			ev := docker.Event{Kind: docker.KindStarted, Action: unseenStartAction, ContainerID: o.id, Name: o.name,
				Labels: o.labels, Time: ps.started}
			reqs = append(reqs, snapRequest{reason: "start of " + o.name + " without an event", minInterval: o.snap.minInterval,
				container: o.snap.described, event: &ev})
		}
	}

	return reqs
}

// policyPaused reports whether a policy's scheduling section is paused.
func policyPaused(policy json.RawMessage) bool {
	var p struct {
		Scheduling struct {
			Manual bool `json:"manual"`
		} `json:"scheduling"`
	}

	return json.Unmarshal(policy, &p) == nil && p.Scheduling.Manual
}

// container finds a container of the pass's listing by ID.
func (ps *pass) container(id string) (docker.Container, bool) {
	i := slices.IndexFunc(ps.containers, func(c docker.Container) bool { return c.ID == id })
	if i < 0 {
		return docker.Container{}, false
	}

	return ps.containers[i], true
}

// trigger queues at most one snapshot for reqs.
//
// A stop asks for one regardless of the minimum interval: while a snapshot
// uploads, kopia server runs the new one after it; while one is pending,
// that one counts, since it reads the files after the stop, and the state
// remembers the stop in case kopia server loses the run.
//
// A start asks for one only when no snapshot is pending or uploading and
// the minimum interval has passed since the later of the last snapshot's
// start and the watcher's last trigger: a snapshot that failed saved
// nothing, but counts. A policy write can make kopia server start a
// snapshot itself (a missed scheduled one), so a pass that wrote the
// policy keeps the start events and leaves the decision to a pass shortly
// after, which sees the status after the write.
//
// No snapshot is queued while kopia server reports the source STOPPED.
// PAUSED does not tell: kopia server shows it once it has dropped a run of
// a source paused through its control API, and still after the source is
// resumed. A run it drops comes back as SnapshotUnknown and is taken once
// more at most.
func (ps *pass) trigger(reqs []snapRequest, status *backend.SourceStatus) error {
	if len(reqs) == 0 {
		return nil
	}

	stop := slices.ContainsFunc(reqs, func(r snapRequest) bool { return r.stop })
	reasons := make([]string, len(reqs))

	for i, r := range reqs {
		reasons[i] = r.reason
	}

	attrs := []any{"source", ps.src.String(), "reasons", reasons}

	var st backend.Status
	if status != nil {
		st = status.Status
	}

	if stop && !ps.due.IsZero() {
		// A scheduled time that comes with a stop is taken by a pass soon
		// after, with the hooks of the containers that still run.
		ps.recheckSoon = true
	}

	switch {
	case ps.restoring():
		// The files change under the restore until it ends.
		ps.keepEvents(reqs)
		ps.log.Info("the snapshot waits for the restore of the source under way", attrs...)

		return nil
	case st == backend.StatusStopped:
		ps.log.Warn("not queuing a snapshot: kopia server does not run snapshots of the source now",
			append(attrs, "status", string(st))...)
		ps.decline(reqs, "kopia server does not run snapshots of the source now (status "+string(st)+")")
		ps.settle(reqs, nil)
		ps.skipDue()

		return nil
	case !stop && ps.want.OwnSchedule:
		return ps.triggerHooked(reqs, status, reasons, attrs)
	case stop && ps.preExecRunning():
		// The snapshot with hooks under way is queued once its pre-exec
		// hooks are done, after the stop; one taken now could read a dump
		// half written.
		ps.keepEvents(reqs)
		ps.log.Info("the snapshot at the stop waits for the pre-exec hooks under way", attrs...)

		return nil
	case stop && st == backend.StatusPending:
		ps.log.Info("not queuing a snapshot: the pending one starts after the stop and counts as the stop snapshot", attrs...)
		ps.decline(reqs, "a snapshot of the source is pending already")
		ps.absorbStop(reqs, "", status)

		return nil
	case stop && st == backend.StatusUploading && ps.queuedWait(status.CurrentTask) != "":
		ps.log.Info("not queuing a snapshot: one the watcher queued runs after the current upload and counts as the stop snapshot",
			attrs...)
		ps.decline(reqs, "a snapshot the watcher queued runs after the current upload")
		ps.absorbStop(reqs, ps.queuedWait(status.CurrentTask), status)

		return nil
	case stop:
	case ps.wrote && !ps.deferred:
		ps.keepEvents(reqs)
		ps.deferStart = true
		ps.log.Debug("deciding on the start snapshot after the policy write has settled", attrs...)

		return nil
	case st == backend.StatusPending || st == backend.StatusUploading:
		ps.log.Debug("not queuing a snapshot: one is "+strings.ToLower(string(st)), attrs...)
		ps.decline(reqs, "a snapshot of the source is "+strings.ToLower(string(st))+" already")
		ps.settle(reqs, nil)

		return nil
	default:
		if next, ok := ps.minIntervalPassed(reqs, status); !ok {
			ps.log.Debug("not queuing a snapshot: the minimum interval since the last one has not passed",
				append(attrs, "next_allowed", next.Format(timeFormat))...)
			ps.decline(reqs, minIntervalMessage(next))
			ps.settle(reqs, nil)

			return nil
		}
	}

	if stop && ps.want.OwnSchedule {
		ps.log.Warn("taking the snapshot at the stop without hooks: hooks do not run around a snapshot at a stop", attrs...)
	}

	ticket, err := ps.backend.TriggerSnapshot(ps.ctx, ps.src)
	if errors.Is(err, backend.ErrNotImplemented) {
		ps.warnOnce("TriggerSnapshot", "the backend cannot queue snapshots; no snapshot is taken at container starts and stops")
		ps.decline(reqs, "the backend cannot queue snapshots")
		ps.settle(reqs, nil)

		return nil
	}

	if err != nil {
		return err
	}

	desc, tags := ps.description(reqs)
	ps.warnTags(tags)

	w := state.SnapshotWait{
		Ticket:      ticket,
		Stop:        stop,
		Reason:      strings.Join(reasons, ", "),
		Description: desc,
		Retried:     !slices.ContainsFunc(reqs, func(r snapRequest) bool { return !r.retry }),
	}

	ps.settle(reqs, &w)
	ps.attachWait(reqs, w.ID)
	ps.log.Info("snapshot queued", attrs...)
	ps.startWait(w)

	return nil
}

// triggerHooked takes the snapshot reqs ask for of a source whose owners
// run hooks, unless it is one at a stop: as a snapshot with hooks, which
// startHookRun begins. The rules of trigger hold, but for a scheduled
// snapshot, which neither the minimum interval nor a snapshot that is
// pending or uploading holds back, as with kopia server's own schedule:
// the snapshot is queued once the pre-exec hooks have run, behind the one
// under way. While a snapshot with hooks is under way, the start events
// are kept, and a scheduled time stays due, until it ends: there is one at
// a time.
func (ps *pass) triggerHooked(reqs []snapRequest, status *backend.SourceStatus, reasons []string, attrs []any) error {
	scheduled := slices.ContainsFunc(reqs, func(r snapRequest) bool { return r.scheduled })
	rec, _ := ps.store.Get(ps.src)

	var st backend.Status
	if status != nil {
		st = status.Status
	}

	switch {
	case rec.HookRun != nil || ps.hooksBusy():
		ps.keepEvents(reqs)
		ps.log.Debug("the snapshot waits for the snapshot with hooks under way", attrs...)

		return nil
	case !ps.canRunHooks():
		ps.keepEvents(reqs)
		ps.log.Debug("not starting a snapshot with hooks: the watcher stops", attrs...)

		return nil
	case ps.waitsForHeld(reqs, attrs):
		return nil
	case ps.wrote && !ps.deferred:
		ps.keepEvents(reqs)
		ps.deferStart = true
		ps.log.Debug("deciding on the snapshot after the policy write has settled", attrs...)

		return nil
	case scheduled:
	case st == backend.StatusPending || st == backend.StatusUploading:
		ps.log.Debug("not taking a snapshot: one is "+strings.ToLower(string(st)), attrs...)
		ps.decline(reqs, "a snapshot of the source is "+strings.ToLower(string(st))+" already")
		ps.settle(reqs, nil)

		return nil
	default:
		if next, ok := ps.minIntervalPassed(reqs, status); !ok {
			ps.log.Debug("not taking a snapshot: the minimum interval since the last one has not passed",
				append(attrs, "next_allowed", next.Format(timeFormat))...)
			ps.decline(reqs, minIntervalMessage(next))
			ps.settle(reqs, nil)

			return nil
		}
	}

	return ps.startHookRun(reqs, status, reasons)
}

// waitsForHeld reports whether a container the snapshot with hooks needs,
// an owner or one it takes down, is held down for another snapshot: the
// start events are kept, and a scheduled time stays due, until it is back.
func (ps *pass) waitsForHeld(reqs []snapRequest, attrs []any) bool {
	h, busy := ps.heldBusy(ps.downPlan(), "")
	if !busy {
		return false
	}

	ps.keepEvents(reqs)
	ps.log.Debug("the snapshot waits: a container it needs is held down for another snapshot",
		append(attrs, "container", h.Name, "held_for", h.Source.Path)...)
	ps.waitFor(h.ID)

	return true
}

// preExecRunning reports whether the snapshot with hooks under way of the
// source has not queued its snapshot yet: its pre-exec hooks run, or have
// just run.
func (ps *pass) preExecRunning() bool {
	rec, _ := ps.store.Get(ps.src)

	return rec.HookRun != nil && rec.HookRun.Phase == state.HookPre
}

// keepEvents keeps the events and manual triggers of reqs for a later
// pass.
func (ps *pass) keepEvents(reqs []snapRequest) {
	for _, r := range reqs {
		if r.event != nil {
			ps.keep = append(ps.keep, *r.event)
		}
	}

	ps.keepManual(reqs)
}

// hooksBusy reports whether a goroutine of this process runs hooks of the
// source of the pass.
func (ps *pass) hooksBusy() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.hooksRunning[ps.src] != ""
}

// canRunHooks reports whether Run runs and is not stopping, so hooks can
// start.
func (ps *pass) canRunHooks() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.runCtx != nil && ps.runCtx.Err() == nil && ps.hookCtx != nil
}

// skipDue moves the watcher's schedule past the due time the pass drops.
func (ps *pass) skipDue() {
	if ps.due.IsZero() {
		return
	}

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		ps.takeDue(r)
		return true
	})
}

// minIntervalPassed reports whether the minimum interval of one of the
// start requests has passed, and when the first one will otherwise.
func (ps *pass) minIntervalPassed(reqs []snapRequest, status *backend.SourceStatus) (time.Time, bool) {
	rec, _ := ps.store.Get(ps.src)

	var lastSnapshot time.Time
	if status != nil && status.LastSnapshot != nil {
		lastSnapshot = status.LastSnapshot.StartTime
	}

	now := ps.now()

	var first time.Time

	for _, r := range reqs {
		if r.manual != nil && r.manual.force {
			return time.Time{}, true
		}

		last := lastSnapshot
		if !r.retry && rec.LastTriggered.After(last) {
			last = rec.LastTriggered
		}

		next := last.Add(r.minInterval)
		if last.IsZero() || !now.Before(next) {
			return time.Time{}, true
		}

		if first.IsZero() || next.Before(first) {
			first = next
		}
	}

	return first, false
}

// settle records in the state that reqs are dealt with: the waits they
// take again go, a stop absorbed earlier is cleared once a stop snapshot
// is queued or the absorbed retry is dropped, and the new wait w, if any,
// is added with the time of the trigger.
func (ps *pass) settle(reqs []snapRequest, w *state.SnapshotWait) {
	replaced := map[string]bool{}
	// An absorbed stop whose snapshot is saved but not described yet stays
	// for its description and pins; a new stop snapshot does not stand in
	// for it.
	clearAbsorbed := w != nil && w.Stop && !ps.absorbedHeld

	for _, r := range reqs {
		if r.replaces != "" {
			replaced[r.replaces] = true
		}

		clearAbsorbed = clearAbsorbed || r.absorbed
	}

	if len(replaced) == 0 && !clearAbsorbed && w == nil {
		return
	}

	now := ps.now()

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		r.SnapshotWaits = slices.DeleteFunc(r.SnapshotWaits, func(x state.SnapshotWait) bool { return replaced[x.ID] })

		if clearAbsorbed {
			clearAbsorbedFields(r)
		}

		if w != nil {
			w.ID = newWaitID(r.SnapshotWaits, w.Ticket.TriggeredAt)
			r.LastTriggered = now
			r.SnapshotWaits = append(r.SnapshotWaits, *w)
			addWaitPins(r, w.ID, requestPins(reqs))
		}

		prunePins(r)

		return true
	})
}

// queuedWait returns the ID of the wait whose run kopia server has queued
// behind the upload of task current: the current task was running when it
// was triggered, and it has no task of its own yet. Empty if none.
func (ps *pass) queuedWait(current string) string {
	if current == "" {
		return ""
	}

	rec, _ := ps.store.Get(ps.src)

	for _, w := range rec.SnapshotWaits {
		if ps.usable(w, false) && w.Result == nil && w.Ticket.TaskID == "" && slices.Contains(w.Ticket.TasksBefore, current) {
			return w.ID
		}
	}

	return ""
}

// absorbStop records a stop that a snapshot already queued counts for. A
// stop while a snapshot is pending keeps its time and description in the
// state, so that the stop snapshot is taken again if kopia server loses
// the run. If the queued snapshot is one the watcher follows, the wait
// with the given ID or, while pending, the one that has no task yet, it
// becomes a stop snapshot, taken again if it is lost, with the description
// worked out now.
//
// The stop counts from the start of the pass, before its policy write: a
// run that was pending then reads the files after the stop, even if it
// got its upload slot during the write.
func (ps *pass) absorbStop(reqs []snapRequest, id string, status *backend.SourceStatus) {
	var reasons []string

	replaced := map[string]bool{}

	for _, r := range reqs {
		if r.stop {
			reasons = append(reasons, r.reason)
		}

		if r.replaces != "" {
			replaced[r.replaces] = true
		}
	}

	desc, tags := ps.description(reqs)
	ps.warnTags(tags)

	pins := requestPins(reqs)

	// An earlier stop that a pending run absorbed, and which that run has
	// saved its snapshot for, gives way to this one: the run pending now
	// is another.
	rec, _ := ps.store.Get(ps.src)
	earlier := rec.StopAbsorbedAt

	if id != "" || earlier.IsZero() || !savedAfter(status, earlier, rec.StopAbsorbedLastStart) {
		earlier = time.Time{}
	} else if !ps.describeAbsorbed(rec, ps.annotateAfter) {
		// The earlier stop keeps its slot until its snapshot is described
		// and pinned, and this one would have none: the stop events are
		// kept for a pass soon after, which describes the earlier stop
		// first and then sees this stop's run pending, or uploading.
		ps.keepEvents(reqs)
		ps.recheckSoon = true
		ps.log.Debug("deciding on the stop snapshot once the snapshot of the earlier absorbed stop is described",
			"source", ps.src.String())

		return
	}

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		r.SnapshotWaits = slices.DeleteFunc(r.SnapshotWaits, func(x state.SnapshotWait) bool { return replaced[x.ID] })

		i := slices.IndexFunc(r.SnapshotWaits, func(x state.SnapshotWait) bool {
			if id != "" {
				return x.ID == id
			}

			return ps.usable(x, false) && x.Result == nil && x.Ticket.TaskID == ""
		})

		if i >= 0 {
			w := &r.SnapshotWaits[i]
			w.Stop = true
			w.StopAt = ps.started
			w.Retried = false
			w.Reason += ", " + strings.Join(reasons, ", ")

			if desc != "" {
				w.Description = desc
			}

			addWaitPins(r, w.ID, pins)
		}

		switch {
		case id != "":
		case r.StopAbsorbedAt.IsZero() || !earlier.IsZero() && r.StopAbsorbedAt.Equal(earlier):
			r.StopAbsorbedAt = ps.started
			r.StopAbsorbedDescription = desc
			r.StopAbsorbedLastStart = time.Time{}
			r.StopAbsorbedPins, r.StopAbsorbedPinsAt = pins, time.Time{}

			if len(pins) > 0 {
				r.StopAbsorbedPinsAt = ps.started
			}

			if status != nil && status.LastSnapshot != nil {
				r.StopAbsorbedLastStart = status.LastSnapshot.StartTime
			}
		default:
			// The stop absorbed earlier waits for the same pending run: its
			// run has not saved, or the stop would have given way above.
			r.StopAbsorbedPins, r.StopAbsorbedPinsAt = unionPins(absorbedPins(*r), pins), r.StopAbsorbedAt
		}

		prunePins(r)

		return true
	})
}

// newWaitID returns an ID for a wait triggered at t that none of waits has.
func newWaitID(waits []state.SnapshotWait, t time.Time) string {
	base := t.UTC().Format(time.RFC3339Nano)
	id := base

	for n := 2; slices.ContainsFunc(waits, func(w state.SnapshotWait) bool { return w.ID == id }); n++ {
		id = fmt.Sprintf("%s-%d", base, n)
	}

	return id
}

// sweep describes the snapshots of the source that started after the mark
// and have no description, scheduled ones included, and moves the mark
// past them. It runs in the passes of a full reconcile, and asks kopia
// server only when the last snapshot started after the mark. It stops
// before the first snapshot the watcher still waits for, which gets the
// description worked out when it was queued, and after maxAnnotations
// attempts. A snapshot that is gone is skipped; any other failure stops
// the sweep before it until the next full reconcile. Checkpoints of an
// upload are left alone.
func (ps *pass) sweep(status *backend.SourceStatus, mark *time.Time) {
	if mark == nil || status == nil || status.LastSnapshot == nil || !status.LastSnapshot.StartTime.After(*mark) {
		return
	}

	desc, tags := ps.description(nil)
	if desc == "" || ps.cannotDescribe() {
		return
	}

	// Snapshots from the first trigger the watcher waits for on belong to
	// the waits, and so does the one that a pending run took after an
	// absorbed stop.
	var until time.Time

	rec, _ := ps.store.Get(ps.src)
	for _, w := range rec.SnapshotWaits {
		if ps.usable(w, false) && (until.IsZero() || w.Ticket.TriggeredAt.Before(until)) {
			until = w.Ticket.TriggeredAt
		}
	}

	if at := rec.StopAbsorbedAt; !at.IsZero() && (until.IsZero() || at.Before(until)) {
		until = at
	}

	if !until.IsZero() {
		until = until.Add(-clockSkew)
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnNotDescribing()

		return
	case err != nil:
		ps.log.Warn("cannot list the snapshots of the source to describe them; trying again at the next full reconcile",
			"source", ps.src.String(), "error", err.Error())

		return
	}

	slices.SortStableFunc(snaps, func(a, b backend.SnapshotInfo) int { return a.StartTime.Compare(b.StartTime) })

	newMark, tried, described := *mark, 0, 0

loop:
	for _, s := range snaps {
		switch {
		case !s.StartTime.After(*mark) || s.Incomplete == "checkpoint":
			continue
		case !until.IsZero() && !s.StartTime.Before(until), tried == maxAnnotations:
			break loop
		case s.Description == "":
			if tried == 0 {
				ps.warnTags(tags)
			}

			tried++

			switch err := ps.annotate(ps.src, s.StartTime, desc, nil); {
			case err == nil:
				described++
			case errors.Is(err, backend.ErrSnapshotNotFound):
			default:
				// Another snapshot fares no better with the same
				// description and kopia server.
				break loop
			}
		}

		newMark = s.StartTime
	}

	if described > 0 {
		ps.log.Info("described snapshots", "source", ps.src.String(), "count", described, "description", desc)
	}

	if newMark.After(*mark) {
		ps.updateRecord(ps.src, func(r *state.Record) bool {
			if r.AnnotateAfter != nil && !newMark.After(*r.AnnotateAfter) {
				return false
			}

			r.AnnotateAfter = &newMark

			return true
		})
	}
}

// annotate sets the description of the snapshot of src that started at
// start, unless desc is empty, and adds pins to it. A snapshot that is gone
// is ErrSnapshotNotFound, logged at debug level when only the description
// is lost; other errors are logged as warnings.
func (ps *pass) annotate(src backend.Source, start time.Time, desc string, pins []string) error {
	err := ps.backend.AnnotateSnapshot(ps.ctx, src, start, desc, pins)
	if err == nil && desc != "" {
		ps.described = append(ps.described, start)
	}

	attrs := []any{"source", src.String(), "start_time", start.Format(timeFormat), "description", desc}
	if len(pins) > 0 {
		attrs = append(attrs, "pins", pins)
	}

	switch {
	case err == nil && len(pins) > 0:
		ps.log.Info("snapshot pinned", attrs...)
	case err == nil:
		ps.log.Debug("snapshot described", attrs...)
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnNotDescribing()
	case errors.Is(err, backend.ErrSnapshotNotFound) && len(pins) > 0:
		ps.log.Warn("cannot pin a snapshot that is gone", append(attrs, "error", err.Error())...)
	case errors.Is(err, backend.ErrSnapshotNotFound):
		ps.log.Debug("not describing a snapshot that is gone", append(attrs, "error", err.Error())...)
	default:
		ps.log.Warn("cannot describe the snapshot", append(attrs, "error", err.Error())...)
	}

	return err
}

// cannotDescribe reports whether the backend said it cannot list or
// describe snapshots, so the sweep does not ask again in this process.
func (ps *pass) cannotDescribe() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.warned[notDescribingKey]
}

// notDescribingKey is the warnOnce key of a backend that cannot describe
// snapshots.
const notDescribingKey = "AnnotateSnapshot"

func (ps *pass) warnNotDescribing() {
	ps.warnOnce(notDescribingKey, "the backend cannot describe snapshots yet; "+
		labels.Name(ps.labelPrefix, labels.KeyDescription)+", "+labels.Name(ps.labelPrefix, labels.KeyTags)+" and "+
		labels.Name(ps.labelPrefix, labels.KeyPinOnStop)+" are not written")
}

// warnTags warns once per path and set of tags that the tags go into the
// description.
func (ps *pass) warnTags(tags []string) {
	if len(tags) == 0 {
		return
	}

	list := strings.Join(tags, " ")

	ps.warnOnce("tags "+ps.path+" "+list, labels.Name(ps.labelPrefix, labels.KeyTags)+
		" are written into the snapshot description: kopia server cannot set the tags of a snapshot through its API yet",
		"path", ps.path, "tags", list)
}

// description returns what a snapshot of the source is described as: for
// each container that backs up the path, and each that asks for the
// snapshot, its description label or else its name, each text once and in
// the order of the names, then their tags as key=value in brackets. It
// returns the tags too. A snapshot taken again keeps the description of
// the first attempt.
func (ps *pass) description(reqs []snapRequest) (string, []string) {
	if len(reqs) > 0 && !slices.ContainsFunc(reqs, func(r snapRequest) bool { return !r.retry || r.description == "" }) {
		return reqs[0].description, nil
	}

	var cs []describedContainer

	for _, o := range ps.owners {
		cs = append(cs, o.snap.described)
	}

	for _, r := range reqs {
		if r.container.name != "" && !slices.ContainsFunc(cs, func(c describedContainer) bool { return c.name == r.container.name }) {
			cs = append(cs, r.container)
		}
	}

	slices.SortStableFunc(cs, func(a, b describedContainer) int { return strings.Compare(a.name, b.name) })

	var texts, tags []string

	for _, c := range cs {
		text := c.description
		if text == "" {
			text = c.name
		}

		if !slices.Contains(texts, text) {
			texts = append(texts, text)
		}

		for _, t := range c.tags {
			if kv := t.Key + "=" + t.Value; !slices.Contains(tags, kv) {
				tags = append(tags, kv)
			}
		}
	}

	if len(texts) == 0 {
		for _, r := range reqs {
			if r.description != "" {
				return r.description, nil
			}
		}

		return "", nil
	}

	desc := strings.Join(texts, ", ")
	if len(tags) > 0 {
		desc += " [" + strings.Join(tags, " ") + "]"
	}

	return desc, tags
}

// updateRecord changes the snapshot fields of the record of src with fn,
// which returns false to leave it alone. A failure is logged: the snapshot
// steps go on without it.
func (r *Reconciler) updateRecord(src backend.Source, fn func(rec *state.Record) bool) {
	err := r.store.Update(src, func(rec *state.Record) error {
		if !fn(rec) {
			return errNoChange
		}

		return nil
	})

	if err != nil && !errors.Is(err, errNoChange) && !errors.Is(err, state.ErrNotPersisted) {
		r.log.Error("cannot store the snapshot state of the source", "path", src.Path, "source", src.String(), "error", err.Error())
	}
}

// warnOnce logs a warning the first time key comes up in this process.
func (r *Reconciler) warnOnce(key, msg string, attrs ...any) {
	if r.once(key) {
		r.log.Warn(msg, attrs...)
	}
}

// once reports whether key comes up for the first time in this process.
func (r *Reconciler) once(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	seen := r.warned[key]
	r.warned[key] = true

	return !seen
}

// watcherStart makes up a start event of every running container that
// backs up a path, for SNAPSHOT_ON_WATCHER_START; the passes then take the
// snapshots that the containers' labels and the minimum interval allow.
// r.mu is held.
func (r *Reconciler) watcherStart(containers []docker.Container) {
	now := r.now()

	for _, c := range containers {
		if !c.State.Running() {
			continue
		}

		pc := r.parse(c)
		if !pc.spec.Enable {
			continue
		}

		ev := docker.Event{Kind: docker.KindStarted, Action: watcherStartAction, ContainerID: c.ID, Name: c.Name,
			Labels: c.Labels, Time: now}

		for _, p := range pc.paths {
			r.pending[p] = compact(append(r.pending[p], ev))
		}
	}
}

// startWait follows the snapshot of w in a goroutine of its own, unless
// this process follows it already or Run is not running: the wait stays
// in the state then, for a later pass. Run's context is checked and the
// goroutine counted under r.mu, so Run, which waits for the waiters after
// its passes, never misses one.
func (r *Reconciler) startWait(w state.SnapshotWait) {
	key := waitKey{src: w.Ticket.Source, id: w.ID}

	r.mu.Lock()
	ctx := r.runCtx

	if ctx == nil || ctx.Err() != nil || r.waiting[key] {
		r.mu.Unlock()
		return
	}

	r.waiting[key] = true
	r.waiters.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.waiters.Done()

		r.wait(ctx, w)

		r.mu.Lock()
		delete(r.waiting, key)
		r.mu.Unlock()

		if r.afterWait != nil {
			r.afterWait(w)
		}
	}()
}

// wait follows the snapshot of w until it ends, keeping the ticket in the
// state up to date, then stores the result with the wait, reports it to
// Metrics, unless it is SnapshotUnknown, and queues the path, whose next
// pass acts on it. A result whose wait is gone from the state is neither
// stored nor reported. A temporary
// error is retried with a growing delay. When ctx is done, kopia server's
// identity changed, or the backend fails otherwise, the wait stays in the
// state and a later pass follows it again. Only a backend that cannot
// follow snapshots drops it.
func (r *Reconciler) wait(ctx context.Context, w state.SnapshotWait) {
	src := w.Ticket.Source
	log := r.log.With("path", src.Path, "source", src.String(), "reason", w.Reason)

	var (
		mu     sync.Mutex
		ticket = w.Ticket
	)

	save := func(t backend.SnapshotTicket) {
		mu.Lock()
		ticket = t
		mu.Unlock()

		r.updateWait(src, w.ID, func(x *state.SnapshotWait) { x.Ticket = t })
	}

	for attempt := 1; ; attempt++ {
		mu.Lock()
		t := ticket
		mu.Unlock()

		res, err := r.backend.WaitSnapshot(ctx, t, save)
		if ctx.Err() != nil {
			return
		}

		switch {
		case err == nil:
			result := &state.SnapshotResult{Outcome: res.Outcome, TaskID: res.TaskID, Error: res.Error}
			if res.Snapshot != nil {
				result.SnapshotStart = res.Snapshot.StartTime
			}

			// The triggers of a snapshot with hooks get the result before a
			// pass can see it and end the run.
			r.waitEnded(src, w.ID, result, "")

			// Reported along with the result, with the store locked: no pass
			// can act on the result, and forget the source, before. A run
			// kopia server lost track of may have saved its snapshot all the
			// same; the pass that looks for it reports it if not.
			if r.updateWait(src, w.ID, func(x *state.SnapshotWait) {
				x.Result = result

				if res.Outcome != backend.SnapshotUnknown {
					r.metrics.SnapshotFinished(src.Path, res)
				}
			}) {
				r.queue.addUnlessRetrying(src.Path)
			}

			return
		case errors.Is(err, backend.ErrNotImplemented):
			r.warnOnce("WaitSnapshot", "the backend cannot follow snapshots yet; their results are not checked")
			r.updateRecord(src, func(rec *state.Record) bool {
				n := len(rec.SnapshotWaits)
				rec.SnapshotWaits = slices.DeleteFunc(rec.SnapshotWaits, func(x state.SnapshotWait) bool { return x.ID == w.ID })
				prunePins(rec)

				return len(rec.SnapshotWaits) != n
			})
			r.waitEnded(src, w.ID, nil, "the backend cannot follow snapshots")

			return
		case errors.Is(err, backend.ErrWrongIdentity):
			r.forgetIdentity(src)
			log.Warn("kopia server's identity changed; the snapshot is followed again by a pass under the source's identity",
				"error", err.Error())

			return
		case backend.IsTemporary(err):
			delay := backoff(r.minBackoff, r.maxBackoff, attempt)
			log.Warn("cannot follow the snapshot; trying again", "error", err.Error(), "delay", delay.String())

			if !sleep(ctx, delay) {
				return
			}
		default:
			log.Error("cannot follow the snapshot; trying again at the next pass over the path", "error", err.Error())

			return
		}
	}
}

// updateWait changes the wait with the given ID in the record of src and
// reports whether there was one.
func (r *Reconciler) updateWait(src backend.Source, id string, fn func(w *state.SnapshotWait)) bool {
	found := false

	r.updateRecord(src, func(rec *state.Record) bool {
		i := slices.IndexFunc(rec.SnapshotWaits, func(w state.SnapshotWait) bool { return w.ID == id })
		if i < 0 {
			return false
		}

		found = true
		fn(&rec.SnapshotWaits[i])

		return true
	})

	return found
}

// compact keeps of each container's events the last one of each kind, in
// their order, and at most maxPending of them, the latest. Snapshots
// depend on whether a container started or stopped, not on how often, so
// a crash-looping container does not push the events of others out.
func compact(evs []docker.Event) []docker.Event {
	type key struct {
		id   string
		kind docker.Kind
	}

	last := make(map[key]int, len(evs))
	for i, ev := range evs {
		last[key{ev.ContainerID, ev.Kind}] = i
	}

	out := make([]docker.Event, 0, len(last))

	for i, ev := range evs {
		if last[key{ev.ContainerID, ev.Kind}] == i {
			out = append(out, ev)
		}
	}

	return latest(out)
}
