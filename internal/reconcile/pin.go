package reconcile

import (
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// The pins of the pin-on-stop label go with the stop snapshot of the
// container whose stop it is, not with those of the other owners of the
// path: the template is filled in when the stop asks for the snapshot, and
// the pins are kept in the state with the wait of the run that counts for
// the stop (state.Record.WaitPins), or with an absorbed stop
// (StopAbsorbedPins) when that run is one kopia server queued. They are
// added along with the description, in one edit: to the snapshot the run
// saved or, when it saved none because nothing had changed, to the last
// complete snapshot, which holds the same files.

// stopPins returns the pins that the stop of a container with labels sl,
// at at (or at fallback if at is zero), asks for.
func stopPins(sl snapLabels, at, fallback time.Time) []string {
	if sl.pin == "" {
		return nil
	}

	if at.IsZero() {
		at = fallback
	}

	return []string{labels.PinName(sl.pin, at)}
}

// requestPins returns the pins reqs ask for, each once.
func requestPins(reqs []snapRequest) []string {
	var pins []string

	for _, r := range reqs {
		pins = unionPins(pins, r.pins)
	}

	return pins
}

// unionPins returns a with the pins of b it lacks appended.
func unionPins(a, b []string) []string {
	out := slices.Clone(a)

	for _, p := range b {
		if !slices.Contains(out, p) {
			out = append(out, p)
		}
	}

	return out
}

// addWaitPins adds pins to those of the wait with the given ID in r.
func addWaitPins(r *state.Record, id string, pins []string) {
	if len(pins) == 0 {
		return
	}

	if r.WaitPins == nil {
		r.WaitPins = map[string][]string{}
	}

	r.WaitPins[id] = unionPins(r.WaitPins[id], pins)
}

// prunePins drops the pins of waits r no longer has.
func prunePins(r *state.Record) {
	maps.DeleteFunc(r.WaitPins, func(id string, _ []string) bool {
		return !slices.ContainsFunc(r.SnapshotWaits, func(w state.SnapshotWait) bool { return w.ID == id })
	})

	if len(r.WaitPins) == 0 {
		r.WaitPins = nil
	}
}

// absorbedPins returns the pins of the absorbed stop of rec, unless they
// belong to another stop.
func absorbedPins(rec state.Record) []string {
	if rec.StopAbsorbedAt.IsZero() || !rec.StopAbsorbedPinsAt.Equal(rec.StopAbsorbedAt) {
		return nil
	}

	return slices.Clone(rec.StopAbsorbedPins)
}

// waitPins returns the pins of the wait with the given ID.
func (ps *pass) waitPins(id string) []string {
	rec, _ := ps.store.Get(ps.src)

	return slices.Clone(rec.WaitPins[id])
}

// nextTrigger returns when the watcher queued the first run after that of
// w, among waits; zero if it queued none.
func (ps *pass) nextTrigger(w state.SnapshotWait, waits []state.SnapshotWait) time.Time {
	var until time.Time

	for _, x := range waits {
		if x.ID != w.ID && ps.usable(x, false) && x.Ticket.TriggeredAt.After(w.Ticket.TriggeredAt) &&
			(until.IsZero() || x.Ticket.TriggeredAt.Before(until)) {
			until = x.Ticket.TriggeredAt
		}
	}

	return until
}

// annotateFinished describes the snapshot of the finished wait w, which
// started at start (zero if the run saved none), and adds the pins of the
// wait. A run that saved nothing because nothing had changed pins the last
// complete snapshot; one that failed or was canceled pins nothing, with a
// warning. It reports whether the wait is done with: false after a passing
// failure, which a later pass tries again.
func (ps *pass) annotateFinished(w state.SnapshotWait, start time.Time, waits []state.SnapshotWait, mark *time.Time) bool {
	desc := w.Description
	if start.IsZero() || mark != nil && !start.After(*mark) || slices.ContainsFunc(ps.described, start.Equal) {
		desc = ""
	}

	target, pins := start, ps.waitPins(w.ID)

	if len(pins) > 0 {
		switch w.Result.Outcome {
		case backend.SnapshotIdentical:
			last, found, decided := ps.lastIdentical(w, waits)

			switch {
			case !decided:
				return false
			case found:
				target = last
			default:
				ps.log.Warn("not pinning the stop snapshot: nothing changed since the last snapshot, but no complete one is listed",
					"source", ps.src.String(), "reason", w.Reason, "pins", pins)

				pins = nil
			}
		case backend.SnapshotSucceeded, backend.SnapshotIncomplete, backend.SnapshotUnknown:
			// SnapshotUnknown comes here only with the snapshot the run saved.
		default:
			ps.log.Warn("not pinning the stop snapshot: the run did not save a complete one", "source", ps.src.String(),
				"reason", w.Reason, "outcome", string(w.Result.Outcome), "pins", pins)

			pins = nil
		}
	}

	if target.IsZero() || desc == "" && len(pins) == 0 {
		return true
	}

	err := ps.annotate(ps.src, target, desc, pins)

	return err == nil || errors.Is(err, backend.ErrSnapshotNotFound) || errors.Is(err, backend.ErrNotImplemented) ||
		backend.IsConfig(err)
}

// lastIdentical returns the start of the last complete snapshot of the
// source that started before w's run was queued: the one that run, which
// saved nothing because nothing had changed, found the same. A snapshot
// saved between the trigger and the run is one that was uploading then,
// which started before. The bound allows kopia server's clock to be ahead
// by clockSkew, and stops at the next run the watcher queued, so a
// snapshot taken after the stop, as after an update, is never the one. found
// is false if there is none; decided is false when listing failed for a
// passing reason.
func (ps *pass) lastIdentical(w state.SnapshotWait, waits []state.SnapshotWait) (last time.Time, found, decided bool) {
	if ps.cannotDescribe() {
		return time.Time{}, false, true
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnNotDescribing()

		return time.Time{}, false, true
	case err != nil:
		ps.log.Warn("cannot list the snapshots of the source to pin the last one; trying again at the next pass",
			"source", ps.src.String(), "error", err.Error())

		return time.Time{}, false, false
	}

	until := w.Ticket.TriggeredAt.Add(clockSkew)
	if next := ps.nextTrigger(w, waits); !next.IsZero() && next.Before(until) {
		until = next
	}

	for _, sn := range snaps {
		if sn.Incomplete == "" && sn.StartTime.Before(until) && sn.StartTime.After(last) {
			last = sn.StartTime
		}
	}

	return last, !last.IsZero(), true
}
