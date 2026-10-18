package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// previewCount is how many upcoming snapshot times a schedule check asks
// for.
const previewCount = 3

// passResult is what one pass over a path ended with.
type passResult struct {
	// err ended the pass before the source reached the desired state; nil
	// also when the pass was canceled.
	err error
	// retry asks for another pass after a delay: err is temporary.
	retry bool
	// restarting reports an owner that was restarting.
	restarting bool
	// recheck asks for another pass after EventDelay, which decides on the
	// start snapshot this pass put off.
	recheck bool
	// problems were reported on the way; the pass went on without them.
	problems []error
}

// pass is one reconcile of one path.
type pass struct {
	*Reconciler

	ctx  context.Context
	path string
	log  *slog.Logger
	// started is when the pass began.
	started time.Time

	// events are the Docker events that queued the path since its last
	// pass; keep are those to put back after the pass, as a die of a
	// container that is restarting.
	events []docker.Event
	keep   []docker.Event

	// containers is the listing of the pass.
	containers []docker.Container

	// described are the start times of the snapshots the pass described.
	described []time.Time

	// wrote reports that the pass wrote the policy. deferred reports that
	// an earlier pass put the start snapshot off already, deferStart that
	// this one does. sweepDue reports a pass of a full reconcile, which
	// describes the snapshots that have no description.
	wrote, deferred, deferStart, sweepDue bool

	src backend.Source
	// last is what the state says was applied to the source; had reports
	// whether the state has a record of it.
	last backend.Applied
	had  bool

	owners []owner
	want   backend.Desired
	// fallback is set when want.Patch is the last applied patch because
	// the labels could not be used, or because the path has no owner. A
	// source kopia server does not know is then not registered.
	fallback bool

	// plan is the plan the pass got to; outcome says why it stopped
	// without one, or without applying it.
	plan    *backend.Plan
	outcome string

	problems []error
	// reported are the keys of the errors this pass reported.
	reported []string
	preview  *backend.SchedulePreview
}

// reconcile runs one pass over path p.
func (r *Reconciler) reconcile(ctx context.Context, p string) passResult {
	ps := &pass{Reconciler: r, ctx: ctx, path: p, log: r.log.With("path", p), started: r.now(), events: r.takePending(p)}
	ps.deferred, ps.sweepDue = r.takeFlags(p)
	ps.log.Debug("reconciling the path", "events", eventNames(ps.events))

	err := ps.run()
	res := passResult{restarting: anyRestarting(ps.owners), problems: ps.problems, recheck: err == nil && ps.deferStart}

	switch {
	case err == nil:
	case ctx.Err() != nil:
		// Shutting down: the error is not the source's.
		ps.log.Debug("reconcile canceled", "error", err.Error())

		return res
	default:
		res.err = err
		res.retry = ps.fail(err)
	}

	if err != nil {
		r.restorePending(p, ps.events)
		r.restoreFlags(p, ps.deferred, ps.sweepDue)
	} else {
		r.restorePending(p, ps.keep)
		r.restoreFlags(p, ps.deferStart, false)
	}

	// A temporary error says nothing about whether the problems of the
	// path are resolved.
	if r.setReported(p, ps.reported, res.retry) && !res.retry && len(ps.reported) == 0 && err == nil {
		ps.log.Info("the problems of the path are resolved", "source", ps.src.String())
	}

	if r.dryRun {
		ps.print(err)
	}

	return res
}

// eventNames describes events for the log, e.g. "die app".
func eventNames(evs []docker.Event) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = ev.Action + " " + ev.Name
	}

	return out
}

// fail reports err and says whether to retry the path after a delay.
func (ps *pass) fail(err error) (retry bool) {
	attrs := []any{"error", err.Error()}
	if ps.src != (backend.Source{}) {
		attrs = append(attrs, "source", ps.src.String())
	}

	var (
		de *dockerError
		se *stateError
	)

	switch {
	case errors.As(err, &de):
		ps.metrics.ReconcileError(ps.path, ReasonDocker)
		ps.log.Warn("cannot list Docker containers; retrying", attrs...)

		return true
	case errors.As(err, &se):
		ps.reportError(ReasonState, "cannot store the state of the source; the policy was not written", attrs...)

		return false
	case errors.Is(err, backend.ErrWrongIdentity):
		ps.forgetIdentity(ps.src)
		ps.metrics.ReconcileError(ps.path, ReasonTemporary)
		ps.log.Warn("kopia server's identity changed; retrying", attrs...)

		return true
	case backend.IsUnavailable(err):
		ps.metrics.ReconcileError(ps.path, ReasonUnavailable)
		ps.log.Warn("kopia server is unavailable; retrying", attrs...)

		return true
	case backend.IsTemporary(err):
		ps.metrics.ReconcileError(ps.path, ReasonTemporary)
		ps.log.Warn("kopia server failed; retrying", attrs...)

		return true
	case backend.IsConfig(err):
		ps.reportError(ReasonConfig, "kopia server rejects the source; fix the labels or kopia server, it is tried "+
			"again when they change and at every full reconcile", attrs...)

		return false
	default:
		ps.reportError(ReasonFailed, "reconcile failed; it is tried again at the next full reconcile", attrs...)

		return false
	}
}

// problem reports something that keeps the labels from being applied as
// they are; the pass goes on.
func (ps *pass) problem(reason Reason, msg string, err error) {
	ps.problems = append(ps.problems, fmt.Errorf("%s: %w", ps.path, err))
	ps.reportError(reason, msg, "source", ps.src.String(), "owners", ownerNames(ps.owners), "error", err.Error())
}

// reportError logs an error that is not retried and counts it. An error
// the last pass over the path reported already is only logged at debug
// level: a crash-looping owner brings a pass every few seconds. Every full
// reconcile reports all errors again.
func (ps *pass) reportError(reason Reason, msg string, attrs ...any) {
	key := string(reason) + " " + msg + fmt.Sprint(attrs...)
	ps.reported = append(ps.reported, key)

	if ps.reportedBefore(key) {
		ps.log.Debug(msg, attrs...)
		return
	}

	ps.metrics.ReconcileError(ps.path, reason)
	ps.log.Error(msg, attrs...)
}

func (ps *pass) run() error {
	src, err := ps.source(ps.ctx, ps.path)
	if err != nil {
		return err
	}

	ps.src = src

	containers, err := ps.Reconciler.containers(ps.ctx)
	if err != nil {
		return &dockerError{err: err}
	}

	ps.containers = containers
	ps.owners = ps.Reconciler.owners(ps.path, containers)

	rec, had := ps.store.Get(src)
	ps.last, ps.had = rec.Applied, had

	if !ps.desire() {
		ps.outcome = "not managed: no container backs the path up"
		ps.log.Debug("no container backs up the path and the watcher never managed it")

		return nil
	}

	plan, err := ps.planSource()
	if err != nil {
		return err
	}

	if plan.Action == backend.ActionRegister {
		switch {
		case ps.fallback:
			ps.outcome = "not registered: no container backs it up with labels that can be applied"
			ps.log.Info("not registering the source: no container backs it up with labels that can be applied",
				"source", src.String(), "owners", ownerNames(ps.owners))

			return nil
		case ps.want.Paused:
			// A container that was only created may not have its bind
			// mounts on the host yet; register when one runs.
			ps.outcome = "not registered until a container that backs it up runs"
			ps.log.Debug("not registering the source until a container that backs it up runs",
				"source", src.String(), "owners", ownerNames(ps.owners))

			return nil
		}
	}

	if plan, err = ps.checkSchedule(plan); err != nil {
		return err
	}

	ps.plan = &plan

	if ps.dryRun {
		return nil
	}

	if err := ps.apply(plan); err != nil {
		return err
	}

	return ps.snapshots()
}

// desire works out the desired state of the source from its owners. It
// returns false when there is nothing to do: no owner, and no record of
// the source.
func (ps *pass) desire() bool {
	ps.want = backend.Desired{Source: ps.src, Owners: ownerNames(ps.owners)}

	if len(ps.owners) == 0 {
		if !ps.had {
			return false
		}

		// The containers are gone or no longer back the path up: pause
		// the source and leave its fields as they are.
		ps.want.Patch = ps.last.Patch.Clone()
		ps.want.Paused = true
		ps.fallback = true

		return true
	}

	ps.want.Paused = !anyRunning(ps.owners)

	patch, err := commonPatch(policyOwners(ps.owners))
	if err != nil {
		patch = ps.last.Patch.Clone()
		ps.fallback = true

		var le *labelsError

		switch {
		case ps.want.Paused:
			// The source is paused and only its saved section would
			// change; the labels of whichever owner starts first decide.
			ps.log.Warn("stopped containers that share the path ask for different policies or have labels that "+
				"cannot be applied; keeping the last applied patch until one of them starts",
				"source", ps.src.String(), "owners", ownerNames(ps.owners), "error", err.Error())
		case errors.As(err, &le):
			ps.problem(ReasonLabels, "labels cannot be applied; keeping the last applied patch", err)
		default:
			ps.problem(ReasonOwnersDisagree, "running containers disagree on the policy of the path; keeping the last applied patch", err)
		}
	}

	ps.want.Patch = patch

	return true
}

// planSource plans the source. When the labels set a field inside one the
// policy holds as a value, it plans again with the last applied patch.
func (ps *pass) planSource() (backend.Plan, error) {
	plan, err := ps.backend.PlanSource(ps.ctx, ps.want, ps.last)
	if err == nil || !backend.IsConfig(err) || !ps.useLastPatch() {
		return plan, err
	}

	ps.problem(ReasonConfig, "the labels do not fit the policy of the path; keeping the last applied patch", err)

	return ps.backend.PlanSource(ps.ctx, ps.want, ps.last)
}

// useLastPatch falls back to the last applied patch. It returns false when
// the pass already wants that patch, so falling back changes nothing.
func (ps *pass) useLastPatch() bool {
	if ps.fallback || ps.want.Patch.Equal(ps.last.Patch) {
		return false
	}

	ps.want.Patch = ps.last.Patch.Clone()
	ps.fallback = true

	return true
}

// checkSchedule asks kopia server whether it accepts the schedule of a new
// source or of a new patch, before anything with it is stored or applied.
// A rejected schedule of a known source falls back to the last applied
// patch; a new source is not registered.
func (ps *pass) checkSchedule(plan backend.Plan) (backend.Plan, error) {
	if plan.Action != backend.ActionRegister && plan.Applied.Patch.Equal(ps.last.Patch) {
		return plan, nil
	}

	preview, err := ps.backend.PreviewSchedule(ps.ctx, ps.src, plan.Unpaused, previewCount)

	switch {
	case err == nil:
		ps.preview = &preview

		return plan, nil
	case !backend.IsConfig(err):
		return plan, err
	case plan.Action == backend.ActionRegister || !ps.useLastPatch():
		ps.outcome = "not applied: kopia server rejects the schedule"

		return plan, err
	}

	ps.problem(ReasonConfig, "kopia server rejects the schedule; keeping the last applied patch", err)

	return backend.Replan(plan, ps.want, ps.last)
}

// apply stores the intent, applies the plan and stores what was applied.
func (ps *pass) apply(plan backend.Plan) error {
	if plan.Action == backend.ActionNone {
		if !plan.Applied.Equal(ps.last) {
			if err := ps.storeApplied(plan.Applied); err != nil {
				return err
			}
		}

		ps.log.Debug("source is up to date", "source", ps.src.String(), "paused", ps.want.Paused)

		return nil
	}

	if err := ps.storeApplied(plan.Intent); err != nil {
		return err
	}

	applied, err := ps.backend.ApplyPlan(ps.ctx, plan)

	switch {
	case err == nil:
	case !backend.IsConfig(err):
		return err
	case plan.Action == backend.ActionRegister:
		// The registration failed before kopia server stored anything
		// (the path does not exist, say): forget the intent, so that the
		// state lists no source the watcher does not manage.
		ps.outcome = "not registered: kopia server rejects the source"

		if rerr := ps.restoreRecord(); rerr != nil {
			return errors.Join(err, rerr)
		}

		return err
	case !ps.useLastPatch():
		return err
	default:
		// kopia server checks more when it stores a policy than when it
		// previews a schedule: fall back to the last applied patch.
		ps.problem(ReasonConfig, "kopia server rejects the policy; keeping the last applied patch", err)

		if plan, err = backend.Replan(plan, ps.want, plan.Intent); err != nil {
			return err
		}

		ps.plan = &plan

		if err := ps.storeApplied(plan.Intent); err != nil {
			return err
		}

		if applied, err = ps.backend.ApplyPlan(ps.ctx, plan); err != nil {
			return err
		}
	}

	ps.wrote = true

	if err := ps.storeApplied(applied); err != nil {
		return err
	}

	ps.logApplied(plan)

	return nil
}

// storeApplied sets the Applied of the source's record. A change kept in
// memory only is enough: the store writes it with the next change.
func (ps *pass) storeApplied(a backend.Applied) error {
	err := ps.store.Update(ps.src, func(r *state.Record) error {
		r.Applied = a.Clone()
		return nil
	})

	return storeErr(err)
}

// restoreRecord puts the source's record back as the pass found it, after
// a registration that kopia server refused. A record that holds nothing
// confirmed, only the intent of an earlier registration that never went
// through, is removed instead, unless it holds the state of snapshots.
func (ps *pass) restoreRecord() error {
	rec, _ := ps.store.Get(ps.src)
	snapshots := len(rec.SnapshotWaits) > 0 || !rec.LastTriggered.IsZero() || rec.AnnotateAfter != nil

	confirmed := !ps.last.Patch.IsZero() || ps.last.SavedScheduling != nil || ps.last.StoredHash != ""
	if ps.had && (confirmed || snapshots) {
		return ps.storeApplied(ps.last)
	}

	return storeErr(ps.store.Delete(ps.src))
}

func storeErr(err error) error {
	if err == nil || errors.Is(err, state.ErrNotPersisted) {
		return nil
	}

	return &stateError{err: err}
}

func (ps *pass) logApplied(plan backend.Plan) {
	msg := "source policy updated"
	if plan.Action == backend.ActionRegister {
		msg = "source registered"
	}

	attrs := []any{"source", ps.src.String(), "owners", ps.want.Owners, "paused", ps.want.Paused, "changes", changes(plan)}
	if len(ps.events) > 0 {
		attrs = append(attrs, "events", eventNames(ps.events))
	}

	if t := ps.nextSnapshot(); t != "" {
		attrs = append(attrs, "next_snapshot", t)
	}

	ps.log.Info(msg, attrs...)
}

// nextSnapshot formats the first upcoming time of the schedule check; empty
// without one or while paused.
func (ps *pass) nextSnapshot() string {
	if ps.preview == nil || ps.want.Paused || len(ps.preview.Times) == 0 {
		return ""
	}

	return ps.preview.Times[0].Format(timeFormat)
}

// dockerError: listing the containers failed.
type dockerError struct{ err error }

func (e *dockerError) Error() string { return "list containers: " + e.err.Error() }
func (e *dockerError) Unwrap() error { return e.err }

// stateError: the state store refused a change.
type stateError struct{ err error }

func (e *stateError) Error() string { return "store state: " + e.err.Error() }
func (e *stateError) Unwrap() error { return e.err }

// reportedBefore reports whether the last pass over the path reported the
// error with key since the last full reconcile.
func (ps *pass) reportedBefore(key string) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	scan, ok := ps.reportedErrors[ps.path][key]

	return ok && scan == ps.scans
}

// setReported remembers the errors a pass over p reported, in place of
// those of the pass before, and returns whether that one reported any.
// A pass that is retried adds its errors instead: it found them again, so
// its retry must not report them once more.
func (r *Reconciler) setReported(p string, keys []string, retried bool) (hadBefore bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	hadBefore = len(r.reportedErrors[p]) > 0

	m := r.reportedErrors[p]
	if !retried || m == nil {
		m = make(map[string]uint64, len(keys))
	}

	for _, k := range keys {
		m[k] = r.scans
	}

	if len(m) == 0 {
		delete(r.reportedErrors, p)
	} else {
		r.reportedErrors[p] = m
	}

	return hadBefore
}

// forgetReported makes the next pass over every path report its errors
// again.
func (r *Reconciler) forgetReported() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.scans++
}
