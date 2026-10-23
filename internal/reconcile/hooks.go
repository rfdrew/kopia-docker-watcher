package reconcile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// hookLabels is what a container's labels say about the hooks around the
// snapshots of its sources.
type hookLabels struct {
	// set reports that the pre-exec or post-exec label is set, also when
	// its value has an error.
	set       bool
	pre, post hookCommand
}

// hookCommand is one hook of a container.
type hookCommand struct {
	// cmd is the command; nil without one.
	cmd     []string
	timeout time.Duration
	// err keeps a hook that is set from running: its label, or the
	// hook-timeout label, has an error.
	err error
}

// isSet reports whether the container has the hook.
func (h hookCommand) isSet() bool { return h.cmd != nil || h.err != nil }

// hookLabels reads what the labels of pc say about hooks.
func (r *Reconciler) hookLabels(pc parsed) hookLabels {
	hl := hookLabels{set: pc.spec.Hooks}
	if !hl.set {
		return hl
	}

	timeout := cmp.Or(pc.spec.HookTimeout, labels.DefaultHookTimeout)
	timeoutErr := r.labelError(pc, labels.KeyHookTimeout)

	hook := func(key string, cmd []string) hookCommand {
		h := hookCommand{cmd: cmd, timeout: timeout, err: r.labelError(pc, key)}
		if h.err == nil && cmd != nil {
			h.err = timeoutErr
		}

		return h
	}

	hl.pre = hook(labels.KeyPreExec, pc.spec.PreExec)
	hl.post = hook(labels.KeyPostExec, pc.spec.PostExec)

	return hl
}

// labelError returns the errors of the label with the key, nil if it has
// none.
func (r *Reconciler) labelError(pc parsed, key string) error {
	name := labels.Name(r.labelPrefix, key)

	var msgs []string

	for _, is := range pc.issues {
		if is.Label == name && is.Severity == labels.Error {
			msgs = append(msgs, is.String())
		}
	}

	if len(msgs) == 0 {
		return nil
	}

	return errors.New(strings.Join(msgs, "; "))
}

// hooked reports whether an owner runs hooks: the watcher then takes the
// scheduled snapshots of the source itself.
func hooked(owners []owner) bool {
	return slices.ContainsFunc(owners, func(o owner) bool { return o.hooks.set })
}

// hookTarget is a container to run a hook in.
type hookTarget struct {
	id, name string
	hookCommand
}

// hookTargets returns the running owners that have the hook, in name
// order.
func hookTargets(owners []owner, hook string) []hookTarget {
	var out []hookTarget

	for _, o := range owners {
		h := o.hooks.post
		if hook == HookPreExec {
			h = o.hooks.pre
		}

		if o.state.Running() && h.isSet() {
			out = append(out, hookTarget{id: o.id, name: o.name, hookCommand: h})
		}
	}

	return out
}

// targetNames returns the names of the targets.
func targetNames(ts []hookTarget) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.name
	}

	return out
}

// startHookRun begins a snapshot with hooks for reqs, which ask for one of
// a source whose owners run hooks: the pre-exec hooks of the running
// owners run in a goroutine of their own, and the pass after them queues
// the snapshot. Without a pre-exec hook, the snapshot is queued at once.
// The run is stored first, with the time of the attempt, which the minimum
// interval counts from even if no snapshot comes of it, and the waits the
// requests take again go; the run takes the requests, so a pass that
// fails after it does not ask for them again. The goroutine is set up
// before the run is stored: a run that no goroutine took would look like
// one a stop of the watcher cut short.
func (ps *pass) startHookRun(reqs []snapRequest, status *backend.SourceStatus, reasons []string) error {
	now := ps.now()
	desc, tags := ps.description(reqs)
	ps.warnTags(tags)

	run := state.HookRun{
		ID:          now.UTC().Format(time.RFC3339Nano),
		Started:     now,
		Phase:       state.HookPre,
		Reason:      strings.Join(reasons, ", "),
		Description: desc,
		Retried:     !slices.ContainsFunc(reqs, func(r snapRequest) bool { return !r.retry }),
	}

	replaced := map[string]bool{}

	for _, r := range reqs {
		if r.replaces != "" {
			replaced[r.replaces] = true
		}
	}

	pre := hookTargets(ps.owners, HookPreExec)
	run.PreDone = len(pre) == 0

	var hookCtx context.Context

	if !run.PreDone {
		ctx, ok := ps.reserveHooks(run.ID)
		if !ok {
			ps.keepEvents(reqs)
			ps.log.Debug("not starting a snapshot with hooks now: hooks of the source run, or the watcher stops",
				"source", ps.src.String(), "reasons", reasons)

			return nil
		}

		hookCtx = ctx
	}

	err := ps.store.Update(ps.src, func(r *state.Record) error {
		if r.HookRun != nil {
			return errNoChange
		}

		r.SnapshotWaits = slices.DeleteFunc(r.SnapshotWaits, func(x state.SnapshotWait) bool { return replaced[x.ID] })
		r.LastTriggered = now
		r.HookRun = &run
		ps.takeDue(r)

		return nil
	})

	if errors.Is(err, errNoChange) || storeErr(err) != nil {
		if hookCtx != nil {
			ps.releaseHooks()
		}

		if errors.Is(err, errNoChange) {
			return nil
		}

		return storeErr(err)
	}

	ps.dropEvents(reqs)

	attrs := []any{"source", ps.src.String(), "reasons", reasons, "pre_exec", targetNames(pre),
		"post_exec", targetNames(hookTargets(ps.owners, HookPostExec))}

	if plan := ps.downPlan(); len(plan.items) > 0 {
		attrs = append(attrs, "take_down", downNames(plan.items))
	}

	ps.log.Info("snapshot with hooks started", attrs...)

	if run.PreDone {
		return ps.afterPre(run, status)
	}

	ps.goHooks(hookCtx, run, HookPreExec, pre)

	return nil
}

// dropEvents removes the events of reqs from those of the pass, which a
// pass that fails puts back.
func (ps *pass) dropEvents(reqs []snapRequest) {
	for _, r := range reqs {
		if r.event == nil {
			continue
		}

		ev := *r.event
		ps.events = slices.DeleteFunc(ps.events, func(x docker.Event) bool {
			return x.ContainerID == ev.ContainerID && x.Kind == ev.Kind && x.Action == ev.Action && x.Time.Equal(ev.Time)
		})
	}
}

// hookRun moves the snapshot with hooks under way on, as far as it can go
// in this pass: once the pre-exec hooks have run, it queues the snapshot,
// or goes to the post-exec hooks if they failed; once the snapshot has
// ended, however it ended, it runs the post-exec hooks. A phase that a stop
// of the watcher cut short is taken up: pre-exec hooks it cut short count
// as failed, as their effect is not known, and post-exec hooks run again.
// While a goroutine runs hooks of the source, the pass leaves the run
// alone: the goroutine queues the path when it is done. It stores what it
// did before it counts as done, so a run read after that check is up to
// date.
func (ps *pass) hookRun(status *backend.SourceStatus) error {
	if ps.hooksBusy() {
		return nil
	}

	rec, _ := ps.store.Get(ps.src)

	run := rec.HookRun
	if run == nil {
		return nil
	}

	switch run.Phase {
	case state.HookPre:
		return ps.afterPre(*run, status)
	case state.HookDown:
		return ps.afterDown(*run, status)
	case state.HookSnapshot:
		// The retry loop brings the containers back by the run's ReturnBy,
		// should the snapshot take longer.
		i := slices.IndexFunc(rec.SnapshotWaits, func(w state.SnapshotWait) bool { return w.ID == run.WaitID })
		if i >= 0 && rec.SnapshotWaits[i].Result == nil {
			return nil
		}

		return ps.toPost(*run)
	case state.HookPost:
		ps.log.Warn("running the post-exec hooks again: the watcher stopped while they ran", "source", ps.src.String(),
			"reason", run.Reason)

		return ps.toPost(*run)
	default:
		ps.log.Warn("dropping a snapshot with hooks in a phase the watcher does not know", "source", ps.src.String(),
			"phase", run.Phase)
		ps.endHookRun(run.ID)

		return nil
	}
}

// afterPre queues the snapshot of a run whose pre-exec hooks succeeded,
// after it took down the containers that ask for it, or goes to the
// post-exec hooks of one whose hooks failed or could not queue it.
func (ps *pass) afterPre(run state.HookRun, status *backend.SourceStatus) error {
	attrs := []any{"source", ps.src.String(), "reason", run.Reason}

	if !run.PreDone {
		ps.log.Warn("no snapshot: the watcher stopped while the pre-exec hooks ran", attrs...)
		return ps.toPost(run)
	}

	if run.PreError != "" {
		ps.log.Warn("no snapshot: a pre-exec hook failed", append(attrs, "error", run.PreError)...)
		return ps.toPost(run)
	}

	if status != nil && status.Status == backend.StatusStopped {
		ps.log.Warn("no snapshot: kopia server does not run snapshots of the source now", append(attrs, "status", string(status.Status))...)
		return ps.toPost(run)
	}

	plan := ps.downPlan()
	if plan.needed() {
		return ps.toDown(run, plan)
	}

	// An owner another snapshot holds down comes back before this
	// snapshot is queued.
	if h, busy := ps.heldBusy(plan, run.ID); busy {
		ps.log.Info("the snapshot waits: a container it needs is held down for another snapshot",
			append(attrs, "container", h.Name, "held_for", h.Source.Path)...)
		ps.waitFor(h.ID)

		return nil
	}

	return ps.queueSnapshot(run)
}

// afterDown queues the snapshot of a run whose containers are down, or
// goes to the post-exec hooks, which bring them back first, of one that
// could not take them all down, or whose containers came back before the
// snapshot could be queued: they had to by then, or the watcher stopped
// meanwhile, or while it took them down, and brought them back. While the
// watcher stops, it queues nothing.
func (ps *pass) afterDown(run state.HookRun, status *backend.SourceStatus) error {
	attrs := []any{"source", ps.src.String(), "reason", run.Reason}

	switch {
	case !run.DownDone:
		ps.log.Warn("no snapshot: the watcher stopped while it took the containers down", attrs...)
		return ps.toPost(run)
	case run.DownError != "":
		ps.metrics.ReconcileError(ps.path, ReasonDocker)
		ps.log.Error("no snapshot: a container could not be stopped or paused for it", append(attrs, "error", run.DownError)...)

		return ps.toPost(run)
	case !ps.now().Before(run.ReturnBy):
		ps.log.Warn("no snapshot: the containers had to come back before it could be queued; raise max-downtime",
			append(attrs, "return_by", run.ReturnBy.Format(timeFormat))...)

		return ps.toPost(run)
	case run.DownTaken > len(ps.heldFor(ps.src, run.ID)):
		ps.log.Warn("no snapshot: the containers came back before it could be queued, as the watcher stopped", attrs...)
		return ps.toPost(run)
	case ps.stopping():
		return nil
	case status != nil && status.Status == backend.StatusStopped:
		ps.log.Warn("no snapshot: kopia server does not run snapshots of the source now", append(attrs, "status", string(status.Status))...)
		return ps.toPost(run)
	}

	return ps.queueSnapshot(run)
}

// queueSnapshot queues the snapshot of a run that is ready for it. An
// error of kopia server that is tried again leaves the run for the retry;
// any other ends it without a snapshot, so that it does not hold the
// post-exec hooks, and the next snapshots, for good.
func (ps *pass) queueSnapshot(run state.HookRun) error {
	attrs := []any{"source", ps.src.String(), "reason", run.Reason}

	ticket, err := ps.backend.TriggerSnapshot(ps.ctx, ps.src)

	switch {
	case errors.Is(err, backend.ErrNotImplemented):
		ps.warnOnce("TriggerSnapshot", "the backend cannot queue snapshots; no snapshot is taken at container starts and stops")
		return ps.toPost(run)
	case err == nil:
	case ps.ctx.Err() != nil, errors.Is(err, backend.ErrWrongIdentity), backend.IsUnavailable(err), backend.IsTemporary(err):
		return err
	default:
		ps.log.Warn("no snapshot: kopia server does not queue it", append(attrs, "error", err.Error())...)

		return errors.Join(err, ps.toPost(run))
	}

	w := state.SnapshotWait{Ticket: ticket, Reason: run.Reason, Description: run.Description, Retried: run.Retried}
	now := ps.now()

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		w.ID = newWaitID(r.SnapshotWaits, w.Ticket.TriggeredAt)
		r.LastTriggered = now
		r.SnapshotWaits = append(r.SnapshotWaits, w)

		if r.HookRun != nil && r.HookRun.ID == run.ID {
			r.HookRun.Phase = state.HookSnapshot
			r.HookRun.PreDone = true
			r.HookRun.WaitID = w.ID
		}

		return true
	})

	ps.log.Info("snapshot queued", "source", ps.src.String(), "reasons", strings.Split(run.Reason, ", "), "hooks", true)
	ps.startWait(w)

	return nil
}

// toPost brings back the containers the run took down and runs the
// post-exec hooks of a run, which ends it. Without a container to bring
// back and a running owner that has a post-exec hook, the run ends at
// once. While no goroutine can run them, as while the watcher stops, the
// run stays as it is, for a later pass or the next start.
func (ps *pass) toPost(run state.HookRun) error {
	post := ps.postTargets(run.ID)

	if len(post) == 0 && len(ps.heldFor(ps.src, run.ID)) == 0 {
		ps.endHookRun(run.ID)
		ps.log.Info("snapshot with hooks done", "source", ps.src.String(), "reason", run.Reason)

		return nil
	}

	ctx, ok := ps.reserveHooks(run.ID)
	if !ok {
		ps.log.Debug("not running the post-exec hooks now: hooks of the source run, or the watcher stops",
			"source", ps.src.String(), "reason", run.Reason)

		return nil
	}

	found := false

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.HookRun == nil || r.HookRun.ID != run.ID {
			return false
		}

		found = true

		if r.HookRun.Phase == state.HookPost {
			return false
		}

		r.HookRun.Phase = state.HookPost

		return true
	})

	if !found {
		ps.releaseHooks()
		return nil
	}

	run.Phase = state.HookPost
	ps.goHooks(ctx, run, HookPostExec, post)

	return nil
}

// postTargets returns the owners whose post-exec hook the run runs: those
// that run, and those it holds down, which it brings back before.
func (ps *pass) postTargets(run string) []hookTarget {
	held := map[string]bool{}
	for _, h := range ps.heldFor(ps.src, run) {
		held[h.ID] = true
	}

	var out []hookTarget

	for _, o := range ps.owners {
		if (o.state.Running() || held[o.id]) && o.hooks.post.isSet() {
			out = append(out, hookTarget{id: o.id, name: o.name, hookCommand: o.hooks.post})
		}
	}

	return out
}

// endHookRun removes the run with the ID from the record.
func (ps *pass) endHookRun(id string) {
	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.HookRun == nil || r.HookRun.ID != id {
			return false
		}

		r.HookRun = nil

		return true
	})
}

// reserveHooks sets up a goroutine to run the hooks of the run with the
// ID, counted under r.mu so that Run, which waits for it after its
// passes, never misses one, and returns the context the hooks run on. It
// fails while Run is not running or is stopping, and while another
// goroutine runs hooks of the source. goHooks starts it, releaseHooks
// gives it up.
func (ps *pass) reserveHooks(id string) (context.Context, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.runCtx == nil || ps.runCtx.Err() != nil || ps.hookCtx == nil || ps.hooksRunning[ps.src] != "" {
		return nil, false
	}

	ps.hooksRunning[ps.src] = id
	ps.hookers.Add(1)

	return ps.hookCtx, true
}

// releaseHooks gives up the goroutine reserveHooks set up.
func (ps *pass) releaseHooks() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.hooksRunning, ps.src)
	ps.hookers.Done()
}

// goHooks runs the hook of the targets in the goroutine reserveHooks set
// up, one target after the other, then stores the outcome in the run and
// queues the path. Before the post-exec hooks, it brings back the
// containers the run took down; the post-exec hook of one that does not
// come back is not run. Hooks a stop of the watcher cut short store
// nothing: the next start takes them up.
func (ps *pass) goHooks(ctx context.Context, run state.HookRun, hook string, targets []hookTarget) {
	r, src, path, log := ps.Reconciler, ps.src, ps.path, ps.log

	go func() {
		defer r.hookers.Done()

		if hook == HookPostExec {
			// They stay held, as back, until the post-exec hooks are done,
			// so that no other snapshot takes them down meanwhile.
			failed := r.returnRun(ctx, log, src, run.ID, true)
			defer r.releaseRun(src, run.ID)

			targets = slices.DeleteFunc(slices.Clone(targets), func(t hookTarget) bool {
				if failed[t.id] {
					log.Warn("post-exec hook not run: the container did not come back", "hook", hook, "container", t.name)
				}

				return failed[t.id]
			})
		}

		failure := r.runHooks(ctx, log, path, hook, targets)

		if ctx.Err() == nil {
			r.updateRecord(src, func(rec *state.Record) bool {
				if rec.HookRun == nil || rec.HookRun.ID != run.ID {
					return false
				}

				if hook == HookPreExec {
					rec.HookRun.PreDone, rec.HookRun.PreError = true, failure
				} else {
					rec.HookRun = nil
				}

				return true
			})

			if hook == HookPostExec {
				log.Info("snapshot with hooks done", "source", src.String(), "reason", run.Reason)
			}
		}

		r.mu.Lock()
		delete(r.hooksRunning, src)
		r.mu.Unlock()

		if ctx.Err() == nil {
			r.queue.addUnlessRetrying(path)
		}

		if r.afterHooks != nil {
			r.afterHooks(src, hook)
		}
	}()
}

// runHooks runs the hook in each target and returns what failed, empty if
// nothing did. The pre-exec hooks stop at the first failure: no snapshot
// comes of the run. The post-exec hooks all run. When ctx ends, the hooks
// left are not run and nothing is reported.
func (r *Reconciler) runHooks(ctx context.Context, log *slog.Logger, path, hook string, targets []hookTarget) string {
	var failures []string

	for _, t := range targets {
		msg := r.runHook(ctx, log, path, hook, t)
		if ctx.Err() != nil {
			return ""
		}

		if msg == "" {
			continue
		}

		failures = append(failures, t.name+": "+msg)

		if hook == HookPreExec {
			break
		}
	}

	return strings.Join(failures, "; ")
}

// runHook runs the hook of one target within its timeout, logs how it
// ended with the end of its output, reports it to Metrics and returns why
// it failed, empty if it did not.
func (r *Reconciler) runHook(ctx context.Context, log *slog.Logger, path, hook string, t hookTarget) string {
	attrs := []any{"hook", hook, "container", t.name}

	if t.err != nil {
		msg := "the hook labels have errors: " + t.err.Error()
		log.Error(hook+" hook not run", append(attrs, "error", msg)...)
		r.metrics.HookFinished(path, HookResult{Hook: hook, Container: t.name, Error: msg})

		return msg
	}

	hctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	// No snapshot of another source takes the container down meanwhile.
	r.mu.Lock()
	r.execs[t.id]++
	r.mu.Unlock()

	start := time.Now()
	res, err := r.docker.Exec(hctx, t.id, t.cmd)
	d := time.Since(start)

	r.mu.Lock()
	if r.execs[t.id]--; r.execs[t.id] <= 0 {
		delete(r.execs, t.id)
	}
	r.mu.Unlock()

	r.wakeWaiters(t.id)

	if ctx.Err() != nil {
		log.Warn(hook+" hook cut short: the watcher stops", attrs...)
		return ""
	}

	var msg string

	switch {
	case err != nil && hctx.Err() != nil:
		msg = fmt.Sprintf("did not finish within %s, the hook timeout; Docker cannot stop it, so it may still run in the container",
			t.timeout)
	case err != nil:
		msg = err.Error()
	case res.ExitCode != 0:
		msg = fmt.Sprintf("exited with code %d", res.ExitCode)
	}

	if res.Started {
		attrs = append(attrs, "duration", d.String())
	} else {
		// A hook Docker did not start has no duration.
		d = 0
	}

	if out := strings.TrimRight(string(res.Output), "\n"); out != "" {
		attrs = append(attrs, "output", out)

		if res.Truncated {
			attrs = append(attrs, "output_truncated", true)
		}
	}

	if msg != "" {
		log.Error(hook+" hook failed", append(attrs, "error", msg)...)
	} else {
		log.Info(hook+" hook finished", attrs...)
	}

	r.metrics.HookFinished(path, HookResult{Hook: hook, Container: t.name, Duration: d, Error: msg})

	return msg
}
