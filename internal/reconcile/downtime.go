package reconcile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	// ownEventWindow is how long the events of the watcher's own stop or
	// start of a container are waited for; one that comes later is taken
	// for someone else's.
	ownEventWindow = 2 * time.Minute

	// heldRecheck is how soon a snapshot with hooks that waits for
	// containers held down for another snapshot looks again, should their
	// return not wake it.
	heldRecheck = 5 * time.Minute

	// composeServiceLabel names the docker compose service of a container.
	composeServiceLabel = "com.docker.compose.service"
)

// downLabels is what a container's labels say about taking it down around
// the snapshots of its sources.
type downLabels struct {
	// mode is ModeStop or ModePause; empty when it is not taken down.
	mode        string
	maxDowntime time.Duration
	// err keeps the container from being taken down, and so the snapshot
	// from being taken: a label that asks for it has an error.
	err error
}

// isSet reports whether the container is to be taken down, or would be
// but for an error of its labels.
func (d downLabels) isSet() bool { return d.mode != "" || d.err != nil }

// downLabels reads what the labels of pc say about taking it down.
func (r *Reconciler) downLabels(pc parsed) downLabels {
	d := downLabels{maxDowntime: cmp.Or(pc.spec.MaxDowntime, labels.DefaultMaxDowntime)}

	switch {
	case pc.spec.StopDuringBackup:
		d.mode = ModeStop
	case pc.spec.PauseDuringBackup:
		d.mode = ModePause
	}

	var errs []error

	for _, key := range []string{labels.KeyStopDuringBackup, labels.KeyPauseDuringBackup} {
		if err := r.labelError(pc, key); err != nil {
			errs = append(errs, err)
		}
	}

	if d.mode != "" || len(errs) > 0 {
		if err := r.labelError(pc, labels.KeyMaxDowntime); err != nil {
			errs = append(errs, err)
		}
	}

	d.err = errors.Join(errs...)

	return d
}

// downItem is a container a snapshot with hooks takes down.
type downItem struct {
	id, name string
	// mode is ModeStop or ModePause.
	mode string
}

// downPlan is what a snapshot with hooks of the pass's source takes down:
// the running owners whose labels ask for it, and with those it stops,
// the running containers that depend on them, in the order they go down.
type downPlan struct {
	items []downItem
	// maxDowntime is the shortest max-downtime of the owners that ask.
	maxDowntime time.Duration
	// err keeps the run from taking anything down, and so from taking its
	// snapshot.
	err error
}

// downPlan works out what a snapshot with hooks of the pass's source takes
// down, from the owners and the listing of the pass. The containers that
// are stopped go first, a container before those it depends on, then those
// that are paused, in name order: a container that stops may still need
// another one that would be paused. A container that both would be paused
// and goes down with one that is stopped is stopped.
func (ps *pass) downPlan() downPlan {
	var (
		plan          downPlan
		stops, pauses []owner
		errs          []string
	)

	for _, o := range ps.owners {
		if !o.down.isSet() || !o.state.Running() {
			continue
		}

		if o.down.err != nil {
			errs = append(errs, o.name+": "+o.down.err.Error())
			continue
		}

		if plan.maxDowntime == 0 || o.down.maxDowntime < plan.maxDowntime {
			plan.maxDowntime = o.down.maxDowntime
		}

		if o.down.mode == ModeStop {
			stops = append(stops, o)
		} else {
			pauses = append(pauses, o)
		}
	}

	if len(stops) > 0 {
		ids := make([]string, len(stops))
		for i, o := range stops {
			ids[i] = o.id
		}

		group, err := ps.stopGroup(ids, ps.containers)
		if err != nil {
			errs = append(errs, err.Error())
		}

		for _, c := range group {
			plan.items = append(plan.items, downItem{id: c.ID, name: c.Name, mode: ModeStop})
		}
	}

	for _, o := range pauses {
		if !slices.ContainsFunc(plan.items, func(it downItem) bool { return it.id == o.id }) {
			plan.items = append(plan.items, downItem{id: o.id, name: o.name, mode: ModePause})
		}
	}

	if len(errs) > 0 {
		plan.err = errors.New(strings.Join(errs, "; "))
	}

	return plan
}

// downNames describes the items for the log, as "db (stop)".
func downNames(items []downItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.name + " (" + it.mode + ")"
	}

	return out
}

// needed reports whether the plan takes anything down, or fails to.
func (p downPlan) needed() bool { return len(p.items) > 0 || p.err != nil }

// dependsGraph is how the containers of a listing depend on each other by
// their depends-on labels.
type dependsGraph struct {
	byID map[string]docker.Container
	// deps holds by ID the IDs of the containers each depends on,
	// dependents those that depend on each.
	deps, dependents map[string][]string
	// missing holds by ID the names of each container's depends-on label
	// that match no container.
	missing map[string][]string
}

// newDependsGraph reads the depends-on labels of the containers under the
// label prefix. A name matches the containers of that name and, for a
// container of a docker compose project, the containers of that service in
// the same project.
func newDependsGraph(containers []docker.Container, prefix string) dependsGraph {
	g := dependsGraph{byID: map[string]docker.Container{}, deps: map[string][]string{}, dependents: map[string][]string{},
		missing: map[string][]string{}}

	for _, c := range containers {
		g.byID[c.ID] = c
	}

	for _, c := range containers {
		project := c.Labels[composeProjectLabel]

		for _, name := range labels.DependsOn(c.Labels, prefix) {
			found := false

			for _, d := range containers {
				if d.Name != name && (project == "" || d.Labels[composeProjectLabel] != project || d.Labels[composeServiceLabel] != name) {
					continue
				}

				found = true

				if !slices.Contains(g.deps[c.ID], d.ID) {
					g.deps[c.ID] = append(g.deps[c.ID], d.ID)
					g.dependents[d.ID] = append(g.dependents[d.ID], c.ID)
				}
			}

			if !found {
				g.missing[c.ID] = append(g.missing[c.ID], name)
			}
		}
	}

	return g
}

// inCycle reports whether the container id depends on itself, directly or
// not.
func (g dependsGraph) inCycle(id string) bool {
	seen := map[string]bool{}
	next := slices.Clone(g.deps[id])

	for len(next) > 0 {
		x := next[0]
		next = next[1:]

		if x == id {
			return true
		}

		if !seen[x] {
			seen[x] = true
			next = append(next, g.deps[x]...)
		}
	}

	return false
}

// problems returns what is wrong with the depends-on labels of the
// containers with the IDs: names that match no container, and containers
// that depend on themselves in a circle; sorted by container name.
func (g dependsGraph) problems(prefix string, ids []string) map[string][]string {
	out := map[string][]string{}

	for _, id := range ids {
		var ps []string

		for _, name := range g.missing[id] {
			ps = append(ps, fmt.Sprintf("%q names no container", name))
		}

		if g.inCycle(id) {
			ps = append(ps, "the container depends on itself through "+labels.Name(prefix, labels.KeyDependsOn))
		}

		if len(ps) > 0 {
			out[id] = ps
		}
	}

	return out
}

// DependsOnProblems returns what is wrong with the depends-on labels of the
// containers, by container ID: names that match no container, and a
// container that depends on itself, directly or through others. prefix is
// LABEL_PREFIX; empty means the default.
func DependsOnProblems(containers []docker.Container, prefix string) map[string][]string {
	prefix = cmp.Or(prefix, config.DefaultLabelPrefix)

	ids := make([]string, 0, len(containers))
	for _, c := range containers {
		ids = append(ids, c.ID)
	}

	return newDependsGraph(containers, prefix).problems(prefix, ids)
}

// stopGroup returns the running containers to stop with the targets: the
// targets and every container that depends on one of them, directly or
// not, ordered so that a container comes before those it depends on, and
// otherwise by name. A depends-on label of one of them that names no
// container, or a cycle among them, is an error: the order is not known.
func (r *Reconciler) stopGroup(targets []string, containers []docker.Container) ([]docker.Container, error) {
	g := newDependsGraph(containers, r.labelPrefix)

	in := map[string]bool{}

	for next := slices.Clone(targets); len(next) > 0; next = next[1:] {
		if id := next[0]; !in[id] {
			in[id] = true
			next = append(next, g.dependents[id]...)
		}
	}

	ids := slices.Collect(maps.Keys(in))

	slices.SortFunc(ids, func(a, b string) int {
		return cmp.Or(strings.Compare(g.byID[a].Name, g.byID[b].Name), strings.Compare(a, b))
	})

	if problems := g.problems(r.labelPrefix, ids); len(problems) > 0 {
		var msgs []string

		for _, id := range ids {
			for _, p := range problems[id] {
				msgs = append(msgs, g.byID[id].Name+": "+p)
			}
		}

		return nil, fmt.Errorf("%s: %s", labels.Name(r.labelPrefix, labels.KeyDependsOn), strings.Join(msgs, "; "))
	}

	// A container goes once no other one of the group depends on it.
	waiting := map[string]int{}

	for _, id := range ids {
		for _, d := range g.deps[id] {
			if in[d] {
				waiting[d]++
			}
		}
	}

	var out []docker.Container

	for len(ids) > 0 {
		i := slices.IndexFunc(ids, func(id string) bool { return waiting[id] == 0 })
		if i < 0 {
			return nil, fmt.Errorf("%s: the containers depend on each other in a circle", labels.Name(r.labelPrefix, labels.KeyDependsOn))
		}

		id := ids[i]
		ids = slices.Delete(ids, i, i+1)

		for _, d := range g.deps[id] {
			if in[d] {
				waiting[d]--
			}
		}

		if c := g.byID[id]; c.State == docker.StateRunning {
			out = append(out, c)
		}
	}

	return out, nil
}

// reportDependsOn logs the problems of the depends-on labels of the
// listed containers at error level, and counts them, once for each set of
// problems of a container.
func (r *Reconciler) reportDependsOn(containers []docker.Container) {
	g := newDependsGraph(containers, r.labelPrefix)

	ids := make([]string, 0, len(containers))
	for _, c := range containers {
		ids = append(ids, c.ID)
	}

	problems := g.problems(r.labelPrefix, ids)
	name := labels.Name(r.labelPrefix, labels.KeyDependsOn)

	var report []string

	r.mu.Lock()

	for id := range r.dependsLogged {
		if _, ok := problems[id]; !ok {
			delete(r.dependsLogged, id)
		}
	}

	for _, id := range ids {
		ps, ok := problems[id]
		if !ok {
			continue
		}

		if key := strings.Join(ps, "\n"); r.dependsLogged[id] != key {
			r.dependsLogged[id] = key
			report = append(report, id)
		}
	}

	r.mu.Unlock()

	for _, id := range report {
		lines := make([]string, len(problems[id]))
		for i, p := range problems[id] {
			lines[i] = "error: " + name + ": " + p
		}

		r.metrics.ReconcileError("", ReasonLabels)
		r.log.Error("container labels have errors: the snapshots that stop a container it depends on are not taken",
			"container", g.byID[id].Name, "problems", lines)
	}
}

// expectation is a set of events the watcher's own change of a container
// causes, each counted, until a time.
type expectation struct {
	actions map[string]int
	until   time.Time
}

// expect makes HandleEvent drop the next event of each action of the
// container id within ownEventWindow: the watcher is about to cause them.
func (r *Reconciler) expect(id string, actions ...string) {
	e := expectation{actions: map[string]int{}, until: r.now().Add(ownEventWindow)}
	for _, a := range actions {
		e.actions[a]++
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.expected[id] = append(r.expected[id], e)
}

// ownEvent reports whether ev is one the watcher caused, and counts it.
func (r *Reconciler) ownEvent(ev docker.Event) bool {
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	es := slices.DeleteFunc(r.expected[ev.ContainerID], func(e expectation) bool { return now.After(e.until) })

	own := false

	for i := range es {
		if es[i].actions[ev.Action] > 0 {
			es[i].actions[ev.Action]--
			own = true

			break
		}
	}

	es = slices.DeleteFunc(es, func(e expectation) bool {
		for _, n := range e.actions {
			if n > 0 {
				return false
			}
		}

		return true
	})

	if len(es) == 0 {
		delete(r.expected, ev.ContainerID)
	} else {
		r.expected[ev.ContainerID] = es
	}

	return own
}

// heldByID returns the held containers by ID.
func (r *Reconciler) heldByID() map[string]state.Held {
	out := map[string]state.Held{}
	for _, h := range r.store.Held() {
		out[h.ID] = h
	}

	return out
}

// heldFor returns the containers held for the run of src, from the last
// taken down to the first.
func (r *Reconciler) heldFor(src backend.Source, run string) []state.Held {
	var out []state.Held

	for _, h := range r.store.Held() {
		if h.Source == src && h.Run == run {
			out = append(out, h)
		}
	}

	slices.SortStableFunc(out, func(a, b state.Held) int { return b.Order - a.Order })

	return out
}

// heldBusy returns a container that the pass's source needs, an owner or
// one its snapshot would take down, and that is held for a snapshot other
// than run, if any: a container is held for one snapshot at a time.
func (ps *pass) heldBusy(plan downPlan, run string) (state.Held, bool) {
	held := ps.heldByID()

	need := make([]string, 0, len(ps.owners)+len(plan.items))
	for _, o := range ps.owners {
		need = append(need, o.id)
	}

	for _, it := range plan.items {
		need = append(need, it.id)
	}

	for _, id := range need {
		if h, ok := held[id]; ok && (h.Source != ps.src || h.Run != run || run == "") {
			return h, true
		}
	}

	return state.Held{}, false
}

// waitForHeld makes the return of the container wake the pass's path, and
// asks for a pass after heldRecheck in case it does not.
func (ps *pass) waitForHeld(h state.Held) {
	ps.mu.Lock()
	if !slices.Contains(ps.heldWaiters[h.ID], ps.path) {
		ps.heldWaiters[h.ID] = append(ps.heldWaiters[h.ID], ps.path)
	}
	ps.mu.Unlock()

	ps.wakeAt(ps.now().Add(heldRecheck))
}

// wakeAt asks for a pass over the path at t at the latest.
func (ps *pass) wakeAt(t time.Time) {
	if ps.wake.IsZero() || t.Before(ps.wake) {
		ps.wake = t
	}
}

// toDown takes down what the plan of the run asks for, once the pre-exec
// hooks have succeeded: it reserves the containers in the state, all of
// them or, when one is held for another snapshot, none, and stops and
// pauses them in a goroutine; the pass after it queues the snapshot. A
// plan that fails, as with a label error, takes no snapshot.
func (ps *pass) toDown(run state.HookRun, plan downPlan) error {
	attrs := []any{"source", ps.src.String(), "reason", run.Reason}

	if plan.err != nil {
		// Every snapshot fails until the labels are fixed: each one is
		// logged and counted.
		ps.metrics.ReconcileError(ps.path, ReasonLabels)
		ps.log.Error("no snapshot: the containers of the source cannot be stopped or paused for it",
			append(attrs, "error", plan.err.Error())...)

		return ps.toPost(run)
	}

	if h, busy := ps.heldBusy(plan, run.ID); busy {
		ps.log.Info("the snapshot waits: a container it needs is held down for another snapshot",
			append(attrs, "container", h.Name, "held_for", h.Source.Path)...)
		ps.waitForHeld(h)

		return nil
	}

	ctx, ok := ps.reserveHooks(run.ID)
	if !ok {
		ps.log.Debug("not taking the containers down now: hooks of the source run, or the watcher stops", attrs...)
		return nil
	}

	now := ps.now()

	var busy *state.Held

	err := ps.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		for _, it := range plan.items {
			if i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == it.id }); i >= 0 {
				busy = &held[i]
				return nil, errNoChange
			}
		}

		for i, it := range plan.items {
			held = append(held, state.Held{ID: it.id, Name: it.name, Mode: it.mode, Source: ps.src, Run: run.ID, Order: i, At: now})
		}

		return held, nil
	})

	switch {
	case busy != nil:
		ps.releaseHooks()
		ps.log.Info("the snapshot waits: a container it needs is held down for another snapshot",
			append(attrs, "container", busy.Name, "held_for", busy.Source.Path)...)
		ps.waitForHeld(*busy)

		return nil
	case storeErr(err) != nil:
		ps.releaseHooks()
		return storeErr(err)
	}

	run.Phase = state.HookDown
	run.ReturnBy = now.Add(plan.maxDowntime)
	found := false

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.HookRun == nil || r.HookRun.ID != run.ID {
			return false
		}

		found = true
		r.HookRun.Phase, r.HookRun.ReturnBy = state.HookDown, run.ReturnBy

		return true
	})

	if !found {
		ps.dropHeldOf(run.ID)
		ps.releaseHooks()

		return nil
	}

	ps.log.Info("taking containers down for the snapshot", append(attrs, "containers", downNames(plan.items),
		"return_by", run.ReturnBy.Format(timeFormat))...)

	ps.goDown(ctx, run, plan.items)

	return nil
}

// dropHeldOf removes the containers reserved for the run of the pass's
// source that were never taken down.
func (ps *pass) dropHeldOf(run string) {
	err := ps.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		return slices.DeleteFunc(held, func(h state.Held) bool {
			return h.Source == ps.src && h.Run == run && h.StartedAt.IsZero()
		}), nil
	})
	if storeErr(err) != nil {
		ps.log.Error("cannot store the containers held down", "source", ps.src.String(), "error", err.Error())
	}
}

// goDown stops and pauses the containers of the run, in the order given,
// in the goroutine reserveHooks set up, then stores how that went in the
// run and queues the path. Once the watcher stops, it takes nothing more
// down and brings back what it took.
func (ps *pass) goDown(ctx context.Context, run state.HookRun, items []downItem) {
	r, src, path, log := ps.Reconciler, ps.src, ps.path, ps.log

	go func() {
		defer r.hookers.Done()

		failure := r.takeDown(ctx, log, src, run, items)

		switch {
		case r.stopping():
			log.Warn("bringing the containers back: the watcher stops", "source", src.String(), "reason", run.Reason)
			r.returnRun(ctx, log, src, run.ID)
		case ctx.Err() == nil:
			r.updateRecord(src, func(rec *state.Record) bool {
				if rec.HookRun == nil || rec.HookRun.ID != run.ID {
					return false
				}

				rec.HookRun.DownDone, rec.HookRun.DownError = true, failure

				return true
			})
		}

		r.mu.Lock()
		delete(r.hooksRunning, src)
		r.mu.Unlock()

		if ctx.Err() == nil {
			r.queue.addUnlessRetrying(path)
		}

		if r.afterHooks != nil {
			r.afterHooks(src, state.HookDown)
		}
	}()
}

// takeDown stops or pauses each container of items that runs, one after
// the other, each within the time left until the run brings them back, and
// returns why it failed, empty if it did not. It stops at the first
// failure, and when the watcher stops. Before it takes a container down, it
// stores when Docker started it, so that a watcher that comes back after a
// crash knows the container is still down as the watcher left it.
func (r *Reconciler) takeDown(ctx context.Context, log *slog.Logger, src backend.Source, run state.HookRun, items []downItem) string {
	dctx, cancel := context.WithTimeout(ctx, max(run.ReturnBy.Sub(r.now()), 0))
	defer cancel()

	for _, it := range items {
		if r.stopping() || ctx.Err() != nil {
			return ""
		}

		attrs := []any{"source", src.String(), "container", it.name}

		c, err := r.docker.Inspect(dctx, it.id)

		switch {
		case errors.Is(err, docker.ErrNotFound):
			log.Info("not taking down a container that is gone", attrs...)
			r.dropHeld(it.id)

			continue
		case err != nil:
			return it.name + ": " + err.Error()
		case c.State != docker.StateRunning:
			log.Info("not taking down a container that is not running; it is left as it is", append(attrs, "state", string(c.State))...)
			r.dropHeld(it.id)

			continue
		}

		started := startedAt(c)

		if err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
			i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == it.id && h.Source == src && h.Run == run.ID })
			if i < 0 {
				return nil, errNoChange
			}

			held[i].StartedAt, held[i].At = started, r.now()

			return held, nil
		}); err != nil && !errors.Is(err, state.ErrNotPersisted) {
			// Without the record a crash would leave it down for good.
			return it.name + ": cannot store that it is held down: " + err.Error()
		}

		start := time.Now()

		if it.mode == ModeStop {
			r.expect(it.id, "die", "stop")
			err = r.docker.Stop(dctx, it.id)
		} else {
			err = r.docker.Pause(dctx, it.id)
		}

		if err != nil {
			if dctx.Err() != nil && ctx.Err() == nil {
				err = fmt.Errorf("did not finish before the containers had to come back: %w", err)
			}

			return it.name + ": " + it.mode + ": " + err.Error()
		}

		msg := "container stopped for the snapshot"
		if it.mode == ModePause {
			msg = "container paused for the snapshot"
		}

		log.Info(msg, append(attrs, "duration", time.Since(start).String())...)
	}

	return ""
}

// startedAt returns when Docker last started c, as Held.StartedAt keeps
// it: a running container has a start time, and one from a daemon that
// reports none gets the start of the Unix epoch, which is not zero, so
// that it still counts as taken down.
func startedAt(c docker.Container) time.Time {
	if c.StartedAt.IsZero() {
		return time.Unix(0, 0).UTC()
	}

	return c.StartedAt
}

// stopping reports whether Run is not running or is stopping.
func (r *Reconciler) stopping() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.runCtx == nil || r.runCtx.Err() != nil
}

// returnEarly brings back the containers held for the run, whose snapshot
// has not ended by the time they had to come back, in a goroutine; the
// snapshot goes on.
func (ps *pass) returnEarly(run state.HookRun) {
	ctx, ok := ps.reserveHooks(run.ID)
	if !ok {
		return
	}

	ps.log.Warn("bringing the containers back before the snapshot has ended: max-downtime has passed; "+
		"the snapshot goes on and reads files the containers may change", "source", ps.src.String(), "reason", run.Reason,
		"return_by", run.ReturnBy.Format(timeFormat))

	r, src, path, log := ps.Reconciler, ps.src, ps.path, ps.log

	go func() {
		defer r.hookers.Done()

		r.returnRun(ctx, log, src, run.ID)

		r.mu.Lock()
		delete(r.hooksRunning, src)
		r.mu.Unlock()

		if ctx.Err() == nil {
			r.queue.addUnlessRetrying(path)
		}

		if r.afterHooks != nil {
			r.afterHooks(src, "return")
		}
	}()
}

// holding reports whether the run of the pass's source holds containers
// down that have not been given up to the retries.
func (ps *pass) holding(run string) bool {
	return slices.ContainsFunc(ps.heldFor(ps.src, run), func(h state.Held) bool { return !h.Released })
}

// returnRun brings back the containers held for the run of src, from the
// last taken down to the first, and returns the IDs of those it could not
// bring back, which the retry loop takes over.
func (r *Reconciler) returnRun(ctx context.Context, log *slog.Logger, src backend.Source, run string) map[string]bool {
	return r.bringBack(ctx, log, r.heldFor(src, run))
}

// bringBack brings back the held containers, one after the other, and
// returns the IDs of those it could not bring back: they stay held,
// released for the retry loop, which it wakes. When ctx ends, the rest
// stays held as it is, for the next start.
func (r *Reconciler) bringBack(ctx context.Context, log *slog.Logger, held []state.Held) map[string]bool {
	failed := map[string]bool{}

	for _, h := range held {
		if ctx.Err() != nil {
			break
		}

		if msg := r.bringBackOne(ctx, log, h); msg != "" && ctx.Err() == nil {
			failed[h.ID] = true
		}
	}

	if len(failed) > 0 {
		select {
		case r.returnWake <- struct{}{}:
		default:
		}
	}

	return failed
}

// bringBackOne brings back one held container and returns why it failed,
// empty if it did not. A container that is gone, or that someone else
// started since the watcher took it down, is no longer held; so is one
// that runs: someone else brought it back. Only a stopped container is
// started and only a paused one unpaused: one the watcher paused and
// someone else stopped stays stopped.
func (r *Reconciler) bringBackOne(ctx context.Context, log *slog.Logger, h state.Held) string {
	r.returnMu.Lock()
	defer r.returnMu.Unlock()

	// Another goroutine may have brought it back meanwhile.
	cur, ok := r.heldByID()[h.ID]
	if !ok || cur.Source != h.Source || cur.Run != h.Run {
		return ""
	}

	h = cur
	attrs := []any{"source", h.Source.String(), "container", h.Name, "mode", h.Mode}

	if h.StartedAt.IsZero() {
		// Reserved, but never taken down.
		r.dropHeld(h.ID)
		return ""
	}

	ictx, cancel := context.WithTimeout(ctx, r.dockerTimeout)
	defer cancel()

	c, err := r.docker.Inspect(ictx, h.ID)

	var (
		do   func(context.Context, string) error
		verb string
	)

	switch {
	case errors.Is(err, docker.ErrNotFound):
		log.Info("not bringing back a container that is gone", attrs...)
		r.notHeld(h, c)

		return ""
	case err != nil:
		return r.returnFailed(log, h, attrs, err)
	case !startedAt(c).Equal(h.StartedAt):
		log.Info("not bringing back a container someone else started meanwhile; it is left as it is",
			append(attrs, "state", string(c.State))...)
		r.notHeld(h, c)

		return ""
	case h.Mode == ModeStop && (c.State == docker.StateExited || c.State == docker.StateCreated || c.State == docker.StateDead):
		do, verb = r.docker.Start, "started"
	case h.Mode == ModePause && c.State == docker.StatePaused:
		do, verb = r.docker.Unpause, "unpaused"
	case c.State.Running():
		log.Info("not bringing back a container that runs again", append(attrs, "state", string(c.State))...)
		r.notHeld(h, c)

		return ""
	default:
		log.Warn("not bringing back a container someone else stopped meanwhile; it is left as it is",
			append(attrs, "state", string(c.State))...)
		r.notHeld(h, c)

		return ""
	}

	if h.Mode == ModeStop {
		r.expect(h.ID, "start")
	}

	if err := do(ictx, h.ID); err != nil {
		return r.returnFailed(log, h, attrs, err)
	}

	downtime := max(r.now().Sub(h.At), 0)
	log.Info("container "+verb+" again after the snapshot", append(attrs, "downtime", downtime.String())...)
	r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode, Downtime: downtime})
	r.dropHeld(h.ID)
	r.queueContainer(c)

	return ""
}

// returnFailed logs and reports a container that could not be brought
// back, which stays held, released for the retry loop.
func (r *Reconciler) returnFailed(log *slog.Logger, h state.Held, attrs []any, err error) string {
	msg := err.Error()

	log.Error("cannot bring back a container the watcher took down for a snapshot; trying again", append(attrs, "error", msg)...)
	r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode, Error: msg})

	if !h.Released {
		uerr := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
			i := slices.IndexFunc(held, func(x state.Held) bool { return x.ID == h.ID })
			if i < 0 || held[i].Released {
				return nil, errNoChange
			}

			held[i].Released = true

			return held, nil
		})
		if uerr != nil && !errors.Is(uerr, errNoChange) && !errors.Is(uerr, state.ErrNotPersisted) {
			log.Error("cannot store the containers held down", "error", uerr.Error())
		}
	}

	return msg
}

// notHeld drops a held container that the watcher no longer has to bring
// back, and queues the paths of one that still exists. One the watcher
// failed to bring back before is reported as back, with no downtime.
func (r *Reconciler) notHeld(h state.Held, c docker.Container) {
	if h.Released {
		r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode})
	}

	r.dropHeld(h.ID)

	if c.ID != "" {
		r.queueContainer(c)
	}
}

// dropHeld removes the held container with the ID from the state, and
// wakes the paths that wait for it.
func (r *Reconciler) dropHeld(id string) {
	err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		n := len(held)
		held = slices.DeleteFunc(held, func(h state.Held) bool { return h.ID == id })

		if len(held) == n {
			return nil, errNoChange
		}

		return held, nil
	})
	if err != nil && !errors.Is(err, errNoChange) && !errors.Is(err, state.ErrNotPersisted) {
		r.log.Error("cannot store the containers held down", "container", id, "error", err.Error())
	}

	r.mu.Lock()
	waiting := r.heldWaiters[id]
	delete(r.heldWaiters, id)
	r.mu.Unlock()

	for _, p := range waiting {
		r.queue.addUnlessRetrying(p)
	}
}

// queueContainer queues the paths of a container that came back, or was
// found back, so that their passes see it run.
func (r *Reconciler) queueContainer(c docker.Container) {
	pc := r.parse(c)
	if !pc.spec.Enable {
		return
	}

	for _, p := range pc.paths {
		r.queue.addUnlessRetrying(p)
	}
}

// returnLeftDown brings back, once each, every container the state lists
// as held: a watcher stopped or crashed while they were down. Those it
// cannot bring back are tried again by the retry loop.
func (r *Reconciler) returnLeftDown(ctx context.Context) {
	held := r.store.Held()
	if len(held) == 0 {
		return
	}

	names := make([]string, len(held))
	for i, h := range held {
		names[i] = h.Name
	}

	r.log.Info("bringing back the containers the watcher took down for a snapshot before it stopped", "containers", names)

	r.bringBack(ctx, r.log, byRunReversed(held))
}

// byRunReversed sorts held containers by their run, and within a run from
// the last taken down to the first.
func byRunReversed(held []state.Held) []state.Held {
	out := slices.Clone(held)
	slices.SortStableFunc(out, func(a, b state.Held) int {
		return cmp.Or(a.Source.Compare(b.Source), strings.Compare(a.Run, b.Run), b.Order-a.Order)
	})

	return out
}

// returnHeld brings back the held containers when the watcher stops: with
// unowned, only those of runs no goroutine of this process is busy with,
// which bring back their own.
func (r *Reconciler) returnHeld(ctx context.Context, unowned bool) {
	var todo []state.Held

	r.mu.Lock()
	for _, h := range r.store.Held() {
		if !unowned || r.hooksRunning[h.Source] != h.Run {
			todo = append(todo, h)
		}
	}
	r.mu.Unlock()

	if len(todo) == 0 {
		return
	}

	r.log.Info("bringing back the containers taken down for a snapshot: the watcher stops", "containers", len(todo))
	r.bringBack(ctx, r.log, byRunReversed(todo))
}

// returnLoop tries again to bring back the containers that could not be
// brought back, with a growing delay, until ctx is done. A failure wakes
// it, and it tries after the delay; a container that a goroutine of its
// run is busy with waits for the next try.
func (r *Reconciler) returnLoop(ctx context.Context) {
	attempt := 0
	armed := false

	t := time.NewTimer(time.Hour)
	t.Stop()

	defer t.Stop()

	arm := func() {
		attempt = max(attempt, 1)
		t.Reset(backoff(r.minBackoff, r.maxBackoff, attempt))
		armed = true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.returnWake:
			if !armed {
				arm()
			}

			continue
		case <-t.C:
			armed = false
		}

		var (
			todo    []state.Held
			waiting bool
		)

		r.mu.Lock()
		for _, h := range r.store.Held() {
			switch {
			case !h.Released:
			case r.hooksRunning[h.Source] == h.Run:
				waiting = true
			default:
				todo = append(todo, h)
			}
		}
		r.mu.Unlock()

		failed := r.bringBack(ctx, r.log, byRunReversed(todo))

		// bringBack wakes the loop for its failures; the delay decides.
		select {
		case <-r.returnWake:
		default:
		}

		switch {
		case len(failed) > 0 || waiting:
			attempt++
			arm()
		default:
			attempt = 0
		}
	}
}
