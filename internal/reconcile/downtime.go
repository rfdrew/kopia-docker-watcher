package reconcile

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
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

	// stopCallTimeout bounds one stop of a container: Docker answers once
	// the container has stopped, after its stop timeout at the most. A
	// container held down that still runs as Docker started it, whose stop
	// did not return, may be on its way down for as long.
	stopCallTimeout = 15 * time.Minute
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
	// need are the IDs of the containers the run needs to itself, besides
	// the owners: those it takes down, and those of the groups of the
	// containers it stops that are down already. While another snapshot
	// holds one of them, the run waits.
	need []string
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

		group, members, err := ps.stopGroup(ids, ps.containers)
		if err != nil {
			errs = append(errs, err.Error())
		}

		for _, c := range group {
			plan.items = append(plan.items, downItem{id: c.ID, name: c.Name, mode: ModeStop})
		}

		plan.need = members
	}

	for _, o := range pauses {
		if !slices.ContainsFunc(plan.items, func(it downItem) bool { return it.id == o.id }) {
			plan.items = append(plan.items, downItem{id: o.id, name: o.name, mode: ModePause})
			plan.need = append(plan.need, o.id)
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
	// that match no container, bad the items that are no name at all.
	missing, bad map[string][]string
}

// newDependsGraph reads the depends-on labels of the containers under the
// label prefix. A name matches the containers of that name and, for a
// container of a docker compose project, the containers of that service in
// the same project.
func newDependsGraph(containers []docker.Container, prefix string) dependsGraph {
	g := dependsGraph{byID: map[string]docker.Container{}, deps: map[string][]string{}, dependents: map[string][]string{},
		missing: map[string][]string{}, bad: map[string][]string{}}

	for _, c := range containers {
		g.byID[c.ID] = c
	}

	for _, c := range containers {
		project := c.Labels[composeProjectLabel]

		names, bad := labels.DependsOn(c.Labels, prefix)
		if len(bad) > 0 {
			g.bad[c.ID] = bad
		}

		// An item that is no name, as "db cache", may still mean the
		// containers its words name: they count as dependencies, so that
		// their group finds the problem.
		var guessed []string

		for _, item := range bad {
			guessed = append(guessed, nameSeparators.Split(item, -1)...)
		}

		for i, name := range slices.Concat(names, guessed) {
			if name == "" {
				continue
			}

			known := i < len(names)
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

			if !found && known {
				g.missing[c.ID] = append(g.missing[c.ID], name)
			}
		}
	}

	return g
}

// nameSeparators splits an item of a depends-on label that is no name
// into the words that may be.
var nameSeparators = regexp.MustCompile(`[^a-zA-Z0-9_.-]+`)

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
// containers with the IDs: items that are no name, and so may hide a
// dependency, names that match no container, and containers that depend
// on themselves in a circle.
func (g dependsGraph) problems(prefix string, ids []string) map[string][]string {
	out := map[string][]string{}

	for _, id := range ids {
		var ps []string

		for _, item := range g.bad[id] {
			ps = append(ps, fmt.Sprintf("%q is not a container or service name", item))
		}

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
// containers, by container ID: items that are no name, names that match no
// container, and a container that depends on itself, directly or through
// others. prefix is LABEL_PREFIX; empty means the default.
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
// otherwise by name. members are the IDs of the whole group, whatever the
// state of its containers. A problem with the depends-on label of one of
// them, or a cycle among them, is an error: the order is not known.
func (r *Reconciler) stopGroup(targets []string, containers []docker.Container) (group []docker.Container, members []string,
	err error,
) {
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

	members = slices.Clone(ids)

	if problems := g.problems(r.labelPrefix, ids); len(problems) > 0 {
		var msgs []string

		for _, id := range ids {
			for _, p := range problems[id] {
				msgs = append(msgs, g.byID[id].Name+": "+p)
			}
		}

		return nil, members, fmt.Errorf("%s: %s", labels.Name(r.labelPrefix, labels.KeyDependsOn), strings.Join(msgs, "; "))
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

	for len(ids) > 0 {
		i := slices.IndexFunc(ids, func(id string) bool { return waiting[id] == 0 })
		if i < 0 {
			return nil, members, fmt.Errorf("%s: the containers depend on each other in a circle",
				labels.Name(r.labelPrefix, labels.KeyDependsOn))
		}

		id := ids[i]
		ids = slices.Delete(ids, i, i+1)

		for _, d := range g.deps[id] {
			if in[d] {
				waiting[d]--
			}
		}

		if c := g.byID[id]; c.State == docker.StateRunning {
			group = append(group, c)
		}
	}

	return group, members, nil
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
	seq     uint64
	actions map[string]int
	until   time.Time
}

// expect makes HandleEvent drop the next event of each action of the
// container id within ownEventWindow: the watcher is about to cause them.
// withdraw takes back what is left of them, after a change that failed.
func (r *Reconciler) expect(id string, actions ...string) (withdraw func()) {
	until := r.now().Add(ownEventWindow)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.expectSeq++
	e := expectation{seq: r.expectSeq, actions: map[string]int{}, until: until}

	for _, a := range actions {
		e.actions[a]++
	}

	r.expected[id] = append(r.expected[id], e)

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.setExpected(id, slices.DeleteFunc(r.expected[id], func(x expectation) bool { return x.seq == e.seq }))
	}
}

// setExpected sets the expectations of the container id; r.mu is held.
func (r *Reconciler) setExpected(id string, es []expectation) {
	if len(es) == 0 {
		delete(r.expected, id)
	} else {
		r.expected[id] = es
	}
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

	r.setExpected(ev.ContainerID, slices.DeleteFunc(es, func(e expectation) bool {
		for _, n := range e.actions {
			if n > 0 {
				return false
			}
		}

		return true
	}))

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

// needs returns the IDs of the containers a snapshot with hooks of the
// pass's source needs to itself under the plan: the owners, those the
// plan takes down, and the rest of the groups of those it stops.
func (ps *pass) needs(plan downPlan) []string {
	var out []string

	add := func(id string) {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}

	for _, o := range ps.owners {
		add(o.id)
	}

	for _, it := range plan.items {
		add(it.id)
	}

	for _, id := range plan.need {
		add(id)
	}

	return out
}

// heldBusy returns a container that a snapshot with hooks of the pass's
// source needs and that is held for a snapshot other than run, if any: a
// container is held for one snapshot at a time. With run empty, every
// held container counts.
func (ps *pass) heldBusy(plan downPlan, run string) (state.Held, bool) {
	held := ps.heldByID()

	for _, id := range ps.needs(plan) {
		if h, ok := held[id]; ok && (h.Source != ps.src || h.Run != run || run == "") {
			return h, true
		}
	}

	return state.Held{}, false
}

// execBusy returns a container the plan takes down that a hook command
// runs in: one of another source, as the source's own have ended.
func (ps *pass) execBusy(plan downPlan) (downItem, bool) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for _, it := range plan.items {
		if ps.execs[it.id] > 0 {
			return it, true
		}
	}

	return downItem{}, false
}

// waitFor makes the pass's path wake when the container with the ID is no
// longer held or busy, and asks for a pass after heldRecheck in case it
// does not.
func (ps *pass) waitFor(id string) {
	ps.mu.Lock()
	if !slices.Contains(ps.heldWaiters[id], ps.path) {
		ps.heldWaiters[id] = append(ps.heldWaiters[id], ps.path)
	}
	ps.mu.Unlock()

	ps.wakeAt(ps.now().Add(heldRecheck))
}

// wakeWaiters queues the paths that wait for the containers with the IDs.
func (r *Reconciler) wakeWaiters(ids ...string) {
	var paths []string

	r.mu.Lock()
	for _, id := range ids {
		paths = append(paths, r.heldWaiters[id]...)
		delete(r.heldWaiters, id)
	}
	r.mu.Unlock()

	for _, p := range paths {
		r.queue.addUnlessRetrying(p)
	}
}

// wakeAt asks for a pass over the path at t at the latest.
func (ps *pass) wakeAt(t time.Time) {
	if ps.wake.IsZero() || t.Before(ps.wake) {
		ps.wake = t
	}
}

// toDown takes down what the plan of the run asks for, once the pre-exec
// hooks have succeeded: it reserves the containers in the state, all of
// them or, when one the run needs is held for another snapshot, none, and
// stops and pauses them in a goroutine; the pass after it queues the
// snapshot. It waits while a hook of another source runs in one of them. A
// plan that fails, as with a label error, takes no snapshot, and so does a
// STATE_FILE that cannot be written: without it, a crash of the watcher
// would leave the containers down.
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
		ps.waitFor(h.ID)

		return nil
	}

	if it, busy := ps.execBusy(plan); busy {
		ps.log.Info("the snapshot waits: a hook of another source runs in a container it takes down",
			append(attrs, "container", it.name)...)
		ps.waitFor(it.id)

		return nil
	}

	ctx, ok := ps.reserveHooks(run.ID)
	if !ok {
		ps.log.Debug("not taking the containers down now: hooks of the source run, or the watcher stops", attrs...)
		return nil
	}

	now := ps.now()
	returnBy := now.Add(plan.maxDowntime)
	need := ps.needs(plan)

	var busy *state.Held

	err := ps.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		for _, id := range need {
			if i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == id }); i >= 0 {
				b := held[i]
				busy = &b

				return nil, errNoChange
			}
		}

		for i, it := range plan.items {
			held = append(held, state.Held{ID: it.id, Name: it.name, Mode: it.mode, Source: ps.src, Run: run.ID, Order: i, At: now,
				ReturnBy: returnBy})
		}

		return held, nil
	})

	switch {
	case busy != nil:
		ps.releaseHooks()
		ps.log.Info("the snapshot waits: a container it needs is held down for another snapshot",
			append(attrs, "container", busy.Name, "held_for", busy.Source.Path)...)
		ps.waitFor(busy.ID)

		return nil
	case errors.Is(err, state.ErrNotPersisted):
		ps.dropHeldOf(run.ID)
		ps.releaseHooks()
		ps.log.Error("no snapshot: STATE_FILE cannot be written, and without it a crash of the watcher could leave the "+
			"containers down", append(attrs, "error", err.Error())...)

		return ps.toPost(run)
	case err != nil:
		ps.releaseHooks()
		return storeErr(err)
	}

	run.Phase, run.ReturnBy = state.HookDown, returnBy
	found := false

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		if r.HookRun == nil || r.HookRun.ID != run.ID {
			return false
		}

		found = true
		r.HookRun.Phase, r.HookRun.ReturnBy = state.HookDown, returnBy

		return true
	})

	if !found {
		ps.dropHeldOf(run.ID)
		ps.releaseHooks()

		return nil
	}

	ps.log.Info("taking containers down for the snapshot", append(attrs, "containers", downNames(plan.items),
		"return_by", returnBy.Format(timeFormat))...)

	// The retry loop brings them back at returnBy, whatever the passes do.
	ps.wakeReturns()
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

		taken, failure := r.takeDown(ctx, log, src, run, items)

		switch {
		case r.stopping():
			log.Warn("bringing the containers back: the watcher stops", "source", src.String(), "reason", run.Reason)

			_, gone := r.bringBack(ctx, log, r.heldFor(src, run.ID), false)
			r.wakeWaiters(gone...)
		case ctx.Err() == nil:
			r.updateRecord(src, func(rec *state.Record) bool {
				if rec.HookRun == nil || rec.HookRun.ID != run.ID {
					return false
				}

				rec.HookRun.DownDone, rec.HookRun.DownError, rec.HookRun.DownTaken = true, failure, taken

				return true
			})
		}

		r.mu.Lock()
		delete(r.hooksRunning, src)
		r.mu.Unlock()

		if ctx.Err() == nil {
			r.queue.addUnlessRetrying(path)
		}

		r.wakeReturns()

		if r.afterHooks != nil {
			r.afterHooks(src, state.HookDown)
		}
	}()
}

// takeDown stops or pauses each container of items that runs, one after
// the other, and returns how many it took down and why it failed, empty if
// it did not. It stops at the first failure, when the run's containers
// have to come back, and when the watcher stops. Before it takes a
// container down, it stores when Docker started it, so that a watcher that
// comes back after a crash knows the container is still down as the
// watcher left it; when that cannot be written to STATE_FILE, it takes
// nothing more down. A stop is waited for as long as Docker takes, so that
// the container is down, not on its way, when the run goes on.
func (r *Reconciler) takeDown(ctx context.Context, log *slog.Logger, src backend.Source, run state.HookRun,
	items []downItem,
) (taken int, failure string) {
	for _, it := range items {
		if r.stopping() || ctx.Err() != nil {
			return taken, ""
		}

		if !r.now().Before(run.ReturnBy) {
			return taken, "max-downtime passed before all the containers were down"
		}

		attrs := []any{"source", src.String(), "container", it.name}

		c, err := r.inspect(ctx, it.id)

		switch {
		case errors.Is(err, docker.ErrNotFound):
			log.Info("not taking down a container that is gone", attrs...)
			r.dropHeld(it.id)

			continue
		case err != nil:
			return taken, it.name + ": " + err.Error()
		case c.State != docker.StateRunning:
			log.Info("not taking down a container that is not running; it is left as it is", append(attrs, "state", string(c.State))...)
			r.dropHeld(it.id)

			continue
		}

		started := startedAt(c)

		if err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
			i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == it.id && h.Source == src && h.Run == run.ID })
			if i < 0 {
				return nil, errors.New("its record is gone")
			}

			held[i].StartedAt, held[i].At = started, r.now()

			return held, nil
		}); err != nil {
			// Without the record in STATE_FILE a crash would leave it down
			// for good.
			return taken, it.name + ": cannot store that it is held down: " + err.Error()
		}

		start := time.Now()

		if it.mode == ModeStop {
			err = r.stopContainer(ctx, it.id)
		} else {
			pctx, cancel := context.WithTimeout(ctx, r.dockerTimeout)
			err = r.docker.Pause(pctx, it.id)
			cancel()
		}

		if err != nil && !r.wentDown(ctx, it) {
			return taken, it.name + ": " + it.mode + ": " + err.Error()
		}

		r.markHeld(it.id, func(h *state.Held) { h.Down = true })

		taken++

		msg := "container stopped for the snapshot"
		if it.mode == ModePause {
			msg = "container paused for the snapshot"
		}

		log.Info(msg, append(attrs, "duration", time.Since(start).String())...)
	}

	return taken, ""
}

// wentDown reports, after a stop or pause of the item that failed, whether
// the container went down all the same. One that did not, as it still
// runs, is no longer held: the watcher does not have to bring it back.
func (r *Reconciler) wentDown(ctx context.Context, it downItem) bool {
	c, err := r.inspect(ctx, it.id)

	switch {
	case err != nil:
		// Not known: it stays held, and its return looks.
		return false
	case it.mode == ModeStop && !c.State.Running(), it.mode == ModePause && c.State == docker.StatePaused:
		return true
	case c.State.Running():
		r.dropHeld(it.id)
	}

	return false
}

// stopContainer stops the container id and waits as long as Docker takes,
// at most stopCallTimeout. A call that fails while the stop may still go
// on, as when a proxy drops the connection, is made once more: Docker
// answers a stop of a container that is stopping once it has stopped. The
// events of the stop are the watcher's own, unless it fails.
func (r *Reconciler) stopContainer(ctx context.Context, id string) error {
	withdraw := r.expect(id, "die", "stop")

	err := r.callStop(ctx, id)
	if err != nil && ctx.Err() == nil && !errors.Is(err, docker.ErrNotFound) && r.callStop(ctx, id) == nil {
		err = nil
	}

	if err != nil {
		withdraw()
	}

	return err
}

// callStop is one Stop within stopCallTimeout.
func (r *Reconciler) callStop(ctx context.Context, id string) error {
	sctx, cancel := context.WithTimeout(ctx, stopCallTimeout)
	defer cancel()

	return r.docker.Stop(sctx, id)
}

// inspect inspects a container within dockerTimeout.
func (r *Reconciler) inspect(ctx context.Context, id string) (docker.Container, error) {
	ictx, cancel := context.WithTimeout(ctx, r.dockerTimeout)
	defer cancel()

	return r.docker.Inspect(ictx, id)
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

// returnRun brings back the containers held for the run of src, from the
// last taken down to the first. With keep, those that come back stay
// listed, as back, until releaseRun. It returns the IDs of those it could
// not bring back, which the retry loop takes over.
func (r *Reconciler) returnRun(ctx context.Context, log *slog.Logger, src backend.Source, run string, keep bool) map[string]bool {
	failed, gone := r.bringBack(ctx, log, r.heldFor(src, run), keep)
	if !keep {
		r.wakeWaiters(gone...)
	}

	return failed
}

// releaseRun drops the containers of the run of src that came back, once
// the run is done with them, and wakes the paths that wait for them.
func (r *Reconciler) releaseRun(src backend.Source, run string) {
	var ids []string

	for _, h := range r.heldFor(src, run) {
		if h.Back {
			r.dropHeld(h.ID)
			ids = append(ids, h.ID)
		}
	}

	r.wakeWaiters(ids...)
}

// bringBack brings back the held containers, one after the other. It
// returns the IDs of those it could not bring back, which stay held,
// released for the retry loop, which it wakes, and of those that are no
// longer held or, with keep, are back. When ctx ends, the rest stays held
// as it is, for the next start.
func (r *Reconciler) bringBack(ctx context.Context, log *slog.Logger, held []state.Held, keep bool) (failed map[string]bool,
	gone []string,
) {
	failed = map[string]bool{}

	for _, h := range held {
		if ctx.Err() != nil {
			break
		}

		switch msg := r.bringBackOne(ctx, log, h, keep); {
		case msg == "":
			gone = append(gone, h.ID)
		case ctx.Err() == nil:
			failed[h.ID] = true
		}
	}

	if len(failed) > 0 {
		r.wakeReturns()
	}

	return failed, gone
}

// bringBackOne brings back one held container and returns why it is still
// down, empty once it is no longer held down. A container that is gone, or
// that someone else started since the watcher took it down, is no longer
// held; so is one that runs: someone else brought it back. Only a stopped
// container is started and only a paused one unpaused: one the watcher
// paused and someone else stopped stays stopped. With keep, a container it
// brings back stays listed as back; one listed as back is left alone, or
// without keep dropped.
func (r *Reconciler) bringBackOne(ctx context.Context, log *slog.Logger, h state.Held, keep bool) string {
	r.returnMu.Lock()
	defer r.returnMu.Unlock()

	// Another goroutine may have brought it back meanwhile.
	cur, ok := r.heldByID()[h.ID]
	if !ok || cur.Source != h.Source || cur.Run != h.Run {
		return ""
	}

	h = cur
	attrs := []any{"source", h.Source.String(), "container", h.Name, "mode", h.Mode}

	switch {
	case h.Back:
		if !keep {
			r.dropHeld(h.ID)
		}

		return ""
	case h.StartedAt.IsZero():
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
		r.notHeld(h, c, "it is gone")

		return ""
	case err != nil:
		return r.returnFailed(log, h, attrs, err)
	case !startedAt(c).Equal(h.StartedAt):
		log.Info("not bringing back a container someone else started meanwhile; it is left as it is",
			append(attrs, "state", string(c.State))...)
		r.notHeld(h, c, "someone else started it")

		return ""
	case h.Mode == ModeStop && (c.State == docker.StateExited || c.State == docker.StateCreated || c.State == docker.StateDead):
		do, verb = r.docker.Start, "started"
	case h.Mode == ModePause && c.State == docker.StatePaused:
		do, verb = r.docker.Unpause, "unpaused"
	case h.Mode == ModeStop && c.State == docker.StateRunning && !h.Down && r.now().Sub(h.At) < stopCallTimeout:
		// A stop of the watcher cut the stop short, and Docker may still be
		// stopping the container.
		log.Info("the container is not down yet; looking again later", attrs...)
		r.retryLater(h)

		return "not down yet"
	case c.State.Running():
		log.Info("not bringing back a container that runs again", append(attrs, "state", string(c.State))...)
		r.notHeld(h, c, "")

		return ""
	default:
		log.Warn("not bringing back a container someone else stopped meanwhile; it is left as it is",
			append(attrs, "state", string(c.State))...)
		r.notHeld(h, c, "someone else stopped it")

		return ""
	}

	withdraw := func() {}
	if h.Mode == ModeStop {
		withdraw = r.expect(h.ID, "start")
	}

	if err := do(ictx, h.ID); err != nil {
		withdraw()
		return r.returnFailed(log, h, attrs, err)
	}

	downtime := max(r.now().Sub(h.At), 0)
	log.Info("container "+verb+" again after the snapshot", append(attrs, "downtime", downtime.String())...)
	r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode, Downtime: downtime})

	if keep {
		r.markHeld(h.ID, func(x *state.Held) { x.Back, x.Released = true, false })
		r.forgetRetries(h.ID)
	} else {
		r.dropHeld(h.ID)
	}

	r.queueContainer(c)

	return ""
}

// returnFailed logs and reports a container that could not be brought
// back, which stays held, released for the retry loop, and returns why.
func (r *Reconciler) returnFailed(log *slog.Logger, h state.Held, attrs []any, err error) string {
	msg := err.Error()

	log.Error("cannot bring back a container the watcher took down for a snapshot; trying again", append(attrs, "error", msg)...)
	r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode, Error: msg})
	r.retryLater(h)

	return msg
}

// retryLater releases a held container to the retry loop, which tries it
// again after a delay that grows with each try.
func (r *Reconciler) retryLater(h state.Held) {
	r.mu.Lock()
	r.returnFails[h.ID]++
	r.returnRetry[h.ID] = r.now().Add(backoff(r.minBackoff, r.maxBackoff, r.returnFails[h.ID]))
	r.mu.Unlock()

	if !h.Released {
		r.markHeld(h.ID, func(x *state.Held) { x.Released = true })
	}
}

// forgetRetries forgets the tries of the container with the ID.
func (r *Reconciler) forgetRetries(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.returnFails, id)
	delete(r.returnRetry, id)
}

// markHeld changes the held container with the ID with fn.
func (r *Reconciler) markHeld(id string, fn func(h *state.Held)) {
	err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == id })
		if i < 0 {
			return nil, errNoChange
		}

		fn(&held[i])

		return held, nil
	})
	if err != nil && !errors.Is(err, errNoChange) && !errors.Is(err, state.ErrNotPersisted) {
		r.log.Error("cannot store the containers held down", "container", id, "error", err.Error())
	}
}

// notHeld drops a held container that the watcher no longer has to bring
// back, and queues the paths of one that still exists. One the watcher
// failed to bring back before is reported as no longer down, with no
// downtime, and note says why, empty when it runs.
func (r *Reconciler) notHeld(h state.Held, c docker.Container, note string) {
	if h.Released {
		r.metrics.ContainerReturned(h.Source.Path, ReturnResult{Container: h.Name, Mode: h.Mode, Note: note})
	}

	r.dropHeld(h.ID)

	if c.ID != "" {
		r.queueContainer(c)
	}
}

// dropHeld removes the held container with the ID from the state.
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

	r.forgetRetries(id)
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

// wakeReturns wakes the retry loop, which then looks at the held
// containers again.
func (r *Reconciler) wakeReturns() {
	select {
	case r.returnWake <- struct{}{}:
	default:
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

	_, gone := r.bringBack(ctx, r.log, byRunReversed(held), false)
	r.wakeWaiters(gone...)
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

	_, gone := r.bringBack(ctx, r.log, byRunReversed(todo), false)
	r.wakeWaiters(gone...)
}

// returnsDue returns the held containers the retry loop has to bring back
// at now, none of a run a goroutine of this process is busy with: those
// whose run has to have them back by now, its snapshot done or not, those
// it failed to bring back whose next try is due, and those left back by a
// run that ended without releasing them. next is when the next one is due;
// zero if none.
func (r *Reconciler) returnsDue(now time.Time) (todo []state.Held, next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	soonest := func(t time.Time) {
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}

	for _, h := range r.store.Held() {
		var at time.Time

		switch {
		case r.hooksRunning[h.Source] == h.Run:
			continue
		case h.Back:
		case h.Released:
			at = r.returnRetry[h.ID]
		case h.ReturnBy.IsZero():
			continue
		default:
			at = h.ReturnBy
		}

		if now.Before(at) {
			soonest(at)
		} else {
			todo = append(todo, h)
		}
	}

	return todo, next
}

// returnLoop brings back the held containers whose run has to have them
// back, its snapshot done or not, without waiting for a pass, and tries
// again those that could not be brought back, with a growing delay, until
// ctx is done.
func (r *Reconciler) returnLoop(ctx context.Context) {
	t := time.NewTimer(time.Hour)
	defer t.Stop()

	for {
		todo, next := r.returnsDue(r.now())

		if len(todo) > 0 {
			for _, h := range todo {
				if !h.Released && !h.Back {
					r.log.Warn("bringing back a container before its snapshot has ended: max-downtime has passed; "+
						"a snapshot that goes on reads files the container may change", "source", h.Source.String(),
						"container", h.Name, "return_by", h.ReturnBy.Format(timeFormat))
				}
			}

			_, gone := r.bringBack(ctx, r.log, byRunReversed(todo), false)
			r.wakeWaiters(gone...)

			if ctx.Err() != nil {
				return
			}

			// What is still due could not be settled: look again soon.
			var still []state.Held
			if still, next = r.returnsDue(r.now()); len(still) > 0 {
				next = r.now().Add(r.minBackoff)
			}
		}

		t.Stop()

		if !next.IsZero() {
			t.Reset(max(next.Sub(r.now()), 0))
		}

		select {
		case <-ctx.Done():
			return
		case <-r.returnWake:
		case <-t.C:
		}
	}
}
