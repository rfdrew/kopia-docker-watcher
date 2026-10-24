package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// restoreRunPrefix starts the run ID under which a restore holds the
// containers it stopped, and reserves the source.
const restoreRunPrefix = "restore-"

// farFuture stands in for the return time of the containers a restore
// stops: they come back when the restore ends, not after a max-downtime.
var farFuture = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	// taskRecheck is how soon a container stopped for a restore is looked
	// at again while kopia server's restore task still runs.
	taskRecheck = 10 * time.Second
	// taskCheckTimeout bounds the question whether the task runs.
	taskCheckTimeout = 30 * time.Second
)

// runningOnly takes a container down for a snapshot only while it runs:
// one that is restarting or paused is left as it is.
func runningOnly(s docker.State) bool { return s == docker.StateRunning }

// takeable reports whether the run with the ID takes down a container in
// state s: a snapshot only a running one, a restore also one that is
// restarting, as in a crash loop, or paused, which would otherwise use
// the files under the restore.
func takeable(run string, s docker.State) bool {
	if strings.HasPrefix(run, restoreRunPrefix) {
		return s.Running()
	}

	return runningOnly(s)
}

// RestoreTarget is the source of one path, for a restore.
type RestoreTarget struct {
	Source backend.Source
	// Owners are the containers that back the path up, Running those of
	// them that run.
	Owners, Running []string
}

// RestoreTarget returns the source of target, a container or a path as
// Trigger takes them, which must make one path.
func (r *Reconciler) RestoreTarget(ctx context.Context, target string) (RestoreTarget, error) {
	if r.dryRun || r.stopping() {
		return RestoreTarget{}, ErrNotRunning
	}

	containers, err := r.containers(ctx)
	if err != nil {
		return RestoreTarget{}, fmt.Errorf("list containers: %w", err)
	}

	paths, c, err := r.resolveTarget(target, containers)
	if err != nil {
		return RestoreTarget{}, err
	}

	if len(paths) > 1 {
		return RestoreTarget{}, fmt.Errorf("container %s backs up %d paths, %s: name the one to restore", c.Name, len(paths),
			strings.Join(paths, ", "))
	}

	src, err := r.source(ctx, paths[0])
	if err != nil {
		return RestoreTarget{}, fmt.Errorf("ask kopia server for its identity: %w", err)
	}

	out := RestoreTarget{Source: src, Owners: []string{}, Running: []string{}}

	for _, o := range r.owners(paths[0], containers) {
		out.Owners = append(out.Owners, o.name)

		if o.running() {
			out.Running = append(out.Running, o.name)
		}
	}

	return out, nil
}

// StopOwnersFor stops the running containers that back up the source of
// path p, with every running container that depends on them by the
// depends-on labels, as a snapshot with stop-during-backup does, runs fn,
// and brings them back once fn returns. Containers that are restarting or
// paused are stopped too. fn runs on a context that ends when the watcher
// stops; the containers then come back within ShutdownGrace, or at the
// next start. The containers are held in the state as for a snapshot, and
// their events are the watcher's own, so their source is not paused and
// takes no snapshot at their stop or start. While fn runs, the source
// takes no snapshot the watcher queues. Progress is told what is done, for
// the user.
//
// fn tells started the ID of kopia server's restore task once it has one.
// It is stored with the held containers, and none of them is brought back
// while Options.RestoreTaskRunning reports the task running: not after fn
// returns, not when the watcher stops and not at the next start, so that
// no container runs on files kopia server still writes.
//
// It fails, without stopping anything, while hooks or a snapshot with
// hooks of the source run, while a container it needs is held down for a
// snapshot, and when the state cannot be written.
func (r *Reconciler) StopOwnersFor(ctx context.Context, src backend.Source, progress func(string),
	fn func(ctx context.Context, started func(taskID string)) error,
) error {
	if r.dryRun {
		return ErrNotRunning
	}

	containers, err := r.containers(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	owners := r.owners(src.Path, containers)

	var ids []string

	for _, o := range owners {
		if o.state.Running() && !o.held {
			ids = append(ids, o.id)
		}
	}

	runID := restoreRunPrefix + r.now().UTC().Format(time.RFC3339Nano)

	runCtx, hookCtx, err := r.reserveRestore(src, runID)
	if err != nil {
		return err
	}

	defer r.releaseRestore(src)

	if rec, _ := r.store.Get(src); rec.HookRun != nil {
		return fmt.Errorf("a snapshot with hooks of the source is under way (phase %s); try again once it is done", rec.HookRun.Phase)
	}

	group, members, err := r.stopGroup(ids, containers, docker.State.Running)
	if err != nil {
		return fmt.Errorf("cannot stop the containers of the source: %w", err)
	}

	need := slices.Clone(members)
	for _, o := range owners {
		need = append(need, o.id)
	}

	items := make([]downItem, len(group))
	for i, c := range group {
		items[i] = downItem{id: c.ID, name: c.Name, mode: ModeStop}
		need = append(need, c.ID)
	}

	if err := r.holdForRestore(src, runID, items, need); err != nil {
		return err
	}

	log := r.log.With("path", src.Path)

	if len(items) > 0 {
		progress("stopping " + strings.Join(downNames(items), ", "))
		log.Info("taking containers down for a restore", "source", src.String(), "containers", downNames(items))
	} else {
		progress("no container of the source runs; nothing to stop")
	}

	_, failure := r.takeDown(hookCtx, log, src, state.HookRun{ID: runID, ReturnBy: farFuture}, items)

	if failure == "" && r.stopping() {
		failure = "the watcher stops"
	}

	if failure == "" {
		fnCtx, cancel := context.WithCancel(runCtx)
		err = fn(fnCtx, func(taskID string) { r.setRestoreTask(src, runID, taskID) })

		cancel()
	} else {
		err = errors.New("cannot stop the containers of the source: " + failure)
	}

	if held := r.heldFor(src, runID); len(held) > 0 {
		progress("starting the stopped containers again")

		if failed := r.returnRun(hookCtx, log, src, runID, false); len(failed) > 0 {
			var names []string

			for _, h := range held {
				if failed[h.ID] {
					names = append(names, h.Name)
				}
			}

			progress(strings.Join(names, ", ") + " not started yet: the watcher starts them as soon as it can; its log says why")
		}
	}

	return err
}

// reserveRestore reserves the source for a restore with the run ID, as a
// goroutine that runs hooks of the source, and returns Run's context and
// the context the containers come back on.
func (r *Reconciler) reserveRestore(src backend.Source, runID string) (runCtx, hookCtx context.Context, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch cur := r.hooksRunning[src]; {
	case r.runCtx == nil || r.runCtx.Err() != nil || r.hookCtx == nil:
		return nil, nil, ErrNotRunning
	case strings.HasPrefix(cur, restoreRunPrefix):
		return nil, nil, errors.New("another restore of the source is under way")
	case cur != "":
		return nil, nil, errors.New("hooks of the source run for a snapshot; try again once it is done")
	}

	r.hooksRunning[src] = runID
	r.hookers.Add(1)

	return r.runCtx, r.hookCtx, nil
}

// releaseRestore gives up the reservation of reserveRestore and queues the
// path, whose snapshots may have waited for the restore.
func (r *Reconciler) releaseRestore(src backend.Source) {
	r.mu.Lock()
	delete(r.hooksRunning, src)
	r.mu.Unlock()

	r.hookers.Done()
	r.wakeReturns()
	r.queue.addUnlessRetrying(src.Path)
}

// holdForRestore stores the items as held for the restore run of src,
// unless a container the restore needs is held already or a hook runs in
// one it stops.
func (r *Reconciler) holdForRestore(src backend.Source, runID string, items []downItem, need []string) error {
	r.mu.Lock()
	for _, it := range items {
		if r.execs[it.id] > 0 {
			r.mu.Unlock()
			return fmt.Errorf("a hook runs in %s for a snapshot of another source; try again once it is done", it.name)
		}
	}
	r.mu.Unlock()

	now := r.now()

	var busy *state.Held

	err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		for _, id := range need {
			if i := slices.IndexFunc(held, func(h state.Held) bool { return h.ID == id }); i >= 0 {
				b := held[i]
				busy = &b

				return nil, errNoChange
			}
		}

		for i, it := range items {
			held = append(held, state.Held{ID: it.id, Name: it.name, Mode: it.mode, Source: src, Run: runID, Order: i, At: now})
		}

		return held, nil
	})

	switch {
	case busy != nil:
		return fmt.Errorf("container %s is held down for a snapshot of %s; try again once it is back", busy.Name, busy.Source.Path)
	case errors.Is(err, state.ErrNotPersisted):
		// Nothing was taken down yet.
		for _, h := range r.heldFor(src, runID) {
			r.dropHeld(h.ID)
		}

		return fmt.Errorf("STATE_FILE cannot be written, and without it a crash of the watcher could leave the containers down: %w", err)
	case err != nil:
		return fmt.Errorf("store the containers held down: %w", err)
	}

	return nil
}

// setRestoreTask stores the restore task the containers held for the run
// wait for.
func (r *Reconciler) setRestoreTask(src backend.Source, runID, taskID string) {
	err := r.store.UpdateHeld(func(held []state.Held) ([]state.Held, error) {
		for i := range held {
			if held[i].Source == src && held[i].Run == runID {
				held[i].RestoreTask = taskID
			}
		}

		return held, nil
	})
	if err != nil && !errors.Is(err, state.ErrNotPersisted) {
		r.log.Error("cannot store the restore task the containers wait for", "source", src.String(), "task", taskID,
			"error", err.Error())
	}
}

// restoreTaskRuns returns why the held container h stays down: kopia
// server's restore task it waits for still runs, or it cannot be told
// whether it does; empty when it may come back. The retry loop looks at it
// again.
func (r *Reconciler) restoreTaskRuns(ctx context.Context, log *slog.Logger, h state.Held, attrs []any) string {
	if h.RestoreTask == "" || r.restoreTaskRunning == nil {
		return ""
	}

	tctx, cancel := context.WithTimeout(ctx, taskCheckTimeout)
	defer cancel()

	running, err := r.restoreTaskRunning(tctx, h.RestoreTask)

	switch {
	case err != nil:
		return r.returnFailed(log, h, append(attrs, "restore_task", h.RestoreTask),
			fmt.Errorf("cannot tell whether kopia server's restore task %s still runs: %w", h.RestoreTask, err))
	case running:
		log.Info("not bringing back a container while kopia server's restore task still runs", append(attrs, "restore_task",
			h.RestoreTask)...)

		r.mu.Lock()
		r.returnRetry[h.ID] = r.now().Add(taskRecheck)
		r.mu.Unlock()

		if !h.Released {
			r.markHeld(h.ID, func(x *state.Held) { x.Released = true })
		}

		return "the restore task runs"
	}

	return ""
}

// restoring reports whether a restore that stopped the containers of the
// pass's source runs.
func (ps *pass) restoring() bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return strings.HasPrefix(ps.hooksRunning[ps.src], restoreRunPrefix)
}
