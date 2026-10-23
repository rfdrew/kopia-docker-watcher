package reconcile

import (
	"context"
	"slices"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// The paths the volumes label picks come from the mounts of a container,
// which Docker leaves out of its events. An event of a container with the
// volumes label therefore queues, besides the paths of the path label:
//
//   - at once, the paths its mounts made when the container was last seen
//     in a listing or an inspect, and those the state records it as an
//     owner of: all that is left after the container is removed;
//   - after a listing of the containers, which runs in the order of the
//     events outside HandleEvent, the paths its mounts make now.
//
// Each path gets the event once.

// volumeEvent is an event waiting for the listing of its container, with
// the paths it was already queued for.
type volumeEvent struct {
	ev     docker.Event
	queued []string
}

// hasVolumes reports whether container labels have the volumes label,
// whatever its value.
func (r *Reconciler) hasVolumes(l map[string]string) bool {
	_, ok := l[labels.Name(r.labelPrefix, labels.KeyVolumes)]
	return ok
}

// noteMounts records the volume paths of a listed or inspected container.
// r.mu is held.
func (r *Reconciler) noteMounts(pc parsed) {
	if len(pc.volumePaths) == 0 {
		delete(r.mounts, pc.container.ID)
		return
	}

	r.mounts[pc.container.ID] = slices.Clone(pc.volumePaths)
}

// forgetMounts drops the volume paths of the containers a listing did not
// have. r.mu is held.
func (r *Reconciler) forgetMounts(seen map[string]bool) {
	for id := range r.mounts {
		if !seen[id] {
			delete(r.mounts, id)
		}
	}
}

// handleVolumeEvent queues the paths of an event of a container with the
// volumes label that the labels in the event cannot name, except those in
// queued, which the event was queued for already. It never blocks.
func (r *Reconciler) handleVolumeEvent(ev docker.Event, queued []string) {
	known := r.ownedPaths(ev.ContainerID)

	r.mu.Lock()
	known = append(known, r.mounts[ev.ContainerID]...)
	r.mu.Unlock()

	queued = r.queueEvent(ev, known, queued)

	r.mu.Lock()
	defer r.mu.Unlock()

	if ev.Kind == docker.KindRemoved {
		delete(r.mounts, ev.ContainerID)
		return
	}

	r.inspects = append(r.inspects, volumeEvent{ev: ev, queued: queued})

	select {
	case r.inspectWake <- struct{}{}:
	default:
	}
}

// ownedPaths returns the paths of the sources the state records the
// container as an owner of.
func (r *Reconciler) ownedPaths(id string) []string {
	var out []string

	for _, src := range r.store.Sources() {
		rec, ok := r.store.Get(src)
		if ok && slices.ContainsFunc(rec.Owners, func(o state.Owner) bool { return o.ID == id }) {
			out = append(out, src.Path)
		}
	}

	return out
}

// queueEvent queues the paths not in queued after EventDelay, keeping ev
// with each, and returns queued with them.
func (r *Reconciler) queueEvent(ev docker.Event, paths, queued []string) []string {
	var fresh []string

	for _, p := range paths {
		if !slices.Contains(queued, p) && !slices.Contains(fresh, p) {
			fresh = append(fresh, p)
		}
	}

	r.mu.Lock()
	for _, p := range fresh {
		r.pending[p] = compact(append(r.pending[p], ev))
	}
	r.mu.Unlock()

	for _, p := range fresh {
		r.queue.addAfter(p, r.eventDelay)
	}

	return append(queued, fresh...)
}

// inspectLoop looks up the containers of volume events in their order
// until ctx is done, and queues the paths their mounts make.
func (r *Reconciler) inspectLoop(ctx context.Context) {
	for {
		r.mu.Lock()

		if len(r.inspects) == 0 {
			r.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-r.inspectWake:
				continue
			}
		}

		ve := r.inspects[0]
		r.inspects = r.inspects[1:]
		r.mu.Unlock()

		r.inspectVolumes(ctx, ve)
	}
}

// inspectVolumes looks up the container of ve in a listing and queues the
// paths its mounts make. A container that is gone has nothing more to
// queue; if Docker fails, a full reconcile finds the paths, though without
// the event.
func (r *Reconciler) inspectVolumes(ctx context.Context, ve volumeEvent) {
	containers, err := r.containers(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("cannot list the containers for the paths of a container's volumes; reconciling everything",
				"container", ve.ev.Name, "error", err.Error())
			r.ReconcileAll()
		}

		return
	}

	i := slices.IndexFunc(containers, func(c docker.Container) bool { return c.ID == ve.ev.ContainerID })
	if i < 0 {
		return
	}

	pc := r.parse(containers[i])
	r.reportLabels(pc)

	r.mu.Lock()
	r.noteMounts(pc)
	r.mu.Unlock()

	r.queueEvent(ve.ev, pc.volumePaths, ve.queued)
}

// eventOwner parses the labels of the container of an event that queued
// the pass's path, and reports whether they back the path up. With the
// volumes label the mounts are those of the listed container; one that is
// no longer listed counts as backing the path up, since its event was
// queued for the path from the mounts it had.
func (ps *pass) eventOwner(id string, ev docker.Event) (parsed, bool) {
	pc := ps.parseLabels(docker.Container{ID: id, Name: ev.Name, Labels: ev.Labels})
	if slices.Contains(pc.paths, ps.path) || len(pc.spec.Volumes) == 0 {
		return pc, slices.Contains(pc.paths, ps.path)
	}

	c, found := ps.container(id)
	if !found {
		return pc, true
	}

	c.Name, c.Labels = ev.Name, ev.Labels
	vpc := ps.parse(c)

	return vpc, slices.Contains(vpc.paths, ps.path)
}
