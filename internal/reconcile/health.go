package reconcile

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

// The require-healthy label holds back the snapshots of a container's
// sources while the container runs and Docker does not report it healthy.
//
// Such a source is on the watcher's own schedule, as with hooks, so that a
// change of the health writes nothing to kopia server. A pass looks at the
// health of the owners that ask for it in the listing, and inspects those
// the listing does not tell about. A snapshot at a start waits while they
// are starting and is not taken once one is unhealthy; a scheduled time
// that comes while one is not healthy stays due, and once they are healthy
// the rules for missed times decide on it; a manual trigger is declined,
// unless forced. A snapshot at a stop is taken regardless: Docker reports a
// stopped container unhealthy. Docker sends an event only when the health
// changes, so HandleEvent queues the paths of a container that asks for it
// at every change.

// healthGated reports whether an owner asks with the require-healthy label
// that the snapshots of the source wait while it is not healthy.
func healthGated(owners []owner) bool {
	return slices.ContainsFunc(owners, func(o owner) bool { return o.requireHealthy })
}

// requiresHealthy reports whether the labels of a container ask for the
// backup and for health.
func (r *Reconciler) requiresHealthy(pc parsed) bool {
	return pc.spec.Enable && pc.spec.RequireHealthy && r.labelOK(pc, labels.KeyRequireHealthy)
}

// handleHealthEvent queues the paths of a container whose health changed,
// if its labels ask for health: those its labels name and, with the volumes
// label, those its mounts made when it was last seen, which a change of the
// health does not change.
func (r *Reconciler) handleHealthEvent(ev docker.Event, pc parsed, volumes bool) {
	if !r.requiresHealthy(pc) {
		return
	}

	paths := slices.Clone(pc.paths)

	if volumes {
		paths = append(paths, r.ownedPaths(ev.ContainerID)...)

		r.mu.Lock()
		paths = append(paths, r.mounts[ev.ContainerID]...)
		r.mu.Unlock()
	}

	r.log.Debug("the health of a container changed", "container", ev.Name, "event", ev.Action)
	r.queueEvent(ev, paths, nil)
}

// checkHealth finds the owners that hold back the snapshots of the source:
// those that ask for health, run and are not healthy. One held down by the
// watcher for a snapshot does not count: Docker reports it unhealthy or
// starting, but its health was looked at before it went down. An owner
// whose health the listing does not tell is inspected. A container without
// a health check is logged once.
func (ps *pass) checkHealth() error {
	ps.unhealthy = nil

	for i := range ps.owners {
		o := &ps.owners[i]
		if !o.requireHealthy || !o.state.Running() || o.held {
			continue
		}

		if o.health == docker.HealthUnknown {
			c, err := ps.inspect(ps.ctx, o.id)

			switch {
			case errors.Is(err, docker.ErrNotFound):
				// Gone since the listing: the pass of its event decides.
				continue
			case err != nil:
				return &inspectError{err: err}
			}

			o.health = c.Health
		}

		switch o.health {
		case docker.HealthHealthy:
		case docker.HealthNone:
			ps.warnOnce("no health check "+o.id, "the container has no health check; "+
				labels.Name(ps.labelPrefix, labels.KeyRequireHealthy)+" has no effect", "container", o.name)
		default:
			ps.unhealthy = append(ps.unhealthy, *o)
		}
	}

	if len(ps.unhealthy) == 0 {
		ps.endHeldDue()
	}

	return nil
}

// holdForHealth holds back the snapshot reqs ask for while owners that ask
// for health are not healthy, and reports whether it did:
//
//   - a snapshot at a start, the watcher's start and one no event told of
//     included, waits while those owners are starting: its event is kept,
//     and the change of their health queues the next pass. Once one of them
//     is unhealthy it is not taken;
//   - the scheduled time stays due and counts as missed;
//   - a manual trigger ends without a snapshot;
//   - a snapshot taken again because kopia server lost the first waits:
//     its wait stays in the state, and a later pass asks again.
//
// A manual trigger with force lets the snapshot through, as it does with
// the minimum interval. Snapshots at a stop do not come here.
func (ps *pass) holdForHealth(reqs []snapRequest, attrs []any) bool {
	if len(ps.unhealthy) == 0 {
		return false
	}

	attrs = append(slices.Clone(attrs), "containers", healthNames(ps.unhealthy))

	if slices.ContainsFunc(reqs, func(r snapRequest) bool { return r.manual != nil && r.manual.force }) {
		ps.log.Info("taking the snapshot asked for by hand with force, although a container is not healthy", attrs...)
		return false
	}

	starting := !slices.ContainsFunc(ps.unhealthy, func(o owner) bool { return o.health != docker.HealthStarting })
	why := "not healthy: " + strings.Join(healthNames(ps.unhealthy), ", ")

	for _, r := range reqs {
		switch {
		case r.manual != nil:
			ps.log.Info("not taking the snapshot asked for by hand: a container is not healthy", attrs...)
			ps.declineTrigger(r.manual, TriggerSkipped, why)
			ps.metrics.UnhealthySkip(ps.path)
		case r.scheduled:
			ps.holdDue(attrs)
		case r.retry:
			ps.log.Debug("the snapshot taken again waits until the containers are healthy", attrs...)
		case r.event == nil:
		case starting:
			ps.keep = append(ps.keep, *r.event)
			ps.log.Debug("the snapshot at the start waits until the containers are healthy", append(attrs, "reason", r.reason)...)
		default:
			ps.log.Info("not taking the snapshot at the start: a container is not healthy", append(attrs, "reason", r.reason)...)
			ps.metrics.UnhealthySkip(ps.path)
		}
	}

	return true
}

// holdDue keeps the scheduled time of the pass due while owners are not
// healthy. It is no longer armed, so it counts as missed when they are
// healthy again. A time is logged and counted once.
func (ps *pass) holdDue(attrs []any) {
	if ps.due.IsZero() {
		return
	}

	ps.disarm()

	ps.mu.Lock()
	counted := ps.heldDue[ps.path].Equal(ps.due)
	ps.heldDue[ps.path] = ps.due
	ps.mu.Unlock()

	if counted {
		return
	}

	ps.metrics.UnhealthySkip(ps.path)
	ps.log.Warn("the scheduled snapshot is held back: a container is not healthy; once the containers are healthy, it "+
		"counts as missed", append(attrs, "due", ps.due.Format(timeFormat))...)
}

// endHeldDue forgets the scheduled time held back for unhealthy owners, and
// says so, once they are healthy.
func (ps *pass) endHeldDue() {
	ps.mu.Lock()
	due, ok := ps.heldDue[ps.path]
	delete(ps.heldDue, ps.path)
	ps.mu.Unlock()

	if ok {
		ps.log.Info("the scheduled snapshot is no longer held back for the health of the containers; it counts as missed",
			"source", ps.src.String(), "due", due.Format(timeFormat))
	}
}

// healthNames describes owners with their health for the log, e.g.
// "app (unhealthy)".
func healthNames(owners []owner) []string {
	out := make([]string, len(owners))
	for i, o := range owners {
		h := string(o.health)
		if h == "" {
			h = "unknown"
		}

		out[i] = fmt.Sprintf("%s (%s)", o.name, h)
	}

	return out
}

// inspectError is a failure to inspect an owner for its health.
type inspectError struct{ err error }

func (e *inspectError) Error() string { return "inspect a container for its health: " + e.err.Error() }
func (e *inspectError) Unwrap() error { return e.err }
