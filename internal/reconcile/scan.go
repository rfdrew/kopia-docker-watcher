package reconcile

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// scanLoop serves ReconcileAll until ctx is done. A scan that fails, as
// when Docker is down, is tried again after a growing delay.
func (r *Reconciler) scanLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.scan:
		}

		for attempt := 1; ; attempt++ {
			paths, err := r.paths(ctx)
			if err == nil {
				for _, p := range paths {
					// A full reconcile does not cut a retry delay short.
					if !r.backingOff(p) {
						r.queue.add(p)
					}
				}

				r.log.Debug("queued every path", "paths", len(paths))

				break
			}

			if ctx.Err() != nil {
				return
			}

			delay := backoff(r.minBackoff, r.maxBackoff, attempt)
			r.metrics.ReconcileError("", ReasonDocker)
			r.log.Warn("cannot list Docker containers for a full reconcile; retrying", "error", err.Error(), "delay", delay.String())

			if !sleep(ctx, delay) {
				return
			}
		}
	}
}

// RunOnce reconciles every path once, one after the other, and returns
// the errors that ended passes and the problems they reported, joined. It
// does not retry and queues nothing. It is meant for a dry run from the
// command line, and fails if Run or another RunOnce is running.
func (r *Reconciler) RunOnce(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("reconcile: RunOnce called while running")
	}

	defer r.running.Store(false)

	paths, err := r.paths(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	var errs []error

	for _, p := range paths {
		res := r.reconcile(ctx, p)
		errs = append(errs, res.problems...)

		if res.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p, res.err))
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return errors.Join(errs...)
}

// paths returns every path to reconcile, sorted: the valid source paths of
// containers that enable the backup, and the paths of the sources in the
// state. It reports label problems on the way, once per container and set
// of labels.
func (r *Reconciler) paths(ctx context.Context) ([]string, error) {
	containers, err := r.containers(ctx)
	if err != nil {
		return nil, err
	}

	r.forgetReported()

	set := map[string]bool{}
	seen := map[string]bool{}

	for _, c := range containers {
		pc := r.parse(c)
		seen[c.ID] = true

		r.reportLabels(pc)

		if !pc.spec.Enable {
			continue
		}

		for _, p := range pc.paths {
			set[p] = true
		}
	}

	r.mu.Lock()
	for id := range r.reported {
		if !seen[id] {
			delete(r.reported, id)
		}
	}
	r.mu.Unlock()

	for _, src := range r.store.Sources() {
		set[src.Path] = true
	}

	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}

	slices.Sort(out)

	return out, nil
}

// reportLabels logs the label problems of a container that has labels
// under the prefix, once for each set of problems.
func (r *Reconciler) reportLabels(pc parsed) {
	if !slices.ContainsFunc(slices.Collect(maps.Keys(pc.container.Labels)), func(k string) bool {
		return strings.HasPrefix(k, r.labelPrefix+".")
	}) {
		return
	}

	var lines []string
	for _, is := range pc.issues {
		lines = append(lines, is.Severity.String()+": "+is.String())
	}

	if pc.spec.Enable {
		for _, pe := range pc.badPaths {
			lines = append(lines, "error: "+r.labelPrefix+".path: "+pe.String())
		}
	}

	key := fmt.Sprint(lines)

	r.mu.Lock()
	last, had := r.reported[pc.container.ID]
	r.reported[pc.container.ID] = key
	r.mu.Unlock()

	if had && last == key || len(lines) == 0 {
		return
	}

	errorsFound := pc.issues.HasErrors() || pc.spec.Enable && len(pc.badPaths) > 0

	attrs := []any{"container", pc.container.Name, "problems", lines}
	if errorsFound {
		r.metrics.ReconcileError("", ReasonLabels)
		r.log.Error("container labels have errors", attrs...)

		return
	}

	r.log.Warn("container labels have warnings", attrs...)
}

// containers lists the containers within dockerTimeout.
func (r *Reconciler) containers(ctx context.Context) ([]docker.Container, error) {
	ctx, cancel := context.WithTimeout(ctx, r.dockerTimeout)
	defer cancel()

	return r.docker.Containers(ctx)
}

// sleep waits for d or until ctx is done; false means ctx is done.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
