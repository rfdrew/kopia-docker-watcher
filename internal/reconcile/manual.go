package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// Outcomes of a manual trigger besides those of backend.SnapshotOutcome,
// which a snapshot that ran ends with.
const (
	// TriggerSkipped: no snapshot was taken, as the minimum interval has
	// not passed or one is under way; Message says why.
	TriggerSkipped = "skipped"
	// TriggerError: the trigger failed before a snapshot ran, as when a
	// pre-exec hook failed or kopia server refused the snapshot.
	TriggerError = "error"
)

// minPrefixLen is how many characters of a container ID a target needs at
// least to name the container by its ID.
const minPrefixLen = 4

// ErrNotRunning means the reconciler does not run, or is stopping, so it
// takes no manual requests.
var ErrNotRunning = errors.New("the watcher is not running or is stopping")

// TriggerResult is how a manual trigger of one path ended.
type TriggerResult struct {
	Path   string `json:"path"`
	Source string `json:"source,omitempty"`
	// Outcome is a backend.SnapshotOutcome when a snapshot ran,
	// TriggerSkipped or TriggerError when none did.
	Outcome string `json:"outcome"`
	// Message says why no snapshot was taken, or what went wrong.
	Message string `json:"message,omitempty"`
	// TaskID is kopia server's task of the snapshot, SnapshotStart the
	// start of the snapshot it saved; zero if none.
	TaskID        string    `json:"taskID,omitempty"`
	SnapshotStart time.Time `json:"snapshotStart,omitzero"`
	// Hooks reports a snapshot with hooks; PostError what of its post-exec
	// hooks failed.
	Hooks     bool   `json:"hooks,omitempty"`
	PostError string `json:"postError,omitempty"`
}

// OK reports whether the trigger saved a snapshot, or found nothing
// changed, and its post-exec hooks did not fail.
func (t TriggerResult) OK() bool {
	ok := t.Outcome == string(backend.SnapshotSucceeded) || t.Outcome == string(backend.SnapshotIdentical)

	return ok && t.PostError == ""
}

// Trigger is a snapshot of one path asked for by hand, which the passes
// over the path take like one at a container's start: through the queue,
// with the hooks and the downtime of its owners and, unless forced, the
// minimum interval. Done is closed once Result is final.
type Trigger struct {
	path string
	// container and containerID name the container the trigger was asked
	// for by, empty when it was asked for by path: its min-interval label
	// applies, and the description names it.
	container, containerID string
	force                  bool
	// log gets the result.
	log *slog.Logger

	once   sync.Once
	done   chan struct{}
	result TriggerResult

	// snapshot is the result of the snapshot of the snapshot with hooks
	// the trigger is part of, until the run ends; guarded by
	// Reconciler.mu.
	snapshot *state.SnapshotResult
}

// Path returns the source path the trigger is for.
func (t *Trigger) Path() string { return t.path }

// Done is closed once the trigger has a result.
func (t *Trigger) Done() <-chan struct{} { return t.done }

// Result returns how the trigger ended; final once Done is closed.
func (t *Trigger) Result() TriggerResult {
	select {
	case <-t.done:
		return t.result
	default:
		return TriggerResult{Path: t.path}
	}
}

// resolve sets the result once, and logs it.
func (t *Trigger) resolve(res TriggerResult) {
	t.once.Do(func() {
		res.Path = t.path
		t.result = res
		close(t.done)

		t.log.Info("snapshot asked for by hand done", "path", res.Path, "outcome", res.Outcome, "message", res.Message,
			"task", res.TaskID, "post_exec_error", res.PostError)
	})
}

// runKey names a snapshot with hooks.
type runKey struct {
	src backend.Source
	id  string
}

// Trigger asks for a snapshot of each path of target, a container (its
// name, or a prefix of its ID) or a source path (a path the watcher manages,
// or a path label joined to HOST_PREFIX), through the queue: the next pass
// over each path takes it as it would take the snapshot at a container's
// start, so the hooks and downtime of the path's owners apply and, unless
// force is set, the minimum interval. The returned triggers tell how each
// ended. A container that backs up no path, or a path that no container
// backs up and the watcher does not manage, is an error.
func (r *Reconciler) Trigger(ctx context.Context, target string, force bool) ([]*Trigger, error) {
	if r.dryRun || r.stopping() {
		return nil, ErrNotRunning
	}

	containers, err := r.containers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}

	paths, c, err := r.resolveTarget(target, containers)
	if err != nil {
		return nil, err
	}

	out := make([]*Trigger, len(paths))

	r.mu.Lock()

	if r.runCtx == nil || r.runCtx.Err() != nil {
		r.mu.Unlock()
		return nil, ErrNotRunning
	}

	for i, p := range paths {
		t := &Trigger{path: p, container: c.Name, containerID: c.ID, force: force, done: make(chan struct{}), log: r.log}
		r.manual[p] = append(r.manual[p], t)
		out[i] = t
	}

	r.mu.Unlock()

	for _, p := range paths {
		r.log.Info("snapshot asked for by hand", "path", p, "target", target, "force", force)
		r.queue.add(p)
	}

	return out, nil
}

// resolveTarget returns the source paths of target, and the container it
// names, if it names one.
func (r *Reconciler) resolveTarget(target string, containers []docker.Container) ([]string, docker.Container, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, docker.Container{}, errors.New("no container or path given")
	}

	var pathErr error

	// A path starts with a slash; so may a container name, as Docker
	// writes it.
	if strings.HasPrefix(target, "/") {
		p, err := r.resolvePath(target, containers)
		if err == nil {
			return []string{p}, docker.Container{}, nil
		}

		pathErr = err
	}

	c, err := findContainer(target, containers)
	if err != nil {
		if pathErr != nil {
			return nil, docker.Container{}, pathErr
		}

		return nil, docker.Container{}, err
	}

	pc := r.parse(c)

	switch {
	case !pc.spec.Enable:
		return nil, c, fmt.Errorf("container %s does not enable the backup (label %s)", c.Name,
			labels.Name(r.labelPrefix, labels.KeyEnable))
	case len(pc.paths) == 0:
		return nil, c, fmt.Errorf("container %s backs up no path its labels make valid; see validate --container %s", c.Name, c.Name)
	}

	return slices.Clone(pc.paths), c, nil
}

// resolvePath returns the source path of target: target itself when a
// container backs it up or the state has a source of it, otherwise target
// as a path label, joined to HOST_PREFIX.
func (r *Reconciler) resolvePath(target string, containers []docker.Container) (string, error) {
	known := func(p string) bool {
		for _, src := range r.store.Sources() {
			if src.Path == p && labels.WithinPrefix(r.watcherPrefix, p) {
				return true
			}
		}

		for _, c := range containers {
			if pc := r.parse(c); pc.spec.Enable && slices.Contains(pc.paths, p) {
				return true
			}
		}

		return false
	}

	clean := path.Clean(target)
	if known(clean) {
		return clean, nil
	}

	if sp, err := labels.SourcePathWithin(r.hostPrefix, r.watcherPrefix, target); err == nil && known(sp) {
		return sp, nil
	}

	return "", fmt.Errorf("no container backs up %s and the watcher manages no source of it", clean)
}

// findContainer finds a container by its name, with or without the
// leading slash, or by a unique prefix of its ID.
func findContainer(target string, containers []docker.Container) (docker.Container, error) {
	name := strings.TrimPrefix(target, "/")

	for _, c := range containers {
		if c.Name == name || c.ID == target {
			return c, nil
		}
	}

	if len(target) >= minPrefixLen {
		var found []docker.Container

		for _, c := range containers {
			if strings.HasPrefix(c.ID, target) {
				found = append(found, c)
			}
		}

		switch len(found) {
		case 1:
			return found[0], nil
		case 0:
		default:
			return docker.Container{}, fmt.Errorf("%q is the start of the ID of %d containers; give more of it", target, len(found))
		}
	}

	return docker.Container{}, fmt.Errorf("no container named %q", name)
}

// takeManual takes the manual triggers waiting for a pass over p.
func (r *Reconciler) takeManual(p string) []*Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()

	ts := r.manual[p]
	delete(r.manual, p)

	return ts
}

// restoreManual puts back manual triggers for the next pass over p.
func (r *Reconciler) restoreManual(p string, ts []*Trigger) {
	if len(ts) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.manual[p] = append(slices.Clone(ts), r.manual[p]...)
}

// manualRequests returns the snapshot requests of the pass's manual
// triggers. One by a container that no longer backs up the path ends
// without a snapshot.
func (ps *pass) manualRequests() []snapRequest {
	var reqs []snapRequest

	for _, t := range ps.manual {
		req := snapRequest{reason: "manual trigger", manual: t, minInterval: labels.DefaultMinInterval}

		if t.containerID != "" {
			i := slices.IndexFunc(ps.owners, func(o owner) bool { return o.id == t.containerID })
			if i < 0 {
				ps.declineTrigger(t, TriggerSkipped, "container "+t.container+" no longer backs up the path")
				continue
			}

			o := ps.owners[i]
			req.reason = "manual trigger of " + o.name
			req.minInterval = o.snap.minInterval
			req.container = o.snap.described
		} else if len(ps.owners) > 0 {
			// Any owner's interval lets a start snapshot through, so the
			// shortest one does.
			req.minInterval = slices.MinFunc(ps.owners, func(a, b owner) int {
				return int(a.snap.minInterval - b.snap.minInterval)
			}).snap.minInterval
		}

		reqs = append(reqs, req)
	}

	return reqs
}

// declineTrigger ends a trigger of the pass without a snapshot.
func (ps *pass) declineTrigger(t *Trigger, outcome, msg string) {
	ps.handled[t] = true
	t.resolve(TriggerResult{Source: ps.src.String(), Outcome: outcome, Message: msg})
}

// decline ends the manual triggers of reqs without a snapshot, as a pass
// that drops reqs does.
func (ps *pass) decline(reqs []snapRequest, msg string) {
	for _, r := range reqs {
		if r.manual != nil {
			ps.declineTrigger(r.manual, TriggerSkipped, msg)
		}
	}
}

// manualOf returns the manual triggers of reqs.
func manualOf(reqs []snapRequest) []*Trigger {
	var out []*Trigger

	for _, r := range reqs {
		if r.manual != nil {
			out = append(out, r.manual)
		}
	}

	return out
}

// attachWait makes the manual triggers of reqs end with the snapshot of
// the wait with the ID.
func (ps *pass) attachWait(reqs []snapRequest, id string) {
	ts := manualOf(reqs)
	if len(ts) == 0 {
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	key := waitKey{src: ps.src, id: id}

	for _, t := range ts {
		ps.handled[t] = true
		ps.manualWaits[key] = append(ps.manualWaits[key], t)
	}
}

// attachRun makes the manual triggers of reqs end with the snapshot with
// hooks with the ID.
func (ps *pass) attachRun(reqs []snapRequest, id string) {
	ts := manualOf(reqs)
	if len(ts) == 0 {
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	key := runKey{src: ps.src, id: id}

	for _, t := range ts {
		ps.handled[t] = true
		ps.manualRuns[key] = append(ps.manualRuns[key], t)
	}
}

// keepManual keeps the manual triggers of reqs for the next pass.
func (ps *pass) keepManual(reqs []snapRequest) {
	for _, t := range manualOf(reqs) {
		if !ps.handled[t] {
			ps.handled[t] = true
			ps.keptManual = append(ps.keptManual, t)
		}
	}
}

// runWaitQueued notes that the snapshot with hooks with the ID queued its
// snapshot as the wait with waitID: its result is kept for the triggers of
// the run.
func (r *Reconciler) runWaitQueued(src backend.Source, run, waitID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.manualRuns[runKey{src: src, id: run}]) > 0 {
		r.runOfWait[waitKey{src: src, id: waitID}] = run
	}
}

// waitEnded ends the manual triggers of the wait of src with the ID with
// the result, or keeps it for those of the snapshot with hooks it belongs
// to. A nil result ends them with msg, without a snapshot result.
func (r *Reconciler) waitEnded(src backend.Source, id string, res *state.SnapshotResult, msg string) {
	key := waitKey{src: src, id: id}

	r.mu.Lock()
	ts := r.manualWaits[key]
	delete(r.manualWaits, key)

	if run, ok := r.runOfWait[key]; ok && res != nil {
		delete(r.runOfWait, key)

		for _, t := range r.manualRuns[runKey{src: src, id: run}] {
			t.snapshot = res
		}
	}
	r.mu.Unlock()

	for _, t := range ts {
		t.resolve(snapshotResult(src, res, msg))
	}
}

// snapshotResult is the TriggerResult of a snapshot that ended with res,
// or that the watcher stopped following, for msg, with res nil.
func snapshotResult(src backend.Source, res *state.SnapshotResult, msg string) TriggerResult {
	if res == nil {
		return TriggerResult{Source: src.String(), Outcome: string(backend.SnapshotUnknown), Message: msg}
	}

	out := TriggerResult{Source: src.String(), Outcome: string(res.Outcome), Message: res.Error, TaskID: res.TaskID,
		SnapshotStart: res.SnapshotStart}

	if res.Outcome == backend.SnapshotUnknown && out.Message == "" {
		out.Message = "kopia server lost track of the run; the watcher looks for its snapshot and takes it once more if there is none"
	}

	return out
}

// runEnded ends the manual triggers of the snapshot with hooks run of src,
// which has ended, with the result of its snapshot, or with why it took
// none. postError is what of its post-exec hooks failed.
func (r *Reconciler) runEnded(src backend.Source, run state.HookRun, postError string) {
	key := runKey{src: src, id: run.ID}

	r.mu.Lock()
	ts := r.manualRuns[key]
	delete(r.manualRuns, key)

	if run.WaitID != "" {
		delete(r.runOfWait, waitKey{src: src, id: run.WaitID})
	}

	snaps := make([]*state.SnapshotResult, len(ts))
	for i, t := range ts {
		snaps[i] = t.snapshot
	}
	r.mu.Unlock()

	for i, t := range ts {
		var res TriggerResult

		switch snap := snaps[i]; {
		case snap != nil:
			res = snapshotResult(src, snap, "")
		case !run.PreDone:
			res = TriggerResult{Source: src.String(), Outcome: TriggerError, Message: "the watcher stopped while the pre-exec hooks ran"}
		case run.PreError != "":
			res = TriggerResult{Source: src.String(), Outcome: TriggerError, Message: "a pre-exec hook failed: " + run.PreError}
		case run.DownError != "":
			res = TriggerResult{Source: src.String(), Outcome: TriggerError,
				Message: "a container could not be stopped or paused: " + run.DownError}
		default:
			res = TriggerResult{Source: src.String(), Outcome: TriggerError,
				Message: "no snapshot was taken; the watcher's log says why"}
		}

		res.Hooks, res.PostError = true, postError
		t.resolve(res)
	}
}

// finishManual settles the manual triggers a pass took and did not deal
// with: back for the next pass if err asks for a retry, otherwise ended
// without a snapshot, for err or what the pass ended with. The kept ones
// go back for the next pass.
func (ps *pass) finishManual(err error, retry bool) {
	var back []*Trigger

	for _, t := range ps.manual {
		switch {
		case ps.handled[t]:
		case err != nil && retry:
			back = append(back, t)
		case err != nil && ps.ctx.Err() != nil:
			t.resolve(TriggerResult{Source: ps.src.String(), Outcome: TriggerError, Message: "the watcher stops"})
		case err != nil:
			t.resolve(TriggerResult{Source: ps.src.String(), Outcome: TriggerError, Message: err.Error()})
		default:
			msg := "no snapshot was taken"
			if ps.outcome != "" {
				msg += ": " + ps.outcome
			}

			t.resolve(TriggerResult{Source: ps.src.String(), Outcome: TriggerSkipped, Message: msg})
		}
	}

	ps.restoreManual(ps.path, append(back, ps.keptManual...))
}

// endManualOf ends the manual triggers followed under another kopia server
// identity than userName@host: the passes no longer follow their
// snapshots.
func (r *Reconciler) endManualOf(userName, host string) {
	other := func(src backend.Source) bool { return src.UserName != userName || src.Host != host }

	r.mu.Lock()

	var ts []*Trigger

	for k, l := range r.manualWaits {
		if other(k.src) {
			ts = append(ts, l...)
			delete(r.manualWaits, k)
		}
	}

	for k, l := range r.manualRuns {
		if other(k.src) {
			ts = append(ts, l...)
			delete(r.manualRuns, k)
		}
	}

	for k := range r.runOfWait {
		if other(k.src) {
			delete(r.runOfWait, k)
		}
	}
	r.mu.Unlock()

	for _, t := range ts {
		t.resolve(TriggerResult{Outcome: string(backend.SnapshotUnknown),
			Message: "kopia server's identity changed, so the watcher no longer follows the snapshot"})
	}
}

// endManual ends every manual trigger still open: the watcher stops.
func (r *Reconciler) endManual() {
	r.mu.Lock()

	var ts []*Trigger

	for _, l := range r.manual {
		ts = append(ts, l...)
	}

	for _, l := range r.manualWaits {
		ts = append(ts, l...)
	}

	for _, l := range r.manualRuns {
		ts = append(ts, l...)
	}

	r.manual = map[string][]*Trigger{}
	r.manualWaits = map[waitKey][]*Trigger{}
	r.manualRuns = map[runKey][]*Trigger{}
	r.runOfWait = map[waitKey]string{}
	r.mu.Unlock()

	for _, t := range ts {
		t.resolve(TriggerResult{Outcome: string(backend.SnapshotUnknown),
			Message: "the watcher stopped before the snapshot ended; a snapshot queued goes on in kopia server"})
	}
}

// minIntervalMessage says why a manual trigger took no snapshot before the
// minimum interval passes at next.
func minIntervalMessage(next time.Time) string {
	return "the minimum interval since the last snapshot has not passed; the next is allowed at " + next.Format(timeFormat) +
		", or now with --force"
}
