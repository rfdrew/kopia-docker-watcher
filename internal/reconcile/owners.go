package reconcile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
)

// parsed is a container with its labels read.
type parsed struct {
	container docker.Container
	spec      labels.ContainerSpec
	issues    labels.Issues
	// paths are the source paths of the path label that are valid, in
	// label order, then those of the mounts the volumes label picks, each
	// once.
	paths []string
	// volumePaths are the source paths of the mounts the volumes label
	// picks, in their order; the mounts come from c, so an event, which
	// has none, has none either.
	volumePaths []string
	// badPaths are the label paths and mount sources that make no source
	// path, with why.
	badPaths []pathError
}

type pathError struct {
	// key is the label the path comes from: the path or the volumes label.
	key  string
	path string
	err  error
}

func (e pathError) String() string { return e.err.Error() }

// parse reads the labels of c, and with the volumes label c's mounts. The
// problems with the mounts are issues of a container whose labels enable
// the backup only.
func (r *Reconciler) parse(c docker.Container) parsed { return r.parseWith(c, true) }

// parseLabels is parse for a container known by its labels only, as from
// an event: the volumes label picks no path and reports no problem.
func (r *Reconciler) parseLabels(c docker.Container) parsed { return r.parseWith(c, false) }

func (r *Reconciler) parseWith(c docker.Container, mounts bool) parsed {
	opts := labels.Options{Prefix: r.labelPrefix, Strict: r.strict}
	spec, issues := labels.Parse(c.Labels, opts)
	pc := parsed{container: c, spec: spec, issues: issues}

	add := func(key, p string) {
		sp, err := labels.SourcePathWithin(r.hostPrefix, r.watcherPrefix, p)
		if err != nil {
			pc.badPaths = append(pc.badPaths, pathError{key: key, path: p, err: err})
			return
		}

		if key == labels.KeyVolumes && !slices.Contains(pc.volumePaths, sp) {
			pc.volumePaths = append(pc.volumePaths, sp)
		}

		if !slices.Contains(pc.paths, sp) {
			pc.paths = append(pc.paths, sp)
		}
	}

	for _, p := range spec.Paths {
		add(labels.KeyPath, p)
	}

	if !mounts {
		return pc
	}

	vols, volIssues := labels.VolumePaths(spec, c, opts)
	for _, v := range vols {
		add(labels.KeyVolumes, v.Path)
	}

	if spec.Enable && len(volIssues) > 0 {
		pc.issues = append(pc.issues, volIssues...)
		slices.SortStableFunc(pc.issues, func(a, b labels.Issue) int { return strings.Compare(a.Label, b.Label) })
	}

	return pc
}

// valid reports whether the labels can be applied.
func (pc parsed) valid() bool { return !pc.issues.HasErrors() }

// labelOK reports whether the label with the key has no error; the other
// labels need not be valid.
func (r *Reconciler) labelOK(pc parsed, key string) bool {
	name := labels.Name(r.labelPrefix, key)

	return !slices.ContainsFunc(pc.issues, func(is labels.Issue) bool { return is.Label == name && is.Severity == labels.Error })
}

// patch returns the policy fields the labels set.
func (pc parsed) patch() backend.PolicyPatch {
	return pc.spec.Policy.Clone()
}

// owner is a container that uses a path.
type owner struct {
	id     string
	name   string
	state  docker.State
	labels map[string]string
	// valid reports whether its labels can be applied; patch is only set
	// then.
	valid  bool
	patch  backend.PolicyPatch
	issues labels.Issues
	// snap is what its labels say about snapshots, hooks about the hooks
	// around them, down about taking it down around them.
	snap  snapLabels
	hooks hookLabels
	down  downLabels
	// held reports that the watcher holds the container down for a
	// snapshot: stopped, it still counts as running.
	held bool
	// cleanup and deletePinned are what its labels ask of the cleanup of
	// the path's source once no container backs it up: the most cautious
	// values when the label in question has an error.
	cleanup      labels.CleanupMode
	deletePinned bool
}

// owners returns the containers that back up path p, stopped ones
// included, sorted by name.
func (r *Reconciler) owners(p string, containers []docker.Container) []owner {
	var out []owner

	held := r.heldByID()

	for _, c := range containers {
		pc := r.parse(c)
		if !pc.spec.Enable || !slices.Contains(pc.paths, p) {
			continue
		}

		o := owner{id: c.ID, name: c.Name, state: c.State, labels: c.Labels, valid: pc.valid(), issues: pc.issues,
			snap: r.snapshotLabels(pc), hooks: r.hookLabels(pc), down: r.downLabels(pc), cleanup: labels.CleanupPause}
		_, o.held = held[c.ID]
		if o.valid {
			o.patch = pc.patch()
		}

		if r.labelOK(pc, labels.KeyCleanupOnRemove) {
			o.cleanup = pc.spec.CleanupOnRemove
		}

		o.deletePinned = pc.spec.CleanupDeletePinned && r.labelOK(pc, labels.KeyCleanupDeletePinned)

		out = append(out, o)
	}

	slices.SortFunc(out, func(a, b owner) int { return strings.Compare(a.name, b.name) })

	return out
}

// ownerNames returns the names of owners in their order.
func ownerNames(owners []owner) []string {
	names := make([]string, len(owners))
	for i, o := range owners {
		names[i] = o.name
	}

	return names
}

// running reports whether the owner counts as running: as its state says,
// or held down by the watcher for a snapshot.
func (o owner) running() bool { return o.state.Running() || o.held }

// anyRunning reports whether an owner counts as running: a crash-looping
// owner, restarting between its runs, does, and so does one the watcher
// stopped for a snapshot.
func anyRunning(owners []owner) bool {
	return slices.ContainsFunc(owners, owner.running)
}

// anyRestarting reports whether an owner is restarting, which can end in
// exited without an event.
func anyRestarting(owners []owner) bool {
	return slices.ContainsFunc(owners, func(o owner) bool { return o.state == docker.StateRestarting })
}

// policyOwners returns the owners whose labels decide the policy: the
// running ones, or all of them when none runs. A stopped container keeps
// the labels it was created with, so a leftover one (an old one-off of
// docker compose run, the old version of a blue/green deploy) must not
// hold back the labels of the containers that run.
func policyOwners(owners []owner) []owner {
	var running []owner

	for _, o := range owners {
		if o.running() {
			running = append(running, o)
		}
	}

	if len(running) == 0 {
		return owners
	}

	return running
}

// commonPatch returns the patch every owner asks for. It fails when an
// owner's labels cannot be applied or when owners ask for different policy
// fields; the error says which containers and fields.
func commonPatch(owners []owner) (backend.PolicyPatch, error) {
	var invalid []string

	for _, o := range owners {
		if !o.valid {
			invalid = append(invalid, fmt.Sprintf("%s (%s)", o.name, errorIssues(o.issues)))
		}
	}

	if len(invalid) > 0 {
		return backend.PolicyPatch{}, &labelsError{msg: "labels of " + strings.Join(invalid, ", ") + " cannot be applied"}
	}

	// Group the owners by the patch they ask for, in name order.
	type group struct {
		patch backend.PolicyPatch
		names []string
	}

	var groups []group

	for _, o := range owners {
		i := slices.IndexFunc(groups, func(g group) bool { return g.patch.Equal(o.patch) })
		if i < 0 {
			groups = append(groups, group{patch: o.patch})
			i = len(groups) - 1
		}

		groups[i].names = append(groups[i].names, o.name)
	}

	if len(groups) == 1 {
		return groups[0].patch, nil
	}

	parts := make([]string, len(groups))
	for i, g := range groups {
		parts[i] = strings.Join(g.names, ", ") + ": " + describePatch(g.patch)
	}

	return backend.PolicyPatch{}, &disagreeError{msg: "containers that share the path ask for different policies: " + strings.Join(parts, "; ")}
}

// errorIssues joins the issues that are errors.
func errorIssues(issues labels.Issues) string {
	var out []string

	for _, is := range issues {
		if is.Severity == labels.Error {
			out = append(out, is.String())
		}
	}

	return strings.Join(out, "; ")
}

// describePatch lists the fields of a patch, e.g. scheduling.cron=["0 * * * *"].
func describePatch(p backend.PolicyPatch) string {
	fields, err := p.Fields()
	if err != nil {
		return "invalid policy: " + err.Error()
	}

	if len(fields) == 0 {
		return "no policy fields"
	}

	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = f.Name() + "=" + string(f.Value)
	}

	return strings.Join(out, ", ")
}

// labelsError: an owner's labels cannot be applied.
type labelsError struct{ msg string }

func (e *labelsError) Error() string { return e.msg }

// disagreeError: owners of a path ask for different policy fields.
type disagreeError struct{ msg string }

func (e *disagreeError) Error() string { return e.msg }
