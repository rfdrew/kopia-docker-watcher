package reconcile

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// Who takes the scheduled snapshots of a source, as SourceReport.Schedule
// has it.
const (
	ScheduleKopia   = "kopia"
	ScheduleWatcher = "watcher"
)

// SourceReport is what the watcher knows of one source path, for the list
// command.
type SourceReport struct {
	Path string `json:"path"`
	// Source is user@host:path; empty while the path has no source yet.
	Source string `json:"source,omitempty"`
	// Owners are the containers that back the path up, stopped ones
	// included.
	Owners []string `json:"owners"`
	// Paused reports that the source is paused, as no owner runs.
	Paused bool `json:"paused"`
	// Schedule says who takes the scheduled snapshots: ScheduleKopia, or
	// ScheduleWatcher for a source with hooks or downtime; empty when not
	// known yet. NextSnapshot is when the next is due; zero if none is.
	Schedule     string    `json:"schedule,omitempty"`
	NextSnapshot time.Time `json:"nextSnapshot,omitzero"`
	// LastSuccess is the start of the last complete snapshot; zero if none
	// is known.
	LastSuccess time.Time `json:"lastSuccess,omitzero"`
	// Status is kopia server's status of the source, such as IDLE or
	// UPLOADING; empty if not known.
	Status string `json:"status,omitempty"`
	// HookRun is the phase of the snapshot with hooks under way: pre,
	// down, snapshot or post; empty if none.
	HookRun string `json:"hookRun,omitempty"`
	// Held are the containers held down for the source.
	Held []string `json:"held,omitempty"`
	// Error is the last problem a pass over the path ran into; empty when
	// the last pass had none.
	Error string `json:"error,omitempty"`
}

// SourcesReport is the answer of Sources.
type SourcesReport struct {
	Sources []SourceReport `json:"sources"`
	// Warning tells what could not be found out, as when kopia server did
	// not answer; the rest is what the watcher last saw.
	Warning string `json:"warning,omitempty"`
}

// listSourcesTimeout bounds the question to kopia server of Sources.
const listSourcesTimeout = 10 * time.Second

// pathStatus is what the passes over a path last found.
type pathStatus struct {
	src backend.Source
	// reported is set once a pass reported the source as managed;
	// paused, owners and lastSuccess are what it reported.
	reported    bool
	paused      bool
	owners      []string
	lastSuccess time.Time
	// err is the last pass's error or problems, empty if none.
	err string
}

// noteSource records what a pass reported about the source of path p.
func (r *Reconciler) noteSource(p string, src backend.Source, paused bool, owners []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.statusOf(p)
	st.src, st.reported, st.paused, st.owners = src, true, paused, slices.Clone(owners)
}

// noteLastSuccess records the start of the last complete snapshot of the
// source of path p.
func (r *Reconciler) noteLastSuccess(p string, start time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.statusOf(p).lastSuccess = start
}

// forgetStatus drops what the passes reported about the source of path p,
// but its error.
func (r *Reconciler) forgetStatus(p string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	st, ok := r.status[p]
	switch {
	case !ok:
	case st.err == "":
		delete(r.status, p)
	default:
		r.status[p] = &pathStatus{err: st.err}
	}
}

// noteStatus records the error of the pass over p that ended with res.
func (r *Reconciler) noteStatus(p string, res passResult) {
	if r.dryRun {
		return
	}

	var msgs []string

	if res.err != nil {
		msgs = append(msgs, res.err.Error())
	}

	for _, e := range res.problems {
		msgs = append(msgs, e.Error())
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	switch st, ok := r.status[p]; {
	case len(msgs) > 0:
		r.statusOf(p).err = strings.Join(msgs, "; ")
	case !ok:
	case !st.reported:
		delete(r.status, p)
	default:
		st.err = ""
	}
}

// statusOf returns the status of p, added if missing. r.mu is held.
func (r *Reconciler) statusOf(p string) *pathStatus {
	st, ok := r.status[p]
	if !ok {
		st = &pathStatus{}
		r.status[p] = st
	}

	return st
}

// Sources reports the sources the watcher manages, and the paths whose
// last pass had a problem: what the passes last found, the state, and
// what kopia server says now. When kopia server does not answer, the rest
// is reported with a warning.
func (r *Reconciler) Sources(ctx context.Context) (SourcesReport, error) {
	if r.dryRun || r.stopping() {
		return SourcesReport{}, ErrNotRunning
	}

	rep := SourcesReport{Sources: []SourceReport{}}

	live := map[string]backend.SourceStatus{}

	// kopia server may hang: the rest is reported all the same.
	lctx, cancel := context.WithTimeout(ctx, listSourcesTimeout)
	statuses, err := r.backend.ListSources(lctx)

	cancel()

	if err != nil {
		if ctx.Err() != nil {
			return SourcesReport{}, ctx.Err()
		}

		rep.Warning = "kopia server did not answer, its status and times are left out: " + err.Error()
	}

	for _, s := range statuses {
		live[s.Source.Path] = s
	}

	r.mu.Lock()
	id := r.identity
	status := make(map[string]pathStatus, len(r.status))

	for p, st := range r.status {
		status[p] = *st
	}
	r.mu.Unlock()

	records := map[string]backend.Source{}

	for _, src := range r.store.Sources() {
		if id.known && (src.UserName != id.userName || src.Host != id.host) || !labels.WithinPrefix(r.watcherPrefix, src.Path) {
			continue
		}

		records[src.Path] = src
	}

	paths := make([]string, 0, len(records)+len(status))

	for p := range records {
		paths = append(paths, p)
	}

	for p := range status {
		if _, ok := records[p]; !ok {
			paths = append(paths, p)
		}
	}

	slices.Sort(paths)

	held := r.store.Held()

	for _, p := range paths {
		rep.Sources = append(rep.Sources, r.sourceReport(p, records, status, live, held))
	}

	return rep, nil
}

// sourceReport puts together what is known of path p.
func (r *Reconciler) sourceReport(p string, records map[string]backend.Source, status map[string]pathStatus,
	live map[string]backend.SourceStatus, held []state.Held,
) SourceReport {
	st, hasStatus := status[p]
	src, hasRecord := records[p]

	if !hasRecord && st.reported {
		src = st.src
	}

	out := SourceReport{Path: p, Owners: []string{}, Error: st.err}
	if src != (backend.Source{}) {
		out.Source = src.String()
	}

	var rec state.Record
	if hasRecord {
		rec, _ = r.store.Get(src)
	}

	switch {
	case hasStatus && st.reported:
		out.Owners = append(out.Owners, st.owners...)
		out.Paused = st.paused
		out.LastSuccess = st.lastSuccess
	case hasRecord:
		for _, o := range rec.Owners {
			if o.Gone.IsZero() {
				out.Owners = append(out.Owners, o.Name)
			}
		}

		out.Paused = rec.SavedScheduling != nil || rec.Schedule != nil && rec.Schedule.Paused
	}

	l, known := live[p]
	if known && (src == (backend.Source{}) || l.Source == src) {
		out.Status = string(l.Status)

		if out.LastSuccess.IsZero() && l.LastSnapshot != nil && l.LastSnapshot.Incomplete == "" {
			out.LastSuccess = l.LastSnapshot.StartTime
		}
	} else {
		known = false
	}

	switch {
	case rec.Schedule != nil:
		out.Schedule = ScheduleWatcher
		if !rec.Schedule.Paused && !out.Paused {
			out.NextSnapshot = rec.Schedule.Next
		}
	case known:
		out.Schedule = ScheduleKopia
		out.NextSnapshot = l.NextSnapshotTime
	}

	if rec.HookRun != nil {
		out.HookRun = rec.HookRun.Phase
	}

	for _, h := range held {
		if h.Source == src && src != (backend.Source{}) {
			out.Held = append(out.Held, h.Name+" ("+h.Mode+")")
		}
	}

	return out
}
