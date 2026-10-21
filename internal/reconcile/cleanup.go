package reconcile

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

const (
	// ownerLinger is how long an owner that stopped backing a source up
	// still counts for the source's cleanup mode, counted from the last
	// pass that saw the source with an owner. docker compose down removes
	// the containers of a project one by one, so without it the mode would
	// depend on the order of the removals: the last container to go, alone,
	// would decide.
	ownerLinger = 10 * time.Minute

	// composeProjectLabel names the docker compose project of a container.
	composeProjectLabel = "com.docker.compose.project"
)

// ownerRecords returns the owners of the pass as the state remembers them.
func (ps *pass) ownerRecords() []state.Owner {
	out := make([]state.Owner, 0, len(ps.owners))

	for _, o := range ps.owners {
		out = append(out, state.Owner{
			ID:             o.id,
			Name:           o.name,
			ComposeProject: o.labels[composeProjectLabel],
			LabelsHash:     labelsHash(ps.labelPrefix, o.labels),
			Cleanup:        string(o.cleanup),
			DeletePinned:   o.deletePinned,
		})
	}

	return out
}

// labelsHash returns the fingerprint of the labels under the prefix.
func labelsHash(prefix string, l map[string]string) string {
	h := sha256.New()

	for _, k := range slices.Sorted(maps.Keys(l)) {
		if strings.HasPrefix(k, prefix+".") {
			_, _ = fmt.Fprintf(h, "%q=%q\n", k, l[k])
		}
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// mergeOwners returns the owners to record for a source whose owners are
// cur at now, given those recorded before, prev, and since when the source
// has been without an owner, orphaned (zero if it had one).
//
// An owner of prev that is not in cur is marked gone at now. Gone owners
// are dropped ownerLinger after they left, but only by a pass that sees
// the source with an owner: a departure recorded late, after kopia server
// or the watcher was down, is never measured against an earlier one, so it
// cannot make the mode less cautious.
//
// The source counts as without an owner only once no container on record
// is listed any more, by its ID or its name (listed reports one that is):
// Docker never changes the labels of a container, so one that no longer
// names the path lost it because the watcher's configuration changed, and
// a container recreated under the same name, as by docker compose up, is
// the same service. CLEANUP_GRACE does not run meanwhile.
func mergeOwners(prev, cur []state.Owner, now, orphaned time.Time, listed func(state.Owner) bool) ([]state.Owner, time.Time) {
	out := slices.Clone(cur)

	for _, p := range prev {
		if slices.ContainsFunc(cur, func(c state.Owner) bool { return c.ID == p.ID }) {
			continue
		}

		if p.Gone.IsZero() {
			p.Gone = now
		}

		out = append(out, p)
	}

	if len(cur) > 0 {
		out = slices.DeleteFunc(out, func(o state.Owner) bool { return !o.Gone.IsZero() && o.Gone.Before(now.Add(-ownerLinger)) })
	}

	slices.SortFunc(out, func(a, b state.Owner) int {
		return cmp.Or(strings.Compare(a.Name, b.Name), strings.Compare(a.ID, b.ID))
	})

	switch {
	case len(cur) > 0, listed != nil && slices.ContainsFunc(out, listed):
		orphaned = time.Time{}
	case orphaned.IsZero():
		orphaned = now
	}

	if len(out) == 0 {
		out = nil
	}

	return out, orphaned
}

// cleanupOf returns the most cautious cleanup mode of the owners, and
// whether every one of them lets an all cleanup delete pinned snapshots. A
// mode the watcher does not know, as one a newer watcher wrote, is pause;
// so is the mode of a source without owners on record.
func cleanupOf(owners []state.Owner) (labels.CleanupMode, bool) {
	if len(owners) == 0 {
		return labels.CleanupPause, false
	}

	mode, pinned := labels.CleanupAll, true

	for _, o := range owners {
		if m := labels.CleanupMode(o.Cleanup); cleanupRank(m) < cleanupRank(mode) {
			mode = m
		}

		pinned = pinned && o.DeletePinned
	}

	if cleanupRank(mode) == 0 {
		mode = labels.CleanupPause
	}

	return mode, pinned && mode == labels.CleanupAll
}

// cleanupRank orders the cleanup modes from the most cautious, pause and
// any mode the watcher does not know.
func cleanupRank(m labels.CleanupMode) int {
	switch m {
	case labels.CleanupPolicy:
		return 1
	case labels.CleanupAll:
		return 2
	default:
		return 0
	}
}

// ownership is what a record says about the owners of its source.
type ownership struct {
	owners   []state.Owner
	orphaned time.Time
}

// takeOwners records the owners of the pass in r, the record of its source,
// and since when the source has had none, and reports what r held before
// and whether that changed. The passes call it in every change of the
// record they make, so that the owners take no write of their own; it is
// idempotent within a pass.
func (ps *pass) takeOwners(r *state.Record) (ownership, bool) {
	before := ownership{owners: r.Owners, orphaned: r.OrphanedSince}

	owners, orphaned := mergeOwners(r.Owners, ps.ownerRecords(), ps.started, r.OrphanedSince, ps.listed)
	if slices.EqualFunc(owners, r.Owners, state.Owner.Equal) && orphaned.Equal(r.OrphanedSince) {
		return before, false
	}

	r.Owners, r.OrphanedSince, r.OwnersUpdated = owners, orphaned, ps.started

	return before, true
}

// trackOwners records the owners of the pass in the record of its source,
// if it has one, unless an earlier change of the pass did. It writes only
// what changed, and nothing in a dry run.
func (ps *pass) trackOwners() {
	if ps.dryRun || ps.ownersTaken {
		return
	}

	owners, orphaned := mergeOwners(ps.owned.owners, ps.ownerRecords(), ps.started, ps.owned.orphaned, ps.listed)
	if slices.EqualFunc(owners, ps.owned.owners, state.Owner.Equal) && orphaned.Equal(ps.owned.orphaned) {
		ps.ownersTaken = true
		return
	}

	if _, ok := ps.store.Get(ps.src); !ok {
		return
	}

	ps.ownersTaken = true

	var (
		before  ownership
		changed bool
	)

	ps.updateRecord(ps.src, func(r *state.Record) bool {
		before, changed = ps.takeOwners(r)

		return changed
	})

	if changed {
		ps.logOwners(before)
	}
}

// logOwners logs a source whose owners ask for a cleanup when it loses its
// last owner, with when the cleanup is due, and when it gets one again
// before, from what its record held before the pass recorded its owners.
func (ps *pass) logOwners(before ownership) {
	rec, _ := ps.store.Get(ps.src)
	mode, _ := cleanupOf(rec.Owners)

	switch {
	case before.orphaned.IsZero() && !rec.OrphanedSince.IsZero() && mode != labels.CleanupPause:
		ps.log.Warn("no container backs the source up any more; it is paused, and cleaned up once CLEANUP_GRACE has "+
			"passed unless a container backs it up again", "source", ps.src.String(), "cleanup", string(mode),
			"cleanup_due", rec.OrphanedSince.Add(ps.cleanupGrace).Format(timeFormat), "last_owners", recordNames(rec.Owners))
	case !before.orphaned.IsZero() && rec.OrphanedSince.IsZero() && len(ps.owners) > 0:
		if m, _ := cleanupOf(before.owners); m != labels.CleanupPause {
			ps.log.Info("a container backs the abandoned source up again; its cleanup is called off", "source", ps.src.String(),
				"cleanup", string(m), "owners", ownerNames(ps.owners))
		}
	}
}

// forgetOwners drops what the record of the source of a path outside
// WATCHER_PATH_PREFIX says about its owners. Passes do not follow such a
// source, so that would go stale: were the prefix widened again, an owner
// replaced meanwhile by one that asks for less could decide the cleanup.
// Without owners on record the source is only paused, until a container
// backs it up again. Only the state changes; nothing in a dry run.
func (ps *pass) forgetOwners() {
	if ps.dryRun {
		return
	}

	src, err := ps.source(ps.ctx, ps.path)
	if err != nil {
		return
	}

	ps.updateRecord(src, func(r *state.Record) bool {
		if r.Owners == nil && r.OrphanedSince.IsZero() {
			return false
		}

		r.Owners, r.OrphanedSince, r.OwnersUpdated = nil, time.Time{}, ps.started

		return true
	})
}

// recordNames returns the names of recorded owners, each once.
func recordNames(owners []state.Owner) []string {
	var out []string

	for _, o := range owners {
		if !slices.Contains(out, o.Name) {
			out = append(out, o.Name)
		}
	}

	return out
}

// cleanupPlan is where the cleanup of a source without owners stands.
type cleanupPlan struct {
	mode labels.CleanupMode
	// pinned lets the cleanup delete pinned snapshots.
	pinned bool
	// since is when the source lost its last owner; due is when the
	// cleanup may go ahead.
	since, due time.Time
	// wait says why the cleanup cannot go ahead yet; empty when it can.
	wait string
}

// planCleanup works out where the cleanup of the source of the pass stands
// by its record. The cleanup waits for CLEANUP_GRACE, for the snapshots the
// watcher queued, such as the one at the stop, for a snapshot that is
// pending or uploading, and as long as a container it remembers as an owner
// still exists: such a container no longer names the path only because the
// watcher's configuration changed, say LABEL_PREFIX or HOST_PREFIX, and its
// source is not abandoned.
func (ps *pass) planCleanup(rec state.Record) cleanupPlan {
	cp := cleanupPlan{since: rec.OrphanedSince}
	cp.mode, cp.pinned = cleanupOf(rec.Owners)

	if cp.since.IsZero() {
		// Not on record: in a dry run, which records nothing, before the
		// pass recorded its owners, or while a container on record still
		// exists, which the switch below tells.
		cp.since = ps.started
	}

	cp.due = cp.since.Add(ps.cleanupGrace)

	var status backend.Status
	if ps.plan != nil && ps.plan.Status != nil {
		status = ps.plan.Status.Status
	}

	switch existing := ps.existingOwners(rec.Owners); {
	case cp.mode == labels.CleanupPause:
	case !labels.WithinPrefix(ps.watcherPrefix, ps.src.Path):
		cp.wait = "the source is outside WATCHER_PATH_PREFIX " + ps.watcherPrefix
	case len(existing) > 0:
		cp.wait = "the containers that backed it up still exist, though their labels no longer name the path " +
			"(did LABEL_PREFIX, HOST_PREFIX or WATCHER_PATH_PREFIX change?): " + strings.Join(existing, ", ")
	case ps.started.Before(cp.due):
		cp.wait = "CLEANUP_GRACE has not passed"
	case len(rec.SnapshotWaits) > 0 || !rec.StopAbsorbedAt.IsZero():
		cp.wait = "a snapshot the watcher queued, such as the one at the stop, has not finished"
	case status == backend.StatusPending || status == backend.StatusUploading:
		cp.wait = "a snapshot is " + strings.ToLower(string(status))
	}

	return cp
}

// existingOwners returns the names of the owners on record whose container
// is in the listing of the pass.
func (ps *pass) existingOwners(owners []state.Owner) []string {
	var out []string

	for _, o := range owners {
		if ps.listed(o) && !slices.Contains(out, o.Name) {
			out = append(out, o.Name)
		}
	}

	return out
}

// listed reports whether the listing of the pass has the container of an
// owner on record, by its ID or by its name: Docker names are unique, and
// a container recreated under the name of an owner, as by docker compose up
// or watchtower, takes its place.
func (ps *pass) listed(o state.Owner) bool {
	return slices.ContainsFunc(ps.containers, func(c docker.Container) bool { return c.ID == o.ID || o.Name != "" && c.Name == o.Name })
}

// cleanup carries out the cleanup-on-remove of a source kopia server knows
// that has no owner left, after the pass paused it and acted on its
// snapshots, once planCleanup lets it go ahead. policy deletes the policy
// of a source without snapshots, and the next pass, soon after, forgets
// the source once kopia server no longer lists it, or pauses it again if
// a snapshot came meanwhile. all deletes the snapshots, the pinned ones
// only with leave to, and forgets a source that went as a whole; one with
// pinned snapshots stays, paused. Every deletion is logged.
func (ps *pass) cleanup() error {
	if len(ps.owners) > 0 || ps.plan == nil || !ps.plan.Known {
		return nil
	}

	rec, ok := ps.store.Get(ps.src)
	if !ok {
		return nil
	}

	cp := ps.planCleanup(rec)

	switch {
	case cp.mode == labels.CleanupPause:
		return nil
	case cp.wait != "":
		if strings.HasPrefix(cp.wait, "the containers") {
			ps.warnOnce("cleanup containers "+ps.src.String(), "not cleaning up the source", "source", ps.src.String(),
				"cleanup", string(cp.mode), "reason", cp.wait)
		}

		ps.log.Debug("the cleanup of the source waits", "source", ps.src.String(), "cleanup", string(cp.mode),
			"reason", cp.wait, "due", cp.due.Format(timeFormat))

		return nil
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)
	if errors.Is(err, backend.ErrNotImplemented) {
		ps.warnOnce("cleanup ListSnapshots", "the backend cannot list snapshots yet; abandoned sources are only paused")
		return nil
	}

	if err != nil {
		return err
	}

	if cp.mode == labels.CleanupPolicy {
		return ps.cleanupPolicy(snaps)
	}

	return ps.cleanupAll(cp, snaps)
}

// cleanupPolicy deletes the policy of the source if it has no snapshots.
func (ps *pass) cleanupPolicy(snaps []backend.SnapshotInfo) error {
	attrs := []any{"source", ps.src.String(), "cleanup", string(labels.CleanupPolicy)}

	if len(snaps) > 0 {
		if ps.once("cleanup kept policy " + ps.src.String()) {
			ps.log.Info("keeping the paused policy of the abandoned source: it has snapshots, and without its policy it "+
				"would inherit the schedule of the host or global policy", append(attrs, "snapshots", len(snaps))...)
		}

		return nil
	}

	if err := ps.checkPrefix(); err != nil {
		return err
	}

	if err := ps.backend.DeletePolicy(ps.ctx, ps.src); err != nil {
		ps.log.Warn("cleanup: deleting the policy of the abandoned source failed; kopia server may have deleted it all the same",
			append(attrs, "error", err.Error())...)

		return err
	}

	ps.log.Info("cleanup: deleted the policy of the abandoned source", append(attrs, "snapshots", 0)...)
	ps.recheckSoon = true

	return nil
}

// cleanupAll deletes the snapshots of the source, and the source itself
// unless pinned snapshots stay.
func (ps *pass) cleanupAll(cp cleanupPlan, snaps []backend.SnapshotInfo) error {
	attrs := []any{"source", ps.src.String(), "cleanup", string(labels.CleanupAll)}
	pinned := 0

	for _, s := range snaps {
		if s.Pinned() {
			pinned++
		}
	}

	if !cp.pinned && pinned > 0 && pinned == len(snaps) {
		if ps.once("cleanup kept pinned " + ps.src.String()) {
			ps.log.Info("keeping the pinned snapshots of the abandoned source and its paused policy; "+
				labels.Name(ps.labelPrefix, labels.KeyCleanupDeletePinned)+"=true lets them go", append(attrs, "pinned", pinned)...)
		}

		return nil
	}

	if err := ps.checkPrefix(); err != nil {
		return err
	}

	res, err := ps.backend.DeleteSnapshots(ps.ctx, ps.src, cp.pinned)
	if errors.Is(err, backend.ErrNotImplemented) {
		ps.warnOnce("cleanup DeleteSnapshots", "the backend cannot delete snapshots yet; abandoned sources are only paused")
		return nil
	}

	if err != nil {
		planned := len(snaps)
		if !cp.pinned {
			planned -= pinned
		}

		ps.log.Warn("cleanup: deleting the snapshots of the abandoned source failed; kopia server may have deleted them all the same",
			append(attrs, "snapshots", planned, "deleted", res.Deleted, "error", err.Error())...)

		return err
	}

	switch {
	case res.Source:
		if cp.pinned {
			attrs = append(attrs, "pinned_deleted", pinned)
		}

		ps.log.Info("cleanup: deleted the abandoned source with its snapshots and its policy", append(attrs, "snapshots", res.Deleted)...)

		return storeErr(ps.store.Delete(ps.src))
	case res.Deleted > 0:
		ps.log.Info("cleanup: deleted the snapshots of the abandoned source that have no pin; the pinned ones and its paused "+
			"policy stay", append(attrs, "snapshots", res.Deleted, "pinned", res.Kept)...)
	}

	return nil
}

// checkPrefix refuses to delete anything of a source outside
// WATCHER_PATH_PREFIX. Passes leave such a source alone from the start;
// this is the check right before the delete.
func (ps *pass) checkPrefix() error {
	if labels.WithinPrefix(ps.watcherPrefix, ps.src.Path) {
		return nil
	}

	return fmt.Errorf("refusing to delete anything of %s: it is outside WATCHER_PATH_PREFIX %s", ps.src, ps.watcherPrefix)
}

// forgetAbandoned forgets the record of a source kopia server does not
// know and no container backs up, once its owners asked for a cleanup and
// planCleanup lets it go ahead: the source was cleaned up, or deleted by
// someone else, and there is nothing left to pause. A source whose owners
// asked for the pause keeps its record, as it always did.
func (ps *pass) forgetAbandoned() error {
	if len(ps.owners) > 0 || !ps.had {
		return nil
	}

	rec, ok := ps.store.Get(ps.src)
	if !ok {
		return nil
	}

	if ps.dryRun {
		rec.Owners, rec.OrphanedSince = mergeOwners(rec.Owners, nil, ps.started, rec.OrphanedSince, ps.listed)
	}

	if cp := ps.planCleanup(rec); cp.mode == labels.CleanupPause || cp.wait != "" {
		return nil
	}

	if ps.dryRun {
		ps.cleanupNote = "would forget the source: kopia server no longer knows it"
		return nil
	}

	if err := storeErr(ps.store.Delete(ps.src)); err != nil {
		return err
	}

	mode, _ := cleanupOf(rec.Owners)
	ps.log.Info("forgot the abandoned source: kopia server no longer knows it", "source", ps.src.String(), "cleanup", string(mode))

	return nil
}

// noteCleanup works out for a dry run what the cleanup of a source without
// owners would do, for print and the log. It lists the source's snapshots
// when the cleanup would go ahead, and deletes nothing.
func (ps *pass) noteCleanup() {
	if len(ps.owners) > 0 || !ps.had || ps.plan == nil || !ps.plan.Known {
		return
	}

	rec, _ := ps.store.Get(ps.src)
	rec.Owners, rec.OrphanedSince = mergeOwners(rec.Owners, nil, ps.started, rec.OrphanedSince, ps.listed)
	cp := ps.planCleanup(rec)
	mode := labels.Name(ps.labelPrefix, labels.KeyCleanupOnRemove) + "=" + string(cp.mode)

	switch {
	case cp.mode == labels.CleanupPause:
		ps.cleanupNote = "none, the source stays paused (" + mode + ")"
		return
	case cp.wait == "CLEANUP_GRACE has not passed":
		ps.cleanupNote = fmt.Sprintf("%s at %s, after CLEANUP_GRACE (%s) without a container", mode, cp.due.Format(timeFormat), ps.cleanupGrace)
		return
	case cp.wait != "":
		ps.cleanupNote = mode + " waits: " + cp.wait
		return
	}

	snaps, err := ps.backend.ListSnapshots(ps.ctx, ps.src)
	if err != nil {
		ps.cleanupNote = mode + ": cannot list the snapshots: " + err.Error()
		return
	}

	pinned := 0

	for _, s := range snaps {
		if s.Pinned() {
			pinned++
		}
	}

	var what string

	switch {
	case cp.mode == labels.CleanupPolicy && len(snaps) > 0:
		ps.cleanupNote = fmt.Sprintf("keeps the paused policy: the source has %d snapshots (%s)", len(snaps), mode)
		return
	case cp.mode == labels.CleanupPolicy:
		what = "policy"
		ps.cleanupNote = "would delete the policy: the source has no snapshots (" + mode + ")"
	case !cp.pinned && pinned > 0 && pinned == len(snaps):
		ps.cleanupNote = fmt.Sprintf("nothing to delete: only %d pinned snapshots are left, which stay with the paused policy (%s)", pinned, mode)
		return
	case !cp.pinned && pinned > 0:
		what = "snapshots without a pin"
		ps.cleanupNote = fmt.Sprintf("would delete %d snapshots without a pin; %d pinned ones and the paused policy stay (%s)",
			len(snaps)-pinned, pinned, mode)
	default:
		what = "source and policy"
		ps.cleanupNote = fmt.Sprintf("would delete the source with its %d snapshots and its policy (%s)", len(snaps), mode)
	}

	n := len(snaps)
	if what == "snapshots without a pin" {
		n -= pinned
	}

	ps.log.Info("dry run: would delete", "source", ps.src.String(), "cleanup", string(cp.mode), "what", what, "snapshots", n)
}
