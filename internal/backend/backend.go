// Package backend is the watcher's view of kopia server: the Backend
// interface the reconciler works through, the types it speaks in, and the
// policy merge that every implementation shares.
//
// The REST implementation over internal/kopiaclient goes into
// internal/backend/rest; backendtest.Fake is an in-memory one for unit
// tests. Both plan with NewPlan, which merges with Merge, so they decide
// alike what to write.
//
// # Policies
//
// A path's policy is shared: the watcher owns only the fields its labels
// set (PolicyPatch) and the pause; everything else belongs to whoever else
// edits the policy (the web UI, the kopia CLI, Terraform) and is kept
// verbatim, including fields this package does not know. kopia server
// replaces a policy whole on every write, so the watcher reads the policy,
// merges its fields into the raw JSON and writes the result back, and only
// when something changes: every policy write makes kopia server refresh
// all its sources under an exclusive lock.
//
// Kopia drops policy labels, so nothing in a policy tells which fields came
// from the watcher. What the watcher applied is kept outside Kopia, in
// Applied, which internal/state stores per source: the last applied patch,
// so that a field whose label was emptied or removed is taken out of the
// policy again; the scheduling section a pause replaced, to put back when
// an owner starts again; a marker for an apply that was not confirmed; and
// a fingerprint of the policy kopia server stored.
//
// # Pause
//
// A source is paused when none of its owners runs, and only then: the pause
// follows Desired.Paused, whatever the previous state, so a pause left by
// the Python version or set in the web UI on the path of a running
// container is lifted. kopia server accepts manual only on its own, so the
// pause replaces the whole scheduling section with {"manual": true}; Merge
// keeps the replaced section in Applied.SavedScheduling and puts it back,
// with the labels' fields on top, when the source is unpaused.
//
// # Reconciling a source
//
// One handler per source reconciles it; handlers of different sources run
// at the same time. Plan first, then apply:
//
//	rec, _ := store.Get(src)
//	plan, err := b.PlanSource(ctx, want, rec.Applied)
//
// Before anything that holds a new patch is stored or applied, the
// schedule is checked: when plan.Applied.Patch differs from
// rec.Applied.Patch, or plan.Action is ActionRegister, the reconciler
// calls PreviewSchedule with plan.Unpaused (also while the source stays
// paused, since the new cron goes into the saved section). On an ErrConfig
// error it reports the labels and falls back to the last confirmed patch,
// rec.Applied.Patch, with Replan; a new source is not registered. The same
// fallback serves owners whose labels disagree. When PlanSource itself
// returns ErrConfig (the labels set a field inside one the policy holds as
// a value), the reconciler plans again with rec.Applied.Patch.
//
// With --dry-run the reconciler prints the plan, for example its Action and
// Diff(plan.Current, plan.Policy), and stops: nothing is written to kopia
// server or to the state. Otherwise, when plan.Action is not ActionNone,
// it stores plan.Intent before calling ApplyPlan. Intent holds the saved
// scheduling section of a pause and marks the apply as pending, so a call
// that fails after kopia server stored the policy (a timeout, a canceled
// context) loses nothing: the next plan writes the policy again. If the
// store refuses the intent, the apply is skipped; if it only fails to
// write the file (state.ErrNotPersisted), the intent is kept in memory and
// the apply goes ahead. After ApplyPlan succeeds the reconciler stores the
// Applied it returns; if only writing the file fails then, nothing has to
// be applied again. kopia server checks more when it stores a policy than
// PreviewSchedule does, so ApplyPlan can still fail with ErrConfig; then
// the reconciler falls back as above, with Replan(plan, want with
// rec.Applied.Patch, plan.Intent), and applies that. With ActionNone it
// stores plan.Applied when it differs from rec.Applied (new labels of a
// paused source, a new fingerprint).
//
// # Errors
//
// Every error a Backend returns is an *Error. errors.Is with ErrTemporary,
// ErrUnavailable and ErrConfig tells what to do; see their documentation.
// A complete source that is not kopia server's own is ErrWrongIdentity,
// an incomplete one ErrIncompleteSource.
//
// When the caller's context is done (ctx.Err() is not nil after the call),
// the error is of neither kind and wraps ctx.Err(), whatever the request
// failed with: the caller stopped waiting, kopia server did not fail.
// Implementations check ctx.Err() for this, not errors.Is(err,
// context.DeadlineExceeded): the HTTP client's own timeout matches that
// too, and it is ErrUnavailable.
//
// The REST backend maps internal/kopiaclient's errors to the kinds like
// this:
//
//   - ErrConfig: ErrInvalidPolicy (a 500 with "failed to validate policy"
//     or "invalid policy path"), ErrMalformedRequest and a 413 (the policy
//     is larger than kopia server reads) from a policy write, POST
//     /api/v1/sources or policy/resolve, except from the PUT after the
//     POST of a registration, where they are ErrTemporary (the POST
//     stored the policy); a schedulingError from
//     policy/resolve; ErrPathNotFound from POST /api/v1/sources, and an
//     ErrInternal of that call with code INTERNAL whose message lacks
//     "unable to set initial policy": kopia server could not stat the
//     path.
//   - ErrUnavailable (also ErrTemporary): ErrUnavailable (408, 429, 502,
//     503, 504, STORAGE_CONNECTION), ErrNotConnected, and transport errors
//     kopiaclient.IsRetryable accepts.
//   - ErrTemporary: any other ErrInternal, the 200 null of GET
//     /api/v1/policy included, except one of a read-only repository
//     ("storage is read-only", or "repository is not writable");
//     ErrNotFound from sources/upload for a source kopia server has not
//     picked up yet.
//   - neither: ErrUnauthorized, ErrCSRF, ErrAccessDenied, ErrUIDisabled,
//     ErrUnexpectedResponse, ErrInvalidArgument, TLS verification errors,
//     a read-only repository, and the caller's context being done.
package backend

import (
	"context"
	"encoding/json"
	"time"
)

// Backend is what the reconciler needs from kopia server. Implementations
// are safe for concurrent use. The reconciler calls them for different
// sources at once; for one source it does not overlap PlanSource,
// ApplyPlan, PreviewSchedule and DeletePolicy, but WaitSnapshot may run
// alongside them.
//
// Every method that takes a Source requires a complete one
// (ErrIncompleteSource) with an absolute path (an error of neither kind
// otherwise), of kopia server's own identity (ErrWrongIdentity).
type Backend interface {
	// ServerIdentity returns the user name and host kopia server snapshots
	// its own sources as (GET /api/v1/sources: localUsername and
	// localHost). Every call asks kopia server. The reconciler caches the
	// result and asks again after ErrWrongIdentity.
	ServerIdentity(ctx context.Context) (userName, host string, err error)

	// ResolvePath normalizes a path the way kopia server does
	// (paths/resolve): ~ is expanded, a relative path is resolved against
	// the server's home directory, then the path is cleaned. Existence is
	// not checked. The source carries kopia server's identity. An absolute
	// path is only cleaned, so the reconciler can build sources from
	// ServerIdentity and path.Clean; ResolvePath is for checking label
	// paths. An empty path is an error of neither kind.
	ResolvePath(ctx context.Context, path string) (Source, error)

	// ListSources returns the sources kopia server knows under its own
	// identity, sorted by path.
	ListSources(ctx context.Context) ([]SourceStatus, error)

	// PlanSource works out what ApplyPlan has to do to bring want.Source to
	// the desired state, from what kopia server has now: whether it knows
	// the source, and the policy defined for the path. last is what the
	// watcher applied before, from the state. It changes nothing; the
	// decision is NewPlan's.
	PlanSource(ctx context.Context, want Desired, last Applied) (Plan, error)

	// ApplyPlan carries out a plan made by PlanSource or Replan and
	// returns what to store in the state: plan.Applied, with StoredHash
	// set to the Fingerprint of the policy as kopia server stored it, read
	// back after the write, or empty if reading it back failed (the write
	// still succeeded) or showed that another client wrote the policy in
	// between. ActionNone sends nothing and returns plan.Applied
	// as it is. ActionRegister and ActionUpdate are described with the
	// actions. A policy written after the plan was made, say in the web UI,
	// is overwritten: kopia server has no conditional write.
	ApplyPlan(ctx context.Context, plan Plan) (Applied, error)

	// PreviewSchedule asks kopia server when the source would be
	// snapshotted next if policy were its own policy (policy/resolve with
	// numUpcomingSnapshotTimes n). policy is a complete policy, such as
	// Plan.Unpaused, not a patch: kopia server replaces the path's policy
	// with it for the calculation. Times count from now in the server's
	// time zone. An invalid schedule is an ErrConfig error.
	PreviewSchedule(ctx context.Context, src Source, policy json.RawMessage, n int) (SchedulePreview, error)

	// DeletePolicy removes the policy of the path. A source with snapshots
	// stays and inherits the host or global policy, so this is only for a
	// source without snapshots. Deleting a missing policy succeeds.
	DeletePolicy(ctx context.Context, src Source) error

	// The snapshot methods below are provisional: the snapshot lifecycle
	// implements them and may still change them, and nothing calls them
	// before that. It will also add a listing of a source's snapshots and
	// a way to wait for a run that is already pending without queuing
	// another one. Until then an implementation may return
	// ErrNotImplemented.

	// TriggerSnapshot queues a snapshot of src (sources/upload) and returns
	// the ticket WaitSnapshot follows it with. kopia server returns no task
	// ID: the ticket records what is needed to find the task later.
	TriggerSnapshot(ctx context.Context, src Source) (SnapshotTicket, error)

	// WaitSnapshot waits until the snapshot of the ticket has finished and
	// returns how it ended. It may take hours and runs outside the
	// handler of the source. Whenever it learns something that must survive
	// a restart of the watcher, such as the task of the snapshot, it calls
	// save with the updated ticket, so the caller can store it and wait
	// with that ticket after a restart. It returns ctx's error when ctx is
	// done.
	WaitSnapshot(ctx context.Context, ticket SnapshotTicket, save func(SnapshotTicket)) (SnapshotResult, error)

	// AnnotateSnapshot sets the description of a snapshot of src and adds
	// pins to it, in one edit. The snapshot is the one that started at
	// startTime and is not a checkpoint; of several, the one that ended
	// first. Snapshots are found by start time because an edit gives a
	// snapshot a new ID. No such snapshot is ErrSnapshotNotFound.
	AnnotateSnapshot(ctx context.Context, src Source, startTime time.Time, description string, addPins []string) error

	// ListSnapshots returns the snapshots of src, checkpoints and
	// incomplete ones included, sorted by start time (GET
	// /api/v1/snapshots with all=1, so that snapshots of identical
	// content are listed one by one).
	ListSnapshots(ctx context.Context, src Source) ([]SnapshotInfo, error)
}

// SchedulePreview is the answer of PreviewSchedule.
type SchedulePreview struct {
	// Times are the next snapshot times, at most n; none for a paused
	// policy.
	Times []time.Time
	// Scheduling is the effective scheduling section, with the values the
	// path inherits from parent, host and global policies.
	Scheduling json.RawMessage
}

// Status is the state of a source as kopia server reports it.
type Status string

// Source states.
const (
	StatusInitializing Status = "INITIALIZING"
	StatusIdle         Status = "IDLE"
	// StatusPending: a snapshot waits for a free upload slot; there is no
	// task yet.
	StatusPending   Status = "PENDING"
	StatusUploading Status = "UPLOADING"
	// StatusPaused: paused through the control API, not by the policy.
	StatusPaused  Status = "PAUSED"
	StatusFailed  Status = "FAILED"
	StatusStopped Status = "STOPPED"
)

// SourceStatus describes a source kopia server knows.
type SourceStatus struct {
	Source Source
	Status Status
	// LastSnapshot is the newest saved snapshot, nil if there is none. It
	// moves only when a snapshot is saved, so it may be a checkpoint of a
	// running upload or a canceled snapshot.
	LastSnapshot *SnapshotInfo
	// NextSnapshotTime is when kopia server's scheduler runs the source
	// next; zero if not scheduled, as with a paused policy.
	NextSnapshotTime time.Time
	// CurrentTask is the ID of the running upload task, only while
	// uploading.
	CurrentTask string
}

// Clone returns a deep copy of s.
func (s SourceStatus) Clone() SourceStatus {
	out := s

	if s.LastSnapshot != nil {
		last := *s.LastSnapshot
		out.LastSnapshot = &last
	}

	return out
}

// SnapshotInfo describes a snapshot manifest. It carries no ID: an edit of
// the description or pins gives a snapshot a new one, so snapshots are
// compared by StartTime.
type SnapshotInfo struct {
	StartTime   time.Time
	EndTime     time.Time
	Description string
	// Incomplete is empty for a complete snapshot, "checkpoint" for an
	// intermediate one of a long upload, otherwise why it is incomplete.
	Incomplete string
}

// SnapshotTicket identifies a snapshot TriggerSnapshot queued. It is plain
// data, stored in STATE_FILE while the snapshot runs.
type SnapshotTicket struct {
	Source Source `json:"source"`
	// TriggeredAt is when the snapshot was queued.
	TriggeredAt time.Time `json:"triggeredAt"`
	// LastSnapshotStart is the start of the source's last snapshot before
	// the trigger; zero if it had none.
	LastSnapshotStart time.Time `json:"lastSnapshotStart,omitzero"`
	// TasksBefore are the IDs of the snapshot tasks kopia server listed
	// before the trigger. Task IDs restart with kopia server, so a task
	// counts as the ticket's only if its description matches as well.
	TasksBefore []string `json:"tasksBefore,omitempty"`
	// TaskID and TaskDescription identify the task once WaitSnapshot has
	// found it.
	TaskID          string `json:"taskID,omitempty"`
	TaskDescription string `json:"taskDescription,omitempty"`
	// LastTaskID is the newest task kopia server listed before the
	// trigger, of any kind, and LastTaskStart its start; "0" and zero if
	// it listed none. kopia server numbers its tasks up from 1, and anew
	// when it restarts and loses queued runs, so tasks numbered at or
	// below it, or another task under its ID, tell WaitSnapshot that
	// kopia server restarted. Empty in a ticket made before the field.
	LastTaskID    string    `json:"lastTaskID,omitempty"`
	LastTaskStart time.Time `json:"lastTaskStart,omitzero"`
}

// SnapshotOutcome is how a snapshot ended.
type SnapshotOutcome string

// Snapshot outcomes.
const (
	// SnapshotSucceeded: a complete snapshot without errors was saved.
	SnapshotSucceeded SnapshotOutcome = "succeeded"
	// SnapshotIdentical: the task succeeded but saved no snapshot, because
	// nothing changed since the last one and the policy ignores identical
	// snapshots.
	SnapshotIdentical SnapshotOutcome = "identical"
	// SnapshotIncomplete: a snapshot was saved but is incomplete or has
	// errors.
	SnapshotIncomplete SnapshotOutcome = "incomplete"
	SnapshotFailed     SnapshotOutcome = "failed"
	// SnapshotCanceled: the upload was canceled; what it had read may
	// have been saved as an incomplete snapshot.
	SnapshotCanceled SnapshotOutcome = "canceled"
	// SnapshotUnknown: how the run ended cannot be told, for example
	// because kopia server restarted and lost it, its task is gone, or
	// the snapshot it saved has been deleted; the reconciler finds out on
	// its next pass.
	SnapshotUnknown SnapshotOutcome = "unknown"
)

// SnapshotResult is how the snapshot of a ticket ended.
type SnapshotResult struct {
	Outcome SnapshotOutcome
	TaskID  string
	// Snapshot is the snapshot the task saved, for SnapshotSucceeded and
	// SnapshotIncomplete, and for SnapshotCanceled if the canceled upload
	// saved one; nil otherwise.
	Snapshot *SnapshotInfo
	// Error tells why the outcome is not SnapshotSucceeded or
	// SnapshotIdentical, such as the task's error message for a failed
	// snapshot; empty for those two.
	Error string
}
