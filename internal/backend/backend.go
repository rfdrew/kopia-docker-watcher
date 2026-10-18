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
// The reconciler handles one source at a time. Plan first, then apply:
//
//	rec, _ := store.Get(src)
//	plan, err := b.PlanSource(ctx, want, rec.Applied)
//
// Before anything that holds a new patch is stored or applied, the
// schedule is checked: when plan.Applied.Patch differs from
// rec.Applied.Patch, or plan.Action is ActionRegister, the reconciler
// calls PreviewSchedule with plan.Unpaused (also while the source stays
// paused, since the new cron goes into the saved section). On an ErrConfig
// error it reports the labels and falls back to the last applied patch
// with Replan, or does not register a new source.
//
// With --dry-run the reconciler prints the plan, for example
// Diff(plan.Current, plan.Policy), and stops: nothing is written to kopia
// server or to the state. Otherwise, when plan.Action is not ActionNone,
// it stores plan.Intent before calling ApplyPlan, and skips the apply if
// that store fails. Intent holds the saved scheduling section of a pause
// and marks the apply as pending, so a call that fails after kopia server
// stored the policy (a timeout, a canceled context) loses nothing: the
// next plan writes the policy again. After ApplyPlan succeeds the
// reconciler stores the Applied it returns; if only writing the state file
// fails then, the store keeps the record in memory and nothing has to be
// applied again. With ActionNone it stores plan.Applied when it differs
// from rec.Applied (new labels of a paused source, a new fingerprint).
//
// # Errors
//
// Every error a Backend returns is an *Error. errors.Is with ErrTemporary,
// ErrUnavailable and ErrConfig tells what to do; see their documentation.
// A source that is not kopia server's own is ErrWrongIdentity.
//
// Implementations map kopia server's answers to the kinds like this:
//
//   - ErrConfig: a policy write or policy/resolve rejected as invalid (500
//     with "failed to validate policy" or "invalid policy path",
//     kopiaclient.ErrInvalidPolicy) or as malformed (400
//     MALFORMED_REQUEST); a schedulingError from policy/resolve;
//     PATH_NOT_FOUND from POST /api/v1/sources, and any other 500 of that
//     call except a failed policy write ("unable to set initial policy"):
//     kopia server could not stat the path.
//   - ErrUnavailable (also ErrTemporary): transport errors and timeouts,
//     502, 503, 504, NOT_CONNECTED, STORAGE_CONNECTION.
//   - ErrTemporary: any other 5xx, except "repository is not writable";
//     200 null from GET /api/v1/policy; 404 from sources/upload for a
//     source kopia server has not picked up yet.
//   - neither: wrong credentials, CSRF, access denied, TLS verification,
//     a server without the UI API, a read-only repository, an answer that
//     does not decode, an argument rejected before sending, a canceled
//     context.
package backend

import (
	"context"
	"encoding/json"
	"time"
)

// Backend is what the reconciler needs from kopia server. Implementations
// are safe for concurrent use; the reconciler calls them for different
// sources at once, one call at a time per source.
//
// Every method that takes a Source requires a complete one of kopia
// server's own identity and fails with ErrWrongIdentity otherwise.
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
	// paths.
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
	// still succeeded). ActionNone sends nothing and returns plan.Applied
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
	SnapshotCanceled   SnapshotOutcome = "canceled"
	// SnapshotUnknown: the task is gone, for example kopia server
	// restarted; the reconciler finds out on its next pass.
	SnapshotUnknown SnapshotOutcome = "unknown"
)

// SnapshotResult is how the snapshot of a ticket ended.
type SnapshotResult struct {
	Outcome SnapshotOutcome
	TaskID  string
	// Snapshot is the snapshot the task saved; nil if it saved none.
	Snapshot *SnapshotInfo
	// Error is the task's error message for a failed snapshot.
	Error string
}
