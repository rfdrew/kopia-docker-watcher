// Package restore restores a snapshot of a source into a directory on
// kopia server, with POST /api/v1/restore, and follows kopia server's task
// until it ends.
//
// kopia server writes the files itself, so the target is a path on kopia
// server, and the volume it lies on has to be mounted into kopia server
// writable. By default nothing is overwritten: kopia server refuses to
// restore into a directory that exists and is not empty, and fails at a
// file that exists. Overwrite replaces the files and symlinks it finds,
// and leaves alone those the snapshot does not have.
package restore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// Defaults of the polling of the task.
const (
	DefaultPollInterval = 2 * time.Second
	maxPollDelay        = 30 * time.Second
	// cancelTimeout bounds the request that cancels the task of a restore
	// whose context ended.
	cancelTimeout = 10 * time.Second
	// minPrefixLen is how many characters of a snapshot ID name it at
	// least.
	minPrefixLen = 6
	// Latest names the newest complete snapshot.
	Latest = "latest"
)

// ErrNoSnapshot means that the source has no snapshot to restore, or none
// the request names.
var ErrNoSnapshot = errors.New("no such snapshot")

// Kopia is what a restore needs of kopia server; *kopiaclient.Client
// implements it.
type Kopia interface {
	ListSnapshots(ctx context.Context, src kopiaclient.SourceInfo, all bool) (*kopiaclient.SnapshotsResponse, error)
	Restore(ctx context.Context, req kopiaclient.RestoreRequest) (*kopiaclient.TaskInfo, error)
	GetTask(ctx context.Context, id string) (*kopiaclient.TaskInfo, error)
	CancelTask(ctx context.Context, id string) error
}

// Snapshot is the snapshot a restore restores.
type Snapshot struct {
	ID          string    `json:"id"`
	RootID      string    `json:"rootID"`
	StartTime   time.Time `json:"startTime"`
	Description string    `json:"description,omitempty"`
	// Incomplete is why the snapshot is incomplete; empty if it is not.
	Incomplete string `json:"incomplete,omitempty"`
}

// Select returns the snapshot of src that want names: its manifest ID, or
// a unique start of it of at least six characters. Empty or Latest names
// the newest complete snapshot; incomplete ones, checkpoints of an upload
// and canceled ones, are only taken by their ID.
func Select(ctx context.Context, k Kopia, src kopiaclient.SourceInfo, want string) (Snapshot, error) {
	resp, err := k.ListSnapshots(ctx, src, true)
	if err != nil {
		return Snapshot{}, fmt.Errorf("list the snapshots of %s: %w", src, err)
	}

	snaps := resp.Snapshots
	want = strings.TrimSpace(want)

	if want == "" || want == Latest {
		for i := len(snaps) - 1; i >= 0; i-- {
			if snaps[i].IncompleteReason == "" && snaps[i].RootID != "" {
				return snapshotOf(snaps[i]), nil
			}
		}

		return Snapshot{}, fmt.Errorf("%w: %s has no complete snapshot", ErrNoSnapshot, src)
	}

	var found []kopiaclient.Snapshot

	for _, s := range snaps {
		switch {
		case s.ID == want:
			return checkRoot(s)
		case len(want) >= minPrefixLen && strings.HasPrefix(s.ID, want):
			found = append(found, s)
		}
	}

	switch len(found) {
	case 0:
		return Snapshot{}, fmt.Errorf("%w: %s has no snapshot %q (give its ID, or a start of it of at least %d characters)",
			ErrNoSnapshot, src, want, minPrefixLen)
	case 1:
		return checkRoot(found[0])
	default:
		return Snapshot{}, fmt.Errorf("%w: %q is the start of the IDs of %d snapshots of %s; give more of it",
			ErrNoSnapshot, want, len(found), src)
	}
}

func checkRoot(s kopiaclient.Snapshot) (Snapshot, error) {
	if s.RootID == "" {
		return Snapshot{}, fmt.Errorf("%w: snapshot %s has no root", ErrNoSnapshot, s.ID)
	}

	return snapshotOf(s), nil
}

func snapshotOf(s kopiaclient.Snapshot) Snapshot {
	return Snapshot{ID: s.ID, RootID: s.RootID, StartTime: s.StartTime, Description: s.Description, Incomplete: s.IncompleteReason}
}

// Options say what to restore where.
type Options struct {
	Snapshot Snapshot
	// Target is the directory on kopia server to restore into: an
	// absolute path.
	Target string
	// Overwrite restores into a directory that is not empty, replacing
	// the files and symlinks it finds there.
	Overwrite bool
	// CancelOnStop cancels kopia server's task when ctx ends; otherwise
	// the task goes on and only following it stops.
	CancelOnStop bool
	// Progress is told how the restore goes; nil tells nothing.
	Progress func(Progress)
	// PollInterval is how often the task is looked at; zero means
	// DefaultPollInterval.
	PollInterval time.Duration
}

// Progress is how far a restore has got.
type Progress struct {
	TaskID        string `json:"taskID"`
	Files         int64  `json:"files"`
	Dirs          int64  `json:"dirs"`
	Bytes         int64  `json:"bytes"`
	Skipped       int64  `json:"skipped,omitempty"`
	IgnoredErrors int64  `json:"ignoredErrors,omitempty"`
}

// Result is how a restore ended.
type Result struct {
	Snapshot Snapshot `json:"snapshot"`
	Target   string   `json:"target"`
	Progress
	// Status is the status of kopia server's task: SUCCESS, FAILED or
	// CANCELED; empty when it is not known how the task ended.
	Status kopiaclient.TaskStatus `json:"status,omitempty"`
	// Error says why the restore failed; empty when it succeeded.
	Error string `json:"error,omitempty"`
}

// OK reports whether the restore succeeded.
func (r Result) OK() bool { return r.Status == kopiaclient.TaskSuccess }

// CheckTarget reports a target that is not a clean absolute path, or is
// the root.
func CheckTarget(target string) error {
	switch {
	case target == "":
		return errors.New("no target directory given")
	case !path.IsAbs(target):
		return fmt.Errorf("target %q is not an absolute path on kopia server", target)
	case path.Clean(target) == "/":
		return errors.New("target / is not allowed")
	}

	return nil
}

// Run starts the restore and follows kopia server's task until it ends.
// An error means that the restore could not be started or followed; a
// restore that ran and failed is a Result with Error set. When ctx ends,
// the task is canceled with CancelOnStop, and Run returns what it knows.
func Run(ctx context.Context, k Kopia, opts Options) (Result, error) {
	res := Result{Snapshot: opts.Snapshot, Target: path.Clean(opts.Target)}

	if err := CheckTarget(opts.Target); err != nil {
		return res, err
	}

	task, err := k.Restore(ctx, kopiaclient.RestoreRequest{
		Root: opts.Snapshot.RootID,
		Filesystem: &kopiaclient.FilesystemOutput{
			TargetPath:           res.Target,
			OverwriteDirectories: opts.Overwrite,
			OverwriteFiles:       opts.Overwrite,
			OverwriteSymlinks:    opts.Overwrite,
			WriteFilesAtomically: true,
		},
	})
	if err != nil {
		return res, fmt.Errorf("start the restore: %w", err)
	}

	res.TaskID = task.ID

	return follow(ctx, k, opts, res, task)
}

// follow polls the task until it ends.
func follow(ctx context.Context, k Kopia, opts Options, res Result, task *kopiaclient.TaskInfo) (Result, error) {
	interval := opts.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	var last Progress

	delay := interval

	for {
		res.Progress = progressOf(task)

		if opts.Progress != nil && res.Progress != last {
			opts.Progress(res.Progress)
			last = res.Progress
		}

		if task.Status.IsFinished() {
			res.Status = task.Status
			res.Error = taskError(task, opts.Overwrite)

			return res, nil
		}

		t := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			t.Stop()

			if opts.CancelOnStop {
				cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
				err := k.CancelTask(cctx, task.ID)

				cancel()

				if err != nil {
					return res, fmt.Errorf("cancel kopia server's restore task %s: %w", task.ID, err)
				}

				res.Error = "canceled: the watcher stops"
			}

			return res, ctx.Err()
		case <-t.C:
		}

		next, err := k.GetTask(ctx, task.ID)

		switch {
		case err == nil:
			task, delay = next, interval
		case ctx.Err() != nil:
			// The select above handles it.
		case errors.Is(err, kopiaclient.ErrNotFound):
			res.Error = "kopia server no longer knows the restore task " + task.ID +
				", as after a restart: whether the restore finished is not known"

			return res, nil
		case kopiaclient.IsRetryable(err):
			delay = min(2*delay, maxPollDelay)
		default:
			return res, fmt.Errorf("follow kopia server's restore task %s: %w", task.ID, err)
		}
	}
}

// progressOf reads the counters of a restore task.
func progressOf(t *kopiaclient.TaskInfo) Progress {
	c := func(name string) int64 { return t.Counters[name].Value }

	return Progress{
		TaskID:        t.ID,
		Files:         c(kopiaclient.CounterRestoredFiles),
		Dirs:          c(kopiaclient.CounterRestoredDirs),
		Bytes:         c(kopiaclient.CounterRestoredBytes),
		Skipped:       c(kopiaclient.CounterSkippedFiles),
		IgnoredErrors: c(kopiaclient.CounterIgnoredErrors),
	}
}

// taskError explains how a task that did not succeed ended.
func taskError(t *kopiaclient.TaskInfo, overwrite bool) string {
	switch t.Status {
	case kopiaclient.TaskSuccess:
		return ""
	case kopiaclient.TaskCanceled:
		return "kopia server's restore task was canceled"
	}

	msg := t.ErrorMessage
	if msg == "" {
		msg = "kopia server's restore task failed"
	}

	if !overwrite && strings.Contains(msg, "already exists") {
		msg += " (nothing that existed was overwritten: restore into an empty or new directory, or pass --overwrite)"
	}

	return msg
}
