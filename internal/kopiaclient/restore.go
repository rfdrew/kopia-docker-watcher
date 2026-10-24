package kopiaclient

import (
	"context"
	"fmt"
	"net/http"
	"path"
)

// TaskKindRestore is the kind of restore tasks.
const TaskKindRestore = "Restore"

// Counters of a restore task.
const (
	CounterRestoredFiles = "Restored Files"
	CounterRestoredDirs  = "Restored Directories"
	CounterRestoredBytes = "Restored Bytes"
	CounterIgnoredErrors = "Ignored Errors"
	CounterSkippedFiles  = "Skipped Files"
)

// RestoreRequest restores a snapshot with POST /api/v1/restore
// (serverapi.RestoreRequest). Only the output to a directory on kopia
// server is supported.
type RestoreRequest struct {
	// Root is the object ID of what to restore: a snapshot's RootID.
	Root       string            `json:"root"`
	Filesystem *FilesystemOutput `json:"fsOutput"`
	Options    RestoreOptions    `json:"options"`
}

// FilesystemOutput is a directory on kopia server to restore into
// (restore.FilesystemOutput). With every Overwrite field false, a restore
// into a directory that exists and is not empty fails before it writes
// anything there, and one that meets an existing file fails at it.
type FilesystemOutput struct {
	// TargetPath is an absolute path on kopia server.
	TargetPath string `json:"targetPath"`
	// OverwriteDirectories restores into a directory that exists and is
	// not empty; OverwriteFiles and OverwriteSymlinks replace the files
	// and symlinks found there.
	OverwriteDirectories bool `json:"overwriteDirectories"`
	OverwriteFiles       bool `json:"overwriteFiles"`
	OverwriteSymlinks    bool `json:"overwriteSymlinks"`
	// WriteFilesAtomically writes each file to a temporary one and
	// renames it, so that no file is left half written.
	WriteFilesAtomically bool `json:"writeFilesAtomically"`
}

// RestoreOptions are the options of a restore (restore.Options).
type RestoreOptions struct {
	// Incremental leaves alone the files that exist with the size and
	// modification time of the snapshot's.
	Incremental bool `json:"incremental"`
	// IgnoreErrors goes on after an error, counting it.
	IgnoreErrors bool `json:"ignoreErrors"`
}

// Restore starts a restore and returns its task, which tells how it goes:
// kopia server restores in the background. A target that is not an
// absolute path is ErrInvalidArgument.
func (c *Client) Restore(ctx context.Context, req RestoreRequest) (*TaskInfo, error) {
	switch {
	case req.Root == "":
		return nil, fmt.Errorf("%w: restore needs a root object ID", ErrInvalidArgument)
	case req.Filesystem == nil:
		return nil, fmt.Errorf("%w: restore needs a target directory", ErrInvalidArgument)
	case !path.IsAbs(req.Filesystem.TargetPath):
		return nil, fmt.Errorf("%w: restore target %q is not an absolute path", ErrInvalidArgument, req.Filesystem.TargetPath)
	}

	var t TaskInfo
	if err := c.call(ctx, request{method: http.MethodPost, path: "restore", body: req}, &t); err != nil {
		return nil, err
	}

	if !taskIDRE.MatchString(t.ID) {
		return nil, &Error{Op: "POST " + c.apiPath("restore"), Message: fmt.Sprintf("task ID %q", t.ID), kind: ErrUnexpectedResponse}
	}

	return &t, nil
}

// CancelTask asks kopia server to cancel a task; a task that has ended,
// or that kopia server no longer keeps, is left alone without an error.
func (c *Client) CancelTask(ctx context.Context, id string) error {
	if !taskIDRE.MatchString(id) {
		return fmt.Errorf("%w: task ID %q", ErrInvalidArgument, id)
	}

	return c.call(ctx, request{method: http.MethodPost, path: "tasks/" + id + "/cancel"}, nil)
}
