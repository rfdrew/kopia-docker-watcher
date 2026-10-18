package kopiaclient

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"time"
)

// SourceInfo identifies a snapshot source: user@host:path.
type SourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

// String formats the source like kopia does: user@host:path.
func (s SourceInfo) String() string {
	switch {
	case s.Host == "" && s.UserName == "" && s.Path == "":
		return "(global)"
	case s.Path == "":
		return s.UserName + "@" + s.Host
	default:
		return s.UserName + "@" + s.Host + ":" + s.Path
	}
}

// query returns the host, userName and path URL filter; empty fields are
// left out, which kopia treats as "any".
func (s SourceInfo) query() url.Values {
	q := url.Values{}

	if s.Host != "" {
		q.Set("host", s.Host)
	}

	if s.UserName != "" {
		q.Set("userName", s.UserName)
	}

	if s.Path != "" {
		q.Set("path", s.Path)
	}

	return q
}

// requireFull rejects a source with an empty field. Without host and
// userName, kopia would match sources of other users on the same host, and
// a policy without a path is a host or global policy, which the watcher
// does not own.
func (s SourceInfo) requireFull() error {
	if s.Host == "" || s.UserName == "" || s.Path == "" {
		return fmt.Errorf("%w: source %q needs host, userName and path", ErrInvalidArgument, s.String())
	}

	return nil
}

// Source states reported by GET /api/v1/sources.
const (
	SourceStatusInitializing = "INITIALIZING"
	SourceStatusIdle         = "IDLE"
	SourceStatusPending      = "PENDING" // waiting for a free upload slot
	SourceStatusUploading    = "UPLOADING"
	SourceStatusPaused       = "PAUSED" // paused through the control API
	SourceStatusFailed       = "FAILED"
	SourceStatusRemote       = "REMOTE" // a source of another user or host
	SourceStatusStopped      = "STOPPED"
)

// SourcesResponse is the response of GET /api/v1/sources.
type SourcesResponse struct {
	// LocalUsername and LocalHost are the identity kopia server snapshots
	// its own sources as.
	LocalUsername string         `json:"localUsername"`
	LocalHost     string         `json:"localHost"`
	MultiUser     bool           `json:"multiUser"`
	Sources       []SourceStatus `json:"sources"`
}

// SourceStatus describes one source known to kopia server.
type SourceStatus struct {
	Source SourceInfo `json:"source"`
	Status string     `json:"status"`
	// Schedule is the effective scheduling policy.
	Schedule SchedulingPolicy `json:"schedule"`
	// LastSnapshot moves only when a snapshot is saved, including
	// checkpoints of a long upload (IncompleteReason "checkpoint").
	LastSnapshot     *SnapshotManifest `json:"lastSnapshot,omitempty"`
	NextSnapshotTime *time.Time        `json:"nextSnapshotTime,omitempty"`
	// Upload and CurrentTask are only set while UPLOADING.
	Upload      *UploadCounters `json:"upload,omitempty"`
	CurrentTask string          `json:"currentTask,omitempty"`
}

// SchedulingPolicy is the scheduling section of a policy.
type SchedulingPolicy struct {
	IntervalSeconds    int64       `json:"intervalSeconds,omitempty"`
	TimesOfDay         []TimeOfDay `json:"timeOfDay,omitempty"`
	NoParentTimesOfDay bool        `json:"noParentTimeOfDay,omitempty"`
	Manual             bool        `json:"manual,omitempty"`
	Cron               []string    `json:"cron,omitempty"`
	RunMissed          *bool       `json:"runMissed,omitempty"`
}

// TimeOfDay is a time of day in the server's time zone.
type TimeOfDay struct {
	Hour   int `json:"hour"`
	Minute int `json:"min"`
}

// UploadCounters is the progress of a running upload.
type UploadCounters struct {
	CachedBytes      int64  `json:"cachedBytes"`
	HashedBytes      int64  `json:"hashedBytes"`
	UploadedBytes    int64  `json:"uploadedBytes"`
	EstimatedBytes   int64  `json:"estimatedBytes"`
	CachedFiles      int32  `json:"cachedFiles"`
	HashedFiles      int32  `json:"hashedFiles"`
	ExcludedFiles    int32  `json:"excludedFiles"`
	ExcludedDirs     int32  `json:"excludedDirs"`
	Errors           int32  `json:"errors"`
	IgnoredErrors    int32  `json:"ignoredErrors"`
	EstimatedFiles   int64  `json:"estimatedFiles"`
	CurrentDirectory string `json:"directory"`
	LastErrorPath    string `json:"lastErrorPath"`
	LastError        string `json:"lastError"`
}

// SnapshotManifest is a snapshot manifest as embedded in SourceStatus.
type SnapshotManifest struct {
	ID               string            `json:"id"`
	Source           SourceInfo        `json:"source"`
	Description      string            `json:"description"`
	StartTime        time.Time         `json:"startTime"`
	EndTime          time.Time         `json:"endTime"`
	Stats            SnapshotStats     `json:"stats"`
	IncompleteReason string            `json:"incomplete,omitempty"`
	RootEntry        *DirEntry         `json:"rootEntry,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
	Pins             []string          `json:"pins,omitempty"`
}

// SnapshotStats are the upload statistics stored in a manifest.
type SnapshotStats struct {
	TotalSize         int64 `json:"totalSize"`
	ExcludedTotalSize int64 `json:"excludedTotalSize"`
	FileCount         int32 `json:"fileCount"`
	CachedFiles       int32 `json:"cachedFiles"`
	NonCachedFiles    int32 `json:"nonCachedFiles"`
	DirCount          int32 `json:"dirCount"`
	ExcludedFileCount int32 `json:"excludedFileCount"`
	ExcludedDirCount  int32 `json:"excludedDirCount"`
	IgnoredErrorCount int32 `json:"ignoredErrorCount"`
	ErrorCount        int32 `json:"errorCount"`
}

// DirEntry is the root entry of a snapshot.
type DirEntry struct {
	Name       string            `json:"name,omitempty"`
	Type       string            `json:"type,omitempty"`
	ObjectID   string            `json:"obj"`
	DirSummary *DirectorySummary `json:"summ,omitempty"`
}

// DirectorySummary summarizes a snapshot directory tree.
type DirectorySummary struct {
	TotalFileSize     int64     `json:"size"`
	TotalFileCount    int64     `json:"files"`
	TotalSymlinkCount int64     `json:"symlinks"`
	TotalDirCount     int64     `json:"dirs"`
	MaxModTime        time.Time `json:"maxTime"`
	IncompleteReason  string    `json:"incomplete,omitempty"`
	FatalErrorCount   int       `json:"numFailed"`
	IgnoredErrorCount int       `json:"numIgnoredErrors,omitempty"`
	FailedEntries     []struct {
		EntryPath string `json:"path"`
		Error     string `json:"error"`
	} `json:"errors,omitempty"`
}

// Snapshot is an entry of GET /api/v1/snapshots.
type Snapshot struct {
	ID          string    `json:"id"`
	Description string    `json:"description"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	// IncompleteReason is "checkpoint" for an intermediate snapshot of a
	// long upload, "canceled" or "limit reached" for an unfinished one.
	IncompleteReason string            `json:"incomplete,omitempty"`
	Summary          *DirectorySummary `json:"summary"`
	RootID           string            `json:"rootID"`
	Retention        []string          `json:"retention"`
	Pins             []string          `json:"pins"`
}

// SnapshotsResponse is the response of GET /api/v1/snapshots.
type SnapshotsResponse struct {
	Snapshots       []Snapshot `json:"snapshots"`
	UnfilteredCount int        `json:"unfilteredCount"`
	UniqueCount     int        `json:"uniqueCount"`
}

// TaskKindSnapshot is the kind of snapshot upload tasks.
const TaskKindSnapshot = "Snapshot"

// TaskStatus is the state of a task.
type TaskStatus string

// Task states.
const (
	TaskRunning   TaskStatus = "RUNNING"
	TaskCanceling TaskStatus = "CANCELING"
	TaskCanceled  TaskStatus = "CANCELED"
	TaskSuccess   TaskStatus = "SUCCESS"
	TaskFailed    TaskStatus = "FAILED"
)

// IsFinished reports whether the task will not change any more.
func (s TaskStatus) IsFinished() bool {
	return s == TaskCanceled || s == TaskSuccess || s == TaskFailed
}

// TaskInfo describes a running or finished task. Kopia keeps the 50 most
// recent finished tasks in memory; IDs, a counter in hexadecimal, restart
// from 1 when kopia server restarts.
type TaskInfo struct {
	ID        string     `json:"id"`
	StartTime time.Time  `json:"startTime"`
	EndTime   *time.Time `json:"endTime,omitempty"`
	Kind      string     `json:"kind"`
	// Description of a snapshot task is "user@host:path at <RFC 3339 time>".
	Description  string                  `json:"description"`
	Status       TaskStatus              `json:"status"`
	ProgressInfo string                  `json:"progressInfo"`
	ErrorMessage string                  `json:"errorMessage,omitempty"`
	Counters     map[string]CounterValue `json:"counters"`
}

// CounterValue is a task counter.
type CounterValue struct {
	Value int64  `json:"value"`
	Units string `json:"units,omitempty"`
	Level string `json:"level"` // "", "notice", "warning" or "error"
}

// Task log levels.
const (
	TaskLogDebug   = 0
	TaskLogInfo    = 1
	TaskLogWarning = 2
	TaskLogError   = 3
)

// TaskLogEntry is one line of a task log.
type TaskLogEntry struct {
	Level   int
	Time    time.Time
	Module  string
	Message string
	// Raw is the entry as sent, including structured fields.
	Raw json.RawMessage
}

// UnmarshalJSON decodes kopia's log line: level, ts (seconds since the
// epoch as a float), mod and msg.
func (e *TaskLogEntry) UnmarshalJSON(b []byte) error {
	var v struct {
		Level  int     `json:"level"`
		TS     float64 `json:"ts"`
		Module string  `json:"mod"`
		Msg    string  `json:"msg"`
	}

	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("decode task log entry: %w", err)
	}

	sec, frac := math.Modf(v.TS)

	*e = TaskLogEntry{
		Level:   v.Level,
		Time:    time.Unix(int64(sec), int64(frac*1e9)).UTC(),
		Module:  v.Module,
		Message: v.Msg,
		Raw:     append(json.RawMessage(nil), b...),
	}

	return nil
}

// ResolvedPolicy is the response of POST /api/v1/policy/resolve. Policies
// are raw JSON so that no field is lost.
type ResolvedPolicy struct {
	// Effective is the policy after merging the hierarchy, with the
	// updates in place of the path's own policy.
	Effective json.RawMessage `json:"effective"`
	// Definition tells which source each effective value comes from.
	Definition json.RawMessage `json:"definition"`
	// Defined is the policy currently stored for the target, without the
	// updates.
	Defined json.RawMessage `json:"defined"`
	// UpcomingSnapshotTimes are computed from now in the server's time
	// zone, not from the last snapshot.
	UpcomingSnapshotTimes []time.Time `json:"upcomingSnapshotTimes"`
	// SchedulingError is set when the scheduling section of the updates is
	// invalid, e.g. a bad cron expression.
	SchedulingError string `json:"schedulingError,omitempty"`
}
