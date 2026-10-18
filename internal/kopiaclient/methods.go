package kopiaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
)

// ListSources returns the sources kopia server knows, filtered by the
// non-empty fields of filter. LocalHost and LocalUsername of the response
// are the identity of kopia server's own sources.
func (c *Client) ListSources(ctx context.Context, filter SourceInfo) (*SourcesResponse, error) {
	var resp SourcesResponse
	if err := c.call(ctx, request{method: http.MethodGet, path: "sources", query: filter.query()}, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// CreateSourceRequest registers a new source with POST /api/v1/sources.
type CreateSourceRequest struct {
	Path string `json:"path"`
	// CreateSnapshot starts the first snapshot right away.
	CreateSnapshot bool `json:"createSnapshot"`
	// Policy replaces the policy of the path; required.
	Policy json.RawMessage `json:"policy"`
}

type createSourceResponse struct {
	SnapshotStarted bool `json:"snapshotted"`
}

// CreateSource registers a source as kopia server's own user@host. Unlike
// a policy PUT it checks that the path exists (ErrPathNotFound), but it
// does not refresh the server: the policy of a source the server already
// knows is not reloaded until the next refresh, so follow it with
// SetPolicy. Reports whether a snapshot was started.
func (c *Client) CreateSource(ctx context.Context, req CreateSourceRequest) (bool, error) {
	if req.Path == "" {
		return false, fmt.Errorf("%w: CreateSource needs a path", ErrInvalidArgument)
	}

	if err := requireObject("policy", req.Policy); err != nil {
		return false, err
	}

	var resp createSourceResponse
	if err := c.call(ctx, request{method: http.MethodPost, path: "sources", body: req}, &resp); err != nil {
		return false, err
	}

	return resp.SnapshotStarted, nil
}

type sourceActionResponse struct {
	Sources map[string]struct {
		Success bool `json:"success"`
	} `json:"sources"`
}

// StartSnapshot queues a snapshot of src with POST /api/v1/sources/upload.
// The snapshot runs asynchronously and no task ID is returned. A source
// kopia server does not know yet is ErrNotFound. While a snapshot is
// pending or uploading, at most one more run is queued after it.
func (c *Client) StartSnapshot(ctx context.Context, src SourceInfo) error {
	if err := src.requireFull(); err != nil {
		return err
	}

	var resp sourceActionResponse
	if err := c.call(ctx, request{method: http.MethodPost, path: "sources/upload", query: src.query()}, &resp); err != nil {
		return err
	}

	for name, r := range resp.Sources {
		if !r.Success {
			return &Error{Op: "POST " + c.apiPath("sources/upload"), StatusCode: http.StatusOK, kind: ErrUnexpectedResponse,
				Message: "kopia server reported failure for " + name}
		}
	}

	return nil
}

// GetPolicy returns the policy defined for the target itself, not the
// effective one, as raw JSON. No policy is ErrNotFound.
func (c *Client) GetPolicy(ctx context.Context, target SourceInfo) (json.RawMessage, error) {
	if err := target.requireFull(); err != nil {
		return nil, err
	}

	var raw json.RawMessage

	r := request{method: http.MethodGet, path: "policy", query: target.query()}
	if err := c.call(ctx, r, &raw); err != nil {
		return nil, err
	}

	// Kopia answers 200 null when reading the policy failed for any reason
	// other than "not found".
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, &Error{Op: r.method + " " + c.apiPath(r.path), StatusCode: http.StatusOK, kind: ErrInternal,
			Message: "kopia server returned null: it could not read the policy from the repository"}
	}

	return raw, nil
}

// SetPolicy replaces the policy of the target with PUT /api/v1/policy. The
// server refreshes all sources before answering, so the source exists when
// SetPolicy returns. A policy kopia rejects (bad cron, manual combined with
// another scheduling field, a path with a trailing slash) is
// ErrInvalidPolicy.
func (c *Client) SetPolicy(ctx context.Context, target SourceInfo, policy json.RawMessage) error {
	if err := target.requireFull(); err != nil {
		return err
	}

	if err := requireObject("policy", policy); err != nil {
		return err
	}

	return c.call(ctx, request{method: http.MethodPut, path: "policy", query: target.query(), body: policy}, nil)
}

// DeletePolicy removes the policy of the target. A source with snapshots
// stays and inherits the host or global policy. Deleting a missing policy
// succeeds.
func (c *Client) DeletePolicy(ctx context.Context, target SourceInfo) error {
	if err := target.requireFull(); err != nil {
		return err
	}

	return c.call(ctx, request{method: http.MethodDelete, path: "policy", query: target.query()}, nil)
}

// ResolvePolicyRequest previews a policy with POST /api/v1/policy/resolve.
type ResolvePolicyRequest struct {
	// Updates replaces the target's own policy for the calculation; pass
	// the complete merged policy, not a patch. Nil resolves the stored one.
	Updates json.RawMessage `json:"updates"`
	// NumUpcomingSnapshotTimes asks for that many upcoming snapshot times.
	NumUpcomingSnapshotTimes int `json:"numUpcomingSnapshotTimes"`
}

// ResolvePolicy computes the effective policy of the target with updates
// in place of its own policy, and the upcoming snapshot times.
func (c *Client) ResolvePolicy(ctx context.Context, target SourceInfo, req ResolvePolicyRequest) (*ResolvedPolicy, error) {
	if err := target.requireFull(); err != nil {
		return nil, err
	}

	if req.Updates != nil {
		if err := requireObject("updates", req.Updates); err != nil {
			return nil, err
		}
	}

	var resp ResolvedPolicy
	if err := c.call(ctx, request{method: http.MethodPost, path: "policy/resolve", query: target.query(), body: req}, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

type resolvePathResponse struct {
	Source SourceInfo `json:"source"`
}

// ResolvePath normalizes a path the way kopia server does: ~ expanded, a
// relative path resolved against the server's home directory, then
// filepath.Clean. Existence is not checked. The result carries kopia
// server's own host and user name.
func (c *Client) ResolvePath(ctx context.Context, path string) (SourceInfo, error) {
	if path == "" {
		return SourceInfo{}, fmt.Errorf("%w: ResolvePath needs a path", ErrInvalidArgument)
	}

	var resp resolvePathResponse
	if err := c.call(ctx, request{method: http.MethodPost, path: "paths/resolve", body: map[string]string{"path": path}}, &resp); err != nil {
		return SourceInfo{}, err
	}

	return resp.Source, nil
}

// ListSnapshots returns the snapshots of src, oldest first: by start time,
// then by end time. With all false kopia merges snapshots with the same
// root and keeps the oldest of them; pass true to see every manifest,
// including checkpoints.
func (c *Client) ListSnapshots(ctx context.Context, src SourceInfo, all bool) (*SnapshotsResponse, error) {
	if err := src.requireFull(); err != nil {
		return nil, err
	}

	q := src.query()
	if all {
		q.Set("all", "1")
	}

	var resp SnapshotsResponse
	if err := c.call(ctx, request{method: http.MethodGet, path: "snapshots", query: q}, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// EditSnapshotsRequest changes snapshots with POST /api/v1/snapshots/edit.
type EditSnapshotsRequest struct {
	Snapshots []string `json:"snapshots"`
	// Description replaces the description when not nil.
	Description *string  `json:"description"`
	AddPins     []string `json:"addPins"`
	RemovePins  []string `json:"removePins"`
}

// EditSnapshots changes the description or pins of snapshots. Kopia saves
// a new manifest for each changed snapshot, so the returned snapshots have
// new IDs; unchanged ones keep theirs.
func (c *Client) EditSnapshots(ctx context.Context, req EditSnapshotsRequest) ([]Snapshot, error) {
	if len(req.Snapshots) == 0 {
		return nil, fmt.Errorf("%w: EditSnapshots needs snapshot IDs", ErrInvalidArgument)
	}

	var resp []Snapshot
	if err := c.call(ctx, request{method: http.MethodPost, path: "snapshots/edit", body: req}, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// DeleteSnapshotsRequest deletes snapshots with POST /api/v1/snapshots/delete.
type DeleteSnapshotsRequest struct {
	Source              SourceInfo `json:"source"`
	SnapshotManifestIDs []string   `json:"snapshotManifestIds"`
	// DeleteSourceAndPolicy deletes every snapshot of the source, pinned
	// ones included, and its policy, and cancels a running upload.
	DeleteSourceAndPolicy bool `json:"deleteSourceAndPolicy"`
}

// DeleteSnapshots deletes the listed snapshots of a source, or the whole
// source. A source kopia server does not know is ErrNotFound. Pins are not
// checked.
func (c *Client) DeleteSnapshots(ctx context.Context, req DeleteSnapshotsRequest) error {
	if err := req.Source.requireFull(); err != nil {
		return err
	}

	switch {
	case req.DeleteSourceAndPolicy && len(req.SnapshotManifestIDs) > 0:
		return fmt.Errorf("%w: DeleteSourceAndPolicy deletes all snapshots, do not list IDs", ErrInvalidArgument)
	case !req.DeleteSourceAndPolicy && len(req.SnapshotManifestIDs) == 0:
		return fmt.Errorf("%w: DeleteSnapshots needs snapshot IDs or DeleteSourceAndPolicy", ErrInvalidArgument)
	}

	return c.call(ctx, request{method: http.MethodPost, path: "snapshots/delete", body: req}, nil)
}

// taskIDRE matches kopia task IDs, decimal counters, and keeps anything
// that would need escaping out of the URL path.
var taskIDRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type taskListResponse struct {
	Tasks []TaskInfo `json:"tasks"`
}

// ListTasks returns running and recently finished tasks, newest first. It
// works without a connected repository.
func (c *Client) ListTasks(ctx context.Context) ([]TaskInfo, error) {
	var resp taskListResponse
	if err := c.call(ctx, request{method: http.MethodGet, path: "tasks"}, &resp); err != nil {
		return nil, err
	}

	return resp.Tasks, nil
}

// GetTask returns one task; a task kopia no longer keeps is ErrNotFound.
func (c *Client) GetTask(ctx context.Context, id string) (*TaskInfo, error) {
	if !taskIDRE.MatchString(id) {
		return nil, fmt.Errorf("%w: task ID %q", ErrInvalidArgument, id)
	}

	var t TaskInfo
	if err := c.call(ctx, request{method: http.MethodGet, path: "tasks/" + id}, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

type taskLogResponse struct {
	Logs []TaskLogEntry `json:"logs"`
}

// TaskLogs returns the log of a task. An unknown task has an empty log.
func (c *Client) TaskLogs(ctx context.Context, id string) ([]TaskLogEntry, error) {
	if !taskIDRE.MatchString(id) {
		return nil, fmt.Errorf("%w: task ID %q", ErrInvalidArgument, id)
	}

	var resp taskLogResponse
	if err := c.call(ctx, request{method: http.MethodGet, path: "tasks/" + id + "/logs"}, &resp); err != nil {
		return nil, err
	}

	return resp.Logs, nil
}

// requireObject checks that raw is a JSON object.
func requireObject(name string, raw json.RawMessage) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return fmt.Errorf("%w: %s must be a JSON object", ErrInvalidArgument, name)
	}

	return nil
}
