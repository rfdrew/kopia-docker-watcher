// Package rest implements backend.Backend over the UI API of kopia server,
// with internal/kopiaclient.
//
// Every request about a source carries its host, user name and path, so it
// never touches the sources of another user. Every method that takes a
// source first asks kopia server for it with GET /api/v1/sources and the
// three fields: the answer tells kopia server's identity, so a source of
// another identity fails with backend.ErrWrongIdentity before anything is
// written, and whether kopia server knows the source.
//
// Policies are read and written as raw JSON and merged by backend.Merge
// through backend.NewPlan, so fields the watcher does not own are written
// back as kopia server returned them.
//
// Errors are *backend.Error with the kinds package backend documents for
// the errors of internal/kopiaclient.
//
// The snapshot lifecycle is not implemented yet: TriggerSnapshot queues a
// snapshot, WaitSnapshot and AnnotateSnapshot return
// backend.ErrNotImplemented.
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// Backend is a backend.Backend that talks to kopia server. It is safe for
// concurrent use. Create it with New.
type Backend struct {
	client *kopiaclient.Client
	log    *slog.Logger
	now    func() time.Time

	// ident is kopia server's identity from its last answer to GET
	// /api/v1/sources; nil before the first. It is only a hint for the
	// filter of ListSources, which checks it against the answer.
	ident atomic.Pointer[identity]
}

type identity struct {
	userName, host string
}

var _ backend.Backend = (*Backend)(nil)

// New returns a Backend that talks to kopia server through client. log
// receives the failures the Backend handles itself, such as a policy it
// could not read back after writing it; nil discards them.
func New(client *kopiaclient.Client, log *slog.Logger) *Backend {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Backend{client: client, log: log, now: time.Now}
}

// Backend method names, the Op of errors.
const (
	opServerIdentity   = "ServerIdentity"
	opResolvePath      = "ResolvePath"
	opListSources      = "ListSources"
	opPlanSource       = "PlanSource"
	opApplyPlan        = "ApplyPlan"
	opPreviewSchedule  = "PreviewSchedule"
	opDeletePolicy     = "DeletePolicy"
	opTriggerSnapshot  = "TriggerSnapshot"
	opWaitSnapshot     = "WaitSnapshot"
	opAnnotateSnapshot = "AnnotateSnapshot"
)

// ServerIdentity implements backend.Backend with GET /api/v1/sources.
func (b *Backend) ServerIdentity(ctx context.Context) (userName, host string, err error) {
	resp, err := b.client.ListSources(ctx, kopiaclient.SourceInfo{})
	if err != nil {
		return "", "", fail(ctx, opServerIdentity, backend.Source{}, reqRead, err)
	}

	if resp.LocalUsername == "" || resp.LocalHost == "" {
		return "", "", &backend.Error{Op: opServerIdentity,
			Err: fmt.Errorf("%w: kopia server reported the identity %q", kopiaclient.ErrUnexpectedResponse,
				resp.LocalUsername+"@"+resp.LocalHost)}
	}

	b.remember(resp)

	return resp.LocalUsername, resp.LocalHost, nil
}

// ResolvePath implements backend.Backend with paths/resolve.
func (b *Backend) ResolvePath(ctx context.Context, path string) (backend.Source, error) {
	if path == "" {
		return backend.Source{}, &backend.Error{Op: opResolvePath, Err: errors.New("empty path")}
	}

	si, err := b.client.ResolvePath(ctx, path)
	if err != nil {
		return backend.Source{}, fail(ctx, opResolvePath, backend.Source{}, reqRead, err)
	}

	src := fromInfo(si)
	if src.Host == "" || src.UserName == "" || src.Path == "" {
		return backend.Source{}, &backend.Error{Op: opResolvePath,
			Err: fmt.Errorf("%w: paths/resolve returned the incomplete source %q", kopiaclient.ErrUnexpectedResponse, si.String())}
	}

	return src, nil
}

// ListSources implements backend.Backend with GET /api/v1/sources, filtered
// by kopia server's host and user name. The identity the filter takes is the
// one kopia server reported last; when its answer reports another one, the
// sources are listed again without a filter and kept by that answer's
// identity, so the list never mixes the sources of two identities.
func (b *Backend) ListSources(ctx context.Context) ([]backend.SourceStatus, error) {
	var filter kopiaclient.SourceInfo

	hint := b.ident.Load()
	if hint != nil {
		filter = kopiaclient.SourceInfo{Host: hint.host, UserName: hint.userName}
	}

	resp, err := b.client.ListSources(ctx, filter)
	if err != nil {
		return nil, fail(ctx, opListSources, backend.Source{}, reqRead, err)
	}

	if hint != nil && (resp.LocalHost != hint.host || resp.LocalUsername != hint.userName) {
		// kopia server's identity changed: the answer holds the sources of
		// the old one.
		if resp, err = b.client.ListSources(ctx, kopiaclient.SourceInfo{}); err != nil {
			return nil, fail(ctx, opListSources, backend.Source{}, reqRead, err)
		}
	}

	b.remember(resp)

	var out []backend.SourceStatus

	for _, s := range resp.Sources {
		if s.Source.Host == resp.LocalHost && s.Source.UserName == resp.LocalUsername && s.Source.Path != "" {
			out = append(out, fromStatus(s))
		}
	}

	slices.SortFunc(out, func(x, y backend.SourceStatus) int { return x.Source.Compare(y.Source) })

	return out, nil
}

// PlanSource implements backend.Backend: GET /api/v1/sources tells whether
// kopia server knows the source, GET /api/v1/policy returns the policy of
// the path (none for NOT_FOUND), and backend.NewPlan decides.
func (b *Backend) PlanSource(ctx context.Context, want backend.Desired, last backend.Applied) (backend.Plan, error) {
	src := want.Source

	status, err := b.lookup(ctx, opPlanSource, src)
	if err != nil {
		return backend.Plan{}, err
	}

	current, err := b.client.GetPolicy(ctx, toInfo(src))

	switch {
	case errors.Is(err, kopiaclient.ErrNotFound):
		current = nil
	case err != nil:
		// A 200 null, kopia server's answer when it could not read the
		// policy, is an error too: taken for a missing policy, it would
		// make the watcher overwrite the policy with its own fields alone.
		return backend.Plan{}, fail(ctx, opPlanSource, src, reqRead, err)
	}

	return backend.NewPlan(want, last, status != nil, status, current)
}

// ApplyPlan implements backend.Backend. A registration is POST
// /api/v1/sources with the policy, which fails with ErrConfig for a path
// that does not exist on kopia server, then PUT /api/v1/policy with the
// same policy, since the POST does not refresh kopia server; an update is
// the PUT alone. The policy is read back with GET /api/v1/policy for
// StoredHash. ActionNone sends nothing.
func (b *Backend) ApplyPlan(ctx context.Context, plan backend.Plan) (backend.Applied, error) {
	src := plan.Source

	switch plan.Action {
	case backend.ActionNone:
		return plan.Applied.Clone(), nil
	case backend.ActionRegister, backend.ActionUpdate:
	default:
		return backend.Applied{}, &backend.Error{Op: opApplyPlan, Source: src, Err: fmt.Errorf("unknown action %v", plan.Action)}
	}

	if !isObject(plan.Policy) {
		return backend.Applied{}, &backend.Error{Op: opApplyPlan, Source: src,
			Err: fmt.Errorf("%w: the policy to write is not a JSON object", kopiaclient.ErrInvalidArgument)}
	}

	if _, err := b.lookup(ctx, opApplyPlan, src); err != nil {
		return backend.Applied{}, err
	}

	if plan.Action == backend.ActionRegister {
		_, err := b.client.CreateSource(ctx, kopiaclient.CreateSourceRequest{Path: src.Path, Policy: plan.Policy})
		if err != nil {
			return backend.Applied{}, fail(ctx, opApplyPlan, src, reqCreateSource, err)
		}
	}

	if err := b.client.SetPolicy(ctx, toInfo(src), plan.Policy); err != nil {
		return backend.Applied{}, fail(ctx, opApplyPlan, src, reqWritePolicy, err)
	}

	applied := plan.Applied.Clone()
	applied.StoredHash = b.storedHash(ctx, src)

	return applied, nil
}

// storedHash reads the policy of src back after a write and returns its
// fingerprint. If that fails it returns "": the write succeeded, and the
// next plan writes the policy again if it looks changed.
func (b *Backend) storedHash(ctx context.Context, src backend.Source) string {
	stored, err := b.client.GetPolicy(ctx, toInfo(src))
	if err == nil {
		var hash string
		if hash, err = backend.Fingerprint(stored); err == nil {
			return hash
		}
	}

	b.log.WarnContext(ctx, "could not read back the policy written to kopia server", "source", src.String(), "error", err)

	return ""
}

// PreviewSchedule implements backend.Backend with policy/resolve: policy
// goes into updates, which replace the path's policy for the calculation.
// A schedulingError of the answer is ErrConfig.
func (b *Backend) PreviewSchedule(ctx context.Context, src backend.Source, policy json.RawMessage, n int) (backend.SchedulePreview, error) {
	if err := src.Validate(); err != nil {
		return backend.SchedulePreview{}, &backend.Error{Op: opPreviewSchedule, Source: src, Err: err}
	}

	// Without updates kopia server would resolve the stored policy.
	if !isObject(policy) {
		return backend.SchedulePreview{}, &backend.Error{Op: opPreviewSchedule, Source: src,
			Err: fmt.Errorf("%w: the policy to preview is not a JSON object", kopiaclient.ErrInvalidArgument)}
	}

	if _, err := b.lookup(ctx, opPreviewSchedule, src); err != nil {
		return backend.SchedulePreview{}, err
	}

	n = max(n, 0)

	res, err := b.client.ResolvePolicy(ctx, toInfo(src), kopiaclient.ResolvePolicyRequest{Updates: policy, NumUpcomingSnapshotTimes: n})
	if err != nil {
		return backend.SchedulePreview{}, fail(ctx, opPreviewSchedule, src, reqResolvePolicy, err)
	}

	if res.SchedulingError != "" {
		return backend.SchedulePreview{}, &backend.Error{Op: opPreviewSchedule, Source: src, Kind: backend.ErrConfig,
			Err: fmt.Errorf("kopia server rejects the schedule: %s", res.SchedulingError)}
	}

	section, err := scheduling(res.Effective)
	if err != nil {
		return backend.SchedulePreview{}, &backend.Error{Op: opPreviewSchedule, Source: src,
			Err: fmt.Errorf("%w: effective policy: %w", kopiaclient.ErrUnexpectedResponse, err)}
	}

	times := res.UpcomingSnapshotTimes
	if len(times) > n {
		times = times[:n]
	}

	return backend.SchedulePreview{Times: slices.Clone(times), Scheduling: section}, nil
}

// scheduling returns the scheduling section of a policy; an empty object if
// it has none.
func scheduling(policy json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Scheduling json.RawMessage `json:"scheduling"`
	}

	if err := json.Unmarshal(policy, &p); err != nil {
		return nil, err
	}

	if isNull(p.Scheduling) {
		return json.RawMessage(`{}`), nil
	}

	if !isObject(p.Scheduling) {
		return nil, fmt.Errorf("scheduling is not a JSON object: %s", p.Scheduling)
	}

	return p.Scheduling, nil
}

// DeletePolicy implements backend.Backend with DELETE /api/v1/policy.
func (b *Backend) DeletePolicy(ctx context.Context, src backend.Source) error {
	if _, err := b.lookup(ctx, opDeletePolicy, src); err != nil {
		return err
	}

	if err := b.client.DeletePolicy(ctx, toInfo(src)); err != nil {
		return fail(ctx, opDeletePolicy, src, reqDeletePolicy, err)
	}

	return nil
}

// TriggerSnapshot implements backend.Backend with sources/upload. The
// ticket records the start of the source's last snapshot and the IDs of
// the source's snapshot tasks kopia server lists before the trigger. A
// source kopia server has not picked up yet is ErrTemporary, as after a
// policy write whose refresh failed.
//
// It is provisional, like the method in backend.Backend: the snapshot
// lifecycle may change it.
func (b *Backend) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	status, err := b.lookup(ctx, opTriggerSnapshot, src)
	if err != nil {
		return backend.SnapshotTicket{}, err
	}

	tasks, err := b.client.ListTasks(ctx)
	if err != nil {
		return backend.SnapshotTicket{}, fail(ctx, opTriggerSnapshot, src, reqRead, err)
	}

	ticket := backend.SnapshotTicket{Source: src, TriggeredAt: b.now()}

	if status != nil && status.LastSnapshot != nil {
		ticket.LastSnapshotStart = status.LastSnapshot.StartTime
	}

	// kopia server describes a snapshot task as "user@host:path at <time>".
	prefix := src.String() + " at "

	for _, t := range tasks {
		if t.Kind == kopiaclient.TaskKindSnapshot && strings.HasPrefix(t.Description, prefix) {
			ticket.TasksBefore = append(ticket.TasksBefore, t.ID)
		}
	}

	if err := b.client.StartSnapshot(ctx, toInfo(src)); err != nil {
		return backend.SnapshotTicket{}, fail(ctx, opTriggerSnapshot, src, reqUpload, err)
	}

	return ticket, nil
}

// WaitSnapshot implements backend.Backend. It is not implemented yet.
func (b *Backend) WaitSnapshot(_ context.Context, ticket backend.SnapshotTicket, _ func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	return backend.SnapshotResult{}, &backend.Error{Op: opWaitSnapshot, Source: ticket.Source, Err: backend.ErrNotImplemented}
}

// AnnotateSnapshot implements backend.Backend. It is not implemented yet.
func (b *Backend) AnnotateSnapshot(_ context.Context, src backend.Source, _ time.Time, _ string, _ []string) error {
	return &backend.Error{Op: opAnnotateSnapshot, Source: src, Err: backend.ErrNotImplemented}
}

// lookup checks that src is a complete source of kopia server's identity
// and returns its status; nil if kopia server does not know it.
func (b *Backend) lookup(ctx context.Context, op string, src backend.Source) (*backend.SourceStatus, error) {
	if err := src.Validate(); err != nil {
		return nil, &backend.Error{Op: op, Source: src, Err: err}
	}

	resp, err := b.client.ListSources(ctx, toInfo(src))
	if err != nil {
		return nil, fail(ctx, op, src, reqRead, err)
	}

	b.remember(resp)

	if resp.LocalHost != src.Host || resp.LocalUsername != src.UserName {
		return nil, &backend.Error{Op: op, Source: src,
			Err: fmt.Errorf("%w: kopia server is %s@%s", backend.ErrWrongIdentity, resp.LocalUsername, resp.LocalHost)}
	}

	for _, s := range resp.Sources {
		if fromInfo(s.Source) == src {
			st := fromStatus(s)

			return &st, nil
		}
	}

	return nil, nil
}

// remember keeps the identity of an answer to GET /api/v1/sources as the
// hint for ListSources.
func (b *Backend) remember(resp *kopiaclient.SourcesResponse) {
	if resp.LocalHost == "" || resp.LocalUsername == "" {
		return
	}

	if cur := b.ident.Load(); cur != nil && cur.host == resp.LocalHost && cur.userName == resp.LocalUsername {
		return
	}

	b.ident.Store(&identity{userName: resp.LocalUsername, host: resp.LocalHost})
}

func toInfo(src backend.Source) kopiaclient.SourceInfo {
	return kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: src.Path}
}

func fromInfo(si kopiaclient.SourceInfo) backend.Source {
	return backend.Source{Host: si.Host, UserName: si.UserName, Path: si.Path}
}

func fromStatus(s kopiaclient.SourceStatus) backend.SourceStatus {
	out := backend.SourceStatus{
		Source:      fromInfo(s.Source),
		Status:      backend.Status(s.Status),
		CurrentTask: s.CurrentTask,
	}

	if m := s.LastSnapshot; m != nil {
		out.LastSnapshot = &backend.SnapshotInfo{
			StartTime:   m.StartTime,
			EndTime:     m.EndTime,
			Description: m.Description,
			Incomplete:  m.IncompleteReason,
		}
	}

	if s.NextSnapshotTime != nil {
		out.NextSnapshotTime = *s.NextSnapshotTime
	}

	return out
}

// isObject reports whether raw is a JSON object.
func isObject(raw json.RawMessage) bool {
	var obj map[string]json.RawMessage

	return json.Unmarshal(raw, &obj) == nil && obj != nil
}

// isNull reports whether raw is empty or the JSON null.
func isNull(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))

	return s == "" || s == "null"
}
