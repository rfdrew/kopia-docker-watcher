// Package rest implements backend.Backend over the UI API of kopia server,
// with internal/kopiaclient.
//
// Every request about a source carries its host, user name and path, so it
// never touches the sources of another user. Every method that takes a
// source first asks kopia server for it with GET /api/v1/sources and the
// three fields: the answer tells kopia server's identity, so a source of
// another identity fails with backend.ErrWrongIdentity before anything is
// written, and whether kopia server knows the source. A source path must
// be absolute: POST /api/v1/sources resolves any other path against kopia
// server's home directory, the policy requests do not, so the two would
// name different sources.
//
// Policies are read and written as raw JSON and merged by backend.Merge
// through backend.NewPlan, so fields the watcher does not own are written
// back as kopia server returned them.
//
// Errors are *backend.Error with the kinds package backend documents for
// the errors of internal/kopiaclient.
//
// kopia server returns no task ID for a snapshot it queues, numbers its
// tasks anew when it restarts, and gives a snapshot a new ID when its
// description or pins change. So a run is followed by the snapshot tasks of
// the source that appear after the trigger, and a snapshot is found by its
// start time; see TriggerSnapshot, WaitSnapshot, AnnotateSnapshot and
// ListSnapshots. For the same reason DeleteSnapshots reads the IDs of the
// snapshots it deletes right before the delete and keeps none.
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"sync"
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

	// pollMin and pollMax bound the interval between two polls of
	// WaitSnapshot, which waits for after.
	pollMin, pollMax time.Duration
	after            func(time.Duration) <-chan time.Time

	// ident is kopia server's identity from its last answer to GET
	// /api/v1/sources; nil before the first. It is only a hint for the
	// filter of ListSources, which checks it against the answer.
	ident atomic.Pointer[identity]

	// algorithms are the compression algorithms of kopia server's first
	// answer to GET /api/v1/repo/algorithms; nil before it. algoMu keeps
	// calls that find no answer yet from asking all at once.
	algoMu     sync.Mutex
	algorithms []backend.Algorithm
}

type identity struct {
	userName, host string
}

var _ backend.Backend = (*Backend)(nil)

// New returns a Backend that talks to kopia server through client. log
// receives the failures the Backend handles itself, such as a policy it
// could not read back after writing it or the log of a failed snapshot
// task it could not read; nil discards them.
func New(client *kopiaclient.Client, log *slog.Logger) *Backend {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Backend{client: client, log: log, now: time.Now, pollMin: defaultPollMin, pollMax: defaultPollMax, after: time.After}
}

// Backend method names, the Op of errors.
const (
	opServerIdentity   = "ServerIdentity"
	opAlgorithms       = "CompressionAlgorithms"
	opResolvePath      = "ResolvePath"
	opListSources      = "ListSources"
	opPlanSource       = "PlanSource"
	opApplyPlan        = "ApplyPlan"
	opPreviewSchedule  = "PreviewSchedule"
	opDeletePolicy     = "DeletePolicy"
	opTriggerSnapshot  = "TriggerSnapshot"
	opWaitSnapshot     = "WaitSnapshot"
	opAnnotateSnapshot = "AnnotateSnapshot"
	opListSnapshots    = "ListSnapshots"
	opDeleteSnapshots  = "DeleteSnapshots"
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

// CompressionAlgorithms implements backend.Backend with GET
// /api/v1/repo/algorithms. The first answer is kept for the life of the
// Backend: the list is that of the kopia server build, and a kopia server
// upgraded under a running watcher is noticed only by its next start.
func (b *Backend) CompressionAlgorithms(ctx context.Context) ([]backend.Algorithm, error) {
	b.algoMu.Lock()
	defer b.algoMu.Unlock()

	if b.algorithms == nil {
		res, err := b.client.SupportedAlgorithms(ctx)
		if err != nil {
			return nil, fail(ctx, opAlgorithms, backend.Source{}, reqRead, err)
		}

		algos := make([]backend.Algorithm, 0, len(res.Compression))
		for _, a := range res.Compression {
			if a.ID != "" {
				algos = append(algos, backend.Algorithm{Name: a.ID, Deprecated: a.Deprecated})
			}
		}

		if len(algos) == 0 {
			return nil, &backend.Error{Op: opAlgorithms,
				Err: fmt.Errorf("%w: kopia server lists no compression algorithms", kopiaclient.ErrUnexpectedResponse)}
		}

		slices.SortFunc(algos, func(x, y backend.Algorithm) int { return strings.Compare(x.Name, y.Name) })
		b.algorithms = algos
	}

	return slices.Clone(b.algorithms), nil
}

// ResolvePath implements backend.Backend with paths/resolve.
func (b *Backend) ResolvePath(ctx context.Context, p string) (backend.Source, error) {
	if p == "" {
		return backend.Source{}, &backend.Error{Op: opResolvePath, Err: errors.New("empty path")}
	}

	si, err := b.client.ResolvePath(ctx, p)
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
// same policy, since the POST does not refresh kopia server; that PUT
// never fails with ErrConfig, as the POST stored the policy. An update is
// the PUT alone. The policy is read back with GET /api/v1/policy for
// StoredHash; see storedHash. ActionNone sends nothing.
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

	put := reqWritePolicy
	if plan.Action == backend.ActionRegister {
		put = reqRegisterPut
	}

	if err := b.client.SetPolicy(ctx, toInfo(src), plan.Policy); err != nil {
		return backend.Applied{}, fail(ctx, opApplyPlan, src, put, err)
	}

	applied := plan.Applied.Clone()
	applied.StoredHash = b.storedHash(ctx, src, plan.Policy)

	return applied, nil
}

// storedHash reads the policy of src back after writing it and returns its
// fingerprint, which tells the next plan what kopia server made of the
// write. It returns "" if the read-back fails, or if its pause or cron
// differ from the written policy's: kopia server stores those as written,
// so another client wrote the policy after the watcher, and the next plan
// compares the policy instead of taking the difference for kopia server's
// own. The write itself succeeded either way.
//
// Another client's change to a field kopia server may rewrite, such as a
// policy-json field, is not told apart from kopia server's own rewrite.
func (b *Backend) storedHash(ctx context.Context, src backend.Source, written json.RawMessage) string {
	stored, err := b.client.GetPolicy(ctx, toInfo(src))
	if err == nil {
		if !sameSchedule(written, stored) {
			b.log.WarnContext(ctx, "the policy changed right after the watcher wrote it", "source", src.String())

			return ""
		}

		var hash string
		if hash, err = backend.Fingerprint(stored); err == nil {
			return hash
		}
	}

	if ctx.Err() != nil {
		// The caller stopped waiting, e.g. at shutdown.
		b.log.DebugContext(ctx, "policy not read back after the write", "source", src.String(), "error", err)
	} else {
		b.log.WarnContext(ctx, "could not read back the policy written to kopia server", "source", src.String(), "error", err)
	}

	return ""
}

// sameSchedule reports whether two policies have the same pause and cron.
func sameSchedule(a, b json.RawMessage) bool {
	type schedule struct {
		Scheduling struct {
			Manual bool     `json:"manual"`
			Cron   []string `json:"cron"`
		} `json:"scheduling"`
	}

	var x, y schedule

	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}

	return x.Scheduling.Manual == y.Scheduling.Manual && slices.Equal(x.Scheduling.Cron, y.Scheduling.Cron)
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

// lookup checks that src is a complete source of kopia server's identity
// with an absolute path, and returns its status; nil if kopia server does
// not know it.
func (b *Backend) lookup(ctx context.Context, op string, src backend.Source) (*backend.SourceStatus, error) {
	if err := src.Validate(); err != nil {
		return nil, &backend.Error{Op: op, Source: src, Err: err}
	}

	if !path.IsAbs(src.Path) {
		return nil, &backend.Error{Op: op, Source: src, Err: fmt.Errorf("source path %q is not absolute", src.Path)}
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
			Pins:        slices.Clone(m.Pins),
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
