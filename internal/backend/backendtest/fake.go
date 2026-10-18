// Package backendtest provides Fake, an in-memory backend.Backend for
// unit tests of the reconciler.
//
// Fake behaves like a small kopia server. It knows sources and the policies
// defined for their paths, plans with backend.NewPlan as every Backend
// does, and rejects what kopia server rejects: a scheduling section that
// combines manual with anything else, a path with a trailing slash, a
// source of another identity, a path that does not exist on registration,
// a cron expression marked invalid. It can also rewrite stored policies the
// way kopia server does (SetNormalizer).
//
// Every call is recorded. Any call can fail before it takes effect
// (FailNext) or after (FailNextAfter, as when kopia server stored a policy
// but the answer was lost), and a registration can stop after the POST
// (FailRegisterPut). A hook sees every call and may change the fake, for
// example to simulate an edit in the web UI between two calls.
package backendtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// Identity of a new Fake.
const (
	DefaultUserName = "root"
	DefaultHost     = "kopia"
	// DefaultHome is the home directory relative paths resolve against.
	DefaultHome = "/root"
)

// Method names a Backend method in calls and failures.
type Method string

// Methods.
const (
	ServerIdentity   Method = "ServerIdentity"
	ResolvePath      Method = "ResolvePath"
	ListSources      Method = "ListSources"
	PlanSource       Method = "PlanSource"
	ApplyPlan        Method = "ApplyPlan"
	PreviewSchedule  Method = "PreviewSchedule"
	DeletePolicy     Method = "DeletePolicy"
	TriggerSnapshot  Method = "TriggerSnapshot"
	WaitSnapshot     Method = "WaitSnapshot"
	AnnotateSnapshot Method = "AnnotateSnapshot"
)

// Call is one recorded call. Only the fields of the method's arguments are
// set.
type Call struct {
	Method Method
	// Source is the source the call is about: the argument,
	// Desired.Source, Plan.Source or the ticket's source.
	Source backend.Source

	Path        string                 // ResolvePath
	Desired     backend.Desired        // PlanSource
	Last        backend.Applied        // PlanSource
	Plan        backend.Plan           // ApplyPlan
	Policy      json.RawMessage        // PreviewSchedule
	N           int                    // PreviewSchedule
	Ticket      backend.SnapshotTicket // WaitSnapshot
	StartTime   time.Time              // AnnotateSnapshot
	Description string                 // AnnotateSnapshot
	AddPins     []string               // AnnotateSnapshot
}

// Fake is an in-memory backend.Backend. It is safe for concurrent use.
// Create it with New.
type Fake struct {
	mu sync.Mutex

	userName, host, home string
	now                  func() time.Time
	normalize            func(json.RawMessage) json.RawMessage

	sources  map[backend.Source]*sourceState
	missing  map[string]bool // paths that do not exist on kopia server
	badCrons map[string]bool
	upcoming []time.Time

	calls       []Call
	failBefore  map[Method][]error
	failAfter   map[Method][]error
	failRegPut  []error
	hook        func(Call) error
	results     map[backend.Source][]backend.SnapshotResult
	nextTaskID  int
	annotations map[backend.Source][]Annotation
}

type sourceState struct {
	status backend.SourceStatus
	// policy is the policy defined for the path; nil if none.
	policy json.RawMessage
	// loaded reports whether kopia server's scheduler has the policy: true
	// after a PUT, false after a POST alone.
	loaded    bool
	snapshots []backend.SnapshotInfo
}

// Annotation is one AnnotateSnapshot call that found its snapshot.
type Annotation struct {
	StartTime   time.Time
	Description string
	AddPins     []string
}

// New returns a Fake with the identity root@kopia, no sources, every path
// existing and the real clock.
func New() *Fake {
	return &Fake{
		userName:    DefaultUserName,
		host:        DefaultHost,
		home:        DefaultHome,
		now:         time.Now,
		sources:     map[backend.Source]*sourceState{},
		missing:     map[string]bool{},
		badCrons:    map[string]bool{},
		failBefore:  map[Method][]error{},
		failAfter:   map[Method][]error{},
		results:     map[backend.Source][]backend.SnapshotResult{},
		annotations: map[backend.Source][]Annotation{},
	}
}

var _ backend.Backend = (*Fake)(nil)

// Source returns the source of path p under the fake's identity.
func (f *Fake) Source(p string) backend.Source {
	f.mu.Lock()
	defer f.mu.Unlock()

	return backend.Source{Host: f.host, UserName: f.userName, Path: p}
}

// SetIdentity changes the user name and host of kopia server, as when it
// connects to the repository under another name.
func (f *Fake) SetIdentity(userName, host string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.userName, f.host = userName, host
}

// SetClock replaces the clock used for tickets.
func (f *Fake) SetClock(now func() time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = now
}

// SetNormalizer makes the fake store every written policy as fn returns
// it, the way kopia server drops unknown fields and fills in defaults. Nil
// stores policies as written.
func (f *Fake) SetNormalizer(fn func(json.RawMessage) json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.normalize = fn
}

// AddSource makes kopia server know src without a policy, as after a
// snapshot taken with the kopia CLI.
func (f *Fake) AddSource(src backend.Source) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.source(src)
}

// SetPolicy makes kopia server know src with policy defined for its path,
// as after an edit in the web UI. It panics on a policy that is not a JSON
// object.
func (f *Fake) SetPolicy(src backend.Source, policy string) {
	if !json.Valid([]byte(policy)) || !strings.HasPrefix(strings.TrimSpace(policy), "{") {
		panic("backendtest: SetPolicy needs a JSON object, got " + policy)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	s := f.source(src)
	s.policy = json.RawMessage(policy)
	s.loaded = true
}

// Policy returns the policy defined for the path of src, and whether there
// is one.
func (f *Fake) Policy(src backend.Source) (json.RawMessage, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sources[src]
	if !ok || s.policy == nil {
		return nil, false
	}

	return slices.Clone(s.policy), true
}

// Known reports whether kopia server knows src.
func (f *Fake) Known(src backend.Source) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.sources[src]

	return ok
}

// Loaded reports whether kopia server's scheduler has the current policy
// of src: a registration whose PUT never came leaves it false.
func (f *Fake) Loaded(src backend.Source) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sources[src]

	return ok && s.loaded
}

// SetStatus sets what ListSources and PlanSource report for src, which
// becomes known. The Source field of status is ignored.
func (f *Fake) SetStatus(src backend.Source, status backend.SourceStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	status.Source = src
	f.source(src).status = status
}

// AddSnapshot adds a snapshot of src, which becomes known; it becomes the
// last snapshot if it started last.
func (f *Fake) AddSnapshot(src backend.Source, snap backend.SnapshotInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.addSnapshot(f.source(src), snap)
}

func (f *Fake) addSnapshot(s *sourceState, snap backend.SnapshotInfo) {
	s.snapshots = append(s.snapshots, snap)

	if last := s.status.LastSnapshot; last == nil || !snap.StartTime.Before(last.StartTime) {
		s.status.LastSnapshot = &snap
	}
}

// Snapshots returns the snapshots of src in the order they were added.
func (f *Fake) Snapshots(src backend.Source) []backend.SnapshotInfo {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, ok := f.sources[src]
	if !ok {
		return nil
	}

	return slices.Clone(s.snapshots)
}

// SetPathMissing makes registering a source with path p fail with
// ErrConfig, as kopia server does for a path that does not exist.
func (f *Fake) SetPathMissing(p string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.missing[p] = true
}

// RejectCron makes kopia server reject the cron expressions: storing a
// policy with one of them fails with ErrConfig, and PreviewSchedule
// reports it as kopia server's schedulingError does.
func (f *Fake) RejectCron(exprs ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, e := range exprs {
		f.badCrons[e] = true
	}
}

// SetUpcoming sets the times PreviewSchedule returns for a policy that is
// not paused; it returns at most n of them.
func (f *Fake) SetUpcoming(times ...time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.upcoming = slices.Clone(times)
}

// QueueSnapshotResult queues the result of the next WaitSnapshot for src.
// Without one, WaitSnapshot saves a complete snapshot started when the
// ticket was triggered and reports success.
func (f *Fake) QueueSnapshotResult(src backend.Source, r backend.SnapshotResult) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.results[src] = append(f.results[src], r)
}

// Annotations returns the AnnotateSnapshot calls that found their
// snapshot of src.
func (f *Fake) Annotations(src backend.Source) []Annotation {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.annotations[src])
}

// FailNext makes the next call of m return err without any effect.
// Several failures queue up in order.
func (f *Fake) FailNext(m Method, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.failBefore[m] = append(f.failBefore[m], err)
}

// FailNextAfter makes the next call of m take effect and then return err,
// as when kopia server stored a policy but the answer was lost.
func (f *Fake) FailNextAfter(m Method, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.failAfter[m] = append(f.failAfter[m], err)
}

// FailRegisterPut makes the next registration stop after its POST: the
// source is known with the policy, but kopia server's scheduler does not
// have it, and ApplyPlan returns err.
func (f *Fake) FailRegisterPut(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.failRegPut = append(f.failRegPut, err)
}

// SetHook installs fn, called with every call after it is recorded and
// before it takes effect; a non-nil error is returned instead. fn runs
// without the fake locked, so it may call the fake's setters. Nil removes
// the hook.
func (f *Fake) SetHook(fn func(Call) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hook = fn
}

// Calls returns the recorded calls in order.
func (f *Fake) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

// CallsTo returns the recorded calls of m in order.
func (f *Fake) CallsTo(m Method) []Call {
	return f.filter(func(c Call) bool { return c.Method == m })
}

// CallsFor returns the recorded calls of m about src in order.
func (f *Fake) CallsFor(src backend.Source, m Method) []Call {
	return f.filter(func(c Call) bool { return c.Method == m && c.Source == src })
}

func (f *Fake) filter(keep func(Call) bool) []Call {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Call

	for _, c := range f.calls {
		if keep(c) {
			out = append(out, c)
		}
	}

	return out
}

// ResetCalls forgets the recorded calls.
func (f *Fake) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = nil
}

// enter records c and returns the error the call fails with before taking
// effect, if any. It must be called without f.mu held.
func (f *Fake) enter(ctx context.Context, c Call) error {
	f.mu.Lock()

	f.calls = append(f.calls, c)

	var err error

	if q := f.failBefore[c.Method]; len(q) > 0 {
		f.failBefore[c.Method], err = q[1:], q[0]
	}

	hook := f.hook

	f.mu.Unlock()

	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return &backend.Error{Op: string(c.Method), Source: c.Source, Err: err}
	}

	if hook != nil {
		return hook(c)
	}

	return nil
}

// leave returns the error the call fails with after taking effect, if any.
// f.mu is held.
func (f *Fake) leave(m Method) error {
	q := f.failAfter[m]
	if len(q) == 0 {
		return nil
	}

	f.failAfter[m] = q[1:]

	return q[0]
}

// source returns the state of src, adding it. f.mu is held.
func (f *Fake) source(src backend.Source) *sourceState {
	s, ok := f.sources[src]
	if !ok {
		s = &sourceState{status: backend.SourceStatus{Source: src, Status: backend.StatusIdle}, loaded: true}
		f.sources[src] = s
	}

	return s
}

// own checks that src is a complete source of kopia server's identity.
// f.mu is held.
func (f *Fake) own(op Method, src backend.Source) error {
	if err := src.Validate(); err != nil {
		return &backend.Error{Op: string(op), Source: src, Err: err}
	}

	if src.Host != f.host || src.UserName != f.userName {
		return &backend.Error{Op: string(op), Source: src,
			Err: fmt.Errorf("%w: kopia server is %s@%s", backend.ErrWrongIdentity, f.userName, f.host)}
	}

	return nil
}

func configErr(op Method, src backend.Source, format string, args ...any) error {
	return &backend.Error{Op: string(op), Source: src, Kind: backend.ErrConfig, Err: fmt.Errorf(format, args...)}
}

// ServerIdentity implements backend.Backend.
func (f *Fake) ServerIdentity(ctx context.Context) (string, string, error) {
	if err := f.enter(ctx, Call{Method: ServerIdentity}); err != nil {
		return "", "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.userName, f.host, f.leave(ServerIdentity)
}

// ResolvePath implements backend.Backend like paths/resolve: a relative
// path is resolved against DefaultHome, then the path is cleaned.
// Existence is not checked.
func (f *Fake) ResolvePath(ctx context.Context, p string) (backend.Source, error) {
	if err := f.enter(ctx, Call{Method: ResolvePath, Path: p}); err != nil {
		return backend.Source{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if p == "" {
		return backend.Source{}, &backend.Error{Op: string(ResolvePath), Err: errors.New("empty path")}
	}

	if !path.IsAbs(p) {
		p = path.Join(f.home, p)
	}

	return backend.Source{Host: f.host, UserName: f.userName, Path: path.Clean(p)}, f.leave(ResolvePath)
}

// ListSources implements backend.Backend: the known sources of the fake's
// identity, sorted by path.
func (f *Fake) ListSources(ctx context.Context) ([]backend.SourceStatus, error) {
	if err := f.enter(ctx, Call{Method: ListSources}); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var out []backend.SourceStatus

	for _, src := range slices.SortedFunc(maps.Keys(f.sources), backend.Source.Compare) {
		if src.Host == f.host && src.UserName == f.userName {
			out = append(out, f.status(src))
		}
	}

	return out, f.leave(ListSources)
}

// status returns a copy of the status of a known source. f.mu is held.
func (f *Fake) status(src backend.Source) backend.SourceStatus {
	st := f.sources[src].status
	if st.LastSnapshot != nil {
		last := *st.LastSnapshot
		st.LastSnapshot = &last
	}

	return st
}

// PlanSource implements backend.Backend with backend.NewPlan.
func (f *Fake) PlanSource(ctx context.Context, want backend.Desired, last backend.Applied) (backend.Plan, error) {
	src := want.Source
	if err := f.enter(ctx, Call{Method: PlanSource, Source: src, Desired: want, Last: last}); err != nil {
		return backend.Plan{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(PlanSource, src); err != nil {
		return backend.Plan{}, err
	}

	var (
		status  *backend.SourceStatus
		current json.RawMessage
	)

	s, known := f.sources[src]
	if known {
		st := f.status(src)
		status = &st
		current = slices.Clone(s.policy)
	}

	plan, err := backend.NewPlan(want, last, known, status, current)
	if err != nil {
		return backend.Plan{}, err
	}

	return plan, f.leave(PlanSource)
}

// ApplyPlan implements backend.Backend. Like kopia server it validates
// the policy, and a registration fails for a path that does not exist.
// The returned Applied carries the fingerprint of the policy as stored.
func (f *Fake) ApplyPlan(ctx context.Context, plan backend.Plan) (backend.Applied, error) {
	src := plan.Source
	if err := f.enter(ctx, Call{Method: ApplyPlan, Source: src, Plan: plan}); err != nil {
		return backend.Applied{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if plan.Action == backend.ActionNone {
		return plan.Applied, f.leave(ApplyPlan)
	}

	if err := f.own(ApplyPlan, src); err != nil {
		return backend.Applied{}, err
	}

	if err := f.validatePolicy(ApplyPlan, src, plan.Policy); err != nil {
		return backend.Applied{}, err
	}

	stored := slices.Clone(plan.Policy)
	if f.normalize != nil {
		stored = f.normalize(stored)
	}

	switch plan.Action {
	case backend.ActionRegister:
		if f.missing[src.Path] {
			return backend.Applied{}, configErr(ApplyPlan, src, "path %s does not exist on kopia server", src.Path)
		}

		s := f.source(src)
		s.policy = stored
		s.loaded = false

		if q := f.failRegPut; len(q) > 0 {
			f.failRegPut = q[1:]

			return backend.Applied{}, q[0]
		}

		s.loaded = true
	case backend.ActionUpdate:
		s := f.source(src)
		s.policy = stored
		s.loaded = true
	default:
		return backend.Applied{}, &backend.Error{Op: string(ApplyPlan), Source: src, Err: fmt.Errorf("unknown action %v", plan.Action)}
	}

	applied := plan.Applied

	hash, err := backend.Fingerprint(stored)
	if err != nil {
		return backend.Applied{}, &backend.Error{Op: string(ApplyPlan), Source: src, Err: err}
	}

	applied.StoredHash = hash

	return applied, f.leave(ApplyPlan)
}

// scheduling decodes the scheduling section of a policy.
func scheduling(op Method, src backend.Source, policy json.RawMessage) (map[string]json.RawMessage, error) {
	var p struct {
		Scheduling map[string]json.RawMessage `json:"scheduling"`
	}

	if err := json.Unmarshal(policy, &p); err != nil {
		return nil, configErr(op, src, "malformed policy: %v", err)
	}

	return p.Scheduling, nil
}

// validatePolicy rejects what kopia server's policy validation rejects.
// f.mu is held.
func (f *Fake) validatePolicy(op Method, src backend.Source, policy json.RawMessage) error {
	sched, err := scheduling(op, src, policy)
	if err != nil {
		return err
	}

	if src.Path != "/" && strings.HasSuffix(src.Path, "/") {
		return configErr(op, src, "invalid policy path %q: trailing slash", src.Path)
	}

	if m, ok := sched["manual"]; ok && string(m) == "true" && len(sched) > 1 {
		return configErr(op, src, "failed to validate policy: manual cannot be combined with other scheduling policies")
	}

	return f.checkCrons(op, src, sched["cron"])
}

// checkCrons reports a cron expression marked with RejectCron. f.mu is held.
func (f *Fake) checkCrons(op Method, src backend.Source, raw json.RawMessage) error {
	if raw == nil {
		return nil
	}

	var crons []string
	if err := json.Unmarshal(raw, &crons); err != nil {
		return configErr(op, src, "malformed cron list: %v", err)
	}

	for _, c := range crons {
		if f.badCrons[c] {
			return configErr(op, src, "invalid cron expression %q", c)
		}
	}

	return nil
}

// PreviewSchedule implements backend.Backend from the policy it is given,
// as kopia server does: no times for a paused one, otherwise the first n
// of SetUpcoming's times. The scheduling section is the policy's own; the
// fake has no parent policies. A cron marked with RejectCron is an
// ErrConfig error.
func (f *Fake) PreviewSchedule(ctx context.Context, src backend.Source, policy json.RawMessage, n int) (backend.SchedulePreview, error) {
	if err := f.enter(ctx, Call{Method: PreviewSchedule, Source: src, Policy: slices.Clone(policy), N: n}); err != nil {
		return backend.SchedulePreview{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(PreviewSchedule, src); err != nil {
		return backend.SchedulePreview{}, err
	}

	sched, err := scheduling(PreviewSchedule, src, policy)
	if err != nil {
		return backend.SchedulePreview{}, err
	}

	if err := f.checkCrons(PreviewSchedule, src, sched["cron"]); err != nil {
		return backend.SchedulePreview{}, err
	}

	if sched == nil {
		sched = map[string]json.RawMessage{}
	}

	section, err := json.Marshal(sched)
	if err != nil {
		return backend.SchedulePreview{}, &backend.Error{Op: string(PreviewSchedule), Source: src, Err: err}
	}

	preview := backend.SchedulePreview{Scheduling: section}

	if string(sched["manual"]) != "true" {
		preview.Times = slices.Clone(f.upcoming[:min(max(n, 0), len(f.upcoming))])
	}

	return preview, f.leave(PreviewSchedule)
}

// DeletePolicy implements backend.Backend: the policy goes, the source
// stays known.
func (f *Fake) DeletePolicy(ctx context.Context, src backend.Source) error {
	if err := f.enter(ctx, Call{Method: DeletePolicy, Source: src}); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(DeletePolicy, src); err != nil {
		return err
	}

	if s, ok := f.sources[src]; ok {
		s.policy = nil
		s.loaded = true
	}

	return f.leave(DeletePolicy)
}

// TriggerSnapshot implements backend.Backend. An unknown source is an
// ErrTemporary error, like the 404 of sources/upload.
func (f *Fake) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	if err := f.enter(ctx, Call{Method: TriggerSnapshot, Source: src}); err != nil {
		return backend.SnapshotTicket{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(TriggerSnapshot, src); err != nil {
		return backend.SnapshotTicket{}, err
	}

	s, ok := f.sources[src]
	if !ok {
		return backend.SnapshotTicket{}, &backend.Error{Op: string(TriggerSnapshot), Source: src, Kind: backend.ErrTemporary,
			Err: errors.New("kopia server does not know the source yet")}
	}

	t := backend.SnapshotTicket{Source: src, TriggeredAt: f.now()}
	if s.status.LastSnapshot != nil {
		t.LastSnapshotStart = s.status.LastSnapshot.StartTime
	}

	return t, f.leave(TriggerSnapshot)
}

// WaitSnapshot implements backend.Backend. It returns the next result
// queued for the ticket's source, saving its snapshot if it has one, or
// else saves a complete snapshot started when the ticket was triggered and
// reports success. A result with a task ID is passed to save first.
func (f *Fake) WaitSnapshot(ctx context.Context, t backend.SnapshotTicket, save func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	if err := f.enter(ctx, Call{Method: WaitSnapshot, Source: t.Source, Ticket: t}); err != nil {
		return backend.SnapshotResult{}, err
	}

	f.mu.Lock()

	if err := f.own(WaitSnapshot, t.Source); err != nil {
		f.mu.Unlock()

		return backend.SnapshotResult{}, err
	}

	var r backend.SnapshotResult

	if q := f.results[t.Source]; len(q) > 0 {
		f.results[t.Source], r = q[1:], q[0]
	} else {
		f.nextTaskID++
		r = backend.SnapshotResult{
			Outcome:  backend.SnapshotSucceeded,
			TaskID:   fmt.Sprint(f.nextTaskID),
			Snapshot: &backend.SnapshotInfo{StartTime: t.TriggeredAt, EndTime: t.TriggeredAt},
		}
	}

	if r.Snapshot != nil {
		f.addSnapshot(f.source(t.Source), *r.Snapshot)
	}

	err := f.leave(WaitSnapshot)

	f.mu.Unlock()

	if r.TaskID != "" && save != nil {
		t.TaskID = r.TaskID
		t.TaskDescription = t.Source.String() + " at " + t.TriggeredAt.Format(time.RFC3339)
		save(t)
	}

	return r, err
}

// AnnotateSnapshot implements backend.Backend: it finds the snapshot like
// a real backend (the start time, not a checkpoint, the one that ended
// first), sets its description and records the annotation.
func (f *Fake) AnnotateSnapshot(ctx context.Context, src backend.Source, startTime time.Time, description string, addPins []string) error {
	c := Call{Method: AnnotateSnapshot, Source: src, StartTime: startTime, Description: description, AddPins: slices.Clone(addPins)}
	if err := f.enter(ctx, c); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(AnnotateSnapshot, src); err != nil {
		return err
	}

	s, ok := f.sources[src]
	if !ok {
		return &backend.Error{Op: string(AnnotateSnapshot), Source: src, Err: backend.ErrSnapshotNotFound}
	}

	found := -1

	for i, snap := range s.snapshots {
		if !snap.StartTime.Equal(startTime) || snap.Incomplete == "checkpoint" {
			continue
		}

		if found < 0 || snap.EndTime.Before(s.snapshots[found].EndTime) {
			found = i
		}
	}

	if found < 0 {
		return &backend.Error{Op: string(AnnotateSnapshot), Source: src, Err: backend.ErrSnapshotNotFound}
	}

	s.snapshots[found].Description = description
	f.annotations[src] = append(f.annotations[src], Annotation{StartTime: startTime, Description: description, AddPins: slices.Clone(addPins)})

	return f.leave(AnnotateSnapshot)
}
