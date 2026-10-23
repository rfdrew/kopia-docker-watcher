// Package backendtest provides Fake, an in-memory backend.Backend for
// unit tests of the reconciler.
//
// Fake behaves like a small kopia server. It knows sources and the policies
// defined for their paths, plans with backend.NewPlan as every Backend
// does, and rejects some of what kopia server rejects: a scheduling section
// that combines manual with anything else, a path with a trailing slash,
// upload.maxParallelSnapshots on a path (only when the policy is stored,
// as kopia server's policy/resolve does not check it), a source of another
// identity or with a path that is not absolute, a path that does not exist
// on registration, a cron expression
// marked invalid, a policy a check set with SetPolicyCheck refuses. It can also rewrite stored policies the way kopia
// server does (SetNormalizer). A source without snapshots is forgotten when
// its policy is deleted, as kopia server forgets it. DeleteSnapshots
// deletes the snapshots without a pin, or the source as a whole, as the
// REST backend does.
//
// Every call is recorded, as a deep copy. Any call can fail before it takes
// effect (FailNext) or after (FailNextAfter, as when kopia server stored a
// policy but the answer was lost), for any source or for one, and a
// registration can stop after the POST (FailRegisterPut). A failed call
// returns zero values. A hook sees every call and may change the fake, for
// example to simulate an edit in the web UI between two calls.
//
// A context that is done fails the call before it takes effect, like a
// request that is never sent, with an error of neither kind that wraps
// ctx.Err().
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

// DefaultAlgorithms are the compression algorithms of a new Fake, a part
// of what kopia 0.23 lists.
var DefaultAlgorithms = []backend.Algorithm{
	{Name: "deflate-default"},
	{Name: "gzip"},
	{Name: "lz4"},
	{Name: "pgzip"},
	{Name: "s2-default"},
	{Name: "zstd"},
	{Name: "zstd-fastest"},
}

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
	ServerIdentity Method = "ServerIdentity"
	ResolvePath    Method = "ResolvePath"
	ListSources    Method = "ListSources"
	// CompressionAlgorithms is recorded on every call; the Fake keeps no
	// cache of its own.
	CompressionAlgorithms Method = "CompressionAlgorithms"
	PlanSource            Method = "PlanSource"
	ApplyPlan             Method = "ApplyPlan"
	PreviewSchedule       Method = "PreviewSchedule"
	DeletePolicy          Method = "DeletePolicy"
	TriggerSnapshot       Method = "TriggerSnapshot"
	WaitSnapshot          Method = "WaitSnapshot"
	AnnotateSnapshot      Method = "AnnotateSnapshot"
	ListSnapshots         Method = "ListSnapshots"
	DeleteSnapshots       Method = "DeleteSnapshots"
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
	Pinned      bool                   // DeleteSnapshots
}

func (c Call) clone() Call {
	c.Desired = c.Desired.Clone()
	c.Last = c.Last.Clone()
	c.Plan = c.Plan.Clone()
	c.Policy = slices.Clone(c.Policy)
	c.Ticket.TasksBefore = slices.Clone(c.Ticket.TasksBefore)
	c.AddPins = slices.Clone(c.AddPins)

	return c
}

// failKey selects the calls a failure is for: one method, and one source
// or, with the zero Source, any.
type failKey struct {
	method Method
	source backend.Source
}

// Fake is an in-memory backend.Backend. It is safe for concurrent use.
// Create it with New.
type Fake struct {
	mu sync.Mutex

	userName, host, home string
	now                  func() time.Time
	normalize            func(json.RawMessage) json.RawMessage
	check                func(json.RawMessage) error

	sources  map[backend.Source]*sourceState
	missing  map[string]bool // paths that do not exist on kopia server
	badCrons map[string]bool
	upcoming []time.Time
	// algorithms are the compression algorithms kopia server supports.
	algorithms []backend.Algorithm

	calls       []Call
	failBefore  map[failKey][]error
	failAfter   map[failKey][]error
	failRegPut  map[backend.Source][]error
	hook        func(Call) error
	results     map[backend.Source][]backend.SnapshotResult
	nextTaskID  int
	annotations map[backend.Source][]Annotation
	holds       map[backend.Source]chan struct{}
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
		algorithms:  slices.Clone(DefaultAlgorithms),
		failBefore:  map[failKey][]error{},
		failAfter:   map[failKey][]error{},
		failRegPut:  map[backend.Source][]error{},
		results:     map[backend.Source][]backend.SnapshotResult{},
		annotations: map[backend.Source][]Annotation{},
		holds:       map[backend.Source]chan struct{}{},
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

// SetClock replaces the clock used for tickets. now runs with the fake
// locked and must not call it.
func (f *Fake) SetClock(now func() time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = now
}

// SetCompressionAlgorithms replaces the compression algorithms kopia
// server supports.
func (f *Fake) SetCompressionAlgorithms(algos ...backend.Algorithm) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.algorithms = slices.Clone(algos)
}

// SetNormalizer makes the fake store every written policy as fn returns
// it, the way kopia server drops unknown fields and fills in defaults. Nil
// stores policies as written. fn runs with the fake locked and must not
// call it.
func (f *Fake) SetNormalizer(fn func(json.RawMessage) json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.normalize = fn
}

// SetPolicyCheck installs fn, which checks a policy the way kopia server
// decodes and validates it, for the fields the test cares about. A policy
// fn rejects makes ApplyPlan and PreviewSchedule fail with ErrConfig, like
// MALFORMED_REQUEST or "failed to validate policy". Nil removes the check.
// fn runs with the fake locked and must not call it.
func (f *Fake) SetPolicyCheck(fn func(policy json.RawMessage) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.check = fn
}

// AddSource makes kopia server know src without a policy and without
// snapshots, as between a snapshot taken with the kopia CLI and its
// deletion; AddSnapshot adds snapshots. DeletePolicy forgets a source
// without snapshots.
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
// becomes known. The Source field of status is ignored. A nil LastSnapshot
// keeps the last snapshot the fake has.
func (f *Fake) SetStatus(src backend.Source, status backend.SourceStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s := f.source(src)
	status = status.Clone()
	status.Source = src

	if status.LastSnapshot == nil {
		status.LastSnapshot = s.status.LastSnapshot
	}

	s.status = status
}

// AddSnapshot adds a snapshot of src, which becomes known; it becomes the
// last snapshot if it started last.
func (f *Fake) AddSnapshot(src backend.Source, snap backend.SnapshotInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()

	addSnapshot(f.source(src), snap)
}

func addSnapshot(s *sourceState, snap backend.SnapshotInfo) {
	snap = snap.Clone()
	s.snapshots = append(s.snapshots, snap)

	if last := s.status.LastSnapshot; last == nil || !snap.StartTime.Before(last.StartTime) {
		last := snap.Clone()
		s.status.LastSnapshot = &last
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

	return cloneSnapshots(s.snapshots)
}

func cloneSnapshots(snaps []backend.SnapshotInfo) []backend.SnapshotInfo {
	out := make([]backend.SnapshotInfo, len(snaps))
	for i, s := range snaps {
		out[i] = s.Clone()
	}

	return out
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

// HoldSnapshots makes the WaitSnapshot calls for src hold, as while a
// snapshot runs: a call gives a ticket without a task a task ID and saves
// it, then waits until ReleaseSnapshots or until its context is done,
// when it returns an error of neither kind that wraps ctx.Err().
func (f *Fake) HoldSnapshots(src backend.Source) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.holds[src]; !ok {
		f.holds[src] = make(chan struct{})
	}
}

// ReleaseSnapshots lets the held WaitSnapshot calls for src go on; later
// calls do not hold.
func (f *Fake) ReleaseSnapshots(src backend.Source) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ch, ok := f.holds[src]; ok {
		close(ch)
		delete(f.holds, src)
	}
}

// Annotations returns the AnnotateSnapshot calls that found their
// snapshot of src.
func (f *Fake) Annotations(src backend.Source) []Annotation {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.annotations[src])
}

// FailNext makes the next call of m fail with err without any effect.
// Failures queue up in order.
func (f *Fake) FailNext(m Method, err error) {
	f.FailNextFor(backend.Source{}, m, err)
}

// FailNextFor is FailNext for the next call of m about src only.
func (f *Fake) FailNextFor(src backend.Source, m Method, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := failKey{m, src}
	f.failBefore[k] = append(f.failBefore[k], err)
}

// FailNextAfter makes the next call of m take effect and then fail with
// err, as when kopia server stored a policy but the answer was lost. The
// failure is used up by that call even if it fails for another reason
// first. ApplyPlan of ActionNone sends nothing and cannot fail; it does
// not use up failures.
func (f *Fake) FailNextAfter(m Method, err error) {
	f.FailNextAfterFor(backend.Source{}, m, err)
}

// FailNextAfterFor is FailNextAfter for the next call of m about src only.
// It panics on an ErrConfig error of ApplyPlan: kopia server checks a
// policy before it stores it, so a write that took effect is never
// refused (see backend.ActionRegister).
func (f *Fake) FailNextAfterFor(src backend.Source, m Method, err error) {
	if m == ApplyPlan && backend.IsConfig(err) {
		panic("backendtest: an ApplyPlan that took effect cannot fail with ErrConfig")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	k := failKey{m, src}
	f.failAfter[k] = append(f.failAfter[k], err)
}

// FailRegisterPut makes the next registration stop after its POST: the
// source is known with the policy, but kopia server's scheduler does not
// have it, and ApplyPlan fails with err. A registration that fails before
// the POST uses it up too. It panics on an ErrConfig error, which a
// registration returns only when kopia server stored nothing (see
// backend.ActionRegister).
func (f *Fake) FailRegisterPut(err error) {
	f.FailRegisterPutFor(backend.Source{}, err)
}

// FailRegisterPutFor is FailRegisterPut for the next registration of src
// only.
func (f *Fake) FailRegisterPutFor(src backend.Source, err error) {
	if backend.IsConfig(err) {
		panic("backendtest: a registration whose POST succeeded cannot fail with ErrConfig")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.failRegPut[src] = append(f.failRegPut[src], err)
}

// SetHook installs fn, called with every call right after it is recorded,
// before any failure set with FailNext applies; a non-nil error fails the
// call without effect. fn runs without the fake locked, so it may call the
// fake's setters. Nil removes the hook.
func (f *Fake) SetHook(fn func(Call) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.hook = fn
}

// Calls returns the recorded calls in order.
func (f *Fake) Calls() []Call {
	return f.filter(func(Call) bool { return true })
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
			out = append(out, c.clone())
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

// record adds c to the recorded calls.
func (f *Fake) record(c Call) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, c.clone())
}

// enter records c and decides how the call fails: err fails it before it
// takes effect; after, if not nil, fails it once it took effect. It must be
// called without f.mu held.
func (f *Fake) enter(ctx context.Context, c Call) (after, err error) {
	f.record(c)

	f.mu.Lock()
	hook := f.hook
	f.mu.Unlock()

	if hook != nil {
		if err := hook(c.clone()); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	before := pop(f.failBefore, c)
	after = pop(f.failAfter, c)
	f.mu.Unlock()

	if before != nil {
		return nil, before
	}

	if err := ctx.Err(); err != nil {
		return nil, &backend.Error{Op: string(c.Method), Source: c.Source, Err: err}
	}

	return after, nil
}

// pop takes the first failure queued for c's method and source, or else
// for its method and any source.
func pop(queues map[failKey][]error, c Call) error {
	for _, k := range []failKey{{c.Method, c.Source}, {c.Method, backend.Source{}}} {
		if q := queues[k]; len(q) > 0 {
			queues[k] = q[1:]

			return q[0]
		}
	}

	return nil
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

// own checks that src is a complete source of kopia server's identity
// with an absolute path. f.mu is held.
func (f *Fake) own(op Method, src backend.Source) error {
	if err := src.Validate(); err != nil {
		return &backend.Error{Op: string(op), Source: src, Err: err}
	}

	if !path.IsAbs(src.Path) {
		return &backend.Error{Op: string(op), Source: src, Err: fmt.Errorf("source path %q is not absolute", src.Path)}
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
	after, err := f.enter(ctx, Call{Method: ServerIdentity})
	if err != nil {
		return "", "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if after != nil {
		return "", "", after
	}

	return f.userName, f.host, nil
}

// ResolvePath implements backend.Backend like paths/resolve: a leading ~
// is replaced by DefaultHome, a relative path is resolved against it, then
// the path is cleaned. Existence is not checked. An empty path is an
// error, as the REST backend rejects it before sending.
func (f *Fake) ResolvePath(ctx context.Context, p string) (backend.Source, error) {
	after, err := f.enter(ctx, Call{Method: ResolvePath, Path: p})
	if err != nil {
		return backend.Source{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if p == "" {
		return backend.Source{}, &backend.Error{Op: string(ResolvePath), Err: errors.New("empty path")}
	}

	if after != nil {
		return backend.Source{}, after
	}

	switch {
	case strings.HasPrefix(p, "~"):
		p = f.home + p[1:]
	case !path.IsAbs(p):
		p = path.Join(f.home, p)
	}

	return backend.Source{Host: f.host, UserName: f.userName, Path: path.Clean(p)}, nil
}

// ListSources implements backend.Backend: the known sources of the fake's
// identity, sorted by path.
func (f *Fake) ListSources(ctx context.Context) ([]backend.SourceStatus, error) {
	after, err := f.enter(ctx, Call{Method: ListSources})
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if after != nil {
		return nil, after
	}

	var out []backend.SourceStatus

	for _, src := range slices.SortedFunc(maps.Keys(f.sources), backend.Source.Compare) {
		if src.Host == f.host && src.UserName == f.userName {
			out = append(out, f.sources[src].status.Clone())
		}
	}

	return out, nil
}

// CompressionAlgorithms implements backend.Backend with the algorithms of
// SetCompressionAlgorithms, DefaultAlgorithms at first, sorted by name.
func (f *Fake) CompressionAlgorithms(ctx context.Context) ([]backend.Algorithm, error) {
	after, err := f.enter(ctx, Call{Method: CompressionAlgorithms})
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if after != nil {
		return nil, after
	}

	out := slices.Clone(f.algorithms)
	slices.SortFunc(out, func(a, b backend.Algorithm) int { return strings.Compare(a.Name, b.Name) })

	return out, nil
}

// PlanSource implements backend.Backend with backend.NewPlan.
func (f *Fake) PlanSource(ctx context.Context, want backend.Desired, last backend.Applied) (backend.Plan, error) {
	src := want.Source

	after, err := f.enter(ctx, Call{Method: PlanSource, Source: src, Desired: want, Last: last})
	if err != nil {
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
		status = &s.status
		current = s.policy
	}

	plan, err := backend.NewPlan(want, last, known, status, current)
	if err != nil {
		return backend.Plan{}, err
	}

	if after != nil {
		return backend.Plan{}, after
	}

	return plan, nil
}

// ApplyPlan implements backend.Backend. Like kopia server it validates
// the policy, and a registration fails for a path that does not exist.
// The returned Applied carries the fingerprint of the policy as stored.
// ActionNone is recorded but sends nothing: no hook or failure applies to
// it.
func (f *Fake) ApplyPlan(ctx context.Context, plan backend.Plan) (backend.Applied, error) {
	src := plan.Source
	c := Call{Method: ApplyPlan, Source: src, Plan: plan}

	if plan.Action == backend.ActionNone {
		f.record(c)

		return plan.Applied.Clone(), nil
	}

	after, err := f.enter(ctx, c)
	if err != nil {
		return backend.Applied{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var regPut error
	if plan.Action == backend.ActionRegister {
		regPut = f.popRegisterPut(src)
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

		if regPut != nil {
			return backend.Applied{}, regPut
		}

		s.loaded = true
	case backend.ActionUpdate:
		s := f.source(src)
		s.policy = stored
		s.loaded = true
	default:
		return backend.Applied{}, &backend.Error{Op: string(ApplyPlan), Source: src, Err: fmt.Errorf("unknown action %v", plan.Action)}
	}

	if after != nil {
		return backend.Applied{}, after
	}

	hash, err := backend.Fingerprint(stored)
	if err != nil {
		return backend.Applied{}, &backend.Error{Op: string(ApplyPlan), Source: src, Err: err}
	}

	applied := plan.Applied.Clone()
	applied.StoredHash = hash

	return applied, nil
}

// popRegisterPut takes the first FailRegisterPut failure for src, or else
// for any source. f.mu is held.
func (f *Fake) popRegisterPut(src backend.Source) error {
	for _, k := range []backend.Source{src, {}} {
		if q := f.failRegPut[k]; len(q) > 0 {
			f.failRegPut[k] = q[1:]

			return q[0]
		}
	}

	return nil
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

	if err := f.checkCrons(op, src, sched["cron"]); err != nil {
		return err
	}

	// kopia server validates the whole policy on a write, but
	// policy/resolve only checks the schedule; this rule is one that
	// PreviewSchedule lets through.
	var upload struct {
		Upload struct {
			MaxParallelSnapshots json.RawMessage `json:"maxParallelSnapshots"`
		} `json:"upload"`
	}

	if json.Unmarshal(policy, &upload) == nil && !isNull(upload.Upload.MaxParallelSnapshots) {
		return configErr(op, src, "failed to validate policy: max parallel snapshots cannot be specified for paths")
	}

	return f.runCheck(op, src, policy)
}

func isNull(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))

	return s == "" || s == "null"
}

// runCheck applies the check set with SetPolicyCheck. f.mu is held.
func (f *Fake) runCheck(op Method, src backend.Source, policy json.RawMessage) error {
	if f.check == nil {
		return nil
	}

	if err := f.check(slices.Clone(policy)); err != nil {
		return configErr(op, src, "policy rejected: %w", err)
	}

	return nil
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
// fake has no parent policies. A cron marked with RejectCron, manual
// combined with other scheduling fields, or a policy the check set with
// SetPolicyCheck refuses, is an ErrConfig error.
func (f *Fake) PreviewSchedule(ctx context.Context, src backend.Source, policy json.RawMessage, n int) (backend.SchedulePreview, error) {
	after, err := f.enter(ctx, Call{Method: PreviewSchedule, Source: src, Policy: policy, N: n})
	if err != nil {
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

	if m, ok := sched["manual"]; ok && string(m) == "true" && len(sched) > 1 {
		return backend.SchedulePreview{}, configErr(PreviewSchedule, src, "invalid scheduling policy: manual cannot be combined with other scheduling policies")
	}

	if err := f.runCheck(PreviewSchedule, src, policy); err != nil {
		return backend.SchedulePreview{}, err
	}

	if after != nil {
		return backend.SchedulePreview{}, after
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

	return preview, nil
}

// DeletePolicy implements backend.Backend. The policy goes; a source
// without snapshots is forgotten, as kopia server's refresh drops it, and
// one with snapshots stays known.
func (f *Fake) DeletePolicy(ctx context.Context, src backend.Source) error {
	after, err := f.enter(ctx, Call{Method: DeletePolicy, Source: src})
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(DeletePolicy, src); err != nil {
		return err
	}

	if s, ok := f.sources[src]; ok {
		if len(s.snapshots) == 0 {
			delete(f.sources, src)
		} else {
			s.policy = nil
			s.loaded = true
		}
	}

	return after
}

// TriggerSnapshot implements backend.Backend. An unknown source is an
// ErrTemporary error, like the 404 of sources/upload.
func (f *Fake) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	after, err := f.enter(ctx, Call{Method: TriggerSnapshot, Source: src})
	if err != nil {
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

	if after != nil {
		return backend.SnapshotTicket{}, after
	}

	t := backend.SnapshotTicket{Source: src, TriggeredAt: f.now()}
	if s.status.LastSnapshot != nil {
		t.LastSnapshotStart = s.status.LastSnapshot.StartTime
	}

	return t, nil
}

// WaitSnapshot implements backend.Backend. It returns the next result
// queued for the ticket's source, saving its snapshot if it has one, or
// else saves a complete snapshot started when the ticket was triggered and
// reports success, with the ticket's task ID if it has one and a task that
// started and ended when the ticket was triggered. The task ID
// of a result is saved first if the ticket does not have it yet. The snapshot of a source kopia server
// does not know is SnapshotUnknown. See HoldSnapshots for a call that
// waits.
func (f *Fake) WaitSnapshot(ctx context.Context, t backend.SnapshotTicket, save func(backend.SnapshotTicket)) (backend.SnapshotResult, error) {
	after, err := f.enter(ctx, Call{Method: WaitSnapshot, Source: t.Source, Ticket: t})
	if err != nil {
		return backend.SnapshotResult{}, err
	}

	f.mu.Lock()
	hold := f.holds[t.Source]
	saveTask := hold != nil && t.TaskID == ""

	if saveTask {
		f.nextTaskID++
		t.TaskID = fmt.Sprint(f.nextTaskID)
		t.TaskDescription = t.Source.String() + " at " + t.TriggeredAt.Format(time.RFC3339)
	}

	f.mu.Unlock()

	if hold != nil {
		if saveTask && save != nil {
			save(t)
		}

		select {
		case <-hold:
		case <-ctx.Done():
			return backend.SnapshotResult{}, &backend.Error{Op: string(WaitSnapshot), Source: t.Source, Err: ctx.Err()}
		}
	}

	r, err := f.finishSnapshot(t)
	if err != nil {
		return backend.SnapshotResult{}, err
	}

	if r.TaskID != "" && r.TaskID != t.TaskID && save != nil {
		t.TaskID = r.TaskID
		t.TaskDescription = t.Source.String() + " at " + t.TriggeredAt.Format(time.RFC3339)
		save(t)
	}

	if after != nil {
		return backend.SnapshotResult{}, after
	}

	return r, nil
}

// finishSnapshot works out how the snapshot of t ends.
func (f *Fake) finishSnapshot(t backend.SnapshotTicket) (backend.SnapshotResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(WaitSnapshot, t.Source); err != nil {
		return backend.SnapshotResult{}, err
	}

	s, ok := f.sources[t.Source]
	if !ok {
		return backend.SnapshotResult{Outcome: backend.SnapshotUnknown, Error: "kopia server does not know the source"}, nil
	}

	var r backend.SnapshotResult

	if q := f.results[t.Source]; len(q) > 0 {
		f.results[t.Source], r = q[1:], q[0]
	} else {
		id := t.TaskID
		if id == "" {
			f.nextTaskID++
			id = fmt.Sprint(f.nextTaskID)
		}

		r = backend.SnapshotResult{
			Outcome:   backend.SnapshotSucceeded,
			TaskID:    id,
			Snapshot:  &backend.SnapshotInfo{StartTime: t.TriggeredAt, EndTime: t.TriggeredAt},
			TaskStart: t.TriggeredAt,
			TaskEnd:   t.TriggeredAt,
		}
	}

	if r.Snapshot != nil {
		snap := *r.Snapshot
		r.Snapshot = &snap
		addSnapshot(s, snap)
	}

	return r, nil
}

// AnnotateSnapshot implements backend.Backend: it finds the snapshot like
// a real backend (the start time, not a checkpoint, the one that ended
// first), sets its description and records the annotation.
func (f *Fake) AnnotateSnapshot(ctx context.Context, src backend.Source, startTime time.Time, description string, addPins []string) error {
	c := Call{Method: AnnotateSnapshot, Source: src, StartTime: startTime, Description: description, AddPins: addPins}

	after, err := f.enter(ctx, c)
	if err != nil {
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

	snap := &s.snapshots[found]
	if description != "" {
		snap.Description = description
	}

	for _, p := range addPins {
		if !slices.Contains(snap.Pins, p) {
			snap.Pins = append(snap.Pins, p)
		}
	}

	f.annotations[src] = append(f.annotations[src], Annotation{StartTime: startTime, Description: description, AddPins: slices.Clone(addPins)})

	return after
}

// ListSnapshots implements backend.Backend: the snapshots of src sorted by
// start time, those that started at the same time in the order they were
// added; none for a source kopia server does not know.
func (f *Fake) ListSnapshots(ctx context.Context, src backend.Source) ([]backend.SnapshotInfo, error) {
	after, err := f.enter(ctx, Call{Method: ListSnapshots, Source: src})
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(ListSnapshots, src); err != nil {
		return nil, err
	}

	if after != nil {
		return nil, after
	}

	s, ok := f.sources[src]
	if !ok {
		return nil, nil
	}

	out := cloneSnapshots(s.snapshots)
	slices.SortStableFunc(out, func(a, b backend.SnapshotInfo) int { return a.StartTime.Compare(b.StartTime) })

	return out, nil
}

// DeleteSnapshots implements backend.Backend like the REST backend: with
// pinned false and a pinned snapshot, the snapshots without a pin go and
// the source stays with its policy and its last snapshot, as kopia server
// does not refresh it; otherwise the source goes as a whole, with its
// snapshots and policy. A source kopia server does not know has nothing to
// delete. A failure after the call took effect, as with FailNextAfter,
// returns the zero result.
func (f *Fake) DeleteSnapshots(ctx context.Context, src backend.Source, pinned bool) (backend.DeleteResult, error) {
	after, err := f.enter(ctx, Call{Method: DeleteSnapshots, Source: src, Pinned: pinned})
	if err != nil {
		return backend.DeleteResult{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.own(DeleteSnapshots, src); err != nil {
		return backend.DeleteResult{}, err
	}

	s, ok := f.sources[src]
	if !ok {
		return backend.DeleteResult{}, after
	}

	var res backend.DeleteResult

	if !pinned {
		for _, snap := range s.snapshots {
			if snap.Pinned() {
				res.Kept++
			}
		}
	}

	if res.Kept == 0 {
		res.Deleted, res.Source = len(s.snapshots), true
		delete(f.sources, src)
	} else {
		// kopia server does not refresh the source after the delete: its
		// last snapshot stays as it was.
		res.Deleted = len(s.snapshots) - res.Kept
		s.snapshots = slices.DeleteFunc(s.snapshots, func(snap backend.SnapshotInfo) bool { return !snap.Pinned() })
	}

	if after != nil {
		return backend.DeleteResult{}, after
	}

	return res, nil
}
