package rest

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

func newBackend(t *testing.T, rt http.RoundTripper) *Backend {
	t.Helper()

	return newBackendLog(t, rt, nil)
}

func newBackendLog(t *testing.T, rt http.RoundTripper, log *slog.Logger) *Backend {
	t.Helper()

	c, err := kopiaclient.New(kopiaclient.Options{
		URL: "http://kopia.test:51515", Username: "admin", Password: "secret", Transport: rt, Timeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	return New(c, log)
}

func src(p string) backend.Source {
	return backend.Source{Host: fakeHost, UserName: fakeUser, Path: p}
}

func info(p string) kopiaclient.SourceInfo {
	return kopiaclient.SourceInfo{Host: fakeHost, UserName: fakeUser, Path: p}
}

func cron(c ...string) backend.PolicyPatch {
	return backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: c}}
}

// assertJSON compares JSON by meaning, nulls and empty objects included.
func assertJSON(t *testing.T, name string, got json.RawMessage, want string) {
	t.Helper()

	var g, w any

	dec := json.NewDecoder(bytes.NewReader(got))
	dec.UseNumber()

	if err := dec.Decode(&g); err != nil {
		t.Fatalf("%s: decode %s: %v", name, got, err)
	}

	dec = json.NewDecoder(strings.NewReader(want))
	dec.UseNumber()

	if err := dec.Decode(&w); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s = %s\nwant %s", name, got, want)
	}
}

func mustPolicy(t *testing.T, f *fakeKopia, si kopiaclient.SourceInfo, want string) {
	t.Helper()

	got, found := f.policy(si)
	if !found {
		t.Fatalf("no policy for %s", si)
	}

	assertJSON(t, "policy", got, want)
}

// kindOf returns the kind of a Backend error: ErrConfig, ErrUnavailable,
// ErrTemporary or nil.
func kindOf(err error) error {
	switch {
	case errors.Is(err, backend.ErrConfig):
		return backend.ErrConfig
	case errors.Is(err, backend.ErrUnavailable):
		return backend.ErrUnavailable
	case errors.Is(err, backend.ErrTemporary):
		return backend.ErrTemporary
	default:
		return nil
	}
}

// sameKind reports whether two kinds are the same; errors.Is would match
// ErrTemporary for ErrUnavailable.
func sameKind(a, b error) bool {
	return a == b //nolint:errorlint // kinds are compared exactly
}

// assertError checks that err is a *backend.Error of op about s with the
// given kind.
func assertError(t *testing.T, err error, op string, s backend.Source, kind error) {
	t.Helper()

	var be *backend.Error
	if !errors.As(err, &be) {
		t.Fatalf("err = %v (%T), want *backend.Error", err, err)
	}

	if be.Op != op || be.Source != s || !sameKind(be.Kind, kind) || !sameKind(kindOf(err), kind) {
		t.Errorf("err = %v: Op %q, Source %v, Kind %v; want %q, %v, %v", err, be.Op, be.Source, be.Kind, op, s, kind)
	}
}

func assertRoutes(t *testing.T, f *fakeKopia, want ...string) {
	t.Helper()

	if got := f.routes(); !slices.Equal(got, want) {
		t.Errorf("requests = %q\nwant %q", got, want)
	}
}

func TestServerIdentity(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	user, host, err := b.ServerIdentity(t.Context())
	if err != nil || user != fakeUser || host != fakeHost {
		t.Fatalf("ServerIdentity = %q, %q, %v", user, host, err)
	}

	if r := f.requestsTo("GET sources"); len(r) != 1 || len(r[0].Query) != 0 {
		t.Errorf("requests = %+v, want one GET sources without a filter", r)
	}

	f.set(func(f *fakeKopia) { f.host = "" })

	_, _, err = b.ServerIdentity(t.Context())
	assertError(t, err, opServerIdentity, backend.Source{}, nil)

	if !errors.Is(err, kopiaclient.ErrUnexpectedResponse) {
		t.Errorf("err = %v, want ErrUnexpectedResponse", err)
	}
}

func TestResolvePath(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	for in, want := range map[string]string{
		"/data/app":    "/data/app",
		"/data/app/":   "/data/app",
		"/data/../x/.": "/x",
		"~/backups":    "/root/backups",
		"rel/dir":      "/root/rel/dir",
	} {
		got, err := b.ResolvePath(t.Context(), in)
		if err != nil || got != src(want) {
			t.Errorf("ResolvePath(%q) = %v, %v; want %v", in, got, err, src(want))
		}
	}

	f.resetRequests()

	_, err := b.ResolvePath(t.Context(), "")
	assertError(t, err, opResolvePath, backend.Source{}, nil)
	assertRoutes(t, f)

	f.set(func(f *fakeKopia) { f.userName = "" })

	_, err = b.ResolvePath(t.Context(), "/data")
	if !errors.Is(err, kopiaclient.ErrUnexpectedResponse) || errors.Is(err, backend.ErrIncompleteSource) {
		t.Errorf("incomplete answer: err = %v", err)
	}
}

func TestListSources(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	start := time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)
	next := start.Add(time.Hour)

	f.addSource(info("/data/b"), &kopiaclient.SnapshotManifest{
		ID: "m1", StartTime: start, EndTime: start.Add(time.Minute), Description: "desc", IncompleteReason: "checkpoint",
	})

	s := f.addSource(info("/data/a"), nil)
	s.status, s.next, s.currentTask = kopiaclient.SourceStatusUploading, &next, "7"

	// Another user on the same host and another host: not kopia server's
	// own sources.
	f.addSource(kopiaclient.SourceInfo{Host: fakeHost, UserName: "other", Path: "/data/a"}, nil)
	f.addSource(kopiaclient.SourceInfo{Host: "laptop", UserName: fakeUser, Path: "/data/a"}, nil)

	got, err := b.ListSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	want := []backend.SourceStatus{
		{Source: src("/data/a"), Status: backend.StatusUploading, NextSnapshotTime: next, CurrentTask: "7"},
		{Source: src("/data/b"), Status: backend.StatusIdle, LastSnapshot: &backend.SnapshotInfo{
			StartTime: start, EndTime: start.Add(time.Minute), Description: "desc", Incomplete: "checkpoint",
		}},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("ListSources = %+v\nwant %+v", got, want)
	}

	// The identity is known now: the next listing filters by it.
	f.resetRequests()

	if _, err := b.ListSources(t.Context()); err != nil {
		t.Fatal(err)
	}

	r := f.requestsTo("GET sources")
	if len(r) != 1 || r[0].Query.Get("host") != fakeHost || r[0].Query.Get("userName") != fakeUser || r[0].Query.Has("path") {
		t.Errorf("requests = %+v, want GET sources with host and userName", r)
	}

	// kopia server now runs under another identity: the filtered answer
	// tells, and the sources are listed again.
	f.set(func(f *fakeKopia) { f.host = "kopia-2" })
	f.addSource(kopiaclient.SourceInfo{Host: "kopia-2", UserName: fakeUser, Path: "/data/c"}, nil)
	f.resetRequests()

	got, err = b.ListSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 1 || got[0].Source != (backend.Source{Host: "kopia-2", UserName: fakeUser, Path: "/data/c"}) {
		t.Errorf("after an identity change ListSources = %+v", got)
	}

	if r := f.requestsTo("GET sources"); len(r) != 2 || len(r[1].Query) != 0 {
		t.Errorf("requests = %+v, want a filtered and an unfiltered GET sources", r)
	}

	// The second listing fails like any request.
	f.set(func(f *fakeKopia) { f.host = "kopia-4" })
	f.inject("GET sources", injection{status: http.StatusOK, body: `{"localHost":"kopia-4","localUsername":"root","sources":[]}`})
	f.inject("GET sources", injection{status: http.StatusServiceUnavailable, body: "unavailable"})

	_, err = b.ListSources(t.Context())
	assertError(t, err, opListSources, backend.Source{}, backend.ErrUnavailable)

	// An empty list is no error.
	f.set(func(f *fakeKopia) { f.host = "kopia-3" })

	if got, err := b.ListSources(t.Context()); err != nil || len(got) != 0 {
		t.Errorf("ListSources = %+v, %v", got, err)
	}
}

func TestPlanSourceNewSource(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionRegister || plan.Known || plan.Status != nil || plan.Current != nil {
		t.Errorf("plan = %+v", plan)
	}

	assertJSON(t, "plan.Policy", plan.Policy, `{"scheduling":{"cron":["0 * * * *"]}}`)
	assertRoutes(t, f, "GET sources", "GET policy")

	for _, r := range f.requestsTo("GET sources") {
		if target(r.Query) != info("/data/app") {
			t.Errorf("GET sources query %v, want the full source", r.Query)
		}
	}

	for _, r := range f.requestsTo("GET policy") {
		if target(r.Query) != info("/data/app") {
			t.Errorf("GET policy query %v, want the full source", r.Query)
		}
	}
}

// A policy written to the repository with the kopia CLI before kopia
// server refreshed: the source is registered with that policy merged.
func TestPlanSourceUnknownWithPolicy(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	f.set(func(f *fakeKopia) {
		f.policies[info("/data/app")] = json.RawMessage(`{"retention":{"keepDaily":7}}`)
	})

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionRegister || plan.Known {
		t.Errorf("Action = %v, Known = %v", plan.Action, plan.Known)
	}

	assertJSON(t, "plan.Current", plan.Current, `{"retention":{"keepDaily":7}}`)
	assertJSON(t, "plan.Policy", plan.Policy, `{"retention":{"keepDaily":7},"scheduling":{"cron":["0 * * * *"]}}`)
}

func TestPlanSourceKnown(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	start := time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)
	f.addSource(info("/data/app"), &kopiaclient.SnapshotManifest{StartTime: start, EndTime: start})
	f.setPolicy(info("/data/app"), `{"scheduling":{"cron":["0 * * * *"]}}`)

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionNone || !plan.Known || plan.Status == nil || plan.Status.LastSnapshot == nil ||
		!plan.Status.LastSnapshot.StartTime.Equal(start) || plan.Status.Source != s {
		t.Errorf("plan = %+v, status %+v", plan, plan.Status)
	}

	plan, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("5 * * * *")}, backend.Applied{Patch: cron("0 * * * *")})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", plan.Action)
	}

	// A known source whose policy was deleted: an update writes it again.
	f.set(func(f *fakeKopia) { delete(f.policies, info("/data/app")) })

	plan, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil || plan.Action != backend.ActionUpdate || plan.Current != nil {
		t.Errorf("plan = %+v, %v", plan, err)
	}
}

// kopia server answers 200 null when it cannot read the policy. Taken for a
// missing policy, it would overwrite the policy with the watcher's fields
// alone.
func TestPlanSourceNullPolicy(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	f.setPolicy(info("/data/app"), `{"retention":{"keepDaily":7}}`)
	f.inject("GET policy", injection{status: http.StatusOK, body: "null"})

	_, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	assertError(t, err, opPlanSource, s, backend.ErrTemporary)

	if !errors.Is(err, kopiaclient.ErrInternal) {
		t.Errorf("err = %v, want kopiaclient.ErrInternal", err)
	}
}

func TestPlanSourceErrors(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	// Incomplete: nothing is sent.
	for _, s := range []backend.Source{{Host: fakeHost, UserName: fakeUser}, {Host: fakeHost, Path: "/x"}, {UserName: fakeUser, Path: "/x"}} {
		_, err := b.PlanSource(t.Context(), backend.Desired{Source: s}, backend.Applied{})
		assertError(t, err, opPlanSource, s, nil)

		if !errors.Is(err, backend.ErrIncompleteSource) {
			t.Errorf("err = %v, want ErrIncompleteSource", err)
		}
	}

	assertRoutes(t, f)

	// Another identity: the policy is not even read.
	other := backend.Source{Host: "laptop", UserName: fakeUser, Path: "/data/app"}

	_, err := b.PlanSource(t.Context(), backend.Desired{Source: other}, backend.Applied{})
	assertError(t, err, opPlanSource, other, nil)

	if !errors.Is(err, backend.ErrWrongIdentity) || !strings.Contains(err.Error(), "root@kopia") {
		t.Errorf("err = %v, want ErrWrongIdentity naming root@kopia", err)
	}

	assertRoutes(t, f, "GET sources")

	// kopia server reports no identity: no source is its own.
	f.set(func(f *fakeKopia) { f.host = "" })

	_, err = b.PlanSource(t.Context(), backend.Desired{Source: src("/data/app")}, backend.Applied{})
	if !errors.Is(err, backend.ErrWrongIdentity) {
		t.Errorf("err = %v, want ErrWrongIdentity", err)
	}

	f.set(func(f *fakeKopia) { f.host = fakeHost })

	// Labels that set a field inside one the policy holds as a value.
	s := src("/data/app")
	f.setPolicy(info("/data/app"), `{"files":"none"}`)

	_, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: backend.PolicyPatch{Raw: json.RawMessage(`{"files":{"ignore":["x"]}}`)}}, backend.Applied{})
	assertError(t, err, opPlanSource, s, backend.ErrConfig)

	// A policy that is not a JSON object: an error of neither kind.
	f.setPolicy(info("/data/app"), `["not", "an", "object"]`)

	_, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	assertError(t, err, opPlanSource, s, nil)
}

// The required check of the merge over REST: fields of the policy that the
// watcher does not own, known to kopia or not, survive GET -> merge -> PUT
// with their exact text.
func TestUnknownPolicyFieldsSurvive(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	stored := `{"retention":{"keepDaily":7,"keepLatest":1.50E1},` +
		`"files":{"ignore":["*.tmp","<cache>&"],"ignoreDotFiles":[".kopiaignore"]},` +
		`"actions":{"beforeSnapshotRoot":{"script":"pg_dump > /data/app/dump.sql","timeout":300,"mode":"essential"}},` +
		`"scheduling":{"cron":["0 * * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}],"futureScheduling":{"x":1}},` +
		`"futureSection":{"nested":[1,2.50,"x",null,{"deep":true}],"empty":{}},` +
		`"noParent":false}`

	f.addSource(info("/data/app"), nil)
	f.setPolicy(info("/data/app"), stored)

	last := backend.Applied{Patch: cron("0 * * * *")}

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("30 2 * * *")}, last)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.ApplyPlan(t.Context(), plan); err != nil {
		t.Fatal(err)
	}

	puts := f.requestsTo("PUT policy")
	if len(puts) != 1 {
		t.Fatalf("PUT requests = %+v", puts)
	}

	body := puts[0].Body

	// Numbers keep their text. Strings keep their value: kopiaclient
	// escapes <, > and & in the request, which kopia server decodes back.
	for _, frag := range []string{
		`"keepLatest":1.50E1`,
		`"futureScheduling":{"x":1}`,
		`"futureSection":{"nested":[1,2.50,"x",null,{"deep":true}],"empty":{}}`,
		`"noParent":false`,
		`"timeOfDay":[{"hour":2,"min":30}]`,
	} {
		if !bytes.Contains(body, []byte(frag)) {
			t.Errorf("PUT body lost %s:\n%s", frag, body)
		}
	}

	want := strings.Replace(stored, `"cron":["0 * * * *"]`, `"cron":["30 2 * * *"]`, 1)
	assertJSON(t, "PUT body", body, want)
	mustPolicy(t, f, info("/data/app"), want)
}

func TestApplyPlanRegister(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	f.resetRequests()

	applied, err := b.ApplyPlan(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}

	// Identity check, POST with the policy (path check), PUT of the same
	// policy (refresh), read-back.
	assertRoutes(t, f, "GET sources", "POST sources", "PUT policy", "GET policy")

	var post struct {
		Path           string          `json:"path"`
		CreateSnapshot bool            `json:"createSnapshot"`
		Policy         json.RawMessage `json:"policy"`
	}

	if err := json.Unmarshal(f.requestsTo("POST sources")[0].Body, &post); err != nil {
		t.Fatal(err)
	}

	if post.Path != "/data/app" || post.CreateSnapshot {
		t.Errorf("POST sources = %+v", post)
	}

	assertJSON(t, "POST policy", post.Policy, string(plan.Policy))

	put := f.requestsTo("PUT policy")[0]
	if target(put.Query) != info("/data/app") {
		t.Errorf("PUT query = %v", put.Query)
	}

	assertJSON(t, "PUT body", put.Body, string(plan.Policy))

	if !f.known(info("/data/app")) || !f.loaded(info("/data/app")) {
		t.Errorf("known %v, loaded %v", f.known(info("/data/app")), f.loaded(info("/data/app")))
	}

	stored, _ := f.policy(info("/data/app"))

	hash, err := backend.Fingerprint(stored)
	if err != nil {
		t.Fatal(err)
	}

	want := plan.Applied.Clone()
	want.StoredHash = hash

	if !applied.Equal(want) || applied.StoredHash == "" {
		t.Errorf("applied = %+v\nwant %+v", applied, want)
	}

	// The next plan settles.
	plan, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, applied)
	if err != nil || plan.Action != backend.ActionNone {
		t.Errorf("next plan: %v, %v", plan.Action, err)
	}
}

func TestApplyPlanUpdateAndNone(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	f.setPolicy(info("/data/app"), `{"retention":{"keepDaily":7}}`)

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil || plan.Action != backend.ActionUpdate {
		t.Fatalf("plan = %v, %v", plan.Action, err)
	}

	f.resetRequests()

	applied, err := b.ApplyPlan(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}

	assertRoutes(t, f, "GET sources", "PUT policy", "GET policy")
	mustPolicy(t, f, info("/data/app"), `{"retention":{"keepDaily":7},"scheduling":{"cron":["0 * * * *"]}}`)

	if applied.StoredHash == "" || !applied.Patch.Equal(cron("0 * * * *")) || applied.Pending != nil {
		t.Errorf("applied = %+v", applied)
	}

	plan, err = b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, applied)
	if err != nil || plan.Action != backend.ActionNone {
		t.Fatalf("plan = %v, %v", plan.Action, err)
	}

	f.resetRequests()

	got, err := b.ApplyPlan(t.Context(), plan)
	if err != nil || !got.Equal(plan.Applied) {
		t.Errorf("ApplyPlan(none) = %+v, %v", got, err)
	}

	assertRoutes(t, f)

	// An unknown action is refused before anything is sent.
	plan.Action = backend.Action(42)

	_, err = b.ApplyPlan(t.Context(), plan)
	assertError(t, err, opApplyPlan, s, nil)
	assertRoutes(t, f)

	// So is a policy that is not a JSON object.
	plan.Action, plan.Policy = backend.ActionUpdate, json.RawMessage(`[]`)

	_, err = b.ApplyPlan(t.Context(), plan)
	assertError(t, err, opApplyPlan, s, nil)
	assertRoutes(t, f)
}

// syncBuffer collects log output written from several goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

// A failed read-back does not fail the apply: the policy is stored.
func TestApplyPlanReadBackFails(t *testing.T) {
	for name, inj := range map[string]injection{
		"500":       {status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`},
		"null":      {status: http.StatusOK, body: "null"},
		"transport": {err: errors.New("connection reset by peer")},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeKopia()

			var logs syncBuffer

			b := newBackendLog(t, f, slog.New(slog.NewTextHandler(&logs, nil)))
			s := src("/data/app")

			plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
			if err != nil {
				t.Fatal(err)
			}

			f.inject("GET policy", inj)

			applied, err := b.ApplyPlan(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}

			if applied.StoredHash != "" || !applied.Patch.Equal(plan.Applied.Patch) {
				t.Errorf("applied = %+v", applied)
			}

			mustPolicy(t, f, info("/data/app"), `{"scheduling":{"cron":["0 * * * *"]}}`)

			if !strings.Contains(logs.String(), "could not read back") || !strings.Contains(logs.String(), "root@kopia:/data/app") {
				t.Errorf("log = %s", logs.String())
			}
		})
	}
}

func TestApplyPlanRegisterFailures(t *testing.T) {
	plan := func(t *testing.T, b *Backend, p string, patch backend.PolicyPatch) backend.Plan {
		t.Helper()

		plan, err := b.PlanSource(t.Context(), backend.Desired{Source: src(p), Patch: patch}, backend.Applied{})
		if err != nil || plan.Action != backend.ActionRegister {
			t.Fatalf("plan = %v, %v", plan.Action, err)
		}

		return plan
	}

	t.Run("missing path", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.set(func(f *fakeKopia) { f.missing["/data/typo"] = true })

		p := plan(t, b, "/data/typo", cron("0 * * * *"))
		f.resetRequests()

		_, err := b.ApplyPlan(t.Context(), p)
		assertError(t, err, opApplyPlan, src("/data/typo"), backend.ErrConfig)

		if !errors.Is(err, kopiaclient.ErrPathNotFound) {
			t.Errorf("err = %v, want ErrPathNotFound", err)
		}

		assertRoutes(t, f, "GET sources", "POST sources")

		if f.known(info("/data/typo")) {
			t.Error("a missing path was registered")
		}
	})

	t.Run("stat error", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.set(func(f *fakeKopia) { f.statErr["/data/secret"] = "stat /data/secret: permission denied" })

		_, err := b.ApplyPlan(t.Context(), plan(t, b, "/data/secret", cron("0 * * * *")))
		assertError(t, err, opApplyPlan, src("/data/secret"), backend.ErrConfig)
	})

	t.Run("invalid policy", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.set(func(f *fakeKopia) { f.badCrons["0 25 * * *"] = true })

		_, err := b.ApplyPlan(t.Context(), plan(t, b, "/data/app", cron("0 25 * * *")))
		assertError(t, err, opApplyPlan, src("/data/app"), backend.ErrConfig)

		if !errors.Is(err, kopiaclient.ErrInvalidPolicy) {
			t.Errorf("err = %v, want ErrInvalidPolicy", err)
		}

		if f.known(info("/data/app")) {
			t.Error("registered with an invalid policy")
		}
	})

	t.Run("trailing slash", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)

		_, err := b.ApplyPlan(t.Context(), plan(t, b, "/data/app/", cron("0 * * * *")))
		assertError(t, err, opApplyPlan, src("/data/app/"), backend.ErrConfig)
	})

	t.Run("PUT fails after the POST", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.inject("PUT policy", injection{status: http.StatusInternalServerError,
			body: `{"code":"INTERNAL","error":"internal server error: unable to set policy: error writing policy manifest: blob error"}`})

		_, err := b.ApplyPlan(t.Context(), plan(t, b, "/data/app", cron("0 * * * *")))
		assertError(t, err, opApplyPlan, src("/data/app"), backend.ErrTemporary)

		if !f.known(info("/data/app")) || f.loaded(info("/data/app")) {
			t.Errorf("known %v, loaded %v: want known after the POST, not loaded", f.known(info("/data/app")), f.loaded(info("/data/app")))
		}
	})

	t.Run("repository write fails in the POST", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.inject("POST sources", injection{status: http.StatusInternalServerError,
			body: `{"code":"INTERNAL","error":"internal server error: unable to set initial policy: error writing policy manifest: blob error"}`})

		_, err := b.ApplyPlan(t.Context(), plan(t, b, "/data/app", cron("0 * * * *")))
		assertError(t, err, opApplyPlan, src("/data/app"), backend.ErrTemporary)
	})
}

// kopia server's identity changes between the plan and the apply: nothing
// is written.
func TestApplyPlanWrongIdentity(t *testing.T) {
	for _, action := range []backend.Action{backend.ActionRegister, backend.ActionUpdate} {
		t.Run(action.String(), func(t *testing.T) {
			f := newFakeKopia()
			b := newBackend(t, f)
			s := src("/data/app")

			if action == backend.ActionUpdate {
				f.setPolicy(info("/data/app"), `{}`)
			}

			plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
			if err != nil || plan.Action != action {
				t.Fatalf("plan = %v, %v", plan.Action, err)
			}

			f.set(func(f *fakeKopia) { f.userName = "someone" })
			f.resetRequests()

			_, err = b.ApplyPlan(t.Context(), plan)
			assertError(t, err, opApplyPlan, s, nil)

			if !errors.Is(err, backend.ErrWrongIdentity) {
				t.Errorf("err = %v, want ErrWrongIdentity", err)
			}

			assertRoutes(t, f, "GET sources")
		})
	}
}

func TestPreviewSchedule(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	t1 := time.Date(2026, 10, 18, 11, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	t2, t3 := t1.Add(time.Hour), t1.Add(2*time.Hour)

	f.set(func(f *fakeKopia) {
		f.upcoming = []time.Time{t1, t2, t3}
		f.inherited = map[string]json.RawMessage{"runMissed": json.RawMessage(`true`), "intervalSeconds": json.RawMessage(`3600`)}
	})
	f.setPolicy(info("/data/app"), `{"scheduling":{"manual":true}}`)

	policy := json.RawMessage(`{"retention":{"keepDaily":7},"scheduling":{"cron":["0 * * * *"],"intervalSeconds":7200}}`)

	got, err := b.PreviewSchedule(t.Context(), s, policy, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(got.Times) != 2 || !got.Times[0].Equal(t1) || !got.Times[1].Equal(t2) {
		t.Errorf("Times = %v", got.Times)
	}

	if _, off := got.Times[0].Zone(); off != 2*3600 {
		t.Errorf("time zone offset %d, want the server's", off)
	}

	assertJSON(t, "Scheduling", got.Scheduling, `{"cron":["0 * * * *"],"intervalSeconds":7200,"runMissed":true}`)

	r := f.requestsTo("POST policy/resolve")
	if len(r) != 1 || target(r[0].Query) != info("/data/app") {
		t.Fatalf("requests = %+v", r)
	}

	var req struct {
		Updates json.RawMessage `json:"updates"`
		N       int             `json:"numUpcomingSnapshotTimes"`
	}

	if err := json.Unmarshal(r[0].Body, &req); err != nil {
		t.Fatal(err)
	}

	if req.N != 2 {
		t.Errorf("numUpcomingSnapshotTimes = %d", req.N)
	}

	assertJSON(t, "updates", req.Updates, string(policy))

	// A paused policy has no times.
	got, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(`{"scheduling":{"manual":true}}`), 3)
	if err != nil || len(got.Times) != 0 {
		t.Errorf("paused: %+v, %v", got, err)
	}

	// Nothing asked for, nothing returned; a negative n counts as 0.
	for _, n := range []int{0, -1} {
		got, err = b.PreviewSchedule(t.Context(), s, policy, n)
		if err != nil || len(got.Times) != 0 {
			t.Errorf("n=%d: %+v, %v", n, got, err)
		}
	}

	// kopia server answers with more times than asked for: they are cut.
	f.inject("POST policy/resolve", injection{status: http.StatusOK,
		body: `{"effective":{},"upcomingSnapshotTimes":["2026-10-18T11:00:00Z","2026-10-18T12:00:00Z"]}`})

	got, err = b.PreviewSchedule(t.Context(), s, policy, 1)
	if err != nil || len(got.Times) != 1 {
		t.Errorf("cut: %+v, %v", got, err)
	}

	assertJSON(t, "Scheduling without a section", got.Scheduling, `{}`)
}

func TestPreviewScheduleErrors(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	f.set(func(f *fakeKopia) { f.badCrons["0 25 * * *"] = true })

	_, err := b.PreviewSchedule(t.Context(), s, json.RawMessage(`{"scheduling":{"cron":["0 25 * * *"]}}`), 1)
	assertError(t, err, opPreviewSchedule, s, backend.ErrConfig)

	if !strings.Contains(err.Error(), `invalid cron expression "0 25 * * *"`) {
		t.Errorf("err = %v, want kopia server's message", err)
	}

	_, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(`{"scheduling":{"manual":true,"cron":[]}}`), 1)
	assertError(t, err, opPreviewSchedule, s, backend.ErrConfig)

	// A value kopia server cannot decode.
	_, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(`{"scheduling":{"cron":"0 * * * *"}}`), 1)
	assertError(t, err, opPreviewSchedule, s, backend.ErrConfig)

	if !errors.Is(err, kopiaclient.ErrMalformedRequest) {
		t.Errorf("err = %v, want ErrMalformedRequest", err)
	}

	// Not a policy: nothing is sent; kopia server would resolve the stored
	// policy for a missing one.
	f.resetRequests()

	for _, p := range []string{``, `null`, `[]`, `"x"`, `{`} {
		_, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(p), 1)
		assertError(t, err, opPreviewSchedule, s, nil)
	}

	assertRoutes(t, f)

	other := backend.Source{Host: fakeHost, UserName: "other", Path: "/data/app"}

	_, err = b.PreviewSchedule(t.Context(), other, json.RawMessage(`{}`), 1)
	if !errors.Is(err, backend.ErrWrongIdentity) {
		t.Errorf("err = %v, want ErrWrongIdentity", err)
	}

	assertRoutes(t, f, "GET sources")

	_, err = b.PreviewSchedule(t.Context(), backend.Source{Path: "/x"}, json.RawMessage(`{}`), 1)
	if !errors.Is(err, backend.ErrIncompleteSource) {
		t.Errorf("err = %v, want ErrIncompleteSource", err)
	}

	// An effective policy that is not an object, or whose scheduling is
	// not.
	f.inject("POST policy/resolve", injection{status: http.StatusOK, body: `{"effective":[]}`})

	_, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(`{}`), 1)
	assertError(t, err, opPreviewSchedule, s, nil)

	f.inject("POST policy/resolve", injection{status: http.StatusOK, body: `{"effective":{"scheduling":[]}}`})

	_, err = b.PreviewSchedule(t.Context(), s, json.RawMessage(`{}`), 1)
	assertError(t, err, opPreviewSchedule, s, nil)

	if !errors.Is(err, kopiaclient.ErrUnexpectedResponse) {
		t.Errorf("err = %v, want ErrUnexpectedResponse", err)
	}
}

func TestDeletePolicy(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	f.setPolicy(info("/data/a"), `{"scheduling":{"manual":true}}`)
	f.addSource(info("/data/b"), &kopiaclient.SnapshotManifest{ID: "m"})
	f.setPolicy(info("/data/b"), `{"scheduling":{"manual":true}}`)
	f.resetRequests()

	if err := b.DeletePolicy(t.Context(), src("/data/a")); err != nil {
		t.Fatal(err)
	}

	assertRoutes(t, f, "GET sources", "DELETE policy")

	if r := f.requestsTo("DELETE policy"); target(r[0].Query) != info("/data/a") {
		t.Errorf("DELETE query = %v", r[0].Query)
	}

	if _, found := f.policy(info("/data/a")); found || f.known(info("/data/a")) {
		t.Error("policy or source of /data/a left")
	}

	// A source with snapshots stays.
	if err := b.DeletePolicy(t.Context(), src("/data/b")); err != nil {
		t.Fatal(err)
	}

	if _, found := f.policy(info("/data/b")); found || !f.known(info("/data/b")) {
		t.Error("want /data/b known without a policy")
	}

	// Deleting a missing policy succeeds.
	if err := b.DeletePolicy(t.Context(), src("/data/none")); err != nil {
		t.Errorf("missing policy: %v", err)
	}

	// Another identity: nothing is deleted.
	f.setPolicy(kopiaclient.SourceInfo{Host: fakeHost, UserName: "other", Path: "/data/a"}, `{}`)
	f.resetRequests()

	err := b.DeletePolicy(t.Context(), backend.Source{Host: fakeHost, UserName: "other", Path: "/data/a"})
	if !errors.Is(err, backend.ErrWrongIdentity) {
		t.Errorf("err = %v, want ErrWrongIdentity", err)
	}

	assertRoutes(t, f, "GET sources")

	f.inject("DELETE policy", injection{status: http.StatusInternalServerError,
		body: `{"code":"INTERNAL","error":"internal server error: repository is not writable"}`})

	err = b.DeletePolicy(t.Context(), src("/data/b"))
	assertError(t, err, opDeletePolicy, src("/data/b"), nil)
}

func TestTriggerSnapshot(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	now := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	b.now = func() time.Time { return now }

	start := now.Add(-24 * time.Hour)
	f.addSource(info("/data/app"), &kopiaclient.SnapshotManifest{StartTime: start, EndTime: start.Add(time.Minute)})
	f.addSource(info("/data/app2"), nil)
	f.set(func(f *fakeKopia) {
		f.tasks = []kopiaclient.TaskInfo{
			{ID: "3", Kind: kopiaclient.TaskKindSnapshot, Description: "root@kopia:/data/app at 2026-10-17T09:00:00Z", Status: kopiaclient.TaskSuccess},
			{ID: "4", Kind: kopiaclient.TaskKindSnapshot, Description: "root@kopia:/data/app2 at 2026-10-17T09:00:00Z"},
			{ID: "5", Kind: "Maintenance", Description: "Periodic maintenance"},
			{ID: "6", Kind: kopiaclient.TaskKindSnapshot, Description: "root@kopia:/data/app at 2026-10-18T08:00:00Z", Status: kopiaclient.TaskRunning},
		}
	})

	ticket, err := b.TriggerSnapshot(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}

	want := backend.SnapshotTicket{Source: s, TriggeredAt: now, LastSnapshotStart: start, TasksBefore: []string{"3", "6"}}
	if !reflect.DeepEqual(ticket, want) {
		t.Errorf("ticket = %+v\nwant %+v", ticket, want)
	}

	if f.uploads(info("/data/app")) != 1 || f.uploads(info("/data/app2")) != 0 {
		t.Errorf("uploads: %d, %d", f.uploads(info("/data/app")), f.uploads(info("/data/app2")))
	}

	if r := f.requestsTo("POST sources/upload"); len(r) != 1 || target(r[0].Query) != info("/data/app") {
		t.Errorf("upload requests = %+v", r)
	}

	// A source kopia server has not picked up: retried later.
	_, err = b.TriggerSnapshot(t.Context(), src("/data/new"))
	assertError(t, err, opTriggerSnapshot, src("/data/new"), backend.ErrTemporary)

	if !errors.Is(err, kopiaclient.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}

	// The task list cannot be read: nothing is queued.
	f.inject("GET tasks", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`})

	_, err = b.TriggerSnapshot(t.Context(), s)
	assertError(t, err, opTriggerSnapshot, s, backend.ErrTemporary)

	if f.uploads(info("/data/app")) != 1 {
		t.Errorf("uploads = %d after a failed task list", f.uploads(info("/data/app")))
	}

	// A source without snapshots has no last start.
	ticket, err = b.TriggerSnapshot(t.Context(), src("/data/app2"))
	if err != nil || !ticket.LastSnapshotStart.IsZero() || !slices.Equal(ticket.TasksBefore, []string{"4"}) {
		t.Errorf("ticket = %+v, %v", ticket, err)
	}
}

func TestSnapshotMethodsNotImplemented(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	_, err := b.WaitSnapshot(t.Context(), backend.SnapshotTicket{Source: s}, nil)
	assertError(t, err, opWaitSnapshot, s, nil)

	if !errors.Is(err, backend.ErrNotImplemented) {
		t.Errorf("WaitSnapshot: %v", err)
	}

	err = b.AnnotateSnapshot(t.Context(), s, time.Now(), "d", nil)
	assertError(t, err, opAnnotateSnapshot, s, nil)

	if !errors.Is(err, backend.ErrNotImplemented) {
		t.Errorf("AnnotateSnapshot: %v", err)
	}

	assertRoutes(t, f)
}

// Every request about a source carries its host, user name and path, so it
// never matches another user's source.
func TestEveryRequestNamesTheSource(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	s := src("/data/app")

	plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.PreviewSchedule(t.Context(), s, plan.Unpaused, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := b.ApplyPlan(t.Context(), plan); err != nil {
		t.Fatal(err)
	}

	if _, err := b.TriggerSnapshot(t.Context(), s); err != nil {
		t.Fatal(err)
	}

	if err := b.DeletePolicy(t.Context(), s); err != nil {
		t.Fatal(err)
	}

	f.mu.Lock()
	requests := slices.Clone(f.requests)
	f.mu.Unlock()

	for _, r := range requests {
		switch r.Route {
		case "GET tasks":
			continue
		case "POST sources":
			// The source is kopia server's own user@host with this path.
			if len(r.Query) != 0 || !bytes.Contains(r.Body, []byte(`"path":"/data/app"`)) {
				t.Errorf("%s %v %s", r.Route, r.Query, r.Body)
			}
		default:
			if target(r.Query) != info("/data/app") {
				t.Errorf("%s with query %v, want host, userName and path", r.Route, r.Query)
			}
		}
	}
}

// blockingTransport holds requests of one route until their context is
// done, like a kopia server that does not answer.
type blockingTransport struct {
	inner http.RoundTripper
	route string
}

func (bt blockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method+" "+strings.TrimPrefix(req.URL.Path, "/api/v1/") == bt.route {
		<-req.Context().Done()

		return nil, req.Context().Err()
	}

	return bt.inner.RoundTrip(req)
}

// The caller's context being done makes an error of neither kind that
// wraps ctx.Err(), whatever the request failed with. The HTTP client's own
// timeout is ErrUnavailable.
func TestContextDone(t *testing.T) {
	s := src("/data/app")

	t.Run("canceled before the call", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, _, err := b.ServerIdentity(ctx)
		assertError(t, err, opServerIdentity, backend.Source{}, nil)

		_, err = b.PlanSource(ctx, backend.Desired{Source: s}, backend.Applied{})
		assertError(t, err, opPlanSource, s, nil)

		_, err = b.PreviewSchedule(ctx, s, json.RawMessage(`{}`), 1)
		assertError(t, err, opPreviewSchedule, s, nil)

		err = b.DeletePolicy(ctx, s)
		assertError(t, err, opDeletePolicy, s, nil)

		_, err = b.ListSources(ctx)
		assertError(t, err, opListSources, backend.Source{}, nil)

		_, err = b.ResolvePath(ctx, "/x")
		assertError(t, err, opResolvePath, backend.Source{}, nil)

		_, err = b.TriggerSnapshot(ctx, s)
		assertError(t, err, opTriggerSnapshot, s, nil)

		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("canceled while kopia server answers 500", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, f)
		f.setPolicy(info("/data/app"), `{}`)

		plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		f.set(func(f *fakeKopia) {
			f.hook = func(r fakeRequest) {
				if r.Route == "PUT policy" {
					cancel()
				}
			}
		})
		f.inject("PUT policy", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"boom"}`})

		_, err = b.ApplyPlan(ctx, plan)
		assertError(t, err, opApplyPlan, s, nil)

		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("caller's deadline", func(t *testing.T) {
		f := newFakeKopia()
		b := newBackend(t, blockingTransport{inner: f, route: "GET policy"})

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, err := b.PlanSource(ctx, backend.Desired{Source: s}, backend.Applied{})
		assertError(t, err, opPlanSource, s, nil)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("client timeout", func(t *testing.T) {
		f := newFakeKopia()

		c, err := kopiaclient.New(kopiaclient.Options{
			URL: "http://kopia.test:51515", Transport: blockingTransport{inner: f, route: "GET policy"}, Timeout: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		b := New(c, nil)

		_, err = b.PlanSource(t.Context(), backend.Desired{Source: s}, backend.Applied{})
		assertError(t, err, opPlanSource, s, backend.ErrUnavailable)
	})
}

// Errors of kopia server's answers map to the kinds package backend lists.
func TestErrorKinds(t *testing.T) {
	const (
		notWritable = `{"code":"INTERNAL","error":"internal server error: repository is not writable"}`
		internal    = `{"code":"INTERNAL","error":"internal server error: unable to set policy: error writing policy manifest: blob error"}`
		validate    = `{"code":"INTERNAL","error":"internal server error: unable to set policy: failed to validate policy: invalid scheduling policy: invalid cron expression \"x\""}`
		policyPath  = `{"code":"INTERNAL","error":"internal server error: unable to set policy: invalid policy path: path cannot end with a slash or a backslash"}`
		malformed   = `{"code":"MALFORMED_REQUEST","error":"malformed request body"}`
		notConn     = `{"code":"NOT_CONNECTED","error":"not connected"}`
		storage     = `{"code":"STORAGE_CONNECTION","error":"storage"}`
		notFound    = `{"code":"NOT_FOUND","error":"not found"}`
		accessDeny  = `{"code":"ACCESS_DENIED","error":"access denied"}`
		unknownCode = `{"code":"SOMETHING_NEW","error":"?"}`
	)

	s := src("/data/app")

	type answer struct {
		status int
		body   string
		err    error
	}

	// Each method is called on a fresh fake whose route answers with the
	// injected response.
	methods := map[string]struct {
		route string
		call  func(t *testing.T, b *Backend, f *fakeKopia) error
	}{
		"read": {"GET sources", func(t *testing.T, b *Backend, _ *fakeKopia) error {
			_, err := b.PlanSource(t.Context(), backend.Desired{Source: s}, backend.Applied{})
			return err
		}},
		"write": {"PUT policy", func(t *testing.T, b *Backend, f *fakeKopia) error {
			f.setPolicy(info("/data/app"), `{}`)

			plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = b.ApplyPlan(t.Context(), plan)

			return err
		}},
		"register": {"POST sources", func(t *testing.T, b *Backend, _ *fakeKopia) error {
			plan, err := b.PlanSource(t.Context(), backend.Desired{Source: s, Patch: cron("0 * * * *")}, backend.Applied{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = b.ApplyPlan(t.Context(), plan)

			return err
		}},
		"resolve": {"POST policy/resolve", func(t *testing.T, b *Backend, _ *fakeKopia) error {
			_, err := b.PreviewSchedule(t.Context(), s, json.RawMessage(`{}`), 1)
			return err
		}},
		"delete": {"DELETE policy", func(t *testing.T, b *Backend, _ *fakeKopia) error {
			return b.DeletePolicy(t.Context(), s)
		}},
		"upload": {"POST sources/upload", func(t *testing.T, b *Backend, f *fakeKopia) error {
			f.addSource(info("/data/app"), nil)

			_, err := b.TriggerSnapshot(t.Context(), s)

			return err
		}},
	}

	tests := []struct {
		method string
		answer answer
		want   error
	}{
		// Any request.
		{"read", answer{status: 500, body: internal}, backend.ErrTemporary},
		{"read", answer{status: 500, body: "proxy error"}, backend.ErrTemporary},
		{"read", answer{status: 500, body: notWritable}, nil},
		{"read", answer{status: 500, body: validate}, backend.ErrTemporary},
		{"read", answer{status: 400, body: notConn}, backend.ErrUnavailable},
		{"read", answer{status: 400, body: storage}, backend.ErrUnavailable},
		{"read", answer{status: 502, body: "bad gateway"}, backend.ErrUnavailable},
		{"read", answer{status: 503, body: "unavailable"}, backend.ErrUnavailable},
		{"read", answer{status: 504, body: "timeout"}, backend.ErrUnavailable},
		{"read", answer{status: 408, body: "timeout"}, backend.ErrUnavailable},
		{"read", answer{status: 429, body: "slow down"}, backend.ErrUnavailable},
		{"read", answer{err: errors.New("connection refused")}, backend.ErrUnavailable},
		{"read", answer{err: x509.UnknownAuthorityError{}}, nil},
		{"read", answer{status: 401, body: "nope"}, nil}, // CSRF twice
		{"read", answer{status: 403, body: accessDeny}, nil},
		{"read", answer{status: 404, body: "404 page not found"}, nil},
		{"read", answer{status: 400, body: notFound}, nil},
		{"read", answer{status: 400, body: malformed}, nil},
		{"read", answer{status: 400, body: unknownCode}, nil},
		{"read", answer{status: 200, body: "<html>"}, nil},
		{"read", answer{status: 418, body: "teapot"}, nil},

		// PUT /api/v1/policy.
		{"write", answer{status: 500, body: validate}, backend.ErrConfig},
		{"write", answer{status: 500, body: policyPath}, backend.ErrConfig},
		{"write", answer{status: 400, body: malformed}, backend.ErrConfig},
		{"write", answer{status: 500, body: internal}, backend.ErrTemporary},
		{"write", answer{status: 500, body: notWritable}, nil},
		{"write", answer{status: 400, body: notConn}, backend.ErrUnavailable},
		{"write", answer{err: errors.New("connection reset")}, backend.ErrUnavailable},

		// POST /api/v1/sources.
		{"register", answer{status: 400, body: `{"code":"PATH_NOT_FOUND","error":"path does not exist"}`}, backend.ErrConfig},
		{"register", answer{status: 500, body: `{"code":"INTERNAL","error":"internal server error: stat /data/app: permission denied"}`}, backend.ErrConfig},
		{"register", answer{status: 500, body: `{"code":"INTERNAL","error":"internal server error: unable to set initial policy: error writing policy manifest: x"}`}, backend.ErrTemporary},
		{"register", answer{status: 500, body: `{"code":"INTERNAL","error":"internal server error: unable to set initial policy: failed to validate policy: x"}`}, backend.ErrConfig},
		{"register", answer{status: 400, body: malformed}, backend.ErrConfig},
		{"register", answer{status: 500, body: "proxy error"}, backend.ErrTemporary},
		{"register", answer{status: 400, body: notConn}, backend.ErrUnavailable},

		// POST /api/v1/policy/resolve.
		{"resolve", answer{status: 400, body: `{"code":"MALFORMED_REQUEST","error":"unable to decode request: x"}`}, backend.ErrConfig},
		{"resolve", answer{status: 500, body: internal}, backend.ErrTemporary},
		{"resolve", answer{status: 200, body: `{"effective":{},"schedulingError":"invalid cron expression"}`}, backend.ErrConfig},

		// DELETE /api/v1/policy.
		{"delete", answer{status: 500, body: internal}, backend.ErrTemporary},
		{"delete", answer{status: 400, body: malformed}, nil},
		{"delete", answer{status: 400, body: notConn}, backend.ErrUnavailable},

		// POST /api/v1/sources/upload.
		{"upload", answer{status: 404, body: `{"code":"NOT_FOUND","error":"no source matching the provided filters"}`}, backend.ErrTemporary},
		{"upload", answer{status: 500, body: internal}, backend.ErrTemporary},
		{"upload", answer{status: 400, body: `{"code":"PATH_NOT_FOUND","error":"x"}`}, nil},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%s %d %s", tt.method, tt.answer.status, tt.answer.body)
		if tt.answer.err != nil {
			name = fmt.Sprintf("%s %v", tt.method, tt.answer.err)
		}

		t.Run(name, func(t *testing.T) {
			m := methods[tt.method]
			f := newFakeKopia()
			b := newBackend(t, f)

			inj := injection{status: tt.answer.status, body: tt.answer.body, err: tt.answer.err}
			f.inject(m.route, inj)

			if tt.answer.status == http.StatusUnauthorized {
				// kopiaclient fetches a new token once and retries.
				f.inject(m.route, inj)
			}

			err := m.call(t, b, f)
			if err == nil {
				t.Fatal("no error")
			}

			if got := kindOf(err); !sameKind(got, tt.want) {
				t.Errorf("kind of %v = %v, want %v", err, got, tt.want)
			}

			var be *backend.Error
			if !errors.As(err, &be) || be.Source != s || !sameKind(be.Kind, tt.want) {
				t.Errorf("err = %#v", err)
			}
		})
	}
}

// The Backend is used for many sources at once.
func TestConcurrentUse(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)

	var wg sync.WaitGroup

	for i := range 16 {
		wg.Go(func() {
			s := src(fmt.Sprintf("/data/app%d", i))
			st := memState{}
			want := backend.Desired{Source: s, Patch: cron(fmt.Sprintf("%d * * * *", i))}

			for range 3 {
				if _, err := reconcile(t.Context(), b, st, want); err != nil {
					t.Errorf("%s: %v", s, err)
				}

				want.Paused = !want.Paused
			}

			if _, err := b.ListSources(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	got, err := b.ListSources(t.Context())
	if err != nil || len(got) != 16 {
		t.Errorf("ListSources = %d sources, %v", len(got), err)
	}
}
