package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// These tests run the reconcile steps package backend documents against
// the REST backend over fakeKopia: the scenarios of backendtest's flow
// tests, so the REST backend is checked to behave like backendtest.Fake.

// memState stands in for internal/state.
type memState map[backend.Source]backend.Applied

// result is what one pass of reconcile did.
type result struct {
	Plan backend.Plan
	// Rejected is the schedule check's error when it rejected the labels
	// and the last applied patch was used instead.
	Rejected error
}

// reconcile handles one source as documented in package backend; it is
// backendtest's reference implementation.
func reconcile(ctx context.Context, b backend.Backend, st memState, want backend.Desired) (result, error) {
	last := st[want.Source]

	plan, err := b.PlanSource(ctx, want, last)
	if err != nil {
		return result{}, err
	}

	var rejected error

	if plan.Action == backend.ActionRegister || !plan.Applied.Patch.Equal(last.Patch) {
		if _, err := b.PreviewSchedule(ctx, plan.Source, plan.Unpaused, 1); err != nil {
			if !backend.IsConfig(err) || plan.Action == backend.ActionRegister {
				return result{Plan: plan}, err
			}

			rejected = err
			fallback := want
			fallback.Patch = last.Patch

			if plan, err = backend.Replan(plan, fallback, last); err != nil {
				return result{}, err
			}
		}
	}

	if plan.Action == backend.ActionNone {
		st[want.Source] = plan.Applied

		return result{Plan: plan, Rejected: rejected}, nil
	}

	st[want.Source] = plan.Intent

	applied, err := b.ApplyPlan(ctx, plan)
	if backend.IsConfig(err) && plan.Action == backend.ActionUpdate && rejected == nil {
		rejected = err
		fallback := want
		fallback.Patch = last.Patch

		if plan, err = backend.Replan(plan, fallback, plan.Intent); err != nil {
			return result{}, err
		}

		st[want.Source] = plan.Intent

		applied, err = b.ApplyPlan(ctx, plan)
	}

	if err != nil {
		return result{Plan: plan, Rejected: rejected}, err
	}

	st[want.Source] = applied

	return result{Plan: plan, Rejected: rejected}, nil
}

func mustReconcile(t *testing.T, b *Backend, st memState, want backend.Desired) result {
	t.Helper()

	r, err := reconcile(t.Context(), b, st, want)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	return r
}

// assertSettled checks that a pass over an unchanged source only plans:
// two reads, no write.
func assertSettled(t *testing.T, f *fakeKopia, b *Backend, st memState, want backend.Desired) {
	t.Helper()

	f.resetRequests()

	r := mustReconcile(t, b, st, want)
	if r.Plan.Action != backend.ActionNone {
		t.Errorf("settled source: Action = %v, policy %s -> %s", r.Plan.Action, r.Plan.Current, r.Plan.Policy)
	}

	assertRoutes(t, f, "GET sources", "GET policy")
}

var errLost = errors.New("connection reset by peer")

func TestFlowLifecycle(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	want := backend.Desired{Source: s, Patch: cron("0 * * * *"), Owners: []string{"app"}}

	// A new container: the schedule is checked, the source registered.
	r := mustReconcile(t, b, st, want)
	if r.Plan.Action != backend.ActionRegister {
		t.Fatalf("Action = %v", r.Plan.Action)
	}

	assertRoutes(t, f, "GET sources", "GET policy", "GET sources", "POST policy/resolve",
		"GET sources", "POST sources", "PUT policy", "GET policy")
	mustPolicy(t, f, si, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if !f.loaded(si) {
		t.Error("kopia server's scheduler does not have the policy")
	}

	assertSettled(t, f, b, st, want)

	// Settings from the web UI.
	f.setPolicy(si, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, b, st, want)

	// The container stops: paused, the section is saved.
	want.Paused = true
	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`)
	assertJSON(t, "saved", st[s].SavedScheduling, `{"cron":["0 * * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]}`)
	assertSettled(t, f, b, st, want)

	// New labels while stopped: checked, and only the saved section
	// changes.
	want.Patch = cron("30 1 * * *")
	f.resetRequests()

	r = mustReconcile(t, b, st, want)
	if r.Plan.Action != backend.ActionNone || len(f.requestsTo("POST policy/resolve")) != 1 || len(f.requestsTo("PUT policy")) != 0 {
		t.Errorf("Action = %v, requests %q", r.Plan.Action, f.routes())
	}

	assertJSON(t, "saved", st[s].SavedScheduling, `{"cron":["30 1 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]}`)

	// It starts again: the web UI's fields are back, with the new cron.
	want.Paused = false
	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"cron":["30 1 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)

	if st[s].SavedScheduling != nil {
		t.Errorf("saved section kept after unpause: %s", st[s].SavedScheduling)
	}

	// The schedule label is emptied: the cron goes, the rest stays.
	want.Patch = backend.PolicyPatch{}
	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, b, st, want)
}

// A pause whose PUT reached kopia server but whose answer was lost keeps
// the saved section, and the next pass writes the pause again.
func TestFlowPauseAnswerLost(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}
	webUI := `{"cron":["0 * * * *"],"intervalSeconds":21600,"runMissed":true}`

	mustReconcile(t, b, st, want)
	f.setPolicy(si, `{"scheduling":`+webUI+`}`)

	want.Paused = true

	f.inject("PUT policy", injection{after: true, err: errLost})

	if _, err := reconcile(t.Context(), b, st, want); !backend.IsUnavailable(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustPolicy(t, f, si, `{"scheduling":{"manual":true}}`)

	if st[s].Pending == nil {
		t.Fatal("the intent was not stored")
	}

	assertJSON(t, "saved", st[s].SavedScheduling, webUI)

	// The next pass confirms the pause with another write.
	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	if st[s].Pending != nil {
		t.Error("pending after a confirmed apply")
	}

	assertSettled(t, f, b, st, want)

	want.Paused = false
	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":`+webUI+`}`)
}

// A pause that never reached kopia server is planned again from scratch.
func TestFlowPauseNotStored(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}

	mustReconcile(t, b, st, want)
	f.setPolicy(si, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true}}`)

	want.Paused = true

	f.inject("PUT policy", injection{status: http.StatusInternalServerError,
		body: `{"code":"INTERNAL","error":"internal server error: unable to set policy: error writing policy manifest: blob error"}`})

	if _, err := reconcile(t.Context(), b, st, want); !backend.IsTemporary(err) || backend.IsUnavailable(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustPolicy(t, f, si, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true}}`)

	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"manual":true}}`)
	assertJSON(t, "saved", st[s].SavedScheduling, `{"cron":["0 * * * *"],"runMissed":true}`)
}

// A registration whose PUT failed after the POST is completed by the next
// pass, although the policy already looks right.
func TestFlowRegisterPutLost(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}

	f.inject("PUT policy", injection{err: errLost})

	if _, err := reconcile(t.Context(), b, st, want); !backend.IsUnavailable(err) {
		t.Fatalf("reconcile: %v", err)
	}

	if !f.known(si) || f.loaded(si) {
		t.Fatalf("known %v, loaded %v", f.known(si), f.loaded(si))
	}

	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	if !f.loaded(si) {
		t.Error("the policy never reached kopia server's scheduler")
	}

	assertSettled(t, f, b, st, want)
}

// After migrating from the Python version the state is empty and some
// sources are paused. A running owner unpauses its source with the labels'
// schedule.
func TestFlowMigratedPause(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	f.setPolicy(si, `{"scheduling":{"manual":true},"retention":{"keepLatest":10}}`)

	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}
	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	mustPolicy(t, f, si, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepLatest":10}}`)
	assertSettled(t, f, b, st, want)
}

// A cron kopia server rejects is caught before anything is written: a
// known source keeps its last patch, a new one is not registered.
func TestFlowRejectedSchedule(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	f.set(func(f *fakeKopia) { f.badCrons["0 25 * * *"] = true })

	if _, err := reconcile(t.Context(), b, st, backend.Desired{Source: s, Patch: cron("0 25 * * *")}); !backend.IsConfig(err) {
		t.Fatalf("new source with a bad cron: %v", err)
	}

	if f.known(si) || len(f.requestsTo("POST sources")) != 0 || len(f.requestsTo("PUT policy")) != 0 {
		t.Fatalf("registered with a bad cron: %q", f.routes())
	}

	mustReconcile(t, b, st, backend.Desired{Source: s, Patch: cron("0 * * * *")})

	r := mustReconcile(t, b, st, backend.Desired{Source: s, Patch: cron("0 25 * * *")})
	if !backend.IsConfig(r.Rejected) || r.Plan.Action != backend.ActionNone {
		t.Errorf("rejected %v, Action %v", r.Rejected, r.Plan.Action)
	}

	mustPolicy(t, f, si, `{"scheduling":{"cron":["0 * * * *"]}}`)

	// While paused the bad cron would only go into the saved section; it
	// is caught all the same.
	r = mustReconcile(t, b, st, backend.Desired{Source: s, Patch: cron("0 25 * * *"), Paused: true})
	if !backend.IsConfig(r.Rejected) {
		t.Errorf("rejected %v", r.Rejected)
	}

	assertJSON(t, "saved", st[s].SavedScheduling, `{"cron":["0 * * * *"]}`)
	mustPolicy(t, f, si, `{"scheduling":{"manual":true}}`)
}

// kopiaNormalize rewrites a policy the way kopia server stores it: it
// decodes into policy.Policy and encodes again, so fields kopia does not
// know are dropped, every section is there, a time of day has both fields
// and a zero maxFileSize is left out.
func kopiaNormalize(p json.RawMessage) json.RawMessage {
	var obj map[string]any
	if err := json.Unmarshal(p, &obj); err != nil {
		panic(err)
	}

	sections := []string{"retention", "files", "errorHandling", "scheduling", "compression", "metadataCompression",
		"splitter", "actions", "osSnapshots", "logging", "upload"}

	for k := range obj {
		if !slices.Contains(sections, k) && k != "noParent" {
			delete(obj, k)
		}
	}

	for _, k := range sections {
		if _, found := obj[k]; !found {
			obj[k] = map[string]any{}
		}
	}

	if s, isMap := obj["scheduling"].(map[string]any); isMap {
		if tod, isList := s["timeOfDay"].([]any); isList {
			for _, e := range tod {
				if m, isMap := e.(map[string]any); isMap {
					if _, found := m["min"]; !found {
						m["min"] = 0
					}
				}
			}
		}
	}

	if files, isMap := obj["files"].(map[string]any); isMap && files["maxFileSize"] == float64(0) {
		delete(files, "maxFileSize")
	}

	out, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}

	return out
}

// kopia server stores policies in its own form. A field it rewrites does
// not make every later pass write the policy again.
func TestFlowKopiaNormalization(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	f.set(func(f *fakeKopia) { f.normalize = kopiaNormalize })

	want := backend.Desired{Source: s, Patch: backend.PolicyPatch{
		Raw: json.RawMessage(`{"scheduling":{"timeOfDay":[{"hour":3}]},"files":{"maxFileSize":0},"futureField":{"x":1}}`),
	}}

	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"retention":{},"files":{},"errorHandling":{},`+
		`"compression":{},"metadataCompression":{},"splitter":{},"actions":{},"osSnapshots":{},"logging":{},"upload":{}}`)
	assertSettled(t, f, b, st, want)

	// An edit elsewhere in the policy costs one write, then it settles
	// again.
	stored, _ := f.policy(si)

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(stored, &obj); err != nil {
		t.Fatal(err)
	}

	obj["retention"] = json.RawMessage(`{"keepDaily":7}`)

	edited, _ := json.Marshal(obj)
	f.setPolicy(si, string(edited))

	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	assertSettled(t, f, b, st, want)

	// A new watcher process (a new fingerprint salt cannot be simulated
	// here) or a lost state: the state's fingerprint is gone, and the
	// policy is written once more, then settles.
	a := st[s]
	a.StoredHash = ""
	st[s] = a

	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("without a fingerprint: Action = %v", r.Plan.Action)
	}

	assertSettled(t, f, b, st, want)
}

// A path that does not exist on kopia server is reported on the source and
// nothing is written.
func TestFlowMissingPath(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/typo")
	f.set(func(f *fakeKopia) { f.missing["/data/typo"] = true })

	_, err := reconcile(t.Context(), b, st, backend.Desired{Source: s, Patch: cron("0 * * * *")})
	if !backend.IsConfig(err) || !errors.Is(err, kopiaclient.ErrPathNotFound) {
		t.Fatalf("reconcile: %v", err)
	}

	if f.known(info("/data/typo")) || len(f.requestsTo("PUT policy")) != 0 {
		t.Errorf("registered a missing path: %q", f.routes())
	}

	if _, found := f.policy(info("/data/typo")); found {
		t.Error("a policy was stored for a missing path")
	}
}

// A label removed while the apply fails before kopia server stores
// anything: the next pass still removes the field.
func TestFlowRemovedLabelApplyNotStored(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	full := backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}, Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}

	mustReconcile(t, b, st, backend.Desired{Source: s, Patch: full})

	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}
	f.inject("PUT policy", injection{status: http.StatusBadGateway, body: "bad gateway"})

	if _, err := reconcile(t.Context(), b, st, want); !backend.IsTemporary(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustReconcile(t, b, st, want)
	mustPolicy(t, f, info("/data/app"), `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// The answer of a registration is lost, then the schedule label is
// emptied: the cron of the lost registration is removed.
func TestFlowRegisterAnswerLostThenLabelRemoved(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")

	f.inject("PUT policy", injection{after: true, err: errLost})

	if _, err := reconcile(t.Context(), b, st, backend.Desired{Source: s, Patch: cron("0 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	mustReconcile(t, b, st, backend.Desired{Source: s})
	mustPolicy(t, f, info("/data/app"), `{}`)
	assertSettled(t, f, b, st, backend.Desired{Source: s})
}

// kopia server's identity changes: the old sources fail with
// ErrWrongIdentity before anything is written, and sources rebuilt from
// the new identity work.
func TestFlowIdentityChange(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	want := backend.Desired{Source: src("/data/app"), Patch: cron("0 * * * *")}

	mustReconcile(t, b, st, want)
	f.set(func(f *fakeKopia) { f.host = "kopia-2" })
	f.resetRequests()

	want.Patch = cron("5 * * * *")

	if _, err := reconcile(t.Context(), b, st, want); !errors.Is(err, backend.ErrWrongIdentity) {
		t.Fatalf("reconcile: %v", err)
	}

	assertRoutes(t, f, "GET sources")

	user, host, err := b.ServerIdentity(t.Context())
	if err != nil || user != fakeUser || host != "kopia-2" {
		t.Fatalf("ServerIdentity = %q, %q, %v", user, host, err)
	}

	want.Source = backend.Source{Host: host, UserName: user, Path: "/data/app"}
	if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionRegister {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	mustPolicy(t, f, kopiaclient.SourceInfo{Host: "kopia-2", UserName: fakeUser, Path: "/data/app"}, `{"scheduling":{"cron":["5 * * * *"]}}`)
}

// After two unconfirmed applies the state's Patch holds the fields of both;
// a rejected label falls back to the patch last asked for, not to that mix,
// so a field an earlier label removed does not come back.
func TestFlowFallbackAfterUnconfirmedApplies(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	f.set(func(f *fakeKopia) { f.badCrons["0 25 * * *"] = true })

	a := backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}, Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}
	mustReconcile(t, b, st, backend.Desired{Source: s, Patch: a})

	f.inject("PUT policy", injection{after: true, err: errLost})

	if _, err := reconcile(t.Context(), b, st, backend.Desired{Source: s, Patch: cron("5 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	f.inject("PUT policy", injection{status: http.StatusInternalServerError, body: `{"code":"INTERNAL","error":"internal server error: boom"}`})

	if _, err := reconcile(t.Context(), b, st, backend.Desired{Source: s, Patch: cron("10 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	r := mustReconcile(t, b, st, backend.Desired{Source: s, Patch: cron("0 25 * * *")})
	if !backend.IsConfig(r.Rejected) {
		t.Fatalf("rejected = %v", r.Rejected)
	}

	// Back to a, the last confirmed patch: the fields of the unconfirmed
	// ones are gone, a's are back.
	mustPolicy(t, f, info("/data/app"), `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, b, st, backend.Desired{Source: s, Patch: a})
}

// kopia server rejects on PUT what policy/resolve does not check (such as
// upload.maxParallelSnapshots on a path): the reconciler falls back to the
// last confirmed patch instead of getting stuck, and the source can still
// be paused.
func TestFlowApplyRejected(t *testing.T) {
	f := newFakeKopia()
	b := newBackend(t, f)
	st := memState{}
	s := src("/data/app")
	si := info("/data/app")
	want := backend.Desired{Source: s, Patch: cron("0 * * * *")}

	mustReconcile(t, b, st, want)

	want.Patch.Raw = json.RawMessage(`{"upload":{"maxParallelSnapshots":2}}`)

	r := mustReconcile(t, b, st, want)
	if !backend.IsConfig(r.Rejected) || !errors.Is(r.Rejected, kopiaclient.ErrInvalidPolicy) {
		t.Fatalf("rejected = %v", r.Rejected)
	}

	mustPolicy(t, f, si, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if st[s].Pending != nil {
		t.Errorf("pending after the fallback: %+v", st[s].Pending)
	}

	want.Paused = true
	mustReconcile(t, b, st, want)
	mustPolicy(t, f, si, `{"scheduling":{"manual":true}}`)
}

// Right after a registration the source can be snapshotted: the POST made
// kopia server know it, even if the refresh of the PUT failed.
func TestFlowSnapshotRightAfterRegister(t *testing.T) {
	for _, refreshFails := range []bool{false, true} {
		f := newFakeKopia()
		b := newBackend(t, f)
		st := memState{}
		s := src("/data/app")
		f.set(func(f *fakeKopia) { f.refreshFails = refreshFails })

		mustReconcile(t, b, st, backend.Desired{Source: s, Patch: cron("0 * * * *")})

		if _, err := b.TriggerSnapshot(t.Context(), s); err != nil {
			t.Errorf("refresh fails %v: TriggerSnapshot: %v", refreshFails, err)
		}

		if f.uploads(info("/data/app")) != 1 {
			t.Errorf("refresh fails %v: uploads = %d", refreshFails, f.uploads(info("/data/app")))
		}
	}
}
