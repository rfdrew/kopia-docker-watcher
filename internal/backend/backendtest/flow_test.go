package backendtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

// These tests run the reconcile steps the backend package documents
// against the fake, to check that the contract holds together: plan, check
// the schedule, store the intent, apply, store the result.

// memState stands in for internal/state.
type memState map[backend.Source]backend.Applied

// result is what one pass of reconcile did.
type result struct {
	Plan backend.Plan
	// Rejected is the schedule check's error when it rejected the labels
	// and the last applied patch was used instead.
	Rejected error
}

// reconcile handles one source as documented in package backend.
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
		// kopia server refused what the schedule check let through: fall
		// back to the last confirmed patch.
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

func mustReconcile(t *testing.T, f *Fake, st memState, want backend.Desired) result {
	t.Helper()

	r, err := reconcile(ctx, f, st, want)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	return r
}

// A pass over an unchanged source plans and does nothing else.
func assertSettled(t *testing.T, f *Fake, st memState, want backend.Desired) {
	t.Helper()

	f.ResetCalls()

	r := mustReconcile(t, f, st, want)
	if r.Plan.Action != backend.ActionNone {
		t.Errorf("settled source: Action = %v, policy %s -> %s", r.Plan.Action, r.Plan.Current, r.Plan.Policy)
	}

	for _, c := range f.Calls() {
		if c.Method != PlanSource {
			t.Errorf("settled source: unexpected %s", c.Method)
		}
	}
}

func TestFlowLifecycle(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	want := backend.Desired{Source: src, Patch: cron("0 * * * *"), Owners: []string{"app"}}

	// A new container: the source is registered.
	r := mustReconcile(t, f, st, want)
	if r.Plan.Action != backend.ActionRegister || len(f.CallsTo(PreviewSchedule)) != 1 {
		t.Fatalf("Action = %v, %d previews", r.Plan.Action, len(f.CallsTo(PreviewSchedule)))
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]}}`)
	assertSettled(t, f, st, want)

	// Settings from the web UI.
	f.SetPolicy(src, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, st, want)

	// The container stops: paused, the section is saved.
	want.Paused = true
	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"manual":true},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, st, want)

	// New labels while stopped: only the saved section changes.
	want.Patch = cron("30 1 * * *")
	r = mustReconcile(t, f, st, want)

	if r.Plan.Action != backend.ActionNone || len(f.CallsTo(PreviewSchedule)) != 1 {
		t.Errorf("Action = %v, %d previews", r.Plan.Action, len(f.CallsTo(PreviewSchedule)))
	}

	assertJSON(t, "saved", st[src].SavedScheduling, `{"cron":["30 1 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]}`)

	// It starts again: the web UI's fields are back, with the new cron.
	want.Paused = false
	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"cron":["30 1 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)

	if st[src].SavedScheduling != nil {
		t.Errorf("saved section kept after unpause: %s", st[src].SavedScheduling)
	}

	// The schedule label is emptied: the cron goes, the rest stays.
	want.Patch = backend.PolicyPatch{}
	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, st, want)
}

// A pause whose PUT reached kopia server but whose answer was lost keeps
// the saved section, and the next pass writes the pause again.
func TestFlowPauseAnswerLost(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}
	webUI := `{"cron":["0 * * * *"],"intervalSeconds":21600,"runMissed":true}`

	mustReconcile(t, f, st, want)
	f.SetPolicy(src, `{"scheduling":`+webUI+`}`)

	want.Paused = true

	f.FailNextAfter(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrUnavailable, Err: context.DeadlineExceeded})

	if _, err := reconcile(ctx, f, st, want); !backend.IsUnavailable(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustPolicy(t, f, src, `{"scheduling":{"manual":true}}`)

	if st[src].Pending == nil {
		t.Fatal("the intent was not stored")
	}

	assertJSON(t, "saved", st[src].SavedScheduling, webUI)

	// The next pass confirms the pause with another write.
	f.ResetCalls()

	if r := mustReconcile(t, f, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	if st[src].Pending != nil {
		t.Error("pending after a confirmed apply")
	}

	assertSettled(t, f, st, want)

	want.Paused = false
	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":`+webUI+`}`)
}

// A pause that never reached kopia server is planned again from scratch.
func TestFlowPauseNotStored(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}

	mustReconcile(t, f, st, want)
	f.SetPolicy(src, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true}}`)

	want.Paused = true

	f.FailNext(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("500")})

	if _, err := reconcile(ctx, f, st, want); !backend.IsTemporary(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"],"runMissed":true}}`)

	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"manual":true}}`)
	assertJSON(t, "saved", st[src].SavedScheduling, `{"cron":["0 * * * *"],"runMissed":true}`)
}

// A registration whose PUT failed after the POST is completed by the next
// pass, although the policy already looks right.
func TestFlowRegisterPutLost(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}

	f.FailRegisterPut(&backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("timeout")})

	if _, err := reconcile(ctx, f, st, want); err == nil {
		t.Fatal("no error")
	}

	if !f.Known(src) || f.Loaded(src) {
		t.Fatalf("known %v, loaded %v", f.Known(src), f.Loaded(src))
	}

	if r := mustReconcile(t, f, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	if !f.Loaded(src) {
		t.Error("the policy never reached kopia server's scheduler")
	}

	assertSettled(t, f, st, want)
}

// After migrating from the Python version the state is empty and some
// sources are paused. A running owner unpauses its source with the labels'
// schedule.
func TestFlowMigratedPause(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	f.SetPolicy(src, `{"scheduling":{"manual":true},"retention":{"keepLatest":10}}`)

	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}
	if r := mustReconcile(t, f, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepLatest":10}}`)
	assertSettled(t, f, st, want)
}

// A cron kopia server rejects is caught before anything is written: a
// known source keeps its last patch, a new one is not registered.
func TestFlowRejectedSchedule(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	f.RejectCron("0 25 * * *")

	if _, err := reconcile(ctx, f, st, backend.Desired{Source: src, Patch: cron("0 25 * * *")}); !backend.IsConfig(err) {
		t.Fatalf("new source with a bad cron: %v", err)
	}

	if f.Known(src) {
		t.Fatal("registered with a bad cron")
	}

	mustReconcile(t, f, st, backend.Desired{Source: src, Patch: cron("0 * * * *")})

	r := mustReconcile(t, f, st, backend.Desired{Source: src, Patch: cron("0 25 * * *")})
	if !backend.IsConfig(r.Rejected) || r.Plan.Action != backend.ActionNone {
		t.Errorf("rejected %v, Action %v", r.Rejected, r.Plan.Action)
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]}}`)

	// While paused the bad cron would only go into the saved section; it
	// is caught all the same.
	r = mustReconcile(t, f, st, backend.Desired{Source: src, Patch: cron("0 25 * * *"), Paused: true})
	if !backend.IsConfig(r.Rejected) {
		t.Errorf("rejected %v", r.Rejected)
	}

	assertJSON(t, "saved", st[src].SavedScheduling, `{"cron":["0 * * * *"]}`)
}

// kopia server stores policies in its own form. A field it rewrites does
// not make every later pass write the policy again.
func TestFlowKopiaNormalization(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")

	// Like kopia server: a time of day always has both fields, and a zero
	// maxFileSize is dropped.
	f.SetNormalizer(func(p json.RawMessage) json.RawMessage {
		var obj map[string]any
		if err := json.Unmarshal(p, &obj); err != nil {
			panic(err)
		}

		if s, ok := obj["scheduling"].(map[string]any); ok {
			if tod, ok := s["timeOfDay"].([]any); ok {
				for _, e := range tod {
					if m, ok := e.(map[string]any); ok {
						if _, ok := m["min"]; !ok {
							m["min"] = 0
						}
					}
				}
			}
		}

		if files, ok := obj["files"].(map[string]any); ok && files["maxFileSize"] == float64(0) {
			delete(files, "maxFileSize")
		}

		out, err := json.Marshal(obj)
		if err != nil {
			panic(err)
		}

		return out
	})

	want := backend.Desired{Source: src, Patch: backend.PolicyPatch{
		Raw: json.RawMessage(`{"scheduling":{"timeOfDay":[{"hour":3}]},"files":{"maxFileSize":0}}`),
	}}

	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"files":{}}`)
	assertSettled(t, f, st, want)

	// An edit elsewhere in the policy costs one write, then it settles
	// again.
	f.SetPolicy(src, `{"scheduling":{"timeOfDay":[{"hour":3,"min":0}]},"files":{},"retention":{"keepDaily":7}}`)

	if r := mustReconcile(t, f, st, want); r.Plan.Action != backend.ActionUpdate {
		t.Errorf("Action = %v", r.Plan.Action)
	}

	assertSettled(t, f, st, want)
}

// Paths whose source kopia server rejects are reported per source.
func TestFlowMissingPath(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/typo")
	f.SetPathMissing(src.Path)

	_, err := reconcile(ctx, f, st, backend.Desired{Source: src, Patch: cron("0 * * * *")})
	if !backend.IsConfig(err) {
		t.Fatalf("reconcile: %v", err)
	}

	if f.Known(src) {
		t.Error("registered a missing path")
	}
}

// A label removed while the apply fails before kopia server stores
// anything: the next pass still removes the field.
func TestFlowRemovedLabelApplyNotStored(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	full := backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}, Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}

	mustReconcile(t, f, st, backend.Desired{Source: src, Patch: full})

	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}
	f.FailNext(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("500")})

	if _, err := reconcile(ctx, f, st, want); !backend.IsTemporary(err) {
		t.Fatalf("reconcile: %v", err)
	}

	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]}}`)
}

// The answer of a registration is lost, then the schedule label is
// emptied: the cron of the lost registration is removed.
func TestFlowRegisterAnswerLostThenLabelRemoved(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")

	f.FailNextAfter(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrUnavailable, Err: errors.New("timeout")})

	if _, err := reconcile(ctx, f, st, backend.Desired{Source: src, Patch: cron("0 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	mustReconcile(t, f, st, backend.Desired{Source: src})
	mustPolicy(t, f, src, `{}`)
	assertSettled(t, f, st, backend.Desired{Source: src})
}

// kopia server's identity changes: the old sources fail with
// ErrWrongIdentity, and sources rebuilt from the new identity work.
func TestFlowIdentityChange(t *testing.T) {
	f := New()
	st := memState{}
	want := backend.Desired{Source: f.Source("/data/app"), Patch: cron("0 * * * *")}

	mustReconcile(t, f, st, want)
	f.SetIdentity("root", "kopia-2")

	if _, err := reconcile(ctx, f, st, want); !errors.Is(err, backend.ErrWrongIdentity) {
		t.Fatalf("reconcile: %v", err)
	}

	user, host, err := f.ServerIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want.Source = backend.Source{Host: host, UserName: user, Path: "/data/app"}
	if r := mustReconcile(t, f, st, want); r.Plan.Action != backend.ActionRegister {
		t.Errorf("Action = %v", r.Plan.Action)
	}
}

// After two unconfirmed applies the state's Patch holds the fields of both;
// a rejected label falls back to the patch last asked for, not to that mix,
// so a field an earlier label removed does not come back.
func TestFlowFallbackAfterUnconfirmedApplies(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	f.RejectCron("0 25 * * *")

	a := backend.PolicyPatch{Scheduling: backend.SchedulingPatch{Cron: []string{"0 * * * *"}}, Raw: json.RawMessage(`{"retention":{"keepDaily":7}}`)}
	mustReconcile(t, f, st, backend.Desired{Source: src, Patch: a})

	f.FailNextAfter(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrUnavailable, Err: errors.New("timeout")})

	if _, err := reconcile(ctx, f, st, backend.Desired{Source: src, Patch: cron("5 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	f.FailNext(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrTemporary, Err: errors.New("500")})

	if _, err := reconcile(ctx, f, st, backend.Desired{Source: src, Patch: cron("10 * * * *")}); err == nil {
		t.Fatal("no error")
	}

	r := mustReconcile(t, f, st, backend.Desired{Source: src, Patch: cron("0 25 * * *")})
	if !backend.IsConfig(r.Rejected) {
		t.Fatalf("rejected = %v", r.Rejected)
	}

	// Back to a, the last confirmed patch: the fields of the unconfirmed
	// ones are gone, a's are back.
	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`)
	assertSettled(t, f, st, backend.Desired{Source: src, Patch: a})
}

// kopia server rejects on PUT what policy/resolve does not check (such as
// upload.maxParallelSnapshots on a path): the reconciler falls back to the
// last confirmed patch instead of getting stuck, and the source can still
// be paused.
func TestFlowApplyRejected(t *testing.T) {
	f := New()
	st := memState{}
	src := f.Source("/data/app")
	want := backend.Desired{Source: src, Patch: cron("0 * * * *")}

	mustReconcile(t, f, st, want)

	want.Patch.Raw = json.RawMessage(`{"upload":{"maxParallelSnapshots":2}}`)

	r := mustReconcile(t, f, st, want)
	if !backend.IsConfig(r.Rejected) {
		t.Fatalf("rejected = %v", r.Rejected)
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]}}`)

	if st[src].Pending != nil {
		t.Errorf("pending after the fallback: %+v", st[src].Pending)
	}

	want.Paused = true
	mustReconcile(t, f, st, want)
	mustPolicy(t, f, src, `{"scheduling":{"manual":true}}`)
}
