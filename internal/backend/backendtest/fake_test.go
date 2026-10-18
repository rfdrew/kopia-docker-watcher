package backendtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
)

var ctx = context.Background()

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

	dec = json.NewDecoder(bytes.NewReader([]byte(want)))
	dec.UseNumber()

	if err := dec.Decode(&w); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s = %s\nwant %s", name, got, want)
	}
}

func mustPolicy(t *testing.T, f *Fake, src backend.Source, want string) {
	t.Helper()

	got, ok := f.Policy(src)
	if !ok {
		t.Fatalf("no policy for %s", src)
	}

	assertJSON(t, "policy", got, want)
}

func TestIdentityAndPaths(t *testing.T) {
	f := New()

	user, host, err := f.ServerIdentity(ctx)
	if err != nil || user != DefaultUserName || host != DefaultHost {
		t.Fatalf("ServerIdentity = %q, %q, %v", user, host, err)
	}

	for in, want := range map[string]string{
		"/data/app/":     "/data/app",
		"/data//x/../y":  "/data/y",
		"backups":        "/root/backups",
		"/":              "/",
		"../outside/dir": "/outside/dir",
		"~":              "/root",
		"~/backups/":     "/root/backups",
		"~data":          "/rootdata",
	} {
		src, err := f.ResolvePath(ctx, in)
		if err != nil {
			t.Fatal(err)
		}

		if src != f.Source(want) {
			t.Errorf("ResolvePath(%q) = %v, want %v", in, src, f.Source(want))
		}
	}

	if _, err := f.ResolvePath(ctx, ""); err == nil {
		t.Error("ResolvePath(\"\"): no error")
	}

	old := f.Source("/data")
	f.SetIdentity("admin", "nas")

	_, err = f.PlanSource(ctx, backend.Desired{Source: old}, backend.Applied{})
	if !errors.Is(err, backend.ErrWrongIdentity) || backend.IsConfig(err) || backend.IsTemporary(err) {
		t.Errorf("PlanSource of the old identity: %v", err)
	}

	if f.Source("/data") != (backend.Source{Host: "nas", UserName: "admin", Path: "/data"}) {
		t.Errorf("Source after SetIdentity = %v", f.Source("/data"))
	}

	if _, err := f.PlanSource(ctx, backend.Desired{Source: backend.Source{Path: "/data"}}, backend.Applied{}); err == nil {
		t.Error("PlanSource of an incomplete source: no error")
	}
}

func TestListSources(t *testing.T) {
	f := New()
	f.AddSource(f.Source("/b"))
	f.SetPolicy(f.Source("/a"), `{}`)
	f.AddSource(backend.Source{Host: "other", UserName: "root", Path: "/c"})
	f.SetStatus(f.Source("/b"), backend.SourceStatus{Status: backend.StatusUploading, CurrentTask: "7"})

	got, err := f.ListSources(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 || got[0].Source != f.Source("/a") || got[1].Source != f.Source("/b") {
		t.Fatalf("ListSources = %+v", got)
	}

	if got[1].Status != backend.StatusUploading || got[1].CurrentTask != "7" || got[0].Status != backend.StatusIdle {
		t.Errorf("statuses = %+v", got)
	}
}

func TestRegisterAndUpdate(t *testing.T) {
	f := New()
	src := f.Source("/data/app")

	plan, err := f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionRegister || plan.Known || plan.Status != nil || plan.Current != nil {
		t.Fatalf("plan = %+v", plan)
	}

	applied, err := f.ApplyPlan(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}

	if !f.Known(src) || !f.Loaded(src) {
		t.Errorf("Known %v, Loaded %v", f.Known(src), f.Loaded(src))
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["0 * * * *"]}}`)

	hash, _ := backend.Fingerprint(json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`))
	if applied.StoredHash != hash || !applied.Patch.Equal(cron("0 * * * *")) || applied.Pending != nil {
		t.Errorf("applied = %+v", applied)
	}

	plan, err = f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 * * * *")}, applied)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionNone || !plan.Known || plan.Status == nil {
		t.Fatalf("second plan = %+v", plan)
	}

	f.SetPolicy(src, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":7}}`)

	plan, err = f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("5 * * * *")}, applied)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Action != backend.ActionUpdate {
		t.Fatalf("Action = %v", plan.Action)
	}

	if _, err := f.ApplyPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}

	mustPolicy(t, f, src, `{"scheduling":{"cron":["5 * * * *"]},"retention":{"keepDaily":7}}`)

	// Without snapshots kopia server forgets the source with its policy.
	if err := f.DeletePolicy(ctx, src); err != nil {
		t.Fatal(err)
	}

	if _, ok := f.Policy(src); ok || f.Known(src) {
		t.Errorf("after DeletePolicy: policy %v, known %v", ok, f.Known(src))
	}

	// With snapshots the source stays.
	f.SetPolicy(src, `{}`)
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: time.Unix(1, 0)})

	if err := f.DeletePolicy(ctx, src); err != nil {
		t.Fatal(err)
	}

	if _, ok := f.Policy(src); ok || !f.Known(src) {
		t.Errorf("after DeletePolicy with snapshots: policy %v, known %v", ok, f.Known(src))
	}
}

func TestKopiaValidation(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	f.SetPathMissing("/data/missing")
	f.RejectCron("0 25 * * *")

	tests := []struct {
		name string
		plan backend.Plan
	}{
		{"missing path", backend.Plan{Source: f.Source("/data/missing"), Action: backend.ActionRegister,
			Merged: backend.Merged{Policy: json.RawMessage(`{}`)}}},
		{"trailing slash", backend.Plan{Source: f.Source("/data/app/"), Action: backend.ActionUpdate,
			Merged: backend.Merged{Policy: json.RawMessage(`{}`)}}},
		{"manual with cron", backend.Plan{Source: src, Action: backend.ActionUpdate,
			Merged: backend.Merged{Policy: json.RawMessage(`{"scheduling":{"manual":true,"cron":[]}}`)}}},
		{"rejected cron", backend.Plan{Source: src, Action: backend.ActionUpdate,
			Merged: backend.Merged{Policy: json.RawMessage(`{"scheduling":{"cron":["0 25 * * *"]}}`)}}},
		{"malformed", backend.Plan{Source: src, Action: backend.ActionUpdate,
			Merged: backend.Merged{Policy: json.RawMessage(`{"scheduling":5}`)}}},
		{"max parallel snapshots on a path", backend.Plan{Source: src, Action: backend.ActionUpdate,
			Merged: backend.Merged{Policy: json.RawMessage(`{"upload":{"maxParallelSnapshots":2}}`)}}},
	}

	for _, tt := range tests {
		_, err := f.ApplyPlan(ctx, tt.plan)
		if !backend.IsConfig(err) {
			t.Errorf("%s: %v, want ErrConfig", tt.name, err)
		}

		if f.Known(tt.plan.Source) {
			t.Errorf("%s: stored", tt.name)
		}
	}

	if _, err := f.ApplyPlan(ctx, backend.Plan{Source: src, Action: backend.Action(9), Merged: backend.Merged{Policy: json.RawMessage(`{}`)}}); err == nil {
		t.Error("unknown action: no error")
	}

	if _, err := f.ApplyPlan(ctx, backend.Plan{Source: f.Source("/x"), Action: backend.ActionNone}); err != nil || f.Known(f.Source("/x")) {
		t.Errorf("ActionNone: %v, known %v", err, f.Known(f.Source("/x")))
	}
}

func TestPreviewSchedule(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	t1 := time.Date(2026, 10, 18, 13, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	f.SetUpcoming(t1, t2)
	f.RejectCron("bad")

	p, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`), 1)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p.Times, []time.Time{t1}) {
		t.Errorf("Times = %v", p.Times)
	}

	assertJSON(t, "Scheduling", p.Scheduling, `{"cron":["0 * * * *"]}`)

	if p, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{}`), 5); err != nil || len(p.Times) != 2 || string(p.Scheduling) != `{}` {
		t.Errorf("preview = %+v, %v", p, err)
	}

	if p, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"scheduling":{"manual":true}}`), 5); err != nil || len(p.Times) != 0 {
		t.Errorf("paused preview = %+v, %v", p, err)
	}

	if _, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"scheduling":{"manual":true,"cron":[]}}`), 1); !backend.IsConfig(err) {
		t.Errorf("manual with cron: %v", err)
	}

	// policy/resolve checks only the schedule.
	if _, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"upload":{"maxParallelSnapshots":2}}`), 1); err != nil {
		t.Errorf("max parallel snapshots: %v", err)
	}

	if _, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"scheduling":{"cron":["bad"]}}`), 1); !backend.IsConfig(err) {
		t.Errorf("rejected cron: %v", err)
	}

	if _, err := f.PreviewSchedule(ctx, f.Source("/x"), json.RawMessage(`[]`), 1); !backend.IsConfig(err) {
		t.Errorf("malformed policy: %v", err)
	}
}

func TestFailures(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	boom := errors.New("boom")

	f.FailNext(ServerIdentity, boom)

	if _, _, err := f.ServerIdentity(ctx); !errors.Is(err, boom) {
		t.Errorf("FailNext: %v", err)
	}

	if _, _, err := f.ServerIdentity(ctx); err != nil {
		t.Errorf("second call: %v", err)
	}

	plan, err := f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	f.FailNext(ApplyPlan, boom)

	if _, err := f.ApplyPlan(ctx, plan); !errors.Is(err, boom) || f.Known(src) {
		t.Errorf("FailNext ApplyPlan: %v, known %v", err, f.Known(src))
	}

	f.FailNextAfter(ApplyPlan, boom)

	if _, err := f.ApplyPlan(ctx, plan); !errors.Is(err, boom) || !f.Known(src) || !f.Loaded(src) {
		t.Errorf("FailNextAfter ApplyPlan: %v, known %v", err, f.Known(src))
	}

	src2 := f.Source("/data/other")

	plan2, err := f.PlanSource(ctx, backend.Desired{Source: src2, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	f.FailRegisterPut(boom)

	if _, err := f.ApplyPlan(ctx, plan2); !errors.Is(err, boom) || !f.Known(src2) || f.Loaded(src2) {
		t.Errorf("FailRegisterPut: %v, known %v, loaded %v", err, f.Known(src2), f.Loaded(src2))
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = f.ListSources(canceled)
	if !errors.Is(err, context.Canceled) || backend.IsTemporary(err) || backend.IsConfig(err) {
		t.Errorf("canceled context: %v", err)
	}

	expired, cancel := context.WithDeadline(ctx, time.Unix(1, 0))
	defer cancel()

	_, err = f.ListSources(expired)
	if !errors.Is(err, context.DeadlineExceeded) || backend.IsTemporary(err) || backend.IsConfig(err) {
		t.Errorf("expired context: %v", err)
	}
}

func TestHook(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	f.SetPolicy(src, `{"scheduling":{"cron":["0 * * * *"]}}`)

	// The hook edits the policy, as the web UI could between two calls.
	f.SetHook(func(c Call) error {
		if c.Method == ApplyPlan {
			f.SetPolicy(src, `{"scheduling":{"cron":["0 * * * *"]},"retention":{"keepDaily":3}}`)
		}

		if c.Method == DeletePolicy {
			return errors.New("refused by hook")
		}

		return nil
	})

	plan, err := f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("5 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.ApplyPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}

	// The plan was made before the edit and overwrites it, as kopia server
	// has no conditional write.
	mustPolicy(t, f, src, `{"scheduling":{"cron":["5 * * * *"]}}`)

	if err := f.DeletePolicy(ctx, src); err == nil {
		t.Error("hook error not returned")
	}

	f.SetHook(nil)

	if err := f.DeletePolicy(ctx, src); err != nil {
		t.Error(err)
	}
}

func TestCalls(t *testing.T) {
	f := New()
	a, b := f.Source("/a"), f.Source("/b")

	_, _ = f.PlanSource(ctx, backend.Desired{Source: a}, backend.Applied{})
	_, _ = f.PlanSource(ctx, backend.Desired{Source: b}, backend.Applied{})
	_, _ = f.PreviewSchedule(ctx, a, json.RawMessage(`{}`), 3)
	_ = f.DeletePolicy(ctx, a)

	if got := len(f.Calls()); got != 4 {
		t.Errorf("%d calls", got)
	}

	if got := f.CallsTo(PlanSource); len(got) != 2 || got[0].Source != a || got[1].Source != b {
		t.Errorf("CallsTo = %+v", got)
	}

	if got := f.CallsFor(a, PreviewSchedule); len(got) != 1 || got[0].N != 3 || string(got[0].Policy) != `{}` {
		t.Errorf("CallsFor = %+v", got)
	}

	if got := f.CallsFor(b, DeletePolicy); len(got) != 0 {
		t.Errorf("CallsFor b = %+v", got)
	}

	f.ResetCalls()

	if got := f.Calls(); len(got) != 0 {
		t.Errorf("after reset: %+v", got)
	}
}

func TestSnapshots(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	f.SetClock(func() time.Time { return now })

	if _, err := f.TriggerSnapshot(ctx, src); !backend.IsTemporary(err) {
		t.Errorf("trigger of an unknown source: %v", err)
	}

	earlier := now.Add(-time.Hour)
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: earlier, EndTime: earlier.Add(time.Minute)})

	ticket, err := f.TriggerSnapshot(ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	if ticket.Source != src || !ticket.TriggeredAt.Equal(now) || !ticket.LastSnapshotStart.Equal(earlier) {
		t.Errorf("ticket = %+v", ticket)
	}

	var saved []backend.SnapshotTicket

	res, err := f.WaitSnapshot(ctx, ticket, func(t backend.SnapshotTicket) { saved = append(saved, t) })
	if err != nil {
		t.Fatal(err)
	}

	if res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil || !res.Snapshot.StartTime.Equal(now) {
		t.Errorf("result = %+v", res)
	}

	if len(saved) != 1 || saved[0].TaskID != res.TaskID || saved[0].TaskDescription == "" {
		t.Errorf("saved tickets = %+v", saved)
	}

	f.QueueSnapshotResult(src, backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "disk full"})

	if res, err := f.WaitSnapshot(ctx, ticket, nil); err != nil || res.Outcome != backend.SnapshotFailed {
		t.Errorf("queued result = %+v, %v", res, err)
	}

	// A checkpoint with the same start time is never annotated.
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: now, EndTime: now.Add(-time.Second), Incomplete: "checkpoint"})

	if err := f.AnnotateSnapshot(ctx, src, now, "app", []string{"pin"}); err != nil {
		t.Fatal(err)
	}

	for _, s := range f.Snapshots(src) {
		if s.Incomplete == "checkpoint" && s.Description != "" {
			t.Error("checkpoint annotated")
		}

		if s.StartTime.Equal(now) && s.Incomplete == "" && s.Description != "app" {
			t.Errorf("snapshot not annotated: %+v", s)
		}
	}

	if got := f.Annotations(src); len(got) != 1 || got[0].Description != "app" || !reflect.DeepEqual(got[0].AddPins, []string{"pin"}) {
		t.Errorf("annotations = %+v", got)
	}

	if err := f.AnnotateSnapshot(ctx, src, now.Add(time.Minute), "x", nil); !errors.Is(err, backend.ErrSnapshotNotFound) {
		t.Errorf("annotate a missing snapshot: %v", err)
	}

	if err := f.AnnotateSnapshot(ctx, f.Source("/none"), now, "x", nil); !errors.Is(err, backend.ErrSnapshotNotFound) {
		t.Errorf("annotate an unknown source: %v", err)
	}
}

func TestConcurrentUse(t *testing.T) {
	f := New()

	var wg sync.WaitGroup
	for i := range 8 {
		src := f.Source(fmt.Sprintf("/data/%d", i))

		wg.Go(func() {
			st := memState{}
			for j := range 10 {
				want := backend.Desired{Source: src, Patch: cron(fmt.Sprintf("%d * * * *", j)), Paused: j%3 == 0}
				if _, err := reconcile(ctx, f, st, want); err != nil {
					t.Error(err)
				}
			}
		})
	}

	wg.Wait()

	if got, _ := f.ListSources(ctx); len(got) != 8 {
		t.Errorf("%d sources", len(got))
	}
}

// Every method that takes a source rejects one of another identity.
func TestWrongIdentityEverywhere(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: time.Unix(1, 0)})

	plan, err := f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 * * * *")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	f.SetIdentity("admin", "nas")

	check := func(name string, err error) {
		t.Helper()

		if !errors.Is(err, backend.ErrWrongIdentity) || backend.IsConfig(err) || backend.IsTemporary(err) {
			t.Errorf("%s: %v", name, err)
		}
	}

	_, err = f.ApplyPlan(ctx, plan)
	check("ApplyPlan", err)

	if p, _ := f.Policy(src); p != nil {
		t.Errorf("ApplyPlan stored %s", p)
	}

	_, err = f.PreviewSchedule(ctx, src, plan.Unpaused, 1)
	check("PreviewSchedule", err)
	check("DeletePolicy", f.DeletePolicy(ctx, src))

	_, err = f.TriggerSnapshot(ctx, src)
	check("TriggerSnapshot", err)

	_, err = f.WaitSnapshot(ctx, backend.SnapshotTicket{Source: src}, nil)
	check("WaitSnapshot", err)
	check("AnnotateSnapshot", f.AnnotateSnapshot(ctx, src, time.Unix(1, 0), "x", nil))

	if got, err := f.ListSources(ctx); err != nil || len(got) != 0 {
		t.Errorf("ListSources of the new identity = %v, %v", got, err)
	}
}

// A source path that is not absolute is refused like the REST backend
// refuses it: kopia server would resolve it against its home directory on
// registration only.
func TestRelativePathRefused(t *testing.T) {
	f := New()

	for _, p := range []string{"data/app", "~/app"} {
		src := f.Source(p)

		_, err := f.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 * * * *")}, backend.Applied{})
		if err == nil || backend.IsConfig(err) || backend.IsTemporary(err) || !strings.Contains(err.Error(), "not absolute") {
			t.Errorf("PlanSource(%q): %v", p, err)
		}

		_, err = f.ApplyPlan(ctx, backend.Plan{Source: src, Action: backend.ActionRegister, Merged: backend.Merged{Policy: json.RawMessage(`{}`)}})
		if err == nil || !strings.Contains(err.Error(), "not absolute") {
			t.Errorf("ApplyPlan(%q): %v", p, err)
		}

		if f.Known(src) {
			t.Errorf("%q registered", p)
		}
	}
}

// A failure set with FailNextAfter is used up by the next call of the
// method, also when that call fails for another reason first, and never by
// an ApplyPlan of ActionNone, which sends nothing.
func TestFailNextAfterUse(t *testing.T) {
	f := New()
	boom := errors.New("boom")
	f.SetPathMissing("/missing")

	f.FailNextAfter(ApplyPlan, boom)

	none := backend.Plan{Source: f.Source("/x"), Action: backend.ActionNone}
	if _, err := f.ApplyPlan(ctx, none); err != nil {
		t.Errorf("ActionNone: %v", err)
	}

	missing, err := f.PlanSource(ctx, backend.Desired{Source: f.Source("/missing")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.ApplyPlan(ctx, missing); !backend.IsConfig(err) {
		t.Errorf("missing path: %v", err)
	}

	good, err := f.PlanSource(ctx, backend.Desired{Source: f.Source("/good")}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	if applied, err := f.ApplyPlan(ctx, good); err != nil || applied.StoredHash == "" {
		t.Errorf("the failure leaked to the next call: %+v, %v", applied, err)
	}

	f.FailNextAfter(ListSources, boom)

	if got, err := f.ListSources(ctx); !errors.Is(err, boom) || got != nil {
		t.Errorf("ListSources = %v, %v; want no result and boom", got, err)
	}

	if _, err := f.ListSources(ctx); err != nil {
		t.Errorf("second ListSources: %v", err)
	}

	if len(f.CallsTo(ApplyPlan)) != 3 {
		t.Errorf("ApplyPlan calls = %d, want the ActionNone one recorded too", len(f.CallsTo(ApplyPlan)))
	}
}

// Failures can be aimed at one source while others go on.
func TestFailuresForOneSource(t *testing.T) {
	f := New()
	a, b := f.Source("/a"), f.Source("/b")
	boom := errors.New("boom")

	f.FailNextFor(a, PlanSource, boom)
	f.FailNextAfterFor(a, ApplyPlan, boom)
	f.FailRegisterPutFor(a, boom)

	if _, err := f.PlanSource(ctx, backend.Desired{Source: b}, backend.Applied{}); err != nil {
		t.Errorf("b: %v", err)
	}

	if _, err := f.PlanSource(ctx, backend.Desired{Source: a}, backend.Applied{}); !errors.Is(err, boom) {
		t.Errorf("a: %v", err)
	}

	planB, _ := f.PlanSource(ctx, backend.Desired{Source: b}, backend.Applied{})
	if _, err := f.ApplyPlan(ctx, planB); err != nil || !f.Loaded(b) {
		t.Errorf("b apply: %v, loaded %v", err, f.Loaded(b))
	}

	planA, _ := f.PlanSource(ctx, backend.Desired{Source: a}, backend.Applied{})
	if _, err := f.ApplyPlan(ctx, planA); !errors.Is(err, boom) || !f.Known(a) || f.Loaded(a) {
		t.Errorf("a apply: %v, known %v, loaded %v", err, f.Known(a), f.Loaded(a))
	}
}

// The hook sees every call, also one that a queued failure or a done
// context fails.
func TestHookSeesEveryCall(t *testing.T) {
	f := New()

	var seen []Method

	f.SetHook(func(c Call) error {
		seen = append(seen, c.Method)

		return nil
	})

	f.FailNext(ListSources, errors.New("boom"))

	_, _ = f.ListSources(ctx)

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, _ = f.ListSources(canceled)

	if len(seen) != 2 || len(f.Calls()) != 2 {
		t.Errorf("hook saw %v of %d calls", seen, len(f.Calls()))
	}
}

// The fake and its records share no memory with the caller.
func TestNoSharedMemory(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	f.SetPolicy(src, `{"x":1}`)
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: time.Unix(1, 0)})

	p, _ := f.Policy(src)
	p[2] = 'y'
	mustPolicy(t, f, src, `{"x":1}`)

	owners := []string{"app"}
	raw := json.RawMessage(`{"retention":{"keepDaily":7}}`)

	plan, err := f.PlanSource(ctx, backend.Desired{Source: src, Owners: owners, Patch: backend.PolicyPatch{Raw: raw}}, backend.Applied{})
	if err != nil {
		t.Fatal(err)
	}

	plan.Status.LastSnapshot.Description = "changed by the caller"
	owners[0] = "other"
	copy(raw, `{"retention":{"keepDaily":9}}`)

	if got, _ := f.ListSources(ctx); got[0].LastSnapshot.Description != "" {
		t.Errorf("LastSnapshot shared with a plan: %+v", got[0].LastSnapshot)
	}

	c := f.CallsTo(PlanSource)[0]
	if c.Desired.Owners[0] != "app" || !strings.Contains(string(c.Desired.Patch.Raw), "7") {
		t.Errorf("recorded call shares memory: %+v", c.Desired)
	}

	c.Desired.Owners[0] = "changed"

	if f.CallsTo(PlanSource)[0].Desired.Owners[0] != "app" {
		t.Error("Calls returns shared memory")
	}
}

func TestSetStatusKeepsLastSnapshot(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	start := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: start})

	f.SetStatus(src, backend.SourceStatus{Status: backend.StatusUploading, CurrentTask: "3"})

	got, _ := f.ListSources(ctx)
	if got[0].Status != backend.StatusUploading || got[0].LastSnapshot == nil || !got[0].LastSnapshot.StartTime.Equal(start) {
		t.Errorf("status = %+v", got[0])
	}

	other := &backend.SnapshotInfo{StartTime: start.Add(time.Hour)}
	f.SetStatus(src, backend.SourceStatus{LastSnapshot: other})
	other.Description = "changed by the caller"

	if got, _ := f.ListSources(ctx); got[0].LastSnapshot.Description != "" || !got[0].LastSnapshot.StartTime.Equal(start.Add(time.Hour)) {
		t.Errorf("status = %+v", got[0].LastSnapshot)
	}
}

// The preview of a paused source's Unpaused policy comes from the policy
// passed in, not from the stored pause.
func TestPreviewOfPausedSource(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	t1 := time.Date(2026, 10, 18, 13, 0, 0, 0, time.UTC)
	f.SetUpcoming(t1)
	f.SetPolicy(src, `{"scheduling":{"manual":true}}`)

	p, err := f.PreviewSchedule(ctx, src, json.RawMessage(`{"scheduling":{"cron":["0 * * * *"]}}`), 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(p.Times) != 1 {
		t.Errorf("Times = %v", p.Times)
	}

	assertJSON(t, "Scheduling", p.Scheduling, `{"cron":["0 * * * *"]}`)
}

// SetPolicyCheck stands for kopia server's decoding and validation of the
// whole policy.
func TestPolicyCheck(t *testing.T) {
	f := New()
	src := f.Source("/data/app")

	f.SetPolicyCheck(func(p json.RawMessage) error {
		var v struct {
			Retention struct {
				KeepDaily *int `json:"keepDaily"`
			} `json:"retention"`
		}

		return json.Unmarshal(p, &v)
	})

	bad := json.RawMessage(`{"retention":{"keepDaily":"seven"}}`)

	if _, err := f.PreviewSchedule(ctx, src, bad, 1); !backend.IsConfig(err) {
		t.Errorf("PreviewSchedule: %v", err)
	}

	plan := backend.Plan{Source: src, Action: backend.ActionRegister, Merged: backend.Merged{Policy: bad}}
	if _, err := f.ApplyPlan(ctx, plan); !backend.IsConfig(err) || f.Known(src) {
		t.Errorf("ApplyPlan: %v, known %v", err, f.Known(src))
	}

	f.SetPolicyCheck(nil)

	if _, err := f.ApplyPlan(ctx, plan); err != nil {
		t.Errorf("without the check: %v", err)
	}
}

// kopia server validates the policy of a registration too, before it
// knows the source.
func TestRegisterValidates(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	f.RejectCron("0 25 * * *")

	for _, policy := range []string{`{"scheduling":{"cron":["0 25 * * *"]}}`, `{"scheduling":{"manual":true,"cron":[]}}`} {
		_, err := f.ApplyPlan(ctx, backend.Plan{Source: src, Action: backend.ActionRegister, Merged: backend.Merged{Policy: json.RawMessage(policy)}})
		if !backend.IsConfig(err) || f.Known(src) {
			t.Errorf("%s: %v, known %v", policy, err, f.Known(src))
		}
	}
}

func TestAnnotateEndedFirst(t *testing.T) {
	f := New()
	src := f.Source("/data/app")
	start := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: start, EndTime: start.Add(2 * time.Minute)})
	f.AddSnapshot(src, backend.SnapshotInfo{StartTime: start, EndTime: start.Add(time.Minute)})

	if err := f.AnnotateSnapshot(ctx, src, start, "app", nil); err != nil {
		t.Fatal(err)
	}

	if snaps := f.Snapshots(src); snaps[1].Description != "app" || snaps[0].Description != "" {
		t.Errorf("snapshots = %+v", snaps)
	}
}

func TestWaitUnknownSource(t *testing.T) {
	f := New()
	src := f.Source("/none")

	r, err := f.WaitSnapshot(ctx, backend.SnapshotTicket{Source: src}, nil)
	if err != nil || r.Outcome != backend.SnapshotUnknown || f.Known(src) {
		t.Errorf("WaitSnapshot = %+v, %v, known %v", r, err, f.Known(src))
	}
}

// A registration whose POST succeeded cannot fail with ErrConfig.
func TestFailRegisterPutRejectsConfigErrors(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()

	New().FailRegisterPut(&backend.Error{Op: "ApplyPlan", Kind: backend.ErrConfig, Err: errors.New("refused")})
}

// A write that took effect cannot fail with ErrConfig.
func TestFailNextAfterRejectsConfigErrors(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()

	New().FailNextAfter(ApplyPlan, &backend.Error{Op: "ApplyPlan", Kind: backend.ErrConfig, Err: errors.New("refused")})
}
