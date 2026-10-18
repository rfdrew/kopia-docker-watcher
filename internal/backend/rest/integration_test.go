//go:build integration

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
)

// TestIntegrationBackend runs the REST backend against a real kopia server
// (kopiatest.DefaultImage unless KOPIA_IMAGE names another), with the
// reconcile steps of package backend.
//
// kopia server snapshots a source registered with a schedule right away:
// runMissed is on by default, and a source never snapshotted counts as
// missed. Assertions that need a source without snapshots register it
// paused.
func TestIntegrationBackend(t *testing.T) {
	s := kopiatest.Start(t, kopiatest.Options{})

	c, err := kopiaclient.New(kopiaclient.Options{
		URL:             s.URL,
		Username:        kopiatest.UIUser,
		Password:        kopiatest.UIPassword,
		CertFingerprint: s.CertFingerprint,
		Timeout:         time.Minute,
		UserAgent:       "kopia-docker-watcher-integration-test",
	})
	if err != nil {
		t.Fatal(err)
	}

	waitConnected(t, c)

	b := New(c, slog.New(slog.NewTextHandler(t.Output(), nil)))
	ctx := t.Context()

	s.Exec(t, "mkdir", "-p", "/data/ui", "/data/pause", "/data/python", "/data/reject", "/data/delete")

	user, host, err := b.ServerIdentity(ctx)
	if err != nil || user != kopiatest.Username || host != kopiatest.Hostname {
		t.Fatalf("ServerIdentity = %q, %q, %v", user, host, err)
	}

	source := func(p string) backend.Source { return backend.Source{Host: host, UserName: user, Path: p} }

	t.Run("ResolvePath", func(t *testing.T) {
		for in, want := range map[string]string{
			kopiatest.HelloDir + "/":           kopiatest.HelloDir,
			kopiatest.HelloDir + "/../hello/.": kopiatest.HelloDir,
			"/no/such/dir/":                    "/no/such/dir",
		} {
			got, err := b.ResolvePath(ctx, in)
			if err != nil || got != source(want) {
				t.Errorf("ResolvePath(%q) = %v, %v", in, got, err)
			}
		}
	})

	t.Run("register, then sources/upload right away", func(t *testing.T) {
		src := source(kopiatest.HelloDir)
		st := memState{}
		want := backend.Desired{Source: src, Patch: cron("0 3 * * *")}

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionRegister {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		// No 404: the POST made kopia server know the source, the PUT
		// refreshed it. kopia server may have snapshotted it already, so
		// the ticket's last snapshot is not checked.
		ticket, err := b.TriggerSnapshot(ctx, src)
		if err != nil {
			t.Fatalf("TriggerSnapshot right after the registration: %v", err)
		}

		if ticket.Source != src {
			t.Errorf("ticket = %+v", ticket)
		}

		assertScheduling(t, policyOf(t, c, src), `{"cron":["0 3 * * *"]}`)

		sources, err := b.ListSources(ctx)
		if err != nil || !hasSource(sources, src) {
			t.Errorf("ListSources = %+v, %v", sources, err)
		}

		// kopia server stores the policy in its own form; the next pass
		// writes nothing.
		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionNone {
			t.Errorf("second pass: Action = %v, %s -> %s", r.Plan.Action, r.Plan.Current, r.Plan.Policy)
		}

		// The schedule shows in the preview.
		plan, err := b.PlanSource(ctx, want, st[src])
		if err != nil {
			t.Fatal(err)
		}

		preview, err := b.PreviewSchedule(ctx, src, plan.Unpaused, 3)
		if err != nil || len(preview.Times) != 3 {
			t.Fatalf("PreviewSchedule = %+v, %v", preview, err)
		}

		for _, tm := range preview.Times {
			if tm.Minute() != 0 || tm.Hour() != 3 {
				t.Errorf("upcoming time %v does not match 0 3 * * *", tm)
			}
		}

		// The effective section has the path's cron and what the global
		// policy adds, such as runMissed.
		var effective struct {
			Cron []string `json:"cron"`
		}

		if err := json.Unmarshal(preview.Scheduling, &effective); err != nil || !reflect.DeepEqual(effective.Cron, []string{"0 3 * * *"}) {
			t.Errorf("effective scheduling = %s (%v)", preview.Scheduling, err)
		}
	})

	t.Run("fields set in the web UI survive the merge", func(t *testing.T) {
		src := source("/data/ui")
		st := memState{}
		want := backend.Desired{Source: src, Patch: cron("0 4 * * *")}

		mustReconcile(t, b, st, want)

		// The web UI writes the whole policy, with its own fields.
		setPolicy(t, c, src, `{"scheduling":{"cron":["0 4 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]},`+
			`"retention":{"keepDaily":14,"keepLatest":3},"files":{"ignoreDotFiles":[".kopiaignore"],"ignore":["*.tmp"]},`+
			`"compression":{"compressorName":"zstd"}}`)

		want.Patch = cron("15 5 * * *")

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got := policyOf(t, c, src)
		assertScheduling(t, got, `{"cron":["15 5 * * *"],"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]}`)
		assertSection(t, got, "retention", `{"keepDaily":14,"keepLatest":3}`)
		assertSection(t, got, "files", `{"ignoreDotFiles":[".kopiaignore"],"ignore":["*.tmp"]}`)
		assertSection(t, got, "compression", `{"compressorName":"zstd"}`)

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionNone {
			t.Errorf("settled: Action = %v", r.Plan.Action)
		}

		// The schedule label is emptied: only the cron goes.
		want.Patch = backend.PolicyPatch{}
		mustReconcile(t, b, st, want)

		got = policyOf(t, c, src)
		assertScheduling(t, got, `{"runMissed":true,"timeOfDay":[{"hour":2,"min":30}]}`)
		assertSection(t, got, "retention", `{"keepDaily":14,"keepLatest":3}`)
	})

	t.Run("pause is set and lifted", func(t *testing.T) {
		src := source("/data/pause")
		st := memState{}
		want := backend.Desired{Source: src, Patch: cron("0 6 * * *")}

		mustReconcile(t, b, st, want)
		setPolicy(t, c, src, `{"scheduling":{"cron":["0 6 * * *"],"runMissed":true,"intervalSeconds":43200},"retention":{"keepDaily":7}}`)

		want.Paused = true

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got := policyOf(t, c, src)
		assertScheduling(t, got, `{"manual":true}`)
		assertSection(t, got, "retention", `{"keepDaily":7}`)
		assertJSON(t, "saved", st[src].SavedScheduling, `{"cron":["0 6 * * *"],"runMissed":true,"intervalSeconds":43200}`)

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionNone {
			t.Errorf("settled while paused: Action = %v", r.Plan.Action)
		}

		// A paused policy has no upcoming snapshots.
		preview, err := b.PreviewSchedule(ctx, src, got, 1)
		if err != nil || len(preview.Times) != 0 {
			t.Errorf("preview of the paused policy = %+v, %v", preview, err)
		}

		want.Paused = false

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got = policyOf(t, c, src)
		assertScheduling(t, got, `{"cron":["0 6 * * *"],"runMissed":true,"intervalSeconds":43200}`)
		assertSection(t, got, "retention", `{"keepDaily":7}`)

		if st[src].SavedScheduling != nil {
			t.Errorf("saved section kept: %s", st[src].SavedScheduling)
		}

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionNone {
			t.Errorf("settled: Action = %v", r.Plan.Action)
		}
	})

	t.Run("a pause without a saved section is lifted with the labels' fields", func(t *testing.T) {
		src := source("/data/python")

		// Paused by the Python version, the watcher's state is empty.
		setPolicy(t, c, src, `{"scheduling":{"manual":true},"retention":{"keepLatest":10}}`)

		st := memState{}
		want := backend.Desired{Source: src, Patch: cron("0 7 * * *")}

		if r := mustReconcile(t, b, st, want); r.Plan.Action != backend.ActionUpdate {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		got := policyOf(t, c, src)
		assertScheduling(t, got, `{"cron":["0 7 * * *"]}`)
		assertSection(t, got, "retention", `{"keepLatest":10}`)
	})

	t.Run("a path that does not exist is PATH_NOT_FOUND", func(t *testing.T) {
		src := source(kopiatest.DataDir + "/missing")

		_, err := reconcile(ctx, b, memState{}, backend.Desired{Source: src, Patch: cron("0 * * * *")})
		if !backend.IsConfig(err) || !errors.Is(err, kopiaclient.ErrPathNotFound) {
			t.Fatalf("err = %v, want ErrConfig with ErrPathNotFound", err)
		}

		sources, err := b.ListSources(ctx)
		if err != nil || hasSource(sources, src) {
			t.Errorf("ListSources = %+v, %v", sources, err)
		}

		if _, err := c.GetPolicy(ctx, toInfo(src)); !errors.Is(err, kopiaclient.ErrNotFound) {
			t.Errorf("GetPolicy: %v, want ErrNotFound", err)
		}
	})

	t.Run("kopia server rejects", func(t *testing.T) {
		src := source("/data/reject")
		st := memState{}
		want := backend.Desired{Source: src, Patch: cron("0 * * * *")}

		mustReconcile(t, b, st, want)

		// A bad cron is caught by the preview: the last patch stays.
		r := mustReconcile(t, b, st, backend.Desired{Source: src, Patch: cron("0 25 * * *")})
		if !backend.IsConfig(r.Rejected) || r.Plan.Action != backend.ActionNone {
			t.Errorf("bad cron: rejected %v, Action %v", r.Rejected, r.Plan.Action)
		}

		// Written anyway, it is ErrConfig too.
		plan, err := b.PlanSource(ctx, backend.Desired{Source: src, Patch: cron("0 25 * * *")}, st[src])
		if err != nil {
			t.Fatal(err)
		}

		_, err = b.ApplyPlan(ctx, plan)
		if !backend.IsConfig(err) || !errors.Is(err, kopiaclient.ErrInvalidPolicy) {
			t.Errorf("bad cron: ApplyPlan = %v", err)
		}

		// What the preview does not check, the PUT rejects: back to the
		// last patch.
		want.Patch.Raw = json.RawMessage(`{"upload":{"maxParallelSnapshots":2}}`)

		r = mustReconcile(t, b, st, want)
		if !backend.IsConfig(r.Rejected) {
			t.Errorf("maxParallelSnapshots: rejected %v", r.Rejected)
		}

		assertScheduling(t, policyOf(t, c, src), `{"cron":["0 * * * *"]}`)

		// A trailing slash is a path kopia server refuses a policy for.
		slash := source(kopiatest.HelloDir + "/")

		_, err = reconcile(ctx, b, memState{}, backend.Desired{Source: slash, Patch: cron("0 * * * *")})
		if !backend.IsConfig(err) {
			t.Errorf("trailing slash: %v", err)
		}
	})

	t.Run("another identity", func(t *testing.T) {
		other := backend.Source{Host: "other-host", UserName: user, Path: "/data/ui"}

		_, err := b.PlanSource(ctx, backend.Desired{Source: other}, backend.Applied{})
		if !errors.Is(err, backend.ErrWrongIdentity) {
			t.Errorf("err = %v, want ErrWrongIdentity", err)
		}
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		// Registered paused, so kopia server does not snapshot it.
		src := source("/data/delete")

		if r := mustReconcile(t, b, memState{}, backend.Desired{Source: src, Patch: cron("0 8 * * *"), Paused: true}); r.Plan.Action != backend.ActionRegister {
			t.Fatalf("Action = %v", r.Plan.Action)
		}

		assertScheduling(t, policyOf(t, c, src), `{"manual":true}`)

		if err := b.DeletePolicy(ctx, src); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetPolicy(ctx, toInfo(src)); !errors.Is(err, kopiaclient.ErrNotFound) {
			t.Errorf("GetPolicy after delete: %v", err)
		}

		// Without snapshots kopia server forgets the source.
		sources, err := b.ListSources(ctx)
		if err != nil || hasSource(sources, src) {
			t.Errorf("ListSources = %+v, %v", sources, err)
		}

		if err := b.DeletePolicy(ctx, src); err != nil {
			t.Errorf("deleting a missing policy: %v", err)
		}
	})
}

func waitConnected(t *testing.T, c *kopiaclient.Client) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	for {
		st, err := c.RepoStatus(ctx)
		if err == nil && st.Connected {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("repository not connected: status %+v, error %v", st, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func hasSource(sources []backend.SourceStatus, src backend.Source) bool {
	for _, s := range sources {
		if s.Source == src {
			return true
		}
	}

	return false
}

func setPolicy(t *testing.T, c *kopiaclient.Client, src backend.Source, policy string) {
	t.Helper()

	if err := c.SetPolicy(t.Context(), toInfo(src), json.RawMessage(policy)); err != nil {
		t.Fatal(err)
	}
}

func policyOf(t *testing.T, c *kopiaclient.Client, src backend.Source) json.RawMessage {
	t.Helper()

	p, err := c.GetPolicy(t.Context(), toInfo(src))
	if err != nil {
		t.Fatalf("GetPolicy %s: %v", src, err)
	}

	return p
}

func assertScheduling(t *testing.T, policy json.RawMessage, want string) {
	t.Helper()

	assertSection(t, policy, "scheduling", want)
}

// assertSection compares one section of a policy by meaning.
func assertSection(t *testing.T, policy json.RawMessage, name, want string) {
	t.Helper()

	var p map[string]any
	if err := json.Unmarshal(policy, &p); err != nil {
		t.Fatal(err)
	}

	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(p[name], w) {
		t.Errorf("%s = %v\nwant %v\n(policy %s)", name, p[name], w, policy)
	}
}
