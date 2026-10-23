//go:build integration

package reconcile

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/rest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// execDocker lists the containers of a fake Docker but runs the hooks
// through a real one.
type execDocker struct {
	*fakeEngine
	real *docker.Client
}

func (d execDocker) Exec(ctx context.Context, id string, cmd []string) (docker.ExecResult, error) {
	return d.real.Exec(ctx, id, cmd)
}

// TestIntegrationHooks takes a snapshot with hooks against a real kopia
// server (kopiatest.DefaultImage unless KOPIA_IMAGE names another), with
// the REST backend and a real state file. The hooks run through the real
// Docker in kopia server's own container, which stands for the container
// of a database: the pre-exec hook writes a dump into the source's
// directory, and the dump is in the snapshot. The fake Docker lists that
// container with the hook labels.
func TestIntegrationHooks(t *testing.T) {
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

	dc, err := docker.New("kopia-docker-watcher-integration-test")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = dc.Close() })

	waitRepo(t, c)

	const p = "/data/hooked"

	s.Exec(t, "mkdir", "-p", p)

	log := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := rest.New(c, log)
	labels := labelMap(
		enable, pathLabel(p), "kopia.min-interval=0",
		"kopia.pre-exec=echo dumped > "+p+"/dump.sql",
		`kopia.post-exec=["sh","-c","echo done > `+p+`/post.txt"]`,
	)

	engine := newFakeEngine()
	engine.add(docker.Container{ID: s.ContainerID(), Name: "db", State: docker.StateRunning, Labels: labels})

	st, err := state.Open(filepath.Join(t.TempDir(), "state.json"), state.Options{Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = st.Close() })

	r, err := New(Options{Backend: b, Docker: execDocker{fakeEngine: engine, real: dc}, Store: st, Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		r.hookers.Wait()
		r.waiters.Wait()
	})

	r.runCtx, r.hookCtx = ctx, ctx

	src := backend.Source{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: p}

	pass := func(t *testing.T) {
		t.Helper()

		if res := r.reconcile(ctx, p); res.err != nil {
			t.Fatalf("pass over %s: %v", p, res.err)
		}

		r.hookers.Wait()
		r.waiters.Wait()
	}

	// The source is registered off kopia server's schedule; the watcher
	// has the next time of its default hourly schedule.
	pass(t)
	assertScheduling(t, c, src, true)

	rec, _ := st.Get(src)
	if rec.Schedule == nil || rec.Schedule.Next.IsZero() || rec.Schedule.Next.Minute() != 0 || !rec.Schedule.Next.After(time.Now()) ||
		rec.Schedule.Next.After(time.Now().Add(time.Hour)) {
		t.Fatalf("schedule %+v", rec.Schedule)
	}

	// A start takes a snapshot with the hooks around it.
	r.HandleEvent(docker.Event{Kind: docker.KindStarted, Action: "start", ContainerID: s.ContainerID(), Name: "db",
		Labels: labels, Time: time.Now()})

	deadline := time.Now().Add(3 * time.Minute)

	for {
		pass(t)

		rec, _ := st.Get(src)
		if rec.HookRun == nil && len(rec.SnapshotWaits) == 0 && !rec.LastTriggered.IsZero() {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("the snapshot with hooks did not end: %+v", rec)
		}

		time.Sleep(200 * time.Millisecond)
	}

	if out := s.Exec(t, "cat", p+"/post.txt"); strings.TrimSpace(out) != "done" {
		t.Fatalf("post-exec did not run: %q", out)
	}

	snaps, err := c.ListSnapshots(ctx, kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: src.Path}, true)
	if err != nil {
		t.Fatal(err)
	}

	if len(snaps.Snapshots) != 1 || snaps.Snapshots[0].IncompleteReason != "" {
		t.Fatalf("snapshots %+v", snaps.Snapshots)
	}

	// The CLI in kopia server's container reads the repository too.
	if out := s.Exec(t, "kopia", "show", snaps.Snapshots[0].RootID+"/dump.sql"); strings.TrimSpace(out) != "dumped" {
		t.Fatalf("dump.sql in the snapshot = %q", out)
	}

	if snaps.Snapshots[0].Description != "db" {
		t.Errorf("description %q", snaps.Snapshots[0].Description)
	}
}
