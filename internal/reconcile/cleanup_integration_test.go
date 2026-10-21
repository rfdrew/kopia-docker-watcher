//go:build integration

package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/rest"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// TestIntegrationCleanup runs the cleanup of abandoned sources with the REST
// backend against a real kopia server (kopiatest.DefaultImage unless
// KOPIA_IMAGE names another), with a fake Docker and a real state file.
// The reconciler's clock is a test clock, moved past CLEANUP_GRACE; kopia
// server's runs on time.
//
// The containers inherit the schedule (an empty schedule label), so kopia
// server never snapshots a source on its own; the test takes the snapshots
// itself.
func TestIntegrationCleanup(t *testing.T) {
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

	ctx := t.Context()
	waitRepo(t, c)

	dirs := []string{"/data/clean-pinned", "/data/clean-all", "/data/clean-policy", "/data/clean-kept"}
	s.Exec(t, append([]string{"mkdir", "-p"}, dirs...)...)

	for _, d := range dirs {
		s.Exec(t, "sh", "-c", "echo one > "+d+"/file.txt")
	}

	log := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := rest.New(c, log)
	engine := newFakeEngine()

	st, err := state.Open(filepath.Join(t.TempDir(), "state.json"), state.Options{Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = st.Close() })

	clock := &testClock{t: time.Now()}

	r, err := New(Options{Backend: b, Docker: engine, Store: st, Logger: log, CleanupGrace: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	r.now = clock.now

	source := func(p string) backend.Source {
		return backend.Source{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: p}
	}

	pass := func(t *testing.T, p string) {
		t.Helper()

		if res := r.reconcile(ctx, p); res.err != nil {
			t.Fatalf("pass over %s: %v", p, res.err)
		}
	}

	// snapshot takes a snapshot of src and waits until kopia server has
	// settled after it.
	snapshot := func(t *testing.T, src backend.Source) time.Time {
		t.Helper()

		ticket, err := b.TriggerSnapshot(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		res, err := b.WaitSnapshot(wctx, ticket, nil)
		if err != nil || res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil {
			t.Fatalf("snapshot of %s: %+v, %v", src, res, err)
		}

		settled(t, b, src, res.Snapshot.StartTime)

		return res.Snapshot.StartTime
	}

	// abandon registers p for a container with the cleanup mode, takes n
	// snapshots, removes the container and pauses the source.
	abandon := func(t *testing.T, p, mode string, n int) []time.Time {
		t.Helper()

		id := engine.run(filepath.Base(p), enable, pathLabel(p), "kopia.schedule=", noOnStart, "kopia.cleanup-on-remove="+mode)
		pass(t, p)

		// Each snapshot with other content: kopia server reports the pins
		// of a snapshot on the oldest one with the same content too.
		var starts []time.Time
		for i := range n {
			s.Exec(t, "sh", "-c", fmt.Sprintf("echo %d >> %s/file.txt", i, p))
			starts = append(starts, snapshot(t, source(p)))
		}

		r.HandleEvent(engine.remove(id))
		pass(t, p)
		assertScheduling(t, c, source(p), true)

		return starts
	}

	t.Run("all keeps the pinned snapshots", func(t *testing.T) {
		p := "/data/clean-pinned"
		src := source(p)
		starts := abandon(t, p, "all", 3)

		if err := b.AnnotateSnapshot(ctx, src, starts[1], "", []string{"keep"}); err != nil {
			t.Fatal(err)
		}

		clock.add(2 * time.Hour)
		pass(t, p)

		snaps, err := b.ListSnapshots(ctx, src)
		if err != nil {
			t.Fatal(err)
		}

		if len(snaps) != 1 || !snaps[0].StartTime.Equal(starts[1]) || !slices.Equal(snaps[0].Pins, []string{"keep"}) {
			t.Fatalf("snapshots left = %+v", snaps)
		}

		// The source stays, paused.
		assertScheduling(t, c, src, true)

		if !known(t, b, src) {
			t.Error("the source is gone")
		}

		if _, ok := st.Get(src); !ok {
			t.Error("the record is gone")
		}

		// Nothing more to delete.
		pass(t, p)

		if snaps, err := b.ListSnapshots(ctx, src); err != nil || len(snaps) != 1 {
			t.Errorf("snapshots = %+v, %v", snaps, err)
		}
	})

	t.Run("all deletes the source", func(t *testing.T) {
		p := "/data/clean-all"
		src := source(p)
		abandon(t, p, "all", 2)

		clock.add(2 * time.Hour)
		pass(t, p)

		if known(t, b, src) {
			t.Error("the source is left")
		}

		if _, err := c.GetPolicy(ctx, kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: p}); !errors.Is(err, kopiaclient.ErrNotFound) {
			t.Errorf("GetPolicy = %v, want ErrNotFound", err)
		}

		if resp, err := c.ListSnapshots(ctx, kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: p}, true); err != nil ||
			len(resp.Snapshots) != 0 {
			t.Errorf("snapshots = %+v, %v", resp, err)
		}

		if _, ok := st.Get(src); ok {
			t.Error("the record is left")
		}
	})

	t.Run("policy deletes the policy of a source without snapshots", func(t *testing.T) {
		p := "/data/clean-policy"
		src := source(p)
		abandon(t, p, "policy", 0)

		clock.add(2 * time.Hour)
		pass(t, p)

		if _, err := c.GetPolicy(ctx, kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: p}); !errors.Is(err, kopiaclient.ErrNotFound) {
			t.Errorf("GetPolicy = %v, want ErrNotFound", err)
		}

		if known(t, b, src) {
			t.Error("kopia server still knows the source")
		}

		// The next pass forgets it.
		pass(t, p)

		if _, ok := st.Get(src); ok {
			t.Error("the record is left")
		}
	})

	t.Run("policy keeps a source with snapshots paused", func(t *testing.T) {
		p := "/data/clean-kept"
		src := source(p)
		abandon(t, p, "policy", 1)

		clock.add(2 * time.Hour)
		pass(t, p)

		assertScheduling(t, c, src, true)

		if snaps, err := b.ListSnapshots(ctx, src); err != nil || len(snaps) != 1 {
			t.Errorf("snapshots = %+v, %v", snaps, err)
		}
	})
}

func waitRepo(t *testing.T, c *kopiaclient.Client) {
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

// settled waits until kopia server reports start as the start of the
// source's last snapshot and the source idle: it refreshes the source's
// status only after the task of a run has ended.
func settled(t *testing.T, b backend.Backend, src backend.Source, start time.Time) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for {
		sources, err := b.ListSources(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		for _, s := range sources {
			if s.Source == src && s.Status == backend.StatusIdle && s.LastSnapshot != nil && s.LastSnapshot.StartTime.Equal(start) {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("the source %s has not settled after the snapshot of %v: %+v", src, start, sources)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func known(t *testing.T, b backend.Backend, src backend.Source) bool {
	t.Helper()

	sources, err := b.ListSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return slices.ContainsFunc(sources, func(s backend.SourceStatus) bool { return s.Source == src })
}

// assertScheduling checks whether the policy of src is paused.
func assertScheduling(t *testing.T, c *kopiaclient.Client, src backend.Source, paused bool) {
	t.Helper()

	p, err := c.GetPolicy(t.Context(), kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: src.Path})
	if err != nil {
		t.Fatalf("GetPolicy %s: %v", src, err)
	}

	if policyPaused(p) != paused {
		t.Errorf("policy of %s = %s, want paused %t", src, p, paused)
	}
}
