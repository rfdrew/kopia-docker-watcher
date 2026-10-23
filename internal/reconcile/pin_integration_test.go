//go:build integration

package reconcile

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/rest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/labels"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// TestIntegrationPinOnStop takes the stop snapshot of a container with
// pin-on-stop against a real kopia server (kopiatest.DefaultImage unless
// KOPIA_IMAGE names another): the pin shows in GET /api/v1/snapshots, with
// and without all=1, and the cleanup in mode all, without
// cleanup-delete-pinned, keeps the pinned snapshot while it deletes the
// others.
func TestIntegrationPinOnStop(t *testing.T) {
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

	const p = "/data/pin"

	s.Exec(t, "mkdir", "-p", p)
	s.Exec(t, "sh", "-c", "echo one > "+p+"/file.txt")

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

	src := backend.Source{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: p}
	si := kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: p}

	pass := func(t *testing.T) {
		t.Helper()

		if res := r.reconcile(ctx, p); res.err != nil {
			t.Fatalf("pass over %s: %v", p, res.err)
		}
	}

	// The container inherits the schedule, so kopia server takes no
	// snapshot of its own.
	id := engine.run("app", enable, pathLabel(p), "kopia.schedule=", noOnStart, onStop, "kopia.cleanup-on-remove=all",
		"kopia.pin-on-stop=before-update-{date}")
	pass(t)

	// A snapshot before, of other content: kopia server reports the pins
	// of a snapshot on the oldest one with the same content too.
	ticket, err := b.TriggerSnapshot(ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	res, err := b.WaitSnapshot(wctx, ticket, nil)
	if err != nil || res.Outcome != backend.SnapshotSucceeded || res.Snapshot == nil {
		t.Fatalf("first snapshot: %+v, %v", res, err)
	}

	settled(t, b, src, res.Snapshot.StartTime)
	s.Exec(t, "sh", "-c", "echo two >> "+p+"/file.txt")

	// The stop: its snapshot is followed here, as the waiter would.
	engine.setState(id, docker.StateExited)

	stopped := time.Now()
	ev := engine.event(docker.KindDied, id)
	ev.Time = stopped
	r.HandleEvent(ev)
	pass(t)

	rec, _ := st.Get(src)
	if len(rec.SnapshotWaits) != 1 {
		t.Fatalf("waits = %+v", rec.SnapshotWaits)
	}

	r.wait(wctx, rec.SnapshotWaits[0])

	rec, _ = st.Get(src)
	if len(rec.SnapshotWaits) != 1 || rec.SnapshotWaits[0].Result == nil ||
		rec.SnapshotWaits[0].Result.Outcome != backend.SnapshotSucceeded {
		t.Fatalf("waits = %+v", rec.SnapshotWaits)
	}

	stopStart := rec.SnapshotWaits[0].Result.SnapshotStart
	settled(t, b, src, stopStart)
	pass(t)

	pin := labels.PinName("before-update-{date}", stopped)

	for _, all := range []bool{false, true} {
		resp, err := c.ListSnapshots(ctx, si, all)
		if err != nil {
			t.Fatal(err)
		}

		var pinned []time.Time

		for _, sn := range resp.Snapshots {
			if slices.Contains(sn.Pins, pin) {
				pinned = append(pinned, sn.StartTime)
			}
		}

		if len(resp.Snapshots) != 2 || len(pinned) != 1 || !pinned[0].Equal(stopStart) {
			t.Fatalf("snapshots (all=%t) = %+v, want %q on the one of %v", all, resp.Snapshots, pin, stopStart)
		}
	}

	// The cleanup: all, without cleanup-delete-pinned.
	r.HandleEvent(engine.remove(id))
	pass(t)

	clock.add(2 * time.Hour)
	pass(t)

	snaps, err := b.ListSnapshots(ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	if len(snaps) != 1 || !snaps[0].StartTime.Equal(stopStart) || !slices.Equal(snaps[0].Pins, []string{pin}) {
		t.Fatalf("snapshots left = %+v", snaps)
	}

	assertScheduling(t, c, src, true)
}
