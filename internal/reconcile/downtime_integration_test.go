//go:build integration

package reconcile

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/rest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiatest"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// stopTestImage stays alive with sleep as PID 1.
const stopTestImage = "busybox:1.37"

// checkedBackend runs check before every TriggerSnapshot.
type checkedBackend struct {
	backend.Backend
	check func()
}

func (b checkedBackend) TriggerSnapshot(ctx context.Context, src backend.Source) (backend.SnapshotTicket, error) {
	b.check()
	return b.Backend.TriggerSnapshot(ctx, src)
}

// TestIntegrationStopDuringBackup takes a snapshot of a container with
// kopia.stop-during-backup against a real kopia server (kopiatest.
// DefaultImage unless KOPIA_IMAGE names another) and a real Docker: the
// container is stopped when the snapshot is queued and runs again, started
// anew, once the snapshot has ended.
func TestIntegrationStopDuringBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()

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

	waitRepo(t, c)

	dc, err := docker.New("kopia-docker-watcher-integration-test")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = dc.Close() })

	api, err := client.New(client.FromEnv)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = api.Close() })

	pull, err := api.ImagePull(ctx, stopTestImage, client.ImagePullOptions{})
	if err != nil {
		t.Fatalf("pull %s: %v", stopTestImage, err)
	}

	if err := pull.Wait(ctx); err != nil {
		t.Fatalf("pull %s: %v", stopTestImage, err)
	}

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	name := fmt.Sprintf("kdw-it-%x", b)
	p := "/data/" + name

	s.Exec(t, "mkdir", "-p", p)
	s.Exec(t, "sh", "-c", "echo data > "+p+"/file.txt")

	stopTimeout := 1
	created, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{Name: name, Config: &container.Config{
		Image:       stopTestImage,
		Cmd:         []string{"sleep", "600"},
		StopTimeout: &stopTimeout,
		Labels: map[string]string{
			"kopia.enable": "true", "kopia.path": p, "kopia.min-interval": "0", "kopia.stop-during-backup": "true",
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	id := created.ID

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		_, _ = api.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true})
	})

	if err := dc.Start(ctx, id); err != nil {
		t.Fatal(err)
	}

	before, err := dc.Inspect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))

	var atTrigger []docker.State

	kb := checkedBackend{Backend: rest.New(c, log), check: func() {
		ctr, err := dc.Inspect(ctx, id)
		if err != nil {
			t.Errorf("inspect at the trigger: %v", err)
		}

		atTrigger = append(atTrigger, ctr.State)
	}}

	st, err := state.Open(filepath.Join(t.TempDir(), "state.json"), state.Options{Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = st.Close() })

	r, err := New(Options{Backend: kb, Docker: dc, Store: st, Logger: log})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(func() {
		stop()
		r.hookers.Wait()
		r.waiters.Wait()
	})

	r.runCtx, r.hookCtx = runCtx, runCtx

	src := backend.Source{Host: kopiatest.Hostname, UserName: kopiatest.Username, Path: p}

	pass := func(t *testing.T) {
		t.Helper()

		if res := r.reconcile(runCtx, p); res.err != nil {
			t.Fatalf("pass over %s: %v", p, res.err)
		}

		r.hookers.Wait()
		r.waiters.Wait()
	}

	pass(t)

	// A start takes a snapshot with the container stopped.
	r.HandleEvent(docker.Event{Kind: docker.KindStarted, Action: "start", ContainerID: id, Name: name,
		Labels: before.Labels, Time: time.Now()})

	deadline := time.Now().Add(3 * time.Minute)

	for {
		pass(t)

		rec, _ := st.Get(src)
		if rec.HookRun == nil && len(rec.SnapshotWaits) == 0 && !rec.LastTriggered.IsZero() {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("the snapshot did not end: %+v", rec)
		}

		time.Sleep(200 * time.Millisecond)
	}

	if len(atTrigger) != 1 || atTrigger[0] != docker.StateExited {
		t.Fatalf("container at the trigger: %v, want exited", atTrigger)
	}

	after, err := dc.Inspect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if after.State != docker.StateRunning || !after.StartedAt.After(before.StartedAt) {
		t.Fatalf("after the snapshot the container is %s, started at %v (before %v)", after.State, after.StartedAt,
			before.StartedAt)
	}

	if held := st.Held(); len(held) != 0 {
		t.Fatalf("held = %+v", held)
	}

	snaps, err := c.ListSnapshots(ctx, kopiaclient.SourceInfo{Host: src.Host, UserName: src.UserName, Path: src.Path}, true)
	if err != nil {
		t.Fatal(err)
	}

	if len(snaps.Snapshots) != 1 || snaps.Snapshots[0].IncompleteReason != "" {
		t.Fatalf("snapshots %+v", snaps.Snapshots)
	}
}
