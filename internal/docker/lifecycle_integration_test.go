//go:build integration

package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// TestIntegrationLifecycle stops, starts, pauses and unpauses a container
// of a real Docker daemon, the one DOCKER_HOST or the local socket points
// to, and checks what Inspect says after each.
func TestIntegrationLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	c, err := New("kopia-docker-watcher-integration-test")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = c.Close() })

	pull, err := c.api.ImagePull(ctx, testImage, client.ImagePullOptions{})
	if err != nil {
		t.Fatalf("pull %s: %v", testImage, err)
	}

	if err := pull.Wait(ctx); err != nil {
		t.Fatalf("pull %s: %v", testImage, err)
	}

	// sleep as PID 1 ignores SIGTERM: a short stop timeout kills it.
	stopTimeout := 1
	id := createContainer(t, c, randomName(t), &container.Config{Image: testImage, Cmd: []string{"sleep", "600"},
		StopTimeout: &stopTimeout}, nil)

	state := func(want State) Container {
		t.Helper()

		ctr, err := c.Inspect(ctx, id)
		if err != nil || ctr.State != want {
			t.Fatalf("container is %s (%v), want %s", ctr.State, err, want)
		}

		return ctr
	}

	if got := state(StateCreated); !got.StartedAt.IsZero() {
		t.Fatalf("a container that never ran started at %v", got.StartedAt)
	}

	if err := c.Start(ctx, id); err != nil {
		t.Fatal(err)
	}

	first := state(StateRunning).StartedAt
	if first.IsZero() {
		t.Fatal("no start time")
	}

	if err := c.Pause(ctx, id); err != nil {
		t.Fatal(err)
	}

	state(StatePaused)

	if err := c.Pause(ctx, id); !errors.Is(err, ErrNotRunning) {
		t.Errorf("pause of a paused container: %v", err)
	}

	if err := c.Unpause(ctx, id); err != nil {
		t.Fatal(err)
	}

	state(StateRunning)

	if err := c.Unpause(ctx, id); !errors.Is(err, ErrNotPaused) {
		t.Errorf("unpause of a running container: %v", err)
	}

	start := time.Now()

	if err := c.Stop(ctx, id); err != nil {
		t.Fatal(err)
	}

	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("the stop took %s: the container's stop timeout was not used", d)
	}

	if got := state(StateExited); !got.StartedAt.Equal(first) {
		t.Errorf("a stop changed the start time: %v, was %v", got.StartedAt, first)
	}

	if err := c.Stop(ctx, id); err != nil {
		t.Errorf("stop of a stopped container: %v", err)
	}

	if err := c.Start(ctx, id); err != nil {
		t.Fatal(err)
	}

	if got := state(StateRunning); !got.StartedAt.After(first) {
		t.Errorf("start time %v after a restart, was %v", got.StartedAt, first)
	}

	if err := c.Start(ctx, id); err != nil {
		t.Errorf("start of a running container: %v", err)
	}

	if err := c.Stop(ctx, "kdw-it-gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("stop of a missing container: %v", err)
	}
}
