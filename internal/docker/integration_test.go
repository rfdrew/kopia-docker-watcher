//go:build integration

package docker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// testImage stays alive with sleep as PID 1, which ignores SIGTERM and
// SIGHUP: there is no handler, and PID 1 gets no default actions.
const testImage = "busybox:1.37"

// TestIntegrationDocker drives a real Docker daemon, the one DOCKER_HOST
// or the local socket points to: lifecycle events through Watcher, list
// and inspect, a replay from since, kill -s HUP, and a restart policy with
// a stop during the restart delay.
func TestIntegrationDocker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
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

	evs := startIntegrationWatcher(t, c)

	// Wait until the subscription is up: events before it are not replayed
	// to a first subscription.
	waitEvent(t, evs, "", KindResync)

	run := randomName(t)
	labels := map[string]string{"kopia.enable": "true", "kopia.path": "/data", "kdw.test": run}

	t.Run("lifecycle", func(t *testing.T) {
		id := createContainer(t, c, run+"-a", &container.Config{
			Image:   testImage,
			Cmd:     []string{"sleep", "600"},
			Labels:  labels,
			Volumes: map[string]struct{}{"/data": {}},
		}, nil)

		created := waitEvent(t, evs, id, KindChanged)

		if _, err := c.api.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
			t.Fatal(err)
		}

		started := waitEvent(t, evs, id, KindStarted)
		if started.Name != run+"-a" || started.Labels["kopia.enable"] != "true" {
			t.Errorf("start event = %+v", started)
		}

		// A new subscription from just before the create event replays it
		// first.
		checkReplay(ctx, t, c, id, created)

		ctr, err := c.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if ctr.State != StateRunning || ctr.Name != run+"-a" || ctr.Labels["kopia.path"] != "/data" {
			t.Errorf("inspect = %+v", ctr)
		}

		if len(ctr.Mounts) != 1 || ctr.Mounts[0].Type != "volume" || ctr.Mounts[0].Destination != "/data" ||
			ctr.Mounts[0].Name == "" || ctr.Mounts[0].Driver != "local" || ctr.Mounts[0].Source == "" {
			t.Errorf("mounts = %+v", ctr.Mounts)
		}

		// kill -s HUP only signals the process, and the container runs on.
		// The daemon records a kill event, which the subscription leaves
		// out. (It also marks the container as stopped by hand, which can
		// switch off a restart policy; this container has none.)
		if _, err := c.api.ContainerKill(ctx, id, client.ContainerKillOptions{Signal: "HUP"}); err != nil {
			t.Fatal(err)
		}

		noEvent(t, evs, id, 2*time.Second)

		if ctr, err := c.Inspect(ctx, id); err != nil || !ctr.State.Running() {
			t.Fatalf("after HUP: %+v, %v", ctr, err)
		}

		timeout := 1
		if _, err := c.api.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
			t.Fatal(err)
		}

		// docker stop sends kill, which is left out, then die and stop in
		// no fixed order: the stop waits only until the state changes,
		// while die is recorded after the state is saved.
		got := map[string]bool{}
		for range 2 {
			got[waitEvent(t, evs, id, KindDied).Action] = true
		}

		if !got["die"] || !got["stop"] {
			t.Errorf("events after docker stop: %v, want die and stop", got)
		}

		ctr, err = c.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if ctr.State != StateExited || !StoppedByDie(true, ctr.State) {
			t.Errorf("after stop: state %q", ctr.State)
		}

		// Stopped containers are listed too.
		if ctr, ok := findContainer(ctx, t, c, id); !ok || ctr.State != StateExited || ctr.Labels["kdw.test"] != run {
			t.Errorf("listed: %+v, %v", ctr, ok)
		}

		if _, err := c.api.ContainerRemove(ctx, id, client.ContainerRemoveOptions{RemoveVolumes: true}); err != nil {
			t.Fatal(err)
		}

		removed := waitEvent(t, evs, id, KindRemoved)
		for k, v := range labels {
			if removed.Labels[k] != v {
				t.Errorf("labels of the removed container = %v, want %v", removed.Labels, labels)
				break
			}
		}

		for _, k := range []string{"name", "image"} {
			if _, ok := removed.Labels[k]; ok {
				t.Errorf("labels of the removed container contain %q: %v", k, removed.Labels)
			}
		}

		if _, err := c.Inspect(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("inspect after removal: %v, want ErrNotFound", err)
		}

		if _, ok := findContainer(ctx, t, c, id); ok {
			t.Error("removed container still listed")
		}
	})

	t.Run("restart policy", func(t *testing.T) {
		// A crash loop: the restart delay doubles from 100ms, so the
		// container soon spends nearly all its time restarting.
		id := createContainer(t, c, run+"-b", &container.Config{
			Image:  testImage,
			Cmd:    []string{"sh", "-c", "exit 3"},
			Labels: labels,
		}, &container.HostConfig{RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyAlways}})

		waitEvent(t, evs, id, KindChanged)

		if _, err := c.api.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
			t.Fatal(err)
		}

		waitEvent(t, evs, id, KindStarted)
		waitEvent(t, evs, id, KindDied)

		// The die of a crash loop is not a stop.
		ctr, err := c.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if StoppedByDie(true, ctr.State) {
			t.Errorf("state after die = %q, counted as stopped", ctr.State)
		}

		// docker stop during the restart delay: Docker cancels the restart
		// and sends stop without die.
		waitState(ctx, t, c, id, StateRestarting)

		timeout := 1
		if _, err := c.api.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &timeout}); err != nil {
			t.Fatal(err)
		}

		waitAction(t, evs, id, "stop")

		ctr, err = c.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if ctr.State != StateExited || !StoppedByDie(true, ctr.State) {
			t.Errorf("after stop: state %q", ctr.State)
		}
	})
}

func startIntegrationWatcher(t *testing.T, c *Client) <-chan Event {
	t.Helper()

	evs := make(chan Event, 1000)
	w := NewWatcher(c, slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug})))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		w.Run(ctx, func(ev Event) { evs <- ev })
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return evs
}

// waitEvent returns the next event of container id, which must be of kind;
// events of other containers are skipped. An empty id waits for
// KindResync.
func waitEvent(t *testing.T, evs <-chan Event, id string, kind Kind) Event {
	t.Helper()

	timeout := time.After(time.Minute)

	for {
		select {
		case ev := <-evs:
			if ev.ContainerID != id {
				continue
			}

			if ev.Kind != kind {
				t.Fatalf("event %v (%s), want %v", ev.Kind, ev.Action, kind)
			}

			return ev
		case <-timeout:
			t.Fatalf("no %v event for %s", kind, id)
		}
	}
}

// waitAction returns the first event of container id with the action,
// skipping the container's other events.
func waitAction(t *testing.T, evs <-chan Event, id, action string) Event {
	t.Helper()

	timeout := time.After(time.Minute)

	for {
		select {
		case ev := <-evs:
			if ev.ContainerID == id && ev.Action == action {
				return ev
			}
		case <-timeout:
			t.Fatalf("no %s event for %s", action, id)
		}
	}
}

// waitState polls until the container is in state.
func waitState(ctx context.Context, t *testing.T, c *Client, id string, state State) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	for {
		ctr, err := c.Inspect(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if ctr.State == state {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("container %s stays %q, want %q", id, ctr.State, state)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func noEvent(t *testing.T, evs <-chan Event, id string, d time.Duration) {
	t.Helper()

	timeout := time.After(d)

	for {
		select {
		case ev := <-evs:
			if ev.ContainerID == id {
				t.Fatalf("unexpected event %v (%s)", ev.Kind, ev.Action)
			}
		case <-timeout:
			return
		}
	}
}

// checkReplay subscribes from shortly before the time of from, as Watcher
// does, and expects from first among the events of its container. From
// its exact time the replay could stop short of it: the daemon's buffer is
// not quite in time order.
func checkReplay(ctx context.Context, t *testing.T, c *Client, id string, from Event) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	evs, errs := c.Events(ctx, from.Time.Add(-replayOverlap))

	for {
		select {
		case ev := <-evs:
			if ev.ContainerID != id {
				continue
			}

			if ev.Action != from.Action || !ev.Time.Equal(from.Time) {
				t.Fatalf("replay starts with %s at %v, want %s at %v", ev.Action, ev.Time, from.Action, from.Time)
			}

			return
		case err := <-errs:
			t.Fatalf("replay: %v", err)
		}
	}
}

func createContainer(t *testing.T, c *Client, name string, cfg *container.Config, hc *container.HostConfig) string {
	t.Helper()

	res, err := c.api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: name, Config: cfg, HostConfig: hc})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		_, _ = c.api.ContainerRemove(ctx, res.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	})

	return res.ID
}

func findContainer(ctx context.Context, t *testing.T, c *Client, id string) (Container, bool) {
	t.Helper()

	list, err := c.Containers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for _, ctr := range list {
		if ctr.ID == id {
			return ctr, true
		}
	}

	return Container{}, false
}

func randomName(t *testing.T) string {
	t.Helper()

	b := make([]byte, 4)
	_, _ = rand.Read(b)

	return fmt.Sprintf("kdw-it-%x", b)
}
