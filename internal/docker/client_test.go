package docker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/events"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
)

func TestNewHonorsDockerHost(t *testing.T) {
	// A docker-socket-proxy is reached through DOCKER_HOST.
	t.Setenv("DOCKER_HOST", "tcp://docker-socket-proxy:2375")

	c, err := New("test-agent")
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if got := c.api.DaemonHost(); got != "tcp://docker-socket-proxy:2375" {
		t.Errorf("daemon host = %q", got)
	}
}

func TestNewWithoutDockerHostUsesLocalSocket(t *testing.T) {
	// Empty is the same as unset.
	t.Setenv("DOCKER_HOST", "")

	c, err := New("test-agent")
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	if got := c.api.DaemonHost(); got != client.DefaultDockerHost {
		t.Errorf("daemon host = %q, want %q", got, client.DefaultDockerHost)
	}
}

func TestNewRejectsBadDockerHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "docker-socket-proxy:2375")

	if _, err := New("test-agent"); err == nil {
		t.Fatal("no error for a DOCKER_HOST without a scheme")
	}
}

func TestContainersIncludesStoppedOnes(t *testing.T) {
	f := newFakeDocker()
	f.set(func(f *fakeDocker) {
		f.containers = []container.Summary{
			{
				ID:     "c1",
				Names:  []string{"/app-db-1"},
				Labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data"},
				State:  container.StateRunning,
				Mounts: []container.MountPoint{{
					Type:        mount.TypeVolume,
					Name:        "app_pgdata",
					Source:      "/var/lib/docker/volumes/app_pgdata/_data",
					Destination: "/var/lib/postgresql/data",
					Driver:      "local",
					RW:          true,
				}},
			},
			{
				ID:     "c2",
				Names:  []string{"/app-web-1/db", "/app-cron-1"},
				Labels: map[string]string{"kopia.enable": "true"},
				State:  container.StateExited,
				Mounts: []container.MountPoint{{
					Type:        mount.TypeBind,
					Source:      "/srv/cron",
					Destination: "/etc/cron.d",
				}},
			},
		}
	})

	c := newFakeDockerClient(t, f)

	got, err := c.Containers(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	want := []Container{
		{
			ID:     "c1",
			Name:   "app-db-1",
			Labels: map[string]string{"kopia.enable": "true", "kopia.path": "/data"},
			State:  StateRunning,
			Mounts: []Mount{{
				Type:        "volume",
				Name:        "app_pgdata",
				Source:      "/var/lib/docker/volumes/app_pgdata/_data",
				Destination: "/var/lib/postgresql/data",
				RW:          true,
			}},
		},
		{
			ID:     "c2",
			Name:   "app-cron-1",
			Labels: map[string]string{"kopia.enable": "true"},
			State:  StateExited,
			Mounts: []Mount{{Type: "bind", Source: "/srv/cron", Destination: "/etc/cron.d"}},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("containers =\n%+v\nwant\n%+v", got, want)
	}

	req := f.lastRequest(t, "/containers/json")
	if req.Query.Get("all") != "1" {
		t.Errorf("query = %v, want all=1", req.Query)
	}

	if req.UserAgent != "test-agent" {
		t.Errorf("User-Agent = %q", req.UserAgent)
	}
}

func TestInspect(t *testing.T) {
	f := newFakeDocker()
	f.set(func(f *fakeDocker) {
		f.inspect["c1"] = container.InspectResponse{
			ID:     "c1",
			Name:   "/app-db-1",
			State:  &container.State{Status: container.StateRestarting, Restarting: true},
			Config: &container.Config{Labels: map[string]string{"kopia.enable": "true"}},
			Mounts: []container.MountPoint{{Type: mount.TypeTmpfs, Destination: "/run"}},
		}
		f.inspect["bare"] = container.InspectResponse{ID: "bare", Name: "/bare"}
	})

	c := newFakeDockerClient(t, f)

	got, err := c.Inspect(t.Context(), "c1")
	if err != nil {
		t.Fatal(err)
	}

	want := Container{
		ID:     "c1",
		Name:   "app-db-1",
		Labels: map[string]string{"kopia.enable": "true"},
		State:  StateRestarting,
		Mounts: []Mount{{Type: "tmpfs", Destination: "/run"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("container = %+v, want %+v", got, want)
	}

	if got, err := c.Inspect(t.Context(), "bare"); err != nil || !reflect.DeepEqual(got, Container{ID: "bare", Name: "bare"}) {
		t.Errorf("bare container = %+v, %v", got, err)
	}

	if _, err := c.Inspect(t.Context(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing container: error = %v, want ErrNotFound", err)
	}
}

func TestEventsQuery(t *testing.T) {
	f := newFakeDocker()
	c := newFakeDockerClient(t, f)

	since := time.Unix(1_760_000_000, 5)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, _ = c.Events(ctx, since)
	sub := f.nextSub(t)

	// The daemon's seconds.nanoseconds format, so no event at the same
	// second is skipped.
	if got := sub.query.Get("since"); got != "1760000000.000000005" {
		t.Errorf("since = %q", got)
	}

	var filters map[string]map[string]bool
	if err := json.Unmarshal([]byte(sub.query.Get("filters")), &filters); err != nil {
		t.Fatalf("filters %q: %v", sub.query.Get("filters"), err)
	}

	want := map[string]map[string]bool{
		"type":  {"container": true},
		"event": {"create": true, "start": true, "die": true, "stop": true, "destroy": true, "rename": true},
	}
	if !reflect.DeepEqual(filters, want) {
		t.Errorf("filters = %v, want %v", filters, want)
	}

	cancel()

	// Without since the stream starts now.
	_, _ = c.Events(t.Context(), time.Time{})
	if sub := f.nextSub(t); sub.query.Has("since") {
		t.Errorf("query = %v, want no since", sub.query)
	}
}

func TestEventsStream(t *testing.T) {
	f := newFakeDocker()
	c := newFakeDockerClient(t, f)

	evs, errs := c.Events(t.Context(), time.Time{})
	sub := f.nextSub(t)

	attrs := map[string]string{"kopia.enable": "true", "image": "postgres:17", "name": "db"}
	msgs := []events.Message{
		{Type: events.ContainerEventType, Action: events.ActionStart, Actor: events.Actor{ID: "c1", Attributes: attrs}, TimeNano: 1},
		// The daemon filters these out; the client drops them too.
		{Type: events.ContainerEventType, Action: events.ActionKill, Actor: events.Actor{ID: "c1", Attributes: attrs}, TimeNano: 2},
		{Type: events.NetworkEventType, Action: events.ActionConnect, Actor: events.Actor{ID: "n1"}, TimeNano: 3},
		{Type: events.ContainerEventType, Action: events.ActionDestroy, Actor: events.Actor{ID: "c1", Attributes: attrs}, TimeNano: 4},
	}

	go func() {
		for _, m := range msgs {
			sub.send <- m
		}

		close(sub.send)
	}()

	for _, want := range []struct {
		kind Kind
		nano int64
	}{{KindStarted, 1}, {KindRemoved, 4}} {
		select {
		case ev := <-evs:
			if ev.Kind != want.kind || ev.ContainerID != "c1" || ev.Name != "db" || ev.Time.UnixNano() != want.nano {
				t.Fatalf("event = %+v, want %v at %d", ev, want.kind, want.nano)
			}

			if !maps.Equal(ev.Labels, map[string]string{"kopia.enable": "true"}) {
				t.Errorf("labels = %v", ev.Labels)
			}
		case err := <-errs:
			t.Fatalf("stream ended: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
		}
	}

	select {
	case err := <-errs:
		if !errors.Is(err, io.EOF) {
			t.Errorf("error = %v, want EOF", err)
		}
	case ev := <-evs:
		t.Fatalf("unexpected event %+v", ev)
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not end")
	}
}

func TestEventsCanceled(t *testing.T) {
	f := newFakeDocker()
	c := newFakeDockerClient(t, f)

	ctx, cancel := context.WithCancel(t.Context())
	_, errs := c.Events(ctx, time.Time{})
	f.nextSub(t)
	cancel()

	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not end")
	}
}

func TestEventsFailure(t *testing.T) {
	f := newFakeDocker()
	f.set(func(f *fakeDocker) { f.failEvents = 1 })
	c := newFakeDockerClient(t, f)

	_, errs := c.Events(t.Context(), time.Time{})

	select {
	case err := <-errs:
		if err == nil || errors.Is(err, io.EOF) {
			t.Errorf("error = %v, want the daemon's", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error")
	}
}

func TestPing(t *testing.T) {
	f := newFakeDocker()
	c := newFakeDockerClient(t, f)

	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	f.set(func(f *fakeDocker) { f.failPing = true })

	if err := c.Ping(t.Context()); err == nil || !strings.Contains(err.Error(), "daemon is busy") {
		t.Fatalf("Ping of a failing daemon: %v", err)
	}
}
