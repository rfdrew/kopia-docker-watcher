package docker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// serveLifecycle serves POST /containers/{id}/stop, start, pause and
// unpause as Docker does: 204 once done, 304 for a stop or start that
// changes nothing, 409 for a pause or unpause that cannot be done, 404 for
// a missing container. It changes the state the fake inspects.
func (f *fakeDocker) serveLifecycle(w http.ResponseWriter, r *http.Request, path string) bool {
	rest, ok := strings.CutPrefix(path, "/containers/")
	if !ok || r.Method != http.MethodPost {
		return false
	}

	id, action, ok := strings.Cut(rest, "/")

	switch action {
	case "stop", "start", "pause", "unpause":
	default:
		return false
	}

	if !ok {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	ctr, found := f.inspect[id]
	if !found {
		writeDockerError(w, http.StatusNotFound, "No such container: "+id)
		return true
	}

	st := ctr.State
	if st == nil {
		st = &container.State{}
	}

	status, to := http.StatusNoContent, st.Status

	switch action {
	case "stop":
		if st.Status != container.StateRunning && st.Status != container.StatePaused && st.Status != container.StateRestarting {
			status = http.StatusNotModified
		}

		to = container.StateExited
	case "start":
		if st.Status == container.StateRunning {
			status = http.StatusNotModified
		}

		to = container.StateRunning
	case "pause":
		if st.Status != container.StateRunning {
			writeDockerError(w, http.StatusConflict, "Container "+id+" is not running")
			return true
		}

		to = container.StatePaused
	case "unpause":
		if st.Status != container.StatePaused {
			writeDockerError(w, http.StatusConflict, "Container "+id+" is not paused")
			return true
		}

		to = container.StateRunning
	}

	next := *st
	next.Status = to

	if action == "start" && status == http.StatusNoContent {
		next.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	ctr.State = &next
	f.inspect[id] = ctr

	w.WriteHeader(status)

	return true
}

func TestLifecycle(t *testing.T) {
	f := newFakeDocker()
	started := time.Date(2026, 10, 20, 8, 0, 0, 123, time.UTC)

	f.set(func(f *fakeDocker) {
		f.inspect["db"] = container.InspectResponse{ID: "db", Name: "/db",
			State: &container.State{Status: container.StateRunning, StartedAt: started.Format(time.RFC3339Nano)}}
	})

	c := newFakeDockerClient(t, f)
	ctx := t.Context()

	state := func(want State) Container {
		t.Helper()

		got, err := c.Inspect(ctx, "db")
		if err != nil || got.State != want {
			t.Fatalf("state = %s, %v; want %s", got.State, err, want)
		}

		return got
	}

	if got := state(StateRunning); !got.StartedAt.Equal(started) {
		t.Fatalf("started at %v, want %v", got.StartedAt, started)
	}

	steps := []struct {
		name string
		do   func(context.Context, string) error
		want State
	}{
		{"pause", c.Pause, StatePaused},
		{"unpause", c.Unpause, StateRunning},
		{"stop", c.Stop, StateExited},
		{"stop again", c.Stop, StateExited},
		{"start", c.Start, StateRunning},
		{"start again", c.Start, StateRunning},
	}

	for _, s := range steps {
		if err := s.do(ctx, "db"); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}

		state(s.want)
	}

	if q := f.lastRequest(t, "/containers/db/stop").Query; len(q) != 0 {
		t.Errorf("stop query = %v, want none: the container's own timeout", q)
	}

	if err := c.Unpause(ctx, "db"); !errors.Is(err, ErrNotPaused) {
		t.Errorf("unpause of a running container: %v, want ErrNotPaused", err)
	}

	if err := c.Stop(ctx, "db"); err != nil {
		t.Fatal(err)
	}

	if err := c.Pause(ctx, "db"); !errors.Is(err, ErrNotRunning) {
		t.Errorf("pause of a stopped container: %v, want ErrNotRunning", err)
	}

	for name, do := range map[string]func(context.Context, string) error{"stop": c.Stop, "start": c.Start,
		"pause": c.Pause, "unpause": c.Unpause} {
		if err := do(ctx, "gone"); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s of a missing container: %v, want ErrNotFound", name, err)
		}
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if err := c.Start(canceled, "db"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled start: %v", err)
	}
}

// A container that never ran has no start time.
func TestInspectNeverStarted(t *testing.T) {
	f := newFakeDocker()
	f.set(func(f *fakeDocker) {
		f.inspect["new"] = container.InspectResponse{ID: "new", Name: "/new",
			State: &container.State{Status: container.StateCreated, StartedAt: "0001-01-01T00:00:00Z"}}
	})

	got, err := newFakeDockerClient(t, f).Inspect(t.Context(), "new")
	if err != nil || !got.StartedAt.IsZero() || got.State != StateCreated {
		t.Fatalf("container = %+v, %v", got, err)
	}
}
