package reconcile

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
)

// fakeEngine is an in-memory Docker Engine: containers by ID, with labels
// and state. Events are not streamed; tests pass events to HandleEvent
// themselves, built with event.
type fakeEngine struct {
	mu         sync.Mutex
	containers map[string]docker.Container
	fail       []error
	nextID     int
}

var _ docker.Engine = (*fakeEngine)(nil)

func newFakeEngine() *fakeEngine {
	return &fakeEngine{containers: map[string]docker.Container{}}
}

func cloneContainer(c docker.Container) docker.Container {
	c.Labels = maps.Clone(c.Labels)
	c.Mounts = slices.Clone(c.Mounts)

	return c
}

// Containers implements docker.Engine: every container, sorted by ID.
func (f *fakeEngine) Containers(ctx context.Context) ([]docker.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.fail) > 0 {
		err := f.fail[0]
		f.fail = f.fail[1:]

		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	out := make([]docker.Container, 0, len(f.containers))
	for _, id := range slices.Sorted(maps.Keys(f.containers)) {
		out = append(out, cloneContainer(f.containers[id]))
	}

	return out, nil
}

// Inspect implements docker.Engine by ID or name.
func (f *fakeEngine) Inspect(ctx context.Context, id string) (docker.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return docker.Container{}, err
	}

	for _, c := range f.containers {
		if c.ID == id || c.Name == id {
			return cloneContainer(c), nil
		}
	}

	return docker.Container{}, fmt.Errorf("inspect container %s: %w", id, docker.ErrNotFound)
}

// Events implements docker.Engine with a subscription that only ends with
// ctx.
func (f *fakeEngine) Events(ctx context.Context, _ time.Time) (<-chan docker.Event, <-chan error) {
	errc := make(chan error, 1)

	go func() {
		<-ctx.Done()
		errc <- ctx.Err()
	}()

	return make(chan docker.Event), errc
}

// run creates a running container with labels given as "key=value" and
// returns its ID.
func (f *fakeEngine) run(name string, labels ...string) string {
	return f.create(name, docker.StateRunning, labels...)
}

// create creates a container in state st and returns its ID.
func (f *fakeEngine) create(name string, st docker.State, labels ...string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextID++
	id := fmt.Sprintf("%s-%d", name, f.nextID)

	f.containers[id] = docker.Container{ID: id, Name: name, Labels: labelMap(labels...), State: st}

	return id
}

func labelMap(labels ...string) map[string]string {
	m := make(map[string]string, len(labels))

	for _, l := range labels {
		k, v, ok := strings.Cut(l, "=")
		if !ok {
			panic("label without '=': " + l)
		}

		m[k] = v
	}

	return m
}

func (f *fakeEngine) setState(id string, st docker.State) {
	f.mu.Lock()
	defer f.mu.Unlock()

	c, ok := f.containers[id]
	if !ok {
		panic("no container " + id)
	}

	c.State = st
	f.containers[id] = c
}

// remove deletes a container and returns the destroy event Docker sends,
// with the labels it carries.
func (f *fakeEngine) remove(id string) docker.Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	c, ok := f.containers[id]
	if !ok {
		panic("no container " + id)
	}

	delete(f.containers, id)

	return docker.Event{Kind: docker.KindRemoved, Action: "destroy", ContainerID: id, Name: c.Name,
		Labels: maps.Clone(c.Labels), Time: time.Now()}
}

// event returns the event Docker sends about a container that exists.
func (f *fakeEngine) event(kind docker.Kind, id string) docker.Event {
	f.mu.Lock()
	defer f.mu.Unlock()

	c, ok := f.containers[id]
	if !ok {
		panic("no container " + id)
	}

	action := map[docker.Kind]string{docker.KindChanged: "create", docker.KindStarted: "start", docker.KindDied: "die"}[kind]

	return docker.Event{Kind: kind, Action: action, ContainerID: id, Name: c.Name, Labels: maps.Clone(c.Labels), Time: time.Now()}
}

// failNext makes the next Containers call fail with err.
func (f *fakeEngine) failNext(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.fail = append(f.fail, err)
}
