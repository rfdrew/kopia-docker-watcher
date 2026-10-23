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
	starts     int

	// exec runs the commands of Exec; nil succeeds at once with no
	// output. execs records them as "container: arg arg".
	exec  func(ctx context.Context, c docker.Container, cmd []string) (docker.ExecResult, error)
	execs []string

	// lifecycle, if set, runs before Stop, Start, Pause and Unpause change
	// a container, and an error it returns fails the call. actions records
	// them as "stop db".
	lifecycle func(ctx context.Context, action string, c docker.Container) error
	actions   []string
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

// Exec implements docker.Engine: a container that does not exist is
// docker.ErrNotFound, one that does not run docker.ErrNotRunning; any
// other runs f.exec, and the command counts as started.
func (f *fakeEngine) Exec(ctx context.Context, id string, cmd []string) (docker.ExecResult, error) {
	f.mu.Lock()

	c, ok := f.containers[id]
	if ok {
		f.execs = append(f.execs, c.Name+": "+strings.Join(cmd, " "))
	}

	run := f.exec
	f.mu.Unlock()

	switch {
	case !ok:
		return docker.ExecResult{}, fmt.Errorf("exec in container %s: %w", id, docker.ErrNotFound)
	case c.State != docker.StateRunning:
		return docker.ExecResult{}, fmt.Errorf("exec in container %s: %w", id, docker.ErrNotRunning)
	case run == nil:
		return docker.ExecResult{Started: true}, ctx.Err()
	}

	res, err := run(ctx, cloneContainer(c), cmd)
	res.Started = true

	return res, err
}

// Stop implements docker.Engine: a running, paused or restarting
// container exits; any other is left as it is.
func (f *fakeEngine) Stop(ctx context.Context, id string) error {
	return f.change(ctx, "stop", id, func(c *docker.Container) error {
		switch c.State {
		case docker.StateRunning, docker.StatePaused, docker.StateRestarting:
			c.State = docker.StateExited
		}

		return nil
	})
}

// Start implements docker.Engine: a container that is not running starts,
// with a new start time.
func (f *fakeEngine) Start(ctx context.Context, id string) error {
	return f.change(ctx, "start", id, func(c *docker.Container) error {
		if c.State != docker.StateRunning {
			c.State, c.StartedAt = docker.StateRunning, f.nextStart()
		}

		return nil
	})
}

// Pause implements docker.Engine: a paused container stays paused, one
// that is neither running nor paused is docker.ErrNotRunning.
func (f *fakeEngine) Pause(ctx context.Context, id string) error {
	return f.change(ctx, "pause", id, func(c *docker.Container) error {
		switch c.State {
		case docker.StateRunning, docker.StatePaused:
			c.State = docker.StatePaused
			return nil
		default:
			return fmt.Errorf("pause container %s: %w", id, docker.ErrNotRunning)
		}
	})
}

// Unpause implements docker.Engine: a running container stays running,
// one that is neither paused nor running is docker.ErrNotPaused.
func (f *fakeEngine) Unpause(ctx context.Context, id string) error {
	return f.change(ctx, "unpause", id, func(c *docker.Container) error {
		switch c.State {
		case docker.StatePaused, docker.StateRunning:
			c.State = docker.StateRunning
			return nil
		default:
			return fmt.Errorf("unpause container %s: %w", id, docker.ErrNotPaused)
		}
	})
}

// change records the action on the container, runs f.lifecycle and then
// fn on it.
func (f *fakeEngine) change(ctx context.Context, action, id string, fn func(c *docker.Container) error) error {
	f.mu.Lock()
	c, ok := f.containers[id]

	if ok {
		f.actions = append(f.actions, action+" "+c.Name)
	}

	hook := f.lifecycle
	f.mu.Unlock()

	if !ok {
		return fmt.Errorf("%s container %s: %w", action, id, docker.ErrNotFound)
	}

	if hook != nil {
		if err := hook(ctx, action, cloneContainer(c)); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	c, ok = f.containers[id]
	if !ok {
		return fmt.Errorf("%s container %s: %w", action, id, docker.ErrNotFound)
	}

	if err := fn(&c); err != nil {
		return err
	}

	f.containers[id] = c

	return nil
}

// nextStart returns a start time later than any before; f.mu is held.
func (f *fakeEngine) nextStart() time.Time {
	f.starts++
	return time.Date(2026, 10, 1, 0, 0, f.starts, 0, time.UTC)
}

// actionLog returns the stops, starts, pauses and unpauses so far, as
// "stop db".
func (f *fakeEngine) actionLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.actions)
}

// setLifecycle sets what runs before each stop, start, pause and unpause.
func (f *fakeEngine) setLifecycle(fn func(ctx context.Context, action string, c docker.Container) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lifecycle = fn
}

// container returns a container by ID.
func (f *fakeEngine) container(id string) docker.Container {
	f.mu.Lock()
	defer f.mu.Unlock()

	return cloneContainer(f.containers[id])
}

// containerID returns the ID of the container with the name.
func (f *fakeEngine) containerID(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	for id, c := range f.containers {
		if c.Name == name {
			return id
		}
	}

	panic("no container " + name)
}

// execLog returns the commands Exec ran, as "container: arg arg".
func (f *fakeEngine) execLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.execs)
}

// setExec sets what Exec runs.
func (f *fakeEngine) setExec(fn func(ctx context.Context, c docker.Container, cmd []string) (docker.ExecResult, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.exec = fn
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

// add adds a container as it is, with its own ID.
func (f *fakeEngine) add(c docker.Container) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.containers[c.ID] = cloneContainer(c)
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

	c := docker.Container{ID: id, Name: name, Labels: labelMap(labels...), State: st}
	if st.Running() {
		c.StartedAt = f.nextStart()
	}

	f.containers[id] = c

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
