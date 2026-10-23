// Package docker is the watcher's view of the Docker Engine: containers with
// their labels, state and mounts, and a stream of container events.
//
// Reconciliation depends on the small Engine interface, so its tests run
// against a fake Docker. Client implements Engine with the Docker SDK, and
// Watcher turns Engine's single subscriptions into an event stream that
// survives a broken connection.
//
// An event never decides anything by itself; it only tells the reconciler
// which container to look at, and the decision rests on what Inspect says
// then. A stop arrives as die, or as stop alone for a container stopped
// while it waits for its restart policy. kill only signals the process:
// docker kill -s HUP leaves the container running, and a signal that ends
// it is followed by die. A restarting container can also end up exited
// without any event at all (killed during its restart delay, or a restart
// that fails), so a container last seen restarting has to be looked at
// again within MaxRestartDelay.
package docker

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Inspect for a container that does not exist.
var ErrNotFound = errors.New("container not found")

// Engine is the part of the Docker Engine API the watcher uses.
type Engine interface {
	// Containers lists all containers, stopped ones included.
	Containers(ctx context.Context) ([]Container, error)

	// Inspect returns one container by ID or name. A missing container is
	// ErrNotFound.
	Inspect(ctx context.Context, id string) (Container, error)

	// Events opens one subscription to the container events that matter:
	// create, start, die, stop, destroy and rename. With a non-zero since
	// the daemon first replays what it still holds of its latest 256 events
	// (of all types, health-check execs included), in the order it added
	// them, back to the first one stamped before since. That order can
	// differ slightly from the time stamps. Events arrive on the first
	// channel until the subscription
	// ends; then the error channel receives exactly one error: ctx's error
	// once ctx is done, io.EOF if the daemon closed the stream. The event
	// channel is never closed.
	Events(ctx context.Context, since time.Time) (<-chan Event, <-chan error)
}

// Container is a container as the reconciler needs it.
type Container struct {
	ID string
	// Name is the container name without the leading slash.
	Name   string
	Labels map[string]string
	State  State
	Mounts []Mount
}

// Mount is a volume or bind mount of a container.
type Mount struct {
	// Type is "bind", "volume", "tmpfs", ...
	Type string
	// Name is the volume name; empty for bind mounts.
	Name string
	// Driver is the volume driver, "local" for Docker's own volumes; empty
	// for bind mounts.
	Driver string
	// Source is the path on the Docker host: the bind source, or the
	// volume's data directory. Empty for tmpfs.
	Source string
	// Destination is the path inside the container.
	Destination string
	RW          bool
}

// State is the state Docker reports for a container.
type State string

// Container states.
const (
	StateCreated    State = "created"
	StateRunning    State = "running"
	StatePaused     State = "paused"
	StateRestarting State = "restarting"
	StateRemoving   State = "removing"
	StateExited     State = "exited"
	StateDead       State = "dead"
)

// MaxRestartDelay is the longest Docker waits before it restarts a
// container under a restart policy; the delay starts at 100ms and doubles.
// A container leaves the restarting state within it, but not always with
// an event.
const MaxRestartDelay = time.Minute

// Running reports whether a container in this state counts as a running
// owner of its backup sources. Running, paused and restarting do: a crash
// loop must not pause backups or take a snapshot on every exit. Created,
// exited, dead and removing are stopped.
//
// Restarting is a passing state. When it ends in exited because the
// container was killed during the restart delay or failed to start again,
// Docker sends no event, so a container seen restarting has to be
// inspected again within MaxRestartDelay.
//
// A state this package does not know, say one added by a newer Docker,
// counts as running: a wrong "running" only keeps the schedule of a
// stopped container, a wrong "stopped" would pause the backups of a
// working one.
func (s State) Running() bool {
	switch s {
	case StateCreated, StateExited, StateDead, StateRemoving:
		return false
	default:
		return true
	}
}

// StoppedByDie reports whether a KindDied event means the container
// stopped, judged by what Inspect says when the event is handled: found is
// false if the container is gone, state is its state otherwise. A
// container that is restarting after the die (restart policy, crash loop)
// or already running again has not stopped. One that no longer exists has,
// as after docker compose down or docker run --rm.
func StoppedByDie(found bool, state State) bool {
	return !found || !state.Running()
}

// Kind says what an event means for reconciliation.
type Kind int

// Event kinds.
const (
	// KindResync is not a Docker event: Watcher sends it when events may
	// have been lost, so everything has to be reconciled. It always
	// precedes the events of the first subscription.
	KindResync Kind = iota + 1
	// KindChanged is a created or renamed container: neither a start nor a
	// stop, but the container has to be reconciled.
	KindChanged
	// KindStarted is a started container.
	KindStarted
	// KindDied is a die or a stop event: the container's main process
	// exited, or docker stop finished. Whether it stopped is up to
	// StoppedByDie. A stop of a running container sends both die and stop,
	// but a container stopped while it waits for its restart policy sends
	// only stop, so reconciliation acts on the state, not on the count.
	KindDied
	// KindRemoved is a removed container. Its labels are only known from
	// the event.
	KindRemoved
)

func (k Kind) String() string {
	switch k {
	case KindResync:
		return "resync"
	case KindChanged:
		return "changed"
	case KindStarted:
		return "started"
	case KindDied:
		return "died"
	case KindRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// Event is a container event, or a KindResync from Watcher.
type Event struct {
	Kind Kind
	// Action is Docker's name of the event, e.g. "die"; empty for
	// KindResync.
	Action      string
	ContainerID string
	// Name is the container name; for a rename, the new one.
	Name string
	// Labels are the container's labels as Docker copies them into the
	// event. For KindRemoved they are the only record of them left. Keys
	// Docker adds to the event itself (image, name, exitCode, ...) are left
	// out, so a label with one of these names is missing.
	Labels map[string]string
	// Time is when the daemon recorded the event.
	Time time.Time
}
