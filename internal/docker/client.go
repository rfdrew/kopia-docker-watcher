package docker

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/events"
	"github.com/moby/moby/client"
)

// Client implements Engine with the Docker SDK.
type Client struct {
	api *client.Client
}

var _ Engine = (*Client)(nil)

// New returns a client set up like the docker CLI, from DOCKER_HOST,
// DOCKER_API_VERSION, DOCKER_CERT_PATH and DOCKER_TLS_VERIFY. DOCKER_HOST
// points it at a docker-socket-proxy, e.g. tcp://docker-socket-proxy:2375;
// unset or empty, it means the local socket. Unless DOCKER_API_VERSION fixes
// the API version, the client negotiates it down to the daemon's on the
// first request, so any daemon from API 1.40 (Docker 19.03) on works; an
// older one rejects every request.
func New(userAgent string) (*Client, error) {
	return newClient(client.FromEnv, client.WithUserAgent(userAgent))
}

func newClient(opts ...client.Opt) (*Client, error) {
	api, err := client.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}

	return &Client{api: api}, nil
}

// Close releases idle connections.
func (c *Client) Close() error {
	return c.api.Close()
}

// Containers lists all containers, stopped ones included.
func (c *Client) Containers(ctx context.Context) ([]Container, error) {
	res, err := c.api.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}

	out := make([]Container, 0, len(res.Items))
	for _, s := range res.Items {
		out = append(out, Container{
			ID:     s.ID,
			Name:   primaryName(s.Names),
			Labels: s.Labels,
			State:  State(s.State),
			Mounts: mountsFromDocker(s.Mounts),
		})
	}

	return out, nil
}

// Inspect returns one container by ID or name.
func (c *Client) Inspect(ctx context.Context, id string) (Container, error) {
	res, err := c.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if cerrdefs.IsNotFound(err) {
		return Container{}, fmt.Errorf("inspect container %s: %w", id, ErrNotFound)
	}

	if err != nil {
		return Container{}, fmt.Errorf("inspect container %s: %w", id, err)
	}

	r := res.Container
	ctr := Container{
		ID:     r.ID,
		Name:   strings.TrimPrefix(r.Name, "/"),
		Mounts: mountsFromDocker(r.Mounts),
	}

	if r.State != nil {
		ctr.State = State(r.State.Status)
	}

	if r.Config != nil {
		ctr.Labels = r.Config.Labels
	}

	return ctr, nil
}

// Events opens one subscription to container events.
func (c *Client) Events(ctx context.Context, since time.Time) (<-chan Event, <-chan error) {
	opts := client.EventsListOptions{Filters: eventFilters()}
	if !since.IsZero() {
		// The daemon's own format, exact to the nanosecond of TimeNano.
		opts.Since = fmt.Sprintf("%d.%09d", since.Unix(), since.Nanosecond())
	}

	res := c.api.Events(ctx, opts)
	out := make(chan Event)
	errc := make(chan error, 1)

	go func() {
		for {
			select {
			case m := <-res.Messages:
				ev, ok := eventFromDocker(m)
				if !ok {
					continue
				}

				select {
				case out <- ev:
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				}
			case err := <-res.Err:
				switch {
				case ctx.Err() != nil:
					// A canceled read can surface as a network error.
					err = ctx.Err()
				case err == nil:
					err = io.EOF
				}

				errc <- err

				return
			}
		}
	}()

	return out, errc
}

// eventKinds maps the container actions the watcher subscribes to. stop
// is among them because a container stopped while it waits for its restart
// policy gets no die. The rest either come with one of these (kill and oom
// before die, restart with start) or change nothing that matters for
// backups (pause keeps the container a running owner); exec and
// health_status events, which every health check produces, are kept out by
// the daemon-side filter.
var eventKinds = map[events.Action]Kind{
	events.ActionCreate:  KindChanged,
	events.ActionRename:  KindChanged,
	events.ActionStart:   KindStarted,
	events.ActionDie:     KindDied,
	events.ActionStop:    KindDied,
	events.ActionDestroy: KindRemoved,
}

// eventAttributes are the keys Docker adds next to the labels in the
// attributes of a container event.
var eventAttributes = []string{"image", "name", "exitCode", "execDuration", "oldName", "signal"}

func eventFilters() client.Filters {
	f := make(client.Filters).Add("type", string(events.ContainerEventType))
	for a := range eventKinds {
		f.Add("event", string(a))
	}

	return f
}

// eventFromDocker converts a container event; ok is false for any event the
// watcher does not act on, kill among them.
func eventFromDocker(m events.Message) (ev Event, ok bool) {
	if m.Type != events.ContainerEventType || m.Actor.ID == "" {
		return Event{}, false
	}

	kind, ok := eventKinds[m.Action]
	if !ok {
		return Event{}, false
	}

	labels := maps.Clone(m.Actor.Attributes)
	for _, k := range eventAttributes {
		delete(labels, k)
	}

	t := time.Unix(0, m.TimeNano)
	if m.TimeNano == 0 {
		t = time.Unix(m.Time, 0)
	}

	return Event{
		Kind:        kind,
		Action:      string(m.Action),
		ContainerID: m.Actor.ID,
		Name:        m.Actor.Attributes["name"],
		Labels:      labels,
		Time:        t,
	}, true
}

// primaryName picks the container's own name from the list, which also
// holds the aliases of legacy links ("/other/alias").
func primaryName(names []string) string {
	for _, n := range names {
		if n = strings.TrimPrefix(n, "/"); !strings.Contains(n, "/") {
			return n
		}
	}

	if len(names) > 0 {
		return strings.TrimPrefix(names[0], "/")
	}

	return ""
}

func mountsFromDocker(in []container.MountPoint) []Mount {
	if len(in) == 0 {
		return nil
	}

	out := make([]Mount, 0, len(in))
	for _, m := range in {
		out = append(out, Mount{
			Type:        string(m.Type),
			Name:        m.Name,
			Source:      m.Source,
			Destination: m.Destination,
			RW:          m.RW,
		})
	}

	return out
}
