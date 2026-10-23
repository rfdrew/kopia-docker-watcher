package docker

import (
	"context"
	"errors"
	"fmt"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// ErrNotPaused is returned by Unpause for a container that is not paused.
var ErrNotPaused = errors.New("container is not paused")

// Stop stops the container as docker stop does, with the container's own
// stop signal and timeout. Docker answers once the container has stopped,
// so the call takes up to the stop timeout, and a little more if the
// container has to be killed.
func (c *Client) Stop(ctx context.Context, id string) error {
	_, err := c.api.ContainerStop(ctx, id, client.ContainerStopOptions{})

	return lifecycleError(ctx, id, "stop", err, nil)
}

// Start starts the container.
func (c *Client) Start(ctx context.Context, id string) error {
	_, err := c.api.ContainerStart(ctx, id, client.ContainerStartOptions{})

	return lifecycleError(ctx, id, "start", err, nil)
}

// Pause freezes the processes of the container.
func (c *Client) Pause(ctx context.Context, id string) error {
	_, err := c.api.ContainerPause(ctx, id, client.ContainerPauseOptions{})

	// Docker refuses to pause a container that is not running, or paused
	// already, with 409.
	return lifecycleError(ctx, id, "pause", err, ErrNotRunning)
}

// Unpause thaws the processes of the container.
func (c *Client) Unpause(ctx context.Context, id string) error {
	_, err := c.api.ContainerUnpause(ctx, id, client.ContainerUnpauseOptions{})

	// Docker refuses to unpause a container that is not paused with 409.
	return lifecycleError(ctx, id, "unpause", err, ErrNotPaused)
}

// lifecycleError explains an error of a request that changes the state of
// a container; nil stays nil. conflict, if set, is what Docker's 409 means
// for the request.
func lifecycleError(ctx context.Context, id, what string, err, conflict error) error {
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("%s container %s: %w", what, id, ctx.Err())
	case cerrdefs.IsNotFound(err):
		return fmt.Errorf("%s container %s: %w: %w", what, id, ErrNotFound, err)
	case conflict != nil && cerrdefs.IsConflict(err):
		return fmt.Errorf("%s container %s: %w: %w", what, id, conflict, err)
	default:
		return fmt.Errorf("%s container %s: %w", what, id, err)
	}
}
