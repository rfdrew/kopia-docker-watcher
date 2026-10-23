package docker

import (
	"context"
	"errors"
	"fmt"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// ErrNotPaused is returned by Unpause for a container that is neither
// paused nor running.
var ErrNotPaused = errors.New("container is not paused")

// Stop stops the container as docker stop does, with the container's own
// stop signal and timeout. Docker answers once the container has stopped,
// so the call takes up to the stop timeout, and a little more if the
// container has to be killed. A container that is not running is left as
// it is: Docker answers 304.
func (c *Client) Stop(ctx context.Context, id string) error {
	_, err := c.api.ContainerStop(ctx, id, client.ContainerStopOptions{})

	return lifecycleError(ctx, id, "stop", err, nil)
}

// Start starts the container. A running container is left as it is:
// Docker answers 304.
func (c *Client) Start(ctx context.Context, id string) error {
	_, err := c.api.ContainerStart(ctx, id, client.ContainerStartOptions{})

	return lifecycleError(ctx, id, "start", err, nil)
}

// Pause freezes the processes of the container. A container that is paused
// already is no error. Docker refuses it, as it refuses one that is not
// running, with an answer that differs between versions, so the state of
// the container decides: paused is done, not running is ErrNotRunning.
func (c *Client) Pause(ctx context.Context, id string) error {
	_, err := c.api.ContainerPause(ctx, id, client.ContainerPauseOptions{})

	return c.settle(ctx, id, "pause", err, func(s State) (done bool, kind error) {
		switch s {
		case StatePaused:
			return true, nil
		case StateRunning:
			return false, nil
		default:
			return false, ErrNotRunning
		}
	})
}

// Unpause thaws the processes of the container. A container that runs, not
// paused, is no error. Docker refuses it, as it refuses one that is
// stopped, with an answer that differs between versions (409, or 500 with
// "is not paused"), so the state of the container decides: running is
// done, neither running nor paused is ErrNotPaused.
func (c *Client) Unpause(ctx context.Context, id string) error {
	_, err := c.api.ContainerUnpause(ctx, id, client.ContainerUnpauseOptions{})

	return c.settle(ctx, id, "unpause", err, func(s State) (done bool, kind error) {
		switch s {
		case StateRunning:
			return true, nil
		case StatePaused:
			return false, nil
		default:
			return false, ErrNotPaused
		}
	})
}

// settle explains an error of a pause or an unpause by the state of the
// container after it: judge says whether that state is what the request
// was for, so there is nothing to do, and otherwise what error it is.
// Without the state, the error is explained as it is.
func (c *Client) settle(ctx context.Context, id, what string, err error, judge func(State) (done bool, kind error)) error {
	if err == nil || ctx.Err() != nil || cerrdefs.IsNotFound(err) {
		return lifecycleError(ctx, id, what, err, nil)
	}

	ctr, ierr := c.Inspect(ctx, id)
	if ierr != nil {
		return lifecycleError(ctx, id, what, err, nil)
	}

	done, kind := judge(ctr.State)
	if done {
		return nil
	}

	return lifecycleError(ctx, id, what, err, kind)
}

// lifecycleError explains an error of a request that changes the state of
// a container; nil stays nil. kind, if set, is what the error means.
func lifecycleError(ctx context.Context, id, what string, err, kind error) error {
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("%s container %s: %w", what, id, ctx.Err())
	case cerrdefs.IsNotFound(err):
		return fmt.Errorf("%s container %s: %w: %w", what, id, ErrNotFound, err)
	case kind != nil:
		return fmt.Errorf("%s container %s: %w: %w", what, id, kind, err)
	default:
		return fmt.Errorf("%s container %s: %w", what, id, err)
	}
}
