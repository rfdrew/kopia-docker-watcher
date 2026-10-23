package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types"
	"github.com/moby/moby/client"
)

// MaxExecOutput bounds the output Exec keeps of a command: the last bytes
// it wrote, which hold the error of a command that failed.
const MaxExecOutput = 4096

// ErrNotRunning is returned by Exec for a container that is not running.
var ErrNotRunning = errors.New("container is not running")

// ExecResult is how a command run with Exec ended.
type ExecResult struct {
	// ExitCode is the exit code of the command.
	ExitCode int
	// Output is what the command wrote to stdout and stderr, interleaved,
	// at most the last MaxExecOutput bytes of it; Truncated reports that
	// what came before was dropped.
	Output    []byte
	Truncated bool
}

// execExitPolls and execExitPoll bound how long Exec waits for the daemon
// to record the exit code once the output has ended: the stream can end a
// moment before the daemon marks the process as exited.
const (
	execExitPolls = 50
	execExitPoll  = 100 * time.Millisecond
)

// Exec runs cmd in the running container id as docker exec does, without a
// terminal or input, and waits until it exits. A command that exits with a
// code other than zero is no error: the result tells. When ctx ends first,
// Exec stops waiting and returns an error that wraps ctx's: Docker cannot
// stop the command, which keeps running in the container.
func (c *Client) Exec(ctx context.Context, id string, cmd []string) (ExecResult, error) {
	created, err := c.api.ExecCreate(ctx, id, client.ExecCreateOptions{Cmd: cmd, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return ExecResult{}, execError(ctx, id, "create exec", err)
	}

	attached, err := c.api.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
	if err != nil {
		return ExecResult{}, execError(ctx, id, "start exec", err)
	}

	defer attached.Close()

	// Reading the hijacked connection does not watch ctx; closing it ends
	// the read.
	stop := context.AfterFunc(ctx, attached.Close)
	defer stop()

	out := &tailBuffer{max: MaxExecOutput}

	if mt, ok := attached.MediaType(); ok && mt == types.MediaTypeRawStream {
		_, err = io.Copy(out, attached.Reader)
	} else {
		_, err = stdcopy.StdCopy(out, out, attached.Reader)
	}

	if ctx.Err() != nil {
		return ExecResult{}, fmt.Errorf("exec in container %s: %w", id, ctx.Err())
	}

	if err != nil {
		return ExecResult{}, fmt.Errorf("exec in container %s: read output: %w", id, err)
	}

	res := ExecResult{Output: out.bytes(), Truncated: out.dropped}

	for range execExitPolls {
		ins, err := c.api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil {
			return ExecResult{}, execError(ctx, id, "inspect exec", err)
		}

		if !ins.Running {
			res.ExitCode = ins.ExitCode
			return res, nil
		}

		t := time.NewTimer(execExitPoll)

		select {
		case <-ctx.Done():
			t.Stop()
			return ExecResult{}, fmt.Errorf("exec in container %s: %w", id, ctx.Err())
		case <-t.C:
		}
	}

	return ExecResult{}, fmt.Errorf("exec in container %s: the output ended, but Docker still reports the command running", id)
}

// execError explains an error of an exec request.
func execError(ctx context.Context, id, what string, err error) error {
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("%s in container %s: %w", what, id, ctx.Err())
	case cerrdefs.IsNotFound(err):
		return fmt.Errorf("%s in container %s: %w: %w", what, id, ErrNotFound, err)
	case cerrdefs.IsConflict(err):
		// Docker refuses an exec in a container that is not running, or
		// paused, with 409.
		return fmt.Errorf("%s in container %s: %w: %w", what, id, ErrNotRunning, err)
	default:
		return fmt.Errorf("%s in container %s: %w", what, id, err)
	}
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	max     int
	buf     []byte
	dropped bool
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)

	if len(p) >= b.max {
		b.dropped = b.dropped || len(b.buf) > 0 || len(p) > b.max
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)

		return n, nil
	}

	if over := len(b.buf) + len(p) - b.max; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
		b.dropped = true
	}

	b.buf = append(b.buf, p...)

	return n, nil
}

// bytes returns what the buffer kept, without the rest of a character
// whose start was dropped.
func (b *tailBuffer) bytes() []byte {
	out := b.buf

	for k := 0; b.dropped && k < utf8.UTFMax-1 && len(out) > 0 && !utf8.RuneStart(out[0]); k++ {
		out = out[1:]
	}

	return bytes.Clone(out)
}
