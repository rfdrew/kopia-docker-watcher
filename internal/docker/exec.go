package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

// MaxExecOutput bounds the output Exec keeps of a command: the last bytes
// it wrote, which hold the error of a command that failed.
const MaxExecOutput = 4096

// ErrNotRunning is returned by Exec for a container that is not running.
var ErrNotRunning = errors.New("container is not running")

// ExecResult is how a command run with Exec ended.
type ExecResult struct {
	// Started reports that Docker started the command; when it is false,
	// the exec could not be created or started, and nothing ran.
	Started bool
	// ExitCode is the exit code of the command.
	ExitCode int
	// Output is what the command wrote to stdout and stderr, interleaved,
	// at most the last MaxExecOutput bytes of it; Truncated reports that
	// what came before was dropped. A command Exec stopped waiting for
	// has the output it wrote until then.
	Output    []byte
	Truncated bool
}

// execExitPoll and execExitPollMax are the first and the longest delay
// between the checks for the exit code once the output has ended. The
// stream can end a moment before the daemon marks the process as exited,
// and much earlier when a proxy closes a connection that was silent for
// too long, as docker-socket-proxy does after ten minutes.
const (
	execExitPoll    = 100 * time.Millisecond
	execExitPollMax = 5 * time.Second
)

// Exec runs cmd in the running container id as docker exec does, without a
// terminal or input, and waits until it exits. A command that exits with a
// code other than zero is no error: the result tells. When ctx ends first,
// Exec stops waiting and returns an error that wraps ctx's, with the output
// so far: Docker cannot stop the command, which keeps running in the
// container. When the output ends before the command, as when a proxy
// closes the connection, Exec asks Docker for the exit code until the
// command has exited or ctx ends.
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

	// Without a terminal the stream is always multiplexed. Daemons before
	// API 1.42 label it as a raw stream all the same, so the label is not
	// asked. A read error ends the output like its end does: a connection
	// cut short, and the exit code tells whether the command ended.
	out := &tailBuffer{max: MaxExecOutput}
	_, _ = stdcopy.StdCopy(out, out, attached.Reader)

	res := func() ExecResult {
		return ExecResult{Started: true, Output: out.bytes(), Truncated: out.dropped}
	}

	if ctx.Err() != nil {
		return res(), fmt.Errorf("exec in container %s: %w", id, ctx.Err())
	}

	delay := execExitPoll

	for {
		ins, err := c.api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil {
			return res(), execError(ctx, id, "inspect exec", err)
		}

		if !ins.Running {
			r := res()
			r.ExitCode = ins.ExitCode

			return r, nil
		}

		t := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			t.Stop()
			return res(), fmt.Errorf("exec in container %s: %w", id, ctx.Err())
		case <-t.C:
		}

		delay = min(2*delay, execExitPollMax)
	}
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
