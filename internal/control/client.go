package control

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
)

// NotRunningError is the error of a client that cannot connect to the
// socket: no watcher runs in the container, or it does not serve the
// socket.
type NotRunningError struct {
	Path string
	Err  error
}

func (e *NotRunningError) Error() string {
	return fmt.Sprintf("no watcher answers at the control socket %s (%v): the watcher must run in this container, "+
		"with the same CONTROL_SOCKET and STATE_FILE; run the command with docker exec in the watcher's container",
		e.Path, e.Err)
}

func (e *NotRunningError) Unwrap() []error { return []error{ErrNotRunning, e.Err} }

// RemoteError is an error the watcher answered with.
type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return e.Msg }

// Do sends req to the watcher at the socket path and returns its result
// Message, after it passed each progress Message to progress, if not nil.
// The version of req is set. A watcher that cannot be reached is a
// NotRunningError, its error answer a RemoteError. When ctx ends, Do
// closes the connection and returns ctx's error.
func Do(ctx context.Context, path string, req Request, progress func(Message)) (Message, error) {
	return DoDial(ctx, path, nil, req, progress)
}

// Dialer connects to the socket at path.
type Dialer func(ctx context.Context, path string) (net.Conn, error)

// DialUnix is the Dialer of Do.
func DialUnix(ctx context.Context, path string) (net.Conn, error) {
	var d net.Dialer

	return d.DialContext(ctx, "unix", path)
}

// DoDial is Do with dial in place of DialUnix; nil means DialUnix.
func DoDial(ctx context.Context, path string, dial Dialer, req Request, progress func(Message)) (Message, error) {
	if dial == nil {
		dial = DialUnix
	}

	conn, err := dial(ctx, path)
	if err != nil {
		if ctx.Err() != nil {
			return Message{}, ctx.Err()
		}

		return Message{}, &NotRunningError{Path: path, Err: err}
	}
	defer conn.Close()

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	req.Version = Version

	b, err := json.Marshal(req)
	if err != nil {
		return Message{}, err
	}

	if _, err := conn.Write(append(b, '\n')); err != nil {
		return Message{}, wrapRead(ctx, err)
	}

	dec := json.NewDecoder(bufio.NewReader(conn))

	for {
		var m Message

		if err := dec.Decode(&m); err != nil {
			return Message{}, wrapRead(ctx, err)
		}

		switch m.Type {
		case TypeProgress:
			if progress != nil {
				progress(m)
			}
		case TypeResult:
			return m, nil
		case TypeError:
			return m, &RemoteError{Msg: m.Error}
		default:
			return m, fmt.Errorf("the watcher answered with a message of unknown type %q", m.Type)
		}
	}
}

// wrapRead explains an error reading the answer.
func wrapRead(ctx context.Context, err error) error {
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return errors.New("the watcher closed the connection without an answer: it may be stopping")
	default:
		return fmt.Errorf("read the watcher's answer: %w", err)
	}
}
