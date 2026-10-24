// Package controltest serves the control protocol in memory, for tests
// that cannot bind a unix socket.
package controltest

import (
	"context"
	"errors"
	"net"
	"sync"
)

// Listener is an in-memory net.Listener; Dial connects to it.
type Listener struct {
	conns chan net.Conn
	once  sync.Once
	done  chan struct{}
}

// NewListener returns a Listener.
func NewListener() *Listener {
	return &Listener{conns: make(chan net.Conn), done: make(chan struct{})}
}

// Accept implements net.Listener.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close implements net.Listener.
func (l *Listener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

// Addr implements net.Listener.
func (l *Listener) Addr() net.Addr { return &net.UnixAddr{Name: "memory", Net: "unix"} }

// Dial connects to the listener; it fails like a socket nobody listens
// on once the listener is closed. The path is ignored.
func (l *Listener) Dial(ctx context.Context, _ string) (net.Conn, error) {
	client, server := net.Pipe()

	select {
	case l.conns <- server:
		return client, nil
	case <-l.done:
		return nil, errors.New("connect: connection refused")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
