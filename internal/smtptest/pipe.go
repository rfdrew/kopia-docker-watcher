package smtptest

import (
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Pipe returns the two ends of an in-memory connection. Unlike with
// net.Pipe, a write does not wait for the other end to read it, as with
// TCP, so both ends may write at once, as TLS does. Reads honor the
// deadlines; writes never block, so write deadlines do nothing.
func Pipe() (net.Conn, net.Conn) {
	a, b := newBuffer(), newBuffer()
	return newConn(a, b), newConn(b, a)
}

// buffer is what one end wrote and the other has not read yet.
type buffer struct {
	mu     sync.Mutex
	data   []byte
	closed bool
	// ready is closed, and replaced, when data comes or the buffer closes.
	ready chan struct{}
}

func newBuffer() *buffer { return &buffer{ready: make(chan struct{})} }

// wake tells the readers that something changed; b.mu is held.
func (b *buffer) wake() {
	close(b.ready)
	b.ready = make(chan struct{})
}

func (b *buffer) close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.closed {
		b.closed = true
		b.wake()
	}
}

// conn is one end of a Pipe.
type conn struct {
	in, out *buffer

	mu     sync.Mutex
	closed bool
	// deadline is the read deadline; changed is closed, and replaced, when
	// it changes or the conn closes.
	deadline time.Time
	changed  chan struct{}
}

func newConn(in, out *buffer) *conn {
	return &conn{in: in, out: out, changed: make(chan struct{})}
}

func (c *conn) Read(p []byte) (int, error) {
	for {
		c.mu.Lock()
		closed, deadline, changed := c.closed, c.deadline, c.changed
		c.mu.Unlock()

		if closed {
			return 0, net.ErrClosed
		}

		c.in.mu.Lock()

		if len(c.in.data) > 0 {
			n := copy(p, c.in.data)
			c.in.data = c.in.data[n:]
			c.in.mu.Unlock()

			return n, nil
		}

		eof, ready := c.in.closed, c.in.ready
		c.in.mu.Unlock()

		if eof {
			return 0, io.EOF
		}

		if !c.wait(ready, changed, deadline) {
			return 0, os.ErrDeadlineExceeded
		}
	}
}

// wait waits for data, a change of the deadline or the close; false when
// the deadline passes first.
func (c *conn) wait(ready, changed <-chan struct{}, deadline time.Time) bool {
	var expired <-chan time.Time

	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return false
		}

		t := time.NewTimer(d)
		defer t.Stop()

		expired = t.C
	}

	select {
	case <-ready:
	case <-changed:
	case <-expired:
		return false
	}

	return true
}

func (c *conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()

	if closed {
		return 0, net.ErrClosed
	}

	c.out.mu.Lock()
	defer c.out.mu.Unlock()

	if c.out.closed {
		return 0, io.ErrClosedPipe
	}

	c.out.data = append(c.out.data, p...)
	c.out.wake()

	return len(p), nil
}

// Close ends both directions: the other end reads what is left, then
// io.EOF.
func (c *conn) Close() error {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}

	c.closed = true
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()

	c.out.close()
	c.in.close()

	return nil
}

func (c *conn) SetDeadline(t time.Time) error { return c.SetReadDeadline(t) }

func (c *conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.deadline = t
	close(c.changed)
	c.changed = make(chan struct{})

	return nil
}

func (c *conn) SetWriteDeadline(time.Time) error { return nil }

func (c *conn) LocalAddr() net.Addr  { return pipeAddr{} }
func (c *conn) RemoteAddr() net.Addr { return pipeAddr{} }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "smtptest" }
func (pipeAddr) String() string  { return "smtptest" }
