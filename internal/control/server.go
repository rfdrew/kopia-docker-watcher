package control

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
)

const (
	// requestTimeout bounds the reading of a request, writeTimeout each
	// line of the answer: a client that stops reading only loses its
	// answer.
	requestTimeout = 10 * time.Second
	writeTimeout   = 10 * time.Second
	// probeTimeout bounds the check whether a watcher answers at an
	// existing socket.
	probeTimeout = time.Second
	// socketMode lets only the watcher's user connect.
	socketMode = 0o600
	dirMode    = 0o700
	// maxAcceptDelay bounds the delay before Accept is tried again after
	// an error, as when the process ran out of file descriptors.
	maxAcceptDelay = time.Second
)

// Handler serves a valid request. It may call progress to send progress
// Messages, and returns the result Message, whose Type it need not set,
// or an error, which the client gets as an error Message. ctx ends when
// the client goes away or the server closes.
type Handler func(ctx context.Context, req Request, progress func(Message)) (Message, error)

// Server serves the control socket.
type Server struct {
	path string
	ln   net.Listener
	// info is the socket file as Listen left it, so that Close removes
	// only its own; nil for a server of NewServer.
	info os.FileInfo
	log  *slog.Logger

	handler  atomic.Pointer[Handler]
	notReady atomic.Pointer[string]

	ctx    context.Context
	cancel context.CancelFunc
	conns  sync.WaitGroup
	// mu guards closed and the conns.Add of a new connection, so that
	// Close's Wait never races an Add.
	mu     sync.Mutex
	closed bool
}

// Listen creates the socket at path, readable and writable by the user of
// the process only, in a directory created if missing. A socket left at
// path by a watcher that is gone is replaced; a watcher that still answers
// there, or a file that is no socket, is an error. Until SetHandler, every
// request is answered with an error that the watcher is starting.
func Listen(path string, log *slog.Logger) (*Server, error) {
	if err := config.ValidateControlSocket(path); err != nil {
		return nil, fmt.Errorf("control socket: %w", err)
	}

	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("create the directory of the control socket: %w", err)
	}

	switch fi, err := os.Lstat(path); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, fmt.Errorf("control socket %s: %w", path, err)
	case fi.Mode()&os.ModeSocket == 0:
		return nil, fmt.Errorf("control socket %s: the file exists and is not a socket", path)
	case answers(path):
		return nil, fmt.Errorf("control socket %s: another watcher answers there", path)
	}

	// The socket is made in a directory only the user can enter, and
	// moved into place once only the user can connect to it, so that it
	// is never open to others, whatever the umask.
	tmpDir, err := os.MkdirTemp(dir, ".")
	if err != nil {
		return nil, fmt.Errorf("create the control socket: %w", err)
	}

	defer os.RemoveAll(tmpDir)

	tmp := filepath.Join(tmpDir, "s")

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: tmp, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on the control socket: %w", err)
	}

	ln.SetUnlinkOnClose(false)

	fail := func(err error) (*Server, error) {
		_ = ln.Close()
		return nil, err
	}

	if err := os.Chmod(tmp, socketMode); err != nil {
		return fail(fmt.Errorf("set the mode of the control socket: %w", err))
	}

	if err := os.Rename(tmp, path); err != nil {
		return fail(fmt.Errorf("move the control socket into place: %w", err))
	}

	info, err := os.Lstat(path)
	if err != nil {
		return fail(fmt.Errorf("control socket %s: %w", path, err))
	}

	s := NewServer(ln, log)
	s.path, s.info = path, info

	return s, nil
}

// NewServer returns a server on ln, which it closes; Listen makes the
// socket file and its listener. Until SetHandler, every request is
// answered with an error that the watcher is starting.
func NewServer(ln net.Listener, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{path: ln.Addr().String(), ln: ln, log: log, ctx: ctx, cancel: cancel}
	s.SetNotReady("the watcher is starting: it waits for kopia server")

	return s
}

// answers reports whether something accepts connections at the socket.
func answers(path string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	var d net.Dialer

	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

// Path returns the path of the socket.
func (s *Server) Path() string { return s.path }

// SetHandler sets what serves the requests; nil answers them with the
// message of SetNotReady.
func (s *Server) SetHandler(h Handler) {
	if h == nil {
		s.handler.Store(nil)
		return
	}

	s.handler.Store(&h)
}

// SetNotReady sets the error the requests get while there is no handler.
func (s *Server) SetNotReady(msg string) { s.notReady.Store(&msg) }

// Serve accepts connections until Close and returns nil then. An error
// of Accept, as when the process ran out of file descriptors, is logged
// and Accept tried again after a delay.
func (s *Server) Serve() error {
	var delay time.Duration

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.isClosed() {
				return nil
			}

			delay = min(max(2*delay, 5*time.Millisecond), maxAcceptDelay)
			s.log.Warn("accept on the control socket failed; trying again", "error", err.Error(), "delay", delay.String())

			if !s.sleep(delay) {
				return nil
			}

			continue
		}

		delay = 0

		s.mu.Lock()

		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()

			return nil
		}

		s.conns.Add(1)
		s.mu.Unlock()

		go func() {
			defer s.conns.Done()
			s.serveConn(conn)
		}()
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// sleep waits d, or until Close, and reports whether d passed.
func (s *Server) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-s.ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Close stops accepting connections, ends the context of the requests
// being served, waits for them until ctx ends and removes the socket.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true
	s.mu.Unlock()

	err := s.ln.Close()

	s.cancel()

	done := make(chan struct{})

	go func() {
		s.conns.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		s.log.Warn("control requests still running at the stop")
	}

	// Only the socket Listen made: another watcher may have taken the
	// path over meanwhile.
	if fi, statErr := os.Lstat(s.path); s.info != nil && statErr == nil && os.SameFile(fi, s.info) {
		if rmErr := os.Remove(s.path); rmErr != nil {
			err = errors.Join(err, rmErr)
		}
	}

	return err
}

// serveConn serves one connection: one request, its answer.
func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(requestTimeout))

	line, err := bufio.NewReader(io.LimitReader(conn, maxRequest+1)).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) || len(line) > maxRequest {
		s.log.Debug("bad control request", "error", fmt.Sprint(err), "size", len(line))
		return
	}

	_ = conn.SetReadDeadline(time.Time{})

	var mu sync.Mutex

	write := func(m Message) error {
		mu.Lock()
		defer mu.Unlock()

		b, err := json.Marshal(m)
		if err != nil {
			return err
		}

		_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		_, err = conn.Write(append(b, '\n'))

		return err
	}

	fail := func(err error) {
		_ = write(Message{Type: TypeError, Error: err.Error()})
	}

	var req Request

	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&req); err != nil {
		fail(fmt.Errorf("malformed request: %w", err))
		return
	}

	if err := req.Validate(); err != nil {
		fail(err)
		return
	}

	h := s.handler.Load()
	if h == nil {
		fail(errors.New(*s.notReady.Load()))
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	// The client sends nothing after its request: a read that returns
	// means it went away.
	go func() {
		_, _ = conn.Read(make([]byte, 1))
		cancel()
	}()

	res, err := (*h)(ctx, req, func(m Message) {
		m.Type = TypeProgress
		_ = write(m)
	})
	if err != nil {
		fail(err)
		return
	}

	res.Type = TypeResult
	_ = write(res)
}
