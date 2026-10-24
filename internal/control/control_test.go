package control

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/control/controltest"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// socketPath returns a socket path short enough for every platform.
func socketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, "control.sock")
}

// listen is Listen, but skips the test where unix sockets cannot be
// bound, as in a sandbox.
func listen(t *testing.T, path string) *Server {
	t.Helper()

	s, err := Listen(path, nil)
	if err != nil && errors.Is(err, os.ErrPermission) {
		t.Skipf("cannot bind a unix socket here: %v", err)
	}

	if err != nil {
		t.Fatal(err)
	}

	return s
}

// run serves s with h until the test ends.
func run(t *testing.T, s *Server, h Handler) {
	t.Helper()

	if h != nil {
		s.SetHandler(h)
	}

	done := make(chan error, 1)

	go func() { done <- s.Serve() }()

	t.Cleanup(func() {
		if err := s.Close(context.Background()); err != nil {
			t.Error(err)
		}

		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

// memory serves h in memory and returns a function that sends a request
// to it.
func memory(t *testing.T, h Handler) (*Server, func(ctx context.Context, req Request, progress func(Message)) (Message, error)) {
	t.Helper()

	ln := controltest.NewListener()
	s := NewServer(ln, nil)
	run(t, s, h)

	return s, func(ctx context.Context, req Request, progress func(Message)) (Message, error) {
		return DoDial(ctx, "memory", ln.Dial, req, progress)
	}
}

// serve starts a server at the unix socket path with h.
func serve(t *testing.T, path string, h Handler) *Server {
	t.Helper()

	s := listen(t, path)
	run(t, s, h)

	return s
}

// A request gets its progress messages, then its result.
func TestRoundTrip(t *testing.T) {
	var got Request

	_, do := memory(t, func(_ context.Context, req Request, progress func(Message)) (Message, error) {
		got = req

		progress(Message{Text: "one"})
		progress(Message{Text: "two"})

		return Message{Triggers: []reconcile.TriggerResult{{Path: "/data", Outcome: "succeeded"}}}, nil
	})

	var texts []string

	res, err := do(t.Context(), Request{Command: CommandTrigger, Target: "app", Force: true},
		func(m Message) { texts = append(texts, m.Type+":"+m.Text) })
	if err != nil {
		t.Fatal(err)
	}

	if got.Version != Version || got.Command != CommandTrigger || got.Target != "app" || !got.Force {
		t.Errorf("request = %+v", got)
	}

	if strings.Join(texts, ",") != "progress:one,progress:two" {
		t.Errorf("progress = %q", texts)
	}

	if res.Type != TypeResult || len(res.Triggers) != 1 || res.Triggers[0].Path != "/data" {
		t.Errorf("result = %+v", res)
	}
}

// The socket is open to its user only, and goes when the server closes.
func TestSocketMode(t *testing.T) {
	path := socketPath(t)
	s := listen(t, path)

	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}

	if fi.Mode()&os.ModeSocket == 0 || fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v", fi.Mode())
	}

	if leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".ctl*")); len(leftovers) != 0 {
		t.Errorf("temporary socket left: %q", leftovers)
	}

	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket left after Close: %v", err)
	}
}

// A handler error, a request the server cannot serve and a server without
// a handler answer with an error.
func TestErrors(t *testing.T) {
	ln := controltest.NewListener()
	s := NewServer(ln, nil)
	run(t, s, nil)

	do := func(req Request) error {
		_, err := DoDial(t.Context(), "memory", ln.Dial, req, nil)
		return err
	}

	err := do(Request{Command: CommandList})

	var re *RemoteError
	if !errors.As(err, &re) || !strings.Contains(re.Msg, "starting") {
		t.Fatalf("no handler: %v", err)
	}

	s.SetHandler(func(context.Context, Request, func(Message)) (Message, error) {
		return Message{}, errors.New("no container named \"x\"")
	})

	if err := do(Request{Command: CommandTrigger, Target: "x"}); err == nil || err.Error() != `no container named "x"` {
		t.Fatalf("handler error: %v", err)
	}

	for _, req := range []Request{
		{Command: "bogus"},
		{Command: CommandTrigger},
		{Command: CommandRestore, Target: "app"},
		{Command: CommandRestore, Target: "app", Into: "relative"},
		{Command: CommandRestore, Target: "app", Into: "/"},
	} {
		if err := do(req); !errors.As(err, &re) {
			t.Errorf("%+v: %v", req, err)
		}
	}

	// A client of another version, and one that sends garbage.
	for line, want := range map[string]string{
		`{"version":99,"command":"list"}`:      "version 99",
		`{"version":1,"command":"list","x":1}`: "malformed request",
		`nonsense`:                             "malformed request",
	} {
		conn, err := ln.Dial(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}

		go func() { _, _ = conn.Write([]byte(line + "\n")) }()

		var m Message
		if err := json.NewDecoder(conn).Decode(&m); err != nil || m.Type != TypeError || !strings.Contains(m.Error, want) {
			t.Errorf("%s: %+v, %v", line, m, err)
		}

		_ = conn.Close()
	}
}

// Without a watcher, a client gets an error that says so.
func TestNotRunning(t *testing.T) {
	path := socketPath(t)

	_, err := Do(t.Context(), path, Request{Command: CommandList}, nil)
	if !errors.Is(err, ErrNotRunning) || !strings.Contains(err.Error(), "docker exec") || !strings.Contains(err.Error(), path) {
		t.Fatalf("missing socket: %v", err)
	}

	// A socket left by a watcher that is gone.
	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "unix", path)
	if err != nil && errors.Is(err, os.ErrPermission) {
		t.Skipf("cannot bind a unix socket here: %v", err)
	}

	if err != nil {
		t.Fatal(err)
	}

	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = ln.Close()

	if _, err := Do(t.Context(), path, Request{Command: CommandList}, nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("stale socket: %v", err)
	}

	// A new watcher replaces it.
	serve(t, path, func(context.Context, Request, func(Message)) (Message, error) { return Message{}, nil })

	if _, err := Do(t.Context(), path, Request{Command: CommandList}, nil); err != nil {
		t.Fatalf("after the restart: %v", err)
	}
}

// A second watcher does not take over the socket of one that answers, nor
// a file that is not a socket.
func TestListenRefuses(t *testing.T) {
	path := socketPath(t)

	file := filepath.Join(filepath.Dir(path), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Listen(file, nil); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("file: %v", err)
	}

	if _, err := Listen("relative.sock", nil); err == nil {
		t.Fatal("relative path accepted")
	}

	serve(t, path, nil)

	if _, err := Listen(path, nil); err == nil || !strings.Contains(err.Error(), "another watcher") {
		t.Fatalf("live socket: %v", err)
	}
}

// The handler's context ends when the client goes away, and when the
// server closes.
func TestContextEnds(t *testing.T) {
	started := make(chan struct{}, 1)
	ended := make(chan struct{}, 1)

	s, do := memory(t, func(ctx context.Context, _ Request, _ func(Message)) (Message, error) {
		started <- struct{}{}
		<-ctx.Done()
		ended <- struct{}{}

		return Message{}, ctx.Err()
	})

	ctx, cancel := context.WithCancel(t.Context())

	errc := make(chan error, 1)

	go func() {
		_, err := do(ctx, Request{Command: CommandList}, nil)
		errc <- err
	}()

	<-started
	cancel()

	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("client: %v", err)
	}

	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler's context did not end with the client")
	}

	go func() {
		_, err := do(t.Context(), Request{Command: CommandList}, nil)
		errc <- err
	}()

	<-started

	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	<-ended

	if err := <-errc; err == nil {
		t.Fatal("no error when the server closed")
	}
}
