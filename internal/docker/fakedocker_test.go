package docker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/events"
	"github.com/moby/moby/client"
)

// fakeAPIVersion is the API version the fake daemon offers in /_ping; the
// client negotiates down to it.
const fakeAPIVersion = "1.47"

// fakeDocker is an in-process Docker daemon with just the endpoints the
// client uses. Every /events request becomes an eventSub the test drives.
type fakeDocker struct {
	subs chan *eventSub

	mu         sync.Mutex
	containers []container.Summary
	inspect    map[string]container.InspectResponse
	failEvents int // /events requests still to answer with 500
	failPing   bool
	requests   []recordedRequest

	// execSpecs say how the commands run in each container end, by
	// container ID; execs are the execs created, by exec ID.
	execSpecs map[string]fakeExecSpec
	execs     map[string]*fakeExec
}

type recordedRequest struct {
	Method, Path, UserAgent string
	Query                   url.Values
}

// eventSub is one /events stream: what the client asked for, and a
// channel whose messages are sent; closing it ends the stream.
type eventSub struct {
	query url.Values
	send  chan events.Message
}

func newFakeDocker() *fakeDocker {
	return &fakeDocker{
		subs:    make(chan *eventSub),
		inspect: make(map[string]container.InspectResponse),
	}
}

func (f *fakeDocker) set(fn func(f *fakeDocker)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(f)
}

// lastRequest returns the latest request to path, the API version stripped.
func (f *fakeDocker) lastRequest(t *testing.T, path string) recordedRequest {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	for i := len(f.requests) - 1; i >= 0; i-- {
		if f.requests[i].Path == path {
			return f.requests[i]
		}
	}

	t.Fatalf("no request to %s", path)

	return recordedRequest{}
}

func (f *fakeDocker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_ping" {
		f.mu.Lock()
		fail := f.failPing
		f.mu.Unlock()

		if fail {
			writeDockerError(w, http.StatusInternalServerError, "daemon is busy")
			return
		}

		w.Header().Set("Api-Version", fakeAPIVersion)
		w.Header().Set("Ostype", "linux")
		w.WriteHeader(http.StatusOK)

		return
	}

	path, ok := strings.CutPrefix(r.URL.Path, "/v"+fakeAPIVersion)
	if !ok {
		writeDockerError(w, http.StatusBadRequest, "unexpected API version in "+r.URL.Path)
		return
	}

	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		Method:    r.Method,
		Path:      path,
		UserAgent: r.UserAgent(),
		Query:     r.URL.Query(),
	})
	f.mu.Unlock()

	if f.serveExec(w, r, path) {
		return
	}

	id, isContainer := strings.CutPrefix(path, "/containers/")
	id, isJSON := strings.CutSuffix(id, "/json")
	isInspect := isContainer && isJSON

	switch {
	case r.Method != http.MethodGet:
		writeDockerError(w, http.StatusMethodNotAllowed, r.Method)
	case path == "/containers/json":
		f.mu.Lock()
		list := f.containers
		f.mu.Unlock()

		writeDockerJSON(w, list)
	case isInspect:
		f.mu.Lock()
		ctr, found := f.inspect[id]
		f.mu.Unlock()

		if !found {
			writeDockerError(w, http.StatusNotFound, "No such container: "+id)
			return
		}

		writeDockerJSON(w, ctr)
	case path == "/events":
		f.serveEvents(w, r)
	default:
		writeDockerError(w, http.StatusNotFound, "page not found")
	}
}

func (f *fakeDocker) serveEvents(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	fail := f.failEvents > 0
	if fail {
		f.failEvents--
	}
	f.mu.Unlock()

	if fail {
		writeDockerError(w, http.StatusInternalServerError, "daemon is busy")
		return
	}

	sub := &eventSub{query: r.URL.Query(), send: make(chan events.Message)}

	w.Header().Set("Content-Type", "application/jsonl")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	select {
	case f.subs <- sub:
	case <-r.Context().Done():
		return
	}

	enc := json.NewEncoder(w)

	for {
		select {
		case m, ok := <-sub.send:
			if !ok {
				return
			}

			if err := enc.Encode(m); err != nil {
				return
			}

			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// nextSub waits for the client's next /events request.
func (f *fakeDocker) nextSub(t *testing.T) *eventSub {
	t.Helper()

	select {
	case s := <-f.subs:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no /events request")
		return nil
	}
}

func writeDockerJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeDockerError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
}

// newFakeDockerClient serves f over in-memory pipes: no port and no socket
// file, which the sandbox the tests may run in does not allow.
func newFakeDockerClient(t *testing.T, f *fakeDocker) *Client {
	t.Helper()

	ln := newPipeListener()
	srv := &http.Server{Handler: f, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	// A unix host keeps HTTP_PROXY out of the way; the dialer replaces the
	// socket.
	c, err := newClient(
		client.WithHost("unix:///run/fake-docker.sock"),
		client.WithDialContext(ln.dial),
		client.WithUserAgent("test-agent"),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = c.Close() })

	return c
}

// pipeListener hands one end of a net.Pipe to the server per dial.
type pipeListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn), done: make(chan struct{})}
}

func (l *pipeListener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	c, s := net.Pipe()

	select {
	case l.conns <- s:
		return c, nil
	case <-l.done:
	case <-ctx.Done():
	}

	_ = c.Close()
	_ = s.Close()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return nil, net.ErrClosed
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddr{}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "fake-docker" }
