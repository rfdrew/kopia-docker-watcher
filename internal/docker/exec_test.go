package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

// fakeExecSpec says how a command run in a container ends.
type fakeExecSpec struct {
	// status answers the exec creation with an error.
	status  int
	message string
	// stdout and stderr are written in this order, each in one frame.
	stdout, stderr string
	exitCode       int
	// hold, if set, keeps the command running until it is closed.
	hold chan struct{}
}

// fakeExec is an exec created in the fake daemon.
type fakeExec struct {
	container string
	cmd       []string
	spec      fakeExecSpec
	running   bool
	done      chan struct{}
}

// serveExec serves the exec endpoints and reports whether path was one.
func (f *fakeDocker) serveExec(w http.ResponseWriter, r *http.Request, path string) bool {
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/containers/") && strings.HasSuffix(path, "/exec"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/exec")

		var req container.ExecCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeDockerError(w, http.StatusBadRequest, err.Error())
			return true
		}

		f.mu.Lock()
		spec, ok := f.execSpecs[id]

		if ok && spec.status == 0 {
			if f.execs == nil {
				f.execs = map[string]*fakeExec{}
			}

			eid := fmt.Sprintf("exec-%d", len(f.execs)+1)
			f.execs[eid] = &fakeExec{container: id, cmd: req.Cmd, spec: spec, done: make(chan struct{})}
			f.mu.Unlock()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(container.ExecCreateResponse{ID: eid})

			return true
		}

		f.mu.Unlock()

		switch {
		case !ok:
			writeDockerError(w, http.StatusNotFound, "No such container: "+id)
		default:
			writeDockerError(w, spec.status, spec.message)
		}

		return true
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/exec/") && strings.HasSuffix(path, "/start"):
		// The daemon reads the request before it takes the connection
		// over; a body left unread would block the client's write.
		_, _ = io.Copy(io.Discard, r.Body)
		f.startExec(w, strings.TrimSuffix(strings.TrimPrefix(path, "/exec/"), "/start"))

		return true
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/exec/") && strings.HasSuffix(path, "/json"):
		eid := strings.TrimSuffix(strings.TrimPrefix(path, "/exec/"), "/json")

		f.mu.Lock()
		ex, ok := f.execs[eid]

		var resp container.ExecInspectResponse
		if ok {
			code := ex.spec.exitCode
			resp = container.ExecInspectResponse{ID: eid, ContainerID: ex.container, Running: ex.running, ExitCode: &code}
		}
		f.mu.Unlock()

		if !ok {
			writeDockerError(w, http.StatusNotFound, "No such exec instance: "+eid)
			return true
		}

		writeDockerJSON(w, resp)

		return true
	}

	return false
}

// startExec answers an exec start with an upgraded connection that carries
// the multiplexed output, then ends the command.
func (f *fakeDocker) startExec(w http.ResponseWriter, eid string) {
	f.mu.Lock()
	ex, ok := f.execs[eid]

	if ok {
		ex.running = true
	}
	f.mu.Unlock()

	if !ok {
		writeDockerError(w, http.StatusNotFound, "No such exec instance: "+eid)
		return
	}

	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	_, _ = buf.WriteString("HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.multiplexed-stream\r\n" +
		"Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
	writeFrame(buf, 1, ex.spec.stdout)
	writeFrame(buf, 2, ex.spec.stderr)
	_ = buf.Flush()

	if ex.spec.hold != nil {
		<-ex.spec.hold
	}

	f.mu.Lock()
	ex.running = false
	f.mu.Unlock()
	close(ex.done)
}

// writeFrame writes s as one frame of a multiplexed stream; nothing if s is
// empty.
func writeFrame(w interface{ Write([]byte) (int, error) }, stream byte, s string) {
	if s == "" {
		return
	}

	header := make([]byte, 8)
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(s)))

	_, _ = w.Write(header)
	_, _ = w.Write([]byte(s))
}

func TestExec(t *testing.T) {
	f := newFakeDocker()
	f.execSpecs = map[string]fakeExecSpec{
		"db":      {stdout: "dumped\n", stderr: "warning: x\n", exitCode: 0},
		"failing": {stderr: "pg_dump: connection refused\n", exitCode: 2},
		"stopped": {status: http.StatusConflict, message: "container stopped is not running"},
	}

	c := newFakeDockerClient(t, f)
	ctx := t.Context()

	res, err := c.Exec(ctx, "db", []string{"pg_dump", "-f", "/backup/db.sql"})
	if err != nil {
		t.Fatal(err)
	}

	if res.ExitCode != 0 || string(res.Output) != "dumped\nwarning: x\n" || res.Truncated {
		t.Fatalf("result %+v (%q)", res, res.Output)
	}

	f.mu.Lock()
	cmd := f.execs["exec-1"].cmd
	f.mu.Unlock()

	if strings.Join(cmd, " ") != "pg_dump -f /backup/db.sql" {
		t.Fatalf("command %q", cmd)
	}

	res, err = c.Exec(ctx, "failing", []string{"sh", "-c", "pg_dump"})
	if err != nil || res.ExitCode != 2 || string(res.Output) != "pg_dump: connection refused\n" {
		t.Fatalf("result %+v, %v", res, err)
	}

	if _, err := c.Exec(ctx, "stopped", []string{"true"}); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("stopped container: %v", err)
	}

	if _, err := c.Exec(ctx, "gone", []string{"true"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing container: %v", err)
	}
}

// Exec keeps the end of a long output.
func TestExecOutputLimit(t *testing.T) {
	long := strings.Repeat("x", MaxExecOutput) + "the end\n"

	f := newFakeDocker()
	f.execSpecs = map[string]fakeExecSpec{"db": {stdout: long}}
	c := newFakeDockerClient(t, f)

	res, err := c.Exec(t.Context(), "db", []string{"true"})
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Output) != MaxExecOutput || !res.Truncated || !strings.HasSuffix(string(res.Output), "the end\n") {
		t.Fatalf("output of %d bytes, truncated %v, ends %q", len(res.Output), res.Truncated, res.Output[len(res.Output)-10:])
	}
}

// When its context ends, Exec stops waiting for the command.
func TestExecCanceled(t *testing.T) {
	hold := make(chan struct{})
	defer close(hold)

	f := newFakeDocker()
	f.execSpecs = map[string]fakeExecSpec{"db": {stdout: "started\n", hold: hold}}
	c := newFakeDockerClient(t, f)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()

	_, err := c.Exec(ctx, "db", []string{"sleep", "3600"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %v", err)
	}

	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("Exec returned after %v", d)
	}
}

func TestTailBuffer(t *testing.T) {
	b := &tailBuffer{max: 8}

	for _, s := range []string{"abc", "defg", "hijkl"} {
		_, _ = b.Write([]byte(s))
	}

	if got := string(b.bytes()); got != "efghijkl" || !b.dropped {
		t.Fatalf("got %q, dropped %v", got, b.dropped)
	}

	// A character cut at the start goes whole.
	b = &tailBuffer{max: 4}
	_, _ = b.Write([]byte("aé€"))

	if got := b.bytes(); !bytes.Equal(got, []byte("€")) {
		t.Fatalf("got %q", got)
	}

	b = &tailBuffer{max: 8}
	_, _ = b.Write([]byte("short"))

	if got := string(b.bytes()); got != "short" || b.dropped {
		t.Fatalf("got %q, dropped %v", got, b.dropped)
	}
}
