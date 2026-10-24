package restore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

var src = kopiaclient.SourceInfo{Host: "nas", UserName: "root", Path: "/srv/app"}

var t0 = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// fakeKopia answers the requests of a restore. Each GET of the task takes
// the next of tasks; the last one stays.
type fakeKopia struct {
	t *testing.T

	mu        sync.Mutex
	snapshots []kopiaclient.Snapshot
	restores  []kopiaclient.RestoreRequest
	tasks     []kopiaclient.TaskInfo
	gets      int
	cancels   []string
	// getErr answers the next GETs of the task with these statuses.
	getErr []int
}

func (f *fakeKopia) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	if r.URL.Path == "/" {
		_, _ = io.WriteString(w, `<html><head><meta name="kopia-csrf-token" content="tok"></head></html>`)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	reply := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/snapshots":
		if r.URL.Query().Get("all") != "1" || r.URL.Query().Get("path") != src.Path {
			f.t.Errorf("snapshots query %s", r.URL.RawQuery)
		}

		reply(kopiaclient.SnapshotsResponse{Snapshots: f.snapshots})
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/restore":
		var req kopiaclient.RestoreRequest
		if err := json.Unmarshal(body, &req); err != nil {
			f.t.Errorf("restore body %s: %v", body, err)
		}

		f.restores = append(f.restores, req)
		reply(f.tasks[0])
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/"+f.tasks[0].ID:
		if len(f.getErr) > 0 {
			code := f.getErr[0]
			f.getErr = f.getErr[1:]

			w.WriteHeader(code)
			_, _ = io.WriteString(w, `{"code":"NOT_FOUND","error":"task not found"}`)

			return
		}

		f.gets++
		reply(f.tasks[min(f.gets, len(f.tasks)-1)])
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks/"+f.tasks[0].ID+"/cancel":
		f.cancels = append(f.cancels, f.tasks[0].ID)
		reply(map[string]any{})
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
	}
}

type transport struct{ h http.Handler }

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if r.Body == nil {
		r.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)

	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

func newClient(t *testing.T, f *fakeKopia) *kopiaclient.Client {
	t.Helper()

	f.t = t

	c, err := kopiaclient.New(kopiaclient.Options{URL: "http://kopia:51515", Password: "secret", Transport: transport{f}})
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func snapshot(id string, start time.Time, incomplete string) kopiaclient.Snapshot {
	return kopiaclient.Snapshot{ID: id, StartTime: start, RootID: "root-" + id, IncompleteReason: incomplete}
}

func task(status kopiaclient.TaskStatus, files int64, msg string) kopiaclient.TaskInfo {
	return kopiaclient.TaskInfo{ID: "1f", Kind: kopiaclient.TaskKindRestore, Status: status, ErrorMessage: msg,
		Counters: map[string]kopiaclient.CounterValue{
			kopiaclient.CounterRestoredFiles: {Value: files},
			kopiaclient.CounterRestoredDirs:  {Value: 1},
			kopiaclient.CounterRestoredBytes: {Value: files * 100},
		}}
}

// Select takes the newest complete snapshot by default, and another by
// its ID or a unique start of it.
func TestSelect(t *testing.T) {
	f := &fakeKopia{snapshots: []kopiaclient.Snapshot{
		snapshot("k1aaaaaa", t0, ""),
		snapshot("k2bbbbbb", t0.Add(time.Hour), ""),
		snapshot("k2bccccc", t0.Add(2*time.Hour), "checkpoint"),
		snapshot("k3dddddd", t0.Add(3*time.Hour), "canceled"),
	}, tasks: []kopiaclient.TaskInfo{task(kopiaclient.TaskSuccess, 1, "")}}
	c := newClient(t, f)

	for want, id := range map[string]string{
		"":         "k2bbbbbb",
		Latest:     "k2bbbbbb",
		"k1aaaaaa": "k1aaaaaa",
		"k1aaaa":   "k1aaaaaa",
		"k3dddddd": "k3dddddd",
	} {
		s, err := Select(t.Context(), c, src, want)
		if err != nil || s.ID != id || s.RootID != "root-"+id {
			t.Errorf("Select(%q) = %+v, %v; want %s", want, s, err, id)
		}
	}

	for want, msg := range map[string]string{
		"k2b":      `no snapshot "k2b"`,
		"k2bbbb00": `no snapshot "k2bbbb00"`,
		"k2b000":   `no snapshot "k2b000"`,
		"k2bxxxxx": `no snapshot`,
	} {
		if _, err := Select(t.Context(), c, src, want); !errors.Is(err, ErrNoSnapshot) || !strings.Contains(err.Error(), msg) {
			t.Errorf("Select(%q) = %v", want, err)
		}
	}

	f.snapshots = append(f.snapshots, snapshot("k2bddddd", t0, ""))

	if _, err := Select(t.Context(), c, src, "k2bccc"); err != nil {
		t.Errorf("unique prefix: %v", err)
	}

	if _, err := Select(t.Context(), c, src, "k2bbbb"); err != nil {
		t.Errorf("unique prefix: %v", err)
	}

	if _, err := Select(t.Context(), c, src, "k2bddd"); err != nil {
		t.Errorf("unique prefix: %v", err)
	}

	if _, err := Select(t.Context(), c, src, "k2b"); err == nil {
		t.Error("short prefix accepted")
	}

	f.snapshots = []kopiaclient.Snapshot{snapshot("k1", t0, "checkpoint")}

	if _, err := Select(t.Context(), c, src, ""); !errors.Is(err, ErrNoSnapshot) || !strings.Contains(err.Error(), "no complete snapshot") {
		t.Errorf("no complete snapshot: %v", err)
	}

	f.snapshots = append(f.snapshots, snapshot("k2bbbbbb", t0, ""), snapshot("k2bbbbbc", t0, ""))

	if _, err := Select(t.Context(), c, src, "k2bbbb"); err == nil || !strings.Contains(err.Error(), "2 snapshots") {
		t.Errorf("ambiguous prefix: %v", err)
	}
}

// Run restores into the target without overwriting anything, and follows
// the task, telling its progress, until it succeeds.
func TestRun(t *testing.T) {
	f := &fakeKopia{tasks: []kopiaclient.TaskInfo{
		task(kopiaclient.TaskRunning, 0, ""),
		task(kopiaclient.TaskRunning, 2, ""),
		task(kopiaclient.TaskRunning, 2, ""),
		task(kopiaclient.TaskSuccess, 5, ""),
	}}
	c := newClient(t, f)

	var progress []int64

	res, err := Run(t.Context(), c, Options{
		Snapshot:     Snapshot{ID: "k1", RootID: "root-k1"},
		Target:       "/restore/app/",
		PollInterval: time.Millisecond,
		Progress:     func(p Progress) { progress = append(progress, p.Files) },
	})
	if err != nil {
		t.Fatal(err)
	}

	if !res.OK() || res.Error != "" || res.Files != 5 || res.Bytes != 500 || res.TaskID != "1f" || res.Target != "/restore/app" {
		t.Errorf("result = %+v", res)
	}

	if len(progress) != 3 || progress[0] != 0 || progress[1] != 2 || progress[2] != 5 {
		t.Errorf("progress = %v", progress)
	}

	if len(f.restores) != 1 {
		t.Fatalf("restores = %+v", f.restores)
	}

	req := f.restores[0]
	if req.Root != "root-k1" || req.Filesystem.TargetPath != "/restore/app" || req.Filesystem.OverwriteFiles ||
		req.Filesystem.OverwriteDirectories || req.Filesystem.OverwriteSymlinks || !req.Filesystem.WriteFilesAtomically ||
		req.Options.Incremental || req.Options.IgnoreErrors {
		t.Errorf("request = %+v %+v", req, req.Filesystem)
	}
}

// Overwrite lets the restore write into a directory that is not empty.
func TestRunOverwrite(t *testing.T) {
	f := &fakeKopia{tasks: []kopiaclient.TaskInfo{task(kopiaclient.TaskSuccess, 1, "")}}
	c := newClient(t, f)

	if _, err := Run(t.Context(), c, Options{Snapshot: Snapshot{RootID: "r"}, Target: "/srv/app", Overwrite: true}); err != nil {
		t.Fatal(err)
	}

	fs := f.restores[0].Filesystem
	if !fs.OverwriteFiles || !fs.OverwriteDirectories || !fs.OverwriteSymlinks {
		t.Errorf("fsOutput = %+v", fs)
	}
}

// Without Overwrite, kopia server refuses a directory that is not empty;
// the result says how to go on.
func TestRunRefusesExisting(t *testing.T) {
	f := &fakeKopia{tasks: []kopiaclient.TaskInfo{
		task(kopiaclient.TaskFailed, 0, `error restoring: non-empty directory already exists, not overwriting it: "/srv/app"`),
	}}
	c := newClient(t, f)

	res, err := Run(t.Context(), c, Options{Snapshot: Snapshot{RootID: "r"}, Target: "/srv/app"})
	if err != nil {
		t.Fatal(err)
	}

	if res.OK() || res.Status != kopiaclient.TaskFailed || !strings.Contains(res.Error, "non-empty directory already exists") ||
		!strings.Contains(res.Error, "--overwrite") {
		t.Errorf("result = %+v", res)
	}
}

// A bad target is refused before kopia server is asked.
func TestRunBadTarget(t *testing.T) {
	f := &fakeKopia{tasks: []kopiaclient.TaskInfo{task(kopiaclient.TaskSuccess, 1, "")}}
	c := newClient(t, f)

	for _, target := range []string{"", "relative", "/", "//"} {
		if _, err := Run(t.Context(), c, Options{Snapshot: Snapshot{RootID: "r"}, Target: target}); err == nil {
			t.Errorf("target %q accepted", target)
		}
	}

	if len(f.restores) != 0 {
		t.Errorf("restores = %+v", f.restores)
	}
}

// A task kopia server no longer knows ends the follow with an unknown
// result; a temporary error is tried again.
func TestRunLostTask(t *testing.T) {
	f := &fakeKopia{tasks: []kopiaclient.TaskInfo{task(kopiaclient.TaskRunning, 0, "")}, getErr: []int{503, 404}}
	c := newClient(t, f)

	res, err := Run(t.Context(), c, Options{Snapshot: Snapshot{RootID: "r"}, Target: "/x", PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	if res.OK() || res.Status != "" || !strings.Contains(res.Error, "no longer knows") {
		t.Errorf("result = %+v", res)
	}
}

// When the context ends, the task is canceled with CancelOnStop, and left
// to run without it.
func TestRunCanceled(t *testing.T) {
	for _, cancelOnStop := range []bool{true, false} {
		f := &fakeKopia{tasks: []kopiaclient.TaskInfo{task(kopiaclient.TaskRunning, 0, "")}}
		c := newClient(t, f)

		ctx, cancel := context.WithCancel(t.Context())

		res, err := Run(ctx, c, Options{Snapshot: Snapshot{RootID: "r"}, Target: "/x", PollInterval: time.Millisecond,
			CancelOnStop: cancelOnStop, Progress: func(Progress) { cancel() }})
		if !errors.Is(err, context.Canceled) || res.TaskID != "1f" {
			t.Fatalf("Run = %+v, %v", res, err)
		}

		if want := map[bool]int{true: 1, false: 0}[cancelOnStop]; len(f.cancels) != want {
			t.Errorf("cancelOnStop %v: cancels = %v", cancelOnStop, f.cancels)
		}
	}
}
