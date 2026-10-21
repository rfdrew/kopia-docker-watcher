package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// fakeKopia answers the read-only calls of validate the way kopia server
// does (internal/server/api_paths.go, api_policies.go) and records every
// request, so that a test can tell nothing was written.
type fakeKopia struct {
	t *testing.T

	mu           sync.Mutex
	disconnected bool
	policies     map[string]string // path -> stored policy
	badCron      string            // a cron expression policy/resolve rejects
	upcoming     []string          // RFC 3339 times policy/resolve returns
	failResolve  bool              // paths/resolve answers 500
	calls        []string          // "METHOD /path"
	updates      []string          // the updates of every policy/resolve, compacted
}

func newFakeKopia(t *testing.T) *fakeKopia {
	return &fakeKopia{
		t:        t,
		policies: map[string]string{},
		upcoming: []string{"2026-10-19T02:00:00+05:30", "2026-10-20T02:00:00+05:30", "2026-10-21T02:00:00+05:30"},
	}
}

func (f *fakeKopia) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, r.Method+" "+r.URL.Path)

	body, _ := io.ReadAll(r.Body)

	if r.URL.Path != "/" && r.Header.Get(kopiaclient.CSRFTokenHeader) != "token" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	switch r.Method + " " + r.URL.Path {
	case "GET /":
		_, _ = io.WriteString(w, `<html><head><meta name="kopia-csrf-token" content="token"></head></html>`)
	case "GET /api/v1/repo/status":
		writeJSONResponse(w, map[string]any{"connected": !f.disconnected})
	case "POST /api/v1/paths/resolve":
		if f.failResolve {
			writeKopiaError(w, http.StatusInternalServerError, "INTERNAL", "boom")
			return
		}

		var req struct{ Path string }
		if err := json.Unmarshal(body, &req); err != nil {
			f.t.Errorf("paths/resolve body %s: %v", body, err)
		}

		writeJSONResponse(w, map[string]any{"source": kopiaclient.SourceInfo{Host: "nas", UserName: "root", Path: path.Clean(req.Path)}})
	case "GET /api/v1/policy":
		f.checkSource(r)

		pol, ok := f.policies[r.URL.Query().Get("path")]
		if !ok {
			writeKopiaError(w, http.StatusBadRequest, "NOT_FOUND", "policy not found")
			return
		}

		_, _ = io.WriteString(w, pol)
	case "POST /api/v1/policy/resolve":
		f.checkSource(r)
		f.resolvePolicy(w, body)
	default:
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeKopia) checkSource(r *http.Request) {
	q := r.URL.Query()
	if q.Get("host") != "nas" || q.Get("userName") != "root" || q.Get("path") == "" {
		f.t.Errorf("%s %s: not the source paths/resolve returned", r.Method, r.URL)
	}
}

func (f *fakeKopia) resolvePolicy(w http.ResponseWriter, body []byte) {
	var req struct {
		Updates struct {
			Scheduling struct {
				Cron   []string `json:"cron"`
				Manual bool     `json:"manual"`
			} `json:"scheduling"`
		} `json:"updates"`
		NumUpcomingSnapshotTimes int `json:"numUpcomingSnapshotTimes"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		f.t.Errorf("policy/resolve body %s: %v", body, err)
	}

	var raw struct {
		Updates json.RawMessage `json:"updates"`
	}

	_ = json.Unmarshal(body, &raw)
	f.updates = append(f.updates, string(raw.Updates))

	resp := map[string]any{"effective": map[string]any{}, "upcomingSnapshotTimes": []string{}}

	sched := req.Updates.Scheduling

	switch {
	case sched.Manual && len(sched.Cron) > 0:
		resp["schedulingError"] = "invalid scheduling policy: manual cannot be combined with other scheduling policies"
	case f.badCron != "" && slices.Contains(sched.Cron, f.badCron):
		resp["schedulingError"] = `invalid cron expression "` + f.badCron + `"`
	case len(sched.Cron) > 0:
		resp["upcomingSnapshotTimes"] = f.upcoming[:min(len(f.upcoming), req.NumUpcomingSnapshotTimes)]
	}

	writeJSONResponse(w, resp)
}

func (f *fakeKopia) recorded() (calls, updates []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls), slices.Clone(f.updates)
}

func writeJSONResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeKopiaError(w http.ResponseWriter, status int, code, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": msg})
}

// handlerTransport serves requests with an http.Handler in-process.
type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if r.Body == nil {
		r.Body = http.NoBody
	}

	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

// fakeEngine is a Docker Engine with fixed containers.
type fakeEngine struct {
	containers []docker.Container
	listErr    error
	inspectErr error
	closed     bool
}

func (e *fakeEngine) Containers(context.Context) ([]docker.Container, error) {
	return slices.Clone(e.containers), e.listErr
}

func (e *fakeEngine) Inspect(_ context.Context, id string) (docker.Container, error) {
	if e.inspectErr != nil {
		return docker.Container{}, e.inspectErr
	}

	for _, c := range e.containers {
		if c.ID == id || c.Name == id {
			return c, nil
		}
	}

	return docker.Container{}, docker.ErrNotFound
}

func (e *fakeEngine) Events(context.Context, time.Time) (<-chan docker.Event, <-chan error) {
	panic("validate must not subscribe to events")
}

func (e *fakeEngine) Close() error {
	e.closed = true
	return nil
}

type validateRun struct {
	env    []string
	stdin  string
	kopia  *fakeKopia
	engine *fakeEngine
}

func (vr validateRun) run(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	td := newTestDeps(env(vr.env...))
	td.stdin = strings.NewReader(vr.stdin)

	if vr.kopia != nil {
		td.kopiaTransport = handlerTransport{vr.kopia}
	}

	td.newDocker = func() (docker.Engine, io.Closer, error) {
		if vr.engine == nil {
			return nil, nil, errors.New("cannot connect to the Docker daemon")
		}

		return vr.engine, vr.engine, nil
	}

	code = run(t.Context(), append([]string{"validate"}, args...), td.deps)

	return code, td.stdout.String(), td.stderr.String()
}

// kopiaEnv is the environment of a watcher pointed at fakeKopia.
func kopiaEnv(extra ...string) []string {
	return append([]string{"KOPIA_SERVER_URL", "http://kopia:51515", "KOPIA_SERVER_PASSWORD", "secret"}, extra...)
}

func TestValidateWithKopiaServer(t *testing.T) {
	k := newFakeKopia(t)
	// A stopped container's paused policy: the preview drops manual.
	k.policies["/host/data"] = `{"scheduling":{"manual":true,"intervalSeconds":3600},"retention":{"keepDaily":7},"future":{"x":1}}`

	code, stdout, stderr := validateRun{env: kopiaEnv("HOST_PREFIX", "/host"), kopia: k}.run(t,
		"kopia.enable=true",
		"kopia.path=/data/,logs/./",
		"kopia.schedule=0 2 * * *",
	)

	if code != 0 || stderr != "" {
		t.Fatalf("exit code = %d, stderr: %s\nstdout: %s", code, stderr, stdout)
	}

	want := `kopia.enable                 true
kopia.path                   /data/, logs/./
kopia.schedule               0 2 * * *
kopia.snapshot-on-start      true
kopia.snapshot-on-stop       false
kopia.min-interval           10m0s
kopia.description            (container name)
kopia.tags                   (none)
kopia.cleanup-on-remove      pause
kopia.cleanup-delete-pinned  false

sources:
  /data/ -> root@nas:/host/data
    next snapshots (kopia server time): 2026-10-19 02:00:00 +05:30, 2026-10-20 02:00:00 +05:30, 2026-10-21 02:00:00 +05:30
  logs/./ -> root@nas:/host/logs
    next snapshots (kopia server time): 2026-10-19 02:00:00 +05:30, 2026-10-20 02:00:00 +05:30, 2026-10-21 02:00:00 +05:30

whether the paths exist is not checked: kopia server checks that only when it registers a source
`
	if stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", stdout, want)
	}

	calls, updates := k.recorded()

	wantCalls := []string{
		"GET /", "GET /api/v1/repo/status",
		"POST /api/v1/paths/resolve", "GET /api/v1/policy", "POST /api/v1/policy/resolve",
		"POST /api/v1/paths/resolve", "GET /api/v1/policy", "POST /api/v1/policy/resolve",
	}
	if !slices.Equal(calls, wantCalls) {
		t.Errorf("calls = %q, want %q", calls, wantCalls)
	}

	wantUpdates := []string{
		`{"future":{"x":1},"retention":{"keepDaily":7},"scheduling":{"cron":["0 2 * * *"],"intervalSeconds":3600}}`,
		`{"scheduling":{"cron":["0 2 * * *"]}}`,
	}
	if !slices.Equal(updates, wantUpdates) {
		t.Errorf("updates = %q, want %q", updates, wantUpdates)
	}
}

func TestValidateKopiaServerFindings(t *testing.T) {
	tests := []struct {
		name   string
		env    []string
		setup  func(k *fakeKopia)
		args   []string
		code   int
		stdout []string // lines expected in stdout, spaces collapsed
		stderr []string // substrings expected in stderr
		once   string   // a line that must appear exactly once
	}{
		{
			name:  "bad cron is reported once for all paths",
			setup: func(k *fakeKopia) { k.badCron = "61 * * * *" },
			args:  []string{"kopia.enable=true", "kopia.path=/a,/b", "kopia.schedule=0 2 * * *;61 * * * *"},
			code:  1,
			stdout: []string{
				"/a -> root@nas:/a", "/b -> root@nas:/b",
				`error: kopia.schedule: kopia server rejects the schedule: invalid cron expression "61 * * * *"`,
				"whether the paths exist is not checked: kopia server checks that only when it registers a source",
			},
			once: `error: kopia.schedule: kopia server rejects the schedule: invalid cron expression "61 * * * *"`,
		},
		{
			name: "empty schedule drops the stored cron",
			setup: func(k *fakeKopia) {
				k.policies["/data"] = `{"scheduling":{"cron":["0 3 * * *"]}}`
			},
			args:   []string{"kopia.enable=true", "kopia.path=/data", "kopia.schedule="},
			stdout: []string{"/data -> root@nas:/data", "next snapshots: none, the effective policy has no schedule"},
		},
		{
			name:   "path kopia server cannot resolve",
			setup:  func(k *fakeKopia) { k.failResolve = true },
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			code:   1,
			stdout: []string{"/data -> /data", "error: kopia.path: /data: kopia server: POST /api/v1/paths/resolve: 500 Internal Server Error (INTERNAL): kopia server internal error: boom"},
		},
		{
			name:   "stored policy that is not an object",
			setup:  func(k *fakeKopia) { k.policies["/data"] = `{"scheduling":[1]}` },
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			code:   1,
			stdout: []string{"error: kopia.path: /data: the stored policy: scheduling is not a JSON object"},
		},
		{
			name:   "relative path is not sent",
			args:   []string{"kopia.enable=true", "kopia.path=data"},
			code:   1,
			stdout: []string{`error: kopia.path: "data" is not an absolute path; set an absolute path or HOST_PREFIX`},
		},
		{
			name:   "no repository",
			setup:  func(k *fakeKopia) { k.disconnected = true },
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			code:   1,
			stderr: []string{"validate: kopia server has no connected repository yet"},
		},
		{
			name:   "invalid configuration",
			env:    kopiaEnv("RECONCILE_INTERVAL", "1s", "LISTEN_ADDR", "nope"),
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			code:   2,
			stderr: []string{"validate: invalid configuration: RECONCILE_INTERVAL", "validate: invalid configuration: LISTEN_ADDR"},
		},
		{
			name:   "configuration warnings go to stderr",
			env:    []string{"KOPIA_SERVER_URL", "http://kopia:51515", "KOPIA_PASSWORD", "x"},
			args:   []string{"kopia.enable=true", "kopia.path=/data"},
			stdout: []string{"/data -> root@nas:/data"},
			stderr: []string{"validate: warning: KOPIA_PASSWORD is ignored"},
		},
		{
			name:   "label prefix from the configuration",
			env:    kopiaEnv("LABEL_PREFIX", "backup"),
			args:   []string{"backup.enable=true", "backup.path=/data"},
			stdout: []string{"backup.path /data", "/data -> root@nas:/data"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := newFakeKopia(t)
			if tt.setup != nil {
				tt.setup(k)
			}

			e := tt.env
			if e == nil {
				e = kopiaEnv()
			}

			code, stdout, stderr := validateRun{env: e, kopia: k}.run(t, tt.args...)
			checkOutput(t, code, stdout, stderr, tt.code, tt.stdout, tt.stderr)

			if tt.once != "" && strings.Count(stdout, tt.once) != 1 {
				t.Errorf("%q is not printed once:\n%s", tt.once, stdout)
			}

			calls, _ := k.recorded()
			for _, c := range calls {
				if !strings.HasPrefix(c, "GET ") && !strings.HasSuffix(c, "/resolve") {
					t.Errorf("validate wrote to kopia server: %s", c)
				}
			}
		})
	}
}

// checkOutput compares an exit code and looks for lines in stdout and
// substrings in stderr; stderr must be empty if none are expected.
func checkOutput(t *testing.T, code int, stdout, stderr string, wantCode int, wantStdout, wantStderr []string) {
	t.Helper()

	if code != wantCode {
		t.Errorf("exit code = %d, want %d\nstdout: %s\nstderr: %s", code, wantCode, stdout, stderr)
	}

	lines := map[string]bool{}
	for _, l := range strings.Split(stdout, "\n") {
		lines[strings.Join(strings.Fields(l), " ")] = true
	}

	for _, want := range wantStdout {
		if !lines[want] {
			t.Errorf("stdout has no line %q:\n%s", want, stdout)
		}
	}

	for _, want := range wantStderr {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not contain %q:\n%s", want, stderr)
		}
	}

	if len(wantStderr) == 0 && stderr != "" {
		t.Errorf("stderr = %q", stderr)
	}
}

func testContainers() []docker.Container {
	return []docker.Container{
		{ID: "a1b2c3d4e5f6a7b8c9d0", Name: "web", State: docker.StateRunning, Labels: map[string]string{
			"kopia.enable": "true", "kopia.path": "/srv/web", "com.docker.compose.project": "app",
		}},
		{ID: "0011223344556677", Name: "db", State: docker.StateExited, Labels: map[string]string{
			"kopia.enable": "yes", "kopia.path": "/srv/db",
		}},
		{ID: "ffeeddccbbaa", Name: "cache", State: docker.StateRunning, Labels: map[string]string{
			"kopia.path": "/srv/cache",
		}},
		{ID: "998877665544", Name: "broken", State: docker.StateCreated, Labels: map[string]string{
			"backup.enable": "true",
		}},
	}
}

func TestValidateAllContainers(t *testing.T) {
	e := &fakeEngine{containers: testContainers()}

	code, stdout, stderr := validateRun{engine: e}.run(t, "--all")
	if code != 0 || stderr != "" {
		t.Fatalf("exit code = %d, stderr: %s\nstdout: %s", code, stderr, stdout)
	}

	want := `container db (001122334455, exited)
kopia.enable                 false
kopia.path                   /srv/db
kopia.schedule               0 * * * *
kopia.snapshot-on-start      true
kopia.snapshot-on-stop       false
kopia.min-interval           10m0s
kopia.description            (container name)
kopia.tags                   (none)
kopia.cleanup-on-remove      pause
kopia.cleanup-delete-pinned  false

sources:
  /srv/db -> /srv/db

warning: kopia.enable: "yes" is not a boolean, treated as false; use true or false

container web (a1b2c3d4e5f6, running)
kopia.enable                 true
kopia.path                   /srv/web
kopia.schedule               0 * * * *
kopia.snapshot-on-start      true
kopia.snapshot-on-stop       false
kopia.min-interval           10m0s
kopia.description            (container name)
kopia.tags                   (none)
kopia.cleanup-on-remove      pause
kopia.cleanup-delete-pinned  false

sources:
  /srv/web -> /srv/web

kopia server checks skipped: KOPIA_SERVER_URL is not set, so cron expressions and the paths as kopia server sees them were not checked
`
	if stdout != want {
		t.Errorf("stdout:\n%s\nwant:\n%s", stdout, want)
	}

	if !e.closed {
		t.Error("the Docker client was not closed")
	}
}

func TestValidateContainers(t *testing.T) {
	tests := []struct {
		name   string
		env    []string
		engine *fakeEngine
		kopia  bool
		args   []string
		code   int
		stdout []string
		stderr []string
		absent []string // lines that must not be in stdout
	}{
		{
			name:   "by name and by ID",
			args:   []string{"--container", "web", "--container", "0011223344556677"},
			stdout: []string{"container web (a1b2c3d4e5f6, running)", "container db (001122334455, exited)"},
		},
		{
			name:   "missing container",
			args:   []string{"--container", "nope", "--container", "web"},
			code:   1,
			stdout: []string{"container nope", "error: no such container", "container web (a1b2c3d4e5f6, running)"},
		},
		{
			name:   "container without kopia labels",
			args:   []string{"--container", "broken"},
			stdout: []string{"container broken (998877665544, created)", "no kopia.* labels on this container"},
		},
		{
			name:   "label prefix",
			env:    []string{"LABEL_PREFIX", "backup"},
			args:   []string{"--all"},
			code:   1,
			stdout: []string{"container broken (998877665544, created)", "error: backup.path: required when backup.enable is true"},
			absent: []string{"container web (a1b2c3d4e5f6, running)"},
		},
		{
			name:   "no enabled containers",
			engine: &fakeEngine{containers: testContainers()[2:3]},
			args:   []string{"--all"},
			stdout: []string{"no containers with the kopia.enable label"},
			absent: []string{"kopia server checks skipped: KOPIA_SERVER_URL is not set, so cron expressions and the paths as kopia server sees them were not checked"},
		},
		{
			name:   "with kopia server",
			kopia:  true,
			env:    kopiaEnv(),
			args:   []string{"--container", "web"},
			stdout: []string{"/srv/web -> root@nas:/srv/web", "whether the paths exist is not checked: kopia server checks that only when it registers a source"},
		},
		{
			name:   "Docker unreachable",
			engine: nil,
			args:   []string{"--all"},
			code:   1,
			stderr: []string{"validate: cannot connect to the Docker daemon"},
		},
		{
			name:   "list fails",
			engine: &fakeEngine{listErr: errors.New("list containers: permission denied")},
			args:   []string{"--all"},
			code:   1,
			stderr: []string{"validate: list containers: permission denied"},
		},
		{
			name:   "inspect fails",
			engine: &fakeEngine{inspectErr: errors.New("inspect container web: 403 Forbidden")},
			args:   []string{"--container", "web"},
			code:   1,
			stderr: []string{"validate: inspect container web: 403 Forbidden"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vr := validateRun{env: tt.env, engine: tt.engine}
			if tt.engine == nil && tt.name != "Docker unreachable" {
				vr.engine = &fakeEngine{containers: testContainers()}
			}

			if tt.kopia {
				vr.kopia = newFakeKopia(t)
			}

			code, stdout, stderr := vr.run(t, tt.args...)
			checkOutput(t, code, stdout, stderr, tt.code, tt.stdout, tt.stderr)

			for _, l := range tt.absent {
				if strings.Contains(stdout, l) {
					t.Errorf("stdout has %q:\n%s", l, stdout)
				}
			}
		})
	}
}

func TestPreviewPolicy(t *testing.T) {
	tests := []struct {
		stored string
		cron   []string
		want   string
	}{
		{`{}`, []string{"0 * * * *"}, `{"scheduling":{"cron":["0 * * * *"]}}`},
		{`{}`, nil, `{}`},
		{`{"scheduling":null}`, nil, `{}`},
		{`{"scheduling":{"manual":true}}`, nil, `{}`},
		{`{"scheduling":{"manual":true}}`, []string{"0 2 * * *"}, `{"scheduling":{"cron":["0 2 * * *"]}}`},
		{`{"scheduling":{"cron":["1 1 * * *"],"runMissed":true}}`, nil, `{"scheduling":{"runMissed":true}}`},
		{`{"retention":{"keepLatest":3},"scheduling":{"timeOfDay":[{"hour":1,"min":0}]}}`, []string{"5 4 * * *"},
			`{"retention":{"keepLatest":3},"scheduling":{"cron":["5 4 * * *"],"timeOfDay":[{"hour":1,"min":0}]}}`},
	}

	for _, tt := range tests {
		got, err := previewPolicy(json.RawMessage(tt.stored), tt.cron)
		if err != nil || string(got) != tt.want {
			t.Errorf("previewPolicy(%s, %q) = %s, %v; want %s", tt.stored, tt.cron, got, err, tt.want)
		}
	}

	for _, bad := range []string{`[]`, `"x"`, `{"scheduling":"x"}`} {
		if _, err := previewPolicy(json.RawMessage(bad), nil); err == nil {
			t.Errorf("previewPolicy(%s): expected an error", bad)
		}
	}
}
