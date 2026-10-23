package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/health"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
)

// get requests a path of the watcher's HTTP server; a status of 0 means it
// does not answer yet.
func (wr *watcherRun) get(t *testing.T, path string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:8080"+path, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := wr.td.httpClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(body)
}

// /healthz reports every component once the watcher runs, and the
// healthcheck subcommand passes.
func TestRunServesHealthz(t *testing.T) {
	k := newRunKopia(t)
	k.statuses = []reply{{status: http.StatusServiceUnavailable, body: "starting"}}

	wr := startWatcher(t, k, newWatchEngine(appContainer()), watcherOptions{retry: 50 * time.Millisecond})

	var report health.Report

	waitFor(t, "a healthy watcher", func() bool {
		code, body := wr.get(t, "/healthz")
		if code != http.StatusOK {
			return false
		}

		if err := json.Unmarshal([]byte(body), &report); err != nil {
			t.Fatalf("/healthz body %q: %v", body, err)
		}

		return report.Status == health.StatusOK
	})

	for _, c := range []string{componentDocker, componentKopia, componentReconcile} {
		if cr := report.Components[c]; cr.Status != health.StatusOK || cr.LastOK.IsZero() {
			t.Errorf("component %s = %+v", c, cr)
		}
	}

	// The failed first attempt to reach kopia server was a failed check.
	if !strings.Contains(findLog(wr.td.stderr.String(), "kopia server is not reachable yet, waiting")["error"].(string), "503") {
		t.Errorf("log:\n%s", wr.td.stderr)
	}

	if code := run(t.Context(), []string{"healthcheck"}, wr.td.deps); code != 0 {
		t.Errorf("healthcheck exit code = %d", code)
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}
}

// hookRecorder records the notifications.
type hookRecorder struct {
	mu     sync.Mutex
	bodies []map[string]any
}

func (h *hookRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	var body map[string]any
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	h.mu.Lock()
	h.bodies = append(h.bodies, body)
	h.mu.Unlock()

	rec := httptest.NewRecorder()
	rec.WriteHeader(http.StatusNoContent)

	return rec.Result(), nil
}

func (h *hookRecorder) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	var out []string
	for _, b := range h.bodies {
		out = append(out, b["event"].(string)+" "+b["path"].(string))
	}

	return out
}

// A path whose owners disagree is notified once, with kopia server's host,
// and its recovery once the disagreement is gone; the metric counts both.
func TestRunNotifies(t *testing.T) {
	a, b := appContainer(), appContainer()
	b.ID, b.Name = "beef", "app-2"
	b.Labels = map[string]string{"kopia.enable": "true", "kopia.path": "/srv/app", "kopia.schedule": "0 3 * * *"}

	e := newWatchEngine(a, b)
	hook := &hookRecorder{}

	wr := startWatcher(t, newRunKopia(t), e, watcherOptions{
		env:     []string{"NOTIFY_WEBHOOK_URL", "https://hooks.example.com/hook?token=secret-token"},
		webhook: hook,
	})

	waitFor(t, "the notification", func() bool { return len(hook.events()) == 1 })

	hook.mu.Lock()
	first := hook.bodies[0]
	hook.mu.Unlock()

	if first["event"] != "reconcile_failed" || first["path"] != "/srv/app" || first["reason"] != "owners_disagree" ||
		first["host"] != fakeHost {
		t.Errorf("notification = %v", first)
	}

	// The next full reconcile finds the same problem: no notification.
	wr.ticks <- time.Now()

	e.mu.Lock()
	e.containers = e.containers[:1]
	e.mu.Unlock()

	e.events <- docker.Event{Kind: docker.KindRemoved, Action: "destroy", ContainerID: b.ID, Name: b.Name, Labels: b.Labels,
		Time: time.Now()}

	waitFor(t, "the recovery", func() bool { return len(hook.events()) == 2 })

	if got := strings.Join(hook.events(), ", "); got != "reconcile_failed /srv/app, reconcile_recovered /srv/app" {
		t.Errorf("notifications = %s", got)
	}

	_, metrics := wr.get(t, "/metrics")
	if !strings.Contains(metrics, "\nkopia_watcher_notifications_total{result=\"sent\"} 2\n") {
		t.Errorf("metrics:\n%s", metrics)
	}

	if strings.Contains(wr.td.stderr.String(), "secret-token") {
		t.Errorf("the log shows the token:\n%s", wr.td.stderr)
	}

	wr.cancel()

	if code := wr.wait(t, 10*time.Second); code != 0 {
		t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
	}
}

// The throttle is set only when a KOPIA_THROTTLE_ variable is, keeping
// the limits that none sets.
func TestRunThrottle(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		k := newRunKopia(t)
		k.throttle = kopiaclient.ThrottleLimits{ReadsPerSecond: 7}

		wr := startWatcher(t, k, newWatchEngine(), watcherOptions{})
		wr.waitLog(t, "watching containers")
		wr.cancel()

		if code := wr.wait(t, 10*time.Second); code != 0 {
			t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
		}

		if n := k.count("GET /api/v1/repo/throttle") + k.count("PUT /api/v1/repo/throttle"); n != 0 {
			t.Errorf("%d throttle requests", n)
		}
	})

	t.Run("set", func(t *testing.T) {
		k := newRunKopia(t)
		k.throttle = kopiaclient.ThrottleLimits{ReadsPerSecond: 7, ConcurrentWrites: 3}

		wr := startWatcher(t, k, newWatchEngine(), watcherOptions{env: []string{
			"KOPIA_THROTTLE_UPLOAD_BYTES_PER_SECOND", "10MiB",
			"KOPIA_THROTTLE_CONCURRENT_WRITES", "0",
		}})
		wr.waitLog(t, "set kopia server's throttle")
		wr.waitLog(t, "watching containers")

		k.mu.Lock()
		got := k.throttle
		k.mu.Unlock()

		if want := (kopiaclient.ThrottleLimits{ReadsPerSecond: 7, UploadBytesPerSecond: 10 << 20}); got != want {
			t.Errorf("throttle = %+v, want %+v", got, want)
		}

		// Set already: the next start reads it and writes nothing.
		wr.cancel()

		if code := wr.wait(t, 10*time.Second); code != 0 {
			t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
		}

		wr = startWatcher(t, k, newWatchEngine(), watcherOptions{env: []string{
			"KOPIA_THROTTLE_UPLOAD_BYTES_PER_SECOND", "10MiB",
			"KOPIA_THROTTLE_CONCURRENT_WRITES", "0",
		}})
		wr.waitLog(t, "kopia server's throttle is as configured")
		wr.cancel()

		if code := wr.wait(t, 10*time.Second); code != 0 {
			t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
		}

		if n := k.count("PUT /api/v1/repo/throttle"); n != 1 {
			t.Errorf("%d throttle writes, want 1", n)
		}
	})

	// kopia server connected to a repository server has no throttle;
	// the watcher says so and goes on.
	t.Run("no storage", func(t *testing.T) {
		k := newRunKopia(t)
		k.throttleErr = true

		wr := startWatcher(t, k, newWatchEngine(), watcherOptions{env: []string{"KOPIA_THROTTLE_READS_PER_SECOND", "5"}})

		l := wr.waitLog(t, "cannot set kopia server's throttle: it has no repository storage of its own")
		if !strings.Contains(l["error"].(string), "STORAGE_CONNECTION") {
			t.Errorf("log line = %v", l)
		}

		wr.waitLog(t, "watching containers")
		wr.cancel()

		if code := wr.wait(t, 10*time.Second); code != 0 {
			t.Fatalf("exit code = %d; log:\n%s", code, wr.td.stderr)
		}
	})
}
