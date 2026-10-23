package metrics_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/backendtest"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/metrics"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// fakeDocker lists the containers it is given.
type fakeDocker struct {
	mu         sync.Mutex
	containers []docker.Container
}

func (d *fakeDocker) Containers(context.Context) ([]docker.Container, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.containers), nil
}

func (d *fakeDocker) set(cs ...docker.Container) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.containers = cs
}

// samples returns what /metrics serves, by series.
func samples(t *testing.T, h http.Handler) map[string]string {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, metrics.MetricsPath, http.NoBody))

	body, _ := io.ReadAll(rec.Body)
	out := map[string]string{}

	for line := range strings.SplitSeq(string(body), "\n") {
		if i := strings.LastIndexByte(line, ' '); i > 0 && !strings.HasPrefix(line, "#") {
			out[line[:i]] = line[i+1:]
		}
	}

	return out
}

// waitMetrics waits until /metrics serves the samples of want and no
// series with a label of the value gone, if set. nudge, if set, is called
// now and then meanwhile.
func waitMetrics(t *testing.T, h http.Handler, nudge func(), want map[string]float64, gone string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for i := 0; ; i++ {
		got := samples(t, h)
		ok := true

		for s, v := range want {
			if g, err := strconv.ParseFloat(got[s], 64); err != nil || g != v {
				ok = false
			}
		}

		for s := range got {
			if gone != "" && strings.Contains(s, `="`+gone+`"`) {
				ok = false
			}
		}

		if ok {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("/metrics does not serve %v without %q:\n%v", want, gone, got)
		}

		if nudge != nil && i%10 == 0 {
			nudge()
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// The reconciler, on a fake kopia server and Docker, reports through
// Metrics what /metrics serves: a managed source with its owner, the last
// success of a snapshot kopia server took on schedule, a snapshot of the
// watcher that failed, and, once the container is gone and the cleanup has
// deleted the source, nothing of it.
func TestReconcilerMetrics(t *testing.T) {
	const p = "/data/app"

	fake := backendtest.New()
	src := fake.Source(p)
	labels := map[string]string{"kopia.enable": "true", "kopia.path": p, "kopia.min-interval": "0",
		"kopia.cleanup-on-remove": "all"}
	app := docker.Container{ID: "c1", Name: "app", State: docker.StateRunning, Labels: labels}
	d := &fakeDocker{containers: []docker.Container{app}}

	st, err := state.Open(filepath.Join(t.TempDir(), "state.json"), state.Options{})
	if err != nil {
		t.Fatal(err)
	}

	m := metrics.New()

	r, err := reconcile.New(reconcile.Options{Backend: fake, Docker: d, Store: st, Metrics: m,
		MinBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, EventDelay: time.Millisecond,
		CleanupGrace: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		r.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		_ = st.Close()
	})

	h := metrics.Handler(m, nil)

	r.ReconcileAll()
	waitMetrics(t, h, nil, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`:                1,
		`kopia_watcher_source_owner{container="app",path="/data/app"}`: 1,
		`kopia_watcher_snapshot_failures_total{path="/data/app"}`:      0,
		`kopia_watcher_source_paused{path="/data/app"}`:                0,
	}, "")

	scheduled := time.Date(2026, 10, 23, 3, 0, 0, 0, time.UTC)
	fake.AddSnapshot(src, backend.SnapshotInfo{StartTime: scheduled, EndTime: scheduled.Add(time.Minute)})
	r.ReconcileAll()
	waitMetrics(t, h, nil, map[string]float64{
		`kopia_watcher_last_success_timestamp_seconds{path="/data/app"}`: float64(scheduled.Unix()),
	}, "")

	fake.QueueSnapshotResult(src, backend.SnapshotResult{Outcome: backend.SnapshotFailed, Error: "upload error: boom",
		TaskStart: scheduled, TaskEnd: scheduled.Add(time.Minute)})
	r.HandleEvent(docker.Event{Kind: docker.KindStarted, Action: "start", ContainerID: app.ID, Name: app.Name,
		Labels: labels, Time: time.Now()})
	waitMetrics(t, h, nil, map[string]float64{
		`kopia_watcher_snapshot_failures_total{path="/data/app"}`: 1,
		`kopia_watcher_snapshot_duration_seconds_count`:           0,
	}, "")

	// The pass the waiter queued acts on the result. It lists the
	// containers before, so the removal below is left to the next pass.
	deadline := time.Now().Add(10 * time.Second)

	for rec, _ := st.Get(src); len(rec.SnapshotWaits) > 0; rec, _ = st.Get(src) {
		if time.Now().After(deadline) {
			t.Fatalf("the snapshot wait is left: %+v", rec.SnapshotWaits)
		}

		time.Sleep(5 * time.Millisecond)
	}

	// The container is removed: the source is paused without an owner,
	// and a later pass deletes it once CLEANUP_GRACE, a nanosecond here,
	// has passed since.
	d.set()
	r.HandleEvent(docker.Event{Kind: docker.KindRemoved, Action: "destroy", ContainerID: app.ID, Name: app.Name,
		Labels: labels, Time: time.Now()})
	waitMetrics(t, h, nil, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`: 0,
		`kopia_watcher_managed_sources{state="paused"}`: 1,
		`kopia_watcher_source_paused{path="/data/app"}`: 1,
	}, "app")

	waitMetrics(t, h, r.ReconcileAll, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`: 0,
		`kopia_watcher_managed_sources{state="paused"}`: 0,
	}, p)

	if fake.Known(src) {
		t.Error("the cleanup did not delete the source")
	}
}
