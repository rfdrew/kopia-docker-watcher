package metrics

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

// scrape returns the samples /metrics serves by series, as the text format
// writes them: kopia_watcher_source_owner{container="app",path="/data/app"}.
func scrape(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()

	rec := httptest.NewRecorder()
	Handler(m).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, MetricsPath, http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d %s", rec.Code, rec.Body)
	}

	out := map[string]float64{}

	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		i := strings.LastIndexByte(line, ' ')

		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			t.Fatalf("sample %q: %v", line, err)
		}

		out[line[:i]] = v
	}

	return out
}

// watcherSeries returns the series of the watcher's own metrics with a
// label naming path.
func watcherSeries(samples map[string]float64, path string) []string {
	var out []string

	for _, s := range slices.Sorted(maps.Keys(samples)) {
		if strings.HasPrefix(s, namespace+"_") && strings.Contains(s, `path="`+path+`"`) {
			out = append(out, s)
		}
	}

	return out
}

func assertSamples(t *testing.T, got map[string]float64, want map[string]float64) {
	t.Helper()

	for s, v := range want {
		if g, ok := got[s]; !ok || g != v {
			t.Errorf("%s = %v (present %t), want %v", s, g, ok, v)
		}
	}
}

func assertAbsent(t *testing.T, got map[string]float64, series ...string) {
	t.Helper()

	for _, s := range series {
		if v, ok := got[s]; ok {
			t.Errorf("%s = %v, want no series", s, v)
		}
	}
}

// A new Metrics has no source and every kind of reconcile error at zero,
// besides the version and the metrics of the Go runtime.
func TestNew(t *testing.T) {
	got := scrape(t, New())

	want := map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`: 0,
		`kopia_watcher_managed_sources{state="paused"}`: 0,
		`kopia_watcher_build_info{version="dev"}`:       1,
		`kopia_watcher_snapshot_duration_seconds_count`: 0,
	}

	for _, r := range reconcile.Reasons() {
		want[`kopia_watcher_reconcile_errors_total{kind="`+string(r)+`"}`] = 0
	}

	assertSamples(t, got, want)

	if _, ok := got["go_goroutines"]; !ok {
		t.Error("no go_goroutines")
	}

	for s := range got {
		if strings.Contains(s, "path=") {
			t.Errorf("a series of a source: %s", s)
		}
	}
}

// Source counts a source as managed, active or paused, with a series per
// owner and its failures at zero; a later report replaces the owners and
// the state.
func TestSource(t *testing.T) {
	m := New()
	m.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"app", "worker"}})
	m.Source("/data/db", reconcile.SourceMetrics{Paused: true, Owners: []string{"db"}})

	got := scrape(t, m)
	assertSamples(t, got, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`:                   1,
		`kopia_watcher_managed_sources{state="paused"}`:                   1,
		`kopia_watcher_source_owner{container="app",path="/data/app"}`:    1,
		`kopia_watcher_source_owner{container="worker",path="/data/app"}`: 1,
		`kopia_watcher_source_owner{container="db",path="/data/db"}`:      1,
		`kopia_watcher_snapshot_failures_total{path="/data/app"}`:         0,
		`kopia_watcher_snapshot_failures_total{path="/data/db"}`:          0,
		`kopia_watcher_source_paused{path="/data/app"}`:                   0,
		`kopia_watcher_source_paused{path="/data/db"}`:                    1,
	})
	assertAbsent(t, got, `kopia_watcher_last_success_timestamp_seconds{path="/data/app"}`)

	// The app stops for good and its worker is replaced by one of another
	// name.
	m.Source("/data/app", reconcile.SourceMetrics{Paused: true, Owners: []string{"app", "worker-2"}})

	got = scrape(t, m)
	assertSamples(t, got, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`:                     0,
		`kopia_watcher_managed_sources{state="paused"}`:                     2,
		`kopia_watcher_source_owner{container="app",path="/data/app"}`:      1,
		`kopia_watcher_source_owner{container="worker-2",path="/data/app"}`: 1,
		`kopia_watcher_source_paused{path="/data/app"}`:                     1,
	})
	assertAbsent(t, got, `kopia_watcher_source_owner{container="worker",path="/data/app"}`)

	// All containers are gone.
	m.Source("/data/app", reconcile.SourceMetrics{Paused: true})

	if s := watcherSeries(scrape(t, m), "/data/app"); !slices.Equal(s, []string{
		`kopia_watcher_snapshot_failures_total{path="/data/app"}`, `kopia_watcher_source_paused{path="/data/app"}`,
	}) {
		t.Errorf("series of /data/app = %q", s)
	}
}

// LastSuccess sets the start of the last complete snapshot in seconds;
// zero removes the series, and a source not reported as managed gets none.
func TestLastSuccess(t *testing.T) {
	m := New()
	m.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"app"}})

	start := time.Date(2026, 10, 23, 3, 0, 0, 500_000_000, time.UTC)
	m.LastSuccess("/data/app", start)
	m.LastSuccess("/data/other", start)

	got := scrape(t, m)
	assertSamples(t, got, map[string]float64{
		`kopia_watcher_last_success_timestamp_seconds{path="/data/app"}`: float64(start.Unix()) + 0.5,
	})
	assertAbsent(t, got, `kopia_watcher_last_success_timestamp_seconds{path="/data/other"}`)

	m.LastSuccess("/data/app", time.Time{})
	assertAbsent(t, scrape(t, m), `kopia_watcher_last_success_timestamp_seconds{path="/data/app"}`)
}

// ForgetSource removes every series of the source and leaves the others.
func TestForgetSource(t *testing.T) {
	m := New()
	start := time.Date(2026, 10, 23, 3, 0, 0, 0, time.UTC)

	for _, p := range []string{"/data/app", "/data/db"} {
		m.Source(p, reconcile.SourceMetrics{Owners: []string{"app", "db"}})
		m.LastSuccess(p, start)
		m.SnapshotFinished(p, backend.SnapshotResult{Outcome: backend.SnapshotFailed})
	}

	m.ForgetSource("/data/app")
	m.ForgetSource("/data/never-reported")

	got := scrape(t, m)

	if s := watcherSeries(got, "/data/app"); len(s) != 0 {
		t.Errorf("series left of a forgotten source: %q", s)
	}

	if s := watcherSeries(got, "/data/db"); len(s) != 5 {
		t.Errorf("series of /data/db = %q", s)
	}

	assertSamples(t, got, map[string]float64{
		`kopia_watcher_managed_sources{state="active"}`:          1,
		`kopia_watcher_snapshot_failures_total{path="/data/db"}`: 1,
	})
}

// A snapshot that saved no complete snapshot counts as a failure of its
// source; the duration of its task goes into the histogram if it ran to
// the end.
func TestSnapshotFinished(t *testing.T) {
	start := time.Date(2026, 10, 23, 3, 0, 0, 0, time.UTC)
	task := func(o backend.SnapshotOutcome, d time.Duration) backend.SnapshotResult {
		return backend.SnapshotResult{Outcome: o, TaskStart: start, TaskEnd: start.Add(d)}
	}

	tests := []struct {
		name     string
		res      backend.SnapshotResult
		failures float64
		// observed is the duration observed, negative for none.
		observed float64
	}{
		{"succeeded", task(backend.SnapshotSucceeded, 90*time.Second), 0, 90},
		{"identical", task(backend.SnapshotIdentical, 2*time.Second), 0, 2},
		{"incomplete", task(backend.SnapshotIncomplete, time.Hour), 1, 3600},
		{"failed", task(backend.SnapshotFailed, time.Minute), 1, -1},
		{"canceled", task(backend.SnapshotCanceled, time.Minute), 1, -1},
		{"unknown", backend.SnapshotResult{Outcome: backend.SnapshotUnknown}, 1, -1},
		{"unknown with its task", task(backend.SnapshotUnknown, time.Minute), 1, -1},
		{"succeeded without its task", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded}, 0, -1},
		{"ended before it started", task(backend.SnapshotSucceeded, -time.Second), 0, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New()
			m.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"app"}})
			m.SnapshotFinished("/data/app", tt.res)

			got := scrape(t, m)
			want := map[string]float64{`kopia_watcher_snapshot_failures_total{path="/data/app"}`: tt.failures}

			if tt.observed < 0 {
				want[`kopia_watcher_snapshot_duration_seconds_count`] = 0
			} else {
				want[`kopia_watcher_snapshot_duration_seconds_count`] = 1
				want[`kopia_watcher_snapshot_duration_seconds_sum`] = tt.observed
			}

			assertSamples(t, got, want)
		})
	}
}

// The buckets of the histogram cover seconds to hours.
func TestSnapshotDurationBuckets(t *testing.T) {
	m := New()
	start := time.Date(2026, 10, 23, 3, 0, 0, 0, time.UTC)

	for _, d := range []time.Duration{3 * time.Second, 10 * time.Minute, 3 * time.Hour, 10 * time.Hour} {
		m.SnapshotFinished("/data/app", backend.SnapshotResult{Outcome: backend.SnapshotSucceeded, TaskStart: start, TaskEnd: start.Add(d)})
	}

	assertSamples(t, scrape(t, m), map[string]float64{
		`kopia_watcher_snapshot_duration_seconds_bucket{le="1"}`:     0,
		`kopia_watcher_snapshot_duration_seconds_bucket{le="5"}`:     1,
		`kopia_watcher_snapshot_duration_seconds_bucket{le="900"}`:   2,
		`kopia_watcher_snapshot_duration_seconds_bucket{le="14400"}`: 3,
		`kopia_watcher_snapshot_duration_seconds_bucket{le="28800"}`: 3,
		`kopia_watcher_snapshot_duration_seconds_bucket{le="+Inf"}`:  4,
	})
}

// Reconcile errors count by kind; the path is no label.
func TestReconcileError(t *testing.T) {
	m := New()
	m.ReconcileError("/data/app", reconcile.ReasonConfig)
	m.ReconcileError("/data/db", reconcile.ReasonConfig)
	m.ReconcileError("", reconcile.ReasonLabels)

	got := scrape(t, m)
	assertSamples(t, got, map[string]float64{
		`kopia_watcher_reconcile_errors_total{kind="config"}`:    2,
		`kopia_watcher_reconcile_errors_total{kind="labels"}`:    1,
		`kopia_watcher_reconcile_errors_total{kind="temporary"}`: 0,
	})

	for s := range got {
		if strings.HasPrefix(s, "kopia_watcher_reconcile_errors_total") && strings.Contains(s, "path=") {
			t.Errorf("a reconcile error with a path: %s", s)
		}
	}
}
