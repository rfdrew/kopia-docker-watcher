package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(New(), nil).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody))

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %q", rec.Code, rec.Body.String())
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	rec = httptest.NewRecorder()
	Handler(New(), nil).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/healthz", http.NoBody))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	Handler(New(), nil).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusNotFound {
		t.Errorf("GET / = %d", rec.Code)
	}
}

// /healthz serves the health handler it is given, 503 included.
func TestHealthzHandler(t *testing.T) {
	healthz := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"status":"unhealthy"}`, http.StatusServiceUnavailable)
	})

	rec := httptest.NewRecorder()
	Handler(New(), healthz).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody))

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "unhealthy") {
		t.Fatalf("GET /healthz = %d %q", rec.Code, rec.Body.String())
	}
}

// /metrics serves the metrics in the Prometheus text format, to GET only.
func TestMetricsEndpoint(t *testing.T) {
	m := New()
	m.Source("/data/app", reconcile.SourceMetrics{Owners: []string{"app"}})
	m.LastSuccess("/data/app", time.Unix(1792731600, 0))

	rec := httptest.NewRecorder()
	Handler(m, nil).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d %q", rec.Code, rec.Body.String())
	}

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type = %q", ct)
	}

	body := rec.Body.String()

	for _, want := range []string{
		"# HELP kopia_watcher_last_success_timestamp_seconds Start of the last complete snapshot",
		"# TYPE kopia_watcher_last_success_timestamp_seconds gauge",
		"# TYPE kopia_watcher_snapshot_duration_seconds histogram",
		"# TYPE kopia_watcher_snapshot_failures_total counter",
		"# TYPE kopia_watcher_reconcile_errors_total counter",
		"# TYPE kopia_watcher_managed_sources gauge",
		"# TYPE kopia_watcher_source_paused gauge",
		"# TYPE kopia_watcher_source_owner gauge",
		"\nkopia_watcher_last_success_timestamp_seconds{path=\"/data/app\"} 1.7927316e+09\n",
		"\nkopia_watcher_managed_sources{state=\"active\"} 1\n",
		"\nkopia_watcher_source_owner{container=\"app\",path=\"/data/app\"} 1\n",
		"\nkopia_watcher_snapshot_failures_total{path=\"/data/app\"} 0\n",
		"\ngo_goroutines ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no %q in\n%s", want, body)
		}
	}

	rec = httptest.NewRecorder()
	Handler(m, nil).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/metrics", http.NoBody))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics = %d", rec.Code)
	}
}

func TestHealthcheckURL(t *testing.T) {
	for in, want := range map[string]string{
		":8080":          "http://127.0.0.1:8080/healthz",
		"0.0.0.0:9000":   "http://127.0.0.1:9000/healthz",
		"[::]:8080":      "http://[::1]:8080/healthz",
		"10.0.0.5:8080":  "http://10.0.0.5:8080/healthz",
		"localhost:8080": "http://localhost:8080/healthz",
	} {
		got, err := HealthcheckURL(in)
		if err != nil || got != want {
			t.Errorf("HealthcheckURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}

	if _, err := HealthcheckURL("8080"); err == nil {
		t.Error("expected error for an address without port")
	}
}

type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, req)

	return rec.Result(), nil
}

func TestProbe(t *testing.T) {
	ok := &http.Client{Transport: handlerTransport{Handler(New(), nil)}}
	if err := Probe(t.Context(), ok, "http://127.0.0.1:8080/healthz"); err != nil {
		t.Fatal(err)
	}

	failing := &http.Client{Transport: handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
	})}}

	err := Probe(t.Context(), failing, "http://127.0.0.1:8080/healthz")
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want 503", err)
	}
}
