package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody))

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("GET /healthz = %d %q", rec.Code, rec.Body.String())
	}

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	rec = httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/healthz", http.NoBody))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusNotFound {
		t.Errorf("GET / = %d", rec.Code)
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
	ok := &http.Client{Transport: handlerTransport{Handler()}}
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
