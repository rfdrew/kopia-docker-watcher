// Package metrics serves the watcher's HTTP endpoints on LISTEN_ADDR.
//
// For now only /healthz exists, answering 200 while the process serves
// HTTP; it backs the image's HEALTHCHECK through the healthcheck
// subcommand. Checks of Docker, kopia server and the reconcile loop, and
// /metrics, come later.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// HealthzPath is the health endpoint.
const HealthzPath = "/healthz"

// Handler returns the HTTP handler with all endpoints.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+HealthzPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, `{"status":"ok"}`+"\n")
	})

	return mux
}

// Server serves Handler on a listener.
type Server struct {
	srv *http.Server
	ln  net.Listener
}

// NewServer serves on ln until Shutdown.
func NewServer(ln net.Listener, log *slog.Logger) *Server {
	return &Server{
		srv: &http.Server{
			Handler:           Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       time.Minute,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		},
		ln: ln,
	}
}

// Serve blocks until Shutdown; it returns nil after a shutdown, also when
// Shutdown came first.
func (s *Server) Serve() error {
	if err := s.srv.Serve(s.ln); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}

	return nil
}

// Shutdown stops accepting connections and waits for active requests.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}

	return nil
}

// HealthcheckURL returns the /healthz URL on the loopback interface for a
// LISTEN_ADDR: ":8080" and "0.0.0.0:8080" become 127.0.0.1:8080, "[::]:8080"
// becomes [::1]:8080, a specific address is kept.
func HealthcheckURL(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("invalid listen address %q: %w", listenAddr, err)
	}

	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}

	return "http://" + net.JoinHostPort(host, port) + HealthzPath, nil
}

// Probe requests url and succeeds on 200.
func Probe(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	return nil
}
