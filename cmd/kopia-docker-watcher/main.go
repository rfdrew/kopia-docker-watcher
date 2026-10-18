// Command kopia-docker-watcher manages Kopia snapshot sources from Docker container labels.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/metrics"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const usage = `Usage: kopia-docker-watcher [command]

Commands:
  run          start the watcher (default)
  healthcheck  exit 0 if the running watcher answers on LISTEN_ADDR /healthz
  version      print the version and exit

Configuration is read from environment variables, see docs/configuration.md.
`

const (
	shutdownTimeout    = 5 * time.Second
	healthcheckTimeout = 5 * time.Second
)

// deps are the process's interfaces to the outside world; tests replace them.
type deps struct {
	getenv func(string) string
	stdout io.Writer
	stderr io.Writer
	listen func(network, address string) (net.Listener, error)
	// httpClient is used by the healthcheck subcommand.
	httpClient *http.Client
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], deps{
		getenv: os.Getenv,
		stdout: os.Stdout,
		stderr: os.Stderr,
		listen: net.Listen,
		// The probe goes to loopback; never through HTTP_PROXY.
		httpClient: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: healthcheckTimeout},
	})

	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, d deps) int {
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "run":
		return runWatcher(ctx, d)
	case "healthcheck":
		return runHealthcheck(ctx, d)
	case "version", "--version", "-version":
		_, _ = fmt.Fprintln(d.stdout, version.Version)
		return 0
	case "help", "--help", "-help", "-h":
		_, _ = fmt.Fprint(d.stdout, usage)
		return 0
	default:
		_, _ = fmt.Fprintf(d.stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
}

func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

func runWatcher(ctx context.Context, d deps) int {
	cfg, err := config.Load(d.getenv)
	if err != nil {
		logConfigError(newLogger(d.stderr, config.DefaultLogLevel), err)
		return 1
	}

	log := newLogger(d.stderr, cfg.LogLevel)

	for _, w := range cfg.Warnings {
		log.Warn(w)
	}

	log.Info("starting kopia-docker-watcher", "version", version.Version, "config", cfg)

	kopia, err := kopiaclient.New(kopiaclient.Options{
		URL:                cfg.Kopia.URL,
		Username:           cfg.Kopia.Username,
		Password:           cfg.Kopia.Password,
		CertFingerprint:    cfg.Kopia.CertFingerprint,
		CAFile:             cfg.Kopia.CAFile,
		InsecureSkipVerify: cfg.Kopia.InsecureSkipVerify,
		UserAgent:          version.UserAgent(),
		Logger:             log,
	})
	if err != nil {
		log.Error("cannot create kopia server client", "error", err.Error())
		return 1
	}

	ln, err := d.listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("cannot listen", "addr", cfg.ListenAddr, "error", err.Error())
		return 1
	}

	srv := metrics.NewServer(ln, log)
	serveErr := make(chan error, 1)

	log.Info("serving HTTP", "addr", ln.Addr().String())

	go func() { serveErr <- srv.Serve() }()

	checkKopia(ctx, log, kopia)

	code := 0

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		serveErr = nil

		log.Error("HTTP server failed", "error", err.Error())

		code = 1
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("HTTP server shutdown", "error", err.Error())
	}

	// Nothing started here outlives runWatcher.
	if serveErr != nil {
		if err := <-serveErr; err != nil {
			log.Error("HTTP server failed", "error", err.Error())

			code = 1
		}
	}

	log.Info("stopped")

	return code
}

// checkKopia logs whether kopia server is reachable. Waiting for it with
// retries is part of the reconcile loop.
func checkKopia(ctx context.Context, log *slog.Logger, kopia *kopiaclient.Client) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	st, err := kopia.RepoStatus(ctx)

	switch {
	case errors.Is(err, context.Canceled):
	case err != nil:
		log.Warn("kopia server check failed", "error", err.Error(), "retryable", kopiaclient.IsRetryable(err))
	case !st.Connected:
		log.Warn("kopia server has no connected repository yet")
	default:
		log.Info("connected to kopia server", "hostname", st.Hostname, "username", st.Username, "storage", st.Storage)
	}
}

// runHealthcheck probes /healthz of the watcher running in the same
// container, for the image's HEALTHCHECK: distroless has no curl. Only
// LISTEN_ADDR is read, so a broken configuration elsewhere does not mask
// the result.
func runHealthcheck(ctx context.Context, d deps) int {
	addr := strings.TrimSpace(d.getenv(config.EnvListenAddr))
	if addr == "" {
		addr = config.DefaultListenAddr
	}

	url, err := metrics.HealthcheckURL(addr)
	if err != nil {
		_, _ = fmt.Fprintln(d.stderr, "healthcheck:", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()

	if err := metrics.Probe(ctx, d.httpClient, url); err != nil {
		_, _ = fmt.Fprintln(d.stderr, "healthcheck:", err)
		return 1
	}

	return 0
}

// logConfigError logs every configuration problem on its own line.
func logConfigError(log *slog.Logger, err error) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range joined.Unwrap() {
			log.Error("invalid configuration", "error", e.Error())
		}

		return
	}

	log.Error("invalid configuration", "error", err.Error())
}
