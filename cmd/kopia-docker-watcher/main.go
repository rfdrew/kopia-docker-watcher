// Command kopia-docker-watcher manages Kopia snapshot sources from Docker container labels.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const usage = `Usage: kopia-docker-watcher [command]

Commands:
  run        start the watcher (default)
  version    print the version and exit

Configuration is read from environment variables, see docs/configuration.md.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)

	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "run":
		return runWatcher(ctx, getenv, stderr)
	case "version", "--version", "-version":
		_, _ = fmt.Fprintln(stdout, version.Version)
		return 0
	case "help", "--help", "-help", "-h":
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
}

func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

func runWatcher(ctx context.Context, getenv func(string) string, stderr io.Writer) int {
	cfg, err := config.Load(getenv)
	if err != nil {
		logConfigError(newLogger(stderr, config.DefaultLogLevel), err)
		return 1
	}

	log := newLogger(stderr, cfg.LogLevel)

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

	checkKopia(ctx, log, kopia)

	<-ctx.Done()

	log.Info("stopped")

	return 0
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
