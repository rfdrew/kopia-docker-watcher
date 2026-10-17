// Command kopia-docker-watcher manages Kopia snapshot sources from Docker container labels.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rfdrew/kopia-docker-watcher/internal/config"
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

	<-ctx.Done()

	log.Info("stopped")

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
