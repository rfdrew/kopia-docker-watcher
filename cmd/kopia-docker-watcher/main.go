// Command kopia-docker-watcher manages Kopia snapshot sources from Docker container labels.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const usage = `Usage: kopia-docker-watcher [command]

Commands:
  run        start the watcher (default)
  version    print the version and exit
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "run":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		return runWatcher(ctx, stderr)
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

func runWatcher(ctx context.Context, stderr io.Writer) int {
	_, _ = fmt.Fprintln(stderr, "kopia-docker-watcher", version.Version, "started")

	<-ctx.Done()

	return 0
}
