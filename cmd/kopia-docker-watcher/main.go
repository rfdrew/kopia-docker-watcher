// Command kopia-docker-watcher manages Kopia snapshot sources from Docker container labels.
package main

import (
	"context"
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
	"github.com/rfdrew/kopia-docker-watcher/internal/control"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/metrics"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const usage = `Usage: kopia-docker-watcher [command]

Commands:
  run          start the watcher (default), see "run --help"
  list         list the sources the running watcher manages, see "list --help"
  trigger      take a snapshot through the running watcher, see "trigger --help"
  restore      restore a snapshot through the running watcher, see "restore --help"
  healthcheck  exit 0 if the running watcher answers 200 on LISTEN_ADDR /healthz
  validate     check container labels, see "validate --help"
  version      print the version and exit

list, trigger and restore ask the watcher that runs in the same container,
through CONTROL_SOCKET: run them with docker exec.

Configuration is read from environment variables, see docs/configuration.md.
`

const healthcheckTimeout = 5 * time.Second

// deps are the process's interfaces to the outside world; tests replace them.
type deps struct {
	getenv func(string) string
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	listen func(network, address string) (net.Listener, error)
	// httpClient is used by the healthcheck subcommand.
	httpClient *http.Client
	// newDocker connects to the Docker Engine, for run and validate.
	newDocker func() (docker.Engine, io.Closer, error)
	// kopiaTransport replaces the HTTP transport of the kopia server client
	// of run and validate; nil means the default one.
	kopiaTransport http.RoundTripper
	// webhookTransport replaces the HTTP transport of the notifications of
	// run; nil means the default one.
	webhookTransport http.RoundTripper
	// listenControl serves the control socket of run at a path; nil means
	// control.Listen. dialControl connects list, trigger and restore to
	// it; nil means control.DialUnix.
	listenControl func(path string) (*control.Server, error)
	dialControl   control.Dialer

	// Timing of run; zero means the default.
	minKopiaRetry, maxKopiaRetry time.Duration
	shutdownGrace                time.Duration
	// healthInterval is how often the health checks run.
	healthInterval time.Duration
	// newTicker replaces time.NewTicker for the full reconciles of run.
	newTicker func(time.Duration) (<-chan time.Time, func())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], deps{
		getenv: os.Getenv,
		stdin:  os.Stdin,
		stdout: os.Stdout,
		stderr: os.Stderr,
		listen: net.Listen,
		// The probe goes to loopback; never through HTTP_PROXY.
		httpClient: &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: healthcheckTimeout},
		newDocker:  newDocker,
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
		return runWatcher(ctx, tail(args), d)
	case "list":
		return runList(ctx, args[1:], d)
	case "trigger":
		return runTrigger(ctx, args[1:], d)
	case "restore":
		return runRestore(ctx, args[1:], d)
	case "healthcheck":
		return runHealthcheck(ctx, d)
	case "validate":
		return runValidate(ctx, args[1:], d)
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

// tail returns the arguments after the command, if any.
func tail(args []string) []string {
	if len(args) == 0 {
		return nil
	}

	return args[1:]
}

func newDocker() (docker.Engine, io.Closer, error) {
	c, err := docker.New(version.UserAgent())
	if err != nil {
		return nil, nil, err
	}

	return c, c, nil
}

func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
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
