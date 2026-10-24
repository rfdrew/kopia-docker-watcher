package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/backend/rest"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/control"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/health"
	"github.com/rfdrew/kopia-docker-watcher/internal/kopiaclient"
	"github.com/rfdrew/kopia-docker-watcher/internal/metrics"
	"github.com/rfdrew/kopia-docker-watcher/internal/notify"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

const runUsage = `Usage:
  kopia-docker-watcher run [--strict]
  kopia-docker-watcher run --dry-run [--strict]

Starts the watcher: it waits until kopia server has a repository connected,
then brings kopia server's sources in line with the labels of the Docker
containers, on every container event and every RECONCILE_INTERVAL. It stops
on SIGTERM or SIGINT, giving the policy writes in progress a few seconds to
finish.

With --dry-run it reconciles every path once and exits: it prints what it
would do, the cleanup of abandoned sources included, deletes nothing,
writes nothing to kopia server or to STATE_FILE and serves no HTTP. kopia
server must be ready, it is not waited for. It may run next to a running
watcher.

Options:
  --dry-run  print the plans instead of applying them, then exit
  --strict   treat label warnings as errors

Exit status: 0 after a stop by a signal or a dry run that found no problem,
1 on a failure, a problem found by a dry run or an interrupted dry run, 2
on a usage error.
`

// Startup and shutdown timing. Docker's stop_grace_period defaults to 10 s:
// a policy write in progress gets defaultShutdownGrace, the HTTP server
// shutdownTimeout, and the state is saved in between, so the watcher is
// gone before Docker kills it.
const (
	defaultShutdownGrace = 6 * time.Second
	shutdownTimeout      = 2 * time.Second

	// The delay between attempts to reach kopia server at startup doubles
	// from minKopiaRetry up to maxKopiaRetry.
	minKopiaRetry = time.Second
	maxKopiaRetry = 30 * time.Second
	// kopiaCheckTimeout bounds one attempt.
	kopiaCheckTimeout = 30 * time.Second

	// minStall is how long a pass may run, or paths may wait for one,
	// before /healthz reports the reconciler stuck; twice
	// RECONCILE_INTERVAL when that is longer.
	minStall = 10 * time.Minute
)

// The components of /healthz.
const (
	componentDocker    = "docker"
	componentKopia     = "kopia"
	componentReconcile = "reconcile"
)

// errStopped means the watcher was asked to stop before it was ready.
var errStopped = errors.New("stopped")

func runWatcher(ctx context.Context, args []string, d deps) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(d.stderr)
	fs.Usage = func() {} // printed below: to stdout on request, to stderr on errors

	dryRun := fs.Bool("dry-run", false, "print the plans instead of applying them, then exit")
	strict := fs.Bool("strict", false, "treat label warnings as errors")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprint(d.stdout, runUsage)
			return 0
		}

		_, _ = fmt.Fprintf(d.stderr, "\n%s", runUsage)

		return 2
	}

	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(d.stderr, "run: unexpected argument %q\n\n%s", fs.Arg(0), runUsage)
		return 2
	}

	cfg, err := config.Load(d.getenv)
	if err != nil {
		logConfigError(newLogger(d.stderr, config.DefaultLogLevel), err)
		return 1
	}

	log := newLogger(d.stderr, cfg.LogLevel)

	for _, w := range cfg.Warnings {
		log.Warn(w)
	}

	log.Info("starting kopia-docker-watcher", "version", version.Version, "config", cfg, "dry_run", *dryRun)

	w := &watcher{d: d, cfg: cfg, log: log, dryRun: *dryRun, strict: *strict}

	if *dryRun {
		return w.runOnce(ctx)
	}

	return w.run(ctx)
}

// watcher is one run of the run subcommand.
type watcher struct {
	d      deps
	cfg    *config.Config
	log    *slog.Logger
	dryRun bool
	strict bool
	// metrics receives what the reconciler reports; nil in a dry run,
	// which serves no HTTP.
	metrics *metrics.Metrics
	// health checks the components for /healthz; nil in a dry run.
	health *health.Checker
	// notifier sends the notifications; nil in a dry run and without
	// NOTIFY_WEBHOOK_URL.
	notifier *notify.Notifier
	// control serves the control socket; nil in a dry run and when it
	// cannot be served.
	control *control.Server
}

// run serves /healthz and /metrics, sends the notifications, waits for
// kopia server and reconciles until ctx is done or the HTTP server fails.
func (w *watcher) run(ctx context.Context) int {
	w.metrics = metrics.New()
	w.health = health.New(health.Options{Interval: w.d.healthInterval, Logger: w.log},
		componentDocker, componentKopia, componentReconcile)

	if w.cfg.NotifyWebhookURL != "" {
		n, err := notify.New(notify.Options{
			URL:        w.cfg.NotifyWebhookURL,
			Format:     w.cfg.NotifyWebhookFormat,
			ErrorAfter: w.cfg.NotifyErrorAfter,
			Transport:  w.d.webhookTransport,
			Logger:     w.log,
			Metrics:    w.metrics,
		})
		if err != nil {
			w.log.Error("cannot set up the notifications", "error", err.Error())
			return 1
		}

		w.notifier = n
	}

	ln, err := w.d.listen("tcp", w.cfg.ListenAddr)
	if err != nil {
		w.log.Error("cannot listen", "addr", w.cfg.ListenAddr, "error", err.Error())
		return 1
	}

	srv := metrics.NewServer(ln, w.log, w.metrics, w.health)
	serveErr := make(chan error, 1)

	w.log.Info("serving HTTP", "addr", ln.Addr().String())

	go func() { serveErr <- srv.Serve() }()

	closeControl := w.serveControl()

	// A failed HTTP server stops the watcher like a signal does, with exit
	// status 1.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The health checks stop with the watcher. The notifications stop only
	// once the reconciler has, as its passes may go on for ShutdownGrace
	// and notify.
	var background sync.WaitGroup

	notifyCtx, stopNotify := context.WithCancel(context.WithoutCancel(ctx))
	defer stopNotify()

	background.Go(func() { w.health.Run(ctx) })

	if w.notifier != nil {
		background.Go(func() { w.notifier.Run(notifyCtx) })
	}

	watchDone := make(chan int, 1)

	go func() { watchDone <- w.watch(ctx) }()

	var code int

	select {
	case code = <-watchDone:
	case err := <-serveErr:
		serveErr = nil

		w.log.Error("HTTP server failed", "error", err.Error())
		cancel()

		<-watchDone

		code = 1
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancelShutdown()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		w.log.Warn("HTTP server shutdown", "error", err.Error())
	} else {
		w.log.Info("HTTP server stopped")
	}

	closeControl(shutdownCtx)

	// Nothing started here outlives run.
	if serveErr != nil {
		if err := <-serveErr; err != nil {
			w.log.Error("HTTP server failed", "error", err.Error())

			code = 1
		}
	}

	cancel()
	stopNotify()
	background.Wait()

	w.log.Info("stopped")

	return code
}

// watch waits for kopia server, sets up the reconciler and runs it until
// ctx is done.
func (w *watcher) watch(ctx context.Context) (code int) {
	kopia, err := w.kopiaClient()
	if err != nil {
		w.log.Error("cannot create kopia server client", "error", err.Error())
		return 1
	}

	// Until kopia server is ready, the attempts to reach it are its health
	// checks.
	w.health.Pending(componentDocker, "waiting for kopia server")
	w.health.Pending(componentReconcile, "waiting for kopia server")

	if err := w.waitForKopia(ctx, kopia); err != nil {
		if errors.Is(err, errStopped) {
			return 0
		}

		return 1
	}

	w.health.Set(componentKopia, kopiaCheck(kopia))

	throttled := w.applyThrottle(ctx, kopia)

	store, err := state.Open(w.cfg.StateFile, state.Options{Logger: w.log})
	if err != nil {
		w.log.Error("cannot open the state file", "path", w.cfg.StateFile, "error", err.Error())
		return 1
	}

	defer func() {
		if err := store.Close(); err != nil {
			w.log.Error("cannot save the state", "path", w.cfg.StateFile, "error", err.Error())

			code = 1

			return
		}

		w.log.Info("state saved", "path", w.cfg.StateFile)
	}()

	engine, closer, err := w.d.newDocker()
	if err != nil {
		w.log.Error("cannot create Docker client", "error", err.Error())
		return 1
	}
	defer closer.Close()

	w.health.Set(componentDocker, dockerCheck(engine))

	rec, err := w.reconciler(kopia, store, engine)
	if err != nil {
		w.log.Error("cannot create the reconciler", "error", err.Error())
		return 1
	}

	if w.control != nil {
		w.control.SetHandler(w.controlHandler(rec, kopia))

		defer func() {
			w.control.SetNotReady("the watcher is stopping")
			w.control.SetHandler(nil)
		}()
	}

	stall := max(2*w.cfg.ReconcileInterval, minStall)
	w.health.Set(componentReconcile, func(context.Context) error { return rec.CheckProgress(time.Now(), stall) })

	recDone := make(chan struct{})
	eventsDone := make(chan struct{})

	go func() {
		defer close(recDone)
		rec.Run(ctx)
	}()

	// The watcher's first event is a resync, which starts the first full
	// reconcile.
	go func() {
		defer close(eventsDone)
		docker.NewWatcher(engine, w.log).Run(ctx, rec.HandleEvent)
	}()

	tick, stopTicker := w.ticker(w.cfg.ReconcileInterval)
	defer stopTicker()

	w.log.Info("watching containers", "reconcile_interval", w.cfg.ReconcileInterval.String())

loop:
	for {
		select {
		case <-tick:
			w.log.Debug("starting the periodic full reconcile")
			rec.ReconcileAll()

			if !throttled {
				throttled = w.applyThrottle(ctx, kopia)
			}
		case <-ctx.Done():
			break loop
		}
	}

	start := time.Now()
	w.log.Info("shutting down", "grace", w.shutdownGrace().String())

	<-eventsDone
	w.log.Info("stopped following Docker events")

	<-recDone
	w.log.Info("reconciler stopped", "took", time.Since(start).String())

	return 0
}

// runOnce reconciles every path once without changing anything.
func (w *watcher) runOnce(ctx context.Context) int {
	kopia, err := w.kopiaClient()
	if err != nil {
		w.log.Error("cannot create kopia server client", "error", err.Error())
		return 1
	}

	st, err := w.checkKopia(ctx, kopia)

	switch {
	case ctx.Err() != nil:
		w.log.Warn("dry run interrupted")
		return 1
	case err != nil:
		w.logKopiaError(err)
		return 1
	case !st.Connected:
		w.log.Error("kopia server has no repository connected")
		return 1
	}

	w.logConnected(st)

	store, err := state.Open(w.cfg.StateFile, state.Options{Logger: w.log, ReadOnly: true})
	if err != nil {
		w.log.Error("cannot read the state file", "path", w.cfg.StateFile, "error", err.Error())
		return 1
	}
	defer store.Close()

	engine, closer, err := w.d.newDocker()
	if err != nil {
		w.log.Error("cannot create Docker client", "error", err.Error())
		return 1
	}
	defer closer.Close()

	rec, err := w.reconciler(kopia, store, engine)
	if err != nil {
		w.log.Error("cannot create the reconciler", "error", err.Error())
		return 1
	}

	if err := rec.RunOnce(ctx); ctx.Err() != nil {
		w.log.Warn("dry run interrupted")
		return 1
	} else if err != nil {
		w.log.Error("dry run found problems", "error", err.Error())
		return 1
	}

	w.log.Info("dry run done")

	return 0
}

func (w *watcher) kopiaClient() (*kopiaclient.Client, error) {
	return kopiaclient.New(kopiaclient.Options{
		URL:                w.cfg.Kopia.URL,
		Username:           w.cfg.Kopia.Username,
		Password:           w.cfg.Kopia.Password,
		CertFingerprint:    w.cfg.Kopia.CertFingerprint,
		CAFile:             w.cfg.Kopia.CAFile,
		InsecureSkipVerify: w.cfg.Kopia.InsecureSkipVerify,
		UserAgent:          version.UserAgent(),
		Logger:             w.log,
		Transport:          w.d.kopiaTransport,
	})
}

func (w *watcher) reconciler(kopia *kopiaclient.Client, store *state.Store, engine docker.Engine) (*reconcile.Reconciler, error) {
	var (
		m reconcile.Metrics
		o reconcile.Observer
	)

	switch {
	case w.metrics != nil && w.notifier != nil:
		m, o = teeMetrics{w.metrics, w.notifier}, w.notifier
	case w.metrics != nil:
		m = w.metrics
	}

	return reconcile.New(reconcile.Options{
		Backend:       rest.New(kopia, w.log),
		Docker:        engine,
		Store:         store,
		Logger:        w.log,
		Metrics:       m,
		Observer:      o,
		HostPrefix:    w.cfg.HostPrefix,
		LabelPrefix:   w.cfg.LabelPrefix,
		Strict:        w.strict,
		DryRun:        w.dryRun,
		Output:        w.d.stdout,
		ShutdownGrace: w.shutdownGrace(),

		WatcherPathPrefix:      w.cfg.WatcherPathPrefix,
		SnapshotOnWatcherStart: w.cfg.SnapshotOnWatcherStart,
		CleanupGrace:           w.cfg.CleanupGrace,
		RestoreTaskRunning:     restoreTaskRunning(kopia),
	})
}

// restoreTaskRunning reports whether kopia server's restore task with the
// ID still runs. A task kopia server no longer knows, as after its
// restart, or that is another one under the same ID, has ended.
func restoreTaskRunning(kopia *kopiaclient.Client) func(context.Context, string) (bool, error) {
	return func(ctx context.Context, id string) (bool, error) {
		t, err := kopia.GetTask(ctx, id)

		switch {
		case errors.Is(err, kopiaclient.ErrNotFound):
			return false, nil
		case err != nil:
			return false, err
		}

		return t.Kind == kopiaclient.TaskKindRestore && !t.Status.IsFinished(), nil
	}
}

// waitForKopia asks kopia server for the repository status until it
// reports a connected repository, with a delay that doubles after every
// attempt (bug 6 of the Python version). Every attempt is logged and
// recorded as a health check of kopia server. An error
// retrying cannot fix, such as wrong credentials, is logged and returned at
// once; errStopped means ctx ended first.
func (w *watcher) waitForKopia(ctx context.Context, kopia *kopiaclient.Client) error {
	delay := orDefault(w.d.minKopiaRetry, minKopiaRetry)
	maxDelay := max(orDefault(w.d.maxKopiaRetry, maxKopiaRetry), delay)

	for attempt := 1; ; attempt++ {
		st, err := w.checkKopia(ctx, kopia)

		if ctx.Err() == nil {
			w.health.Record(componentKopia, connectedErr(st, err))
		}

		switch {
		case ctx.Err() != nil:
			return errStopped
		case err == nil && st.Connected:
			w.logConnected(st)
			return nil
		case err == nil:
			w.log.Warn("kopia server has no repository connected yet, waiting",
				"attempt", attempt, "delay", delay.String())
		case kopiaclient.IsRetryable(err):
			w.log.Warn("kopia server is not reachable yet, waiting",
				"attempt", attempt, "delay", delay.String(), "error", err.Error())
		default:
			w.logKopiaError(err)
			return err
		}

		t := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			t.Stop()
			return errStopped
		case <-t.C:
		}

		delay = min(2*delay, maxDelay)
	}
}

// checkKopia makes one attempt to read the repository status.
func (w *watcher) checkKopia(ctx context.Context, kopia *kopiaclient.Client) (*kopiaclient.RepoStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, kopiaCheckTimeout)
	defer cancel()

	return kopia.RepoStatus(ctx)
}

func (w *watcher) logConnected(st *kopiaclient.RepoStatus) {
	attrs := []any{"hostname", st.Hostname, "username", st.Username, "readonly", st.ReadOnly}

	// kopia server reports the storage type only when it opens the
	// repository storage itself. Connected to a repository server it
	// reports that server's URL instead, and an empty storage= in the log
	// would look like a fault.
	if st.Storage != "" {
		attrs = append(attrs, "storage", st.Storage)
	}

	if st.APIServerURL != "" {
		attrs = append(attrs, "api_server_url", st.APIServerURL)
	}

	w.log.Info("connected to kopia server", attrs...)

	if w.notifier != nil {
		w.notifier.SetHost(st.Hostname)
	}
}

// kopiaCheck is the health check of kopia server: it must answer and have
// a repository connected.
func kopiaCheck(kopia *kopiaclient.Client) health.CheckFunc {
	return func(ctx context.Context) error {
		return connectedErr(kopia.RepoStatus(ctx))
	}
}

// connectedErr is the error of a repository status that has no repository
// connected, or err.
func connectedErr(st *kopiaclient.RepoStatus, err error) error {
	switch {
	case err != nil:
		return err
	case !st.Connected:
		return errors.New("kopia server has no repository connected")
	default:
		return nil
	}
}

// pinger is a Docker engine that can ping, such as *docker.Client.
type pinger interface {
	Ping(ctx context.Context) error
}

// dockerCheck is the health check of Docker: a ping, or a listing of the
// containers for an engine that cannot ping.
func dockerCheck(engine docker.Engine) health.CheckFunc {
	if p, ok := engine.(pinger); ok {
		return p.Ping
	}

	return func(ctx context.Context) error {
		_, err := engine.Containers(ctx)
		return err
	}
}

// applyThrottle sets the limits of the KOPIA_THROTTLE_ variables on kopia
// server, if any is set, keeping the others as kopia server has them; with
// none set, it leaves the limits alone, to kopia server or to Terraform.
// It reports false when it failed in a way that the next full reconcile
// may fix.
func (w *watcher) applyThrottle(ctx context.Context, kopia *kopiaclient.Client) (done bool) {
	t := w.cfg.Throttle
	if !t.Set() {
		return true
	}

	ctx, cancel := context.WithTimeout(ctx, kopiaCheckTimeout)
	defer cancel()

	have, err := kopia.Throttle(ctx)
	if err == nil {
		want := mergeThrottle(*have, t)
		if want == *have {
			w.log.Info("kopia server's throttle is as configured", "throttle", want)
			return true
		}

		if err = kopia.SetThrottle(ctx, want); err == nil {
			w.log.Info("set kopia server's throttle", "throttle", want, "was", *have)
			return true
		}
	}

	switch {
	case errors.Is(err, context.Canceled):
		return true
	case errors.Is(err, kopiaclient.ErrNoStorage):
		w.log.Error("cannot set kopia server's throttle: it has no repository storage of its own to limit; "+
			"unset the KOPIA_THROTTLE_ variables", "error", err.Error())

		return true
	case kopiaclient.IsRetryable(err):
		w.log.Warn("cannot set kopia server's throttle; trying again at the next full reconcile", "error", err.Error())
		return false
	default:
		w.log.Error("cannot set kopia server's throttle", "error", err.Error())
		return true
	}
}

// mergeThrottle returns l with the limits set in t.
func mergeThrottle(l kopiaclient.ThrottleLimits, t config.Throttle) kopiaclient.ThrottleLimits {
	set := func(dst *float64, v *float64) {
		if v != nil {
			*dst = *v
		}
	}

	set(&l.UploadBytesPerSecond, t.UploadBytesPerSecond)
	set(&l.DownloadBytesPerSecond, t.DownloadBytesPerSecond)
	set(&l.ReadsPerSecond, t.ReadsPerSecond)
	set(&l.WritesPerSecond, t.WritesPerSecond)
	set(&l.ListsPerSecond, t.ListsPerSecond)

	if t.ConcurrentReads != nil {
		l.ConcurrentReads = *t.ConcurrentReads
	}

	if t.ConcurrentWrites != nil {
		l.ConcurrentWrites = *t.ConcurrentWrites
	}

	return l
}

// teeMetrics reports to the metrics and to the notifier.
type teeMetrics struct {
	m *metrics.Metrics
	n *notify.Notifier
}

func (t teeMetrics) ReconcileError(path string, reason reconcile.Reason) {
	t.m.ReconcileError(path, reason)
	t.n.ReconcileError(path, reason)
}

func (t teeMetrics) Source(path string, s reconcile.SourceMetrics) {
	t.m.Source(path, s)
	t.n.Source(path, s)
}

func (t teeMetrics) LastSuccess(path string, start time.Time) {
	t.m.LastSuccess(path, start)
	t.n.LastSuccess(path, start)
}

func (t teeMetrics) ForgetSource(path string) {
	t.m.ForgetSource(path)
	t.n.ForgetSource(path)
}

func (t teeMetrics) SnapshotFinished(path string, res backend.SnapshotResult) {
	t.m.SnapshotFinished(path, res)
	t.n.SnapshotFinished(path, res)
}

func (t teeMetrics) HookFinished(path string, res reconcile.HookResult) {
	t.m.HookFinished(path, res)
	t.n.HookFinished(path, res)
}

func (t teeMetrics) ContainerReturned(path string, res reconcile.ReturnResult) {
	t.m.ContainerReturned(path, res)
	t.n.ContainerReturned(path, res)
}

// logKopiaError explains an error of kopia server that retrying cannot fix.
func (w *watcher) logKopiaError(err error) {
	var msg string

	switch {
	case errors.Is(err, kopiaclient.ErrUIDisabled):
		msg = "kopia server does not serve its UI API, which the watcher uses: " +
			"start kopia server without --no-ui, and check that KOPIA_SERVER_URL points at it"
	case errors.Is(err, kopiaclient.ErrUnauthorized):
		msg = "kopia server rejected the credentials: check " + config.EnvKopiaServerUsername +
			" and " + config.EnvKopiaServerPassword
	case errors.Is(err, kopiaclient.ErrAccessDenied):
		msg = "kopia server denied access: " + config.EnvKopiaServerUsername +
			" must be the user of kopia server's --server-username; users added with kopia server user add cannot use the UI API"
	default:
		msg = "cannot use kopia server"
	}

	w.log.Error(msg, "error", err.Error())
}

func (w *watcher) shutdownGrace() time.Duration {
	return orDefault(w.d.shutdownGrace, defaultShutdownGrace)
}

// orDefault returns d, or def when d is zero or negative.
func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}

	return d
}

// ticker returns a channel that ticks every d and a function that stops it.
func (w *watcher) ticker(d time.Duration) (<-chan time.Time, func()) {
	if w.d.newTicker != nil {
		return w.d.newTicker(d)
	}

	t := time.NewTicker(d)

	return t.C, t.Stop
}
