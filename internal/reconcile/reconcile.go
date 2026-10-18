// Package reconcile brings kopia server's sources in line with the labels
// of Docker containers.
//
// # Desired state
//
// The unit of work is a source path: a path of a container's path label,
// joined to HOST_PREFIX and cleaned (labels.SourcePath). A path that is not
// absolute after that, or leads out of HOST_PREFIX, is a label error and
// makes no source. The owners of a path are all containers whose enable
// label is true and whose path label names it, stopped ones included. From
// them the reconciler works out the desired state of the path's source:
//
//   - paused when no owner runs. docker.State.Running decides, so an owner
//     that is restarting after a die, as in a crash loop, still runs and
//     the pause does not flap;
//   - the policy patch the labels ask for: those of the running owners, or
//     of all owners when none runs. A stopped container keeps the labels it
//     was created with, so a leftover one (an old one-off container of
//     docker compose run, the old version of a blue/green deploy) does not
//     hold back the labels of the containers that run. While docker compose
//     recreates a container, the new one is only created until the old one
//     has stopped, so the old labels stay until then;
//   - the names of all owners, sorted.
//
// The owners that decide the patch have to agree on it. When they do not,
// or when the labels of one of them cannot be applied, the source keeps
// the last applied patch while the pause still follows the owners. With a
// running owner that is an error, logged and counted. When all owners are
// stopped it is only a warning: the source is paused, only its saved
// section would change, and the labels of whichever owner starts first
// decide then. A path that has lost all its owners (the
// containers were removed, or no longer enable the backup) is paused with
// its last applied patch if the watcher manages it; a path the watcher
// never managed is left alone. A source kopia server does not know is
// registered only with the labels of a running owner: never in these
// cases, and not while its containers were only created, when their bind
// mounts may not exist yet.
//
// # Passes
//
// Paths wait in a queue, each at most once, and every pass handles one
// path; passes over different paths run at the same time, but never two
// over the same path. A Docker event only queues the paths of its
// container (HandleEvent); ReconcileAll queues every path of every
// container and of every source in the state. A pass reads all containers
// afresh, so it acts on their state, not on the event, and a container
// recreated with new labels simply converges.
//
// A pass plans with Backend.PlanSource from the desired state and what the
// state store says was applied, checks a new schedule with
// PreviewSchedule, stores the plan's intent, applies the plan with
// ApplyPlan and stores the result, as package backend documents. With
// DryRun it writes nothing, neither to kopia server nor to the state: it
// logs and prints what it would do for every path, including the paths it
// would not register and the passes that fail.
//
// # Errors
//
// A temporary error of kopia server or Docker retries the path with an
// exponential delay, which a full reconcile does not cut short. A
// configuration error (kopia server rejects the schedule, the policy or
// the path) is not retried: the path is tried again when its labels change
// and at every full reconcile. Such an error, and a problem with the
// labels, is logged at error level and counted once; while the next passes
// over the path find it again, as those of a crash-looping owner do, it is
// only logged at debug level, until the next full reconcile reports it
// again. A path with a restarting owner is looked at again after
// RestartRecheck, since a restarting container can end up exited without
// any event.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/config"
	"github.com/rfdrew/kopia-docker-watcher/internal/docker"
	"github.com/rfdrew/kopia-docker-watcher/internal/state"
)

// Defaults of Options.
const (
	DefaultWorkers    = 4
	DefaultMinBackoff = time.Second
	DefaultMaxBackoff = 5 * time.Minute
	// DefaultEventDelay lets the events of one change settle. It counts
	// from the first event that queues a path, so it is no debounce: when
	// docker compose recreates a container whose old process takes long to
	// stop, the pass may come in between and see both containers, which
	// the rule that running owners decide the policy keeps harmless.
	DefaultEventDelay = 2 * time.Second
	// DefaultRestartRecheck is how soon a path with a restarting owner is
	// looked at again.
	DefaultRestartRecheck = docker.MaxRestartDelay

	// maxPending bounds the events kept per path between passes, as for a
	// crash-looping container while its path waits for a retry.
	maxPending = 32

	// dockerTimeout bounds a container listing: the Docker client sets no
	// timeout of its own, and a daemon that hangs, say on a container stuck
	// in removal, would hold every worker.
	dockerTimeout = time.Minute
)

// Docker lists the containers; docker.Engine implements it.
type Docker interface {
	Containers(ctx context.Context) ([]docker.Container, error)
}

// Store keeps what was applied to each source; *state.Store implements it.
type Store interface {
	Get(src backend.Source) (state.Record, bool)
	Update(src backend.Source, fn func(r *state.Record) error) error
	Delete(src backend.Source) error
	Sources() []backend.Source
}

// Reason says why a reconcile did not go as the labels ask.
type Reason string

// Reasons.
const (
	// ReasonLabels: a container's labels cannot be applied.
	ReasonLabels Reason = "labels"
	// ReasonOwnersDisagree: owners of a path ask for different policies.
	ReasonOwnersDisagree Reason = "owners_disagree"
	// ReasonConfig: kopia server rejects what the labels ask for.
	ReasonConfig Reason = "config"
	// ReasonTemporary: kopia server failed; the path is retried.
	ReasonTemporary Reason = "temporary"
	// ReasonUnavailable: kopia server cannot be reached or has no
	// repository connected; the path is retried.
	ReasonUnavailable Reason = "unavailable"
	// ReasonFailed: kopia server failed in a way that does not go away by
	// itself, such as wrong credentials.
	ReasonFailed Reason = "failed"
	// ReasonDocker: the containers could not be listed.
	ReasonDocker Reason = "docker"
	// ReasonState: the state store refused a change.
	ReasonState Reason = "state"
)

// Metrics counts what goes wrong. The no-op default stands in until the
// watcher serves /metrics.
type Metrics interface {
	// ReconcileError counts one problem with a path. The path is empty for
	// a problem that is not about one source: the label errors of a
	// container, counted once per container and set of labels, and a full
	// reconcile that cannot list the containers.
	ReconcileError(path string, reason Reason)
}

type noMetrics struct{}

func (noMetrics) ReconcileError(string, Reason) {}

// Options configure a Reconciler.
type Options struct {
	Backend backend.Backend
	Docker  Docker
	Store   Store

	// Logger receives the log; nil discards it.
	Logger *slog.Logger
	// Metrics counts problems; nil counts nothing.
	Metrics Metrics

	// HostPrefix is HOST_PREFIX: empty or a clean absolute path.
	HostPrefix string
	// LabelPrefix is LABEL_PREFIX; empty means config.DefaultLabelPrefix.
	LabelPrefix string
	// Strict makes every label warning an error.
	Strict bool

	// DryRun logs every plan and prints it to Output instead of applying
	// it, and writes nothing to the state.
	DryRun bool
	// Output receives the plans of a dry run; nil only logs them.
	Output io.Writer

	// Workers is how many paths are reconciled at the same time; zero
	// means DefaultWorkers.
	Workers int
	// MinBackoff and MaxBackoff bound the delay before a path is retried
	// after a temporary error; zero means the default.
	MinBackoff, MaxBackoff time.Duration
	// EventDelay is how long after an event its paths are reconciled; zero
	// means DefaultEventDelay.
	EventDelay time.Duration
	// RestartRecheck is how soon a path with a restarting owner is
	// reconciled again; zero means DefaultRestartRecheck.
	RestartRecheck time.Duration
	// ShutdownGrace is how long passes that are running when Run's context
	// ends may go on before they are canceled; zero cancels them at once.
	// A canceled pass loses nothing, since the intent is stored before a
	// policy is written, but the next start writes the policy again.
	ShutdownGrace time.Duration
}

// Reconciler reconciles source paths. Create it with New, start it with
// Run.
type Reconciler struct {
	backend backend.Backend
	docker  Docker
	store   Store
	log     *slog.Logger
	metrics Metrics

	hostPrefix  string
	labelPrefix string
	strict      bool
	dryRun      bool
	output      io.Writer
	printMu     sync.Mutex

	workers                int
	minBackoff, maxBackoff time.Duration
	eventDelay             time.Duration
	restartRecheck         time.Duration
	shutdownGrace          time.Duration

	queue   *queue
	scan    chan struct{}
	running atomic.Bool

	mu sync.Mutex
	// failures counts the temporary errors in a row per path.
	failures map[string]int
	// identity is kopia server's user name and host; known is false until
	// asked and after ErrWrongIdentity.
	identity identity
	// reported holds what was last reported about each container's labels;
	// reports counts the changes to it.
	reported map[string]labelReport
	reports  uint64
	// pending holds per path the events that queued it since its last
	// pass began, at most maxPending, the latest ones.
	pending map[string][]docker.Event
	// reportedErrors holds per path the errors its last pass reported,
	// with the number of the full reconcile they were last reported after.
	reportedErrors map[string]map[string]uint64
	// scans counts full reconciles.
	scans uint64

	// dockerTimeout bounds every container listing.
	dockerTimeout time.Duration

	// afterPass, if set, is called after every pass of Run; for tests.
	afterPass func(path string, res passResult)
}

// labelReport is what was reported about a container's labels.
type labelReport struct {
	key string
	// seq is the value of Reconciler.reports when it was reported.
	seq uint64
	// gone marks a container that was removed; its labels are not reported
	// again.
	gone bool
}

type identity struct {
	userName, host string
	known          bool
}

// New returns a Reconciler.
func New(opts Options) (*Reconciler, error) {
	switch {
	case opts.Backend == nil:
		return nil, errors.New("reconcile: no backend")
	case opts.Docker == nil:
		return nil, errors.New("reconcile: no Docker")
	case opts.Store == nil:
		return nil, errors.New("reconcile: no state store")
	}

	if opts.HostPrefix != "" && (!path.IsAbs(opts.HostPrefix) || path.Clean(opts.HostPrefix) != opts.HostPrefix) {
		return nil, fmt.Errorf("reconcile: host prefix %q is not a clean absolute path", opts.HostPrefix)
	}

	// A prefix of / is no prefix, as config.ParseHostPrefix has it.
	if opts.HostPrefix == "/" {
		opts.HostPrefix = ""
	}

	r := &Reconciler{
		backend:        opts.Backend,
		docker:         opts.Docker,
		store:          opts.Store,
		log:            opts.Logger,
		metrics:        opts.Metrics,
		hostPrefix:     opts.HostPrefix,
		labelPrefix:    opts.LabelPrefix,
		strict:         opts.Strict,
		dryRun:         opts.DryRun,
		output:         opts.Output,
		workers:        orDefault(opts.Workers, DefaultWorkers),
		minBackoff:     orDefault(opts.MinBackoff, DefaultMinBackoff),
		maxBackoff:     orDefault(opts.MaxBackoff, DefaultMaxBackoff),
		eventDelay:     orDefault(opts.EventDelay, DefaultEventDelay),
		restartRecheck: orDefault(opts.RestartRecheck, DefaultRestartRecheck),
		shutdownGrace:  max(opts.ShutdownGrace, 0),
		queue:          newQueue(),
		scan:           make(chan struct{}, 1),
		failures:       map[string]int{},
		reported:       map[string]labelReport{},
		pending:        map[string][]docker.Event{},
		reportedErrors: map[string]map[string]uint64{},
		dockerTimeout:  dockerTimeout,
	}

	if r.labelPrefix == "" {
		r.labelPrefix = config.DefaultLabelPrefix
	}

	if r.log == nil {
		r.log = slog.New(slog.DiscardHandler)
	}

	if r.metrics == nil {
		r.metrics = noMetrics{}
	}

	r.maxBackoff = max(r.maxBackoff, r.minBackoff)

	return r, nil
}

// orDefault returns v, or def when v is zero or negative.
func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}

	return v
}

// Run reconciles queued paths until ctx is done, with Workers passes at a
// time, and scans for every path whenever ReconcileAll asks. It does not
// scan by itself: docker.Watcher sends KindResync first, which HandleEvent
// turns into ReconcileAll.
//
// When ctx is done, Run takes no more paths and gives the passes that are
// running ShutdownGrace to finish. It returns once every goroutine it
// started has returned; after that it calls neither the store nor the
// backend, so the caller may close them. Run must be called once.
func (r *Reconciler) Run(ctx context.Context) {
	if !r.running.CompareAndSwap(false, true) {
		panic("reconcile: Run called twice or along with RunOnce")
	}

	// Passes run on their own context, canceled ShutdownGrace after ctx.
	passCtx, cancelPasses := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelPasses()

	var wg sync.WaitGroup

	for range r.workers {
		wg.Go(func() { r.work(ctx, passCtx) })
	}

	wg.Go(func() { r.scanLoop(ctx) })

	<-ctx.Done()
	r.queue.close()

	grace := time.AfterFunc(r.shutdownGrace, cancelPasses)
	defer grace.Stop()

	wg.Wait()
}

// work takes paths until ctx is done and runs their passes with passCtx.
func (r *Reconciler) work(ctx, passCtx context.Context) {
	for {
		p, ok := r.queue.get(ctx)
		if !ok {
			return
		}

		res := r.reconcile(passCtx, p)
		r.schedule(ctx, p, res)

		if r.afterPass != nil {
			r.afterPass(p, res)
		}

		r.queue.done(p)
	}
}

// schedule queues p again after a pass as its result asks.
func (r *Reconciler) schedule(ctx context.Context, p string, res passResult) {
	if ctx.Err() != nil {
		return
	}

	r.mu.Lock()

	var delay time.Duration

	if res.retry {
		r.failures[p]++
		delay = backoff(r.minBackoff, r.maxBackoff, r.failures[p])
	} else {
		delete(r.failures, p)
	}

	r.mu.Unlock()

	if res.retry {
		r.log.Debug("retrying the path later", "path", p, "delay", delay.String())
		r.queue.retryAfter(p, delay)
	}

	// A pending retry comes first; it looks at the restarting owner too.
	if res.restarting {
		r.queue.addAfter(p, r.restartRecheck)
	}
}

// backoff returns the delay before retry n (1 for the first): min doubled
// for each earlier retry, at most max.
func backoff(minDelay, maxDelay time.Duration, n int) time.Duration {
	d := minDelay

	for i := 1; i < n && d < maxDelay; i++ {
		d *= 2
	}

	return min(d, maxDelay)
}

// Enqueue queues source paths for a pass now. A path that is not absolute
// and clean is ignored with a warning.
func (r *Reconciler) Enqueue(paths ...string) {
	for _, p := range paths {
		if !path.IsAbs(p) || path.Clean(p) != p {
			r.log.Warn("not reconciling a path that is not absolute and clean", "path", p)
			continue
		}

		r.queue.add(p)
	}
}

// HandleEvent queues the paths of the container an event is about, after
// EventDelay, and turns KindResync into ReconcileAll. It never blocks, so
// it can be docker.Watcher's handler.
//
// The paths come from the labels in the event, the only ones left after a
// destroy. They are queued whether or not the labels enable the backup, so
// a container recreated without it frees its paths. The event is kept with
// each path until a pass over it ends without an error, and that pass logs
// it. Problems with the labels of a container that is not gone are logged
// here already, once per set of labels.
func (r *Reconciler) HandleEvent(ev docker.Event) {
	if ev.Kind == docker.KindResync {
		r.ReconcileAll()
		return
	}

	pc := r.parse(docker.Container{ID: ev.ContainerID, Name: ev.Name, Labels: ev.Labels})

	if ev.Kind == docker.KindRemoved {
		r.mu.Lock()
		r.reports++
		r.reported[ev.ContainerID] = labelReport{seq: r.reports, gone: true}
		r.mu.Unlock()
	} else {
		r.reportLabels(pc)
	}

	r.mu.Lock()
	for _, p := range pc.paths {
		r.pending[p] = latest(append(r.pending[p], ev))
	}
	r.mu.Unlock()

	for _, p := range pc.paths {
		r.queue.addAfter(p, r.eventDelay)
	}
}

// takePending takes the events kept for path p.
func (r *Reconciler) takePending(p string) []docker.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	evs := r.pending[p]
	delete(r.pending, p)

	return evs
}

// restorePending puts back events taken for a pass that failed, before
// those that came meanwhile.
func (r *Reconciler) restorePending(p string, evs []docker.Event) {
	if len(evs) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending[p] = latest(append(slices.Clone(evs), r.pending[p]...))
}

// latest returns the last maxPending events of evs.
func latest(evs []docker.Event) []docker.Event {
	if len(evs) <= maxPending {
		return evs
	}

	return slices.Clone(evs[len(evs)-maxPending:])
}

// ReconcileAll asks Run to queue every path: those of every container's
// labels and of every source in the state. It never blocks; requests that
// come while a scan waits are served by one scan.
func (r *Reconciler) ReconcileAll() {
	select {
	case r.scan <- struct{}{}:
	default:
	}
}
