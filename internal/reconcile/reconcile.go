// Package reconcile brings kopia server's sources in line with the labels
// of Docker containers.
//
// # Desired state
//
// The unit of work is a source path: a path of a container's path label,
// joined to HOST_PREFIX and cleaned (labels.SourcePath). A path that is not
// absolute after that, leads out of HOST_PREFIX, or lies outside
// WATCHER_PATH_PREFIX, is a label error and makes no source; a source of
// the state outside WATCHER_PATH_PREFIX is left alone, neither paused nor
// cleaned up. The owners of a path are all containers whose enable
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
// # Snapshots
//
// After the policy, a pass over a source kopia server knows takes the
// snapshots its containers ask for, one at most per pass, with
// Backend.TriggerSnapshot. The labels of the container whose event it is
// decide: a start asks for one if the snapshot-on-start label is true and
// the container still runs; a stop if snapshot-on-stop is true and the
// container has stopped or is gone (its labels come from the event then).
// A die after which the container is restarting is no stop; the event is
// kept until a pass sees the container run again or stay stopped, so a
// crash loop takes no snapshots at its stops. The one-off containers of
// docker compose run take none. A start or stop the watcher missed shows
// in the pause: a pass that pauses a source the watcher had running, or
// lifts its pause, decides as if the event had come. With
// SnapshotOnWatcherStart the first full reconcile of Run makes up a start
// event for every running container; without it, a restart of the
// watcher takes no snapshots.
//
// The pause at a stop is applied first and does not wait for anything. A
// start snapshot is not taken while one is pending or uploading, nor
// before the container's min-interval has passed since the later of the
// last snapshot's start and the watcher's last trigger, which counts a
// snapshot that failed and saved nothing. A stop snapshot is taken
// regardless: while one uploads, kopia server runs it after that one;
// while one is pending, that one counts, since it reads the files after
// the stop. A policy write can make kopia server start a missed scheduled
// snapshot at once, so a pass that wrote the policy puts its start
// snapshot off to a pass EventDelay later, which sees the status after the
// write.
//
// Backend.WaitSnapshot follows each snapshot outside the passes, in a
// goroutine of Run, so a snapshot that runs for hours holds up neither the
// pause nor new labels. The wait is stored in the state with its ticket,
// so it is followed again after a restart, and so is the result until a
// pass has acted on it: logged it, described a saved snapshot, and taken
// a lost one once more. Waits are taken up by the passes over their
// source, so those of another kopia server identity are left alone.
//
// A snapshot is described by the description labels of the path's
// containers, or their names, with their tags in brackets: kopia server
// cannot set snapshot tags through its API. Besides the watcher's own
// snapshots, each full reconcile describes the source's snapshots that
// have no description and started after the mark in the state, scheduled
// ones included; the mark starts at the source's last snapshot when the
// watcher first manages it and moves past every snapshot described, so
// snapshots taken before the watcher stay as they are.
//
// A backend that cannot do one of these yet (backend.ErrNotImplemented) is
// reported once, and the rest goes on without it.
//
// # Hooks
//
// A container can run commands around the snapshots of its sources: its
// pre-exec label before them, its post-exec label after. kopia server's
// schedule would snapshot such a source without them, so the policy keeps
// the scheduling section {"manual": true}, as while paused, also while its
// owners run (backend.Desired.OwnSchedule), and the watcher takes the
// scheduled snapshots itself. It works their times out with policy/resolve
// from the policy the source would have without the hooks and the pause,
// so cron, the interval, the times of day and what the source inherits
// keep kopia server's meaning, and keeps the next time in the state: one
// that passed while the watcher was not running, while no owner ran, or
// while a snapshot with hooks was under way, is taken late only as kopia
// server would take a missed one, with an interval, or with runMissed when
// the next time is more than half an hour away. While no owner runs, no
// scheduled snapshot is taken, and nothing is written to kopia server for
// that; a source whose containers are all gone keeps its schedule for when
// they come back. Once the owners have no hooks, the saved section comes
// back as after a pause.
//
// Every snapshot of such a source the watcher takes, scheduled or at a
// start, is a snapshot with hooks. The pre-exec hooks of the running owners
// run one after the other, outside the passes, each within its timeout;
// when one fails, no snapshot is taken. Otherwise the next pass queues the
// snapshot, which a waiter follows as any other; an error of kopia server
// that is not retried ends the run without a snapshot. Once the snapshot
// has ended, however it ended, or once there is none, the post-exec hooks
// of the running owners run. There is one such snapshot at a time per
// source, and the state keeps how far it got, so that a restart of the
// watcher takes it up: pre-exec hooks a stop of the watcher cut short
// count as failed, post-exec hooks run again. A snapshot at a stop is
// taken without hooks; while pre-exec hooks run, it waits until their
// snapshot is queued, so that it does not read a dump half written.
//
// # Downtime
//
// A container can also be stopped or paused around the snapshots of its
// sources, with its stop-during-backup or pause-during-backup label, which
// puts the sources on the watcher's schedule as a hook does. Once the
// pre-exec hooks have succeeded, a snapshot with hooks stops the running
// owners that ask for it, each with every running container that depends
// on it by the depends-on labels, a container before those it depends on,
// then pauses those that ask for that, each stop waited for as long as
// Docker takes. It queues the snapshot once they are down, and once the
// snapshot has ended, however it ended, it brings them back in the reverse
// order, before the post-exec hooks. A failure to take one down, or a
// depends-on label that names no container or goes round in a circle,
// takes no snapshot. The containers come back max-downtime after they went
// down at the latest, the snapshot done or not: the retry loop of Run sees
// to that, whatever the passes and kopia server do. When the watcher
// stops, they come back at once, within ShutdownGrace, and a snapshot not
// queued yet is not taken; after a crash, at the next start, before any
// other work.
//
// The state lists every container held down, stored before it goes down,
// with the time Docker last started it: a container someone else started
// since, or that is gone, is not brought back. While the state cannot be
// written, nothing is taken down. A container is held for one snapshot at
// a time, until the post-exec hooks of that snapshot have run in it; a
// snapshot with hooks that needs a container held for another, an owner or
// one of the group of a container it stops, waits until it is back, and
// none takes a container down while a hook of another source runs in it.
// One that cannot be brought back is tried again with a growing delay,
// logged, counted and notified.
//
// While it is held, a stopped container counts as a running owner: its
// sources are not paused, and no snapshot at a stop is taken. The events
// the watcher's own stops and starts cause, die, stop and start, are
// dropped, so they take no snapshot at a start or a stop either.
//
// # Manual requests
//
// Trigger asks for a snapshot of the paths of a container, or of one path,
// by hand: the next pass over each path takes it as it takes the snapshot
// at a container's start, with the hooks and the downtime of the owners
// and, unless forced, the minimum interval, and the Trigger it returns
// ends with the snapshot's result, or with why none was taken. Triggers
// are kept in memory only: a stop of the watcher ends them. Sources reports
// what the passes last found about each path, with the state and kopia
// server's status, for the list command. StopOwnersFor stops the running
// owners of a source and those that depend on them for a restore, as a
// snapshot with stop-during-backup does, and brings them back once the
// restore has ended; meanwhile the source takes no snapshot the watcher
// queues.
//
// # Cleanup
//
// The state remembers the owners of each source the watcher manages, with
// what their labels ask of the cleanup: Docker forgets the labels of a
// container it removes. Once a source has no owner left, also after a
// downtime of the watcher in which its containers were removed, the state
// records since when, and the source is paused. cleanup-on-remove decides
// what else becomes of it, the most cautious mode of its last owners, and
// of those that left within ownerLinger before the last pass that saw one
// of them, so that the order in which docker compose down removes them
// does not matter:
//
//   - pause, the default: nothing else;
//   - policy: the policy of a source without snapshots is deleted, and the
//     state forgets the source once kopia server does. A source with
//     snapshots stays paused: without its policy it would inherit the
//     schedule of the host or global policy;
//   - all: the source is deleted with its snapshots and its policy. A
//     pinned snapshot stays unless every owner allowed with
//     cleanup-delete-pinned that it goes: then only the snapshots without
//     a pin are deleted, and the source stays with its paused policy.
//
// Nothing is deleted before CLEANUP_GRACE has passed since the source lost
// its last owner, nor while a snapshot the watcher queued, such as the one
// at the stop, has not finished, nor while a snapshot is pending or
// uploading. A container that backs the source up again before calls the
// cleanup off. Nor does CLEANUP_GRACE run while a container the state
// remembers as an owner still exists, by its ID or its name: Docker labels
// never change, so it lost the path only because the watcher's
// configuration did, and its source is not abandoned; a container
// recreated under its name is the same service. A source kopia server had
// before a container first named it is managed like any other, so all
// deletes the snapshots it had before too. A source outside
// WATCHER_PATH_PREFIX loses its owners on record, so that they cannot
// decide its cleanup should the prefix be widened again. Every deletion is
// logged with the source, the mode and how many snapshots went, and a
// failed one with what it was about to delete, as kopia server may have
// carried it out all the same. A dry run prints what the cleanup would
// delete, and when.
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
//
// # Metrics
//
// Besides the errors, the reconciler reports to Metrics the sources it
// manages. A pass whose policy step succeeded reports its source: paused
// or not, its owners, and the start of its last complete snapshot. That is
// kopia server's last snapshot unless it is a checkpoint of an upload or
// incomplete otherwise, when the source's snapshots are listed, once for
// each such last snapshot; so the snapshots kopia server takes on schedule
// count too, which are most of them. A pass after which the state has no
// record of the source, or kopia server does not know it and the pass did
// not register it, or whose path is outside WATCHER_PATH_PREFIX, drops
// what was reported about the path; a pass that fails before its policy
// step leaves it as it was. The snapshot waiters report how each snapshot
// the watcher queued ended, and the passes the runs kopia server lost
// track of that saved no snapshot. A dry run reports nothing but the
// errors.
//
// The Observer is told how each pass of Run ended, after its calls to
// Metrics, for the notifications; Progress and CheckProgress tell the
// health check whether Run is stuck.
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
	// Inspect returns one container, with the time it last started.
	Inspect(ctx context.Context, id string) (docker.Container, error)
	// Exec runs a hook command in a running container.
	Exec(ctx context.Context, id string, cmd []string) (docker.ExecResult, error)
	// Stop, Start, Pause and Unpause take a container down around a
	// snapshot and bring it back.
	Stop(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Pause(ctx context.Context, id string) error
	Unpause(ctx context.Context, id string) error
}

// Store keeps what was applied to each source, and the containers the
// watcher holds down; *state.Store implements it.
type Store interface {
	Get(src backend.Source) (state.Record, bool)
	Update(src backend.Source, fn func(r *state.Record) error) error
	Delete(src backend.Source) error
	Sources() []backend.Source
	Held() []state.Held
	UpdateHeld(fn func(held []state.Held) ([]state.Held, error)) error
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

// Reasons returns every Reason, in the order above.
func Reasons() []Reason {
	return []Reason{ReasonLabels, ReasonOwnersDisagree, ReasonConfig, ReasonTemporary, ReasonUnavailable,
		ReasonFailed, ReasonDocker, ReasonState}
}

// Options configure a Reconciler.
type Options struct {
	Backend backend.Backend
	Docker  Docker
	Store   Store

	// Logger receives the log; nil discards it.
	Logger *slog.Logger
	// Metrics receives the metrics; nil reports nothing.
	Metrics Metrics
	// Observer is told how the passes of Run end; nil tells nothing.
	Observer Observer

	// HostPrefix is HOST_PREFIX: empty or a clean absolute path.
	HostPrefix string
	// WatcherPathPrefix is WATCHER_PATH_PREFIX: empty or a clean absolute
	// path. Only sources under it are registered, paused and cleaned up: a
	// label path whose source path lies outside it is a label error, and a
	// source of the state outside it is left alone. It applies to source
	// paths, after HostPrefix.
	WatcherPathPrefix string
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

	// SnapshotOnWatcherStart is SNAPSHOT_ON_WATCHER_START: the first full
	// reconcile of Run takes a snapshot of every path with a running
	// container that asks for one at its start, as far as the minimum
	// interval allows.
	SnapshotOnWatcherStart bool

	// CleanupGrace is CLEANUP_GRACE: how long a source must have been
	// without an owner before cleanup-on-remove deletes anything of it;
	// zero means config.DefaultCleanupGrace.
	CleanupGrace time.Duration
}

// Reconciler reconciles source paths. Create it with New, start it with
// Run.
type Reconciler struct {
	backend  backend.Backend
	docker   Docker
	store    Store
	log      *slog.Logger
	metrics  Metrics
	observer Observer

	hostPrefix    string
	watcherPrefix string
	labelPrefix   string
	strict        bool
	dryRun        bool
	output        io.Writer
	printMu       sync.Mutex

	workers                int
	minBackoff, maxBackoff time.Duration
	eventDelay             time.Duration
	restartRecheck         time.Duration
	shutdownGrace          time.Duration
	snapshotOnWatcherStart bool
	cleanupGrace           time.Duration

	// now is the clock of the snapshot steps; tests replace it.
	now func() time.Time

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

	// runCtx is Run's context, which the snapshot waiters run on; nil
	// before Run.
	runCtx context.Context
	// waiters counts the running snapshot waiters, waiting holds their
	// keys.
	waiters sync.WaitGroup
	waiting map[waitKey]bool
	// startScanned is set once the first full reconcile is queued.
	startScanned bool
	// firstScan counts the passes of the first full reconcile until each
	// of its paths had one; nil before and after.
	firstScan *scanSummary
	// deferred holds the paths whose start snapshot a pass put off until
	// the next one; sweepDue those a full reconcile queued.
	deferred map[string]bool
	sweepDue map[string]bool
	// warned holds the keys of the warnings logged once per process.
	warned map[string]bool
	// lastComplete holds per path the last complete snapshot found while
	// the source's last snapshot is incomplete. reportedLost holds the
	// waits whose lost run was reported to Metrics: one entry for each
	// run kopia server lost, which is rare.
	lastComplete map[string]lastComplete
	reportedLost map[waitKey]bool

	// mounts holds the volume paths of the containers last listed or
	// inspected, removed the containers removed since the last full
	// listing; inspects the events of containers with the volumes label
	// whose inspect is due, in their order, and inspectWake signals a new
	// one. See volumes.go.
	mounts      map[string][]string
	removed     map[string]bool
	inspects    []volumeEvent
	inspectWake chan struct{}

	// hookCtx is the context the hook commands run on: Run's passes'
	// context, which ends ShutdownGrace after Run's. hookers counts the
	// goroutines that run hooks, hooksRunning holds the ID of the hook
	// run each runs, by source.
	hookCtx      context.Context
	hookers      sync.WaitGroup
	hooksRunning map[backend.Source]string
	// armed holds per path the scheduled snapshot time this process armed
	// a timer for: a time that passes while it is armed is taken late,
	// one that passed before is a missed one.
	armed map[string]time.Time

	// expected holds by container ID the events that the watcher's own
	// stops and starts of the container cause, which HandleEvent drops;
	// heldWaiters holds by container ID the paths whose snapshot with hooks
	// waits for the container to come back. returnMu lets one goroutine at
	// a time bring containers back; returnWake wakes the loop that tries
	// again those it could not. See downtime.go.
	expected    map[string][]expectation
	expectSeq   uint64
	heldWaiters map[string][]string
	returnMu    sync.Mutex
	returnWake  chan struct{}
	// returnFails counts by container ID the failed tries to bring it
	// back, returnRetry holds when the next one is due; execs counts the
	// hook commands that run in each container.
	returnFails map[string]int
	returnRetry map[string]time.Time
	execs       map[string]int
	// dependsLogged holds by container ID the problems of its depends-on
	// label last reported.
	dependsLogged map[string]string

	// manual holds per path the manual triggers waiting for a pass;
	// manualWaits those whose snapshot a wait follows, manualRuns those of
	// a snapshot with hooks, and runOfWait the run of a wait whose result
	// the triggers of the run take. status holds what the passes last
	// found per path, for Sources. See manual.go and status.go.
	manual      map[string][]*Trigger
	manualWaits map[waitKey][]*Trigger
	manualRuns  map[runKey][]*Trigger
	runOfWait   map[waitKey]string
	status      map[string]*pathStatus

	// dockerTimeout bounds every container listing.
	dockerTimeout time.Duration

	// progressMu guards runStart, passStarts, the start of every running
	// pass by path, and lastPassDone, for Progress.
	progressMu   sync.Mutex
	runStart     time.Time
	passStarts   map[string]time.Time
	lastPassDone time.Time

	// afterPass, if set, is called after every pass of Run; for tests.
	afterPass func(path string, res passResult)
	// afterWait, if set, is called after every snapshot waiter; for tests.
	afterWait func(w state.SnapshotWait)
	// afterHooks, if set, is called after every goroutine that ran hooks;
	// for tests.
	afterHooks func(src backend.Source, phase string)
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

	if p := opts.WatcherPathPrefix; p != "" && (!path.IsAbs(p) || path.Clean(p) != p) {
		return nil, fmt.Errorf("reconcile: watcher path prefix %q is not a clean absolute path", p)
	}

	// A prefix of / is no prefix, as config.ParseHostPrefix and
	// config.ParseWatcherPathPrefix have it.
	if opts.HostPrefix == "/" {
		opts.HostPrefix = ""
	}

	if opts.WatcherPathPrefix == "/" {
		opts.WatcherPathPrefix = ""
	}

	r := &Reconciler{
		backend:        opts.Backend,
		docker:         opts.Docker,
		store:          opts.Store,
		log:            opts.Logger,
		metrics:        opts.Metrics,
		observer:       opts.Observer,
		hostPrefix:     opts.HostPrefix,
		watcherPrefix:  opts.WatcherPathPrefix,
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
		now:            time.Now,
		queue:          newQueue(),
		scan:           make(chan struct{}, 1),
		failures:       map[string]int{},
		reported:       map[string]labelReport{},
		pending:        map[string][]docker.Event{},
		reportedErrors: map[string]map[string]uint64{},
		dockerTimeout:  dockerTimeout,
		waiting:        map[waitKey]bool{},
		warned:         map[string]bool{},
		lastComplete:   map[string]lastComplete{},
		reportedLost:   map[waitKey]bool{},
		deferred:       map[string]bool{},
		sweepDue:       map[string]bool{},
		mounts:         map[string][]string{},
		removed:        map[string]bool{},
		inspectWake:    make(chan struct{}, 1),
		hooksRunning:   map[backend.Source]string{},
		armed:          map[string]time.Time{},
		expected:       map[string][]expectation{},
		heldWaiters:    map[string][]string{},
		dependsLogged:  map[string]string{},
		returnFails:    map[string]int{},
		returnRetry:    map[string]time.Time{},
		execs:          map[string]int{},
		returnWake:     make(chan struct{}, 1),
		passStarts:     map[string]time.Time{},
		manual:         map[string][]*Trigger{},
		manualWaits:    map[waitKey][]*Trigger{},
		manualRuns:     map[runKey][]*Trigger{},
		runOfWait:      map[waitKey]string{},
		status:         map[string]*pathStatus{},

		snapshotOnWatcherStart: opts.SnapshotOnWatcherStart,
		cleanupGrace:           orDefault(opts.CleanupGrace, config.DefaultCleanupGrace),
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

	if r.observer == nil {
		r.observer = noObserver{}
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
// Before anything else, Run brings back the containers the state lists as
// held down for a snapshot, as after a stop or a crash of the watcher
// while they were down; it tries each once, and those it cannot bring back
// are tried again in the background.
//
// When ctx is done, Run takes no more paths and starts no more hooks, and
// gives the passes and hooks that are running ShutdownGrace to finish. The
// containers held down for a snapshot are brought back at once, the
// snapshot done or not, within ShutdownGrace too; those it cannot bring
// back stay in the state for the next start. The snapshot waiters stop at
// once; their waits stay in the state for the next start. Run returns once
// every goroutine it started has returned, the waiters and the hooks after
// the passes; after that it calls neither the store, nor the backend, nor
// Docker, so the caller may close them. Run must be called once.
func (r *Reconciler) Run(ctx context.Context) {
	if !r.running.CompareAndSwap(false, true) {
		panic("reconcile: Run called twice or along with RunOnce")
	}

	// Passes run on their own context, canceled ShutdownGrace after ctx.
	// So do the hooks.
	passCtx, cancelPasses := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelPasses()

	r.mu.Lock()
	r.runCtx = ctx
	r.hookCtx = passCtx
	r.mu.Unlock()

	r.progressMu.Lock()
	r.runStart = time.Now()
	r.progressMu.Unlock()

	// The containers a watcher left down come back before any other work.
	r.returnLeftDown(ctx)

	var wg sync.WaitGroup

	for range r.workers {
		wg.Go(func() { r.work(ctx, passCtx) })
	}

	wg.Go(func() { r.scanLoop(ctx) })
	wg.Go(func() { r.inspectLoop(ctx) })
	wg.Go(func() { r.returnLoop(ctx) })

	<-ctx.Done()
	r.queue.close()

	grace := time.AfterFunc(r.shutdownGrace, cancelPasses)
	defer grace.Stop()

	// The containers held down for a snapshot come back at once, but for
	// those a goroutine of their run brings back itself.
	wg.Go(func() { r.returnHeld(passCtx, true) })

	wg.Wait()

	// The waiters run on ctx and stop at once; their waits stay in the
	// state for the next start. Hooks have ShutdownGrace, like the passes;
	// a hook run cut short is taken up after the next start.
	r.waiters.Wait()
	r.hookers.Wait()

	// What the goroutines of the runs left down, within ShutdownGrace.
	r.returnHeld(passCtx, false)

	r.endManual()
}

// work takes paths until ctx is done and runs their passes with passCtx.
func (r *Reconciler) work(ctx, passCtx context.Context) {
	for {
		p, ok := r.queue.get(ctx)
		if !ok {
			return
		}

		r.passStarted(p)
		res := r.reconcile(passCtx, p)
		r.passDone(passCtx, p, res)
		r.schedule(ctx, p, res)
		r.noteFirstScan(p, res)

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

	if res.recheck {
		r.queue.addAfter(p, r.eventDelay)
	}

	// The watcher's own schedule of a source with hooks.
	if !res.nextRun.IsZero() {
		r.queue.addAfter(p, res.nextRun.Sub(r.now()))
	}

	if !res.wake.IsZero() {
		r.queue.addAfter(p, res.wake.Sub(r.now()))
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
// a container recreated without it frees its paths. An event the watcher
// caused itself, by stopping or starting a container around a snapshot, is
// dropped: it asks for no snapshot and changes no pause. The event is kept with
// each path until a pass over it ends without an error, and that pass logs
// it. Problems with the labels of a container that is not gone are logged
// here already, once per set of labels.
func (r *Reconciler) HandleEvent(ev docker.Event) {
	if ev.Kind == docker.KindResync {
		r.ReconcileAll()
		return
	}

	if r.ownEvent(ev) {
		r.log.Debug("ignoring an event the watcher caused itself", "container", ev.Name, "event", ev.Action)
		return
	}

	pc := r.parseLabels(docker.Container{ID: ev.ContainerID, Name: ev.Name, Labels: ev.Labels})
	volumes := r.hasVolumes(ev.Labels)

	if ev.Kind == docker.KindRemoved {
		r.mu.Lock()
		r.reports++
		r.reported[ev.ContainerID] = labelReport{seq: r.reports, gone: true}
		r.mu.Unlock()
	} else if !volumes {
		// With the volumes label, the inspect reports them with the
		// problems of the mounts.
		r.reportLabels(pc)
	}

	r.mu.Lock()
	for _, p := range pc.paths {
		r.pending[p] = compact(append(r.pending[p], ev))
	}
	r.mu.Unlock()

	for _, p := range pc.paths {
		r.queue.addAfter(p, r.eventDelay)
	}

	if volumes {
		r.handleVolumeEvent(ev, pc.paths)
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

	r.pending[p] = compact(append(slices.Clone(evs), r.pending[p]...))
}

// takeFlags takes the flags kept for path p: whether a pass put its start
// snapshot off, and whether a full reconcile queued it.
func (r *Reconciler) takeFlags(p string) (deferred, sweep bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	deferred, sweep = r.deferred[p], r.sweepDue[p]
	delete(r.deferred, p)
	delete(r.sweepDue, p)

	return deferred, sweep
}

// restoreFlags sets the flags of path p that are true.
func (r *Reconciler) restoreFlags(p string, deferred, sweep bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if deferred {
		r.deferred[p] = true
	}

	if sweep {
		r.sweepDue[p] = true
	}
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
