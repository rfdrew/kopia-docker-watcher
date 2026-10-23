package metrics

import (
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/rfdrew/kopia-docker-watcher/internal/backend"
	"github.com/rfdrew/kopia-docker-watcher/internal/reconcile"
	"github.com/rfdrew/kopia-docker-watcher/internal/version"
)

// namespace prefixes the names of the watcher's own metrics.
const namespace = "kopia_watcher"

// The states of kopia_watcher_managed_sources.
const (
	stateActive = "active"
	statePaused = "paused"
)

// durationBuckets are the upper bounds of the snapshot duration buckets,
// in seconds: from a second to eight hours.
var durationBuckets = []float64{1, 5, 15, 30, 60, 300, 900, 1800, 3600, 7200, 14400, 28800}

// Metrics holds the metrics the watcher serves on /metrics: its own, which
// the reconciler reports through reconcile.Metrics, and those of the Go
// runtime and the process. Create it with New; it is safe for concurrent
// use.
//
// Only the path of a source and the names of its containers go into
// labels, so the number of series grows with the sources and their
// containers, and with nothing else. The series of a source go when the
// reconciler forgets it.
type Metrics struct {
	reg *prometheus.Registry

	lastSuccess     *prometheus.GaugeVec
	duration        prometheus.Histogram
	failures        *prometheus.CounterVec
	reconcileErrors *prometheus.CounterVec
	managed         *prometheus.GaugeVec
	paused          *prometheus.GaugeVec
	owner           *prometheus.GaugeVec

	// mu guards sources, the sources reported as managed by path, and
	// keeps the series of one source consistent with each other.
	mu      sync.Mutex
	sources map[string]reconcile.SourceMetrics
}

var _ reconcile.Metrics = (*Metrics)(nil)

// New returns Metrics with every series at its start: no source, no
// snapshot, every kind of reconcile error at zero.
func New() *Metrics {
	m := &Metrics{
		reg:     prometheus.NewRegistry(),
		sources: map[string]reconcile.SourceMetrics{},
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "last_success_timestamp_seconds",
			Help: "Start of the last complete snapshot of a source the watcher manages, scheduled ones included, " +
				"in seconds since the epoch. No series while the source has none.",
		}, []string{"path"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "snapshot_duration_seconds",
			Help: "How long the snapshots the watcher queued ran on kopia server, from the start to the end of their task, " +
				"for those that ran to the end: saved, incomplete or with errors, or not saved as nothing changed.",
			Buckets: durationBuckets,
		}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "snapshot_failures_total",
			Help: "Snapshots the watcher queued of a source that saved no complete snapshot: failed, canceled, " +
				"incomplete or with errors, or lost by kopia server, as when it restarts, with no snapshot of the run found.",
		}, []string{"path"}),
		reconcileErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "reconcile_errors_total",
			Help: "Problems of the reconciler by kind. temporary, unavailable and docker are retried and count every " +
				"attempt; the others count when they come up, and again at every full reconcile while they last.",
		}, []string{"kind"}),
		managed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "managed_sources",
			Help:      "Sources the watcher manages, by state: active, or paused while none of their containers runs.",
		}, []string{"state"}),
		paused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "source_paused",
			Help:      "1 while a source the watcher manages is paused, as none of its containers runs; 0 while it is active.",
		}, []string{"path"}),
		owner: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "source_owner",
			Help:      "1 for each container that backs up a source the watcher manages, stopped ones included.",
		}, []string{"path", "container"}),
	}

	buildInfo := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   namespace,
		Name:        "build_info",
		Help:        "The version of the watcher in a label; always 1.",
		ConstLabels: prometheus.Labels{"version": version.Version},
	}, func() float64 { return 1 })

	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo, m.lastSuccess, m.duration, m.failures, m.reconcileErrors, m.managed, m.paused, m.owner,
	)

	// Every kind starts at zero, so that the first error shows as an
	// increase.
	for _, r := range reconcile.Reasons() {
		m.reconcileErrors.WithLabelValues(string(r))
	}

	m.countSources()

	return m
}

// ReconcileError implements reconcile.Metrics: it counts the error by its
// reason. The path is not a label.
func (m *Metrics) ReconcileError(_ string, reason reconcile.Reason) {
	m.reconcileErrors.WithLabelValues(string(reason)).Inc()
}

// Source implements reconcile.Metrics: the source counts as managed,
// active or paused, with a series per owner, and its snapshot failures
// start at zero.
func (m *Metrics) Source(path string, s reconcile.SourceMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, c := range m.sources[path].Owners {
		if !slices.Contains(s.Owners, c) {
			m.owner.DeleteLabelValues(path, c)
		}
	}

	for _, c := range s.Owners {
		m.owner.WithLabelValues(path, c).Set(1)
	}

	m.failures.WithLabelValues(path)

	paused := 0.0
	if s.Paused {
		paused = 1
	}

	m.paused.WithLabelValues(path).Set(paused)

	s.Owners = slices.Clone(s.Owners)
	m.sources[path] = s
	m.countSources()
}

// LastSuccess implements reconcile.Metrics. A zero start removes the
// series; so does nothing for a source not reported as managed.
func (m *Metrics) LastSuccess(path string, start time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.sources[path]; !ok {
		return
	}

	if start.IsZero() {
		m.lastSuccess.DeleteLabelValues(path)
		return
	}

	m.lastSuccess.WithLabelValues(path).Set(float64(start.UnixNano()) / float64(time.Second))
}

// ForgetSource implements reconcile.Metrics: every series of the source
// goes.
func (m *Metrics) ForgetSource(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sources, path)
	m.lastSuccess.DeleteLabelValues(path)
	m.failures.DeleteLabelValues(path)
	m.paused.DeleteLabelValues(path)
	m.owner.DeletePartialMatch(prometheus.Labels{"path": path})
	m.countSources()
}

// SnapshotFinished implements reconcile.Metrics: a snapshot that saved no
// complete snapshot counts as a failure of its source, and the duration of
// one that ran to the end goes into the histogram. It does not wait for
// mu: it is called with the state store locked.
func (m *Metrics) SnapshotFinished(path string, res backend.SnapshotResult) {
	switch res.Outcome {
	case backend.SnapshotSucceeded, backend.SnapshotIdentical:
	default:
		m.failures.WithLabelValues(path).Inc()
	}

	switch res.Outcome {
	case backend.SnapshotSucceeded, backend.SnapshotIdentical, backend.SnapshotIncomplete:
		if !res.TaskStart.IsZero() && !res.TaskEnd.IsZero() && !res.TaskEnd.Before(res.TaskStart) {
			m.duration.Observe(res.TaskEnd.Sub(res.TaskStart).Seconds())
		}
	}
}

// countSources sets managed_sources from sources; mu is held.
func (m *Metrics) countSources() {
	paused := 0

	for _, s := range m.sources {
		if s.Paused {
			paused++
		}
	}

	m.managed.WithLabelValues(stateActive).Set(float64(len(m.sources) - paused))
	m.managed.WithLabelValues(statePaused).Set(float64(paused))
}
