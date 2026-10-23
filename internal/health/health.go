// Package health tells whether the watcher can do its work, for /healthz.
//
// A Checker runs a check per component every Interval, in the background,
// so /healthz answers at once from what the last checks found and costs
// kopia server and Docker nothing more when it is polled often. A
// component fails once its checks have failed for FailAfter without a
// success in between, so one lost request does not make the watcher
// unhealthy. A component whose check is not set yet, such as Docker while
// the watcher waits for kopia server, is pending, which is no failure.
//
// For StartGrace after the Checker was made a failing component makes the
// watcher starting, not unhealthy: /healthz answers 200 until then, so
// the health does not flap while kopia server and Docker come up.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"
)

// Defaults of Options.
const (
	DefaultInterval   = 15 * time.Second
	DefaultTimeout    = 10 * time.Second
	DefaultFailAfter  = 45 * time.Second
	DefaultStartGrace = 2 * time.Minute
)

// Status is the state of the watcher or of one component.
type Status string

// Statuses of the watcher.
const (
	// StatusOK: every component is ok or pending.
	StatusOK Status = "ok"
	// StatusStarting: a component is failing or pending within
	// StartGrace.
	StatusStarting Status = "starting"
	// StatusUnhealthy: a component is failing after StartGrace.
	StatusUnhealthy Status = "unhealthy"
)

// Statuses of a component; StatusOK too.
const (
	// StatusPending: the component has no check yet, or its first check
	// has not ended.
	StatusPending Status = "pending"
	// StatusFailing: its checks have failed for FailAfter.
	StatusFailing Status = "failing"
)

// CheckFunc checks a component; nil means it is fine. It returns once ctx
// is done.
type CheckFunc func(ctx context.Context) error

// Options configure a Checker. Zero values mean the defaults.
type Options struct {
	// Interval is how often every check runs.
	Interval time.Duration
	// Timeout bounds one check.
	Timeout time.Duration
	// FailAfter is how long the checks of a component must fail before
	// it is failing.
	FailAfter time.Duration
	// StartGrace is how long after New a failing component makes the
	// watcher starting rather than unhealthy.
	StartGrace time.Duration
	// Logger receives a warning when a component starts failing and a
	// message when it recovers; nil discards them.
	Logger *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Checker checks the components of the watcher. Create it with New; it is
// safe for concurrent use.
type Checker struct {
	opts  Options
	start time.Time
	kick  chan struct{}

	mu    sync.Mutex
	comps map[string]*component
}

type component struct {
	check CheckFunc
	// note says why a component without a check is pending.
	note string
	// running is set while a check runs, so a check that ignores its
	// context does not pile up.
	running bool
	checked bool
	err     string
	// lastOK is when a check last succeeded; failingSince is when the
	// checks started failing, zero while the last one succeeded.
	lastOK, failingSince time.Time
	// reported is set once the failure was logged.
	reported bool
}

// New returns a Checker of the named components, each pending with the
// note "not started" until Set gives it a check.
func New(opts Options, names ...string) *Checker {
	if opts.Interval <= 0 {
		opts.Interval = DefaultInterval
	}

	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}

	if opts.FailAfter <= 0 {
		opts.FailAfter = DefaultFailAfter
	}

	if opts.StartGrace <= 0 {
		opts.StartGrace = DefaultStartGrace
	}

	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	c := &Checker{opts: opts, start: opts.Now(), kick: make(chan struct{}, 1), comps: map[string]*component{}}

	for _, n := range names {
		c.comps[n] = &component{note: "not started"}
	}

	return c
}

// Pending makes a component pending with a note, without a check.
func (c *Checker) Pending(name, note string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.comps[name] = &component{note: note}
}

// Record records the result of a check of a component that the caller
// made itself, as the watcher does while it waits for kopia server. The
// component keeps its check, if it has one.
func (c *Checker) Record(name string, err error) {
	c.mu.Lock()
	comp, ok := c.comps[name]

	if !ok {
		comp = &component{}
		c.comps[name] = comp
	}
	c.mu.Unlock()

	c.record(name, comp, err)
}

// Set gives a component its check, which Run runs right away and then
// every Interval. What earlier checks found stays until it runs.
func (c *Checker) Set(name string, check CheckFunc) {
	c.mu.Lock()

	if comp, ok := c.comps[name]; ok && comp.checked {
		comp.check = check
	} else {
		c.comps[name] = &component{check: check}
	}

	c.mu.Unlock()

	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// Run runs the checks every Interval, and when Set adds one, until ctx is
// done. It returns once no check it started runs.
func (c *Checker) Run(ctx context.Context) {
	t := time.NewTicker(c.opts.Interval)
	defer t.Stop()

	for {
		c.CheckNow(ctx)

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-c.kick:
		}
	}
}

// CheckNow runs every check that is not running yet, at the same time, and
// waits for them.
func (c *Checker) CheckNow(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	type job struct {
		name  string
		comp  *component
		check CheckFunc
	}

	var jobs []job

	c.mu.Lock()

	for name, comp := range c.comps {
		if comp.check != nil && !comp.running {
			comp.running = true
			jobs = append(jobs, job{name, comp, comp.check})
		}
	}

	c.mu.Unlock()

	var wg sync.WaitGroup

	for _, j := range jobs {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, c.opts.Timeout)
			err := j.check(cctx)

			cancel()

			if ctx.Err() != nil {
				// Shutting down: the error is not the component's.
				c.mu.Lock()
				j.comp.running = false
				c.mu.Unlock()

				return
			}

			c.record(j.name, j.comp, err)
		})
	}

	wg.Wait()
}

// record stores the result of a check of comp.
func (c *Checker) record(name string, comp *component, err error) {
	now := c.opts.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	comp.running = false

	if c.comps[name] != comp {
		// Pending or Set replaced the component meanwhile.
		return
	}

	comp.checked = true

	if err == nil {
		if comp.reported {
			c.opts.Logger.Info("health check passes again", "component", name)
		}

		comp.err, comp.lastOK, comp.failingSince, comp.reported = "", now, time.Time{}, false

		return
	}

	comp.err = err.Error()

	if comp.failingSince.IsZero() {
		comp.failingSince = now
	}

	if !comp.reported && now.Sub(comp.failingSince) >= c.opts.FailAfter {
		comp.reported = true
		c.opts.Logger.Warn("health check failing", "component", name, "since", comp.failingSince, "error", comp.err)
	}
}

// Report is the health of the watcher, as /healthz serves it.
type Report struct {
	Status     Status                     `json:"status"`
	Components map[string]ComponentReport `json:"components"`
}

// ComponentReport is the health of one component.
type ComponentReport struct {
	Status Status `json:"status"`
	// Note says why the component is pending.
	Note string `json:"note,omitempty"`
	// Error is the error of the last check, also while the component is
	// ok, as its checks have not failed for FailAfter yet.
	Error string `json:"error,omitempty"`
	// LastOK is when a check last succeeded.
	LastOK time.Time `json:"last_ok,omitzero"`
	// FailingSince is when the checks started failing.
	FailingSince time.Time `json:"failing_since,omitzero"`
}

// Report returns the health now.
func (c *Checker) Report() Report {
	now := c.opts.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	r := Report{Status: StatusOK, Components: make(map[string]ComponentReport, len(c.comps))}
	failing, pending := false, false

	for _, name := range slices.Sorted(maps.Keys(c.comps)) {
		comp := c.comps[name]
		cr := ComponentReport{Error: comp.err, LastOK: comp.lastOK, FailingSince: comp.failingSince}

		switch {
		case !comp.checked && comp.check == nil:
			cr.Status, cr.Note = StatusPending, comp.note
		case !comp.checked:
			cr.Status, cr.Note = StatusPending, "first check running"
		case !comp.failingSince.IsZero() && now.Sub(comp.failingSince) >= c.opts.FailAfter:
			cr.Status = StatusFailing
		default:
			cr.Status = StatusOK
		}

		failing = failing || cr.Status == StatusFailing
		pending = pending || cr.Status == StatusPending
		r.Components[name] = cr
	}

	inGrace := now.Sub(c.start) < c.opts.StartGrace

	switch {
	case failing && !inGrace:
		r.Status = StatusUnhealthy
	case (failing || pending) && inGrace:
		r.Status = StatusStarting
	}

	return r
}

// ServeHTTP serves the Report as JSON: 200 unless the watcher is
// unhealthy, 503 then.
func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r := c.Report()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	if r.Status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	_ = json.NewEncoder(w).Encode(r)
}
