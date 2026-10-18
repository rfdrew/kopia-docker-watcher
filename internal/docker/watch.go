package docker

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// Defaults of Watcher.
const (
	DefaultMinDelay = time.Second
	DefaultMaxDelay = 30 * time.Second

	// defaultSettle is how long a quiet subscription has to stay up to
	// count as established. The daemon answers /events at once and sends
	// its replay right after the headers.
	defaultSettle = 2 * time.Second
)

// Watcher follows container events across broken connections, so the
// watcher never ends up alive but deaf. When a subscription ends it
// subscribes again after an exponential delay, with since set to the time
// of the last event it delivered.
//
// The daemon replays from since inclusive, so the last delivered event
// comes again and is skipped. Its arrival also proves that nothing was
// lost in between: the daemon keeps only its latest 256 events of all
// types, health-check execs included, and loses them when it restarts.
// When a new subscription does not start with that event, or nothing
// arrives within a short settle time, Watcher sends KindResync before
// anything else from it. The first subscription has nothing to resume and
// always starts with KindResync.
type Watcher struct {
	engine   Engine
	log      *slog.Logger
	minDelay time.Duration
	maxDelay time.Duration
	settle   time.Duration
	// wait sleeps for d or until ctx is done; false means ctx is done.
	wait func(ctx context.Context, d time.Duration) bool
}

// NewWatcher returns a Watcher with the default delays. A nil logger
// discards the log.
func NewWatcher(e Engine, log *slog.Logger) *Watcher {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	return &Watcher{
		engine:   e,
		log:      log,
		minDelay: DefaultMinDelay,
		maxDelay: DefaultMaxDelay,
		settle:   defaultSettle,
		wait:     sleep,
	}
}

// Run passes events to handle until ctx is done. handle is called from
// Run's goroutine, one event at a time, and must return quickly: the
// daemon drops events for a subscriber that falls behind.
func (w *Watcher) Run(ctx context.Context, handle func(Event)) {
	var (
		cur   cursor
		delay time.Duration
	)

	for {
		established, err := w.follow(ctx, &cur, handle)
		if ctx.Err() != nil {
			return
		}

		if established || delay == 0 {
			delay = w.minDelay
		} else {
			delay = min(2*delay, w.maxDelay)
		}

		w.log.Warn("docker event stream ended, reconnecting", "error", err.Error(), "delay", delay.String())

		if !w.wait(ctx, delay) {
			return
		}
	}
}

// follow runs one subscription until it ends and reports whether it was
// established: it delivered an event or stayed up for the settle time.
func (w *Watcher) follow(ctx context.Context, cur *cursor, handle func(Event)) (established bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	since := cur.since()
	events, errs := w.engine.Events(ctx, since)

	settle := time.NewTimer(w.settle)
	defer settle.Stop()

	settled := settle.C

	establish := func(first *Event) {
		established, settled = true, nil

		if first != nil && cur.delivered(*first) {
			w.log.Info("docker event stream resumed", sinceAttr(since))
			return
		}

		w.log.Info("docker event stream connected, resyncing", sinceAttr(since))
		handle(Event{Kind: KindResync})
	}

	for {
		select {
		case ev := <-events:
			if !established {
				establish(&ev)
			}

			if cur.delivered(ev) {
				continue
			}

			cur.record(ev)
			w.log.Debug("docker event", "action", ev.Action, "container", ev.ContainerID, "name", ev.Name)
			handle(ev)
		case <-settled:
			establish(nil)
		case err := <-errs:
			if err == nil {
				err = io.EOF
			}

			return established, err
		case <-ctx.Done():
			return established, ctx.Err()
		}
	}
}

// cursor remembers where the delivered events end: the latest event time
// and every event delivered at exactly that time, since several can share
// a nanosecond and all of them are replayed.
type cursor struct {
	last time.Time
	at   map[eventKey]struct{}
}

type eventKey struct {
	action, id string
}

func (c *cursor) since() time.Time {
	return c.last
}

// delivered reports whether ev is one that was already passed on.
func (c *cursor) delivered(ev Event) bool {
	_, ok := c.at[eventKey{ev.Action, ev.ContainerID}]
	return ok && ev.Time.Equal(c.last)
}

func (c *cursor) record(ev Event) {
	switch {
	case ev.Time.After(c.last):
		c.last = ev.Time
		c.at = map[eventKey]struct{}{{ev.Action, ev.ContainerID}: {}}
	case ev.Time.Equal(c.last):
		if c.at == nil {
			c.at = make(map[eventKey]struct{})
		}

		c.at[eventKey{ev.Action, ev.ContainerID}] = struct{}{}
	default:
		// The daemon stamps events before ordering them, so an event can
		// trail the previous one by a little. A replay from the latest
		// time leaves it out, so there is nothing to remember.
	}
}

func sinceAttr(t time.Time) slog.Attr {
	if t.IsZero() {
		return slog.String("since", "now")
	}

	return slog.Time("since", t)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
